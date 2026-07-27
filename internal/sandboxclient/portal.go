package sandboxclient

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const portalFrameBytes = 64 * 1024

// DialPortal returns an unpooled opaque byte connection to one declared
// service. Infrastructure addresses and credentials never leave the connector.
func (c Connector) DialPortal(ctx, lifetimeCtx context.Context, fence lifecycle.ExecutionFence, snapshot ServiceDeclarationSnapshot, serviceName string) (net.Conn, error) {
	declaration, err := exactServiceDeclaration(snapshot, serviceName)
	if err != nil {
		return nil, err
	}
	observed, err := c.ObserveServices(ctx, fence, snapshot)
	if err != nil || observed.Failed {
		if err != nil {
			return nil, fmt.Errorf("observe portal service: %w", err)
		}
		return nil, fmt.Errorf("observe portal service failed")
	}
	connected := false
	for _, probe := range observed.Probes {
		if probe.Name == serviceName {
			connected = probe.Outcome == ServiceProbeConnected
		}
	}
	if !connected {
		return nil, fmt.Errorf("portal service is not connected")
	}
	if current, currentErr := c.ServiceObservationCurrent(ctx, fence, snapshot, observed); currentErr != nil || !current {
		if currentErr != nil {
			return nil, fmt.Errorf("service observation is stale: %w", currentErr)
		}
		return nil, fmt.Errorf("service observation is stale")
	}

	env, secret, proof, err := c.resolvePortalTarget(ctx, fence, snapshot)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(secret.Data[sandboxdauth.TLSCertKey]) {
		return nil, fmt.Errorf("invalid portal trust certificate")
	}
	streamCtx, cancel := context.WithCancel(lifetimeCtx)
	stopOpenDeadline := context.AfterFunc(ctx, cancel)
	opening := true
	defer func() {
		if opening {
			stopOpenDeadline()
		}
	}()
	grpcConn, err := grpc.NewClient(env.Status.Endpoints.Sandboxd,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: proof.identity, MinVersion: tls.VersionTLS13})),
		grpc.WithPerRPCCredentials(sandboxdauth.BearerCredentials{Token: string(secret.Data[sandboxdauth.PortalTokenKey])}))
	if err != nil {
		cancel()
		return nil, err
	}
	stream, err := sandboxdv1.NewPortalServiceClient(grpcConn).Tunnel(streamCtx)
	if err == nil {
		err = stream.Send(&sandboxdv1.PortalFrame{TargetPort: uint32(declaration.TargetPort)})
	}
	var ack *sandboxdv1.PortalFrame
	if err == nil {
		ack, err = stream.Recv()
	}
	if err != nil || ack == nil || !ack.Opened || ack.TargetPort != 0 || len(ack.Data) != 0 || ack.WriteEof {
		cancel()
		_ = grpcConn.Close()
		if err != nil {
			return nil, fmt.Errorf("open portal tunnel: %w", err)
		}
		return nil, fmt.Errorf("open portal tunnel: invalid acknowledgement")
	}
	_, _, currentProof, proofErr := c.resolvePortalTarget(ctx, fence, snapshot)
	if proofErr != nil || !proof.matches(currentProof) {
		cancel()
		_ = grpcConn.Close()
		if proofErr != nil {
			return nil, fmt.Errorf("post-open portal proof: %w", proofErr)
		}
		return nil, fmt.Errorf("post-open portal target changed")
	}
	if !stopOpenDeadline() {
		cancel()
		_ = grpcConn.Close()
		return nil, fmt.Errorf("open portal tunnel: deadline exceeded")
	}
	opening = false
	return &portalConn{stream: stream, grpcConn: grpcConn, cancel: cancel}, nil
}

func exactServiceDeclaration(snapshot ServiceDeclarationSnapshot, name string) (platformv1alpha1.EnvironmentServiceDeclaration, error) {
	var found *platformv1alpha1.EnvironmentServiceDeclaration
	for i := range snapshot.declarations {
		if snapshot.declarations[i].Name == name {
			if found != nil {
				return platformv1alpha1.EnvironmentServiceDeclaration{}, fmt.Errorf("portal service declaration is not unique")
			}
			found = &snapshot.declarations[i]
		}
	}
	if found == nil || found.TargetPort < 1 || found.TargetPort > 65535 {
		return platformv1alpha1.EnvironmentServiceDeclaration{}, fmt.Errorf("portal service declaration not found")
	}
	return *found, nil
}

type portalProof struct {
	execution                                    Execution
	templateUID                                  types.UID
	templateGeneration                           int64
	templateSpec                                 platformv1alpha1.EnvironmentTemplateSpec
	secretName                                   string
	secretUID                                    types.UID
	secretResourceVersion                        string
	identity, secretIdentity, secretPodUID       string
	certificateHash, capabilitiesHash, tokenHash [sha256.Size]byte
}

func (c Connector) resolvePortalTarget(ctx context.Context, fence lifecycle.ExecutionFence, snapshot ServiceDeclarationSnapshot) (*platformv1alpha1.Environment, *corev1.Secret, portalProof, error) {
	env, pod, err := c.resolvePod(ctx, fence)
	if err != nil {
		return nil, nil, portalProof{}, err
	}
	if !snapshot.Matches(env) {
		return nil, nil, portalProof{}, fmt.Errorf("service declaration snapshot changed")
	}
	var template platformv1alpha1.EnvironmentTemplate
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: env.Spec.TemplateRef}, &template); err != nil {
		return nil, nil, portalProof{}, fmt.Errorf("resolve environment template: %w", err)
	}
	if platformv1alpha1.EffectiveEnvironmentBackend(env, &template) != platformv1alpha1.EnvironmentBackendPod {
		return nil, nil, portalProof{}, fmt.Errorf("environment backend is not supported")
	}
	secretName := pod.Annotations[sandboxdauth.SecretNameAnnotation]
	var secret corev1.Secret
	if secretName == "" {
		return nil, nil, portalProof{}, fmt.Errorf("sandboxd endpoint does not identify its credential")
	}
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: secretName}, &secret); err != nil {
		return nil, nil, portalProof{}, fmt.Errorf("resolve portal credential: %w", err)
	}
	identity := pod.Annotations[sandboxdauth.IdentityAnnotation]
	token := string(secret.Data[sandboxdauth.PortalTokenKey])
	if identity == "" || secret.UID == "" || pod.Annotations[sandboxdauth.SecretUIDAnnotation] != string(secret.UID) || !exactEnvironmentOwner(&secret, env) || secret.Annotations[sandboxdauth.IdentityAnnotation] != identity || secret.Annotations[sandboxdauth.PodUIDAnnotation] != string(pod.UID) {
		return nil, nil, portalProof{}, fmt.Errorf("sandboxd credential does not identify the current environment pod")
	}
	if !exactPortalCapability(secret.Data[sandboxdauth.CapabilitiesKey], token) {
		return nil, nil, portalProof{}, fmt.Errorf("sandboxd credential has no exact portal capability")
	}
	p := portalProof{execution: executionForPod(env, pod), templateUID: template.UID, templateGeneration: template.Generation, templateSpec: template.Spec, secretName: secret.Name, secretUID: secret.UID, secretResourceVersion: secret.ResourceVersion, identity: identity, secretIdentity: secret.Annotations[sandboxdauth.IdentityAnnotation], secretPodUID: secret.Annotations[sandboxdauth.PodUIDAnnotation], certificateHash: sha256.Sum256(secret.Data[sandboxdauth.TLSCertKey]), capabilitiesHash: sha256.Sum256(secret.Data[sandboxdauth.CapabilitiesKey]), tokenHash: sha256.Sum256([]byte(token))}
	return env, &secret, p, nil
}

func exactPortalCapability(contents []byte, token string) bool {
	if token == "" {
		return false
	}
	var config sandboxdauth.Config
	if json.Unmarshal(contents, &config) != nil {
		return false
	}
	found := false
	for _, grant := range config.Grants {
		if grant.TokenHash == sandboxdauth.TokenVerifier(token) {
			if found || len(grant.Capabilities) != 1 || grant.Capabilities[0] != sandboxdauth.CapabilityPortal {
				return false
			}
			found = true
		}
	}
	return found
}

func (p portalProof) matches(q portalProof) bool {
	return p.execution == q.execution && p.templateUID == q.templateUID && p.templateGeneration == q.templateGeneration && reflect.DeepEqual(p.templateSpec, q.templateSpec) && p.secretName == q.secretName && p.secretUID == q.secretUID && p.secretResourceVersion == q.secretResourceVersion && p.identity == q.identity && p.secretIdentity == q.secretIdentity && p.secretPodUID == q.secretPodUID && p.certificateHash == q.certificateHash && p.capabilitiesHash == q.capabilitiesHash && p.tokenHash == q.tokenHash
}

type portalConn struct {
	stream    sandboxdv1.PortalService_TunnelClient
	grpcConn  *grpc.ClientConn
	cancel    context.CancelFunc
	sendMu    sync.Mutex
	readMu    sync.Mutex
	pending   []byte
	closeOnce sync.Once
}

func (c *portalConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for len(c.pending) == 0 {
		frame, err := c.stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, io.EOF
			}
			return 0, err
		}
		if frame.Opened || frame.TargetPort != 0 || frame.WriteEof || len(frame.Data) > portalFrameBytes {
			return 0, fmt.Errorf("invalid portal response frame")
		}
		c.pending = frame.Data
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}
func (c *portalConn) Write(p []byte) (int, error) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	written := 0
	for len(p) > 0 {
		n := len(p)
		if n > portalFrameBytes {
			n = portalFrameBytes
		}
		if err := c.stream.Send(&sandboxdv1.PortalFrame{Data: p[:n]}); err != nil {
			return written, err
		}
		written += n
		p = p[n:]
	}
	return written, nil
}
func (c *portalConn) CloseWrite() error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.stream.Send(&sandboxdv1.PortalFrame{WriteEof: true})
}
func (c *portalConn) Close() error {
	var err error
	c.closeOnce.Do(func() { c.cancel(); err = c.grpcConn.Close() })
	return err
}
func (c *portalConn) LocalAddr() net.Addr  { return portalAddr("portal-client") }
func (c *portalConn) RemoteAddr() net.Addr { return portalAddr("declared-service") }
func (c *portalConn) SetDeadline(time.Time) error {
	return errors.New("portal deadlines are controlled by context")
}
func (c *portalConn) SetReadDeadline(time.Time) error {
	return errors.New("portal deadlines are controlled by context")
}
func (c *portalConn) SetWriteDeadline(time.Time) error {
	return errors.New("portal deadlines are controlled by context")
}

type portalAddr string

func (a portalAddr) Network() string { return "portal" }
func (a portalAddr) String() string  { return string(a) }
