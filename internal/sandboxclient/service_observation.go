package sandboxclient

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"reflect"
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

// ServiceDeclarationSnapshot is an opaque exact copy of the complete service
// intent and Environment incarnation captured by a caller before observation.
type ServiceDeclarationSnapshot struct {
	key          types.NamespacedName
	uid          types.UID
	generation   int64
	templateRef  string
	backend      platformv1alpha1.EnvironmentBackend
	declarations []platformv1alpha1.EnvironmentServiceDeclaration
}

func CaptureServiceDeclarationSnapshot(env *platformv1alpha1.Environment) ServiceDeclarationSnapshot {
	return ServiceDeclarationSnapshot{key: types.NamespacedName{Namespace: env.Namespace, Name: env.Name}, uid: env.UID,
		generation: env.Generation, templateRef: env.Spec.TemplateRef, backend: env.Spec.Backend,
		declarations: append([]platformv1alpha1.EnvironmentServiceDeclaration(nil), env.Spec.Services...)}
}

func (s ServiceDeclarationSnapshot) Matches(env *platformv1alpha1.Environment) bool {
	return s.key == (types.NamespacedName{Namespace: env.Namespace, Name: env.Name}) && s.uid == env.UID &&
		s.generation == env.Generation && s.templateRef == env.Spec.TemplateRef && s.backend == env.Spec.Backend &&
		reflect.DeepEqual(s.declarations, env.Spec.Services)
}

type ServiceProbeOutcome int

const (
	ServiceProbeConnected ServiceProbeOutcome = iota + 1
	ServiceProbeNotConnected
	ServiceProbeTimedOut
)

type ServiceProbeResult struct {
	Name    string
	Outcome ServiceProbeOutcome
}
type ServiceObservationResult struct {
	// Failed means the RPC had a transport failure or malformed response, but
	// exact post-call execution and intent proof completed successfully.
	Failed bool
	Probes []ServiceProbeResult
	proof  serviceObservationProof
}

type serviceObservationProof struct {
	execution             Execution
	templateUID           types.UID
	templateGeneration    int64
	templateSpec          platformv1alpha1.EnvironmentTemplateSpec
	secretName            string
	secretUID             types.UID
	secretResourceVersion string
	identity              string
	secretIdentity        string
	secretPodUID          string
	certificateHash       [sha256.Size]byte
	capabilitiesHash      [sha256.Size]byte
	tokenHash             [sha256.Size]byte
}

// ObserveServices performs one intent-shaped, observation-only operation. It
// returns data only after proving the exact execution, fence, and declaration
// snapshot after the RPC outcome.
func (c Connector) ObserveServices(ctx context.Context, fence lifecycle.ExecutionFence, snapshot ServiceDeclarationSnapshot) (ServiceObservationResult, error) {
	env, _, secret, proof, err := c.resolveServiceObservationTarget(ctx, fence, snapshot)
	if err != nil {
		return ServiceObservationResult{}, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(secret.Data[sandboxdauth.TLSCertKey]) {
		return ServiceObservationResult{}, fmt.Errorf("invalid service observation trust certificate")
	}
	token := string(secret.Data[sandboxdauth.ServiceObservationTokenKey])
	conn, err := grpc.NewClient(env.Status.Endpoints.Sandboxd,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: proof.identity, MinVersion: tls.VersionTLS13})),
		grpc.WithPerRPCCredentials(sandboxdauth.BearerCredentials{Token: token}))
	if err != nil {
		return ServiceObservationResult{}, err
	}
	defer conn.Close()

	request := &sandboxdv1.ObserveServicesRequest{Probes: make([]*sandboxdv1.ServiceProbe, len(snapshot.declarations))}
	for i, declaration := range snapshot.declarations {
		request.Probes[i] = &sandboxdv1.ServiceProbe{Id: declaration.Name, TargetPort: uint32(declaration.TargetPort), Probe: &sandboxdv1.ServiceProbe_TcpConnect{TcpConnect: &sandboxdv1.TCPConnectProbe{}}}
	}
	// Keep proof budget inside the controller's <=2s outer deadline.
	rpcCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	response, rpcErr := sandboxdv1.NewServiceObservationServiceClient(conn).Observe(rpcCtx, request)
	cancel()
	_, _, _, currentProof, proofErr := c.resolveServiceObservationTarget(ctx, fence, snapshot)
	if proofErr != nil || !proof.matches(currentProof) {
		if proofErr != nil {
			return ServiceObservationResult{}, fmt.Errorf("post-observation proof: %w", proofErr)
		}
		return ServiceObservationResult{}, fmt.Errorf("post-observation target changed")
	}
	if rpcErr != nil {
		return ServiceObservationResult{Failed: true, proof: currentProof}, nil
	}
	if response == nil || len(response.Observations) != len(request.Probes) {
		return ServiceObservationResult{Failed: true, proof: currentProof}, nil
	}
	result := ServiceObservationResult{Probes: make([]ServiceProbeResult, len(response.Observations)), proof: currentProof}
	for i, observation := range response.Observations {
		if observation.Id != request.Probes[i].Id {
			return ServiceObservationResult{Failed: true, proof: currentProof}, nil
		}
		var outcome ServiceProbeOutcome
		switch {
		case observation.Outcome == sandboxdv1.ServiceProbeOutcome_SERVICE_PROBE_OUTCOME_CONNECTED && observation.Reason == sandboxdv1.ServiceProbeReason_SERVICE_PROBE_REASON_CONNECTION_ACCEPTED:
			outcome = ServiceProbeConnected
		case observation.Outcome == sandboxdv1.ServiceProbeOutcome_SERVICE_PROBE_OUTCOME_NOT_CONNECTED && observation.Reason == sandboxdv1.ServiceProbeReason_SERVICE_PROBE_REASON_CONNECTION_FAILED:
			outcome = ServiceProbeNotConnected
		case observation.Outcome == sandboxdv1.ServiceProbeOutcome_SERVICE_PROBE_OUTCOME_TIMED_OUT && (observation.Reason == sandboxdv1.ServiceProbeReason_SERVICE_PROBE_REASON_DEADLINE_EXCEEDED || observation.Reason == sandboxdv1.ServiceProbeReason_SERVICE_PROBE_REASON_CANCELED):
			outcome = ServiceProbeTimedOut
		default:
			return ServiceObservationResult{Failed: true, proof: currentProof}, nil
		}
		result.Probes[i] = ServiceProbeResult{Name: observation.Id, Outcome: outcome}
	}
	return result, nil
}

// ServiceObservationCurrent revalidates an opaque publishable result against
// the complete connector-private execution, backend, TLS, and capability
// boundary immediately before its consumer writes advisory status.
func (c Connector) ServiceObservationCurrent(ctx context.Context, fence lifecycle.ExecutionFence, snapshot ServiceDeclarationSnapshot, result ServiceObservationResult) (bool, error) {
	if result.proof.secretUID == "" {
		return false, nil
	}
	_, _, _, current, err := c.resolveServiceObservationTarget(ctx, fence, snapshot)
	if err != nil {
		return false, err
	}
	return result.proof.matches(current), nil
}

func (c Connector) resolveServiceObservationTarget(ctx context.Context, fence lifecycle.ExecutionFence, snapshot ServiceDeclarationSnapshot) (*platformv1alpha1.Environment, *corev1.Pod, *corev1.Secret, serviceObservationProof, error) {
	env, pod, err := c.resolvePod(ctx, fence)
	if err != nil {
		return nil, nil, nil, serviceObservationProof{}, err
	}
	if !snapshot.Matches(env) {
		return nil, nil, nil, serviceObservationProof{}, fmt.Errorf("service declaration snapshot changed")
	}
	var template platformv1alpha1.EnvironmentTemplate
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: env.Spec.TemplateRef}, &template); err != nil {
		return nil, nil, nil, serviceObservationProof{}, fmt.Errorf("resolve environment template: %w", err)
	}
	if platformv1alpha1.EffectiveEnvironmentBackend(env, &template) != platformv1alpha1.EnvironmentBackendPod {
		return nil, nil, nil, serviceObservationProof{}, fmt.Errorf("environment backend is not supported")
	}
	secretName := pod.Annotations[sandboxdauth.SecretNameAnnotation]
	if secretName == "" {
		return nil, nil, nil, serviceObservationProof{}, fmt.Errorf("sandboxd endpoint does not identify its credential")
	}
	var secret corev1.Secret
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: secretName}, &secret); err != nil {
		return nil, nil, nil, serviceObservationProof{}, fmt.Errorf("resolve service observation credential: %w", err)
	}
	identity := pod.Annotations[sandboxdauth.IdentityAnnotation]
	token := string(secret.Data[sandboxdauth.ServiceObservationTokenKey])
	if identity == "" || secret.UID == "" || pod.Annotations[sandboxdauth.SecretUIDAnnotation] != string(secret.UID) ||
		!exactEnvironmentOwner(&secret, env) || secret.Annotations[sandboxdauth.IdentityAnnotation] != identity ||
		secret.Annotations[sandboxdauth.PodUIDAnnotation] != string(pod.UID) {
		return nil, nil, nil, serviceObservationProof{}, fmt.Errorf("sandboxd credential does not identify the current environment pod")
	}
	if !serviceObservationCapabilityGranted(secret.Data[sandboxdauth.CapabilitiesKey], token) {
		return nil, nil, nil, serviceObservationProof{}, fmt.Errorf("sandboxd credential has no exact service observation capability")
	}
	proof := serviceObservationProof{
		execution: executionForPod(env, pod), templateUID: template.UID, templateGeneration: template.Generation,
		templateSpec: template.Spec, secretName: secret.Name, secretUID: secret.UID, secretResourceVersion: secret.ResourceVersion,
		identity: identity, secretIdentity: secret.Annotations[sandboxdauth.IdentityAnnotation], secretPodUID: secret.Annotations[sandboxdauth.PodUIDAnnotation],
		certificateHash: sha256.Sum256(secret.Data[sandboxdauth.TLSCertKey]), capabilitiesHash: sha256.Sum256(secret.Data[sandboxdauth.CapabilitiesKey]), tokenHash: sha256.Sum256([]byte(token)),
	}
	return env, pod, &secret, proof, nil
}

func (p serviceObservationProof) matches(other serviceObservationProof) bool {
	return p.execution == other.execution && p.templateUID == other.templateUID && p.templateGeneration == other.templateGeneration &&
		reflect.DeepEqual(p.templateSpec, other.templateSpec) && p.secretName == other.secretName && p.secretUID == other.secretUID &&
		p.secretResourceVersion == other.secretResourceVersion && p.identity == other.identity && p.secretIdentity == other.secretIdentity &&
		p.secretPodUID == other.secretPodUID && p.certificateHash == other.certificateHash && p.capabilitiesHash == other.capabilitiesHash && p.tokenHash == other.tokenHash
}

func serviceObservationCapabilityGranted(contents []byte, token string) bool {
	if token == "" {
		return false
	}
	var config sandboxdauth.Config
	if json.Unmarshal(contents, &config) != nil {
		return false
	}
	verifier := sandboxdauth.TokenVerifier(token)
	for _, grant := range config.Grants {
		if grant.TokenHash == verifier {
			return len(grant.Capabilities) == 1 && grant.Capabilities[0] == sandboxdauth.CapabilityServiceObservation
		}
	}
	return false
}
