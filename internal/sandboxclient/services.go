package sandboxclient

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const servicesPath = ".swe/services.yaml"

type WorkspaceServicesFile struct {
	Data    []byte
	Version string
	Missing bool
}

type filesystemProof struct {
	process          processConnectionProof
	capabilitiesHash [sha256.Size]byte
	tokenHash        [sha256.Size]byte
}

// ReadWorkspaceServices performs one private, unpooled, bounded filesystem RPC.
func (c Connector) ReadWorkspaceServices(ctx context.Context, fence lifecycle.ExecutionFence) (WorkspaceServicesFile, error) {
	env, secret, proof, err := c.resolveProcessTarget(ctx, fence)
	if err != nil {
		return WorkspaceServicesFile{}, err
	}
	filesystemToken := string(secret.Data[sandboxdauth.FilesystemTokenKey])
	if !exactCapability(secret.Data[sandboxdauth.CapabilitiesKey], filesystemToken, sandboxdauth.CapabilityFilesystem) {
		return WorkspaceServicesFile{}, errors.New("sandboxd credential has no exact filesystem capability")
	}
	operationProof := filesystemProof{
		process:          proof,
		capabilitiesHash: sha256.Sum256(secret.Data[sandboxdauth.CapabilitiesKey]),
		tokenHash:        sha256.Sum256([]byte(filesystemToken)),
	}
	opts, err := privateDialOptions(secret.Data[sandboxdauth.TLSCertKey], proof.identity, filesystemToken)
	if err != nil {
		return WorkspaceServicesFile{}, err
	}
	conn, err := grpc.NewClient(env.Status.Endpoints.Sandboxd, opts...)
	if err != nil {
		return WorkspaceServicesFile{}, err
	}
	defer conn.Close()
	response, err := sandboxdv1.NewFilesystemServiceClient(conn).Read(ctx, &sandboxdv1.ReadRequest{Path: servicesPath, MaxBytes: 64 << 10, IncludeVersion: true})
	if status.Code(err) == codes.NotFound {
		if err := c.revalidateFilesystemProof(ctx, fence, operationProof); err != nil {
			return WorkspaceServicesFile{}, err
		}
		return WorkspaceServicesFile{Missing: true}, nil
	}
	if err != nil {
		return WorkspaceServicesFile{}, fmt.Errorf("read repository services file: %w", err)
	}
	if !response.Eof || response.Size > 64<<10 || uint64(len(response.Data)) != response.Size || len(response.Version) != sha256.Size*2 {
		return WorkspaceServicesFile{}, errors.New("sandboxd returned an invalid bounded services file")
	}
	if _, err := hex.DecodeString(response.Version); err != nil {
		return WorkspaceServicesFile{}, errors.New("sandboxd returned an invalid services file version")
	}
	digest := sha256.Sum256(response.Data)
	if response.Version != hex.EncodeToString(digest[:]) {
		return WorkspaceServicesFile{}, errors.New("sandboxd returned services content that does not match its version")
	}
	if err := c.revalidateFilesystemProof(ctx, fence, operationProof); err != nil {
		return WorkspaceServicesFile{}, err
	}
	return WorkspaceServicesFile{Data: append([]byte(nil), response.Data...), Version: response.Version}, nil
}

func (c Connector) revalidateFilesystemProof(ctx context.Context, fence lifecycle.ExecutionFence, proof filesystemProof) error {
	_, secret, current, err := c.resolveProcessTarget(ctx, fence)
	if err != nil {
		return err
	}
	token := string(secret.Data[sandboxdauth.FilesystemTokenKey])
	if !proof.process.matches(current) || proof.capabilitiesHash != sha256.Sum256(secret.Data[sandboxdauth.CapabilitiesKey]) ||
		proof.tokenHash != sha256.Sum256([]byte(token)) || !exactCapability(secret.Data[sandboxdauth.CapabilitiesKey], token, sandboxdauth.CapabilityFilesystem) {
		return errors.New("environment execution changed during sandboxd operation")
	}
	return nil
}

func privateDialOptions(cert []byte, identity, token string) ([]grpc.DialOption, error) {
	roots := x509.NewCertPool()
	if token == "" || !roots.AppendCertsFromPEM(cert) {
		return nil, errors.New("sandboxd private credential is incomplete")
	}
	return []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: identity, MinVersion: tls.VersionTLS13})), grpc.WithPerRPCCredentials(sandboxdauth.BearerCredentials{Token: token})}, nil
}

func exactCapability(data []byte, token string, capability sandboxdauth.Capability) bool {
	if token == "" {
		return false
	}
	var config sandboxdauth.Config
	if json.Unmarshal(data, &config) != nil {
		return false
	}
	want := sandboxdauth.TokenVerifier(token)
	count := 0
	for _, grant := range config.Grants {
		if grant.TokenHash == want {
			count++
			if len(grant.Capabilities) != 1 || grant.Capabilities[0] != capability {
				return false
			}
		}
	}
	return count == 1
}

// ReconcileRepositoryServices publishes a complete declaration-fenced process set.
func (c Connector) ReconcileRepositoryServices(ctx context.Context, fence lifecycle.ExecutionFence, declarations []platformv1alpha1.EnvironmentServiceDeclaration, intentRevision uint64, services []*sandboxdv1.ManagedServiceSpec) error {
	if intentRevision == 0 {
		return errors.New("managed service intent revision must be positive")
	}
	env, _, proof, err := c.resolveProcessTarget(ctx, fence)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(env.Spec.Services, declarations) {
		return errors.New("service declarations changed before process reconcile")
	}
	client, release, err := c.DialProcess(ctx, fence)
	if err != nil {
		return err
	}
	defer release()
	response, err := client.ReconcileManagedServices(ctx, &sandboxdv1.ReconcileManagedServicesRequest{OwnerId: string(fence.EnvironmentUID()), IntentRevision: intentRevision, Services: services})
	if err != nil {
		return fmt.Errorf("reconcile repository services: %w", err)
	}
	if response.OwnerId != string(fence.EnvironmentUID()) || response.IntentRevision != intentRevision {
		return errors.New("sandboxd returned mismatched managed service intent")
	}
	env, _, currentProof, err := c.resolveProcessTarget(ctx, fence)
	if err != nil {
		return err
	}
	if !proof.matches(currentProof) || !reflect.DeepEqual(env.Spec.Services, declarations) {
		return errors.New("service declarations changed during process reconcile")
	}
	return nil
}
