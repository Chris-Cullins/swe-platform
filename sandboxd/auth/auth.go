// Package auth implements sandboxd transport authentication and capability
// authorization without depending on a particular environment backend.
package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Capability identifies one sandboxd service boundary.
type Capability string

const (
	CapabilityHealth     Capability = "health"
	CapabilityExec       Capability = "exec"
	CapabilityProcess    Capability = "process"
	CapabilityFilesystem Capability = "filesystem"
	CapabilityTerminal   Capability = "terminal"
	CapabilityPorts      Capability = "ports"
)

// Credential bundle keys and identity annotation shared by provisioners and clients.
const (
	TLSCertKey           = "tls.crt"
	TLSKeyKey            = "tls.key"
	CapabilitiesKey      = "capabilities.json"
	HealthTokenKey       = "health-token"
	ProcessTokenKey      = "process-token"
	IdentityAnnotation   = "swe.dev/sandboxd-identity"
	TrustAnnotation      = "swe.dev/sandboxd-trust"
	TokenAnnotation      = "swe.dev/sandboxd-terminal-token"
	PodUIDAnnotation     = "swe.dev/sandboxd-pod-uid"
	SecretUIDAnnotation  = "swe.dev/sandboxd-secret-uid"
	SecretNameAnnotation = "swe.dev/sandboxd-secret-name"
)

// Grant binds a bearer token to the services it may call.
type Grant struct {
	TokenHash    string       `json:"tokenHash"`
	Capabilities []Capability `json:"capabilities"`
}

// Config is the on-disk capability configuration consumed by sandboxd.
type Config struct {
	Grants []Grant `json:"grants"`
}

// Authorizer authenticates bearer tokens and authorizes gRPC service calls.
type Authorizer struct {
	grants []Grant
}

// Load reads and validates an authorization configuration.
func Load(path string) (*Authorizer, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read capability file: %w", err)
	}
	var config Config
	if err := json.Unmarshal(contents, &config); err != nil {
		return nil, fmt.Errorf("decode capability file: %w", err)
	}
	if len(config.Grants) == 0 {
		return nil, fmt.Errorf("capability file contains no grants")
	}
	for i, grant := range config.Grants {
		verifier, err := hex.DecodeString(grant.TokenHash)
		if err != nil || len(verifier) != sha256.Size || len(grant.Capabilities) == 0 {
			return nil, fmt.Errorf("grant %d requires a SHA-256 token verifier and capabilities", i)
		}
		for _, capability := range grant.Capabilities {
			if !validCapability(capability) {
				return nil, fmt.Errorf("grant %d contains unknown capability %q", i, capability)
			}
		}
	}
	return &Authorizer{grants: config.Grants}, nil
}

// UnaryServerInterceptor enforces authorization for unary RPCs.
func (a *Authorizer) UnaryServerInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := a.authorize(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// StreamServerInterceptor enforces authorization before a streaming RPC starts.
func (a *Authorizer) StreamServerInterceptor(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := a.authorize(stream.Context(), info.FullMethod); err != nil {
		return err
	}
	return handler(srv, stream)
}

func (a *Authorizer) authorize(ctx context.Context, method string) error {
	capability, ok := methodCapability(method)
	if !ok {
		return status.Error(codes.PermissionDenied, "RPC has no configured capability")
	}
	token, err := bearerToken(ctx)
	if err != nil {
		return err
	}
	presented := sha256.Sum256([]byte(token))
	for _, grant := range a.grants {
		verifier, _ := hex.DecodeString(grant.TokenHash)
		if subtle.ConstantTimeCompare(presented[:], verifier) != 1 {
			continue
		}
		for _, granted := range grant.Capabilities {
			if granted == capability {
				return nil
			}
		}
		return status.Errorf(codes.PermissionDenied, "token lacks %s capability", capability)
	}
	return status.Error(codes.Unauthenticated, "invalid bearer token")
}

// TokenVerifier returns the non-reversible verifier stored in sandboxd's
// capability file. Raw high-entropy bearer tokens remain client-only.
func TokenVerifier(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}

func bearerToken(ctx context.Context) (string, error) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") || len(values[0]) == len("Bearer ") {
		return "", status.Error(codes.Unauthenticated, "bearer token required")
	}
	return strings.TrimPrefix(values[0], "Bearer "), nil
}

func methodCapability(method string) (Capability, bool) {
	service := strings.TrimPrefix(method, "/")
	service, _, _ = strings.Cut(service, "/")
	switch service {
	case "sandboxd.v1.HealthService":
		return CapabilityHealth, true
	case "sandboxd.v1.ExecService":
		return CapabilityExec, true
	case "sandboxd.v1.ProcessService":
		return CapabilityProcess, true
	case "sandboxd.v1.FilesystemService":
		return CapabilityFilesystem, true
	case "sandboxd.v1.TerminalService":
		return CapabilityTerminal, true
	case "sandboxd.v1.PortService":
		return CapabilityPorts, true
	default:
		return "", false
	}
}

func validCapability(capability Capability) bool {
	switch capability {
	case CapabilityHealth, CapabilityExec, CapabilityProcess, CapabilityFilesystem, CapabilityTerminal, CapabilityPorts:
		return true
	default:
		return false
	}
}

// BearerCredentials adds a sandboxd capability token to each TLS-protected RPC.
type BearerCredentials struct {
	Token string
}

func (c BearerCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.Token}, nil
}

func (BearerCredentials) RequireTransportSecurity() bool { return true }

var _ credentials.PerRPCCredentials = BearerCredentials{}
