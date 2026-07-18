package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

func TestAuthorizerRejectsUnauthenticatedAndWrongEnvironmentTokens(t *testing.T) {
	authorizer := newTestAuthorizer(t, Config{Grants: []Grant{{
		Token:        "environment-a-token",
		Capabilities: []Capability{CapabilityHealth},
	}}})

	for name, ctx := range map[string]context.Context{
		"missing": context.Background(),
		"wrong environment": metadata.NewIncomingContext(context.Background(), metadata.Pairs(
			"authorization", "Bearer environment-b-token",
		)),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := authorizer.UnaryServerInterceptor(ctx, nil, &grpc.UnaryServerInfo{
				FullMethod: "/sandboxd.v1.HealthService/Check",
			}, func(context.Context, any) (any, error) {
				t.Fatal("unauthorized request reached handler")
				return nil, nil
			})
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("status = %v, want Unauthenticated", err)
			}
		})
	}
}

func TestLimitedCapabilityCannotInvokeUnrelatedService(t *testing.T) {
	authorizer := newTestAuthorizer(t, Config{Grants: []Grant{{
		Token:        "terminal-only-token",
		Capabilities: []Capability{CapabilityTerminal},
	}}})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer terminal-only-token",
	))

	if err := authorizer.authorize(ctx, "/sandboxd.v1.TerminalService/Terminal"); err != nil {
		t.Fatalf("authorize terminal: %v", err)
	}
	if err := authorizer.authorize(ctx, "/sandboxd.v1.ExecService/Exec"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("exec status = %v, want PermissionDenied", err)
	}
}

func TestTLSAndCapabilityInterceptorsEndToEnd(t *testing.T) {
	const (
		serverName = "incarnation-a.sandboxd.swe.dev"
		token      = "environment-a-terminal-token"
	)
	certificate, roots := testCertificate(t, serverName)
	authorizer := newTestAuthorizer(t, Config{Grants: []Grant{{
		Token: token, Capabilities: []Capability{CapabilityHealth, CapabilityTerminal},
	}}})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	execServer := &authorizationTestExecServer{}
	grpcServer := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13})),
		grpc.UnaryInterceptor(authorizer.UnaryServerInterceptor),
		grpc.StreamInterceptor(authorizer.StreamServerInterceptor),
	)
	sandboxdv1.RegisterHealthServiceServer(grpcServer, authorizationTestHealthServer{})
	sandboxdv1.RegisterExecServiceServer(grpcServer, execServer)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dial := func(t *testing.T, name, bearer string) *grpc.ClientConn {
		t.Helper()
		options := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			RootCAs: roots, ServerName: name, MinVersion: tls.VersionTLS13,
		}))}
		if bearer != "" {
			options = append(options, grpc.WithPerRPCCredentials(BearerCredentials{Token: bearer}))
		}
		connection, err := grpc.NewClient(listener.Addr().String(), options...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = connection.Close() })
		return connection
	}

	response, err := sandboxdv1.NewHealthServiceClient(dial(t, serverName, token)).Check(context.Background(), &sandboxdv1.HealthCheckRequest{})
	if err != nil || !response.Ok {
		t.Fatalf("authenticated TLS health call = (%#v, %v)", response, err)
	}
	for name, connection := range map[string]*grpc.ClientConn{
		"missing token":           dial(t, serverName, ""),
		"wrong environment token": dial(t, serverName, "environment-b-token"),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := sandboxdv1.NewHealthServiceClient(connection).Check(context.Background(), &sandboxdv1.HealthCheckRequest{})
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("status = %v, want Unauthenticated", err)
			}
		})
	}
	if _, err := sandboxdv1.NewHealthServiceClient(dial(t, "incarnation-b.sandboxd.swe.dev", token)).Check(context.Background(), &sandboxdv1.HealthCheckRequest{}); err == nil {
		t.Fatal("wrong server incarnation completed TLS handshake")
	}

	exec, err := sandboxdv1.NewExecServiceClient(dial(t, serverName, token)).Exec(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.Send(&sandboxdv1.ExecRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("limited token Exec status = %v, want PermissionDenied", err)
	}
	if execServer.called.Load() {
		t.Fatal("unauthorized stream reached Exec handler")
	}

	plaintext, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plaintext.Close() })
	if _, err := sandboxdv1.NewHealthServiceClient(plaintext).Check(context.Background(), &sandboxdv1.HealthCheckRequest{}); err == nil {
		t.Fatal("plaintext call reached TLS-only sandboxd")
	}
}

type authorizationTestHealthServer struct {
	sandboxdv1.UnimplementedHealthServiceServer
}

func (authorizationTestHealthServer) Check(context.Context, *sandboxdv1.HealthCheckRequest) (*sandboxdv1.HealthCheckResponse, error) {
	return &sandboxdv1.HealthCheckResponse{Ok: true}, nil
}

type authorizationTestExecServer struct {
	sandboxdv1.UnimplementedExecServiceServer
	called atomic.Bool
}

func (s *authorizationTestExecServer) Exec(sandboxdv1.ExecService_ExecServer) error {
	s.called.Store(true)
	return io.EOF
}

func testCertificate(t *testing.T, serverName string) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: serverName}, DNSNames: []string{serverName},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
	certificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificatePEM) {
		t.Fatal("append test certificate")
	}
	return certificate, roots
}

func newTestAuthorizer(t *testing.T, config Config) *Authorizer {
	t.Helper()
	contents, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "capabilities.json")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	authorizer, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return authorizer
}
