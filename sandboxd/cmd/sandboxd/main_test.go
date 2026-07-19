package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/Chris-Cullins/swe-platform/sandboxd/auth"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
	"github.com/Chris-Cullins/swe-platform/sandboxd/internal/server"
)

func TestHealthcheckCallsAuthenticatedHealthRPC(t *testing.T) {
	directory := t.TempDir()
	certificatePath, certificate := writeHealthcheckCertificate(t, directory, "probe.sandboxd.swe.dev")
	validTokenPath := filepath.Join(directory, "valid-token")
	wrongTokenPath := filepath.Join(directory, "wrong-token")
	if err := os.WriteFile(validTokenPath, []byte("health-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(wrongTokenPath, []byte("wrong-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	capabilities, err := json.Marshal(auth.Config{Grants: []auth.Grant{{
		TokenHash: auth.TokenVerifier("health-token"), Capabilities: []auth.Capability{auth.CapabilityHealth},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	capabilitiesPath := filepath.Join(directory, "capabilities.json")
	if err := os.WriteFile(capabilitiesPath, capabilities, 0o600); err != nil {
		t.Fatal(err)
	}
	authorizer, err := auth.Load(capabilitiesPath)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13})),
		grpc.UnaryInterceptor(authorizer.UnaryServerInterceptor),
	)
	sandboxdv1.RegisterHealthServiceServer(grpcServer, &server.HealthServer{Version: "test"})
	go grpcServer.Serve(listener)
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	arguments := []string{"-addr=" + listener.Addr().String(), "-ca=" + certificatePath, "-token=" + validTokenPath}
	if err := healthcheck(arguments); err != nil {
		t.Fatalf("valid healthcheck: %v", err)
	}
	arguments[2] = "-token=" + wrongTokenPath
	if err := healthcheck(arguments); err == nil {
		t.Fatal("healthcheck accepted a wrong capability token")
	}
}

func writeHealthcheckCertificate(t *testing.T, directory, serverName string) (string, tls.Certificate) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: serverName}, DNSNames: []string{serverName},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA: true, BasicConstraintsValid: true,
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
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
	certificatePath := filepath.Join(directory, "tls.crt")
	if err := os.WriteFile(certificatePath, certificatePEM, 0o600); err != nil {
		t.Fatal(err)
	}
	certificate, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return certificatePath, certificate
}
