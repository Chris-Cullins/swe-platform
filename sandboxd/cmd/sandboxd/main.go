package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/Chris-Cullins/swe-platform/sandboxd/auth"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
	"github.com/Chris-Cullins/swe-platform/sandboxd/internal/server"
)

// Version is stamped at build time via -ldflags "-X main.Version=...".
var Version = "dev"

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "healthcheck" {
		if err := healthcheck(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
		return
	}
	if len(os.Args) < 2 || os.Args[1] != "serve" {
		fmt.Fprintln(os.Stderr, "usage: sandboxd serve ... | sandboxd healthcheck -ca FILE -token FILE")
		os.Exit(2)
	}

	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":50051", "gRPC listen address")
	workspace := fs.String("workspace", "/workspace", "default working directory")
	tlsCert := fs.String("tls-cert", "", "TLS server certificate file")
	tlsKey := fs.String("tls-key", "", "TLS server private key file")
	capabilities := fs.String("capabilities", "", "bearer capability configuration file")
	_ = fs.Parse(os.Args[2:])
	if *tlsCert == "" || *tlsKey == "" || *capabilities == "" {
		log.Fatal("-tls-cert, -tls-key, and -capabilities are required")
	}

	certificate, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
	if err != nil {
		log.Fatalf("load TLS identity: %v", err)
	}
	authorizer, err := auth.Load(*capabilities)
	if err != nil {
		log.Fatalf("load capabilities: %v", err)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}

	supervisor := server.NewSupervisor()
	processServer := server.NewProcessServer(*workspace, supervisor)
	filesystemServer, err := server.NewFilesystemServer(*workspace)
	if err != nil {
		log.Fatalf("open workspace filesystem: %v", err)
	}
	grpcServer := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{certificate},
			MinVersion:   tls.VersionTLS13,
		})),
		grpc.UnaryInterceptor(authorizer.UnaryServerInterceptor),
		grpc.StreamInterceptor(authorizer.StreamServerInterceptor),
	)
	sandboxdv1.RegisterHealthServiceServer(grpcServer, &server.HealthServer{Version: Version})
	sandboxdv1.RegisterExecServiceServer(grpcServer, server.NewExecServer(*workspace, supervisor))
	sandboxdv1.RegisterProcessServiceServer(grpcServer, processServer)
	sandboxdv1.RegisterFilesystemServiceServer(grpcServer, filesystemServer)
	sandboxdv1.RegisterTerminalServiceServer(grpcServer, server.NewTerminalServer(*workspace))
	sandboxdv1.RegisterPortServiceServer(grpcServer, server.NewPortServer())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownDone := make(chan struct{})
	go func() {
		<-ctx.Done()
		defer close(shutdownDone)
		log.Println("shutting down")
		filesystemServer.BeginShutdown()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := processServer.CloseContext(shutdownCtx); err != nil {
			log.Printf("sandboxd shutdown fencing failed: %v", err)
		}
		cancel()
		gracefulDone := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(gracefulDone)
		}()
		select {
		case <-gracefulDone:
		case <-time.After(10 * time.Second):
			log.Println("gRPC graceful shutdown timed out; canceling active RPCs")
			grpcServer.Stop()
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := filesystemServer.CloseContext(cleanupCtx); err != nil {
			log.Printf("filesystem shutdown cleanup failed: %v", err)
		}
		cleanupCancel()
	}()

	log.Printf("sandboxd %s listening on %s (workspace %s)", Version, *addr, *workspace)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
	if ctx.Err() != nil {
		<-shutdownDone
	} else if err := filesystemServer.Close(); err != nil {
		log.Printf("close filesystem: %v", err)
	}
}

func healthcheck(args []string) error {
	fs := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:50051", "sandboxd gRPC address")
	caPath := fs.String("ca", "", "sandboxd TLS certificate")
	tokenPath := fs.String("token", "", "health capability token")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *caPath == "" || *tokenPath == "" {
		return fmt.Errorf("-ca and -token are required")
	}
	certificatePEM, err := os.ReadFile(*caPath)
	if err != nil {
		return fmt.Errorf("read health trust certificate: %w", err)
	}
	block, _ := pem.Decode(certificatePEM)
	if block == nil {
		return fmt.Errorf("decode health trust certificate: no PEM certificate")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("parse health trust certificate: %w", err)
	}
	if len(certificate.DNSNames) == 0 {
		return fmt.Errorf("health trust certificate has no DNS identity")
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	token, err := os.ReadFile(*tokenPath)
	if err != nil {
		return fmt.Errorf("read health capability: %w", err)
	}
	connection, err := grpc.NewClient(*addr,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: certificate.DNSNames[0], MinVersion: tls.VersionTLS13})),
		grpc.WithPerRPCCredentials(auth.BearerCredentials{Token: strings.TrimSpace(string(token))}),
	)
	if err != nil {
		return fmt.Errorf("create health client: %w", err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	response, err := sandboxdv1.NewHealthServiceClient(connection).Check(ctx, &sandboxdv1.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("check sandboxd health: %w", err)
	}
	if !response.Ok {
		return fmt.Errorf("sandboxd reported unhealthy")
	}
	return nil
}
