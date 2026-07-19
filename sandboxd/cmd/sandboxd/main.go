package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/Chris-Cullins/swe-platform/sandboxd/auth"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
	"github.com/Chris-Cullins/swe-platform/sandboxd/internal/server"
)

// Version is stamped at build time via -ldflags "-X main.Version=...".
var Version = "dev"

func main() {
	if len(os.Args) < 2 || os.Args[1] != "serve" {
		fmt.Fprintln(os.Stderr, "usage: sandboxd serve [-addr :50051] [-workspace /workspace] -tls-cert FILE -tls-key FILE -capabilities FILE")
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

	processServer := server.NewProcessServer(*workspace)
	grpcServer := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{certificate},
			MinVersion:   tls.VersionTLS13,
		})),
		grpc.UnaryInterceptor(authorizer.UnaryServerInterceptor),
		grpc.StreamInterceptor(authorizer.StreamServerInterceptor),
	)
	sandboxdv1.RegisterHealthServiceServer(grpcServer, &server.HealthServer{Version: Version})
	sandboxdv1.RegisterExecServiceServer(grpcServer, &server.ExecServer{Workspace: *workspace})
	sandboxdv1.RegisterProcessServiceServer(grpcServer, processServer)
	sandboxdv1.RegisterFilesystemServiceServer(grpcServer, &server.FilesystemServer{Workspace: *workspace})
	sandboxdv1.RegisterTerminalServiceServer(grpcServer, &server.TerminalServer{Workspace: *workspace})
	sandboxdv1.RegisterPortServiceServer(grpcServer, server.NewPortServer())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		log.Println("shutting down")
		processServer.Close()
		grpcServer.GracefulStop()
	}()

	log.Printf("sandboxd %s listening on %s (workspace %s)", Version, *addr, *workspace)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
