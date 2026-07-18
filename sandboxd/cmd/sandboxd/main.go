package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
	"github.com/Chris-Cullins/swe-platform/sandboxd/internal/server"
)

// Version is stamped at build time via -ldflags "-X main.Version=...".
var Version = "dev"

func main() {
	if len(os.Args) < 2 || os.Args[1] != "serve" {
		fmt.Fprintln(os.Stderr, "usage: sandboxd serve [-addr :50051] [-workspace /workspace]")
		os.Exit(2)
	}

	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":50051", "gRPC listen address")
	workspace := fs.String("workspace", "/workspace", "default working directory")
	_ = fs.Parse(os.Args[2:])

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}

	grpcServer := grpc.NewServer()
	sandboxdv1.RegisterHealthServiceServer(grpcServer, &server.HealthServer{Version: Version})
	sandboxdv1.RegisterExecServiceServer(grpcServer, &server.ExecServer{Workspace: *workspace})
	sandboxdv1.RegisterFilesystemServiceServer(grpcServer, &server.FilesystemServer{Workspace: *workspace})
	sandboxdv1.RegisterTerminalServiceServer(grpcServer, &server.TerminalServer{Workspace: *workspace})
	sandboxdv1.RegisterPortServiceServer(grpcServer, server.NewPortServer())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		log.Println("shutting down")
		grpcServer.GracefulStop()
	}()

	log.Printf("sandboxd %s listening on %s (workspace %s)", Version, *addr, *workspace)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
