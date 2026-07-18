package server

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

// newConn spins up an in-process gRPC server with all services registered
// and returns a client connection to it.
func newConn(t *testing.T, workspace string) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	sandboxdv1.RegisterHealthServiceServer(grpcServer, &HealthServer{Version: "test"})
	sandboxdv1.RegisterExecServiceServer(grpcServer, &ExecServer{Workspace: workspace})
	sandboxdv1.RegisterFilesystemServiceServer(grpcServer, &FilesystemServer{Workspace: workspace})
	sandboxdv1.RegisterPortServiceServer(grpcServer, NewPortServer())
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufnet: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestHealth(t *testing.T) {
	conn := newConn(t, t.TempDir())
	resp, err := sandboxdv1.NewHealthServiceClient(conn).Check(context.Background(), &sandboxdv1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !resp.Ok || resp.Version != "test" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestExecStreamsOutputAndExitCode(t *testing.T) {
	conn := newConn(t, t.TempDir())
	client := sandboxdv1.NewExecServiceClient(conn)

	var argv []string
	if runtime.GOOS == "windows" {
		argv = []string{"cmd", "/c", "echo hello-sandboxd"}
	} else {
		argv = []string{"sh", "-c", "echo hello-sandboxd"}
	}

	stream, err := client.Exec(context.Background())
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if err := stream.Send(&sandboxdv1.ExecRequest{
		Kind: &sandboxdv1.ExecRequest_Start{Start: &sandboxdv1.ExecStart{Argv: argv}},
	}); err != nil {
		t.Fatalf("send start: %v", err)
	}

	var stdout []byte
	var exit *sandboxdv1.ExecExit
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if out := resp.GetStdout(); out != nil {
			stdout = append(stdout, out...)
		}
		if e := resp.GetExit(); e != nil {
			exit = e
		}
	}

	if exit == nil {
		t.Fatal("no exit message received")
	}
	if exit.Code != 0 || exit.Error != "" {
		t.Fatalf("unexpected exit: %+v", exit)
	}
	if got := string(stdout); !strings.HasPrefix(strings.TrimSpace(got), "hello-sandboxd") {
		t.Fatalf("unexpected stdout: %q", got)
	}
}

func TestFilesystemRoundTrip(t *testing.T) {
	workspace := t.TempDir()
	conn := newConn(t, workspace)
	fs := sandboxdv1.NewFilesystemServiceClient(conn)
	ctx := context.Background()

	if _, err := fs.Write(ctx, &sandboxdv1.WriteRequest{Path: "notes/hello.txt", Content: []byte("hi there")}); err != nil {
		t.Fatalf("write: %v", err)
	}

	read, err := fs.Read(ctx, &sandboxdv1.ReadRequest{Path: "notes/hello.txt"})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(read.Content) != "hi there" {
		t.Fatalf("unexpected content: %q", read.Content)
	}

	list, err := fs.List(ctx, &sandboxdv1.ListRequest{Path: "notes"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Entries) != 1 || list.Entries[0].Name != "hello.txt" {
		t.Fatalf("unexpected entries: %+v", list.Entries)
	}

	// The file must have landed inside the workspace.
	if _, err := os.Stat(filepath.Join(workspace, "notes", "hello.txt")); err != nil {
		t.Fatalf("file not in workspace: %v", err)
	}
}

func TestPortRegistry(t *testing.T) {
	conn := newConn(t, t.TempDir())
	ports := sandboxdv1.NewPortServiceClient(conn)
	ctx := context.Background()

	p, err := ports.Register(ctx, &sandboxdv1.RegisterPortRequest{Port: 0, Label: "web"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if p.Port == 0 {
		t.Fatal("expected an assigned port")
	}

	list, err := ports.List(ctx, &sandboxdv1.ListPortsRequest{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Ports) != 1 || list.Ports[0].Label != "web" {
		t.Fatalf("unexpected ports: %+v", list.Ports)
	}
}
