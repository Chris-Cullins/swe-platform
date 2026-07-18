package server

import (
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

// newConn spins up an in-process gRPC server with all services registered
// and returns a client connection to it.
func newConn(t *testing.T, workspace string) *grpc.ClientConn {
	t.Helper()
	socketName := "swe-test-" + filepath.Base(workspace)
	t.Cleanup(func() {
		_ = exec.Command("tmux", "-L", socketName, "kill-server").Run()
	})
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	sandboxdv1.RegisterHealthServiceServer(grpcServer, &HealthServer{Version: "test"})
	sandboxdv1.RegisterExecServiceServer(grpcServer, &ExecServer{Workspace: workspace})
	sandboxdv1.RegisterProcessServiceServer(grpcServer, NewProcessServer(workspace))
	sandboxdv1.RegisterFilesystemServiceServer(grpcServer, &FilesystemServer{Workspace: workspace})
	sandboxdv1.RegisterTerminalServiceServer(grpcServer, &TerminalServer{
		Workspace:  workspace,
		SocketName: socketName,
		Shell:      []string{"sh"},
	})
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

func TestTerminalStreamsSharedTmuxSession(t *testing.T) {
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux is not installed")
	}

	conn := newConn(t, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := sandboxdv1.NewTerminalServiceClient(conn).Terminal(ctx)
	if err != nil {
		t.Fatalf("terminal: %v", err)
	}
	if err := stream.Send(&sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Open{
		Open: &sandboxdv1.TerminalOpen{Cols: 80, Rows: 24},
	}}); err != nil {
		t.Fatalf("send open: %v", err)
	}
	if err := stream.Send(&sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Data{
		Data: []byte("printf terminal-ok; exit\n"),
	}}); err != nil {
		t.Fatalf("send input: %v", err)
	}
	var output []byte
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		output = append(output, message.GetData()...)
	}
	if !strings.Contains(string(output), "terminal-ok") {
		t.Fatalf("terminal output did not contain marker: %q", output)
	}
}

func TestTerminalCloseSendDetaches(t *testing.T) {
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux is not installed")
	}

	conn := newConn(t, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := sandboxdv1.NewTerminalServiceClient(conn).Terminal(ctx)
	if err != nil {
		t.Fatalf("terminal: %v", err)
	}
	if err := stream.Send(&sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Open{
		Open: &sandboxdv1.TerminalOpen{Cols: 80, Rows: 24},
	}}); err != nil {
		t.Fatalf("send open: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close input: %v", err)
	}
	for {
		if _, err := stream.Recv(); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("recv: %v", err)
		}
	}
}

func TestTerminalRequiresOpenFirst(t *testing.T) {
	conn := newConn(t, t.TempDir())
	stream, err := sandboxdv1.NewTerminalServiceClient(conn).Terminal(context.Background())
	if err != nil {
		t.Fatalf("terminal: %v", err)
	}
	if err := stream.Send(&sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Data{Data: []byte("no")}}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
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

func processTestSpec(t *testing.T, workspace, mode string) *sandboxdv1.ProcessSpec {
	t.Helper()
	return &sandboxdv1.ProcessSpec{
		Argv: []string{os.Args[0], "-test.run=TestManagedProcessHelper", "--", mode, workspace},
		Env:  map[string]string{"SANDBOXD_PROCESS_HELPER": "1"},
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for managed process")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestProcessDuplicateStartAndConflict(t *testing.T) {
	workspace := t.TempDir()
	client := sandboxdv1.NewProcessServiceClient(newConn(t, workspace))
	key := &sandboxdv1.ProcessKey{OwnerId: "task-1", Role: "agent"}
	spec := processTestSpec(t, workspace, "wait")

	const callers = 8
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: key, Spec: spec})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("duplicate start: %v", err)
		}
	}
	waitFor(t, func() bool {
		content, _ := os.ReadFile(filepath.Join(workspace, "starts"))
		return strings.Count(string(content), "start\n") == 1
	})

	conflict := processTestSpec(t, workspace, "exit")
	if _, err := client.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: key, Spec: conflict}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("conflicting start: expected FailedPrecondition, got %v", err)
	}
	_, _ = client.Stop(context.Background(), &sandboxdv1.StopProcessRequest{Key: key, Mode: sandboxdv1.StopMode_STOP_MODE_FORCE})
}

func TestProcessRPCancellationDoesNotKillAndStopIsIdempotent(t *testing.T) {
	workspace := t.TempDir()
	client := sandboxdv1.NewProcessServiceClient(newConn(t, workspace))
	key := &sandboxdv1.ProcessKey{OwnerId: "task-2", Role: "agent"}
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := client.Start(ctx, &sandboxdv1.StartProcessRequest{Key: key, Spec: processTestSpec(t, workspace, "wait")}); err != nil {
		t.Fatalf("start: %v", err)
	}
	cancel()
	waitFor(t, func() bool {
		p, err := client.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
		return err == nil && p.State == sandboxdv1.ProcessState_PROCESS_STATE_RUNNING
	})

	stop := &sandboxdv1.StopProcessRequest{Key: key, Mode: sandboxdv1.StopMode_STOP_MODE_FORCE}
	if _, err := client.Stop(context.Background(), stop); err != nil {
		t.Fatalf("first stop: %v", err)
	}
	if _, err := client.Stop(context.Background(), stop); err != nil {
		t.Fatalf("duplicate stop: %v", err)
	}
	waitFor(t, func() bool {
		p, err := client.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
		return err == nil && p.State == sandboxdv1.ProcessState_PROCESS_STATE_EXITED
	})
	absent, err := client.Stop(context.Background(), &sandboxdv1.StopProcessRequest{Key: &sandboxdv1.ProcessKey{OwnerId: "absent", Role: "agent"}})
	if err != nil || absent.State != sandboxdv1.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("stop absent process: process=%v error=%v", absent, err)
	}
}

func TestProcessGetRetainsTerminalState(t *testing.T) {
	workspace := t.TempDir()
	client := sandboxdv1.NewProcessServiceClient(newConn(t, workspace))
	key := &sandboxdv1.ProcessKey{OwnerId: "task-3", Role: "setup"}
	if _, err := client.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: key, Spec: processTestSpec(t, workspace, "exit")}); err != nil {
		t.Fatalf("start: %v", err)
	}
	waitFor(t, func() bool {
		p, err := client.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
		return err == nil && p.State == sandboxdv1.ProcessState_PROCESS_STATE_EXITED && p.ExitCode == 0
	})
	p, err := client.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
	if err != nil || p.State != sandboxdv1.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("retained Get: process=%v error=%v", p, err)
	}
}

func TestProcessServerCloseFencesEpoch(t *testing.T) {
	workspace := t.TempDir()
	server := NewProcessServer(workspace)
	key := &sandboxdv1.ProcessKey{OwnerId: "task-close", Role: "service"}
	if _, err := server.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: key, Spec: processTestSpec(t, workspace, "wait")}); err != nil {
		t.Fatalf("start: %v", err)
	}
	server.Close()
	server.Close() // idempotent
	waitFor(t, func() bool {
		process, err := server.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
		return err == nil && process.State == sandboxdv1.ProcessState_PROCESS_STATE_EXITED
	})
	if _, err := server.Start(context.Background(), &sandboxdv1.StartProcessRequest{
		Key:  &sandboxdv1.ProcessKey{OwnerId: "after-close", Role: "agent"},
		Spec: processTestSpec(t, workspace, "exit"),
	}); status.Code(err) != codes.Unavailable {
		t.Fatalf("start after close: want Unavailable, got %v", err)
	}
}

func TestManagedProcessHelper(t *testing.T) {
	if os.Getenv("SANDBOXD_PROCESS_HELPER") != "1" {
		return
	}
	args := os.Args
	for len(args) > 0 && args[0] != "--" {
		args = args[1:]
	}
	if len(args) != 3 {
		os.Exit(2)
	}
	mode, workspace := args[1], args[2]
	if mode == "exit" {
		return
	}
	f, err := os.OpenFile(filepath.Join(workspace, "starts"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		os.Exit(3)
	}
	_, _ = f.WriteString("start\n")
	_ = f.Close()
	for {
		time.Sleep(time.Second)
	}
}
