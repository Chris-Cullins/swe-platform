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
	"google.golang.org/grpc/metadata"
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
	supervisor := NewSupervisor()
	processServer := NewProcessServer(workspace, supervisor)
	sandboxdv1.RegisterHealthServiceServer(grpcServer, &HealthServer{Version: "test"})
	sandboxdv1.RegisterExecServiceServer(grpcServer, NewExecServer(workspace, supervisor))
	sandboxdv1.RegisterProcessServiceServer(grpcServer, processServer)
	sandboxdv1.RegisterFilesystemServiceServer(grpcServer, &FilesystemServer{Workspace: workspace})
	sandboxdv1.RegisterTerminalServiceServer(grpcServer, &TerminalServer{
		Workspace:  workspace,
		SocketName: socketName,
		Shell:      []string{"sh"},
	})
	sandboxdv1.RegisterPortServiceServer(grpcServer, NewPortServer())
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(func() { grpcServer.Stop(); processServer.Close() })

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
			stdout = append(stdout, out.Data...)
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

func TestExecExplicitAndCloseSendEOFStillPublishOutputAndExit(t *testing.T) {
	for _, explicit := range []bool{true, false} {
		t.Run(map[bool]string{true: "explicit", false: "close-send"}[explicit], func(t *testing.T) {
			client := sandboxdv1.NewExecServiceClient(newConn(t, t.TempDir()))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			stream, err := client.Exec(ctx)
			if err != nil {
				t.Fatal(err)
			}
			argv := []string{"sh", "-c", "cat >/dev/null; printf after-eof"}
			if runtime.GOOS == "windows" {
				argv = []string{"cmd", "/c", "more >nul & echo after-eof"}
			}
			if err := stream.Send(&sandboxdv1.ExecRequest{Kind: &sandboxdv1.ExecRequest_Start{Start: &sandboxdv1.ExecStart{Argv: argv}}}); err != nil {
				t.Fatal(err)
			}
			if explicit {
				if err := stream.Send(&sandboxdv1.ExecRequest{Kind: &sandboxdv1.ExecRequest_StdinEof{StdinEof: &sandboxdv1.ExecStdinEOF{}}}); err != nil {
					t.Fatal(err)
				}
			} else if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
			var out []byte
			var exit *sandboxdv1.ExecExit
			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if c := resp.GetStdout(); c != nil {
					out = append(out, c.Data...)
				}
				if e := resp.GetExit(); e != nil {
					exit = e
				}
			}
			if strings.TrimSpace(string(out)) != "after-eof" || exit == nil || exit.Code != 0 || exit.Reason != sandboxdv1.TerminationReason_TERMINATION_REASON_EXITED {
				t.Fatalf("output=%q exit=%v", out, exit)
			}
		})
	}
}

func TestProcessBoundedOutputOffsetsGapEOFAndStaleID(t *testing.T) {
	s := NewProcessServer(t.TempDir())
	s.OutputCapacity = 5
	t.Cleanup(s.Close)
	key := &sandboxdv1.ProcessKey{OwnerId: "output", Role: "test"}
	p, err := s.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: key, Spec: &sandboxdv1.ProcessSpec{Argv: []string{"sh", "-c", "printf 0123456789"}}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		got, _ := s.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
		return got != nil && got.State == sandboxdv1.ProcessState_PROCESS_STATE_EXITED
	})
	got, err := s.ReadOutput(context.Background(), &sandboxdv1.ReadOutputRequest{Key: key, ExecutionId: p.ExecutionId, Stream: sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT, MaxBytes: 99})
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Data) != "56789" || got.Offset != 5 || got.NextOffset != 10 || got.GapBytes != 5 || got.RetainedStart != 5 || got.ProducedEnd != 10 || !got.Eof {
		t.Fatalf("unexpected retained output: %+v", got)
	}
	_, err = s.ReadOutput(context.Background(), &sandboxdv1.ReadOutputRequest{Key: key, ExecutionId: "stale", Stream: sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale id: %v", err)
	}
}

func TestNormalizeEnvReplaceAndStable(t *testing.T) {
	env, err := normalizeEnv(sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_REPLACE, map[string]string{"B": "2", "A": "1"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(env, ",") != "A=1,B=2" {
		t.Fatalf("replace env = %v", env)
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
	ids := make(chan string, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			process, err := client.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: key, Spec: spec})
			if process != nil {
				ids <- process.ExecutionId
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	close(ids)
	for err := range errs {
		if err != nil {
			t.Fatalf("duplicate start: %v", err)
		}
	}
	var executionID string
	for id := range ids {
		if id == "" {
			t.Fatal("duplicate Start returned an empty execution ID")
		}
		if executionID != "" && id != executionID {
			t.Fatalf("duplicate Start IDs differ: %q and %q", executionID, id)
		}
		executionID = id
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
	waitFor(t, func() bool {
		p, _ := client.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
		return p != nil && p.State == sandboxdv1.ProcessState_PROCESS_STATE_EXITED
	})
	retry, err := client.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: key, Spec: spec})
	if err != nil || retry.ExecutionId != executionID {
		t.Fatalf("terminal retry: process=%v error=%v", retry, err)
	}
	content, _ := os.ReadFile(filepath.Join(workspace, "starts"))
	if strings.Count(string(content), "start\n") != 1 {
		t.Fatalf("terminal retry relaunched process: %q", content)
	}
}

func TestProcessRecordLimitPreservesRetry(t *testing.T) {
	s := NewProcessServer(t.TempDir())
	s.MaxRecords = 1
	t.Cleanup(s.Close)
	key := &sandboxdv1.ProcessKey{OwnerId: "existing", Role: "agent"}
	spec := &sandboxdv1.ProcessSpec{Argv: []string{"sh", "-c", "exit 0"}}
	first, err := s.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: key, Spec: spec})
	if err != nil || first.ExecutionId == "" {
		t.Fatalf("first Start: %v %v", first, err)
	}
	_, err = s.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: &sandboxdv1.ProcessKey{OwnerId: "new", Role: "agent"}, Spec: spec})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("new key: %v", err)
	}
	retry, err := s.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: key, Spec: spec})
	if err != nil || retry.ExecutionId != first.ExecutionId {
		t.Fatalf("retry: %v %v", retry, err)
	}
}

func TestProcessOutputPaginationAndBothStreams(t *testing.T) {
	s := NewProcessServer(t.TempDir())
	t.Cleanup(s.Close)
	key := &sandboxdv1.ProcessKey{OwnerId: "pages", Role: "agent"}
	p, err := s.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: key, Spec: &sandboxdv1.ProcessSpec{Argv: []string{"sh", "-c", "printf abcdef; printf 12345 >&2"}}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		got, _ := s.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
		return got != nil && got.State == sandboxdv1.ProcessState_PROCESS_STATE_EXITED
	})
	for stream, want := range map[sandboxdv1.OutputStream]string{sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT: "abcdef", sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR: "12345"} {
		var got []byte
		var cursor uint64
		for {
			page, err := s.ReadOutput(context.Background(), &sandboxdv1.ReadOutputRequest{Key: key, ExecutionId: p.ExecutionId, Stream: stream, Offset: cursor, MaxBytes: 2})
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Data) > 2 || page.Offset != cursor || page.GapBytes != 0 {
				t.Fatalf("bad page: %+v", page)
			}
			got = append(got, page.Data...)
			cursor = page.NextOffset
			if page.Eof {
				break
			}
		}
		if string(got) != want {
			t.Fatalf("stream %s = %q", stream, got)
		}
	}
}

func TestForegroundAndServiceProcessShapes(t *testing.T) {
	t.Run("foreground", func(t *testing.T) {
		s := NewProcessServer(t.TempDir())
		t.Cleanup(s.Close)
		key := &sandboxdv1.ProcessKey{OwnerId: "fg", Role: "agent"}
		p, err := s.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: key, Spec: &sandboxdv1.ProcessSpec{Argv: []string{"sh", "-c", "printf foreground"}}})
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool {
			x, _ := s.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
			return x != nil && x.State == sandboxdv1.ProcessState_PROCESS_STATE_EXITED
		})
		out, err := s.ReadOutput(context.Background(), &sandboxdv1.ReadOutputRequest{Key: key, ExecutionId: p.ExecutionId, Stream: sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT})
		if err != nil || string(out.Data) != "foreground" || !out.Eof {
			t.Fatalf("output=%v err=%v", out, err)
		}
	})
	t.Run("service", func(t *testing.T) {
		workspace := t.TempDir()
		s := NewProcessServer(workspace)
		t.Cleanup(s.Close)
		key := &sandboxdv1.ProcessKey{OwnerId: "svc", Role: "service"}
		ctx, cancel := context.WithCancel(context.Background())
		p, err := s.Start(ctx, &sandboxdv1.StartProcessRequest{Key: key, Spec: processTestSpec(t, workspace, "wait")})
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		again, err := s.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
		if err != nil || again.ExecutionId != p.ExecutionId || again.State != sandboxdv1.ProcessState_PROCESS_STATE_RUNNING {
			t.Fatalf("reconnect=%v err=%v", again, err)
		}
		_, err = s.Stop(context.Background(), &sandboxdv1.StopProcessRequest{Key: key, Mode: sandboxdv1.StopMode_STOP_MODE_FORCE})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestTimeoutOverflowValidation(t *testing.T) {
	overflow := uint64(1<<63-1)/uint64(time.Millisecond) + 1
	s := NewProcessServer(t.TempDir())
	t.Cleanup(s.Close)
	_, err := s.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: &sandboxdv1.ProcessKey{OwnerId: "x", Role: "x"}, Spec: &sandboxdv1.ProcessSpec{Argv: []string{"sh"}, TimeoutMs: overflow}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ProcessSpec overflow: %v", err)
	}
	client := sandboxdv1.NewExecServiceClient(newConn(t, t.TempDir()))
	stream, err := client.Exec(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&sandboxdv1.ExecRequest{Kind: &sandboxdv1.ExecRequest_Start{Start: &sandboxdv1.ExecStart{Argv: []string{"sh"}, TimeoutMs: overflow}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ExecStart overflow: %v", err)
	}
}

func TestExecRejectsUnknownAndUnspecifiedControls(t *testing.T) {
	for _, control := range []sandboxdv1.ProcessControl{sandboxdv1.ProcessControl_PROCESS_CONTROL_UNSPECIFIED, sandboxdv1.ProcessControl(99)} {
		client := sandboxdv1.NewExecServiceClient(newConn(t, t.TempDir()))
		stream, err := client.Exec(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&sandboxdv1.ExecRequest{Kind: &sandboxdv1.ExecRequest_Start{Start: &sandboxdv1.ExecStart{Argv: []string{"sh", "-c", "sleep 10"}}}}); err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&sandboxdv1.ExecRequest{Kind: &sandboxdv1.ExecRequest_Control{Control: &sandboxdv1.ExecControl{Control: control}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("control %d: %v", control, err)
		}
	}
}

type blockedExecStream struct {
	sandboxdv1.ExecService_ExecServer
	ctx       context.Context
	start     *sandboxdv1.ExecRequest
	once      sync.Once
	release   chan struct{}
	mu        sync.Mutex
	responses []*sandboxdv1.ExecResponse
}

func (s *blockedExecStream) Context() context.Context     { return s.ctx }
func (s *blockedExecStream) SetHeader(metadata.MD) error  { return nil }
func (s *blockedExecStream) SendHeader(metadata.MD) error { return nil }
func (s *blockedExecStream) SetTrailer(metadata.MD)       {}
func (s *blockedExecStream) SendMsg(any) error            { return nil }
func (s *blockedExecStream) RecvMsg(any) error            { return io.EOF }
func (s *blockedExecStream) Recv() (*sandboxdv1.ExecRequest, error) {
	var result *sandboxdv1.ExecRequest
	s.once.Do(func() { result = s.start })
	if result != nil {
		return result, nil
	}
	return nil, io.EOF
}
func (s *blockedExecStream) Send(r *sandboxdv1.ExecResponse) error {
	if r.GetStdout() != nil || r.GetStderr() != nil {
		<-s.release
	}
	s.mu.Lock()
	s.responses = append(s.responses, r)
	s.mu.Unlock()
	return nil
}

func TestExecOutputLossIsObservable(t *testing.T) {
	stream := &blockedExecStream{ctx: context.Background(), release: make(chan struct{}), start: &sandboxdv1.ExecRequest{Kind: &sandboxdv1.ExecRequest_Start{Start: &sandboxdv1.ExecStart{Argv: []string{"sh", "-c", "head -c 4194304 /dev/zero"}}}}}
	done := make(chan error, 1)
	go func() { done <- NewExecServer(t.TempDir(), NewSupervisor()).Exec(stream) }()
	time.Sleep(150 * time.Millisecond)
	close(stream.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var delivered, gaps uint64
	for _, r := range stream.responses {
		if out := r.GetStdout(); out != nil {
			delivered += uint64(len(out.Data))
			gaps += out.GapBytes
		}
	}
	if delivered+gaps != 4194304 || gaps == 0 {
		t.Fatalf("delivered=%d gaps=%d", delivered, gaps)
	}
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
		return err == nil && p.State == sandboxdv1.ProcessState_PROCESS_STATE_EXITED && p.GetExitCode() == 0
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
