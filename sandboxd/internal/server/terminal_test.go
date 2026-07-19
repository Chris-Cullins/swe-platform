package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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

func TestTerminalHandlerTranslatesProtocolToBackend(t *testing.T) {
	backend := newFakeTerminalBackend()
	conn := newTerminalConn(t, backend)
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
	opened := receive(t, backend.opened)
	if opened != [2]uint32{80, 24} {
		t.Fatalf("unexpected open dimensions: %v", opened)
	}

	input := []byte{0x00, 0xff, '\n', '\\'}
	if err := stream.Send(&sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Data{Data: input}}); err != nil {
		t.Fatalf("send data: %v", err)
	}
	if got := receive(t, backend.session.writes); !bytes.Equal(got, input) {
		t.Fatalf("backend input = %v, want %v", got, input)
	}

	if err := stream.Send(&sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Resize{
		Resize: &sandboxdv1.TerminalResize{Cols: 132, Rows: 43},
	}}); err != nil {
		t.Fatalf("send resize: %v", err)
	}
	if got := receive(t, backend.session.resizes); got != [2]uint32{132, 43} {
		t.Fatalf("backend resize = %v", got)
	}

	output := []byte{0xfe, 0x00, 'o', 'k'}
	backend.session.reads <- terminalRead{data: output}
	message, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive output: %v", err)
	}
	if got := message.GetData(); !bytes.Equal(got, output) {
		t.Fatalf("client output = %v, want %v", got, output)
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close input: %v", err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("receive after close = %v, want EOF", err)
	}
	receive(t, backend.session.closed)
}

func TestTerminalHandlerRejectsInvalidResize(t *testing.T) {
	backend := newFakeTerminalBackend()
	conn := newTerminalConn(t, backend)
	stream, err := sandboxdv1.NewTerminalServiceClient(conn).Terminal(context.Background())
	if err != nil {
		t.Fatalf("terminal: %v", err)
	}
	if err := stream.Send(&sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Open{
		Open: &sandboxdv1.TerminalOpen{Cols: 80, Rows: 24},
	}}); err != nil {
		t.Fatalf("send open: %v", err)
	}
	receive(t, backend.opened)
	if err := stream.Send(&sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Resize{
		Resize: &sandboxdv1.TerminalResize{Cols: 0, Rows: 24},
	}}); err != nil {
		t.Fatalf("send resize: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("receive = %v, want InvalidArgument", err)
	}
}

func TestTerminalHandlerCancelsBackend(t *testing.T) {
	backend := newFakeTerminalBackend()
	conn := newTerminalConn(t, backend)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := sandboxdv1.NewTerminalServiceClient(conn).Terminal(ctx)
	if err != nil {
		t.Fatalf("terminal: %v", err)
	}
	if err := stream.Send(&sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Open{
		Open: &sandboxdv1.TerminalOpen{Cols: 80, Rows: 24},
	}}); err != nil {
		t.Fatalf("send open: %v", err)
	}
	receive(t, backend.opened)
	cancel()
	receive(t, backend.session.contextDone)
	receive(t, backend.session.closed)
}

func TestTerminalHandlerCancelsBackendAfterSendFailure(t *testing.T) {
	backend := newFakeTerminalBackend()
	backend.session.reads <- terminalRead{data: []byte("output")}
	sendErr := errors.New("send failed")
	stream := &failingTerminalStream{
		open:        &sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Open{Open: &sandboxdv1.TerminalOpen{Cols: 80, Rows: 24}}},
		backendDone: backend.session.contextDone,
		sendErr:     sendErr,
	}

	err := (&TerminalServer{backend: backend}).Terminal(stream)
	if !errors.Is(err, sendErr) {
		t.Fatalf("terminal error = %v, want %v", err, sendErr)
	}
	receive(t, backend.session.contextDone)
	receive(t, backend.session.closed)
}

func TestTmuxSessionEncodesBinaryInputAndResize(t *testing.T) {
	if got := sendKeysCommand([]byte{0x00, 0xff, '\n', '\\'}); got != "send-keys -t swe: -H 00 ff 0a 5c" {
		t.Fatalf("binary input command = %q", got)
	}

	var controlMode bytes.Buffer
	session := &tmuxTerminalSession{stdin: nopWriteCloser{Writer: &controlMode}}
	input := make([]byte, 513)
	for i := range input {
		input[i] = byte(i)
	}
	if err := session.Write(input); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := session.Resize(120, 40); err != nil {
		t.Fatalf("resize: %v", err)
	}
	lines := bytes.Split(bytes.TrimSpace(controlMode.Bytes()), []byte("\n"))
	if len(lines) != 3 {
		t.Fatalf("control-mode commands = %d, want 3", len(lines))
	}
	if got := string(lines[0]); got != sendKeysCommand(input[:tmuxSendKeysChunkBytes]) {
		t.Fatalf("first input command was not binary-safe")
	}
	if got := string(lines[1]); got != sendKeysCommand(input[tmuxSendKeysChunkBytes:]) {
		t.Fatalf("second input command = %q", got)
	}
	if got := string(lines[2]); got != "refresh-client -C 120,40" {
		t.Fatalf("resize command = %q", got)
	}
}

func TestTmuxSessionCloseUnblocksWrite(t *testing.T) {
	stdin := &blockingWriteCloser{
		started: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	session := &tmuxTerminalSession{stdin: stdin}
	writeDone := make(chan error, 1)
	go func() { writeDone <- session.Write([]byte("blocked")) }()
	receive(t, stdin.started)

	closeDone := make(chan error, 1)
	go func() { closeDone <- session.Close() }()
	if err := receive(t, closeDone); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := receive(t, writeDone); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write error = %v, want closed pipe", err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if err := session.Resize(120, 40); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("resize after close = %v, want closed pipe", err)
	}
}

func TestTmuxOutputDecodesBinaryData(t *testing.T) {
	got, ok := tmuxOutput("%output %0 A\\000\\134" + string([]byte{0xff}) + "Z")
	if !ok {
		t.Fatal("expected output record")
	}
	want := []byte{'A', 0x00, '\\', 0xff, 'Z'}
	if !bytes.Equal(got, want) {
		t.Fatalf("decoded output = %v, want %v", got, want)
	}
	got, ok = tmuxOutput("%output %0 trailing\\")
	if !ok || string(got) != "trailing\\" {
		t.Fatalf("decoded trailing backslash = (%q, %t), want preserved backslash", got, ok)
	}
	if _, ok := tmuxOutput("%begin 1 2 3"); ok {
		t.Fatal("decoded a non-output control record")
	}
}

func TestWaitForTmuxReady(t *testing.T) {
	t.Run("waits for tagged probe after delayed pipe setup", func(t *testing.T) {
		scanner := bufio.NewScanner(strings.NewReader("%begin 1 1 0\n%end 1 1 0\n%begin 1 2 0\n%end 1 2 0\n%session-changed $0 swe\n%begin 1 3 0\n%output %0 ready\\015\\012\n%end 1 3 0\n%begin 1 4 0\nswe-output-drain-ready:1\n%end 1 4 0\n"))
		output, err := waitForTmuxReady(scanner, "swe")
		if err != nil {
			t.Fatalf("wait for tmux: %v", err)
		}
		if string(output) != "ready\r\n" {
			t.Fatalf("startup output = %q", output)
		}
	})
	t.Run("reports delayed pipe setup failure", func(t *testing.T) {
		scanner := bufio.NewScanner(strings.NewReader("%begin 1 1 0\n%end 1 1 0\n%begin 1 2 0\n%end 1 2 0\n%begin 1 3 0\n%error 1 3 0\n"))
		if _, err := waitForTmuxReady(scanner, "swe"); err == nil {
			t.Fatal("expected command failure")
		}
	})
	t.Run("rejects completion before readiness probe", func(t *testing.T) {
		scanner := bufio.NewScanner(strings.NewReader("%begin 1 1 0\n%end 1 1 0\n%begin 1 2 0\n%end 1 2 0\n%session-changed $0 swe\n"))
		if _, err := waitForTmuxReady(scanner, "swe"); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("incomplete setup error = %v, want unexpected EOF", err)
		}
	})
	t.Run("rejects inactive drain probe", func(t *testing.T) {
		scanner := bufio.NewScanner(strings.NewReader("%begin 1 3 0\nswe-output-drain-ready:0\n%end 1 3 0\n%session-changed $0 swe\n"))
		if _, err := waitForTmuxReady(scanner, "swe"); err == nil {
			t.Fatal("expected inactive drain failure")
		}
	})
}

func TestTerminalStatusMapsBackendErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{name: "unavailable", err: fmt.Errorf("wrapped: %w", errTerminalUnavailable), want: codes.Unavailable},
		{name: "internal", err: errors.New("write failed"), want: codes.Internal},
		{name: "existing status", err: status.Error(codes.InvalidArgument, "bad size"), want: codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := status.Code(terminalStatus("terminal", tt.err)); got != tt.want {
				t.Fatalf("status code = %v, want %v", got, tt.want)
			}
		})
	}
}

func newTerminalConn(t *testing.T, backend terminalBackend) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	sandboxdv1.RegisterTerminalServiceServer(grpcServer, &TerminalServer{backend: backend})
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
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

type fakeTerminalBackend struct {
	opened  chan [2]uint32
	session *fakeTerminalSession
}

func newFakeTerminalBackend() *fakeTerminalBackend {
	return &fakeTerminalBackend{
		opened: make(chan [2]uint32, 1),
		session: &fakeTerminalSession{
			reads:       make(chan terminalRead, 1),
			writes:      make(chan []byte, 1),
			resizes:     make(chan [2]uint32, 1),
			closed:      make(chan struct{}),
			contextDone: make(chan struct{}),
		},
	}
}

func (b *fakeTerminalBackend) Open(ctx context.Context, cols, rows uint32) (terminalSession, error) {
	b.session.ctx = ctx
	go func() {
		<-ctx.Done()
		b.session.contextOnce.Do(func() { close(b.session.contextDone) })
	}()
	b.opened <- [2]uint32{cols, rows}
	return b.session, nil
}

type terminalRead struct {
	data []byte
	err  error
}

type fakeTerminalSession struct {
	ctx         context.Context
	reads       chan terminalRead
	writes      chan []byte
	resizes     chan [2]uint32
	closed      chan struct{}
	contextDone chan struct{}
	closeOnce   sync.Once
	contextOnce sync.Once
}

func (s *fakeTerminalSession) Read() ([]byte, error) {
	select {
	case read := <-s.reads:
		return read.data, read.err
	case <-s.closed:
		return nil, io.EOF
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *fakeTerminalSession) Write(data []byte) error {
	s.writes <- bytes.Clone(data)
	return nil
}

func (s *fakeTerminalSession) Resize(cols, rows uint32) error {
	s.resizes <- [2]uint32{cols, rows}
	return nil
}

func (s *fakeTerminalSession) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }

type blockingWriteCloser struct {
	started   chan struct{}
	closed    chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
}

func (w *blockingWriteCloser) Write([]byte) (int, error) {
	w.startOnce.Do(func() { close(w.started) })
	<-w.closed
	return 0, io.ErrClosedPipe
}

func (w *blockingWriteCloser) Close() error {
	w.closeOnce.Do(func() { close(w.closed) })
	return nil
}

type failingTerminalStream struct {
	grpc.ServerStream
	open        *sandboxdv1.TerminalMessage
	backendDone <-chan struct{}
	sendErr     error
	received    bool
}

func (s *failingTerminalStream) Context() context.Context { return context.Background() }

func (s *failingTerminalStream) Recv() (*sandboxdv1.TerminalMessage, error) {
	if !s.received {
		s.received = true
		return s.open, nil
	}
	<-s.backendDone
	return nil, context.Canceled
}

func (s *failingTerminalStream) Send(*sandboxdv1.TerminalMessage) error { return s.sendErr }
func (s *failingTerminalStream) SetHeader(metadata.MD) error            { return nil }
func (s *failingTerminalStream) SendHeader(metadata.MD) error           { return nil }
func (s *failingTerminalStream) SetTrailer(metadata.MD)                 {}
func (s *failingTerminalStream) SendMsg(any) error                      { return nil }
func (s *failingTerminalStream) RecvMsg(any) error                      { return nil }

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for terminal operation")
		var zero T
		return zero
	}
}
