package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const defaultTmuxSocket = "swe-platform"

var errTerminalUnavailable = errors.New("terminal backend unavailable")

// terminalBackend is the OS-portable boundary for a shared terminal. A future
// ConPTY implementation can provide the same session operations without
// changing TerminalService or its protocol handler.
type terminalBackend interface {
	// Canceling ctx must abort this attachment and release its resources; it
	// must not destroy the shared terminal itself.
	Open(ctx context.Context, cols, rows uint32) (terminalSession, error)
}

type terminalSession interface {
	Read() ([]byte, error)
	Write([]byte) error
	Resize(cols, rows uint32) error
	// Close detaches this client without destroying the shared terminal. It
	// must be idempotent, concurrency-safe, and unblock Read and Write.
	Close() error
}

// TerminalServer translates TerminalService messages to backend operations.
type TerminalServer struct {
	sandboxdv1.UnimplementedTerminalServiceServer
	backend terminalBackend
}

func NewTerminalServer(workspace string) *TerminalServer {
	return &TerminalServer{
		backend: newTmuxTerminalBackend(workspace, defaultTmuxSocket, nil),
	}
}

func (s *TerminalServer) Terminal(stream sandboxdv1.TerminalService_TerminalServer) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "read open message: %v", err)
	}
	open := first.GetOpen()
	if open == nil {
		return status.Error(codes.InvalidArgument, "first message must be TerminalOpen")
	}
	if open.Cols == 0 || open.Rows == 0 {
		return status.Error(codes.InvalidArgument, "terminal dimensions must be non-zero")
	}

	backendCtx, cancelBackend := context.WithCancel(stream.Context())
	defer cancelBackend()
	session, err := s.backend.Open(backendCtx, open.Cols, open.Rows)
	if err != nil {
		return terminalStatus("open terminal", err)
	}
	defer session.Close()

	recvDone := make(chan error, 1)
	go func() {
		finish := func(err error) {
			recvDone <- err
			_ = session.Close()
		}
		for {
			message, err := stream.Recv()
			if err != nil {
				finish(err)
				return
			}
			switch kind := message.Kind.(type) {
			case *sandboxdv1.TerminalMessage_Data:
				if err := session.Write(kind.Data); err != nil {
					finish(terminalStatus("terminal input", err))
					return
				}
			case *sandboxdv1.TerminalMessage_Resize:
				if kind.Resize.Cols == 0 || kind.Resize.Rows == 0 {
					finish(status.Error(codes.InvalidArgument, "terminal dimensions must be non-zero"))
					return
				}
				if err := session.Resize(kind.Resize.Cols, kind.Resize.Rows); err != nil {
					finish(terminalStatus("resize terminal", err))
					return
				}
			default:
				finish(status.Error(codes.InvalidArgument, "terminal is already open"))
				return
			}
		}
	}()

	var readErr, sendErr error
	for {
		data, err := session.Read()
		if len(data) > 0 && sendErr == nil {
			if err := stream.Send(&sandboxdv1.TerminalMessage{
				Kind: &sandboxdv1.TerminalMessage_Data{Data: data},
			}); err != nil {
				sendErr = err
				cancelBackend()
			}
		}
		if err != nil {
			readErr = err
			break
		}
	}
	if sendErr != nil {
		return sendErr
	}

	select {
	case recvErr := <-recvDone:
		if recvErr != nil && !errors.Is(recvErr, io.EOF) && stream.Context().Err() == nil {
			return recvErr
		}
	default:
	}
	if readErr != nil && !errors.Is(readErr, io.EOF) && stream.Context().Err() == nil {
		return terminalStatus("terminal output", readErr)
	}
	return nil
}

func terminalStatus(operation string, err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, errTerminalUnavailable) {
		return status.Errorf(codes.Unavailable, "%s: %v", operation, err)
	}
	return status.Errorf(codes.Internal, "%s: %v", operation, err)
}

type tmuxTerminalBackend struct {
	workspace  string
	socketName string
	shell      []string
}

func newTmuxTerminalBackend(workspace, socketName string, shell []string) terminalBackend {
	return &tmuxTerminalBackend{workspace: workspace, socketName: socketName, shell: shell}
}

func (b *tmuxTerminalBackend) Open(ctx context.Context, cols, rows uint32) (terminalSession, error) {
	args := []string{"-C", "-L", b.socketName, "new-session", "-A", "-s", "swe"}
	if b.workspace != "" {
		args = append(args, "-c", b.workspace)
	}
	args = append(args, b.shell...)

	commandCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(commandCtx, "tmux", args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("tmux stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("tmux stdout: %w", err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("%w: start tmux: %v", errTerminalUnavailable, err)
	}

	session := &tmuxTerminalSession{
		ctx:    ctx,
		cancel: cancel,
		cmd:    cmd,
		stdin:  stdin,
		stderr: &stderr,
		scanner: func() *bufio.Scanner {
			scanner := bufio.NewScanner(stdout)
			scanner.Buffer(make([]byte, 4096), 1024*1024)
			return scanner
		}(),
	}
	if err := session.Resize(cols, rows); err != nil {
		session.abortAndWait()
		return nil, fmt.Errorf("size terminal: %w", err)
	}
	return session, nil
}

type tmuxTerminalSession struct {
	ctx     context.Context
	cancel  context.CancelFunc
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	scanner *bufio.Scanner
	stderr  *strings.Builder

	writeMu  sync.Mutex
	closed   bool
	waitOnce sync.Once
	waitErr  error
}

func (s *tmuxTerminalSession) Read() ([]byte, error) {
	for s.scanner.Scan() {
		if data, ok := tmuxOutput(s.scanner.Text()); ok {
			return data, nil
		}
	}
	if err := s.scanner.Err(); err != nil {
		s.abortAndWait()
		return nil, fmt.Errorf("read tmux output: %w", err)
	}
	if err := s.wait(); err != nil {
		if s.ctx.Err() != nil {
			return nil, s.ctx.Err()
		}
		return nil, fmt.Errorf("%w: tmux exited: %v: %s", errTerminalUnavailable, err, strings.TrimSpace(s.stderr.String()))
	}
	return nil, io.EOF
}

func (s *tmuxTerminalSession) Write(data []byte) error {
	for len(data) > 0 {
		n := min(len(data), 512)
		if err := s.writeCommand(sendKeysCommand(data[:n])); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func (s *tmuxTerminalSession) Resize(cols, rows uint32) error {
	return s.writeCommand(resizeClientCommand(cols, rows))
}

func (s *tmuxTerminalSession) Close() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.stdin.Close()
}

func (s *tmuxTerminalSession) writeCommand(command string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed {
		return io.ErrClosedPipe
	}
	_, err := io.WriteString(s.stdin, command+"\n")
	return err
}

func (s *tmuxTerminalSession) wait() error {
	s.waitOnce.Do(func() { s.waitErr = s.cmd.Wait() })
	return s.waitErr
}

func (s *tmuxTerminalSession) abortAndWait() {
	s.cancel()
	_ = s.Close()
	_ = s.wait()
}

func resizeClientCommand(cols, rows uint32) string {
	return fmt.Sprintf("refresh-client -C %d,%d", cols, rows)
}

func sendKeysCommand(data []byte) string {
	var command strings.Builder
	command.WriteString("send-keys -t swe: -H")
	for _, b := range data {
		fmt.Fprintf(&command, " %02x", b)
	}
	return command.String()
}

func tmuxOutput(line string) ([]byte, bool) {
	const prefix = "%output "
	if !strings.HasPrefix(line, prefix) {
		return nil, false
	}
	_, escaped, ok := strings.Cut(strings.TrimPrefix(line, prefix), " ")
	if !ok {
		return nil, false
	}

	data := make([]byte, 0, len(escaped))
	for i := 0; i < len(escaped); i++ {
		if escaped[i] != '\\' {
			data = append(data, escaped[i])
			continue
		}
		if i+3 < len(escaped) {
			if value, err := strconv.ParseUint(escaped[i+1:i+4], 8, 8); err == nil {
				data = append(data, byte(value))
				i += 3
				continue
			}
		}
		if i+1 < len(escaped) {
			i++
			data = append(data, escaped[i])
		}
	}
	return data, true
}
