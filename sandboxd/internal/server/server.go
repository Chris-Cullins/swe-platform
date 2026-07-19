// Package server implements the sandboxd gRPC API: the single contract
// the control plane uses to interact with an environment.
//
// Portability invariant: this package must stay OS-portable. No Linux-only
// syscalls; terminal handling is abstracted (tmux vs ConPTY) behind an
// interface that lands with P1.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const execChunkSize = 32 * 1024
const execQueueSize = 32

type execOutput struct {
	stream sandboxdv1.OutputStream
	data   []byte
	offset uint64
}

// HealthServer implements HealthService.
type HealthServer struct {
	sandboxdv1.UnimplementedHealthServiceServer
	Version string
}

func (s *HealthServer) Check(context.Context, *sandboxdv1.HealthCheckRequest) (*sandboxdv1.HealthCheckResponse, error) {
	return &sandboxdv1.HealthCheckResponse{Ok: true, Version: s.Version}, nil
}

// ExecServer implements ExecService.
type ExecServer struct {
	sandboxdv1.UnimplementedExecServiceServer
	Workspace  string
	supervisor *Supervisor
}

func NewExecServer(workspace string, supervisor *Supervisor) *ExecServer {
	if supervisor == nil {
		supervisor = NewSupervisor()
	}
	return &ExecServer{Workspace: workspace, supervisor: supervisor}
}

// Exec runs a command and streams its output. The first client message must
// be ExecStart; subsequent messages feed stdin.
func (s *ExecServer) Exec(stream sandboxdv1.ExecService_ExecServer) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "read start message: %v", err)
	}
	start := first.GetStart()
	if start == nil {
		return status.Error(codes.InvalidArgument, "first message must be ExecStart")
	}
	if len(start.Argv) == 0 {
		return status.Error(codes.InvalidArgument, "argv must not be empty")
	}
	if !validTimeout(start.TimeoutMs) {
		return status.Error(codes.InvalidArgument, "timeout_ms overflows duration")
	}

	cwd := start.Cwd
	if cwd == "" {
		cwd = s.Workspace
	}

	cmd := exec.Command(start.Argv[0], start.Argv[1:]...)
	cmd.Dir = cwd
	cmd.Env, err = normalizeEnv(start.EnvMode, start.Env)
	if err != nil {
		return err
	}
	domain := newProcessDomain(cmd)
	if s.supervisor == nil {
		s.supervisor = NewSupervisor()
	}
	var causeMu sync.Mutex
	reason := sandboxdv1.TerminationReason_TERMINATION_REASON_UNSPECIFIED
	setCause := func(r sandboxdv1.TerminationReason) bool {
		causeMu.Lock()
		defer causeMu.Unlock()
		if reason != sandboxdv1.TerminationReason_TERMINATION_REASON_UNSPECIFIED {
			return false
		}
		reason = r
		return true
	}
	deliverCause := func(r sandboxdv1.TerminationReason, deliver func() error) error {
		causeMu.Lock()
		defer causeMu.Unlock()
		if reason != sandboxdv1.TerminationReason_TERMINATION_REASON_UNSPECIFIED {
			return nil
		}
		if err := deliver(); err != nil {
			return err
		}
		reason = r
		return nil
	}

	stdinR, stdin, err := os.Pipe()
	if err != nil {
		return status.Errorf(codes.Internal, "stdin pipe: %v", err)
	}
	defer stdin.Close()
	cmd.Stdin = stdinR
	stdout, stdoutW, err := os.Pipe()
	if err != nil {
		_ = stdinR.Close()
		_ = stdin.Close()
		return status.Errorf(codes.Internal, "stdout pipe: %v", err)
	}
	stderr, stderrW, err := os.Pipe()
	if err != nil {
		_ = stdinR.Close()
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stdoutW.Close()
		return status.Errorf(codes.Internal, "stderr pipe: %v", err)
	}
	cmd.Stdout, cmd.Stderr = stdoutW, stderrW

	if err := s.supervisor.start(domain, func() { setCause(sandboxdv1.TerminationReason_TERMINATION_REASON_DAEMON_CLOSED) }); err != nil {
		_ = stdinR.Close()
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stdoutW.Close()
		_ = stderr.Close()
		_ = stderrW.Close()
		if errors.Is(err, context.Canceled) {
			return status.Error(codes.Unavailable, "process supervisor epoch is closed")
		}
		_ = stdinR.Close()
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stdoutW.Close()
		_ = stderr.Close()
		_ = stderrW.Close()
		return stream.Send(&sandboxdv1.ExecResponse{
			Kind: &sandboxdv1.ExecResponse_Exit{
				Exit: &sandboxdv1.ExecExit{Code: -1, Error: fmt.Sprintf("start: %v", err), Reason: sandboxdv1.TerminationReason_TERMINATION_REASON_START_FAILED},
			},
		})
	}
	defer s.supervisor.done(domain)

	killFor := func(r sandboxdv1.TerminationReason) {
		_ = deliverCause(r, domain.force)
	}
	_ = stdinR.Close()
	_ = stdoutW.Close()
	_ = stderrW.Close()
	controlErr := make(chan error, 1)

	// Feed stdin from the client.
	recvDone := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				_ = stdin.Close()
				recvDone <- err
				return
			}
			if data := req.GetStdin(); data != nil {
				if _, err := stdin.Write(data); err != nil {
					select {
					case recvDone <- err:
					default:
					}
					return
				}
			}
			if req.GetStdinEof() != nil {
				_ = stdin.Close()
			}
			if control := req.GetControl(); control != nil {
				var controlReason sandboxdv1.TerminationReason
				var deliveryErr error
				switch control.Control {
				case sandboxdv1.ProcessControl_PROCESS_CONTROL_INTERRUPT:
					controlReason = sandboxdv1.TerminationReason_TERMINATION_REASON_INTERRUPTED
					deliveryErr = deliverCause(controlReason, domain.interrupt)
				case sandboxdv1.ProcessControl_PROCESS_CONTROL_TERMINATE:
					controlReason = sandboxdv1.TerminationReason_TERMINATION_REASON_TERMINATED
					deliveryErr = deliverCause(controlReason, domain.terminate)
				case sandboxdv1.ProcessControl_PROCESS_CONTROL_FORCE:
					controlReason = sandboxdv1.TerminationReason_TERMINATION_REASON_FORCED
					deliveryErr = deliverCause(controlReason, domain.force)
				default:
					deliveryErr = status.Error(codes.InvalidArgument, "unknown or unspecified process control")
				}
				if deliveryErr != nil {
					_ = domain.force()
					if errors.Is(deliveryErr, errInterruptUnsupported) {
						deliveryErr = status.Error(codes.Unimplemented, deliveryErr.Error())
					} else if status.Code(deliveryErr) == codes.OK {
						deliveryErr = status.Errorf(codes.Internal, "deliver process control: %v", deliveryErr)
					}
					select {
					case controlErr <- deliveryErr:
					default:
					}
					return
				}
			}
		}
	}()
	// A transport cancellation is a full disconnect; half-close (io.EOF) only
	// closes stdin and output remains readable.
	go func() {
		select {
		case err := <-recvDone:
			if err != io.EOF {
				killFor(sandboxdv1.TerminationReason_TERMINATION_REASON_DISCONNECTED)
			}
		case <-stream.Context().Done():
			killFor(sandboxdv1.TerminationReason_TERMINATION_REASON_DISCONNECTED)
		}
	}()
	var timer *time.Timer
	if start.TimeoutMs > 0 {
		timer = time.AfterFunc(time.Duration(start.TimeoutMs)*time.Millisecond, func() {
			killFor(sandboxdv1.TerminationReason_TERMINATION_REASON_TIMEOUT)
		})
	}

	q := make(chan execOutput, execQueueSize)
	dispatched := make(chan [2]uint64, 1)
	sendErr := make(chan error, 1)
	go func() {
		var next [2]uint64
		for event := range q {
			i := int(event.stream) - 1
			chunk := &sandboxdv1.OutputChunk{Stream: event.stream, Data: event.data, Offset: event.offset, GapBytes: event.offset - next[i]}
			resp := &sandboxdv1.ExecResponse{}
			if i == 0 {
				resp.Kind = &sandboxdv1.ExecResponse_Stdout{Stdout: chunk}
			} else {
				resp.Kind = &sandboxdv1.ExecResponse_Stderr{Stderr: chunk}
			}
			if err := stream.Send(resp); err != nil {
				killFor(sandboxdv1.TerminationReason_TERMINATION_REASON_DISCONNECTED)
				sendErr <- err
				return
			}
			next[i] = event.offset + uint64(len(event.data))
		}
		dispatched <- next
	}()

	var wg sync.WaitGroup
	var producedMu sync.Mutex
	var produced [2]uint64
	pump := func(r *os.File, outputStream sandboxdv1.OutputStream) {
		defer wg.Done()
		defer r.Close()
		buf := make([]byte, execChunkSize)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				i := int(outputStream) - 1
				producedMu.Lock()
				offset := produced[i]
				produced[i] += uint64(n)
				producedMu.Unlock()
				chunk := append([]byte(nil), buf[:n]...)
				select {
				case q <- execOutput{stream: outputStream, data: chunk, offset: offset}:
				default: // drains never wait for a slow network consumer
				}
			}
			if err != nil {
				return
			}
		}
	}
	wg.Add(2)
	go pump(stdout, sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT)
	go pump(stderr, sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR)

	waitErr := domain.wait()
	setCause(sandboxdv1.TerminationReason_TERMINATION_REASON_EXITED)
	if timer != nil {
		timer.Stop()
	}
	_ = domain.force() // fence descendants before terminal publication
	_ = domain.close()
	_ = stdoutW.Close()
	_ = stderrW.Close()
	wg.Wait()
	close(q)
	select {
	case err := <-controlErr:
		return err
	default:
	}
	var next [2]uint64
	select {
	case next = <-dispatched:
	case err := <-sendErr:
		return err
	}
	producedMu.Lock()
	ends := produced
	producedMu.Unlock()
	for i := range 2 {
		if ends[i] <= next[i] {
			continue
		}
		outputStream := sandboxdv1.OutputStream(i + 1)
		chunk := &sandboxdv1.OutputChunk{Stream: outputStream, Offset: ends[i], GapBytes: ends[i] - next[i]}
		resp := &sandboxdv1.ExecResponse{}
		if i == 0 {
			resp.Kind = &sandboxdv1.ExecResponse_Stdout{Stdout: chunk}
		} else {
			resp.Kind = &sandboxdv1.ExecResponse_Stderr{Stderr: chunk}
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	code := int32(0)
	errStr := ""
	if waitErr != nil {
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			code = int32(exitErr.ExitCode())
		} else {
			code = -1
			errStr = waitErr.Error()
		}
	}
	causeMu.Lock()
	terminalReason := reason
	causeMu.Unlock()
	if terminalReason == sandboxdv1.TerminationReason_TERMINATION_REASON_UNSPECIFIED {
		terminalReason = sandboxdv1.TerminationReason_TERMINATION_REASON_EXITED
	}
	return stream.Send(&sandboxdv1.ExecResponse{
		Kind: &sandboxdv1.ExecResponse_Exit{
			Exit: &sandboxdv1.ExecExit{Code: code, Error: errStr, Reason: terminalReason},
		},
	})
}

// PortServer implements PortService.
type PortServer struct {
	sandboxdv1.UnimplementedPortServiceServer

	mu    sync.Mutex
	ports map[uint32]*sandboxdv1.Port
}

// NewPortServer builds a PortServer with an empty registry.
func NewPortServer() *PortServer {
	return &PortServer{ports: map[uint32]*sandboxdv1.Port{}}
}

func (s *PortServer) Register(_ context.Context, req *sandboxdv1.RegisterPortRequest) (*sandboxdv1.Port, error) {
	port := req.Port
	if port == 0 {
		free, err := freePort()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "pick free port: %v", err)
		}
		port = free
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.ports[port]; ok {
		return p, nil
	}
	p := &sandboxdv1.Port{Port: port, Label: req.Label}
	s.ports[port] = p
	return p, nil
}

func (s *PortServer) List(_ context.Context, _ *sandboxdv1.ListPortsRequest) (*sandboxdv1.ListPortsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &sandboxdv1.ListPortsResponse{Ports: make([]*sandboxdv1.Port, 0, len(s.ports))}
	for _, p := range s.ports {
		resp.Ports = append(resp.Ports, p)
	}
	sort.Slice(resp.Ports, func(i, j int) bool { return resp.Ports[i].Port < resp.Ports[j].Port })
	return resp, nil
}

// freePort asks the OS for an ephemeral port. There is an inherent race
// between closing the probe listener and the caller binding the port;
// acceptable for a registry of convenience.
func freePort() (uint32, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("unexpected address type %T", l.Addr())
	}
	return uint32(addr.Port), nil
}
