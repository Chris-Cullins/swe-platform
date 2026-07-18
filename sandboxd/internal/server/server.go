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
	"path/filepath"
	"sort"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const execChunkSize = 32 * 1024

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
	Workspace string
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

	cwd := start.Cwd
	if cwd == "" {
		cwd = s.Workspace
	}

	cmd := exec.CommandContext(stream.Context(), start.Argv[0], start.Argv[1:]...)
	cmd.Dir = cwd
	cmd.Env = os.Environ()
	for k, v := range start.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return status.Errorf(codes.Internal, "stdin pipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return status.Errorf(codes.Internal, "stdout pipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return status.Errorf(codes.Internal, "stderr pipe: %v", err)
	}

	if err := cmd.Start(); err != nil {
		return stream.Send(&sandboxdv1.ExecResponse{
			Kind: &sandboxdv1.ExecResponse_Exit{
				Exit: &sandboxdv1.ExecExit{Code: -1, Error: fmt.Sprintf("start: %v", err)},
			},
		})
	}

	// Feed stdin from the client.
	go func() {
		defer stdin.Close()
		for {
			req, err := stream.Recv()
			if err != nil {
				return
			}
			if data := req.GetStdin(); data != nil {
				if _, err := stdin.Write(data); err != nil {
					return
				}
			}
		}
	}()

	var sendMu sync.Mutex
	send := func(resp *sandboxdv1.ExecResponse) {
		sendMu.Lock()
		defer sendMu.Unlock()
		_ = stream.Send(resp) // client disconnect ends the command via context
	}

	var wg sync.WaitGroup
	pump := func(r io.Reader, stdoutStream bool) {
		defer wg.Done()
		buf := make([]byte, execChunkSize)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])
				resp := &sandboxdv1.ExecResponse{}
				if stdoutStream {
					resp.Kind = &sandboxdv1.ExecResponse_Stdout{Stdout: chunk}
				} else {
					resp.Kind = &sandboxdv1.ExecResponse_Stderr{Stderr: chunk}
				}
				send(resp)
			}
			if err != nil {
				return
			}
		}
	}
	wg.Add(2)
	go pump(stdout, true)
	go pump(stderr, false)

	waitErr := cmd.Wait()
	wg.Wait()

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
	send(&sandboxdv1.ExecResponse{
		Kind: &sandboxdv1.ExecResponse_Exit{
			Exit: &sandboxdv1.ExecExit{Code: code, Error: errStr},
		},
	})
	return nil
}

// FilesystemServer implements FilesystemService.
type FilesystemServer struct {
	sandboxdv1.UnimplementedFilesystemServiceServer
	Workspace string
}

// resolve maps a request path onto the filesystem. Relative paths resolve
// against the workspace.
// TODO(P1): jail paths to the workspace root.
func (s *FilesystemServer) resolve(path string) string {
	if path == "" {
		return s.Workspace
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(s.Workspace, path)
}

func (s *FilesystemServer) Read(_ context.Context, req *sandboxdv1.ReadRequest) (*sandboxdv1.ReadResponse, error) {
	content, err := os.ReadFile(s.resolve(req.Path))
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "read %s: %v", req.Path, err)
	}
	return &sandboxdv1.ReadResponse{Content: content}, nil
}

func (s *FilesystemServer) Write(_ context.Context, req *sandboxdv1.WriteRequest) (*sandboxdv1.WriteResponse, error) {
	path := s.resolve(req.Path)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, status.Errorf(codes.Internal, "mkdir for %s: %v", req.Path, err)
	}
	mode := os.FileMode(req.Mode)
	if req.Mode == 0 {
		mode = 0o644
	}
	if err := os.WriteFile(path, req.Content, mode); err != nil {
		return nil, status.Errorf(codes.Internal, "write %s: %v", req.Path, err)
	}
	return &sandboxdv1.WriteResponse{}, nil
}

func (s *FilesystemServer) List(_ context.Context, req *sandboxdv1.ListRequest) (*sandboxdv1.ListResponse, error) {
	entries, err := os.ReadDir(s.resolve(req.Path))
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "list %s: %v", req.Path, err)
	}
	resp := &sandboxdv1.ListResponse{Entries: make([]*sandboxdv1.Entry, 0, len(entries))}
	for _, e := range entries {
		entry := &sandboxdv1.Entry{Name: e.Name(), IsDir: e.IsDir()}
		if info, err := e.Info(); err == nil {
			entry.Size = info.Size()
		}
		resp.Entries = append(resp.Entries, entry)
	}
	return resp, nil
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

// TerminalServer implements TerminalService.
//
// Terminal is intentionally unimplemented in the scaffold: it needs the
// tmux/ConPTY abstraction (P1). The embedded UnimplementedTerminalServiceServer
// returns codes.Unimplemented to callers.
type TerminalServer struct {
	sandboxdv1.UnimplementedTerminalServiceServer
}
