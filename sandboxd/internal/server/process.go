package server

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const defaultProcessGracePeriod = 5 * time.Second

type processKey struct {
	ownerID string
	role    string
}

type managedProcess struct {
	key           *sandboxdv1.ProcessKey
	spec          *sandboxdv1.ProcessSpec
	cmd           *exec.Cmd
	started       bool
	state         sandboxdv1.ProcessState
	exitCode      int32
	err           string
	stopRequested bool
}

// ProcessServer manages processes in memory for one sandboxd daemon epoch.
type ProcessServer struct {
	sandboxdv1.UnimplementedProcessServiceServer

	Workspace string
	mu        sync.Mutex
	processes map[processKey]*managedProcess
	closed    bool
}

func NewProcessServer(workspace string) *ProcessServer {
	return &ProcessServer{Workspace: workspace, processes: make(map[processKey]*managedProcess)}
}

func requestKey(key *sandboxdv1.ProcessKey) (processKey, error) {
	if key == nil || key.OwnerId == "" || key.Role == "" {
		return processKey{}, status.Error(codes.InvalidArgument, "key owner_id and role must not be empty")
	}
	return processKey{ownerID: key.OwnerId, role: key.Role}, nil
}

func (s *ProcessServer) normalizeSpec(spec *sandboxdv1.ProcessSpec) (*sandboxdv1.ProcessSpec, error) {
	if spec == nil || len(spec.Argv) == 0 || spec.Argv[0] == "" {
		return nil, status.Error(codes.InvalidArgument, "spec argv must not be empty")
	}
	cwd := spec.Cwd
	if cwd == "" {
		cwd = s.Workspace
	}
	result := &sandboxdv1.ProcessSpec{Argv: append([]string(nil), spec.Argv...), Cwd: cwd}
	if spec.Env != nil {
		result.Env = make(map[string]string, len(spec.Env))
		for k, v := range spec.Env {
			result.Env[k] = v
		}
	}
	return result, nil
}

func processResponse(p *managedProcess) *sandboxdv1.Process {
	key := &sandboxdv1.ProcessKey{OwnerId: p.key.OwnerId, Role: p.key.Role}
	spec := &sandboxdv1.ProcessSpec{Argv: append([]string(nil), p.spec.Argv...), Cwd: p.spec.Cwd}
	if p.spec.Env != nil {
		spec.Env = make(map[string]string, len(p.spec.Env))
		for k, v := range p.spec.Env {
			spec.Env[k] = v
		}
	}
	return &sandboxdv1.Process{Key: key, Spec: spec, State: p.state, ExitCode: p.exitCode, Error: p.err}
}

func (s *ProcessServer) Start(_ context.Context, req *sandboxdv1.StartProcessRequest) (*sandboxdv1.Process, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	key, err := requestKey(req.Key)
	if err != nil {
		return nil, err
	}
	spec, err := s.normalizeSpec(req.Spec)
	if err != nil {
		return nil, err
	}

	// Hold the lock through cmd.Start so concurrent requests for a key can
	// never create two children.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, status.Error(codes.Unavailable, "process supervisor epoch is closed")
	}
	if existing, ok := s.processes[key]; ok {
		if !reflect.DeepEqual(existing.spec, spec) {
			return nil, status.Error(codes.FailedPrecondition, "process key already has a different spec")
		}
		return processResponse(existing), nil
	}
	p := &managedProcess{key: &sandboxdv1.ProcessKey{OwnerId: key.ownerID, Role: key.role}, spec: spec}
	s.processes[key] = p
	cmd := exec.Command(spec.Argv[0], spec.Argv[1:]...) // intentionally detached from the RPC context
	cmd.Dir = spec.Cwd
	cmd.Env = os.Environ()
	configureManagedProcess(cmd)
	for k, v := range spec.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	p.cmd = cmd
	if err := cmd.Start(); err != nil {
		p.state, p.exitCode, p.err = sandboxdv1.ProcessState_PROCESS_STATE_FAILED, -1, err.Error()
		return processResponse(p), nil
	}
	p.started = true
	p.state = sandboxdv1.ProcessState_PROCESS_STATE_RUNNING
	go s.wait(key, p)
	return processResponse(p), nil
}

func (s *ProcessServer) wait(key processKey, p *managedProcess) {
	err := p.cmd.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	// The table never replaces entries, but retain this guard against future
	// lifecycle changes making an old waiter update a new process.
	if s.processes[key] != p {
		return
	}
	p.state = sandboxdv1.ProcessState_PROCESS_STATE_EXITED
	p.exitCode = 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			p.exitCode = int32(exitErr.ExitCode())
		} else {
			p.state, p.exitCode, p.err = sandboxdv1.ProcessState_PROCESS_STATE_FAILED, -1, err.Error()
		}
	}
}

func (s *ProcessServer) Get(_ context.Context, req *sandboxdv1.GetProcessRequest) (*sandboxdv1.Process, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	key, err := requestKey(req.Key)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.processes[key]
	if !ok {
		return nil, status.Error(codes.NotFound, "process not found")
	}
	return processResponse(p), nil
}

func (s *ProcessServer) Stop(_ context.Context, req *sandboxdv1.StopProcessRequest) (*sandboxdv1.Process, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	key, err := requestKey(req.Key)
	if err != nil {
		return nil, err
	}
	if req.Mode != sandboxdv1.StopMode_STOP_MODE_UNSPECIFIED && req.Mode != sandboxdv1.StopMode_STOP_MODE_GRACEFUL && req.Mode != sandboxdv1.StopMode_STOP_MODE_FORCE {
		return nil, status.Error(codes.InvalidArgument, "unknown stop mode")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.processes[key]
	if !ok {
		// Absence is the desired result, including in a fresh sandboxd epoch.
		return &sandboxdv1.Process{
			Key:   &sandboxdv1.ProcessKey{OwnerId: key.ownerID, Role: key.role},
			State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED,
		}, nil
	}
	if !p.started {
		return processResponse(p), nil
	}
	if req.Mode == sandboxdv1.StopMode_STOP_MODE_FORCE {
		p.stopRequested = true
		if p.state == sandboxdv1.ProcessState_PROCESS_STATE_RUNNING || p.state == sandboxdv1.ProcessState_PROCESS_STATE_STOPPING {
			p.state = sandboxdv1.ProcessState_PROCESS_STATE_STOPPING
		}
		_ = killManagedProcess(p.cmd)
		return processResponse(p), nil
	}
	if !p.stopRequested {
		p.stopRequested = true
		if p.state == sandboxdv1.ProcessState_PROCESS_STATE_RUNNING {
			p.state = sandboxdv1.ProcessState_PROCESS_STATE_STOPPING
		}
		if err := interruptManagedProcess(p.cmd); err != nil {
			_ = killManagedProcess(p.cmd) // interruption is unsupported on some hosts
		} else {
			grace := time.Duration(req.GracePeriodMs) * time.Millisecond
			if grace == 0 {
				grace = defaultProcessGracePeriod
			}
			time.AfterFunc(grace, func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				if s.processes[key] == p && p.stopRequested {
					_ = killManagedProcess(p.cmd)
				}
			})
		}
	}
	return processResponse(p), nil
}

// Close fences this sandboxd epoch by force-stopping every managed process.
// The environment backend must call it before replacing or stopping sandboxd.
func (s *ProcessServer) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	commands := make([]*exec.Cmd, 0, len(s.processes))
	for _, p := range s.processes {
		if p.started {
			p.stopRequested = true
			if p.state == sandboxdv1.ProcessState_PROCESS_STATE_RUNNING || p.state == sandboxdv1.ProcessState_PROCESS_STATE_STOPPING {
				p.state = sandboxdv1.ProcessState_PROCESS_STATE_STOPPING
			}
			commands = append(commands, p.cmd)
		}
	}
	s.mu.Unlock()
	for _, cmd := range commands {
		_ = killManagedProcess(cmd)
	}
}
