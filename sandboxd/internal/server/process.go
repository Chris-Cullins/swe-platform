package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const (
	defaultProcessGracePeriod = 5 * time.Second
	defaultOutputCapacity     = 1024 * 1024
	defaultMaxRecords         = 1024
	defaultReadMax            = 64 * 1024
)

type processKey struct{ ownerID, role string }
type outputBuffer struct {
	data  []byte
	start uint64
	total uint64
	eof   bool
}

func (b *outputBuffer) write(p []byte, cap int) {
	b.total += uint64(len(p))
	if len(p) >= cap {
		b.data = append(b.data[:0], p[len(p)-cap:]...)
		b.start = b.total - uint64(len(b.data))
		return
	}
	b.data = append(b.data, p...)
	if n := len(b.data) - cap; n > 0 {
		copy(b.data, b.data[n:])
		b.data = b.data[:cap]
		b.start += uint64(n)
	}
}

type managedProcess struct {
	key               *sandboxdv1.ProcessKey
	spec              *sandboxdv1.ProcessSpec
	cmd               *exec.Cmd
	domain            *processDomain
	executionID       string
	started           bool
	state             sandboxdv1.ProcessState
	exitCode          *int32
	err               string
	reason            sandboxdv1.TerminationReason
	stopRequested     bool
	gracefulStop      bool
	leaderWaited      bool
	waitErr           error
	terminalRequested bool
	stdout, stderr    outputBuffer
	drains            int
	timer             *time.Timer
	graceTimer        *time.Timer
	doneOnce          sync.Once
}

type ProcessServer struct {
	sandboxdv1.UnimplementedProcessServiceServer
	Workspace      string
	mu             sync.Mutex
	processes      map[processKey]*managedProcess
	closed         bool
	OutputCapacity int
	MaxRecords     int
	supervisor     *Supervisor
}

func NewProcessServer(workspace string, supervisors ...*Supervisor) *ProcessServer {
	sup := NewSupervisor()
	if len(supervisors) != 0 && supervisors[0] != nil {
		sup = supervisors[0]
	}
	return &ProcessServer{Workspace: workspace, processes: make(map[processKey]*managedProcess), OutputCapacity: defaultOutputCapacity, MaxRecords: defaultMaxRecords, supervisor: sup}
}

func validTimeout(ms uint64) bool { return ms <= uint64((time.Duration(1<<63-1))/time.Millisecond) }

func requestKey(key *sandboxdv1.ProcessKey) (processKey, error) {
	if key == nil || key.OwnerId == "" || key.Role == "" {
		return processKey{}, status.Error(codes.InvalidArgument, "key owner_id and role must not be empty")
	}
	return processKey{key.OwnerId, key.Role}, nil
}

func normalizeEnv(mode sandboxdv1.EnvironmentMode, overrides map[string]string) ([]string, error) {
	if mode == sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_UNSPECIFIED {
		mode = sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT
	}
	if mode != sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT && mode != sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_REPLACE {
		return nil, status.Error(codes.InvalidArgument, "unknown environment mode")
	}
	m := map[string]string{}
	names := map[string]string{}
	canonical := func(k string) string {
		if runtime.GOOS == "windows" {
			return strings.ToUpper(k)
		}
		return k
	}
	if mode == sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT {
		for _, item := range os.Environ() {
			if i := strings.IndexByte(item, '='); i >= 0 {
				ck := canonical(item[:i])
				names[ck], m[ck] = item[:i], item[i+1:]
			}
		}
	}
	for k, v := range overrides {
		if k == "" || strings.ContainsAny(k, "=\x00") || strings.ContainsRune(v, 0) {
			return nil, status.Error(codes.InvalidArgument, "invalid environment entry")
		}
		ck := canonical(k)
		names[ck], m[ck] = k, v
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, k := range keys {
		result = append(result, names[k]+"="+m[k])
	}
	return result, nil
}

func (s *ProcessServer) normalizeSpec(spec *sandboxdv1.ProcessSpec) (*sandboxdv1.ProcessSpec, error) {
	if spec == nil || len(spec.Argv) == 0 || spec.Argv[0] == "" {
		return nil, status.Error(codes.InvalidArgument, "spec argv must not be empty")
	}
	cwd := spec.Cwd
	if cwd == "" {
		cwd = s.Workspace
	}
	mode := spec.EnvMode
	if mode == sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_UNSPECIFIED {
		mode = sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT
	}
	if _, err := normalizeEnv(mode, spec.Env); err != nil {
		return nil, err
	}
	if !validTimeout(spec.TimeoutMs) {
		return nil, status.Error(codes.InvalidArgument, "timeout_ms overflows duration")
	}
	return &sandboxdv1.ProcessSpec{Argv: append([]string(nil), spec.Argv...), Cwd: cwd, Env: cloneMap(spec.Env), EnvMode: mode, TimeoutMs: spec.TimeoutMs}, nil
}
func cloneMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func newExecutionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func processResponse(p *managedProcess) *sandboxdv1.Process {
	return &sandboxdv1.Process{Key: &sandboxdv1.ProcessKey{OwnerId: p.key.OwnerId, Role: p.key.Role}, Spec: &sandboxdv1.ProcessSpec{Argv: append([]string(nil), p.spec.Argv...), Cwd: p.spec.Cwd, Env: cloneMap(p.spec.Env), EnvMode: p.spec.EnvMode, TimeoutMs: p.spec.TimeoutMs}, State: p.state, ExitCode: p.exitCode, Error: p.err, ExecutionId: p.executionID, Reason: p.reason}
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, status.Error(codes.Unavailable, "process supervisor epoch is closed")
	}
	if p, ok := s.processes[key]; ok {
		if !reflect.DeepEqual(p.spec, spec) {
			return nil, status.Error(codes.FailedPrecondition, "process key already has a different spec")
		}
		return processResponse(p), nil
	}
	maxRecords := s.MaxRecords
	if maxRecords <= 0 {
		maxRecords = defaultMaxRecords
	}
	if len(s.processes) >= maxRecords {
		return nil, status.Error(codes.ResourceExhausted, "process record limit reached")
	}
	executionID, idErr := newExecutionID()
	if idErr != nil {
		return nil, status.Errorf(codes.Internal, "generate execution id: %v", idErr)
	}
	p := &managedProcess{key: &sandboxdv1.ProcessKey{OwnerId: key.ownerID, Role: key.role}, spec: spec, executionID: executionID}
	cmd := exec.Command(spec.Argv[0], spec.Argv[1:]...)
	cmd.Dir = spec.Cwd
	cmd.Env, _ = normalizeEnv(spec.EnvMode, spec.Env)
	stdin, e := os.Open(os.DevNull)
	if e != nil {
		return nil, status.Errorf(codes.Internal, "open null stdin: %v", e)
	}
	cmd.Stdin = stdin
	p.cmd = cmd
	p.domain = newProcessDomain(cmd)
	stdout, stdoutW, e := os.Pipe()
	if e != nil {
		stdin.Close()
		return nil, status.Errorf(codes.Internal, "stdout pipe: %v", e)
	}
	stderr, stderrW, e := os.Pipe()
	if e != nil {
		stdin.Close()
		stdout.Close()
		stdoutW.Close()
		return nil, status.Errorf(codes.Internal, "stderr pipe: %v", e)
	}
	// Publish only after every fallible pre-launch resource is allocated.
	s.processes[key] = p
	cmd.Stdout, cmd.Stderr = stdoutW, stderrW
	if e = s.supervisor.start(p.domain, func() { s.shutdownManaged(key, p) }); e != nil {
		stdin.Close()
		stdout.Close()
		stdoutW.Close()
		stderr.Close()
		stderrW.Close()
		if errors.Is(e, context.Canceled) {
			delete(s.processes, key)
			return nil, status.Error(codes.Unavailable, "process supervisor epoch is closed")
		}
		stdin.Close()
		stdout.Close()
		stdoutW.Close()
		stderr.Close()
		stderrW.Close()
		p.state = sandboxdv1.ProcessState_PROCESS_STATE_FAILED
		p.err = e.Error()
		p.reason = sandboxdv1.TerminationReason_TERMINATION_REASON_START_FAILED
		p.stdout.eof = true
		p.stderr.eof = true
		return processResponse(p), nil
	}
	stdin.Close()
	stdoutW.Close()
	stderrW.Close()
	p.started = true
	p.state = sandboxdv1.ProcessState_PROCESS_STATE_RUNNING
	p.drains = 2
	go s.drain(p, &p.stdout, stdout)
	go s.drain(p, &p.stderr, stderr)
	if spec.TimeoutMs > 0 {
		p.timer = time.AfterFunc(time.Duration(spec.TimeoutMs)*time.Millisecond, func() { s.requestTermination(key, p, sandboxdv1.TerminationReason_TERMINATION_REASON_TIMEOUT, true) })
	}
	go s.wait(key, p, stdoutW, stderrW)
	return processResponse(p), nil
}
func (s *ProcessServer) drain(p *managedProcess, b *outputBuffer, r io.ReadCloser) {
	defer r.Close()
	buf := make([]byte, 32*1024)
	for {
		n, e := r.Read(buf)
		if n > 0 {
			s.mu.Lock()
			capacity := s.OutputCapacity
			if capacity <= 0 {
				capacity = defaultOutputCapacity
			}
			b.write(buf[:n], capacity)
			s.mu.Unlock()
		}
		if e != nil {
			break
		}
	}
	s.mu.Lock()
	b.eof = true
	p.drains--
	s.finishLocked(p)
	complete := p.terminalRequested && p.drains == 0
	s.mu.Unlock()
	if complete {
		p.doneOnce.Do(func() { s.supervisor.done(p.domain) })
	}
}
func (s *ProcessServer) wait(key processKey, p *managedProcess, writers ...*os.File) {
	e := p.domain.wait()
	if p.timer != nil {
		p.timer.Stop()
	}
	s.mu.Lock()
	if s.processes[key] != p {
		s.mu.Unlock()
		return
	}
	// The waiter wins natural completion before any descendant fencing. An
	// already accepted stop/timeout/close cause remains authoritative.
	if p.reason == sandboxdv1.TerminationReason_TERMINATION_REASON_UNSPECIFIED {
		p.reason = sandboxdv1.TerminationReason_TERMINATION_REASON_EXITED
	}
	p.leaderWaited = true
	p.waitErr = e
	deferFence := p.gracefulStop && p.state == sandboxdv1.ProcessState_PROCESS_STATE_STOPPING
	s.mu.Unlock()
	if deferFence {
		return
	}
	s.finalizeWait(key, p, writers...)
}

func (s *ProcessServer) finalizeWait(key processKey, p *managedProcess, writers ...*os.File) {
	_ = p.domain.force()
	_ = p.domain.close()
	for _, w := range writers {
		_ = w.Close()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.processes[key] != p || p.terminalRequested {
		return
	}
	e := p.waitErr
	code := int32(0)
	if e != nil {
		var ee *exec.ExitError
		if errors.As(e, &ee) {
			code = int32(ee.ExitCode())
		} else {
			p.err = e.Error()
			if p.reason == sandboxdv1.TerminationReason_TERMINATION_REASON_UNSPECIFIED {
				p.reason = sandboxdv1.TerminationReason_TERMINATION_REASON_WAIT_FAILED
			}
		}
	}
	p.exitCode = &code
	p.terminalRequested = true
	s.finishLocked(p)
	if p.terminalRequested && p.drains == 0 {
		p.doneOnce.Do(func() { s.supervisor.done(p.domain) })
	}
}

func (s *ProcessServer) claimManagedCause(key processKey, p *managedProcess, reason sandboxdv1.TerminationReason) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.processes[key] == p && p.reason == sandboxdv1.TerminationReason_TERMINATION_REASON_UNSPECIFIED {
		p.reason = reason
		if p.state == sandboxdv1.ProcessState_PROCESS_STATE_RUNNING {
			p.state = sandboxdv1.ProcessState_PROCESS_STATE_STOPPING
		}
	}
}

func (s *ProcessServer) shutdownManaged(key processKey, p *managedProcess) {
	s.mu.Lock()
	if s.processes[key] != p || p.terminalRequested {
		s.mu.Unlock()
		return
	}
	if p.reason == sandboxdv1.TerminationReason_TERMINATION_REASON_UNSPECIFIED {
		p.reason = sandboxdv1.TerminationReason_TERMINATION_REASON_DAEMON_CLOSED
	}
	if p.state == sandboxdv1.ProcessState_PROCESS_STATE_RUNNING {
		p.state = sandboxdv1.ProcessState_PROCESS_STATE_STOPPING
	}
	p.gracefulStop = false
	if p.timer != nil {
		p.timer.Stop()
	}
	if p.graceTimer != nil {
		p.graceTimer.Stop()
	}
	leaderWaited := p.leaderWaited
	s.mu.Unlock()
	if leaderWaited {
		s.finalizeWait(key, p)
	}
}

func (s *ProcessServer) finishLocked(p *managedProcess) {
	if p.terminalRequested && p.drains == 0 {
		p.state = sandboxdv1.ProcessState_PROCESS_STATE_EXITED
		if p.reason == sandboxdv1.TerminationReason_TERMINATION_REASON_START_FAILED || p.reason == sandboxdv1.TerminationReason_TERMINATION_REASON_WAIT_FAILED {
			p.state = sandboxdv1.ProcessState_PROCESS_STATE_FAILED
		}
		if p.timer != nil {
			p.timer.Stop()
		}
		if p.graceTimer != nil {
			p.graceTimer.Stop()
		}
	}
}
func (s *ProcessServer) requestTermination(key processKey, p *managedProcess, reason sandboxdv1.TerminationReason, force bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.processes[key] != p || p.state != sandboxdv1.ProcessState_PROCESS_STATE_RUNNING {
		return
	}
	p.stopRequested = true
	if p.state == sandboxdv1.ProcessState_PROCESS_STATE_RUNNING {
		p.state = sandboxdv1.ProcessState_PROCESS_STATE_STOPPING
	}
	if p.reason == sandboxdv1.TerminationReason_TERMINATION_REASON_UNSPECIFIED {
		p.reason = reason
	}
	domain := p.domain
	s.mu.Unlock()
	if force {
		_ = domain.force()
	} else {
		_ = domain.terminate()
	}
	s.mu.Lock()
}
func (s *ProcessServer) Get(_ context.Context, req *sandboxdv1.GetProcessRequest) (*sandboxdv1.Process, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	k, e := requestKey(req.Key)
	if e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.processes[k]
	if !ok {
		return nil, status.Error(codes.NotFound, "process not found")
	}
	return processResponse(p), nil
}
func (s *ProcessServer) Stop(_ context.Context, req *sandboxdv1.StopProcessRequest) (*sandboxdv1.Process, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	k, e := requestKey(req.Key)
	if e != nil {
		return nil, e
	}
	mode := req.Mode
	if mode == sandboxdv1.StopMode_STOP_MODE_UNSPECIFIED {
		mode = sandboxdv1.StopMode_STOP_MODE_GRACEFUL
	}
	switch mode {
	case sandboxdv1.StopMode_STOP_MODE_GRACEFUL, sandboxdv1.StopMode_STOP_MODE_INTERRUPT,
		sandboxdv1.StopMode_STOP_MODE_TERMINATE, sandboxdv1.StopMode_STOP_MODE_FORCE:
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown stop mode")
	}
	if mode == sandboxdv1.StopMode_STOP_MODE_INTERRUPT && runtime.GOOS == "windows" {
		return nil, status.Error(codes.Unimplemented, "interrupt control is unsupported on Windows")
	}
	s.mu.Lock()
	p, ok := s.processes[k]
	if !ok {
		s.mu.Unlock()
		return &sandboxdv1.Process{Key: req.Key, State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED}, nil
	}
	if !p.started {
		s.mu.Unlock()
		return processResponse(p), nil
	}
	if p.state == sandboxdv1.ProcessState_PROCESS_STATE_EXITED || p.state == sandboxdv1.ProcessState_PROCESS_STATE_FAILED {
		response := processResponse(p)
		s.mu.Unlock()
		return response, nil
	}
	var reason sandboxdv1.TerminationReason
	force := false
	switch mode {
	case sandboxdv1.StopMode_STOP_MODE_GRACEFUL:
		reason = sandboxdv1.TerminationReason_TERMINATION_REASON_INTERRUPTED
		if runtime.GOOS == "windows" {
			reason = sandboxdv1.TerminationReason_TERMINATION_REASON_TERMINATED
		}
	case sandboxdv1.StopMode_STOP_MODE_INTERRUPT:
		reason = sandboxdv1.TerminationReason_TERMINATION_REASON_INTERRUPTED
	case sandboxdv1.StopMode_STOP_MODE_TERMINATE:
		reason = sandboxdv1.TerminationReason_TERMINATION_REASON_TERMINATED
	case sandboxdv1.StopMode_STOP_MODE_FORCE:
		reason = sandboxdv1.TerminationReason_TERMINATION_REASON_FORCED
		force = true
	}
	firstStop := !p.stopRequested
	p.stopRequested = true
	startGrace := mode == sandboxdv1.StopMode_STOP_MODE_GRACEFUL && firstStop
	if startGrace {
		p.gracefulStop = true
	}
	if mode == sandboxdv1.StopMode_STOP_MODE_FORCE || mode == sandboxdv1.StopMode_STOP_MODE_TERMINATE {
		p.gracefulStop = false
		if p.graceTimer != nil {
			p.graceTimer.Stop()
		}
	}
	if p.state == sandboxdv1.ProcessState_PROCESS_STATE_RUNNING {
		p.state = sandboxdv1.ProcessState_PROCESS_STATE_STOPPING
	}
	if p.reason == 0 {
		p.reason = reason
	}
	domain := p.domain
	resp := processResponse(p)
	s.mu.Unlock()
	switch mode {
	case sandboxdv1.StopMode_STOP_MODE_FORCE:
		_ = domain.force()
		s.mu.Lock()
		leaderWaited := p.leaderWaited
		s.mu.Unlock()
		if leaderWaited {
			s.finalizeWait(k, p)
		}
	case sandboxdv1.StopMode_STOP_MODE_TERMINATE:
		_ = domain.terminate()
		s.mu.Lock()
		leaderWaited := p.leaderWaited
		s.mu.Unlock()
		if leaderWaited {
			s.finalizeWait(k, p)
		}
	case sandboxdv1.StopMode_STOP_MODE_GRACEFUL:
		if !startGrace {
			break
		}
		if runtime.GOOS != "windows" {
			_ = domain.interrupt()
		}
	default:
		_ = domain.interrupt()
	}
	if !force && startGrace {
		grace := time.Duration(req.GracePeriodMs) * time.Millisecond
		if grace == 0 {
			grace = defaultProcessGracePeriod
		}
		graceTimer := time.AfterFunc(grace, func() {
			s.mu.Lock()
			forceNow := s.processes[k] == p && p.gracefulStop && p.state == sandboxdv1.ProcessState_PROCESS_STATE_STOPPING
			p.gracefulStop = false
			leaderWaited := p.leaderWaited
			s.mu.Unlock()
			if forceNow {
				_ = p.domain.force()
				if leaderWaited {
					s.finalizeWait(k, p)
				}
			}
		})
		s.mu.Lock()
		if p.gracefulStop && p.state == sandboxdv1.ProcessState_PROCESS_STATE_STOPPING {
			p.graceTimer = graceTimer
		} else {
			graceTimer.Stop()
		}
		s.mu.Unlock()
	}
	return resp, nil
}
func (s *ProcessServer) ReadOutput(_ context.Context, req *sandboxdv1.ReadOutputRequest) (*sandboxdv1.ReadOutputResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	k, e := requestKey(req.Key)
	if e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.processes[k]
	if !ok {
		return nil, status.Error(codes.NotFound, "process not found")
	}
	if req.ExecutionId != p.executionID {
		return nil, status.Error(codes.FailedPrecondition, "execution_id does not match current epoch record")
	}
	var b *outputBuffer
	switch req.Stream {
	case sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT:
		b = &p.stdout
	case sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR:
		b = &p.stderr
	default:
		return nil, status.Error(codes.InvalidArgument, "stream must be stdout or stderr")
	}
	off := req.Offset
	gap := uint64(0)
	if off < b.start {
		gap = b.start - off
		off = b.start
	}
	if off > b.total {
		return nil, status.Error(codes.OutOfRange, "offset is beyond output end")
	}
	max := int(req.MaxBytes)
	if max == 0 || max > defaultReadMax {
		max = defaultReadMax
	}
	i := int(off - b.start)
	n := min(max, len(b.data)-i)
	data := append([]byte(nil), b.data[i:i+n]...)
	next := off + uint64(n)
	return &sandboxdv1.ReadOutputResponse{Data: data, Offset: off, NextOffset: next, GapBytes: gap, Eof: b.eof && next == b.total, RetainedStart: b.start, ProducedEnd: b.total}, nil
}
func (s *ProcessServer) Close() {
	_ = s.CloseContext(context.Background())
}
func (s *ProcessServer) CloseContext(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return s.supervisor.Close(ctx)
	}
	s.closed = true
	s.mu.Unlock()
	return s.supervisor.Close(ctx)
}
