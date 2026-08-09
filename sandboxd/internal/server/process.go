package server

import (
	"bytes"
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
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const (
	defaultProcessGracePeriod = 5 * time.Second
	defaultOutputCapacity     = 1024 * 1024
	defaultMaxRecords         = 1024
	defaultReadMax            = 64 * 1024
	maxManagedServices        = 32
	maxManagedOwnerBytes      = 256
	maxManagedRoleBytes       = 32
	maxManagedArgs            = 64
	maxManagedArgBytes        = 4096
	maxManagedArgvBytes       = 16 * 1024
	maxManagedCWDBytes        = 4096
	maxManagedEnvEntries      = 64
	maxLaunchMaterialEntries  = 64
	maxLaunchMaterialName     = 256
	maxLaunchMaterialValue    = 64 * 1024
	maxLaunchMaterialTotal    = 256 * 1024
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
	secretLaunch      bool
	timer             *time.Timer
	graceTimer        *time.Timer
	doneOnce          sync.Once
	managed           bool
	managedGeneration uint64
	restartAttempt    uint
	startedAt         time.Time
	managedSuppressed bool
}

type ProcessServer struct {
	sandboxdv1.UnimplementedProcessServiceServer
	Workspace          string
	mu                 sync.Mutex
	processes          map[processKey]*managedProcess
	closed             bool
	OutputCapacity     int
	MaxRecords         int
	supervisor         *Supervisor
	reconcileMu        sync.Mutex
	managedOwners      map[string]*managedOwner
	restartInitial     time.Duration
	restartMax         time.Duration
	restartStable      time.Duration
	beforeManagedStart func()
}

type managedOwner struct {
	revision      uint64
	routeRevision uint64
	desired       map[string]*sandboxdv1.ProcessSpec
	restarts      map[string]*time.Timer
	gen           uint64
}

func NewProcessServer(workspace string, supervisors ...*Supervisor) *ProcessServer {
	sup := NewSupervisor()
	if len(supervisors) != 0 && supervisors[0] != nil {
		sup = supervisors[0]
	}
	return &ProcessServer{Workspace: workspace, processes: make(map[processKey]*managedProcess), managedOwners: make(map[string]*managedOwner), OutputCapacity: defaultOutputCapacity, MaxRecords: defaultMaxRecords, supervisor: sup, restartInitial: 25 * time.Millisecond, restartMax: 5 * time.Second, restartStable: 30 * time.Second}
}

func (s *ProcessServer) maxRecords() int {
	if s.MaxRecords > 0 {
		return s.MaxRecords
	}
	return defaultMaxRecords
}

func validTimeout(ms uint64) bool { return ms <= uint64((time.Duration(1<<63-1))/time.Millisecond) }

func requestKey(key *sandboxdv1.ProcessKey) (processKey, error) {
	if key == nil || key.OwnerId == "" || key.Role == "" {
		return processKey{}, status.Error(codes.InvalidArgument, "key owner_id and role must not be empty")
	}
	return processKey{key.OwnerId, key.Role}, nil
}

func canonicalEnvName(name string) string {
	if runtime.GOOS == "windows" {
		return strings.ToUpper(name)
	}
	return name
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
	if mode == sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT {
		for _, item := range os.Environ() {
			if i := strings.IndexByte(item, '='); i >= 0 {
				ck := canonicalEnvName(item[:i])
				names[ck], m[ck] = item[:i], item[i+1:]
			}
		}
	}
	for k, v := range overrides {
		if k == "" || strings.ContainsAny(k, "=\x00") || strings.ContainsRune(v, 0) {
			return nil, status.Error(codes.InvalidArgument, "invalid environment entry")
		}
		ck := canonicalEnvName(k)
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
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	if err := s.rejectManagedKey(req.Key); err != nil {
		return nil, err
	}
	return s.start(req.Key, req.Spec, nil, false)
}

func (s *ProcessServer) StartWithLaunchMaterial(_ context.Context, req *sandboxdv1.StartProcessWithLaunchMaterialRequest) (*sandboxdv1.Process, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	secretEnv := req.GetLaunchMaterial().GetSecretEnv()
	defer clearSecretEnv(secretEnv)
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	if err := s.rejectManagedKey(req.Key); err != nil {
		return nil, err
	}
	return s.start(req.Key, req.Spec, secretEnv, true)
}

func (s *ProcessServer) rejectManagedKey(keyRequest *sandboxdv1.ProcessKey) error {
	key, err := requestKey(keyRequest)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner := s.managedOwners[key.ownerID]; owner != nil && owner.desired[key.role] != nil {
		return status.Error(codes.FailedPrecondition, "process key is owned by managed service intent")
	}
	return nil
}

// ReconcileManagedServices accepts a complete desired service set. Reconcile
// calls are serialized so an older call can never perform launches after a
// newer revision has been accepted.
func (s *ProcessServer) ReconcileManagedServices(_ context.Context, req *sandboxdv1.ReconcileManagedServicesRequest) (*sandboxdv1.ReconcileManagedServicesResponse, error) {
	if req == nil || req.OwnerId == "" || req.IntentRevision == 0 {
		return nil, status.Error(codes.InvalidArgument, "owner_id and positive intent_revision are required")
	}
	if len(req.OwnerId) > maxManagedOwnerBytes || !utf8.ValidString(req.OwnerId) || strings.ContainsRune(req.OwnerId, 0) {
		return nil, status.Errorf(codes.InvalidArgument, "owner_id must be valid UTF-8 without NUL and at most %d bytes", maxManagedOwnerBytes)
	}
	if len(req.Services) > maxManagedServices {
		return nil, status.Errorf(codes.InvalidArgument, "services exceeds maximum of %d", maxManagedServices)
	}
	if !s.reconcileMu.TryLock() {
		return nil, status.Error(codes.ResourceExhausted, "managed service reconciliation is busy")
	}
	defer s.reconcileMu.Unlock()
	desired := make(map[string]*sandboxdv1.ProcessSpec, len(req.Services))
	roles := make([]string, 0, len(req.Services))
	for _, service := range req.Services {
		if service == nil || service.Role == "" || len(service.Role) > maxManagedRoleBytes || !utf8.ValidString(service.Role) || strings.ContainsRune(service.Role, 0) {
			return nil, status.Errorf(codes.InvalidArgument, "service role must be valid UTF-8 without NUL and 1-%d bytes", maxManagedRoleBytes)
		}
		if _, exists := desired[service.Role]; exists {
			return nil, status.Error(codes.InvalidArgument, "service roles must be unique")
		}
		if err := validateManagedSpec(service.Spec); err != nil {
			return nil, err
		}
		spec, err := s.normalizeSpec(service.Spec)
		if err != nil {
			return nil, err
		}
		if len(spec.Env) == 0 {
			spec.Env = nil
		}
		desired[service.Role] = spec
		roles = append(roles, service.Role)
	}
	sort.Strings(roles)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "process supervisor epoch is closed")
	}
	owner := s.managedOwners[req.OwnerId]
	if owner != nil {
		if req.IntentRevision < owner.revision || req.IntentRevision == owner.revision && req.RouteRevision < owner.routeRevision {
			s.mu.Unlock()
			return nil, status.Error(codes.FailedPrecondition, "managed service revision is stale")
		}
		if req.IntentRevision == owner.revision && req.RouteRevision == owner.routeRevision {
			if !reflect.DeepEqual(desired, owner.desired) {
				s.mu.Unlock()
				return nil, status.Error(codes.FailedPrecondition, "intent_revision has a different desired set")
			}
			resp := s.managedResponseLocked(req.OwnerId, owner)
			s.mu.Unlock()
			return resp, nil
		}
	}
	maxRecords := s.maxRecords()
	if owner == nil && len(s.managedOwners) >= maxRecords {
		s.mu.Unlock()
		return nil, status.Error(codes.ResourceExhausted, "managed service owner limit reached")
	}
	desiredRecords := len(desired)
	for ownerID, existing := range s.managedOwners {
		if ownerID != req.OwnerId {
			desiredRecords += len(existing.desired)
		}
	}
	if desiredRecords > maxRecords {
		s.mu.Unlock()
		return nil, status.Error(codes.ResourceExhausted, "managed service desired record limit reached")
	}
	gen := uint64(1)
	if owner != nil {
		gen = owner.gen + 1
		s.cancelManagedRestartsLocked(owner)
	}
	owner = &managedOwner{revision: req.IntentRevision, routeRevision: req.RouteRevision, desired: desired, restarts: make(map[string]*time.Timer), gen: gen}
	s.managedOwners[req.OwnerId] = owner
	var stop []*managedProcess
	toStart := make(map[string]struct{}, len(desired))
	for role := range desired {
		toStart[role] = struct{}{}
	}
	for key, p := range s.processes {
		if key.ownerID != req.OwnerId {
			continue
		}
		spec, wanted := desired[key.role]
		if !p.managed || !wanted || !reflect.DeepEqual(spec, p.spec) {
			p.managed = false
			p.managedSuppressed = false
			if p.state == sandboxdv1.ProcessState_PROCESS_STATE_RUNNING {
				stop = append(stop, p)
			}
		} else {
			p.managedGeneration = gen
			if p.state != sandboxdv1.ProcessState_PROCESS_STATE_EXITED && p.state != sandboxdv1.ProcessState_PROCESS_STATE_FAILED {
				delete(toStart, key.role)
			}
		}
	}
	s.mu.Unlock()
	for _, p := range stop {
		s.requestTermination(processKey{p.key.OwnerId, p.key.Role}, p, sandboxdv1.TerminationReason_TERMINATION_REASON_TERMINATED, true)
	}
	// Changed slots are launched by the old execution's completion; brand-new
	// slots can be admitted immediately.
	for role := range toStart {
		s.ensureManagedUnderReconcile(req.OwnerId, role, gen, 0)
	}
	s.mu.Lock()
	resp := s.managedResponseLocked(req.OwnerId, owner)
	s.mu.Unlock()
	return resp, nil
}

func validateManagedSpec(spec *sandboxdv1.ProcessSpec) error {
	if spec == nil || len(spec.Argv) == 0 {
		return status.Error(codes.InvalidArgument, "managed service spec argv must not be empty")
	}
	if len(spec.Argv) > maxManagedArgs {
		return status.Errorf(codes.InvalidArgument, "managed service argv exceeds maximum of %d arguments", maxManagedArgs)
	}
	argvBytes := 0
	for _, arg := range spec.Argv {
		if arg == "" || len(arg) > maxManagedArgBytes || !utf8.ValidString(arg) || strings.ContainsRune(arg, 0) {
			return status.Errorf(codes.InvalidArgument, "managed service argument must be valid UTF-8 without NUL and 1-%d bytes", maxManagedArgBytes)
		}
		argvBytes += len(arg)
	}
	if argvBytes > maxManagedArgvBytes {
		return status.Errorf(codes.InvalidArgument, "managed service argv exceeds %d aggregate bytes", maxManagedArgvBytes)
	}
	if len(spec.Cwd) > maxManagedCWDBytes || !utf8.ValidString(spec.Cwd) || strings.ContainsRune(spec.Cwd, 0) {
		return status.Errorf(codes.InvalidArgument, "managed service cwd must be valid UTF-8 without NUL and at most %d bytes", maxManagedCWDBytes)
	}
	if len(spec.Env) > maxManagedEnvEntries {
		return status.Errorf(codes.InvalidArgument, "managed service environment exceeds maximum of %d entries", maxManagedEnvEntries)
	}
	envBytes := 0
	for name, value := range spec.Env {
		if len(name) > maxLaunchMaterialName || !utf8.ValidString(name) || len(value) > maxLaunchMaterialValue || !utf8.ValidString(value) {
			return status.Error(codes.InvalidArgument, "managed service environment entry exceeds portable size or encoding limits")
		}
		envBytes += len(name) + len(value)
	}
	if envBytes > maxLaunchMaterialTotal {
		return status.Errorf(codes.InvalidArgument, "managed service environment exceeds %d aggregate bytes", maxLaunchMaterialTotal)
	}
	return nil
}

func (s *ProcessServer) managedResponseLocked(ownerID string, owner *managedOwner) *sandboxdv1.ReconcileManagedServicesResponse {
	resp := &sandboxdv1.ReconcileManagedServicesResponse{OwnerId: ownerID, IntentRevision: owner.revision, RouteRevision: owner.routeRevision}
	roles := make([]string, 0, len(owner.desired))
	for role := range owner.desired {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	for _, role := range roles {
		var process *sandboxdv1.Process
		if p := s.processes[processKey{ownerID, role}]; p != nil {
			process = processResponse(p)
		}
		resp.Services = append(resp.Services, &sandboxdv1.ManagedServiceStatus{Role: role, Process: process})
	}
	return resp
}

// ensureManagedUnderReconcile is called only while reconcileMu is held. The
// admission gap around start therefore cannot overlap a newer desired set or
// an explicit Stop and resurrect an obsolete execution.
func (s *ProcessServer) ensureManagedUnderReconcile(ownerID, role string, gen uint64, attempt uint) {
	s.mu.Lock()
	owner := s.managedOwners[ownerID]
	if s.closed || owner == nil || owner.gen != gen || owner.desired[role] == nil {
		s.mu.Unlock()
		return
	}
	key := processKey{ownerID, role}
	if old := s.processes[key]; old != nil {
		if old.managedSuppressed && old.managedGeneration == gen {
			s.mu.Unlock()
			return
		}
		if old.state != sandboxdv1.ProcessState_PROCESS_STATE_EXITED && old.state != sandboxdv1.ProcessState_PROCESS_STATE_FAILED {
			s.mu.Unlock()
			return
		}
	}
	delete(s.processes, key)
	spec := owner.desired[role]
	s.mu.Unlock()
	if s.beforeManagedStart != nil {
		s.beforeManagedStart()
	}
	_, startErr := s.start(&sandboxdv1.ProcessKey{OwnerId: ownerID, Role: role}, spec, nil, false)
	s.mu.Lock()
	if startErr != nil {
		if owner == s.managedOwners[ownerID] && owner.gen == gen && owner.desired[role] != nil && !s.closed {
			delay := s.managedRestartDelay(attempt)
			s.scheduleManagedEnsureLocked(ownerID, role, owner, attempt+1, delay)
		}
		s.mu.Unlock()
		return
	}
	if p := s.processes[key]; p != nil && owner == s.managedOwners[ownerID] && owner.gen == gen && reflect.DeepEqual(p.spec, spec) {
		p.managed, p.managedGeneration, p.restartAttempt, p.startedAt = true, gen, attempt, time.Now()
		if p.state == sandboxdv1.ProcessState_PROCESS_STATE_EXITED || p.state == sandboxdv1.ProcessState_PROCESS_STATE_FAILED {
			s.scheduleManagedRestartLocked(key, p)
		}
	}
	s.mu.Unlock()
}

func (s *ProcessServer) scheduleManagedRestartLocked(key processKey, p *managedProcess) {
	if !p.managed || s.closed {
		return
	}
	owner := s.managedOwners[key.ownerID]
	if owner == nil || owner.gen != p.managedGeneration || !reflect.DeepEqual(owner.desired[key.role], p.spec) {
		return
	}
	attempt := p.restartAttempt
	if time.Since(p.startedAt) >= s.restartStable {
		attempt = 0
	}
	delay := s.managedRestartDelay(attempt)
	s.scheduleManagedEnsureLocked(key.ownerID, key.role, owner, attempt+1, delay)
}

func (s *ProcessServer) scheduleManagedEnsureLocked(ownerID, role string, owner *managedOwner, attempt uint, delay time.Duration) {
	if timer := owner.restarts[role]; timer != nil {
		timer.Stop()
	}
	var timer *time.Timer
	timer = time.AfterFunc(delay, func() {
		s.mu.Lock()
		current := s.managedOwners[ownerID]
		valid := current == owner && !s.closed && current.restarts[role] == timer && current.desired[role] != nil
		s.mu.Unlock()
		if !valid {
			return
		}
		if !s.reconcileMu.TryLock() {
			s.mu.Lock()
			current = s.managedOwners[ownerID]
			if current == owner && !s.closed && current.restarts[role] == timer && current.desired[role] != nil {
				s.scheduleManagedEnsureLocked(ownerID, role, owner, attempt, s.restartInitial)
			}
			s.mu.Unlock()
			return
		}
		defer s.reconcileMu.Unlock()
		s.mu.Lock()
		current = s.managedOwners[ownerID]
		valid = current == owner && !s.closed && current.restarts[role] == timer && current.desired[role] != nil
		if valid {
			delete(current.restarts, role)
		}
		s.mu.Unlock()
		if valid {
			s.ensureManagedUnderReconcile(ownerID, role, owner.gen, attempt)
		}
	})
	owner.restarts[role] = timer
}

func (s *ProcessServer) cancelManagedRestartsLocked(owner *managedOwner) {
	for role, timer := range owner.restarts {
		timer.Stop()
		delete(owner.restarts, role)
	}
}

func (s *ProcessServer) managedRestartDelay(attempt uint) time.Duration {
	delay := s.restartInitial
	if delay >= s.restartMax {
		return s.restartMax
	}
	for i := uint(0); i < attempt; i++ {
		if delay >= s.restartMax-delay {
			return s.restartMax
		}
		delay *= 2
	}
	return delay
}

func clearSecretEnv(env map[string][]byte) {
	for _, value := range env {
		clear(value)
	}
}

func mergeSecretEnv(environment []string, secret map[string][]byte) []string {
	secretNames := make(map[string]struct{}, len(secret))
	for name := range secret {
		secretNames[canonicalEnvName(name)] = struct{}{}
	}
	merged := environment[:0]
	for _, item := range environment {
		separator := strings.IndexByte(item, '=')
		if separator < 0 {
			continue
		}
		if _, replaced := secretNames[canonicalEnvName(item[:separator])]; !replaced {
			merged = append(merged, item)
		}
	}
	for name, value := range secret {
		merged = append(merged, name+"="+string(value))
	}
	return merged
}

func portableEnvName(name string) bool {
	if name == "" || !utf8.ValidString(name) || len(name) > maxLaunchMaterialName {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' || (i > 0 && c >= '0' && c <= '9') {
			continue
		}
		return false
	}
	return true
}

func validateLaunchMaterial(secret map[string][]byte, public map[string]string) error {
	if len(secret) > maxLaunchMaterialEntries {
		return status.Error(codes.InvalidArgument, "launch material has too many environment entries")
	}
	total := 0
	names := make(map[string]struct{}, len(secret))
	for name, value := range secret {
		if !portableEnvName(name) {
			return status.Error(codes.InvalidArgument, "launch material has an invalid environment name")
		}
		if len(value) > maxLaunchMaterialValue || bytes.IndexByte(value, 0) >= 0 {
			return status.Error(codes.InvalidArgument, "launch material has an invalid environment value")
		}
		total += len(name) + len(value)
		if total > maxLaunchMaterialTotal {
			return status.Error(codes.InvalidArgument, "launch material exceeds aggregate size limit")
		}
		canonical := canonicalEnvName(name)
		if _, exists := names[canonical]; exists {
			return status.Error(codes.InvalidArgument, "launch material has duplicate environment names")
		}
		names[canonical] = struct{}{}
	}
	for name := range public {
		canonical := canonicalEnvName(name)
		if _, exists := names[canonical]; exists {
			return status.Error(codes.InvalidArgument, "launch material conflicts with public environment")
		}
	}
	return nil
}

func (s *ProcessServer) start(keyRequest *sandboxdv1.ProcessKey, specRequest *sandboxdv1.ProcessSpec, secretEnv map[string][]byte, secretLaunch bool) (*sandboxdv1.Process, error) {
	key, err := requestKey(keyRequest)
	if err != nil {
		return nil, err
	}
	spec, err := s.normalizeSpec(specRequest)
	if err != nil {
		return nil, err
	}
	if secretLaunch {
		if err := validateLaunchMaterial(secretEnv, spec.Env); err != nil {
			return nil, err
		}
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
		if p.secretLaunch != secretLaunch {
			return nil, status.Error(codes.FailedPrecondition, "process key already has a different launch mode")
		}
		return processResponse(p), nil
	}
	maxRecords := s.maxRecords()
	if len(s.processes) >= maxRecords {
		return nil, status.Error(codes.ResourceExhausted, "process record limit reached")
	}
	executionID, idErr := newExecutionID()
	if idErr != nil {
		return nil, status.Errorf(codes.Internal, "generate execution id: %v", idErr)
	}
	p := &managedProcess{key: &sandboxdv1.ProcessKey{OwnerId: key.ownerID, Role: key.role}, spec: spec, executionID: executionID, secretLaunch: secretLaunch}
	cmd := exec.Command(spec.Argv[0], spec.Argv[1:]...)
	cmd.Dir = spec.Cwd
	cmd.Env, _ = normalizeEnv(spec.EnvMode, spec.Env)
	cmd.Env = mergeSecretEnv(cmd.Env, secretEnv)
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
	e = s.supervisor.start(p.domain, func() { s.shutdownManaged(key, p) })
	// exec.Cmd has copied the environment into OS-owned launch storage by now.
	// Drop our only retained copy whether launch succeeded or failed.
	clear(cmd.Env)
	cmd.Env = nil
	if e != nil {
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
		go s.finalizeWait(key, p)
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
		key := processKey{p.key.OwnerId, p.key.Role}
		if p.managed {
			s.scheduleManagedRestartLocked(key, p)
		} else if owner := s.managedOwners[key.ownerID]; owner != nil && owner.desired[key.role] != nil && !p.managedSuppressed && !s.closed {
			s.scheduleManagedEnsureLocked(key.ownerID, key.role, owner, 0, 0)
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
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
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
	if p.managed {
		p.managed = false
		p.managedSuppressed = true
	}
	if !p.started {
		response := processResponse(p)
		s.mu.Unlock()
		return response, nil
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
	for _, owner := range s.managedOwners {
		s.cancelManagedRestartsLocked(owner)
	}
	s.mu.Unlock()
	return s.supervisor.Close(ctx)
}
