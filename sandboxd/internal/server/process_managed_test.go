package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSetManagedProcessHelper(t *testing.T) {
	if os.Getenv("SANDBOXD_MANAGED_HELPER") == "" {
		return
	}
	marker := os.Getenv("MARKER")
	f, err := os.OpenFile(marker, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		os.Exit(91)
	}
	_, _ = f.WriteString(os.Getenv("VALUE") + ":" + strings.Join(os.Environ(), ",") + "\n")
	_ = f.Close()
	if os.Getenv("WAIT") != "" {
		select {}
	}
	if os.Getenv("FAIL") != "" {
		os.Exit(7)
	}
	os.Exit(0)
}

func managedRequest(owner string, rev uint64, role, marker, value string, extra map[string]string) *sandboxdv1.ReconcileManagedServicesRequest {
	env := map[string]string{"SANDBOXD_MANAGED_HELPER": "1", "MARKER": marker, "VALUE": value}
	for k, v := range extra {
		env[k] = v
	}
	services := []*sandboxdv1.ManagedServiceSpec{}
	if role != "" {
		services = append(services, &sandboxdv1.ManagedServiceSpec{Role: role, Spec: &sandboxdv1.ProcessSpec{Argv: []string{os.Args[0], "-test.run=TestSetManagedProcessHelper"}, EnvMode: sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_REPLACE, Env: env}})
	}
	return &sandboxdv1.ReconcileManagedServicesRequest{OwnerId: owner, IntentRevision: rev, Services: services}
}

func waitLines(t *testing.T, path string, n int) []string {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		data, _ := os.ReadFile(path)
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		if len(lines) >= n && lines[0] != "" {
			return lines
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d launches", n)
	return nil
}

func TestManagedServicesRestartSuccessFailureAndEnvironmentIsolation(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			s := NewProcessServer(t.TempDir())
			s.restartInitial = 5 * time.Millisecond
			s.restartMax = 10 * time.Millisecond
			marker := filepath.Join(t.TempDir(), "starts")
			extra := map[string]string{"ONLY_PUBLIC": "yes"}
			if fail {
				extra["FAIL"] = "1"
			}
			if _, err := s.ReconcileManagedServices(context.Background(), managedRequest("uid", 1, "svc", marker, "a", extra)); err != nil {
				t.Fatal(err)
			}
			lines := waitLines(t, marker, 2)
			if !strings.Contains(lines[0], "ONLY_PUBLIC=yes") || strings.Contains(lines[0], "PATH=") {
				t.Fatalf("replace environment leaked or omitted values: %q", lines[0])
			}
			s.Close()
		})
	}
}

func TestManagedRestartDelayProgressionAndCap(t *testing.T) {
	s := NewProcessServer(t.TempDir())
	s.restartInitial = 10 * time.Millisecond
	s.restartMax = 40 * time.Millisecond
	for attempt, want := range []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond, 40 * time.Millisecond, 40 * time.Millisecond} {
		if got := s.managedRestartDelay(uint(attempt)); got != want {
			t.Fatalf("attempt %d delay = %s, want %s", attempt, got, want)
		}
	}
	if got := s.managedRestartDelay(^uint(0)); got != 40*time.Millisecond {
		t.Fatalf("overflow-scale attempt delay = %s, want cap", got)
	}
}

func TestManagedStartRecordExhaustionUsesExponentialBackoff(t *testing.T) {
	s := NewProcessServer(t.TempDir())
	s.MaxRecords = 1
	s.restartInitial = 40 * time.Millisecond
	s.restartMax = 160 * time.Millisecond
	marker := filepath.Join(t.TempDir(), "occupied")
	occupied := managedRequest("other", 1, "slot", marker, "occupied", map[string]string{"WAIT": "1"})
	if _, err := s.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: &sandboxdv1.ProcessKey{OwnerId: "other", Role: "slot"}, Spec: occupied.Services[0].Spec}); err != nil {
		t.Fatal(err)
	}
	waitLines(t, marker, 1)

	attempted := make(chan time.Time, 4)
	s.beforeManagedStart = func() { attempted <- time.Now() }
	request := &sandboxdv1.ReconcileManagedServicesRequest{OwnerId: "uid", IntentRevision: 1, Services: []*sandboxdv1.ManagedServiceSpec{{Role: "svc", Spec: &sandboxdv1.ProcessSpec{Argv: []string{"never-admitted"}}}}}
	if _, err := s.ReconcileManagedServices(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	times := make([]time.Time, 3)
	for i := range times {
		select {
		case times[i] = <-attempted:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for exhausted-record attempt %d", i)
		}
	}
	if first := times[1].Sub(times[0]); first < 30*time.Millisecond {
		t.Fatalf("first exhausted-record retry delay = %s, want about 40ms", first)
	}
	if second := times[2].Sub(times[1]); second < 65*time.Millisecond {
		t.Fatalf("second exhausted-record retry delay = %s, want about 80ms", second)
	}
	s.Close()
}

func TestManagedServicesRevisionReplaceRemoveReaddAndClose(t *testing.T) {
	s := NewProcessServer(t.TempDir())
	s.restartInitial = 5 * time.Millisecond
	marker := filepath.Join(t.TempDir(), "starts")
	r1 := managedRequest("exact-uid", 1, "svc", marker, "old", nil)
	first, err := s.ReconcileManagedServices(context.Background(), r1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReconcileManagedServices(context.Background(), r1); err != nil {
		t.Fatalf("duplicate: %v", err)
	}
	changedSame := managedRequest("exact-uid", 1, "svc", marker, "different", nil)
	if _, err = s.ReconcileManagedServices(context.Background(), changedSame); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("same revision conflict = %v", err)
	}
	if _, err = s.ReconcileManagedServices(context.Background(), managedRequest("exact-uid", 0, "", marker, "", nil)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("zero revision = %v", err)
	}
	if _, err = s.ReconcileManagedServices(context.Background(), managedRequest("exact-uid", 2, "svc", marker, "new", nil)); err != nil {
		t.Fatal(err)
	}
	lines := waitLines(t, marker, 2)
	if !strings.HasPrefix(lines[1], "new:") {
		t.Fatalf("replacement did not use new spec: %v (first execution %s)", lines, first.Services[0].Process.ExecutionId)
	}
	if _, err = s.ReconcileManagedServices(context.Background(), managedRequest("exact-uid", 1, "", marker, "", nil)); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale revision = %v", err)
	}
	if _, err = s.ReconcileManagedServices(context.Background(), managedRequest("exact-uid", 3, "", marker, "", nil)); err != nil {
		t.Fatal(err)
	}
	waitLines(t, marker, 2)
	time.Sleep(40 * time.Millisecond) // allow an already-admitted execution to finish stopping
	data, _ := os.ReadFile(marker)
	before := len(strings.Split(strings.TrimSpace(string(data)), "\n"))
	time.Sleep(40 * time.Millisecond)
	data, _ = os.ReadFile(marker)
	if got := len(strings.Split(strings.TrimSpace(string(data)), "\n")); got != before {
		t.Fatalf("removed service restarted: %d -> %d", before, got)
	}
	if _, err = s.ReconcileManagedServices(context.Background(), managedRequest("exact-uid", 4, "svc", marker, "again", nil)); err != nil {
		t.Fatal(err)
	}
	waitLines(t, marker, before+1)
	s.Close()
	data, _ = os.ReadFile(marker)
	before = len(strings.Split(strings.TrimSpace(string(data)), "\n"))
	time.Sleep(40 * time.Millisecond)
	data, _ = os.ReadFile(marker)
	if got := len(strings.Split(strings.TrimSpace(string(data)), "\n")); got != before {
		t.Fatalf("close did not suppress restart: %d -> %d", before, got)
	}
}

func TestManagedServicesRouteRevisionOrdersSameIntentRevision(t *testing.T) {
	s := NewProcessServer(t.TempDir())
	marker := filepath.Join(t.TempDir(), "starts")
	first := managedRequest("uid", 4, "svc", marker, "first", nil)
	first.RouteRevision = 7
	if _, err := s.ReconcileManagedServices(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	changed := managedRequest("uid", 4, "svc", marker, "changed-url", nil)
	changed.RouteRevision = 8
	if _, err := s.ReconcileManagedServices(context.Background(), changed); err != nil {
		t.Fatalf("higher gateway route revision was rejected: %v", err)
	}
	stale := managedRequest("uid", 4, "svc", marker, "stale", nil)
	stale.RouteRevision = 7
	if _, err := s.ReconcileManagedServices(context.Background(), stale); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale gateway route revision = %v", err)
	}
	s.Close()
}

func TestNewerManagedRevisionRestartsTerminalProcessDuringOldBackoff(t *testing.T) {
	s := NewProcessServer(t.TempDir())
	s.restartInitial = 10 * time.Second
	request := &sandboxdv1.ReconcileManagedServicesRequest{OwnerId: "uid", IntentRevision: 1, Services: []*sandboxdv1.ManagedServiceSpec{{Role: "svc", Spec: &sandboxdv1.ProcessSpec{Argv: []string{filepath.Join(t.TempDir(), "missing")}}}}}
	first, err := s.ReconcileManagedServices(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	failed := first.Services[0].Process
	if failed.State != sandboxdv1.ProcessState_PROCESS_STATE_FAILED {
		t.Fatalf("first process = %#v, want failed", failed)
	}
	request.IntentRevision = 2
	second, err := s.ReconcileManagedServices(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	restarted := second.Services[0].Process
	if restarted.State != sandboxdv1.ProcessState_PROCESS_STATE_FAILED || restarted.ExecutionId == failed.ExecutionId {
		t.Fatalf("new revision did not replace failed execution: first=%#v second=%#v", failed, restarted)
	}
	s.Close()
}

func TestManagedServiceIntentRejectsOrdinaryAdmission(t *testing.T) {
	s := NewProcessServer(t.TempDir())
	marker := filepath.Join(t.TempDir(), "starts")
	request := managedRequest("uid", 1, "svc", marker, "managed", map[string]string{"WAIT": "1"})
	if _, err := s.ReconcileManagedServices(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	waitLines(t, marker, 1)
	key := &sandboxdv1.ProcessKey{OwnerId: "uid", Role: "svc"}
	for name, ordinary := range map[string]func() error{
		"identical spec": func() error {
			_, err := s.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: key, Spec: request.Services[0].Spec})
			return err
		},
		"different spec": func() error {
			_, err := s.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: key, Spec: &sandboxdv1.ProcessSpec{Argv: []string{os.Args[0], "-test.run=TestSetManagedProcessHelper"}, EnvMode: sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_REPLACE, Env: map[string]string{"SANDBOXD_MANAGED_HELPER": "1", "MARKER": marker, "VALUE": "ordinary"}}})
			return err
		},
		"launch material": func() error {
			_, err := s.StartWithLaunchMaterial(context.Background(), &sandboxdv1.StartProcessWithLaunchMaterialRequest{Key: key, Spec: request.Services[0].Spec, LaunchMaterial: &sandboxdv1.LaunchMaterial{SecretEnv: map[string][]byte{"TOKEN": []byte("secret")}}})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ordinary(); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("ordinary collision error = %v", err)
			}
		})
	}
	s.Close()
}

func TestStopSuppressesManagedStartFailureRestart(t *testing.T) {
	s := NewProcessServer(t.TempDir())
	s.restartInitial = 100 * time.Millisecond
	request := &sandboxdv1.ReconcileManagedServicesRequest{OwnerId: "uid", IntentRevision: 1, Services: []*sandboxdv1.ManagedServiceSpec{{Role: "svc", Spec: &sandboxdv1.ProcessSpec{Argv: []string{filepath.Join(t.TempDir(), "missing")}}}}}
	response, err := s.ReconcileManagedServices(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	failed := response.Services[0].Process
	if failed.State != sandboxdv1.ProcessState_PROCESS_STATE_FAILED || failed.ExecutionId == "" {
		t.Fatalf("managed start = %#v, want failed execution", failed)
	}
	if _, err := s.Stop(context.Background(), &sandboxdv1.StopProcessRequest{Key: failed.Key, Mode: sandboxdv1.StopMode_STOP_MODE_FORCE}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	current, err := s.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: failed.Key})
	if err != nil {
		t.Fatal(err)
	}
	if current.ExecutionId != failed.ExecutionId || current.State != sandboxdv1.ProcessState_PROCESS_STATE_FAILED {
		t.Fatalf("stopped failed execution restarted: before=%#v after=%#v", failed, current)
	}
	marker := filepath.Join(t.TempDir(), "re-enabled")
	if _, err := s.ReconcileManagedServices(context.Background(), managedRequest("uid", 2, "svc", marker, "new-intent", nil)); err != nil {
		t.Fatal(err)
	}
	if lines := waitLines(t, marker, 1); !strings.HasPrefix(lines[0], "new-intent:") {
		t.Fatalf("newer intent did not clear explicit suppression: %v", lines)
	}
	s.Close()
}

func TestManagedRestartAdmissionSerializesNewerRemoval(t *testing.T) {
	s := NewProcessServer(t.TempDir())
	s.restartInitial = 5 * time.Millisecond
	marker := filepath.Join(t.TempDir(), "starts")
	request := managedRequest("uid", 1, "svc", marker, "old", map[string]string{"WAIT": "1"})
	if _, err := s.ReconcileManagedServices(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	waitLines(t, marker, 1)

	entered, release := make(chan struct{}), make(chan struct{})
	s.beforeManagedStart = func() {
		close(entered)
		<-release
	}
	s.mu.Lock()
	p := s.processes[processKey{"uid", "svc"}]
	s.mu.Unlock()
	s.requestTermination(processKey{"uid", "svc"}, p, sandboxdv1.TerminationReason_TERMINATION_REASON_TERMINATED, true)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("restart did not reach managed admission hook")
	}

	done := make(chan error, 1)
	go func() {
		_, err := s.ReconcileManagedServices(context.Background(), managedRequest("uid", 2, "", marker, "", nil))
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("newer removal crossed in-flight admission: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	s.beforeManagedStart = nil
	time.Sleep(100 * time.Millisecond)
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner := s.managedOwners["uid"]; owner == nil || len(owner.desired) != 0 {
		t.Fatalf("newer empty desired set was not retained: %#v", owner)
	}
	if current := s.processes[processKey{"uid", "svc"}]; current != nil && (current.state == sandboxdv1.ProcessState_PROCESS_STATE_RUNNING || current.state == sandboxdv1.ProcessState_PROCESS_STATE_STOPPING) {
		t.Fatalf("obsolete service survived newer removal: %#v", processResponse(current))
	}
}
