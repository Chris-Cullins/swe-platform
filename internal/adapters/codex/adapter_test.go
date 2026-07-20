package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/types"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/controllers"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

type launchClient struct {
	sandboxdv1.ProcessServiceClient
	starts        int
	launches      int
	key           *sandboxdv1.ProcessKey
	spec          *sandboxdv1.ProcessSpec
	material      *sandboxdv1.LaunchMaterial
	launchRequest *sandboxdv1.StartProcessWithLaunchMaterialRequest
	launchErr     error
}

func (f *launchClient) Start(_ context.Context, r *sandboxdv1.StartProcessRequest, _ ...grpc.CallOption) (*sandboxdv1.Process, error) {
	f.starts++
	f.key = r.Key
	f.spec = r.Spec
	return &sandboxdv1.Process{}, nil
}
func (f *launchClient) StartWithLaunchMaterial(_ context.Context, r *sandboxdv1.StartProcessWithLaunchMaterialRequest, _ ...grpc.CallOption) (*sandboxdv1.Process, error) {
	f.launches++
	f.key = r.Key
	f.spec = r.Spec
	f.launchRequest = r
	f.material = &sandboxdv1.LaunchMaterial{SecretEnv: map[string][]byte{"CODEX_API_KEY": append([]byte(nil), r.LaunchMaterial.SecretEnv["CODEX_API_KEY"]...)}}
	if f.launchErr != nil {
		return nil, f.launchErr
	}
	return &sandboxdv1.Process{}, nil
}

func sandboxFor(c sandboxdv1.ProcessServiceClient, dials *int) controllers.AdapterSandbox {
	return controllers.AdapterSandbox{EnvironmentUID: "epoch", DialProcess: func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
		*dials++
		return c, func() error { return nil }, nil
	}}
}

func TestAcceptancePromptAndCredentialSafety(t *testing.T) {
	dials := 0
	c := &launchClient{}
	a := &Adapter{Executable: "fake-codex"}
	task := controllers.AdapterTask{ID: "run-uid", Prompt: "--leading\nline"}
	if err := a.EnsureAccepted(context.Background(), task, sandboxFor(c, &dials), nil); err != nil {
		t.Fatal(err)
	}
	want := []string{"fake-codex", "exec", "--json", "--ephemeral", "--ignore-user-config", "--ignore-rules", "--sandbox", "workspace-write", "--color", "never", "--skip-git-repo-check", "--", task.Prompt}
	if !reflect.DeepEqual(c.spec.Argv, want) || c.spec.Env["CODEX_API_KEY"] != "" || c.starts != 1 || c.key.OwnerId != task.ID || c.key.Role != processRole {
		t.Fatalf("spec/start = %#v/%d", c.spec, c.starts)
	}
	credential := &controllers.AdapterCredential{Type: platformv1alpha1.AgentCredentialTypeAPIKey, APIKey: []byte("secret")}
	if err := a.EnsureAccepted(context.Background(), task, sandboxFor(c, &dials), credential); err != nil {
		t.Fatal(err)
	}
	if c.launches != 1 || string(c.material.SecretEnv["CODEX_API_KEY"]) != "secret" || string(credential.APIKey) != "secret" {
		t.Fatalf("launch material = %#v", c.material)
	}
	if c.launchRequest == nil || !reflect.DeepEqual(c.launchRequest.LaunchMaterial.SecretEnv["CODEX_API_KEY"], make([]byte, len("secret"))) {
		t.Fatal("adapter did not clear its temporary launch-material copy")
	}
	before := dials
	for _, rejected := range []struct {
		task controllers.AdapterTask
		cred *controllers.AdapterCredential
	}{{task: controllers.AdapterTask{Prompt: "-"}}, {task: task, cred: &controllers.AdapterCredential{Type: "OAuth"}}} {
		if err := a.EnsureAccepted(context.Background(), rejected.task, sandboxFor(c, &dials), rejected.cred); !errors.Is(err, controllers.ErrAdapterTaskRejected) {
			t.Fatalf("rejection = %v", err)
		}
	}
	if dials != before {
		t.Fatalf("rejections dialed: %d -> %d", before, dials)
	}
}

func TestLaunchMaterialFailureDoesNotFallbackToAmbientStart(t *testing.T) {
	dials := 0
	client := &launchClient{launchErr: status.Error(codes.Unimplemented, "old sandboxd")}
	err := (&Adapter{}).EnsureAccepted(context.Background(), controllers.AdapterTask{ID: "run", Prompt: "task"}, sandboxFor(client, &dials), &controllers.AdapterCredential{Type: platformv1alpha1.AgentCredentialTypeAPIKey, APIKey: []byte("secret")})
	if status.Code(err) != codes.Unimplemented || client.starts != 0 || client.launches != 1 {
		t.Fatalf("EnsureAccepted = %v, starts/launches %d/%d", err, client.starts, client.launches)
	}
}

func TestAcceptanceIsDuplicateSafeAndFreshEpoch(t *testing.T) {
	task := controllers.AdapterTask{ID: "run-uid", Prompt: "task"}
	adapter := &Adapter{}
	first := &launchClient{}
	firstDials := 0
	for range 2 {
		if err := adapter.EnsureAccepted(context.Background(), task, sandboxFor(first, &firstDials), nil); err != nil {
			t.Fatal(err)
		}
	}
	if first.starts != 2 || firstDials != 2 || first.key.OwnerId != task.ID {
		t.Fatalf("first epoch starts/dials/key = %d/%d/%#v", first.starts, firstDials, first.key)
	}
	second := &launchClient{}
	secondDials := 0
	if err := adapter.EnsureAccepted(context.Background(), task, sandboxFor(second, &secondDials), nil); err != nil {
		t.Fatal(err)
	}
	if second.starts != 1 || secondDials != 1 || second.key.OwnerId != task.ID {
		t.Fatalf("second epoch starts/dials/key = %d/%d/%#v", second.starts, secondDials, second.key)
	}
}

func TestTerminalContract(t *testing.T) {
	tests := []struct {
		name, output, thread, detail string
		ok                           bool
	}{
		{"success", `{"type":"thread.started","thread_id":"thread-1"}` + "\n" + `{"type":"turn.completed","usage":{"input_tokens":1}}`, "thread-1", "", true},
		{"missing thread", `{"type":"turn.completed","usage":{}}`, "", "turn.completed before thread.started", false},
		{"empty thread", `{"type":"thread.started","thread_id":""}` + "\n" + `{"type":"turn.completed","usage":{}}`, "", "empty thread.started thread_id", false},
		{"missing terminal", `{"type":"thread.started","thread_id":"t"}`, "t", "missing turn.completed usage", false},
		{"missing usage", `{"type":"thread.started","thread_id":"t"}` + "\n" + `{"type":"turn.completed"}`, "t", "missing turn.completed usage", false},
		{"scalar usage", `{"type":"thread.started","thread_id":"t"}` + "\n" + `{"type":"turn.completed","usage":"invalid"}`, "t", "missing turn.completed usage", false},
		{"failed", `{"type":"thread.started","thread_id":"t"}` + "\n" + `{"type":"turn.failed","error":{"message":"denied"}}`, "t", "denied", false},
		{"error", `{"type":"thread.started","thread_id":"t"}` + "\n" + `{"type":"error","message":"boom"}`, "t", "boom", false},
		{"recovered error", `{"type":"thread.started","thread_id":"t"}` + "\n" + `{"type":"error","message":"retrying"}` + "\n" + `{"type":"item.completed"}` + "\n" + `{"type":"turn.completed","usage":{}}`, "t", "", true},
		{"malformed between events", `{"type":"thread.started","thread_id":"t"}` + "\nnope\n" + `{"type":"turn.completed","usage":{}}`, "t", "malformed JSONL output", false},
		{"later malformed", `{"type":"thread.started","thread_id":"t"}` + "\n" + `{"type":"turn.completed","usage":{}}` + "\nnope", "t", "malformed JSONL output", false},
		{"later event", `{"type":"thread.started","thread_id":"t"}` + "\n" + `{"type":"turn.completed","usage":{}}` + "\n" + `{"type":"item.completed"}`, "t", "output after completed turn", false},
		{"duplicate thread", `{"type":"thread.started","thread_id":"t"}` + "\n" + `{"type":"thread.started","thread_id":"other"}`, "t", "duplicate or late thread.started", false},
		{"valid item", `{"type":"thread.started","thread_id":"t"}` + "\n" + `{"type":"item.completed"}` + "\n" + `{"type":"turn.completed","usage":{}}`, "t", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			thread, detail, ok := terminal([]byte(tc.output))
			if thread != tc.thread || detail != tc.detail || ok != tc.ok {
				t.Fatalf("terminal = %q, %q, %v", thread, detail, ok)
			}
		})
	}
}

type fakeProcessClient struct {
	sandboxdv1.ProcessServiceClient
	process      *sandboxdv1.Process
	stdout       []byte
	stderr       []byte
	gapBytes     uint64
	retainedFrom uint64
	getErr       error
	stopErr      error
	stoppedKey   *sandboxdv1.ProcessKey
	stopMode     sandboxdv1.StopMode
	readRequests []*sandboxdv1.ReadOutputRequest
}

func (f *fakeProcessClient) Get(context.Context, *sandboxdv1.GetProcessRequest, ...grpc.CallOption) (*sandboxdv1.Process, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.process, nil
}

func (f *fakeProcessClient) Stop(_ context.Context, request *sandboxdv1.StopProcessRequest, _ ...grpc.CallOption) (*sandboxdv1.Process, error) {
	f.stoppedKey, f.stopMode = request.Key, request.Mode
	if f.stopErr != nil {
		return nil, f.stopErr
	}
	if f.process == nil {
		return &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED}, nil
	}
	return f.process, nil
}

func (f *fakeProcessClient) ReadOutput(_ context.Context, request *sandboxdv1.ReadOutputRequest, _ ...grpc.CallOption) (*sandboxdv1.ReadOutputResponse, error) {
	f.readRequests = append(f.readRequests, request)
	data := f.stdout
	retainedFrom := f.retainedFrom
	if request.Stream == sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR {
		data, retainedFrom = f.stderr, 0
	}
	producedEnd := retainedFrom + uint64(len(data))
	offset := request.Offset
	var gapBytes uint64
	if offset < retainedFrom {
		gapBytes = retainedFrom - offset
		offset = retainedFrom
	}
	if offset > producedEnd {
		return nil, status.Error(codes.OutOfRange, "offset")
	}
	start := offset - retainedFrom
	end := min(len(data), int(start)+int(request.MaxBytes))
	return &sandboxdv1.ReadOutputResponse{
		Data: append([]byte(nil), data[start:end]...), Offset: offset,
		NextOffset: retainedFrom + uint64(end), GapBytes: gapBytes, RetainedStart: retainedFrom,
		ProducedEnd: producedEnd, Eof: end == len(data),
	}, nil
}

func processSandbox(client sandboxdv1.ProcessServiceClient, epoch string) controllers.AdapterSandbox {
	return controllers.AdapterSandbox{
		EnvironmentUID: types.UID(epoch),
		DialProcess: func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
			return client, func() error { return nil }, nil
		},
	}
}

func TestObservationOutcomes(t *testing.T) {
	exit0, exit1 := int32(0), int32(1)
	success := `{"type":"thread.started","thread_id":"thread-1"}` + "\n" + `{"type":"turn.completed","usage":{"input_tokens":1}}` + "\n"
	tests := []struct {
		name    string
		process *sandboxdv1.Process
		stdout  string
		want    controllers.AdapterObservation
	}{
		{"running", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "e"}, "", controllers.AdapterObservationRunning},
		{"stopping", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_STOPPING, ExecutionId: "e"}, "", controllers.AdapterObservationRunning},
		{"start failure", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_FAILED, ExecutionId: "e", Error: "not found"}, "", controllers.AdapterObservationFailed},
		{"success", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e", ExitCode: &exit0}, success, controllers.AdapterObservationSucceeded},
		{"missing exit", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e"}, success, controllers.AdapterObservationFailed},
		{"nonzero", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e", ExitCode: &exit1}, success, controllers.AdapterObservationFailed},
		{"malformed", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e", ExitCode: &exit0}, "not-json\n", controllers.AdapterObservationFailed},
		{"missing thread", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e", ExitCode: &exit0}, `{"type":"turn.completed","usage":{}}`, controllers.AdapterObservationFailed},
		{"missing terminal", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e", ExitCode: &exit0}, `{"type":"thread.started","thread_id":"t"}`, controllers.AdapterObservationFailed},
		{"turn failed", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e", ExitCode: &exit0}, `{"type":"thread.started","thread_id":"t"}` + "\n" + `{"type":"turn.failed","error":{"message":"failed"}}`, controllers.AdapterObservationFailed},
		{"error", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e", ExitCode: &exit0}, `{"type":"thread.started","thread_id":"t"}` + "\n" + `{"type":"error","message":"failed"}`, controllers.AdapterObservationFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, processSandbox(&fakeProcessClient{process: test.process, stdout: []byte(test.stdout)}, "epoch"))
			if err != nil || got != test.want {
				t.Fatalf("Observe = %q, %v; want %q", got, err, test.want)
			}
		})
	}
	absent, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, processSandbox(&fakeProcessClient{getErr: status.Error(codes.NotFound, "gone")}, "epoch"))
	if err != nil || absent != controllers.AdapterObservationFailed {
		t.Fatalf("absent Observe = %q, %v", absent, err)
	}
}

func TestTerminalObservationFailsOnRetainedOutputGap(t *testing.T) {
	exit0 := int32(0)
	client := &fakeProcessClient{
		process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "execution", ExitCode: &exit0},
		stdout:  []byte(`{"type":"turn.completed","usage":{}}`), retainedFrom: 1024,
	}
	got, detail, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, processSandbox(client, "epoch"))
	if err != nil || got != controllers.AdapterObservationFailed || !strings.Contains(detail, "retained from offset 1024") {
		t.Fatalf("Observe = %q, %q, %v", got, detail, err)
	}
}

func TestCancellationIsRunFencedAndWaitsForTerminalProcess(t *testing.T) {
	client := &fakeProcessClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_STOPPING, ExecutionId: "execution"}}
	err := (&Adapter{}).Cancel(context.Background(), controllers.AdapterTask{ID: "run-uid"}, processSandbox(client, "epoch"))
	if !errors.Is(err, controllers.ErrAdapterCancellationPending) || client.stoppedKey.OwnerId != "run-uid" || client.stoppedKey.Role != processRole || client.stopMode != sandboxdv1.StopMode_STOP_MODE_GRACEFUL {
		t.Fatalf("Cancel = %v, key %#v, mode %s", err, client.stoppedKey, client.stopMode)
	}
	client.process.State = sandboxdv1.ProcessState_PROCESS_STATE_EXITED
	if err := (&Adapter{}).Cancel(context.Background(), controllers.AdapterTask{ID: "run-uid"}, processSandbox(client, "epoch")); err != nil {
		t.Fatalf("terminal Cancel = %v", err)
	}
}

func TestCancellationTreatsAbsentProcessAsComplete(t *testing.T) {
	client := &fakeProcessClient{stopErr: status.Error(codes.NotFound, "absent")}
	if err := (&Adapter{}).Cancel(context.Background(), controllers.AdapterTask{ID: "run-uid"}, processSandbox(client, "epoch")); err != nil {
		t.Fatalf("Cancel absent process = %v", err)
	}
}

func TestOutputIsBoundedGapVisibleAndAdapterOwned(t *testing.T) {
	client := &fakeProcessClient{
		process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"},
		stdout:  []byte("stdout"), stderr: []byte("stderr"), gapBytes: 9, retainedFrom: 9,
	}
	var events []controllers.AdapterEvent
	sandbox := processSandbox(client, "epoch")
	sandbox.EmitEvent = func(_ context.Context, event controllers.AdapterEvent) error {
		events = append(events, event)
		return nil
	}
	adapter := &Adapter{}
	for range 2 {
		if _, _, err := adapter.Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err != nil {
			t.Fatal(err)
		}
	}
	if len(events) != 2 || client.readRequests[0].MaxBytes != pageMax {
		t.Fatalf("events/read requests = %d/%#v", len(events), client.readRequests)
	}
	for _, event := range events {
		if event.Source != "codex" || event.Type != "codex.process-output" || !strings.HasPrefix(event.IdempotencyKey, "v1:") {
			t.Fatalf("event = %#v", event)
		}
	}
	var output outputEvent
	if err := json.Unmarshal(events[0].Data, &output); err != nil || output.GapBytes != 9 || output.RetainedFrom != 9 || string(output.Data) != "stdout" {
		t.Fatalf("stdout output = %#v, %v", output, err)
	}
}

func TestTransientOutputFailureRetriesExactEvent(t *testing.T) {
	client := &fakeProcessClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: []byte("output")}
	var events []controllers.AdapterEvent
	sandbox := processSandbox(client, "epoch")
	sandbox.EmitEvent = func(_ context.Context, event controllers.AdapterEvent) error {
		events = append(events, event)
		if len(events) == 1 {
			return errors.New("retry")
		}
		return nil
	}
	adapter := &Adapter{}
	if _, _, err := adapter.Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err == nil {
		t.Fatal("wanted transient error")
	}
	client.stdout = []byte("output appended")
	if _, _, err := adapter.Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || !reflect.DeepEqual(events[0], events[1]) {
		t.Fatalf("events = %#v", events)
	}
	var appended outputEvent
	if json.Unmarshal(events[2].Data, &appended) != nil || appended.Offset != 6 || string(appended.Data) != " appended" {
		t.Fatalf("appended event = %#v", appended)
	}
}

func TestPermanentOutputRejectionFailsObserveButNotTerminalCancel(t *testing.T) {
	exit0 := int32(0)
	client := &fakeProcessClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: []byte("output")}
	sandbox := processSandbox(client, "epoch")
	sandbox.EmitEvent = func(context.Context, controllers.AdapterEvent) error { return controllers.ErrAdapterEventRejected }
	got, message, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox)
	if err != nil || got != controllers.AdapterObservationFailed || !strings.Contains(message, "permanently rejected") {
		t.Fatalf("Observe = %q, %q, %v", got, message, err)
	}
	client.process.State, client.process.ExitCode = sandboxdv1.ProcessState_PROCESS_STATE_EXITED, &exit0
	if err := (&Adapter{}).Cancel(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err != nil {
		t.Fatalf("Cancel = %v", err)
	}
}

func TestEpochAndSnapshotMetadataFenceOutputKeys(t *testing.T) {
	client := &fakeProcessClient{
		process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"},
		stdout:  bytes.Repeat([]byte("x"), pageMax+1),
	}
	var first controllers.AdapterEvent
	firstSandbox := processSandbox(client, "epoch-one")
	firstSandbox.EmitEvent = func(_ context.Context, event controllers.AdapterEvent) error {
		first = event
		return errors.New("uncertain append")
	}
	if _, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, firstSandbox); err == nil {
		t.Fatal("wanted uncertain append error")
	}
	client.stdout = append(client.stdout, 'y')
	var changed controllers.AdapterEvent
	secondSandbox := processSandbox(client, "epoch-two")
	secondSandbox.EmitEvent = func(_ context.Context, event controllers.AdapterEvent) error {
		changed = event
		return errors.New("stop")
	}
	if _, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, secondSandbox); err == nil {
		t.Fatal("wanted replay stop error")
	}
	if first.IdempotencyKey == changed.IdempotencyKey || bytes.Equal(first.Data, changed.Data) {
		t.Fatalf("keys/payloads did not reflect changed snapshot: %q/%q", first.IdempotencyKey, changed.IdempotencyKey)
	}
}
