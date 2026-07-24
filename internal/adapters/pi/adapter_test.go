package pi

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

	"github.com/Chris-Cullins/swe-platform/internal/controllers"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

type startClient struct {
	sandboxdv1.ProcessServiceClient
	requests []*sandboxdv1.StartProcessRequest
	launches int
}

func (c *startClient) Start(_ context.Context, r *sandboxdv1.StartProcessRequest, _ ...grpc.CallOption) (*sandboxdv1.Process, error) {
	c.requests = append(c.requests, r)
	return &sandboxdv1.Process{}, nil
}
func (c *startClient) StartWithLaunchMaterial(context.Context, *sandboxdv1.StartProcessWithLaunchMaterialRequest, ...grpc.CallOption) (*sandboxdv1.Process, error) {
	c.launches++
	return nil, errors.New("unexpected launch material")
}

func acceptanceSandbox(client sandboxdv1.ProcessServiceClient, dials *int) controllers.AdapterSandbox {
	return controllers.AdapterSandbox{EnvironmentUID: "epoch", DialProcess: func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
		*dials++
		return client, func() error { return nil }, nil
	}}
}

func TestAcceptanceArgvAndPreDialRejections(t *testing.T) {
	client := &startClient{}
	dials := 0
	sandbox := acceptanceSandbox(client, &dials)
	task := controllers.AdapterTask{ID: "run-uid", Prompt: "line one\nline two"}
	adapter := &Adapter{Executable: "fake-pi"}
	if err := adapter.EnsureAccepted(context.Background(), task, sandbox, nil); err != nil {
		t.Fatal(err)
	}
	want := []string{"fake-pi", "--mode", "json", "--no-session", "-p", task.Prompt}
	request := client.requests[0]
	if !reflect.DeepEqual(request.Spec.Argv, want) || request.Key.OwnerId != task.ID || request.Key.Role != processRole || request.Spec.EnvMode != sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT || request.Spec.Env != nil || client.launches != 0 {
		t.Fatalf("start request = %#v; launches = %d", request, client.launches)
	}
	for _, prompt := range []string{"-", "--flag", "@file"} {
		if err := adapter.EnsureAccepted(context.Background(), controllers.AdapterTask{Prompt: prompt}, sandbox, nil); !errors.Is(err, controllers.ErrAdapterTaskRejected) {
			t.Fatalf("prompt %q: %v", prompt, err)
		}
	}
	if err := adapter.EnsureAccepted(context.Background(), task, sandbox, &controllers.AdapterCredential{}); !errors.Is(err, controllers.ErrAdapterTaskRejected) {
		t.Fatalf("credential rejection = %v", err)
	}
	if dials != 1 {
		t.Fatalf("preflight rejections dialed sandboxd: %d", dials)
	}
	if adapter.SupportsCredentialProfiles() {
		t.Fatal("Pi unexpectedly supports credential profiles")
	}
}

func TestAcceptanceIsDuplicateSafeAndRestartsInFreshEpoch(t *testing.T) {
	task := controllers.AdapterTask{ID: "run-uid", Prompt: "task"}
	adapter := &Adapter{}
	first, firstDials := &startClient{}, 0
	for range 2 {
		if err := adapter.EnsureAccepted(context.Background(), task, acceptanceSandbox(first, &firstDials), nil); err != nil {
			t.Fatal(err)
		}
	}
	second, secondDials := &startClient{}, 0
	if err := adapter.EnsureAccepted(context.Background(), task, acceptanceSandbox(second, &secondDials), nil); err != nil {
		t.Fatal(err)
	}
	if len(first.requests) != 2 || firstDials != 2 || len(second.requests) != 1 || secondDials != 1 || second.requests[0].Key.OwnerId != task.ID {
		t.Fatalf("first requests/dials=%d/%d second=%d/%d", len(first.requests), firstDials, len(second.requests), secondDials)
	}
}

func TestTerminalContract(t *testing.T) {
	tests := []struct {
		name, output string
		ok           bool
	}{
		{"success", `{"type":"agent_end","messages":[{"role":"user"},{"role":"assistant","stopReason":"stop"}]}`, true},
		{"length", `{"type":"agent_end","messages":[{"role":"assistant","stopReason":"length"}]}`, true},
		{"last assistant before trailing tool", `{"type":"agent_end","messages":[{"role":"assistant","stopReason":"stop"},{"role":"toolResult"}]}`, true},
		{"malformed", `{`, false},
		{"missing event type", `{}`, false},
		{"missing", `{"type":"message_end"}`, false},
		{"empty", `{"type":"agent_end","messages":[]}`, false},
		{"non-assistant", `{"type":"agent_end","messages":[{"role":"user","stopReason":"stop"}]}`, false},
		{"error", `{"type":"agent_end","messages":[{"role":"assistant","stopReason":"error"}]}`, false},
		{"aborted", `{"type":"agent_end","messages":[{"role":"assistant","stopReason":"aborted"}]}`, false},
		{"unknown stop", `{"type":"agent_end","messages":[{"role":"assistant","stopReason":"future"}]}`, false},
		{"after boundary", `{"type":"agent_end","messages":[{"role":"assistant","stopReason":"stop"}]}` + "\n" + `{"type":"message_end"}`, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := terminal([]byte(tc.output)); ok != tc.ok {
				t.Fatalf("terminal ok = %v", ok)
			}
		})
	}
}

type processClient struct {
	sandboxdv1.ProcessServiceClient
	process      *sandboxdv1.Process
	stdout       []byte
	stderr       []byte
	retainedFrom uint64
	getErr       error
	stopErr      error
	stoppedKey   *sandboxdv1.ProcessKey
	stopMode     sandboxdv1.StopMode
	reads        []*sandboxdv1.ReadOutputRequest
}

func (c *processClient) Get(context.Context, *sandboxdv1.GetProcessRequest, ...grpc.CallOption) (*sandboxdv1.Process, error) {
	if c.getErr != nil {
		return nil, c.getErr
	}
	return c.process, nil
}
func (c *processClient) Stop(_ context.Context, r *sandboxdv1.StopProcessRequest, _ ...grpc.CallOption) (*sandboxdv1.Process, error) {
	c.stoppedKey, c.stopMode = r.Key, r.Mode
	if c.stopErr != nil {
		return nil, c.stopErr
	}
	return c.process, nil
}
func (c *processClient) ReadOutput(_ context.Context, r *sandboxdv1.ReadOutputRequest, _ ...grpc.CallOption) (*sandboxdv1.ReadOutputResponse, error) {
	c.reads = append(c.reads, r)
	data, retained := c.stdout, c.retainedFrom
	if r.Stream == sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR {
		data, retained = c.stderr, 0
	}
	produced := retained + uint64(len(data))
	offset := r.Offset
	var gap uint64
	if offset < retained {
		gap, offset = retained-offset, retained
	}
	if offset > produced {
		return nil, status.Error(codes.OutOfRange, "offset")
	}
	start := offset - retained
	end := min(len(data), int(start)+int(r.MaxBytes))
	return &sandboxdv1.ReadOutputResponse{Data: append([]byte(nil), data[start:end]...), Offset: offset, NextOffset: retained + uint64(end), GapBytes: gap, RetainedStart: retained, ProducedEnd: produced, Eof: end == len(data)}, nil
}

func processSandbox(client sandboxdv1.ProcessServiceClient, epoch string) controllers.AdapterSandbox {
	return controllers.AdapterSandbox{EnvironmentUID: types.UID(epoch), DialProcess: func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
		return client, func() error { return nil }, nil
	}}
}

func TestObservationOutcomes(t *testing.T) {
	exit0, exit1 := int32(0), int32(1)
	success := `{"type":"session"}` + "\n" + `{"type":"agent_end","messages":[{"role":"assistant","stopReason":"stop"}]}` + "\n"
	tests := []struct {
		name    string
		process *sandboxdv1.Process
		stdout  string
		want    controllers.AdapterObservation
	}{
		{"running", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "e"}, "", controllers.AdapterObservationRunning},
		{"stopping", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_STOPPING, ExecutionId: "e"}, "", controllers.AdapterObservationRunning},
		{"start failure", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_FAILED, ExecutionId: "e", Error: "missing"}, "", controllers.AdapterObservationFailed},
		{"success", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e", ExitCode: &exit0}, success, controllers.AdapterObservationSucceeded},
		{"missing exit", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e"}, success, controllers.AdapterObservationFailed},
		{"nonzero", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e", ExitCode: &exit1}, success, controllers.AdapterObservationFailed},
		{"malformed", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e", ExitCode: &exit0}, "not-json\n", controllers.AdapterObservationFailed},
		{"missing terminal", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e", ExitCode: &exit0}, `{"type":"session"}`, controllers.AdapterObservationFailed},
		{"assistant error despite zero exit", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e", ExitCode: &exit0}, `{"type":"agent_end","messages":[{"role":"assistant","stopReason":"error"}]}`, controllers.AdapterObservationFailed},
		{"assistant aborted despite zero exit", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e", ExitCode: &exit0}, `{"type":"agent_end","messages":[{"role":"assistant","stopReason":"aborted"}]}`, controllers.AdapterObservationFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, processSandbox(&processClient{process: tc.process, stdout: []byte(tc.stdout)}, "epoch"))
			if err != nil || got != tc.want {
				t.Fatalf("Observe = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
	absent, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, processSandbox(&processClient{getErr: status.Error(codes.NotFound, "gone")}, "epoch"))
	if err != nil || absent != controllers.AdapterObservationFailed {
		t.Fatalf("absent Observe = %q, %v", absent, err)
	}
}

func TestTerminalValidationFailsOnRetainedOutputGap(t *testing.T) {
	exit0 := int32(0)
	client := &processClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e", ExitCode: &exit0}, stdout: []byte(`{"type":"agent_end","messages":[]}`), retainedFrom: 41}
	got, detail, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, processSandbox(client, "epoch"))
	if err != nil || got != controllers.AdapterObservationFailed || !strings.Contains(detail, "retained from offset 41") {
		t.Fatalf("Observe = %q, %q, %v", got, detail, err)
	}
}

func TestCancellationIsRunFencedAndAbsentIsComplete(t *testing.T) {
	client := &processClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_STOPPING, ExecutionId: "e"}}
	err := (&Adapter{}).Cancel(context.Background(), controllers.AdapterTask{ID: "run-uid"}, processSandbox(client, "epoch"))
	if !errors.Is(err, controllers.ErrAdapterCancellationPending) || client.stoppedKey.OwnerId != "run-uid" || client.stoppedKey.Role != processRole || client.stopMode != sandboxdv1.StopMode_STOP_MODE_GRACEFUL {
		t.Fatalf("Cancel = %v, key=%#v, mode=%s", err, client.stoppedKey, client.stopMode)
	}
	client.process.State = sandboxdv1.ProcessState_PROCESS_STATE_EXITED
	if err := (&Adapter{}).Cancel(context.Background(), controllers.AdapterTask{ID: "run-uid"}, processSandbox(client, "epoch")); err != nil {
		t.Fatal(err)
	}
	absent := &processClient{stopErr: status.Error(codes.NotFound, "gone")}
	if err := (&Adapter{}).Cancel(context.Background(), controllers.AdapterTask{ID: "run-uid"}, processSandbox(absent, "epoch")); err != nil {
		t.Fatal(err)
	}
}

func TestOutputIsBoundedGapVisibleAndAdapterOwned(t *testing.T) {
	client := &processClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: []byte("stdout"), stderr: []byte("stderr"), retainedFrom: 9}
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
	if len(events) != 2 || client.reads[0].MaxBytes != pageMax {
		t.Fatalf("events/reads = %d/%#v", len(events), client.reads)
	}
	for _, event := range events {
		if event.Source != "pi" || event.Type != "pi.process-output" || !strings.HasPrefix(event.IdempotencyKey, "v1:") {
			t.Fatalf("event = %#v", event)
		}
	}
	var output outputEvent
	if err := json.Unmarshal(events[0].Data, &output); err != nil || output.GapBytes != 9 || output.RetainedFrom != 9 || output.Offset != 9 || string(output.Data) != "stdout" {
		t.Fatalf("output = %#v, %v", output, err)
	}
}

func TestTransientOutputFailureRetriesExactEvent(t *testing.T) {
	client := &processClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: []byte("output")}
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
		t.Fatalf("appended = %#v", appended)
	}
}

func TestPermanentOutputRejectionFailsObserveButNotTerminalCancel(t *testing.T) {
	exit0 := int32(0)
	client := &processClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: []byte("output")}
	sandbox := processSandbox(client, "epoch")
	sandbox.EmitEvent = func(context.Context, controllers.AdapterEvent) error { return controllers.ErrAdapterEventRejected }
	got, detail, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox)
	if err != nil || got != controllers.AdapterObservationFailed || !strings.Contains(detail, "permanently rejected") {
		t.Fatalf("Observe = %q, %q, %v", got, detail, err)
	}
	client.process.State, client.process.ExitCode = sandboxdv1.ProcessState_PROCESS_STATE_EXITED, &exit0
	if err := (&Adapter{}).Cancel(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err != nil {
		t.Fatal(err)
	}
}

func TestEpochAndSnapshotMetadataFenceOutputKeys(t *testing.T) {
	client := &processClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: bytes.Repeat([]byte("x"), pageMax+1)}
	var first controllers.AdapterEvent
	firstSandbox := processSandbox(client, "epoch-one")
	firstSandbox.EmitEvent = func(_ context.Context, event controllers.AdapterEvent) error {
		first = event
		return errors.New("uncertain")
	}
	if _, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, firstSandbox); err == nil {
		t.Fatal("wanted uncertain append")
	}
	client.stdout = append(client.stdout, 'y')
	var second controllers.AdapterEvent
	secondSandbox := processSandbox(client, "epoch-two")
	secondSandbox.EmitEvent = func(_ context.Context, event controllers.AdapterEvent) error {
		second = event
		return errors.New("stop")
	}
	if _, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, secondSandbox); err == nil {
		t.Fatal("wanted replay stop")
	}
	if first.IdempotencyKey == second.IdempotencyKey || bytes.Equal(first.Data, second.Data) {
		t.Fatalf("keys/payloads did not reflect snapshot change: %q/%q", first.IdempotencyKey, second.IdempotencyKey)
	}
}
