package amp

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

	"github.com/Chris-Cullins/swe-platform/internal/controllers"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

type fakeClient struct {
	process      *sandboxdv1.Process
	stdout       []byte
	stderr       []byte
	gapBytes     uint64
	retainedFrom uint64
	getErr       error
	starts       int
	launches     int
	startedKey   *sandboxdv1.ProcessKey
	startedSpec  *sandboxdv1.ProcessSpec
	stoppedKey   *sandboxdv1.ProcessKey
	readRequests []*sandboxdv1.ReadOutputRequest
}

func (f *fakeClient) Start(_ context.Context, request *sandboxdv1.StartProcessRequest, _ ...grpc.CallOption) (*sandboxdv1.Process, error) {
	f.starts++
	if f.startedKey == nil {
		f.launches++
		f.startedKey, f.startedSpec = request.Key, request.Spec
	} else if !reflect.DeepEqual(f.startedKey, request.Key) || !reflect.DeepEqual(f.startedSpec, request.Spec) {
		return nil, status.Error(codes.FailedPrecondition, "conflicting start")
	}
	if f.process == nil {
		f.process = &sandboxdv1.Process{Key: request.Key, Spec: request.Spec, State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}
	}
	return f.process, nil
}

func (f *fakeClient) StartWithLaunchMaterial(context.Context, *sandboxdv1.StartProcessWithLaunchMaterialRequest, ...grpc.CallOption) (*sandboxdv1.Process, error) {
	return nil, errors.New("unexpected launch material")
}
func (f *fakeClient) Get(context.Context, *sandboxdv1.GetProcessRequest, ...grpc.CallOption) (*sandboxdv1.Process, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.process, nil
}
func (f *fakeClient) Stop(_ context.Context, request *sandboxdv1.StopProcessRequest, _ ...grpc.CallOption) (*sandboxdv1.Process, error) {
	f.stoppedKey = request.Key
	if f.process == nil {
		return &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED}, nil
	}
	return f.process, nil
}
func (f *fakeClient) ReadOutput(_ context.Context, request *sandboxdv1.ReadOutputRequest, _ ...grpc.CallOption) (*sandboxdv1.ReadOutputResponse, error) {
	f.readRequests = append(f.readRequests, request)
	data := f.stdout
	if request.Stream == sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR {
		data = f.stderr
	}
	gapBytes, retainedFrom := f.gapBytes, f.retainedFrom
	if request.Stream == sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR {
		gapBytes, retainedFrom = 0, 0
	}
	start := request.Offset
	if start > uint64(len(data)) {
		return nil, status.Error(codes.OutOfRange, "offset")
	}
	end := min(len(data), int(start)+int(request.MaxBytes))
	return &sandboxdv1.ReadOutputResponse{Data: append([]byte(nil), data[start:end]...), Offset: start, NextOffset: uint64(end), GapBytes: gapBytes, RetainedStart: retainedFrom, ProducedEnd: uint64(len(data)), Eof: end == len(data)}, nil
}

func TestOutputIncludesGapMetadata(t *testing.T) {
	client := &fakeClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: []byte("kept"), gapBytes: 9, retainedFrom: 9}
	var event controllers.AdapterEvent
	sandbox := testSandbox(client)
	sandbox.EmitEvent = func(_ context.Context, got controllers.AdapterEvent) error { event = got; return nil }
	if _, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err != nil {
		t.Fatal(err)
	}
	var output outputEvent
	if err := json.Unmarshal(event.Data, &output); err != nil || output.GapBytes != 9 || output.RetainedFrom != 9 || string(output.Data) != "kept" {
		t.Fatalf("output = %#v, error = %v", output, err)
	}
}

func testSandbox(client sandboxdv1.ProcessServiceClient) controllers.AdapterSandbox {
	return controllers.AdapterSandbox{EnvironmentUID: "epoch", DialProcess: func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
		return client, func() error { return nil }, nil
	}}
}

func TestAcceptanceIsDuplicateSafePromptSafeAndFreshEpoch(t *testing.T) {
	task := controllers.AdapterTask{ID: "run-uid", Prompt: "--version\nsecond line"}
	adapter := &Adapter{Executable: "fake-amp"}
	first := &fakeClient{}
	for range 2 {
		if err := adapter.EnsureAccepted(context.Background(), task, testSandbox(first), nil); err != nil {
			t.Fatal(err)
		}
	}
	if first.starts != 2 || first.launches != 1 || first.startedKey.OwnerId != task.ID || first.startedKey.Role != processRole {
		t.Fatalf("start/launch/key = %d/%d/%#v", first.starts, first.launches, first.startedKey)
	}
	want := []string{"fake-amp", "--execute=" + task.Prompt, "--stream-json", "--no-ide", "--no-notifications"}
	if !reflect.DeepEqual(first.startedSpec.Argv, want) || first.startedSpec.Env["AMP_SKIP_UPDATE_CHECK"] != "1" {
		t.Fatalf("spec = %#v", first.startedSpec)
	}
	second := &fakeClient{}
	if err := adapter.EnsureAccepted(context.Background(), task, testSandbox(second), nil); err != nil {
		t.Fatal(err)
	}
	if second.launches != 1 || second.startedKey.OwnerId != task.ID {
		t.Fatalf("fresh epoch = %d/%#v", second.launches, second.startedKey)
	}
}

func TestCredentialIsRejectedWithoutDial(t *testing.T) {
	dials := 0
	sandbox := controllers.AdapterSandbox{DialProcess: func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
		dials++
		return nil, nil, nil
	}}
	err := (&Adapter{}).EnsureAccepted(context.Background(), controllers.AdapterTask{}, sandbox, &controllers.AdapterCredential{APIKey: []byte("secret")})
	if !errors.Is(err, controllers.ErrAdapterTaskRejected) || dials != 0 {
		t.Fatalf("error/dials = %v/%d", err, dials)
	}
}

func TestObservationOutcomes(t *testing.T) {
	exit0, exit1 := int32(0), int32(1)
	tests := []struct {
		name    string
		process *sandboxdv1.Process
		output  string
		want    controllers.AdapterObservation
	}{
		{"running", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "e"}, "", controllers.AdapterObservationRunning},
		{"success", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, `{"type":"result","subtype":"success","is_error":false,"result":"done"}` + "\n", controllers.AdapterObservationSucceeded},
		{"truncated prefix then success", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, "{truncated\n" + `{"type":"result","subtype":"success","is_error":false,"result":"done"}`, controllers.AdapterObservationSucceeded},
		{"success then assistant", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, `{"type":"result","subtype":"success","is_error":false}` + "\n" + `{"type":"assistant"}`, controllers.AdapterObservationFailed},
		{"success then malformed", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, `{"type":"result","subtype":"success","is_error":false}` + "\ntruncated", controllers.AdapterObservationFailed},
		{"failed result", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, `{"type":"result","subtype":"error_during_execution","is_error":true}` + "\n", controllers.AdapterObservationFailed},
		{"malformed", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, "not json\n", controllers.AdapterObservationFailed},
		{"missing terminal", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, `{"type":"assistant"}` + "\n", controllers.AdapterObservationFailed},
		{"nonzero", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit1, ExecutionId: "e"}, `{"type":"result","subtype":"success","is_error":false}` + "\n", controllers.AdapterObservationFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, testSandbox(&fakeClient{process: test.process, stdout: []byte(test.output)}))
			if err != nil || got != test.want {
				t.Fatalf("Observe = %q, %v", got, err)
			}
		})
	}
	absent, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, testSandbox(&fakeClient{getErr: status.Error(codes.NotFound, "gone")}))
	if err != nil || absent != controllers.AdapterObservationFailed {
		t.Fatalf("absent = %q, %v", absent, err)
	}
}

func TestCancellationAndBoundedIdempotentOutput(t *testing.T) {
	client := &fakeClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: []byte("stdout"), stderr: []byte("stderr")}
	var events []controllers.AdapterEvent
	sandbox := testSandbox(client)
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
	if len(events) != 2 {
		t.Fatalf("events = %d", len(events))
	}
	for _, event := range events {
		if event.Source != "amp" || event.Type != "amp.process-output" || !strings.HasPrefix(event.IdempotencyKey, "v1:") {
			t.Fatalf("event = %#v", event)
		}
	}
	if client.readRequests[0].MaxBytes != pageMax {
		t.Fatalf("read max = %d", client.readRequests[0].MaxBytes)
	}
	err := adapter.Cancel(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox)
	if !errors.Is(err, controllers.ErrAdapterCancellationPending) || client.stoppedKey.OwnerId != "run" {
		t.Fatalf("cancel = %v/%#v", err, client.stoppedKey)
	}
}

func TestOutputRetryUsesStableKey(t *testing.T) {
	client := &fakeClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: []byte("output")}
	var events []controllers.AdapterEvent
	sandbox := testSandbox(client)
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
	var retry, appended outputEvent
	if json.Unmarshal(events[1].Data, &retry) != nil || json.Unmarshal(events[2].Data, &appended) != nil ||
		retry.Offset != 0 || retry.NextOffset != 6 || appended.Offset != 6 || string(appended.Data) != " appended" {
		t.Fatalf("retry/appended = %#v/%#v", retry, appended)
	}
}

func TestRestartChangesKeyWhenSnapshotMetadataChanges(t *testing.T) {
	client := &fakeClient{
		process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"},
		stdout:  bytes.Repeat([]byte("x"), pageMax+1),
	}
	var first controllers.AdapterEvent
	sandbox := testSandbox(client)
	sandbox.EmitEvent = func(_ context.Context, event controllers.AdapterEvent) error {
		if first.Data == nil {
			first = event
			return errors.New("uncertain append")
		}
		return nil
	}
	if _, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err == nil {
		t.Fatal("wanted uncertain append error")
	}

	client.stdout = append(client.stdout, 'y')
	var replay controllers.AdapterEvent
	sandbox.EmitEvent = func(_ context.Context, event controllers.AdapterEvent) error {
		replay = event
		return errors.New("stop after replay")
	}
	if _, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err == nil {
		t.Fatal("wanted replay stop error")
	}
	if first.IdempotencyKey == replay.IdempotencyKey || bytes.Equal(first.Data, replay.Data) {
		t.Fatalf("restart replay key/payload did not reflect changed snapshot metadata: %q/%q", first.IdempotencyKey, replay.IdempotencyKey)
	}
	var before, after outputEvent
	if json.Unmarshal(first.Data, &before) != nil || json.Unmarshal(replay.Data, &after) != nil ||
		before.Offset != after.Offset || before.NextOffset != after.NextOffset || !bytes.Equal(before.Data, after.Data) || before.ProducedEnd == after.ProducedEnd {
		t.Fatalf("restart ranges = %#v/%#v", before, after)
	}
}
