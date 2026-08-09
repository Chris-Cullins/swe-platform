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

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/agent"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

func launchCredential(credential *agent.AdapterCredential) *agent.AdapterLaunchMaterial {
	return &agent.AdapterLaunchMaterial{AgentCredential: credential}
}

func TestRepositoryOnlyLaunchMaterial(t *testing.T) {
	client := &fakeClient{}
	material := &agent.AdapterLaunchMaterial{RepositorySecretEnv: map[string][]byte{"REPOSITORY_TOKEN": []byte("secret")}}
	if err := (&Adapter{}).EnsureAccepted(context.Background(), agent.AdapterTask{ID: "run", Prompt: "task"}, testSandbox(client), material); err != nil {
		t.Fatal(err)
	}
	if client.launchCalls != 1 || client.starts != 0 || client.launchRequest.Spec.Env["REPOSITORY_TOKEN"] != "" {
		t.Fatalf("repository launch = %#v", client.launchRequest)
	}
}

type fakeClient struct {
	process         *sandboxdv1.Process
	stdout          []byte
	stderr          []byte
	retainedFrom    uint64
	getErr          error
	starts          int
	launchCalls     int
	launches        int
	startedKey      *sandboxdv1.ProcessKey
	startedSpec     *sandboxdv1.ProcessSpec
	launchValue     []byte
	submittedValue  []byte
	launchRequest   *sandboxdv1.StartProcessWithLaunchMaterialRequest
	beforeLaunchErr error
	afterLaunchErr  error
	stoppedKey      *sandboxdv1.ProcessKey
	stopErr         error
	readRequests    []*sandboxdv1.ReadOutputRequest
}

func (f *fakeClient) ReconcileManagedServices(context.Context, *sandboxdv1.ReconcileManagedServicesRequest, ...grpc.CallOption) (*sandboxdv1.ReconcileManagedServicesResponse, error) {
	return nil, errors.New("unexpected ReconcileManagedServices call")
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

func (f *fakeClient) StartWithLaunchMaterial(_ context.Context, request *sandboxdv1.StartProcessWithLaunchMaterialRequest, _ ...grpc.CallOption) (*sandboxdv1.Process, error) {
	f.launchCalls++
	f.launchRequest = request
	f.submittedValue = append([]byte(nil), request.LaunchMaterial.SecretEnv["AMP_API_KEY"]...)
	if f.beforeLaunchErr != nil {
		return nil, f.beforeLaunchErr
	}
	if f.startedKey == nil {
		f.launches++
		f.startedKey, f.startedSpec = request.Key, request.Spec
		f.launchValue = append([]byte(nil), request.LaunchMaterial.SecretEnv["AMP_API_KEY"]...)
	} else if !reflect.DeepEqual(f.startedKey, request.Key) || !reflect.DeepEqual(f.startedSpec, request.Spec) {
		return nil, status.Error(codes.FailedPrecondition, "conflicting start")
	}
	if f.process == nil {
		f.process = &sandboxdv1.Process{Key: request.Key, Spec: request.Spec, State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}
	}
	if f.afterLaunchErr != nil {
		return nil, f.afterLaunchErr
	}
	return f.process, nil
}
func (f *fakeClient) Get(context.Context, *sandboxdv1.GetProcessRequest, ...grpc.CallOption) (*sandboxdv1.Process, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.process, nil
}
func (f *fakeClient) Stop(_ context.Context, request *sandboxdv1.StopProcessRequest, _ ...grpc.CallOption) (*sandboxdv1.Process, error) {
	f.stoppedKey = request.Key
	if f.stopErr != nil {
		return nil, f.stopErr
	}
	if f.process == nil {
		return &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED}, nil
	}
	return f.process, nil
}
func (f *fakeClient) ReadOutput(_ context.Context, request *sandboxdv1.ReadOutputRequest, _ ...grpc.CallOption) (*sandboxdv1.ReadOutputResponse, error) {
	f.readRequests = append(f.readRequests, request)
	data, retainedFrom := f.stdout, f.retainedFrom
	if request.Stream == sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR {
		data, retainedFrom = f.stderr, 0
	}
	producedEnd := retainedFrom + uint64(len(data))
	offset := request.Offset
	var gapBytes uint64
	if offset < retainedFrom {
		gapBytes, offset = retainedFrom-offset, retainedFrom
	}
	if offset > producedEnd {
		return nil, status.Error(codes.OutOfRange, "offset")
	}
	start := offset - retainedFrom
	end := min(len(data), int(start)+int(request.MaxBytes))
	return &sandboxdv1.ReadOutputResponse{Data: append([]byte(nil), data[start:end]...), Offset: offset, NextOffset: retainedFrom + uint64(end), GapBytes: gapBytes, RetainedStart: retainedFrom, ProducedEnd: producedEnd, Eof: end == len(data)}, nil
}

func TestOutputIncludesGapMetadata(t *testing.T) {
	client := &fakeClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: []byte("kept"), retainedFrom: 9}
	var event agent.AdapterEvent
	sandbox := testSandbox(client)
	sandbox.EmitEvent = func(_ context.Context, got agent.AdapterEvent) error { event = got; return nil }
	if _, _, err := (&Adapter{}).Observe(context.Background(), agent.AdapterTask{ID: "run"}, sandbox); err != nil {
		t.Fatal(err)
	}
	var output outputEvent
	if err := json.Unmarshal(event.Data, &output); err != nil || output.GapBytes != 9 || output.RetainedFrom != 9 || string(output.Data) != "kept" {
		t.Fatalf("output = %#v, error = %v", output, err)
	}
}

func testSandbox(client sandboxdv1.ProcessServiceClient) agent.AdapterSandbox {
	return agent.AdapterSandbox{EnvironmentUID: agent.EnvironmentUID("epoch"), DialProcess: func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
		return client, func() error { return nil }, nil
	}}
}

func TestTerminalValidationFailsOnRetainedOutputGap(t *testing.T) {
	exit0 := int32(0)
	// sandboxd dropped the first 1024 bytes of stdout before the requested
	// offset; the retained tail still contains a valid-looking success event,
	// but the transport-reported gap makes the terminal result untrustworthy.
	client := &fakeClient{
		process:      &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "execution", ExitCode: &exit0},
		stdout:       []byte(`{"type":"result","subtype":"success","is_error":false,"result":"done"}` + "\n"),
		retainedFrom: 1024,
	}
	got, detail, err := (&Adapter{}).Observe(context.Background(), agent.AdapterTask{ID: "run"}, testSandbox(client))
	if err != nil || got != agent.AdapterObservationFailed || detail != got.StatusMessage() {
		t.Fatalf("Observe = %q, %q, %v", got, detail, err)
	}
}

func TestAcceptanceIsDuplicateSafePromptSafeAndFreshEpoch(t *testing.T) {
	task := agent.AdapterTask{ID: "run-uid", Prompt: "--version\nsecond line"}
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

func TestAPIKeyUsesLaunchMaterialOnlyAndClearsTemporaryCopy(t *testing.T) {
	client := &fakeClient{}
	key := []byte("!!AMP-API-KEY-FIXTURE!!")
	credential := &agent.AdapterCredential{Type: platformv1alpha1.AgentCredentialTypeAPIKey, APIKey: key}
	if err := (&Adapter{}).EnsureAccepted(context.Background(), agent.AdapterTask{ID: "run", Prompt: "task"}, testSandbox(client), launchCredential(credential)); err != nil {
		t.Fatal(err)
	}
	if client.launchCalls != 1 || client.starts != 0 || string(client.launchValue) != string(key) || string(credential.APIKey) != string(key) {
		t.Fatalf("launch/plain calls = %d/%d", client.launchCalls, client.starts)
	}
	if client.launchRequest == nil || !reflect.DeepEqual(client.launchRequest.LaunchMaterial.SecretEnv["AMP_API_KEY"], make([]byte, len(key))) {
		t.Fatal("adapter did not clear its temporary launch-material copy")
	}
	if client.launchRequest.Spec == nil {
		t.Fatal("launch request is missing its public process spec")
	}
	if _, exposed := client.launchRequest.Spec.Env["AMP_API_KEY"]; exposed {
		t.Fatalf("public process spec contains credential material: %#v", client.launchRequest.Spec)
	}
}

func TestUnsupportedCredentialTypeFailsBeforeDial(t *testing.T) {
	dials := 0
	sandbox := agent.AdapterSandbox{DialProcess: func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
		dials++
		return nil, nil, nil
	}}
	err := (&Adapter{}).EnsureAccepted(context.Background(), agent.AdapterTask{}, sandbox, launchCredential(&agent.AdapterCredential{Type: "OAuth", APIKey: []byte("!!UNUSED-KEY-FIXTURE!!")}))
	if !errors.Is(err, agent.ErrAdapterTaskRejected) || dials != 0 {
		t.Fatalf("error/dials = %v/%d", err, dials)
	}
}

func TestLaunchMaterialFailureDoesNotFallbackOrExposeKey(t *testing.T) {
	key := []byte("!!AMP-FAILURE-KEY-FIXTURE!!")
	client := &fakeClient{beforeLaunchErr: status.Error(codes.Unimplemented, "old sandboxd")}
	err := (&Adapter{}).EnsureAccepted(context.Background(), agent.AdapterTask{ID: "run", Prompt: "task"}, testSandbox(client), launchCredential(&agent.AdapterCredential{Type: platformv1alpha1.AgentCredentialTypeAPIKey, APIKey: key}))
	if status.Code(err) != codes.Unimplemented || client.starts != 0 || client.launchCalls != 1 || client.launches != 0 || strings.Contains(err.Error(), string(key)) {
		t.Fatalf("error/start/calls/launches = %v/%d/%d/%d", err, client.starts, client.launchCalls, client.launches)
	}
}

func TestKeyedAcceptancePreservesDuplicateRotationAndFreshEpochSemantics(t *testing.T) {
	task := agent.AdapterTask{ID: "run-uid", Prompt: "task"}
	adapter := &Adapter{}
	first := &fakeClient{}
	for _, value := range []string{"first-key", "rotated-key"} {
		credential := &agent.AdapterCredential{Type: platformv1alpha1.AgentCredentialTypeAPIKey, APIKey: []byte(value)}
		if err := adapter.EnsureAccepted(context.Background(), task, testSandbox(first), launchCredential(credential)); err != nil {
			t.Fatal(err)
		}
	}
	if first.launchCalls != 2 || first.launches != 1 || string(first.launchValue) != "first-key" || string(first.submittedValue) != "rotated-key" || first.startedKey.OwnerId != task.ID {
		t.Fatalf("duplicate calls/launches/launched/submitted/key = %d/%d/%q/%q/%#v", first.launchCalls, first.launches, first.launchValue, first.submittedValue, first.startedKey)
	}
	second := &fakeClient{}
	credential := &agent.AdapterCredential{Type: platformv1alpha1.AgentCredentialTypeAPIKey, APIKey: []byte("current-key")}
	if err := adapter.EnsureAccepted(context.Background(), task, testSandbox(second), launchCredential(credential)); err != nil {
		t.Fatal(err)
	}
	if second.launches != 1 || string(second.launchValue) != "current-key" || second.startedKey.OwnerId != task.ID {
		t.Fatalf("fresh epoch launch/value/key = %d/%q/%#v", second.launches, second.launchValue, second.startedKey)
	}
}

func TestUncertainKeyedStartRetryDoesNotUsePlainStart(t *testing.T) {
	client := &fakeClient{afterLaunchErr: status.Error(codes.Unavailable, "response lost")}
	credential := &agent.AdapterCredential{Type: platformv1alpha1.AgentCredentialTypeAPIKey, APIKey: []byte("first-key")}
	task := agent.AdapterTask{ID: "run", Prompt: "task"}
	if err := (&Adapter{}).EnsureAccepted(context.Background(), task, testSandbox(client), launchCredential(credential)); status.Code(err) != codes.Unavailable {
		t.Fatalf("first start error = %v", err)
	}
	client.afterLaunchErr = nil
	credential.APIKey = []byte("rotated-key")
	if err := (&Adapter{}).EnsureAccepted(context.Background(), task, testSandbox(client), launchCredential(credential)); err != nil {
		t.Fatal(err)
	}
	if client.launchCalls != 2 || client.launches != 1 || client.starts != 0 || string(client.launchValue) != "first-key" || string(client.submittedValue) != "rotated-key" {
		t.Fatalf("calls/launches/plain/launched/submitted = %d/%d/%d/%q/%q", client.launchCalls, client.launches, client.starts, client.launchValue, client.submittedValue)
	}
}

func TestObservationOutcomes(t *testing.T) {
	exit0, exit1 := int32(0), int32(1)
	tests := []struct {
		name    string
		process *sandboxdv1.Process
		output  string
		want    agent.AdapterObservation
	}{
		{"running", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "e"}, "", agent.AdapterObservationRunning},
		{"success", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, `{"type":"result","subtype":"success","is_error":false,"result":"done"}` + "\n", agent.AdapterObservationSucceeded},
		{"truncated prefix then success", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, "{truncated\n" + `{"type":"result","subtype":"success","is_error":false,"result":"done"}`, agent.AdapterObservationSucceeded},
		{"success then assistant", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, `{"type":"result","subtype":"success","is_error":false}` + "\n" + `{"type":"assistant"}`, agent.AdapterObservationFailed},
		{"success then malformed", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, `{"type":"result","subtype":"success","is_error":false}` + "\ntruncated", agent.AdapterObservationFailed},
		{"failed result", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, `{"type":"result","subtype":"error_during_execution","is_error":true}` + "\n", agent.AdapterObservationFailed},
		{"malformed", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, "not json\n", agent.AdapterObservationFailed},
		{"missing terminal", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, `{"type":"assistant"}` + "\n", agent.AdapterObservationFailed},
		{"nonzero", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit1, ExecutionId: "e"}, `{"type":"result","subtype":"success","is_error":false}` + "\n", agent.AdapterObservationFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, _, err := (&Adapter{}).Observe(context.Background(), agent.AdapterTask{ID: "run"}, testSandbox(&fakeClient{process: test.process, stdout: []byte(test.output)}))
			if err != nil || got != test.want {
				t.Fatalf("Observe = %q, %v", got, err)
			}
		})
	}
	absent, _, err := (&Adapter{}).Observe(context.Background(), agent.AdapterTask{ID: "run"}, testSandbox(&fakeClient{getErr: status.Error(codes.NotFound, "gone")}))
	if err != nil || absent != agent.AdapterObservationFailed {
		t.Fatalf("absent = %q, %v", absent, err)
	}
}

func TestObservationMessagesExcludeAgentControlledDetails(t *testing.T) {
	const sentinel = "!!AMP-STATUS-SECRET!!"
	exit0 := int32(0)
	tests := []struct {
		name    string
		process *sandboxdv1.Process
		output  string
		want    agent.AdapterObservation
	}{
		{"process failure", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_FAILED, Error: sentinel, ExecutionId: "e"}, "", agent.AdapterObservationFailed},
		{"provider failure", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, `{"type":"result","subtype":"error","is_error":true,"error":{"message":"` + sentinel + `"}}` + "\n", agent.AdapterObservationFailed},
		{"success result", &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, `{"type":"result","subtype":"success","is_error":false,"result":"` + sentinel + `"}` + "\n", agent.AdapterObservationSucceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observation, message, err := (&Adapter{}).Observe(context.Background(), agent.AdapterTask{ID: "run"}, testSandbox(&fakeClient{process: test.process, stdout: []byte(test.output)}))
			if err != nil || observation != test.want || message != test.want.StatusMessage() || strings.Contains(message, sentinel) {
				t.Fatalf("Observe = (%q, %q, %v), want fixed %q message", observation, message, err, test.want)
			}
		})
	}
}

func TestCancellationAndBoundedIdempotentOutput(t *testing.T) {
	client := &fakeClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: []byte("stdout"), stderr: []byte("stderr")}
	var events []agent.AdapterEvent
	sandbox := testSandbox(client)
	sandbox.EmitEvent = func(_ context.Context, event agent.AdapterEvent) error {
		events = append(events, event)
		return nil
	}
	adapter := &Adapter{}
	for range 2 {
		if _, _, err := adapter.Observe(context.Background(), agent.AdapterTask{ID: "run"}, sandbox); err != nil {
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
	err := adapter.Cancel(context.Background(), agent.AdapterTask{ID: "run"}, sandbox)
	if !errors.Is(err, agent.ErrAdapterCancellationPending) || client.stoppedKey.OwnerId != "run" {
		t.Fatalf("cancel = %v/%#v", err, client.stoppedKey)
	}
}

func TestCancellationTreatsAbsentProcessAsComplete(t *testing.T) {
	client := &fakeClient{stopErr: status.Error(codes.NotFound, "absent")}
	if err := (&Adapter{}).Cancel(context.Background(), agent.AdapterTask{ID: "run"}, testSandbox(client)); err != nil {
		t.Fatalf("Cancel() error = %v, want absent process to be complete", err)
	}
}

func TestOutputRetryUsesStableKey(t *testing.T) {
	client := &fakeClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: []byte("output")}
	var events []agent.AdapterEvent
	sandbox := testSandbox(client)
	sandbox.EmitEvent = func(_ context.Context, event agent.AdapterEvent) error {
		events = append(events, event)
		if len(events) == 1 {
			return errors.New("retry")
		}
		return nil
	}
	adapter := &Adapter{}
	if _, _, err := adapter.Observe(context.Background(), agent.AdapterTask{ID: "run"}, sandbox); err == nil {
		t.Fatal("wanted transient error")
	}
	client.stdout = []byte("output appended")
	if _, _, err := adapter.Observe(context.Background(), agent.AdapterTask{ID: "run"}, sandbox); err != nil {
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
	var first agent.AdapterEvent
	sandbox := testSandbox(client)
	sandbox.EmitEvent = func(_ context.Context, event agent.AdapterEvent) error {
		if first.Data == nil {
			first = event
			return errors.New("uncertain append")
		}
		return nil
	}
	if _, _, err := (&Adapter{}).Observe(context.Background(), agent.AdapterTask{ID: "run"}, sandbox); err == nil {
		t.Fatal("wanted uncertain append error")
	}

	client.stdout = append(client.stdout, 'y')
	var replay agent.AdapterEvent
	sandbox.EmitEvent = func(_ context.Context, event agent.AdapterEvent) error {
		replay = event
		return errors.New("stop after replay")
	}
	if _, _, err := (&Adapter{}).Observe(context.Background(), agent.AdapterTask{ID: "run"}, sandbox); err == nil {
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
