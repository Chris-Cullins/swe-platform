package claudecode

import (
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
	client := &fakeProcessClient{}
	material := &agent.AdapterLaunchMaterial{RepositorySecretEnv: map[string][]byte{"REPOSITORY_TOKEN": []byte("secret")}}
	if err := (&Adapter{}).EnsureAccepted(context.Background(), agent.AdapterTask{ID: "run", Prompt: "task"}, sandboxFor(client), material); err != nil {
		t.Fatal(err)
	}
	if client.launchCalls != 1 || client.startCalls != 0 || len(client.launchRequest.Spec.Env) != 0 {
		t.Fatalf("repository launch = %#v", client.launchRequest)
	}
}

type fakeProcessClient struct {
	process       *sandboxdv1.Process
	stdout        []byte
	stderr        []byte
	retainedFrom  uint64
	startCalls    int
	launches      int
	startedKey    *sandboxdv1.ProcessKey
	startedSpec   *sandboxdv1.ProcessSpec
	stoppedKey    *sandboxdv1.ProcessKey
	stopMode      sandboxdv1.StopMode
	getErr        error
	stopErr       error
	readRequests  []*sandboxdv1.ReadOutputRequest
	launchRequest *sandboxdv1.StartProcessWithLaunchMaterialRequest
	launchValue   []byte
	launchCalls   int
	launchErr     error
}

func (f *fakeProcessClient) ReconcileManagedServices(context.Context, *sandboxdv1.ReconcileManagedServicesRequest, ...grpc.CallOption) (*sandboxdv1.ReconcileManagedServicesResponse, error) {
	return nil, errors.New("unexpected ReconcileManagedServices call")
}

func (f *fakeProcessClient) StartWithLaunchMaterial(_ context.Context, request *sandboxdv1.StartProcessWithLaunchMaterialRequest, _ ...grpc.CallOption) (*sandboxdv1.Process, error) {
	f.launchCalls++
	f.launchRequest = request
	f.launchValue = append([]byte(nil), request.GetLaunchMaterial().GetSecretEnv()["ANTHROPIC_API_KEY"]...)
	if f.launchErr != nil {
		return nil, f.launchErr
	}
	if f.process == nil {
		f.process = &sandboxdv1.Process{Key: request.Key, Spec: request.Spec, State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution-1"}
	}
	return f.process, nil
}

func (f *fakeProcessClient) Start(_ context.Context, request *sandboxdv1.StartProcessRequest, _ ...grpc.CallOption) (*sandboxdv1.Process, error) {
	f.startCalls++
	if f.startedKey == nil {
		f.launches++
		f.startedKey, f.startedSpec = request.Key, request.Spec
	} else if !reflect.DeepEqual(f.startedKey, request.Key) || !reflect.DeepEqual(f.startedSpec, request.Spec) {
		return nil, status.Error(codes.FailedPrecondition, "conflicting start")
	}
	if f.process == nil {
		f.process = &sandboxdv1.Process{Key: request.Key, Spec: request.Spec, State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution-1"}
	}
	return f.process, nil
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
	if f.process != nil {
		return f.process, nil
	}
	return &sandboxdv1.Process{Key: request.Key, State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED}, nil
}

func (f *fakeProcessClient) ReadOutput(_ context.Context, request *sandboxdv1.ReadOutputRequest, _ ...grpc.CallOption) (*sandboxdv1.ReadOutputResponse, error) {
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

func sandboxFor(client sandboxdv1.ProcessServiceClient) agent.AdapterSandbox {
	return agent.AdapterSandbox{
		EnvironmentName: "environment",
		EnvironmentUID:  agent.EnvironmentUID("environment-uid"),
		DialProcess: func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
			return client, func() error { return nil }, nil
		},
	}
}

func TestAcceptanceIsDuplicateSafeAndResumeKeepsTaskIdentity(t *testing.T) {
	task := agent.AdapterTask{ID: "run-uid", Prompt: "fix the test"}
	adapter := &Adapter{Executable: "fake-claude"}
	firstEpoch := &fakeProcessClient{}
	for range 2 {
		if err := adapter.EnsureAccepted(context.Background(), task, sandboxFor(firstEpoch), nil); err != nil {
			t.Fatal(err)
		}
	}
	if firstEpoch.startCalls != 2 || firstEpoch.launches != 1 {
		t.Fatalf("start calls/launches = %d/%d, want 2/1", firstEpoch.startCalls, firstEpoch.launches)
	}
	if firstEpoch.startedKey.OwnerId != task.ID || firstEpoch.startedKey.Role != processRole {
		t.Fatalf("process key = %#v", firstEpoch.startedKey)
	}
	if got := firstEpoch.startedSpec.Argv; got[0] != "fake-claude" || got[len(got)-1] != task.Prompt || !contains(got, "stream-json") {
		t.Fatalf("argv = %#v", got)
	}

	secondEpoch := &fakeProcessClient{}
	if err := adapter.EnsureAccepted(context.Background(), task, sandboxFor(secondEpoch), nil); err != nil {
		t.Fatal(err)
	}
	if secondEpoch.launches != 1 || secondEpoch.startedKey.OwnerId != task.ID {
		t.Fatalf("resume launch/key = %d/%#v", secondEpoch.launches, secondEpoch.startedKey)
	}
}

func TestPromptIsSeparatedFromClaudeFlags(t *testing.T) {
	spec := (&Adapter{}).processSpec(agent.AdapterTask{Prompt: "--version"})
	if got := spec.Argv[len(spec.Argv)-2:]; !reflect.DeepEqual(got, []string{"--", "--version"}) {
		t.Fatalf("argv suffix = %#v, want flag terminator and prompt", got)
	}
}

func TestAPIKeyUsesLaunchMaterialOnly(t *testing.T) {
	client := &fakeProcessClient{}
	key := []byte("!!CLAUDE-API-KEY-FIXTURE!!")
	err := (&Adapter{}).EnsureAccepted(context.Background(), agent.AdapterTask{ID: "run", Prompt: "task"}, sandboxFor(client), launchCredential(&agent.AdapterCredential{Type: platformv1alpha1.AgentCredentialTypeAPIKey, APIKey: key}))
	if err != nil {
		t.Fatal(err)
	}
	if client.launchCalls != 1 || client.startCalls != 0 || string(client.launchValue) != string(key) {
		t.Fatalf("launch/plain calls = %d/%d, delivered value = %q", client.launchCalls, client.startCalls, client.launchValue)
	}
	if client.launchRequest == nil || !reflect.DeepEqual(client.launchRequest.LaunchMaterial.SecretEnv["ANTHROPIC_API_KEY"], make([]byte, len(key))) {
		t.Fatal("adapter did not clear its temporary launch-material copy")
	}
	if client.launchRequest.Spec == nil || len(client.launchRequest.Spec.Env) != 0 {
		t.Fatalf("public process spec contains credential material: %#v", client.launchRequest.Spec)
	}
}

func TestLaunchMaterialUnimplementedDoesNotFallback(t *testing.T) {
	client := &fakeProcessClient{launchErr: status.Error(codes.Unimplemented, "old sandboxd")}
	err := (&Adapter{}).EnsureAccepted(context.Background(), agent.AdapterTask{ID: "run"}, sandboxFor(client), launchCredential(&agent.AdapterCredential{Type: platformv1alpha1.AgentCredentialTypeAPIKey, APIKey: []byte("key")}))
	if status.Code(err) != codes.Unimplemented || client.startCalls != 0 {
		t.Fatalf("error/start calls = %v/%d", err, client.startCalls)
	}
}

func TestUnsupportedCredentialTypeFailsBeforeDial(t *testing.T) {
	dials := 0
	sandbox := agent.AdapterSandbox{DialProcess: func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
		dials++
		return &fakeProcessClient{}, func() error { return nil }, nil
	}}
	err := (&Adapter{}).EnsureAccepted(context.Background(), agent.AdapterTask{ID: "run"}, sandbox, launchCredential(&agent.AdapterCredential{Type: "FutureType", APIKey: []byte("!!UNUSED-KEY-FIXTURE!!")}))
	if err == nil || dials != 0 {
		t.Fatalf("unsupported credential = error %v, dials %d", err, dials)
	}
}

func TestCancelStopsOnlyRunOwnedProcessTree(t *testing.T) {
	client := &fakeProcessClient{}
	task := agent.AdapterTask{ID: "run-uid"}
	if err := (&Adapter{}).Cancel(context.Background(), task, sandboxFor(client)); err != nil {
		t.Fatal(err)
	}
	if client.stoppedKey.OwnerId != task.ID || client.stoppedKey.Role != processRole || client.stopMode != sandboxdv1.StopMode_STOP_MODE_GRACEFUL {
		t.Fatalf("stop = key %#v mode %s", client.stoppedKey, client.stopMode)
	}
}

func TestCancelWaitsForProcessTreeToBecomeTerminal(t *testing.T) {
	client := &fakeProcessClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_STOPPING, ExecutionId: "execution"}}
	err := (&Adapter{}).Cancel(context.Background(), agent.AdapterTask{ID: "run-uid"}, sandboxFor(client))
	if !errors.Is(err, agent.ErrAdapterCancellationPending) {
		t.Fatalf("Cancel() error = %v, want cancellation pending", err)
	}
}

func TestCancelTreatsAbsentProcessAsComplete(t *testing.T) {
	client := &fakeProcessClient{stopErr: status.Error(codes.NotFound, "absent")}
	if err := (&Adapter{}).Cancel(context.Background(), agent.AdapterTask{ID: "run-uid"}, sandboxFor(client)); err != nil {
		t.Fatalf("Cancel() error = %v, want absent process to be complete", err)
	}
}

func TestObservationMapping(t *testing.T) {
	exit0, exit1 := int32(0), int32(1)
	tests := []struct {
		name    string
		process *sandboxdv1.Process
		stdout  string
		want    agent.AdapterObservation
	}{
		{name: "running", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "e"}, want: agent.AdapterObservationRunning},
		{name: "start failure", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_FAILED, Error: "executable not found", ExecutionId: "e"}, want: agent.AdapterObservationFailed},
		{name: "nonzero exit", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit1, ExecutionId: "e"}, want: agent.AdapterObservationFailed},
		{name: "success", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"done\"}\n", want: agent.AdapterObservationSucceeded},
		{name: "success with historical denial", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"permission_denials\":[{\"tool\":\"Bash\"}]}\n", want: agent.AdapterObservationSucceeded},
		{name: "reported error", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"result\",\"subtype\":\"error_max_turns\",\"is_error\":true}\n", want: agent.AdapterObservationFailed},
		{name: "error with denial", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"result\",\"subtype\":\"error_during_execution\",\"is_error\":true,\"permission_denials\":[{}]}\n", want: agent.AdapterObservationFailed},
		{name: "missing error marker", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"result\",\"subtype\":\"success\"}\n", want: agent.AdapterObservationFailed},
		{name: "malformed result", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "not-json\n", want: agent.AdapterObservationFailed},
		{name: "missing exit code", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e"}, want: agent.AdapterObservationFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeProcessClient{process: test.process, stdout: []byte(test.stdout)}
			got, _, err := (&Adapter{}).Observe(context.Background(), agent.AdapterTask{ID: "run"}, sandboxFor(client))
			if err != nil || got != test.want {
				t.Fatalf("Observe() = (%q, %v), want %q", got, err, test.want)
			}
		})
	}
}

func TestObservationMessagesExcludeAgentControlledDetails(t *testing.T) {
	const sentinel = "!!CLAUDE-STATUS-SECRET!!"
	exit0 := int32(0)
	tests := []struct {
		name    string
		process *sandboxdv1.Process
		stdout  string
		want    agent.AdapterObservation
	}{
		{name: "process failure", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_FAILED, Error: sentinel, ExecutionId: "e"}, want: agent.AdapterObservationFailed},
		{name: "provider failure", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: `{"type":"result","subtype":"` + sentinel + `","is_error":true,"result":"` + sentinel + `"}` + "\n", want: agent.AdapterObservationFailed},
		{name: "success result", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: `{"type":"result","subtype":"success","is_error":false,"result":"` + sentinel + `"}` + "\n", want: agent.AdapterObservationSucceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observation, message, err := (&Adapter{}).Observe(context.Background(), agent.AdapterTask{ID: "run"}, sandboxFor(&fakeProcessClient{process: test.process, stdout: []byte(test.stdout)}))
			if err != nil || observation != test.want || message != test.want.StatusMessage() || strings.Contains(message, sentinel) {
				t.Fatalf("Observe() = (%q, %q, %v), want fixed %q message", observation, message, err, test.want)
			}
		})
	}
}

func TestUsageLookingOutputRemainsOpaque(t *testing.T) {
	exit0 := int32(0)
	raw := []byte(`{"type":"result","subtype":"success","is_error":false,"usage":{"input_tokens":17,"output_tokens":5},"total_cost_usd":0.0042}` + "\n")
	client := &fakeProcessClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "execution"}, stdout: raw}
	var event agent.AdapterEvent
	sandbox := sandboxFor(client)
	sandbox.EmitEvent = func(_ context.Context, got agent.AdapterEvent) error { event = got; return nil }

	observation, message, err := (&Adapter{}).Observe(context.Background(), agent.AdapterTask{ID: "run"}, sandbox)
	if err != nil || observation != agent.AdapterObservationSucceeded || message != observation.StatusMessage() {
		t.Fatalf("Observe() = (%q, %q, %v)", observation, message, err)
	}
	var output outputEvent
	if err := json.Unmarshal(event.Data, &output); err != nil || event.Source != "claude-code" || event.Type != "claude-code.process-output" || !reflect.DeepEqual(output.Data, raw) {
		t.Fatalf("opaque output event = %#v, output = %#v, error = %v", event, output, err)
	}
}

func TestUnavailableAndAbsentExecution(t *testing.T) {
	dialError := errors.New("sandbox unavailable")
	sandbox := agent.AdapterSandbox{DialProcess: func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
		return nil, nil, dialError
	}}
	if err := (&Adapter{}).EnsureAccepted(context.Background(), agent.AdapterTask{ID: "run"}, sandbox, nil); !errors.Is(err, dialError) {
		t.Fatalf("EnsureAccepted() error = %v", err)
	}
	client := &fakeProcessClient{getErr: status.Error(codes.NotFound, "absent")}
	got, _, err := (&Adapter{}).Observe(context.Background(), agent.AdapterTask{ID: "run"}, sandboxFor(client))
	if err != nil || got != agent.AdapterObservationFailed {
		t.Fatalf("Observe(absent) = (%q, %v)", got, err)
	}
}

func TestOutputForwardingIsBoundedCursorBasedAndAdapterOwned(t *testing.T) {
	client := &fakeProcessClient{
		process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"},
		stdout:  []byte("stdout"), stderr: []byte("stderr"),
	}
	var events []agent.AdapterEvent
	sandbox := sandboxFor(client)
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
		t.Fatalf("events = %d, want one per stream without in-process replay", len(events))
	}
	for _, event := range events {
		if event.Source != "claude-code" || event.Type != "claude-code.process-output" || !strings.HasPrefix(event.IdempotencyKey, "v1:") {
			t.Fatalf("event = %#v", event)
		}
	}
}

func TestOutputRetryAfterRestartUsesContentAddressedKeys(t *testing.T) {
	client := &fakeProcessClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: []byte("first")}
	var keys []string
	sandbox := sandboxFor(client)
	sandbox.EmitEvent = func(_ context.Context, event agent.AdapterEvent) error {
		keys = append(keys, event.IdempotencyKey)
		return nil
	}
	if _, _, err := (&Adapter{}).Observe(context.Background(), agent.AdapterTask{ID: "run"}, sandbox); err != nil {
		t.Fatal(err)
	}
	client.stdout = []byte("first-second")
	if _, _, err := (&Adapter{}).Observe(context.Background(), agent.AdapterTask{ID: "run"}, sandbox); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] == keys[1] {
		t.Fatalf("restart output keys = %#v, want distinct keys for evolved payloads", keys)
	}
}

func TestTransientOutputFailureRetriesSameEvent(t *testing.T) {
	client := &fakeProcessClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: []byte("output")}
	transient := errors.New("temporary transcript outage")
	var events []agent.AdapterEvent
	sandbox := sandboxFor(client)
	sandbox.EmitEvent = func(_ context.Context, event agent.AdapterEvent) error {
		events = append(events, event)
		if len(events) == 1 {
			return transient
		}
		return nil
	}
	adapter := &Adapter{}
	if _, _, err := adapter.Observe(context.Background(), agent.AdapterTask{ID: "run"}, sandbox); !errors.Is(err, transient) {
		t.Fatalf("first Observe() error = %v, want transient error", err)
	}
	client.stdout = []byte("output appended")
	got, _, err := adapter.Observe(context.Background(), agent.AdapterTask{ID: "run"}, sandbox)
	if err != nil || got != agent.AdapterObservationRunning {
		t.Fatalf("retry Observe() = (%q, %v)", got, err)
	}
	if len(events) != 3 || !reflect.DeepEqual(events[0], events[1]) {
		t.Fatalf("retry events = %#v, want exact pending event followed by appended output", events)
	}
}

func TestPermanentOutputRejectionFailsObservation(t *testing.T) {
	client := &fakeProcessClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: []byte("output")}
	sandbox := sandboxFor(client)
	sandbox.EmitEvent = func(context.Context, agent.AdapterEvent) error {
		return agent.ErrAdapterEventRejected
	}
	adapter := &Adapter{}
	got, message, err := adapter.Observe(context.Background(), agent.AdapterTask{ID: "run"}, sandbox)
	if err != nil || got != agent.AdapterObservationFailed || message != got.StatusMessage() {
		t.Fatalf("Observe() = (%q, %q, %v)", got, message, err)
	}
	if len(adapter.pending) != 0 {
		t.Fatalf("permanently rejected pending events retained: %d", len(adapter.pending))
	}
}

func TestTerminalValidationFailsOnRetainedOutputGap(t *testing.T) {
	exit0 := int32(0)
	// sandboxd dropped the first 512 bytes of stdout before the requested
	// offset; the retained tail still contains a valid-looking success event,
	// but the transport-reported gap makes the terminal result untrustworthy.
	client := &fakeProcessClient{
		process:      &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "execution"},
		stdout:       []byte(`{"type":"result","subtype":"success","is_error":false,"result":"done"}` + "\n"),
		retainedFrom: 512,
	}
	got, detail, err := (&Adapter{}).Observe(context.Background(), agent.AdapterTask{ID: "run"}, sandboxFor(client))
	if err != nil || got != agent.AdapterObservationFailed || detail != got.StatusMessage() {
		t.Fatalf("Observe() = (%q, %q, %v)", got, detail, err)
	}
}

func TestTerminalCancellationIgnoresPermanentOutputRejection(t *testing.T) {
	exitCode := int32(0)
	client := &fakeProcessClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "execution", ExitCode: &exitCode}, stdout: []byte("output")}
	sandbox := sandboxFor(client)
	sandbox.EmitEvent = func(context.Context, agent.AdapterEvent) error {
		return agent.ErrAdapterEventRejected
	}
	adapter := &Adapter{}
	if err := adapter.Cancel(context.Background(), agent.AdapterTask{ID: "run"}, sandbox); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if len(adapter.pending) != 0 {
		t.Fatalf("permanently rejected pending events retained after cancellation: %d", len(adapter.pending))
	}
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
