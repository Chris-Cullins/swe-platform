package claudecode

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/controllers"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

type fakeProcessClient struct {
	process       *sandboxdv1.Process
	stdout        []byte
	stderr        []byte
	startCalls    int
	launches      int
	startedKey    *sandboxdv1.ProcessKey
	startedSpec   *sandboxdv1.ProcessSpec
	stoppedKey    *sandboxdv1.ProcessKey
	stopMode      sandboxdv1.StopMode
	getErr        error
	readRequests  []*sandboxdv1.ReadOutputRequest
	launchRequest *sandboxdv1.StartProcessWithLaunchMaterialRequest
	launchValue   []byte
	launchCalls   int
	launchErr     error
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
	if f.process != nil {
		return f.process, nil
	}
	return &sandboxdv1.Process{Key: request.Key, State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED}, nil
}

func (f *fakeProcessClient) ReadOutput(_ context.Context, request *sandboxdv1.ReadOutputRequest, _ ...grpc.CallOption) (*sandboxdv1.ReadOutputResponse, error) {
	f.readRequests = append(f.readRequests, request)
	data := f.stdout
	if request.Stream == sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR {
		data = f.stderr
	}
	offset := request.Offset
	if offset > uint64(len(data)) {
		return nil, status.Error(codes.OutOfRange, "offset")
	}
	end := min(len(data), int(offset)+int(request.MaxBytes))
	chunk := append([]byte(nil), data[offset:end]...)
	return &sandboxdv1.ReadOutputResponse{Data: chunk, Offset: offset, NextOffset: uint64(end), ProducedEnd: uint64(len(data)), Eof: end == len(data)}, nil
}

func sandboxFor(client sandboxdv1.ProcessServiceClient) controllers.AdapterSandbox {
	return controllers.AdapterSandbox{
		EnvironmentName: "environment",
		EnvironmentUID:  "environment-uid",
		DialProcess: func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
			return client, func() error { return nil }, nil
		},
	}
}

func TestAcceptanceIsDuplicateSafeAndResumeKeepsTaskIdentity(t *testing.T) {
	task := controllers.AdapterTask{ID: "run-uid", Prompt: "fix the test"}
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
	spec := (&Adapter{}).processSpec(controllers.AdapterTask{Prompt: "--version"})
	if got := spec.Argv[len(spec.Argv)-2:]; !reflect.DeepEqual(got, []string{"--", "--version"}) {
		t.Fatalf("argv suffix = %#v, want flag terminator and prompt", got)
	}
}

func TestAPIKeyUsesLaunchMaterialOnly(t *testing.T) {
	client := &fakeProcessClient{}
	key := []byte("!!CLAUDE-API-KEY-FIXTURE!!")
	err := (&Adapter{}).EnsureAccepted(context.Background(), controllers.AdapterTask{ID: "run", Prompt: "task"}, sandboxFor(client), &controllers.AdapterCredential{Type: platformv1alpha1.AgentCredentialTypeAPIKey, APIKey: key})
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
	err := (&Adapter{}).EnsureAccepted(context.Background(), controllers.AdapterTask{ID: "run"}, sandboxFor(client), &controllers.AdapterCredential{Type: platformv1alpha1.AgentCredentialTypeAPIKey, APIKey: []byte("key")})
	if status.Code(err) != codes.Unimplemented || client.startCalls != 0 {
		t.Fatalf("error/start calls = %v/%d", err, client.startCalls)
	}
}

func TestUnsupportedCredentialTypeFailsBeforeDial(t *testing.T) {
	dials := 0
	sandbox := controllers.AdapterSandbox{DialProcess: func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
		dials++
		return &fakeProcessClient{}, func() error { return nil }, nil
	}}
	err := (&Adapter{}).EnsureAccepted(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox, &controllers.AdapterCredential{Type: "FutureType", APIKey: []byte("!!UNUSED-KEY-FIXTURE!!")})
	if err == nil || dials != 0 {
		t.Fatalf("unsupported credential = error %v, dials %d", err, dials)
	}
}

func TestCancelStopsOnlyRunOwnedProcessTree(t *testing.T) {
	client := &fakeProcessClient{}
	task := controllers.AdapterTask{ID: "run-uid"}
	if err := (&Adapter{}).Cancel(context.Background(), task, sandboxFor(client)); err != nil {
		t.Fatal(err)
	}
	if client.stoppedKey.OwnerId != task.ID || client.stoppedKey.Role != processRole || client.stopMode != sandboxdv1.StopMode_STOP_MODE_GRACEFUL {
		t.Fatalf("stop = key %#v mode %s", client.stoppedKey, client.stopMode)
	}
}

func TestCancelWaitsForProcessTreeToBecomeTerminal(t *testing.T) {
	client := &fakeProcessClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_STOPPING, ExecutionId: "execution"}}
	err := (&Adapter{}).Cancel(context.Background(), controllers.AdapterTask{ID: "run-uid"}, sandboxFor(client))
	if !errors.Is(err, controllers.ErrAdapterCancellationPending) {
		t.Fatalf("Cancel() error = %v, want cancellation pending", err)
	}
}

func TestObservationMapping(t *testing.T) {
	exit0, exit1 := int32(0), int32(1)
	tests := []struct {
		name    string
		process *sandboxdv1.Process
		stdout  string
		want    controllers.AdapterObservation
	}{
		{name: "running", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "e"}, want: controllers.AdapterObservationRunning},
		{name: "start failure", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_FAILED, Error: "executable not found", ExecutionId: "e"}, want: controllers.AdapterObservationFailed},
		{name: "nonzero exit", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit1, ExecutionId: "e"}, want: controllers.AdapterObservationFailed},
		{name: "success", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"done\"}\n", want: controllers.AdapterObservationSucceeded},
		{name: "success with historical denial", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"permission_denials\":[{\"tool\":\"Bash\"}]}\n", want: controllers.AdapterObservationSucceeded},
		{name: "reported error", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"result\",\"subtype\":\"error_max_turns\",\"is_error\":true}\n", want: controllers.AdapterObservationFailed},
		{name: "error with denial", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"result\",\"subtype\":\"error_during_execution\",\"is_error\":true,\"permission_denials\":[{}]}\n", want: controllers.AdapterObservationFailed},
		{name: "missing error marker", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"result\",\"subtype\":\"success\"}\n", want: controllers.AdapterObservationFailed},
		{name: "malformed result", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "not-json\n", want: controllers.AdapterObservationFailed},
		{name: "missing exit code", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "e"}, want: controllers.AdapterObservationFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeProcessClient{process: test.process, stdout: []byte(test.stdout)}
			got, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandboxFor(client))
			if err != nil || got != test.want {
				t.Fatalf("Observe() = (%q, %v), want %q", got, err, test.want)
			}
		})
	}
}

func TestUnavailableAndAbsentExecution(t *testing.T) {
	dialError := errors.New("sandbox unavailable")
	sandbox := controllers.AdapterSandbox{DialProcess: func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
		return nil, nil, dialError
	}}
	if err := (&Adapter{}).EnsureAccepted(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox, nil); !errors.Is(err, dialError) {
		t.Fatalf("EnsureAccepted() error = %v", err)
	}
	client := &fakeProcessClient{getErr: status.Error(codes.NotFound, "absent")}
	got, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandboxFor(client))
	if err != nil || got != controllers.AdapterObservationFailed {
		t.Fatalf("Observe(absent) = (%q, %v)", got, err)
	}
}

func TestOutputForwardingIsBoundedCursorBasedAndAdapterOwned(t *testing.T) {
	client := &fakeProcessClient{
		process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"},
		stdout:  []byte("stdout"), stderr: []byte("stderr"),
	}
	var events []controllers.AdapterEvent
	sandbox := sandboxFor(client)
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
	sandbox.EmitEvent = func(_ context.Context, event controllers.AdapterEvent) error {
		keys = append(keys, event.IdempotencyKey)
		return nil
	}
	if _, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err != nil {
		t.Fatal(err)
	}
	client.stdout = []byte("first-second")
	if _, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] == keys[1] {
		t.Fatalf("restart output keys = %#v, want distinct keys for evolved payloads", keys)
	}
}

func TestTransientOutputFailureRetriesSameEvent(t *testing.T) {
	client := &fakeProcessClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: []byte("output")}
	transient := errors.New("temporary transcript outage")
	var keys []string
	sandbox := sandboxFor(client)
	sandbox.EmitEvent = func(_ context.Context, event controllers.AdapterEvent) error {
		keys = append(keys, event.IdempotencyKey)
		if len(keys) == 1 {
			return transient
		}
		return nil
	}
	adapter := &Adapter{}
	if _, _, err := adapter.Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); !errors.Is(err, transient) {
		t.Fatalf("first Observe() error = %v, want transient error", err)
	}
	got, _, err := adapter.Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox)
	if err != nil || got != controllers.AdapterObservationRunning {
		t.Fatalf("retry Observe() = (%q, %v)", got, err)
	}
	if len(keys) != 2 || keys[0] != keys[1] {
		t.Fatalf("retry keys = %#v, want same idempotency key", keys)
	}
}

func TestPermanentOutputRejectionFailsObservation(t *testing.T) {
	client := &fakeProcessClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}, stdout: []byte("output")}
	sandbox := sandboxFor(client)
	sandbox.EmitEvent = func(context.Context, controllers.AdapterEvent) error {
		return controllers.ErrAdapterEventRejected
	}
	got, message, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox)
	if err != nil || got != controllers.AdapterObservationFailed || !strings.Contains(message, "permanently rejected") {
		t.Fatalf("Observe() = (%q, %q, %v)", got, message, err)
	}
}

func TestTerminalCancellationIgnoresPermanentOutputRejection(t *testing.T) {
	exitCode := int32(0)
	client := &fakeProcessClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "execution", ExitCode: &exitCode}, stdout: []byte("output")}
	sandbox := sandboxFor(client)
	sandbox.EmitEvent = func(context.Context, controllers.AdapterEvent) error {
		return controllers.ErrAdapterEventRejected
	}
	if err := (&Adapter{}).Cancel(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err != nil {
		t.Fatalf("Cancel() error = %v", err)
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
