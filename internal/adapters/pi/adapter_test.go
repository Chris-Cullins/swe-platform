package pi

import (
	"bytes"
	"context"
	"encoding/base64"
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

type fakeProcessClient struct {
	process     *sandboxdv1.Process
	stdout      []byte
	stderr      []byte
	stdoutStart uint64
	stderrStart uint64
	startCalls  int
	launches    int
	startedKey  *sandboxdv1.ProcessKey
	startedSpec *sandboxdv1.ProcessSpec
	stoppedKey  *sandboxdv1.ProcessKey
	stopMode    sandboxdv1.StopMode
	getErr      error
	stopErr     error
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

func (f *fakeProcessClient) StartWithLaunchMaterial(context.Context, *sandboxdv1.StartProcessWithLaunchMaterialRequest, ...grpc.CallOption) (*sandboxdv1.Process, error) {
	return nil, errors.New("unexpected launch material")
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
	data := f.stdout
	bufferStart := f.stdoutStart
	if request.Stream == sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR {
		data = f.stderr
		bufferStart = f.stderrStart
	}
	producedEnd := bufferStart + uint64(len(data))
	offset := request.Offset
	gap := offset < bufferStart
	if gap {
		offset = bufferStart
	}
	if offset > producedEnd {
		return nil, status.Error(codes.OutOfRange, "offset")
	}
	startIndex := int(offset - bufferStart)
	endIndex := min(len(data), startIndex+int(request.MaxBytes))
	nextOffset := bufferStart + uint64(endIndex)
	return &sandboxdv1.ReadOutputResponse{
		Data:          append([]byte(nil), data[startIndex:endIndex]...),
		Offset:        offset,
		NextOffset:    nextOffset,
		ProducedEnd:   producedEnd,
		Eof:           endIndex == len(data),
		GapBytes:      offset - request.Offset,
		RetainedStart: bufferStart,
	}, nil
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

func sandboxWithEvents(client sandboxdv1.ProcessServiceClient, events *[]controllers.AdapterEvent) controllers.AdapterSandbox {
	sandbox := sandboxFor(client)
	sandbox.EmitEvent = func(_ context.Context, event controllers.AdapterEvent) error {
		*events = append(*events, event)
		return nil
	}
	return sandbox
}

func TestAcceptanceIsDuplicateSafeAndEpochFenced(t *testing.T) {
	task := controllers.AdapterTask{ID: "run-uid", Prompt: "--version"}
	adapter := &Adapter{Executable: "fake-pi"}
	firstEpoch := &fakeProcessClient{}
	for range 2 {
		if err := adapter.EnsureAccepted(context.Background(), task, sandboxFor(firstEpoch), nil); err != nil {
			t.Fatal(err)
		}
	}
	if firstEpoch.startCalls != 2 || firstEpoch.launches != 1 {
		t.Fatalf("start calls/launches = %d/%d, want 2/1", firstEpoch.startCalls, firstEpoch.launches)
	}
	if got, want := firstEpoch.startedKey, (&sandboxdv1.ProcessKey{OwnerId: task.ID, Role: "agent"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("process key = %#v, want %#v", got, want)
	}
	wantArgv := []string{
		"fake-pi", "--mode", "json", "--no-session", "--no-approve",
		"--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes",
		"--no-context-files", "--offline", "\n" + task.Prompt,
	}
	if got := firstEpoch.startedSpec.Argv; !reflect.DeepEqual(got, wantArgv) {
		t.Fatalf("argv = %#v, want %#v", got, wantArgv)
	}
	if firstEpoch.startedSpec.EnvMode != sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT || !reflect.DeepEqual(firstEpoch.startedSpec.Env, map[string]string{
		"PI_CODING_AGENT_DIR": ".swe-platform/pi/run-uid",
	}) {
		t.Fatalf("environment = %s %#v", firstEpoch.startedSpec.EnvMode, firstEpoch.startedSpec.Env)
	}

	secondEpoch := &fakeProcessClient{}
	if err := adapter.EnsureAccepted(context.Background(), task, sandboxFor(secondEpoch), nil); err != nil {
		t.Fatal(err)
	}
	if secondEpoch.launches != 1 || !reflect.DeepEqual(secondEpoch.startedKey, firstEpoch.startedKey) {
		t.Fatalf("replacement epoch launch/key = %d/%#v", secondEpoch.launches, secondEpoch.startedKey)
	}
}

func TestCredentialIsRejectedBeforeDial(t *testing.T) {
	dials := 0
	sandbox := controllers.AdapterSandbox{DialProcess: func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
		dials++
		return nil, nil, errors.New("unexpected dial")
	}}
	err := (&Adapter{}).EnsureAccepted(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox, &controllers.AdapterCredential{APIKey: []byte("secret")})
	if !errors.Is(err, controllers.ErrAdapterTaskRejected) || dials != 0 {
		t.Fatalf("EnsureAccepted() = %v with %d dials", err, dials)
	}
}

func TestObservationRequiresNormalAgentEndAndCoherentProcessExit(t *testing.T) {
	exit0, exit1 := int32(0), int32(1)
	settled := "\n{\"type\":\"agent_settled\"}\n"
	tests := []struct {
		name    string
		process *sandboxdv1.Process
		stdout  string
		want    controllers.AdapterObservation
	}{
		{name: "running", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "e"}, want: controllers.AdapterObservationRunning},
		{name: "start failure", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_FAILED, Error: "executable not found", ExecutionId: "e"}, want: controllers.AdapterObservationFailed},
		{name: "nonzero exit", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit1, ExecutionId: "e"}, want: controllers.AdapterObservationFailed},
		{name: "success", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"agent_end\",\"messages\":[{\"role\":\"assistant\",\"stopReason\":\"stop\"}],\"willRetry\":false}" + settled, want: controllers.AdapterObservationSucceeded},
		{name: "success after retry", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"agent_end\",\"messages\":[{\"role\":\"assistant\",\"stopReason\":\"error\"}],\"willRetry\":true}\n{\"type\":\"auto_retry_end\"}\n{\"type\":\"agent_end\",\"messages\":[{\"role\":\"assistant\",\"stopReason\":\"stop\"}],\"willRetry\":false}" + settled, want: controllers.AdapterObservationSucceeded},
		{name: "reported error", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"agent_end\",\"messages\":[{\"role\":\"assistant\",\"stopReason\":\"error\",\"errorMessage\":\"provider failed\"}],\"willRetry\":false}" + settled, want: controllers.AdapterObservationFailed},
		{name: "reported abort", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"agent_end\",\"messages\":[{\"role\":\"assistant\",\"stopReason\":\"aborted\",\"errorMessage\":\"request aborted\"}],\"willRetry\":false}" + settled, want: controllers.AdapterObservationFailed},
		{name: "length stop", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"agent_end\",\"messages\":[{\"role\":\"assistant\",\"stopReason\":\"length\"}],\"willRetry\":false}" + settled, want: controllers.AdapterObservationFailed},
		{name: "malformed stream", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "not-json\n", want: controllers.AdapterObservationFailed},
		{name: "unknown event", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"future_event\"}\n", want: controllers.AdapterObservationFailed},
		{name: "terminal event without assistant", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"agent_end\",\"messages\":[],\"willRetry\":false}" + settled, want: controllers.AdapterObservationFailed},
		{name: "missing settlement", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"agent_end\",\"messages\":[{\"role\":\"assistant\",\"stopReason\":\"stop\"}],\"willRetry\":false}\n", want: controllers.AdapterObservationFailed},
		{name: "output after settlement", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"agent_end\",\"messages\":[{\"role\":\"assistant\",\"stopReason\":\"stop\"}],\"willRetry\":false}" + settled + "{\"type\":\"future_event\"}\n", want: controllers.AdapterObservationFailed},
		{name: "missing retry marker", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "e"}, stdout: "{\"type\":\"agent_end\",\"messages\":[{\"role\":\"assistant\",\"stopReason\":\"stop\"}]}" + settled, want: controllers.AdapterObservationFailed},
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
	client.stopErr = status.Error(codes.NotFound, "absent")
	if err := (&Adapter{}).Cancel(context.Background(), controllers.AdapterTask{ID: "run"}, sandboxFor(client)); err != nil {
		t.Fatalf("Cancel(absent) error = %v, want success", err)
	}
}

func TestCancelStopsOnlyRunOwnedProcessTreeAndWaitsForTerminalState(t *testing.T) {
	task := controllers.AdapterTask{ID: "run-uid"}
	client := &fakeProcessClient{}
	if err := (&Adapter{}).Cancel(context.Background(), task, sandboxFor(client)); err != nil {
		t.Fatal(err)
	}
	if got, want := client.stoppedKey, (&sandboxdv1.ProcessKey{OwnerId: task.ID, Role: "agent"}); !reflect.DeepEqual(got, want) || client.stopMode != sandboxdv1.StopMode_STOP_MODE_GRACEFUL {
		t.Fatalf("stop = key %#v mode %s", got, client.stopMode)
	}

	client.process = &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_STOPPING, ExecutionId: "execution"}
	err := (&Adapter{}).Cancel(context.Background(), task, sandboxFor(client))
	if !errors.Is(err, controllers.ErrAdapterCancellationPending) {
		t.Fatalf("Cancel() error = %v, want cancellation pending", err)
	}
}

func TestOutputForwardingIsOpaqueGapVisibleAndExecutionScoped(t *testing.T) {
	task := controllers.AdapterTask{ID: "run-uid"}
	stdout := []byte("not-json\n{\"type\":\"future_event\"}\n{\"type\":\"agent_end\",\"messages\":[{\"role\":\"assistant\",\"stopReason\":\"error\"}]}\n")
	stderr := []byte("provider diagnostic\n")
	client := &fakeProcessClient{
		process:     &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution-1"},
		stdout:      stdout,
		stderr:      stderr,
		stdoutStart: 7,
		stderrStart: 3,
	}
	var events []controllers.AdapterEvent
	adapter := &Adapter{}
	sandbox := sandboxWithEvents(client, &events)
	if _, _, err := adapter.Observe(context.Background(), task, sandbox); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want stdout and stderr", len(events))
	}
	for i, want := range []struct {
		stream      string
		data        []byte
		bufferStart uint64
	}{{"stdout", stdout, 7}, {"stderr", stderr, 3}} {
		event := events[i]
		if event.Source != "pi" || event.Type != "pi.process-output" || event.IdempotencyKey == "" {
			t.Fatalf("event = %#v", event)
		}
		var payload struct {
			Stream       string `json:"stream"`
			Data         string `json:"data"`
			Offset       uint64 `json:"offset"`
			GapBytes     uint64 `json:"gapBytes"`
			RetainedFrom uint64 `json:"retainedFrom"`
		}
		if err := json.Unmarshal(event.Data, &payload); err != nil {
			t.Fatal(err)
		}
		decoded, err := base64.StdEncoding.DecodeString(payload.Data)
		if err != nil {
			t.Fatal(err)
		}
		if payload.Stream != want.stream || !reflect.DeepEqual(decoded, want.data) || payload.GapBytes != want.bufferStart || payload.Offset != want.bufferStart || payload.RetainedFrom != want.bufferStart {
			t.Fatalf("payload = %#v data %q", payload, decoded)
		}
	}

	if _, _, err := adapter.Observe(context.Background(), task, sandbox); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("duplicate observation emitted %d events, want 2 total", len(events))
	}

	client.process.ExecutionId = "execution-2"
	client.stdoutStart, client.stderrStart = 0, 0
	client.stdout, client.stderr = []byte("{\"type\":\"agent_end\",\"messages\":[{\"role\":\"assistant\",\"stopReason\":\"aborted\"}]}\n"), nil
	if _, _, err := adapter.Observe(context.Background(), task, sandbox); err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("replacement execution did not restart output cursor: %d events", len(events))
	}

	client.stdout = append(client.stdout, []byte("terminal trailer\n")...)
	client.process.State = sandboxdv1.ProcessState_PROCESS_STATE_EXITED
	if err := adapter.Cancel(context.Background(), task, sandbox); err != nil {
		t.Fatal(err)
	}
	if len(events) != 4 {
		t.Fatalf("cancellation did not drain terminal output: %d events", len(events))
	}
}

func TestOutputPaginationRetriesTheSameIdempotencyKey(t *testing.T) {
	transient := errors.New("transcript unavailable")
	client := &fakeProcessClient{
		process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"},
		stdout:  bytes.Repeat([]byte("x"), outputPageMax+1),
	}
	var keys []string
	var payloads [][]byte
	sandbox := sandboxFor(client)
	sandbox.EmitEvent = func(_ context.Context, event controllers.AdapterEvent) error {
		keys = append(keys, event.IdempotencyKey)
		payloads = append(payloads, append([]byte(nil), event.Data...))
		if len(keys) == 1 {
			client.stdout = append(client.stdout, []byte("grew-after-uncertain-append")...)
			return transient
		}
		return nil
	}
	adapter := &Adapter{}
	if _, _, err := adapter.Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); !errors.Is(err, transient) {
		t.Fatalf("first Observe() error = %v, want transient error", err)
	}
	if got, _, err := adapter.Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err != nil || got != controllers.AdapterObservationRunning {
		t.Fatalf("retry Observe() = (%q, %v)", got, err)
	}
	if len(keys) != 3 || keys[0] != keys[1] || !bytes.Equal(payloads[0], payloads[1]) || keys[1] == keys[2] {
		t.Fatalf("event keys = %#v, want identical retry followed by a second page", keys)
	}
}

func TestRetainedOutputGapFailsTerminalValidation(t *testing.T) {
	exitCode := int32(0)
	client := &fakeProcessClient{
		process:     &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "execution", ExitCode: &exitCode},
		stdoutStart: 7,
		stdout:      []byte("{\"type\":\"agent_end\",\"messages\":[{\"role\":\"assistant\",\"stopReason\":\"stop\"}],\"willRetry\":false}\n{\"type\":\"agent_settled\"}\n"),
	}
	got, message, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandboxFor(client))
	if err != nil || got != controllers.AdapterObservationFailed || !strings.Contains(message, "truncated") {
		t.Fatalf("Observe() = (%q, %q, %v)", got, message, err)
	}
}

func TestPermanentOutputRejectionFailsObservation(t *testing.T) {
	client := &fakeProcessClient{
		process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"},
		stdout:  []byte("output"),
	}
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
	client := &fakeProcessClient{
		process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExecutionId: "execution", ExitCode: &exitCode},
		stdout:  []byte("output"),
	}
	sandbox := sandboxFor(client)
	sandbox.EmitEvent = func(context.Context, controllers.AdapterEvent) error {
		return controllers.ErrAdapterEventRejected
	}
	if err := (&Adapter{}).Cancel(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
}
