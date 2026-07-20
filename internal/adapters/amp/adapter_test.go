package amp

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
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
	process       *sandboxdv1.Process
	stopProcess   *sandboxdv1.Process
	stdout        []byte
	stderr        []byte
	stdoutStart   uint64
	stderrStart   uint64
	startCalls    int
	launches      int
	startedKey    *sandboxdv1.ProcessKey
	startedSpec   *sandboxdv1.ProcessSpec
	stoppedKey    *sandboxdv1.ProcessKey
	stopMode      sandboxdv1.StopMode
	getErr        error
	startErr      error
	stopErr       error
	readErr       error
	uncertainOnce bool
	readRequests  []*sandboxdv1.ReadOutputRequest
}

func (f *fakeProcessClient) Start(_ context.Context, request *sandboxdv1.StartProcessRequest, _ ...grpc.CallOption) (*sandboxdv1.Process, error) {
	f.startCalls++
	if f.startErr != nil {
		return nil, f.startErr
	}
	if f.startedKey == nil {
		f.launches++
		f.startedKey, f.startedSpec = request.Key, request.Spec
		if f.process == nil {
			f.process = &sandboxdv1.Process{Key: request.Key, Spec: request.Spec, State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution-1"}
		}
		if f.uncertainOnce {
			f.uncertainOnce = false
			return nil, status.Error(codes.Unavailable, "response lost")
		}
	} else if !reflect.DeepEqual(f.startedKey, request.Key) || !reflect.DeepEqual(f.startedSpec, request.Spec) {
		return nil, status.Error(codes.FailedPrecondition, "conflicting start")
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
	if f.stopProcess != nil {
		return f.stopProcess, nil
	}
	if f.process != nil {
		return f.process, nil
	}
	return &sandboxdv1.Process{Key: request.Key, State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED}, nil
}

func (f *fakeProcessClient) ReadOutput(_ context.Context, request *sandboxdv1.ReadOutputRequest, _ ...grpc.CallOption) (*sandboxdv1.ReadOutputResponse, error) {
	f.readRequests = append(f.readRequests, request)
	if f.readErr != nil {
		return nil, f.readErr
	}
	data, retainedStart := f.stdout, f.stdoutStart
	if request.Stream == sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR {
		data, retainedStart = f.stderr, f.stderrStart
	}
	producedEnd := retainedStart + uint64(len(data))
	offset := request.Offset
	gap := uint64(0)
	if offset < retainedStart {
		gap = retainedStart - offset
		offset = retainedStart
	}
	if offset > producedEnd {
		return nil, status.Error(codes.OutOfRange, "offset")
	}
	end := min(len(data), int(offset-retainedStart)+int(request.MaxBytes))
	chunk := append([]byte(nil), data[offset-retainedStart:uint64(end)]...)
	nextOffset := offset + uint64(len(chunk))
	return &sandboxdv1.ReadOutputResponse{
		Data: chunk, Offset: offset, NextOffset: nextOffset, GapBytes: gap,
		RetainedStart: retainedStart, ProducedEnd: producedEnd, Eof: nextOffset == producedEnd,
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

func TestAcceptanceIsDuplicateSafeAcrossUncertainResponseAndFreshEpoch(t *testing.T) {
	task := controllers.AdapterTask{ID: "run-uid", Prompt: "--fix the test\nwithout parsing flags"}
	adapter := &Adapter{Executable: "fake-amp"}
	firstEpoch := &fakeProcessClient{uncertainOnce: true}
	if err := adapter.EnsureAccepted(context.Background(), task, sandboxFor(firstEpoch)); status.Code(err) != codes.Unavailable {
		t.Fatalf("first acceptance error = %v, want uncertain Unavailable", err)
	}
	if err := adapter.EnsureAccepted(context.Background(), task, sandboxFor(firstEpoch)); err != nil {
		t.Fatal(err)
	}
	if firstEpoch.startCalls != 2 || firstEpoch.launches != 1 {
		t.Fatalf("start calls/launches = %d/%d, want 2/1", firstEpoch.startCalls, firstEpoch.launches)
	}
	if firstEpoch.startedKey.OwnerId != task.ID || firstEpoch.startedKey.Role != processRole {
		t.Fatalf("process key = %#v", firstEpoch.startedKey)
	}
	wantArgv := []string{"fake-amp", "--stream-json", "--execute=" + task.Prompt}
	if !reflect.DeepEqual(firstEpoch.startedSpec.Argv, wantArgv) {
		t.Fatalf("argv = %#v, want %#v", firstEpoch.startedSpec.Argv, wantArgv)
	}
	if !reflect.DeepEqual(firstEpoch.startedSpec.Env, map[string]string{"AMP_SKIP_UPDATE_CHECK": "1"}) || firstEpoch.startedSpec.EnvMode != sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT {
		t.Fatalf("process environment = %#v mode %s", firstEpoch.startedSpec.Env, firstEpoch.startedSpec.EnvMode)
	}
	for _, argument := range firstEpoch.startedSpec.Argv {
		if argument == "last" || argument == "continue" || argument == "threads" {
			t.Fatalf("argv selects shared Amp thread: %#v", firstEpoch.startedSpec.Argv)
		}
	}

	secondEpoch := &fakeProcessClient{}
	if err := adapter.EnsureAccepted(context.Background(), task, sandboxFor(secondEpoch)); err != nil {
		t.Fatal(err)
	}
	if secondEpoch.launches != 1 || secondEpoch.startedKey.OwnerId != task.ID || !reflect.DeepEqual(secondEpoch.startedSpec, firstEpoch.startedSpec) {
		t.Fatalf("fresh epoch launch/key/spec = %d/%#v/%#v", secondEpoch.launches, secondEpoch.startedKey, secondEpoch.startedSpec)
	}
}

func TestStartFailureIsReturned(t *testing.T) {
	want := errors.New("start unavailable")
	client := &fakeProcessClient{startErr: want}
	err := (&Adapter{}).EnsureAccepted(context.Background(), controllers.AdapterTask{ID: "run"}, sandboxFor(client))
	if !errors.Is(err, want) {
		t.Fatalf("EnsureAccepted() error = %v, want %v", err, want)
	}
}

func TestObservationMapping(t *testing.T) {
	exit0, exit1 := int32(0), int32(1)
	natural := sandboxdv1.TerminationReason_TERMINATION_REASON_EXITED
	interrupted := sandboxdv1.TerminationReason_TERMINATION_REASON_INTERRUPTED
	success := `{"type":"result","subtype":"success","duration_ms":12,"is_error":false,"num_turns":1,"result":"done","session_id":"T-success"}` + "\n"
	errorExecution := `{"type":"result","subtype":"error_during_execution","duration_ms":12,"is_error":true,"num_turns":1,"error":"tool failed","session_id":"T-error"}` + "\n"
	errorTurns := `{"type":"result","subtype":"error_max_turns","duration_ms":12,"is_error":true,"num_turns":4,"error":"turn limit","session_id":"T-error"}` + "\n"
	tests := []struct {
		name        string
		process     *sandboxdv1.Process
		stdout      string
		want        controllers.AdapterObservation
		messagePart string
	}{
		{name: "running", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "e"}, want: controllers.AdapterObservationRunning},
		{name: "start failure", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_FAILED, Error: "executable not found", ExecutionId: "e"}, want: controllers.AdapterObservationFailed, messagePart: "failed to start"},
		{name: "nonzero exit", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit1, Reason: natural, ExecutionId: "e"}, stdout: success, want: controllers.AdapterObservationFailed, messagePart: "code 1"},
		{name: "incoherent termination", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, Reason: interrupted, ExecutionId: "e"}, stdout: success, want: controllers.AdapterObservationFailed, messagePart: "termination reason"},
		{name: "success", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, Reason: natural, ExecutionId: "e"}, stdout: success, want: controllers.AdapterObservationSucceeded, messagePart: "done"},
		{name: "execution error result", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, Reason: natural, ExecutionId: "e"}, stdout: errorExecution, want: controllers.AdapterObservationFailed, messagePart: "tool failed"},
		{name: "max turns error result", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, Reason: natural, ExecutionId: "e"}, stdout: errorTurns, want: controllers.AdapterObservationFailed, messagePart: "turn limit"},
		{name: "malformed terminal result", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, Reason: natural, ExecutionId: "e"}, stdout: "not-json\n", want: controllers.AdapterObservationFailed, messagePart: "malformed JSON"},
		{name: "missing terminal result", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, Reason: natural, ExecutionId: "e"}, stdout: `{"type":"assistant"}` + "\n", want: controllers.AdapterObservationFailed, messagePart: "not result"},
		{name: "record after result", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, Reason: natural, ExecutionId: "e"}, stdout: success + `{"type":"assistant"}` + "\n", want: controllers.AdapterObservationFailed, messagePart: "not result"},
		{name: "missing exit code", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, Reason: natural, ExecutionId: "e"}, want: controllers.AdapterObservationFailed, messagePart: "without an exit code"},
		{name: "invalid process state", process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_UNSPECIFIED, ExecutionId: "e"}, want: controllers.AdapterObservationFailed, messagePart: "invalid process state"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeProcessClient{process: test.process, stdout: []byte(test.stdout)}
			got, message, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandboxFor(client))
			if err != nil || got != test.want || (test.messagePart != "" && !strings.Contains(message, test.messagePart)) {
				t.Fatalf("Observe() = (%q, %q, %v), want %q containing %q", got, message, err, test.want, test.messagePart)
			}
		})
	}
}

func TestTerminalResultRequiresDocumentedFieldsAndCoherentVariant(t *testing.T) {
	tests := []struct {
		name   string
		record string
	}{
		{name: "no output"},
		{name: "malformed", record: "{"},
		{name: "missing duration", record: `{"type":"result","subtype":"success","is_error":false,"num_turns":1,"result":"done","session_id":"T"}`},
		{name: "negative duration", record: `{"type":"result","subtype":"success","duration_ms":-1,"is_error":false,"num_turns":1,"result":"done","session_id":"T"}`},
		{name: "fractional turns", record: `{"type":"result","subtype":"success","duration_ms":1,"is_error":false,"num_turns":1.5,"result":"done","session_id":"T"}`},
		{name: "missing session", record: `{"type":"result","subtype":"success","duration_ms":1,"is_error":false,"num_turns":1,"result":"done"}`},
		{name: "missing is error", record: `{"type":"result","subtype":"success","duration_ms":1,"num_turns":1,"result":"done","session_id":"T"}`},
		{name: "success marked error", record: `{"type":"result","subtype":"success","duration_ms":1,"is_error":true,"num_turns":1,"result":"done","session_id":"T"}`},
		{name: "success missing result", record: `{"type":"result","subtype":"success","duration_ms":1,"is_error":false,"num_turns":1,"session_id":"T"}`},
		{name: "error missing error", record: `{"type":"result","subtype":"error_max_turns","duration_ms":1,"is_error":true,"num_turns":1,"session_id":"T"}`},
		{name: "unknown subtype", record: `{"type":"result","subtype":"cancelled","duration_ms":1,"is_error":true,"num_turns":1,"error":"stop","session_id":"T"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := terminalResult([]byte(test.record)); err == nil {
				t.Fatalf("terminalResult(%q) unexpectedly succeeded", test.record)
			}
		})
	}
}

func TestUnavailableAbsentAndMissingExecution(t *testing.T) {
	dialError := errors.New("sandbox unavailable")
	sandbox := controllers.AdapterSandbox{DialProcess: func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error) {
		return nil, nil, dialError
	}}
	if err := (&Adapter{}).EnsureAccepted(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); !errors.Is(err, dialError) {
		t.Fatalf("EnsureAccepted() error = %v", err)
	}

	tests := []struct {
		name   string
		client *fakeProcessClient
		part   string
	}{
		{name: "absent", client: &fakeProcessClient{getErr: status.Error(codes.NotFound, "absent")}, part: "absent"},
		{name: "missing execution ID", client: &fakeProcessClient{process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING}}, part: "execution ID"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, message, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandboxFor(test.client))
			if err != nil || got != controllers.AdapterObservationFailed || !strings.Contains(message, test.part) {
				t.Fatalf("Observe() = (%q, %q, %v)", got, message, err)
			}
		})
	}
}

func TestOutputForwardingPreservesBoundsGapsOpaqueBytesAndRetryKeys(t *testing.T) {
	client := &fakeProcessClient{
		process: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"},
		stdout:  []byte{0xff, 0x00, 'x'}, stdoutStart: 7,
		stderr: []byte("diagnostic"), stderrStart: 3,
	}
	var events []controllers.AdapterEvent
	sandbox := sandboxFor(client)
	sandbox.EmitEvent = func(_ context.Context, event controllers.AdapterEvent) error {
		events = append(events, event)
		return nil
	}
	adapter := &Adapter{}
	if got, _, err := adapter.Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err != nil || got != controllers.AdapterObservationRunning {
		t.Fatalf("Observe() = (%q, %v)", got, err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want stdout and stderr", len(events))
	}
	type payload struct {
		ExecutionID  string `json:"executionId"`
		Stream       string `json:"stream"`
		Offset       uint64 `json:"offset"`
		NextOffset   uint64 `json:"nextOffset"`
		GapBytes     uint64 `json:"gapBytes"`
		RetainedFrom uint64 `json:"retainedFrom"`
		ProducedEnd  uint64 `json:"producedEnd"`
		EOF          bool   `json:"eof"`
		Data         []byte `json:"data"`
	}
	for index, event := range events {
		if event.Source != "amp" || event.Type != "amp.process-output" || !strings.HasPrefix(event.IdempotencyKey, "v1:") {
			t.Fatalf("event = %#v", event)
		}
		digest := sha256.Sum256(event.Data)
		if !strings.HasSuffix(event.IdempotencyKey, fmt.Sprintf("%x", digest)) {
			t.Fatalf("key %q does not address payload", event.IdempotencyKey)
		}
		var decoded payload
		if err := json.Unmarshal(event.Data, &decoded); err != nil {
			t.Fatal(err)
		}
		wantStart, wantData := uint64(7), []byte{0xff, 0x00, 'x'}
		if index == 1 {
			wantStart, wantData = 3, []byte("diagnostic")
		}
		if decoded.ExecutionID != "execution" || decoded.Offset != wantStart || decoded.GapBytes != wantStart || decoded.RetainedFrom != wantStart || decoded.NextOffset != wantStart+uint64(len(wantData)) || decoded.ProducedEnd != wantStart+uint64(len(wantData)) || !decoded.EOF || !reflect.DeepEqual(decoded.Data, wantData) {
			t.Fatalf("payload = %#v, want retained start %d data %#v", decoded, wantStart, wantData)
		}
	}
	for _, request := range client.readRequests {
		if request.ExecutionId != "execution" || request.Key.OwnerId != "run" || request.Key.Role != processRole || request.MaxBytes == 0 || request.MaxBytes > 64*1024 {
			t.Fatalf("read request = %#v", request)
		}
	}
	if _, _, err := adapter.Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("same adapter replayed events: %d", len(events))
	}

	var replay []controllers.AdapterEvent
	sandbox.EmitEvent = func(_ context.Context, event controllers.AdapterEvent) error {
		replay = append(replay, event)
		return nil
	}
	if _, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err != nil {
		t.Fatal(err)
	}
	if len(replay) != 2 || replay[0].IdempotencyKey != events[0].IdempotencyKey || replay[1].IdempotencyKey != events[1].IdempotencyKey {
		t.Fatalf("restart replay keys = %#v, original = %#v", replay, events)
	}
}

func TestOutputFailuresAreRetryableOrTerminalAsDeclared(t *testing.T) {
	process := &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, ExecutionId: "execution"}
	readFailure := errors.New("read unavailable")
	client := &fakeProcessClient{process: process, readErr: readFailure}
	readSandbox := sandboxFor(client)
	readSandbox.EmitEvent = func(context.Context, controllers.AdapterEvent) error { return nil }
	if _, _, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, readSandbox); !errors.Is(err, readFailure) {
		t.Fatalf("read failure = %v", err)
	}

	client = &fakeProcessClient{process: process, stdout: []byte("output")}
	transient := errors.New("transcript unavailable")
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
		t.Fatalf("first append error = %v", err)
	}
	if got, _, err := adapter.Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err != nil || got != controllers.AdapterObservationRunning {
		t.Fatalf("retry Observe() = (%q, %v)", got, err)
	}
	if len(keys) != 2 || keys[0] != keys[1] {
		t.Fatalf("retry keys = %#v", keys)
	}

	sandbox.EmitEvent = func(context.Context, controllers.AdapterEvent) error {
		return controllers.ErrAdapterEventRejected
	}
	got, message, err := (&Adapter{}).Observe(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox)
	if err != nil || got != controllers.AdapterObservationFailed || !strings.Contains(message, "permanently rejected") {
		t.Fatalf("permanent rejection = (%q, %q, %v)", got, message, err)
	}
}

func TestCancelStopsOnlyRunOwnedProcessAndWaitsForTerminal(t *testing.T) {
	client := &fakeProcessClient{stopProcess: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_STOPPING, ExecutionId: "execution"}}
	task := controllers.AdapterTask{ID: "run-uid"}
	err := (&Adapter{}).Cancel(context.Background(), task, sandboxFor(client))
	if !errors.Is(err, controllers.ErrAdapterCancellationPending) {
		t.Fatalf("Cancel() error = %v", err)
	}
	if client.stoppedKey.OwnerId != task.ID || client.stoppedKey.Role != processRole || client.stopMode != sandboxdv1.StopMode_STOP_MODE_GRACEFUL {
		t.Fatalf("stop = key %#v mode %s", client.stoppedKey, client.stopMode)
	}
}

func TestTerminalCancelForwardsOutputAndIgnoresPermanentRejection(t *testing.T) {
	exit0 := int32(0)
	client := &fakeProcessClient{
		stopProcess: &sandboxdv1.Process{State: sandboxdv1.ProcessState_PROCESS_STATE_EXITED, ExitCode: &exit0, ExecutionId: "execution"},
		stdout:      []byte("terminal output"),
	}
	sandbox := sandboxFor(client)
	var calls int
	sandbox.EmitEvent = func(context.Context, controllers.AdapterEvent) error {
		calls++
		return controllers.ErrAdapterEventRejected
	}
	if err := (&Adapter{}).Cancel(context.Background(), controllers.AdapterTask{ID: "run"}, sandbox); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("transcript calls = %d, want 1", calls)
	}
}
