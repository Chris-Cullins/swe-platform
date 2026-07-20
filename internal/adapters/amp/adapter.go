// Package amp implements the Amp foreground-process adapter.
package amp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Chris-Cullins/swe-platform/internal/controllers"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const (
	processRole = "agent"
	pageMax     = 64 * 1024
)

// Adapter drives one non-interactive Amp process per Run UID.
type Adapter struct {
	Executable string
	mu         sync.Mutex
	cursors    map[cursor]uint64
	pending    map[cursor]pendingEvent
}

type pendingEvent struct {
	event      controllers.AdapterEvent
	nextOffset uint64
}

type cursor struct {
	environment, owner, execution string
	stream                        sandboxdv1.OutputStream
}

type outputEvent struct {
	ExecutionID  string `json:"executionId"`
	Stream       string `json:"stream"`
	Offset       uint64 `json:"offset"`
	NextOffset   uint64 `json:"nextOffset"`
	GapBytes     uint64 `json:"gapBytes,omitempty"`
	RetainedFrom uint64 `json:"retainedFrom"`
	ProducedEnd  uint64 `json:"producedEnd"`
	EOF          bool   `json:"eof"`
	Data         []byte `json:"data,omitempty"`
}

type resultEvent struct {
	Type, Subtype string
	IsError       *bool           `json:"is_error"`
	Result        string          `json:"result"`
	Error         json.RawMessage `json:"error"`
}

func (a *Adapter) executable() string {
	if a.Executable != "" {
		return a.Executable
	}
	return "amp"
}

func key(task controllers.AdapterTask) *sandboxdv1.ProcessKey {
	return &sandboxdv1.ProcessKey{OwnerId: task.ID, Role: processRole}
}

func (a *Adapter) spec(task controllers.AdapterTask) *sandboxdv1.ProcessSpec {
	// --execute=<prompt> is deliberately one argv element. The pinned CLI does
	// not treat "--" after --execute as a flag terminator for a flag-like prompt.
	return &sandboxdv1.ProcessSpec{
		Argv: []string{a.executable(), "--execute=" + task.Prompt, "--stream-json", "--no-ide", "--no-notifications"},
		Env:  map[string]string{"AMP_SKIP_UPDATE_CHECK": "1"}, EnvMode: sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT,
	}
}

// EnsureAccepted duplicate-safely starts (or recovers) the Run-keyed process.
func (a *Adapter) EnsureAccepted(ctx context.Context, task controllers.AdapterTask, sandbox controllers.AdapterSandbox, credential *controllers.AdapterCredential) error {
	if credential != nil {
		return errors.New("Amp credential delivery is not implemented; AMP_API_KEY is a runtime prerequisite")
	}
	client, closeConnection, err := sandbox.DialProcess(ctx)
	if err != nil {
		return err
	}
	defer closeConnection()
	_, err = client.Start(ctx, &sandboxdv1.StartProcessRequest{Key: key(task), Spec: a.spec(task)})
	return err
}

// Observe forwards bounded process output and maps Amp's terminal result event.
func (a *Adapter) Observe(ctx context.Context, task controllers.AdapterTask, sandbox controllers.AdapterSandbox) (controllers.AdapterObservation, string, error) {
	client, closeConnection, err := sandbox.DialProcess(ctx)
	if err != nil {
		return "", "", err
	}
	defer closeConnection()
	process, err := client.Get(ctx, &sandboxdv1.GetProcessRequest{Key: key(task)})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return controllers.AdapterObservationFailed, "Amp execution is absent in the current sandbox epoch", nil
		}
		return "", "", err
	}
	if err := a.forward(ctx, client, task, sandbox, process); err != nil {
		if errors.Is(err, controllers.ErrAdapterEventRejected) {
			return controllers.AdapterObservationFailed, "Amp transcript output was permanently rejected", nil
		}
		return "", "", err
	}
	switch process.State {
	case sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, sandboxdv1.ProcessState_PROCESS_STATE_STOPPING:
		return controllers.AdapterObservationRunning, "Amp is running", nil
	case sandboxdv1.ProcessState_PROCESS_STATE_FAILED:
		return controllers.AdapterObservationFailed, message("Amp failed to start", process.Error), nil
	case sandboxdv1.ProcessState_PROCESS_STATE_EXITED:
		if process.ExitCode == nil {
			return controllers.AdapterObservationFailed, "Amp exited without an exit code", nil
		}
		if process.GetExitCode() != 0 {
			return controllers.AdapterObservationFailed, fmt.Sprintf("Amp exited with code %d", process.GetExitCode()), nil
		}
		output, err := readOutput(ctx, client, key(task), process.ExecutionId)
		if err != nil {
			return "", "", err
		}
		result, ok := finalResult(output)
		if !ok {
			return controllers.AdapterObservationFailed, "Amp exited without a valid result event", nil
		}
		if result.IsError == nil {
			return controllers.AdapterObservationFailed, "Amp result is missing is_error", nil
		}
		if *result.IsError || result.Subtype != "success" {
			detail := result.Result
			if detail == "" {
				detail = errorDetail(result.Error)
			}
			return controllers.AdapterObservationFailed, message("Amp reported "+result.Subtype, detail), nil
		}
		return controllers.AdapterObservationSucceeded, message("Amp completed", result.Result), nil
	default:
		return controllers.AdapterObservationFailed, fmt.Sprintf("Amp returned invalid process state %s", process.State), nil
	}
}

// Cancel idempotently stops only this Run UID's managed process tree.
func (a *Adapter) Cancel(ctx context.Context, task controllers.AdapterTask, sandbox controllers.AdapterSandbox) error {
	client, closeConnection, err := sandbox.DialProcess(ctx)
	if err != nil {
		return err
	}
	defer closeConnection()
	process, err := client.Stop(ctx, &sandboxdv1.StopProcessRequest{Key: key(task), Mode: sandboxdv1.StopMode_STOP_MODE_GRACEFUL, GracePeriodMs: 10_000})
	if err != nil {
		return err
	}
	switch process.State {
	case sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, sandboxdv1.ProcessState_PROCESS_STATE_STOPPING:
		return controllers.ErrAdapterCancellationPending
	case sandboxdv1.ProcessState_PROCESS_STATE_EXITED, sandboxdv1.ProcessState_PROCESS_STATE_FAILED:
		err := a.forward(ctx, client, task, sandbox, process)
		if errors.Is(err, controllers.ErrAdapterEventRejected) {
			return nil
		}
		return err
	default:
		return fmt.Errorf("Amp cancellation returned invalid process state %s", process.State)
	}
}

func (a *Adapter) forward(ctx context.Context, client sandboxdv1.ProcessServiceClient, task controllers.AdapterTask, sandbox controllers.AdapterSandbox, process *sandboxdv1.Process) error {
	if sandbox.EmitEvent == nil || process.ExecutionId == "" {
		return nil
	}
	for _, stream := range []sandboxdv1.OutputStream{sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT, sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR} {
		c := cursor{string(sandbox.EnvironmentUID), task.ID, process.ExecutionId, stream}
		offset := a.getCursor(c)
		for {
			if pending, ok := a.getPending(c); ok {
				if err := sandbox.EmitEvent(ctx, pending.event); err != nil {
					return err
				}
				offset = pending.nextOffset
				a.commitPending(c, offset)
				continue
			}
			response, err := client.ReadOutput(ctx, &sandboxdv1.ReadOutputRequest{Key: key(task), ExecutionId: process.ExecutionId, Stream: stream, Offset: offset, MaxBytes: pageMax})
			if err != nil {
				return err
			}
			if len(response.Data) == 0 && response.GapBytes == 0 {
				break
			}
			payload, err := json.Marshal(outputEvent{process.ExecutionId, streamName(stream), response.Offset, response.NextOffset, response.GapBytes, response.RetainedStart, response.ProducedEnd, response.Eof, response.Data})
			if err != nil {
				return err
			}
			digest := sha256.Sum256(payload)
			event := controllers.AdapterEvent{Source: "amp", IdempotencyKey: fmt.Sprintf("v1:%s:%x", streamName(stream), digest), Type: "amp.process-output", Data: payload}
			a.setPending(c, pendingEvent{event: event, nextOffset: response.NextOffset})
			if err := sandbox.EmitEvent(ctx, event); err != nil {
				return err
			}
			offset = response.NextOffset
			a.commitPending(c, offset)
			if response.Eof || offset >= response.ProducedEnd {
				break
			}
		}
	}
	return nil
}

func readOutput(ctx context.Context, client sandboxdv1.ProcessServiceClient, processKey *sandboxdv1.ProcessKey, execution string) ([]byte, error) {
	var output bytes.Buffer
	var offset uint64
	for {
		response, err := client.ReadOutput(ctx, &sandboxdv1.ReadOutputRequest{Key: processKey, ExecutionId: execution, Stream: sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT, Offset: offset, MaxBytes: pageMax})
		if err != nil {
			return nil, err
		}
		output.Write(response.Data)
		offset = response.NextOffset
		if response.Eof || offset >= response.ProducedEnd {
			return output.Bytes(), nil
		}
	}
}

func finalResult(output []byte) (resultEvent, bool) {
	lines := bytes.Split(output, []byte("\n"))
	for i := len(lines) - 1; i >= 0; i-- {
		if len(bytes.TrimSpace(lines[i])) == 0 {
			continue
		}
		var result resultEvent
		err := json.Unmarshal(lines[i], &result)
		return result, err == nil && result.Type == "result"
	}
	return resultEvent{}, false
}

func streamName(stream sandboxdv1.OutputStream) string {
	if stream == sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR {
		return "stderr"
	}
	return "stdout"
}
func message(summary, detail string) string {
	if detail == "" {
		return summary
	}
	if len(detail) > 512 {
		detail = detail[:512] + "…"
	}
	return summary + ": " + detail
}
func errorDetail(raw json.RawMessage) string {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	return string(raw)
}
func (a *Adapter) getCursor(c cursor) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cursors == nil {
		a.cursors = make(map[cursor]uint64)
	}
	return a.cursors[c]
}
func (a *Adapter) getPending(c cursor) (pendingEvent, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	p, ok := a.pending[c]
	return p, ok
}
func (a *Adapter) setPending(c cursor, p pendingEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.pending == nil {
		a.pending = make(map[cursor]pendingEvent)
	}
	a.pending[c] = p
}
func (a *Adapter) commitPending(c cursor, offset uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cursors == nil {
		a.cursors = make(map[cursor]uint64)
	}
	a.cursors[c] = offset
	delete(a.pending, c)
}
