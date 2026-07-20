// Package pi implements the Pi foreground-process adapter.
package pi

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

const processRole = "agent"

const outputPageMax = 64 * 1024

type assistantMessage struct {
	Role         string `json:"role"`
	StopReason   string `json:"stopReason"`
	ErrorMessage string `json:"errorMessage"`
}

type agentEndEvent struct {
	Type     string             `json:"type"`
	Messages []assistantMessage `json:"messages"`
}

// Adapter drives one non-interactive Pi process per Run UID.
type Adapter struct {
	Executable string

	mu      sync.Mutex
	cursors map[outputCursor]uint64
}

type outputCursor struct {
	environment string
	owner       string
	execution   string
	stream      sandboxdv1.OutputStream
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

func (a *Adapter) executable() string {
	if a.Executable != "" {
		return a.Executable
	}
	return "pi"
}

func processKey(task controllers.AdapterTask) *sandboxdv1.ProcessKey {
	return &sandboxdv1.ProcessKey{OwnerId: task.ID, Role: processRole}
}

func (a *Adapter) processSpec(task controllers.AdapterTask) *sandboxdv1.ProcessSpec {
	return &sandboxdv1.ProcessSpec{
		Argv: []string{
			a.executable(),
			"--mode", "json",
			"--no-session",
			"--no-approve",
			"--no-extensions",
			"--no-skills",
			"--no-prompt-templates",
			"--no-themes",
			"--offline",
			// Pi 0.80.10 has no flag terminator. Leading whitespace keeps a
			// flag-shaped task in the positional-message parser.
			"\n" + task.Prompt,
		},
		Env: map[string]string{
			"PI_CODING_AGENT_DIR": "/tmp/swe-platform/pi/" + task.ID,
		},
		EnvMode: sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT,
	}
}

// EnsureAccepted duplicate-safely starts (or recovers) the Run-keyed process.
func (a *Adapter) EnsureAccepted(ctx context.Context, task controllers.AdapterTask, sandbox controllers.AdapterSandbox) error {
	client, closeConnection, err := sandbox.DialProcess(ctx)
	if err != nil {
		return err
	}
	defer closeConnection()
	_, err = client.Start(ctx, &sandboxdv1.StartProcessRequest{Key: processKey(task), Spec: a.processSpec(task)})
	return err
}

// Observe maps Pi's terminal agent_end event and managed process state to the
// adapter-neutral Run lifecycle.
func (a *Adapter) Observe(ctx context.Context, task controllers.AdapterTask, sandbox controllers.AdapterSandbox) (controllers.AdapterObservation, string, error) {
	client, closeConnection, err := sandbox.DialProcess(ctx)
	if err != nil {
		return "", "", err
	}
	defer closeConnection()
	process, err := client.Get(ctx, &sandboxdv1.GetProcessRequest{Key: processKey(task)})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return controllers.AdapterObservationFailed, "Pi execution is absent in the current sandbox epoch", nil
		}
		return "", "", err
	}
	if err := a.forwardOutput(ctx, client, task, sandbox, process); err != nil {
		if errors.Is(err, controllers.ErrAdapterEventRejected) {
			return controllers.AdapterObservationFailed, "Pi transcript output was permanently rejected", nil
		}
		return "", "", err
	}

	switch process.State {
	case sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, sandboxdv1.ProcessState_PROCESS_STATE_STOPPING:
		return controllers.AdapterObservationRunning, "Pi is running", nil
	case sandboxdv1.ProcessState_PROCESS_STATE_FAILED:
		return controllers.AdapterObservationFailed, processMessage("Pi failed to start", process.Error), nil
	case sandboxdv1.ProcessState_PROCESS_STATE_EXITED:
		if process.ExitCode == nil {
			return controllers.AdapterObservationFailed, "Pi exited without an exit code", nil
		}
		if process.GetExitCode() != 0 {
			return controllers.AdapterObservationFailed, fmt.Sprintf("Pi exited with code %d", process.GetExitCode()), nil
		}
		output, err := readRetainedOutput(ctx, client, processKey(task), process.ExecutionId, sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT)
		if err != nil {
			return "", "", err
		}
		message, ok := finalAssistant(output)
		if !ok {
			return controllers.AdapterObservationFailed, "Pi exited without a valid agent_end assistant message", nil
		}
		switch message.StopReason {
		case "stop":
			return controllers.AdapterObservationSucceeded, "Pi completed", nil
		case "error", "aborted":
			return controllers.AdapterObservationFailed, processMessage("Pi reported "+message.StopReason, message.ErrorMessage), nil
		default:
			return controllers.AdapterObservationFailed, fmt.Sprintf("Pi ended with stop reason %q", message.StopReason), nil
		}
	default:
		return controllers.AdapterObservationFailed, fmt.Sprintf("Pi returned invalid process state %s", process.State), nil
	}
}

// Cancel idempotently stops only this Run UID's managed process tree.
func (a *Adapter) Cancel(ctx context.Context, task controllers.AdapterTask, sandbox controllers.AdapterSandbox) error {
	client, closeConnection, err := sandbox.DialProcess(ctx)
	if err != nil {
		return err
	}
	defer closeConnection()
	process, err := client.Stop(ctx, &sandboxdv1.StopProcessRequest{
		Key:           processKey(task),
		Mode:          sandboxdv1.StopMode_STOP_MODE_GRACEFUL,
		GracePeriodMs: 10_000,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return err
	}
	switch process.State {
	case sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, sandboxdv1.ProcessState_PROCESS_STATE_STOPPING:
		return controllers.ErrAdapterCancellationPending
	case sandboxdv1.ProcessState_PROCESS_STATE_EXITED, sandboxdv1.ProcessState_PROCESS_STATE_FAILED:
		err := a.forwardOutput(ctx, client, task, sandbox, process)
		if errors.Is(err, controllers.ErrAdapterEventRejected) {
			return nil
		}
		return err
	default:
		return fmt.Errorf("Pi cancellation returned invalid process state %s", process.State)
	}
}

func (a *Adapter) forwardOutput(ctx context.Context, client sandboxdv1.ProcessServiceClient, task controllers.AdapterTask, sandbox controllers.AdapterSandbox, process *sandboxdv1.Process) error {
	if sandbox.EmitEvent == nil || process.ExecutionId == "" {
		return nil
	}
	for _, stream := range []sandboxdv1.OutputStream{sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT, sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR} {
		cursor := outputCursor{environment: string(sandbox.EnvironmentUID), owner: task.ID, execution: process.ExecutionId, stream: stream}
		offset := a.cursor(cursor)
		for {
			response, err := client.ReadOutput(ctx, &sandboxdv1.ReadOutputRequest{Key: processKey(task), ExecutionId: process.ExecutionId, Stream: stream, Offset: offset, MaxBytes: outputPageMax})
			if err != nil {
				return err
			}
			if len(response.Data) == 0 && response.GapBytes == 0 {
				break
			}
			payload, err := json.Marshal(outputEvent{
				ExecutionID: process.ExecutionId, Stream: streamName(stream), Offset: response.Offset,
				NextOffset: response.NextOffset, GapBytes: response.GapBytes, RetainedFrom: response.RetainedStart,
				ProducedEnd: response.ProducedEnd, EOF: response.Eof, Data: response.Data,
			})
			if err != nil {
				return err
			}
			digest := sha256.Sum256(payload)
			key := fmt.Sprintf("v1:%s:%x", streamName(stream), digest)
			if err := sandbox.EmitEvent(ctx, controllers.AdapterEvent{Source: "pi", IdempotencyKey: key, Type: "pi.process-output", Data: payload}); err != nil {
				return err
			}
			offset = response.NextOffset
			a.setCursor(cursor, offset)
			if response.Eof || offset >= response.ProducedEnd {
				break
			}
		}
	}
	return nil
}

func readRetainedOutput(ctx context.Context, client sandboxdv1.ProcessServiceClient, key *sandboxdv1.ProcessKey, executionID string, stream sandboxdv1.OutputStream) ([]byte, error) {
	var output bytes.Buffer
	var offset uint64
	for {
		response, err := client.ReadOutput(ctx, &sandboxdv1.ReadOutputRequest{Key: key, ExecutionId: executionID, Stream: stream, Offset: offset, MaxBytes: outputPageMax})
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

func finalAssistant(output []byte) (assistantMessage, bool) {
	var terminal agentEndEvent
	found := false
	for _, line := range bytes.Split(output, []byte("\n")) {
		var candidate agentEndEvent
		if json.Unmarshal(line, &candidate) == nil && candidate.Type == "agent_end" {
			terminal, found = candidate, true
		}
	}
	if !found {
		return assistantMessage{}, false
	}
	for i := len(terminal.Messages) - 1; i >= 0; i-- {
		if terminal.Messages[i].Role == "assistant" {
			return terminal.Messages[i], true
		}
	}
	return assistantMessage{}, false
}

func processMessage(summary, detail string) string {
	if detail == "" {
		return summary
	}
	const maxDetail = 512
	if len(detail) > maxDetail {
		detail = detail[:maxDetail] + "…"
	}
	return summary + ": " + detail
}

func streamName(stream sandboxdv1.OutputStream) string {
	if stream == sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR {
		return "stderr"
	}
	return "stdout"
}

func (a *Adapter) cursor(key outputCursor) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cursors == nil {
		a.cursors = make(map[outputCursor]uint64)
	}
	return a.cursors[key]
}

func (a *Adapter) setCursor(key outputCursor, offset uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cursors == nil {
		a.cursors = make(map[outputCursor]uint64)
	}
	a.cursors[key] = offset
}
