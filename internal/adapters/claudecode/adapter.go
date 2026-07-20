// Package claudecode implements the Claude Code foreground-process adapter.
package claudecode

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

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/controllers"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const (
	processRole   = "agent"
	outputPageMax = 64 * 1024
)

// Adapter drives one non-interactive Claude Code process per Run UID.
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

type resultEvent struct {
	Type    string `json:"type"`
	Subtype string `json:"subtype"`
	IsError *bool  `json:"is_error"`
	Result  string `json:"result"`
}

func (a *Adapter) executable() string {
	if a.Executable != "" {
		return a.Executable
	}
	return "claude"
}

func processKey(task controllers.AdapterTask) *sandboxdv1.ProcessKey {
	return &sandboxdv1.ProcessKey{OwnerId: task.ID, Role: processRole}
}

func (a *Adapter) processSpec(task controllers.AdapterTask) *sandboxdv1.ProcessSpec {
	return &sandboxdv1.ProcessSpec{
		Argv: []string{
			a.executable(),
			"--print",
			"--output-format", "stream-json",
			"--verbose",
			"--permission-mode", "bypassPermissions",
			"--no-session-persistence",
			"--",
			task.Prompt,
		},
		EnvMode: sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT,
	}
}

// EnsureAccepted duplicate-safely starts (or recovers) the Run-keyed process.
func (a *Adapter) EnsureAccepted(ctx context.Context, task controllers.AdapterTask, sandbox controllers.AdapterSandbox, credential *controllers.AdapterCredential) error {
	if credential != nil && credential.Type != platformv1alpha1.AgentCredentialTypeAPIKey {
		return fmt.Errorf("unsupported credential type %q", credential.Type)
	}
	client, closeConnection, err := sandbox.DialProcess(ctx)
	if err != nil {
		return err
	}
	defer closeConnection()
	if credential == nil {
		_, err = client.Start(ctx, &sandboxdv1.StartProcessRequest{Key: processKey(task), Spec: a.processSpec(task)})
		return err
	}
	apiKey := append([]byte(nil), credential.APIKey...)
	defer clear(apiKey)
	_, err = client.StartWithLaunchMaterial(ctx, &sandboxdv1.StartProcessWithLaunchMaterialRequest{
		Key: processKey(task), Spec: a.processSpec(task),
		LaunchMaterial: &sandboxdv1.LaunchMaterial{SecretEnv: map[string][]byte{"ANTHROPIC_API_KEY": apiKey}},
	})
	return err
}

// Observe forwards bounded process output and maps Claude's terminal result to
// the adapter-neutral Run lifecycle.
func (a *Adapter) Observe(ctx context.Context, task controllers.AdapterTask, sandbox controllers.AdapterSandbox) (controllers.AdapterObservation, string, error) {
	client, closeConnection, err := sandbox.DialProcess(ctx)
	if err != nil {
		return "", "", err
	}
	defer closeConnection()
	process, err := client.Get(ctx, &sandboxdv1.GetProcessRequest{Key: processKey(task)})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return controllers.AdapterObservationFailed, "Claude Code execution is absent in the current sandbox epoch", nil
		}
		return "", "", err
	}
	if err := a.forwardOutput(ctx, client, task, sandbox, process); err != nil {
		if errors.Is(err, controllers.ErrAdapterEventRejected) {
			return controllers.AdapterObservationFailed, "Claude Code transcript output was permanently rejected", nil
		}
		return "", "", err
	}

	switch process.State {
	case sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, sandboxdv1.ProcessState_PROCESS_STATE_STOPPING:
		return controllers.AdapterObservationRunning, "Claude Code is running", nil
	case sandboxdv1.ProcessState_PROCESS_STATE_FAILED:
		return controllers.AdapterObservationFailed, processMessage("Claude Code failed to start", process.Error), nil
	case sandboxdv1.ProcessState_PROCESS_STATE_EXITED:
		if process.ExitCode == nil {
			return controllers.AdapterObservationFailed, "Claude Code exited without an exit code", nil
		}
		if process.GetExitCode() != 0 {
			return controllers.AdapterObservationFailed, fmt.Sprintf("Claude Code exited with code %d", process.GetExitCode()), nil
		}
		output, err := readRetainedOutput(ctx, client, processKey(task), process.ExecutionId, sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT)
		if err != nil {
			return "", "", err
		}
		result, ok := finalResult(output)
		if !ok {
			return controllers.AdapterObservationFailed, "Claude Code exited without a valid result event", nil
		}
		if result.IsError == nil {
			return controllers.AdapterObservationFailed, "Claude Code result is missing is_error", nil
		}
		if *result.IsError || result.Subtype != "success" {
			return controllers.AdapterObservationFailed, processMessage("Claude Code reported "+result.Subtype, result.Result), nil
		}
		return controllers.AdapterObservationSucceeded, processMessage("Claude Code completed", result.Result), nil
	default:
		return controllers.AdapterObservationFailed, fmt.Sprintf("Claude Code returned invalid process state %s", process.State), nil
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
		return fmt.Errorf("Claude Code cancellation returned invalid process state %s", process.State)
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
			if err := sandbox.EmitEvent(ctx, controllers.AdapterEvent{Source: "claude-code", IdempotencyKey: key, Type: "claude-code.process-output", Data: payload}); err != nil {
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

func finalResult(output []byte) (resultEvent, bool) {
	var result resultEvent
	found := false
	for _, line := range bytes.Split(output, []byte("\n")) {
		var candidate resultEvent
		if json.Unmarshal(line, &candidate) == nil && candidate.Type == "result" {
			result, found = candidate, true
		}
	}
	return result, found
}

func streamName(stream sandboxdv1.OutputStream) string {
	if stream == sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR {
		return "stderr"
	}
	return "stdout"
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
