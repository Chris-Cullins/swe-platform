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

	"github.com/Chris-Cullins/swe-platform/internal/agent"
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
	pending map[outputCursor]pendingEvent
}

var _ agent.AdapterLifecycle = (*Adapter)(nil)

type outputCursor struct {
	environment string
	owner       string
	execution   string
	stream      sandboxdv1.OutputStream
}

type pendingEvent struct {
	event      agent.AdapterEvent
	nextOffset uint64
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

func processKey(task agent.AdapterTask) *sandboxdv1.ProcessKey {
	return &sandboxdv1.ProcessKey{OwnerId: task.ID, Role: processRole}
}

func (a *Adapter) processSpec(task agent.AdapterTask) *sandboxdv1.ProcessSpec {
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
func (a *Adapter) EnsureAccepted(ctx context.Context, task agent.AdapterTask, sandbox agent.AdapterSandbox, material *agent.AdapterLaunchMaterial) error {
	launch, cleanup, err := agent.PrepareLaunchMaterial(material, "ANTHROPIC_API_KEY", true)
	if err != nil {
		return err
	}
	defer cleanup()
	client, closeConnection, err := sandbox.DialProcess(ctx)
	if err != nil {
		return err
	}
	defer closeConnection()
	if launch == nil || len(launch.SecretEnv) == 0 {
		_, err = client.Start(ctx, &sandboxdv1.StartProcessRequest{Key: processKey(task), Spec: a.processSpec(task)})
		return err
	}
	_, err = client.StartWithLaunchMaterial(ctx, &sandboxdv1.StartProcessWithLaunchMaterialRequest{
		Key: processKey(task), Spec: a.processSpec(task),
		LaunchMaterial: launch,
	})
	return err
}

// Observe forwards bounded process output and maps Claude's terminal result to
// the adapter-neutral Run lifecycle.
func (a *Adapter) Observe(ctx context.Context, task agent.AdapterTask, sandbox agent.AdapterSandbox) (agent.AdapterObservation, string, error) {
	client, closeConnection, err := sandbox.DialProcess(ctx)
	if err != nil {
		return "", "", err
	}
	defer closeConnection()
	process, err := client.Get(ctx, &sandboxdv1.GetProcessRequest{Key: processKey(task)})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
		}
		return "", "", err
	}
	if err := a.forwardOutput(ctx, client, task, sandbox, process); err != nil {
		if errors.Is(err, agent.ErrAdapterEventRejected) {
			return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
		}
		return "", "", err
	}

	switch process.State {
	case sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, sandboxdv1.ProcessState_PROCESS_STATE_STOPPING:
		return agent.AdapterObservationRunning, agent.AdapterObservationRunning.StatusMessage(), nil
	case sandboxdv1.ProcessState_PROCESS_STATE_FAILED:
		return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
	case sandboxdv1.ProcessState_PROCESS_STATE_EXITED:
		if process.ExitCode == nil {
			return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
		}
		if process.GetExitCode() != 0 {
			return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
		}
		output, err := readRetainedOutput(ctx, client, processKey(task), process.ExecutionId, sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT)
		if err != nil {
			var truncated *outputTruncatedError
			if errors.As(err, &truncated) {
				return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
			}
			return "", "", err
		}
		result, ok := finalResult(output)
		if !ok {
			return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
		}
		if result.IsError == nil {
			return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
		}
		if *result.IsError || result.Subtype != "success" {
			return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
		}
		return agent.AdapterObservationSucceeded, agent.AdapterObservationSucceeded.StatusMessage(), nil
	default:
		return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
	}
}

// Cancel idempotently stops only this Run UID's managed process tree.
func (a *Adapter) Cancel(ctx context.Context, task agent.AdapterTask, sandbox agent.AdapterSandbox) error {
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
		return agent.ErrAdapterCancellationPending
	case sandboxdv1.ProcessState_PROCESS_STATE_EXITED, sandboxdv1.ProcessState_PROCESS_STATE_FAILED:
		err := a.forwardOutput(ctx, client, task, sandbox, process)
		if errors.Is(err, agent.ErrAdapterEventRejected) {
			return nil
		}
		return err
	default:
		return fmt.Errorf("Claude Code cancellation returned invalid process state %s", process.State)
	}
}

func (a *Adapter) forwardOutput(ctx context.Context, client sandboxdv1.ProcessServiceClient, task agent.AdapterTask, sandbox agent.AdapterSandbox, process *sandboxdv1.Process) error {
	if sandbox.EmitEvent == nil || process.ExecutionId == "" {
		return nil
	}
	for _, stream := range []sandboxdv1.OutputStream{sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT, sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR} {
		cursor := outputCursor{environment: string(sandbox.EnvironmentUID), owner: task.ID, execution: process.ExecutionId, stream: stream}
		offset := a.cursor(cursor)
		for {
			if pending, ok := a.pendingEvent(cursor); ok {
				if err := sandbox.EmitEvent(ctx, pending.event); err != nil {
					if errors.Is(err, agent.ErrAdapterEventRejected) {
						a.dropPending(cursor)
					}
					return err
				}
				offset = pending.nextOffset
				a.commitPending(cursor, offset)
				continue
			}
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
			event := agent.AdapterEvent{Source: "claude-code", IdempotencyKey: key, Type: "claude-code.process-output", Data: payload}
			a.setPending(cursor, pendingEvent{event: event, nextOffset: response.NextOffset})
			if err := sandbox.EmitEvent(ctx, event); err != nil {
				if errors.Is(err, agent.ErrAdapterEventRejected) {
					a.dropPending(cursor)
				}
				return err
			}
			offset = response.NextOffset
			a.commitPending(cursor, offset)
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
		if response.GapBytes != 0 || response.Offset != offset {
			return nil, &outputTruncatedError{retainedFrom: response.RetainedStart}
		}
		output.Write(response.Data)
		offset = response.NextOffset
		if response.Eof || offset >= response.ProducedEnd {
			return output.Bytes(), nil
		}
	}
}

type outputTruncatedError struct {
	retainedFrom uint64
}

func (e *outputTruncatedError) Error() string {
	return fmt.Sprintf("retained from offset %d", e.retainedFrom)
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

func (a *Adapter) cursor(key outputCursor) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cursors == nil {
		a.cursors = make(map[outputCursor]uint64)
	}
	return a.cursors[key]
}

func (a *Adapter) pendingEvent(key outputCursor) (pendingEvent, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	pending, ok := a.pending[key]
	return pending, ok
}

func (a *Adapter) setPending(key outputCursor, pending pendingEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.pending == nil {
		a.pending = make(map[outputCursor]pendingEvent)
	}
	a.pending[key] = pending
}

func (a *Adapter) dropPending(key outputCursor) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.pending, key)
}

func (a *Adapter) commitPending(key outputCursor, offset uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cursors == nil {
		a.cursors = make(map[outputCursor]uint64)
	}
	a.cursors[key] = offset
	delete(a.pending, key)
}
