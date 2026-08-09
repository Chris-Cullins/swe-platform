// Package codex implements the OpenAI Codex foreground-process adapter.
package codex

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
	processRole = "agent"
	pageMax     = 64 * 1024
)

type Adapter struct {
	Executable string
	mu         sync.Mutex
	cursors    map[cursor]uint64
	pending    map[cursor]pendingEvent
}

var _ agent.AdapterLifecycle = (*Adapter)(nil)

type cursor struct {
	environment, owner, execution string
	stream                        sandboxdv1.OutputStream
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
type terminalEvent struct {
	Type     string          `json:"type"`
	ThreadID string          `json:"thread_id"`
	Usage    json.RawMessage `json:"usage"`
	Error    struct {
		Message string `json:"message"`
	} `json:"error"`
	Message string `json:"message"`
}

type outputTruncatedError struct {
	retainedFrom uint64
}

func (e *outputTruncatedError) Error() string {
	return fmt.Sprintf("retained from offset %d", e.retainedFrom)
}

func (a *Adapter) executable() string {
	if a.Executable != "" {
		return a.Executable
	}
	return "codex"
}
func key(t agent.AdapterTask) *sandboxdv1.ProcessKey {
	return &sandboxdv1.ProcessKey{OwnerId: t.ID, Role: processRole}
}
func (a *Adapter) spec(t agent.AdapterTask) *sandboxdv1.ProcessSpec {
	return &sandboxdv1.ProcessSpec{Argv: []string{a.executable(), "exec", "--json", "--ephemeral", "--ignore-user-config", "--ignore-rules", "--sandbox", "workspace-write", "--color", "never", "--skip-git-repo-check", "--", t.Prompt}, EnvMode: sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT}
}

func (a *Adapter) EnsureAccepted(ctx context.Context, task agent.AdapterTask, sandbox agent.AdapterSandbox, material *agent.AdapterLaunchMaterial) error {
	if task.Prompt == "-" {
		return fmt.Errorf("%w: Codex prompt '-' requires managed stdin", agent.ErrAdapterTaskRejected)
	}
	launch, cleanup, err := agent.PrepareLaunchMaterial(material, "CODEX_API_KEY", true)
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
		_, err = client.Start(ctx, &sandboxdv1.StartProcessRequest{Key: key(task), Spec: a.spec(task)})
		return err
	}
	_, err = client.StartWithLaunchMaterial(ctx, &sandboxdv1.StartProcessWithLaunchMaterialRequest{Key: key(task), Spec: a.spec(task), LaunchMaterial: launch})
	return err
}

func (a *Adapter) Observe(ctx context.Context, task agent.AdapterTask, sandbox agent.AdapterSandbox) (agent.AdapterObservation, string, error) {
	client, closeConnection, err := sandbox.DialProcess(ctx)
	if err != nil {
		return "", "", err
	}
	defer closeConnection()
	p, err := client.Get(ctx, &sandboxdv1.GetProcessRequest{Key: key(task)})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
		}
		return "", "", err
	}
	if err = a.forward(ctx, client, task, sandbox, p); err != nil {
		if errors.Is(err, agent.ErrAdapterEventRejected) {
			return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
		}
		return "", "", err
	}
	switch p.State {
	case sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, sandboxdv1.ProcessState_PROCESS_STATE_STOPPING:
		return agent.AdapterObservationRunning, agent.AdapterObservationRunning.StatusMessage(), nil
	case sandboxdv1.ProcessState_PROCESS_STATE_FAILED:
		return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
	case sandboxdv1.ProcessState_PROCESS_STATE_EXITED:
		if p.ExitCode == nil {
			return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
		}
		if p.GetExitCode() != 0 {
			return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
		}
		out, e := readOutput(ctx, client, key(task), p.ExecutionId)
		if e != nil {
			var truncated *outputTruncatedError
			if errors.As(e, &truncated) {
				return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
			}
			return "", "", e
		}
		_, _, ok := terminal(out)
		if !ok {
			return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
		}
		return agent.AdapterObservationSucceeded, agent.AdapterObservationSucceeded.StatusMessage(), nil
	default:
		return agent.AdapterObservationFailed, agent.AdapterObservationFailed.StatusMessage(), nil
	}
}

func terminal(output []byte) (string, string, bool) {
	var thread string
	completed := false
	terminalSeen := false
	eventCount := 0
	lastError := ""
	for _, line := range bytes.Split(output, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var e terminalEvent
		if json.Unmarshal(line, &e) != nil {
			return thread, "malformed JSONL output", false
		}
		if terminalSeen {
			return thread, "output after completed turn", false
		}
		switch e.Type {
		case "thread.started":
			if eventCount != 0 || thread != "" {
				return thread, "duplicate or late thread.started", false
			}
			if e.ThreadID == "" {
				return "", "empty thread.started thread_id", false
			}
			thread = e.ThreadID
		case "turn.completed":
			if thread == "" {
				return "", "turn.completed before thread.started", false
			}
			terminalSeen = true
			var usage map[string]json.RawMessage
			completed = json.Unmarshal(e.Usage, &usage) == nil && usage != nil
		case "turn.failed":
			terminalSeen = true
			return thread, e.Error.Message, false
		case "error":
			lastError = e.Message
		}
		eventCount++
	}
	if thread == "" {
		return "", "missing thread.started", false
	}
	if !completed {
		if lastError != "" {
			return thread, lastError, false
		}
		return thread, "missing turn.completed usage", false
	}
	return thread, "", true
}

func (a *Adapter) Cancel(ctx context.Context, task agent.AdapterTask, sandbox agent.AdapterSandbox) error {
	client, closeConnection, err := sandbox.DialProcess(ctx)
	if err != nil {
		return err
	}
	defer closeConnection()
	p, err := client.Stop(ctx, &sandboxdv1.StopProcessRequest{Key: key(task), Mode: sandboxdv1.StopMode_STOP_MODE_GRACEFUL, GracePeriodMs: 10_000})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return err
	}
	switch p.State {
	case sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, sandboxdv1.ProcessState_PROCESS_STATE_STOPPING:
		return agent.ErrAdapterCancellationPending
	case sandboxdv1.ProcessState_PROCESS_STATE_EXITED, sandboxdv1.ProcessState_PROCESS_STATE_FAILED:
		err = a.forward(ctx, client, task, sandbox, p)
		if errors.Is(err, agent.ErrAdapterEventRejected) {
			return nil
		}
		return err
	default:
		return fmt.Errorf("Codex cancellation returned invalid process state %s", p.State)
	}
}

func (a *Adapter) forward(ctx context.Context, client sandboxdv1.ProcessServiceClient, task agent.AdapterTask, sandbox agent.AdapterSandbox, p *sandboxdv1.Process) error {
	if sandbox.EmitEvent == nil || p.ExecutionId == "" {
		return nil
	}
	for _, stream := range []sandboxdv1.OutputStream{sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT, sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR} {
		c := cursor{string(sandbox.EnvironmentUID), task.ID, p.ExecutionId, stream}
		offset := a.getCursor(c)
		for {
			if pending, ok := a.getPending(c); ok {
				if err := sandbox.EmitEvent(ctx, pending.event); err != nil {
					return err
				}
				offset = pending.nextOffset
				a.commit(c, offset)
				continue
			}
			r, err := client.ReadOutput(ctx, &sandboxdv1.ReadOutputRequest{Key: key(task), ExecutionId: p.ExecutionId, Stream: stream, Offset: offset, MaxBytes: pageMax})
			if err != nil {
				return err
			}
			if len(r.Data) == 0 && r.GapBytes == 0 {
				break
			}
			payload, _ := json.Marshal(outputEvent{p.ExecutionId, streamName(stream), r.Offset, r.NextOffset, r.GapBytes, r.RetainedStart, r.ProducedEnd, r.Eof, r.Data})
			digest := sha256.Sum256(payload)
			event := agent.AdapterEvent{Source: "codex", Type: "codex.process-output", IdempotencyKey: fmt.Sprintf("v1:%s:%x", streamName(stream), digest), Data: payload}
			a.setPending(c, pendingEvent{event, r.NextOffset})
			if err = sandbox.EmitEvent(ctx, event); err != nil {
				return err
			}
			offset = r.NextOffset
			a.commit(c, offset)
			if r.Eof || offset >= r.ProducedEnd {
				break
			}
		}
	}
	return nil
}
func readOutput(ctx context.Context, client sandboxdv1.ProcessServiceClient, k *sandboxdv1.ProcessKey, execution string) ([]byte, error) {
	var b bytes.Buffer
	var offset uint64
	for {
		r, e := client.ReadOutput(ctx, &sandboxdv1.ReadOutputRequest{Key: k, ExecutionId: execution, Stream: sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT, Offset: offset, MaxBytes: pageMax})
		if e != nil {
			return nil, e
		}
		if r.GapBytes != 0 || r.Offset != offset {
			return nil, &outputTruncatedError{retainedFrom: r.RetainedStart}
		}
		b.Write(r.Data)
		offset = r.NextOffset
		if r.Eof || offset >= r.ProducedEnd {
			return b.Bytes(), nil
		}
	}
}
func streamName(s sandboxdv1.OutputStream) string {
	if s == sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR {
		return "stderr"
	}
	return "stdout"
}
func (a *Adapter) getCursor(c cursor) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cursors == nil {
		a.cursors = map[cursor]uint64{}
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
		a.pending = map[cursor]pendingEvent{}
	}
	a.pending[c] = p
}
func (a *Adapter) commit(c cursor, o uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cursors == nil {
		a.cursors = map[cursor]uint64{}
	}
	a.cursors[c] = o
	delete(a.pending, c)
}
