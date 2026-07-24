// Package pi implements the Pi coding agent foreground-process adapter.
package pi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

type Adapter struct {
	Executable string
	mu         sync.Mutex
	cursors    map[cursor]uint64
	pending    map[cursor]pendingEvent
}
type cursor struct {
	environment, owner, execution string
	stream                        sandboxdv1.OutputStream
}
type pendingEvent struct {
	event      controllers.AdapterEvent
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
type piEvent struct {
	Type     string      `json:"type"`
	Messages []piMessage `json:"messages"`
}
type piMessage struct {
	Role       string `json:"role"`
	StopReason string `json:"stopReason"`
	Error      string `json:"errorMessage"`
}
type outputTruncatedError struct{ retainedFrom uint64 }

func (e *outputTruncatedError) Error() string {
	return fmt.Sprintf("retained from offset %d", e.retainedFrom)
}
func (a *Adapter) SupportsCredentialProfiles() bool { return false }
func (a *Adapter) executable() string {
	if a.Executable != "" {
		return a.Executable
	}
	return "pi"
}
func key(t controllers.AdapterTask) *sandboxdv1.ProcessKey {
	return &sandboxdv1.ProcessKey{OwnerId: t.ID, Role: processRole}
}
func (a *Adapter) spec(t controllers.AdapterTask) *sandboxdv1.ProcessSpec {
	return &sandboxdv1.ProcessSpec{Argv: []string{a.executable(), "--mode", "json", "--no-session", "-p", t.Prompt}, EnvMode: sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT}
}

func (a *Adapter) EnsureAccepted(ctx context.Context, task controllers.AdapterTask, sandbox controllers.AdapterSandbox, credential *controllers.AdapterCredential) error {
	if credential != nil {
		return fmt.Errorf("%w: Pi does not support credential profiles", controllers.ErrAdapterTaskRejected)
	}
	if strings.HasPrefix(task.Prompt, "-") || strings.HasPrefix(task.Prompt, "@") {
		return fmt.Errorf("%w: Pi prompt begins with unsupported parser prefix", controllers.ErrAdapterTaskRejected)
	}
	c, close, err := sandbox.DialProcess(ctx)
	if err != nil {
		return err
	}
	defer close()
	_, err = c.Start(ctx, &sandboxdv1.StartProcessRequest{Key: key(task), Spec: a.spec(task)})
	return err
}
func (a *Adapter) Observe(ctx context.Context, task controllers.AdapterTask, sandbox controllers.AdapterSandbox) (controllers.AdapterObservation, string, error) {
	c, close, err := sandbox.DialProcess(ctx)
	if err != nil {
		return "", "", err
	}
	defer close()
	p, err := c.Get(ctx, &sandboxdv1.GetProcessRequest{Key: key(task)})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return controllers.AdapterObservationFailed, "Pi execution is absent in the current sandbox epoch", nil
		}
		return "", "", err
	}
	if err = a.forward(ctx, c, task, sandbox, p); err != nil {
		if errors.Is(err, controllers.ErrAdapterEventRejected) {
			return controllers.AdapterObservationFailed, "Pi transcript output was permanently rejected", nil
		}
		return "", "", err
	}
	switch p.State {
	case sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, sandboxdv1.ProcessState_PROCESS_STATE_STOPPING:
		return controllers.AdapterObservationRunning, "Pi is running", nil
	case sandboxdv1.ProcessState_PROCESS_STATE_FAILED:
		return controllers.AdapterObservationFailed, message("Pi failed to start", p.Error), nil
	case sandboxdv1.ProcessState_PROCESS_STATE_EXITED:
		if p.ExitCode == nil {
			return controllers.AdapterObservationFailed, "Pi exited without an exit code", nil
		}
		if p.GetExitCode() != 0 {
			return controllers.AdapterObservationFailed, fmt.Sprintf("Pi exited with code %d", p.GetExitCode()), nil
		}
		out, e := readOutput(ctx, c, key(task), p.ExecutionId)
		if e != nil {
			var gap *outputTruncatedError
			if errors.As(e, &gap) {
				return controllers.AdapterObservationFailed, message("Pi stdout was truncated before terminal validation", gap.Error()), nil
			}
			return "", "", e
		}
		detail, ok := terminal(out)
		if !ok {
			return controllers.AdapterObservationFailed, message("Pi exited without a coherent agent_end", detail), nil
		}
		return controllers.AdapterObservationSucceeded, "Pi completed", nil
	default:
		return controllers.AdapterObservationFailed, fmt.Sprintf("Pi returned invalid process state %s", p.State), nil
	}
}
func terminal(out []byte) (string, bool) {
	var end *piEvent
	for _, line := range bytes.Split(out, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var e piEvent
		if json.Unmarshal(line, &e) != nil {
			return "malformed JSONL output", false
		}
		if e.Type == "" {
			return "JSONL event is missing type", false
		}
		if end != nil {
			return "output after agent_end", false
		}
		if e.Type == "agent_end" {
			copy := e
			end = &copy
		}
	}
	if end == nil {
		return "missing agent_end", false
	}
	if len(end.Messages) == 0 {
		return "agent_end has no messages", false
	}
	var final *piMessage
	for i := len(end.Messages) - 1; i >= 0; i-- {
		if end.Messages[i].Role == "assistant" {
			final = &end.Messages[i]
			break
		}
	}
	if final == nil || final.StopReason == "" {
		return "agent_end has no coherent final assistant", false
	}
	switch final.StopReason {
	case "stop", "length", "toolUse":
		return "", true
	case "error", "aborted":
		return message("final assistant stopReason is "+final.StopReason, final.Error), false
	default:
		return "final assistant has invalid stopReason", false
	}
}
func (a *Adapter) Cancel(ctx context.Context, task controllers.AdapterTask, sandbox controllers.AdapterSandbox) error {
	c, close, err := sandbox.DialProcess(ctx)
	if err != nil {
		return err
	}
	defer close()
	p, err := c.Stop(ctx, &sandboxdv1.StopProcessRequest{Key: key(task), Mode: sandboxdv1.StopMode_STOP_MODE_GRACEFUL, GracePeriodMs: 10000})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return err
	}
	switch p.State {
	case sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, sandboxdv1.ProcessState_PROCESS_STATE_STOPPING:
		return controllers.ErrAdapterCancellationPending
	case sandboxdv1.ProcessState_PROCESS_STATE_EXITED, sandboxdv1.ProcessState_PROCESS_STATE_FAILED:
		err = a.forward(ctx, c, task, sandbox, p)
		if errors.Is(err, controllers.ErrAdapterEventRejected) {
			return nil
		}
		return err
	default:
		return fmt.Errorf("Pi cancellation returned invalid process state %s", p.State)
	}
}
func (a *Adapter) forward(ctx context.Context, c sandboxdv1.ProcessServiceClient, task controllers.AdapterTask, s controllers.AdapterSandbox, p *sandboxdv1.Process) error {
	if s.EmitEvent == nil || p.ExecutionId == "" {
		return nil
	}
	for _, stream := range []sandboxdv1.OutputStream{sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT, sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR} {
		k := cursor{string(s.EnvironmentUID), task.ID, p.ExecutionId, stream}
		o := a.getCursor(k)
		for {
			if pending, ok := a.getPending(k); ok {
				if err := s.EmitEvent(ctx, pending.event); err != nil {
					return err
				}
				o = pending.nextOffset
				a.commit(k, o)
				continue
			}
			r, err := c.ReadOutput(ctx, &sandboxdv1.ReadOutputRequest{Key: key(task), ExecutionId: p.ExecutionId, Stream: stream, Offset: o, MaxBytes: pageMax})
			if err != nil {
				return err
			}
			if len(r.Data) == 0 && r.GapBytes == 0 {
				break
			}
			payload, _ := json.Marshal(outputEvent{p.ExecutionId, streamName(stream), r.Offset, r.NextOffset, r.GapBytes, r.RetainedStart, r.ProducedEnd, r.Eof, r.Data})
			digest := sha256.Sum256(payload)
			event := controllers.AdapterEvent{Source: "pi", Type: "pi.process-output", IdempotencyKey: fmt.Sprintf("v1:%s:%x", streamName(stream), digest), Data: payload}
			a.setPending(k, pendingEvent{event, r.NextOffset})
			if err = s.EmitEvent(ctx, event); err != nil {
				return err
			}
			o = r.NextOffset
			a.commit(k, o)
			if r.Eof || o >= r.ProducedEnd {
				break
			}
		}
	}
	return nil
}
func readOutput(ctx context.Context, c sandboxdv1.ProcessServiceClient, k *sandboxdv1.ProcessKey, execution string) ([]byte, error) {
	var b bytes.Buffer
	var o uint64
	for {
		r, e := c.ReadOutput(ctx, &sandboxdv1.ReadOutputRequest{Key: k, ExecutionId: execution, Stream: sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT, Offset: o, MaxBytes: pageMax})
		if e != nil {
			return nil, e
		}
		if r.GapBytes != 0 || r.Offset != o {
			return nil, &outputTruncatedError{r.RetainedStart}
		}
		b.Write(r.Data)
		o = r.NextOffset
		if r.Eof || o >= r.ProducedEnd {
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
func message(a, b string) string {
	if b == "" {
		return a
	}
	if len(b) > 512 {
		b = b[:512] + "…"
	}
	return a + ": " + b
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
