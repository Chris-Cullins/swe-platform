// Package amp implements the Amp CLI foreground-process adapter.
package amp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Chris-Cullins/swe-platform/internal/adapters/processoutput"
	"github.com/Chris-Cullins/swe-platform/internal/controllers"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const processRole = "agent"

// Adapter drives one new, non-interactive Amp thread per Run UID and sandbox
// epoch. It never selects or continues a global/latest Amp thread.
type Adapter struct {
	Executable string

	output processoutput.Forwarder
}

type resultEvent struct {
	Type       string   `json:"type"`
	Subtype    string   `json:"subtype"`
	DurationMS *float64 `json:"duration_ms"`
	IsError    *bool    `json:"is_error"`
	NumTurns   *float64 `json:"num_turns"`
	Result     *string  `json:"result"`
	Error      *string  `json:"error"`
	SessionID  string   `json:"session_id"`
}

func (a *Adapter) executable() string {
	if a.Executable != "" {
		return a.Executable
	}
	return "amp"
}

func processKey(task controllers.AdapterTask) *sandboxdv1.ProcessKey {
	return &sandboxdv1.ProcessKey{OwnerId: task.ID, Role: processRole}
}

func (a *Adapter) processSpec(task controllers.AdapterTask) *sandboxdv1.ProcessSpec {
	return &sandboxdv1.ProcessSpec{
		// The pinned CLI declares --execute's message as an optional option
		// argument. Its separate-argv form treats a leading flag as another
		// option, so assignment form is required to preserve arbitrary prompts.
		Argv:    []string{a.executable(), "--stream-json", "--execute=" + task.Prompt},
		Env:     map[string]string{"AMP_SKIP_UPDATE_CHECK": "1"},
		EnvMode: sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT,
	}
}

// EnsureAccepted duplicate-safely starts (or recovers) this Run-keyed Amp
// process in the current sandbox epoch.
func (a *Adapter) EnsureAccepted(ctx context.Context, task controllers.AdapterTask, sandbox controllers.AdapterSandbox) error {
	client, closeConnection, err := sandbox.DialProcess(ctx)
	if err != nil {
		return err
	}
	defer closeConnection()
	_, err = client.Start(ctx, &sandboxdv1.StartProcessRequest{Key: processKey(task), Spec: a.processSpec(task)})
	return err
}

// Observe forwards opaque process output and combines sandboxd's terminal
// process state with Amp's documented final stream JSON result record.
func (a *Adapter) Observe(ctx context.Context, task controllers.AdapterTask, sandbox controllers.AdapterSandbox) (controllers.AdapterObservation, string, error) {
	client, closeConnection, err := sandbox.DialProcess(ctx)
	if err != nil {
		return "", "", err
	}
	defer closeConnection()
	process, err := client.Get(ctx, &sandboxdv1.GetProcessRequest{Key: processKey(task)})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return controllers.AdapterObservationFailed, "Amp execution is absent in the current sandbox epoch", nil
		}
		return "", "", err
	}
	if process.ExecutionId == "" {
		return controllers.AdapterObservationFailed, "Amp process record is missing its execution ID", nil
	}
	if err := a.output.Forward(ctx, client, processKey(task), sandbox, process, "amp", "amp.process-output"); err != nil {
		if errors.Is(err, controllers.ErrAdapterEventRejected) {
			return controllers.AdapterObservationFailed, "Amp transcript output was permanently rejected", nil
		}
		return "", "", err
	}

	switch process.State {
	case sandboxdv1.ProcessState_PROCESS_STATE_RUNNING, sandboxdv1.ProcessState_PROCESS_STATE_STOPPING:
		return controllers.AdapterObservationRunning, "Amp is running", nil
	case sandboxdv1.ProcessState_PROCESS_STATE_FAILED:
		return controllers.AdapterObservationFailed, processMessage("Amp failed to start", process.Error), nil
	case sandboxdv1.ProcessState_PROCESS_STATE_EXITED:
		if process.ExitCode == nil {
			return controllers.AdapterObservationFailed, "Amp exited without an exit code", nil
		}
		if process.Reason != sandboxdv1.TerminationReason_TERMINATION_REASON_EXITED {
			return controllers.AdapterObservationFailed, fmt.Sprintf("Amp process ended with termination reason %s", process.Reason), nil
		}
		if process.GetExitCode() != 0 {
			return controllers.AdapterObservationFailed, fmt.Sprintf("Amp exited with code %d", process.GetExitCode()), nil
		}
		output, err := processoutput.ReadRetained(ctx, client, processKey(task), process.ExecutionId, sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT)
		if err != nil {
			return "", "", err
		}
		result, err := terminalResult(output)
		if err != nil {
			return controllers.AdapterObservationFailed, "Amp exited without a valid terminal result record: " + err.Error(), nil
		}
		if result.Subtype != "success" {
			return controllers.AdapterObservationFailed, processMessage("Amp reported "+result.Subtype, *result.Error), nil
		}
		return controllers.AdapterObservationSucceeded, processMessage("Amp completed", *result.Result), nil
	default:
		return controllers.AdapterObservationFailed, fmt.Sprintf("Amp returned invalid process state %s", process.State), nil
	}
}

// Cancel idempotently stops only this Run UID's managed process tree. Amp's
// public contract does not guarantee that abrupt local termination also stops
// already-dispatched remote work.
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
		err := a.output.Forward(ctx, client, processKey(task), sandbox, process, "amp", "amp.process-output")
		if errors.Is(err, controllers.ErrAdapterEventRejected) {
			return nil
		}
		return err
	default:
		return fmt.Errorf("Amp cancellation returned invalid process state %s", process.State)
	}
}

func terminalResult(output []byte) (resultEvent, error) {
	var line []byte
	lines := bytes.Split(output, []byte("\n"))
	for index := len(lines) - 1; index >= 0; index-- {
		if candidate := bytes.TrimSpace(lines[index]); len(candidate) != 0 {
			line = candidate
			break
		}
	}
	if len(line) == 0 {
		return resultEvent{}, errors.New("stdout has no records")
	}

	var result resultEvent
	if err := json.Unmarshal(line, &result); err != nil {
		return resultEvent{}, errors.New("last stdout record is malformed JSON")
	}
	if result.Type != "result" {
		return resultEvent{}, fmt.Errorf("last stdout record has type %q, not result", result.Type)
	}
	if result.DurationMS == nil || *result.DurationMS < 0 {
		return resultEvent{}, errors.New("result is missing a valid duration_ms")
	}
	if result.NumTurns == nil || *result.NumTurns < 0 || math.Trunc(*result.NumTurns) != *result.NumTurns {
		return resultEvent{}, errors.New("result is missing a valid num_turns")
	}
	if result.SessionID == "" {
		return resultEvent{}, errors.New("result is missing session_id")
	}
	if result.IsError == nil {
		return resultEvent{}, errors.New("result is missing is_error")
	}

	switch result.Subtype {
	case "success":
		if *result.IsError || result.Result == nil {
			return resultEvent{}, errors.New("success result has inconsistent is_error or result")
		}
	case "error_during_execution", "error_max_turns":
		if !*result.IsError || result.Error == nil {
			return resultEvent{}, errors.New("error result has inconsistent is_error or error")
		}
	default:
		return resultEvent{}, fmt.Errorf("result has unsupported subtype %q", result.Subtype)
	}
	return result, nil
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
