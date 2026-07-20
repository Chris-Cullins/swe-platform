// Package claudecode implements the Claude Code foreground-process adapter.
package claudecode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Chris-Cullins/swe-platform/internal/adapters/processoutput"
	"github.com/Chris-Cullins/swe-platform/internal/controllers"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const processRole = "agent"

// Adapter drives one non-interactive Claude Code process per Run UID.
type Adapter struct {
	Executable string

	output processoutput.Forwarder
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
func (a *Adapter) EnsureAccepted(ctx context.Context, task controllers.AdapterTask, sandbox controllers.AdapterSandbox) error {
	client, closeConnection, err := sandbox.DialProcess(ctx)
	if err != nil {
		return err
	}
	defer closeConnection()
	_, err = client.Start(ctx, &sandboxdv1.StartProcessRequest{Key: processKey(task), Spec: a.processSpec(task)})
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
	if err := a.output.Forward(ctx, client, processKey(task), sandbox, process, "claude-code", "claude-code.process-output"); err != nil {
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
		output, err := processoutput.ReadRetained(ctx, client, processKey(task), process.ExecutionId, sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT)
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
		err := a.output.Forward(ctx, client, processKey(task), sandbox, process, "claude-code", "claude-code.process-output")
		if errors.Is(err, controllers.ErrAdapterEventRejected) {
			return nil
		}
		return err
	default:
		return fmt.Errorf("Claude Code cancellation returned invalid process state %s", process.State)
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
