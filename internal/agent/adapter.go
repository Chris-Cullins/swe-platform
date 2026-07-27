// Package agent defines the adapter contract between agent integrations and the platform.
package agent

import (
	"context"
	"encoding/json"
	"errors"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

// ErrAdapterCancellationPending means cancellation was accepted but the
// adapter-owned execution tree has not reached a terminal state yet.
var ErrAdapterCancellationPending = errors.New("adapter cancellation is pending")

// ErrAdapterEventRejected means the transcript transport permanently rejected
// an adapter event. Retrying the same event cannot make progress.
var ErrAdapterEventRejected = errors.New("adapter event permanently rejected")

// ErrAdapterTaskRejected means an adapter permanently rejected the immutable
// task intent. Retrying acceptance cannot make progress.
var ErrAdapterTaskRejected = errors.New("adapter task permanently rejected")

// AdapterObservation is the adapter-neutral state observed for accepted work.
type AdapterObservation string

const (
	AdapterObservationAccepted   AdapterObservation = "Accepted"
	AdapterObservationRunning    AdapterObservation = "Running"
	AdapterObservationNeedsInput AdapterObservation = "NeedsInput"
	AdapterObservationSucceeded  AdapterObservation = "Succeeded"
	AdapterObservationFailed     AdapterObservation = "Failed"
)

// AdapterTask contains immutable task identity and input. ID is the Run UID
// and is the adapter's idempotency key across retries and controller restarts.
type AdapterTask struct {
	ID     string
	Prompt string
}

// AdapterCredential is ephemeral launch-only credential material. Callers must
// not retain APIKey after EnsureAccepted returns.
type AdapterCredential struct {
	Type   platformv1alpha1.AgentCredentialType
	APIKey []byte
}

// AdapterEvent is an adapter-owned transcript event carried by the platform's
// generic transcript transport. Data is opaque to the controller.
type AdapterEvent struct {
	Source         string
	IdempotencyKey string
	Type           string
	Data           json.RawMessage
}

// AdapterEventSink forwards opaque adapter events for one namespaced Run. The
// runUID is the exact reconciled immutable Run UID; the control plane fences
// append identity against it so a delete/recreate replacement never receives
// stale events. Permanent rejection wraps ErrAdapterEventRejected; other
// errors are retryable.
type AdapterEventSink interface {
	Append(context.Context, string, string, string, AdapterEvent) error
}

// AdapterSandbox is the backend-neutral handle exposed to adapters. Adapters
// use sandboxd and never inspect pods, containers, VMs, PIDs, or OS signals.
type AdapterSandbox struct {
	EnvironmentName string
	EnvironmentUID  string
	DialProcess     func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error)
	EmitEvent       func(context.Context, AdapterEvent) error
}

// AdapterLifecycle translates one agent's execution model into normalized Run
// lifecycle events. Every operation must be idempotent. EnsureAccepted may be
// repeated after an uncertain response or environment resume and may wrap
// ErrAdapterTaskRejected for permanent intent rejection; Cancel succeeds when
// work is already absent or terminal and returns
// ErrAdapterCancellationPending while its execution tree is still stopping.
type AdapterLifecycle interface {
	EnsureAccepted(context.Context, AdapterTask, AdapterSandbox, *AdapterCredential) error
	Observe(context.Context, AdapterTask, AdapterSandbox) (AdapterObservation, string, error)
	Cancel(context.Context, AdapterTask, AdapterSandbox) error
}

// AdapterCredentialPolicy is an optional adapter capability. Adapters that
// return false reject credential profiles before any profile or Secret read.
type AdapterCredentialPolicy interface {
	SupportsCredentialProfiles() bool
}
