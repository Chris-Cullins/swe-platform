// Package agent defines the adapter contract between agent integrations and the platform.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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

// StatusMessage returns the fixed platform-owned message for a normalized
// observation. Adapter- and provider-controlled details belong only in opaque
// transcript events and must never be included in normalized status.
func (o AdapterObservation) StatusMessage() string {
	switch o {
	case AdapterObservationAccepted:
		return "adapter accepted the task"
	case AdapterObservationRunning:
		return "adapter is running"
	case AdapterObservationNeedsInput:
		return "adapter needs input"
	case AdapterObservationSucceeded:
		return "adapter completed successfully"
	case AdapterObservationFailed:
		return "adapter reported failure"
	default:
		return ""
	}
}

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

// AdapterLaunchMaterial is ephemeral, write-only material supplied only when
// starting an adapter process. RepositorySecretEnv is provider-neutral; its
// values must never be copied into the public ProcessSpec environment.
type AdapterLaunchMaterial struct {
	AgentCredential     *AdapterCredential
	RepositorySecretEnv map[string][]byte
}

// PrepareLaunchMaterial validates and defensively copies launch secrets for an
// adapter RPC. The returned cleanup must be called immediately after the RPC.
func PrepareLaunchMaterial(material *AdapterLaunchMaterial, apiEnvName string, supportsCredential bool) (*sandboxdv1.LaunchMaterial, func(), error) {
	if material == nil {
		return nil, func() {}, nil
	}
	credential := material.AgentCredential
	if credential != nil && !supportsCredential {
		return nil, func() {}, fmt.Errorf("%w: adapter does not support credential profiles", ErrAdapterTaskRejected)
	}
	if credential != nil && credential.Type != platformv1alpha1.AgentCredentialTypeAPIKey {
		return nil, func() {}, fmt.Errorf("%w: unsupported credential type %q", ErrAdapterTaskRejected, credential.Type)
	}
	if credential != nil {
		if _, exists := material.RepositorySecretEnv[apiEnvName]; exists {
			return nil, func() {}, fmt.Errorf("%w: duplicate adapter API secret environment name", ErrAdapterTaskRejected)
		}
	}
	secretEnv := make(map[string][]byte, len(material.RepositorySecretEnv)+1)
	for name, value := range material.RepositorySecretEnv {
		secretEnv[name] = append([]byte(nil), value...)
	}
	if credential != nil {
		secretEnv[apiEnvName] = append([]byte(nil), credential.APIKey...)
	}
	cleanup := func() {
		for _, value := range secretEnv {
			clear(value)
		}
	}
	return &sandboxdv1.LaunchMaterial{SecretEnv: secretEnv}, cleanup, nil
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

// EnvironmentUID is the immutable platform Environment identity. Its string
// representation keeps the adapter contract independent of any backend API.
type EnvironmentUID string

// AdapterSandbox is the backend-neutral handle exposed to adapters. Adapters
// use sandboxd and never inspect pods, containers, VMs, PIDs, or OS signals.
type AdapterSandbox struct {
	EnvironmentName string
	EnvironmentUID  EnvironmentUID
	DialProcess     func(context.Context) (sandboxdv1.ProcessServiceClient, func() error, error)
	EmitEvent       func(context.Context, AdapterEvent) error
}

// AdapterLifecycle translates one agent's execution model into normalized Run
// lifecycle events. Every operation must be idempotent. EnsureAccepted may be
// repeated after an uncertain response or environment resume and may wrap
// ErrAdapterTaskRejected for permanent intent rejection; Cancel succeeds when
// work is already absent or terminal and returns
// ErrAdapterCancellationPending while its execution tree is still stopping.
// Observe's message must equal observation.StatusMessage(); arbitrary process,
// provider, result, or transcript bytes are forbidden from normalized status.
type AdapterLifecycle interface {
	EnsureAccepted(context.Context, AdapterTask, AdapterSandbox, *AdapterLaunchMaterial) error
	Observe(context.Context, AdapterTask, AdapterSandbox) (AdapterObservation, string, error)
	Cancel(context.Context, AdapterTask, AdapterSandbox) error
}

// AdapterCredentialPolicy is an optional adapter capability. Adapters that
// return false reject credential profiles before any profile or Secret read.
type AdapterCredentialPolicy interface {
	SupportsCredentialProfiles() bool
}
