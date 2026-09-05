package controllers

import (
	"context"
	"errors"
	"testing"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type transcriptCleanupFunc func(context.Context, string, string, string, string) error

func (f transcriptCleanupFunc) Delete(ctx context.Context, ns, nsUID, run, uid string) error {
	return f(ctx, ns, nsUID, run, uid)
}

func TestTranscriptCleanupRetriesAfterClaimRelease(t *testing.T) {
	now := metav1.Now()
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-shared", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns", UID: "namespace-uid"}})
	r.TranscriptsDisabled = false
	calls := 0
	r.TranscriptCleanup = transcriptCleanupFunc(func(ctx context.Context, ns, nsUID, name, uid string) error {
		calls++
		if ns != "ns" || nsUID != "namespace-uid" || name != "r" || uid != "run-uid" {
			t.Fatalf("wrong cleanup identity: %s/%s/%s/%s", ns, nsUID, name, uid)
		}
		var current platformv1alpha1.Environment
		if err := r.Get(ctx, client.ObjectKeyFromObject(env), &current); err != nil || current.Status.ClaimedBy != nil || adapter.cancelled != 1 {
			t.Fatalf("cleanup before cancellation and claim release: %v", err)
		}
		if calls == 1 {
			return errors.New("uncertain commit")
		}
		return nil
	})
	if _, err := r.finalize(context.Background(), run); err == nil {
		t.Fatal("uncertainty removed finalizer")
	}
	var pending platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &pending); err != nil || len(pending.Finalizers) == 0 {
		t.Fatal("retry authority lost")
	}
	// Simulate a restart: the exact pending Run, not an in-memory success marker,
	// drives the retry even though its reusable Environment claim is gone.
	if _, err := r.finalize(context.Background(), &pending); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("cleanup attempts = %d", calls)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &pending); !apierrors.IsNotFound(err) {
		t.Fatalf("Run not deleted: %v", err)
	}
}

func TestTranscriptCleanupMissingConfigurationAndNamespaceFailClosed(t *testing.T) {
	for _, missingClient := range []bool{true, false} {
		now := metav1.Now()
		run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}}
		r := reconciler(t, &scriptedAdapter{}, run)
		r.TranscriptsDisabled = false
		if !missingClient {
			r.TranscriptCleanup = transcriptCleanupFunc(func(context.Context, string, string, string, string) error {
				t.Fatal("cleanup without Namespace proof")
				return nil
			})
		}
		if _, err := r.finalize(context.Background(), run); err == nil {
			t.Fatal("missing authority accepted")
		}
		var pending platformv1alpha1.Run
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &pending); err != nil || len(pending.Finalizers) == 0 {
			t.Fatal("finalizer lost")
		}
	}
}

func TestTerminalTranscriptIsRetained(t *testing.T) {
	for _, state := range []platformv1alpha1.RunState{platformv1alpha1.RunStateSucceeded, platformv1alpha1.RunStateFailed, platformv1alpha1.RunStateCancelled} {
		run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "default", UID: "uid", Finalizers: []string{runFinalizer}}, Status: platformv1alpha1.RunStatus{State: state}}
		r := reconciler(t, &scriptedAdapter{}, run)
		r.TranscriptsDisabled = false
		r.TranscriptCleanup = transcriptCleanupFunc(func(context.Context, string, string, string, string) error {
			t.Fatal("terminal state purged transcript")
			return nil
		})
		if _, err := r.cleanupTerminal(context.Background(), run); err != nil {
			t.Fatal(err)
		}
	}
}
