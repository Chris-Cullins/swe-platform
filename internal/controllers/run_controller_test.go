package controllers

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
)

type scriptedAdapter struct {
	observations          []AdapterObservation
	accepted, observed    int
	cancelled             int
	acceptErr             error
	onCancel              func()
	cancelErr             error
	acceptedCredentials   [][]byte
	retainedCredentialKey []byte
}

type failAcceptedStatusClient struct {
	client.Client
	fail bool
}

func (c *failAcceptedStatusClient) Status() client.SubResourceWriter {
	return &failAcceptedStatusWriter{SubResourceWriter: c.Client.Status(), client: c}
}

type failAcceptedStatusWriter struct {
	client.SubResourceWriter
	client *failAcceptedStatusClient
}

func (w *failAcceptedStatusWriter) Update(ctx context.Context, object client.Object, opts ...client.SubResourceUpdateOption) error {
	if run, ok := object.(*platformv1alpha1.Run); ok && run.Status.State == platformv1alpha1.RunStateAdapterAccepted && w.client.fail {
		w.client.fail = false
		return errors.New("simulated lost acceptance status update")
	}
	return w.SubResourceWriter.Update(ctx, object, opts...)
}

// foregroundAdapter models a CLI agent whose managed process exit is the task
// outcome.
type foregroundAdapter struct{ process AdapterObservation }

func (*foregroundAdapter) EnsureAccepted(context.Context, AdapterTask, AdapterSandbox, *AdapterCredential) error {
	return nil
}
func (a *foregroundAdapter) Observe(context.Context, AdapterTask, AdapterSandbox) (AdapterObservation, string, error) {
	return a.process, "managed process state", nil
}
func (*foregroundAdapter) Cancel(context.Context, AdapterTask, AdapterSandbox) error { return nil }

// serviceAdapter models a long-lived agent service: task events change while
// the service process remains running, so service exit is not task completion.
type serviceAdapter struct {
	serviceRunning bool
	event          AdapterObservation
}

func (a *serviceAdapter) EnsureAccepted(context.Context, AdapterTask, AdapterSandbox, *AdapterCredential) error {
	a.serviceRunning = true
	return nil
}
func (a *serviceAdapter) Observe(context.Context, AdapterTask, AdapterSandbox) (AdapterObservation, string, error) {
	return a.event, "service task event", nil
}
func (a *serviceAdapter) Cancel(context.Context, AdapterTask, AdapterSandbox) error {
	a.serviceRunning = false
	return nil
}

func (a *scriptedAdapter) EnsureAccepted(_ context.Context, _ AdapterTask, _ AdapterSandbox, credential *AdapterCredential) error {
	a.accepted++
	if credential == nil {
		a.acceptedCredentials = append(a.acceptedCredentials, nil)
	} else {
		a.acceptedCredentials = append(a.acceptedCredentials, append([]byte(nil), credential.APIKey...))
		a.retainedCredentialKey = credential.APIKey
	}
	return a.acceptErr
}
func (a *scriptedAdapter) Cancel(context.Context, AdapterTask, AdapterSandbox) error {
	a.cancelled++
	if a.onCancel != nil {
		a.onCancel()
	}
	return a.cancelErr
}
func (a *scriptedAdapter) Observe(context.Context, AdapterTask, AdapterSandbox) (AdapterObservation, string, error) {
	a.observed++
	o := a.observations[0]
	if len(a.observations) > 1 {
		a.observations = a.observations[1:]
	}
	return o, string(o), nil
}

func runScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func reconciler(t *testing.T, adapter AdapterLifecycle, objects ...client.Object) *RunReconciler {
	t.Helper()
	s := runScheme(t)
	for _, object := range objects {
		env, ok := object.(*platformv1alpha1.Environment)
		if ok && (env.Status.Phase == platformv1alpha1.EnvironmentPhaseReady || env.Status.Phase == platformv1alpha1.EnvironmentPhaseRunning) {
			applyEnvironmentStatus(env, env.Status.Phase, env.Status.PodName, env.Status.Endpoints.Sandboxd, "SandboxdReady", "sandboxd is ready", env.Status.LastActiveAt)
		}
	}
	// Most lifecycle tests predate the ownership/endpoint security fences. Give
	// their intentionally valid fixtures the exact current Run owner and a
	// reachable endpoint; mismatch tests construct their reconciler directly.
	for _, object := range objects {
		run, ok := object.(*platformv1alpha1.Run)
		if !ok || run.Status.EnvironmentRef == nil || run.Status.EnvironmentRef.Ownership != platformv1alpha1.EnvironmentOwnershipOwned {
			continue
		}
		for _, candidate := range objects {
			env, ok := candidate.(*platformv1alpha1.Environment)
			if !ok || env.Name != run.Status.EnvironmentRef.Name || env.UID != run.Status.EnvironmentRef.UID {
				continue
			}
			env.OwnerReferences = []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true)}}
			if env.Status.Phase == platformv1alpha1.EnvironmentPhaseReady || env.Status.Phase == platformv1alpha1.EnvironmentPhaseRunning {
				env.Status.PodName = "env-" + env.Name
				env.Status.Endpoints.Sandboxd = "10.0.0.1:50051"
			}
		}
	}
	c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&platformv1alpha1.Run{}, &platformv1alpha1.Environment{}).WithObjects(objects...).Build()
	return &RunReconciler{Client: c, Scheme: s, Adapters: map[string]AdapterLifecycle{"test": adapter}}
}

func credentialProfileAndSecret(run *platformv1alpha1.Run, value []byte) (*platformv1alpha1.AgentCredentialProfile, *corev1.Secret) {
	profile := &platformv1alpha1.AgentCredentialProfile{
		ObjectMeta: metav1.ObjectMeta{Name: run.Spec.CredentialProfileRef, Namespace: run.Namespace, UID: "profile-uid"},
		Spec: platformv1alpha1.AgentCredentialProfileSpec{
			Adapter: run.Spec.Agent, CredentialType: platformv1alpha1.AgentCredentialTypeAPIKey,
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      platformv1alpha1.AgentCredentialSecretName(profile.UID),
			Namespace: profile.Namespace,
			UID:       "secret-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "AgentCredentialProfile",
				Name: profile.Name, UID: profile.UID, Controller: ptr(true), BlockOwnerDeletion: ptr(true),
			}},
		},
		Type: platformv1alpha1.AgentCredentialAPIKeySecretType,
		Data: map[string][]byte{platformv1alpha1.AgentCredentialAPIKeySecretKey: append([]byte(nil), value...)},
	}
	return profile, secret
}

func reconcileRun(t *testing.T, r *RunReconciler, name string) platformv1alpha1.Run {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: name}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: name}, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestAllocationIsDeterministicAndRecoversBeforeStatus(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test"}}
	// This models a successful create followed by a lost Run status update.
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "run-run-uid", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: "r", UID: run.UID, Controller: ptr(true)}}}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "small"}}
	r := reconciler(t, &scriptedAdapter{}, run, env)
	for range 3 {
		reconcileRun(t, r, run.Name)
	}
	var environments platformv1alpha1.EnvironmentList
	if err := r.List(context.Background(), &environments, client.InNamespace("ns")); err != nil {
		t.Fatal(err)
	}
	if len(environments.Items) != 1 {
		t.Fatalf("environments = %d, want 1", len(environments.Items))
	}
	got := reconcileRun(t, r, run.Name)
	if got.Status.EnvironmentRef == nil || got.Status.EnvironmentRef.Name != "run-run-uid" || got.Status.EnvironmentRef.UID != "env-uid" {
		t.Fatalf("reference = %#v", got.Status.EnvironmentRef)
	}
	if !metav1.IsControlledBy(&environments.Items[0], run) {
		t.Fatal("environment lacks Run controller owner")
	}
}

func TestRepeatedAllocationCreatesOneOwnedEnvironment(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test"}}
	r := reconciler(t, &scriptedAdapter{}, run)
	for range 2 {
		ref, err := r.allocateEnvironment(context.Background(), run)
		if err != nil {
			t.Fatal(err)
		}
		if ref.Name != "run-run-uid" {
			t.Fatalf("name = %q", ref.Name)
		}
	}
	var environments platformv1alpha1.EnvironmentList
	if err := r.List(context.Background(), &environments); err != nil {
		t.Fatal(err)
	}
	if len(environments.Items) != 1 || !metav1.IsControlledBy(&environments.Items[0], run) {
		t.Fatalf("environments = %#v", environments.Items)
	}
}

func TestRunClaimsAndRecoversWarmEnvironment(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", ProjectRef: "project", Agent: "test"}}
	warm := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "warm-small-1",
			Namespace: "ns",
			UID:       "warm-uid",
			Labels:    map[string]string{warmPoolLabel: "small"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "EnvironmentTemplate", Name: "small", UID: "template-uid", Controller: ptr(true),
			}},
		},
		Spec:   platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
		Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
	}
	r := reconciler(t, &scriptedAdapter{}, run, warm)
	ref, err := r.allocateEnvironment(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Name != warm.Name || ref.UID != warm.UID || ref.Ownership != platformv1alpha1.EnvironmentOwnershipClaimed {
		t.Fatalf("reference = %#v", ref)
	}
	var claimed platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(warm), &claimed); err != nil {
		t.Fatal(err)
	}
	if claimed.Status.ClaimedBy == nil || claimed.Status.ClaimedBy.UID != run.UID || claimed.Status.LastActiveAt == nil || claimed.Status.Phase != platformv1alpha1.EnvironmentPhaseSetup || claimed.Status.PodName != "" || claimed.Status.Endpoints.Sandboxd != "" {
		t.Fatalf("claim status = %#v", claimed.Status)
	}
	if claimed.Spec.ProjectRef != run.Spec.ProjectRef || claimed.Labels[warmPoolLabel] != "" || metav1.GetControllerOf(&claimed) != nil {
		t.Fatalf("promoted environment = %#v", claimed)
	}
	recovered, err := r.recoverEnvironmentReference(context.Background(), run)
	if err != nil || recovered == nil || recovered.UID != warm.UID || recovered.Ownership != platformv1alpha1.EnvironmentOwnershipClaimed {
		t.Fatalf("recovered = %#v, error = %v", recovered, err)
	}
}

func TestWarmPromotionWithdrawsReadinessBeforeAdapterAcceptance(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", ProjectRef: "project", Agent: "test"}}
	warm := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "warm-small-1", Namespace: "ns", UID: "warm-uid", Labels: map[string]string{warmPoolLabel: "small"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "EnvironmentTemplate", Name: "small", UID: "template-uid", Controller: ptr(true)}},
		},
		Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
		Status: platformv1alpha1.EnvironmentStatus{
			Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-warm-small-1", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		},
	}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, warm)
	got := reconcileRun(t, r, run.Name)
	if got.Status.State != platformv1alpha1.RunStateAllocating || got.Status.EnvironmentRef == nil {
		t.Fatalf("Run status = %#v, want allocated but not ready", got.Status)
	}
	got = reconcileRun(t, r, run.Name)
	if got.Status.State != platformv1alpha1.RunStateAllocating || adapter.accepted != 0 {
		t.Fatalf("state = %s, adapter accepts = %d", got.Status.State, adapter.accepted)
	}
}

func TestClaimsAreExclusiveUIDFencedAndReleasedSafely(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "new"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: "shared", Agent: "test"}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env"}, Status: platformv1alpha1.EnvironmentStatus{ClaimedBy: &platformv1alpha1.RunReference{Name: "r", UID: "old"}}}
	r := reconciler(t, &scriptedAdapter{}, run, env)
	if _, err := r.allocateEnvironment(context.Background(), run); err == nil {
		t.Fatal("stale same-name claim was accepted")
	}
	var stored platformv1alpha1.Environment
	_ = r.Get(context.Background(), client.ObjectKeyFromObject(env), &stored)
	if err := r.releaseClaim(context.Background(), run, &stored); err != nil {
		t.Fatal(err)
	}
	_ = r.Get(context.Background(), client.ObjectKeyFromObject(env), &stored)
	if stored.Status.ClaimedBy == nil || stored.Status.ClaimedBy.UID != "old" {
		t.Fatal("non-matching claim was released")
	}
	stored.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: "r", UID: "new"}
	if err := r.Status().Update(context.Background(), &stored); err != nil {
		t.Fatal(err)
	}
	if err := r.releaseClaim(context.Background(), run, &stored); err != nil {
		t.Fatal(err)
	}
	_ = r.Get(context.Background(), client.ObjectKeyFromObject(env), &stored)
	if stored.Status.ClaimedBy != nil {
		t.Fatal("matching claim was not released")
	}
}

func TestConcurrentClaimHasOneWinner(t *testing.T) {
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env"}}
	runA := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns", UID: "a-uid"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: env.Name, Agent: "test"}}
	runB := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns", UID: "b-uid"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: env.Name, Agent: "test"}}
	r := reconciler(t, &scriptedAdapter{}, env, runA, runB)
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, run := range []*platformv1alpha1.Run{runA, runB} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := r.allocateEnvironment(context.Background(), run)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful claims = %d, want 1", successes)
	}
	var stored platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.ClaimedBy == nil || (stored.Status.ClaimedBy.UID != runA.UID && stored.Status.ClaimedBy.UID != runB.UID) {
		t.Fatalf("claim = %#v", stored.Status.ClaimedBy)
	}
}

func TestRunEnvironmentWatchIgnoresOnlyActivityUpdates(t *testing.T) {
	oldEnvironment := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
	activity := oldEnvironment.DeepCopy()
	now := metav1.Now()
	activity.Status.LastActiveAt = &now
	if runRelevantEnvironmentUpdate(oldEnvironment, activity) {
		t.Fatal("lastActiveAt-only update would feed back into Run reconciliation")
	}
	activityIntent := activity.DeepCopy()
	activityIntent.Generation++
	activityIntent.Spec.Lifecycle.Activity = []platformv1alpha1.EnvironmentActivityRequest{{
		Source:                      platformv1alpha1.EnvironmentActivitySourceTerminal,
		EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "terminal-1", EnvironmentUID: activityIntent.UID},
	}}
	if runRelevantEnvironmentUpdate(activity, activityIntent) {
		t.Fatal("activity-intent generation would feed back into Run reconciliation")
	}
	activityReceipt := activityIntent.DeepCopy()
	activityReceipt.Status.Lifecycle.ActivityReceipts = []platformv1alpha1.EnvironmentActivityReceipt{{Source: platformv1alpha1.EnvironmentActivitySourceTerminal, RequestID: "terminal-1"}}
	activityReceipt.Status.LastActiveAt = &now
	if runRelevantEnvironmentUpdate(activityIntent, activityReceipt) {
		t.Fatal("activity receipt would feed back into Run reconciliation")
	}
	activityMetadata := activity.DeepCopy()
	activityMetadata.Annotations = map[string]string{"lifecycle.swe.dev/activity-terminal": `{"source":"Terminal"}`}
	if runRelevantEnvironmentUpdate(activity, activityMetadata) {
		t.Fatal("metadata activity intent would feed back into Run reconciliation")
	}
	claim := activity.DeepCopy()
	claim.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: "r", UID: "run-uid"}
	if !runRelevantEnvironmentUpdate(activity, claim) {
		t.Fatal("claim update was filtered from Run reconciliation")
	}
	recovery := activity.DeepCopy()
	recovery.Status.PodRecoveryAttempts = 1
	if !runRelevantEnvironmentUpdate(activity, recovery) {
		t.Fatal("pod recovery update was filtered from Run reconciliation")
	}
	spec := activity.DeepCopy()
	spec.Generation++
	spec.Spec.Paused = true
	if !runRelevantEnvironmentUpdate(activity, spec) {
		t.Fatal("spec update was filtered from Run reconciliation")
	}
}

func TestAcceptedRunIgnoresActivityWithoutRereadingCredentials(t *testing.T) {
	for _, test := range []struct {
		name        string
		state       platformv1alpha1.RunState
		observation AdapterObservation
	}{
		{name: "running", state: platformv1alpha1.RunStateRunning, observation: AdapterObservationRunning},
		{name: "needs input", state: platformv1alpha1.RunStateNeedsInput, observation: AdapterObservationNeedsInput},
	} {
		t.Run(test.name, func(t *testing.T) {
			acceptedEpoch := int64(0)
			run := &platformv1alpha1.Run{
				ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}},
				Spec:       platformv1alpha1.RunSpec{Agent: "test", CredentialProfileRef: "removed-profile"},
				Status: platformv1alpha1.RunStatus{
					State:                    test.state,
					AcceptedEnvironmentEpoch: &acceptedEpoch,
					EnvironmentRef:           &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
					CredentialProfileRef:     &platformv1alpha1.RunCredentialProfileReference{Name: "removed-profile", UID: "removed-profile-uid"},
					Conditions:               []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue, Reason: "AdapterAccepted"}},
				},
			}
			environment := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "env-uid", Generation: 1}}
			applyEnvironmentStatus(environment, platformv1alpha1.EnvironmentPhaseReady, "env-e", "10.0.0.1:50051", "SandboxdReady", "ready", nil)
			adapter := &scriptedAdapter{observations: []AdapterObservation{test.observation}}
			r := reconciler(t, adapter, run, environment)
			credentialReads := 0
			r.APIReader = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				credentialReads++
				return apierrors.NewNotFound(platformv1alpha1.GroupVersion.WithResource("agentcredentialprofiles").GroupResource(), "removed-profile")
			}})

			if err := lifecycle.RecordActivity(context.Background(), r.Client, client.ObjectKeyFromObject(environment), environment.UID, 0, platformv1alpha1.EnvironmentActivitySourceTerminal, "terminal-1"); err != nil {
				t.Fatal(err)
			}
			var activity platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(environment), &activity); err != nil {
				t.Fatal(err)
			}
			if activity.Generation != environment.Generation || activity.Status.ObservedGeneration != activity.Generation || !platformv1alpha1.IsEnvironmentReady(&activity) {
				t.Fatalf("activity exposed stale environment status: generation=%d status=%#v", activity.Generation, activity.Status)
			}
			// A pre-migration publisher can still advance generation through the
			// legacy spec slot. Accepted work must remain stable while the
			// Environment controller consumes and converges that generation.
			activity.Generation++
			activity.Spec.Lifecycle.Activity = []platformv1alpha1.EnvironmentActivityRequest{{
				Source:                      platformv1alpha1.EnvironmentActivitySourceTerminal,
				EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "legacy-terminal-1", EnvironmentUID: activity.UID},
			}}
			if err := r.Update(context.Background(), &activity); err != nil {
				t.Fatal(err)
			}

			got := reconcileRun(t, r, run.Name)
			if got.Status.State != test.state || got.Status.AcceptedEnvironmentEpoch == nil || *got.Status.AcceptedEnvironmentEpoch != 0 || adapter.accepted != 0 || credentialReads != 0 {
				t.Fatalf("activity reconcile = state %s, acceptances %d, credential reads %d", got.Status.State, adapter.accepted, credentialReads)
			}
		})
	}
}

func TestExplicitClaimContentionFailsPermanently(t *testing.T) {
	loser := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "loser", Namespace: "ns", UID: "loser-uid"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: "shared", Agent: "test"}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
		ClaimedBy: &platformv1alpha1.RunReference{Name: "winner", UID: "winner-uid"},
	}}
	r := reconciler(t, &scriptedAdapter{}, loser, env)
	reconcileRun(t, r, loser.Name) // finalizer
	failed := reconcileRun(t, r, loser.Name)
	if failed.Status.State != platformv1alpha1.RunStateFailed || failed.Status.EnvironmentRef != nil {
		t.Fatalf("contending Run status = %#v", failed.Status)
	}
	var released platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &released); err != nil {
		t.Fatal(err)
	}
	released.Status.ClaimedBy = nil
	if err := r.Status().Update(context.Background(), &released); err != nil {
		t.Fatal(err)
	}
	reconcileRun(t, r, loser.Name)
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &released); err != nil {
		t.Fatal(err)
	}
	if released.Status.ClaimedBy != nil {
		t.Fatalf("failed loser later claimed released Environment: %#v", released.Status.ClaimedBy)
	}
}

func TestCancelBeforeAllocationCreatesNothing(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", Cancel: true}}
	r := reconciler(t, &scriptedAdapter{}, run)
	got := reconcileRun(t, r, "r")
	var list platformv1alpha1.EnvironmentList
	_ = r.List(context.Background(), &list)
	if got.Status.State != platformv1alpha1.RunStateCancelled || len(list.Items) != 0 {
		t.Fatalf("state=%s environments=%d", got.Status.State, len(list.Items))
	}
}

func TestCancelRecoversAllocationCreatedBeforeStatus(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", Cancel: true}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "run-uid", Namespace: "ns", UID: "euid", OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true)}}}}
	r := reconciler(t, &scriptedAdapter{}, run, env)
	got := reconcileRun(t, r, run.Name)
	if got.Status.EnvironmentRef == nil || got.Status.EnvironmentRef.UID != env.UID || got.Status.State != platformv1alpha1.RunStateAllocating {
		t.Fatalf("status = %#v, want recovered allocation", got.Status)
	}
	got = reconcileRun(t, r, run.Name)
	if got.Status.State != platformv1alpha1.RunStateCancelled {
		t.Fatalf("state = %s, want Cancelled", got.Status.State)
	}
}

func TestTerminalCleanupPausesOwnedEnvironment(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid"}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateSucceeded, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "euid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "euid"}}
	a := &scriptedAdapter{}
	r := reconciler(t, a, run, env)
	reconcileRun(t, r, "r")
	var got platformv1alpha1.Environment
	_ = r.Get(context.Background(), client.ObjectKeyFromObject(env), &got)
	if got.Spec.Lifecycle.Suspend == nil || a.cancelled != 0 {
		t.Fatalf("suspend=%#v cancels=%d", got.Spec.Lifecycle.Suspend, a.cancelled)
	}
}

func TestTerminalClaimReleaseRemainsReleasedAcrossReconcileAndRestart(t *testing.T) {
	for _, state := range []platformv1alpha1.RunState{
		platformv1alpha1.RunStateSucceeded,
		platformv1alpha1.RunStateFailed,
		platformv1alpha1.RunStateCancelled,
	} {
		t.Run(string(state), func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Generation: 1}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
				State:          state,
				EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
				Conditions:     []metav1.Condition{{Type: runConditionEnvironmentReady, Status: metav1.ConditionTrue, Reason: "EnvironmentReady", Message: "sandboxd is ready", ObservedGeneration: 1}},
			}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
				Phase: platformv1alpha1.EnvironmentPhaseReady, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
			}}
			adapter := &scriptedAdapter{}
			r := reconciler(t, adapter, run, env)

			for range 2 {
				got := reconcileRun(t, r, run.Name)
				assertTerminalEnvironmentCondition(t, got, state, "EnvironmentReleased", "claimed environment was released")
				if got.Status.EnvironmentRef == nil || got.Status.EnvironmentRef.Name != env.Name || got.Status.EnvironmentRef.UID != env.UID || got.Status.EnvironmentRef.Ownership != platformv1alpha1.EnvironmentOwnershipClaimed {
					t.Fatalf("historical Environment reference = %#v", got.Status.EnvironmentRef)
				}
			}
			var released platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &released); err != nil || released.Status.ClaimedBy != nil || adapter.cancelled != 0 {
				t.Fatalf("released Environment = %#v, cancellations = %d, error = %v", released, adapter.cancelled, err)
			}

			if err := r.Delete(context.Background(), &released); err != nil {
				t.Fatal(err)
			}
			replacement := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: env.Name, Namespace: env.Namespace, UID: "replacement-uid"}, Status: platformv1alpha1.EnvironmentStatus{
				ClaimedBy: &platformv1alpha1.RunReference{Name: "other", UID: "other-run-uid"},
			}}
			if err := r.Create(context.Background(), replacement); err != nil {
				t.Fatal(err)
			}
			if err := r.Status().Update(context.Background(), replacement); err != nil {
				t.Fatal(err)
			}

			restarted := &RunReconciler{Client: r.Client, Scheme: r.Scheme, Adapters: map[string]AdapterLifecycle{"test": adapter}}
			got := reconcileRun(t, restarted, run.Name)
			assertTerminalEnvironmentCondition(t, got, state, "EnvironmentReleased", "claimed environment was released")
			var retainedReplacement platformv1alpha1.Environment
			if err := restarted.Get(context.Background(), client.ObjectKeyFromObject(replacement), &retainedReplacement); err != nil || retainedReplacement.UID != replacement.UID || retainedReplacement.Status.ClaimedBy == nil || retainedReplacement.Status.ClaimedBy.UID != "other-run-uid" {
				t.Fatalf("replacement Environment = %#v, error = %v", retainedReplacement, err)
			}
		})
	}
}

func TestTerminalClaimLossBeforeReleaseRemainsStrict(t *testing.T) {
	for _, tc := range []struct {
		name        string
		environment *platformv1alpha1.Environment
		wantMessage string
	}{
		{
			name:        "deleted",
			wantMessage: `environments.swe.dev "shared" not found`,
		},
		{
			name: "same-name UID replacement",
			environment: &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "replacement-uid"}, Status: platformv1alpha1.EnvironmentStatus{
				ClaimedBy: &platformv1alpha1.RunReference{Name: "other", UID: "other-run-uid"},
			}},
			wantMessage: `allocated environment is gone or no longer claimed by this run: environment "shared" was replaced (wanted UID env-uid, got replacement-uid)`,
		},
		{
			name: "unexpected claim mismatch",
			environment: &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
				ClaimedBy: &platformv1alpha1.RunReference{Name: "other", UID: "other-run-uid"},
			}},
			wantMessage: `allocated environment is gone or no longer claimed by this run: environment "shared" claim does not match run UID run-uid`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
				State:          platformv1alpha1.RunStateSucceeded,
				EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
				Conditions:     []metav1.Condition{{Type: runConditionEnvironmentReady, Status: metav1.ConditionTrue, Reason: "EnvironmentReady", Message: "sandboxd is ready"}},
			}}
			objects := []client.Object{run}
			if tc.environment != nil {
				objects = append(objects, tc.environment)
			}
			r := reconciler(t, &scriptedAdapter{}, objects...)
			got := reconcileRun(t, r, run.Name)
			assertTerminalEnvironmentCondition(t, got, platformv1alpha1.RunStateSucceeded, "EnvironmentLost", tc.wantMessage)
			if tc.environment != nil {
				var retained platformv1alpha1.Environment
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(tc.environment), &retained); err != nil || retained.Spec.Paused || retained.Status.ClaimedBy == nil || retained.Status.ClaimedBy.UID != "other-run-uid" {
					t.Fatalf("foreign Environment = %#v, error = %v", retained, err)
				}
			}
		})
	}
}

func TestOwnedTerminalCleanupAndActiveEnvironmentLossRemainStrict(t *testing.T) {
	t.Run("terminal owned Environment deletion", func(t *testing.T) {
		run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
			State:          platformv1alpha1.RunStateSucceeded,
			EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "owned", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		}}
		r := reconciler(t, &scriptedAdapter{}, run)
		got := reconcileRun(t, r, run.Name)
		assertTerminalEnvironmentCondition(t, got, platformv1alpha1.RunStateSucceeded, "EnvironmentLost", `environments.swe.dev "owned" not found`)
	})

	t.Run("terminal owned Environment fencing", func(t *testing.T) {
		run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
			State:          platformv1alpha1.RunStateSucceeded,
			EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "owned", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		}}
		env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{Paused: true}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhasePaused}}
		r := reconciler(t, &scriptedAdapter{}, run, env)
		got := reconcileRun(t, r, run.Name)
		assertTerminalEnvironmentCondition(t, got, platformv1alpha1.RunStateSucceeded, "EnvironmentFenced", "owned environment is paused and fenced")
	})

	t.Run("active claimed Environment deletion", func(t *testing.T) {
		run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
			State:          platformv1alpha1.RunStateRunning,
			EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
			Conditions:     []metav1.Condition{{Type: runConditionEnvironmentReady, Status: metav1.ConditionTrue}, {Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
		}}
		r := reconciler(t, &scriptedAdapter{}, run)
		got := reconcileRun(t, r, run.Name)
		assertTerminalEnvironmentCondition(t, got, platformv1alpha1.RunStateFailed, "EnvironmentLost", `environments.swe.dev "shared" not found`)
		got = reconcileRun(t, r, run.Name)
		assertTerminalEnvironmentCondition(t, got, platformv1alpha1.RunStateFailed, "EnvironmentLost", `environments.swe.dev "shared" not found`)
	})
}

func assertTerminalEnvironmentCondition(t *testing.T, run platformv1alpha1.Run, wantState platformv1alpha1.RunState, wantReason, wantMessage string) {
	t.Helper()
	condition := apiMeta.FindStatusCondition(run.Status.Conditions, runConditionEnvironmentReady)
	if run.Status.State != wantState || condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != wantReason || condition.Message != wantMessage {
		t.Fatalf("Run status = %#v, EnvironmentReady = %#v; want state %s, reason %q, message %q", run.Status, condition, wantState, wantReason, wantMessage)
	}
}

func TestAdapterShapesPauseResumeAndStatus(t *testing.T) {
	for _, tc := range []struct {
		name         string
		observations []AdapterObservation
		want         []platformv1alpha1.RunState
	}{
		{"foreground-process", []AdapterObservation{AdapterObservationRunning, AdapterObservationSucceeded}, []platformv1alpha1.RunState{platformv1alpha1.RunStateRunning, platformv1alpha1.RunStateSucceeded}},
		{"foreground-process-failure", []AdapterObservation{AdapterObservationFailed}, []platformv1alpha1.RunState{platformv1alpha1.RunStateFailed}},
		{"service-events", []AdapterObservation{AdapterObservationRunning, AdapterObservationNeedsInput}, []platformv1alpha1.RunState{platformv1alpha1.RunStateRunning, platformv1alpha1.RunStateNeedsInput}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateAllocating, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "euid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "euid"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
			a := &scriptedAdapter{observations: tc.observations}
			r := reconciler(t, a, run, env)
			if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStateEnvironmentReady {
				t.Fatal(got.Status.State)
			}
			if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStateEnvironmentReady || !acceptanceAttempted(&got) {
				t.Fatalf("acceptance marker state=%s attempted=%t", got.Status.State, acceptanceAttempted(&got))
			}
			if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStateAdapterAccepted {
				t.Fatal(got.Status.State)
			}
			for _, want := range tc.want {
				if got := reconcileRun(t, r, "r"); got.Status.State != want {
					t.Fatalf("state=%s want=%s", got.Status.State, want)
				}
			}
			if tc.name == "service-events" {
				_ = r.Get(context.Background(), client.ObjectKeyFromObject(env), env)
				env.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
				_ = r.Status().Update(context.Background(), env)
				if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStatePaused {
					t.Fatal(got.Status.State)
				}
				env.Status.Phase = platformv1alpha1.EnvironmentPhaseReady
				_ = r.Status().Update(context.Background(), env)
				if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStateEnvironmentReady {
					t.Fatal(got.Status.State)
				}
				if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStateAdapterAccepted || a.accepted != 2 {
					t.Fatalf("resume state=%s accepts=%d", got.Status.State, a.accepted)
				}
			}
		})
	}
}

func TestPermanentAdapterAcceptanceRejectionFailsRun(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStateEnvironmentReady,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "euid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "euid"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
	adapter := &scriptedAdapter{acceptErr: fmt.Errorf("%w: unsupported task configuration", ErrAdapterTaskRejected)}
	r := reconciler(t, adapter, run, env)
	got := reconcileRun(t, r, run.Name)
	condition := apiMeta.FindStatusCondition(got.Status.Conditions, runConditionAdapterAccepted)
	if got.Status.State != platformv1alpha1.RunStateFailed || condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "AdapterRejected" || !strings.Contains(condition.Message, "unsupported task configuration") || adapter.accepted != 1 {
		t.Fatalf("Run status = %#v, AdapterAccepted = %#v, accepts = %d", got.Status, condition, adapter.accepted)
	}
}

func TestDifferentAdapterShapesDriveSameLifecycleContract(t *testing.T) {
	readyRun := func(name string) *platformv1alpha1.Run {
		return &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(name), Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateEnvironmentReady, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e-" + name, UID: types.UID("e-" + name), Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	}
	readyEnvironment := func(run *platformv1alpha1.Run) *platformv1alpha1.Environment {
		return &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: run.Status.EnvironmentRef.Name, Namespace: "ns", UID: run.Status.EnvironmentRef.UID}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
	}

	foregroundRun := readyRun("foreground")
	foreground := &foregroundAdapter{process: AdapterObservationRunning}
	foregroundReconciler := reconciler(t, foreground, foregroundRun, readyEnvironment(foregroundRun))
	reconcileRun(t, foregroundReconciler, foregroundRun.Name) // acceptance attempt marker
	reconcileRun(t, foregroundReconciler, foregroundRun.Name) // acceptance
	if got := reconcileRun(t, foregroundReconciler, foregroundRun.Name); got.Status.State != platformv1alpha1.RunStateRunning {
		t.Fatalf("foreground state = %s", got.Status.State)
	}
	foreground.process = AdapterObservationSucceeded
	if got := reconcileRun(t, foregroundReconciler, foregroundRun.Name); got.Status.State != platformv1alpha1.RunStateSucceeded {
		t.Fatalf("foreground terminal state = %s", got.Status.State)
	}

	serviceRun := readyRun("service")
	service := &serviceAdapter{event: AdapterObservationRunning}
	serviceReconciler := reconciler(t, service, serviceRun, readyEnvironment(serviceRun))
	reconcileRun(t, serviceReconciler, serviceRun.Name) // acceptance attempt marker
	reconcileRun(t, serviceReconciler, serviceRun.Name) // task acknowledgement
	reconcileRun(t, serviceReconciler, serviceRun.Name) // running event
	service.event = AdapterObservationNeedsInput
	if got := reconcileRun(t, serviceReconciler, serviceRun.Name); got.Status.State != platformv1alpha1.RunStateNeedsInput || !service.serviceRunning {
		t.Fatalf("service state = %s, serviceRunning = %v", got.Status.State, service.serviceRunning)
	}
}

func TestNonterminalAdapterObservationSchedulesPolling(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateAdapterAccepted, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "euid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "euid"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
	r := reconciler(t, &foregroundAdapter{process: AdapterObservationRunning}, run, env)
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != adapterPollInterval {
		t.Fatalf("RequeueAfter = %s, want %s", result.RequeueAfter, adapterPollInterval)
	}
}

func TestEnvironmentReachableRequiresCurrentGenerationReady(t *testing.T) {
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Generation: 2}, Status: platformv1alpha1.EnvironmentStatus{
		ObservedGeneration: 1,
		Phase:              platformv1alpha1.EnvironmentPhaseReady,
		PodName:            "env-test",
		Endpoints:          platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		Conditions: []metav1.Condition{{
			Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue,
			ObservedGeneration: 1, Reason: "SandboxdReady", Message: "stale readiness",
		}},
	}}
	if environmentReachable(env) {
		t.Fatal("stale-generation Ready condition was accepted")
	}
	applyEnvironmentStatus(env, platformv1alpha1.EnvironmentPhaseReady, env.Status.PodName, env.Status.Endpoints.Sandboxd, "SandboxdReady", "current readiness", nil)
	if !environmentReachable(env) {
		t.Fatal("current-generation Ready condition was rejected")
	}
}

func TestRunDoesNotFailOnStaleEnvironmentFailure(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStateAllocating,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "env-uid", Generation: 2}, Status: platformv1alpha1.EnvironmentStatus{
		ObservedGeneration: 1, Phase: platformv1alpha1.EnvironmentPhaseFailed,
	}}
	r := reconciler(t, &scriptedAdapter{}, run, env)
	got := reconcileRun(t, r, run.Name)
	if got.Status.State != platformv1alpha1.RunStateAllocating {
		t.Fatalf("Run state = %s, stale Environment failure became terminal", got.Status.State)
	}
}

func TestEnvironmentReadyConditionIsIndependentFromTaskOutcome(t *testing.T) {
	for _, tc := range []struct {
		name         string
		runState     platformv1alpha1.RunState
		envPhase     platformv1alpha1.EnvironmentPhase
		adapter      AdapterLifecycle
		cancel       bool
		accepted     bool
		wantReady    metav1.ConditionStatus
		wantRunState platformv1alpha1.RunState
	}{
		{
			name: "adapter failure leaves ready Environment", runState: platformv1alpha1.RunStateAdapterAccepted,
			envPhase: platformv1alpha1.EnvironmentPhaseReady, adapter: &scriptedAdapter{observations: []AdapterObservation{AdapterObservationFailed}},
			accepted: true, wantReady: metav1.ConditionTrue, wantRunState: platformv1alpha1.RunStateFailed,
		},
		{
			name: "unavailable adapter leaves ready Environment", runState: platformv1alpha1.RunStateEnvironmentReady,
			envPhase: platformv1alpha1.EnvironmentPhaseReady, wantReady: metav1.ConditionTrue, wantRunState: platformv1alpha1.RunStateFailed,
		},
		{
			name: "Environment failure clears readiness", runState: platformv1alpha1.RunStateAdapterAccepted,
			envPhase: platformv1alpha1.EnvironmentPhaseFailed, adapter: &scriptedAdapter{}, accepted: true, wantReady: metav1.ConditionFalse, wantRunState: platformv1alpha1.RunStateFailed,
		},
		{
			name: "cancellation leaves reachable Environment ready", runState: platformv1alpha1.RunStateAdapterAccepted,
			envPhase: platformv1alpha1.EnvironmentPhaseReady, adapter: &scriptedAdapter{}, cancel: true, accepted: true,
			wantReady: metav1.ConditionTrue, wantRunState: platformv1alpha1.RunStateCancelled,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", Cancel: tc.cancel}, Status: platformv1alpha1.RunStatus{
				State:          tc.runState,
				EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
				Conditions:     []metav1.Condition{{Type: runConditionEnvironmentReady, Status: metav1.ConditionTrue}},
			}}
			if tc.accepted {
				run.Status.Conditions = append(run.Status.Conditions, metav1.Condition{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue})
			}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
				Phase: tc.envPhase, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
			}}
			if tc.envPhase == platformv1alpha1.EnvironmentPhaseReady {
				env.Status.PodName = "env-shared"
				env.Status.Endpoints.Sandboxd = "10.0.0.1:50051"
			}
			r := reconciler(t, tc.adapter, run, env)
			if tc.adapter == nil {
				r.Adapters = map[string]AdapterLifecycle{}
			}
			got := reconcileRun(t, r, run.Name)
			condition := apiMeta.FindStatusCondition(got.Status.Conditions, runConditionEnvironmentReady)
			if got.Status.State != tc.wantRunState || condition == nil || condition.Status != tc.wantReady {
				t.Fatalf("Run status = %#v, EnvironmentReady = %#v", got.Status, condition)
			}
			if tc.wantReady == metav1.ConditionTrue {
				cleaned := reconcileRun(t, r, run.Name)
				condition = apiMeta.FindStatusCondition(cleaned.Status.Conditions, runConditionEnvironmentReady)
				if condition == nil || condition.Status != metav1.ConditionFalse {
					t.Fatalf("EnvironmentReady after terminal claim release = %#v", condition)
				}
				var released platformv1alpha1.Environment
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &released); err != nil || released.Status.ClaimedBy != nil {
					t.Fatalf("terminal claimed Environment = %#v, error = %v", released, err)
				}
			}
		})
	}
}

func TestExplicitSuspendedClaimPublishesReasonScopedWakeDuringAllocationAndRecovery(t *testing.T) {
	for _, reason := range []platformv1alpha1.EnvironmentSuspensionReason{platformv1alpha1.EnvironmentSuspensionReasonIdle, platformv1alpha1.EnvironmentSuspensionReasonRequested} {
		for _, tc := range []struct {
			name     string
			recovery bool
		}{
			{name: "allocation"},
			{name: "claim-before-status recovery", recovery: true},
		} {
			t.Run(string(reason)+"/"+tc.name, func(t *testing.T) {
				run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: "shared", Agent: "test"}}
				env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{
					Suspended: true, SuspensionReason: reason, Epoch: 1,
				}}}
				if tc.recovery {
					env.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}
					run.Finalizers = []string{runFinalizer}
				}
				r := reconciler(t, &scriptedAdapter{}, run, env)
				var ref *platformv1alpha1.RunEnvironmentReference
				var err error
				if tc.recovery {
					got := reconcileRun(t, r, run.Name)
					ref = got.Status.EnvironmentRef
				} else {
					ref, err = r.allocateEnvironment(context.Background(), run)
				}
				if err != nil || ref == nil || ref.Ownership != platformv1alpha1.EnvironmentOwnershipClaimed {
					t.Fatalf("reference = %#v, error = %v", ref, err)
				}
				var got platformv1alpha1.Environment
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
					t.Fatal(err)
				}
				if got.Spec.Lifecycle.Wake == nil || got.Spec.Lifecycle.Wake.EnvironmentUID != env.UID || got.Spec.Lifecycle.Wake.ExpectedSuspensionReason != reason || got.Status.ClaimedBy == nil || got.Status.ClaimedBy.UID != run.UID {
					t.Fatalf("claimed environment = %#v", got)
				}
			})
		}
	}
}

func TestExplicitHeldClaimFailsImmediatelyDuringAllocationAndRecovery(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		name := "allocation"
		if recovery {
			name = "claim-before-status recovery"
		}
		t.Run(name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: "shared", Agent: "test"}}
			env := &platformv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"},
				Spec: platformv1alpha1.EnvironmentSpec{Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{Hold: &platformv1alpha1.EnvironmentHoldPolicy{
					Enabled: true, Revision: 3,
				}}},
			}
			if recovery {
				env.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}
				run.Finalizers = []string{runFinalizer}
			}
			r := reconciler(t, &scriptedAdapter{}, run, env)
			if recovery {
				got := reconcileRun(t, r, run.Name)
				if got.Status.State != platformv1alpha1.RunStateFailed || got.Status.EnvironmentRef != nil {
					t.Fatalf("recovered held claim Run status = %#v", got.Status)
				}
			} else {
				if ref, err := r.allocateEnvironment(context.Background(), run); ref != nil || !errors.Is(err, errExplicitEnvironmentHeld) {
					t.Fatalf("held allocation = (%#v, %v)", ref, err)
				}
			}
			var retained platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
				t.Fatal(err)
			}
			if retained.Status.ClaimedBy != nil || retained.Spec.Lifecycle.Wake != nil {
				t.Fatalf("held Environment was claimed or woken: %#v", retained)
			}
		})
	}
}

func TestExplicitNonWakeableSuspensionFailsBeforeRetainingClaim(t *testing.T) {
	for _, reason := range []platformv1alpha1.EnvironmentSuspensionReason{"", platformv1alpha1.EnvironmentSuspensionReasonHold} {
		for _, recovery := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/recovery=%t", reason, recovery), func(t *testing.T) {
				run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: "shared", Agent: "test"}}
				env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{
					Suspended: true, SuspensionReason: reason, Epoch: 1,
				}}}
				if recovery {
					env.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}
					run.Finalizers = []string{runFinalizer}
				}
				r := reconciler(t, &scriptedAdapter{}, run, env)
				if recovery {
					got := reconcileRun(t, r, run.Name)
					if got.Status.State != platformv1alpha1.RunStateFailed || got.Status.EnvironmentRef != nil {
						t.Fatalf("non-wakeable recovery status = %#v", got.Status)
					}
				} else if ref, err := r.allocateEnvironment(context.Background(), run); ref != nil || !errors.Is(err, errExplicitEnvironmentSuspensionNotWakeable) {
					t.Fatalf("non-wakeable allocation = (%#v, %v)", ref, err)
				}
				var retained platformv1alpha1.Environment
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
					t.Fatal(err)
				}
				if retained.Status.ClaimedBy != nil || retained.Spec.Lifecycle.Wake != nil {
					t.Fatalf("non-wakeable Environment retained traffic: status=%#v spec=%#v", retained.Status, retained.Spec.Lifecycle)
				}
			})
		}
	}
}

func TestCancellationWhilePausedRequiresNoAdapterRPC(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", Cancel: true}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStatePaused,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{Paused: true}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhasePaused, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	if got := reconcileRun(t, r, run.Name); got.Status.State != platformv1alpha1.RunStateCancelled {
		t.Fatalf("state = %s, want Cancelled", got.Status.State)
	}
	reconcileRun(t, r, run.Name)
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal("claimed Environment was deleted")
	}
	if retained.Status.ClaimedBy != nil || adapter.cancelled != 0 {
		t.Fatalf("claim = %#v, adapter cancellations = %d", retained.Status.ClaimedBy, adapter.cancelled)
	}
}

func TestLostAcceptanceStatusCancelsBeforeClaimRelease(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		name := "cancel"
		if deleting {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
				State:          platformv1alpha1.RunStateEnvironmentReady,
				EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
				Conditions:     []metav1.Condition{{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue}},
			}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
				Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-shared", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
				ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
			}}
			adapter := &scriptedAdapter{}
			r := reconciler(t, adapter, run, env)
			fault := &failAcceptedStatusClient{Client: r.Client, fail: true}
			r.Client = fault
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err == nil {
				t.Fatal("acceptance status update unexpectedly succeeded")
			}
			if adapter.accepted != 1 {
				t.Fatalf("acceptance calls = %d, want 1", adapter.accepted)
			}
			var storedRun platformv1alpha1.Run
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &storedRun); err != nil {
				t.Fatal(err)
			}
			if storedRun.Status.State != platformv1alpha1.RunStateEnvironmentReady || !acceptanceAttempted(&storedRun) || runAccepted(&storedRun) {
				t.Fatalf("stored status after lost update = %#v", storedRun.Status)
			}
			if deleting {
				if err := r.Delete(context.Background(), &storedRun); err != nil {
					t.Fatal(err)
				}
			} else {
				storedRun.Spec.Cancel = true
				if err := r.Update(context.Background(), &storedRun); err != nil {
					t.Fatal(err)
				}
			}
			adapter.onCancel = func() {
				var current platformv1alpha1.Environment
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil || current.Status.ClaimedBy == nil {
					t.Fatal("claim was released before uncertain acceptance was cancelled")
				}
			}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
				t.Fatal(err)
			}
			if !deleting {
				reconcileRun(t, r, run.Name)
			}
			var retained platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
				t.Fatal(err)
			}
			if adapter.cancelled == 0 || retained.Status.ClaimedBy != nil {
				t.Fatalf("cancellations = %d, claim = %#v", adapter.cancelled, retained.Status.ClaimedBy)
			}
		})
	}
}

func TestAcceptedUnreachableClaimIsFencedBeforeCancellationCleanup(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", Cancel: true}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStateAdapterAccepted,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhaseFailed, PodName: "env-shared", ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("fence request = (%#v, %v)", result, err)
	}
	var fencing platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil {
		t.Fatal(err)
	}
	if fencing.Spec.Lifecycle.Suspend == nil || fencing.Status.ClaimedBy == nil || adapter.cancelled != 0 {
		t.Fatalf("fencing Environment = %#v, cancellations = %d", fencing, adapter.cancelled)
	}
	fencing.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	fencing.Status.Lifecycle.Suspended = true
	fencing.Status.PodName = ""
	fencing.Status.Endpoints = platformv1alpha1.EnvironmentEndpoints{}
	if err := r.Status().Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if got := reconcileRun(t, r, run.Name); got.Status.State != platformv1alpha1.RunStateCancelled {
		t.Fatalf("state after fence = %s", got.Status.State)
	}
	result, err = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("cleanup before suspend acknowledgement = (%#v, %v)", result, err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil || fencing.Status.ClaimedBy == nil {
		t.Fatalf("claim released before suspend acknowledgement: environment=%#v error=%v", fencing, err)
	}
	fencing.Spec.Lifecycle.Suspend = nil
	if err := r.Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	reconcileRun(t, r, run.Name)
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil {
		t.Fatal(err)
	}
	if fencing.Status.ClaimedBy != nil || adapter.cancelled != 0 {
		t.Fatalf("post-fence claim = %#v, cancellations = %d", fencing.Status.ClaimedBy, adapter.cancelled)
	}
}

func TestCancellationPendingRetainsRunAndClaim(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", Cancel: true}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStateAdapterAccepted,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-shared", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	adapter := &scriptedAdapter{cancelErr: ErrAdapterCancellationPending}
	r := reconciler(t, adapter, run, env)
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("pending cancellation = (%#v, %v)", result, err)
	}
	var retainedRun platformv1alpha1.Run
	var retainedEnv platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &retainedRun); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retainedEnv); err != nil {
		t.Fatal(err)
	}
	if retainedRun.Status.State != platformv1alpha1.RunStateAdapterAccepted || retainedEnv.Status.ClaimedBy == nil {
		t.Fatalf("pending cancellation released state/claim: %s/%#v", retainedRun.Status.State, retainedEnv.Status.ClaimedBy)
	}
	adapter.cancelErr = nil
	if got := reconcileRun(t, r, run.Name); got.Status.State != platformv1alpha1.RunStateCancelled {
		t.Fatalf("completed cancellation state = %s", got.Status.State)
	}
}

func TestAcceptedUnreachableOwnedFinalizerWaitsForFence(t *testing.T) {
	now := metav1.Now()
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "owned", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{
		APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true),
	}}}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseCreating}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	result, err := r.finalize(context.Background(), run)
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("fence request = (%#v, %v)", result, err)
	}
	var fencing platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil {
		t.Fatal(err)
	}
	var retainedRun platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &retainedRun); err != nil || !controllerutil.ContainsFinalizer(&retainedRun, runFinalizer) || fencing.Spec.Lifecycle.Suspend == nil {
		t.Fatal("owned Run finalizer or fence request was not retained")
	}
	fencing.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	fencing.Status.Lifecycle.Suspended = true
	if err := r.Status().Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	fencing.Spec.Lifecycle.Suspend = nil
	if err := r.Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if _, err := r.finalize(context.Background(), &retainedRun); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil || adapter.cancelled != 0 || !exactControllerOwner(&fencing, platformv1alpha1.GroupVersion.String(), "Run", run.Name, run.UID) {
		t.Fatalf("owned Environment after fenced finalization = %#v, cancellations = %d", fencing, adapter.cancelled)
	}
}

func TestAcceptedUnreachableOwnedTerminalCleanupWaitsForFence(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStateSucceeded,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "owned", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{
		APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true),
	}}}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseFailed, PodName: "env-owned"}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("terminal fence request = (%#v, %v)", result, err)
	}
	var fencing platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil || fencing.Spec.Lifecycle.Suspend == nil || adapter.cancelled != 0 {
		t.Fatalf("terminal fencing Environment = %#v, cancellations = %d, error = %v", fencing, adapter.cancelled, err)
	}
	fencing.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	fencing.Status.Lifecycle.Suspended = true
	fencing.Status.PodName = ""
	if err := r.Status().Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	fencing.Spec.Lifecycle.Suspend = nil
	if err := r.Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil || fencing.Spec.Lifecycle.Suspend != nil || adapter.cancelled != 0 {
		t.Fatalf("terminal cleanup after fence = %#v, cancellations = %d, error = %v", fencing, adapter.cancelled, err)
	}
}

func TestOwnedEnvironmentFenceUsesNextSuspendSequence(t *testing.T) {
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "ns", UID: "env-uid"},
		Status: platformv1alpha1.EnvironmentStatus{Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{
			Epoch: 1, LastSuspendRequestID: "environment/env-uid/fence/4", LastSuspendRequestSequence: 4,
		}},
	}
	r := reconciler(t, &scriptedAdapter{}, environment)
	result, err := r.requestEnvironmentFence(context.Background(), environment)
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("requestEnvironmentFence() = (%#v, %v)", result, err)
	}
	var updated platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(environment), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Spec.Lifecycle.Suspend == nil || updated.Spec.Lifecycle.Suspend.ID != "environment/env-uid/fence/5" || updated.Spec.Lifecycle.Suspend.Sequence != 5 {
		t.Fatalf("next fence request = %#v", updated.Spec.Lifecycle.Suspend)
	}
	if _, err := r.requestEnvironmentFence(context.Background(), &updated); err != nil {
		t.Fatal(err)
	}
	var replay platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(environment), &replay); err != nil {
		t.Fatal(err)
	}
	if replay.Spec.Lifecycle.Suspend == nil || replay.Spec.Lifecycle.Suspend.ID != "environment/env-uid/fence/5" || replay.Spec.Lifecycle.Suspend.Sequence != 5 {
		t.Fatalf("retried fence request = %#v", replay.Spec.Lifecycle.Suspend)
	}
}

func TestFinalizeClaimedEnvironmentOrdersCancellationBeforeRelease(t *testing.T) {
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
	r := reconciler(t, adapter, run, env)
	adapter.onCancel = func() {
		var current platformv1alpha1.Environment
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil || current.Status.ClaimedBy == nil {
			t.Fatal("claim was released before adapter cancellation")
		}
	}
	if result, err := r.finalize(context.Background(), run); err != nil || result.RequeueAfter != 0 {
		t.Fatalf("finalize = (%#v, %v)", result, err)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal("claimed Environment was deleted")
	}
	if retained.Status.ClaimedBy != nil || adapter.cancelled != 1 {
		t.Fatalf("claim = %#v, cancellations = %d", retained.Status.ClaimedBy, adapter.cancelled)
	}
	var deletedRun platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &deletedRun); err == nil {
		if controllerutil.ContainsFinalizer(&deletedRun, runFinalizer) {
			t.Fatal("Run finalizer was not removed")
		}
	} else if !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
}

func TestFinalizeOwnedEnvironmentLeavesGCReference(t *testing.T) {
	now := metav1.Now()
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "owned", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{
		APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true),
	}}}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-owned", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"}}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	if _, err := r.finalize(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal("owned Environment was directly deleted instead of left to GC")
	}
	if !metav1.IsControlledBy(&retained, run) || adapter.cancelled != 1 {
		t.Fatalf("owner = %#v, cancellations = %d", metav1.GetControllerOf(&retained), adapter.cancelled)
	}
}

func TestFinalizePausedAndTransientlyUnreachableAcceptedClaims(t *testing.T) {
	for _, tc := range []struct {
		name        string
		phase       platformv1alpha1.EnvironmentPhase
		specPaused  bool
		wantRequeue bool
		wantClaim   bool
	}{
		{name: "paused is fenced", phase: platformv1alpha1.EnvironmentPhasePaused, specPaused: true},
		{name: "stale paused status is not fenced", phase: platformv1alpha1.EnvironmentPhasePaused, wantRequeue: true, wantClaim: true},
		{name: "setup is ambiguous", phase: platformv1alpha1.EnvironmentPhaseSetup, wantRequeue: true, wantClaim: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := metav1.Now()
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
				EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
				Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
			}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{Paused: tc.specPaused}, Status: platformv1alpha1.EnvironmentStatus{Phase: tc.phase, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}}}
			adapter := &scriptedAdapter{}
			r := reconciler(t, adapter, run, env)
			result, err := r.finalize(context.Background(), run)
			if err != nil || (result.RequeueAfter != 0) != tc.wantRequeue {
				t.Fatalf("finalize = (%#v, %v)", result, err)
			}
			var retained platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
				t.Fatal(err)
			}
			if (retained.Status.ClaimedBy != nil) != tc.wantClaim || adapter.cancelled != 0 {
				t.Fatalf("claim = %#v, cancellations = %d", retained.Status.ClaimedBy, adapter.cancelled)
			}
			if tc.wantRequeue {
				var retainedRun platformv1alpha1.Run
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &retainedRun); err != nil || !controllerutil.ContainsFinalizer(&retainedRun, runFinalizer) {
					t.Fatal("ambiguous accepted work did not retain Run finalizer")
				}
			}
		})
	}
}

func TestMissingAdapterCleanupFallsBackToBackendFence(t *testing.T) {
	now := metav1.Now()
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "removed-adapter"}, Status: platformv1alpha1.RunStatus{
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-shared", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	r := reconciler(t, &scriptedAdapter{}, run, env)
	r.Adapters = map[string]AdapterLifecycle{}
	result, err := r.finalize(context.Background(), run)
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("missing-adapter fence = (%#v, %v)", result, err)
	}
	var fencing platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil {
		t.Fatal(err)
	}
	var retainedRun platformv1alpha1.Run
	if fencing.Spec.Lifecycle.Suspend == nil || fencing.Status.ClaimedBy == nil || r.Get(context.Background(), client.ObjectKeyFromObject(run), &retainedRun) != nil || !controllerutil.ContainsFinalizer(&retainedRun, runFinalizer) {
		t.Fatal("missing adapter released ownership before backend fencing")
	}
	fencing.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	fencing.Status.Lifecycle.Suspended = true
	fencing.Status.PodName = ""
	fencing.Status.Endpoints = platformv1alpha1.EnvironmentEndpoints{}
	if err := r.Status().Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	fencing.Spec.Lifecycle.Suspend = nil
	if err := r.Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if _, err := r.finalize(context.Background(), &retainedRun); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil || fencing.Status.ClaimedBy != nil {
		t.Fatalf("fenced missing-adapter claim = %#v, error = %v", fencing.Status.ClaimedBy, err)
	}
}

func TestFinalizeRecoversLostClaimStatusAndOnlyReleasesMatchingUID(t *testing.T) {
	for _, tc := range []struct {
		name      string
		claimUID  types.UID
		wantClaim bool
	}{
		{name: "matching recovery", claimUID: "run-uid"},
		{name: "same-name replacement claim", claimUID: "other-run", wantClaim: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := metav1.Now()
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test", EnvironmentRef: "shared"}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{Paused: true}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhasePaused, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: tc.claimUID}}}
			r := reconciler(t, &scriptedAdapter{}, run, env)
			if _, err := r.finalize(context.Background(), run); err != nil {
				t.Fatal(err)
			}
			var retained platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
				t.Fatal(err)
			}
			if (retained.Status.ClaimedBy != nil) != tc.wantClaim {
				t.Fatalf("claim = %#v", retained.Status.ClaimedBy)
			}
			if !retained.Spec.Paused {
				t.Fatal("deletion recovery unexpectedly woke the paused Environment")
			}
		})
	}
}

func TestCancelRecoveryDoesNotWakePausedClaim(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", EnvironmentRef: "shared", Cancel: true}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{Paused: true}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhasePaused, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	reconcileRun(t, r, run.Name)
	if got := reconcileRun(t, r, run.Name); got.Status.State != platformv1alpha1.RunStateCancelled {
		t.Fatalf("state = %s, want Cancelled", got.Status.State)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal(err)
	}
	if !retained.Spec.Paused || adapter.cancelled != 0 {
		t.Fatalf("paused = %t, cancellations = %d", retained.Spec.Paused, adapter.cancelled)
	}
}

func TestFinalizeRecoversOwnedEnvironmentWithLostRunStatus(t *testing.T) {
	now := metav1.Now()
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test", TemplateRef: "small"}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "run-run-uid", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{
		APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true),
	}}}, Spec: platformv1alpha1.EnvironmentSpec{Paused: true}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhasePaused}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	if _, err := r.finalize(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal("owned Environment should remain for API-server GC")
	}
	if !exactControllerOwner(&retained, platformv1alpha1.GroupVersion.String(), "Run", run.Name, run.UID) || adapter.cancelled != 0 {
		t.Fatalf("owner = %#v, cancellations = %d", metav1.GetControllerOf(&retained), adapter.cancelled)
	}
}

func TestCleanupRecoveryDoesNotPromoteWarmClaim(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		name := "cancel"
		if deleting {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", TemplateRef: "small", ProjectRef: "project", Cancel: !deleting}}
			if deleting {
				now := metav1.Now()
				run.DeletionTimestamp = &now
			}
			warm := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "warm-small-1", Namespace: "ns", UID: "warm-uid", Labels: map[string]string{warmPoolLabel: "small"}, OwnerReferences: []metav1.OwnerReference{{
				APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "EnvironmentTemplate", Name: "small", UID: "template-uid", Controller: ptr(true),
			}}}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "small"}, Status: platformv1alpha1.EnvironmentStatus{
				Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-warm-small-1", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
				ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
			}}
			adapter := &scriptedAdapter{}
			r := reconciler(t, adapter, run, warm)
			if deleting {
				if _, err := r.finalize(context.Background(), run); err != nil {
					t.Fatal(err)
				}
			} else {
				reconcileRun(t, r, run.Name)
				reconcileRun(t, r, run.Name)
			}
			var retained platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(warm), &retained); err != nil {
				t.Fatal(err)
			}
			owner := metav1.GetControllerOf(&retained)
			if retained.Labels[warmPoolLabel] != "small" || owner == nil || owner.Kind != "EnvironmentTemplate" || owner.Name != "small" ||
				retained.Spec.ProjectRef != "" || retained.Status.ClaimedBy != nil || retained.Status.Phase != platformv1alpha1.EnvironmentPhaseReady || retained.Status.PodName == "" || retained.Status.Endpoints.Sandboxd == "" || adapter.cancelled != 0 {
				t.Fatalf("warm Environment was promoted during cleanup recovery: %#v, cancellations = %d", retained, adapter.cancelled)
			}
		})
	}
}

func TestOwnershipMismatchNeverCancelsOrMutatesEnvironment(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStateSucceeded,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "owned", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{
		APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: "other", UID: run.UID, Controller: ptr(true),
	}}}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-owned", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"}}}
	s := runScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&platformv1alpha1.Run{}, &platformv1alpha1.Environment{}).WithObjects(run, env).Build()
	adapter := &scriptedAdapter{}
	r := &RunReconciler{Client: c, Scheme: s, Adapters: map[string]AdapterLifecycle{"test": adapter}}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal(err)
	}
	if retained.Spec.Paused || adapter.cancelled != 0 || metav1.GetControllerOf(&retained).Name != "other" {
		t.Fatalf("environment mutated or cancelled: %#v, cancellations = %d", retained, adapter.cancelled)
	}
}

func TestCredentialProfileBindsExactUIDBeforeAllocation(t *testing.T) {
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"},
		Spec:       platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", CredentialProfileRef: "profile"},
	}
	profile, secret := credentialProfileAndSecret(run, []byte("!!BOUND-KEY-FIXTURE!!"))
	r := reconciler(t, &scriptedAdapter{}, run, profile, secret)

	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || !result.Requeue {
		t.Fatalf("binding reconcile = (%#v, %v)", result, err)
	}
	var bound platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &bound); err != nil {
		t.Fatal(err)
	}
	condition := apiMeta.FindStatusCondition(bound.Status.Conditions, runConditionCredentialProfileBound)
	if bound.Status.CredentialProfileRef == nil || bound.Status.CredentialProfileRef.Name != profile.Name || bound.Status.CredentialProfileRef.UID != profile.UID ||
		condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "Bound" {
		t.Fatalf("binding status = %#v, condition = %#v", bound.Status.CredentialProfileRef, condition)
	}
	var environments platformv1alpha1.EnvironmentList
	if err := r.List(context.Background(), &environments, client.InNamespace(run.Namespace)); err != nil || len(environments.Items) != 0 {
		t.Fatalf("binding allocated environments = %d, error = %v", len(environments.Items), err)
	}
}

type credentiallessOnlyAdapter struct{ scriptedAdapter }

func (*credentiallessOnlyAdapter) SupportsCredentialProfiles() bool { return false }

type countingReader struct{ reads int }

func (r *countingReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	r.reads++
	return errors.New("unexpected credential read")
}
func (r *countingReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	r.reads++
	return errors.New("unexpected credential list")
}

func TestUnsupportedAdapterCredentialProfileFailsBeforeReadsOrAllocation(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", CredentialProfileRef: "must-not-read"}}
	r := reconciler(t, &credentiallessOnlyAdapter{}, run)
	reader := &countingReader{}
	r.APIReader = reader
	got := reconcileRun(t, r, run.Name)
	var environments platformv1alpha1.EnvironmentList
	if err := r.List(context.Background(), &environments, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	condition := apiMeta.FindStatusCondition(got.Status.Conditions, runConditionCredentialProfileBound)
	if got.Status.State != platformv1alpha1.RunStateFailed || got.Status.EnvironmentRef != nil || len(environments.Items) != 0 || reader.reads != 0 || condition == nil || condition.Reason != "CredentialProfilesUnsupported" {
		t.Fatalf("status=%#v environments=%d reads=%d", got.Status, len(environments.Items), reader.reads)
	}
}

func TestCredentiallessRunPreservesAllocationBehavior(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test"}}
	r := reconciler(t, &scriptedAdapter{}, run)
	reconcileRun(t, r, run.Name) // Finalizer.
	got := reconcileRun(t, r, run.Name)
	condition := apiMeta.FindStatusCondition(got.Status.Conditions, runConditionCredentialProfileBound)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "Credentialless" || got.Status.EnvironmentRef == nil {
		t.Fatalf("credentialless status = %#v", got.Status)
	}
}

func TestCredentialBindingWaitsBoundedlyAndRecovers(t *testing.T) {
	t.Run("profile", func(t *testing.T) {
		run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", CredentialProfileRef: "profile"}}
		r := reconciler(t, &scriptedAdapter{}, run)
		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
		if err != nil || result.RequeueAfter <= 0 {
			t.Fatalf("missing profile = (%#v, %v)", result, err)
		}
		var waiting platformv1alpha1.Run
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &waiting); err != nil {
			t.Fatal(err)
		}
		condition := apiMeta.FindStatusCondition(waiting.Status.Conditions, runConditionCredentialProfileBound)
		if condition == nil || condition.Reason != "ProfileNotFound" || waiting.Status.EnvironmentRef != nil {
			t.Fatalf("waiting status = %#v", waiting.Status)
		}
		profile, secret := credentialProfileAndSecret(&waiting, []byte("!!RECOVERED-KEY-FIXTURE!!"))
		if err := r.Create(context.Background(), profile); err != nil {
			t.Fatal(err)
		}
		if err := r.Create(context.Background(), secret); err != nil {
			t.Fatal(err)
		}
		got := reconcileRun(t, r, run.Name)
		if got.Status.CredentialProfileRef == nil || got.Status.CredentialProfileRef.UID != profile.UID || got.Status.EnvironmentRef != nil {
			t.Fatalf("recovered binding = %#v", got.Status)
		}
	})

	t.Run("secret timeout", func(t *testing.T) {
		run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", CredentialProfileRef: "profile"}}
		profile, _ := credentialProfileAndSecret(run, nil)
		r := reconciler(t, &scriptedAdapter{}, run, profile)
		reconcileRun(t, r, run.Name) // Bind the profile UID.
		waiting := reconcileRun(t, r, run.Name)
		condition := apiMeta.FindStatusCondition(waiting.Status.Conditions, runConditionCredentialProfileBound)
		if condition == nil || condition.Reason != "SecretNotReady" || waiting.Status.EnvironmentRef != nil {
			t.Fatalf("secret waiting status = %#v", waiting.Status)
		}
		condition.LastTransitionTime = metav1.NewTime(time.Now().Add(-credentialReadyTimeout - time.Second))
		if err := r.Status().Update(context.Background(), &waiting); err != nil {
			t.Fatal(err)
		}
		failed := reconcileRun(t, r, run.Name)
		condition = apiMeta.FindStatusCondition(failed.Status.Conditions, runConditionCredentialProfileBound)
		if failed.Status.State != platformv1alpha1.RunStateFailed || condition == nil || condition.Reason != "SecretNotReady" || failed.Status.EnvironmentRef != nil {
			t.Fatalf("timed out status = %#v", failed.Status)
		}
	})
}

func TestCredentialBindingRejectsAdapterTypeAndReplacement(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*platformv1alpha1.Run, *platformv1alpha1.AgentCredentialProfile)
		reason string
	}{
		{name: "adapter mismatch", mutate: func(_ *platformv1alpha1.Run, profile *platformv1alpha1.AgentCredentialProfile) {
			profile.Spec.Adapter = "other"
		}, reason: "AdapterMismatch"},
		{name: "unsupported type", mutate: func(_ *platformv1alpha1.Run, profile *platformv1alpha1.AgentCredentialProfile) {
			profile.Spec.CredentialType = "FutureType"
		}, reason: "UnsupportedCredentialType"},
		{name: "same-name replacement", mutate: func(run *platformv1alpha1.Run, _ *platformv1alpha1.AgentCredentialProfile) {
			run.Status.CredentialProfileRef = &platformv1alpha1.RunCredentialProfileReference{Name: run.Spec.CredentialProfileRef, UID: "old-profile-uid"}
		}, reason: "ProfileReplaced"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", CredentialProfileRef: "profile"}}
			profile, secret := credentialProfileAndSecret(run, []byte("!!REJECTED-KEY-FIXTURE!!"))
			test.mutate(run, profile)
			r := reconciler(t, &scriptedAdapter{}, run, profile, secret)
			got := reconcileRun(t, r, run.Name)
			condition := apiMeta.FindStatusCondition(got.Status.Conditions, runConditionCredentialProfileBound)
			if got.Status.State != platformv1alpha1.RunStateFailed || condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != test.reason || got.Status.EnvironmentRef != nil {
				t.Fatalf("rejected status = %#v", got.Status)
			}
		})
	}
}

func TestResolveCredentialRejectsMalformedForeignAndWrongNamespaceSecrets(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*platformv1alpha1.AgentCredentialProfile, *corev1.Secret)
		reason string
	}{
		{name: "wrong type", mutate: func(_ *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			secret.Type = corev1.SecretTypeOpaque
		}, reason: "MalformedSecret"},
		{name: "extra key", mutate: func(_ *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			secret.Data["extra"] = []byte("x")
		}, reason: "MalformedSecret"},
		{name: "empty", mutate: func(_ *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey] = nil
		}, reason: "MalformedSecret"},
		{name: "invalid utf8", mutate: func(_ *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey] = []byte{0xff}
		}, reason: "MalformedSecret"},
		{name: "nul", mutate: func(_ *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey] = []byte{'x', 0}
		}, reason: "MalformedSecret"},
		{name: "oversize", mutate: func(_ *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey] = make([]byte, platformv1alpha1.AgentCredentialAPIKeyMaxBytes+1)
		}, reason: "MalformedSecret"},
		{name: "foreign owner", mutate: func(_ *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			secret.OwnerReferences[0].UID = "foreign-profile-uid"
		}, reason: "ForeignSecret"},
		{name: "extra owner", mutate: func(_ *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			secret.OwnerReferences = append(secret.OwnerReferences, metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "other", UID: "other"})
		}, reason: "ForeignSecret"},
		{name: "wrong namespace", mutate: func(profile *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			profile.Namespace = "other"
			secret.Namespace = "other"
		}, reason: "ProfileNotFound"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := &platformv1alpha1.Run{
				ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"},
				Spec:       platformv1alpha1.RunSpec{Agent: "test", CredentialProfileRef: "profile"},
				Status: platformv1alpha1.RunStatus{CredentialProfileRef: &platformv1alpha1.RunCredentialProfileReference{
					Name: "profile", UID: "profile-uid",
				}},
			}
			profile, secret := credentialProfileAndSecret(run, []byte("!!VALIDATION-KEY-FIXTURE!!"))
			test.mutate(profile, secret)
			r := reconciler(t, &scriptedAdapter{}, run, profile, secret)
			credential, reason, err := r.resolveCredential(context.Background(), run)
			if credential != nil || err == nil || reason != test.reason {
				t.Fatalf("resolve = (%#v, %q, %v), want reason %q", credential, reason, err, test.reason)
			}
		})
	}
}

func TestResolveCredentialRejectsForeignMetadataWithoutReadingSecretData(t *testing.T) {
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"},
		Spec:       platformv1alpha1.RunSpec{Agent: "test", CredentialProfileRef: "profile"},
		Status: platformv1alpha1.RunStatus{CredentialProfileRef: &platformv1alpha1.RunCredentialProfileReference{
			Name: "profile", UID: "profile-uid",
		}},
	}
	profile, secret := credentialProfileAndSecret(run, []byte("!!FOREIGN-METADATA-KEY-FIXTURE!!"))
	secret.OwnerReferences[0].UID = "foreign-profile"
	r := reconciler(t, &scriptedAdapter{}, run)
	base := fake.NewClientBuilder().WithScheme(r.Scheme).WithObjects(profile, secret).Build()
	secretValueReads := 0
	r.APIReader = interceptor.NewClient(base, interceptor.Funcs{Get: func(ctx context.Context, underlying client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
		if _, ok := object.(*corev1.Secret); ok {
			secretValueReads++
		}
		return underlying.Get(ctx, key, object, options...)
	}})
	credential, reason, err := r.resolveCredential(context.Background(), run)
	if credential != nil || err == nil || reason != "ForeignSecret" || secretValueReads != 0 {
		t.Fatalf("resolve = (%#v, %q, %v), Secret value reads = %d", credential, reason, err, secretValueReads)
	}
}

func TestCredentialAcceptanceUsesUncachedCurrentKeyAndClearsCopy(t *testing.T) {
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Generation: 1, Finalizers: []string{runFinalizer}},
		Spec:       platformv1alpha1.RunSpec{Agent: "test", CredentialProfileRef: "profile"},
		Status: platformv1alpha1.RunStatus{
			State: platformv1alpha1.RunStateEnvironmentReady,
			EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{
				Name: "e", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned,
			},
			CredentialProfileRef: &platformv1alpha1.RunCredentialProfileReference{Name: "profile", UID: "profile-uid"},
			Conditions: []metav1.Condition{
				{Type: runConditionCredentialProfileBound, Status: metav1.ConditionTrue, Reason: "Bound", ObservedGeneration: 1},
				{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue, Reason: "AcceptancePending", ObservedGeneration: 1},
			},
		},
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "env-uid"},
		Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
	}
	profile, staleSecret := credentialProfileAndSecret(run, []byte("!!STALE-CACHED-KEY!!"))
	_, currentSecret := credentialProfileAndSecret(run, []byte("!!CURRENT-UNCACHED-KEY!!"))
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env, profile, staleSecret)
	liveReader := fake.NewClientBuilder().WithScheme(r.Scheme).WithObjects(profile.DeepCopy(), currentSecret).Build()
	r.APIReader = liveReader

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	if len(adapter.acceptedCredentials) != 1 || string(adapter.acceptedCredentials[0]) != "!!CURRENT-UNCACHED-KEY!!" {
		t.Fatalf("accepted credentials = %#v", adapter.acceptedCredentials)
	}
	if !reflect.DeepEqual(adapter.retainedCredentialKey, make([]byte, len(adapter.retainedCredentialKey))) {
		t.Fatal("Run controller retained credential copy after EnsureAccepted")
	}
	if !reflect.DeepEqual(staleSecret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey], []byte("!!STALE-CACHED-KEY!!")) {
		t.Fatal("cached Secret fixture was mutated")
	}
}

func TestCredentialRotationIsRematerializedAfterResumeEpoch(t *testing.T) {
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Generation: 1, Finalizers: []string{runFinalizer}},
		Spec:       platformv1alpha1.RunSpec{Agent: "test", CredentialProfileRef: "profile"},
		Status: platformv1alpha1.RunStatus{
			State:                platformv1alpha1.RunStateEnvironmentReady,
			EnvironmentRef:       &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
			CredentialProfileRef: &platformv1alpha1.RunCredentialProfileReference{Name: "profile", UID: "profile-uid"},
			Conditions: []metav1.Condition{
				{Type: runConditionCredentialProfileBound, Status: metav1.ConditionTrue, Reason: "Bound", ObservedGeneration: 1},
				{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue, Reason: "AcceptancePending", ObservedGeneration: 1},
			},
		},
	}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
	profile, secret := credentialProfileAndSecret(run, []byte("!!FIRST-EPOCH-KEY!!"))
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env, profile, secret)
	reconcileRun(t, r, run.Name)

	var rotated corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(secret), &rotated); err != nil {
		t.Fatal(err)
	}
	rotated.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey] = []byte("!!SECOND-EPOCH-KEY!!")
	if err := r.Update(context.Background(), &rotated); err != nil {
		t.Fatal(err)
	}
	var currentEnv platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &currentEnv); err != nil {
		t.Fatal(err)
	}
	currentEnv.Spec.Paused = true
	if err := r.Update(context.Background(), &currentEnv); err != nil {
		t.Fatal(err)
	}
	applyEnvironmentStatus(&currentEnv, platformv1alpha1.EnvironmentPhasePaused, "", "", "Paused", "paused", nil)
	currentEnv.Status.Lifecycle.Suspended = true
	currentEnv.Status.Lifecycle.SuspensionReason = platformv1alpha1.EnvironmentSuspensionReasonIdle
	currentEnv.Status.Lifecycle.Epoch = 1
	if err := r.Status().Update(context.Background(), &currentEnv); err != nil {
		t.Fatal(err)
	}
	reconcileRun(t, r, run.Name)
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &currentEnv); err != nil {
		t.Fatal(err)
	}
	currentEnv.Spec.Paused = false
	if err := r.Update(context.Background(), &currentEnv); err != nil {
		t.Fatal(err)
	}
	applyEnvironmentStatus(&currentEnv, platformv1alpha1.EnvironmentPhaseReady, "env-e", "10.0.0.2:50051", "SandboxdReady", "ready", nil)
	currentEnv.Status.Lifecycle.Suspended = false
	currentEnv.Status.Lifecycle.SuspensionReason = ""
	if err := r.Status().Update(context.Background(), &currentEnv); err != nil {
		t.Fatal(err)
	}
	reconcileRun(t, r, run.Name) // Paused -> EnvironmentReady.
	reconcileRun(t, r, run.Name) // Reaccept in the fresh sandbox epoch.
	adapter.observations = []AdapterObservation{AdapterObservationAccepted}
	reconcileRun(t, r, run.Name) // Observe without accepting the same epoch again.
	if len(adapter.acceptedCredentials) != 2 || string(adapter.acceptedCredentials[0]) != "!!FIRST-EPOCH-KEY!!" || string(adapter.acceptedCredentials[1]) != "!!SECOND-EPOCH-KEY!!" {
		t.Fatalf("epoch credentials = %#v", adapter.acceptedCredentials)
	}
}

func TestMissedPauseTransitionReacceptsFreshEnvironmentEpoch(t *testing.T) {
	for _, test := range []struct {
		name        string
		state       platformv1alpha1.RunState
		observation AdapterObservation
	}{
		{name: "running", state: platformv1alpha1.RunStateRunning, observation: AdapterObservationRunning},
		{name: "needs input", state: platformv1alpha1.RunStateNeedsInput, observation: AdapterObservationNeedsInput},
	} {
		t.Run(test.name, func(t *testing.T) {
			epoch0 := int64(0)
			run := &platformv1alpha1.Run{
				ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Generation: 1, Finalizers: []string{runFinalizer}},
				Spec:       platformv1alpha1.RunSpec{Agent: "test", CredentialProfileRef: "profile"},
				Status: platformv1alpha1.RunStatus{
					State:                    test.state,
					AcceptedEnvironmentEpoch: &epoch0,
					EnvironmentRef:           &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
					CredentialProfileRef:     &platformv1alpha1.RunCredentialProfileReference{Name: "profile", UID: "profile-uid"},
					Conditions: []metav1.Condition{
						{Type: runConditionCredentialProfileBound, Status: metav1.ConditionTrue, Reason: "Bound", ObservedGeneration: 1},
						{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue, Reason: "AcceptancePending", ObservedGeneration: 1},
						{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue, Reason: "AdapterAccepted", ObservedGeneration: 1},
					},
				},
			}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "env-uid", Generation: 1}}
			applyEnvironmentStatus(env, platformv1alpha1.EnvironmentPhaseReady, "env-e-0", "10.0.0.1:50051", "SandboxdReady", "ready", nil)
			profile, staleSecret := credentialProfileAndSecret(run, []byte("!!EPOCH-ZERO-KEY!!"))
			_, rotatedSecret := credentialProfileAndSecret(run, []byte("!!EPOCH-ONE-KEY!!"))
			adapter := &scriptedAdapter{observations: []AdapterObservation{test.observation}}
			r := reconciler(t, adapter, run, env, profile, staleSecret)
			r.APIReader = fake.NewClientBuilder().WithScheme(r.Scheme).WithObjects(profile.DeepCopy(), rotatedSecret).Build()

			// The Environment completes an entire pause/resume while the Run
			// controller is unavailable. No Run reconcile occurs in this window.
			var currentEnv platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &currentEnv); err != nil {
				t.Fatal(err)
			}
			currentEnv.Generation++
			currentEnv.Spec.Paused = true
			if err := r.Update(context.Background(), &currentEnv); err != nil {
				t.Fatal(err)
			}
			applyEnvironmentStatus(&currentEnv, platformv1alpha1.EnvironmentPhasePaused, "", "", "Paused", "paused", nil)
			currentEnv.Status.Lifecycle.Suspended = true
			currentEnv.Status.Lifecycle.SuspensionReason = platformv1alpha1.EnvironmentSuspensionReasonIdle
			currentEnv.Status.Lifecycle.Epoch = 1
			if err := r.Status().Update(context.Background(), &currentEnv); err != nil {
				t.Fatal(err)
			}
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &currentEnv); err != nil {
				t.Fatal(err)
			}
			currentEnv.Generation++
			currentEnv.Spec.Paused = false
			if err := r.Update(context.Background(), &currentEnv); err != nil {
				t.Fatal(err)
			}
			applyEnvironmentStatus(&currentEnv, platformv1alpha1.EnvironmentPhaseReady, "env-e-1", "10.0.0.2:50051", "SandboxdReady", "ready", nil)
			currentEnv.Status.Lifecycle.Suspended = false
			currentEnv.Status.Lifecycle.SuspensionReason = ""
			currentEnv.Status.Lifecycle.Epoch = 1
			if err := r.Status().Update(context.Background(), &currentEnv); err != nil {
				t.Fatal(err)
			}

			fenced := reconcileRun(t, r, run.Name)
			if fenced.Status.State != platformv1alpha1.RunStateEnvironmentReady || adapter.accepted != 0 || adapter.observed != 0 {
				t.Fatalf("epoch fence = state %s, acceptances %d, observations %d", fenced.Status.State, adapter.accepted, adapter.observed)
			}
			accepted := reconcileRun(t, r, run.Name)
			if accepted.Status.State != platformv1alpha1.RunStateAdapterAccepted || adapter.accepted != 1 || adapter.observed != 0 ||
				len(adapter.acceptedCredentials) != 1 || string(adapter.acceptedCredentials[0]) != "!!EPOCH-ONE-KEY!!" ||
				accepted.Status.AcceptedEnvironmentEpoch == nil || *accepted.Status.AcceptedEnvironmentEpoch != 1 {
				t.Fatalf("fresh epoch acceptance = status %#v, acceptances %d, observations %d, credentials %#v", accepted.Status, adapter.accepted, adapter.observed, adapter.acceptedCredentials)
			}
			observed := reconcileRun(t, r, run.Name)
			if observed.Status.State != test.state || adapter.accepted != 1 || adapter.observed != 1 {
				t.Fatalf("post-accept observation = state %s, acceptances %d, observations %d", observed.Status.State, adapter.accepted, adapter.observed)
			}
		})
	}
}

func TestCancellationBeforeAllocationDoesNotReadCredentialProfileOrSecret(t *testing.T) {
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"},
		Spec:       platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", CredentialProfileRef: "profile", Cancel: true},
	}
	r := reconciler(t, &scriptedAdapter{}, run)
	reads := 0
	r.APIReader = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
		reads++
		return errors.New("credential reader must not be called")
	}})
	got := reconcileRun(t, r, run.Name)
	if got.Status.State != platformv1alpha1.RunStateCancelled || reads != 0 {
		t.Fatalf("cancellation state = %s, credential reads = %d", got.Status.State, reads)
	}
}
