package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func resourceScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func TestResourceServiceListPassesPaginationAndRedacts(t *testing.T) {
	base := fake.NewClientBuilder().WithScheme(resourceScheme(t)).Build()
	var got client.ListOptions
	c := interceptor.NewClient(base, interceptor.Funcs{List: func(ctx context.Context, underlying client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		for _, opt := range opts {
			opt.ApplyToList(&got)
		}
		runs := list.(*platformv1alpha1.RunList)
		runs.Continue = "next"
		runs.ResourceVersion = "opaque-rv"
		runs.Items = []platformv1alpha1.Run{{
			ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "team", UID: "uid", ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "secret-manager"}}},
			Spec:       platformv1alpha1.RunSpec{Agent: "agent", Prompt: "prompt", Notify: []string{"private"}, ParentRef: "hidden"},
			Status:     platformv1alpha1.RunStatus{TranscriptRef: "secret-transcript", Conditions: []metav1.Condition{{Type: "Hidden"}}},
		}}
		return nil
	}})
	page, err := (&KubernetesResourceService{Client: c}).ListRuns(context.Background(), "team", 17, "opaque")
	if err != nil {
		t.Fatal(err)
	}
	if got.Namespace != "team" || got.Limit != 17 || got.Continue != "opaque" || page.Continue != "next" || page.ResourceVersion != "opaque-rv" || len(page.Items) != 1 {
		t.Fatalf("options/page = %#v, %#v", got, page)
	}
	assertJSONRedacted(t, page, "transcript", "conditions", "managedFields", "notify", "parentRef")
}

func TestResourceServiceCreateRunIntentOnlyAndAlreadyExistsDoesNotGet(t *testing.T) {
	scheme := resourceScheme(t)
	base := fake.NewClientBuilder().WithScheme(scheme).Build()
	service := &KubernetesResourceService{Client: base}
	req := CreateRunRequest{Name: "new", Selector: RunSelector{Project: "p", Template: "t"}, Agent: "a", Prompt: "do it"}
	created, err := service.CreateRun(context.Background(), "ns", req)
	if err != nil {
		t.Fatal(err)
	}
	if created.Intent.Selector != req.Selector {
		t.Fatalf("selector = %#v", created.Intent.Selector)
	}
	var runs platformv1alpha1.RunList
	var environments platformv1alpha1.EnvironmentList
	if err := base.List(context.Background(), &runs); err != nil || len(runs.Items) != 1 {
		t.Fatalf("runs = %d, err %v", len(runs.Items), err)
	}
	if err := base.List(context.Background(), &environments); err != nil || len(environments.Items) != 0 {
		t.Fatalf("environments = %d, err %v", len(environments.Items), err)
	}
	gets := 0
	c := interceptor.NewClient(base, interceptor.Funcs{Get: func(ctx context.Context, underlying client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		gets++
		return underlying.Get(ctx, key, obj, opts...)
	}})
	_, err = (&KubernetesResourceService{Client: c}).CreateRun(context.Background(), "ns", req)
	if !apierrors.IsAlreadyExists(err) || gets != 0 {
		t.Fatalf("error = %v, gets = %d", err, gets)
	}
}

func TestResourceServiceCreateCollisionComparesFullRunSpec(t *testing.T) {
	request := CreateRunRequest{Name: "same", Selector: RunSelector{Template: "small"}, Agent: "agent", Prompt: "prompt", CredentialProfile: "profile"}
	for _, test := range []struct {
		name   string
		mutate func(*platformv1alpha1.RunSpec)
		want   error
	}{
		{name: "same intent", mutate: func(*platformv1alpha1.RunSpec) {}},
		{name: "cancel ignored", mutate: func(spec *platformv1alpha1.RunSpec) { spec.Cancel = true }},
		{name: "empty notify normalized", mutate: func(spec *platformv1alpha1.RunSpec) { spec.Notify = []string{} }},
		{name: "parent conflicts", mutate: func(spec *platformv1alpha1.RunSpec) { spec.ParentRef = "parent" }, want: errRunIntentConflict},
		{name: "notify conflicts", mutate: func(spec *platformv1alpha1.RunSpec) { spec.Notify = []string{"parent"} }, want: errRunIntentConflict},
		{name: "selector conflicts", mutate: func(spec *platformv1alpha1.RunSpec) { spec.TemplateRef = "other" }, want: errRunIntentConflict},
		{name: "credential profile conflicts", mutate: func(spec *platformv1alpha1.RunSpec) { spec.CredentialProfileRef = "other" }, want: errRunIntentConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			spec := desiredRunSpec(request)
			test.mutate(&spec)
			existing := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: request.Name, Namespace: "ns"}, Spec: spec}
			service := &KubernetesResourceService{Client: fake.NewClientBuilder().WithScheme(resourceScheme(t)).WithObjects(existing).Build()}
			_, err := service.ResolveRunCreateCollision(context.Background(), "ns", request)
			if !errors.Is(err, test.want) || (test.want == nil && err != nil) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestResourceServiceCancelIdempotentRetriesAndErrors(t *testing.T) {
	scheme := resourceScheme(t)
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns"}}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).Build()
	updates := 0
	c := interceptor.NewClient(base, interceptor.Funcs{Update: func(ctx context.Context, underlying client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		updates++
		if updates < 3 {
			return apierrors.NewConflict(schema.GroupResource{Group: platformv1alpha1.GroupVersion.Group, Resource: "runs"}, obj.GetName(), errors.New("race"))
		}
		return underlying.Update(ctx, obj, opts...)
	}})
	service := &KubernetesResourceService{Client: c}
	got, err := service.CancelRun(context.Background(), "ns", "r", "")
	if err != nil || !got.CancelRequested || updates != 3 {
		t.Fatalf("run = %#v, updates = %d, err = %v", got, updates, err)
	}
	if _, err := service.CancelRun(context.Background(), "ns", "r", ""); err != nil || updates != 3 {
		t.Fatalf("idempotent updates = %d, err = %v", updates, err)
	}

	final := apierrors.NewConflict(schema.GroupResource{Resource: "runs"}, "r", errors.New("persistent"))
	attempts := 0
	alwaysConflict := interceptor.NewClient(fake.NewClientBuilder().WithScheme(scheme).WithObjects(run.DeepCopy()).Build(), interceptor.Funcs{
		Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
			attempts++
			return final
		},
	})
	_, err = (&KubernetesResourceService{Client: alwaysConflict}).CancelRun(context.Background(), "ns", "r", "")
	if err != final || attempts != 5 {
		t.Fatalf("final error = %v, attempts = %d", err, attempts)
	}
	_, err = service.GetRun(context.Background(), "ns", "missing")
	if !apierrors.IsNotFound(err) {
		t.Fatalf("get error = %v", err)
	}
}

func TestResourceServiceCancelRunFencesEveryRetryByUID(t *testing.T) {
	scheme := resourceScheme(t)
	newRun := func(uid types.UID, cancel bool) *platformv1alpha1.Run {
		return &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: uid}, Spec: platformv1alpha1.RunSpec{Cancel: cancel}}
	}

	t.Run("wrong UID conflicts without mutation", func(t *testing.T) {
		updates := 0
		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(newRun("current", false)).Build()
		wrapped := interceptor.NewClient(base, interceptor.Funcs{Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
			updates++
			return nil
		}})
		_, err := (&KubernetesResourceService{Client: wrapped}).CancelRun(context.Background(), "ns", "r", "stale")
		if !errors.Is(err, errRunUIDConflict) || updates != 0 {
			t.Fatalf("error = %v, updates = %d", err, updates)
		}
	})

	t.Run("replacement after conflict is rechecked", func(t *testing.T) {
		old := newRun("old", false)
		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(old).Build()
		updates := 0
		wrapped := interceptor.NewClient(base, interceptor.Funcs{Update: func(ctx context.Context, underlying client.WithWatch, obj client.Object, _ ...client.UpdateOption) error {
			updates++
			if err := underlying.Delete(ctx, old); err != nil {
				t.Fatal(err)
			}
			if err := underlying.Create(ctx, newRun("replacement", false)); err != nil {
				t.Fatal(err)
			}
			return apierrors.NewConflict(schema.GroupResource{Resource: "runs"}, "r", errors.New("replaced"))
		}})
		_, err := (&KubernetesResourceService{Client: wrapped}).CancelRun(context.Background(), "ns", "r", "old")
		if !errors.Is(err, errRunUIDConflict) || updates != 1 {
			t.Fatalf("error = %v, updates = %d", err, updates)
		}
		var retained platformv1alpha1.Run
		if err := base.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "r"}, &retained); err != nil || retained.Spec.Cancel {
			t.Fatalf("replacement = %#v, error = %v", retained, err)
		}
	})

	t.Run("exact UID idempotent retry succeeds", func(t *testing.T) {
		service := &KubernetesResourceService{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(newRun("exact", true)).Build()}
		got, err := service.CancelRun(context.Background(), "ns", "r", "exact")
		if err != nil || !got.CancelRequested || got.UID != "exact" {
			t.Fatalf("run = %#v, error = %v", got, err)
		}
	})
}

func TestResourceServiceRunSummaryBoundsPromptByRunes(t *testing.T) {
	prompt := strings.Repeat(`<>&界`, runPromptPreviewRunes/4) + strings.Repeat("x", (1<<20)-4096)
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns"}, Spec: platformv1alpha1.RunSpec{Prompt: prompt}}
	service := &KubernetesResourceService{Client: fake.NewClientBuilder().WithScheme(resourceScheme(t)).WithObjects(run).Build()}
	page, err := service.ListRunSummaries(context.Background(), "ns", 1, "")
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("page = %#v, error = %v", page, err)
	}
	preview := page.Items[0].PromptPreview
	if got := len([]rune(strings.TrimSuffix(preview, "…"))); got != runPromptPreviewRunes || !strings.HasSuffix(preview, "…") || !strings.Contains(preview, "<>&") {
		t.Fatalf("preview runes/content = %d/%q", got, preview)
	}
}

func TestResourceServiceGetRunPublishesOnlyExactAttachableEnvironmentUID(t *testing.T) {
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "ns", UID: "run-uid"},
		Status: platformv1alpha1.RunStatus{EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{
			Name: "env", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed,
		}},
	}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "ns", UID: "env-uid"},
		Status:     platformv1alpha1.EnvironmentStatus{ClaimedBy: &platformv1alpha1.RunReference{Name: "run", UID: "run-uid"}},
	}
	service := &KubernetesResourceService{Client: fake.NewClientBuilder().WithScheme(resourceScheme(t)).WithObjects(run, environment).Build()}
	got, err := service.GetRun(context.Background(), "ns", "run")
	if err != nil || !got.TerminalAvailable || got.Environment == nil || got.Environment.UID != "env-uid" {
		t.Fatalf("GetRun() = %#v, %v", got, err)
	}

	environment.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: "other", UID: "other-uid"}
	service = &KubernetesResourceService{Client: fake.NewClientBuilder().WithScheme(resourceScheme(t)).WithObjects(run, environment).Build()}
	got, err = service.GetRun(context.Background(), "ns", "run")
	if err != nil || got.TerminalAvailable || got.Environment == nil || got.Environment.UID != "" {
		t.Fatalf("foreign GetRun() = %#v, %v", got, err)
	}
}

func TestResourceServiceEnvironmentReadinessDefaultAndRedaction(t *testing.T) {
	now := metav1.NewTime(time.Now())
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "ns", UID: "uid", Generation: 2},
		Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
		Status: platformv1alpha1.EnvironmentStatus{
			ObservedGeneration: 1, Phase: platformv1alpha1.EnvironmentPhaseReady,
			Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "secret:123"}, PodName: "pod", ImageID: "sha256:secret", LastActiveAt: &now,
			Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1}},
		},
	}
	service := &KubernetesResourceService{Client: fake.NewClientBuilder().WithScheme(resourceScheme(t)).WithObjects(env).Build()}
	got, err := service.GetEnvironment(context.Background(), "ns", "env")
	if err != nil {
		t.Fatal(err)
	}
	if got.Ready || got.Backend != "pod" || got.LastActiveAt == nil {
		t.Fatalf("environment = %#v", got)
	}
	env.Status.ObservedGeneration = env.Generation
	env.Status.Conditions[0].ObservedGeneration = env.Generation
	if current := environmentDTO(env); !current.Ready {
		t.Fatalf("current-generation Ready condition mapped to ready=false: %#v", current)
	}
	assertJSONRedacted(t, got, "endpoint", "sandboxd", "podName", "image", "conditions", "managedFields", "secret")
	_, err = service.GetEnvironment(context.Background(), "ns", "missing")
	if !apierrors.IsNotFound(err) {
		t.Fatalf("get environment error = %v", err)
	}
}

func assertJSONRedacted(t *testing.T, value any, forbidden ...string) {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(string(b))
	for _, word := range forbidden {
		if strings.Contains(lower, strings.ToLower(word)) {
			t.Errorf("JSON %s contains forbidden %q", b, word)
		}
	}
}
