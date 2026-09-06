package controlplane

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
)

type orderedAccess struct {
	called bool
	err    error
	access []ResourceAccess
}

func (a *orderedAccess) Authorize(_ *http.Request, access ResourceAccess, _ bool) error {
	a.called = true
	a.access = append(a.access, access)
	return a.err
}

func (a *orderedAccess) AuthenticatePrincipal(*http.Request, bool) (string, error) {
	a.called = true
	return "principal", a.err
}

func tenancyAccessFixture(t *testing.T, lifecycle tenancy.Lifecycle, claimed bool) *tenancy.Verifier {
	t.Helper()
	operation := ""
	if lifecycle == tenancy.LifecycleFencing {
		operation = tenancy.OperationOffboarding
	}
	verifier, _ := tenancyAccessFixtureForOperation(t, lifecycle, operation, claimed)
	return verifier
}

func tenancyAccessFixtureForOperation(t *testing.T, lifecycle tenancy.Lifecycle, operation string, claimed bool) (*tenancy.Verifier, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	installation := &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: "installation-uid"}}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team", UID: "namespace-uid"}}
	objects := []runtime.Object{installation, namespace}
	if claimed {
		namespace.Annotations = map[string]string{
			tenancy.InstallationNamespaceAnnotation: "system",
			tenancy.InstallationNameAnnotation:      "main",
			tenancy.InstallationUIDAnnotation:       "installation-uid",
			tenancy.ProjectNameAnnotation:           "project",
			tenancy.ProjectUIDAnnotation:            "project-uid",
			tenancy.LifecycleAnnotation:             string(lifecycle),
			tenancy.LifecycleOperationAnnotation:    operation,
		}
		objects = append(objects, &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "team", UID: "project-uid"}})
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()
	return &tenancy.Verifier{Reader: kubeClient, Installation: tenancy.InstallationIdentity{Key: types.NamespacedName{Namespace: "system", Name: "main"}, UID: "installation-uid"}, Mode: tenancy.ModeScoped}, kubeClient
}

func TestTenancyAccessPreservesAuthorizationBeforeScopeValidation(t *testing.T) {
	underlying := &orderedAccess{err: errUnauthenticated}
	controller := TenancyAccessController{Access: underlying}
	request := httptest.NewRequest(http.MethodGet, "https://api.test/api/v1/namespaces/team/runs", nil)
	err := controller.Authorize(request, ResourceAccess{Namespace: "team", Verb: "list", Resource: "runs"}, true)
	if !underlying.called || !errors.Is(err, errUnauthenticated) {
		t.Fatalf("authorization ordering: called=%v err=%v", underlying.called, err)
	}
}

func TestTenancyAccessDelegatesAuthenticationWithoutNamespaceLookup(t *testing.T) {
	underlying := &orderedAccess{}
	controller := TenancyAccessController{Access: underlying}
	request := httptest.NewRequest(http.MethodGet, "https://portal.test/", nil)
	principal, err := controller.AuthenticatePrincipal(request, true)
	if err != nil || principal != "principal" || !underlying.called || namespaceUIDFromRequest(request) != "" {
		t.Fatalf("authentication delegation = principal %q called %v namespace %q err %v", principal, underlying.called, namespaceUIDFromRequest(request), err)
	}
}

func TestTenancyAccessRequiresConfiguredExactActiveClaim(t *testing.T) {
	access := ResourceAccess{Namespace: "team", Verb: "list", Resource: "runs"}
	for _, test := range []struct {
		name       string
		lifecycle  tenancy.Lifecycle
		namespaces map[string]struct{}
		want       error
	}{
		{name: "unlisted", lifecycle: tenancy.LifecycleActive, namespaces: map[string]struct{}{}, want: errForbidden},
		{name: "fencing", lifecycle: tenancy.LifecycleFencing, namespaces: map[string]struct{}{"team": {}}, want: errForbidden},
		{name: "active", lifecycle: tenancy.LifecycleActive, namespaces: map[string]struct{}{"team": {}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			underlying := &orderedAccess{}
			controller := TenancyAccessController{Access: underlying, Verifier: tenancyAccessFixture(t, test.lifecycle, true), Namespaces: test.namespaces}
			request := httptest.NewRequest(http.MethodGet, "https://api.test/api/v1/namespaces/team/runs", nil)
			err := controller.Authorize(request, access, true)
			if !underlying.called || !errors.Is(err, test.want) {
				t.Fatalf("called=%v err=%v want=%v", underlying.called, err, test.want)
			}
			if test.want == nil && namespaceUIDFromRequest(request) != "namespace-uid" {
				t.Fatalf("Namespace UID context = %q", namespaceUIDFromRequest(request))
			}
		})
	}
}

func TestTenancyAccessAllowsOnlyExactOffboardingTranscriptDelete(t *testing.T) {
	exactDelete := ResourceAccess{Namespace: "team", Verb: "delete", Resource: "runs", Subresource: "transcript", Name: "run"}
	tests := []struct {
		name         string
		lifecycle    tenancy.Lifecycle
		operation    string
		access       ResourceAccess
		allowSession bool
		bearer       bool
		configured   bool
		want         error
	}{
		{name: "offboarding exact bearer delete", lifecycle: tenancy.LifecycleFencing, operation: tenancy.OperationOffboarding, access: exactDelete, bearer: true, configured: true},
		{name: "offboarding session-capable delete", lifecycle: tenancy.LifecycleFencing, operation: tenancy.OperationOffboarding, access: exactDelete, allowSession: true, bearer: true, configured: true, want: errForbidden},
		{name: "offboarding delete without bearer", lifecycle: tenancy.LifecycleFencing, operation: tenancy.OperationOffboarding, access: exactDelete, configured: true, want: errForbidden},
		{name: "offboarding Changes read", lifecycle: tenancy.LifecycleFencing, operation: tenancy.OperationOffboarding, access: ResourceAccess{Namespace: "team", Verb: "get", Resource: "runs", Subresource: "changes", Name: "run"}, allowSession: true, bearer: true, configured: true, want: errForbidden},
		{name: "offboarding Changes capture", lifecycle: tenancy.LifecycleFencing, operation: tenancy.OperationOffboarding, access: ResourceAccess{Namespace: "team", Verb: "update", Resource: "runs", Subresource: "changes", Name: "run"}, bearer: true, configured: true, want: errForbidden},
		{name: "offboarding transcript get and SSE", lifecycle: tenancy.LifecycleFencing, operation: tenancy.OperationOffboarding, access: ResourceAccess{Namespace: "team", Verb: "get", Resource: "runs", Subresource: "transcript", Name: "run"}, allowSession: true, bearer: true, configured: true, want: errForbidden},
		{name: "offboarding transcript post", lifecycle: tenancy.LifecycleFencing, operation: tenancy.OperationOffboarding, access: ResourceAccess{Namespace: "team", Verb: "update", Resource: "runs", Subresource: "transcript", Name: "run"}, bearer: true, configured: true, want: errForbidden},
		{name: "offboarding other resource", lifecycle: tenancy.LifecycleFencing, operation: tenancy.OperationOffboarding, access: ResourceAccess{Namespace: "team", Verb: "delete", Resource: "environments", Subresource: "transcript", Name: "run"}, bearer: true, configured: true, want: errForbidden},
		{name: "offboarding other subresource", lifecycle: tenancy.LifecycleFencing, operation: tenancy.OperationOffboarding, access: ResourceAccess{Namespace: "team", Verb: "delete", Resource: "runs", Subresource: "terminal", Name: "run"}, bearer: true, configured: true, want: errForbidden},
		{name: "offboarding unnamed delete", lifecycle: tenancy.LifecycleFencing, operation: tenancy.OperationOffboarding, access: ResourceAccess{Namespace: "team", Verb: "delete", Resource: "runs", Subresource: "transcript"}, bearer: true, configured: true, want: errForbidden},
		{name: "offboarding unconfigured namespace", lifecycle: tenancy.LifecycleFencing, operation: tenancy.OperationOffboarding, access: exactDelete, bearer: true, want: errForbidden},
		{name: "onboarding fencing", lifecycle: tenancy.LifecycleFencing, operation: tenancy.OperationOnboarding, access: exactDelete, bearer: true, configured: true, want: errForbidden},
		{name: "fenced", lifecycle: tenancy.LifecycleFenced, access: exactDelete, bearer: true, configured: true, want: errForbidden},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			verifier, _ := tenancyAccessFixtureForOperation(t, test.lifecycle, test.operation, true)
			namespaces := map[string]struct{}{}
			if test.configured {
				namespaces["team"] = struct{}{}
			}
			underlying := &orderedAccess{}
			controller := TenancyAccessController{Access: underlying, Verifier: verifier, Namespaces: namespaces}
			request := httptest.NewRequest(http.MethodDelete, "https://api.test/api/v1/namespaces/team/runs/run/transcript", nil)
			if test.bearer {
				request.Header.Set("Authorization", "Bearer cleanup")
			}
			err := controller.Authorize(request, test.access, test.allowSession)
			if !underlying.called || !errors.Is(err, test.want) {
				t.Fatalf("called=%v err=%v want=%v", underlying.called, err, test.want)
			}
			if test.want == nil && namespaceUIDFromRequest(request) != "namespace-uid" {
				t.Fatalf("Namespace UID context = %q", namespaceUIDFromRequest(request))
			}
		})
	}
}

func TestTenancyAccessTrustedAdminIsExplicit(t *testing.T) {
	verifier := tenancyAccessFixture(t, tenancy.LifecycleActive, true)
	verifier.Mode = tenancy.ModeTrustedAdmin
	controller := TenancyAccessController{Access: &orderedAccess{}, Verifier: verifier}
	request := httptest.NewRequest(http.MethodGet, "https://api.test/api/v1/namespaces/team/runs", nil)
	if err := controller.Authorize(request, ResourceAccess{Namespace: "team", Verb: "list", Resource: "runs"}, true); err != nil {
		t.Fatal(err)
	}
	if namespaceUIDFromRequest(request) != "namespace-uid" {
		t.Fatalf("Namespace UID context = %q", namespaceUIDFromRequest(request))
	}
	unclaimed := tenancyAccessFixture(t, tenancy.LifecycleActive, false)
	unclaimed.Mode = tenancy.ModeTrustedAdmin
	controller.Verifier = unclaimed
	request = httptest.NewRequest(http.MethodGet, "https://api.test/api/v1/namespaces/team/runs", nil)
	if err := controller.Authorize(request, ResourceAccess{Namespace: "team", Verb: "list", Resource: "runs"}, true); !errors.Is(err, errForbidden) {
		t.Fatalf("trusted admin unclaimed Namespace error = %v, want forbidden", err)
	}
}

func TestTranscriptDeleteRequiresFreshTenancyProof(t *testing.T) {
	underlying := &orderedAccess{}
	access := TenancyAccessController{
		Access: underlying, Verifier: tenancyAccessFixture(t, tenancy.LifecycleActive, true),
		Namespaces: map[string]struct{}{"team": {}},
	}
	api := NewServer(nil, ServerOptions{Access: access, Runs: &fakeRunResolver{uid: "run-uid", deleting: true}, TranscriptStore: NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})})
	request := httptest.NewRequest(http.MethodDelete, "/api/v1/namespaces/team/runs/run/transcript", nil)
	request.Header.Set("Authorization", "Bearer cleanup")
	request.Header.Set(RunUIDHeader, "run-uid")
	request.Header.Set(NamespaceUIDHeader, "namespace-uid")
	response := httptest.NewRecorder()
	api.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("active tenancy cleanup = %d %q", response.Code, response.Body.String())
	}
	if len(underlying.access) != 2 {
		t.Fatalf("authorization proofs = %d, want initial and post-drain fresh proof", len(underlying.access))
	}
	for _, proof := range underlying.access {
		if proof != (ResourceAccess{Namespace: "team", Verb: "delete", Resource: "runs", Subresource: "transcript", Name: "run"}) {
			t.Fatalf("cleanup authorization = %#v", proof)
		}
	}

	onboardingVerifier, _ := tenancyAccessFixtureForOperation(t, tenancy.LifecycleFencing, tenancy.OperationOnboarding, true)
	deniedAPI := NewServer(nil, ServerOptions{
		Access: TenancyAccessController{
			Access: &orderedAccess{}, Verifier: onboardingVerifier,
			Namespaces: map[string]struct{}{"team": {}},
		},
		Runs: &fakeRunResolver{uid: "run-uid", deleting: true}, TranscriptStore: NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{}),
	})
	deniedResponse := httptest.NewRecorder()
	deniedAPI.Handler().ServeHTTP(deniedResponse, request.Clone(request.Context()))
	if deniedResponse.Code != http.StatusForbidden || len(deniedAPI.transcriptGate.entries) != 0 {
		t.Fatalf("onboarding-fencing cleanup response/gates = %d/%d", deniedResponse.Code, len(deniedAPI.transcriptGate.entries))
	}
}

func TestTranscriptDeleteReauthorizesAcrossOffboardingFencingDuringDrain(t *testing.T) {
	verifier, kubeClient := tenancyAccessFixtureForOperation(t, tenancy.LifecycleActive, "", true)
	underlying := &orderedAccess{}
	access := TenancyAccessController{Access: underlying, Verifier: verifier, Namespaces: map[string]struct{}{"team": {}}}
	run := RunIdentity{Namespace: "team", NamespaceUID: "namespace-uid", UID: "run-uid"}
	memory := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})
	appendStoreEvent(t, memory, run, "retained")
	store := &blockingUnsubscribeTranscriptStore{
		TranscriptStore:    memory,
		unsubscribeStarted: make(chan struct{}),
		releaseUnsubscribe: make(chan struct{}),
		deleteStarted:      make(chan struct{}),
	}
	released := false
	releaseDrain := func() {
		if !released {
			close(store.releaseUnsubscribe)
			released = true
		}
	}
	resolver := &mutableRunResolver{uid: "run-uid"}
	api := NewServer(nil, ServerOptions{Access: access, Runs: resolver, TranscriptStore: store})
	server := httptest.NewServer(api.Handler())
	defer server.Close()

	streamContext, cancelStream := context.WithCancel(context.Background())
	defer cancelStream()
	streamRequest, err := http.NewRequestWithContext(streamContext, http.MethodGet, server.URL+"/api/v1/namespaces/team/runs/run/transcript", nil)
	if err != nil {
		t.Fatal(err)
	}
	streamRequest.Header.Set("Authorization", "Bearer reader")
	streamRequest.Header.Set(RunUIDHeader, "run-uid")
	streamResponse, err := http.DefaultClient.Do(streamRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer streamResponse.Body.Close()
	defer releaseDrain()

	resolver.setDeleting(true)
	deleteRequest, err := http.NewRequest(http.MethodDelete, server.URL+"/api/v1/namespaces/team/runs/run/transcript", nil)
	if err != nil {
		t.Fatal(err)
	}
	deleteRequest.Header.Set("Authorization", "Bearer cleanup")
	deleteRequest.Header.Set(RunUIDHeader, "run-uid")
	deleteRequest.Header.Set(NamespaceUIDHeader, "namespace-uid")
	type deleteResult struct {
		response *http.Response
		err      error
	}
	deleteDone := make(chan deleteResult, 1)
	go func() {
		response, err := http.DefaultClient.Do(deleteRequest)
		deleteDone <- deleteResult{response: response, err: err}
	}()
	select {
	case <-store.unsubscribeStarted:
	case <-time.After(time.Second):
		t.Fatal("DELETE did not enter transcript drain")
	}

	var namespace corev1.Namespace
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: "team"}, &namespace); err != nil {
		t.Fatal(err)
	}
	namespace.Annotations[tenancy.LifecycleAnnotation] = string(tenancy.LifecycleFencing)
	namespace.Annotations[tenancy.LifecycleOperationAnnotation] = tenancy.OperationOffboarding
	if err := kubeClient.Update(context.Background(), &namespace); err != nil {
		t.Fatal(err)
	}
	releaseDrain()

	var result deleteResult
	select {
	case result = <-deleteDone:
	case <-time.After(time.Second):
		t.Fatal("DELETE did not finish after offboarding-fencing reauthorization")
	}
	if result.err != nil {
		t.Fatal(result.err)
	}
	result.response.Body.Close()
	if result.response.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d", result.response.StatusCode, http.StatusNoContent)
	}
	deleteProof := ResourceAccess{Namespace: "team", Verb: "delete", Resource: "runs", Subresource: "transcript", Name: "run"}
	proofs := 0
	for _, proof := range underlying.access {
		if proof == deleteProof {
			proofs++
		}
	}
	if proofs != 2 {
		t.Fatalf("exact DELETE authorization proofs = %d, want initial and post-drain", proofs)
	}
	if _, exists := memory.(*memoryTranscriptStore).runs[run]; exists {
		t.Fatal("exact transcript remained after offboarding-fencing cleanup")
	}
}
