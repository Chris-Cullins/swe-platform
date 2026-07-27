package controlplane

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
)

type orderedAccess struct {
	called bool
	err    error
}

func (a *orderedAccess) Authorize(*http.Request, ResourceAccess, bool) error {
	a.called = true
	return a.err
}

func (a *orderedAccess) AuthenticatePrincipal(*http.Request, bool) (string, error) {
	a.called = true
	return "principal", a.err
}

func tenancyAccessFixture(t *testing.T, lifecycle tenancy.Lifecycle, claimed bool) *tenancy.Verifier {
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
		operation := ""
		if lifecycle == tenancy.LifecycleFencing {
			operation = tenancy.OperationOffboarding
		}
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
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()
	return &tenancy.Verifier{Reader: client, Installation: tenancy.InstallationIdentity{Key: types.NamespacedName{Namespace: "system", Name: "main"}, UID: "installation-uid"}, Mode: tenancy.ModeScoped}
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
