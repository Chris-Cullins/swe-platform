package controlplane

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	platform "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestProjectCollection(t *testing.T) {
	for _, tc := range []struct {
		query  string
		status int
		limit  int64
		token  string
	}{
		{"", 200, 50, ""}, {"?limit=1&continue=a%2B%2F%3D", 200, 1, "a+/="}, {"?limit=200", 200, 200, ""},
		{"?limit=0", 400, 0, ""}, {"?limit=201", 400, 0, ""}, {"?limit=", 400, 0, ""}, {"?limit=x", 400, 0, ""},
		{"?limit=1&limit=2", 400, 0, ""}, {"?namespace=other", 400, 0, ""}, {"?view=summary", 400, 0, ""},
		{"?watch=true", 400, 0, ""}, {"?continue=x&continue=y", 400, 0, ""}, {"?continue=%zz", 400, 0, ""},
		{"?continue=" + strings.Repeat("a", 4097), 400, 0, ""},
		{"?continue=" + strings.Repeat("a", 4096), 200, 50, strings.Repeat("a", 4096)},
	} {
		f := &fakeResources{}
		a := &recordingAccess{}
		w := resourceRequest(NewServer(nil, ServerOptions{Access: a, Resources: f}), "GET", "/api/v1/namespaces/team/projects"+tc.query, "", "")
		if w.Code != tc.status || len(a.calls) != 1 || a.calls[0] != (ResourceAccess{Namespace: "team", Verb: "list", Resource: "projects"}) {
			t.Fatalf("query %q status %d access %+v", tc.query, w.Code, a.calls)
		}
		if tc.status == 200 {
			if !reflect.DeepEqual(f.calls, []string{"projects:team"}) || f.listLimit != tc.limit || f.listContinue != tc.token || w.Body.String() != "{\"items\":[],\"continue\":\"next\"}\n" {
				t.Fatalf("page/options mismatch: %s %+v", w.Body.String(), f)
			}
		} else if len(f.calls) != 0 {
			t.Fatal("invalid query read resources")
		}
	}
	for _, ns := range []string{"team", "other"} {
		f := &fakeResources{}
		w := resourceRequest(NewServer(nil, ServerOptions{Access: &recordingAccess{err: errForbidden}, Resources: f}), "GET", "/api/v1/namespaces/"+ns+"/projects?limit=invalid", "", "")
		if w.Code != 403 || len(f.calls) != 0 || strings.Contains(w.Body.String(), "invalid-query") {
			t.Fatal("denial must precede parsing and reads")
		}
	}
	f := &fakeResources{listErr: apierrors.NewResourceExpired("sensitive backend detail")}
	w := resourceRequest(NewServer(nil, ServerOptions{Access: &recordingAccess{}, Resources: f}), "GET", "/api/v1/namespaces/team/projects", "", "")
	if w.Code != 410 || strings.Contains(w.Body.String(), "sensitive") {
		t.Fatal("expired cursor not sanitized")
	}
}

func TestProjectDiscoveryProjection(t *testing.T) {
	base := fake.NewClientBuilder().WithScheme(resourceScheme(t)).Build()
	for _, empty := range []bool{false, true} {
		c := interceptor.NewClient(base, interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			var got client.ListOptions
			for _, opt := range opts {
				opt.ApplyToList(&got)
			}
			if got.Namespace != "team" || got.Limit != 17 || got.Continue != "a+/=" {
				t.Fatalf("list options %+v", got)
			}
			p := list.(*platform.ProjectList)
			if !empty {
				p.Continue = "opaque/next+="
				p.Items = []platform.Project{{ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "p", UID: "immutable", Generation: 7, Annotations: map[string]string{"secret": "hidden"}}, Spec: platform.ProjectSpec{TemplateRef: "small", Repositories: []string{"https://sensitive@example.test/repo"}, EgressAllowlist: []string{"private.test"}}}}
			}
			return nil
		}, Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			t.Fatal("discovery must not resolve references or credentials")
			return nil
		}})
		page, err := (&KubernetesResourceService{Client: c}).ListProjects(context.Background(), "team", 17, "a+/=")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(page)
		want := `{"items":[{"namespace":"team","name":"p","uid":"immutable","generation":7,"defaultTemplate":"small"}],"continue":"opaque/next+="}`
		if empty {
			want = `{"items":[]}`
		}
		if string(b) != want {
			t.Fatalf("projection %s", b)
		}
	}
}

func TestProjectCollectionTenancyAndRouting(t *testing.T) {
	for _, tc := range []struct {
		namespace string
		lifecycle tenancy.Lifecycle
		status    int
	}{
		{"team", tenancy.LifecycleActive, 200}, {"other", tenancy.LifecycleActive, 403}, {"team", tenancy.LifecycleFencing, 403},
	} {
		f := &fakeResources{}
		a := TenancyAccessController{Access: &recordingAccess{}, Verifier: tenancyAccessFixture(t, tc.lifecycle, true), Namespaces: map[string]struct{}{"team": {}}}
		w := resourceRequest(NewServer(nil, ServerOptions{Access: a, Resources: f}), "GET", "/api/v1/namespaces/"+tc.namespace+"/projects", "", "")
		if w.Code != tc.status || (tc.status != 200 && len(f.calls) != 0) {
			t.Fatalf("namespace %s lifecycle %s status %d calls %v", tc.namespace, tc.lifecycle, w.Code, f.calls)
		}
	}
	for _, tc := range []struct {
		method, path string
		status       int
	}{{"GET", "/api/v1/projects", 404}, {"POST", "/api/v1/namespaces/team/projects", 405}, {"GET", "/api/v1/namespaces/team/projects/name", 404}} {
		f := &fakeResources{}
		w := resourceRequest(NewServer(nil, ServerOptions{Access: &recordingAccess{}, Resources: f}), tc.method, tc.path, "", "")
		if w.Code != tc.status || len(f.calls) != 0 {
			t.Fatalf("routing %s %s: %d", tc.method, tc.path, w.Code)
		}
	}
	f := &fakeResources{}
	w := resourceRequest(NewServer(nil, ServerOptions{Resources: f}), "GET", "/api/v1/namespaces/team/projects", "", "")
	if w.Code != 401 || len(f.calls) != 0 {
		t.Fatal("unauthenticated request must not read resources")
	}
}
