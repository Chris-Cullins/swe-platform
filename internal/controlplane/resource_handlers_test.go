package controlplane

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type recordingAccess struct {
	calls   []ResourceAccess
	err     error
	denyGet bool
}

func (a *recordingAccess) Authorize(_ *http.Request, x ResourceAccess, _ bool) error {
	a.calls = append(a.calls, x)
	if a.denyGet && x.Verb == "get" {
		return errForbidden
	}
	return a.err
}

type fakeResources struct {
	calls               []string
	createErr, errorGet error
	created, existing   Run
	cancel              Run
	listLimit           int64
	listContinue        string
}

func (f *fakeResources) ListRuns(_ context.Context, n string, l int64, c string) (RunList, error) {
	f.calls = append(f.calls, "list")
	f.listLimit, f.listContinue = l, c
	return RunList{}, nil
}
func (f *fakeResources) CreateRun(_ context.Context, n string, r CreateRunRequest) (Run, error) {
	f.calls = append(f.calls, "create")
	return f.created, f.createErr
}
func (f *fakeResources) GetRun(_ context.Context, n, x string) (Run, error) {
	f.calls = append(f.calls, "get")
	return f.existing, f.errorGet
}
func (f *fakeResources) CancelRun(_ context.Context, n, x string) (Run, error) {
	f.calls = append(f.calls, "cancel")
	return f.cancel, nil
}
func (f *fakeResources) GetEnvironment(_ context.Context, n, x string) (Environment, error) {
	f.calls = append(f.calls, "environment")
	return Environment{Name: x}, nil
}

func resourceRequest(s *Server, method, path, body, origin string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "https://api.test"+path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer x")
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}

func TestResourceExactAuthorizationTuples(t *testing.T) {
	cases := []struct {
		method, path string
		want         ResourceAccess
	}{{"GET", "/api/v1/namespaces/ns/runs", ResourceAccess{Namespace: "ns", Verb: "list", Resource: "runs"}}, {"POST", "/api/v1/namespaces/ns/runs", ResourceAccess{Namespace: "ns", Verb: "create", Resource: "runs"}}, {"GET", "/api/v1/namespaces/ns/runs/r1", ResourceAccess{Namespace: "ns", Verb: "get", Resource: "runs", Name: "r1"}}, {"POST", "/api/v1/namespaces/ns/runs/r1/cancel", ResourceAccess{Namespace: "ns", Verb: "update", Resource: "runs", Name: "r1"}}, {"GET", "/api/v1/namespaces/ns/environments/e1", ResourceAccess{Namespace: "ns", Verb: "get", Resource: "environments", Name: "e1"}}}
	for _, tc := range cases {
		a := &recordingAccess{err: errForbidden}
		f := &fakeResources{}
		w := resourceRequest(NewServer(nil, ServerOptions{Access: a, Resources: f}), tc.method, tc.path, "", "https://api.test")
		if w.Code != 403 || len(f.calls) != 0 || len(a.calls) != 1 || a.calls[0] != tc.want {
			t.Errorf("%s %s: status=%d access=%+v service=%v", tc.method, tc.path, w.Code, a.calls, f.calls)
		}
	}
}

func TestResourceMutationOrigins(t *testing.T) {
	valid := `{"name":"r","selector":{"template":"small"},"agent":"amp","prompt":"go"}`
	for _, tc := range []struct {
		name, auth, origin string
		want               int
	}{{"cookie exact", "", "https://api.test", 201}, {"cookie scheme", "", "http://api.test", 403}, {"cookie port", "", "https://api.test:444", 403}, {"bearer no origin", "Bearer x", "", 201}} {
		t.Run(tc.name, func(t *testing.T) {
			a := &recordingAccess{}
			f := &fakeResources{}
			r := httptest.NewRequest("POST", "https://api.test/api/v1/namespaces/ns/runs", strings.NewReader(valid))
			r.Header.Set("Authorization", tc.auth)
			if tc.auth == "" {
				r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "x"})
			}
			r.Header.Set("Origin", tc.origin)
			w := httptest.NewRecorder()
			NewServer(nil, ServerOptions{Access: a, Resources: f}).Handler().ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestRunListQueryValidation(t *testing.T) {
	for _, tc := range []struct {
		query        string
		want         int
		limit        int64
		continuation string
	}{{"", 200, 50, ""}, {"?limit=17&continue=next", 200, 17, "next"}, {"?limit=0", 400, 0, ""}, {"?limit=201", 400, 0, ""}, {"?limit=x", 400, 0, ""}, {"?limit=1&limit=2", 400, 0, ""}, {"?continue=a&continue=b", 400, 0, ""}, {"?wat=1", 400, 0, ""}, {"?continue=" + strings.Repeat("x", 4097), 400, 0, ""}} {
		f := &fakeResources{}
		w := resourceRequest(NewServer(nil, ServerOptions{Access: &recordingAccess{}, Resources: f}), "GET", "/api/v1/namespaces/ns/runs"+tc.query, "", "")
		if w.Code != tc.want {
			t.Errorf("%q status=%d", tc.query, w.Code)
		}
		if tc.want == 200 && (f.listLimit != tc.limit || f.listContinue != tc.continuation) {
			t.Errorf("%q args=(%d,%q)", tc.query, f.listLimit, f.listContinue)
		}
	}
}

func TestCreateRunValidation(t *testing.T) {
	valid := `{"name":"r","selector":{"template":"small"},"agent":"amp","prompt":"go"}`
	cases := []string{"{", valid + " {}", `{"name":"r","selector":{"template":"small"},"agent":"amp","prompt":"go","x":1}`, `{"name":"r","selector":{"template":"small","x":1},"agent":"amp","prompt":"go"}`, `{"name":"r","selector":{},"agent":"amp","prompt":"go"}`, `{"name":"r","selector":{"environment":"e","project":"p"},"agent":"amp","prompt":"go"}`, `{"name":"BAD NAME","selector":{"template":"small"},"agent":"amp","prompt":"go"}`, `{"name":"r","selector":{"template":"BAD NAME"},"agent":"amp","prompt":"go"}`, `{"name":"r","selector":{"template":"small"},"agent":"","prompt":"go"}`, `{"name":"r","selector":{"template":"small"},"agent":"` + strings.Repeat("a", 129) + `","prompt":"go"}`, `{"name":"r","selector":{"template":"small"},"agent":"amp","prompt":""}`}
	for i, body := range cases {
		f := &fakeResources{}
		w := resourceRequest(NewServer(nil, ServerOptions{Access: &recordingAccess{}, Resources: f}), "POST", "/api/v1/namespaces/ns/runs", body, "")
		if w.Code != 400 || len(f.calls) != 0 {
			t.Errorf("case %d status=%d calls=%v", i, w.Code, f.calls)
		}
	}
	body := valid + strings.Repeat(" ", maxCreateRunBody-len(valid)+1)
	w := resourceRequest(NewServer(nil, ServerOptions{Access: &recordingAccess{}, Resources: &fakeResources{}}), "POST", "/api/v1/namespaces/ns/runs", body, "")
	if w.Code != 400 {
		t.Errorf("oversize status=%d", w.Code)
	}
	f := &fakeResources{created: Run{Name: "r"}}
	w = resourceRequest(NewServer(nil, ServerOptions{Access: &recordingAccess{}, Resources: f}), "POST", "/api/v1/namespaces/ns/runs", valid, "")
	if w.Code != 201 || !strings.Contains(w.Body.String(), `"name":"r"`) {
		t.Errorf("success=%d %s", w.Code, w.Body.String())
	}
}

func TestCreateCollisionAuthorizationAndIntent(t *testing.T) {
	req := CreateRunRequest{Name: "r", Selector: RunSelector{Template: "small"}, Agent: "amp", Prompt: "go"}
	body := `{"name":"r","selector":{"template":"small"},"agent":"amp","prompt":"go"}`
	exists := apierrors.NewAlreadyExists(schema.GroupResource{Group: "swe.dev", Resource: "runs"}, "r")
	for _, tc := range []struct {
		name     string
		deny     bool
		existing Run
		want     int
		calls    string
	}{{"same intent", false, Run{Name: "r", Intent: createRunIntent(req)}, 200, "create,get"}, {"different intent", false, Run{Name: "r", Intent: RunIntent{Agent: "other"}}, 409, "create,get"}, {"denied collision", true, Run{}, 409, "create"}} {
		t.Run(tc.name, func(t *testing.T) {
			a := &recordingAccess{denyGet: tc.deny}
			f := &fakeResources{createErr: exists, existing: tc.existing}
			w := resourceRequest(NewServer(nil, ServerOptions{Access: a, Resources: f}), "POST", "/api/v1/namespaces/ns/runs", body, "")
			if w.Code != tc.want || strings.Join(f.calls, ",") != tc.calls {
				t.Fatalf("status=%d calls=%v", w.Code, f.calls)
			}
			if !tc.deny && (len(a.calls) != 2 || a.calls[1] != (ResourceAccess{Namespace: "ns", Verb: "get", Resource: "runs", Name: "r"})) {
				t.Fatalf("access=%+v", a.calls)
			}
		})
	}
}

func TestCancelBodyResponseAndDelegation(t *testing.T) {
	f := &fakeResources{cancel: Run{Name: "r", CancelRequested: true}}
	s := NewServer(nil, ServerOptions{Access: &recordingAccess{}, Resources: f})
	if w := resourceRequest(s, "POST", "/api/v1/namespaces/ns/runs/r/cancel", "x", ""); w.Code != 400 || len(f.calls) != 0 {
		t.Fatalf("nonempty=%d calls=%v", w.Code, f.calls)
	}
	for range 2 {
		w := resourceRequest(s, "POST", "/api/v1/namespaces/ns/runs/r/cancel", "", "")
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"cancelRequested":true`) {
			t.Fatalf("response=%d %s", w.Code, w.Body.String())
		}
	}
	if strings.Join(f.calls, ",") != "cancel,cancel" {
		t.Fatalf("calls=%v", f.calls)
	}
}

func TestKubernetesResourceErrorsAreProblems(t *testing.T) {
	errs := []struct {
		err    error
		status int
	}{{apierrors.NewNotFound(schema.GroupResource{Resource: "runs"}, "r"), 404}, {apierrors.NewConflict(schema.GroupResource{Resource: "runs"}, "r", errors.New("conflict")), 409}, {apierrors.NewBadRequest("bad"), 400}, {apierrors.NewServiceUnavailable("down"), 503}, {errors.New("boom"), 500}}
	for _, tc := range errs {
		f := &fakeResources{errorGet: tc.err}
		w := resourceRequest(NewServer(nil, ServerOptions{Access: &recordingAccess{}, Resources: f}), "GET", "/api/v1/namespaces/ns/runs/r", "", "")
		if w.Code != tc.status || w.Header().Get("Content-Type") != "application/problem+json" {
			t.Errorf("err=%v response=%d/%q", tc.err, w.Code, w.Header().Get("Content-Type"))
		}
	}
}
