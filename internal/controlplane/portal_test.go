package controlplane

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	"github.com/Chris-Cullins/swe-platform/internal/sandboxclient"
)

type recordingPrincipalAccess struct {
	accesses  []ResourceAccess
	err       error
	principal string
}

func (a *recordingPrincipalAccess) Authorize(*http.Request, ResourceAccess, bool) error {
	return a.err
}

func (a *recordingPrincipalAccess) AuthorizePrincipal(_ *http.Request, access ResourceAccess, _ bool) (string, error) {
	a.accesses = append(a.accesses, access)
	principal := a.principal
	if principal == "" {
		principal = "user"
	}
	return principal, a.err
}

func TestPortalHostValidation(t *testing.T) {
	valid := "abcdefghijklmnopqrst.portal.example"
	for _, tc := range []struct {
		name, scheme, host string
		portal, valid      bool
	}{
		{"http", "http", valid, true, true},
		{"http port", "http", valid + ":8080", true, true},
		{"https", "https", valid, true, true},
		{"https port", "https", valid + ":8443", true, true},
		{"wrong scheme", "https", valid, true, false},
		{"malformed port", "http", valid + ":nope", true, false},
		{"empty port", "http", valid + ":", true, false},
		{"overflow port", "http", valid + ":65536", true, false},
		{"extra label", "http", "x." + valid, true, false},
		{"trailing dot", "http", valid + ".", true, false},
		{"suffix itself", "http", "portal.example", false, false},
		{"outside suffix", "http", "notportal.example", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := &portalGateway{server: &Server{}, suffix: "portal.example", scheme: tc.scheme}
			r := httptest.NewRequest("GET", "http://example.invalid/", nil)
			r.Host = tc.host
			if tc.scheme == "https" && tc.name != "wrong scheme" {
				r.TLS = &tls.ConnectionState{}
			}
			locator, got := g.locatorForRequest(r)
			if got != tc.portal || (locator != "") != tc.valid {
				t.Errorf("classification = (%q, %v), want portal=%v valid=%v", locator, got, tc.portal, tc.valid)
			}
		})
	}
}

func TestBoundedPortalBody(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		max  int64
		want string
		err  bool
	}{
		{"below", "abc", 4, "abc", false},
		{"exact", "abcd", 4, "abcd", false},
		{"over", "abcde", 4, "abcd", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &boundedPortalBody{ReadCloser: io.NopCloser(strings.NewReader(tc.body)), remaining: tc.max}
			got, err := io.ReadAll(body)
			if string(got) != tc.want || (err != nil) != tc.err {
				t.Fatalf("ReadAll = %q, %v", got, err)
			}
		})
	}
}

func TestReconcilePortalRoutesStableRevisionAndRemoval(t *testing.T) {
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "env"}, Spec: platformv1alpha1.EnvironmentSpec{Services: []platformv1alpha1.EnvironmentServiceDeclaration{{Name: "web", InstanceID: "aaaaaaaaaaaaaaaaaaaa", Revision: 1, Protocol: platformv1alpha1.EnvironmentServiceProtocolHTTP, Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject}}}}
	changed, first, err := reconcilePortalRoutes(env, "web", "aaaaaaaaaaaaaaaaaaaa", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || !first.Active || first.Generation != 1 {
		t.Fatalf("first route = %#v", first)
	}
	changed, stable, err := reconcilePortalRoutes(env, "web", "aaaaaaaaaaaaaaaaaaaa", 1)
	if err != nil {
		t.Fatal(err)
	}
	if changed || stable.Locator != first.Locator {
		t.Fatalf("route not stable: %#v", stable)
	}
	env.Spec.Services[0].Revision = 2
	changed, second, err := reconcilePortalRoutes(env, "web", "aaaaaaaaaaaaaaaaaaaa", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || second.Locator == first.Locator || second.Generation != 2 || env.Status.PortalRoutes[0].Active {
		t.Fatalf("revision route = %#v status=%#v", second, env.Status)
	}
	env.Spec.Services = nil
	changed, _, err = reconcilePortalRoutes(env, "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || env.Status.PortalRoutes[1].Active {
		t.Fatal("removed route was not tombstoned")
	}
	env.Spec.Services = []platformv1alpha1.EnvironmentServiceDeclaration{{Name: "web", InstanceID: "bbbbbbbbbbbbbbbbbbbb", Revision: 2, Protocol: platformv1alpha1.EnvironmentServiceProtocolHTTP, Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject}}
	_, third, err := reconcilePortalRoutes(env, "web", "bbbbbbbbbbbbbbbbbbbb", 2)
	if err != nil {
		t.Fatal(err)
	}
	if third.Locator == second.Locator || third.Generation != 3 {
		t.Fatalf("re-added route = %#v", third)
	}
}

func TestPortalNotFoundIsMinimalAndStable(t *testing.T) {
	for range 3 {
		w := httptest.NewRecorder()
		portal404(w)
		if w.Code != 404 || w.Body.String() != portalNotFoundBody || w.Header().Get("Cache-Control") != "private, no-store" {
			t.Fatalf("response %d %q", w.Code, w.Body.String())
		}
	}
}

func TestPortalHostUsesHostLocalSessionExchange(t *testing.T) {
	sessions := &fakeSessions{create: func(r *http.Request) (Session, string, error) {
		if r.Header.Get("Authorization") != "Bearer explicit" {
			return Session{}, "", errUnauthenticated
		}
		return Session{Authenticated: true, Username: "portal-user"}, "host-local-session", nil
	}}
	server := &Server{sessions: sessions, allowInsecureSessions: true}
	gateway := &portalGateway{server: server, suffix: "portal.example", scheme: "http"}
	handler := gateway.wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "console", http.StatusTeapot) }))
	req := httptest.NewRequest(http.MethodPost, "http://abcdefghijklmnopqrst.portal.example/api/v1/session", nil)
	req.Header.Set("Authorization", "Bearer explicit")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("session exchange status = %d body=%s", w.Code, w.Body.String())
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != sessionCookieName || cookies[0].Value != "host-local-session" || cookies[0].Domain != "" || cookies[0].SameSite != http.SameSiteStrictMode || !cookies[0].HttpOnly {
		t.Fatalf("portal session cookie = %#v", cookies)
	}

	badHost := httptest.NewRequest(http.MethodPost, "http://short.portal.example/api/v1/session", nil)
	badHost.Header.Set("Authorization", "Bearer explicit")
	bad := httptest.NewRecorder()
	handler.ServeHTTP(bad, badHost)
	if bad.Code != http.StatusNotFound || bad.Body.String() != portalNotFoundBody {
		t.Fatalf("malformed portal session host = %d %q", bad.Code, bad.Body.String())
	}
}

func TestPortalAssociationUsesOnlyCurrentRunOrProject(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	controller := true
	project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "ns", UID: "project-uid"}}
	baseEnvironment := platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{ProjectRef: "project"}}
	claimedRun := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "ns", UID: "run-uid"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateRunning, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "env", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed}}}
	ownedRun := claimedRun.DeepCopy()
	ownedRun.Status.EnvironmentRef.Ownership = platformv1alpha1.EnvironmentOwnershipOwned

	for _, tc := range []struct {
		name        string
		environment platformv1alpha1.Environment
		run         *platformv1alpha1.Run
		wantErr     bool
		want        ResourceAccess
	}{
		{name: "current claim", environment: func() platformv1alpha1.Environment {
			e := *baseEnvironment.DeepCopy()
			e.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: "run", UID: "run-uid"}
			return e
		}(), run: claimedRun, want: ResourceAccess{Namespace: "ns", Verb: "get", Resource: "runs", Name: "run"}},
		{name: "current owner", environment: func() platformv1alpha1.Environment {
			e := *baseEnvironment.DeepCopy()
			e.OwnerReferences = []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: "run", UID: "run-uid", Controller: &controller}}
			return e
		}(), run: ownedRun, want: ResourceAccess{Namespace: "ns", Verb: "get", Resource: "runs", Name: "run"}},
		{name: "stale claim uid", environment: func() platformv1alpha1.Environment {
			e := *baseEnvironment.DeepCopy()
			e.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: "run", UID: "stale"}
			return e
		}(), run: claimedRun, wantErr: true},
		{name: "terminal owner", environment: func() platformv1alpha1.Environment {
			e := *baseEnvironment.DeepCopy()
			e.OwnerReferences = []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: "run", UID: "run-uid", Controller: &controller}}
			return e
		}(), run: func() *platformv1alpha1.Run {
			r := ownedRun.DeepCopy()
			r.Status.State = platformv1alpha1.RunStateSucceeded
			return r
		}(), wantErr: true},
		{name: "released claim uses project", environment: baseEnvironment, run: claimedRun, want: ResourceAccess{Namespace: "ns", Verb: "get", Resource: "projects", Name: "project"}},
		{name: "template owner uses project", environment: func() platformv1alpha1.Environment {
			e := *baseEnvironment.DeepCopy()
			e.OwnerReferences = []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "EnvironmentTemplate", Name: "template", UID: "template-uid", Controller: &controller}}
			return e
		}(), want: ResourceAccess{Namespace: "ns", Verb: "get", Resource: "projects", Name: "project"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objects := []runtime.Object{project.DeepCopy()}
			if tc.run != nil {
				objects = append(objects, tc.run.DeepCopy())
			}
			access := &recordingPrincipalAccess{}
			gateway := &portalGateway{server: &Server{access: access}, resolver: fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()}
			err := gateway.authorizeAssociation(httptest.NewRequest(http.MethodGet, "http://portal.test/", nil), &tc.environment, access)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && (len(access.accesses) != 1 || access.accesses[0] != tc.want) {
				t.Fatalf("accesses = %#v, want %#v", access.accesses, tc.want)
			}
		})
	}
}

func TestPortalAuthorizationUsesExactEnvironmentServiceAndProjectSARs(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "ns", UID: "project-uid"}}
	environment := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{ProjectRef: "project"}}
	access := &recordingPrincipalAccess{}
	gateway := &portalGateway{server: &Server{access: access}, resolver: fake.NewClientBuilder().WithScheme(scheme).WithObjects(project).Build()}
	request := httptest.NewRequest(http.MethodGet, "http://abcdefghijklmnopqrst.portal.example/", nil)
	if err := gateway.authorize(request, "ns", "env", "web", environment); err != nil {
		t.Fatal(err)
	}
	want := []ResourceAccess{
		{Namespace: "ns", Verb: "get", Resource: "environments", Subresource: "portal", Name: "env"},
		{Namespace: "ns", Verb: "get", Resource: "environmentservices", Subresource: "portal", Name: "env.web"},
		{Namespace: "ns", Verb: "get", Resource: "projects", Name: "project"},
	}
	if len(access.accesses) != len(want) {
		t.Fatalf("accesses = %#v", access.accesses)
	}
	for i := range want {
		if access.accesses[i] != want[i] {
			t.Fatalf("access %d = %#v, want %#v", i, access.accesses[i], want[i])
		}
	}

	bootstrap := &recordingPrincipalAccess{principal: "bootstrap"}
	gateway.server.access = bootstrap
	if err := gateway.authorize(request, "ns", "env", "web", environment); !errors.Is(err, errForbidden) || len(bootstrap.accesses) != 1 {
		t.Fatalf("bootstrap authorization = %v, accesses %#v", err, bootstrap.accesses)
	}
}

func TestPortalProxyStripsGatewayCredentialsAndUnsafeCookies(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || strings.Contains(r.Header.Get("Cookie"), sessionCookieName) {
			t.Errorf("gateway credential reached upstream: authorization=%q cookie=%q", r.Header.Get("Authorization"), r.Header.Get("Cookie"))
		}
		if r.Header.Get("Cookie") != "application=visible" || r.Header.Get("X-Spoofed") != "" || r.Header.Get("Forwarded") != "" || r.Header.Get("X-Forwarded-Host") != "abcdefghijklmnopqrst.portal.example" || r.Header.Get("X-Forwarded-Proto") != "http" {
			t.Errorf("upstream headers = %#v", r.Header)
		}
		w.Header().Add("Set-Cookie", "application=kept; Path=/; HttpOnly")
		w.Header().Add("Set-Cookie", sessionCookieName+"=replace; Path=/")
		w.Header().Add("Set-Cookie", "sibling=bad; Domain=portal.example; Path=/")
		w.Header().Set("Connection", "X-Response-Hop")
		w.Header().Set("X-Response-Hop", "remove")
		_ = json.NewEncoder(w).Encode(map[string]string{"ok": "yes"})
	}))
	defer upstream.Close()
	address := strings.TrimPrefix(upstream.URL, "http://")

	gateway := &portalGateway{server: &Server{}, scheme: "http"}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			t.Error(err)
			portal404(w)
			return
		}
		gateway.proxy(w, r, conn)
	}))
	defer proxy.Close()
	req, err := http.NewRequest(http.MethodGet, proxy.URL+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "abcdefghijklmnopqrst.portal.example"
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Cookie", sessionCookieName+"=secret; application=visible")
	req.Header.Set("Forwarded", "for=attacker")
	req.Header.Set("X-Forwarded-For", "attacker")
	req.Header.Set("Connection", "X-Spoofed")
	req.Header.Set("X-Spoofed", "remove")
	resp, err := proxy.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("response status/headers = %s %#v", resp.Status, resp.Header)
	}
	setCookies := resp.Header.Values("Set-Cookie")
	if len(setCookies) != 1 || !strings.HasPrefix(setCookies[0], "application=kept") {
		t.Fatalf("Set-Cookie = %#v", setCookies)
	}
	responseHeaders := http.Header{"Connection": []string{"X-Response-Hop"}, "X-Response-Hop": []string{"remove"}}
	stripHopHeaders(responseHeaders)
	if responseHeaders.Get("Connection") != "" || responseHeaders.Get("X-Response-Hop") != "" {
		t.Fatalf("connection-nominated response headers survived: %#v", responseHeaders)
	}
}

func TestPortalProxyWebSocketEcho(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		kind, payload, err := conn.ReadMessage()
		if err == nil {
			err = conn.WriteMessage(kind, payload)
		}
		if err != nil {
			t.Logf("upstream websocket closed: %v", err)
		}
	}))
	defer upstream.Close()
	address := strings.TrimPrefix(upstream.URL, "http://")
	gateway := &portalGateway{server: &Server{}, scheme: "http"}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			portal404(w)
			return
		}
		gateway.proxy(w, r, conn)
	}))
	defer proxy.Close()
	wsURL := "ws" + strings.TrimPrefix(proxy.URL, "http") + "/socket"
	header := http.Header{"Host": []string{"abcdefghijklmnopqrst.portal.example"}}
	conn, response, err := websocket.DefaultDialer.DialContext(context.Background(), wsURL, header)
	if err != nil {
		if response != nil {
			t.Fatalf("dial websocket: %v (%s)", err, response.Status)
		}
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	kind, payload, err := conn.ReadMessage()
	if err != nil || kind != websocket.TextMessage || string(payload) != "hello" {
		t.Fatalf("echo = kind %d payload %q err %v", kind, payload, err)
	}
}

func TestPortalLeaseClosesOnRouteRevocation(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	route := platformv1alpha1.EnvironmentPortalRoute{Name: "web", DeclarationInstanceID: "aaaaaaaaaaaaaaaaaaaa", DeclarationRevision: 1, Locator: "bbbbbbbbbbbbbbbbbbbb", Generation: 1, Active: true}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "ns", UID: "env-uid", Generation: 1},
		Spec:       platformv1alpha1.EnvironmentSpec{ProjectRef: "project", TemplateRef: "template", Services: []platformv1alpha1.EnvironmentServiceDeclaration{{Name: "web", InstanceID: route.DeclarationInstanceID, Revision: 1, Protocol: platformv1alpha1.EnvironmentServiceProtocolHTTP, TargetPort: 8080, Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject, Readiness: platformv1alpha1.EnvironmentServiceReadinessTCPConnect}}},
		Status:     platformv1alpha1.EnvironmentStatus{ExecutionGeneration: 1, PortalRoutes: []platformv1alpha1.EnvironmentPortalRoute{route}},
	}
	project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "ns", UID: "project-uid"}}
	resolver := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment, project).Build()
	current := environment.DeepCopy()
	current.Status.PortalRoutes[0].Active = false
	if err := resolver.Status().Update(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	access := &recordingPrincipalAccess{}
	gateway := &portalGateway{server: &Server{access: access}, resolver: resolver, leasePoll: 10 * time.Millisecond}
	clientConn, peer := net.Pipe()
	defer peer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go gateway.revalidatePortalLease(ctx, httptest.NewRequest(http.MethodGet, "http://bbbbbbbbbbbbbbbbbbbb.portal.example/", nil), clientConn, types.NamespacedName{Namespace: "ns", Name: "env"}, lifecycle.CaptureExecutionFence(environment), sandboxclient.CaptureServiceDeclarationSnapshot(environment), route)
	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := peer.Read(one[:]); err == nil {
		t.Fatal("revoked route did not close established portal lease")
	}
}
