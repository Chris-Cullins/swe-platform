package controlplane

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

type filteringPortalAccess struct {
	deniedService string
}

func (a filteringPortalAccess) Authorize(r *http.Request, access ResourceAccess, allowSession bool) error {
	_, err := a.AuthorizePrincipal(r, access, allowSession)
	return err
}

func (a filteringPortalAccess) AuthorizePrincipal(_ *http.Request, access ResourceAccess, _ bool) (string, error) {
	if access.Resource == "environmentservices" && access.Name == a.deniedService {
		return "", errForbidden
	}
	return "user", nil
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

func (a *recordingPrincipalAccess) AuthenticatePrincipal(_ *http.Request, _ bool) (string, error) {
	principal := a.principal
	if principal == "" {
		principal = "user"
	}
	return principal, a.err
}

type countingPortalEnumerator struct{ calls int }

func (e *countingPortalEnumerator) ListPortalEnvironments(context.Context) ([]platformv1alpha1.Environment, error) {
	e.calls++
	return nil, nil
}

type staticPortalEnumerator struct{ environment platformv1alpha1.Environment }

func (e staticPortalEnumerator) ListPortalEnvironments(context.Context) ([]platformv1alpha1.Environment, error) {
	return []platformv1alpha1.Environment{*e.environment.DeepCopy()}, nil
}

type statusDenyingPortalResolver struct {
	client.Client
	updates atomic.Int64
}

func (r *statusDenyingPortalResolver) Status() client.StatusWriter {
	return statusDenyingWriter{StatusWriter: r.Client.Status(), updates: &r.updates}
}

type statusDenyingWriter struct {
	client.StatusWriter
	updates *atomic.Int64
}

func (w statusDenyingWriter) Update(context.Context, client.Object, ...client.SubResourceUpdateOption) error {
	w.updates.Add(1)
	return apierrors.NewForbidden(schema.GroupResource{Group: platformv1alpha1.GroupVersion.Group, Resource: "environments/status"}, "env", errors.New("status update denied"))
}

type headerPortalAccess struct {
	projectCalls atomic.Int64
}

func (a *headerPortalAccess) Authorize(r *http.Request, _ ResourceAccess, _ bool) error {
	_, err := a.AuthenticatePrincipal(r, true)
	return err
}

func (a *headerPortalAccess) AuthorizePrincipal(r *http.Request, access ResourceAccess, _ bool) (string, error) {
	if access.Resource == "projects" {
		a.projectCalls.Add(1)
	}
	return a.AuthenticatePrincipal(r, true)
}

func (a *headerPortalAccess) AuthenticatePrincipal(r *http.Request, _ bool) (string, error) {
	if r.Header.Get("Authorization") != "Bearer portal-user" {
		return "", errUnauthenticated
	}
	return "portal-user", nil
}

type trackingPortalDialer struct {
	address string
	active  chan struct{}
	opened  chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func newTrackingPortalDialer(address string) *trackingPortalDialer {
	return &trackingPortalDialer{address: address, active: make(chan struct{}, 1), opened: make(chan struct{}), closed: make(chan struct{})}
}

func (d *trackingPortalDialer) DialPortal(_ context.Context, lifetime context.Context, _ lifecycle.ExecutionFence, _ sandboxclient.ServiceDeclarationSnapshot, _ string) (net.Conn, error) {
	select {
	case d.active <- struct{}{}:
	default:
		return nil, errors.New("test sandboxd tunnel limit reached")
	}
	conn, err := net.DialTimeout("tcp", d.address, time.Second)
	if err != nil {
		<-d.active
		return nil, err
	}
	tracked := &trackedPortalConn{Conn: conn, release: func() {
		<-d.active
		d.once.Do(func() { close(d.closed) })
	}}
	close(d.opened)
	go func() {
		<-lifetime.Done()
		_ = tracked.Close()
	}()
	return tracked, nil
}

type trackedPortalConn struct {
	net.Conn
	release func()
	once    sync.Once
}

func (c *trackedPortalConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

type fixedPortalDialer struct{ conn net.Conn }

func (d fixedPortalDialer) DialPortal(context.Context, context.Context, lifecycle.ExecutionFence, sandboxclient.ServiceDeclarationSnapshot, string) (net.Conn, error) {
	return d.conn, nil
}

type idempotentCloseConn struct {
	net.Conn
	closes atomic.Int64
	once   sync.Once
}

type countingReader struct {
	reader io.Reader
	read   int
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += n
	return n, err
}

type deadlineRecorder struct {
	*httptest.ResponseRecorder
	deadlines []time.Time
	err       error
}

func newDeadlineRecorder() *deadlineRecorder {
	return &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
}

func (r *deadlineRecorder) SetReadDeadline(deadline time.Time) error {
	r.deadlines = append(r.deadlines, deadline)
	return r.err
}

func (c *idempotentCloseConn) Close() error {
	var err error
	c.once.Do(func() {
		c.closes.Add(1)
		err = c.Conn.Close()
	})
	return err
}

type portalWebSocketFixture struct {
	gateway       *portalGateway
	resolver      client.Client
	dialer        *trackingPortalDialer
	access        *headerPortalAccess
	route         platformv1alpha1.EnvironmentPortalRoute
	streamCancel  context.CancelFunc
	webSocketURL  string
	requestHeader http.Header
}

func newPortalWebSocketFixture(t *testing.T, leasePoll time.Duration) portalWebSocketFixture {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			kind, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if err := conn.WriteMessage(kind, payload); err != nil {
				return
			}
		}
	}))
	t.Cleanup(upstream.Close)

	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	presentationID := (&portalGateway{scheme: "http", suffix: "portal.example"}).presentationID()
	route := platformv1alpha1.EnvironmentPortalRoute{Name: "web", PresentationID: presentationID, DeclarationInstanceID: "aaaaaaaaaaaaaaaaaaaa", DeclarationRevision: 1, Locator: "bbbbbbbbbbbbbbbbbbbb", Generation: 1, Active: true}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "ns", UID: "env-uid", Generation: 1},
		Spec: platformv1alpha1.EnvironmentSpec{ProjectRef: "project", TemplateRef: "template", Services: []platformv1alpha1.EnvironmentServiceDeclaration{{
			Name: "web", InstanceID: route.DeclarationInstanceID, Revision: 1, Protocol: platformv1alpha1.EnvironmentServiceProtocolHTTP,
			TargetPort: 8080, Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject, Readiness: platformv1alpha1.EnvironmentServiceReadinessTCPConnect,
		}}},
		Status: platformv1alpha1.EnvironmentStatus{
			Phase: platformv1alpha1.EnvironmentPhaseReady, ObservedGeneration: 1, ExecutionGeneration: 1,
			Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{Epoch: 1}, PortalRoutes: []platformv1alpha1.EnvironmentPortalRoute{route},
			Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1}},
		},
	}
	project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "ns", UID: "project-uid"}}
	resolver := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment, project).Build()
	streamContext, streamCancel := context.WithCancel(context.Background())
	t.Cleanup(streamCancel)
	dialer := newTrackingPortalDialer(strings.TrimPrefix(upstream.URL, "http://"))
	access := &headerPortalAccess{}
	server := &Server{access: access, streams: streamContext}
	gateway := &portalGateway{
		server: server, resolver: resolver, enumerator: staticPortalEnumerator{environment: *environment.DeepCopy()}, dialer: dialer,
		suffix: "portal.example", scheme: "http", requests: make(chan struct{}, 1), leasePoll: leasePoll,
	}
	proxy := httptest.NewServer(gateway.wrap(http.NotFoundHandler()))
	t.Cleanup(proxy.Close)
	return portalWebSocketFixture{
		gateway: gateway, resolver: resolver, dialer: dialer, access: access, route: route, streamCancel: streamCancel,
		webSocketURL:  "ws" + strings.TrimPrefix(proxy.URL, "http") + "/socket",
		requestHeader: http.Header{"Host": []string{route.Locator + ".portal.example"}, "Authorization": []string{"Bearer portal-user"}},
	}
}

func waitPortalSlotsReleased(t *testing.T, fixture portalWebSocketFixture) {
	t.Helper()
	select {
	case <-fixture.dialer.closed:
	case <-time.After(time.Second):
		t.Fatal("sandboxd portal stream was not closed")
	}
	deadline := time.Now().Add(time.Second)
	for (len(fixture.gateway.requests) != 0 || len(fixture.dialer.active) != 0) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(fixture.gateway.requests) != 0 || len(fixture.dialer.active) != 0 {
		t.Fatalf("portal slots were not released: gateway=%d sandboxd=%d", len(fixture.gateway.requests), len(fixture.dialer.active))
	}
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
	const presentation = "0123456789abcdef"
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "env"}, Spec: platformv1alpha1.EnvironmentSpec{Services: []platformv1alpha1.EnvironmentServiceDeclaration{{Name: "web", InstanceID: "aaaaaaaaaaaaaaaaaaaa", Revision: 1, Protocol: platformv1alpha1.EnvironmentServiceProtocolHTTP, Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject}}}}
	changed, first, err := reconcilePortalRoutes(env, "web", "aaaaaaaaaaaaaaaaaaaa", 1, presentation)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || !first.Active || first.Generation != 1 {
		t.Fatalf("first route = %#v", first)
	}
	changed, stable, err := reconcilePortalRoutes(env, "web", "aaaaaaaaaaaaaaaaaaaa", 1, presentation)
	if err != nil {
		t.Fatal(err)
	}
	if changed || stable.Locator != first.Locator {
		t.Fatalf("route not stable: %#v", stable)
	}
	env.Spec.Services[0].Revision = 2
	changed, second, err := reconcilePortalRoutes(env, "web", "aaaaaaaaaaaaaaaaaaaa", 2, presentation)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || second.Locator == first.Locator || second.Generation != 2 || env.Status.PortalRoutes[0].Active {
		t.Fatalf("revision route = %#v status=%#v", second, env.Status)
	}
	env.Spec.Services = nil
	changed, _, err = reconcilePortalRoutes(env, "", "", 0, presentation)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || env.Status.PortalRoutes[1].Active {
		t.Fatal("removed route was not tombstoned")
	}
	env.Spec.Services = []platformv1alpha1.EnvironmentServiceDeclaration{{Name: "web", InstanceID: "bbbbbbbbbbbbbbbbbbbb", Revision: 2, Protocol: platformv1alpha1.EnvironmentServiceProtocolHTTP, Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject}}
	_, third, err := reconcilePortalRoutes(env, "web", "bbbbbbbbbbbbbbbbbbbb", 2, presentation)
	if err != nil {
		t.Fatal(err)
	}
	if third.Locator == second.Locator || third.Generation != 3 {
		t.Fatalf("re-added route = %#v", third)
	}
}

func TestRunPortalListFiltersExactServiceAuthorizationAndReportsCurrentState(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	execution := int64(3)
	controller := true
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "ns", UID: "run-uid"},
		Status:     platformv1alpha1.RunStatus{EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "env", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned}},
	}
	services := []platformv1alpha1.EnvironmentServiceDeclaration{
		{Name: "denied", InstanceID: "dddddddddddddddddddd", Revision: 1, Protocol: platformv1alpha1.EnvironmentServiceProtocolHTTP, TargetPort: 9090, Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject, Readiness: platformv1alpha1.EnvironmentServiceReadinessTCPConnect},
		{Name: "web", InstanceID: "wwwwwwwwwwwwwwwwwwww", Revision: 2, Protocol: platformv1alpha1.EnvironmentServiceProtocolHTTP, TargetPort: 8080, Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject, Readiness: platformv1alpha1.EnvironmentServiceReadinessTCPConnect},
	}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "ns", UID: "env-uid", Generation: 4, OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: "run", UID: "run-uid", Controller: &controller}}},
		Spec:       platformv1alpha1.EnvironmentSpec{ProjectRef: "project", Services: services},
		Status: platformv1alpha1.EnvironmentStatus{
			ObservedGeneration: 4, ExecutionGeneration: execution, Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{Epoch: 2},
			Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 4}},
			ServiceObservations: &platformv1alpha1.EnvironmentServiceObservations{ObservedGeneration: 4, ExecutionGeneration: &execution, LifecycleEpoch: 2, ObservedAt: now, Records: []platformv1alpha1.EnvironmentServiceObservation{
				{Name: "denied", DeclarationRevision: 1, State: platformv1alpha1.EnvironmentServiceObservationHealthy, Reason: platformv1alpha1.EnvironmentServiceReasonConnectionAccepted},
				{Name: "web", DeclarationRevision: 2, State: platformv1alpha1.EnvironmentServiceObservationHealthy, Reason: platformv1alpha1.EnvironmentServiceReasonConnectionAccepted},
			}},
		},
	}
	resolver := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(run, environment).Build()
	access := filteringPortalAccess{deniedService: "env.denied"}
	server := &Server{access: access}
	gateway := &portalGateway{server: server, resolver: resolver, suffix: "portal.example", scheme: "https", handoffs: make(map[string]portalSessionHandoff)}
	request := httptest.NewRequest(http.MethodGet, "https://console.example/api/v1/namespaces/ns/runs/run/portals/run-uid/env-uid", nil)
	response := httptest.NewRecorder()
	gateway.listRunPortals(response, request, "ns", RunTerminalAssociation{RunName: "run", RunUID: "run-uid", EnvironmentName: "env", EnvironmentUID: "env-uid", EnvironmentOwnership: "Owned"})
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("portal list = %d: %s", response.Code, response.Body.String())
	}
	var result RunPortalServiceList
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].Name != "web" || result.Items[0].TargetPort != 8080 || result.Items[0].Status != "Ready" || result.Items[0].URL == "" || result.Items[0].OpenURL == "" {
		t.Fatalf("portal list = %#v", result)
	}
	if strings.Contains(response.Body.String(), "denied") {
		t.Fatalf("portal list disclosed denied service: %s", response.Body.String())
	}
	openRequest := httptest.NewRequest(http.MethodPost, "https://console.example"+result.Items[0].OpenURL, nil)
	openRequest.Header.Set("Origin", "https://console.example")
	openRequest.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "browser-session"})
	openResponse := httptest.NewRecorder()
	gateway.openRunPortal(openResponse, openRequest, "ns", RunTerminalAssociation{RunName: "run", RunUID: "run-uid", EnvironmentName: "env", EnvironmentUID: "env-uid", EnvironmentOwnership: "Owned"}, "web")
	if openResponse.Code != http.StatusOK || !strings.Contains(openResponse.Body.String(), `<form method="post" action="https://`) || !strings.Contains(openResponse.Body.String(), `name="code" value="`) {
		t.Fatalf("portal opener = %d: %s", openResponse.Code, openResponse.Body.String())
	}
}

func TestDisabledRunPortalListIsReadOnlyThroughTypedAPI(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	controller := true
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "ns", UID: "run-uid"},
		Status: platformv1alpha1.RunStatus{EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{
			Name: "env", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned,
		}},
	}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{
			APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: "run", UID: "run-uid", Controller: &controller,
		}}},
		Spec: platformv1alpha1.EnvironmentSpec{Services: []platformv1alpha1.EnvironmentServiceDeclaration{{
			Name: "web", InstanceID: "wwwwwwwwwwwwwwwwwwww", Revision: 1, Protocol: platformv1alpha1.EnvironmentServiceProtocolHTTP,
			TargetPort: 8080, Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject, Readiness: platformv1alpha1.EnvironmentServiceReadinessTCPConnect,
		}}},
		Status: platformv1alpha1.EnvironmentStatus{PortalRoutes: []platformv1alpha1.EnvironmentPortalRoute{{
			Name: "web", DeclarationInstanceID: "wwwwwwwwwwwwwwwwwwww", DeclarationRevision: 1, Locator: "llllllllllllllllllll", Generation: 1, Active: true,
		}}},
	}
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(run, environment).Build()
	resolver := &statusDenyingPortalResolver{Client: base}
	resources := &fakeResources{terminalAssociation: RunTerminalAssociation{
		RunName: "run", RunUID: "run-uid", EnvironmentName: "env", EnvironmentUID: "env-uid", EnvironmentOwnership: string(platformv1alpha1.EnvironmentOwnershipOwned),
	}}
	server := NewServer(nil, ServerOptions{Access: filteringPortalAccess{}, Resources: resources, PortalResolver: resolver})
	response := resourceRequest(server, http.MethodGet, "/api/v1/namespaces/ns/runs/run/portals/run-uid/env-uid", "", "")
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("disabled portal list = %d: %s", response.Code, response.Body.String())
	}
	var result RunPortalServiceList
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].Name != "web" || result.Items[0].URL != "" || result.Items[0].OpenURL != "" {
		t.Fatalf("disabled portal list = %#v", result)
	}
	if resolver.updates.Load() != 0 {
		t.Fatalf("disabled portal list attempted %d status updates", resolver.updates.Load())
	}
}

func TestPortalServiceStateHonorsLifecycleAndFreshness(t *testing.T) {
	now := time.Now()
	execution := int64(2)
	declaration := platformv1alpha1.EnvironmentServiceDeclaration{Name: "web", Revision: 1}
	base := platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Generation: 1}, Status: platformv1alpha1.EnvironmentStatus{
		ObservedGeneration: 1, ExecutionGeneration: execution,
		Conditions:          []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1}},
		ServiceObservations: &platformv1alpha1.EnvironmentServiceObservations{ObservedGeneration: 1, ExecutionGeneration: &execution, ObservedAt: metav1.NewTime(now), Records: []platformv1alpha1.EnvironmentServiceObservation{{Name: "web", DeclarationRevision: 1, State: platformv1alpha1.EnvironmentServiceObservationHealthy}}},
	}}
	if state, _ := portalServiceState(&base, declaration, now); state != "Ready" {
		t.Fatalf("healthy state = %q", state)
	}
	pending := base.DeepCopy()
	pending.Status.Conditions = nil
	pending.Status.ServiceObservations.ExecutionGeneration = nil
	pending.Status.ServiceObservations.Records[0].State = platformv1alpha1.EnvironmentServiceObservationPending
	if state, _ := portalServiceState(pending, declaration, now); state != "Waking" {
		t.Fatalf("pending state = %q", state)
	}
	unhealthy := base.DeepCopy()
	unhealthy.Status.ServiceObservations.Records[0].State = platformv1alpha1.EnvironmentServiceObservationUnhealthy
	if state, _ := portalServiceState(unhealthy, declaration, now); state != "Unavailable" {
		t.Fatalf("unhealthy state = %q", state)
	}
	failed := base.DeepCopy()
	failed.Status.ServiceObservations.Records[0].State = platformv1alpha1.EnvironmentServiceObservationUnknown
	if state, _ := portalServiceState(failed, declaration, now); state != "Failed" {
		t.Fatalf("failed state = %q", state)
	}
	stale := base.DeepCopy()
	stale.Status.ServiceObservations.ObservedAt = metav1.NewTime(now.Add(-16 * time.Second))
	if state, _ := portalServiceState(stale, declaration, now); state != "Stale" {
		t.Fatalf("stale state = %q", state)
	}
	idle := base.DeepCopy()
	idle.Status.Lifecycle.Suspended = true
	idle.Status.Lifecycle.SuspensionReason = platformv1alpha1.EnvironmentSuspensionReasonIdle
	idle.Status.ServiceObservations = nil
	if state, _ := portalServiceState(idle, declaration, now); state != "Paused" {
		t.Fatalf("idle state = %q", state)
	}
	held := idle.DeepCopy()
	held.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Enabled: true, Revision: 1}
	if state, _ := portalServiceState(held, declaration, now); state != "Unavailable" {
		t.Fatalf("held state = %q", state)
	}
}

func TestPortalSessionHandoffIsHostBoundOneTimeAndKeepsSessionOutOfURL(t *testing.T) {
	const code = "abcdefghijklmnopqrstuvwxyz234567"
	gateway := &portalGateway{server: &Server{allowInsecureSessions: true}, suffix: "portal.example", scheme: "http", handoffReads: make(chan struct{}, 1), handoffs: make(map[string]portalSessionHandoff)}
	handler := gateway.wrap(http.NotFoundHandler())
	installCode := func() {
		gateway.handoffs[code] = portalSessionHandoff{sessionID: "console-session", locator: "abcdefghijklmnopqrst", origin: "http://console.test", expiresAt: time.Now().Add(time.Minute)}
	}
	request := func(method, target string, body io.Reader) *http.Request {
		r := httptest.NewRequest(method, target, body)
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Header.Set("Origin", "http://console.test")
		return r
	}
	for _, name := range []string{"content length", "chunked"} {
		installCode()
		body := &countingReader{reader: strings.NewReader("code=" + code + strings.Repeat("x", 128))}
		oversized := request(http.MethodPost, "http://abcdefghijklmnopqrst.portal.example/.swe/session-handoff", body)
		if name == "chunked" {
			oversized.ContentLength = -1
			oversized.TransferEncoding = []string{"chunked"}
		}
		response := newDeadlineRecorder()
		handler.ServeHTTP(response, oversized)
		if response.Code != http.StatusNotFound || response.Body.String() != portalNotFoundBody || response.Header().Get("Cache-Control") != "private, no-store" {
			t.Fatalf("%s oversized handoff = %d %q", name, response.Code, response.Body.String())
		}
		if int64(body.read) > portalSessionHandoffBodyLimit+1 {
			t.Fatalf("%s oversized handoff read %d bytes, want at most %d", name, body.read, portalSessionHandoffBodyLimit+1)
		}
		if _, ok := gateway.handoffs[code]; !ok {
			t.Fatalf("%s oversized handoff consumed the one-time code", name)
		}
	}

	installCode()
	queryBody := &countingReader{reader: strings.NewReader("code=" + code)}
	largeQuery := request(http.MethodPost, "http://abcdefghijklmnopqrst.portal.example/.swe/session-handoff?padding="+strings.Repeat("x", 1<<20), queryBody)
	largeQueryResponse := newDeadlineRecorder()
	handler.ServeHTTP(largeQueryResponse, largeQuery)
	if largeQueryResponse.Code != http.StatusNotFound || queryBody.read != 0 {
		t.Fatalf("large-query handoff = %d with %d body bytes read", largeQueryResponse.Code, queryBody.read)
	}
	if _, ok := gateway.handoffs[code]; !ok {
		t.Fatal("large-query handoff consumed the one-time code")
	}
	bareQueryBody := &countingReader{reader: strings.NewReader("code=" + code)}
	bareQueryResponse := newDeadlineRecorder()
	handler.ServeHTTP(bareQueryResponse, request(http.MethodPost, "http://abcdefghijklmnopqrst.portal.example/.swe/session-handoff?", bareQueryBody))
	if bareQueryResponse.Code != http.StatusNotFound || bareQueryBody.read != 0 {
		t.Fatalf("bare-query handoff = %d with %d body bytes read", bareQueryResponse.Code, bareQueryBody.read)
	}

	wrongMethodBody := &countingReader{reader: strings.NewReader("code=" + code)}
	wrongMethod := request(http.MethodGet, "http://abcdefghijklmnopqrst.portal.example/.swe/session-handoff", wrongMethodBody)
	wrongMethodResponse := newDeadlineRecorder()
	handler.ServeHTTP(wrongMethodResponse, wrongMethod)
	if wrongMethodResponse.Code != http.StatusNotFound || wrongMethodBody.read != 0 {
		t.Fatalf("wrong-method handoff = %d with %d body bytes read", wrongMethodResponse.Code, wrongMethodBody.read)
	}

	wrongContentType := request(http.MethodPost, "http://abcdefghijklmnopqrst.portal.example/.swe/session-handoff", strings.NewReader("code="+code))
	wrongContentType.Header.Set("Content-Type", "text/plain")
	wrongContentTypeResponse := newDeadlineRecorder()
	handler.ServeHTTP(wrongContentTypeResponse, wrongContentType)
	if wrongContentTypeResponse.Code != http.StatusNotFound {
		t.Fatalf("wrong-content-type handoff = %d", wrongContentTypeResponse.Code)
	}
	if _, ok := gateway.handoffs[code]; !ok {
		t.Fatal("wrong-content-type handoff consumed the one-time code")
	}

	saturatedBody := &countingReader{reader: strings.NewReader("code=" + code)}
	gateway.handoffReads <- struct{}{}
	saturatedResponse := newDeadlineRecorder()
	handler.ServeHTTP(saturatedResponse, request(http.MethodPost, "http://abcdefghijklmnopqrst.portal.example/.swe/session-handoff", saturatedBody))
	if saturatedResponse.Code != http.StatusNotFound || saturatedBody.read != 0 {
		t.Fatalf("saturated handoff = %d with %d body bytes read", saturatedResponse.Code, saturatedBody.read)
	}
	if _, ok := gateway.handoffs[code]; !ok {
		t.Fatal("saturated handoff consumed the one-time code")
	}
	<-gateway.handoffReads

	deadlineFailureBody := &countingReader{reader: strings.NewReader("code=" + code)}
	deadlineFailureResponse := newDeadlineRecorder()
	deadlineFailureResponse.err = errors.New("deadlines unsupported")
	func() {
		defer func() {
			if recovered := recover(); recovered != http.ErrAbortHandler {
				t.Fatalf("unsupported-deadline panic = %v", recovered)
			}
		}()
		handler.ServeHTTP(deadlineFailureResponse, request(http.MethodPost, "http://abcdefghijklmnopqrst.portal.example/.swe/session-handoff", deadlineFailureBody))
	}()
	if deadlineFailureBody.read != 0 || len(gateway.handoffReads) != 0 {
		t.Fatalf("unsupported-deadline handoff read %d body bytes and retained %d slots", deadlineFailureBody.read, len(gateway.handoffReads))
	}
	if _, ok := gateway.handoffs[code]; !ok {
		t.Fatal("unsupported-deadline handoff consumed the one-time code")
	}

	wrongOrigin := request(http.MethodPost, "http://abcdefghijklmnopqrst.portal.example/.swe/session-handoff", strings.NewReader("code="+code))
	wrongOrigin.Header.Set("Origin", "http://attacker.test")
	wrongOriginResponse := newDeadlineRecorder()
	handler.ServeHTTP(wrongOriginResponse, wrongOrigin)
	if wrongOriginResponse.Code != http.StatusNotFound || wrongOriginResponse.Body.String() != portalNotFoundBody {
		t.Fatalf("wrong-origin handoff = %d %q", wrongOriginResponse.Code, wrongOriginResponse.Body.String())
	}

	installCode()
	response := newDeadlineRecorder()
	handler.ServeHTTP(response, request(http.MethodPost, "http://abcdefghijklmnopqrst.portal.example/.swe/session-handoff", strings.NewReader("code="+code)))
	if response.Code != http.StatusOK || response.Header().Get("Location") != "" || !strings.Contains(response.Body.String(), `location.replace("/")`) {
		t.Fatalf("handoff = %d headers=%v", response.Code, response.Header())
	}
	if response.Header().Get("Cache-Control") != "private, no-store" || response.Header().Get("Referrer-Policy") != "no-referrer" {
		t.Fatalf("handoff cache/referrer headers = %v", response.Header())
	}
	if len(response.deadlines) != 2 || response.deadlines[0].IsZero() || !response.deadlines[1].IsZero() {
		t.Fatalf("handoff read deadlines = %v", response.deadlines)
	}
	cookies := response.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Value != "console-session" || !cookies[0].HttpOnly || cookies[0].Domain != "" || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatalf("handoff cookie = %#v", cookies)
	}
	replay := newDeadlineRecorder()
	handler.ServeHTTP(replay, request(http.MethodPost, "http://abcdefghijklmnopqrst.portal.example/.swe/session-handoff", strings.NewReader("code="+code)))
	if replay.Code != http.StatusNotFound {
		t.Fatalf("handoff replay = %d", replay.Code)
	}
}

func TestPortalSessionHandoffBoundsConcurrentRealServerReads(t *testing.T) {
	const code = "abcdefghijklmnopqrstuvwxyz234567"
	gateway := &portalGateway{
		server:             &Server{allowInsecureSessions: true},
		suffix:             "portal.example",
		scheme:             "http",
		handoffReadTimeout: 500 * time.Millisecond,
		handoffReads:       make(chan struct{}, 1),
		handoffs: map[string]portalSessionHandoff{code: {
			sessionID: "console-session", locator: "abcdefghijklmnopqrst", origin: "http://console.test", expiresAt: time.Now().Add(time.Minute),
		}},
	}
	server := httptest.NewServer(gateway.wrap(http.NotFoundHandler()))
	t.Cleanup(server.Close)

	openConnection := func() net.Conn {
		connection, err := net.Dial("tcp", server.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = connection.Close() })
		return connection
	}
	readResponse := func(connection net.Conn) *http.Response {
		t.Helper()
		if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodPost})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = response.Body.Close() })
		return response
	}

	stalled := openConnection()
	if _, err := io.WriteString(stalled, "POST /.swe/session-handoff HTTP/1.1\r\nHost: abcdefghijklmnopqrst.portal.example\r\nContent-Type: application/x-www-form-urlencoded\r\nOrigin: http://console.test\r\nTransfer-Encoding: chunked\r\n\r\n5\r\ncode=\r\n"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for len(gateway.handoffReads) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(gateway.handoffReads) != 1 {
		t.Fatal("stalled handoff did not occupy its read slot")
	}

	malformed := openConnection()
	if _, err := io.WriteString(malformed, "POST /.swe/session-handoff HTTP/1.1\r\nHost: abcdefghijklmnopqrst.portal.example\r\nContent-Type: text/plain\r\nOrigin: http://console.test\r\nContent-Length: 37\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	malformedResponse := readResponse(malformed)
	if malformedResponse.StatusCode != http.StatusNotFound || !malformedResponse.Close {
		t.Fatalf("malformed handoff status = %d, close = %v", malformedResponse.StatusCode, malformedResponse.Close)
	}
	if _, ok := gateway.handoffs[code]; !ok {
		t.Fatal("malformed handoff consumed the one-time code")
	}

	excess := openConnection()
	if _, err := io.WriteString(excess, "POST /.swe/session-handoff HTTP/1.1\r\nHost: abcdefghijklmnopqrst.portal.example\r\nContent-Type: application/x-www-form-urlencoded\r\nOrigin: http://console.test\r\nContent-Length: 37\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	if response := readResponse(excess); response.StatusCode != http.StatusNotFound {
		t.Fatalf("excess handoff status = %d", response.StatusCode)
	}
	if _, ok := gateway.handoffs[code]; !ok {
		t.Fatal("excess handoff consumed the one-time code")
	}

	if response := readResponse(stalled); response.StatusCode != http.StatusNotFound {
		t.Fatalf("stalled handoff status = %d", response.StatusCode)
	}
	if len(gateway.handoffReads) != 0 {
		t.Fatalf("stalled handoff retained %d read slots after its deadline", len(gateway.handoffReads))
	}
	if _, ok := gateway.handoffs[code]; !ok {
		t.Fatal("timed-out handoff consumed the one-time code")
	}

	validRequest, err := http.NewRequest(http.MethodPost, server.URL+"/.swe/session-handoff", strings.NewReader("code="+code))
	if err != nil {
		t.Fatal(err)
	}
	validRequest.Host = "abcdefghijklmnopqrst.portal.example"
	validRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	validRequest.Header.Set("Origin", "http://console.test")
	validResponse, err := server.Client().Do(validRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer validResponse.Body.Close()
	if validResponse.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(validResponse.Body)
		t.Fatalf("handoff after stalled slot release = %d: %s", validResponse.StatusCode, body)
	}
	if _, ok := gateway.handoffs[code]; ok {
		t.Fatal("successful handoff did not consume the one-time code")
	}
}

func TestExactPortalDeclarationRejectsLegacyMissingInstanceID(t *testing.T) {
	environment := &platformv1alpha1.Environment{Spec: platformv1alpha1.EnvironmentSpec{Services: []platformv1alpha1.EnvironmentServiceDeclaration{{
		Name: "web", Revision: 1, Protocol: platformv1alpha1.EnvironmentServiceProtocolHTTP, Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject,
	}}}}
	if _, ok := exactPortalDeclaration(environment, "web"); ok {
		t.Fatal("legacy declaration without instanceID received portal eligibility")
	}
	environment.Spec.Services[0].InstanceID = "invalid-with-hyphens"
	if _, ok := exactPortalDeclaration(environment, "web"); ok {
		t.Fatal("invalid instanceID received portal eligibility")
	}
}

func TestReconcilePortalRoutesRotatesChangedPresentation(t *testing.T) {
	env := &platformv1alpha1.Environment{Spec: platformv1alpha1.EnvironmentSpec{Services: []platformv1alpha1.EnvironmentServiceDeclaration{{Name: "web", InstanceID: "aaaaaaaaaaaaaaaaaaaa", Revision: 1, Protocol: platformv1alpha1.EnvironmentServiceProtocolHTTP, Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject}}}}
	_, first, err := reconcilePortalRoutes(env, "web", "aaaaaaaaaaaaaaaaaaaa", 1, "0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	changed, second, err := reconcilePortalRoutes(env, "web", "aaaaaaaaaaaaaaaaaaaa", 1, "fedcba9876543210")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || first.Locator == second.Locator || first.Generation >= second.Generation || env.Status.PortalRoutes[0].Active || second.PresentationID != "fedcba9876543210" {
		t.Fatalf("presentation rotation first=%#v second=%#v status=%#v", first, second, env.Status.PortalRoutes)
	}
}

func TestReconcileDisabledPortalRouteTombstonesAndStabilizesDenial(t *testing.T) {
	env := &platformv1alpha1.Environment{Status: platformv1alpha1.EnvironmentStatus{NextPortalRouteGeneration: 4, PortalRoutes: []platformv1alpha1.EnvironmentPortalRoute{
		{Name: "web", PresentationID: "0123456789abcdef", DeclarationInstanceID: "aaaaaaaaaaaaaaaaaaaa", DeclarationRevision: 1, Locator: "aaaaaaaaaaaaaaaaaaaa", Generation: 3, Active: true},
		{Name: "other", PresentationID: "0123456789abcdef", DeclarationInstanceID: "bbbbbbbbbbbbbbbbbbbb", DeclarationRevision: 1, Locator: "bbbbbbbbbbbbbbbbbbbb", Generation: 4, Active: true},
	}}}
	changed, denial, err := reconcileDisabledPortalRoute(env, "web", "aaaaaaaaaaaaaaaaaaaa", 1, "fedcba9876543210")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || denial.Active || denial.Generation != 5 || env.Status.PortalRoutes[0].Active || env.Status.PortalRoutes[1].Active {
		t.Fatalf("disabled denial=%#v status=%#v", denial, env.Status)
	}
	changed, stable, err := reconcileDisabledPortalRoute(env, "web", "aaaaaaaaaaaaaaaaaaaa", 1, "fedcba9876543210")
	if err != nil {
		t.Fatal(err)
	}
	if changed || stable.Generation != denial.Generation || stable.Locator != denial.Locator {
		t.Fatalf("disabled denial was not stable: %#v", stable)
	}
	env.Status.NextPortalRouteGeneration++
	env.Status.PortalRoutes = append(env.Status.PortalRoutes, platformv1alpha1.EnvironmentPortalRoute{Name: "web", PresentationID: "0123456789abcdef", DeclarationInstanceID: "aaaaaaaaaaaaaaaaaaaa", DeclarationRevision: 1, Locator: "cccccccccccccccccccc", Generation: env.Status.NextPortalRouteGeneration, Active: true})
	changed, secondDenial, err := reconcileDisabledPortalRoute(env, "web", "aaaaaaaaaaaaaaaaaaaa", 1, "fedcba9876543210")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || secondDenial.Generation <= env.Status.PortalRoutes[len(env.Status.PortalRoutes)-2].Generation || secondDenial.Generation <= denial.Generation {
		t.Fatalf("re-disabled route reused stale denial: first=%#v second=%#v status=%#v", denial, secondDenial, env.Status)
	}
}

func TestTombstoneStaleRoutePersistsGatewayDenial(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	route := platformv1alpha1.EnvironmentPortalRoute{Name: "web", Locator: "aaaaaaaaaaaaaaaaaaaa", Generation: 3, Active: true}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "ns", UID: "uid"}, Status: platformv1alpha1.EnvironmentStatus{PortalRoutes: []platformv1alpha1.EnvironmentPortalRoute{route}}}
	resolver := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(env).WithObjects(env).Build()
	gateway := &portalGateway{resolver: resolver}
	gateway.tombstoneStaleRoute(context.Background(), client.ObjectKeyFromObject(env), env.UID, route)
	var got platformv1alpha1.Environment
	if err := resolver.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.PortalRoutes) != 1 || got.Status.PortalRoutes[0].Active {
		t.Fatalf("stale route was not tombstoned: %#v", got.Status.PortalRoutes)
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

func TestPortalAuthenticatesBeforeEnvironmentEnumeration(t *testing.T) {
	access := &recordingPrincipalAccess{err: errUnauthenticated}
	enumerator := &countingPortalEnumerator{}
	gateway := &portalGateway{server: &Server{access: access}, enumerator: enumerator, requests: make(chan struct{}, 1)}
	request := httptest.NewRequest(http.MethodGet, "http://abcdefghijklmnopqrst.portal.example/", nil)
	response := httptest.NewRecorder()
	gateway.serveLocator(response, request, "abcdefghijklmnopqrst")
	if response.Code != http.StatusNotFound || response.Body.String() != portalNotFoundBody || enumerator.calls != 0 || len(gateway.requests) != 0 {
		t.Fatalf("unauthenticated portal = status %d body %q enumerations %d admissions %d", response.Code, response.Body.String(), enumerator.calls, len(gateway.requests))
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

func TestPortalWebSocketSurvivesLeaseRevalidationThenClosesOnRouteRevocation(t *testing.T) {
	leasePoll := 20 * time.Millisecond
	fixture := newPortalWebSocketFixture(t, leasePoll)
	connection, response, err := websocket.DefaultDialer.DialContext(context.Background(), fixture.webSocketURL, fixture.requestHeader)
	if err != nil {
		if response != nil {
			t.Fatalf("dial websocket: %v (%s)", err, response.Status)
		}
		t.Fatal(err)
	}
	defer connection.Close()
	select {
	case <-fixture.dialer.opened:
	case <-time.After(time.Second):
		t.Fatal("portal tunnel did not open")
	}

	// The proxy mutates only its deep request clone. Project authorization runs
	// once per complete g.authorize call. Seeing four after the admission
	// baseline proves that at least three serial lease checks completed before
	// the currently observed one, all with the bearer on the lease clone.
	initialAuthorizations := fixture.access.projectCalls.Load()
	deadline := time.Now().Add(time.Second)
	for fixture.access.projectCalls.Load() < initialAuthorizations+4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := fixture.access.projectCalls.Load(); got < initialAuthorizations+4 {
		t.Fatalf("lease authorizations = %d after admission count %d", got, initialAuthorizations)
	}
	if err := connection.WriteMessage(websocket.TextMessage, []byte("after-lease-polls")); err != nil {
		t.Fatal(err)
	}
	kind, payload, err := connection.ReadMessage()
	if err != nil || kind != websocket.TextMessage || string(payload) != "after-lease-polls" {
		t.Fatalf("post-poll echo = kind %d payload %q err %v", kind, payload, err)
	}

	key := types.NamespacedName{Namespace: "ns", Name: "env"}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var environment platformv1alpha1.Environment
		if err := fixture.resolver.Get(context.Background(), key, &environment); err != nil {
			return err
		}
		environment.Status.PortalRoutes[0].Active = false
		return fixture.resolver.Status().Update(context.Background(), &environment)
	}); err != nil {
		t.Fatal(err)
	}
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := connection.ReadMessage(); err == nil {
		t.Fatal("revoked portal route retained its WebSocket")
	}
	waitPortalSlotsReleased(t, fixture)
}

func TestPortalMismatchedWebSocketUpgradeClosesTunnel(t *testing.T) {
	fixture := newPortalWebSocketFixture(t, time.Second)
	gatewayConn, upstreamConn := net.Pipe()
	tracked := &idempotentCloseConn{Conn: gatewayConn}
	fixture.gateway.dialer = fixedPortalDialer{conn: tracked}
	serveDone := make(chan struct{})
	handler := fixture.gateway.wrap(http.NotFoundHandler())
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
		close(serveDone)
	}))
	defer proxy.Close()
	webSocketURL := "ws" + strings.TrimPrefix(proxy.URL, "http") + "/socket"

	upstreamResult := make(chan error, 1)
	go func() {
		defer upstreamConn.Close()
		request, err := http.ReadRequest(bufio.NewReader(upstreamConn))
		if err == nil {
			_ = request.Body.Close()
			_, err = io.WriteString(upstreamConn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: not-websocket\r\n\r\n")
		}
		upstreamResult <- err
	}()

	connection, response, err := websocket.DefaultDialer.DialContext(context.Background(), webSocketURL, fixture.requestHeader)
	if connection != nil {
		_ = connection.Close()
		t.Fatal("mismatched upstream upgrade established a client WebSocket")
	}
	if err == nil {
		t.Fatal("mismatched upstream upgrade returned no error")
	}
	if response != nil {
		_ = response.Body.Close()
	}
	select {
	case err := <-upstreamResult:
		if err != nil {
			t.Fatalf("script mismatched upstream response: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("mismatched upstream did not receive the proxy request")
	}
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("serveLocator did not return from mismatched upstream upgrade")
	}
	deadline := time.Now().Add(time.Second)
	for tracked.closes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := tracked.closes.Load(); got != 1 {
		t.Fatalf("gateway tunnel close count = %d, want exactly one idempotent close", got)
	}
	_ = tracked.Close()
	if got := tracked.closes.Load(); got != 1 {
		t.Fatalf("gateway tunnel close count after repeat = %d, want one", got)
	}
}

func TestPortalStreamLifecycleCancellationClosesWebSocketAndReleasesSlots(t *testing.T) {
	fixture := newPortalWebSocketFixture(t, time.Second)
	connection, response, err := websocket.DefaultDialer.DialContext(context.Background(), fixture.webSocketURL, fixture.requestHeader)
	if err != nil {
		if response != nil {
			t.Fatalf("dial websocket: %v (%s)", err, response.Status)
		}
		t.Fatal(err)
	}
	defer connection.Close()
	select {
	case <-fixture.dialer.opened:
	case <-time.After(time.Second):
		t.Fatal("portal tunnel did not open")
	}

	fixture.streamCancel()
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := connection.ReadMessage(); err == nil {
		t.Fatal("stream lifecycle cancellation retained its WebSocket")
	}
	waitPortalSlotsReleased(t, fixture)
}

func TestPortalAdmissionActivityRefreshesForCurrentExecution(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "ns", UID: "env-uid"},
		Status: platformv1alpha1.EnvironmentStatus{
			ExecutionGeneration: 1,
			Lifecycle:           platformv1alpha1.EnvironmentLifecycleStatus{Epoch: 1, Suspended: true, SuspensionReason: platformv1alpha1.EnvironmentSuspensionReasonIdle},
		},
	}
	resolver := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment).Build()
	gateway := &portalGateway{resolver: resolver, admissionHeartbeat: time.Millisecond}
	next := time.Time{}
	previousFence := lifecycle.ExecutionFence{}
	if err := gateway.recordAdmissionActivity(context.Background(), environment, &next, &previousFence); err != nil {
		t.Fatal(err)
	}
	var current platformv1alpha1.Environment
	if err := resolver.Get(context.Background(), client.ObjectKeyFromObject(environment), &current); err != nil {
		t.Fatal(err)
	}
	first := lifecycle.ActivityRequests(&current)
	if len(first) != 1 || first[0].Source != platformv1alpha1.EnvironmentActivitySourcePortal || first[0].ExecutionGeneration != 1 {
		t.Fatalf("initial portal admission activity = %#v", first)
	}
	current.Status.ExecutionGeneration = 2
	current.Status.Lifecycle.Suspended = false
	if err := resolver.Status().Update(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	if err := gateway.recordAdmissionActivity(context.Background(), &current, &next, &previousFence); err != nil {
		t.Fatal(err)
	}
	if err := resolver.Get(context.Background(), client.ObjectKeyFromObject(environment), &current); err != nil {
		t.Fatal(err)
	}
	refreshed := lifecycle.ActivityRequests(&current)
	if len(refreshed) != 1 || refreshed[0].ExecutionGeneration != 2 || refreshed[0].ID == first[0].ID {
		t.Fatalf("refreshed portal admission activity = %#v, first %#v", refreshed, first)
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
