package controlplane

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

type watchResources struct {
	*fakeResources
	watch   watch.Interface
	err     error
	started chan struct{}
}

func (r *watchResources) WatchRuns(context.Context, string, string, time.Duration) (watch.Interface, error) {
	if r.started != nil {
		close(r.started)
		r.started = nil
	}
	return r.watch, r.err
}

type principalAccess struct {
	access ResourceAccess
	key    string
	err    error
}

func (a *principalAccess) Authorize(*http.Request, ResourceAccess, bool) error { return a.err }
func (a *principalAccess) AuthorizePrincipal(_ *http.Request, access ResourceAccess, _ bool) (string, error) {
	a.access = access
	return a.key, a.err
}

type revocableWatchAccess struct {
	mu     sync.Mutex
	denied bool
}

func (a *revocableWatchAccess) Authorize(*http.Request, ResourceAccess, bool) error {
	_, err := a.AuthorizePrincipal(nil, ResourceAccess{}, true)
	return err
}

func (a *revocableWatchAccess) AuthorizePrincipal(*http.Request, ResourceAccess, bool) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.denied {
		return "", errForbidden
	}
	return "uid", nil
}

func (a *revocableWatchAccess) revoke() {
	a.mu.Lock()
	a.denied = true
	a.mu.Unlock()
}

type blockingReauthAccess struct {
	calls           atomic.Int32
	initialDeadline chan bool
	reauthDeadline  chan time.Duration
	reauthCanceled  chan struct{}
}

func (a *blockingReauthAccess) Authorize(*http.Request, ResourceAccess, bool) error {
	return errors.New("unexpected non-principal authorization")
}

func (a *blockingReauthAccess) AuthorizePrincipal(r *http.Request, _ ResourceAccess, _ bool) (string, error) {
	deadline, hasDeadline := r.Context().Deadline()
	if a.calls.Add(1) == 1 {
		a.initialDeadline <- hasDeadline
		return "uid", nil
	}
	if hasDeadline {
		a.reauthDeadline <- time.Until(deadline)
	} else {
		a.reauthDeadline <- 0
	}
	<-r.Context().Done()
	close(a.reauthCanceled)
	return "", r.Context().Err()
}

func TestRunWatchQueryRequiresExactOpaqueCursorAndLastEventIDWins(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/ns/runs?watch=true&view=summary&resourceVersion=query", nil)
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("Last-Event-ID", "reconnect")
	rv, err := runWatchQuery(request)
	if err != nil || rv != "reconnect" {
		t.Fatalf("cursor/error = %q/%v", rv, err)
	}
	for _, target := range []string{
		"/api/v1/namespaces/ns/runs?watch=true&view=summary",
		"/api/v1/namespaces/ns/runs?watch=false&view=summary&resourceVersion=1",
		"/api/v1/namespaces/ns/runs?watch=true&view=summary&resourceVersion=",
		"/api/v1/namespaces/ns/runs?watch=true&view=summary&resourceVersion=1&extra=x",
	} {
		request := httptest.NewRequest(http.MethodGet, target, nil)
		request.Header.Set("Accept", "text/event-stream")
		if _, err := runWatchQuery(request); err == nil {
			t.Fatalf("query accepted: %s", target)
		}
	}
}

func TestRunWatchStreamsBoundedSummaryAndExactWatchSAR(t *testing.T) {
	upstream := watch.NewRaceFreeFake()
	access := &principalAccess{key: "uid-1"}
	started := make(chan struct{})
	server := NewServer(nil, ServerOptions{Access: access, Resources: &watchResources{fakeResources: &fakeResources{}, watch: upstream, started: started}})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/ns/runs?watch=true&view=summary&resourceVersion=10", nil)
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("Authorization", "Bearer hidden")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		server.Handler().ServeHTTP(response, request)
		close(done)
	}()
	<-started
	upstream.Add(&platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "ns", UID: "uid", Generation: 2, ResourceVersion: "11"}, Spec: platformv1alpha1.RunSpec{Agent: strings.Repeat("a", 200), Prompt: strings.Repeat("p", 1000)}})
	upstream.Stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watch did not close with upstream")
	}
	if response.Code != http.StatusOK || access.access != (ResourceAccess{Namespace: "ns", Verb: "watch", Resource: "runs"}) {
		t.Fatalf("status/access = %d/%#v", response.Code, access.access)
	}
	body := response.Body.String()
	if !strings.Contains(body, "id: 11\nevent: run") || !strings.Contains(body, `"generation":2`) || strings.Contains(body, strings.Repeat("p", 200)) {
		t.Fatalf("watch body = %q", body)
	}
}

func TestRunWatchReauthorizesBeforeForwardingEvents(t *testing.T) {
	upstream := watch.NewRaceFreeFake()
	access := &revocableWatchAccess{}
	started := make(chan struct{})
	server := NewServer(nil, ServerOptions{Access: access, Resources: &watchResources{fakeResources: &fakeResources{}, watch: upstream, started: started}})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/ns/runs?watch=true&view=summary&resourceVersion=10", nil)
	request.Header.Set("Accept", "text/event-stream")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		server.Handler().ServeHTTP(response, request)
		close(done)
	}()
	<-started
	access.revoke()
	upstream.Add(&platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "private", Namespace: "ns", UID: "uid", ResourceVersion: "11"}})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("authorization loss did not close the watch")
	}
	if strings.Contains(response.Body.String(), "private") {
		t.Fatalf("event was forwarded after authorization loss: %q", response.Body.String())
	}
}

func TestRunWatchBoundsAndCancelsStalledReauthorization(t *testing.T) {
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	upstream := &stoppedWatch{result: make(chan watch.Event, 1)}
	upstream.result <- watch.Event{Type: watch.Added, Object: &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "private", Namespace: "ns", UID: "uid", ResourceVersion: "11"}}}
	access := &blockingReauthAccess{initialDeadline: make(chan bool, 1), reauthDeadline: make(chan time.Duration, 1), reauthCanceled: make(chan struct{})}
	server := NewServer(nil, ServerOptions{Access: access, Resources: &watchResources{fakeResources: &fakeResources{}, watch: upstream}})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/ns/runs?watch=true&view=summary&resourceVersion=10", nil).WithContext(requestCtx)
	request.Header.Set("Accept", "text/event-stream")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		server.Handler().ServeHTTP(response, request)
		close(done)
	}()
	if <-access.initialDeadline {
		t.Fatal("initial authorization was unexpectedly bounded by the watch lifetime")
	}
	deadline := <-access.reauthDeadline
	if deadline <= 0 || deadline > runWatchReauthorizeMax {
		t.Fatalf("reauthorization deadline = %v, want within (0, %v]", deadline, runWatchReauthorizeMax)
	}
	cancelRequest()
	select {
	case <-access.reauthCanceled:
	case <-time.After(time.Second):
		t.Fatal("watch cancellation did not cancel stalled reauthorization")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stalled reauthorization retained the watch handler")
	}
	if !upstream.stopped.Load() {
		t.Fatal("stalled reauthorization retained the upstream watch")
	}
	if strings.Contains(response.Body.String(), "private") {
		t.Fatalf("event was forwarded while reauthorization stalled: %q", response.Body.String())
	}
}

func TestRunWatchStaleBeforeAndAfterHeaders(t *testing.T) {
	request := func() *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/ns/runs?watch=true&view=summary&resourceVersion=old", nil)
		r.Header.Set("Accept", "text/event-stream")
		return r
	}
	t.Run("startup", func(t *testing.T) {
		for name, err := range map[string]error{
			"resource expired": apierrors.NewResourceExpired("old"),
			"gone":             apierrors.NewGone("old"),
		} {
			t.Run(name, func(t *testing.T) {
				w := httptest.NewRecorder()
				NewServer(nil, ServerOptions{Access: &principalAccess{key: "uid"}, Resources: &watchResources{fakeResources: &fakeResources{}, err: err}}).Handler().ServeHTTP(w, request())
				if w.Code != http.StatusGone || strings.Contains(w.Body.String(), "old") {
					t.Fatalf("response = %d %q", w.Code, w.Body.String())
				}
			})
		}
	})
	t.Run("streamed", func(t *testing.T) {
		upstream := watch.NewRaceFreeFake()
		event := watch.Event{Type: watch.Error, Object: &metav1.Status{Status: metav1.StatusFailure, Reason: metav1.StatusReasonExpired, Code: http.StatusGone, Message: "secret Kubernetes detail"}}
		if !staleWatchEvent(event) {
			t.Fatal("test expiry event was not recognized")
		}
		started := make(chan struct{})
		w := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			NewServer(nil, ServerOptions{Access: &principalAccess{key: "uid"}, Resources: &watchResources{fakeResources: &fakeResources{}, watch: upstream, started: started}}).Handler().ServeHTTP(w, request())
			close(done)
		}()
		<-started
		time.Sleep(10 * time.Millisecond)
		upstream.Error(event.Object)
		<-done
		body := w.Body.String()
		if w.Code != http.StatusOK || !strings.Contains(body, "event: run-relist") || !strings.Contains(body, `data: {"reason":"resource-version-expired"}`) || strings.Contains(body, "secret Kubernetes detail") || strings.Contains(body, "id: ") {
			t.Fatalf("response = %d %q", w.Code, body)
		}
	})
}

type stoppedWatch struct {
	result  chan watch.Event
	stopped atomic.Bool
}

func (w *stoppedWatch) Stop() {
	if w.stopped.CompareAndSwap(false, true) {
		close(w.result)
	}
}
func (w *stoppedWatch) ResultChan() <-chan watch.Event { return w.result }

func TestRunWatchShutdownStopsUpstream(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	upstream := &stoppedWatch{result: make(chan watch.Event)}
	server := NewServer(nil, ServerOptions{Access: &principalAccess{key: "uid"}, Resources: &watchResources{fakeResources: &fakeResources{}, watch: upstream}, StreamLifecycle: lifecycle})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/ns/runs?watch=true&view=summary&resourceVersion=1", nil)
	request.Header.Set("Accept", "text/event-stream")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { server.Handler().ServeHTTP(response, request); close(done) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not close watch")
	}
	if !upstream.stopped.Load() {
		t.Fatal("upstream Stop was not called")
	}
}

func TestWatchAdmissionBoundsAndReleases(t *testing.T) {
	a := newWatchAdmission()
	for i := 0; i < 4; i++ {
		if !a.reserve("ns", "principal") {
			t.Fatalf("reservation %d rejected", i)
		}
	}
	if a.reserve("ns", "principal") {
		t.Fatal("fifth principal watch was admitted")
	}
	for i := 0; i < 4; i++ {
		a.release("ns", "principal")
	}
	if !a.reserve("ns", "principal") {
		t.Fatal("released reservation was not reusable")
	}
	a.release("ns", "principal")
}

func TestWatchAdmissionDeletesZeroCountKeysOnFinalRelease(t *testing.T) {
	a := newWatchAdmission()
	if !a.reserve("ns-a", "principal-x") || !a.reserve("ns-b", "principal-y") {
		t.Fatal("reservations rejected")
	}
	if a.active != 2 || len(a.namespaces) != 2 || len(a.principals) != 2 {
		t.Fatalf("active/namespaces/principals = %d/%d/%d", a.active, len(a.namespaces), len(a.principals))
	}
	a.release("ns-a", "principal-x")
	if _, ok := a.namespaces["ns-a"]; ok {
		t.Fatal("ns-a key retained after final release")
	}
	if _, ok := a.principals["principal-x"]; ok {
		t.Fatal("principal-x key retained after final release")
	}
	if a.namespaces["ns-b"] != 1 || a.principals["principal-y"] != 1 || a.active != 1 {
		t.Fatalf("remaining counts = ns-b:%d principal-y:%d active:%d", a.namespaces["ns-b"], a.principals["principal-y"], a.active)
	}
	a.release("ns-b", "principal-y")
	if len(a.namespaces) != 0 || len(a.principals) != 0 || a.active != 0 {
		t.Fatalf("maps/active after final release = %d/%d/%d", len(a.namespaces), len(a.principals), a.active)
	}
}

func TestWatchAdmissionOverlappingReleaseRetainsPositiveCounts(t *testing.T) {
	a := newWatchAdmission()
	for _, r := range []struct{ namespace, principal string }{
		{"ns", "principal-a"},
		{"ns", "principal-b"},
		{"ns", "principal-c"},
	} {
		if !a.reserve(r.namespace, r.principal) {
			t.Fatalf("reservation %v rejected", r)
		}
	}
	if a.namespaces["ns"] != 3 || len(a.principals) != 3 || a.active != 3 {
		t.Fatalf("initial counts = ns:%d principals:%d active:%d", a.namespaces["ns"], len(a.principals), a.active)
	}
	a.release("ns", "principal-a")
	if _, ok := a.principals["principal-a"]; ok {
		t.Fatal("principal-a key retained after final release")
	}
	if a.namespaces["ns"] != 2 {
		t.Fatalf("ns count = %d, want 2 retained for overlapping watches", a.namespaces["ns"])
	}
	if a.principals["principal-b"] != 1 || a.principals["principal-c"] != 1 || a.active != 2 {
		t.Fatalf("remaining counts = principal-b:%d principal-c:%d active:%d", a.principals["principal-b"], a.principals["principal-c"], a.active)
	}
	a.release("ns", "principal-b")
	if a.namespaces["ns"] != 1 || len(a.principals) != 1 || a.principals["principal-c"] != 1 {
		t.Fatalf("counts after second release = ns:%d principals:%d principal-c:%d", a.namespaces["ns"], len(a.principals), a.principals["principal-c"])
	}
	a.release("ns", "principal-c")
	if _, ok := a.namespaces["ns"]; ok {
		t.Fatal("ns key retained after final overlapping release")
	}
	if len(a.principals) != 0 || a.active != 0 {
		t.Fatalf("maps/active after final release = %d/%d", len(a.principals), a.active)
	}
}
