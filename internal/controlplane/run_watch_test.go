package controlplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestRunWatchStaleBeforeAndAfterHeaders(t *testing.T) {
	request := func() *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/ns/runs?watch=true&view=summary&resourceVersion=old", nil)
		r.Header.Set("Accept", "text/event-stream")
		return r
	}
	t.Run("startup", func(t *testing.T) {
		w := httptest.NewRecorder()
		NewServer(nil, ServerOptions{Access: &principalAccess{key: "uid"}, Resources: &watchResources{fakeResources: &fakeResources{}, err: apierrors.NewResourceExpired("old")}}).Handler().ServeHTTP(w, request())
		if w.Code != http.StatusGone || strings.Contains(w.Body.String(), "old") {
			t.Fatalf("response = %d %q", w.Code, w.Body.String())
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
		if w.Code != http.StatusOK || !strings.Contains(body, "event: run-relist") || strings.Contains(body, "secret Kubernetes detail") || strings.Contains(body, "id: ") {
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
