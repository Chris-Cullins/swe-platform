package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

const (
	maxRunWatchRV = 4096
	maxRunEvent   = 64 << 10
	runWatchLife  = 5 * time.Minute
)

type runWatchService interface {
	WatchRuns(context.Context, string, string, time.Duration) (watch.Interface, error)
}

type watchAdmission struct {
	mu         sync.Mutex
	tokens     float64
	last       time.Time
	active     int
	namespaces map[string]int
	principals map[string]int
	establish  chan struct{}
}

var processWatchAdmission = newWatchAdmission()

func newWatchAdmission() *watchAdmission {
	return &watchAdmission{tokens: 40, last: time.Now(), namespaces: map[string]int{}, principals: map[string]int{}, establish: make(chan struct{}, 32)}
}

func (a *watchAdmission) preauth() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	a.tokens += now.Sub(a.last).Seconds() * 20
	a.last = now
	if a.tokens > 40 {
		a.tokens = 40
	}
	if a.tokens < 1 {
		return false
	}
	a.tokens--
	return true
}

func (a *watchAdmission) reserve(namespace, principal string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active >= 128 || a.namespaces[namespace] >= 16 || a.principals[principal] >= 4 {
		return false
	}
	a.active++
	a.namespaces[namespace]++
	a.principals[principal]++
	return true
}

func (a *watchAdmission) release(namespace, principal string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.active--
	a.namespaces[namespace]--
	a.principals[principal]--
}

func runWatchQuery(r *http.Request) (string, error) {
	q := r.URL.Query()
	if len(q) != 3 || len(q["watch"]) != 1 || q.Get("watch") != "true" || len(q["view"]) != 1 || q.Get("view") != "summary" || len(q["resourceVersion"]) != 1 {
		return "", errors.New("watch query must be exactly watch=true&view=summary&resourceVersion=<opaque>")
	}
	rv := q.Get("resourceVersion")
	if last := r.Header.Get("Last-Event-ID"); last != "" {
		rv = last
	}
	if rv == "" || len(rv) > maxRunWatchRV {
		return "", fmt.Errorf("resourceVersion must contain 1 through %d bytes", maxRunWatchRV)
	}
	if !strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		return "", errors.New("Accept must include text/event-stream")
	}
	return rv, nil
}

func (s *Server) watchRuns(w http.ResponseWriter, r *http.Request, namespace string) {
	rv, err := runWatchQuery(r)
	if err != nil {
		writeProblem(w, 400, "invalid-query", "Invalid query", err.Error())
		return
	}
	if !s.watchAdmission.preauth() {
		writeWatchAdmission(w)
		return
	}
	select {
	case s.watchAdmission.establish <- struct{}{}:
	default:
		writeWatchAdmission(w)
		return
	}
	establishing := true
	defer func() {
		if establishing {
			<-s.watchAdmission.establish
		}
	}()
	principal := "authorized"
	access := ResourceAccess{Namespace: namespace, Verb: "watch", Resource: "runs"}
	if pa, ok := s.access.(principalAccessController); ok {
		principal, err = pa.AuthorizePrincipal(r, access, true)
	} else if s.access == nil {
		err = errUnauthenticated
	} else {
		err = s.access.Authorize(r, access, true)
	}
	if err != nil {
		writeRESTAccessError(w, err)
		return
	}
	if !s.watchAdmission.reserve(namespace, principal) {
		writeWatchAdmission(w)
		return
	}
	defer s.watchAdmission.release(namespace, principal)
	service, ok := s.resources.(runWatchService)
	if !ok {
		writeProblem(w, 503, "resource-service-unavailable", "Resource service unavailable", "Run watch is not configured")
		return
	}
	r, cleanup := s.withStreamLifecycle(r)
	defer cleanup()
	ctx, cancel := context.WithTimeout(r.Context(), runWatchLife)
	defer cancel()
	upstream, err := service.WatchRuns(ctx, namespace, rv, time.Duration(240+rand.Intn(40))*time.Second)
	if err != nil {
		if apierrors.IsResourceExpired(err) {
			writeProblem(w, 410, "run-relist", "Run relist required", "resourceVersion is no longer available")
		} else {
			s.writeResourceError(w, "watch runs", namespace, "", err)
		}
		return
	}
	defer upstream.Stop()
	// The Kubernetes watch request has been accepted. Establishment admission no
	// longer applies once downstream headers bind the stream.
	<-s.watchAdmission.establish
	establishing = false
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(200)
	controller := http.NewResponseController(w)
	if err := controller.Flush(); err != nil {
		return
	}
	write := func(event, id string, value any) bool {
		data, e := json.Marshal(value)
		if e != nil || len(data)+len(id)+len(event)+32 > maxRunEvent {
			return false
		}
		_ = controller.SetWriteDeadline(time.Now().Add(15 * time.Second))
		if id != "" {
			_, e = fmt.Fprintf(w, "id: %s\n", id)
		}
		if e == nil {
			_, e = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
		}
		if e == nil {
			e = controller.Flush()
		}
		_ = controller.SetWriteDeadline(time.Time{})
		return e == nil
	}
	process := func(event watch.Event) bool {
		switch event.Type {
		case watch.Added, watch.Modified, watch.Deleted:
			run, ok := event.Object.(*platformv1alpha1.Run)
			if !ok || run.ResourceVersion == "" || len(run.ResourceVersion) > maxRunWatchRV {
				return false
			}
			return write("run", run.ResourceVersion, RunWatchEvent{Type: string(event.Type), ResourceVersion: run.ResourceVersion, Run: runSummaryDTO(run)})
		case watch.Bookmark:
			meta, ok := event.Object.(metav1.Object)
			if !ok || meta.GetResourceVersion() == "" || len(meta.GetResourceVersion()) > maxRunWatchRV {
				return false
			}
			return write("run-checkpoint", meta.GetResourceVersion(), RunWatchCheckpoint{ResourceVersion: meta.GetResourceVersion()})
		case watch.Error:
			if staleWatchEvent(event) {
				write("run-relist", "", RunWatchCheckpoint{ResourceVersion: ""})
			}
			return false
		}
		return true
	}
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case event, open := <-upstream.ResultChan():
			if !open || !process(event) {
				return
			}
		case <-heartbeat.C:
			_ = controller.SetWriteDeadline(time.Now().Add(15 * time.Second))
			if _, err := w.Write([]byte(": heartbeat\n\n")); err != nil {
				return
			}
			if err := controller.Flush(); err != nil {
				return
			}
			_ = controller.SetWriteDeadline(time.Time{})
		}
	}
}

func staleWatchEvent(event watch.Event) bool {
	if event.Type != watch.Error {
		return false
	}
	status, ok := event.Object.(*metav1.Status)
	return ok && (status.Code == 410 || status.Reason == metav1.StatusReasonExpired)
}

func writeWatchAdmission(w http.ResponseWriter) {
	w.Header().Set("Retry-After", strconv.Itoa(1))
	writeProblem(w, 429, "watch-capacity", "Watch capacity exceeded", "retry the watch later")
}
