package controlplane

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

const transcriptURL = "/api/v1/namespaces/project-1/runs/run-1/transcript"

func TestTranscriptReplayAndLiveStream(t *testing.T) {
	server := httptest.NewServer(newTestServer(&fakeAccess{}).Handler())
	defer server.Close()

	postEvent(t, server.URL, `{"type":"output","data":{"text":"first"}}`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+transcriptURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("stream status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	if got := response.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", got)
	}

	lines := make(chan string, 8)
	go func() {
		scanner := bufio.NewScanner(response.Body)
		for scanner.Scan() {
			if strings.HasPrefix(scanner.Text(), "data: ") {
				lines <- strings.TrimPrefix(scanner.Text(), "data: ")
			}
		}
	}()

	assertEventText(t, nextLine(t, lines), "first")
	postEvent(t, server.URL, `{"type":"output","data":{"text":"second"}}`)
	assertEventText(t, nextLine(t, lines), "second")
}

func TestTranscriptValidation(t *testing.T) {
	handler := newTestServer(&fakeAccess{}).Handler()
	request := httptest.NewRequest(http.MethodPost, transcriptURL, bytes.NewBufferString(`{"type":"output"}`))
	request.Header.Set("Authorization", "Bearer producer")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestTranscriptReconnectUsesLastEventID(t *testing.T) {
	api := newTestServer(&fakeAccess{})
	first := api.store.append("project-1/run-1-uid", "output", json.RawMessage(`{"text":"first"}`))
	api.store.append("project-1/run-1-uid", "output", json.RawMessage(`{"text":"second"}`))

	server := httptest.NewServer(api.Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+transcriptURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Last-Event-ID", strconv.FormatUint(first.ID, 10))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "data: ") {
			assertEventText(t, strings.TrimPrefix(scanner.Text(), "data: "), "second")
			return
		}
	}
	t.Fatal("stream ended before replaying the second event")
}

func TestTranscriptRejectsAnonymousBeforeRunResolutionOrStoreAccess(t *testing.T) {
	resolver := &fakeRunResolver{}
	api := NewServer(nil, ServerOptions{Access: &fakeAccess{err: errUnauthenticated}, Runs: resolver})

	for _, method := range []string{http.MethodGet, http.MethodPost} {
		request := httptest.NewRequest(method, transcriptURL, strings.NewReader(`{"type":"output","data":{}}`))
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("%s status = %d, want %d", method, response.Code, http.StatusUnauthorized)
		}
	}
	if resolver.calls != 0 || len(api.store.events) != 0 || len(api.store.subscribers) != 0 {
		t.Fatalf("unauthorized request reached backend: resolves=%d events=%d subscribers=%d", resolver.calls, len(api.store.events), len(api.store.subscribers))
	}
}

func TestTranscriptAuthorizationScopesNamespaceAndRun(t *testing.T) {
	access := &fakeAccess{allow: func(resource ResourceAccess) bool {
		return resource.Namespace == "project-a" && resource.Name == "run-1"
	}}
	api := NewServer(nil, ServerOptions{Access: access, Runs: &fakeRunResolver{}})

	allowed := httptest.NewRequest(http.MethodPost, "/api/v1/namespaces/project-a/runs/run-1/transcript", strings.NewReader(`{"type":"output","data":{"source":"a"}}`))
	allowed.Header.Set("Authorization", "Bearer producer-a")
	allowedResponse := httptest.NewRecorder()
	api.Handler().ServeHTTP(allowedResponse, allowed)
	if allowedResponse.Code != http.StatusAccepted {
		t.Fatalf("allowed append status = %d, want %d", allowedResponse.Code, http.StatusAccepted)
	}

	for _, path := range []string{
		"/api/v1/namespaces/project-b/runs/run-1/transcript",
		"/api/v1/namespaces/project-a/runs/run-2/transcript",
	} {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"type":"output","data":{"forged":true}}`))
		request.Header.Set("Authorization", "Bearer producer-a")
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf("%s status = %d, want %d", path, response.Code, http.StatusForbidden)
		}
	}
	if len(api.store.events) != 1 || len(api.store.events["project-a/run-1-uid"]) != 1 {
		t.Fatalf("forbidden append changed transcript store: %+v", api.store.events)
	}
}

func TestUnknownRunRejectedBeforeTranscriptState(t *testing.T) {
	api := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: &fakeRunResolver{err: apierrors.NewNotFound(schema.GroupResource{Group: "swe.dev", Resource: "runs"}, "run-1")}})
	request := httptest.NewRequest(http.MethodPost, transcriptURL, strings.NewReader(`{"type":"output","data":{}}`))
	request.Header.Set("Authorization", "Bearer producer")
	response := httptest.NewRecorder()
	api.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
	if len(api.store.events) != 0 || len(api.store.subscribers) != 0 {
		t.Fatal("unknown run allocated transcript state")
	}
}

func TestRecreatedRunUsesNewTranscriptIdentity(t *testing.T) {
	resolver := &fakeRunResolver{uid: "run-uid-1"}
	api := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: resolver})
	appendEvent := func() {
		request := httptest.NewRequest(http.MethodPost, transcriptURL, strings.NewReader(`{"type":"output","data":{}}`))
		request.Header.Set("Authorization", "Bearer producer")
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusAccepted {
			t.Fatalf("append status = %d, want %d", response.Code, http.StatusAccepted)
		}
	}
	appendEvent()
	resolver.uid = "run-uid-2"
	appendEvent()
	if len(api.store.events["project-1/run-uid-1"]) != 1 || len(api.store.events["project-1/run-uid-2"]) != 1 {
		t.Fatalf("recreated Run transcripts were not isolated by UID: %+v", api.store.events)
	}
}

func TestTranscriptStoreRemovesEmptySubscriberMap(t *testing.T) {
	store := newTranscriptStore()
	_, _, _, unsubscribe := store.subscribe("run-1", 0)
	unsubscribe()
	if _, ok := store.subscribers["run-1"]; ok {
		t.Fatal("empty subscriber map was retained")
	}
}

func postEvent(t *testing.T, baseURL, body string) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, baseURL+transcriptURL, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer producer")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		message, _ := io.ReadAll(response.Body)
		t.Fatalf("post status = %d, want %d: %s", response.StatusCode, http.StatusAccepted, message)
	}
}

func newTestServer(access AccessController) *Server {
	return NewServer(nil, ServerOptions{Access: access, Runs: &fakeRunResolver{}})
}

type fakeAccess struct {
	mu    sync.Mutex
	err   error
	allow func(ResourceAccess) bool
	calls []ResourceAccess
}

func (a *fakeAccess) Authorize(_ *http.Request, access ResourceAccess, _ bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, access)
	if a.err != nil {
		return a.err
	}
	if a.allow != nil && !a.allow(access) {
		return errForbidden
	}
	return nil
}

type fakeRunResolver struct {
	calls int
	err   error
	uid   types.UID
}

func (r *fakeRunResolver) ResolveRun(_ context.Context, _, name string) (types.UID, error) {
	r.calls++
	if r.uid != "" {
		return r.uid, r.err
	}
	return types.UID(name + "-uid"), r.err
}

func nextLine(t *testing.T, lines <-chan string) string {
	t.Helper()
	select {
	case line := <-lines:
		return line
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for transcript event")
		return ""
	}
}

func assertEventText(t *testing.T, line, want string) {
	t.Helper()
	var event TranscriptEvent
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		t.Fatal(err)
	}
	var data struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(event.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.Text != want {
		t.Fatalf("event text = %q, want %q", data.Text, want)
	}
}
