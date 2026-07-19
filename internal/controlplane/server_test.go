package controlplane

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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

	postEvent(t, server.URL, `{"source":"adapter","idempotencyKey":"first","type":"output","data":{"text":"first"}}`)

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
	if got := response.Header.Get("X-Accel-Buffering"); got != "no" {
		t.Fatalf("X-Accel-Buffering = %q, want no", got)
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
	postEvent(t, server.URL, `{"source":"adapter","idempotencyKey":"second","type":"output","data":{"text":"second"}}`)
	assertEventText(t, nextLine(t, lines), "second")
}

func TestTranscriptStreamSendsHeartbeat(t *testing.T) {
	api := NewServer(nil, ServerOptions{
		Access:                      &fakeAccess{},
		Runs:                        &fakeRunResolver{},
		TranscriptStore:             NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{}),
		TranscriptHeartbeatInterval: time.Millisecond,
	})
	server := httptest.NewServer(api.Handler())
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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

	scanner := bufio.NewScanner(response.Body)
	if !scanner.Scan() {
		t.Fatalf("stream ended before heartbeat: %v", scanner.Err())
	}
	if got := scanner.Text(); got != ": ping" {
		t.Fatalf("heartbeat = %q, want %q", got, ": ping")
	}
}

func TestTranscriptStreamLifecycleUnsubscribes(t *testing.T) {
	streamLifecycle, cancelStreams := context.WithCancel(context.Background())
	store := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{}).(*memoryTranscriptStore)
	api := NewServer(nil, ServerOptions{
		Access:          &fakeAccess{},
		Runs:            &fakeRunResolver{},
		TranscriptStore: store,
		StreamLifecycle: streamLifecycle,
	})
	server := httptest.NewServer(api.Handler())
	defer server.Close()

	response, err := http.Get(server.URL + transcriptURL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	run := RunIdentity{Namespace: "project-1", UID: "run-1-uid"}
	store.mu.Lock()
	subscribers := len(store.subscribers[run])
	store.mu.Unlock()
	if subscribers != 1 {
		t.Fatalf("subscribers before shutdown = %d, want 1", subscribers)
	}

	cancelStreams()
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, response.Body)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("read canceled SSE stream: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SSE stream did not exit after lifecycle cancellation")
	}
	store.mu.Lock()
	_, subscribed := store.subscribers[run]
	store.mu.Unlock()
	if subscribed {
		t.Fatal("SSE subscription remained after lifecycle cancellation")
	}
}

func TestTranscriptSharedStoreFansOutAcrossServers(t *testing.T) {
	store := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})
	options := ServerOptions{Access: &fakeAccess{}, Runs: &fakeRunResolver{}, TranscriptStore: store}
	producer := httptest.NewServer(NewServer(nil, options).Handler())
	defer producer.Close()
	consumer := httptest.NewServer(NewServer(nil, options).Handler())
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, consumer.URL+transcriptURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body := `{"source":"adapter","idempotencyKey":"shared","type":"output","data":{"text":"shared"}}`
	postEvent(t, producer.URL, body)
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "data: ") {
			assertEventText(t, strings.TrimPrefix(scanner.Text(), "data: "), "shared")
			return
		}
	}
	t.Fatal("shared store stream ended before replay")
}

func TestTranscriptAppendIsIdempotent(t *testing.T) {
	server := httptest.NewServer(newTestServer(&fakeAccess{}).Handler())
	defer server.Close()
	body := `{"source":"adapter","idempotencyKey":"event-1","type":"output","data":{"text":"same"}}`

	post := func(body string) *http.Response {
		request, err := http.NewRequest(http.MethodPost, server.URL+transcriptURL, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer producer")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	first := post(body)
	defer first.Body.Close()
	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first status = %d, want %d", first.StatusCode, http.StatusCreated)
	}
	retry := post(body)
	defer retry.Body.Close()
	if retry.StatusCode != http.StatusOK || retry.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("retry status/header = %d/%q", retry.StatusCode, retry.Header.Get("Idempotent-Replayed"))
	}
	conflict := post(`{"source":"adapter","idempotencyKey":"event-1","type":"output","data":{"text":"changed"}}`)
	defer conflict.Body.Close()
	if conflict.StatusCode != http.StatusConflict {
		t.Fatalf("conflict status = %d, want %d", conflict.StatusCode, http.StatusConflict)
	}
}

func TestTranscriptLegacyAppendRemainsNonIdempotent(t *testing.T) {
	api := newTestServer(&fakeAccess{})
	body := `{"type":"output","data":{"text":"legacy"}}`
	for range 2 {
		request := httptest.NewRequest(http.MethodPost, transcriptURL, strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer producer")
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusAccepted {
			t.Fatalf("legacy status = %d, want %d", response.Code, http.StatusAccepted)
		}
	}
	store := api.store.(*memoryTranscriptStore)
	run := RunIdentity{Namespace: "project-1", UID: "run-1-uid"}
	if len(store.runs[run].events) != 2 {
		t.Fatalf("legacy retained events = %d, want 2", len(store.runs[run].events))
	}
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

func TestTranscriptRejectsOversizedRequest(t *testing.T) {
	handler := newTestServer(&fakeAccess{}).Handler()
	body := `{"source":"adapter","idempotencyKey":"large","type":"output","data":"` + strings.Repeat("x", (1<<20)+1) + `"}`
	request := httptest.NewRequest(http.MethodPost, transcriptURL, strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer producer")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge || response.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("status/content-type = %d/%q, want 413 problem JSON", response.Code, response.Header().Get("Content-Type"))
	}
}

func TestTranscriptReconnectUsesLastEventID(t *testing.T) {
	api := newTestServer(&fakeAccess{})
	run := RunIdentity{Namespace: "project-1", UID: "run-1-uid"}
	first, err := api.store.Append(context.Background(), run, AppendTranscriptInput{Source: "adapter", IdempotencyKey: "first", Type: "output", Data: json.RawMessage(`{"text":"first"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.store.Append(context.Background(), run, AppendTranscriptInput{Source: "adapter", IdempotencyKey: "second", Type: "output", Data: json.RawMessage(`{"text":"second"}`)}); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(api.Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+transcriptURL+"?after=not-the-reconnect-cursor", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Last-Event-ID", first.Event.ID)
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
	api := NewServer(nil, ServerOptions{Access: &fakeAccess{err: errUnauthenticated}, Runs: resolver, TranscriptStore: NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})})

	for _, method := range []string{http.MethodGet, http.MethodPost} {
		request := httptest.NewRequest(method, transcriptURL, strings.NewReader(`{"source":"adapter","idempotencyKey":"event-1","type":"output","data":{}}`))
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("%s status = %d, want %d", method, response.Code, http.StatusUnauthorized)
		}
	}
	store := api.store.(*memoryTranscriptStore)
	if resolver.calls != 0 || len(store.runs) != 0 || len(store.subscribers) != 0 {
		t.Fatalf("unauthorized request reached backend: resolves=%d runs=%d subscribers=%d", resolver.calls, len(store.runs), len(store.subscribers))
	}
}

func TestTranscriptAuthorizationScopesNamespaceAndRun(t *testing.T) {
	access := &fakeAccess{allow: func(resource ResourceAccess) bool {
		return resource.Namespace == "project-a" && resource.Name == "run-1"
	}}
	api := NewServer(nil, ServerOptions{Access: access, Runs: &fakeRunResolver{}, TranscriptStore: NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})})

	allowed := httptest.NewRequest(http.MethodPost, "/api/v1/namespaces/project-a/runs/run-1/transcript", strings.NewReader(`{"source":"adapter","idempotencyKey":"event-1","type":"output","data":{"source":"a"}}`))
	allowed.Header.Set("Authorization", "Bearer producer-a")
	allowedResponse := httptest.NewRecorder()
	api.Handler().ServeHTTP(allowedResponse, allowed)
	if allowedResponse.Code != http.StatusCreated {
		t.Fatalf("allowed append status = %d, want %d", allowedResponse.Code, http.StatusCreated)
	}

	for _, path := range []string{
		"/api/v1/namespaces/project-b/runs/run-1/transcript",
		"/api/v1/namespaces/project-a/runs/run-2/transcript",
	} {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"source":"adapter","idempotencyKey":"forged","type":"output","data":{"forged":true}}`))
		request.Header.Set("Authorization", "Bearer producer-a")
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf("%s status = %d, want %d", path, response.Code, http.StatusForbidden)
		}
	}
	store := api.store.(*memoryTranscriptStore)
	identity := RunIdentity{Namespace: "project-a", UID: "run-1-uid"}
	if len(store.runs) != 1 || len(store.runs[identity].events) != 1 {
		t.Fatalf("forbidden append changed transcript store: %+v", store.runs)
	}
}

func TestUnknownRunRejectedBeforeTranscriptState(t *testing.T) {
	api := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: &fakeRunResolver{err: apierrors.NewNotFound(schema.GroupResource{Group: "swe.dev", Resource: "runs"}, "run-1")}, TranscriptStore: NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})})
	request := httptest.NewRequest(http.MethodPost, transcriptURL, strings.NewReader(`{"source":"adapter","idempotencyKey":"event-1","type":"output","data":{}}`))
	request.Header.Set("Authorization", "Bearer producer")
	response := httptest.NewRecorder()
	api.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
	store := api.store.(*memoryTranscriptStore)
	if len(store.runs) != 0 || len(store.subscribers) != 0 {
		t.Fatal("unknown run allocated transcript state")
	}
}

func TestRecreatedRunUsesNewTranscriptIdentity(t *testing.T) {
	resolver := &fakeRunResolver{uid: "run-uid-1"}
	api := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: resolver, TranscriptStore: NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})})
	appendEvent := func() {
		request := httptest.NewRequest(http.MethodPost, transcriptURL, strings.NewReader(`{"source":"adapter","idempotencyKey":"event-1","type":"output","data":{}}`))
		request.Header.Set("Authorization", "Bearer producer")
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusCreated {
			t.Fatalf("append status = %d, want %d", response.Code, http.StatusCreated)
		}
	}
	appendEvent()
	resolver.uid = "run-uid-2"
	appendEvent()
	store := api.store.(*memoryTranscriptStore)
	first := RunIdentity{Namespace: "project-1", UID: "run-uid-1"}
	second := RunIdentity{Namespace: "project-1", UID: "run-uid-2"}
	if len(store.runs[first].events) != 1 || len(store.runs[second].events) != 1 {
		t.Fatalf("recreated Run transcripts were not isolated by UID: %+v", store.runs)
	}
}

func TestTranscriptStoreRemovesEmptySubscriberMap(t *testing.T) {
	store := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{}).(*memoryTranscriptStore)
	run := RunIdentity{Namespace: "project-1", UID: "run-1"}
	subscription, err := store.Subscribe(context.Background(), run, "")
	if err != nil {
		t.Fatal(err)
	}
	subscription.Unsubscribe()
	if _, ok := store.subscribers[run]; ok {
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
	if response.StatusCode != http.StatusCreated {
		message, _ := io.ReadAll(response.Body)
		t.Fatalf("post status = %d, want %d: %s", response.StatusCode, http.StatusCreated, message)
	}
}

func newTestServer(access AccessController) *Server {
	return NewServer(nil, ServerOptions{Access: access, Runs: &fakeRunResolver{}, TranscriptStore: NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})})
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
