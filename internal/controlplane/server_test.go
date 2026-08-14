package controlplane

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

const transcriptURL = "/api/v1/namespaces/project-1/runs/run-1/transcript"

func TestKubernetesRunResolverIncludesDeletionState(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	deletingAt := metav1.Now()
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{
			Namespace: "project-1", Name: "run-1", UID: "run-1-uid",
			DeletionTimestamp: &deletingAt, Finalizers: []string{"test/finalizer"},
		}},
	).Build()
	resolved, err := (KubernetesRunResolver{Client: client}).ResolveRun(context.Background(), "project-1", "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.UID != "run-1-uid" || !resolved.Deleting {
		t.Fatalf("Run resolution = %#v", resolved)
	}
}

func TestTranscriptReplayAndLiveStream(t *testing.T) {
	metrics := NewMetrics(prometheus.NewRegistry())
	server := httptest.NewServer(NewServer(nil, ServerOptions{
		Access: &fakeAccess{}, Runs: &fakeRunResolver{},
		TranscriptStore: NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{}), Metrics: metrics,
	}).Handler())
	defer server.Close()

	postEvent(t, server.URL, `{"source":"adapter","idempotencyKey":"first","type":"output","data":{"text":"first"}}`)
	if got := testutil.ToFloat64(metrics.transcriptAppends.WithLabelValues("committed")); got != 1 {
		t.Fatalf("committed append metric = %v, want 1", got)
	}
	if got := histogramCount(t, metrics.transcriptAppendLatency.WithLabelValues("committed")); got != 1 {
		t.Fatalf("append latency observations = %d, want 1", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+transcriptURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(RunUIDHeader, "run-1-uid")
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
	if got := testutil.ToFloat64(metrics.transcriptSubscribers); got != 1 {
		t.Fatalf("SSE subscriber gauge = %v, want 1", got)
	}
	postEvent(t, server.URL, `{"source":"adapter","idempotencyKey":"second","type":"output","data":{"text":"second"}}`)
	assertEventText(t, nextLine(t, lines), "second")
	if got := testutil.ToFloat64(metrics.transcriptDeliveries.WithLabelValues("replay", "delivered")); got != 1 {
		t.Fatalf("replay delivery metric = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.transcriptDeliveries.WithLabelValues("live", "delivered")); got != 1 {
		t.Fatalf("live delivery metric = %v, want 1", got)
	}
	if got := histogramCount(t, metrics.transcriptDeliveryLag.WithLabelValues("replay")); got != 1 {
		t.Fatalf("replay lag observations = %d, want 1", got)
	}
	cancel()
	_ = response.Body.Close()
	for deadline := time.Now().Add(time.Second); testutil.ToFloat64(metrics.transcriptSubscribers) != 0 && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	if got := testutil.ToFloat64(metrics.transcriptSubscribers); got != 0 {
		t.Fatalf("SSE subscriber gauge after close = %v, want 0", got)
	}
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
	request.Header.Set(RunUIDHeader, "run-1-uid")
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

func TestTranscriptStreamClosesWhenHeartbeatReauthorizationFails(t *testing.T) {
	access := &fakeAccess{}
	api := NewServer(nil, ServerOptions{
		Access:                      access,
		Runs:                        &fakeRunResolver{},
		TranscriptStore:             NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{}),
		TranscriptHeartbeatInterval: 10 * time.Millisecond,
	})
	server := httptest.NewServer(api.Handler())
	defer server.Close()

	request, err := http.NewRequest(http.MethodGet, server.URL+transcriptURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(RunUIDHeader, "run-1-uid")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	access.setError(errForbidden)

	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, response.Body)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("read reauthorization-closed SSE stream: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SSE stream remained open after authorization revocation")
	}
	if got := access.callCount(); got < 2 {
		t.Fatalf("authorization calls = %d, want establishment plus lease check", got)
	}
}

func TestTranscriptStreamClosesAfterBrowserLogout(t *testing.T) {
	store := NewMemorySessionStore(MemorySessionStoreOptions{})
	sessionID, err := store.Create(context.Background(), "kubernetes-token")
	if err != nil {
		t.Fatal(err)
	}
	api := NewServer(nil, ServerOptions{
		Access:                      sessionResolvingAccess{store: store},
		Sessions:                    KubernetesAccessController{Sessions: store},
		Runs:                        &fakeRunResolver{},
		TranscriptStore:             NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{}),
		TranscriptHeartbeatInterval: 10 * time.Millisecond,
		AllowInsecureSessions:       true,
	})
	server := httptest.NewServer(api.Handler())
	defer server.Close()

	request, err := http.NewRequest(http.MethodGet, server.URL+transcriptURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(RunUIDHeader, "run-1-uid")
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionID})
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	logout, err := http.NewRequest(http.MethodDelete, server.URL+"/api/v1/session", nil)
	if err != nil {
		t.Fatal(err)
	}
	logout.Header.Set("Origin", server.URL)
	logout.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionID})
	logoutResponse, err := http.DefaultClient.Do(logout)
	if err != nil {
		t.Fatal(err)
	}
	logoutResponse.Body.Close()
	if logoutResponse.StatusCode != http.StatusNoContent {
		t.Fatalf("logout status = %d, want %d", logoutResponse.StatusCode, http.StatusNoContent)
	}

	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, response.Body)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("read logout-closed SSE stream: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SSE stream remained open after browser logout")
	}
}

func TestTranscriptStreamClosesWhenRunIdentityChangesAtRenewal(t *testing.T) {
	resolver := &mutableRunResolver{uid: "run-1-uid"}
	api := NewServer(nil, ServerOptions{
		Access:                      &fakeAccess{},
		Runs:                        resolver,
		TranscriptStore:             NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{}),
		TranscriptHeartbeatInterval: 10 * time.Millisecond,
	})
	server := httptest.NewServer(api.Handler())
	defer server.Close()

	request, err := http.NewRequest(http.MethodGet, server.URL+transcriptURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(RunUIDHeader, "run-1-uid")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	resolver.setUID("replacement-run-uid")

	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, response.Body)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("read identity-closed SSE stream: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SSE stream remained open after Run replacement")
	}
}

func TestMetricsHandlerDoesNotExposeApplicationRoutes(t *testing.T) {
	registry := prometheus.NewRegistry()
	NewMetrics(registry)
	handler := MetricsHandler(registry)
	metrics := httptest.NewRecorder()
	handler.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metrics.Code != http.StatusOK || !strings.Contains(metrics.Body.String(), "swe_control_plane_transcript_sse_subscribers") {
		t.Fatalf("metrics response = %d %q", metrics.Code, metrics.Body.String())
	}
	for _, path := range []string{"/healthz", "/api/v1/session", transcriptURL, "/"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("metrics listener path %q status = %d, want 404", path, response.Code)
		}
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

	request, err := http.NewRequest(http.MethodGet, server.URL+transcriptURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(RunUIDHeader, "run-1-uid")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	run := RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-1-uid"}
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
	request.Header.Set(RunUIDHeader, "run-1-uid")
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
		request.Header.Set(RunUIDHeader, "run-1-uid")
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
		request.Header.Set(RunUIDHeader, "run-1-uid")
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusAccepted {
			t.Fatalf("legacy status = %d, want %d", response.Code, http.StatusAccepted)
		}
	}
	store := api.store.(*memoryTranscriptStore)
	run := RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-1-uid"}
	if len(store.runs[run].events) != 2 {
		t.Fatalf("legacy retained events = %d, want 2", len(store.runs[run].events))
	}
}

func TestTranscriptValidation(t *testing.T) {
	handler := newTestServer(&fakeAccess{}).Handler()
	request := httptest.NewRequest(http.MethodPost, transcriptURL, bytes.NewBufferString(`{"type":"output"}`))
	request.Header.Set("Authorization", "Bearer producer")
	request.Header.Set(RunUIDHeader, "run-1-uid")
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
	request.Header.Set(RunUIDHeader, "run-1-uid")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge || response.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("status/content-type = %d/%q, want 413 problem JSON", response.Code, response.Header().Get("Content-Type"))
	}
}

func TestTranscriptReconnectUsesLastEventID(t *testing.T) {
	api := newTestServer(&fakeAccess{})
	run := RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-1-uid"}
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
	request.Header.Set(RunUIDHeader, "run-1-uid")
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

	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
		request := httptest.NewRequest(method, transcriptURL, strings.NewReader(`{"source":"adapter","idempotencyKey":"event-1","type":"output","data":{}}`))
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("%s status = %d, want %d", method, response.Code, http.StatusUnauthorized)
		}
	}
	store := api.store.(*memoryTranscriptStore)
	if resolver.calls != 0 || len(store.runs) != 0 || len(store.subscribers) != 0 || len(api.transcriptGate.entries) != 0 {
		t.Fatalf("unauthorized request reached backend: resolves=%d runs=%d subscribers=%d gates=%d", resolver.calls, len(store.runs), len(store.subscribers), len(api.transcriptGate.entries))
	}
}

func TestTranscriptRejectsForbiddenBeforeUIDValidation(t *testing.T) {
	resolver := &fakeRunResolver{}
	store := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})
	api := NewServer(nil, ServerOptions{Access: &fakeAccess{err: errForbidden}, Runs: resolver, TranscriptStore: store})
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
		request := httptest.NewRequest(method, transcriptURL, nil)
		request.Header.Set("Authorization", "Bearer denied")
		request.Header.Set(RunUIDHeader, strings.Repeat("x", 129))
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf("%s status = %d, want %d", method, response.Code, http.StatusForbidden)
		}
	}
	memStore := store.(*memoryTranscriptStore)
	if resolver.calls != 0 || len(memStore.runs) != 0 || len(memStore.subscribers) != 0 || len(api.transcriptGate.entries) != 0 {
		t.Fatalf("forbidden request reached backend: resolves=%d runs=%d subscribers=%d gates=%d", resolver.calls, len(memStore.runs), len(memStore.subscribers), len(api.transcriptGate.entries))
	}
}

func TestTranscriptAuthorizationScopesNamespaceAndRun(t *testing.T) {
	access := &fakeAccess{allow: func(resource ResourceAccess) bool {
		return resource.Namespace == "project-a" && resource.Name == "run-1"
	}}
	api := NewServer(nil, ServerOptions{Access: access, Runs: &fakeRunResolver{}, TranscriptStore: NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})})

	allowed := httptest.NewRequest(http.MethodPost, "/api/v1/namespaces/project-a/runs/run-1/transcript", strings.NewReader(`{"source":"adapter","idempotencyKey":"event-1","type":"output","data":{"source":"a"}}`))
	allowed.Header.Set("Authorization", "Bearer producer-a")
	allowed.Header.Set(RunUIDHeader, "run-1-uid")
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
		request.Header.Set(RunUIDHeader, "run-1-uid")
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf("%s status = %d, want %d", path, response.Code, http.StatusForbidden)
		}
	}
	store := api.store.(*memoryTranscriptStore)
	identity := RunIdentity{Namespace: "project-a", NamespaceUID: testNamespaceUID("project-a"), UID: "run-1-uid"}
	if len(store.runs) != 1 || len(store.runs[identity].events) != 1 {
		t.Fatalf("forbidden append changed transcript store: %+v", store.runs)
	}
}

func TestUnknownRunRejectedBeforeTranscriptState(t *testing.T) {
	api := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: &fakeRunResolver{err: apierrors.NewNotFound(schema.GroupResource{Group: "swe.dev", Resource: "runs"}, "run-1")}, TranscriptStore: NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})})
	request := httptest.NewRequest(http.MethodPost, transcriptURL, strings.NewReader(`{"source":"adapter","idempotencyKey":"event-1","type":"output","data":{}}`))
	request.Header.Set("Authorization", "Bearer producer")
	request.Header.Set(RunUIDHeader, "run-1-uid")
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
	appendEvent := func(uid string) {
		request := httptest.NewRequest(http.MethodPost, transcriptURL, strings.NewReader(`{"source":"adapter","idempotencyKey":"event-1","type":"output","data":{}}`))
		request.Header.Set("Authorization", "Bearer producer")
		request.Header.Set(RunUIDHeader, uid)
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusCreated {
			t.Fatalf("append status = %d, want %d", response.Code, http.StatusCreated)
		}
	}
	appendEvent("run-uid-1")
	resolver.uid = "run-uid-2"
	appendEvent("run-uid-2")
	store := api.store.(*memoryTranscriptStore)
	first := RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-uid-1"}
	second := RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-uid-2"}
	if len(store.runs[first].events) != 1 || len(store.runs[second].events) != 1 {
		t.Fatalf("recreated Run transcripts were not isolated by UID: %+v", store.runs)
	}
}

func TestTranscriptRequestsRequireBoundedRunUIDBeforeBackendAccess(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		for _, test := range []struct {
			name   string
			uid    string
			status int
		}{
			{name: "missing", status: http.StatusPreconditionRequired},
			{name: "whitespace", uid: "  ", status: http.StatusPreconditionRequired},
			{name: "overlong", uid: strings.Repeat("x", 129), status: http.StatusBadRequest},
		} {
			t.Run(method+"/"+test.name, func(t *testing.T) {
				resolver := &fakeRunResolver{}
				store := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})
				api := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: resolver, TranscriptStore: store})
				request := httptest.NewRequest(method, transcriptURL, strings.NewReader(`{"source":"adapter","idempotencyKey":"event-1","type":"output","data":{}}`))
				request.Header.Set("Authorization", "Bearer transcript-user")
				request.Header.Set(RunUIDHeader, test.uid)
				response := httptest.NewRecorder()
				api.Handler().ServeHTTP(response, request)
				if response.Code != test.status {
					t.Fatalf("status = %d, want %d", response.Code, test.status)
				}
				memStore := store.(*memoryTranscriptStore)
				if resolver.calls != 0 || len(memStore.runs) != 0 || len(memStore.subscribers) != 0 {
					t.Fatalf("rejected request reached backend: resolves=%d runs=%d subscribers=%d", resolver.calls, len(memStore.runs), len(memStore.subscribers))
				}
			})
		}
	}
}

func TestTranscriptAppendRejectsStaleRunUIDAfterReplacement(t *testing.T) {
	resolver := &fakeRunResolver{uid: "run-uid-1"}
	store := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})
	api := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: resolver, TranscriptStore: store})

	first := httptest.NewRequest(http.MethodPost, transcriptURL, strings.NewReader(`{"source":"adapter","idempotencyKey":"event-1","type":"output","data":{"text":"original"}}`))
	first.Header.Set("Authorization", "Bearer producer")
	first.Header.Set(RunUIDHeader, "run-uid-1")
	firstResponse := httptest.NewRecorder()
	api.Handler().ServeHTTP(firstResponse, first)
	if firstResponse.Code != http.StatusCreated {
		t.Fatalf("first append status = %d, want %d", firstResponse.Code, http.StatusCreated)
	}

	// The Run was deleted and recreated under the same name with a new UID.
	resolver.uid = "run-uid-2"
	stale := httptest.NewRequest(http.MethodPost, transcriptURL, strings.NewReader(`{"source":"adapter","idempotencyKey":"event-2","type":"output","data":{"text":"stale"}}`))
	stale.Header.Set("Authorization", "Bearer producer")
	stale.Header.Set(RunUIDHeader, "run-uid-1")
	staleResponse := httptest.NewRecorder()
	api.Handler().ServeHTTP(staleResponse, stale)
	if staleResponse.Code != http.StatusConflict {
		t.Fatalf("stale UID status = %d, want %d", staleResponse.Code, http.StatusConflict)
	}

	memStore := store.(*memoryTranscriptStore)
	oldIdentity := RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-uid-1"}
	newIdentity := RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-uid-2"}
	if len(memStore.runs[oldIdentity].events) != 1 {
		t.Fatalf("original Run transcript events = %d, want 1", len(memStore.runs[oldIdentity].events))
	}
	if _, exists := memStore.runs[newIdentity]; exists {
		t.Fatalf("stale append allocated store state for the replacement Run: %+v", memStore.runs)
	}
}

func TestTranscriptReadRejectsStaleRunUIDAfterReplacementBeforeStoreAccess(t *testing.T) {
	resolver := &fakeRunResolver{uid: "replacement-uid"}
	store := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})
	api := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: resolver, TranscriptStore: store})
	request := httptest.NewRequest(http.MethodGet, transcriptURL, nil)
	request.Header.Set("Authorization", "Bearer reader")
	request.Header.Set(RunUIDHeader, "original-uid")
	response := httptest.NewRecorder()
	api.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusConflict)
	}
	memStore := store.(*memoryTranscriptStore)
	if resolver.calls != 1 || len(memStore.runs) != 0 || len(memStore.subscribers) != 0 {
		t.Fatalf("stale read reached store: resolves=%d runs=%d subscribers=%d", resolver.calls, len(memStore.runs), len(memStore.subscribers))
	}
}

func TestTranscriptStoreRemovesEmptySubscriberMap(t *testing.T) {
	store := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{}).(*memoryTranscriptStore)
	run := RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-1"}
	subscription, err := store.Subscribe(context.Background(), run, "")
	if err != nil {
		t.Fatal(err)
	}
	subscription.Unsubscribe()
	if _, ok := store.subscribers[run]; ok {
		t.Fatal("empty subscriber map was retained")
	}
}

func TestDeletingRunRejectsTranscriptReadsAndAppendsWithFixedCutoff(t *testing.T) {
	store := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{}).(*memoryTranscriptStore)
	resolver := &fakeRunResolver{deleting: true}
	api := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: resolver, TranscriptStore: store})
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		request := httptest.NewRequest(method, transcriptURL, strings.NewReader(`{"source":"adapter","idempotencyKey":"event","type":"output","data":{}}`))
		request.Header.Set("Authorization", "Bearer transcript-user")
		request.Header.Set(RunUIDHeader, "run-1-uid")
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusGone || response.Header().Get("Content-Type") != "application/problem+json" || !strings.Contains(response.Body.String(), "/transcript-retention-cutoff") {
			t.Fatalf("%s cutoff response = %d %q", method, response.Code, response.Body.String())
		}
	}
	if len(store.runs) != 0 || len(store.subscribers) != 0 || len(api.transcriptGate.entries) != 0 {
		t.Fatalf("deleting Run rejection retained state: runs=%d subscribers=%d gates=%d", len(store.runs), len(store.subscribers), len(api.transcriptGate.entries))
	}
}

func TestTranscriptDeleteRequiresExactBearerIdentityAndIsIdempotent(t *testing.T) {
	metrics := NewMetrics(prometheus.NewRegistry())
	access := &fakeAccess{}
	store := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{}).(*memoryTranscriptStore)
	run := RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-1-uid"}
	appendStoreEvent(t, store, run, "first")
	api := NewServer(nil, ServerOptions{Access: access, Runs: &fakeRunResolver{deleting: true}, TranscriptStore: store, Metrics: metrics})

	missingNamespaceUID := httptest.NewRequest(http.MethodDelete, transcriptURL, nil)
	missingNamespaceUID.Header.Set("Authorization", "Bearer cleanup")
	missingNamespaceUID.Header.Set(RunUIDHeader, "run-1-uid")
	missingResponse := httptest.NewRecorder()
	api.Handler().ServeHTTP(missingResponse, missingNamespaceUID)
	if missingResponse.Code != http.StatusPreconditionRequired || len(api.transcriptGate.entries) != 0 {
		t.Fatalf("missing Namespace UID response/gates = %d/%d", missingResponse.Code, len(api.transcriptGate.entries))
	}

	requestDelete := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodDelete, transcriptURL, nil)
		request.Header.Set("Authorization", "Bearer cleanup")
		request.Header.Set(RunUIDHeader, "run-1-uid")
		request.Header.Set(NamespaceUIDHeader, string(run.NamespaceUID))
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, request)
		return response
	}
	if response := requestDelete(); response.Code != http.StatusNoContent {
		t.Fatalf("delete response = %d %q", response.Code, response.Body.String())
	}
	if _, exists := store.runs[run]; exists {
		t.Fatal("exact transcript remained after cleanup")
	}
	if response := requestDelete(); response.Code != http.StatusNoContent {
		t.Fatalf("idempotent delete response = %d %q", response.Code, response.Body.String())
	}
	if got := testutil.ToFloat64(metrics.transcriptCleanups.WithLabelValues("deleted")); got != 1 {
		t.Fatalf("deleted cleanup metric = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.transcriptCleanups.WithLabelValues("absent")); got != 1 {
		t.Fatalf("absent cleanup metric = %v, want 1", got)
	}
	access.mu.Lock()
	calls := append([]ResourceAccess(nil), access.calls...)
	access.mu.Unlock()
	deleteCalls := 0
	for _, call := range calls {
		if call.Verb == "delete" && call.Resource == "runs" && call.Subresource == "transcript" && call.Name == "run-1" {
			deleteCalls++
		}
	}
	if deleteCalls != 5 { // rejected header, then initial + fresh proof for each cleanup attempt
		t.Fatalf("exact delete SAR calls = %d, want 5; calls=%#v", deleteCalls, calls)
	}

	sessionStore := NewMemorySessionStore(MemorySessionStoreOptions{})
	sessionID, err := sessionStore.Create(context.Background(), "browser-token")
	if err != nil {
		t.Fatal(err)
	}
	sessionAPI := NewServer(nil, ServerOptions{Access: sessionResolvingAccess{store: sessionStore}, Runs: &fakeRunResolver{deleting: true}, TranscriptStore: store})
	sessionRequest := httptest.NewRequest(http.MethodDelete, transcriptURL, nil)
	sessionRequest.Header.Set(RunUIDHeader, "run-1-uid")
	sessionRequest.Header.Set(NamespaceUIDHeader, string(run.NamespaceUID))
	sessionRequest.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionID})
	sessionResponse := httptest.NewRecorder()
	sessionAPI.Handler().ServeHTTP(sessionResponse, sessionRequest)
	if sessionResponse.Code != http.StatusUnauthorized || len(sessionAPI.transcriptGate.entries) != 0 {
		t.Fatalf("browser cleanup response/gates = %d/%d", sessionResponse.Code, len(sessionAPI.transcriptGate.entries))
	}
}

func TestTranscriptDeleteCancelsExistingStream(t *testing.T) {
	resolver := &mutableRunResolver{uid: "run-1-uid"}
	api := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: resolver, TranscriptStore: NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})})
	server := httptest.NewServer(api.Handler())
	defer server.Close()

	streamRequest, err := http.NewRequest(http.MethodGet, server.URL+transcriptURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	streamRequest.Header.Set(RunUIDHeader, "run-1-uid")
	streamResponse, err := http.DefaultClient.Do(streamRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer streamResponse.Body.Close()
	resolver.setDeleting(true)
	deleteRequest, err := http.NewRequest(http.MethodDelete, server.URL+transcriptURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	deleteRequest.Header.Set("Authorization", "Bearer cleanup")
	deleteRequest.Header.Set(RunUIDHeader, "run-1-uid")
	deleteRequest.Header.Set(NamespaceUIDHeader, string(testNamespaceUID("project-1")))
	deleteResponse, err := http.DefaultClient.Do(deleteRequest)
	if err != nil {
		t.Fatal(err)
	}
	deleteResponse.Body.Close()
	if deleteResponse.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d", deleteResponse.StatusCode)
	}
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, streamResponse.Body)
		done <- err
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("existing transcript stream remained open after cutoff")
	}
}

type flakyDeleteTranscriptStore struct {
	TranscriptStore
	mu       sync.Mutex
	failures int
}

func (s *flakyDeleteTranscriptStore) Delete(ctx context.Context, run RunIdentity) (DeleteTranscriptResult, error) {
	s.mu.Lock()
	if s.failures > 0 {
		s.failures--
		s.mu.Unlock()
		return DeleteTranscriptResult{}, errors.New("configured durable store unavailable")
	}
	s.mu.Unlock()
	return s.TranscriptStore.Delete(ctx, run)
}

func TestTranscriptDeleteFailureRemainsCutOffForRetry(t *testing.T) {
	inner := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})
	run := RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-1-uid"}
	appendStoreEvent(t, inner, run, "first")
	store := &flakyDeleteTranscriptStore{TranscriptStore: inner, failures: 1}
	api := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: &fakeRunResolver{deleting: true}, TranscriptStore: store})
	deleteRequest := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodDelete, transcriptURL, nil)
		request.Header.Set("Authorization", "Bearer cleanup")
		request.Header.Set(RunUIDHeader, "run-1-uid")
		request.Header.Set(NamespaceUIDHeader, string(run.NamespaceUID))
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, request)
		return response
	}
	if response := deleteRequest(); response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "configured durable") {
		t.Fatalf("failed cleanup response = %d %q", response.Code, response.Body.String())
	}
	appendRequest := httptest.NewRequest(http.MethodPost, transcriptURL, strings.NewReader(`{"source":"adapter","idempotencyKey":"late","type":"output","data":{}}`))
	appendRequest.Header.Set("Authorization", "Bearer producer")
	appendRequest.Header.Set(RunUIDHeader, "run-1-uid")
	appendResponse := httptest.NewRecorder()
	api.Handler().ServeHTTP(appendResponse, appendRequest)
	if appendResponse.Code != http.StatusGone || !strings.Contains(appendResponse.Body.String(), "/transcript-retention-cutoff") {
		t.Fatalf("append after failed cleanup = %d %q", appendResponse.Code, appendResponse.Body.String())
	}
	if response := deleteRequest(); response.Code != http.StatusNoContent {
		t.Fatalf("cleanup retry response = %d %q", response.Code, response.Body.String())
	}
}

type blockingAppendTranscriptStore struct {
	TranscriptStore
	appendStarted chan struct{}
	releaseAppend chan struct{}
	deleteStarted chan struct{}
	appendOnce    sync.Once
	deleteOnce    sync.Once
}

func (s *blockingAppendTranscriptStore) Append(ctx context.Context, run RunIdentity, input AppendTranscriptInput) (AppendTranscriptResult, error) {
	s.appendOnce.Do(func() { close(s.appendStarted) })
	select {
	case <-ctx.Done():
		return AppendTranscriptResult{}, ctx.Err()
	case <-s.releaseAppend:
	}
	return s.TranscriptStore.Append(ctx, run, input)
}

func (s *blockingAppendTranscriptStore) Delete(ctx context.Context, run RunIdentity) (DeleteTranscriptResult, error) {
	s.deleteOnce.Do(func() { close(s.deleteStarted) })
	return s.TranscriptStore.Delete(ctx, run)
}

func TestTranscriptDeleteDrainsRacingAppendBeforeStoreDeletion(t *testing.T) {
	inner := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{})
	store := &blockingAppendTranscriptStore{
		TranscriptStore: inner,
		appendStarted:   make(chan struct{}),
		releaseAppend:   make(chan struct{}),
		deleteStarted:   make(chan struct{}),
	}
	resolver := &mutableRunResolver{uid: "run-1-uid"}
	api := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: resolver, TranscriptStore: store})

	appendRequest := httptest.NewRequest(http.MethodPost, transcriptURL, strings.NewReader(`{"source":"adapter","idempotencyKey":"racing","type":"output","data":{}}`))
	appendRequest.Header.Set("Authorization", "Bearer producer")
	appendRequest.Header.Set(RunUIDHeader, "run-1-uid")
	appendResponse := httptest.NewRecorder()
	appendDone := make(chan struct{})
	go func() {
		api.Handler().ServeHTTP(appendResponse, appendRequest)
		close(appendDone)
	}()
	select {
	case <-store.appendStarted:
	case <-time.After(time.Second):
		t.Fatal("append did not reach the store")
	}

	resolver.setDeleting(true)
	deleteRequest := httptest.NewRequest(http.MethodDelete, transcriptURL, nil)
	deleteRequest.Header.Set("Authorization", "Bearer cleanup")
	deleteRequest.Header.Set(RunUIDHeader, "run-1-uid")
	deleteRequest.Header.Set(NamespaceUIDHeader, string(testNamespaceUID("project-1")))
	deleteResponse := httptest.NewRecorder()
	deleteDone := make(chan struct{})
	go func() {
		api.Handler().ServeHTTP(deleteResponse, deleteRequest)
		close(deleteDone)
	}()
	select {
	case <-store.deleteStarted:
		t.Fatal("store deletion raced ahead of the admitted append")
	case <-time.After(20 * time.Millisecond):
	}
	close(store.releaseAppend)
	select {
	case <-appendDone:
	case <-time.After(time.Second):
		t.Fatal("append did not drain")
	}
	select {
	case <-deleteDone:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish after append drained")
	}
	if appendResponse.Code != http.StatusCreated || deleteResponse.Code != http.StatusNoContent {
		t.Fatalf("append/delete statuses = %d/%d, bodies %q/%q", appendResponse.Code, deleteResponse.Code, appendResponse.Body.String(), deleteResponse.Body.String())
	}
	run := RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-1-uid"}
	memory := inner.(*memoryTranscriptStore)
	if _, exists := memory.runs[run]; exists {
		t.Fatal("racing append recreated transcript after cleanup")
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
	request.Header.Set(RunUIDHeader, "run-1-uid")
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

func (a *fakeAccess) Authorize(request *http.Request, access ResourceAccess, _ bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, access)
	if a.err != nil {
		return a.err
	}
	if a.allow != nil && !a.allow(access) {
		return errForbidden
	}
	if access.Namespace != "" {
		*request = *request.WithContext(context.WithValue(request.Context(), namespaceUIDContextKey{}, testNamespaceUID(access.Namespace)))
	}
	return nil
}

func (a *fakeAccess) setError(err error) {
	a.mu.Lock()
	a.err = err
	a.mu.Unlock()
}

func (a *fakeAccess) callCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.calls)
}

type sessionResolvingAccess struct {
	store SessionStore
}

func (a sessionResolvingAccess) Authorize(request *http.Request, access ResourceAccess, allowSession bool) error {
	if !allowSession {
		return errUnauthenticated
	}
	cookie, err := request.Cookie(sessionCookieName)
	if err != nil {
		return errUnauthenticated
	}
	if _, err := a.store.Resolve(request.Context(), cookie.Value); err != nil {
		return errUnauthenticated
	}
	*request = *request.WithContext(context.WithValue(request.Context(), namespaceUIDContextKey{}, testNamespaceUID(access.Namespace)))
	return nil
}

func testNamespaceUID(namespace string) types.UID { return types.UID("namespace-uid-" + namespace) }

type fakeRunResolver struct {
	calls    int
	err      error
	uid      types.UID
	deleting bool
}

type mutableRunResolver struct {
	mu       sync.Mutex
	uid      types.UID
	deleting bool
}

func (r *mutableRunResolver) ResolveRun(_ context.Context, _, _ string) (RunResolution, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return RunResolution{UID: r.uid, Deleting: r.deleting}, nil
}

func (r *mutableRunResolver) setUID(uid types.UID) {
	r.mu.Lock()
	r.uid = uid
	r.mu.Unlock()
}

func (r *mutableRunResolver) setDeleting(deleting bool) {
	r.mu.Lock()
	r.deleting = deleting
	r.mu.Unlock()
}

func (r *fakeRunResolver) ResolveRun(_ context.Context, _, name string) (RunResolution, error) {
	r.calls++
	if r.uid != "" {
		return RunResolution{UID: r.uid, Deleting: r.deleting}, r.err
	}
	return RunResolution{UID: types.UID(name + "-uid"), Deleting: r.deleting}, r.err
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

func histogramCount(t *testing.T, observer prometheus.Observer) uint64 {
	t.Helper()
	metric, ok := observer.(prometheus.Metric)
	if !ok {
		t.Fatal("histogram observer does not implement prometheus.Metric")
	}
	var value dto.Metric
	if err := metric.Write(&value); err != nil {
		t.Fatal(err)
	}
	return value.GetHistogram().GetSampleCount()
}
