package controlplaneclient

import (
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
)

func TestClientBuildsEscapedAuthenticatedRequest(t *testing.T) {
	var requestURI, authorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestURI = r.RequestURI
		authorization = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := New(server.URL+"/platform/", "reader-token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	endpoint := client.Endpoint("api", "v1", "namespaces", "team/a", "runs", "run one", "transcript")
	request, err := client.NewRequest(context.Background(), http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if requestURI != "/platform/api/v1/namespaces/team%2Fa/runs/run%20one/transcript" {
		t.Fatalf("request URI = %q", requestURI)
	}
	if authorization != "Bearer reader-token" {
		t.Fatalf("Authorization = %q", authorization)
	}

	websocketURL, err := WebSocketEndpoint("HTTPS://control.example/platform/", "environments", "env/a", "terminal")
	if err != nil {
		t.Fatal(err)
	}
	if websocketURL != "wss://control.example/platform/environments/env%2Fa/terminal" {
		t.Fatalf("WebSocket endpoint = %q", websocketURL)
	}
}

func TestClientReturnsProblemResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusGone)
		_, _ = io.WriteString(w, `{"type":"https://swe-platform.dev/problems/cursor_expired","title":"transcript cursor expired","status":400,"resumeAfter":"next"}`)
	}))
	defer server.Close()

	client, err := New(server.URL, "token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	request, _ := client.NewRequest(context.Background(), http.MethodGet, server.URL, nil)
	_, err = client.Do(request)
	var problem *ProblemError
	if !errors.As(err, &problem) {
		t.Fatalf("Do() error = %v, want ProblemError", err)
	}
	if problem.Problem.Status != http.StatusGone || problem.Problem.Title != "transcript cursor expired" || problem.retryAfter != "7" || !strings.Contains(string(problem.Body), `"resumeAfter":"next"`) {
		t.Fatalf("problem = %#v, body = %s", problem.Problem, problem.Body)
	}
	recovery, ok := TranscriptCursorRecovery(err)
	if !ok || recovery.ResumeAfter != "next" {
		t.Fatalf("cursor recovery = %#v/%t", recovery, ok)
	}
}

func TestClientClassifiesTruncatedErrorResponseAsProblem(t *testing.T) {
	readErr := errors.New("truncated problem body")
	client, err := New("http://control-plane", "token", &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusGone,
			Status:     "410 Gone",
			Header:     http.Header{"Content-Type": []string{"application/problem+json"}},
			Body:       &errorAfterReader{Reader: strings.NewReader(`{"title":"expired"`), err: readErr},
			Request:    request,
		}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := client.NewRequest(context.Background(), http.MethodGet, client.Endpoint("stream"), nil)
	_, err = client.Do(request)
	var problem *ProblemError
	if !errors.As(err, &problem) || problem.Problem.Status != http.StatusGone || !errors.Is(err, readErr) || string(problem.Body) != `{"title":"expired"` {
		t.Fatalf("Do() error = %#v", err)
	}
}

func TestConsumeSSEFramingAndCursorCommit(t *testing.T) {
	input := ": comment\r\n" +
		"id: cursor-1\r\n" +
		"event: transcript\r\n" +
		"data: {\"line\":\r\n" +
		"data: 1}\r\n\r\n" +
		"id: cursor-2\n\n" +
		"id:\n\n" +
		"event: transcript-gap\n" +
		"data: {}\n\n" +
		"data: {\"default\":true}\n\n" +
		"id: uncommitted\n" +
		"event: transcript\n" +
		"data: {}"
	var events []SSEEvent
	cursor, _, err := consumeSSE(strings.NewReader(input), "initial", func(event SSEEvent) error {
		events = append(events, event)
		return nil
	})
	if !errors.Is(err, io.EOF) {
		t.Fatalf("consumeSSE() error = %v", err)
	}
	if cursor != "" {
		t.Fatalf("committed cursor = %q, want empty ID reset", cursor)
	}
	want := []SSEEvent{
		{ID: "cursor-1", HasID: true, Event: "transcript", Data: []byte("{\"line\":\n1}")},
		{ID: "", HasID: false, Event: "transcript-gap", Data: []byte("{}")},
		{ID: "", HasID: false, Event: "message", Data: []byte("{\"default\":true}")},
	}
	if len(events) != len(want) {
		t.Fatalf("events = %#v", events)
	}
	for i := range want {
		if events[i].ID != want[i].ID || events[i].Event != want[i].Event || string(events[i].Data) != string(want[i].Data) {
			t.Fatalf("event %d = %#v, want %#v", i, events[i], want[i])
		}
	}
}

func TestConsumeSSEDoesNotCommitIDWhenHandlerFails(t *testing.T) {
	sentinel := errors.New("output failed")
	cursor, _, err := consumeSSE(strings.NewReader("id: next\nevent: transcript\ndata: {}\n\n"), "previous", func(SSEEvent) error {
		return sentinel
	})
	if err == nil || !errors.Is(err, sentinel) || cursor != "previous" {
		t.Fatalf("cursor/error = %q/%v", cursor, err)
	}
}

func TestStreamSSEAcceptsWorstCaseEscapedServerEnvelope(t *testing.T) {
	text := strings.Repeat("<", 350<<10)
	requestBody := []byte(`{"type":"output","data":{"text":"` + text + `"}}`)
	if !json.Valid(requestBody) || len(requestBody) >= 1<<20 {
		t.Fatalf("request fixture size/validity = %d/%t", len(requestBody), json.Valid(requestBody))
	}
	envelope, err := json.Marshal(struct {
		Data json.RawMessage `json:"data"`
	}{Data: json.RawMessage(`{"text":"` + text + `"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if len(envelope) <= 2<<20 || len(envelope) >= maxSSELineSize {
		t.Fatalf("escaped envelope size = %d, want between old and new limits", len(envelope))
	}
	var stream bytes.Buffer
	stream.WriteString("id: large-cursor\nevent: transcript\ndata: ")
	stream.Write(envelope)
	stream.WriteString("\n\n")

	requests := 0
	dispatches := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write(stream.Bytes())
			return
		}
		if r.Header.Get("Last-Event-ID") != "large-cursor" {
			t.Errorf("Last-Event-ID = %q", r.Header.Get("Last-Event-ID"))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, _ := New(server.URL, "token", server.Client())
	client.reconnectWait = time.Millisecond
	err = client.StreamSSE(context.Background(), server.URL, "", func(event SSEEvent) error {
		dispatches++
		if event.ID != "large-cursor" || !bytes.Equal(event.Data, envelope) {
			t.Fatalf("large event ID/data size = %q/%d", event.ID, len(event.Data))
		}
		return nil
	})
	if err != nil || dispatches != 1 || requests != 2 {
		t.Fatalf("error/dispatches/requests = %v/%d/%d", err, dispatches, requests)
	}
}

func TestStreamSSEReconnectUsesCommittedLastEventID(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		requestNumber := requests
		mu.Unlock()
		switch requestNumber {
		case 1:
			if r.URL.Query().Get("after") != "starting cursor" || r.Header.Get("Last-Event-ID") != "" {
				t.Errorf("initial cursor query/header = %q/%q", r.URL.Query().Get("after"), r.Header.Get("Last-Event-ID"))
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "retry: 1\n\nid: cursor-1\nevent: transcript\ndata: {}\n\nid: partial\ndata: {}")
		case 2:
			if _, present := r.URL.Query()["after"]; present || r.Header.Get("Last-Event-ID") != "cursor-1" {
				t.Errorf("reconnect query/header = %q/%q", r.URL.RawQuery, r.Header.Get("Last-Event-ID"))
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %d", requestNumber)
		}
	}))
	defer server.Close()

	client, err := New(server.URL, "token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	var events []SSEEvent
	err = client.StreamSSE(context.Background(), server.URL, "starting cursor", func(event SSEEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("SSE retry hint was not respected")
	}
	if len(events) != 1 || events[0].ID != "cursor-1" {
		t.Fatalf("events = %#v", events)
	}
}

func TestStreamSSEReconnectCheckFencesNameBasedIdentity(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "id: cursor-1\nevent: transcript\ndata: {}\n\n")
	}))
	defer server.Close()
	client, _ := New(server.URL, "token", server.Client())
	client.reconnectWait = time.Millisecond
	checks := 0
	replaced := errors.New("Run UID changed")
	err := client.StreamSSEWithReconnectCheck(context.Background(), server.URL, "", func(context.Context) error {
		checks++
		if checks == 2 {
			return replaced
		}
		return nil
	}, func(SSEEvent) error { return nil })
	if !errors.Is(err, replaced) || checks != 2 || requests != 1 {
		t.Fatalf("error/checks/requests = %v/%d/%d", err, checks, requests)
	}
}

func TestStreamSSEEmptyIDResetRemovesInitialCursor(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "id:\n\n")
			return
		}
		if r.URL.RawQuery != "" || r.Header.Get("Last-Event-ID") != "" {
			t.Errorf("reset reconnect query/header = %q/%q", r.URL.RawQuery, r.Header.Get("Last-Event-ID"))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, _ := New(server.URL, "token", server.Client())
	client.reconnectWait = time.Millisecond
	if err := client.StreamSSE(context.Background(), server.URL, "old", func(SSEEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestStreamSSESurfacesGaps(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: transcript-gap\ndata: {\"resumeAfter\":\"cursor-3\"}\n\n")
	}))
	defer server.Close()
	client, _ := New(server.URL, "token", server.Client())
	sentinel := errors.New("gap observed")
	err := client.StreamSSE(context.Background(), server.URL, "", func(event SSEEvent) error {
		if event.Event != "transcript-gap" || string(event.Data) != `{"resumeAfter":"cursor-3"}` {
			t.Fatalf("gap = %#v", event)
		}
		return sentinel
	})
	if err != sentinel {
		t.Fatalf("StreamSSE() error = %v, want unchanged handler error", err)
	}
}

func TestStreamSSEStatusAndProtocolHandling(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		contentType string
		body        string
		wantProblem bool
		wantNil     bool
	}{
		{name: "no content", status: http.StatusNoContent, wantNil: true},
		{name: "invalid cursor", status: http.StatusBadRequest, contentType: "application/problem+json", body: `{"type":"https://swe-platform.dev/problems/invalid_cursor","title":"invalid transcript cursor","status":400}`, wantProblem: true},
		{name: "expired cursor", status: http.StatusGone, contentType: "application/problem+json", body: `{"type":"https://swe-platform.dev/problems/cursor_expired","title":"transcript cursor expired","status":410}`, wantProblem: true},
		{name: "initial unavailable", status: http.StatusServiceUnavailable, contentType: "application/problem+json", body: `{"title":"unavailable","status":503}`, wantProblem: true},
		{name: "non SSE success", status: http.StatusOK, contentType: "application/json", body: `{}`},
		{name: "other success", status: http.StatusAccepted, contentType: "text/event-stream"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests++
				if test.contentType != "" {
					w.Header().Set("Content-Type", test.contentType)
				}
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			client, _ := New(server.URL, "token", server.Client())
			err := client.StreamSSE(context.Background(), server.URL, "cursor", func(SSEEvent) error { return nil })
			if test.wantNil && err != nil {
				t.Fatalf("StreamSSE() error = %v", err)
			}
			if !test.wantNil && err == nil {
				t.Fatal("StreamSSE() succeeded")
			}
			var problem *ProblemError
			if errors.As(err, &problem) != test.wantProblem {
				t.Fatalf("ProblemError = %t, error = %v", errors.As(err, &problem), err)
			}
			if requests != 1 {
				t.Fatalf("requests = %d, want no retry", requests)
			}
		})
	}
}

func TestStreamSSEDoesNotRetryInitialTransportFailure(t *testing.T) {
	sentinel := errors.New("initial dial failed")
	requests := 0
	client, err := New("http://control-plane", "token", &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		requests++
		return nil, sentinel
	})})
	if err != nil {
		t.Fatal(err)
	}
	err = client.StreamSSE(context.Background(), client.Endpoint("stream"), "", func(SSEEvent) error { return nil })
	if !errors.Is(err, sentinel) || requests != 1 {
		t.Fatalf("error/requests = %v/%d", err, requests)
	}
}

func TestRetryableHTTPStatus(t *testing.T) {
	for _, test := range []struct {
		status int
		want   bool
	}{
		{http.StatusBadRequest, false},
		{http.StatusRequestTimeout, true},
		{http.StatusGone, false},
		{http.StatusTooManyRequests, true},
		{499, false},
		{http.StatusInternalServerError, true},
		{599, true},
		{600, false},
	} {
		if got := retryableHTTPStatus(test.status); got != test.want {
			t.Errorf("retryableHTTPStatus(%d) = %t, want %t", test.status, got, test.want)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

type errorAfterReader struct {
	*strings.Reader
	err error
}

func (r *errorAfterReader) Read(buffer []byte) (int, error) {
	n, err := r.Reader.Read(buffer)
	if err == io.EOF {
		return n, r.err
	}
	return n, err
}

func (r *errorAfterReader) Close() error { return nil }

func TestStreamSSEReconnectsAfterBodyReadFailure(t *testing.T) {
	requests := 0
	client, err := New("http://control-plane", "token", &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		if requests == 1 {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     http.Header{"Content-Type": []string{"text/event-stream; charset=utf-8"}},
				Body:       &errorAfterReader{Reader: strings.NewReader("id: cursor-1\nevent: transcript\ndata: {}\n\n"), err: io.ErrUnexpectedEOF},
				Request:    request,
			}, nil
		}
		if request.Header.Get("Last-Event-ID") != "cursor-1" {
			t.Errorf("Last-Event-ID = %q", request.Header.Get("Last-Event-ID"))
		}
		return &http.Response{StatusCode: http.StatusNoContent, Status: "204 No Content", Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	client.reconnectWait = time.Millisecond
	if err := client.StreamSSE(context.Background(), client.Endpoint("stream"), "", func(SSEEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("requests = %d", requests)
	}
}

func TestStreamSSERetriesTransientHTTPAfterEstablishment(t *testing.T) {
	requests := 0
	dispatches := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests > 1 && r.Header.Get("Last-Event-ID") != "cursor-1" {
			t.Errorf("request %d Last-Event-ID = %q", requests, r.Header.Get("Last-Event-ID"))
		}
		switch requests {
		case 1:
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "id: cursor-1\nevent: transcript\ndata: {}\n\n")
		case 2:
			w.Header().Set("Content-Type", "application/problem+json")
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"title":"subscriber capacity","status":503}`)
		case 3:
			w.WriteHeader(http.StatusBadGateway)
		case 4:
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %d", requests)
		}
	}))
	defer server.Close()
	client, _ := New(server.URL, "token", server.Client())
	client.reconnectWait = time.Millisecond
	err := client.StreamSSE(context.Background(), server.URL, "", func(SSEEvent) error {
		dispatches++
		return nil
	})
	if err != nil || requests != 4 || dispatches != 1 {
		t.Fatalf("error/requests/dispatches = %v/%d/%d", err, requests, dispatches)
	}
}

func TestStreamSSEReconnectCursorErrorsRemainTerminal(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusGone} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests++
				if requests == 1 {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "id: cursor-1\nevent: transcript\ndata: {}\n\n")
					return
				}
				w.Header().Set("Content-Type", "application/problem+json")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"title":"cursor rejected"}`)
			}))
			defer server.Close()
			client, _ := New(server.URL, "token", server.Client())
			client.reconnectWait = time.Millisecond
			err := client.StreamSSE(context.Background(), server.URL, "", func(SSEEvent) error { return nil })
			var problem *ProblemError
			if !errors.As(err, &problem) || problem.Problem.Status != status || requests != 2 {
				t.Fatalf("error/requests = %#v/%d", err, requests)
			}
		})
	}
}

func TestStreamSSEReconnectProblemBodyFailureIsTerminal(t *testing.T) {
	readErr := errors.New("truncated invalid cursor response")
	requests := 0
	client, err := New("http://control-plane", "token", &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		if requests == 1 {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader("id: cursor-1\nevent: transcript\ndata: {}\n\n")),
				Request:    request,
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusBadRequest,
			Status:     "400 Bad Request",
			Header:     http.Header{"Content-Type": []string{"application/problem+json"}},
			Body:       &errorAfterReader{Reader: strings.NewReader(`{"type":"invalid_cursor"`), err: readErr},
			Request:    request,
		}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	client.reconnectWait = time.Millisecond
	err = client.StreamSSE(context.Background(), client.Endpoint("stream"), "", func(SSEEvent) error { return nil })
	var problem *ProblemError
	if !errors.As(err, &problem) || problem.Problem.Status != http.StatusBadRequest || !errors.Is(err, readErr) || requests != 2 {
		t.Fatalf("error/requests = %#v/%d", err, requests)
	}
}

func TestStreamSSEPropagatesHandlerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: transcript\ndata: {}\n\n")
	}))
	defer server.Close()
	client, _ := New(server.URL, "token", server.Client())
	sentinel := io.EOF
	if err := client.StreamSSE(context.Background(), server.URL, "", func(SSEEvent) error { return sentinel }); err != sentinel {
		t.Fatalf("StreamSSE() error = %v", err)
	}
}

func TestStreamSSECancellationClosesRequest(t *testing.T) {
	opened := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		close(opened)
		<-r.Context().Done()
	}))
	defer server.Close()
	client, _ := New(server.URL, "token", server.Client())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.StreamSSE(ctx, server.URL, "", func(SSEEvent) error { return nil }) }()
	<-opened
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("StreamSSE() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StreamSSE did not return after cancellation")
	}
}

func TestStreamSSECancellationInterruptsRetryWait(t *testing.T) {
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "retry: 10000\n\n")
		close(closed)
	}))
	defer server.Close()
	client, _ := New(server.URL, "token", server.Client())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.StreamSSE(ctx, server.URL, "", func(SSEEvent) error { return nil }) }()
	<-closed
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("StreamSSE() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StreamSSE did not cancel during retry wait")
	}
}

func TestStreamSSECancellationInterruptsHTTPRetryWait(t *testing.T) {
	retryResponse := make(chan struct{})
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		if requests == 1 {
			w.Header().Set("Content-Type", "text/event-stream")
			return
		}
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusServiceUnavailable)
		close(retryResponse)
	}))
	defer server.Close()
	client, _ := New(server.URL, "token", server.Client())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.StreamSSE(ctx, server.URL, "", func(SSEEvent) error { return nil }) }()
	<-retryResponse
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || requests != 2 {
			t.Fatalf("error/requests = %v/%d", err, requests)
		}
	case <-time.After(time.Second):
		t.Fatal("StreamSSE did not cancel during HTTP retry wait")
	}
}
