package transcriptclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Chris-Cullins/swe-platform/internal/controllers"
)

func TestAppendUsesProjectedCredentialAndOpaqueEvent(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("projected-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var gotPath, gotToken string
	var got map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		gotPath, gotToken = request.URL.Path, request.Header.Get("Authorization")
		body, _ := io.ReadAll(request.Body)
		if err := json.Unmarshal(body, &got); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	event := controllers.AdapterEvent{Source: "claude-code", IdempotencyKey: "execution:stdout:0", Type: "claude.output", Data: json.RawMessage(`{"adapter":"owned"}`)}
	if err := (Client{BaseURL: server.URL, TokenFile: tokenFile, HTTP: server.Client()}).Append(context.Background(), "team", "run-1", event); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/api/v1/namespaces/team/runs/run-1/transcript" || gotToken != "Bearer projected-token" {
		t.Fatalf("request path/token = %q/%q", gotPath, gotToken)
	}
	if string(got["data"]) != string(event.Data) {
		t.Fatalf("opaque data = %s", got["data"])
	}
}

func TestAppendClassifiesControlPlaneFailures(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		status    int
		permanent bool
	}{
		{http.StatusBadRequest, true},
		{http.StatusForbidden, true},
		{http.StatusConflict, true},
		{http.StatusRequestEntityTooLarge, true},
		{http.StatusInsufficientStorage, true},
		{http.StatusUnauthorized, false},
		{http.StatusRequestTimeout, false},
		{http.StatusTooManyRequests, false},
		{http.StatusMisdirectedRequest, false},
		{http.StatusTooEarly, false},
		{http.StatusInternalServerError, false},
		{http.StatusFound, false},
	}
	for _, test := range tests {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "rejected", test.status)
			}))
			defer server.Close()
			err := (Client{BaseURL: server.URL, TokenFile: tokenFile}).Append(context.Background(), "ns", "run", controllers.AdapterEvent{Source: "a", IdempotencyKey: "k", Type: "t", Data: json.RawMessage(`{}`)})
			if err == nil || errors.Is(err, controllers.ErrAdapterEventRejected) != test.permanent {
				t.Fatalf("Append() error = %v, permanent = %t", err, test.permanent)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

type trackingBody struct {
	*strings.Reader
	closed bool
}

func (b *trackingBody) Close() error {
	b.closed = true
	return nil
}

func TestAppendDrainsSuccessfulResponse(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	body := &trackingBody{Reader: strings.NewReader(strings.Repeat("x", 32*1024))}
	httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusCreated, Status: "201 Created", Body: body, Header: make(http.Header)}, nil
	})}
	err := (Client{BaseURL: "http://control-plane", TokenFile: tokenFile, HTTP: httpClient}).Append(context.Background(), "ns", "run", controllers.AdapterEvent{Source: "a", IdempotencyKey: "k", Type: "t", Data: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if body.Len() != 0 || !body.closed {
		t.Fatalf("successful response remaining/closed = %d/%t", body.Len(), body.closed)
	}
}

func TestAppendTransportFailureIsRetryable(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	transportErr := errors.New("connection reset after uncertain append")
	httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, transportErr
	})}
	err := (Client{BaseURL: "http://control-plane", TokenFile: tokenFile, HTTP: httpClient}).Append(context.Background(), "ns", "run", controllers.AdapterEvent{Source: "a", IdempotencyKey: "k", Type: "t", Data: json.RawMessage(`{}`)})
	if !errors.Is(err, transportErr) || errors.Is(err, controllers.ErrAdapterEventRejected) {
		t.Fatalf("Append() error = %v, want retryable transport error", err)
	}
}
