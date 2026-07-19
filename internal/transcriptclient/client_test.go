package transcriptclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

func TestAppendReportsControlPlaneFailure(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "full", http.StatusInsufficientStorage)
	}))
	defer server.Close()
	if err := (Client{BaseURL: server.URL, TokenFile: tokenFile}).Append(context.Background(), "ns", "run", controllers.AdapterEvent{Source: "a", IdempotencyKey: "k", Type: "t", Data: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("Append() succeeded on control-plane failure")
	}
}
