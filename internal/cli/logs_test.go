package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
	"github.com/spf13/cobra"
)

func TestStreamRunTranscriptWritesOpaqueNDJSON(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.EscapedPath() != "/api/v1/namespaces/team%2Fa/runs/run%20one/transcript" || r.Header.Get("Authorization") != "Bearer reader" {
			t.Errorf("request path/token = %q/%q", r.URL.EscapedPath(), r.Header.Get("Authorization"))
		}
		if requests > 1 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "id: cursor-1\nevent: transcript\ndata: {\"source\":\"adapter\",\"type\":\"adapter.owned\",\"data\":{\"anything\":true}}\n\nevent: transcript-gap\ndata: {\"resumeAfter\":\"cursor-0\",\"earliestSequence\":4}\n\n")
	}))
	defer server.Close()

	command := &cobra.Command{}
	command.SetContext(context.Background())
	var output bytes.Buffer
	command.SetOut(&output)
	if err := streamRunTranscript(command, server.URL, "reader", "team/a", "run one", ""); err != nil {
		t.Fatal(err)
	}
	wantOutput := "{\"event\":\"transcript\",\"id\":\"cursor-1\",\"data\":{\"source\":\"adapter\",\"type\":\"adapter.owned\",\"data\":{\"anything\":true}}}\n" +
		"{\"event\":\"transcript-gap\",\"id\":\"cursor-1\",\"data\":{\"resumeAfter\":\"cursor-0\",\"earliestSequence\":4}}\n"
	if output.String() != wantOutput {
		t.Fatalf("NDJSON = %q, want %q", output.String(), wantOutput)
	}

	decoder := json.NewDecoder(&output)
	var events []struct {
		Event string          `json:"event"`
		ID    string          `json:"id"`
		Data  json.RawMessage `json:"data"`
	}
	for decoder.More() {
		var event struct {
			Event string          `json:"event"`
			ID    string          `json:"id"`
			Data  json.RawMessage `json:"data"`
		}
		if err := decoder.Decode(&event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	if len(events) != 2 {
		t.Fatalf("events = %#v", events)
	}
	if events[0].Event != "transcript" || events[0].ID != "cursor-1" || string(events[0].Data) != `{"source":"adapter","type":"adapter.owned","data":{"anything":true}}` {
		t.Fatalf("transcript event = %#v", events[0])
	}
	if events[1].Event != "transcript-gap" || events[1].ID != "cursor-1" || string(events[1].Data) != `{"resumeAfter":"cursor-0","earliestSequence":4}` {
		t.Fatalf("gap event = %#v", events[1])
	}
}

func TestLogsCommandRequiresExactlyOneMode(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"environment", "--run", "run"},
		{"environment", "--control-plane", "https://control.example"},
		{"environment", "--token", "token"},
		{"environment", "--after="},
	} {
		command := newLogsCommand()
		command.SetArgs(args)
		if err := command.Execute(); err == nil {
			t.Fatalf("logs %v succeeded", args)
		}
	}
}

type shortWriter struct{}

func (shortWriter) Write(data []byte) (int, error) { return len(data) - 1, nil }

func TestWriteTranscriptOutputRejectsInvalidJSONAndShortWrites(t *testing.T) {
	event := controlplaneclient.SSEEvent{Event: "transcript", ID: "cursor", Data: []byte(`{"valid":true}`)}
	if err := writeTranscriptOutput(shortWriter{}, event); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write error = %v", err)
	}
	event.Data = []byte(`{"invalid"`)
	if err := writeTranscriptOutput(io.Discard, event); err == nil {
		t.Fatal("invalid JSON was emitted")
	}
}

func TestLogsControlPlaneEnvironmentDefaultsDoNotSelectRunMode(t *testing.T) {
	t.Setenv("SWE_CONTROL_PLANE_URL", "https://control.example")
	t.Setenv("SWE_CONTROL_PLANE_TOKEN", "token")
	command := newLogsCommand()
	for _, flag := range []string{"control-plane", "token"} {
		if command.Flags().Changed(flag) {
			t.Fatalf("environment default marked --%s as explicitly changed", flag)
		}
	}
}
