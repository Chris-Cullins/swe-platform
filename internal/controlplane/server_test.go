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
	"testing"
	"time"
)

func TestTranscriptReplayAndLiveStream(t *testing.T) {
	server := httptest.NewServer(NewServer(nil).Handler())
	defer server.Close()

	postEvent(t, server.URL, `{"type":"output","data":{"text":"first"}}`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/v1/runs/run-1/transcript", nil)
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
	handler := NewServer(nil).Handler()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/runs/run-1/transcript", bytes.NewBufferString(`{"type":"output"}`))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestTranscriptReconnectUsesLastEventID(t *testing.T) {
	api := NewServer(nil)
	first := api.store.append("run-1", "output", json.RawMessage(`{"text":"first"}`))
	api.store.append("run-1", "output", json.RawMessage(`{"text":"second"}`))

	server := httptest.NewServer(api.Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/v1/runs/run-1/transcript", nil)
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
	response, err := http.Post(baseURL+"/api/v1/runs/run-1/transcript", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		message, _ := io.ReadAll(response.Body)
		t.Fatalf("post status = %d, want %d: %s", response.StatusCode, http.StatusAccepted, message)
	}
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
