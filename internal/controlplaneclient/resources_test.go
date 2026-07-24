package controlplaneclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
)

func TestResourceClientUsesAuthenticatedTypedEndpoints(t *testing.T) {
	token := "api-token"
	requests := make([]string, 0, 6)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
		}
		requests = append(requests, r.Method+" "+r.URL.RequestURI())
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/team-a/runs" && r.URL.Query().Get("continue") == "":
			_, _ = io.WriteString(w, `{"items":[{"name":"one","uid":"uid-one","createdAt":"2026-07-24T00:00:00Z","agent":"future-agent","promptPreview":"task","cancelRequested":false,"state":"Running"}],"continue":"next"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/team-a/runs" && r.URL.Query().Get("continue") == "next":
			_, _ = io.WriteString(w, `{"items":[{"name":"two","uid":"uid-two","createdAt":"2026-07-24T00:00:00Z","agent":"another-agent","promptPreview":"task","cancelRequested":false,"state":"Succeeded"}]}`)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/team-a/runs/run-one":
			writeTestRun(w, "run-one", false)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/namespaces/team-a/runs":
			if r.Header.Get("Content-Type") != "application/json" {
				t.Errorf("Content-Type = %q", r.Header.Get("Content-Type"))
			}
			var request controlplane.CreateRunRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if request.Agent != "future-agent" || request.Name != "run-one" {
				t.Errorf("create request = %#v", request)
			}
			writeTestRun(w, "run-one", false)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/namespaces/team-a/runs/run-one/cancel":
			var request controlplane.CancelRunRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.RunUID != "uid-one" {
				t.Errorf("cancel request = %#v, %v", request, err)
			}
			writeTestRun(w, "run-one", true)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/team-a/environments/env-one":
			_, _ = io.WriteString(w, `{"name":"env-one","uid":"env-uid","createdAt":"2026-07-24T00:00:00Z","template":"small","backend":"pod","paused":false,"phase":"Running","ready":true}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := New(server.URL, token, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	runs, err := client.ListRunSummaries(context.Background(), "team-a")
	if err != nil || len(runs) != 2 || runs[0].Agent != "future-agent" {
		t.Fatalf("ListRunSummaries() = %#v, %v", runs, err)
	}
	if _, err := client.GetRun(context.Background(), "team-a", "run-one"); err != nil {
		t.Fatal(err)
	}
	request := controlplane.CreateRunRequest{Name: "run-one", Agent: "future-agent", Prompt: "task", Selector: controlplane.RunSelector{Template: "small"}}
	if _, err := client.CreateRun(context.Background(), "team-a", request); err != nil {
		t.Fatal(err)
	}
	cancelled, err := client.CancelRun(context.Background(), "team-a", "run-one", "uid-one")
	if err != nil || !cancelled.CancelRequested {
		t.Fatalf("CancelRun() = %#v, %v", cancelled, err)
	}
	environment, err := client.GetEnvironment(context.Background(), "team-a", "env-one")
	if err != nil || !environment.Ready {
		t.Fatalf("GetEnvironment() = %#v, %v", environment, err)
	}
	if len(requests) != 6 {
		t.Fatalf("requests = %#v", requests)
	}
}

func TestResourceClientSurfacesAPIStatusesWithoutCredentialDisclosure(t *testing.T) {
	statuses := []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusConflict, http.StatusServiceUnavailable}
	for _, status := range statuses {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			const token = "must-not-be-rendered"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/problem+json")
				w.WriteHeader(status)
				fmt.Fprintf(w, `{"title":"request failed","status":%d,"detail":"reflected %s"}`, status, token)
			}))
			defer server.Close()
			client, _ := New(server.URL, token, server.Client())
			_, err := client.ListRunSummaries(context.Background(), "default")
			var problem *ProblemError
			if !errors.As(err, &problem) || problem.Problem.Status != status {
				t.Fatalf("error = %#v", err)
			}
			if strings.Contains(err.Error(), token) || strings.Contains(string(problem.Body), token) {
				t.Fatalf("credential leaked in error: %#v", problem)
			}
		})
	}
}

func TestListRunsRejectsRepeatedPaginationCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"items":[],"continue":"same"}`)
	}))
	defer server.Close()
	client, _ := New(server.URL, "token", server.Client())
	_, err := client.ListRunSummaries(context.Background(), "default")
	if err == nil || !strings.Contains(err.Error(), "repeated") {
		t.Fatalf("error = %v", err)
	}
}

func TestControlPlaneURLRejectsUserInformation(t *testing.T) {
	_, err := New("https://user:secret@example.test", "token", nil)
	if err == nil || !strings.Contains(err.Error(), "user information") {
		t.Fatalf("error = %v", err)
	}
}

func writeTestRun(w http.ResponseWriter, name string, cancelled bool) {
	_ = json.NewEncoder(w).Encode(controlplane.Run{
		Name: name, UID: "uid-one", Intent: controlplane.RunIntent{
			Selector: controlplane.RunSelector{Template: "small"}, Agent: "future-agent", Prompt: "task",
		}, State: "Running", CancelRequested: cancelled,
	})
}

func TestListRunsEscapesContinuation(t *testing.T) {
	var queries []url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query())
		w.Header().Set("Content-Type", "application/json")
		if len(queries) == 1 {
			_, _ = io.WriteString(w, `{"items":[],"continue":"a&b"}`)
		} else {
			_, _ = io.WriteString(w, `{"items":[]}`)
		}
	}))
	defer server.Close()
	client, _ := New(server.URL, "token", server.Client())
	_, err := client.ListRunSummaries(context.Background(), "default")
	if err != nil || len(queries) != 2 || !reflect.DeepEqual(queries[1]["continue"], []string{"a&b"}) {
		t.Fatalf("queries/error = %#v/%v", queries, err)
	}
}

func TestListRunSummariesPaginatesWithoutFullNearMiBPrompt(t *testing.T) {
	fullPrompt := `<script>&` + strings.Repeat("界", 340000)
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Query().Get("view") != "summary" || r.URL.Query().Get("limit") != "200" {
			t.Errorf("query = %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		if requests == 1 {
			_, _ = io.WriteString(w, `{"items":[{"name":"one","promptPreview":"\u003cscript\u003e\u0026"}],"continue":"next"}`)
			return
		}
		if r.URL.Query().Get("continue") != "next" {
			t.Errorf("continuation = %q", r.URL.Query().Get("continue"))
		}
		_, _ = io.WriteString(w, `{"items":[{"name":"two","promptPreview":"done"}]}`)
	}))
	defer server.Close()
	client, _ := New(server.URL, "token", server.Client())
	items, err := client.ListRunSummaries(context.Background(), "ns")
	if err != nil || requests != 2 || len(items) != 2 || items[0].PromptPreview != "<script>&" {
		t.Fatalf("items/requests/error = %#v/%d/%v", items, requests, err)
	}
	for _, item := range items {
		if item.PromptPreview == fullPrompt || len(item.PromptPreview) > 1024 {
			t.Fatalf("received full prompt: %d bytes", len(item.PromptPreview))
		}
	}
}

func TestRunSnapshotRestartsInconsistentAndExpiredChains(t *testing.T) {
	request := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request++
		w.Header().Set("Content-Type", "application/json")
		switch request {
		case 1:
			_, _ = io.WriteString(w, `{"items":[],"continue":"next","resourceVersion":"1"}`)
		case 2:
			_, _ = io.WriteString(w, `{"items":[],"resourceVersion":"2"}`)
		case 3:
			w.Header().Set("Content-Type", "application/problem+json")
			w.WriteHeader(http.StatusGone)
			_, _ = io.WriteString(w, `{"title":"expired","status":410}`)
		case 4:
			_, _ = io.WriteString(w, `{"items":[{"name":"current"}],"resourceVersion":"3"}`)
		default:
			t.Fatalf("unexpected request %d: %s", request, r.URL.RequestURI())
		}
	}))
	defer server.Close()
	client, _ := New(server.URL, "token", server.Client())
	snapshot, err := client.ListRunSummarySnapshot(context.Background(), "ns")
	if err != nil || request != 4 || snapshot.ResourceVersion != "3" || len(snapshot.Items) != 1 || snapshot.Items[0].Name != "current" {
		t.Fatalf("snapshot/requests/error = %#v/%d/%v", snapshot, request, err)
	}
}

func TestRunWatchFallbackOnlyBeforeFirstSuccessfulConnection(t *testing.T) {
	t.Run("initial", func(t *testing.T) {
		server := httptest.NewServer(http.NotFoundHandler())
		defer server.Close()
		client, _ := New(server.URL, "token", server.Client())
		err := client.StreamRunSummaries(context.Background(), "ns", "1", nil, func(controlplane.RunWatchEvent) error { return nil })
		if !RunWatchCompatibilityFallback(err) {
			t.Fatalf("initial error = %#v", err)
		}
	})
	t.Run("after-connect", func(t *testing.T) {
		requests := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests++
			if requests == 1 {
				w.Header().Set("Content-Type", "text/event-stream")
				return
			}
			http.NotFound(w, nil)
		}))
		defer server.Close()
		client, _ := New(server.URL, "token", server.Client())
		client.reconnectWait = 0
		established := 0
		err := client.StreamRunSummaries(context.Background(), "ns", "1", func() { established++ }, func(controlplane.RunWatchEvent) error { return nil })
		if established != 1 || requests != 2 || RunWatchCompatibilityFallback(err) {
			t.Fatalf("established/requests/error = %d/%d/%#v", established, requests, err)
		}
	})
}

func TestRunWatchRecognizesIDLessRelistAfterCheckpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: run-checkpoint\nid: 2\ndata: {\"resourceVersion\":\"2\"}\n\nevent: run-relist\ndata: {}\n\n")
	}))
	defer server.Close()
	client, _ := New(server.URL, "token", server.Client())
	err := client.StreamRunSummaries(context.Background(), "ns", "1", nil, func(controlplane.RunWatchEvent) error {
		t.Fatal("unexpected Run event")
		return nil
	})
	if !errors.Is(err, ErrRunRelist) {
		t.Fatalf("StreamRunSummaries() error = %v, want ErrRunRelist", err)
	}
}

func TestRunWatchRetriesInitialAdmissionFailure(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		if requests == 1 {
			w.Header().Set("Content-Type", "application/problem+json")
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"title":"watch capacity reached","status":429}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: run\nid: 2\ndata: {\"type\":\"ADDED\",\"resourceVersion\":\"2\",\"run\":{\"name\":\"one\",\"uid\":\"uid-one\"}}\n\n")
	}))
	defer server.Close()
	client, _ := New(server.URL, "token", server.Client())
	sentinel := errors.New("handled")
	established := 0
	err := client.StreamRunSummaries(context.Background(), "ns", "1", func() { established++ }, func(controlplane.RunWatchEvent) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) || requests != 2 || established != 1 {
		t.Fatalf("error/requests/established = %v/%d/%d", err, requests, established)
	}
}
