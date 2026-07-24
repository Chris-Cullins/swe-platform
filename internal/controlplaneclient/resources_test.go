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
			_, _ = io.WriteString(w, `{"items":[{"name":"one","uid":"uid-one","createdAt":"2026-07-24T00:00:00Z","intent":{"selector":{"template":"small"},"agent":"future-agent","prompt":"task"},"cancelRequested":false,"state":"Running","usage":{"cpuSeconds":0,"tokensIn":0,"tokensOut":0}}],"continue":"next"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/team-a/runs" && r.URL.Query().Get("continue") == "next":
			_, _ = io.WriteString(w, `{"items":[{"name":"two","uid":"uid-two","createdAt":"2026-07-24T00:00:00Z","intent":{"selector":{"project":"repo"},"agent":"another-agent","prompt":"task"},"cancelRequested":false,"state":"Succeeded","usage":{"cpuSeconds":0,"tokensIn":0,"tokensOut":0}}]}`)
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
			if r.ContentLength > 0 {
				t.Errorf("cancel body length = %d", r.ContentLength)
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

	runs, err := client.ListRuns(context.Background(), "team-a")
	if err != nil || len(runs) != 2 || runs[0].Intent.Agent != "future-agent" {
		t.Fatalf("ListRuns() = %#v, %v", runs, err)
	}
	if _, err := client.GetRun(context.Background(), "team-a", "run-one"); err != nil {
		t.Fatal(err)
	}
	request := controlplane.CreateRunRequest{Name: "run-one", Agent: "future-agent", Prompt: "task", Selector: controlplane.RunSelector{Template: "small"}}
	if _, err := client.CreateRun(context.Background(), "team-a", request); err != nil {
		t.Fatal(err)
	}
	cancelled, err := client.CancelRun(context.Background(), "team-a", "run-one")
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
			_, err := client.ListRuns(context.Background(), "default")
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
	_, err := client.ListRuns(context.Background(), "default")
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
	_, err := client.ListRuns(context.Background(), "default")
	if err != nil || len(queries) != 2 || !reflect.DeepEqual(queries[1]["continue"], []string{"a&b"}) {
		t.Fatalf("queries/error = %#v/%v", queries, err)
	}
}
