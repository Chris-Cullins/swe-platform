package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
)

func executeListRuns(args ...string) (string, string, error) {
	cmd := NewRootCommand()
	var output, diagnostics bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&diagnostics)
	cmd.SetArgs(append([]string{"list-runs"}, args...))
	err := cmd.Execute()
	return output.String(), diagnostics.String(), err
}

func TestListRunsFiltersCompleteSnapshotAndPreservesSummaries(t *testing.T) {
	created := time.Date(2026, 9, 17, 8, 4, 3, 0, time.FixedZone("offset", -5*60*60))
	runs := []controlplane.RunSummary{
		{Name: "z-running", UID: "uid-z", State: "Running", Agent: "amp", CreatedAt: created},
		{Name: "b-failed", UID: "uid-b", State: "Failed", Agent: "codex", CreatedAt: created},
		{Name: "a-failed", UID: "uid-a", State: "Failed", Agent: "amp", CreatedAt: created, Generation: 7, PromptPreview: "private preview", CancelRequested: true, Environment: &controlplane.RunEnvironment{Name: "env", UID: "env-uid"}},
		{Name: "c-future", UID: "uid-c", State: "FutureState", Agent: "future-agent", CreatedAt: created},
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/namespaces/team-a/runs" || r.Header.Get("Authorization") != "Bearer test-token" || r.URL.Query().Get("view") != "summary" || r.URL.Query().Get("limit") != "200" {
			t.Error("unexpected Run summary request")
		}
		if r.URL.Query().Has("state") || r.URL.Query().Has("agent") {
			t.Error("filters must be local")
		}
		page := controlplane.RunSummaryList{ResourceVersion: "snapshot", Items: runs[:2], Continue: "next"}
		if r.URL.Query().Get("continue") == "next" {
			page.Items, page.Continue = runs[2:], ""
		}
		_ = json.NewEncoder(w).Encode(page)
	}))
	defer server.Close()
	t.Setenv("SWE_CONTROL_PLANE_URL", server.URL)
	t.Setenv("SWE_CONTROL_PLANE_TOKEN", "test-token")
	for _, tc := range []struct {
		name string
		args []string
		want []int
	}{
		{"unfiltered includes future state", nil, []int{2, 1, 3, 0}},
		{"state", []string{"--state", "Failed"}, []int{2, 1}},
		{"agent", []string{"--agent", "amp"}, []int{2, 0}},
		{"AND", []string{"--state", "Failed", "--agent", "amp"}, []int{2}},
		{"future agent", []string{"--agent", "future-agent"}, []int{3}},
		{"no matches", []string{"--state", "Succeeded"}, nil},
		{"agent is exact", []string{"--agent", "am"}, nil},
		{"agent is case sensitive", []string{"--agent", "AMP"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests = 0
			args := append([]string{"--namespace", "team-a", "--json"}, tc.args...)
			output, diagnostics, err := executeListRuns(args...)
			if err != nil || diagnostics != "" || requests != 2 {
				t.Fatalf("requests=%d error=%v diagnostics=%q", requests, err, diagnostics)
			}
			var got []controlplane.RunSummary
			if err := json.Unmarshal([]byte(output), &got); err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tc.want) || got == nil {
				t.Fatalf("items=%d, want=%d; output=%q", len(got), len(tc.want), output)
			}
			for i, index := range tc.want {
				// Compare the complete wire representation, not only names or counts.
				wantJSON, _ := json.Marshal(runs[index])
				gotJSON, _ := json.Marshal(got[i])
				if !bytes.Equal(gotJSON, wantJSON) {
					t.Fatalf("summary %d = %s, want %s", i, gotJSON, wantJSON)
				}
			}
		})
	}
	output, _, err := executeListRuns("--namespace", "team-a", "--state", "Failed", "--agent", "amp")
	wantFields := []string{"NAME", "UID", "STATE", "AGENT", "CREATED", "a-failed", "uid-a", "Failed", "amp", "2026-09-17T13:04:03Z"}
	if err != nil || !reflect.DeepEqual(strings.Fields(output), wantFields) || strings.Contains(output, "private preview") {
		t.Fatalf("table=%q error=%v", output, err)
	}
}

func TestListRunsEmptyResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"items":[],"resourceVersion":"1"}`)
	}))
	defer server.Close()
	t.Setenv("SWE_CONTROL_PLANE_URL", server.URL)
	t.Setenv("SWE_CONTROL_PLANE_TOKEN", "test-token")
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "No Runs found.\n"},
		{[]string{"--state", "Running"}, "No Runs match the filters.\n"},
		{[]string{"--json"}, "[]\n"},
	} {
		output, _, err := executeListRuns(append([]string{"--namespace", "team-a"}, tc.args...)...)
		if err != nil || output != tc.want {
			t.Fatalf("output=%q want=%q error=%v", output, tc.want, err)
		}
	}
}

func TestListRunsRequiresExplicitConfigurationAndValidState(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = fmt.Fprint(w, `{"items":[]}`)
	}))
	defer server.Close()
	t.Setenv("SWE_CONTROL_PLANE_URL", "")
	t.Setenv("SWE_CONTROL_PLANE_TOKEN", "")
	t.Setenv("KUBECONFIG", t.TempDir()+"/must-not-be-used")
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "--namespace is required"},
		{[]string{"--namespace", "team-a"}, "control-plane URL is required"},
		{[]string{"--namespace", "team-a", "--control-plane", server.URL}, "control-plane token is required"},
		{[]string{"--namespace", "team-a", "--state", "running"}, "invalid --state"},
		{[]string{"--namespace", "team-a", "--state", "FutureState"}, "invalid --state"},
		{[]string{"--namespace", "team-a", "--state", "Failed "}, "invalid --state"},
	} {
		output, _, err := executeListRuns(tc.args...)
		if err == nil || !strings.Contains(err.Error(), tc.want) || output != "" || requests != 0 {
			t.Fatalf("output=%q error=%v requests=%d; want %q", output, err, requests, tc.want)
		}
	}
	for _, state := range []string{"Allocating", "EnvironmentReady", "AdapterAccepted", "Running", "NeedsInput", "Paused", "Succeeded", "Failed", "Cancelled"} {
		output, _, err := executeListRuns("--namespace", "team-a", "--control-plane", server.URL, "--token", "test-token", "--state", state, "--json")
		if err != nil || output != "[]\n" {
			t.Fatalf("state %s: output=%q error=%v", state, output, err)
		}
	}
}

func TestListRunsDenialAndPageFailureNeverPrintPartialResults(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusServiceUnavailable} {
		for _, asJSON := range []bool{false, true} {
			t.Run(fmt.Sprintf("status=%d/json=%t", status, asJSON), func(t *testing.T) {
				requests := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests++
					if status == http.StatusServiceUnavailable && requests == 1 {
						_, _ = fmt.Fprint(w, `{"items":[{"name":"partial","uid":"exact-uid","state":"Failed","agent":"amp"}],"continue":"next","resourceVersion":"1"}`)
						return
					}
					w.Header().Set("Content-Type", "application/problem+json")
					w.WriteHeader(status)
					_, _ = fmt.Fprint(w, `{"title":"request denied test-token"}`)
				}))
				defer server.Close()
				t.Setenv("SWE_CONTROL_PLANE_URL", server.URL)
				t.Setenv("SWE_CONTROL_PLANE_TOKEN", "test-token")
				args := []string{"--namespace", "team-a", "--state", "Failed", "--agent", "amp"}
				if asJSON {
					args = append(args, "--json")
				}
				output, diagnostics, err := executeListRuns(args...)
				wantRequests := 1
				if status == http.StatusServiceUnavailable {
					wantRequests = 2
				}
				if err == nil || !strings.Contains(err.Error(), fmt.Sprint(status)) || output != "" || requests != wantRequests || strings.Contains(diagnostics+err.Error(), "test-token") {
					t.Fatalf("output=%q requests=%d error=%v", output, requests, err)
				}
			})
		}
	}
}
