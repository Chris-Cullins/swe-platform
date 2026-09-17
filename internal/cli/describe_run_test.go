package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
)

func executeDescribeRun(args ...string) (string, string, error) {
	cmd := NewRootCommand()
	var output, diagnostics bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&diagnostics)
	cmd.SetArgs(append([]string{"describe-run"}, args...))
	err := cmd.Execute()
	return output.String(), diagnostics.String(), err
}

func TestDescribeRunExactDetailAndSafeOutput(t *testing.T) {
	created := time.Date(2026, 9, 17, 8, 4, 3, 0, time.FixedZone("offset", -5*60*60))
	started, finished := created.Add(time.Minute), created.Add(3*time.Minute)
	run := controlplane.Run{Name: "task", UID: "uid-a", Generation: 3, State: "Failed", CreatedAt: created, StartedAt: &started, FinishedAt: &finished,
		Intent:     controlplane.RunIntent{Agent: "codex\x1b\r\n", Prompt: "private task prompt", CredentialProfile: "private profile"},
		Diagnostic: &controlplane.RunDiagnostic{Code: "AdapterFailed", Message: "The agent reported a failure.\a", NextAction: "Review the transcript.\n"},
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/namespaces/team-a/runs/task" || r.URL.RawQuery != "" || r.Header.Get("SWE-Run-UID") != "uid-a" || r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("unexpected exact Run request")
		}
		_ = json.NewEncoder(w).Encode(run)
	}))
	defer server.Close()
	t.Setenv("SWE_CONTROL_PLANE_URL", server.URL)
	t.Setenv("SWE_CONTROL_PLANE_TOKEN", "test-token")
	t.Setenv("KUBECONFIG", t.TempDir()+"/must-not-be-used")
	args := []string{"task", "--namespace", "team-a", "--run-uid", "uid-a"}
	output, diagnostics, err := executeDescribeRun(args...)
	want := "Namespace: team-a\nRun: task\nUID: uid-a\nState: Failed\nAgent: codex   \nCreated: 2026-09-17T13:04:03Z\nStarted: 2026-09-17T13:05:03Z\nFinished: 2026-09-17T13:07:03Z\nDiagnostic: AdapterFailed\nThe agent reported a failure. \nNext action: Review the transcript. \nAgent outcome is not verification evidence.\n"
	if err != nil || diagnostics != "" || requests != 1 || output != want {
		t.Fatalf("output=%q diagnostics=%q requests=%d error=%v", output, diagnostics, requests, err)
	}
	requests = 0
	output, diagnostics, err = executeDescribeRun(append(args, "--json")...)
	var got controlplane.Run
	if err != nil || diagnostics != "" || requests != 1 || json.Unmarshal([]byte(output), &got) != nil {
		t.Fatalf("JSON requests=%d error=%v diagnostics=%q", requests, err, diagnostics)
	}
	wantJSON, _ := json.Marshal(run)
	gotJSON, _ := json.Marshal(got)
	if !bytes.Equal(wantJSON, gotJSON) || got.Intent.Prompt != "private task prompt" {
		t.Fatalf("JSON did not preserve authorized typed detail: %s", gotJSON)
	}
}

func TestDescribeRunMissingDiagnosticIsUnknown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"name":"task","uid":"uid-a","state":"Succeeded","intent":{"agent":"amp","prompt":"private prompt"},"conditions":[{"message":"raw provider message"}],"transcript":"private transcript"}`)
	}))
	defer server.Close()
	output, _, err := executeDescribeRun("task", "--namespace", "team-a", "--run-uid", "uid-a", "--control-plane", server.URL, "--token", "test-token")
	if err != nil || !strings.Contains(output, "State: Succeeded\n") || !strings.Contains(output, "Diagnostic: unknown (no current recognized diagnostic).") || !strings.Contains(output, "Agent outcome is not verification evidence.") {
		t.Fatalf("output=%q error=%v", output, err)
	}
	for _, forbidden := range []string{"private", "raw provider", "Created:", "Started:", "Finished:", "healthy", "Next action:"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("unexpected %q in output %q", forbidden, output)
		}
	}
}

func TestDescribeRunRequiresExplicitIdentityAndConfiguration(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests++ }))
	defer server.Close()
	t.Setenv("SWE_CONTROL_PLANE_URL", "")
	t.Setenv("SWE_CONTROL_PLANE_TOKEN", "")
	t.Setenv("KUBECONFIG", t.TempDir()+"/must-not-be-used")
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "accepts 1 arg(s)"},
		{[]string{"task", "extra"}, "accepts 1 arg(s)"},
		{[]string{"task"}, "--namespace is required"},
		{[]string{"task", "--namespace", " "}, "--namespace is required"},
		{[]string{"task", "--namespace", "team-a"}, "required flag(s)"},
		{[]string{" ", "--namespace", "team-a", "--run-uid", "uid-a"}, "Run name is required"},
		{[]string{"task", "--namespace", "team-a", "--run-uid", "uid-a"}, "control-plane URL is required"},
		{[]string{"task", "--namespace", "team-a", "--run-uid", "uid-a", "--control-plane", server.URL}, "control-plane token is required"},
		{[]string{"task", "--namespace", "team-a", "--run-uid", " ", "--control-plane", server.URL, "--token", "test-token"}, "expected Run UID is required"},
		{[]string{"task", "--namespace", "team-a", "--run-uid", strings.Repeat("u", 129), "--control-plane", server.URL, "--token", "test-token"}, "exceeds 128 bytes"},
	} {
		output, _, err := executeDescribeRun(tc.args...)
		if err == nil || !strings.Contains(err.Error(), tc.want) || output != "" || requests != 0 {
			t.Fatalf("output=%q requests=%d error=%v; want %q", output, requests, err, tc.want)
		}
	}
}

func TestDescribeRunFailureNeverPrintsOrFollowsReplacement(t *testing.T) {
	for _, status := range []int{403, 404, 409, 200} {
		for _, asJSON := range []bool{false, true} {
			t.Run(fmt.Sprintf("status=%d/json=%t", status, asJSON), func(t *testing.T) {
				requests := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests++
					if r.Header.Get("SWE-Run-UID") != "uid-a" {
						t.Error("missing original UID")
					}
					w.Header().Set("Content-Type", "application/problem+json")
					w.WriteHeader(status)
					_, _ = fmt.Fprint(w, `{"name":"task","uid":"replacement","intent":{"prompt":"private replacement"},"title":"test-token"}`)
				}))
				defer server.Close()
				args := []string{"task", "--namespace", "team-a", "--run-uid", "uid-a", "--control-plane", server.URL, "--token", "test-token"}
				if asJSON {
					args = append(args, "--json")
				}
				output, diagnostics, err := executeDescribeRun(args...)
				if err == nil || output != "" || requests != 1 || strings.Contains(diagnostics+err.Error(), "test-token") || strings.Contains(diagnostics+err.Error(), "private replacement") {
					t.Fatalf("output=%q requests=%d error=%v diagnostics=%q", output, requests, err, diagnostics)
				}
			})
		}
	}
}
