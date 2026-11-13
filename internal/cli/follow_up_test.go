package cli

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFollowUpInspectExactReadOnlyCLI(t *testing.T) {
	calls := 0
	body := `{"namespace":"team","namespaceUID":"namespace-a","sourceName":"source","sourceUID":"source-a","reason":"WakeConsentRequired","environmentName":"retained","environmentUID":"env-a","workspaceIncarnation":"incarnation-a","suspensionReason":"Requested","holdPolicyRevision":3,"lastSuspendRequestSequence":7}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "GET" || r.URL.Path != "/api/v1/namespaces/team/runs/source/follow-up" || r.Header.Get("SWE-Run-UID") != "source-a" || r.Header.Get("Authorization") != "Bearer test-token" || r.URL.RawQuery != "" {
			t.Error("wrong inspection request")
		}
		fmt.Fprint(w, body)
	}))
	defer server.Close()
	t.Setenv("KUBECONFIG", t.TempDir()+"/must-not-open")
	for _, asJSON := range []bool{false, true} {
		cmd := NewRootCommand()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&bytes.Buffer{})
		args := []string{"follow-up", "inspect", "source", "--namespace", "team", "--run-uid", "source-a", "--control-plane", server.URL, "--token", "test-token"}
		if asJSON {
			args = append(args, "--json")
		}
		cmd.SetArgs(args)
		if err := cmd.Execute(); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "WakeConsentRequired") {
			t.Fatal(out.String())
		}
		if asJSON && !strings.Contains(out.String(), `"lastSuspendRequestSequence":7`) {
			t.Fatal("consent observation missing")
		}
		if !asJSON && !strings.Contains(out.String(), "no reservation, wake or follow-up execution") {
			t.Fatal("missing advisory boundary")
		}
	}
	if calls != 2 {
		t.Fatalf("expected single read per invocation, got %d", calls)
	}
	for _, invalid := range []string{`{}`, strings.Replace(body, `"source-a"`, `"replacement"`, 1), strings.Repeat("x", 17<<10)} {
		body = invalid
		cmd := NewRootCommand()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&bytes.Buffer{})
		cmd.SetArgs([]string{"follow-up", "inspect", "source", "--namespace", "team", "--run-uid", "source-a", "--control-plane", server.URL, "--token", "test-token"})
		if err := cmd.Execute(); err == nil || out.Len() != 0 {
			t.Fatal("invalid response produced success or partial output")
		}
	}
}

func TestFollowUpInspectCancellationAndRequiredIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	cmd := NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"follow-up", "inspect", "source", "--namespace", "team", "--run-uid", "source-a", "--control-plane", server.URL, "--token", "test-token"})
	if err := cmd.Execute(); err == nil || out.Len() != 0 {
		t.Fatal("cancelled inspection returned success")
	}
	cmd = NewRootCommand()
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"follow-up", "inspect", "source", "--namespace", "team"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("missing source UID accepted")
	}
}
