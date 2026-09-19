package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
)

func executeAttention(ctx context.Context, args ...string) (string, error) {
	cmd := NewRootCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(append([]string{"attention"}, args...))
	err := cmd.ExecuteContext(ctx)
	return output.String(), err
}

func TestAttentionPrecedence(t *testing.T) {
	for _, tc := range []struct{ state, normal, cancelling string }{
		{"", "unknown", "unknown"}, {"FutureState", "unknown", "unknown"},
		{"Cancelled", "cancelled", "cancelled"}, {"Failed", "failed", "failed"},
		{"Succeeded", "review-candidate", "review-candidate"},
		{"NeedsInput", "input-reported", "cancelling"}, {"Paused", "paused", "cancelling"},
		{"Allocating", "in-progress", "cancelling"}, {"EnvironmentReady", "in-progress", "cancelling"},
		{"AdapterAccepted", "in-progress", "cancelling"}, {"Running", "in-progress", "cancelling"},
	} {
		for _, cancel := range []bool{false, true} {
			want := tc.normal
			if cancel {
				want = tc.cancelling
			}
			if got := attentionBucket(tc.state, cancel); got != want {
				t.Errorf("state=%q cancel=%t: %s, want %s", tc.state, cancel, got, want)
			}
		}
	}
}

func TestAttentionSnapshotFiltersProjectionAndOrder(t *testing.T) {
	runs := []controlplane.RunSummary{
		{Name: "z", UID: "running", State: "Running", Agent: "amp"},
		{Name: "a", UID: "failed-z", State: "Failed", Agent: "codex", CancelRequested: true},
		{Name: "a", UID: "failed-a", State: "Failed", Agent: "amp"},
		{Name: "b", UID: "success", State: "Succeeded", Agent: "amp", CancelRequested: true},
		{Name: "c", UID: "input", State: "NeedsInput", Agent: "codex"},
		{Name: "d", UID: "paused", State: "Paused", Agent: "amp"},
		{Name: "e", UID: "future", State: "FutureState", Agent: "future", CancelRequested: true},
		{Name: "f", UID: "cancelled", State: "Cancelled", Agent: "amp", CancelRequested: true},
		{Name: "g", UID: "cancelling", State: "NeedsInput", Agent: "amp", CancelRequested: true},
	}
	for i := range runs {
		runs[i].CreatedAt = time.Date(2026, 9, 19, 8, 4, 3, 0, time.UTC)
		runs[i].PromptPreview = "private-preview"
		runs[i].Environment = &controlplane.RunEnvironment{Name: "private-env"}
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != "GET" || r.URL.Path != "/api/v1/namespaces/team-a/runs" || r.Header.Get("Authorization") != "Bearer test-token" || r.URL.Query().Get("view") != "summary" || r.URL.Query().Get("limit") != "200" {
			t.Error("unexpected request; attention must use only summary pages")
		}
		if r.URL.Query().Has("bucket") || r.URL.Query().Has("agent") {
			t.Error("filters must be local")
		}
		page := controlplane.RunSummaryList{Items: runs[:3], Continue: "next", ResourceVersion: "snapshot"}
		if r.URL.Query().Get("continue") == "next" {
			page.Items, page.Continue = runs[3:], ""
		}
		_ = json.NewEncoder(w).Encode(page)
	}))
	defer server.Close()
	t.Setenv("SWE_CONTROL_PLANE_URL", server.URL)
	t.Setenv("SWE_CONTROL_PLANE_TOKEN", "test-token")
	for _, tc := range []struct {
		args []string
		want []string
	}{
		{nil, []string{"failed-a", "failed-z", "success", "input", "paused", "future"}},
		{[]string{"--agent", "amp"}, []string{"failed-a", "success", "paused"}},
		{[]string{"--bucket", "failed", "--agent", "amp"}, []string{"failed-a"}},
		{[]string{"--bucket", "failed", "--agent", "AMP"}, []string{}},
		{[]string{"--bucket", "failed", "--agent", "am"}, []string{}},
		{[]string{"--bucket", "unknown"}, []string{"future"}},
		{[]string{"--bucket", "cancelled"}, []string{"cancelled"}},
		{[]string{"--bucket", "cancelling"}, []string{"cancelling"}},
		{[]string{"--bucket", "in-progress"}, []string{"running"}},
		{[]string{"--bucket", "review-candidate"}, []string{"success"}},
		{[]string{"--bucket", "input-reported"}, []string{"input"}},
		{[]string{"--bucket", "paused"}, []string{"paused"}},
	} {
		requests = 0
		output, err := executeAttention(context.Background(), append([]string{"--namespace", "team-a", "--json"}, tc.args...)...)
		if err != nil || requests != 2 || strings.Contains(output, "private-") {
			t.Fatalf("args=%v requests=%d err=%v output=%q", tc.args, requests, err, output)
		}
		var rows []map[string]any
		if err := json.Unmarshal([]byte(output), &rows); err != nil || rows == nil {
			t.Fatalf("invalid array %q: %v", output, err)
		}
		got := []string{}
		for _, row := range rows {
			if len(row) != 8 || row["namespace"] != "team-a" || row["createdAt"] != "2026-09-19T08:04:03Z" {
				t.Fatalf("unexpected projection: %#v", row)
			}
			wantBucket := map[string]string{"running": "in-progress", "failed-z": "failed", "failed-a": "failed", "success": "review-candidate", "input": "input-reported", "paused": "paused", "future": "unknown", "cancelled": "cancelled", "cancelling": "cancelling"}
			if row["bucket"] != wantBucket[row["uid"].(string)] {
				t.Fatalf("wrong projected bucket: %#v", row)
			}
			for _, source := range runs {
				if row["uid"] == source.UID && (row["name"] != source.Name || row["reportedState"] != source.State || row["agent"] != source.Agent || row["cancelRequested"] != source.CancelRequested) {
					t.Fatalf("projection lost source fields: %#v", row)
				}
			}
			got = append(got, row["uid"].(string))
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("args=%v got=%v want=%v", tc.args, got, tc.want)
		}
	}
	output, err := executeAttention(context.Background(), "--namespace", "team-a", "--bucket", "failed", "--agent", "codex")
	want := "NAMESPACE NAME UID REPORTED STATE AGENT CREATED CANCEL REQUESTED BUCKET team-a a failed-z Failed codex 2026-09-19T08:04:03Z true failed"
	if err != nil || strings.Join(strings.Fields(output), " ") != want {
		t.Fatalf("table=%q err=%v", output, err)
	}
	// The new projection must not silently change the existing JSON contract.
	output, _, err = executeListRuns("--namespace", "team-a", "--json")
	if err != nil || !strings.Contains(output, `"promptPreview":"private-preview"`) {
		t.Fatalf("list-runs JSON changed: %q, %v", output, err)
	}
}

func TestAttentionEmptyErrorsAndValidation(t *testing.T) {
	requests := 0
	status := http.StatusOK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if status == http.StatusServiceUnavailable && r.URL.Query().Get("continue") == "" {
			_, _ = fmt.Fprint(w, `{"items":[{"name":"partial","uid":"u","state":"Failed"}],"resourceVersion":"s","continue":"next"}`)
			return
		}
		w.WriteHeader(status)
		_, _ = fmt.Fprint(w, `{"items":[],"resourceVersion":"s"}`)
	}))
	defer server.Close()
	t.Setenv("SWE_CONTROL_PLANE_URL", server.URL)
	t.Setenv("SWE_CONTROL_PLANE_TOKEN", "test-token")
	for _, bucket := range []string{"", "Failed", "failed ", "all", "future"} {
		output, err := executeAttention(context.Background(), "--namespace", "ns", "--bucket", bucket)
		if err == nil || !strings.Contains(err.Error(), "invalid --bucket") || requests != 0 || output != "" {
			t.Fatalf("invalid option made request/output: %q %v %d", output, err, requests)
		}
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "No attention candidates found.\n"},
		{[]string{"--bucket", "failed"}, "No Runs match the attention filters.\n"},
		{[]string{"--agent", "missing"}, "No Runs match the attention filters.\n"},
		{[]string{"--json"}, "[]\n"},
	} {
		output, err := executeAttention(context.Background(), append([]string{"--namespace", "ns"}, tc.args...)...)
		if err != nil || output != tc.want {
			t.Fatalf("empty output=%q err=%v", output, err)
		}
	}
	for _, code := range []int{http.StatusForbidden, http.StatusServiceUnavailable} {
		status = code
		for _, format := range [][]string{nil, {"--json"}} {
			requests = 0
			output, err := executeAttention(context.Background(), append([]string{"--namespace", "ns"}, format...)...)
			wantRequests := 1
			if code == http.StatusServiceUnavailable {
				wantRequests = 2
			}
			if err == nil || output != "" || requests != wantRequests {
				t.Fatalf("failed snapshot output=%q err=%v requests=%d", output, err, requests)
			}
		}
	}
	t.Setenv("SWE_CONTROL_PLANE_URL", "")
	t.Setenv("SWE_CONTROL_PLANE_TOKEN", "")
	t.Setenv("KUBECONFIG", t.TempDir()+"/unused")
	for _, args := range [][]string{nil, {"--namespace", "ns"}, {"--namespace", "ns", "--control-plane", server.URL}} {
		before := requests
		output, err := executeAttention(context.Background(), args...)
		if err == nil || output != "" || requests != before {
			t.Fatalf("missing configuration: output=%q err=%v", output, err)
		}
	}
}

type attentionTransport func(*http.Request) (*http.Response, error)

func (f attentionTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAttentionDeadlineIsCommandLocal(t *testing.T) {
	old := http.DefaultClient
	t.Cleanup(func() { http.DefaultClient = old })
	t.Setenv("SWE_CONTROL_PLANE_URL", "http://attention.test")
	t.Setenv("SWE_CONTROL_PLANE_TOKEN", "test-token")
	for _, mode := range []string{"default", "earlier", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			var earlier time.Time
			if mode == "earlier" {
				earlier = time.Now().Add(20 * time.Millisecond)
				var stop context.CancelFunc
				parent, stop = context.WithDeadline(parent, earlier)
				defer stop()
			}
			start := time.Now()
			var requestContext context.Context
			http.DefaultClient = &http.Client{Transport: attentionTransport(func(r *http.Request) (*http.Response, error) {
				requestContext = r.Context()
				deadline, ok := requestContext.Deadline()
				if !ok || mode == "earlier" && !deadline.Equal(earlier) || mode != "earlier" && (deadline.Before(start.Add(30*time.Second)) || deadline.After(time.Now().Add(30*time.Second))) {
					t.Errorf("unexpected command deadline: %v", deadline)
				}
				if mode == "cancel" {
					cancel()
				}
				if mode != "default" {
					<-requestContext.Done()
					return nil, requestContext.Err()
				}
				return nil, errors.New("transport probe")
			})}
			output, err := executeAttention(parent, "--namespace", "ns")
			if err == nil || output != "" || requestContext == nil || requestContext.Err() == nil || http.DefaultClient.Timeout != 0 {
				t.Fatalf("deadline/cancel scope: output=%q err=%v", output, err)
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) || mode == "earlier" && !errors.Is(err, context.DeadlineExceeded) || mode == "default" && parent.Err() != nil {
				t.Fatalf("parent cancellation scope: %v", err)
			}
		})
	}
}

func TestAttentionTableSanitizesCallerControlledFields(t *testing.T) {
	old := http.DefaultClient
	t.Cleanup(func() { http.DefaultClient = old })
	t.Setenv("SWE_CONTROL_PLANE_URL", "http://attention.test")
	t.Setenv("SWE_CONTROL_PLANE_TOKEN", "test-token")
	http.DefaultClient = &http.Client{Transport: attentionTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"items":[{"name":"run\nescape","uid":"u\u001b[2J","state":"future\rstate","agent":"a\tagent","promptPreview":"private"}],"resourceVersion":"s"}`)), Header: make(http.Header)}, nil
	})}
	output, err := executeAttention(context.Background(), "--namespace", "n\ns")
	if err != nil || strings.Count(output, "\n") != 2 || strings.ContainsAny(output, "\r\x1b\t") || strings.Contains(output, "private") {
		t.Fatalf("unsafe table: %q, %v", output, err)
	}
}
