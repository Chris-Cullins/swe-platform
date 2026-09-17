package cli

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProjectListCLI(t *testing.T) {
	t.Setenv("KUBECONFIG", "/nonexistent/no-kubernetes-fallback")
	for _, tc := range []struct {
		name, body string
		json       bool
	}{
		{"table", `{"items":[{"namespace":"team","name":"app","uid":"uid-app","generation":7,"defaultTemplate":"small"}],"continue":"next+/="}`, false},
		{"json", `{"items":[{"namespace":"team","name":"app","uid":"uid-app","generation":7,"defaultTemplate":"small"}],"continue":"next+/="}`, true},
		{"empty table", `{"items":[]}`, false}, {"empty json", `{"items":[]}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/namespaces/team/projects" || r.Header.Get("Authorization") != "Bearer test-token" || r.URL.Query().Get("continue") != "prior+/=" || r.URL.Query().Get("limit") != "3" {
					t.Error("incorrect CLI request")
				}
				fmt.Fprint(w, tc.body)
			}))
			defer s.Close()
			t.Setenv("SWE_CONTROL_PLANE_URL", s.URL)
			t.Setenv("SWE_CONTROL_PLANE_TOKEN", "test-token")
			cmd := NewRootCommand()
			var out, stderr bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&stderr)
			args := []string{"project", "list", "--namespace", "team", "--limit", "3", "--continue", "prior+/="}
			if tc.json {
				args = append(args, "--json")
			}
			cmd.SetArgs(args)
			if err := cmd.Execute(); err != nil {
				t.Fatal(err)
			}
			if tc.json {
				if strings.TrimSpace(out.String()) != tc.body || stderr.Len() != 0 {
					t.Fatalf("JSON %s stderr %s", out.String(), stderr.String())
				}
			} else {
				if !strings.Contains(out.String(), "NAMESPACE") || !strings.Contains(out.String(), "DEFAULT TEMPLATE") {
					t.Fatal("missing table header")
				}
				if strings.Contains(tc.body, "uid-app") {
					for _, v := range []string{"team", "app", "uid-app", "7", "small"} {
						if !strings.Contains(out.String(), v) {
							t.Fatal("missing table field")
						}
					}
					if !strings.Contains(stderr.String(), `--continue "next+/="`) {
						t.Fatal("missing continuation")
					}
				} else if !strings.Contains(stderr.String(), "No Projects found") {
					t.Fatal("missing empty message")
				}
			}
		})
	}
}

func TestProjectListCLIRequiresExplicitConfiguration(t *testing.T) {
	t.Setenv("SWE_CONTROL_PLANE_URL", "")
	t.Setenv("SWE_CONTROL_PLANE_TOKEN", "")
	t.Setenv("KUBECONFIG", "/nonexistent/no-kubernetes-fallback")
	for _, args := range [][]string{{"project", "list"}, {"project", "list", "--namespace", "team"}} {
		cmd := NewRootCommand()
		cmd.SetArgs(args)
		var b bytes.Buffer
		cmd.SetOut(&b)
		cmd.SetErr(&b)
		err := cmd.Execute()
		if err == nil || strings.Contains(err.Error(), "kubeconfig") {
			t.Fatalf("expected explicit control-plane/namespace error, got %v", err)
		}
	}
}

func TestProjectListCLIDenialDoesNotFallBack(t *testing.T) {
	t.Setenv("KUBECONFIG", "/nonexistent/no-kubernetes-fallback")
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"type":"forbidden","title":"Forbidden","status":403}`)
	}))
	defer s.Close()
	t.Setenv("SWE_CONTROL_PLANE_URL", s.URL)
	t.Setenv("SWE_CONTROL_PLANE_TOKEN", "test-token")
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"project", "list", "--namespace", "team"})
	var out, stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "403") || out.Len() != 0 {
		t.Fatalf("denial error=%v stdout=%q", err, out.String())
	}
}
