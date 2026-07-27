package cli

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPortalCommandPrintsOnlyAuthenticatedStableURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/api/v1/namespaces/team-a/environments/env-a/portal/web" || r.Header.Get("Authorization") != "Bearer portal-token" {
			t.Fatalf("request path/auth = %q/%q", r.URL.EscapedPath(), r.Header.Get("Authorization"))
		}
		fmt.Fprint(w, `{"url":"https://abcdefghijklmnopqrst.portal.example","environmentUID":"env-uid","service":"web","revision":1}`)
	}))
	defer server.Close()

	command := NewRootCommand()
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs([]string{"portal", "env-a", "web", "--namespace", "team-a", "--control-plane", server.URL, "--token", "portal-token"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); got != "https://abcdefghijklmnopqrst.portal.example\n" {
		t.Fatalf("portal output = %q", got)
	}
}
