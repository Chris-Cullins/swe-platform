package controlplaneclient

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRotatingPortalResolverRereadsProjectedToken(t *testing.T) {
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte("first-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var authorizations []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorizations = append(authorizations, r.Header.Get("Authorization"))
		fmt.Fprint(w, `{"url":"https://portal.example","environmentUID":"uid","service":"web","revision":1,"declarationInstanceID":"abcdefghijklmnopqrstuvwx","routeGeneration":2}`)
	}))
	defer server.Close()
	resolver := RotatingPortalResolver{BaseURL: server.URL, TokenFile: tokenFile, HTTP: server.Client()}
	if _, err := resolver.GetPortalRoute(context.Background(), "ns", "env", "web"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenFile, []byte("second-token"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.GetPortalRoute(context.Background(), "ns", "env", "web"); err != nil {
		t.Fatal(err)
	}
	want := []string{"Bearer first-token", "Bearer second-token"}
	if len(authorizations) != 2 || authorizations[0] != want[0] || authorizations[1] != want[1] {
		t.Fatalf("authorization headers = %#v", authorizations)
	}
}

func TestRotatingPortalResolverErrorsRedactToken(t *testing.T) {
	token := "highly-secret-projected-token"
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, token, http.StatusInternalServerError) }))
	defer server.Close()
	_, err := (RotatingPortalResolver{BaseURL: server.URL, TokenFile: tokenFile, HTTP: server.Client()}).GetPortalRoute(context.Background(), "ns", "env", "web")
	if err == nil {
		t.Fatal("expected route error")
	}
	if strings.Contains(err.Error(), token) {
		t.Fatalf("error disclosed projected token: %v", err)
	}
}
