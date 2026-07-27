package controlplaneclient

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetPortalRouteEscapesPathAndAuthenticates(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/api/v1/namespaces/team%2Fone/environments/env%2Fone/portal/web%2Fui" {
			t.Errorf("path %q", r.URL.EscapedPath())
		}
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Errorf("authorization %q", r.Header.Get("Authorization"))
		}
		fmt.Fprint(w, `{"url":"https://locator.portal.example","environmentUID":"uid","service":"web/ui","revision":2,"declarationInstanceID":"abcdefghijklmnopqrstuvwx","routeGeneration":3}`)
	}))
	defer s.Close()
	c, _ := New(s.URL, "token", s.Client())
	route, err := c.GetPortalRoute(context.Background(), "team/one", "env/one", "web/ui")
	if err != nil || route.Revision != 2 {
		t.Fatalf("route=%#v err=%v", route, err)
	}
}
