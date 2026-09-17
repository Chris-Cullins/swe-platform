package controlplaneclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestListProjectsPage(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "GET" || r.URL.Path != "/api/v1/namespaces/team/projects" || r.Header.Get("Authorization") != "Bearer test-token" || r.URL.Query().Get("limit") != "17" || r.URL.Query().Get("continue") != "opaque+/=&" {
			t.Error("incorrect authenticated page request")
		}
		fmt.Fprint(w, `{"items":[],"continue":"next+/="}`)
	}))
	defer server.Close()
	c, err := New(server.URL, "test-token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.ListProjects(context.Background(), "team", 17, "opaque+/=&")
	if err != nil || p.Items == nil || len(p.Items) != 0 || p.Continue != "next+/=" || calls != 1 {
		t.Fatalf("page %+v error %v calls %d", p, err, calls)
	}
	for _, tc := range []struct {
		ns    string
		limit int64
		token string
	}{{"", 17, ""}, {"team", 0, ""}, {"team", 201, ""}, {"team", 1, strings.Repeat("x", 4097)}} {
		if _, err := c.ListProjects(context.Background(), tc.ns, tc.limit, tc.token); err == nil {
			t.Fatal("invalid input accepted")
		}
	}
	if calls != 1 {
		t.Fatal("invalid input made a request")
	}
}

func TestListProjectsErrors(t *testing.T) {
	for _, status := range []int{401, 403, 410} {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			fmt.Fprintf(w, `{"type":"denied","title":"Unavailable","status":%d}`, status)
		}))
		c, _ := New(s.URL, "test-token", s.Client())
		_, err := c.ListProjects(context.Background(), "team", 50, "")
		var problem *ProblemError
		if !errors.As(err, &problem) || problem.Problem.Status != status {
			t.Fatalf("error %v", err)
		}
		s.Close()
	}
}
