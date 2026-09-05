package transcriptclient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
)

func TestDeleteExactIdentityAndRotatingCredential(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	for _, token := range []string{"first", "rotated"} {
		if err := os.WriteFile(path, []byte(token+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodDelete || r.URL.Path != "/api/v1/namespaces/ns/runs/run/transcript" || r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get(controlplane.RunUIDHeader) != "run-uid" || r.Header.Get(controlplane.NamespaceUIDHeader) != "ns-uid" {
				t.Error("incorrect exact-identity DELETE")
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		c := Client{BaseURL: server.URL, TokenFile: path}
		for range 2 {
			if err := c.Delete(context.Background(), "ns", "ns-uid", "run", "run-uid"); err != nil {
				t.Fatal(err)
			}
		}
		server.Close()
	}
}

func TestDeleteFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("token"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, status := range []int{200, 201, 202, 301, 307, 400, 401, 403, 404, 409, 410, 428, 500, 503} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/redirect" {
				t.Error("followed redirect")
				w.WriteHeader(204)
				return
			}
			w.Header().Set("Location", "/redirect")
			w.WriteHeader(status)
		}))
		if err := (Client{BaseURL: server.URL, TokenFile: path}).Delete(context.Background(), "ns", "ns-uid", "run", "run-uid"); err == nil {
			t.Errorf("HTTP %d accepted", status)
		}
		server.Close()
	}
	c := Client{BaseURL: "http://control-plane", TokenFile: path, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if _, ok := r.Context().Deadline(); !ok {
			t.Error("missing bounded cleanup deadline")
		}
		return nil, errors.New("uncertain connection")
	})}}
	if err := c.Delete(context.Background(), "ns", "ns-uid", "run", "run-uid"); err == nil {
		t.Fatal("transport failure accepted")
	}
	c.HTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("request without identity or credential")
		return nil, nil
	})}
	if err := c.Delete(context.Background(), "ns", "", "run", "run-uid"); err == nil {
		t.Fatal("missing UID accepted")
	}
	c.TokenFile = ""
	if err := c.Delete(context.Background(), "ns", "ns-uid", "run", "run-uid"); err == nil {
		t.Fatal("missing credential accepted")
	}
}
