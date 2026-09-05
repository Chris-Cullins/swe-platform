package controlplaneclient

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestChangesClientPinsIdentityAndRevisionWithoutHiddenReads(t *testing.T) {
	for _, responseUID := range []string{"run-uid", "replacement"} {
		t.Run(responseUID, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Method != "GET" || r.URL.Path != "/api/v1/namespaces/ns/runs/run/changes" || r.Header.Get("SWE-Run-UID") != "run-uid" || r.Header.Get("Authorization") != "Bearer test-token" || r.URL.Query().Get("revision") != "7" || r.URL.Query().Get("path") != "a & b.go" {
					t.Error("incorrect exact changes request", r.URL)
				}
				fmt.Fprintf(w, `{"runUID":%q,"revision":7,"state":"clean","files":[]}`, responseUID)
			}))
			defer server.Close()
			c, err := New(server.URL, "test-token", server.Client())
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.GetRunChanges(context.Background(), "ns", "run", "run-uid", 0, 7, "a & b.go")
			if (err == nil) != (responseUID == "run-uid") || requests != 1 {
				t.Fatalf("requests=%d err=%v", requests, err)
			}
			if _, err := c.GetRunChanges(context.Background(), "ns", "run", "", 0, 0, ""); err == nil || requests != 1 {
				t.Fatal("missing UID reached server")
			}
		})
	}
}
