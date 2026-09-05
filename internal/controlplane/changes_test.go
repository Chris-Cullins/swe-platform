package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Chris-Cullins/swe-platform/sandboxd/changes"
	"k8s.io/apimachinery/pkg/types"
)

type fakeChangesCapturer struct {
	capture func(CaptureChangesRequest) (changes.Snapshot, error)
}

func (c fakeChangesCapturer) Capture(_ context.Context, _, _, _ string, r CaptureChangesRequest, _ []string) (changes.Snapshot, error) {
	return c.capture(r)
}

func changesFixture() changes.Snapshot {
	return changes.Snapshot{State: "available", Files: []changes.File{{Path: "file.go", State: "text", Data: []byte("preexisting dirty state\n")}}}
}

func TestChangesRetentionContract(t *testing.T) {
	t.Run("memory", func(t *testing.T) { testChangesStore(t, NewChangesStore(nil)) })
	t.Run("postgres", func(t *testing.T) {
		url := os.Getenv("SWE_TEST_POSTGRES_URL")
		if url == "" {
			t.Skip("SWE_TEST_POSTGRES_URL is not set")
		}
		db, err := NewPostgresDatabase(context.Background(), url)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		testChangesStore(t, &postgresChangesStore{pool: db.pool})
	})
}

func testChangesStore(t *testing.T, s ChangesStore) {
	ctx := context.Background()
	id := RunIdentity{Namespace: "changes-test", NamespaceUID: types.UID(fmt.Sprint(time.Now().UnixNano())), UID: "run"}
	other := id
	other.NamespaceUID += "-replacement"
	defer s.Delete(ctx, id)
	defer s.Delete(ctx, other)
	r := ChangesRecord{Revision: 1, EnvironmentUID: "env", Baseline: changesFixture(), Current: changesFixture()}
	if err := s.Save(ctx, id, 0, r); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, other, 0, r); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, id, 0, r); !errors.Is(err, ErrChangesConflict) {
		t.Fatal("baseline overwrite accepted", err)
	}
	loaded, err := s.Load(ctx, id)
	if err != nil || string(loaded.Baseline.Files[0].Data) != "preexisting dirty state\n" {
		t.Fatalf("load: %+v %v", loaded, err)
	}
	r.Revision = 2
	r.Final = true
	var wg sync.WaitGroup
	successes := make(chan bool, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); successes <- s.Save(ctx, id, 1, r) == nil }()
	}
	wg.Wait()
	close(successes)
	won := 0
	for success := range successes {
		if success {
			won++
		}
	}
	if won != 1 {
		t.Fatalf("CAS winners %d", won)
	}
	r.Revision = 3
	if err := s.Save(ctx, id, 2, r); !errors.Is(err, ErrChangesConflict) {
		t.Fatal("final changed", err)
	}
	if err := s.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Load(ctx, id); got.Revision != 0 {
		t.Fatal("exact deletion failed")
	}
	if got, _ := s.Load(ctx, other); got.Revision != 1 {
		t.Fatal("replacement namespace deleted")
	}
}

func TestChangesAPIExactIdentityCapturePagesAndDeletion(t *testing.T) {
	store := NewChangesStore(nil)
	resolver := &fakeRunResolver{}
	access := &fakeAccess{}
	captures := 0
	server := NewServer(nil, ServerOptions{Access: access, Runs: resolver, ChangesStore: store, ChangesCapturer: fakeChangesCapturer{capture: func(r CaptureChangesRequest) (changes.Snapshot, error) {
		captures++
		snapshot := changesFixture()
		if !r.Baseline {
			snapshot.Files[0].Data = []byte("agent edit\n")
		}
		return snapshot, nil
	}}})
	call := func(method, query, uid, body string) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(method, "/api/v1/namespaces/project-1/runs/run-1/changes"+query, strings.NewReader(body))
		request.Header.Set(RunUIDHeader, uid)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		return response
	}
	if got := call("GET", "", "", ""); got.Code != 428 {
		t.Fatal(got.Code, got.Body)
	}
	if got := call("GET", "", "replacement", ""); got.Code != 409 {
		t.Fatal(got.Code, got.Body)
	}
	access.setError(errForbidden)
	if got := call("POST", "", "", `{}`); got.Code != 403 || captures != 0 {
		t.Fatal("authorization must precede identity and capture")
	}
	access.setError(nil)
	for range 2 {
		if got := call("POST", "", "run-1-uid", `{"baseline":true,"environmentUID":"env"}`); got.Code != 204 {
			t.Fatal(got.Code, got.Body)
		}
	}
	if captures != 1 {
		t.Fatal("baseline recaptured")
	}
	if got := call("POST", "", "run-1-uid", `{"environmentUID":"env","final":true}`); got.Code != 204 {
		t.Fatal(got.Code, got.Body)
	}
	response := call("GET", "", "run-1-uid", "")
	var result RunChanges
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.State != "changed" || !result.Final || result.Revision != 2 || len(result.Files) != 1 || result.Files[0].Diff != "" {
		t.Fatalf("list: %+v", result)
	}
	if got := call("GET", "?path=file.go&revision=2", "run-1-uid", ""); got.Code != 200 || !strings.Contains(got.Body.String(), "preexisting dirty state") {
		t.Fatal(got.Code, got.Body)
	}
	if got := call("GET", "?revision=1", "run-1-uid", ""); got.Code != 409 {
		t.Fatal("mixed revision accepted")
	}
	if got := call("GET", "?offset=9223372036854775807", "run-1-uid", ""); got.Code != 400 {
		t.Fatal("unbounded offset accepted")
	}
	resolver.deleting = true
	request := httptest.NewRequest(http.MethodDelete, transcriptURL, nil)
	request.Header.Set(RunUIDHeader, "run-1-uid")
	request.Header.Set(NamespaceUIDHeader, string(testNamespaceUID("project-1")))
	deleted := httptest.NewRecorder()
	server.Handler().ServeHTTP(deleted, request)
	if deleted.Code != 204 {
		t.Fatal(deleted.Code, deleted.Body)
	}
	id := RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-1-uid"}
	if got, _ := store.Load(context.Background(), id); got.Revision != 0 {
		t.Fatal("Run cleanup left Changes bytes")
	}
	if got := call("POST", "", "run-1-uid", `{}`); got.Code < 400 {
		t.Fatal("deleting Run recreated changes")
	}
}

func TestChangesUnavailableRetainsLastObservation(t *testing.T) {
	s := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: &fakeRunResolver{}, ChangesStore: NewChangesStore(nil), ChangesCapturer: fakeChangesCapturer{capture: func(CaptureChangesRequest) (changes.Snapshot, error) {
		return changes.Snapshot{State: "unavailable"}, nil
	}}})
	id := RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-1-uid"}
	r := ChangesRecord{Revision: 1, EnvironmentUID: "env", Baseline: changesFixture(), Current: changesFixture(), CapturedAt: time.Now()}
	if err := s.changes.Save(context.Background(), id, 0, r); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("POST", "/api/v1/namespaces/project-1/runs/run-1/changes", strings.NewReader(`{"environmentUID":"env","final":true}`))
	request.Header.Set(RunUIDHeader, "run-1-uid")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, request)
	if w.Code != 204 {
		t.Fatal(w.Code, w.Body)
	}
	got, err := s.changes.Load(context.Background(), id)
	if err != nil || !got.Final || !got.Unavailable || len(got.Current.Files) != 1 || !got.CapturedAt.Equal(r.CapturedAt) {
		t.Fatalf("lost retained capture %+v %v", got, err)
	}
}
