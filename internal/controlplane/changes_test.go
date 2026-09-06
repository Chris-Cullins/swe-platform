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

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/sandboxd/changes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeChangesCapturer struct {
	capture func(CaptureChangesRequest) (changes.Snapshot, error)
	current func(context.Context) error
}

type changesReadFailure struct {
	client.Reader
	remaining int
	err       error
}

func (r *changesReadFailure) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*platformv1alpha1.Environment); ok {
		r.remaining--
		if r.remaining == 0 {
			return r.err
		}
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestChangesBaselinePropagatesEnvironmentAndExecutionReadFailures(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = platformv1alpha1.AddToScheme(scheme)
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "r"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateEnvironmentReady, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "e", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed}}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "e"}, Status: platformv1alpha1.EnvironmentStatus{ExecutionGeneration: 1, ClaimedBy: &platformv1alpha1.RunReference{Name: "r", UID: "r"}}}
	for _, read := range []int{1, 2} {
		t.Run(fmt.Sprint(read), func(t *testing.T) {
			uncertain := errors.New("temporary API read failure")
			reader := &changesReadFailure{Reader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, env).Build(), remaining: read, err: uncertain}
			got, err := (KubernetesChangesCapturer{Reader: reader}).Capture(context.Background(), "ns", "r", "r", CaptureChangesRequest{Baseline: true, EnvironmentUID: "e", ExecutionGeneration: 1}, nil)
			if !errors.Is(err, uncertain) || got.Snapshot.State != "" {
				t.Fatalf("uncertainty became baseline: %+v %v", got, err)
			}
		})
	}
}

func TestChangesTransientBaselineCannotPersistAndRetrySucceeds(t *testing.T) {
	fail := true
	s := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: &fakeRunResolver{}, ChangesStore: NewChangesStore(nil), ChangesCapturer: fakeChangesCapturer{capture: func(CaptureChangesRequest) (changes.Snapshot, error) {
		if fail {
			return changes.Snapshot{}, context.DeadlineExceeded
		}
		return changesFixture(), nil
	}}})
	id := RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-1-uid"}
	for _, want := range []int{503, 204} {
		r := httptest.NewRequest("POST", "/api/v1/namespaces/project-1/runs/run-1/changes", strings.NewReader(`{"baseline":true,"environmentUID":"env"}`))
		r.Header.Set(RunUIDHeader, "run-1-uid")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != want {
			t.Fatal(w.Code, w.Body)
		}
		got, err := s.changes.Load(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if fail && got.Revision != 0 {
			t.Fatal("transient baseline persisted")
		}
		if !fail && (got.Revision != 1 || got.Baseline.State != "available") {
			t.Fatalf("retry failed: %+v", got)
		}
		fail = false
	}
}

func (c fakeChangesCapturer) Capture(_ context.Context, _, _, _ string, r CaptureChangesRequest, _ []string) (CapturedChanges, error) {
	snapshot, err := c.capture(r)
	current := c.current
	if current == nil {
		current = func(context.Context) error { return nil }
	}
	return CapturedChanges{Snapshot: snapshot, Current: current}, err
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

func TestChangesBoundedPagesAndStablePollingRevision(t *testing.T) {
	snapshot := changesFixture()
	for i := range 60 {
		snapshot.Files = append(snapshot.Files, changes.File{Path: fmt.Sprintf("z%02d", i), State: "text", Data: []byte("new\n")})
	}
	store := NewChangesStore(nil)
	id := RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-1-uid"}
	if err := store.Save(context.Background(), id, 0, ChangesRecord{Revision: 1, EnvironmentUID: "env", Baseline: changesFixture(), Current: snapshot}); err != nil {
		t.Fatal(err)
	}
	s := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: &fakeRunResolver{}, ChangesStore: store, ChangesCapturer: fakeChangesCapturer{capture: func(CaptureChangesRequest) (changes.Snapshot, error) { return snapshot, nil }}})
	for range 3 {
		r := httptest.NewRequest("POST", "/api/v1/namespaces/project-1/runs/run-1/changes", strings.NewReader(`{"environmentUID":"env"}`))
		r.Header.Set(RunUIDHeader, "run-1-uid")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 204 {
			t.Fatal(w.Code, w.Body)
		}
	}
	for _, offset := range []int{0, 50} {
		r := httptest.NewRequest("GET", fmt.Sprintf("/api/v1/namespaces/project-1/runs/run-1/changes?revision=1&offset=%d", offset), nil)
		r.Header.Set(RunUIDHeader, "run-1-uid")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		var result RunChanges
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err, w.Body)
		}
		if w.Code != 200 || result.Revision != 1 || len(result.Files) != min(50, 60-offset) || result.Total != 60 {
			t.Fatalf("page %+v", result)
		}
		for _, f := range result.Files {
			if f.Diff != "" {
				t.Fatal("list included unbounded diff")
			}
		}
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

func TestChangesCaptureIsAdmittedForEntireDeletionDrain(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	resolver := &mutableRunResolver{uid: "run-1-uid"}
	s := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: resolver, ChangesStore: NewChangesStore(nil), ChangesCapturer: fakeChangesCapturer{capture: func(CaptureChangesRequest) (changes.Snapshot, error) {
		close(entered)
		<-release
		return changesFixture(), nil
	}}})
	id := RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-1-uid"}
	request := httptest.NewRequest("POST", "/api/v1/namespaces/project-1/runs/run-1/changes", strings.NewReader(`{"baseline":true}`))
	request.Header.Set(RunUIDHeader, string(id.UID))
	done := make(chan int, 1)
	go func() { w := httptest.NewRecorder(); s.Handler().ServeHTTP(w, request); done <- w.Code }()
	<-entered
	cutoff, err := s.transcriptGate.cutoff(id)
	if err != nil {
		t.Fatal(err)
	}
	s.transcriptGate.mu.Lock()
	admitted := cutoff.entry.appends
	s.transcriptGate.mu.Unlock()
	if admitted != 1 {
		t.Fatalf("capture admission count=%d", admitted)
	}
	resolver.mu.Lock()
	resolver.deleting = true
	resolver.mu.Unlock()
	close(release)
	if code := <-done; code != 409 {
		t.Fatalf("late capture returned %d", code)
	}
	if _, err := cutoff.beginCleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.changes.Delete(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	cutoff.finish(true)
	if record, _ := s.changes.Load(context.Background(), id); record.Revision != 0 {
		t.Fatal("late capture resurrected deleted bytes")
	}
}

func TestChangesPublicationRepeatsExecutionProof(t *testing.T) {
	s := NewServer(nil, ServerOptions{Access: &fakeAccess{}, Runs: &fakeRunResolver{}, ChangesStore: NewChangesStore(nil), ChangesCapturer: fakeChangesCapturer{
		capture: func(CaptureChangesRequest) (changes.Snapshot, error) { return changesFixture(), nil },
		current: func(context.Context) error {
			return errors.New("allocation released or backend replaced during reauthorization")
		},
	}})
	request := httptest.NewRequest("POST", "/api/v1/namespaces/project-1/runs/run-1/changes", strings.NewReader(`{"baseline":true}`))
	request.Header.Set(RunUIDHeader, "run-1-uid")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, request)
	if w.Code != 409 {
		t.Fatal(w.Code, w.Body)
	}
	if record, _ := s.changes.Load(context.Background(), RunIdentity{Namespace: "project-1", NamespaceUID: testNamespaceUID("project-1"), UID: "run-1-uid"}); record.Revision != 0 {
		t.Fatal("stale bytes published")
	}
}
