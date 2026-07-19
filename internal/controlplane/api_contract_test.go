package controlplane

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestAPIContractFixturesAreCanonicalHandlerAndMapperOutput(t *testing.T) {
	created := mustContractTime(t, "2026-07-19T18:04:13Z")
	lastActive := metav1.NewTime(mustContractTime(t, "2026-07-19T18:14:13Z"))
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "fix-flaky-42", UID: "eb7e3ff7-8117-4caf-912d-167b70eaee21", CreationTimestamp: metav1.NewTime(created)},
		Spec:       platformv1alpha1.RunSpec{ProjectRef: "org-repo", TemplateRef: "small", Agent: "claude-code", Prompt: "Fix the flaky test"},
		Status: platformv1alpha1.RunStatus{
			State:          platformv1alpha1.RunStateRunning,
			EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "run-fix-flaky-42", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
			Branch:         "swe/fix-flaky-42",
			Usage:          platformv1alpha1.RunUsage{CPUSeconds: 42, TokensIn: 1200, TokensOut: 340},
		},
	}
	listRun := run.DeepCopy()
	listRun.Spec.TemplateRef = ""
	listRun.Status = platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateAllocating}
	environmentCreated := mustContractTime(t, "2026-07-19T18:04:14Z")
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "run-fix-flaky-42", UID: "676d16c3-e625-4c36-a97c-e93065d05121", CreationTimestamp: metav1.NewTime(environmentCreated), Generation: 2},
		Spec:       platformv1alpha1.EnvironmentSpec{ProjectRef: "org-repo", TemplateRef: "small", Backend: platformv1alpha1.EnvironmentBackendPod},
		Status: platformv1alpha1.EnvironmentStatus{
			ObservedGeneration: 2,
			Phase:              platformv1alpha1.EnvironmentPhaseRunning,
			ClaimedBy:          &platformv1alpha1.RunReference{Name: "fix-flaky-42", UID: types.UID("eb7e3ff7-8117-4caf-912d-167b70eaee21")},
			LastActiveAt:       &lastActive,
			Conditions:         []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 2}},
		},
	}

	tests := []struct {
		name        string
		fixture     string
		status      int
		contentType string
		write       func(http.ResponseWriter)
	}{
		{name: "session handler", fixture: "session.json", status: http.StatusOK, contentType: "application/json", write: func(w http.ResponseWriter) {
			sessions := &fakeSessions{current: func(*http.Request) (Session, error) {
				return Session{Authenticated: true, Username: "console-user"}, nil
			}}
			request := httptest.NewRequest(http.MethodGet, "/api/v1/session", nil)
			NewServer(nil, ServerOptions{Sessions: sessions}).Handler().ServeHTTP(w, request)
		}},
		{name: "run detail handler and mapper", fixture: "run.json", status: http.StatusOK, contentType: "application/json", write: func(w http.ResponseWriter) {
			resources := &fakeResources{existing: runDTO(run)}
			request := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/default/runs/fix-flaky-42", nil)
			request.Header.Set("Authorization", "Bearer fixture")
			NewServer(nil, ServerOptions{Access: &recordingAccess{}, Resources: resources}).Handler().ServeHTTP(w, request)
		}},
		{name: "run list handler and mapper", fixture: "run-list.json", status: http.StatusOK, contentType: "application/json", write: func(w http.ResponseWriter) {
			resources := &fakeResources{listPage: RunList{Items: []Run{runDTO(listRun)}, Continue: "opaque-kubernetes-list-cursor"}}
			request := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/default/runs", nil)
			request.Header.Set("Authorization", "Bearer fixture")
			NewServer(nil, ServerOptions{Access: &recordingAccess{}, Resources: resources}).Handler().ServeHTTP(w, request)
		}},
		{name: "run create handler and mapper", fixture: "run.json", status: http.StatusCreated, contentType: "application/json", write: func(w http.ResponseWriter) {
			resources := &fakeResources{created: runDTO(run)}
			request := httptest.NewRequest(http.MethodPost, "https://console.test/api/v1/namespaces/default/runs", bytes.NewReader(readCanonicalFixture(t, "create-run.json", nil)))
			request.Header.Set("Authorization", "Bearer fixture")
			NewServer(nil, ServerOptions{Access: &recordingAccess{}, Resources: resources}).Handler().ServeHTTP(w, request)
		}},
		{name: "run cancel handler and mapper", fixture: "cancel-run.json", status: http.StatusOK, contentType: "application/json", write: func(w http.ResponseWriter) {
			cancelled := run.DeepCopy()
			cancelled.Spec.Cancel = true
			resources := &fakeResources{cancel: runDTO(cancelled)}
			request := httptest.NewRequest(http.MethodPost, "https://console.test/api/v1/namespaces/default/runs/fix-flaky-42/cancel", nil)
			request.Header.Set("Authorization", "Bearer fixture")
			NewServer(nil, ServerOptions{Access: &recordingAccess{}, Resources: resources}).Handler().ServeHTTP(w, request)
		}},
		{name: "environment handler and mapper", fixture: "environment.json", status: http.StatusOK, contentType: "application/json", write: func(w http.ResponseWriter) {
			resources := &fakeResources{environment: environmentDTO(environment)}
			request := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/default/environments/run-fix-flaky-42", nil)
			request.Header.Set("Authorization", "Bearer fixture")
			NewServer(nil, ServerOptions{Access: &recordingAccess{}, Resources: resources}).Handler().ServeHTTP(w, request)
		}},
		{name: "problem writer", fixture: "problem.json", status: http.StatusBadRequest, contentType: "application/problem+json", write: func(w http.ResponseWriter) {
			writeProblem(w, http.StatusBadRequest, "invalid-request", "Invalid request", "name is required")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expected := readCanonicalFixture(t, test.fixture, nil)
			response := httptest.NewRecorder()
			test.write(response)
			if response.Code != test.status || response.Header().Get("Content-Type") != test.contentType {
				t.Fatalf("status/content-type = %d/%q", response.Code, response.Header().Get("Content-Type"))
			}
			if !bytes.Equal(response.Body.Bytes(), expected) {
				t.Fatalf("response contract mismatch\n got: %s\nwant: %s", response.Body.Bytes(), expected)
			}
		})
	}
}

func TestCreateRunContractFixturePassesStrictRequestValidation(t *testing.T) {
	var expected CreateRunRequest
	fixture := readCanonicalFixture(t, "create-run.json", &expected)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/namespaces/default/runs", bytes.NewReader(fixture))
	response := httptest.NewRecorder()
	got, err := decodeCreateRun(response, request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("decoded request = %#v, want %#v", got, expected)
	}
}

func readCanonicalFixture(t *testing.T, name string, into any) []byte {
	t.Helper()
	fixture, err := os.ReadFile("testdata/contracts/" + name)
	if err != nil {
		t.Fatal(err)
	}
	if len(fixture) == 0 || fixture[len(fixture)-1] != '\n' {
		t.Fatalf("fixture %s must end with one newline", name)
	}
	var decoded any
	target := into
	if target == nil {
		target = &decoded
	}
	decoder := json.NewDecoder(bytes.NewReader(fixture))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		t.Fatalf("fixture %s does not match contract: %v", name, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("fixture %s has trailing JSON: %v", name, err)
	}
	var canonical []byte
	if into != nil {
		canonical, err = json.Marshal(target)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		var compact bytes.Buffer
		if err := json.Compact(&compact, bytes.TrimSuffix(fixture, []byte{'\n'})); err != nil {
			t.Fatal(err)
		}
		canonical = compact.Bytes()
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(fixture, canonical) {
		t.Fatalf("fixture %s is not canonical\n got: %s\nwant: %s", name, fixture, canonical)
	}
	return fixture
}

func mustContractTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
