package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	"github.com/Chris-Cullins/swe-platform/internal/sandboxclient"
	"github.com/Chris-Cullins/swe-platform/sandboxd/changes"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CaptureChangesRequest is internal operator intent, not caller-provided bytes.
type CaptureChangesRequest struct {
	Baseline            bool   `json:"baseline"`
	Final               bool   `json:"final"`
	EnvironmentUID      string `json:"environmentUID"`
	ExecutionGeneration int64  `json:"executionGeneration"`
	LifecycleEpoch      int64  `json:"lifecycleEpoch"`
	HoldPolicyRevision  int64  `json:"holdPolicyRevision"`
}

type RunChanges struct {
	RunUID      string           `json:"runUID"`
	Revision    int64            `json:"revision"`
	State       string           `json:"state"`
	CapturedAt  time.Time        `json:"capturedAt"`
	Final       bool             `json:"final"`
	Unavailable bool             `json:"unavailable"`
	Total       int              `json:"total"`
	Next        int              `json:"next,omitempty"`
	Files       []changes.Change `json:"files"`
}

var changesRequestSlots = make(chan struct{}, 4)

type ChangesCapturer interface {
	Capture(context.Context, string, string, string, CaptureChangesRequest, []string) (CapturedChanges, error)
}

type CapturedChanges struct {
	Snapshot changes.Snapshot
	// Current repeats exact allocation and the private backend/credential proof
	// immediately before persistence, after potentially slow reauthorization.
	Current func(context.Context) error
}

type KubernetesChangesCapturer struct{ Reader client.Reader }

func (c KubernetesChangesCapturer) Capture(ctx context.Context, namespace, name, uid string, request CaptureChangesRequest, baselinePaths []string) (CapturedChanges, error) {
	unavailable := CapturedChanges{Snapshot: changes.Snapshot{State: "unavailable"}}
	var run platformv1alpha1.Run
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &run); err != nil {
		return CapturedChanges{}, err
	}
	if string(run.UID) != uid || !run.DeletionTimestamp.IsZero() {
		return CapturedChanges{}, ErrChangesConflict
	}
	terminal := run.Status.State == platformv1alpha1.RunStateSucceeded || run.Status.State == platformv1alpha1.RunStateFailed || run.Status.State == platformv1alpha1.RunStateCancelled
	if request.Final != terminal || request.Baseline && (terminal || apiMeta.IsStatusConditionTrue(run.Status.Conditions, "AdapterAcceptanceAttempted")) {
		return CapturedChanges{}, ErrChangesConflict
	}
	if run.Status.EnvironmentRef == nil {
		return unavailable, nil
	}
	var env platformv1alpha1.Environment
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: run.Status.EnvironmentRef.Name}, &env); err != nil {
		return unavailable, nil
	}
	if !runOwnsOrClaimsEnvironment(&run, &env) || string(env.UID) != request.EnvironmentUID {
		if request.Final {
			return unavailable, nil
		}
		return CapturedChanges{}, ErrChangesConflict
	}
	if !request.Baseline && (run.Status.AcceptedEnvironmentExecutionGeneration == nil || *run.Status.AcceptedEnvironmentExecutionGeneration != env.Status.ExecutionGeneration) {
		return unavailable, nil
	}
	fence := lifecycle.CaptureExecutionFence(&env)
	if fence.ExecutionGeneration() != request.ExecutionGeneration || fence.LifecycleEpoch() != request.LifecycleEpoch || fence.HoldPolicyRevision() != request.HoldPolicyRevision {
		return CapturedChanges{}, ErrChangesConflict
	}
	connector := sandboxclient.Connector{Reader: c.Reader}
	execution, err := connector.ResolveExecution(ctx, fence)
	if err != nil {
		return unavailable, nil
	}
	observation, captureErr := connector.SnapshotChanges(ctx, fence, baselinePaths)
	current := func(ctx context.Context) error {
		var currentRun platformv1alpha1.Run
		if err := c.Reader.Get(ctx, client.ObjectKeyFromObject(&run), &currentRun); err != nil {
			return err
		}
		currentEnv, err := fence.Revalidate(ctx, c.Reader)
		if err != nil || currentRun.UID != run.UID || !currentRun.DeletionTimestamp.IsZero() || currentRun.Status.State != run.Status.State || !runOwnsOrClaimsEnvironment(&currentRun, currentEnv) || request.Baseline && apiMeta.IsStatusConditionTrue(currentRun.Status.Conditions, "AdapterAcceptanceAttempted") {
			return ErrChangesConflict
		}
		ok, err := connector.ExecutionCurrent(ctx, fence, execution)
		if err != nil || !ok {
			return ErrChangesConflict
		}
		if captureErr == nil {
			return connector.ChangesCurrent(ctx, fence, observation)
		}
		return nil
	}
	if err := current(ctx); err != nil {
		return CapturedChanges{}, err
	}
	if captureErr != nil {
		unavailable.Current = current
		return unavailable, nil
	}
	return CapturedChanges{Snapshot: observation.Snapshot, Current: current}, nil
}

func (s *Server) handleChanges(w http.ResponseWriter, r *http.Request, namespace, name string) {
	w.Header().Set("Cache-Control", "no-store")
	verb := "get"
	if r.Method == http.MethodPost {
		verb = "update"
	} else if r.Method != http.MethodGet {
		writeResourceMethodError(w, "GET, POST")
		return
	}
	if s.access == nil {
		writeAccessError(w, errUnauthenticated)
		return
	}
	authorization := ResourceAccess{Namespace: namespace, Resource: "runs", Subresource: "changes", Name: name, Verb: verb}
	if err := s.access.Authorize(r, authorization, verb == "get"); err != nil {
		writeAccessError(w, err)
		return
	}
	uid := strings.TrimSpace(r.Header.Get(RunUIDHeader))
	if uid == "" {
		writeProblem(w, 428, "run_uid_required", "Run UID required", "select an exact Run UID")
		return
	}
	if len(uid) > 128 {
		http.Error(w, "invalid Run UID", 400)
		return
	}
	id := RunIdentity{Namespace: namespace, NamespaceUID: namespaceUIDFromRequest(r), UID: types.UID(uid)}
	if id.NamespaceUID == "" || s.runs == nil || s.changes == nil {
		http.Error(w, "changes unavailable", 503)
		return
	}
	select {
	case changesRequestSlots <- struct{}{}:
		defer func() { <-changesRequestSlots }()
	default:
		http.Error(w, "changes request capacity exceeded", 429)
		return
	}
	ctx, admission, err := s.transcriptGate.admit(r.Context(), id, false)
	if err != nil {
		writeTranscriptStoreError(w, err)
		return
	}
	defer admission.release()
	r = r.WithContext(ctx)
	resolved, err := s.runs.ResolveRun(ctx, namespace, name)
	if err != nil {
		s.writeResourceError(w, "resolve Run changes", namespace, name, err)
		return
	}
	if resolved.UID != id.UID {
		writeTranscriptProblem(w, 409, "run_uid_mismatch", "Run UID mismatch")
		return
	}
	if resolved.Deleting {
		writeTranscriptStoreError(w, ErrTranscriptCutoff)
		return
	}
	record, err := s.changes.Load(ctx, id)
	if err != nil {
		http.Error(w, "changes storage unavailable", 503)
		return
	}
	if r.Method == http.MethodGet {
		s.writeChanges(w, r, uid, record)
		return
	}
	var request CaptureChangesRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid capture request", 400)
		return
	}
	// Baselines and terminal results are immutable and retry-safe.
	if record.Final || request.Baseline && record.Revision > 0 {
		w.WriteHeader(204)
		return
	}
	if s.changesCapturer == nil {
		http.Error(w, "changes capture unavailable", 503)
		return
	}
	baselinePaths := make([]string, 0, len(record.Baseline.Files))
	for _, file := range record.Baseline.Files {
		baselinePaths = append(baselinePaths, file.Path)
	}
	captured, err := s.changesCapturer.Capture(ctx, namespace, name, uid, request, baselinePaths)
	if err != nil {
		http.Error(w, "changes execution is no longer current", 409)
		return
	}
	// Repeat authorization and exact identity after potentially slow capture.
	if err := s.access.Authorize(r, authorization, false); err != nil {
		writeAccessError(w, err)
		return
	}
	resolved, err = s.runs.ResolveRun(ctx, namespace, name)
	if err != nil || resolved.UID != id.UID || resolved.Deleting || namespaceUIDFromRequest(r) != id.NamespaceUID {
		http.Error(w, "changes identity changed", 409)
		return
	}
	snapshot := captured.Snapshot
	expected := record.Revision
	unchanged := expected > 0 && !request.Final && !record.Unavailable && snapshot.State == "available" && reflect.DeepEqual(record.Current, snapshot)
	if expected == 0 {
		record.EnvironmentUID = request.EnvironmentUID
		record.Baseline = changes.Snapshot{State: "unavailable"}
		record.Current = changes.Snapshot{State: "unavailable"}
		if request.Baseline {
			record.Baseline = snapshot
		}
	} else if record.EnvironmentUID != request.EnvironmentUID {
		http.Error(w, "changes environment changed", 409)
		return
	}
	record.Revision++
	record.Final = request.Final
	record.Unavailable = snapshot.State != "available"
	if snapshot.State == "available" {
		record.Current = snapshot
		record.CapturedAt = time.Now().UTC()
	}
	if captured.Current != nil {
		if err := captured.Current(ctx); err != nil {
			http.Error(w, "changes execution changed before publication", 409)
			return
		}
	} else if snapshot.State == "available" {
		http.Error(w, "changes proof unavailable", 503)
		return
	}
	// Identical polling observations keep their revision so a person can select
	// a file without racing an otherwise meaningless two-second revision bump.
	if unchanged {
		w.WriteHeader(204)
		return
	}
	if err := s.changes.Save(ctx, id, expected, record); err != nil {
		if errors.Is(err, ErrChangesConflict) {
			http.Error(w, "changes revision changed", 409)
		} else {
			http.Error(w, "changes storage unavailable", 503)
		}
		return
	}
	w.WriteHeader(204)
}

func (s *Server) writeChanges(w http.ResponseWriter, r *http.Request, uid string, record ChangesRecord) {
	if revision := r.URL.Query().Get("revision"); revision != "" && revision != strconv.FormatInt(record.Revision, 10) {
		http.Error(w, "changes revision changed; refresh the file list", 409)
		return
	}
	offset := 0
	if value := r.URL.Query().Get("offset"); value != "" {
		var err error
		offset, err = strconv.Atoi(value)
		if err != nil || offset < 0 {
			http.Error(w, "invalid offset", 400)
			return
		}
	}
	state, files := changes.Compare(record.Baseline, record.Current)
	response := RunChanges{RunUID: uid, Revision: record.Revision, State: state, CapturedAt: record.CapturedAt, Final: record.Final, Unavailable: record.Unavailable || record.Revision == 0, Total: len(files), Files: []changes.Change{}}
	if path := r.URL.Query().Get("path"); path != "" {
		for _, f := range files {
			if f.Path == path {
				response.Files = append(response.Files, f)
				break
			}
		}
		if len(response.Files) == 0 {
			http.Error(w, "changed file not found", 404)
			return
		}
	} else {
		if offset > len(files) {
			http.Error(w, "invalid offset", 400)
			return
		}
		end := min(len(files), offset+changes.PageSize)
		for _, f := range files[offset:end] {
			f.Diff = ""
			response.Files = append(response.Files, f)
		}
		if end < len(files) {
			response.Next = end
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}
