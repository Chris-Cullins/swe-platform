package controlplane

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	defaultRunListLimit = int64(50)
	maxRunListLimit     = int64(200)
	maxContinueLength   = 4096
	maxCreateRunBody    = 1 << 20
	maxAgentLength      = 128
)

func (s *Server) handleRunCollection(w http.ResponseWriter, r *http.Request, namespace string) {
	switch r.Method {
	case http.MethodGet:
		s.listRuns(w, r, namespace)
	case http.MethodPost:
		s.createRun(w, r, namespace)
	default:
		w.Header().Set("Allow", "GET, POST")
		writeProblem(w, http.StatusMethodNotAllowed, "method-not-allowed", "Method not allowed", "Run collections support GET and POST")
	}
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request, namespace string) {
	if !s.authorizeResource(w, r, ResourceAccess{Namespace: namespace, Verb: "list", Resource: "runs"}, true) {
		return
	}
	limit, continueToken, err := runListQuery(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid-query", "Invalid query", err.Error())
		return
	}
	if s.resources == nil {
		writeProblem(w, http.StatusServiceUnavailable, "resource-service-unavailable", "Resource service unavailable", "Run resources are not configured")
		return
	}
	page, err := s.resources.ListRuns(r.Context(), namespace, limit, continueToken)
	if err != nil {
		s.writeResourceError(w, "list runs", namespace, "", err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) createRun(w http.ResponseWriter, r *http.Request, namespace string) {
	if !s.authorizeResource(w, r, ResourceAccess{Namespace: namespace, Verb: "create", Resource: "runs"}, true) {
		return
	}
	if !s.requireMutationOrigin(w, r) {
		return
	}
	request, err := decodeCreateRun(w, r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid-request", "Invalid request", err.Error())
		return
	}
	if s.resources == nil {
		writeProblem(w, http.StatusServiceUnavailable, "resource-service-unavailable", "Resource service unavailable", "Run resources are not configured")
		return
	}
	created, err := s.resources.CreateRun(r.Context(), namespace, request)
	if err == nil {
		writeJSON(w, http.StatusCreated, created)
		return
	}
	if !apierrors.IsAlreadyExists(err) {
		s.writeResourceError(w, "create run", namespace, request.Name, err)
		return
	}

	// A retry may follow a response lost after Kubernetes persisted the Run.
	// Reading the collision requires its own exact-name authorization; otherwise
	// return only a generic conflict and never inspect or expose that Run.
	if s.access == nil || s.access.Authorize(r, ResourceAccess{Namespace: namespace, Verb: "get", Resource: "runs", Name: request.Name}, true) != nil {
		writeProblem(w, http.StatusConflict, "run-name-unavailable", "Run name unavailable", "the requested Run name cannot be reused")
		return
	}
	existing, getErr := s.resources.ResolveRunCreateCollision(r.Context(), namespace, request)
	if getErr != nil {
		if errors.Is(getErr, errRunIntentConflict) {
			writeProblem(w, http.StatusConflict, "run-intent-conflict", "Run intent conflict", "the Run name already exists with different immutable intent")
			return
		}
		s.writeResourceError(w, "resolve Run create collision", namespace, request.Name, getErr)
		return
	}
	writeJSON(w, http.StatusOK, existing)
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request, namespace, name string) {
	if !s.authorizeResource(w, r, ResourceAccess{Namespace: namespace, Verb: "get", Resource: "runs", Name: name}, true) {
		return
	}
	if s.resources == nil {
		writeProblem(w, http.StatusServiceUnavailable, "resource-service-unavailable", "Resource service unavailable", "Run resources are not configured")
		return
	}
	run, err := s.resources.GetRun(r.Context(), namespace, name)
	if err != nil {
		s.writeResourceError(w, "get run", namespace, name, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) handleCancelRun(w http.ResponseWriter, r *http.Request, namespace, name string) {
	if !s.authorizeResource(w, r, ResourceAccess{Namespace: namespace, Verb: "update", Resource: "runs", Name: name}, true) {
		return
	}
	if !s.requireMutationOrigin(w, r) {
		return
	}
	if err := requireEmptyBody(r); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid-request", "Invalid request", err.Error())
		return
	}
	if s.resources == nil {
		writeProblem(w, http.StatusServiceUnavailable, "resource-service-unavailable", "Resource service unavailable", "Run resources are not configured")
		return
	}
	run, err := s.resources.CancelRun(r.Context(), namespace, name)
	if err != nil {
		s.writeResourceError(w, "cancel run", namespace, name, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) handleGetEnvironment(w http.ResponseWriter, r *http.Request, namespace, name string) {
	if !s.authorizeResource(w, r, ResourceAccess{Namespace: namespace, Verb: "get", Resource: "environments", Name: name}, true) {
		return
	}
	if s.resources == nil {
		writeProblem(w, http.StatusServiceUnavailable, "resource-service-unavailable", "Resource service unavailable", "Environment resources are not configured")
		return
	}
	environment, err := s.resources.GetEnvironment(r.Context(), namespace, name)
	if err != nil {
		s.writeResourceError(w, "get environment", namespace, name, err)
		return
	}
	writeJSON(w, http.StatusOK, environment)
}

func (s *Server) authorizeResource(w http.ResponseWriter, r *http.Request, access ResourceAccess, allowSession bool) bool {
	if s.access == nil {
		writeRESTAccessError(w, errUnauthenticated)
		return false
	}
	if err := s.access.Authorize(r, access, allowSession); err != nil {
		writeRESTAccessError(w, err)
		return false
	}
	return true
}

func runListQuery(r *http.Request) (int64, string, error) {
	query := r.URL.Query()
	for key, values := range query {
		if key != "limit" && key != "continue" {
			return 0, "", fmt.Errorf("unknown query parameter %q", key)
		}
		if len(values) != 1 {
			return 0, "", fmt.Errorf("query parameter %q must be supplied once", key)
		}
	}
	limit := defaultRunListLimit
	if value := query.Get("limit"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 1 || parsed > maxRunListLimit {
			return 0, "", fmt.Errorf("limit must be an integer from 1 through %d", maxRunListLimit)
		}
		limit = parsed
	}
	continueToken := query.Get("continue")
	if len(continueToken) > maxContinueLength {
		return 0, "", fmt.Errorf("continue exceeds %d bytes", maxContinueLength)
	}
	return limit, continueToken, nil
}

func decodeCreateRun(w http.ResponseWriter, r *http.Request) (CreateRunRequest, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxCreateRunBody)
	defer r.Body.Close()
	var request CreateRunRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return CreateRunRequest{}, fmt.Errorf("request body exceeds %d bytes", maxCreateRunBody)
		}
		return CreateRunRequest{}, fmt.Errorf("invalid JSON body: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return CreateRunRequest{}, fmt.Errorf("request body must contain exactly one JSON object")
	}
	if problems := validation.IsDNS1123Subdomain(request.Name); len(problems) != 0 {
		return CreateRunRequest{}, fmt.Errorf("name must be a Kubernetes DNS subdomain: %s", strings.Join(problems, ", "))
	}
	for field, value := range map[string]string{
		"credentialProfile":    request.CredentialProfile,
		"selector.environment": request.Selector.Environment,
		"selector.project":     request.Selector.Project,
		"selector.template":    request.Selector.Template,
	} {
		if value != "" {
			if problems := validation.IsDNS1123Subdomain(value); len(problems) != 0 {
				return CreateRunRequest{}, fmt.Errorf("%s must be a Kubernetes DNS subdomain: %s", field, strings.Join(problems, ", "))
			}
		}
	}
	selector := request.Selector
	if selector.Environment == "" && selector.Project == "" && selector.Template == "" {
		return CreateRunRequest{}, fmt.Errorf("selector must set environment, project, or template")
	}
	if selector.Environment != "" && (selector.Project != "" || selector.Template != "") {
		return CreateRunRequest{}, fmt.Errorf("selector.environment cannot be combined with selector.project or selector.template")
	}
	if request.Agent == "" || len(request.Agent) > maxAgentLength {
		return CreateRunRequest{}, fmt.Errorf("agent is required and must not exceed %d bytes", maxAgentLength)
	}
	if request.Prompt == "" {
		return CreateRunRequest{}, fmt.Errorf("prompt is required")
	}
	return request, nil
}

func requireEmptyBody(r *http.Request) error {
	defer r.Body.Close()
	var one [1]byte
	if count, err := r.Body.Read(one[:]); count != 0 || (err != nil && err != io.EOF) {
		return fmt.Errorf("request body must be empty")
	}
	return nil
}

func (s *Server) writeResourceError(w http.ResponseWriter, operation, namespace, name string, err error) {
	switch {
	case apierrors.IsNotFound(err):
		writeProblem(w, http.StatusNotFound, "resource-not-found", "Resource not found", "the requested resource does not exist")
	case apierrors.IsAlreadyExists(err):
		writeProblem(w, http.StatusConflict, "resource-already-exists", "Resource already exists", "the requested resource name is already in use")
	case apierrors.IsInvalid(err), apierrors.IsBadRequest(err):
		writeProblem(w, http.StatusBadRequest, "invalid-resource", "Invalid resource", "Kubernetes rejected the resource intent")
	case apierrors.IsConflict(err):
		writeProblem(w, http.StatusConflict, "resource-conflict", "Resource conflict", "the resource changed concurrently; retry the request")
	case apierrors.IsTimeout(err), apierrors.IsServerTimeout(err), apierrors.IsServiceUnavailable(err):
		writeProblem(w, http.StatusServiceUnavailable, "kubernetes-unavailable", "Kubernetes unavailable", "the Kubernetes API is temporarily unavailable")
	default:
		s.log.Warn(operation, "namespace", namespace, "name", name, "error", err)
		writeProblem(w, http.StatusInternalServerError, "resource-error", "Resource operation failed", "the resource operation could not be completed")
	}
}
