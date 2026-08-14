// Package controlplane implements the user-facing HTTP API.
package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

const (
	namespacedPathPrefix               = "/api/v1/namespaces/"
	defaultTranscriptHeartbeatInterval = 15 * time.Second
	streamAuthorizationTimeout         = 5 * time.Second
)

type appendTranscriptRequest struct {
	Source         string          `json:"source"`
	SourceSequence *uint64         `json:"sourceSequence,omitempty"`
	IdempotencyKey string          `json:"idempotencyKey"`
	Type           string          `json:"type"`
	Data           json.RawMessage `json:"data"`
}

// Server serves the control-plane API and live transcript streams.
type Server struct {
	log                   *slog.Logger
	store                 TranscriptStore
	access                AccessController
	sessions              SessionAuthenticator
	resources             ResourceService
	runs                  RunResolver
	terminalDialer        TerminalDialer
	consoleAssets         fs.FS
	trustProxy            bool
	allowInsecureSessions bool
	heartbeat             time.Duration
	terminalAuthPoll      time.Duration
	streams               context.Context
	watchAdmission        *watchAdmission
	terminalOpenTimeout   time.Duration
	terminalWriteTimeout  time.Duration
	metrics               *Metrics
	portal                *portalGateway
	transcriptGate        *transcriptGate
}

// RunResolution is the exact current identity and deletion state of a Run.
type RunResolution struct {
	UID      types.UID
	Deleting bool
}

// RunResolver verifies that a namespaced Run exists before transcript state is used.
type RunResolver interface {
	ResolveRun(context.Context, string, string) (RunResolution, error)
}

// KubernetesRunResolver resolves Runs through the Kubernetes API.
type KubernetesRunResolver struct {
	Client client.Client
}

// ResolveRun verifies that the requested Run exists in the authorized namespace.
func (r KubernetesRunResolver) ResolveRun(ctx context.Context, namespace, name string) (RunResolution, error) {
	var run platformv1alpha1.Run
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &run); err != nil {
		return RunResolution{}, err
	}
	return RunResolution{UID: run.UID, Deleting: !run.DeletionTimestamp.IsZero()}, nil
}

// ServerOptions supplies the control plane's resource and authorization dependencies.
type ServerOptions struct {
	Access                      AccessController
	Sessions                    SessionAuthenticator
	Resources                   ResourceService
	Runs                        RunResolver
	TranscriptStore             TranscriptStore
	TerminalDialer              TerminalDialer
	PortalResolver              PortalResolver
	PortalEnvironmentEnumerator PortalEnvironmentEnumerator
	PortalDialer                PortalDialer
	PortalSuffix                string
	PortalScheme                string
	ConsoleAssets               fs.FS
	TrustProxy                  bool
	AllowInsecureSessions       bool
	// StreamLifecycle is canceled when long-lived SSE and terminal handlers
	// must exit during process shutdown. Ordinary requests do not use it.
	StreamLifecycle context.Context
	// TranscriptHeartbeatInterval controls SSE keepalive comments. Values less
	// than or equal to zero use the production default.
	TranscriptHeartbeatInterval time.Duration
	Metrics                     *Metrics
}

// NewServer constructs a control-plane API handler.
func NewServer(log *slog.Logger, options ServerOptions) *Server {
	if log == nil {
		log = slog.Default()
	}
	heartbeat := options.TranscriptHeartbeatInterval
	if heartbeat <= 0 {
		heartbeat = defaultTranscriptHeartbeatInterval
	}
	streams := options.StreamLifecycle
	if streams == nil {
		streams = context.Background()
	}
	server := &Server{
		log:                   log,
		store:                 options.TranscriptStore,
		access:                options.Access,
		sessions:              options.Sessions,
		resources:             options.Resources,
		runs:                  options.Runs,
		terminalDialer:        options.TerminalDialer,
		consoleAssets:         options.ConsoleAssets,
		trustProxy:            options.TrustProxy,
		allowInsecureSessions: options.AllowInsecureSessions,
		heartbeat:             heartbeat,
		terminalAuthPoll:      terminalPolicyPollInterval,
		streams:               streams,
		watchAdmission:        processWatchAdmission,
		terminalOpenTimeout:   terminalHandshakeTimeout,
		terminalWriteTimeout:  terminalStreamingWriteTimeout,
		metrics:               options.Metrics,
		transcriptGate:        newTranscriptGate(maxTranscriptGateEntries),
	}
	server.portal = newPortalGateway(server, options)
	return server
}

func (s *Server) withStreamLifecycle(r *http.Request) (*http.Request, func()) {
	ctx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(s.streams, cancel)
	if s.streams.Err() != nil {
		cancel()
	}
	return r.WithContext(ctx), func() {
		stop()
		cancel()
	}
}

// Handler returns the HTTP handler for the API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/api/v1/session", s.handleSession)
	mux.HandleFunc(namespacedPathPrefix, s.handleNamespacedAPI)
	base := http.Handler(mux)
	if s.consoleAssets == nil {
		if !s.portal.enabled() {
			return base
		}
		return s.portal.wrap(base)
	}
	console := newConsoleHandler(s.consoleAssets)
	base = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isReservedServerPath(r.URL.Path) {
			mux.ServeHTTP(w, r)
			return
		}
		console.ServeHTTP(w, r)
	})
	if s.portal.enabled() {
		return s.portal.wrap(base)
	}
	return base
}

func (s *Server) handleNamespacedAPI(w http.ResponseWriter, r *http.Request) {
	if s.portal != nil {
		if namespace, environment, service, ok := portalDiscoveryPath(r.URL.EscapedPath()); ok {
			if r.Method == http.MethodGet {
				s.portal.discover(w, r, namespace, environment, service)
			} else {
				writeResourceMethodError(w, "GET")
			}
			return
		}
	}
	if namespace, run, runUID, environmentUID, ok := browserRunTerminalPath(r.URL.EscapedPath()); ok {
		if r.Method == http.MethodGet {
			s.handleBrowserRunTerminal(w, r, namespace, run, runUID, environmentUID)
		} else {
			writeResourceMethodError(w, "GET")
		}
		return
	}
	if namespace, run, runUID, environmentUID, ok := browserRunPortalPath(r.URL.EscapedPath()); ok {
		if r.Method == http.MethodGet {
			s.handleBrowserRunPortals(w, r, namespace, run, runUID, environmentUID)
		} else {
			writeResourceMethodError(w, "GET")
		}
		return
	}
	if namespace, run, runUID, environmentUID, service, ok := browserRunPortalOpenPath(r.URL.EscapedPath()); ok {
		if r.Method == http.MethodPost {
			s.handleBrowserRunPortalOpen(w, r, namespace, run, runUID, environmentUID, service)
		} else {
			writeResourceMethodError(w, "POST")
		}
		return
	}
	namespace, resource, name, subresource, ok := namespacedResource(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if resource == "runs" && name == "" && subresource == "" {
		s.handleRunCollection(w, r, namespace)
		return
	}
	if resource == "runs" && name != "" && subresource == "" {
		if r.Method == http.MethodGet {
			s.handleGetRun(w, r, namespace, name)
		} else {
			writeResourceMethodError(w, "GET")
		}
		return
	}
	if resource == "runs" && name != "" && subresource == "cancel" {
		if r.Method == http.MethodPost {
			s.handleCancelRun(w, r, namespace, name)
		} else {
			writeResourceMethodError(w, "POST")
		}
		return
	}
	if resource == "runs" && name != "" && subresource == "terminal" && r.Method == http.MethodGet {
		s.handleRunTerminal(w, r, namespace, name)
		return
	}
	if resource == "environments" && name != "" && subresource == "" {
		if r.Method == http.MethodGet {
			s.handleGetEnvironment(w, r, namespace, name)
		} else {
			writeResourceMethodError(w, "GET")
		}
		return
	}
	if resource == "runs" && subresource == "transcript" {
		s.handleTranscript(w, r, namespace, name)
		return
	}
	if resource == "environments" && subresource == "terminal" && r.Method == http.MethodGet {
		s.handleTerminal(w, r, namespace, name)
		return
	}
	http.NotFound(w, r)
}

// browserRunTerminalPath keeps each immutable UID in one percent-encoded path
// segment. It returns the UID segments undecoded so Run authorization can happen
// before malformed identity is distinguished from stale identity.
func browserRunTerminalPath(path string) (namespace, run, runUID, environmentUID string, ok bool) {
	return browserRunIdentityPath(path, "terminal")
}

func browserRunPortalPath(path string) (namespace, run, runUID, environmentUID string, ok bool) {
	return browserRunIdentityPath(path, "portals")
}

func browserRunPortalOpenPath(path string) (namespace, run, runUID, environmentUID, service string, ok bool) {
	remainder := strings.TrimPrefix(path, namespacedPathPrefix)
	parts := strings.Split(remainder, "/")
	if remainder == path || len(parts) != 8 || parts[1] != "runs" || parts[3] != "portals" || parts[7] != "open" {
		return "", "", "", "", "", false
	}
	values := make([]string, 5)
	for i, part := range []string{parts[0], parts[2], parts[4], parts[5], parts[6]} {
		decoded, err := url.PathUnescape(part)
		if err != nil || decoded == "" || strings.Contains(decoded, "/") {
			return "", "", "", "", "", false
		}
		values[i] = decoded
	}
	return values[0], values[1], values[2], values[3], values[4], true
}

func browserRunIdentityPath(path, subresource string) (namespace, run, runUID, environmentUID string, ok bool) {
	remainder := strings.TrimPrefix(path, namespacedPathPrefix)
	if remainder == path {
		return "", "", "", "", false
	}
	parts := strings.Split(remainder, "/")
	if len(parts) != 6 || parts[1] != "runs" || parts[3] != subresource {
		return "", "", "", "", false
	}
	decodedNamespace, err := url.PathUnescape(parts[0])
	if err != nil || decodedNamespace == "" {
		return "", "", "", "", false
	}
	decodedRun, err := url.PathUnescape(parts[2])
	if err != nil || decodedRun == "" {
		return "", "", "", "", false
	}
	return decodedNamespace, decodedRun, parts[4], parts[5], true
}

func writeResourceMethodError(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeProblem(w, http.StatusMethodNotAllowed, "method-not-allowed", "Method not allowed", "this resource supports "+allow)
}

func namespacedResource(path string) (namespace, resource, name, subresource string, ok bool) {
	remainder := strings.TrimPrefix(path, namespacedPathPrefix)
	if remainder == path {
		return "", "", "", "", false
	}
	parts := strings.Split(remainder, "/")
	if len(parts) < 2 || len(parts) > 4 {
		return "", "", "", "", false
	}
	for _, part := range parts {
		if part == "" {
			return "", "", "", "", false
		}
	}
	namespace, resource = parts[0], parts[1]
	if len(parts) >= 3 {
		name = parts[2]
	}
	if len(parts) == 4 {
		subresource = parts[3]
	}
	return namespace, resource, name, subresource, true
}

func (s *Server) handleTranscript(w http.ResponseWriter, r *http.Request, namespace, run string) {
	verb := "get"
	allowSession := true
	if r.Method == http.MethodPost {
		verb = "update"
		allowSession = false
	} else if r.Method == http.MethodDelete {
		verb = "delete"
		allowSession = false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPost && r.Method != http.MethodDelete {
		w.Header().Set("Allow", "GET, POST, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.access == nil {
		writeAccessError(w, errUnauthenticated)
		return
	}
	if err := s.access.Authorize(r, ResourceAccess{Namespace: namespace, Verb: verb, Resource: "runs", Subresource: "transcript", Name: run}, allowSession); err != nil {
		writeAccessError(w, err)
		return
	}
	// Every transcript operation carries the exact expected Run UID so a
	// delete/recreate replacement never receives stale events or readers.
	// Authorization is deliberately checked first, then identity validation
	// happens before the resolver or store is touched.
	expectedUID := types.UID(strings.TrimSpace(r.Header.Get(RunUIDHeader)))
	if expectedUID == "" {
		writeProblem(w, http.StatusPreconditionRequired, "run_uid_required", "Run UID required", "the "+RunUIDHeader+" header is required for transcript requests")
		return
	}
	if len(expectedUID) > 128 {
		http.Error(w, "run UID exceeds its size limit", http.StatusBadRequest)
		return
	}
	if s.runs == nil {
		http.Error(w, "run resolver is unavailable", http.StatusServiceUnavailable)
		return
	}
	namespaceUID := namespaceUIDFromRequest(r)
	if namespaceUID == "" {
		s.log.Warn("authorized transcript request without a Namespace UID", "namespace", namespace, "run", run)
		http.Error(w, "namespace identity is unavailable", http.StatusServiceUnavailable)
		return
	}
	if r.Method == http.MethodDelete {
		expectedNamespaceUID := types.UID(strings.TrimSpace(r.Header.Get(NamespaceUIDHeader)))
		if expectedNamespaceUID == "" {
			writeProblem(w, http.StatusPreconditionRequired, "namespace-uid-required", "Namespace UID required", "the "+NamespaceUIDHeader+" header is required for transcript deletion")
			return
		}
		if len(expectedNamespaceUID) > 128 {
			http.Error(w, "namespace UID exceeds its size limit", http.StatusBadRequest)
			return
		}
		if expectedNamespaceUID != namespaceUID {
			writeTranscriptProblem(w, http.StatusConflict, "namespace_uid_mismatch", "Namespace UID mismatch")
			return
		}
	}
	identity := RunIdentity{Namespace: namespace, NamespaceUID: namespaceUID, UID: expectedUID}

	var admission *transcriptAdmission
	var cutoff *transcriptCutoff
	var err error
	if r.Method == http.MethodDelete {
		cutoff, err = s.transcriptGate.cutoff(identity)
		if err != nil {
			writeTranscriptStoreError(w, err)
			return
		}
		defer cutoff.abort()
	} else {
		var admittedContext context.Context
		admittedContext, admission, err = s.transcriptGate.admit(r.Context(), identity, r.Method == http.MethodGet)
		if err != nil {
			writeTranscriptStoreError(w, err)
			return
		}
		defer admission.release()
		r = r.WithContext(admittedContext)
	}

	resolved, err := s.runs.ResolveRun(r.Context(), namespace, run)
	if err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, "run not found", http.StatusNotFound)
		} else {
			s.log.Warn("resolve transcript run", "namespace", namespace, "run", run, "error", err)
			http.Error(w, "run resolver is unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	if resolved.UID == "" {
		s.log.Warn("resolved transcript run without a UID", "namespace", namespace, "run", run)
		http.Error(w, "run identity is unavailable", http.StatusServiceUnavailable)
		return
	}

	if expectedUID != resolved.UID {
		writeTranscriptProblem(w, http.StatusConflict, "run_uid_mismatch", "Run UID mismatch")
		return
	}
	if r.Method != http.MethodDelete && resolved.Deleting {
		writeTranscriptStoreError(w, ErrTranscriptCutoff)
		return
	}
	if r.Method == http.MethodDelete {
		if !resolved.Deleting {
			writeTranscriptProblem(w, http.StatusConflict, "run-not-deleting", "Run is not deleting")
			return
		}
		cutoff.validate()
		s.deleteTranscript(w, r, identity, ResourceAccess{Namespace: namespace, Verb: "delete", Resource: "runs", Subresource: "transcript", Name: run}, cutoff)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.streamTranscript(w, r, identity, ResourceAccess{Namespace: namespace, Verb: "get", Resource: "runs", Subresource: "transcript", Name: run})
	case http.MethodPost:
		s.appendTranscript(w, r, identity)
	}
}

func (s *Server) deleteTranscript(w http.ResponseWriter, r *http.Request, run RunIdentity, authorization ResourceAccess, cutoff *transcriptCutoff) {
	started := time.Now()
	completed, err := cutoff.beginCleanup(r.Context())
	if err != nil {
		s.metrics.observeCleanup(started, "error")
		writeTranscriptStoreError(w, err)
		return
	}
	if completed {
		s.metrics.observeCleanup(started, "already_complete")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// Draining may block, so repeat the complete live TokenReview -> exact SAR ->
	// tenancy/Namespace -> deleting Run proof immediately before physical deletion.
	if err := s.access.Authorize(r, authorization, false); err != nil {
		cutoff.finish(false)
		s.metrics.observeCleanup(started, "error")
		writeAccessError(w, err)
		return
	}
	if namespaceUIDFromRequest(r) != run.NamespaceUID {
		cutoff.finish(false)
		s.metrics.observeCleanup(started, "error")
		writeTranscriptProblem(w, http.StatusConflict, "namespace_uid_mismatch", "Namespace UID mismatch")
		return
	}
	resolved, err := s.runs.ResolveRun(r.Context(), run.Namespace, authorization.Name)
	if err != nil || resolved.UID != run.UID || !resolved.Deleting {
		cutoff.finish(false)
		s.metrics.observeCleanup(started, "error")
		if err != nil {
			s.log.Warn("revalidate deleting transcript run", "namespace", run.Namespace, "run", authorization.Name, "error", err)
			http.Error(w, "run resolver is unavailable", http.StatusServiceUnavailable)
		} else if resolved.UID != run.UID {
			writeTranscriptProblem(w, http.StatusConflict, "run_uid_mismatch", "Run UID mismatch")
		} else {
			writeTranscriptProblem(w, http.StatusConflict, "run-not-deleting", "Run is not deleting")
		}
		return
	}
	if s.store == nil {
		cutoff.finish(true)
		s.metrics.observeCleanup(started, "absent")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	result, err := s.store.Delete(r.Context(), run)
	if err != nil {
		cutoff.finish(false)
		s.metrics.observeCleanup(started, "error")
		s.log.Error("delete transcript", "namespace", run.Namespace, "runUID", run.UID, "error", err)
		writeTranscriptStoreError(w, err)
		return
	}
	cutoff.finish(true)
	outcome := "absent"
	if result.Deleted {
		outcome = "deleted"
	}
	s.metrics.observeCleanup(started, outcome)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) appendTranscript(w http.ResponseWriter, r *http.Request, run RunIdentity) {
	if s.store == nil {
		http.Error(w, "transcript store is unavailable", http.StatusServiceUnavailable)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()

	var request appendTranscriptRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeTranscriptProblem(w, http.StatusRequestEntityTooLarge, "event_too_large", "transcript event exceeds the 1 MiB request limit")
			return
		}
		http.Error(w, "invalid transcript event: "+err.Error(), http.StatusBadRequest)
		return
	}
	legacy := request.Source == "" && request.IdempotencyKey == "" && request.SourceSequence == nil
	if request.Type == "" || len(request.Data) == 0 || !json.Valid(request.Data) {
		http.Error(w, "type and valid JSON data are required", http.StatusBadRequest)
		return
	}
	if !legacy && (request.Source == "" || request.IdempotencyKey == "") {
		http.Error(w, "source and idempotencyKey must be supplied together", http.StatusBadRequest)
		return
	}
	if len(request.Source) > 128 || len(request.IdempotencyKey) > 256 || len(request.Type) > 128 {
		http.Error(w, "source, idempotencyKey, or type exceeds its size limit", http.StatusBadRequest)
		return
	}
	if legacy {
		request.Source = "legacy-unkeyed"
		request.IdempotencyKey = newLegacyTranscriptKey()
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeTranscriptProblem(w, http.StatusRequestEntityTooLarge, "event_too_large", "transcript event exceeds the 1 MiB request limit")
			return
		}
		http.Error(w, "request body must contain one JSON object", http.StatusBadRequest)
		return
	}

	appendStarted := time.Now()
	result, err := s.store.Append(r.Context(), run, AppendTranscriptInput{
		Source:         request.Source,
		SourceSequence: request.SourceSequence,
		IdempotencyKey: request.IdempotencyKey,
		Type:           request.Type,
		Data:           request.Data,
	})
	if err != nil {
		outcome := "error"
		if isTranscriptContractError(err) {
			outcome = "rejected"
		}
		s.metrics.observeAppend(appendStarted, outcome)
		if !isTranscriptContractError(err) {
			s.log.Error("append transcript event", "namespace", run.Namespace, "runUID", run.UID, "error", err)
		}
		writeTranscriptStoreError(w, err)
		return
	}
	if result.Replayed {
		s.metrics.observeAppend(appendStarted, "replayed")
	} else {
		s.metrics.observeAppend(appendStarted, "committed")
	}
	w.Header().Set("Content-Type", "application/json")
	if legacy {
		w.WriteHeader(http.StatusAccepted)
	} else if result.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
	_ = json.NewEncoder(w).Encode(result.Event)
}

func (s *Server) streamTranscript(w http.ResponseWriter, r *http.Request, run RunIdentity, authorization ResourceAccess) {
	if s.store == nil {
		http.Error(w, "transcript store is unavailable", http.StatusServiceUnavailable)
		return
	}
	r, cancelStream := s.withStreamLifecycle(r)
	defer cancelStream()
	authorizationDeadline := time.Now().Add(s.heartbeat)
	reauthorize := func() error {
		timeout := streamAuthorizationTimeout
		if s.heartbeat < timeout {
			timeout = s.heartbeat
		}
		authContext, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		authRequest := r.Clone(authContext)
		if err := s.access.Authorize(authRequest, authorization, true); err != nil {
			return err
		}
		if namespaceUIDFromRequest(authRequest) != run.NamespaceUID {
			return ErrTranscriptIdentity
		}
		resolved, err := s.runs.ResolveRun(authContext, run.Namespace, authorization.Name)
		if err != nil {
			return err
		}
		if resolved.UID != run.UID {
			return ErrTranscriptIdentity
		}
		if resolved.Deleting {
			return ErrTranscriptCutoff
		}
		authorizationDeadline = time.Now().Add(s.heartbeat)
		return nil
	}
	ensureAuthorization := func() error {
		if time.Now().Before(authorizationDeadline) {
			return nil
		}
		return reauthorize()
	}
	cursor := r.Header.Get("Last-Event-ID")
	if cursor == "" {
		cursor = r.URL.Query().Get("after")
	}

	subscription, err := s.store.Subscribe(r.Context(), run, cursor)
	if err != nil {
		if !isTranscriptContractError(err) {
			s.log.Error("subscribe to transcript", "namespace", run.Namespace, "runUID", run.UID, "error", err)
		}
		writeTranscriptStoreError(w, err)
		return
	}
	defer subscription.Unsubscribe()
	if s.metrics != nil {
		s.metrics.transcriptSubscribers.Inc()
		defer s.metrics.transcriptSubscribers.Dec()
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	controller := http.NewResponseController(w)
	write := func(payload string) error {
		writeDeadline := time.Now().Add(15 * time.Second)
		if authorizationDeadline.Before(writeDeadline) {
			writeDeadline = authorizationDeadline
		}
		if err := controller.SetWriteDeadline(writeDeadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return err
		}
		if _, err := io.WriteString(w, payload); err != nil {
			return err
		}
		if err := controller.Flush(); err != nil {
			return err
		}
		if err := controller.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return err
		}
		return nil
	}
	writeEvent := func(event TranscriptEvent) error {
		if err := ensureAuthorization(); err != nil {
			return err
		}
		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if err := write(fmt.Sprintf("id: %s\nevent: transcript\ndata: %s\n\n", event.ID, payload)); err != nil {
			return err
		}
		return nil
	}
	if subscription.Gap != nil {
		if err := ensureAuthorization(); err != nil {
			return
		}
		payload, marshalErr := json.Marshal(subscription.Gap)
		if marshalErr != nil {
			s.metrics.observeDelivery("gap", "error", nil)
			return
		}
		if err = write(fmt.Sprintf("event: transcript-gap\ndata: %s\n\n", payload)); err != nil {
			s.metrics.observeDelivery("gap", "error", nil)
			return
		}
		s.metrics.observeDelivery("gap", "delivered", nil)
	}

	for _, event := range subscription.History {
		if err := writeEvent(event); err != nil {
			s.metrics.observeDelivery("replay", "error", nil)
			return
		}
		s.metrics.observeDelivery("replay", "delivered", &event)
	}
	subscription.History = nil
	if err := controller.Flush(); err != nil {
		return
	}
	for {
		if err := ensureAuthorization(); err != nil {
			return
		}
		authorizationTimer := time.NewTimer(time.Until(authorizationDeadline))
		select {
		case event := <-subscription.Events:
			if !authorizationTimer.Stop() {
				<-authorizationTimer.C
			}
			if err := writeEvent(event); err != nil {
				s.metrics.observeDelivery("live", "error", nil)
				return
			}
			s.metrics.observeDelivery("live", "delivered", &event)
		case <-subscription.Dropped:
			if !authorizationTimer.Stop() {
				<-authorizationTimer.C
			}
			s.metrics.observeDelivery("live", "dropped", nil)
			s.log.Warn("closing dropped transcript subscriber", "namespace", run.Namespace, "runUID", run.UID)
			return
		case <-authorizationTimer.C:
			if err := reauthorize(); err != nil {
				return
			}
			if err := write(": ping\n\n"); err != nil {
				return
			}
		case <-r.Context().Done():
			if !authorizationTimer.Stop() {
				<-authorizationTimer.C
			}
			return
		}
	}
}
