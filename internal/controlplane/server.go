// Package controlplane implements the user-facing HTTP API.
package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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
	log            *slog.Logger
	store          TranscriptStore
	access         AccessController
	runs           RunResolver
	terminalDialer TerminalDialer
	trustProxy     bool
	heartbeat      time.Duration
	streams        context.Context
}

// RunResolver verifies that a namespaced Run exists before transcript state is used.
type RunResolver interface {
	ResolveRun(context.Context, string, string) (types.UID, error)
}

// KubernetesRunResolver resolves Runs through the Kubernetes API.
type KubernetesRunResolver struct {
	Client client.Client
}

// ResolveRun verifies that the requested Run exists in the authorized namespace.
func (r KubernetesRunResolver) ResolveRun(ctx context.Context, namespace, name string) (types.UID, error) {
	var run platformv1alpha1.Run
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &run); err != nil {
		return "", err
	}
	return run.UID, nil
}

// ServerOptions supplies the control plane's resource and authorization dependencies.
type ServerOptions struct {
	Access          AccessController
	Runs            RunResolver
	TranscriptStore TranscriptStore
	TerminalDialer  TerminalDialer
	TrustProxy      bool
	// StreamLifecycle is canceled when long-lived SSE and terminal handlers
	// must exit during process shutdown. Ordinary requests do not use it.
	StreamLifecycle context.Context
	// TranscriptHeartbeatInterval controls SSE keepalive comments. Values less
	// than or equal to zero use the production default.
	TranscriptHeartbeatInterval time.Duration
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
	return &Server{
		log:            log,
		store:          options.TranscriptStore,
		access:         options.Access,
		runs:           options.Runs,
		terminalDialer: options.TerminalDialer,
		trustProxy:     options.TrustProxy,
		heartbeat:      heartbeat,
		streams:        streams,
	}
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
	mux.HandleFunc(namespacedPathPrefix, s.handleNamespacedAPI)
	return mux
}

func (s *Server) handleNamespacedAPI(w http.ResponseWriter, r *http.Request) {
	namespace, resource, name, subresource, ok := namespacedResource(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
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

func namespacedResource(path string) (namespace, resource, name, subresource string, ok bool) {
	remainder := strings.TrimPrefix(path, namespacedPathPrefix)
	if remainder == path {
		return "", "", "", "", false
	}
	parts := strings.Split(remainder, "/")
	if len(parts) != 4 {
		return "", "", "", "", false
	}
	for _, part := range parts {
		if part == "" {
			return "", "", "", "", false
		}
	}
	return parts[0], parts[1], parts[2], parts[3], true
}

func (s *Server) handleTranscript(w http.ResponseWriter, r *http.Request, namespace, run string) {
	verb := "get"
	allowSession := true
	if r.Method == http.MethodPost {
		verb = "update"
		allowSession = false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
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
	if s.runs == nil {
		http.Error(w, "run resolver is unavailable", http.StatusServiceUnavailable)
		return
	}
	uid, err := s.runs.ResolveRun(r.Context(), namespace, run)
	if err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, "run not found", http.StatusNotFound)
		} else {
			s.log.Warn("resolve transcript run", "namespace", namespace, "run", run, "error", err)
			http.Error(w, "run resolver is unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	if uid == "" {
		s.log.Warn("resolved transcript run without a UID", "namespace", namespace, "run", run)
		http.Error(w, "run identity is unavailable", http.StatusServiceUnavailable)
		return
	}
	identity := RunIdentity{Namespace: namespace, UID: uid}

	switch r.Method {
	case http.MethodGet:
		s.streamTranscript(w, r, identity)
	case http.MethodPost:
		s.appendTranscript(w, r, identity)
	}
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

	result, err := s.store.Append(r.Context(), run, AppendTranscriptInput{
		Source:         request.Source,
		SourceSequence: request.SourceSequence,
		IdempotencyKey: request.IdempotencyKey,
		Type:           request.Type,
		Data:           request.Data,
	})
	if err != nil {
		writeTranscriptStoreError(w, err)
		return
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

func (s *Server) streamTranscript(w http.ResponseWriter, r *http.Request, run RunIdentity) {
	if s.store == nil {
		http.Error(w, "transcript store is unavailable", http.StatusServiceUnavailable)
		return
	}
	r, cancelStream := s.withStreamLifecycle(r)
	defer cancelStream()
	cursor := r.Header.Get("Last-Event-ID")
	if cursor == "" {
		cursor = r.URL.Query().Get("after")
	}

	subscription, err := s.store.Subscribe(r.Context(), run, cursor)
	if err != nil {
		writeTranscriptStoreError(w, err)
		return
	}
	defer subscription.Unsubscribe()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	controller := http.NewResponseController(w)
	write := func(payload string) error {
		if err := controller.SetWriteDeadline(time.Now().Add(15 * time.Second)); err != nil && !errors.Is(err, http.ErrNotSupported) {
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
		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}
		return write(fmt.Sprintf("id: %s\nevent: transcript\ndata: %s\n\n", event.ID, payload))
	}
	if subscription.Gap != nil {
		payload, marshalErr := json.Marshal(subscription.Gap)
		if marshalErr != nil {
			return
		}
		if err = write(fmt.Sprintf("event: transcript-gap\ndata: %s\n\n", payload)); err != nil {
			return
		}
	}

	for _, event := range subscription.History {
		if err := writeEvent(event); err != nil {
			return
		}
	}
	subscription.History = nil
	if err := controller.Flush(); err != nil {
		return
	}
	heartbeats := time.NewTicker(s.heartbeat)
	defer heartbeats.Stop()

	for {
		select {
		case event := <-subscription.Events:
			if err := writeEvent(event); err != nil {
				return
			}
		case <-subscription.Dropped:
			s.log.Warn("closing slow transcript subscriber", "namespace", run.Namespace, "runUID", run.UID)
			return
		case <-heartbeats.C:
			if err := write(": ping\n\n"); err != nil {
				return
			}
		case <-r.Context().Done():
			return
		}
	}
}
