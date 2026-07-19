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
	"strconv"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

const namespacedPathPrefix = "/api/v1/namespaces/"

// TranscriptEvent is one adapter-owned transcript event.
type TranscriptEvent struct {
	ID        uint64          `json:"id"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	CreatedAt time.Time       `json:"createdAt"`
}

type appendTranscriptRequest struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// Server serves the control-plane API and live transcript streams.
type Server struct {
	log            *slog.Logger
	store          *transcriptStore
	access         AccessController
	runs           RunResolver
	terminalDialer TerminalDialer
	trustProxy     bool
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
	Access         AccessController
	Runs           RunResolver
	TerminalDialer TerminalDialer
	TrustProxy     bool
}

// NewServer constructs a control-plane API handler.
func NewServer(log *slog.Logger, options ServerOptions) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		log:            log,
		store:          newTranscriptStore(),
		access:         options.Access,
		runs:           options.Runs,
		terminalDialer: options.TerminalDialer,
		trustProxy:     options.TrustProxy,
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
	key := namespace + "/" + string(uid)

	switch r.Method {
	case http.MethodGet:
		s.streamTranscript(w, r, key)
	case http.MethodPost:
		s.appendTranscript(w, r, key)
	}
}

func (s *Server) appendTranscript(w http.ResponseWriter, r *http.Request, run string) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()

	var request appendTranscriptRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid transcript event: "+err.Error(), http.StatusBadRequest)
		return
	}
	if request.Type == "" || len(request.Data) == 0 || !json.Valid(request.Data) {
		http.Error(w, "type and valid JSON data are required", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "request body must contain one JSON object", http.StatusBadRequest)
		return
	}

	event := s.store.append(run, request.Type, request.Data)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(event)
}

func (s *Server) streamTranscript(w http.ResponseWriter, r *http.Request, run string) {
	afterID := uint64(0)
	cursor := r.URL.Query().Get("after")
	if cursor == "" {
		cursor = r.Header.Get("Last-Event-ID")
	}
	if cursor != "" {
		parsed, err := strconv.ParseUint(cursor, 10, 64)
		if err != nil {
			http.Error(w, "transcript cursor must be an event ID", http.StatusBadRequest)
			return
		}
		afterID = parsed
	}

	history, events, dropped, unsubscribe := s.store.subscribe(run, afterID)
	defer unsubscribe()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	controller := http.NewResponseController(w)
	write := func(event TranscriptEvent) error {
		if err := controller.SetWriteDeadline(time.Now().Add(15 * time.Second)); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return err
		}
		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if _, err = fmt.Fprintf(w, "id: %d\nevent: transcript\ndata: %s\n\n", event.ID, payload); err != nil {
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

	for _, event := range history {
		if err := write(event); err != nil {
			return
		}
	}
	if err := controller.Flush(); err != nil {
		return
	}

	for {
		select {
		case event := <-events:
			if err := write(event); err != nil {
				return
			}
		case <-dropped:
			s.log.Warn("closing slow transcript subscriber", "run", run)
			return
		case <-r.Context().Done():
			return
		}
	}
}

type subscriber struct {
	events  chan TranscriptEvent
	dropped chan struct{}
}

type transcriptStore struct {
	mu          sync.Mutex
	nextID      uint64
	events      map[string][]TranscriptEvent
	subscribers map[string]map[*subscriber]struct{}
}

func newTranscriptStore() *transcriptStore {
	return &transcriptStore{
		events:      make(map[string][]TranscriptEvent),
		subscribers: make(map[string]map[*subscriber]struct{}),
	}
}

func (s *transcriptStore) append(run, eventType string, data json.RawMessage) TranscriptEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextID++
	event := TranscriptEvent{
		ID:        s.nextID,
		Type:      eventType,
		Data:      append(json.RawMessage(nil), data...),
		CreatedAt: time.Now().UTC(),
	}
	s.events[run] = append(s.events[run], event)
	for subscription := range s.subscribers[run] {
		select {
		case subscription.events <- event:
		default:
			close(subscription.dropped)
			delete(s.subscribers[run], subscription)
		}
	}
	if len(s.subscribers[run]) == 0 {
		delete(s.subscribers, run)
	}
	return event
}

func (s *transcriptStore) subscribe(run string, afterID uint64) ([]TranscriptEvent, <-chan TranscriptEvent, <-chan struct{}, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	history := make([]TranscriptEvent, 0, len(s.events[run]))
	for _, event := range s.events[run] {
		if event.ID > afterID {
			history = append(history, event)
		}
	}
	subscription := &subscriber{events: make(chan TranscriptEvent, 64), dropped: make(chan struct{})}
	if s.subscribers[run] == nil {
		s.subscribers[run] = make(map[*subscriber]struct{})
	}
	s.subscribers[run][subscription] = struct{}{}

	return history, subscription.events, subscription.dropped, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.subscribers[run], subscription)
		if len(s.subscribers[run]) == 0 {
			delete(s.subscribers, run)
		}
	}
}
