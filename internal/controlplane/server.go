// Package controlplane implements the user-facing HTTP API.
package controlplane

import (
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
)

const transcriptPathPrefix = "/api/v1/runs/"
const terminalPathPrefix = "/api/v1/environments/"

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
	terminalDialer TerminalDialer
}

// NewServer constructs a control-plane API handler.
func NewServer(log *slog.Logger, terminalDialer ...TerminalDialer) *Server {
	if log == nil {
		log = slog.Default()
	}
	server := &Server{log: log, store: newTranscriptStore()}
	if len(terminalDialer) > 0 {
		server.terminalDialer = terminalDialer[0]
	}
	return server
}

// Handler returns the HTTP handler for the API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET "+terminalPathPrefix, s.handleTerminal)
	mux.HandleFunc(transcriptPathPrefix, s.handleTranscript)
	return mux
}

func (s *Server) handleTranscript(w http.ResponseWriter, r *http.Request) {
	run, ok := transcriptRun(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.streamTranscript(w, r, run)
	case http.MethodPost:
		s.appendTranscript(w, r, run)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func transcriptRun(path string) (string, bool) {
	remainder := strings.TrimPrefix(path, transcriptPathPrefix)
	if remainder == path || !strings.HasSuffix(remainder, "/transcript") {
		return "", false
	}
	run := strings.TrimSuffix(remainder, "/transcript")
	return run, run != "" && !strings.Contains(run, "/")
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
