package controlplane

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

var (
	ErrIdempotencyConflict = errors.New("transcript idempotency key was reused with different content")
	ErrTranscriptCapacity  = errors.New("transcript store capacity exceeded")
	ErrSubscriberCapacity  = errors.New("transcript subscriber capacity exceeded")
	ErrReplayLimit         = errors.New("transcript replay limit exceeded")
	ErrInvalidCursor       = errors.New("invalid transcript cursor")
	ErrExpiredCursor       = errors.New("transcript cursor expired")
)

// RunIdentity is the immutable tenant-aware identity of a transcript owner.
type RunIdentity struct {
	Namespace string
	UID       types.UID
}

// AppendTranscriptInput contains platform transport metadata and opaque adapter data.
type AppendTranscriptInput struct {
	Source         string
	SourceSequence *uint64
	IdempotencyKey string
	Type           string
	Data           json.RawMessage
}

// TranscriptEvent is one durably ordered adapter-owned transcript event.
type TranscriptEvent struct {
	ID             string          `json:"id"`
	Sequence       uint64          `json:"sequence"`
	Source         string          `json:"source"`
	SourceSequence *uint64         `json:"sourceSequence,omitempty"`
	Type           string          `json:"type"`
	Data           json.RawMessage `json:"data"`
	CreatedAt      time.Time       `json:"createdAt"`
}

// AppendTranscriptResult reports whether an append committed or replayed a prior result.
type AppendTranscriptResult struct {
	Event    TranscriptEvent
	Replayed bool
}

// TranscriptGap describes an explicitly skipped portion of retained history.
type TranscriptGap struct {
	ResumeAfter      string `json:"resumeAfter"`
	EarliestSequence uint64 `json:"earliestSequence,omitempty"`
	LatestSequence   uint64 `json:"latestSequence,omitempty"`
}

// TranscriptSubscription atomically joins retained replay to live publication.
type TranscriptSubscription struct {
	History     []TranscriptEvent
	Events      <-chan TranscriptEvent
	Dropped     <-chan struct{}
	Gap         *TranscriptGap
	Unsubscribe func()
}

// TranscriptStore defines the durability and fan-out boundary. Append must be
// linearizable per Run and atomically allocate sequence, persist idempotency state,
// and publish. Subscribe must establish one atomic replay/live cut so every later
// committed event appears exactly once unless Dropped is closed. Callers must treat
// returned events and their opaque Data as immutable.
type TranscriptStore interface {
	Append(context.Context, RunIdentity, AppendTranscriptInput) (AppendTranscriptResult, error)
	Subscribe(context.Context, RunIdentity, string) (TranscriptSubscription, error)
}

// MemoryTranscriptStoreOptions bounds every variable-size part of the reference store.
type MemoryTranscriptStoreOptions struct {
	MaxRuns              int
	MaxEventsPerRun      int
	MaxBytesPerRun       int
	MaxTotalEvents       int
	MaxTotalBytes        int
	MaxSubscribersPerRun int
	MaxSubscribers       int
	SubscriberBuffer     int
	MaxReplayEvents      int
}

// DefaultMemoryTranscriptStoreOptions returns conservative scaffold limits.
func DefaultMemoryTranscriptStoreOptions() MemoryTranscriptStoreOptions {
	return MemoryTranscriptStoreOptions{
		MaxRuns:              1024,
		MaxEventsPerRun:      1024,
		MaxBytesPerRun:       16 << 20,
		MaxTotalEvents:       16 * 1024,
		MaxTotalBytes:        128 << 20,
		MaxSubscribersPerRun: 64,
		MaxSubscribers:       1024,
		SubscriberBuffer:     64,
		MaxReplayEvents:      1024,
	}
}

type memoryTranscriptStore struct {
	mu               sync.Mutex
	options          MemoryTranscriptStoreOptions
	generation       string
	cursorKey        []byte
	runs             map[RunIdentity]*memoryRunTranscript
	subscribers      map[RunIdentity]map[*memorySubscriber]struct{}
	totalEvents      int
	totalBytes       int
	totalSubscribers int
}

type memoryRunTranscript struct {
	highWater   uint64
	events      []memoryEvent
	bytes       int
	idempotency map[memoryIdempotencyKey]memoryEvent
}

type memoryEvent struct {
	event          TranscriptEvent
	idempotencyKey memoryIdempotencyKey
	size           int
}

type memorySubscriber struct {
	events    chan TranscriptEvent
	dropped   chan struct{}
	isDropped bool
}

type memoryIdempotencyKey struct {
	source string
	key    string
}

// NewMemoryTranscriptStore constructs a bounded, process-local reference store.
func NewMemoryTranscriptStore(options MemoryTranscriptStoreOptions) TranscriptStore {
	defaults := DefaultMemoryTranscriptStoreOptions()
	if options.MaxRuns <= 0 {
		options.MaxRuns = defaults.MaxRuns
	}
	if options.MaxEventsPerRun <= 0 {
		options.MaxEventsPerRun = defaults.MaxEventsPerRun
	}
	if options.MaxBytesPerRun <= 0 {
		options.MaxBytesPerRun = defaults.MaxBytesPerRun
	}
	if options.MaxTotalEvents <= 0 {
		options.MaxTotalEvents = defaults.MaxTotalEvents
	}
	if options.MaxTotalBytes <= 0 {
		options.MaxTotalBytes = defaults.MaxTotalBytes
	}
	if options.MaxSubscribersPerRun <= 0 {
		options.MaxSubscribersPerRun = defaults.MaxSubscribersPerRun
	}
	if options.MaxSubscribers <= 0 {
		options.MaxSubscribers = defaults.MaxSubscribers
	}
	if options.SubscriberBuffer <= 0 {
		options.SubscriberBuffer = defaults.SubscriberBuffer
	}
	if options.MaxReplayEvents <= 0 {
		options.MaxReplayEvents = defaults.MaxReplayEvents
	}

	generationBytes := make([]byte, 16)
	cursorKey := make([]byte, 32)
	if _, err := rand.Read(generationBytes); err != nil {
		panic(fmt.Sprintf("generate transcript store identity: %v", err))
	}
	if _, err := rand.Read(cursorKey); err != nil {
		panic(fmt.Sprintf("generate transcript cursor key: %v", err))
	}
	return &memoryTranscriptStore{
		options:     options,
		generation:  base64.RawURLEncoding.EncodeToString(generationBytes),
		cursorKey:   cursorKey,
		runs:        make(map[RunIdentity]*memoryRunTranscript),
		subscribers: make(map[RunIdentity]map[*memorySubscriber]struct{}),
	}
}

func (s *memoryTranscriptStore) Append(_ context.Context, run RunIdentity, input AppendTranscriptInput) (AppendTranscriptResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.runs[run]
	key := memoryIdempotencyKey{source: input.Source, key: input.IdempotencyKey}
	if state != nil {
		if prior, ok := state.idempotency[key]; ok {
			if equalAppendInput(prior.event, input) {
				return AppendTranscriptResult{Event: cloneTranscriptEvent(prior.event), Replayed: true}, nil
			}
			return AppendTranscriptResult{}, ErrIdempotencyConflict
		}
	}

	size := transcriptEventSize(input)
	if size > s.options.MaxBytesPerRun || size > s.options.MaxTotalBytes {
		return AppendTranscriptResult{}, ErrTranscriptCapacity
	}
	evict := 0
	freedBytes := 0
	if state != nil {
		for len(state.events)-evict+1 > s.options.MaxEventsPerRun || state.bytes-freedBytes+size > s.options.MaxBytesPerRun {
			freedBytes += state.events[evict].size
			evict++
		}
	}
	if s.totalEvents-evict+1 > s.options.MaxTotalEvents || s.totalBytes-freedBytes+size > s.options.MaxTotalBytes {
		return AppendTranscriptResult{}, ErrTranscriptCapacity
	}
	if state == nil {
		if len(s.runs) >= s.options.MaxRuns {
			return AppendTranscriptResult{}, ErrTranscriptCapacity
		}
		state = &memoryRunTranscript{idempotency: make(map[memoryIdempotencyKey]memoryEvent)}
		s.runs[run] = state
	}
	for i := 0; i < evict; i++ {
		old := state.events[i]
		delete(state.idempotency, old.idempotencyKey)
		state.bytes -= old.size
		s.totalBytes -= old.size
		s.totalEvents--
	}
	state.events = state.events[evict:]

	state.highWater++
	event := TranscriptEvent{
		ID: s.cursor(run, state.highWater), Sequence: state.highWater,
		Source: input.Source, SourceSequence: cloneUint64(input.SourceSequence),
		Type: input.Type, Data: append(json.RawMessage(nil), input.Data...), CreatedAt: time.Now().UTC(),
	}
	stored := memoryEvent{event: event, idempotencyKey: key, size: size}
	state.events = append(state.events, stored)
	state.idempotency[key] = stored
	state.bytes += size
	s.totalBytes += size
	s.totalEvents++
	for subscriber := range s.subscribers[run] {
		if subscriber.isDropped {
			continue
		}
		select {
		case subscriber.events <- event:
		default:
			close(subscriber.dropped)
			subscriber.isDropped = true
		}
	}
	return AppendTranscriptResult{Event: cloneTranscriptEvent(event)}, nil
}

func (s *memoryTranscriptStore) Subscribe(_ context.Context, run RunIdentity, cursor string) (TranscriptSubscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.runs[run]
	highWater := uint64(0)
	if state != nil {
		highWater = state.highWater
	}
	sequence := uint64(0)
	if cursor != "" {
		parsed, err := s.parseCursor(run, cursor)
		if err != nil {
			return TranscriptSubscription{}, err
		}
		sequence = parsed
		if sequence > highWater {
			return TranscriptSubscription{}, ErrInvalidCursor
		}
	}

	earliest := uint64(1)
	if state != nil && len(state.events) > 0 {
		earliest = state.events[0].event.Sequence
	}
	if cursor != "" && sequence+1 < earliest {
		return TranscriptSubscription{}, &TranscriptCursorError{Err: ErrExpiredCursor, Gap: s.gap(run, earliest, highWater)}
	}
	history := make([]TranscriptEvent, 0)
	if state != nil {
		for _, stored := range state.events {
			if stored.event.Sequence > sequence {
				history = append(history, stored.event)
			}
		}
	}
	if len(history) > s.options.MaxReplayEvents {
		resumeSequence := highWater - uint64(s.options.MaxReplayEvents)
		return TranscriptSubscription{}, &TranscriptCursorError{Err: ErrReplayLimit, Gap: &TranscriptGap{
			ResumeAfter: s.cursor(run, resumeSequence), EarliestSequence: earliest, LatestSequence: highWater,
		}}
	}
	if len(s.subscribers[run]) >= s.options.MaxSubscribersPerRun || s.totalSubscribers >= s.options.MaxSubscribers {
		return TranscriptSubscription{}, ErrSubscriberCapacity
	}
	subscriber := &memorySubscriber{events: make(chan TranscriptEvent, s.options.SubscriberBuffer), dropped: make(chan struct{})}
	if s.subscribers[run] == nil {
		s.subscribers[run] = make(map[*memorySubscriber]struct{})
	}
	s.subscribers[run][subscriber] = struct{}{}
	s.totalSubscribers++

	var gap *TranscriptGap
	if cursor == "" && earliest > 1 {
		gap = s.gap(run, earliest, highWater)
	}
	var once sync.Once
	return TranscriptSubscription{
		History: history, Events: subscriber.events, Dropped: subscriber.dropped, Gap: gap,
		Unsubscribe: func() { once.Do(func() { s.unsubscribe(run, subscriber) }) },
	}, nil
}

func (s *memoryTranscriptStore) unsubscribe(run RunIdentity, subscriber *memorySubscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscribers[run][subscriber]; !ok {
		return
	}
	delete(s.subscribers[run], subscriber)
	s.totalSubscribers--
	if len(s.subscribers[run]) == 0 {
		delete(s.subscribers, run)
	}
}

func (s *memoryTranscriptStore) cursor(run RunIdentity, sequence uint64) string {
	runHash := sha256.Sum256([]byte(run.Namespace + "\x00" + string(run.UID)))
	payload := "v1." + s.generation + "." + base64.RawURLEncoding.EncodeToString(runHash[:16]) + "." + strconv.FormatUint(sequence, 10)
	signature := hmac.New(sha256.New, s.cursorKey)
	_, _ = signature.Write([]byte(payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(signature.Sum(nil)[:16])
}

func (s *memoryTranscriptStore) parseCursor(run RunIdentity, cursor string) (uint64, error) {
	parts := strings.Split(cursor, ".")
	if len(parts) != 5 || parts[0] != "v1" {
		return 0, ErrInvalidCursor
	}
	if parts[1] != s.generation {
		return 0, ErrInvalidCursor
	}
	sequence, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return 0, ErrInvalidCursor
	}
	want := s.cursor(run, sequence)
	if !hmac.Equal([]byte(want), []byte(cursor)) {
		return 0, ErrInvalidCursor
	}
	return sequence, nil
}

func (s *memoryTranscriptStore) gap(run RunIdentity, earliest, latest uint64) *TranscriptGap {
	resume := uint64(0)
	if earliest > 0 {
		resume = earliest - 1
	}
	return &TranscriptGap{ResumeAfter: s.cursor(run, resume), EarliestSequence: earliest, LatestSequence: latest}
}

// TranscriptCursorError adds a store-issued recovery boundary to a cursor failure.
type TranscriptCursorError struct {
	Err error
	Gap *TranscriptGap
}

func (e *TranscriptCursorError) Error() string { return e.Err.Error() }
func (e *TranscriptCursorError) Unwrap() error { return e.Err }

func writeTranscriptStoreError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := "transcript_store_error"
	switch {
	case errors.Is(err, ErrIdempotencyConflict):
		status, code = http.StatusConflict, "idempotency_conflict"
	case errors.Is(err, ErrInvalidCursor):
		status, code = http.StatusBadRequest, "invalid_cursor"
	case errors.Is(err, ErrExpiredCursor):
		status, code = http.StatusGone, "cursor_expired"
	case errors.Is(err, ErrReplayLimit):
		status, code = http.StatusConflict, "replay_limit_exceeded"
	case errors.Is(err, ErrSubscriberCapacity):
		status, code = http.StatusServiceUnavailable, "subscriber_capacity"
	case errors.Is(err, ErrTranscriptCapacity):
		status, code = http.StatusInsufficientStorage, "transcript_capacity"
	}
	var cursorError *TranscriptCursorError
	problem := struct {
		Type        string         `json:"type"`
		Title       string         `json:"title"`
		Status      int            `json:"status"`
		ResumeAfter string         `json:"resumeAfter,omitempty"`
		Available   *TranscriptGap `json:"available,omitempty"`
	}{Type: "https://swe-platform.dev/problems/" + code, Title: err.Error(), Status: status}
	if errors.As(err, &cursorError) && cursorError.Gap != nil {
		problem.ResumeAfter = cursorError.Gap.ResumeAfter
		problem.Available = cursorError.Gap
	}
	w.Header().Set("Content-Type", "application/problem+json")
	if errors.Is(err, ErrSubscriberCapacity) {
		w.Header().Set("Retry-After", "1")
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem)
}

func writeTranscriptProblem(w http.ResponseWriter, status int, code, title string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Status int    `json:"status"`
	}{Type: "https://swe-platform.dev/problems/" + code, Title: title, Status: status})
}

func equalAppendInput(event TranscriptEvent, input AppendTranscriptInput) bool {
	return event.Source == input.Source && event.Type == input.Type && equalUint64(event.SourceSequence, input.SourceSequence) && bytes.Equal(event.Data, input.Data)
}

func equalUint64(a, b *uint64) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}
func cloneUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
func cloneTranscriptEvent(event TranscriptEvent) TranscriptEvent {
	event.Data = append(json.RawMessage(nil), event.Data...)
	event.SourceSequence = cloneUint64(event.SourceSequence)
	return event
}
func transcriptEventSize(input AppendTranscriptInput) int {
	return len(input.Source) + len(input.IdempotencyKey) + len(input.Type) + len(input.Data) + 256
}

func newLegacyTranscriptKey() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		panic(fmt.Sprintf("generate legacy transcript key: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(value)
}
