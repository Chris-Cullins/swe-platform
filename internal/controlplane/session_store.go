package controlplane

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	defaultSessionAbsoluteTTL   = time.Hour
	defaultMaxSessionTokenBytes = 16 << 10
	defaultMaxActiveSessions    = 10_000
)

type sessionStoreErrorKind string

const (
	sessionStoreUnauthenticated sessionStoreErrorKind = "unauthenticated"
	sessionStoreNotFound        sessionStoreErrorKind = "not-found"
	sessionStoreExpired         sessionStoreErrorKind = "expired"
	sessionStoreCapacity        sessionStoreErrorKind = "capacity"
	sessionStoreUnavailable     sessionStoreErrorKind = "unavailable"
)

// sessionStoreError classifies failures consistently across session store
// implementations without exposing backend details to authentication handlers.
type sessionStoreError struct {
	kind sessionStoreErrorKind
	err  error
}

func (e *sessionStoreError) Error() string {
	if e.err != nil {
		return fmt.Sprintf("session store %s: %v", e.kind, e.err)
	}
	return "session store " + string(e.kind)
}

func (e *sessionStoreError) Unwrap() error { return e.err }

func (e *sessionStoreError) Is(target error) bool {
	other, ok := target.(*sessionStoreError)
	return ok && e.kind == other.kind
}

var (
	errSessionUnauthenticated = &sessionStoreError{kind: sessionStoreUnauthenticated}
	errSessionNotFound        = &sessionStoreError{kind: sessionStoreNotFound}
	errSessionExpired         = &sessionStoreError{kind: sessionStoreExpired}
	errSessionCapacity        = &sessionStoreError{kind: sessionStoreCapacity}
	errSessionUnavailable     = &sessionStoreError{kind: sessionStoreUnavailable}
)

// SessionStore retains Kubernetes bearer credentials behind opaque browser
// session identifiers. ValidateToken must perform non-I/O input validation so
// invalid credentials are rejected before TokenReview. Methods must classify
// unauthenticated, absent, expired, capacity, and backend failures with the
// typed errors above.
type SessionStore interface {
	ValidateToken(string) error
	Create(context.Context, string) (string, error)
	Resolve(context.Context, string) (string, error)
	// Delete returns nil for absent or already-deleted identifiers. A nil result
	// means revocation committed and subsequent Resolve calls cannot return the
	// credential; cancellation or backend failures must be returned.
	Delete(context.Context, string) error
}

type memorySession struct {
	token     string
	createdAt time.Time
	expiresAt time.Time
}

// MemorySessionStoreOptions bounds the process-local browser session store.
type MemorySessionStoreOptions struct {
	AbsoluteTTL       time.Duration
	MaxTokenBytes     int
	MaxActiveSessions int
	Now               func() time.Time
	Random            io.Reader
}

// MemorySessionStore keeps Kubernetes credentials server-side and exposes only
// random opaque identifiers to browsers. Session IDs are hashed before use as
// map keys so the live cookie value is not retained alongside its credential.
type MemorySessionStore struct {
	mu                sync.Mutex
	sessions          map[[sha256.Size]byte]memorySession
	absoluteTTL       time.Duration
	maxTokenBytes     int
	maxActiveSessions int
	now               func() time.Time
	random            io.Reader
}

func NewMemorySessionStore(options MemorySessionStoreOptions) *MemorySessionStore {
	if options.AbsoluteTTL <= 0 {
		options.AbsoluteTTL = defaultSessionAbsoluteTTL
	}
	if options.MaxTokenBytes <= 0 {
		options.MaxTokenBytes = defaultMaxSessionTokenBytes
	}
	if options.MaxActiveSessions <= 0 {
		options.MaxActiveSessions = defaultMaxActiveSessions
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Random == nil {
		options.Random = rand.Reader
	}
	return &MemorySessionStore{
		sessions:          make(map[[sha256.Size]byte]memorySession),
		absoluteTTL:       options.AbsoluteTTL,
		maxTokenBytes:     options.MaxTokenBytes,
		maxActiveSessions: options.MaxActiveSessions,
		now:               options.Now,
		random:            options.Random,
	}
}

func (s *MemorySessionStore) ValidateToken(token string) error {
	if len(token) == 0 || len(token) > s.maxTokenBytes {
		return errSessionUnauthenticated
	}
	return nil
}

func (s *MemorySessionStore) Create(_ context.Context, token string) (string, error) {
	if err := s.ValidateToken(token); err != nil {
		return "", err
	}
	now := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()
	for existingKey, session := range s.sessions {
		if !now.Before(session.expiresAt) {
			delete(s.sessions, existingKey)
		}
	}
	if len(s.sessions) >= s.maxActiveSessions {
		return "", errSessionCapacity
	}
	for attempt := 0; attempt < 4; attempt++ {
		var raw [32]byte
		if _, err := io.ReadFull(s.random, raw[:]); err != nil {
			return "", &sessionStoreError{kind: sessionStoreUnavailable, err: fmt.Errorf("generate session identifier: %w", err)}
		}
		id := base64.RawURLEncoding.EncodeToString(raw[:])
		key := sha256.Sum256([]byte(id))
		if _, exists := s.sessions[key]; exists {
			continue
		}
		s.sessions[key] = memorySession{token: token, createdAt: now, expiresAt: now.Add(s.absoluteTTL)}
		return id, nil
	}
	return "", &sessionStoreError{kind: sessionStoreUnavailable, err: errors.New("generate unique session identifier")}
}

func (s *MemorySessionStore) Resolve(_ context.Context, id string) (string, error) {
	if id == "" {
		return "", errSessionUnauthenticated
	}
	key := sha256.Sum256([]byte(id))
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.sessions[key]
	if !ok {
		return "", errSessionNotFound
	}
	if !now.Before(session.expiresAt) {
		delete(s.sessions, key)
		return "", errSessionExpired
	}
	return session.token, nil
}

func (s *MemorySessionStore) Delete(_ context.Context, id string) error {
	key := sha256.Sum256([]byte(id))
	s.mu.Lock()
	delete(s.sessions, key)
	s.mu.Unlock()
	return nil
}
