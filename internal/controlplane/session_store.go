package controlplane

import (
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

var errSessionCapacity = errors.New("session capacity reached")

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

func (s *MemorySessionStore) Create(token string) (string, error) {
	if len(token) == 0 || len(token) > s.maxTokenBytes {
		return "", errUnauthenticated
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
			return "", fmt.Errorf("generate session identifier: %w", err)
		}
		id := base64.RawURLEncoding.EncodeToString(raw[:])
		key := sha256.Sum256([]byte(id))
		if _, exists := s.sessions[key]; exists {
			continue
		}
		s.sessions[key] = memorySession{token: token, createdAt: now, expiresAt: now.Add(s.absoluteTTL)}
		return id, nil
	}
	return "", fmt.Errorf("generate unique session identifier")
}

func (s *MemorySessionStore) Resolve(id string) (string, error) {
	if id == "" {
		return "", errUnauthenticated
	}
	key := sha256.Sum256([]byte(id))
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.sessions[key]
	if !ok {
		return "", errUnauthenticated
	}
	if !now.Before(session.expiresAt) {
		delete(s.sessions, key)
		return "", errUnauthenticated
	}
	return session.token, nil
}

func (s *MemorySessionStore) Delete(id string) {
	key := sha256.Sum256([]byte(id))
	s.mu.Lock()
	delete(s.sessions, key)
	s.mu.Unlock()
}

func (s *MemorySessionStore) acceptsToken(token string) bool {
	return len(token) > 0 && len(token) <= s.maxTokenBytes
}
