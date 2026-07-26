package controlplane

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	maxSessionKeyringBytes = 1 << 20
	sessionAdvisoryLock    = int64(739675093427891377)
)

var sessionKeyIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

type sessionDerivedKey struct{ lookup, encryption [32]byte }

// SessionKeyring is an immutable, derived in-memory keyring. The accepted v1
// file schema is exactly {"version":1,"activeKeyId":"<id>","keys":[{"id":
// "<id>","masterKey":"<canonical unpadded base64url of exactly 32 bytes>"}]}.
type SessionKeyring struct {
	active string
	keys   map[string]sessionDerivedKey
}

type keyringDocument struct {
	Version     int            `json:"version"`
	ActiveKeyID string         `json:"activeKeyId"`
	Keys        []keyringEntry `json:"keys"`
}
type keyringEntry struct {
	ID        string `json:"id"`
	MasterKey string `json:"masterKey"`
}

func LoadSessionKeyring(path string) (*SessionKeyring, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open session keyring: %w", err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxSessionKeyringBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read session keyring: %w", err)
	}
	if len(b) == 0 || len(b) > maxSessionKeyringBytes {
		return nil, errors.New("session keyring must be non-empty and at most 1 MiB")
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	var doc keyringDocument
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode session keyring: %w", err)
	}
	if err := ensureJSONEOF(dec); err != nil {
		return nil, err
	}
	if doc.Version != 1 {
		return nil, fmt.Errorf("unsupported session keyring version %d", doc.Version)
	}
	if !validSessionKeyID(doc.ActiveKeyID) {
		return nil, errors.New("invalid active session key ID")
	}
	kr := &SessionKeyring{active: doc.ActiveKeyID, keys: make(map[string]sessionDerivedKey, len(doc.Keys))}
	for _, entry := range doc.Keys {
		if !validSessionKeyID(entry.ID) {
			return nil, fmt.Errorf("invalid session key ID %q", entry.ID)
		}
		if _, exists := kr.keys[entry.ID]; exists {
			return nil, fmt.Errorf("duplicate session key ID %q", entry.ID)
		}
		master, err := base64.RawURLEncoding.DecodeString(entry.MasterKey)
		if err != nil || len(master) != 32 || base64.RawURLEncoding.EncodeToString(master) != entry.MasterKey {
			return nil, fmt.Errorf("session key %q masterKey must be canonical unpadded base64url encoding of 32 bytes", entry.ID)
		}
		lookup, err := hkdf.Key(sha256.New, master, nil, "swe-platform/browser-session/v1/lookup/"+entry.ID, 32)
		if err != nil {
			return nil, err
		}
		encryption, err := hkdf.Key(sha256.New, master, nil, "swe-platform/browser-session/v1/encryption/"+entry.ID, 32)
		if err != nil {
			return nil, err
		}
		var derived sessionDerivedKey
		copy(derived.lookup[:], lookup)
		copy(derived.encryption[:], encryption)
		kr.keys[entry.ID] = derived
	}
	if _, ok := kr.keys[kr.active]; !ok {
		return nil, errors.New("active session key is absent from keys")
	}
	return kr, nil
}

func ensureJSONEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("session keyring has trailing JSON value")
		}
		return fmt.Errorf("decode trailing session keyring data: %w", err)
	}
	return nil
}
func validSessionKeyID(id string) bool { return sessionKeyIDPattern.MatchString(id) }

type PostgresSessionStoreOptions struct {
	AbsoluteTTL       time.Duration
	MaxTokenBytes     int
	MaxActiveSessions int
	Now               func() time.Time
	Random            io.Reader
}

type PostgresSessionStore struct {
	db      *PostgresDatabase
	keyring *SessionKeyring
	options PostgresSessionStoreOptions
}

func NewPostgresSessionStore(ctx context.Context, db *PostgresDatabase, keyring *SessionKeyring, options PostgresSessionStoreOptions) (*PostgresSessionStore, error) {
	if db == nil || db.pool == nil {
		return nil, errors.New("PostgreSQL database is required")
	}
	if keyring == nil || len(keyring.keys) == 0 {
		return nil, errors.New("session keyring is required")
	}
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
	s := &PostgresSessionStore{db: db, keyring: keyring, options: options}
	now := postgresSessionTime(options.Now())
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("initialize session store: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `DELETE FROM browser_sessions WHERE expires_at <= $1`, now); err != nil {
		return nil, fmt.Errorf("purge expired sessions: %w", err)
	}
	rows, err := tx.Query(ctx, `SELECT DISTINCT key_id FROM browser_sessions WHERE expires_at > $1`, now)
	if err != nil {
		return nil, fmt.Errorf("validate session keys: %w", err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		if _, ok := keyring.keys[id]; !ok {
			rows.Close()
			return nil, fmt.Errorf("unexpired browser sessions require missing key %q", id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("initialize session store: %w", err)
	}
	return s, nil
}

func (s *PostgresSessionStore) ValidateToken(token string) error {
	if len(token) == 0 || len(token) > s.options.MaxTokenBytes {
		return ErrSessionUnauthenticated
	}
	return nil
}

func (s *PostgresSessionStore) Create(ctx context.Context, token string) (string, error) {
	if err := s.ValidateToken(token); err != nil {
		return "", err
	}
	for attempt := 0; attempt < 4; attempt++ {
		var raw [32]byte
		if _, err := io.ReadFull(s.options.Random, raw[:]); err != nil {
			return "", unavailable("generate session cookie", err)
		}
		cookie := "s1." + s.keyring.active + "." + base64.RawURLEncoding.EncodeToString(raw[:])
		key := s.keyring.keys[s.keyring.active]
		selector := sessionSelector(key, cookie)
		created := postgresSessionTime(s.options.Now())
		expires := postgresSessionTime(created.Add(s.options.AbsoluteTTL))
		var nonce [12]byte
		if _, err := io.ReadFull(s.options.Random, nonce[:]); err != nil {
			return "", unavailable("generate session nonce", err)
		}
		aead, err := sessionAEAD(key)
		if err != nil {
			return "", unavailable("initialize session encryption", err)
		}
		ciphertext := aead.Seal(nil, nonce[:], []byte(token), sessionAAD(selector, s.keyring.active, created, expires))
		tx, err := s.db.pool.Begin(ctx)
		if err != nil {
			return "", unavailable("begin session creation", err)
		}
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, sessionAdvisoryLock); err == nil {
			_, err = tx.Exec(ctx, `DELETE FROM browser_sessions WHERE expires_at <= $1`, created)
		}
		var count int
		if err == nil {
			err = tx.QueryRow(ctx, `SELECT count(*) FROM browser_sessions WHERE expires_at > $1`, created).Scan(&count)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return "", unavailable("prepare session creation", err)
		}
		if count >= s.options.MaxActiveSessions {
			_ = tx.Rollback(ctx)
			return "", ErrSessionCapacity
		}
		_, err = tx.Exec(ctx, `INSERT INTO browser_sessions(selector,key_id,nonce,ciphertext,created_at,expires_at) VALUES($1,$2,$3,$4,$5,$6)`, selector[:], s.keyring.active, nonce[:], ciphertext, created, expires)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				_ = tx.Rollback(ctx)
				continue
			}
			_ = tx.Rollback(ctx)
			return "", unavailable("insert session", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return "", unavailable("commit session creation", err)
		}
		return cookie, nil
	}
	return "", unavailable("generate unique session cookie", errors.New("collision limit reached"))
}

func (s *PostgresSessionStore) Resolve(ctx context.Context, cookie string) (string, error) {
	id, key, ok := s.parseCookie(cookie)
	if !ok {
		return "", ErrSessionUnauthenticated
	}
	selector := sessionSelector(key, cookie)
	var rowKey string
	var nonce, ciphertext []byte
	var created, expires time.Time
	err := s.db.pool.QueryRow(ctx, `SELECT key_id,nonce,ciphertext,created_at,expires_at FROM browser_sessions WHERE selector=$1`, selector[:]).Scan(&rowKey, &nonce, &ciphertext, &created, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrSessionNotFound
	}
	if err != nil {
		return "", unavailable("resolve session", err)
	}
	now := postgresSessionTime(s.options.Now())
	created = postgresSessionTime(created)
	expires = postgresSessionTime(expires)
	if !now.Before(expires) {
		if _, err := s.db.pool.Exec(ctx, `DELETE FROM browser_sessions WHERE selector=$1 AND expires_at <= $2`, selector[:], now); err != nil {
			return "", unavailable("purge expired session", err)
		}
		return "", ErrSessionExpired
	}
	if rowKey != id || len(nonce) != 12 || !expires.After(created) {
		return "", unavailable("validate session row", errors.New("invalid encrypted session metadata"))
	}
	aead, err := sessionAEAD(key)
	if err != nil {
		return "", unavailable("initialize session decryption", err)
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, sessionAAD(selector, id, created, expires))
	if err != nil {
		return "", unavailable("decrypt session", err)
	}
	if err := s.ValidateToken(string(plaintext)); err != nil {
		return "", unavailable("validate decrypted session", errors.New("decrypted session token violates store bounds"))
	}
	return string(plaintext), nil
}

func (s *PostgresSessionStore) Delete(ctx context.Context, cookie string) error {
	_, key, ok := s.parseCookie(cookie)
	if !ok {
		// Logout is idempotent. Malformed cookies and cookies whose key has
		// already been retired cannot identify a row this keyring can revoke.
		return nil
	}
	selector := sessionSelector(key, cookie)
	if _, err := s.db.pool.Exec(ctx, `DELETE FROM browser_sessions WHERE selector=$1`, selector[:]); err != nil {
		return unavailable("delete session", err)
	}
	return nil
}
func (s *PostgresSessionStore) parseCookie(cookie string) (string, sessionDerivedKey, bool) {
	parts := strings.Split(cookie, ".")
	if len(parts) != 3 || parts[0] != "s1" || !validSessionKeyID(parts[1]) {
		return "", sessionDerivedKey{}, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(raw) != 32 || base64.RawURLEncoding.EncodeToString(raw) != parts[2] {
		return "", sessionDerivedKey{}, false
	}
	key, ok := s.keyring.keys[parts[1]]
	return parts[1], key, ok
}
func sessionSelector(key sessionDerivedKey, cookie string) [32]byte {
	h := hmac.New(sha256.New, key.lookup[:])
	_, _ = h.Write([]byte(cookie))
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
func sessionAEAD(key sessionDerivedKey) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key.encryption[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
func postgresSessionTime(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }
func sessionAAD(selector [32]byte, keyID string, created, expires time.Time) []byte {
	b := make([]byte, 0, 64+len(keyID))
	b = append(b, []byte("swe-platform/browser-session/v1\x00")...)
	b = append(b, selector[:]...)
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(postgresSessionTime(created).UnixMicro()))
	b = append(b, n[:]...)
	binary.BigEndian.PutUint64(n[:], uint64(postgresSessionTime(expires).UnixMicro()))
	b = append(b, n[:]...)
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(keyID)))
	b = append(b, l[:]...)
	return append(b, keyID...)
}
func unavailable(operation string, err error) error {
	return &sessionStoreError{kind: sessionStoreUnavailable, err: fmt.Errorf("%s: %w", operation, err)}
}
