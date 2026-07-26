package controlplane

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type deterministicSessionReader struct {
	mu sync.Mutex
	n  byte
}

func (r *deterministicSessionReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range p {
		r.n++
		p[i] = r.n
	}
	return len(p), nil
}

func sessionTestKeyring(t *testing.T, active string, entries ...keyringEntry) *SessionKeyring {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, `{"version":1,"activeKeyId":%q,"keys":[`, active)
	for i, entry := range entries {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id":%q,"masterKey":%q}`, entry.ID, entry.MasterKey)
	}
	b.WriteString(`]}`)
	kr, err := LoadSessionKeyring(writeSessionKeyring(t, b.String()))
	if err != nil {
		t.Fatal(err)
	}
	return kr
}

func TestPostgresSessionStoreContract(t *testing.T) {
	baseURL := os.Getenv("SWE_TEST_POSTGRES_URL")
	if baseURL == "" {
		t.Skip("SWE_TEST_POSTGRES_URL is not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, baseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("session_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	u, err := url.Parse(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	databaseURL := u.String()

	oldMaster := bytes.Repeat([]byte{0x41}, 32)
	newMaster := bytes.Repeat([]byte{0x42}, 32)
	encoded := func(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
	oldOnly := sessionTestKeyring(t, "old", keyringEntry{ID: "old", MasterKey: encoded(oldMaster)})
	oldAndNew := sessionTestKeyring(t, "new", keyringEntry{ID: "old", MasterKey: encoded(oldMaster)}, keyringEntry{ID: "new", MasterKey: encoded(newMaster)})
	newOnly := sessionTestKeyring(t, "new", keyringEntry{ID: "new", MasterKey: encoded(newMaster)})
	fixed := postgresSessionTime(time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC))
	now := fixed
	options := PostgresSessionStoreOptions{AbsoluteTTL: time.Hour, MaxTokenBytes: 1024, MaxActiveSessions: 100, Now: func() time.Time { return now }, Random: &deterministicSessionReader{}}
	bootstrapDB, err := NewPostgresDatabase(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapDB.Close()
	open := func(t *testing.T, kr *SessionKeyring, opts PostgresSessionStoreOptions) (*PostgresDatabase, *PostgresSessionStore) {
		t.Helper()
		db, err := NewPostgresDatabase(ctx, databaseURL)
		if err != nil {
			t.Fatal(err)
		}
		store, err := NewPostgresSessionStore(ctx, db, kr, opts)
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		return db, store
	}
	reset := func(t *testing.T) {
		t.Helper()
		if _, err := admin.Exec(ctx, "TRUNCATE "+schema+".browser_sessions"); err != nil {
			t.Fatal(err)
		}
		now = fixed
	}

	t.Run("restart continuity and database secrecy", func(t *testing.T) {
		reset(t)
		db, store := open(t, oldOnly, options)
		cookie, err := store.Create(ctx, "bearer-secret")
		if err != nil {
			t.Fatal(err)
		}
		parts := strings.Split(cookie, ".")
		if len(parts) != 3 || parts[0] != "s1" || parts[1] != "old" || len(parts[2]) != 43 || strings.Contains(cookie, "=") {
			t.Fatalf("cookie format = %q", cookie)
		}
		var selector, nonce, ciphertext []byte
		var keyID string
		if err := db.pool.QueryRow(ctx, `SELECT selector,key_id,nonce,ciphertext FROM browser_sessions`).Scan(&selector, &keyID, &nonce, &ciphertext); err != nil {
			t.Fatal(err)
		}
		row := bytes.Join([][]byte{selector, []byte(keyID), nonce, ciphertext}, nil)
		for name, secret := range map[string][]byte{"cookie": []byte(cookie), "bearer": []byte("bearer-secret"), "master": oldMaster} {
			if bytes.Contains(row, secret) {
				t.Errorf("raw row contains %s", name)
			}
		}
		if len(selector) != 32 {
			t.Fatalf("selector length = %d", len(selector))
		}
		for _, fake := range []string{base64.RawURLEncoding.EncodeToString(selector), string(selector)} {
			if _, err := store.Resolve(ctx, fake); !errors.Is(err, ErrSessionUnauthenticated) {
				t.Fatalf("selector cookie resolve = %v", err)
			}
		}
		db.Close()
		db, store = open(t, oldOnly, options)
		defer db.Close()
		if got, err := store.Resolve(ctx, cookie); err != nil || got != "bearer-secret" {
			t.Fatalf("resolve after restart = %q, %v", got, err)
		}
	})

	t.Run("authenticated metadata and ciphertext detect tampering", func(t *testing.T) {
		reset(t)
		db, store := open(t, oldOnly, options)
		defer db.Close()
		cookie, _ := store.Create(ctx, "token-one")
		other, _ := store.Create(ctx, "token-two")
		_, key, _ := store.parseCookie(cookie)
		selector := sessionSelector(key, cookie)
		otherSelector := sessionSelector(key, other)
		var originalKey string
		var originalNonce, originalCiphertext []byte
		var originalCreated, originalExpires time.Time
		if err := db.pool.QueryRow(ctx, `SELECT key_id,nonce,ciphertext,created_at,expires_at FROM browser_sessions WHERE selector=$1`, selector[:]).Scan(&originalKey, &originalNonce, &originalCiphertext, &originalCreated, &originalExpires); err != nil {
			t.Fatal(err)
		}
		restore := func() {
			t.Helper()
			if _, err := db.pool.Exec(ctx, `UPDATE browser_sessions SET key_id=$2,nonce=$3,ciphertext=$4,created_at=$5,expires_at=$6 WHERE selector=$1`, selector[:], originalKey, originalNonce, originalCiphertext, originalCreated, originalExpires); err != nil {
				t.Fatal(err)
			}
		}
		mutations := []string{
			`ciphertext = set_byte(ciphertext,0,get_byte(ciphertext,0)#1)`,
			`nonce = set_byte(nonce,0,get_byte(nonce,0)#1)`,
			`key_id = 'wrong'`,
			`created_at = created_at + interval '1 microsecond'`,
			`expires_at = expires_at + interval '1 microsecond'`,
		}
		for _, mutation := range mutations {
			if _, err := db.pool.Exec(ctx, `UPDATE browser_sessions SET `+mutation+` WHERE selector=$1`, selector[:]); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Resolve(ctx, cookie); !errors.Is(err, ErrSessionUnavailable) {
				t.Fatalf("mutation %q error = %v", mutation, err)
			}
			restore()
		}
		// Put another session's encrypted fields at this cookie's selector.
		_, err := db.pool.Exec(ctx, `UPDATE browser_sessions dst SET (key_id,nonce,ciphertext,created_at,expires_at) = (SELECT key_id,nonce,ciphertext,created_at,expires_at FROM browser_sessions WHERE selector=$2) WHERE selector=$1`, selector[:], otherSelector[:])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Resolve(ctx, cookie); !errors.Is(err, ErrSessionUnavailable) {
			t.Fatalf("moved encrypted fields error = %v", err)
		}
		restore()

		// A validly encrypted but impossible token length is corrupt backend
		// state, not an unauthenticated browser credential.
		aead, err := sessionAEAD(key)
		if err != nil {
			t.Fatal(err)
		}
		oversized := aead.Seal(nil, originalNonce, bytes.Repeat([]byte{'x'}, options.MaxTokenBytes+1), sessionAAD(selector, originalKey, originalCreated, originalExpires))
		if _, err := db.pool.Exec(ctx, `UPDATE browser_sessions SET ciphertext=$2 WHERE selector=$1`, selector[:], oversized); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Resolve(ctx, cookie); !errors.Is(err, ErrSessionUnavailable) || errors.Is(err, ErrSessionUnauthenticated) {
			t.Fatalf("oversized decrypted token classification = %v", err)
		}
		restore()
	})

	t.Run("rotation missing key and deterministic purge", func(t *testing.T) {
		reset(t)
		db, oldStore := open(t, oldOnly, options)
		oldCookie, _ := oldStore.Create(ctx, "old-token")
		db.Close()
		db, rotated := open(t, oldAndNew, options)
		if got, err := rotated.Resolve(ctx, oldCookie); err != nil || got != "old-token" {
			t.Fatalf("old resolve = %q, %v", got, err)
		}
		newCookie, err := rotated.Create(ctx, "new-token")
		if err != nil {
			t.Fatal(err)
		}
		if got, err := rotated.Resolve(ctx, newCookie); err != nil || got != "new-token" {
			t.Fatalf("new resolve = %q, %v", got, err)
		}
		db.Close()
		db, err = NewPostgresDatabase(ctx, databaseURL)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := NewPostgresSessionStore(ctx, db, newOnly, options); err == nil {
			db.Close()
			t.Fatal("startup accepted unexpired row with missing key")
		}
		db.Close()
		now = fixed.Add(2 * time.Hour)
		db, store := open(t, newOnly, options)
		defer db.Close()
		var count int
		_ = db.pool.QueryRow(ctx, `SELECT count(*) FROM browser_sessions`).Scan(&count)
		if count != 0 {
			t.Fatalf("expired rows after startup purge = %d", count)
		}
		_ = store
	})

	t.Run("concurrent capacity has no eviction", func(t *testing.T) {
		reset(t)
		limited := options
		limited.MaxActiveSessions = 4
		limited.Random = &deterministicSessionReader{}
		db, store := open(t, oldOnly, limited)
		defer db.Close()
		const attempts = 24
		errs := make(chan error, attempts)
		var wg sync.WaitGroup
		for i := 0; i < attempts; i++ {
			wg.Add(1)
			go func(i int) { defer wg.Done(); _, err := store.Create(ctx, fmt.Sprintf("token-%d", i)); errs <- err }(i)
		}
		wg.Wait()
		close(errs)
		success, capacity := 0, 0
		for err := range errs {
			switch {
			case err == nil:
				success++
			case errors.Is(err, ErrSessionCapacity):
				capacity++
			default:
				t.Fatalf("create error = %v", err)
			}
		}
		if success != 4 || capacity != attempts-4 {
			t.Fatalf("success/capacity = %d/%d", success, capacity)
		}
	})

	t.Run("expiry delete idempotency and restart absence", func(t *testing.T) {
		reset(t)
		db, store := open(t, oldOnly, options)
		expired, _ := store.Create(ctx, "expires")
		now = fixed.Add(time.Hour)
		if _, err := store.Resolve(ctx, expired); !errors.Is(err, ErrSessionExpired) {
			t.Fatalf("expiry error = %v", err)
		}
		cookie, _ := store.Create(ctx, "delete")
		if err := store.Delete(ctx, cookie); err != nil {
			t.Fatal(err)
		}
		if err := store.Delete(ctx, cookie); err != nil {
			t.Fatal(err)
		}
		if err := store.Delete(ctx, "malformed"); err != nil {
			t.Fatalf("malformed delete = %v", err)
		}
		if err := (&PostgresSessionStore{keyring: newOnly}).Delete(ctx, cookie); err != nil {
			t.Fatalf("retired-key delete = %v", err)
		}
		db.Close()
		db, store = open(t, oldOnly, options)
		defer db.Close()
		if _, err := store.Resolve(ctx, cookie); !errors.Is(err, ErrSessionNotFound) {
			t.Fatalf("deleted resolve after restart = %v", err)
		}
	})

	t.Run("closed database never falls back", func(t *testing.T) {
		reset(t)
		db, store := open(t, oldOnly, options)
		cookie, _ := store.Create(ctx, "durable")
		db.Close()
		if _, err := store.Resolve(ctx, cookie); !errors.Is(err, ErrSessionUnavailable) {
			t.Fatalf("resolve outage = %v", err)
		}
		if _, err := store.Create(ctx, "new"); !errors.Is(err, ErrSessionUnavailable) {
			t.Fatalf("create outage = %v", err)
		}
		if err := store.Delete(ctx, cookie); !errors.Is(err, ErrSessionUnavailable) {
			t.Fatalf("delete outage = %v", err)
		}
		wrapped := fmt.Errorf("outer: %w", unavailable("test", io.ErrClosedPipe))
		if !errors.Is(wrapped, ErrSessionUnavailable) {
			t.Fatalf("wrapped unavailable classification lost: %v", wrapped)
		}
	})
}
