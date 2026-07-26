package controlplane

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeSessionKeyring(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "keyring.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func validKeyringJSON(id string, key []byte) string {
	return "{\"version\":1,\"activeKeyId\":\"" + id + "\",\"keys\":[{\"id\":\"" + id + "\",\"masterKey\":\"" + base64.RawURLEncoding.EncodeToString(key) + "\"}]}"
}

func TestLoadSessionKeyringValidation(t *testing.T) {
	master := bytes.Repeat([]byte{7}, 32)
	valid := validKeyringJSON("key_1", master)
	kr, err := LoadSessionKeyring(writeSessionKeyring(t, valid))
	if err != nil {
		t.Fatal(err)
	}
	if kr.active != "key_1" || kr.keys["key_1"].lookup == kr.keys["key_1"].encryption {
		t.Fatal("keyring did not derive independent active keys")
	}
	tests := []string{
		``, `{"version":2,"activeKeyId":"x","keys":[]}`,
		`{"version":1,"activeKeyId":"bad.id","keys":[]}`,
		`{"version":1,"activeKeyId":"missing","keys":[{"id":"x","masterKey":"` + base64.RawURLEncoding.EncodeToString(master) + `"}]}`,
		`{"version":1,"activeKeyId":"x","keys":[{"id":"x","masterKey":"AA=="}]}`,
		valid + ` {}`, stringsReplaceOnce(valid, `"version":1`, `"version":1,"unknown":true`),
		`{"version":1,"activeKeyId":"x","keys":[{"id":"x","masterKey":"` + base64.RawURLEncoding.EncodeToString(master) + `"},{"id":"x","masterKey":"` + base64.RawURLEncoding.EncodeToString(master) + `"}]}`,
	}
	for i, body := range tests {
		if _, err := LoadSessionKeyring(writeSessionKeyring(t, body)); err == nil {
			t.Errorf("case %d accepted invalid keyring", i)
		}
	}
}

func stringsReplaceOnce(s, old, new string) string {
	i := bytes.Index([]byte(s), []byte(old))
	if i < 0 {
		return s
	}
	return s[:i] + new + s[i+len(old):]
}

func TestPostgresSessionCookieParsing(t *testing.T) {
	master := bytes.Repeat([]byte{3}, 32)
	kr, err := LoadSessionKeyring(writeSessionKeyring(t, validKeyringJSON("k", master)))
	if err != nil {
		t.Fatal(err)
	}
	s := &PostgresSessionStore{keyring: kr}
	raw := bytes.Repeat([]byte{1}, 32)
	good := "s1.k." + base64.RawURLEncoding.EncodeToString(raw)
	if id, _, ok := s.parseCookie(good); !ok || id != "k" {
		t.Fatal("valid cookie rejected")
	}
	for _, bad := range []string{"", good + "=", "s2.k." + base64.RawURLEncoding.EncodeToString(raw), "s1.missing." + base64.RawURLEncoding.EncodeToString(raw), "s1.k.AA"} {
		if _, _, ok := s.parseCookie(bad); ok {
			t.Errorf("accepted %q", bad)
		}
		if err := s.Delete(t.Context(), bad); err != nil {
			t.Errorf("idempotent Delete(%q) = %v", bad, err)
		}
	}
}

func TestSessionAADStableUTCPostgresPrecision(t *testing.T) {
	var selector [32]byte
	for i := range selector {
		selector[i] = byte(i)
	}
	created := time.Date(2026, 7, 26, 1, 2, 3, 456789999, time.FixedZone("offset", 3600))
	expires := created.Add(time.Hour)
	got := sessionAAD(selector, "key-1", created, expires)
	want, err := hex.DecodeString("7377652d706c6174666f726d2f62726f777365722d73657373696f6e2f763100000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0006577850cb4d1500065779275ef11500056b65792d31")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("AAD = %x, want %x", got, want)
	}
	if !bytes.Equal(got, sessionAAD(selector, "key-1", created.Truncate(time.Microsecond), expires.Truncate(time.Microsecond))) {
		t.Fatal("AAD changed below PostgreSQL microsecond precision")
	}
	if errors.Is(ErrSessionUnavailable, ErrSessionNotFound) {
		t.Fatal("sentinel categories overlap")
	}
}
