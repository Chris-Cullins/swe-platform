package controlplane

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeSessions struct {
	create  func(*http.Request) (Session, string, error)
	current func(*http.Request) (Session, error)
	deleted bool
}

func (f *fakeSessions) CreateSession(r *http.Request) (Session, string, error) {
	return f.create(r)
}
func (f *fakeSessions) CurrentSession(r *http.Request) (Session, error) { return f.current(r) }
func (f *fakeSessions) DeleteSession(*http.Request)                     { f.deleted = true }

func TestSessionCreateSecurityAndExplicitBearer(t *testing.T) {
	for _, tc := range []struct {
		name, target, auth string
		insecure           bool
		want               int
		secure             bool
	}{
		{"secure rejects HTTP", "http://example.test/api/v1/session", "Bearer good", false, 400, false},
		{"secure HTTPS", "https://example.test/api/v1/session", "Bearer good", false, 200, true},
		{"insecure HTTP", "http://example.test/api/v1/session", "Bearer good", true, 200, false},
		{"cookie is not explicit bearer", "https://example.test/api/v1/session", "", false, 401, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sessions := &fakeSessions{create: func(r *http.Request) (Session, string, error) {
				if r.Header.Get("Authorization") != "Bearer good" {
					return Session{}, "", errUnauthenticated
				}
				return Session{Authenticated: true, Username: "alice"}, "credential", nil
			}}
			s := NewServer(nil, ServerOptions{Sessions: sessions, AllowInsecureSessions: tc.insecure})
			r := httptest.NewRequest(http.MethodPost, tc.target, nil)
			r.Header.Set("Authorization", tc.auth)
			if tc.auth == "" {
				r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "good"})
			}
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if tc.want == 200 {
				c := w.Result().Cookies()[0]
				if !c.HttpOnly || c.Secure != tc.secure || c.SameSite != http.SameSiteStrictMode || c.Path != "/" {
					t.Fatalf("cookie=%+v", c)
				}
				if strings.Contains(w.Body.String(), "credential") {
					t.Fatalf("response leaked credential: %s", w.Body.String())
				}
			}
		})
	}
}

func TestSessionHTTPSPolicyWithTrustedProxy(t *testing.T) {
	sessions := &fakeSessions{create: func(*http.Request) (Session, string, error) {
		return Session{Authenticated: true, Username: "alice"}, "credential", nil
	}}
	for _, tc := range []struct {
		host, proto string
		want        int
	}{{"console.test", "https", 200}, {"", "https", 400}, {"console.test, attacker.test", "https", 400}, {"console.test", "ftp", 400}} {
		r := httptest.NewRequest(http.MethodPost, "http://control-plane/api/v1/session", nil)
		r.Header.Set("Authorization", "Bearer good")
		r.Header.Set("X-Forwarded-Host", tc.host)
		r.Header.Set("X-Forwarded-Proto", tc.proto)
		w := httptest.NewRecorder()
		NewServer(nil, ServerOptions{Sessions: sessions, TrustProxy: true}).Handler().ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Errorf("forwarded %q/%q status=%d body=%s", tc.host, tc.proto, w.Code, w.Body.String())
		}
	}
	for _, header := range []string{"X-Forwarded-Host", "X-Forwarded-Proto"} {
		r := httptest.NewRequest(http.MethodPost, "http://control-plane/api/v1/session", nil)
		r.Header.Set("Authorization", "Bearer good")
		r.Header.Set("X-Forwarded-Host", "console.test")
		r.Header.Set("X-Forwarded-Proto", "https")
		r.Header.Add(header, "appended.test")
		w := httptest.NewRecorder()
		NewServer(nil, ServerOptions{Sessions: sessions, TrustProxy: true}).Handler().ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Errorf("duplicate %s status=%d body=%s", header, w.Code, w.Body.String())
		}
	}
}

func TestSessionErrorsAndCurrentValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
	}{{"authentication", errUnauthenticated, 401, "authentication-required"}, {"bootstrap forbidden", errForbidden, 403, "access-denied"}, {"review failure", errors.New("down"), 503, "authorization-unavailable"}} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeSessions{create: func(*http.Request) (Session, string, error) { return Session{}, "", tc.err }}
			r := httptest.NewRequest(http.MethodPost, "https://example.test/api/v1/session", nil)
			r.Header.Set("Authorization", "Bearer x")
			w := httptest.NewRecorder()
			NewServer(nil, ServerOptions{Sessions: f}).Handler().ServeHTTP(w, r)
			if w.Code != tc.status || w.Header().Get("Content-Type") != "application/problem+json" || !strings.Contains(w.Body.String(), tc.code) {
				t.Fatalf("response=%d %s", w.Code, w.Body.String())
			}
		})
	}
	called := false
	f := &fakeSessions{current: func(r *http.Request) (Session, error) {
		called = true
		c, err := r.Cookie(sessionCookieName)
		if err != nil || c.Value != "token" {
			return Session{}, errUnauthenticated
		}
		return Session{Authenticated: true, Username: "alice"}, nil
	}}
	r := httptest.NewRequest(http.MethodGet, "/api/v1/session", nil)
	r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "token"})
	w := httptest.NewRecorder()
	NewServer(nil, ServerOptions{Sessions: f}).Handler().ServeHTTP(w, r)
	if w.Code != 200 || !called {
		t.Fatalf("status=%d called=%v", w.Code, called)
	}
}

func TestSessionDeleteOriginAndExpiry(t *testing.T) {
	for _, tc := range []struct {
		origin string
		cookie bool
		want   int
	}{{"", false, 204}, {"https://example.test", true, 204}, {"http://example.test", true, 403}, {"https://example.test:444", true, 403}, {"https://other.test", true, 403}} {
		r := httptest.NewRequest(http.MethodDelete, "https://example.test/api/v1/session", nil)
		r.Header.Set("Origin", tc.origin)
		if tc.cookie {
			r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "x"})
		}
		w := httptest.NewRecorder()
		NewServer(nil, ServerOptions{Sessions: &fakeSessions{}}).Handler().ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Errorf("origin %q status=%d", tc.origin, w.Code)
			continue
		}
		if tc.want == 204 {
			c := w.Result().Cookies()[0]
			if c.MaxAge != -1 || c.Value != "" || c.Path != "/" || !c.HttpOnly || !c.Secure || c.SameSite != http.SameSiteStrictMode {
				t.Errorf("expired cookie=%+v", c)
			}
		}
	}
	r := httptest.NewRequest(http.MethodDelete, "https://example.test/api/v1/session", nil)
	r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "x"})
	r.Header.Add("Origin", "https://example.test")
	r.Header.Add("Origin", "https://attacker.test")
	w := httptest.NewRecorder()
	NewServer(nil, ServerOptions{Sessions: &fakeSessions{}}).Handler().ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("duplicate Origin status=%d", w.Code)
	}
}
