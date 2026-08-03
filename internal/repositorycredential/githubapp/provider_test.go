package githubapp

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/repositorycredential"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCanonicalRepository(t *testing.T) {
	p := &Provider{}
	for _, test := range []struct{ input, want string }{
		{"https://github.com/acme/widget", "https://github.com/acme/widget"},
		{"https://github.com/acme/widget.git", "https://github.com/acme/widget"},
	} {
		got, err := p.CanonicalRepository(test.input)
		if err != nil || got != test.want {
			t.Fatalf("CanonicalRepository(%q) = %q, %v", test.input, got, err)
		}
	}
	for _, invalid := range []string{"http://github.com/a/b", "https://evil.test/a/b", "https://github.com/a/b/c", "git@github.com:a/b"} {
		if _, err := p.CanonicalRepository(invalid); err == nil {
			t.Errorf("CanonicalRepository(%q) succeeded", invalid)
		}
	}
}

func TestGitHubRateLimitClassification(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		status    int
		headers   http.Header
		retryable bool
		delay     time.Duration
	}{
		{name: "ordinary forbidden", status: 403, headers: http.Header{}, retryable: false},
		{name: "forbidden retry after", status: 403, headers: http.Header{"Retry-After": []string{"30"}}, retryable: true, delay: 30 * time.Second},
		{name: "forbidden exhausted", status: 403, headers: http.Header{"X-Ratelimit-Remaining": []string{"0"}, "X-Ratelimit-Reset": []string{strconv.FormatInt(now.Add(time.Minute).Unix(), 10)}}, retryable: true, delay: time.Minute},
		{name: "too many requests fallback", status: 429, headers: http.Header{}, retryable: true, delay: repositorycredential.DefaultRetryDelay},
		{name: "clamped", status: 429, headers: http.Header{"Retry-After": []string{"9999"}}, retryable: true, delay: 15 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &Provider{Now: func() time.Time { return now }, Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tc.status, Header: tc.headers, Body: io.NopCloser(strings.NewReader("sensitive body"))}, nil
			})}}
			err := p.request(context.Background(), http.MethodGet, "/test", "token", nil, nil)
			if repositorycredential.IsRetryable(err) != tc.retryable || repositorycredential.RetryDelay(err) != func() time.Duration {
				if tc.delay > 0 {
					return tc.delay
				}
				return repositorycredential.DefaultRetryDelay
			}() || strings.Contains(err.Error(), "sensitive") {
				t.Fatalf("error classification = %v retry=%v delay=%v", err, repositorycredential.IsRetryable(err), repositorycredential.RetryDelay(err))
			}
		})
	}
}

func TestIssueAndRevokeGitHubContract(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	var calls []string
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.Method+" "+req.URL.Path)
		if req.Header.Get("Accept") != "application/vnd.github+json" || req.Header.Get("X-GitHub-Api-Version") != "2022-11-28" {
			t.Errorf("fixed headers missing: %#v", req.Header)
		}
		auth := strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")
		if req.Method != http.MethodDelete {
			parts := strings.Split(auth, ".")
			if len(parts) != 3 {
				t.Fatalf("authorization is not JWT")
			}
			payload, decodeErr := base64.RawURLEncoding.DecodeString(parts[1])
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			var claims struct {
				Iss      string `json:"iss"`
				Iat, Exp int64
			}
			if json.Unmarshal(payload, &claims) != nil || claims.Iss != "client" || claims.Iat != now.Add(-time.Minute).Unix() || claims.Exp != now.Add(9*time.Minute).Unix() {
				t.Errorf("JWT claims = %#v", claims)
			}
		}
		body := `{"id":42}`
		if req.Method == http.MethodPost {
			data, _ := io.ReadAll(req.Body)
			if string(data) != `{"permissions":{"contents":"write"},"repositories":["widget"]}` {
				t.Errorf("token body = %s", data)
			}
			body = `{"token":"secret-token","expires_at":"2026-08-03T13:00:00Z"}`
		}
		if req.Method == http.MethodDelete {
			if auth != "secret-token" {
				t.Errorf("revoke authorization = %q", auth)
			}
			body = ""
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	p := &Provider{ClientID: "client", Key: key, Client: client, Now: func() time.Time { return now }}
	credential, err := p.Issue(context.Background(), "https://github.com/acme/widget.git")
	if err != nil {
		t.Fatal(err)
	}
	if credential.Repository != "https://github.com/acme/widget" || credential.InstallationID != 42 {
		t.Fatalf("credential = %#v", credential)
	}
	if err := p.Revoke(context.Background(), credential); err != nil {
		t.Fatal(err)
	}
	want := []string{"GET /repos/acme/widget/installation", "POST /app/installations/42/access_tokens", "DELETE /installation/token"}
	if strings.Join(calls, "|") != strings.Join(want, "|") {
		t.Fatalf("calls = %v", calls)
	}
}

func TestProviderResponseBoundsAndStableRetry(t *testing.T) {
	p := &Provider{Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("provider detail must not escape")), Header: make(http.Header)}, nil
	})}}
	err := p.request(context.Background(), http.MethodGet, "/test", "token", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "provider API failed") || strings.Contains(err.Error(), "detail") {
		t.Fatalf("error = %v", err)
	}
	p.Client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(strings.Repeat("x", maxBody+1))), Header: make(http.Header)}, nil
	})
	if err := p.request(context.Background(), http.MethodGet, "/test", "token", nil, nil); err == nil {
		t.Fatal("oversize body accepted")
	}
}
