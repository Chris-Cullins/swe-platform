package githubapp

import (
	"bytes"
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
	for _, invalid := range []string{"http://github.com/a/b", "https://evil.test/a/b", "https://github.com/a/b/c", "git@github.com:a/b", "https://github.com/a/b/", "https://github.com/a/%3Freleases", "https://github.com/-bad/repo", "https://github.com/acme/.."} {
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
		{name: "secondary limit", status: 403, headers: http.Header{}, retryable: true, delay: time.Minute},
		{name: "forbidden retry after", status: 403, headers: http.Header{"Retry-After": []string{"30"}}, retryable: true, delay: 30 * time.Second},
		{name: "forbidden exhausted", status: 403, headers: http.Header{"X-Ratelimit-Remaining": []string{"0"}, "X-Ratelimit-Reset": []string{strconv.FormatInt(now.Add(time.Minute).Unix(), 10)}}, retryable: true, delay: time.Minute},
		{name: "too many requests fallback", status: 429, headers: http.Header{}, retryable: true, delay: repositorycredential.DefaultRetryDelay},
		{name: "clamped", status: 429, headers: http.Header{"Retry-After": []string{"9999"}}, retryable: true, delay: 15 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &Provider{Now: func() time.Time { return now }, Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				body := "sensitive body"
				if tc.name == "secondary limit" {
					encoded, _ := json.Marshal(map[string]string{"message": "You have exceeded a secondary rate limit."})
					body = string(encoded)
				}
				return &http.Response{StatusCode: tc.status, Header: tc.headers, Body: io.NopCloser(strings.NewReader(body))}, nil
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
			header, decodeErr := base64.RawURLEncoding.DecodeString(parts[0])
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			var protected struct {
				Alg string `json:"alg"`
				Typ string `json:"typ"`
			}
			if json.Unmarshal(header, &protected) != nil || protected.Alg != "RS256" || protected.Typ != "JWT" {
				t.Errorf("JWT header = %q", header)
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
		encoded, _ := json.Marshal(map[string]any{"id": int64(42)})
		body := string(encoded)
		if req.Method == http.MethodPost {
			data, _ := io.ReadAll(req.Body)
			var request struct {
				Repositories []string          `json:"repositories"`
				Permissions  map[string]string `json:"permissions"`
			}
			if json.Unmarshal(data, &request) != nil || len(request.Repositories) != 1 || request.Repositories[0] != "widget" || request.Permissions["contents"] != "write" || len(request.Permissions) != 1 {
				t.Errorf("token body = %s", data)
			}
			encoded, _ = json.Marshal(map[string]any{"token": "secret-token", "expires_at": "2026-08-03T13:00:00Z", "repository_selection": "selected", "permissions": map[string]string{"contents": "write", "metadata": "read"}, "repositories": []map[string]string{{"full_name": "acme/widget"}}})
			body = string(encoded)
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

func TestIssueValidatesExactTokenBoundaryAndScope(t *testing.T) {
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name           string
		expires        any
		selection      string
		permissions    map[string]string
		repositories   []string
		token          string
		wantCredential bool
		wantError      bool
		wantDeadline   time.Time
	}{
		{name: "missing expiry", expires: nil, selection: "selected", permissions: map[string]string{"contents": "write"}, repositories: []string{"acme/widget"}, token: "token", wantCredential: true, wantError: true, wantDeadline: now.Add(time.Hour)},
		{name: "malformed expiry", expires: "not-a-time", selection: "selected", permissions: map[string]string{"contents": "write"}, repositories: []string{"acme/widget"}, token: "token", wantCredential: true, wantError: true, wantDeadline: now.Add(time.Hour)},
		{name: "zero expiry", expires: time.Time{}, selection: "selected", permissions: map[string]string{"contents": "write"}, repositories: []string{"acme/widget"}, token: "token", wantCredential: true, wantError: true, wantDeadline: now.Add(time.Hour)},
		{name: "past expiry", expires: now.Add(-time.Minute), selection: "selected", permissions: map[string]string{"contents": "write"}, repositories: []string{"acme/widget"}, token: "token", wantCredential: true, wantError: true, wantDeadline: now.Add(time.Hour)},
		{name: "exact minimum", expires: now.Add(repositorycredential.MinimumValidity), selection: "selected", permissions: map[string]string{"contents": "write"}, repositories: []string{"acme/widget"}, token: "token", wantCredential: true, wantError: true, wantDeadline: now.Add(time.Hour)},
		{name: "below minimum", expires: now.Add(repositorycredential.MinimumValidity - time.Nanosecond), selection: "selected", permissions: map[string]string{"contents": "write"}, repositories: []string{"acme/widget"}, token: "token", wantCredential: true, wantError: true, wantDeadline: now.Add(time.Hour)},
		{name: "above exact scope", expires: now.Add(repositorycredential.MinimumValidity + time.Nanosecond), selection: "selected", permissions: map[string]string{"contents": "write"}, repositories: []string{"acme/widget"}, token: "token", wantCredential: true},
		{name: "beyond maximum", expires: now.Add(time.Hour + time.Nanosecond), selection: "selected", permissions: map[string]string{"contents": "write"}, repositories: []string{"acme/widget"}, token: "token", wantCredential: true, wantError: true, wantDeadline: now.Add(time.Hour)},
		{name: "wrong selection", expires: now.Add(time.Hour), selection: "all", permissions: map[string]string{"contents": "write"}, repositories: []string{"acme/widget"}, token: "token", wantCredential: true, wantError: true},
		{name: "broader permissions", expires: now.Add(time.Hour), selection: "selected", permissions: map[string]string{"contents": "write", "issues": "write"}, repositories: []string{"acme/widget"}, token: "token", wantCredential: true, wantError: true},
		{name: "returned repository mismatch", expires: now.Add(time.Hour), selection: "selected", permissions: map[string]string{"contents": "write"}, repositories: []string{"acme/other"}, token: "token", wantCredential: true, wantError: true},
		{name: "empty token", expires: now.Add(time.Hour), selection: "selected", permissions: map[string]string{"contents": "write"}, token: "", wantError: true},
		{name: "NUL token", expires: now.Add(time.Hour), selection: "selected", permissions: map[string]string{"contents": "write"}, token: "bad\x00token", wantError: true},
		{name: "oversize token", expires: now.Add(time.Hour), selection: "selected", permissions: map[string]string{"contents": "write"}, token: strings.Repeat("x", repositorycredential.MaxTokenBytes+1), wantError: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			p := &Provider{ClientID: "client", Key: key, Now: func() time.Time { return now }}
			p.Client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				var body []byte
				if calls == 1 {
					body, _ = json.Marshal(map[string]any{"id": 42})
				} else {
					repositories := make([]map[string]string, len(tc.repositories))
					for i, repository := range tc.repositories {
						repositories[i] = map[string]string{"full_name": repository}
					}
					body, _ = json.Marshal(map[string]any{"token": tc.token, "expires_at": tc.expires, "repository_selection": tc.selection, "permissions": tc.permissions, "repositories": repositories})
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
			})}
			credential, issueErr := p.Issue(context.Background(), "https://github.com/acme/widget")
			if (credential != nil) != tc.wantCredential || (issueErr != nil) != tc.wantError {
				t.Fatalf("Issue credential=%#v error=%v", credential, issueErr)
			}
			if tc.wantError && repositorycredential.Reason(issueErr) != "ProviderInvalidToken" {
				t.Fatalf("reason = %q, error %v", repositorycredential.Reason(issueErr), issueErr)
			}
			if tc.wantDeadline.IsZero() == false && (credential == nil || !credential.ExpiresAt.Equal(tc.wantDeadline)) {
				t.Fatalf("cleanup deadline = %#v, want %v", credential, tc.wantDeadline)
			}
		})
	}
}

func TestRevokeTreatsUnauthorizedAsAlreadyInactive(t *testing.T) {
	p := &Provider{Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		body, _ := json.Marshal(map[string]string{"message": "Bad credentials"})
		return &http.Response{StatusCode: http.StatusUnauthorized, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
	})}}
	if err := p.Revoke(context.Background(), &repositorycredential.Credential{Token: []byte("already-revoked")}); err != nil {
		t.Fatal(err)
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
