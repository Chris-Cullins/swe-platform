// Package githubapp implements repository-scoped GitHub App installation tokens.
package githubapp

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/repositorycredential"
)

const apiBase = "https://api.github.com"
const maxBody = 64 << 10

var (
	ownerPattern      = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$`)
	repositoryPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,100}$`)
)

type Provider struct {
	ClientID string
	Key      *rsa.PrivateKey
	Client   *http.Client
	Now      func() time.Time
}

func New(clientID string, keyPEM []byte, client *http.Client) (*Provider, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, fmt.Errorf("invalid GitHub App private key")
	}
	var key *rsa.PrivateKey
	if parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		key, _ = parsed.(*rsa.PrivateKey)
	} else {
		key, _ = x509.ParsePKCS1PrivateKey(block.Bytes)
	}
	if clientID == "" || key == nil || key.Validate() != nil {
		return nil, fmt.Errorf("invalid GitHub App configuration")
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	return &Provider{ClientID: clientID, Key: key, Client: client, Now: time.Now}, nil
}

func canonical(repository string) (string, string, string, error) {
	u, err := url.Parse(repository)
	if err != nil || u.Scheme != "https" || u.Host != "github.com" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Port() != "" || u.RawPath != "" || strings.Contains(repository, "%") {
		return "", "", "", fmt.Errorf("unsupported repository URL")
	}
	if !strings.HasPrefix(u.Path, "/") || strings.HasSuffix(u.Path, "/") {
		return "", "", "", fmt.Errorf("unsupported repository URL")
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) != 2 {
		return "", "", "", fmt.Errorf("unsupported repository URL")
	}
	owner := parts[0]
	repo := strings.TrimSuffix(parts[1], ".git")
	if !ownerPattern.MatchString(owner) || !repositoryPattern.MatchString(repo) || repo == "." || repo == ".." {
		return "", "", "", fmt.Errorf("unsupported repository URL")
	}
	return "https://github.com/" + owner + "/" + repo, owner, repo, nil
}

func (p *Provider) CanonicalRepository(repository string) (string, error) {
	return Canonicalizer{}.CanonicalRepository(repository)
}

// Canonicalizer performs no I/O and holds no provider credentials.
type Canonicalizer struct{}

func (Canonicalizer) CanonicalRepository(repository string) (string, error) {
	canonicalURL, _, _, err := canonical(repository)
	if err != nil {
		return "", &repositorycredential.Error{Operation: "repository", Reason: "RepositoryUnsupported"}
	}
	return canonicalURL, nil
}

func (p *Provider) jwt() (string, error) {
	now := p.Now()
	enc := base64.RawURLEncoding
	headerJSON, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	header := enc.EncodeToString(headerJSON)
	payload, _ := json.Marshal(map[string]any{"iat": now.Add(-time.Minute).Unix(), "exp": now.Add(9 * time.Minute).Unix(), "iss": p.ClientID})
	unsigned := header + "." + enc.EncodeToString(payload)
	h := sha256.Sum256([]byte(unsigned))
	sig, err := rsa.SignPKCS1v15(rand.Reader, p.Key, crypto.SHA256, h[:])
	if err != nil {
		return "", err
	}
	return unsigned + "." + enc.EncodeToString(sig), nil
}

func (p *Provider) request(ctx context.Context, method, path, authorization string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, apiBase+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+authorization)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := *p.Client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return &repositorycredential.Error{Retryable: true, Operation: "request", Reason: "ProviderUnavailable"}
	}
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, maxBody+1)
	data, err := io.ReadAll(limited)
	if err != nil || len(data) > maxBody {
		return &repositorycredential.Error{Retryable: true, Operation: "response", Reason: "ProviderInvalidResponse"}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		retryable := resp.StatusCode == 429 || resp.StatusCode >= 500
		delay := time.Duration(0)
		var envelope struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(data, &envelope)
		message := strings.ToLower(envelope.Message)
		secondaryLimit := resp.StatusCode == http.StatusForbidden &&
			(strings.Contains(message, "secondary rate limit") || strings.Contains(message, "abuse detection"))
		if value := resp.Header.Get("Retry-After"); value != "" {
			if seconds, parseErr := strconv.ParseInt(value, 10, 64); parseErr == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			} else if at, parseErr := http.ParseTime(value); parseErr == nil {
				delay = at.Sub(p.Now())
			}
			retryable = retryable || resp.StatusCode == http.StatusForbidden
		}
		if resp.Header.Get("X-RateLimit-Remaining") == "0" {
			retryable = retryable || resp.StatusCode == http.StatusForbidden
			if reset, parseErr := strconv.ParseInt(resp.Header.Get("X-RateLimit-Reset"), 10, 64); parseErr == nil {
				delay = time.Unix(reset, 0).Sub(p.Now())
			}
		}
		if secondaryLimit {
			retryable = true
			if delay < time.Minute {
				delay = time.Minute
			}
		}
		if retryable && delay <= 0 {
			delay = repositorycredential.DefaultRetryDelay
		}
		if delay > 15*time.Minute {
			delay = 15 * time.Minute
		}
		return &repositorycredential.Error{Retryable: retryable, RetryAfter: delay, Operation: "API", Reason: "ProviderAPIError", StatusCode: resp.StatusCode}
	}
	if out != nil && json.Unmarshal(data, out) != nil {
		return &repositorycredential.Error{Retryable: false, Operation: "response", Reason: "ProviderInvalidResponse"}
	}
	return nil
}

func (p *Provider) Issue(ctx context.Context, repository string) (*repositorycredential.Credential, error) {
	canonicalURL, owner, repo, err := canonical(repository)
	if err != nil {
		return nil, &repositorycredential.Error{Operation: "repository", Reason: "RepositoryUnsupported"}
	}
	j, err := p.jwt()
	if err != nil {
		return nil, &repositorycredential.Error{Operation: "sign", Reason: "ProviderSigningFailed"}
	}
	var installation struct {
		ID int64 `json:"id"`
	}
	if err = p.request(ctx, http.MethodGet, "/repos/"+url.PathEscape(owner)+"/"+url.PathEscape(repo)+"/installation", j, nil, &installation); err != nil {
		return nil, err
	}
	if installation.ID < 1 {
		return nil, &repositorycredential.Error{Operation: "installation", Reason: "ProviderInvalidResponse"}
	}
	var token struct {
		Token               string            `json:"token"`
		ExpiresAt           json.RawMessage   `json:"expires_at"`
		RepositorySelection string            `json:"repository_selection"`
		Permissions         map[string]string `json:"permissions"`
		Repositories        []struct {
			FullName string `json:"full_name"`
		} `json:"repositories"`
	}
	body := map[string]any{"repositories": []string{repo}, "permissions": map[string]string{"contents": "write"}}
	if err = p.request(ctx, http.MethodPost, fmt.Sprintf("/app/installations/%d/access_tokens", installation.ID), j, body, &token); err != nil {
		return nil, err
	}
	cleanupNow := p.Now()
	cleanupDeadline := cleanupNow.Add(time.Hour)
	var expiresAt time.Time
	var expiresText string
	expiryTrusted := json.Unmarshal(token.ExpiresAt, &expiresText) == nil
	if expiryTrusted {
		expiresAt, err = time.Parse(time.RFC3339Nano, expiresText)
		expiryTrusted = err == nil && expiresAt.After(cleanupNow.Add(repositorycredential.MinimumValidity)) && !expiresAt.After(cleanupDeadline)
	}
	validPermissions := len(token.Permissions) >= 1 && len(token.Permissions) <= 2 && token.Permissions["contents"] == "write"
	for name, access := range token.Permissions {
		if name != "contents" && (name != "metadata" || access != "read") {
			validPermissions = false
		}
	}
	validRepositories := len(token.Repositories) == 0 || len(token.Repositories) == 1 && strings.EqualFold(token.Repositories[0].FullName, owner+"/"+repo)
	credential := &repositorycredential.Credential{Token: []byte(token.Token), Repository: canonicalURL, InstallationID: installation.ID, ExpiresAt: cleanupDeadline}
	if token.Token == "" || len(token.Token) > repositorycredential.MaxTokenBytes || strings.ContainsAny(token.Token, "\x00\r\n") {
		return nil, &repositorycredential.Error{Operation: "token", Reason: "ProviderInvalidToken"}
	}
	if expiryTrusted {
		credential.ExpiresAt = expiresAt
	}
	if !expiryTrusted || token.RepositorySelection != "selected" || !validPermissions || !validRepositories {
		return credential, &repositorycredential.Error{Operation: "token", Reason: "ProviderInvalidToken"}
	}
	return credential, nil
}

func (p *Provider) Revoke(ctx context.Context, credential *repositorycredential.Credential) error {
	if credential == nil || len(credential.Token) == 0 {
		return nil
	}
	err := p.request(ctx, http.MethodDelete, "/installation/token", string(credential.Token), nil, nil)
	var providerErr *repositorycredential.Error
	if errors.As(err, &providerErr) && providerErr.StatusCode == http.StatusUnauthorized {
		return nil
	}
	return err
}
