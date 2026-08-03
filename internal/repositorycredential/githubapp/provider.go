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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/repositorycredential"
)

const apiBase = "https://api.github.com"
const maxBody = 64 << 10

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
	if err != nil || u.Scheme != "https" || u.Host != "github.com" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Port() != "" {
		return "", "", "", fmt.Errorf("unsupported repository URL")
	}
	parts := strings.Split(strings.Trim(u.EscapedPath(), "/"), "/")
	if len(parts) != 2 {
		return "", "", "", fmt.Errorf("unsupported repository URL")
	}
	owner, err1 := url.PathUnescape(parts[0])
	repo, err2 := url.PathUnescape(strings.TrimSuffix(parts[1], ".git"))
	if err1 != nil || err2 != nil || owner == "" || repo == "" || strings.ContainsAny(owner+repo, "/\\") {
		return "", "", "", fmt.Errorf("unsupported repository URL")
	}
	return "https://github.com/" + owner + "/" + repo, owner, repo, nil
}

func (p *Provider) jwt() (string, error) {
	now := p.Now()
	enc := base64.RawURLEncoding
	header := enc.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
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
	resp, err := p.Client.Do(req)
	if err != nil {
		return &repositorycredential.Error{Retryable: true, Operation: "request"}
	}
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, maxBody+1)
	data, err := io.ReadAll(limited)
	if err != nil || len(data) > maxBody {
		return &repositorycredential.Error{Retryable: true, Operation: "response"}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &repositorycredential.Error{Retryable: resp.StatusCode == 429 || resp.StatusCode >= 500, Operation: "API"}
	}
	if out != nil && json.Unmarshal(data, out) != nil {
		return &repositorycredential.Error{Retryable: false, Operation: "response"}
	}
	return nil
}

func (p *Provider) Issue(ctx context.Context, repository string) (*repositorycredential.Credential, error) {
	canonicalURL, owner, repo, err := canonical(repository)
	if err != nil {
		return nil, &repositorycredential.Error{Operation: "repository"}
	}
	j, err := p.jwt()
	if err != nil {
		return nil, &repositorycredential.Error{Operation: "sign"}
	}
	var installation struct {
		ID int64 `json:"id"`
	}
	if err = p.request(ctx, http.MethodGet, "/repos/"+url.PathEscape(owner)+"/"+url.PathEscape(repo)+"/installation", j, nil, &installation); err != nil {
		return nil, err
	}
	var token struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	body := map[string]any{"repositories": []string{repo}, "permissions": map[string]string{"contents": "write"}}
	if err = p.request(ctx, http.MethodPost, fmt.Sprintf("/app/installations/%d/access_tokens", installation.ID), j, body, &token); err != nil {
		return nil, err
	}
	if token.Token == "" || len(token.Token) > repositorycredential.MaxTokenBytes || token.ExpiresAt.IsZero() {
		return nil, &repositorycredential.Error{Operation: "token"}
	}
	return &repositorycredential.Credential{Token: []byte(token.Token), Repository: canonicalURL, InstallationID: installation.ID, ExpiresAt: token.ExpiresAt}, nil
}

func (p *Provider) Revoke(ctx context.Context, credential *repositorycredential.Credential) error {
	if credential == nil || len(credential.Token) == 0 {
		return nil
	}
	return p.request(ctx, http.MethodDelete, "/installation/token", string(credential.Token), nil, nil)
}
