// Package transcriptclient forwards adapter-owned events to the control plane.
package transcriptclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/agent"
	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
)

// Client is an AdapterEventSink backed by the authenticated transcript API.
// TokenFile is read for each request so projected service-account rotation is
// observed without restarting the operator.
type Client struct {
	BaseURL   string
	TokenFile string
	HTTP      *http.Client
}

var _ agent.AdapterEventSink = Client{}

// Delete acknowledges only the cleanup endpoint's committed/idempotent 204.
// In particular, an old server's 404 or HTML fallback is never cleanup success.
func (c Client) Delete(ctx context.Context, namespace, namespaceUID, run, runUID string) error {
	if namespace == "" || namespaceUID == "" || run == "" || runUID == "" {
		return fmt.Errorf("transcript cleanup requires complete identity")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	token, err := os.ReadFile(c.TokenFile)
	if err != nil || strings.TrimSpace(string(token)) == "" {
		return fmt.Errorf("transcript cleanup credential unavailable")
	}
	endpoint := strings.TrimRight(c.BaseURL, "/") + "/api/v1/namespaces/" + url.PathEscape(namespace) + "/runs/" + url.PathEscape(run) + "/transcript"
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return fmt.Errorf("create transcript cleanup request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	request.Header.Set(controlplane.RunUIDHeader, runUID)
	request.Header.Set(controlplane.NamespaceUIDHeader, namespaceUID)
	httpClient := http.DefaultClient
	if c.HTTP != nil {
		httpClient = c.HTTP
	}
	// Never forward credentials or accept success from a redirected endpoint.
	isolated := *httpClient
	isolated.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := isolated.Do(request)
	if err != nil {
		return fmt.Errorf("transcript cleanup transport unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("transcript cleanup returned HTTP %d", response.StatusCode)
	}
	return nil
}

func (c Client) Append(ctx context.Context, namespace, run, runUID string, event agent.AdapterEvent) error {
	token, err := os.ReadFile(c.TokenFile)
	if err != nil {
		return fmt.Errorf("read transcript credential: %w", err)
	}
	body, err := json.Marshal(struct {
		Source         string          `json:"source"`
		IdempotencyKey string          `json:"idempotencyKey"`
		Type           string          `json:"type"`
		Data           json.RawMessage `json:"data"`
	}{event.Source, event.IdempotencyKey, event.Type, event.Data})
	if err != nil {
		return fmt.Errorf("encode transcript event: %w", err)
	}
	endpoint := strings.TrimRight(c.BaseURL, "/") + "/api/v1/namespaces/" + url.PathEscape(namespace) + "/runs/" + url.PathEscape(run) + "/transcript"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create transcript request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(controlplane.RunUIDHeader, runUID)
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("append transcript event: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		if _, err := io.Copy(io.Discard, io.LimitReader(response.Body, 2<<20)); err != nil {
			return fmt.Errorf("drain transcript response: %w", err)
		}
		return nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		err := fmt.Errorf("append transcript event: control plane returned %s: %s", response.Status, strings.TrimSpace(string(message)))
		if permanentRejection(response.StatusCode) {
			return fmt.Errorf("%w: %v", agent.ErrAdapterEventRejected, err)
		}
		return err
	}
	return nil
}

func permanentRejection(statusCode int) bool {
	switch statusCode {
	case http.StatusBadRequest,
		http.StatusForbidden,
		http.StatusConflict,
		http.StatusRequestEntityTooLarge,
		http.StatusInsufficientStorage:
		return true
	default:
		return false
	}
}
