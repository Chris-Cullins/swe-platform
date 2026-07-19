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

	"github.com/Chris-Cullins/swe-platform/internal/controllers"
)

// Client is an AdapterEventSink backed by the authenticated transcript API.
// TokenFile is read for each request so projected service-account rotation is
// observed without restarting the operator.
type Client struct {
	BaseURL   string
	TokenFile string
	HTTP      *http.Client
}

func (c Client) Append(ctx context.Context, namespace, run string, event controllers.AdapterEvent) error {
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
			return fmt.Errorf("%w: %v", controllers.ErrAdapterEventRejected, err)
		}
		return err
	}
	return nil
}

func permanentRejection(statusCode int) bool {
	if statusCode == http.StatusUnauthorized || statusCode == http.StatusRequestTimeout || statusCode == http.StatusTooManyRequests {
		return false
	}
	if statusCode >= 500 && statusCode != http.StatusInsufficientStorage {
		return false
	}
	return true
}
