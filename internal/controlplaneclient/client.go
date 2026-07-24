// Package controlplaneclient provides authenticated control-plane HTTP and SSE primitives.
package controlplaneclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client is an explicit bearer-authenticated control-plane client.
type Client struct {
	baseURL       *url.URL
	token         string
	httpClient    *http.Client
	reconnectWait time.Duration
}

// New validates a control-plane base URL and bearer token.
func New(baseURL, token string, httpClient *http.Client) (*Client, error) {
	parsed, err := parseBaseURL(baseURL)
	if err != nil {
		return nil, err
	}
	if token == "" {
		return nil, fmt.Errorf("control-plane token is required (set --token or SWE_CONTROL_PLANE_TOKEN)")
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{baseURL: parsed, token: token, httpClient: httpClient, reconnectWait: time.Second}, nil
}

// Endpoint builds an escaped URL below the configured base path.
func (c *Client) Endpoint(segments ...string) string {
	return endpoint(c.baseURL, segments...)
}

// WebSocketEndpoint builds an escaped WebSocket URL below the configured base path.
func (c *Client) WebSocketEndpoint(segments ...string) string {
	parsed := *c.baseURL
	if parsed.Scheme == "http" {
		parsed.Scheme = "ws"
	} else {
		parsed.Scheme = "wss"
	}
	return endpoint(&parsed, segments...)
}

// WebSocketEndpoint builds an escaped WebSocket URL below an HTTP(S) base URL.
func WebSocketEndpoint(baseURL string, segments ...string) (string, error) {
	parsed, err := parseBaseURL(baseURL)
	if err != nil {
		return "", err
	}
	client := &Client{baseURL: parsed}
	return client.WebSocketEndpoint(segments...), nil
}

func parseBaseURL(baseURL string) (*url.URL, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("control-plane URL is required (set --control-plane or SWE_CONTROL_PLANE_URL)")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse control-plane URL: %w", err)
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("control-plane URL must be an HTTP(S) base URL without a query or fragment")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("control-plane URL scheme must be http or https")
	}
	return parsed, nil
}

func endpoint(base *url.URL, segments ...string) string {
	escaped := make([]string, len(segments))
	for i, segment := range segments {
		escaped[i] = url.PathEscape(segment)
	}
	return strings.TrimRight(base.String(), "/") + "/" + strings.Join(escaped, "/")
}

// AuthorizationHeader returns a fresh bearer header suitable for HTTP or WebSocket requests.
func (c *Client) AuthorizationHeader() http.Header {
	return http.Header{"Authorization": []string{"Bearer " + c.token}}
}

// NewRequest creates an authenticated request to endpoint.
func (c *Client) NewRequest(ctx context.Context, method, endpoint string, body io.Reader) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, err
	}
	request.Header = c.AuthorizationHeader()
	return request, nil
}

// Do sends a request and converts non-2xx responses into ProblemError values.
func (c *Client) Do(request *http.Request) (*http.Response, error) {
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return response, nil
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	problem := Problem{}
	mediaType, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if readErr == nil && mediaType == "application/problem+json" {
		_ = json.Unmarshal(body, &problem)
	}
	problem.Status = response.StatusCode
	return nil, &ProblemError{
		Status:     response.Status,
		Problem:    problem,
		Body:       bytes.TrimSpace(body),
		ReadErr:    readErr,
		retryAfter: response.Header.Get("Retry-After"),
	}
}

// Problem is an RFC problem response. Extra endpoint-specific fields remain in Body.
type Problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
}

// ProblemError reports a non-successful control-plane HTTP response.
type ProblemError struct {
	Status  string
	Problem Problem
	Body    []byte
	ReadErr error
	// Retain only the reconnect hint, not arbitrary response headers.
	retryAfter string
}

func (e *ProblemError) Error() string {
	detail := e.Problem.Detail
	if detail == "" {
		detail = e.Problem.Title
	}
	if detail == "" {
		detail = string(e.Body)
	}
	if detail == "" {
		detail = "control plane returned " + e.Status
	} else {
		detail = fmt.Sprintf("control plane returned %s: %s", e.Status, detail)
	}
	if e.ReadErr != nil {
		return fmt.Sprintf("%s (read response body: %v)", detail, e.ReadErr)
	}
	return detail
}

// Unwrap exposes a response-body read failure without losing the known HTTP status.
func (e *ProblemError) Unwrap() error { return e.ReadErr }
