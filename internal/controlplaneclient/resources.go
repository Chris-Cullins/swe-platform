package controlplaneclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
)

const maxRunListPages = 100

// ListRuns returns every Run in a namespace using bounded API pagination.
func (c *Client) ListRuns(ctx context.Context, namespace string) ([]controlplane.Run, error) {
	items := make([]controlplane.Run, 0)
	seen := make(map[string]struct{})
	continuation := ""
	for pageNumber := 0; pageNumber < maxRunListPages; pageNumber++ {
		endpoint := c.Endpoint("api", "v1", "namespaces", namespace, "runs")
		query := url.Values{"limit": {"200"}}
		if continuation != "" {
			query.Set("continue", continuation)
		}
		var page controlplane.RunList
		if err := c.getJSON(ctx, endpoint+"?"+query.Encode(), &page); err != nil {
			return nil, err
		}
		items = append(items, page.Items...)
		if page.Continue == "" {
			return items, nil
		}
		if _, duplicate := seen[page.Continue]; duplicate {
			return nil, fmt.Errorf("control plane repeated a Run list cursor")
		}
		seen[page.Continue] = struct{}{}
		continuation = page.Continue
	}
	return nil, fmt.Errorf("control plane Run list exceeded %d pages", maxRunListPages)
}

// GetRun returns an exact namespaced Run.
func (c *Client) GetRun(ctx context.Context, namespace, name string) (controlplane.Run, error) {
	var run controlplane.Run
	err := c.getJSON(ctx, c.Endpoint("api", "v1", "namespaces", namespace, "runs", name), &run)
	return run, err
}

// CreateRun creates a Run or returns the existing Run when the server accepts
// an exact immutable-intent retry under the same name.
func (c *Client) CreateRun(ctx context.Context, namespace string, value controlplane.CreateRunRequest) (controlplane.Run, error) {
	var run controlplane.Run
	err := c.sendJSON(ctx, http.MethodPost, c.Endpoint("api", "v1", "namespaces", namespace, "runs"), value, &run)
	return run, err
}

// CancelRun requests idempotent cancellation of an exact namespaced Run.
func (c *Client) CancelRun(ctx context.Context, namespace, name string) (controlplane.Run, error) {
	var run controlplane.Run
	err := c.sendJSON(ctx, http.MethodPost, c.Endpoint("api", "v1", "namespaces", namespace, "runs", name, "cancel"), nil, &run)
	return run, err
}

// GetEnvironment returns an exact namespaced Environment.
func (c *Client) GetEnvironment(ctx context.Context, namespace, name string) (controlplane.Environment, error) {
	var environment controlplane.Environment
	err := c.getJSON(ctx, c.Endpoint("api", "v1", "namespaces", namespace, "environments", name), &environment)
	return environment, err
}

func (c *Client) getJSON(ctx context.Context, endpoint string, destination any) error {
	request, err := c.NewRequest(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	response, err := c.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	return decodeJSON(response.Body, destination)
}

func (c *Client) sendJSON(ctx context.Context, method, endpoint string, value, destination any) error {
	var body io.Reader
	if value != nil {
		encoded, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("encode control-plane request: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := c.NewRequest(ctx, method, endpoint, body)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	if value != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	return decodeJSON(response.Body, destination)
}

func decodeJSON(reader io.Reader, destination any) error {
	decoder := json.NewDecoder(io.LimitReader(reader, 2<<20))
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode control-plane response: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("decode control-plane response: expected one JSON value")
	}
	return nil
}
