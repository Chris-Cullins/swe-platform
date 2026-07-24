package controlplaneclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
)

const (
	maxRunListPages        = 100
	maxRunSnapshotAttempts = 3
	maxRunWatchEvent       = 64 << 10
	// A server page has at most 200 summaries; every caller-controlled field is
	// length-bounded and each prompt preview has at most 160 runes. This leaves
	// ample room for JSON's worst-case escaping without accepting unbounded data.
	maxRunSummaryResponse = 2 << 20
	// Create accepts at most 1 MiB of JSON. Go's HTML-safe response encoding can
	// expand one-byte prompt characters sixfold, so exact resources need more
	// than 6 MiB while remaining bounded.
	maxExactResourceResponse = 8 << 20
)

var ErrRunRelist = errors.New("Run watch requires a full relist")

type initialRunWatchCompatibilityError struct{ error }

// RunSummarySnapshot is one fully paginated, internally consistent collection
// snapshot. WatchSupported is false only for a legacy server that omitted its
// required resource version.
type RunSummarySnapshot struct {
	Items           []controlplane.RunSummary
	ResourceVersion string
	WatchSupported  bool
}

// ListRunSummaries returns every bounded Run summary in a namespace using
// bounded API pagination. Full prompts are fetched only from exact Run detail.
func (c *Client) ListRunSummaries(ctx context.Context, namespace string) ([]controlplane.RunSummary, error) {
	snapshot, err := c.ListRunSummarySnapshot(ctx, namespace)
	return snapshot.Items, err
}

// ListRunSummarySnapshot restarts the complete continuation chain when its
// Kubernetes snapshot expires or a server returns inconsistent page versions.
func (c *Client) ListRunSummarySnapshot(ctx context.Context, namespace string) (RunSummarySnapshot, error) {
	for attempt := 0; attempt < maxRunSnapshotAttempts; attempt++ {
		snapshot, restart, err := c.listRunSummarySnapshot(ctx, namespace)
		if err != nil {
			var problem *ProblemError
			if errors.As(err, &problem) && problem.Problem.Status == http.StatusGone {
				continue
			}
			return RunSummarySnapshot{}, err
		}
		if !restart {
			return snapshot, nil
		}
	}
	return RunSummarySnapshot{}, fmt.Errorf("control plane Run snapshot did not remain consistent after %d attempts", maxRunSnapshotAttempts)
}

func (c *Client) listRunSummarySnapshot(ctx context.Context, namespace string) (RunSummarySnapshot, bool, error) {
	items := make([]controlplane.RunSummary, 0)
	seen := make(map[string]struct{})
	continuation := ""
	resourceVersion := ""
	for pageNumber := 0; pageNumber < maxRunListPages; pageNumber++ {
		endpoint := c.Endpoint("api", "v1", "namespaces", namespace, "runs")
		query := url.Values{"limit": {"200"}, "view": {"summary"}}
		if continuation != "" {
			query.Set("continue", continuation)
		}
		var page controlplane.RunSummaryList
		if err := c.getJSONLimit(ctx, endpoint+"?"+query.Encode(), &page, maxRunSummaryResponse); err != nil {
			return RunSummarySnapshot{}, false, err
		}
		if pageNumber == 0 {
			resourceVersion = page.ResourceVersion
		} else if page.ResourceVersion != resourceVersion {
			return RunSummarySnapshot{}, true, nil
		}
		items = append(items, page.Items...)
		if page.Continue == "" {
			return RunSummarySnapshot{Items: items, ResourceVersion: resourceVersion, WatchSupported: resourceVersion != ""}, false, nil
		}
		if _, duplicate := seen[page.Continue]; duplicate {
			return RunSummarySnapshot{}, false, fmt.Errorf("control plane repeated a Run list cursor")
		}
		seen[page.Continue] = struct{}{}
		continuation = page.Continue
	}
	return RunSummarySnapshot{}, false, fmt.Errorf("control plane Run list exceeded %d pages", maxRunListPages)
}

// StreamRunSummaries consumes the typed collection watch. It reconnects with
// Last-Event-ID only after the callback has successfully handled an event.
func (c *Client) StreamRunSummaries(ctx context.Context, namespace, resourceVersion string, established func(), handle func(controlplane.RunWatchEvent) error) error {
	endpoint := c.Endpoint("api", "v1", "namespaces", namespace, "runs")
	query := url.Values{"watch": {"true"}, "view": {"summary"}, "resourceVersion": {resourceVersion}}
	connected := false
	err := c.streamSSEWithReconnectCheck(ctx, endpoint+"?"+query.Encode(), "", nil, retryInitialRunWatch, func() {
		connected = true
		if established != nil {
			established()
		}
	}, func(event SSEEvent) error {
		if len(event.Data) > maxRunWatchEvent || len(event.ID) > 4096 {
			return fmt.Errorf("Run watch event exceeds its bounded contract")
		}
		switch event.Event {
		case "run-relist":
			if event.HasID {
				return fmt.Errorf("Run relist event must not carry an ID")
			}
			return ErrRunRelist
		case "run-checkpoint":
			var checkpoint controlplane.RunWatchCheckpoint
			if err := json.Unmarshal(event.Data, &checkpoint); err != nil || !event.HasID || checkpoint.ResourceVersion == "" || checkpoint.ResourceVersion != event.ID {
				return fmt.Errorf("invalid Run watch checkpoint")
			}
			return nil
		case "run":
			var update controlplane.RunWatchEvent
			if err := json.Unmarshal(event.Data, &update); err != nil {
				return fmt.Errorf("decode Run watch event: %w", err)
			}
			if !event.HasID || update.ResourceVersion == "" || update.ResourceVersion != event.ID || (update.Type != "ADDED" && update.Type != "MODIFIED" && update.Type != "DELETED") {
				return fmt.Errorf("invalid Run watch event")
			}
			return handle(update)
		default:
			return nil
		}
	})
	if !connected && runWatchCompatibilityStatus(err) {
		return &initialRunWatchCompatibilityError{error: err}
	}
	return err
}

// RunWatchCompatibilityFallback reports only explicit legacy endpoint statuses.
func RunWatchCompatibilityFallback(err error) bool {
	var initial *initialRunWatchCompatibilityError
	return errors.As(err, &initial)
}

func runWatchCompatibilityStatus(err error) bool {
	var problem *ProblemError
	return errors.As(err, &problem) && (problem.Problem.Status == http.StatusNotFound || problem.Problem.Status == http.StatusMethodNotAllowed || problem.Problem.Status == http.StatusNotImplemented)
}

func retryInitialRunWatch(err error) bool {
	var problem *ProblemError
	if !errors.As(err, &problem) {
		return true
	}
	return retryableHTTPStatus(problem.Problem.Status) && !runWatchCompatibilityStatus(err)
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
func (c *Client) CancelRun(ctx context.Context, namespace, name, expectedUID string) (controlplane.Run, error) {
	var run controlplane.Run
	var body any
	if expectedUID != "" {
		body = controlplane.CancelRunRequest{RunUID: expectedUID}
	}
	err := c.sendJSON(ctx, http.MethodPost, c.Endpoint("api", "v1", "namespaces", namespace, "runs", name, "cancel"), body, &run)
	return run, err
}

// GetEnvironment returns an exact namespaced Environment.
func (c *Client) GetEnvironment(ctx context.Context, namespace, name string) (controlplane.Environment, error) {
	var environment controlplane.Environment
	err := c.getJSON(ctx, c.Endpoint("api", "v1", "namespaces", namespace, "environments", name), &environment)
	return environment, err
}

func (c *Client) getJSON(ctx context.Context, endpoint string, destination any) error {
	return c.getJSONLimit(ctx, endpoint, destination, maxExactResourceResponse)
}

func (c *Client) getJSONLimit(ctx context.Context, endpoint string, destination any, limit int64) error {
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
	return decodeJSON(response.Body, destination, limit)
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
	return decodeJSON(response.Body, destination, maxExactResourceResponse)
}

func decodeJSON(reader io.Reader, destination any, limit int64) error {
	decoder := json.NewDecoder(io.LimitReader(reader, limit))
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode control-plane response: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("decode control-plane response: expected one JSON value")
	}
	return nil
}
