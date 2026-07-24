package controlplaneclient

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// The server accepts JSON request bodies up to 1 MiB. json.Marshal may expand
	// each HTML-sensitive byte to a six-byte Unicode escape in the SSE envelope;
	// 8 MiB leaves room for that worst case plus transport metadata.
	maxSSELineSize   = 8 << 20
	maxReconnectWait = 30 * time.Second
)

// SSEEvent is one decoded Server-Sent Event. Data remains application-owned.
type SSEEvent struct {
	ID    string
	Event string
	Data  []byte
}

// StreamSSE consumes an SSE endpoint and reconnects unexpected closures after
// the first successful connection. The initial cursor is sent as ?after=; a
// reconnect carries the last completely committed event ID in Last-Event-ID.
func (c *Client) StreamSSE(ctx context.Context, endpoint, cursor string, handle func(SSEEvent) error) error {
	connected := false
	reconnectWait := c.reconnectWait
	for {
		requestEndpoint, err := streamEndpoint(endpoint, cursor, connected)
		if err != nil {
			return err
		}
		request, err := c.NewRequest(ctx, http.MethodGet, requestEndpoint, nil)
		if err != nil {
			return err
		}
		request.Header.Set("Accept", "text/event-stream")
		if connected && cursor != "" {
			request.Header.Set("Last-Event-ID", cursor)
		}
		response, err := c.Do(request)
		if err != nil {
			var problem *ProblemError
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !connected {
				return err
			}
			wait := reconnectWait
			if errors.As(err, &problem) {
				if !retryableHTTPStatus(problem.Problem.Status) {
					return err
				}
				if hinted, ok := parseRetryAfter(problem.retryAfter, time.Now()); ok {
					wait = hinted
				}
			}
			if err := waitForReconnect(ctx, boundedReconnectWait(wait)); err != nil {
				return err
			}
			continue
		}
		if response.StatusCode == http.StatusNoContent {
			response.Body.Close()
			return nil
		}
		mediaType, _, parseErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
		if parseErr != nil || response.StatusCode != http.StatusOK || mediaType != "text/event-stream" {
			response.Body.Close()
			return fmt.Errorf("control plane returned %s with Content-Type %q, want 200 text/event-stream", response.Status, response.Header.Get("Content-Type"))
		}
		connected = true
		streamRetry := time.Duration(-1)
		cursor, streamRetry, err = consumeSSE(response.Body, cursor, handle)
		response.Body.Close()
		if streamRetry >= 0 {
			reconnectWait = streamRetry
		}
		var callback *handlerError
		if errors.As(err, &callback) {
			return callback.err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := waitForReconnect(ctx, boundedReconnectWait(reconnectWait)); err != nil {
			return err
		}
	}
}

func retryableHTTPStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500 && status <= 599
}

func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	if seconds, err := strconv.ParseUint(strings.TrimSpace(value), 10, 31); err == nil {
		return time.Duration(seconds) * time.Second, true
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	if delay := when.Sub(now); delay > 0 {
		return delay, true
	}
	return 0, true
}

func boundedReconnectWait(delay time.Duration) time.Duration {
	if delay < 0 {
		return 0
	}
	if delay > maxReconnectWait {
		return maxReconnectWait
	}
	return delay
}

func streamEndpoint(endpoint, cursor string, reconnect bool) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("parse SSE endpoint: %w", err)
	}
	query := parsed.Query()
	query.Del("after")
	if !reconnect && cursor != "" {
		query.Set("after", cursor)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func waitForReconnect(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type handlerError struct{ err error }

func (e *handlerError) Error() string { return e.err.Error() }
func (e *handlerError) Unwrap() error { return e.err }

func consumeSSE(reader io.Reader, cursor string, handle func(SSEEvent) error) (string, time.Duration, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), maxSSELineSize)
	var event SSEEvent
	var data []string
	var blockID string
	var hasBlockID bool
	retry := time.Duration(-1)
	dispatch := func() error {
		nextCursor := cursor
		if hasBlockID {
			nextCursor = blockID
		}
		if len(data) != 0 {
			event.ID = nextCursor
			if event.Event == "" {
				event.Event = "message"
			}
			event.Data = []byte(strings.Join(data, "\n"))
			if err := handle(event); err != nil {
				return &handlerError{err: err}
			}
		}
		cursor = nextCursor
		event = SSEEvent{}
		data = nil
		blockID = ""
		hasBlockID = false
		return nil
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := dispatch(); err != nil {
				return cursor, retry, err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "id":
			if !strings.ContainsRune(value, '\x00') {
				blockID = value
				hasBlockID = true
			}
		case "event":
			event.Event = value
		case "data":
			data = append(data, value)
		case "retry":
			milliseconds, err := strconv.ParseUint(value, 10, 31)
			if err == nil {
				retry = time.Duration(milliseconds) * time.Millisecond
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return cursor, retry, err
	}
	// Only blank-line-terminated blocks commit. An interrupted final block is
	// replayed from the prior cursor after reconnect.
	return cursor, retry, io.EOF
}
