package controlplaneclient

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
)

// GetRunChanges returns one bounded file-list page or one diff. Revision pins
// subsequent pages/file reads and rejects mixed observations while a Run works.
func (c *Client) GetRunChanges(ctx context.Context, namespace, name, uid string, offset int, revision int64, path string) (controlplane.RunChanges, error) {
	var result controlplane.RunChanges
	if strings.TrimSpace(uid) == "" || len(uid) > 128 {
		return result, fmt.Errorf("expected Run UID is required (at most 128 bytes)")
	}
	query := url.Values{}
	if offset > 0 {
		query.Set("offset", strconv.Itoa(offset))
	}
	if revision > 0 {
		query.Set("revision", strconv.FormatInt(revision, 10))
	}
	if path != "" {
		query.Set("path", path)
	}
	request, err := c.NewRequest(ctx, http.MethodGet, c.Endpoint("api", "v1", "namespaces", namespace, "runs", name, "changes")+"?"+query.Encode(), nil)
	if err != nil {
		return result, err
	}
	request.Header.Set(controlplane.RunUIDHeader, uid)
	response, err := c.Do(request)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	if err := decodeJSON(response.Body, &result, 4<<20); err != nil {
		return result, err
	}
	if result.RunUID != uid {
		return controlplane.RunChanges{}, ErrRunIdentityMismatch
	}
	if revision > 0 && result.Revision != revision {
		return controlplane.RunChanges{}, fmt.Errorf("changes revision mismatch")
	}
	return result, nil
}
