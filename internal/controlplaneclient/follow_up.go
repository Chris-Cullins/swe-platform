package controlplaneclient

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
)

// InspectFollowUp performs one bounded, exact read. It never retries or resolves
// a replacement identity, and grants no execution or reservation.
func (c *Client) InspectFollowUp(ctx context.Context, namespace, name, uid string) (controlplane.FollowUpInspection, error) {
	var result controlplane.FollowUpInspection
	if strings.TrimSpace(namespace) == "" || strings.TrimSpace(name) == "" || strings.TrimSpace(uid) == "" || len(uid) > 128 {
		return result, fmt.Errorf("explicit namespace, Run name and exact Run UID (at most 128 bytes) are required")
	}
	request, err := c.NewRequest(ctx, http.MethodGet, c.Endpoint("api", "v1", "namespaces", namespace, "runs", name, "follow-up"), nil)
	if err != nil {
		return result, err
	}
	request.Header.Set(controlplane.RunUIDHeader, uid)
	request.Header.Set("Accept", "application/json")
	response, err := c.Do(request)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	if err := decodeJSON(response.Body, &result, 16<<10); err != nil {
		return controlplane.FollowUpInspection{}, err
	}
	if result.Namespace != namespace || result.NamespaceUID == "" || result.SourceName != name || result.SourceUID != uid || result.Reason == "" {
		return controlplane.FollowUpInspection{}, fmt.Errorf("inspection returned inconsistent identity or missing classification")
	}
	return result, nil
}
