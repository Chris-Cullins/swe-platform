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

	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
)

func (c Client) CaptureChanges(ctx context.Context, namespace, name, uid string, input controlplane.CaptureChangesRequest) error {
	token, err := os.ReadFile(c.TokenFile)
	if err != nil {
		return fmt.Errorf("read changes credential: %w", err)
	}
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	endpoint := strings.TrimRight(c.BaseURL, "/") + "/api/v1/namespaces/" + url.PathEscape(namespace) + "/runs/" + url.PathEscape(name) + "/changes"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(controlplane.RunUIDHeader, uid)
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("changes capture returned HTTP %d", response.StatusCode)
	}
	return nil
}
