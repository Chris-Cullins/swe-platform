package controlplaneclient

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// RotatingPortalResolver reads its projected service-account token for every
// discovery request so Kubernetes token rotation needs no process restart.
type RotatingPortalResolver struct {
	BaseURL   string
	TokenFile string
	HTTP      *http.Client
}

func (r RotatingPortalResolver) GetPortalRoute(ctx context.Context, namespace, environment, service string) (PortalRoute, error) {
	token, err := os.ReadFile(r.TokenFile)
	if err != nil {
		return PortalRoute{}, fmt.Errorf("read projected control-plane token: %w", err)
	}
	client, err := New(r.BaseURL, strings.TrimSpace(string(token)), r.HTTP)
	if err != nil {
		return PortalRoute{}, fmt.Errorf("configure control-plane portal client: %w", err)
	}
	route, err := client.GetPortalRoute(ctx, namespace, environment, service)
	if err != nil {
		return PortalRoute{}, fmt.Errorf("discover portal route: %w", err)
	}
	return route, nil
}
