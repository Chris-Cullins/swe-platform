//go:build windows

package server

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPlatformTerminalBackendRejectsMissingConPTY(t *testing.T) {
	backend := newPlatformTerminalBackend(t.TempDir())
	_, err := backend.Open(context.Background(), 80, 24)
	if !errors.Is(err, errTerminalUnavailable) {
		t.Fatalf("open error = %v, want terminal unavailable", err)
	}
	if !strings.Contains(err.Error(), "ConPTY") {
		t.Fatalf("open error = %q, want explicit ConPTY requirement", err)
	}
}
