//go:build !windows

package server

import "testing"

func TestPlatformTerminalBackendUsesTmux(t *testing.T) {
	if _, ok := newPlatformTerminalBackend(t.TempDir()).(*tmuxTerminalBackend); !ok {
		t.Fatal("non-Windows terminal backend is not tmux")
	}
}
