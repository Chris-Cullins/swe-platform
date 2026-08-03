//go:build windows

package server

import (
	"context"
	"fmt"
)

// windowsTerminalBackend fails closed until the shared-session and attachment
// semantics of TerminalService have a real ConPTY implementation. Falling back
// to tmux would make a Windows sandboxd binary depend on a Unix compatibility
// layer and falsely imply native terminal support.
type windowsTerminalBackend struct{}

func newPlatformTerminalBackend(string) terminalBackend {
	return windowsTerminalBackend{}
}

func (windowsTerminalBackend) Open(context.Context, uint32, uint32) (terminalSession, error) {
	return nil, fmt.Errorf("%w: native Windows terminal requires a ConPTY backend", errTerminalUnavailable)
}
