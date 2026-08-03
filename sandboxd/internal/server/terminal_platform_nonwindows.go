//go:build !windows

package server

func newPlatformTerminalBackend(workspace string) terminalBackend {
	return newTmuxTerminalBackend(workspace, defaultTmuxSocket, nil)
}
