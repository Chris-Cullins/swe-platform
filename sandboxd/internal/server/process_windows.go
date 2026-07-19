//go:build windows

package server

import (
	"os"
	"os/exec"
)

// The portable API exposes no Windows process identifiers or signal values.
// Environment backends additionally fence the whole execution domain on
// pause/replacement; this fallback controls the direct process during a live
// Windows sandboxd epoch.
func configureManagedProcess(*exec.Cmd) {}

func interruptManagedProcess(cmd *exec.Cmd) error {
	return cmd.Process.Signal(os.Interrupt)
}

func killManagedProcess(cmd *exec.Cmd) error {
	return cmd.Process.Kill()
}
