//go:build !windows

package server

import (
	"os/exec"
	"syscall"
)

// Each managed command leads a process group so cancellation and daemon
// shutdown terminate descendants as well as the adapter's direct child.
func configureManagedProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func interruptManagedProcess(cmd *exec.Cmd) error {
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
}

func killManagedProcess(cmd *exec.Cmd) error {
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
