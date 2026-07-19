//go:build !windows

package server

import (
	"errors"
	"os/exec"
	"sync"
	"syscall"
)

var errInterruptUnsupported = errors.New("interrupt unsupported")

// Each launched command owns a private process group.
type processDomain struct {
	cmd      *exec.Cmd
	mu       sync.Mutex
	terminal bool
	closed   bool
}

func newProcessDomain(cmd *exec.Cmd) *processDomain {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return &processDomain{cmd: cmd}
}

func (d *processDomain) start() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return errors.New("process domain is closed")
	}
	return d.cmd.Start()
}
func (d *processDomain) wait() error {
	return d.cmd.Wait()
}

func (d *processDomain) signal(sig syscall.Signal) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cmd.Process == nil || d.terminal || d.closed {
		return nil
	}
	return syscall.Kill(-d.cmd.Process.Pid, sig)
}

func (d *processDomain) interrupt() error { return d.signal(syscall.SIGINT) }
func (d *processDomain) terminate() error { return d.signal(syscall.SIGTERM) }
func (d *processDomain) force() error     { return d.signal(syscall.SIGKILL) }
func (d *processDomain) close() error {
	d.mu.Lock()
	d.terminal = true
	d.closed = true
	d.mu.Unlock()
	return nil
}
