//go:build !windows

package server

import (
	"errors"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

var errInterruptUnsupported = errors.New("interrupt unsupported")

// Each launched command owns a private process group.
type processDomain struct {
	cmd      *exec.Cmd
	mu       sync.Mutex
	terminal bool
	closed   bool
	forced   bool
	closing  chan struct{}
	closeErr error
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
	if d.cmd.Process == nil || d.terminal || d.closed || d.closing != nil {
		return nil
	}
	return syscall.Kill(-d.cmd.Process.Pid, sig)
}

func (d *processDomain) interrupt() error { return d.signal(syscall.SIGINT) }
func (d *processDomain) terminate() error { return d.signal(syscall.SIGTERM) }
func (d *processDomain) force() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cmd.Process == nil || d.terminal || d.closed || d.forced || d.closing != nil {
		return nil
	}
	err := syscall.Kill(-d.cmd.Process.Pid, syscall.SIGKILL)
	if err == nil || errors.Is(err, syscall.ESRCH) {
		d.forced = true
		return nil
	}
	return err
}

// close does not publish a terminal domain until SIGKILL has taken effect for
// every running member. Signal delivery alone is asynchronous.
func (d *processDomain) close() error {
	forceErr := d.force()
	d.mu.Lock()
	if d.closed {
		err := d.closeErr
		d.mu.Unlock()
		return err
	}
	if d.closing != nil {
		closing := d.closing
		d.mu.Unlock()
		<-closing
		d.mu.Lock()
		err := d.closeErr
		d.mu.Unlock()
		return err
	}
	d.closing = make(chan struct{})
	closing := d.closing
	pid := 0
	if d.cmd.Process != nil {
		pid = d.cmd.Process.Pid
	}
	d.mu.Unlock()

	if pid != 0 {
		for processGroupRunning(pid) {
			time.Sleep(time.Millisecond)
		}
	}

	d.mu.Lock()
	d.terminal = true
	d.closed = true
	d.closeErr = forceErr
	close(closing)
	d.closing = nil
	d.mu.Unlock()
	return forceErr
}
