//go:build !windows

package server

import (
	"os"
	"os/exec"
	"syscall"
	"testing"
)

func TestProcessDomainCloseClaimsBeforeForce(t *testing.T) {
	domain := newProcessDomain(exec.Command("unused"))
	domain.cmd.Process = &os.Process{Pid: 1 << 30}
	originalSignal := signalProcessGroup
	t.Cleanup(func() { signalProcessGroup = originalSignal })

	closingClaimed := false
	signalProcessGroup = func(_ int, signal syscall.Signal) error {
		if signal != syscall.SIGKILL {
			t.Fatalf("teardown signal = %v, want SIGKILL", signal)
		}
		// forceLocked calls this while holding domain.mu, so this observes the
		// exact ordering without racing the production state.
		closingClaimed = domain.closing != nil
		return syscall.ESRCH
	}

	if err := domain.close(); err != nil {
		t.Fatal(err)
	}
	if !closingClaimed {
		t.Fatal("close issued SIGKILL before claiming teardown ownership")
	}
}
