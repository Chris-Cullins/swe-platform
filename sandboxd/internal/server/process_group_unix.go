//go:build !windows && !linux

package server

import (
	"errors"
	"syscall"
)

func processGroupRunning(pgid int) bool {
	err := syscall.Kill(-pgid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
