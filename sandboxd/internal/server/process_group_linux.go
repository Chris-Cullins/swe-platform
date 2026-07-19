//go:build linux

package server

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
)

func processGroupRunning(pgid int) bool {
	// Signal zero includes zombies, which may persist when sandboxd is PID 1.
	// Linux procfs lets close wait for running members without waiting for reap.
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return processGroupExists(pgid)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		stat, err := os.ReadFile("/proc/" + entry.Name() + "/stat")
		if err != nil {
			continue
		}
		fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
		if len(fields) < 3 || fields[0] == "Z" || fields[0] == "X" {
			continue
		}
		group, err := strconv.Atoi(fields[2])
		if err == nil && group == pgid {
			return true
		}
	}
	return false
}

func processGroupExists(pgid int) bool {
	err := syscall.Kill(-pgid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
