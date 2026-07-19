//go:build !windows

package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

func TestGracefulStopKillsProcessGroupAfterLeaderExits(t *testing.T) {
	workspace := t.TempDir()
	childPIDFile := filepath.Join(workspace, "child.pid")
	server := NewProcessServer(workspace)
	t.Cleanup(server.Close)
	key := &sandboxdv1.ProcessKey{OwnerId: "run", Role: "agent"}
	script := `trap 'exit 0' INT
(trap '' INT; while :; do sleep 1; done) &
echo $! > "$CHILD_PID_FILE"
wait`
	if _, err := server.Start(context.Background(), &sandboxdv1.StartProcessRequest{
		Key:  key,
		Spec: &sandboxdv1.ProcessSpec{Argv: []string{"/bin/sh", "-c", script}, Env: map[string]string{"CHILD_PID_FILE": childPIDFile}},
	}); err != nil {
		t.Fatal(err)
	}
	childPID := waitForChildPID(t, childPIDFile)
	processGroup, err := syscall.Getpgid(childPID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(-processGroup, syscall.SIGKILL) })
	if _, err := server.Stop(context.Background(), &sandboxdv1.StopProcessRequest{
		Key: key, Mode: sandboxdv1.StopMode_STOP_MODE_GRACEFUL, GracePeriodMs: 800,
	}); err != nil {
		t.Fatal(err)
	}

	leaderExited := false
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		process, err := server.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
		if err != nil {
			t.Fatal(err)
		}
		if process.State == sandboxdv1.ProcessState_PROCESS_STATE_EXITED {
			leaderExited = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !leaderExited {
		t.Fatal("direct leader did not exit before the force-kill grace period")
	}
	if !processAlive(childPID) {
		t.Fatal("descendant exited before the force-kill grace period")
	}

	deadline = time.Now().Add(3 * time.Second)
	for processAlive(childPID) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if processAlive(childPID) {
		t.Fatalf("descendant %d survived graceful Stop force-kill", childPID)
	}
}

func TestForceStopKillsProcessGroupAfterNaturalLeaderExit(t *testing.T) {
	server, key, childPID := startExitedLeaderWithLiveDescendant(t)

	process, err := server.Stop(context.Background(), &sandboxdv1.StopProcessRequest{
		Key: key, Mode: sandboxdv1.StopMode_STOP_MODE_FORCE,
	})
	if err != nil {
		t.Fatal(err)
	}
	if process.State != sandboxdv1.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("Stop state = %s, want direct leader state EXITED", process.State)
	}
	waitForProcessExit(t, childPID, "descendant survived FORCE Stop after its leader exited")
}

func TestProcessServerCloseKillsProcessGroupAfterNaturalLeaderExit(t *testing.T) {
	server, key, childPID := startExitedLeaderWithLiveDescendant(t)

	server.Close()
	process, err := server.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	if process.State != sandboxdv1.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("state after Close = %s, want direct leader state EXITED", process.State)
	}
	waitForProcessExit(t, childPID, "descendant survived process supervisor Close after its leader exited")
}

func startExitedLeaderWithLiveDescendant(t *testing.T) (*ProcessServer, *sandboxdv1.ProcessKey, int) {
	t.Helper()
	workspace := t.TempDir()
	childPIDFile := filepath.Join(workspace, "child.pid")
	server := NewProcessServer(workspace)
	t.Cleanup(server.Close)
	key := &sandboxdv1.ProcessKey{OwnerId: "run", Role: "agent"}
	script := `(trap '' INT TERM; while :; do sleep 1; done) &
echo $! > "$CHILD_PID_FILE"`
	if _, err := server.Start(context.Background(), &sandboxdv1.StartProcessRequest{
		Key:  key,
		Spec: &sandboxdv1.ProcessSpec{Argv: []string{"/bin/sh", "-c", script}, Env: map[string]string{"CHILD_PID_FILE": childPIDFile}},
	}); err != nil {
		t.Fatal(err)
	}
	childPID := waitForChildPID(t, childPIDFile)
	processGroup, err := syscall.Getpgid(childPID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(-processGroup, syscall.SIGKILL) })

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		process, err := server.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
		if err != nil {
			t.Fatal(err)
		}
		if process.State == sandboxdv1.ProcessState_PROCESS_STATE_EXITED {
			if !processAlive(childPID) {
				t.Fatal("descendant exited with its leader")
			}
			return server, key, childPID
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("direct leader did not exit")
	return nil, nil, 0
}

func waitForProcessExit(t *testing.T, pid int, message string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for processAlive(pid) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if processAlive(pid) {
		t.Fatalf("%s: pid %d", message, pid)
	}
}

func waitForChildPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		contents, err := os.ReadFile(path)
		if err == nil {
			pid, err := strconv.Atoi(strings.TrimSpace(string(contents)))
			if err != nil {
				t.Fatal(err)
			}
			return pid
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("child PID was not written")
	return 0
}

func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	if errors.Is(err, syscall.ESRCH) {
		return false
	}
	// Treat a zombie as gone even if a container PID 1 has not reaped it yet.
	if contents, readErr := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat"); readErr == nil {
		fields := strings.Fields(string(contents))
		return len(fields) < 3 || fields[2] != "Z"
	}
	return err == nil || errors.Is(err, syscall.EPERM)
}
