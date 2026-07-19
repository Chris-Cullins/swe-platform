//go:build windows

package server

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

func TestProcessDomainAssignsLeaderToJob(t *testing.T) {
	cmd := exec.Command("cmd", "/c", "ping -n 30 127.0.0.1 >nul")
	nul, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nul.Close() })
	cmd.Stdin, cmd.Stdout, cmd.Stderr = nul, nul, nul
	domain := newProcessDomain(cmd)
	if err := domain.start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = domain.force()
		_ = domain.wait()
		_ = domain.close()
	})
	// The Job-list startup attribute makes membership part of CreateProcess.
	if domain.job == 0 {
		t.Fatal("successfully started process has no retained job")
	}
}

func TestProcessServiceForceContainsDescendant(t *testing.T) {
	if level := os.Getenv("SANDBOXD_WINDOWS_TREE_HELPER"); level != "" {
		runWindowsTreeHelper(level)
		return
	}
	server := NewProcessServer(t.TempDir())
	t.Cleanup(server.Close)
	marker := filepath.Join(t.TempDir(), "descendant-survived")
	ready := filepath.Join(t.TempDir(), "descendant-ready")
	key := &sandboxdv1.ProcessKey{OwnerId: "windows", Role: "tree"}
	// The child writes only after the parent has had time to be force-stopped.
	p, err := server.Start(context.Background(), &sandboxdv1.StartProcessRequest{
		Key: key, Spec: &sandboxdv1.ProcessSpec{
			Argv: []string{os.Args[0], "-test.run=^TestProcessServiceForceContainsDescendant$", "--", ready, marker},
			Env:  map[string]string{"SANDBOXD_WINDOWS_TREE_HELPER": "parent"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.State != sandboxdv1.ProcessState_PROCESS_STATE_RUNNING {
		t.Fatalf("start state = %s, error = %q", p.State, p.Error)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("descendant did not report ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := server.Stop(context.Background(), &sandboxdv1.StopProcessRequest{Key: key, Mode: sandboxdv1.StopMode_STOP_MODE_FORCE}); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	domain := server.processes[processKey{ownerID: key.OwnerId, role: key.Role}].domain
	server.mu.Unlock()
	domain.mu.Lock()
	active := uint32(0)
	var queryErr error
	if domain.job != 0 {
		active, queryErr = domain.activeProcessesLocked()
	}
	domain.mu.Unlock()
	if queryErr != nil {
		t.Fatalf("query job after force: %v", queryErr)
	}
	if active != 0 {
		t.Fatalf("force returned with %d active Job processes", active)
	}
	time.Sleep(4 * time.Second)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("descendant escaped force containment: stat error = %v", err)
	}
}

func runWindowsTreeHelper(level string) {
	args := os.Args
	for len(args) > 0 && args[0] != "--" {
		args = args[1:]
	}
	if len(args) != 3 {
		os.Exit(2)
	}
	ready, marker := args[1], args[2]
	if level == "parent" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestProcessServiceForceContainsDescendant$", "--", ready, marker)
		cmd.Env = append(os.Environ(), "SANDBOXD_WINDOWS_TREE_HELPER=descendant")
		if err := cmd.Start(); err != nil {
			os.Exit(3)
		}
		_ = cmd.Wait()
		return
	}
	if level != "descendant" {
		os.Exit(4)
	}
	if err := os.WriteFile(ready, []byte("ready"), 0o600); err != nil {
		os.Exit(5)
	}
	time.Sleep(3 * time.Second)
	if err := os.WriteFile(marker, []byte("survived"), 0o600); err != nil {
		os.Exit(6)
	}
	time.Sleep(30 * time.Second)
}
