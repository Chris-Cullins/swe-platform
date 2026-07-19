//go:build !windows

package server

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

	time.Sleep(300 * time.Millisecond)
	process, err := server.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	if process.State != sandboxdv1.ProcessState_PROCESS_STATE_STOPPING {
		t.Fatalf("state before grace deadline = %s, want STOPPING", process.State)
	}
	if !processAlive(childPID) {
		t.Fatalf("descendant %d was killed before grace deadline", childPID)
	}
	deadline := time.Now().Add(3 * time.Second)
	for processAlive(childPID) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if processAlive(childPID) {
		t.Fatalf("descendant %d survived graceful Stop force-kill", childPID)
	}
	waitFor(t, func() bool {
		p, _ := server.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
		return p != nil && p.State == sandboxdv1.ProcessState_PROCESS_STATE_EXITED
	})
}

func TestForceStopCannotBeDowngradedByGracefulRetry(t *testing.T) {
	server := NewProcessServer(t.TempDir())
	t.Cleanup(server.Close)
	key := &sandboxdv1.ProcessKey{OwnerId: "force-first", Role: "agent"}
	if _, err := server.Start(context.Background(), &sandboxdv1.StartProcessRequest{
		Key:  key,
		Spec: &sandboxdv1.ProcessSpec{Argv: []string{"/bin/sh", "-c", `trap '' INT TERM; while :; do sleep 1; done`}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Stop(context.Background(), &sandboxdv1.StopProcessRequest{Key: key, Mode: sandboxdv1.StopMode_STOP_MODE_FORCE}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Stop(context.Background(), &sandboxdv1.StopProcessRequest{
		Key: key, Mode: sandboxdv1.StopMode_STOP_MODE_GRACEFUL, GracePeriodMs: ^uint32(0),
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		process, _ := server.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
		return process != nil && process.State == sandboxdv1.ProcessState_PROCESS_STATE_EXITED
	})
	process, err := server.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	if process.Reason != sandboxdv1.TerminationReason_TERMINATION_REASON_FORCED {
		t.Fatalf("reason after graceful retry = %s, want FORCED", process.Reason)
	}
}

func TestCloseContextCancelsLongGracePeriod(t *testing.T) {
	workspace := t.TempDir()
	childPIDFile := filepath.Join(workspace, "child.pid")
	server := NewProcessServer(workspace)
	key := &sandboxdv1.ProcessKey{OwnerId: "shutdown-grace", Role: "agent"}
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
	if _, err := server.Stop(context.Background(), &sandboxdv1.StopProcessRequest{
		Key: key, Mode: sandboxdv1.StopMode_STOP_MODE_GRACEFUL, GracePeriodMs: ^uint32(0),
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		process, _ := server.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
		return process != nil && process.State == sandboxdv1.ProcessState_PROCESS_STATE_STOPPING
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := server.CloseContext(ctx); err != nil {
		t.Fatalf("CloseContext waited for graceful deadline: %v", err)
	}
	waitForProcessExit(t, childPID, "CloseContext left descendant in long grace period")
}

func execTree(t *testing.T, timeout uint64) (sandboxdv1.ExecService_ExecClient, int, context.CancelFunc) {
	t.Helper()
	workspace := t.TempDir()
	marker := filepath.Join(workspace, "exec-child.pid")
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := sandboxdv1.NewExecServiceClient(newConn(t, workspace)).Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	script := `(trap '' INT TERM; while :; do sleep 1; done) & echo $! > "$MARKER"; wait`
	err = stream.Send(&sandboxdv1.ExecRequest{Kind: &sandboxdv1.ExecRequest_Start{Start: &sandboxdv1.ExecStart{Argv: []string{"/bin/sh", "-c", script}, Env: map[string]string{"MARKER": marker}, TimeoutMs: timeout}}})
	if err != nil {
		t.Fatal(err)
	}
	return stream, waitForChildPID(t, marker), cancel
}

func TestExecCancellationKillsDescendants(t *testing.T) {
	_, pid, cancel := execTree(t, 0)
	cancel()
	waitForProcessExit(t, pid, "Exec cancellation left descendant")
}

func TestExecTimeoutKillsDescendantsAndReportsReason(t *testing.T) {
	stream, pid, cancel := execTree(t, 100)
	defer cancel()
	var exit *sandboxdv1.ExecExit
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if response.GetExit() != nil {
			exit = response.GetExit()
		}
	}
	if exit == nil || exit.Reason != sandboxdv1.TerminationReason_TERMINATION_REASON_TIMEOUT {
		t.Fatalf("exit=%v", exit)
	}
	waitForProcessExit(t, pid, "Exec timeout left descendant")
}

func TestExecControlsReportReasons(t *testing.T) {
	for _, tc := range []struct {
		name    string
		control sandboxdv1.ProcessControl
		reason  sandboxdv1.TerminationReason
	}{
		{"INTERRUPT", sandboxdv1.ProcessControl_PROCESS_CONTROL_INTERRUPT, sandboxdv1.TerminationReason_TERMINATION_REASON_INTERRUPTED},
		{"TERMINATE", sandboxdv1.ProcessControl_PROCESS_CONTROL_TERMINATE, sandboxdv1.TerminationReason_TERMINATION_REASON_TERMINATED},
		{"FORCE", sandboxdv1.ProcessControl_PROCESS_CONTROL_FORCE, sandboxdv1.TerminationReason_TERMINATION_REASON_FORCED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream, _, cancel := execTree(t, 0)
			defer cancel()
			if err := stream.Send(&sandboxdv1.ExecRequest{Kind: &sandboxdv1.ExecRequest_Control{Control: &sandboxdv1.ExecControl{Control: tc.control}}}); err != nil {
				t.Fatal(err)
			}
			for {
				response, err := stream.Recv()
				if err != nil {
					t.Fatal(err)
				}
				if exit := response.GetExit(); exit != nil {
					if exit.Reason != tc.reason {
						t.Fatalf("reason=%s", exit.Reason)
					}
					return
				}
			}
		})
	}
}

func TestProcessTimeoutKillsDescendantsAndRetainsReason(t *testing.T) {
	workspace := t.TempDir()
	marker := filepath.Join(workspace, "pid")
	s := NewProcessServer(workspace)
	t.Cleanup(s.Close)
	key := &sandboxdv1.ProcessKey{OwnerId: "timeout", Role: "agent"}
	_, err := s.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: key, Spec: &sandboxdv1.ProcessSpec{Argv: []string{"sh", "-c", `(while :; do sleep 1; done) & echo $! > "$MARKER"; wait`}, Env: map[string]string{"MARKER": marker}, TimeoutMs: 100}})
	if err != nil {
		t.Fatal(err)
	}
	pid := waitForChildPID(t, marker)
	waitForProcessExit(t, pid, "process timeout left descendant")
	waitFor(t, func() bool {
		p, _ := s.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
		return p != nil && p.State == sandboxdv1.ProcessState_PROCESS_STATE_EXITED
	})
	p, _ := s.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
	if p.Reason != sandboxdv1.TerminationReason_TERMINATION_REASON_TIMEOUT {
		t.Fatalf("reason=%s", p.Reason)
	}
}

func TestProcessControlsAndGracefulEscalation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mode   sandboxdv1.StopMode
		reason sandboxdv1.TerminationReason
	}{
		{"interrupt", sandboxdv1.StopMode_STOP_MODE_INTERRUPT, sandboxdv1.TerminationReason_TERMINATION_REASON_INTERRUPTED}, {"terminate", sandboxdv1.StopMode_STOP_MODE_TERMINATE, sandboxdv1.TerminationReason_TERMINATION_REASON_TERMINATED}, {"force", sandboxdv1.StopMode_STOP_MODE_FORCE, sandboxdv1.TerminationReason_TERMINATION_REASON_FORCED}, {"graceful-escalation", sandboxdv1.StopMode_STOP_MODE_GRACEFUL, sandboxdv1.TerminationReason_TERMINATION_REASON_INTERRUPTED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workspace := t.TempDir()
			s := NewProcessServer(workspace)
			t.Cleanup(s.Close)
			key := &sandboxdv1.ProcessKey{OwnerId: tc.name, Role: "agent"}
			_, err := s.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: key, Spec: &sandboxdv1.ProcessSpec{Argv: []string{"sh", "-c", `trap '' INT TERM; while :; do sleep 1; done`}}})
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.Stop(context.Background(), &sandboxdv1.StopProcessRequest{Key: key, Mode: tc.mode, GracePeriodMs: 50})
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool {
				p, _ := s.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
				return p != nil && p.State == sandboxdv1.ProcessState_PROCESS_STATE_EXITED
			})
			p, _ := s.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: key})
			if p.Reason != tc.reason {
				t.Fatalf("reason=%s", p.Reason)
			}
		})
	}
}

func TestSupervisorCloseFencesExecAndManaged(t *testing.T) {
	workspace := t.TempDir()
	sup := NewSupervisor()
	processes := NewProcessServer(workspace, sup)
	key := &sandboxdv1.ProcessKey{OwnerId: "managed", Role: "agent"}
	marker := filepath.Join(workspace, "managed.pid")
	_, err := processes.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: key, Spec: &sandboxdv1.ProcessSpec{Argv: []string{"sh", "-c", `(while :; do sleep 1; done) & echo $! > "$MARKER"; wait`}, Env: map[string]string{"MARKER": marker}}})
	if err != nil {
		t.Fatal(err)
	}
	pid := waitForChildPID(t, marker)
	execMarker := filepath.Join(workspace, "exec.pid")
	execStream := &blockedExecStream{ctx: context.Background(), release: make(chan struct{}), start: &sandboxdv1.ExecRequest{Kind: &sandboxdv1.ExecRequest_Start{Start: &sandboxdv1.ExecStart{Argv: []string{"sh", "-c", `(while :; do sleep 1; done) & echo $! > "$MARKER"; wait`}, Env: map[string]string{"MARKER": execMarker}}}}}
	close(execStream.release)
	execDone := make(chan error, 1)
	go func() { execDone <- NewExecServer(workspace, sup).Exec(execStream) }()
	execPID := waitForChildPID(t, execMarker)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := sup.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-execDone; err != nil {
		t.Fatal(err)
	}
	waitForProcessExit(t, pid, "supervisor Close left managed descendant")
	waitForProcessExit(t, execPID, "supervisor Close left Exec descendant")
	if _, err := processes.Start(context.Background(), &sandboxdv1.StartProcessRequest{Key: &sandboxdv1.ProcessKey{OwnerId: "fenced", Role: "agent"}, Spec: &sandboxdv1.ProcessSpec{Argv: []string{"true"}}}); status.Code(err) != codes.Unavailable {
		t.Fatalf("fence=%v", err)
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
echo $! > "$CHILD_PID_FILE"
sleep .2`
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
			if processAlive(childPID) {
				t.Fatal("terminal state published before descendant fence")
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
