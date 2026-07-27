package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSetManagedProcessHelper(t *testing.T) {
	if os.Getenv("SANDBOXD_MANAGED_HELPER") == "" {
		return
	}
	marker := os.Getenv("MARKER")
	f, err := os.OpenFile(marker, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		os.Exit(91)
	}
	_, _ = f.WriteString(os.Getenv("VALUE") + ":" + strings.Join(os.Environ(), ",") + "\n")
	_ = f.Close()
	if os.Getenv("WAIT") != "" {
		select {}
	}
	if os.Getenv("FAIL") != "" {
		os.Exit(7)
	}
	os.Exit(0)
}

func managedRequest(owner string, rev uint64, role, marker, value string, extra map[string]string) *sandboxdv1.ReconcileManagedServicesRequest {
	env := map[string]string{"SANDBOXD_MANAGED_HELPER": "1", "MARKER": marker, "VALUE": value}
	for k, v := range extra {
		env[k] = v
	}
	services := []*sandboxdv1.ManagedServiceSpec{}
	if role != "" {
		services = append(services, &sandboxdv1.ManagedServiceSpec{Role: role, Spec: &sandboxdv1.ProcessSpec{Argv: []string{os.Args[0], "-test.run=TestSetManagedProcessHelper"}, EnvMode: sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_REPLACE, Env: env}})
	}
	return &sandboxdv1.ReconcileManagedServicesRequest{OwnerId: owner, IntentRevision: rev, Services: services}
}

func waitLines(t *testing.T, path string, n int) []string {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		data, _ := os.ReadFile(path)
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		if len(lines) >= n && lines[0] != "" {
			return lines
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d launches", n)
	return nil
}

func TestManagedServicesRestartSuccessFailureAndEnvironmentIsolation(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			s := NewProcessServer(t.TempDir())
			s.restartInitial = 5 * time.Millisecond
			s.restartMax = 10 * time.Millisecond
			marker := filepath.Join(t.TempDir(), "starts")
			extra := map[string]string{"ONLY_PUBLIC": "yes"}
			if fail {
				extra["FAIL"] = "1"
			}
			if _, err := s.ReconcileManagedServices(context.Background(), managedRequest("uid", 1, "svc", marker, "a", extra)); err != nil {
				t.Fatal(err)
			}
			lines := waitLines(t, marker, 2)
			if !strings.Contains(lines[0], "ONLY_PUBLIC=yes") || strings.Contains(lines[0], "PATH=") {
				t.Fatalf("replace environment leaked or omitted values: %q", lines[0])
			}
			s.Close()
		})
	}
}

func TestManagedServicesRevisionReplaceRemoveReaddAndClose(t *testing.T) {
	s := NewProcessServer(t.TempDir())
	s.restartInitial = 5 * time.Millisecond
	marker := filepath.Join(t.TempDir(), "starts")
	r1 := managedRequest("exact-uid", 1, "svc", marker, "old", nil)
	first, err := s.ReconcileManagedServices(context.Background(), r1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReconcileManagedServices(context.Background(), r1); err != nil {
		t.Fatalf("duplicate: %v", err)
	}
	changedSame := managedRequest("exact-uid", 1, "svc", marker, "different", nil)
	if _, err = s.ReconcileManagedServices(context.Background(), changedSame); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("same revision conflict = %v", err)
	}
	if _, err = s.ReconcileManagedServices(context.Background(), managedRequest("exact-uid", 0, "", marker, "", nil)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("zero revision = %v", err)
	}
	if _, err = s.ReconcileManagedServices(context.Background(), managedRequest("exact-uid", 2, "svc", marker, "new", nil)); err != nil {
		t.Fatal(err)
	}
	lines := waitLines(t, marker, 2)
	if !strings.HasPrefix(lines[1], "new:") {
		t.Fatalf("replacement did not use new spec: %v (first execution %s)", lines, first.Services[0].Process.ExecutionId)
	}
	if _, err = s.ReconcileManagedServices(context.Background(), managedRequest("exact-uid", 1, "", marker, "", nil)); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale revision = %v", err)
	}
	if _, err = s.ReconcileManagedServices(context.Background(), managedRequest("exact-uid", 3, "", marker, "", nil)); err != nil {
		t.Fatal(err)
	}
	waitLines(t, marker, 2)
	time.Sleep(40 * time.Millisecond) // allow an already-admitted execution to finish stopping
	data, _ := os.ReadFile(marker)
	before := len(strings.Split(strings.TrimSpace(string(data)), "\n"))
	time.Sleep(40 * time.Millisecond)
	data, _ = os.ReadFile(marker)
	if got := len(strings.Split(strings.TrimSpace(string(data)), "\n")); got != before {
		t.Fatalf("removed service restarted: %d -> %d", before, got)
	}
	if _, err = s.ReconcileManagedServices(context.Background(), managedRequest("exact-uid", 4, "svc", marker, "again", nil)); err != nil {
		t.Fatal(err)
	}
	waitLines(t, marker, before+1)
	s.Close()
	data, _ = os.ReadFile(marker)
	before = len(strings.Split(strings.TrimSpace(string(data)), "\n"))
	time.Sleep(40 * time.Millisecond)
	data, _ = os.ReadFile(marker)
	if got := len(strings.Split(strings.TrimSpace(string(data)), "\n")); got != before {
		t.Fatalf("close did not suppress restart: %d -> %d", before, got)
	}
}
