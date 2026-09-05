//go:build !windows

package server

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestChangesGroupWritableWorkspaceOwnership(t *testing.T) {
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if out, err := exec.Command(git, "init", dir).CombinedOutput(); err != nil {
		t.Fatalf("init: %v %s", err, out)
	}
	if err := os.WriteFile(filepath.Join(dir, "dirty"), []byte("preexisting\n"), 0600); err != nil {
		t.Fatal(err)
	}
	// Git's test switch reproduces the different-owner check without requiring
	// chown/root in tests. CSI workspaces are group-writable, not necessarily
	// owned by the sandboxd user. The wrapper affects only this test's Git child.
	bin := t.TempDir()
	wrapper := fmt.Sprintf("#!/bin/sh\nGIT_TEST_ASSUME_DIFFERENT_OWNER=1 exec %q \"$@\"\n", git)
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(wrapper), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	s := &ChangesServer{Workspace: dir}
	got := s.capture(context.Background(), nil)
	if got.State != "available" || len(got.Files) != 1 || string(got.Files[0].Data) != "preexisting\n" {
		t.Fatalf("group-writable workspace unavailable: %+v", got)
	}
}
