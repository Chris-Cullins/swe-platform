package server

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Chris-Cullins/swe-platform/sandboxd/changes"
)

func TestChangesDirtyBaselineAndReadOnlyGit(t *testing.T) {
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		data, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git: %v %s", err, data)
		}
		return string(data)
	}
	write := func(path, data string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, path), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	git("init")
	write("tracked", "original\n")
	git("add", "tracked")
	write("tracked", "preexisting dirt\n")
	write("untracked", "already here\n")
	index, err := os.ReadFile(filepath.Join(dir, ".git", "index"))
	if err != nil {
		t.Fatal(err)
	}
	s := &ChangesServer{Workspace: dir}
	base := s.capture(context.Background(), nil)
	if err := base.Validate(); err != nil || base.State != "available" {
		t.Fatalf("base: %+v %v", base, err)
	}
	if state, files := changes.Compare(base, s.capture(context.Background(), nil)); state != "clean" || len(files) != 0 {
		t.Fatalf("preexisting dirt attributed: %s %+v", state, files)
	}
	write("tracked", "agent edit\n")
	write("untracked", "agent untracked edit\n")
	write(".gitignore", "untracked\n")
	write("binary", "\x00\x01")
	write("large", strings.Repeat("x", changes.MaxFileBytes+1))
	current := s.capture(context.Background(), []string{"tracked", "untracked"})
	state, files := changes.Compare(base, current)
	if state != "changed" || len(files) != 5 {
		t.Fatalf("changes: %s %+v", state, files)
	}
	byPath := map[string]changes.Change{}
	for _, f := range files {
		byPath[f.Path] = f
	}
	if !strings.Contains(byPath["tracked"].Diff, "-preexisting dirt") || strings.Contains(byPath["tracked"].Diff, "original") || byPath["untracked"].Kind != "modified" || byPath["binary"].State != "binary" || byPath["large"].State != "oversized" {
		t.Fatalf("wrong diff: %+v", byPath)
	}
	after, err := os.ReadFile(filepath.Join(dir, ".git", "index"))
	if err != nil || string(index) != string(after) {
		t.Fatal("snapshot mutated Git index")
	}
	if _, err := os.Stat(filepath.Join(dir, ".git", "index.lock")); !os.IsNotExist(err) {
		t.Fatal("snapshot left index lock")
	}
	if err := os.Remove(filepath.Join(dir, "tracked")); err != nil {
		t.Fatal(err)
	}
	_, files = changes.Compare(base, s.capture(context.Background(), []string{"tracked", "untracked"}))
	found := false
	for _, f := range files {
		if f.Path == "tracked" && f.Kind == "deleted" {
			found = true
		}
	}
	if !found {
		t.Fatal("tracked deletion missing")
	}
}

func TestChangesMissingReplacedAndUnsafeWorkspace(t *testing.T) {
	dir := t.TempDir()
	s := &ChangesServer{Workspace: dir}
	if s.capture(context.Background(), nil).State != "unavailable" {
		t.Fatal("non-repository reported clean")
	}
	cmd := exec.Command("git", "init", dir)
	if data, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git init: %s %v", data, err)
	}
	if s.capture(context.Background(), nil).State != "available" {
		t.Fatal("empty repository unavailable")
	}
	for _, path := range []string{"../secret", ".git/config", "/etc/passwd", "a\x00b"} {
		if s.capture(context.Background(), []string{path}).State != "unavailable" {
			t.Fatalf("unsafe path %q accepted", path)
		}
	}
	if err := os.Rename(filepath.Join(dir, ".git"), filepath.Join(dir, "old-git")); err != nil {
		t.Fatal(err)
	}
	cmd = exec.Command("git", "init", dir)
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	if s.capture(context.Background(), nil).State != "unavailable" {
		t.Fatal("replaced repository reported available")
	}
}
