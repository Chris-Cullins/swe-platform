package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Chris-Cullins/swe-platform/sandboxd/changes"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

type ChangesServer struct {
	sandboxdv1.UnimplementedChangesServiceServer
	Workspace    string
	mu           sync.Mutex
	rootIdentity os.FileInfo
	gitIdentity  os.FileInfo
}

var changesCaptureSlots = make(chan struct{}, 2)

func (s *ChangesServer) Snapshot(ctx context.Context, request *sandboxdv1.ChangesSnapshotRequest) (*sandboxdv1.ChangesSnapshotResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	snapshot := changes.Snapshot{State: "unavailable"}
	select {
	case changesCaptureSlots <- struct{}{}:
		result := make(chan changes.Snapshot, 1)
		go func() { defer func() { <-changesCaptureSlots }(); result <- s.capture(ctx, request.BaselinePaths) }()
		select {
		case snapshot = <-result:
		case <-ctx.Done():
		}
	default:
	}
	// A raced special-file open may be uninterruptible on some backends. The
	// worker retains its slot until it exits; RPC deadline and memory/concurrency
	// remain bounded rather than spawning unbounded abandoned readers.
	data, err := json.Marshal(snapshot)
	if len(data) > changes.MaxEncodedBytes {
		data = []byte(`{"state":"unavailable"}`)
	}
	return &sandboxdv1.ChangesSnapshotResponse{SnapshotJson: data}, err
}

type boundedGitOutput struct{ bytes.Buffer }

func (b *boundedGitOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 1<<20 {
		return 0, errors.New("repository path limit")
	}
	return b.Buffer.Write(p)
}

func (s *ChangesServer) capture(ctx context.Context, baselinePaths []string) changes.Snapshot {
	unavailable := changes.Snapshot{State: "unavailable"}
	if len(baselinePaths) > changes.MaxFiles || len(strings.Join(baselinePaths, "\x00")) > 1<<20 {
		return unavailable
	}
	root, err := os.OpenRoot(s.Workspace)
	if err != nil {
		return unavailable
	}
	defer root.Close()
	initial, err := root.Stat(".")
	if err != nil {
		return unavailable
	}
	// Worktrees with external .git indirection and nested repositories are not
	// followed. All content reads use a pinned portable confinement handle.
	gitInfo, err := root.Lstat(".git")
	if err != nil || !gitInfo.IsDir() {
		return unavailable
	}
	s.mu.Lock()
	if s.rootIdentity == nil {
		s.rootIdentity = initial
		s.gitIdentity = gitInfo
	}
	identityCurrent := os.SameFile(s.rootIdentity, initial) && os.SameFile(s.gitIdentity, gitInfo)
	s.mu.Unlock()
	if !identityCurrent {
		return unavailable
	}
	list := func() ([]string, error) {
		// CSI may grant workspace access via fsGroup rather than owner UID.
		// Trust only this configured root for this read-only command, as the
		// project-hook contract does; never write global configuration or use *.
		cmd := exec.CommandContext(ctx, "git", "-c", "safe.directory="+s.Workspace, "-c", "core.fsmonitor=false", "-c", "core.untrackedCache=false", "ls-files", "--cached", "--others", "--exclude-standard", "-z")
		cmd.Dir = s.Workspace
		// Discard all ambient Git command/config injection and credentials.
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "SystemRoot=" + os.Getenv("SystemRoot"), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=" + os.DevNull, "GIT_OPTIONAL_LOCKS=0", "GIT_TERMINAL_PROMPT=0"}
		var output boundedGitOutput
		cmd.Stdout = &output
		cmd.Stderr = io.Discard
		if err := cmd.Run(); err != nil {
			return nil, err
		}
		paths := strings.Split(strings.TrimSuffix(output.String(), "\x00"), "\x00")
		if len(paths) == 1 && paths[0] == "" {
			paths = nil
		}
		paths = append(paths, baselinePaths...)
		sort.Strings(paths)
		unique := paths[:0]
		for _, p := range paths {
			if !utf8.ValidString(p) || len(p) > 4096 || !filepath.IsLocal(p) || path.Clean(p) != p || p == "." || strings.ContainsAny(p, "\\\x00") || p == ".git" || strings.HasPrefix(p, ".git/") {
				return nil, errors.New("unsupported path")
			}
			if len(unique) == 0 || unique[len(unique)-1] != p {
				unique = append(unique, p)
			}
		}
		if len(unique) > changes.MaxFiles {
			return nil, errors.New("too many files")
		}
		return unique, nil
	}
	paths, err := list()
	if err != nil {
		return unavailable
	}
	result := changes.Snapshot{State: "available"}
	total := 0
	observed := make(map[string]os.FileInfo)
	for _, p := range paths {
		if ctx.Err() != nil {
			return unavailable
		}
		info, err := root.Lstat(p)
		if errors.Is(err, os.ErrNotExist) {
			observed[p] = nil
			continue
		} // tracked deletion
		f := changes.File{Path: p, State: "unavailable"}
		if err != nil {
			result.Files = append(result.Files, f)
			continue
		}
		f.Mode = uint32(info.Mode())
		observed[p] = info
		if !info.Mode().IsRegular() {
			result.Files = append(result.Files, f)
			continue
		}
		if info.Size() > changes.MaxFileBytes || total+int(info.Size()) > changes.MaxSnapshotBytes {
			f.State = "oversized"
			result.Files = append(result.Files, f)
			continue
		}
		file, err := root.Open(p)
		if err != nil {
			result.Files = append(result.Files, f)
			continue
		}
		opened, statErr := file.Stat()
		// A raced replacement, symlink, or special file never becomes source data.
		if statErr != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
			file.Close()
			return unavailable
		}
		data, readErr := io.ReadAll(io.LimitReader(file, changes.MaxFileBytes+1))
		after, statErr := file.Stat()
		file.Close()
		current, pathErr := root.Lstat(p)
		if readErr != nil || statErr != nil || pathErr != nil || !os.SameFile(info, current) || info.Size() != after.Size() || !info.ModTime().Equal(after.ModTime()) {
			return unavailable
		}
		if len(data) > changes.MaxFileBytes || total+len(data) > changes.MaxSnapshotBytes {
			f.State = "oversized"
		} else {
			f.State = changes.ContentState(data)
			f.Data = data
			total += len(data)
		}
		result.Files = append(result.Files, f)
	}
	finalPaths, err := list()
	if err != nil || strings.Join(paths, "\x00") != strings.Join(finalPaths, "\x00") {
		return unavailable
	}
	for p, info := range observed {
		current, err := root.Lstat(p)
		if info == nil {
			if !errors.Is(err, os.ErrNotExist) {
				return unavailable
			}
			continue
		}
		if err != nil || !os.SameFile(info, current) || info.Size() != current.Size() || info.Mode() != current.Mode() || !info.ModTime().Equal(current.ModTime()) {
			return unavailable
		}
	}
	final, err := os.Stat(s.Workspace)
	if err != nil || !os.SameFile(initial, final) {
		return unavailable
	}
	finalGit, err := root.Lstat(".git")
	if err != nil || !os.SameFile(gitInfo, finalGit) {
		return unavailable
	}
	return result
}
