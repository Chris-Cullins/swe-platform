// Package changes defines bounded, observation-only workspace snapshots. It
// neither reads adapter transcripts nor writes Git objects, refs, or the index.
package changes

import (
	"bytes"
	"fmt"
	"path"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	MaxFiles         = 4096
	MaxFileBytes     = 256 << 10
	MaxSnapshotBytes = 16 << 20
	MaxEncodedBytes  = 24 << 20
	MaxDiffBytes     = 512 << 10
	PageSize         = 50
)

type File struct {
	Path  string `json:"path"`
	State string `json:"state"` // text, binary, oversized, unavailable
	Mode  uint32 `json:"mode"`
	Data  []byte `json:"data,omitempty"`
}

type Snapshot struct {
	State string `json:"state"` // available or unavailable; never partially clean
	Files []File `json:"files,omitempty"`
}

type Change struct {
	Path  string `json:"path"`
	Kind  string `json:"kind"` // added, modified, deleted
	State string `json:"state"`
	Diff  string `json:"diff,omitempty"`
}

// Compare reports whole-workspace differences against an immutable pre-start
// snapshot. Renames deliberately appear as delete/add, not inferred attribution.
func Compare(base, current Snapshot) (string, []Change) {
	if base.State != "available" || current.State != "available" {
		return "unavailable", nil
	}
	a, b := make(map[string]File), make(map[string]File)
	for _, f := range base.Files {
		a[f.Path] = f
	}
	for _, f := range current.Files {
		b[f.Path] = f
	}
	paths := make([]string, 0, len(a)+len(b))
	for p := range a {
		paths = append(paths, p)
	}
	for p := range b {
		if _, ok := a[p]; !ok {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	result := make([]Change, 0)
	for _, p := range paths {
		old, oldOK := a[p]
		next, nextOK := b[p]
		// Unsupported files cannot establish equality even if metadata matches.
		if oldOK && nextOK && old.State != "oversized" && old.State != "unavailable" && old.State == next.State && old.Mode == next.Mode && bytes.Equal(old.Data, next.Data) {
			continue
		}
		c := Change{Path: p, Kind: "modified", State: "text"}
		if !oldOK {
			c.Kind = "added"
		}
		if !nextOK {
			c.Kind = "deleted"
		}
		for _, state := range []string{"binary", "oversized", "unavailable"} {
			if old.State == state || next.State == state {
				c.State = state
			}
		}
		if c.State == "text" {
			c.Diff = unified(p, old.Data, next.Data)
			if old.Mode != next.Mode {
				c.Diff = fmt.Sprintf("mode %06o → %06o\n", old.Mode, next.Mode) + c.Diff
			}
			if len(c.Diff) > MaxDiffBytes {
				c.Diff = ""
				c.State = "oversized"
			}
		}
		result = append(result, c)
	}
	if len(result) == 0 {
		return "clean", result
	}
	return "changed", result
}

// A single bounded hunk trims common prefix/suffix while retaining three context
// lines. This is linear rather than quadratic in adversarial source text.
func unified(path string, old, next []byte) string {
	lines := func(b []byte) []string {
		if len(b) == 0 {
			return nil
		}
		return strings.SplitAfter(string(b), "\n")
	}
	a, b := lines(old), lines(next)
	if len(a) > 0 && a[len(a)-1] == "" {
		a = a[:len(a)-1]
	}
	if len(b) > 0 && b[len(b)-1] == "" {
		b = b[:len(b)-1]
	}
	prefix := 0
	for prefix < len(a) && prefix < len(b) && a[prefix] == b[prefix] {
		prefix++
	}
	suffix := 0
	for suffix < len(a)-prefix && suffix < len(b)-prefix && a[len(a)-1-suffix] == b[len(b)-1-suffix] {
		suffix++
	}
	start := max(0, prefix-3)
	endA := min(len(a), len(a)-suffix+3)
	endB := min(len(b), len(b)-suffix+3)
	var out strings.Builder
	fmt.Fprintf(&out, "--- %q\n+++ %q\n@@ -%d,%d +%d,%d @@\n", "a/"+path, "b/"+path, min(start+1, endA), endA-start, min(start+1, endB), endB-start)
	write := func(marker byte, line string) {
		out.WriteByte(marker)
		out.WriteString(line)
		if !strings.HasSuffix(line, "\n") {
			out.WriteString("\n\\ No newline at end of file\n")
		}
	}
	for _, line := range a[start:prefix] {
		write(' ', line)
	}
	for _, line := range a[prefix : len(a)-suffix] {
		write('-', line)
	}
	for _, line := range b[prefix : len(b)-suffix] {
		write('+', line)
	}
	for _, line := range a[len(a)-suffix : endA] {
		write(' ', line)
	}
	return out.String()
}

func ContentState(data []byte) string {
	if !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0 {
		return "binary"
	}
	return "text"
}

func (s Snapshot) Validate() error {
	if s.State == "unavailable" && len(s.Files) == 0 {
		return nil
	}
	if s.State != "available" || len(s.Files) > MaxFiles {
		return fmt.Errorf("invalid snapshot")
	}
	total, previous := 0, ""
	for _, f := range s.Files {
		if f.Path <= previous || len(f.Path) > 4096 || !utf8.ValidString(f.Path) || path.Clean(f.Path) != f.Path || strings.HasPrefix(f.Path, "/") || f.Path == ".." || strings.HasPrefix(f.Path, "../") || strings.ContainsAny(f.Path, "\\\x00") || f.Path == ".git" || strings.HasPrefix(f.Path, ".git/") {
			return fmt.Errorf("invalid snapshot path")
		}
		previous = f.Path
		total += len(f.Data)
		if len(f.Data) > MaxFileBytes || total > MaxSnapshotBytes {
			return fmt.Errorf("snapshot exceeds content limit")
		}
		switch f.State {
		case "text", "binary":
			if ContentState(f.Data) != f.State {
				return fmt.Errorf("invalid snapshot content")
			}
		case "oversized", "unavailable":
			if len(f.Data) != 0 {
				return fmt.Errorf("unsupported snapshot content")
			}
		default:
			return fmt.Errorf("invalid snapshot file state")
		}
	}
	return nil
}
