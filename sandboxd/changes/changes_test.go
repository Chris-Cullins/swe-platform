package changes

import (
	"strings"
	"testing"
)

func TestCompareStatesAndBoundedDiff(t *testing.T) {
	for _, state := range []string{"binary", "oversized", "unavailable"} {
		f := File{Path: "file", State: state}
		if state == "binary" {
			f.Data = []byte{0}
		}
		_, result := Compare(Snapshot{State: "available"}, Snapshot{State: "available", Files: []File{f}})
		if len(result) != 1 || result[0].State != state || result[0].Diff != "" {
			t.Fatalf("%s: %+v", state, result)
		}
	}
	if state, _ := Compare(Snapshot{State: "unavailable"}, Snapshot{State: "available"}); state != "unavailable" {
		t.Fatal(state)
	}
	base := Snapshot{State: "available", Files: []File{{Path: "a", State: "text", Data: []byte("one\ntwo\nthree")}}}
	current := Snapshot{State: "available", Files: []File{{Path: "a", State: "text", Data: []byte("one\nchanged\nthree")}}}
	_, result := Compare(base, current)
	if !strings.Contains(result[0].Diff, "-two\n+changed\n") || !strings.Contains(result[0].Diff, "No newline at end of file") {
		t.Fatal(result)
	}
	base.Files[0].Data = []byte(strings.Repeat("\n", MaxFileBytes))
	current.Files[0].Data = []byte(strings.Repeat("x", MaxFileBytes))
	_, result = Compare(base, current)
	if result[0].State != "oversized" || result[0].Diff != "" {
		t.Fatal("diff not bounded")
	}
}
