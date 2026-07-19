//go:build console

package ui

import (
	"io/fs"
	"testing"
)

func TestEmbeddedAssetsContainViteEntryPoint(t *testing.T) {
	entry, err := fs.ReadFile(Assets(), "index.html")
	if err != nil {
		t.Fatal(err)
	}
	if len(entry) == 0 {
		t.Fatal("embedded index.html is empty")
	}
	matches, err := fs.Glob(Assets(), "assets/*")
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Fatal("embedded Vite asset directory is empty")
	}
}
