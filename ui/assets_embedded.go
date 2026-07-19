//go:build console

package ui

import (
	"embed"
	"io/fs"
)

// Both patterns are intentional: a tagged build fails at compile time when
// either the Vite entry point or its generated asset directory is absent.
//
//go:embed dist/index.html dist/assets
var embeddedAssets embed.FS

var assets = mustSub(embeddedAssets, "dist")

func mustSub(root fs.FS, dir string) fs.FS {
	assets, err := fs.Sub(root, dir)
	if err != nil {
		panic(err)
	}
	return assets
}
