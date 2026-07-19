// Package ui exposes the production operations-console assets to the control plane.
package ui

import "io/fs"

// Assets returns the embedded production console, or nil in ordinary Go builds.
func Assets() fs.FS {
	return assets
}
