package controlplane

import (
	"bytes"
	"errors"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"regexp"
	"strings"
	"time"
)

// xterm's DOM renderer creates generated style elements for dimensions, ANSI
// colors, and cursor state, so styles need unsafe-inline. Scripts remain
// self-only. The ws/wss sources keep same-origin terminals working in browsers
// that do not treat a scheme-changing WebSocket URL as 'self'; the terminal
// handler still enforces its existing same-origin policy before upgrading.
const consoleCSP = "default-src 'none'; base-uri 'none'; connect-src 'self' ws: wss:; font-src 'self' data:; form-action 'self'; frame-ancestors 'none'; img-src 'self' data:; manifest-src 'self'; object-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; worker-src 'none'"

var viteHashedAsset = regexp.MustCompile(`^assets/(?:.*/)?[^/]+-[A-Za-z0-9_-]{8,}\.[^/]+$`)

type consoleHandler struct {
	assets fs.FS
	index  []byte
	err    error
}

func newConsoleHandler(assets fs.FS) http.Handler {
	index, err := fs.ReadFile(assets, "index.html")
	return &consoleHandler{assets: assets, index: index, err: err}
}

func (h *consoleHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isReservedServerPath(r.URL.Path) {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setConsoleSecurityHeaders(w.Header())
	if h.err != nil {
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "operations console is unavailable", http.StatusInternalServerError)
		return
	}

	name := strings.TrimPrefix(r.URL.Path, "/")
	if name == "" {
		h.serve(w, r, "index.html", h.index, "no-store")
		return
	}
	if !fs.ValidPath(name) || strings.Contains(name, `\`) {
		w.Header().Set("Cache-Control", "no-store")
		http.NotFound(w, r)
		return
	}
	info, err := fs.Stat(h.assets, name)
	if err == nil && info.IsDir() {
		w.Header().Set("Cache-Control", "no-store")
		http.NotFound(w, r)
		return
	}
	if err == nil {
		data, readErr := fs.ReadFile(h.assets, name)
		if readErr != nil {
			w.Header().Set("Cache-Control", "no-store")
			http.Error(w, "operations console is unavailable", http.StatusInternalServerError)
			return
		}
		cacheControl := "no-cache"
		if viteHashedAsset.MatchString(name) {
			cacheControl = "public, max-age=31536000, immutable"
		}
		h.serve(w, r, name, data, cacheControl)
		return
	}
	if !errors.Is(err, fs.ErrNotExist) {
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "operations console is unavailable", http.StatusInternalServerError)
		return
	}
	if strings.HasPrefix(name, "assets/") {
		w.Header().Set("Cache-Control", "no-store")
		http.NotFound(w, r)
		return
	}
	h.serve(w, r, "index.html", h.index, "no-store")
}

func (h *consoleHandler) serve(w http.ResponseWriter, r *http.Request, name string, data []byte, cacheControl string) {
	contentType := mime.TypeByExtension(path.Ext(name))
	if contentType == "" {
		contentType = http.DetectContentType(data)
	}
	w.Header().Set("Cache-Control", cacheControl)
	w.Header().Set("Content-Type", contentType)
	http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(data))
}

func isReservedServerPath(requestPath string) bool {
	return requestPath == "/api" || strings.HasPrefix(requestPath, "/api/") ||
		requestPath == "/healthz" || strings.HasPrefix(requestPath, "/healthz/")
}

func setConsoleSecurityHeaders(header http.Header) {
	header.Set("Content-Security-Policy", consoleCSP)
	header.Set("Cross-Origin-Resource-Policy", "same-origin")
	header.Set("Permissions-Policy", "camera=(), display-capture=(), geolocation=(), microphone=(), payment=(), usb=()")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("X-Frame-Options", "DENY")
}
