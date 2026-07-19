package controlplane

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

const testConsoleIndex = `<!doctype html><title>SWE Operations</title><div id="root"></div>`

func TestConsoleServesEntryPointFallbackAndAssets(t *testing.T) {
	handler := NewServer(nil, ServerOptions{ConsoleAssets: testConsoleAssets()}).Handler()
	for _, test := range []struct {
		name        string
		requestPath string
		wantBody    string
		wantCache   string
		wantType    string
	}{
		{name: "entry point", requestPath: "/", wantBody: testConsoleIndex, wantCache: "no-store", wantType: "text/html; charset=utf-8"},
		{name: "direct entry point", requestPath: "/index.html", wantBody: testConsoleIndex, wantCache: "no-store", wantType: "text/html; charset=utf-8"},
		{name: "client route fallback", requestPath: "/namespaces/default/runs/run-1/overview", wantBody: testConsoleIndex, wantCache: "no-store", wantType: "text/html; charset=utf-8"},
		{name: "hashed Vite asset", requestPath: "/assets/index-AbCdEf12.js", wantBody: "console.log('ok')", wantCache: "public, max-age=31536000, immutable", wantType: "text/javascript; charset=utf-8"},
		{name: "other asset", requestPath: "/favicon.svg", wantBody: "<svg></svg>", wantCache: "no-cache", wantType: "image/svg+xml"},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := requestConsole(handler, http.MethodGet, test.requestPath)
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
			}
			if body := response.Body.String(); body != test.wantBody {
				t.Fatalf("body = %q, want %q", body, test.wantBody)
			}
			if cache := response.Header().Get("Cache-Control"); cache != test.wantCache {
				t.Fatalf("Cache-Control = %q, want %q", cache, test.wantCache)
			}
			if contentType := response.Header().Get("Content-Type"); contentType != test.wantType {
				t.Fatalf("Content-Type = %q, want %q", contentType, test.wantType)
			}
			assertConsoleSecurityHeaders(t, response.Header())
		})
	}
}

func TestConsoleFallbackDoesNotSwallowServerRoutesOrMethods(t *testing.T) {
	handler := NewServer(nil, ServerOptions{ConsoleAssets: testConsoleAssets()}).Handler()
	for _, test := range []struct {
		name        string
		method      string
		requestPath string
		wantStatus  int
	}{
		{name: "API root", method: http.MethodGet, requestPath: "/api", wantStatus: http.StatusNotFound},
		{name: "unknown API", method: http.MethodGet, requestPath: "/api/v2/unknown", wantStatus: http.StatusNotFound},
		{name: "existing API handler error", method: http.MethodGet, requestPath: "/api/v1/namespaces/default/unknown", wantStatus: http.StatusNotFound},
		{name: "health subtree", method: http.MethodGet, requestPath: "/healthz/unknown", wantStatus: http.StatusNotFound},
		{name: "health method error", method: http.MethodPost, requestPath: "/healthz", wantStatus: http.StatusMethodNotAllowed},
		{name: "unsupported SPA method", method: http.MethodPost, requestPath: "/namespaces/default/runs", wantStatus: http.StatusMethodNotAllowed},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := requestConsole(handler, test.method, test.requestPath)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, test.wantStatus)
			}
			if strings.Contains(response.Body.String(), testConsoleIndex) {
				t.Fatalf("server route returned the SPA entry point: %s", response.Body.String())
			}
			if response.Header().Get("Content-Security-Policy") != "" {
				t.Fatal("non-UI response received UI security headers")
			}
		})
	}
}

func TestConsoleRejectsMissingAssetPaths(t *testing.T) {
	handler := NewServer(nil, ServerOptions{ConsoleAssets: testConsoleAssets()}).Handler()
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		for _, requestPath := range []string{"/assets/missing.js", "/manifest.webmanifest"} {
			response := requestConsole(handler, method, requestPath)
			if response.Code != http.StatusNotFound {
				t.Fatalf("%s %s status = %d, want %d", method, requestPath, response.Code, http.StatusNotFound)
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("%s %s Cache-Control = %q, want no-store", method, requestPath, response.Header().Get("Cache-Control"))
			}
			if strings.Contains(response.Body.String(), testConsoleIndex) {
				t.Fatalf("%s %s exposed the SPA entry point", method, requestPath)
			}
		}
	}
}

func TestConsoleRejectsDirectoriesAndTraversal(t *testing.T) {
	handler := NewServer(nil, ServerOptions{ConsoleAssets: testConsoleAssets()}).Handler()
	for _, requestPath := range []string{"/assets", "/assets/%2e%2e/index.html", "/assets/..%5cindex.html"} {
		response := requestConsole(handler, http.MethodGet, requestPath)
		if response.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want %d", requestPath, response.Code, http.StatusNotFound)
		}
	}
}

func TestConsoleSupportsHeadWithoutBody(t *testing.T) {
	handler := NewServer(nil, ServerOptions{ConsoleAssets: testConsoleAssets()}).Handler()
	for _, requestPath := range []string{"/", "/index.html", "/namespaces/default/runs/run-1/overview"} {
		response := requestConsole(handler, http.MethodHead, requestPath)
		if response.Code != http.StatusOK || response.Body.Len() != 0 {
			t.Fatalf("HEAD %s status/body = %d/%q, want 200 with an empty body", requestPath, response.Code, response.Body.String())
		}
		if response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("HEAD %s Cache-Control = %q, want no-store", requestPath, response.Header().Get("Cache-Control"))
		}
	}
}

func testConsoleAssets() fs.FS {
	return fstest.MapFS{
		"index.html":               {Data: []byte(testConsoleIndex)},
		"assets/index-AbCdEf12.js": {Data: []byte("console.log('ok')")},
		"favicon.svg":              {Data: []byte("<svg></svg>")},
	}
}

func requestConsole(handler http.Handler, method, requestPath string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, requestPath, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertConsoleSecurityHeaders(t *testing.T, header http.Header) {
	t.Helper()
	want := map[string]string{
		"Cross-Origin-Resource-Policy": "same-origin",
		"Referrer-Policy":              "no-referrer",
		"X-Content-Type-Options":       "nosniff",
		"X-Frame-Options":              "DENY",
	}
	for name, value := range want {
		if got := header.Get(name); got != value {
			t.Errorf("%s = %q, want %q", name, got, value)
		}
	}
	csp := header.Get("Content-Security-Policy")
	for _, directive := range []string{"default-src 'none'", "script-src 'self'", "style-src 'self' 'unsafe-inline'", "connect-src 'self'", "frame-ancestors 'none'"} {
		if !strings.Contains(csp, directive) {
			t.Errorf("Content-Security-Policy %q does not contain %q", csp, directive)
		}
	}
	if strings.Contains(csp, " ws:") || strings.Contains(csp, " wss:") {
		t.Errorf("Content-Security-Policy permits WebSockets to arbitrary hosts: %q", csp)
	}
	if strings.Contains(csp, "script-src 'self' 'unsafe-inline'") || strings.Contains(csp, "'unsafe-eval'") {
		t.Errorf("Content-Security-Policy is weaker than the Vite output requires: %q", csp)
	}
}
