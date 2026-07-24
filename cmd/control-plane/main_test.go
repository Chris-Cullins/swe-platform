package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestTranscriptStoreFromEnvironmentUsesMemoryFallback(t *testing.T) {
	t.Setenv("SWE_POSTGRES_URL", "")
	store, closeStore, err := transcriptStoreFromEnvironment(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer closeStore()
	if store == nil {
		t.Fatal("memory fallback returned a nil transcript store")
	}
}

func TestTranscriptStoreFromEnvironmentRejectsInvalidOptionsBeforeConnecting(t *testing.T) {
	t.Setenv("SWE_POSTGRES_URL", "postgres://unused.invalid/test")
	t.Setenv("SWE_TRANSCRIPT_MAX_EVENTS_PER_RUN", "zero")
	_, _, err := transcriptStoreFromEnvironment(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || err.Error() != "SWE_TRANSCRIPT_MAX_EVENTS_PER_RUN must be a positive integer" {
		t.Fatalf("configuration error = %v", err)
	}
}

func TestRunHTTPServerWaitsForHijackedHandlerCleanup(t *testing.T) {
	requestStarted := make(chan struct{})
	requestCanceled := make(chan struct{})
	releaseCleanup := make(chan struct{})
	cleanupFinished := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCleanup) }) }
	t.Cleanup(release)
	streamLifecycle, cancelStreams := context.WithCancel(context.Background())
	t.Cleanup(cancelStreams)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		defer close(cleanupFinished)
		close(requestStarted)
		<-streamLifecycle.Done()
		close(requestCanceled)
		<-releaseCleanup
	})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- runHTTPServer(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), server, listener, time.Second, cancelStreams)
	}()

	connection, _, err := websocket.DefaultDialer.Dial("ws://"+listener.Addr().String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}

	cancel()
	select {
	case <-requestCanceled:
	case <-time.After(time.Second):
		t.Fatal("active request was not canceled during shutdown")
	}
	select {
	case err := <-serveResult:
		t.Fatalf("HTTP server returned before hijacked handler cleanup: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case <-cleanupFinished:
	case <-time.After(time.Second):
		t.Fatal("hijacked handler cleanup did not finish")
	}
	select {
	case err := <-serveResult:
		if err != nil {
			t.Fatalf("runHTTPServer() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("HTTP server did not shut down")
	}
}

func TestRunHTTPServerDrainsOrdinaryRequestWithoutCancelingIt(t *testing.T) {
	requestStarted := make(chan struct{})
	requestContexts := make(chan context.Context, 1)
	releaseRequest := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRequest) }) }
	t.Cleanup(release)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		requestContexts <- r.Context()
		<-releaseRequest
		w.WriteHeader(http.StatusNoContent)
	})}
	baseListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := &closeNotifyListener{Listener: baseListener, closed: make(chan struct{})}
	t.Cleanup(func() { _ = listener.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- runHTTPServer(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), server, listener, time.Second, func() {})
	}()
	requestResult := make(chan error, 1)
	go func() {
		response, err := http.Get("http://" + listener.Addr().String())
		if err == nil {
			response.Body.Close()
		}
		requestResult <- err
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}
	requestContext := <-requestContexts

	cancel()
	select {
	case <-listener.closed:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not close the listener")
	}
	if err := requestContext.Err(); err != nil {
		t.Fatalf("ordinary request context canceled during graceful drain: %v", err)
	}
	release()
	select {
	case err := <-requestResult:
		if err != nil {
			t.Fatalf("ordinary request failed during graceful drain: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ordinary request did not complete")
	}
	select {
	case err := <-serveResult:
		if err != nil {
			t.Fatalf("runHTTPServer() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("HTTP server did not finish graceful drain")
	}
}

func TestRunHTTPServerUsesSingleDeadlineForStuckHandler(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRequest) }) }
	t.Cleanup(release)
	server := &http.Server{Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(requestStarted)
		<-releaseRequest
	})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	const timeout = 300 * time.Millisecond
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- runHTTPServer(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), server, listener, timeout, func() {})
	}()
	clientResult := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 2 * time.Second}
		response, err := client.Get("http://" + listener.Addr().String())
		if err == nil {
			response.Body.Close()
		}
		clientResult <- err
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}

	started := time.Now()
	cancel()
	select {
	case err = <-serveResult:
	case <-time.After(3 * timeout):
		t.Fatal("shutdown exceeded the bounded drain deadline")
	}
	elapsed := time.Since(started)
	release()
	if err == nil {
		t.Fatal("runHTTPServer() returned nil for stuck handler")
	}
	if elapsed < timeout*3/4 || elapsed > timeout*3/2 {
		t.Fatalf("shutdown elapsed = %v, want one %v deadline", elapsed, timeout)
	}
	select {
	case <-clientResult:
	case <-time.After(time.Second):
		t.Fatal("client did not exit after forced close")
	}
}

type closeNotifyListener struct {
	net.Listener
	closed chan struct{}
	once   sync.Once
}

func (l *closeNotifyListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return l.Listener.Close()
}
