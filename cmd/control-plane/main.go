package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
)

const shutdownTimeout = 10 * time.Second

func main() {
	address := flag.String("listen-address", ":8080", "Address for the control-plane HTTP API")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		log.Error("register Kubernetes API types", "error", err)
		os.Exit(1)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		log.Error("register API types", "error", err)
		os.Exit(1)
	}
	restConfig := config.GetConfigOrDie()
	kubeClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		log.Error("create Kubernetes client", "error", err)
		os.Exit(1)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.Error("create Kubernetes authentication client", "error", err)
		os.Exit(1)
	}
	bootstrapToken := os.Getenv("SWE_BOOTSTRAP_TOKEN")
	if bootstrapToken != "" && len(bootstrapToken) < 32 {
		log.Error("SWE_BOOTSTRAP_TOKEN must contain at least 32 characters")
		os.Exit(1)
	}
	access := controlplane.KubernetesAccessController{
		Client:         clientset,
		BootstrapToken: bootstrapToken,
		Audience:       os.Getenv("SWE_TOKEN_AUDIENCE"),
	}
	resources := &controlplane.KubernetesResourceService{Client: kubeClient}
	transcripts := controlplane.NewMemoryTranscriptStore(controlplane.DefaultMemoryTranscriptStoreOptions())
	streamLifecycle, cancelStreams := context.WithCancel(context.Background())
	defer cancelStreams()
	server := &http.Server{
		Addr: *address,
		Handler: controlplane.NewServer(log, controlplane.ServerOptions{
			Access:                access,
			Sessions:              access,
			Resources:             resources,
			Runs:                  controlplane.KubernetesRunResolver{Client: kubeClient},
			TranscriptStore:       transcripts,
			TerminalDialer:        controlplane.KubernetesTerminalDialer{Client: kubeClient},
			TrustProxy:            strings.EqualFold(os.Getenv("SWE_TRUST_PROXY_HEADERS"), "true"),
			AllowInsecureSessions: strings.EqualFold(os.Getenv("SWE_ALLOW_INSECURE_SESSIONS"), "true"),
			StreamLifecycle:       streamLifecycle,
		}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		log.Error("listen for control-plane API", "address", *address, "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Info("starting control-plane API", "address", *address)
	if err := runHTTPServer(ctx, log, server, listener, shutdownTimeout, cancelStreams); err != nil {
		log.Error("control-plane API stopped", "error", err)
		os.Exit(1)
	}
}

func runHTTPServer(ctx context.Context, log *slog.Logger, server *http.Server, listener net.Listener, drainTimeout time.Duration, cancelStreams context.CancelFunc) error {
	tracker := &handlerTracker{}
	tracker.track(server)

	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- server.Serve(listener)
	}()

	select {
	case err := <-serveErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		log.Info("shutting down control-plane API", "drainTimeout", drainTimeout)
	}

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), drainTimeout)
	defer cancelShutdown()
	cancelStreams()
	shutdownErr := server.Shutdown(shutdownContext)
	if shutdownErr != nil {
		_ = server.Close()
	}
	serveErr := <-serveErrors
	drainErr := tracker.wait(shutdownContext)
	if shutdownErr != nil {
		return fmt.Errorf("drain control-plane API: %w", shutdownErr)
	}
	if drainErr != nil {
		_ = server.Close()
		return fmt.Errorf("drain control-plane API handlers: %w", drainErr)
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return nil
}

type handlerTracker struct {
	active sync.WaitGroup
}

func (t *handlerTracker) track(server *http.Server) {
	handler := server.Handler
	if handler == nil {
		handler = http.DefaultServeMux
	}
	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.active.Add(1)
		defer t.active.Done()
		handler.ServeHTTP(w, r)
	})
}

func (t *handlerTracker) wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		t.active.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
