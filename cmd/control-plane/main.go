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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
	consoleui "github.com/Chris-Cullins/swe-platform/ui"
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
	kubeClient, err := client.NewWithWatch(restConfig, client.Options{Scheme: scheme})
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
	sessions := controlplane.NewMemorySessionStore(controlplane.MemorySessionStoreOptions{})
	access := controlplane.KubernetesAccessController{
		Client:         clientset,
		BootstrapToken: bootstrapToken,
		Audience:       os.Getenv("SWE_TOKEN_AUDIENCE"),
		Sessions:       sessions,
	}
	mode, err := tenancy.ParseMode(os.Getenv("SWE_TENANCY_MODE"))
	if err != nil {
		log.Error("invalid tenancy configuration", "error", err)
		os.Exit(1)
	}
	installationKey := types.NamespacedName{
		Namespace: strings.TrimSpace(os.Getenv("SWE_INSTALLATION_NAMESPACE")),
		Name:      strings.TrimSpace(os.Getenv("SWE_INSTALLATION_NAME")),
	}
	identity, _, err := tenancy.LoadInstallation(context.Background(), kubeClient, installationKey)
	if err != nil {
		log.Error("load installation identity", "error", err)
		os.Exit(1)
	}
	verifier := &tenancy.Verifier{Reader: kubeClient, Installation: identity, Mode: mode}
	configuredNamespaces, err := parseTenancyNamespaces(os.Getenv("SWE_TENANCY_NAMESPACES"))
	if err != nil {
		log.Error("parse configured Project namespaces", "error", err)
		os.Exit(1)
	}
	if mode == tenancy.ModeTrustedAdmin && len(configuredNamespaces) != 0 {
		log.Error("SWE_TENANCY_NAMESPACES must be empty in trusted-admin mode")
		os.Exit(1)
	}
	namespaceList := make([]string, 0, len(configuredNamespaces))
	for namespace := range configuredNamespaces {
		namespaceList = append(namespaceList, namespace)
	}
	if err := verifier.ValidateConfiguredNamespaces(context.Background(), namespaceList); err != nil {
		log.Error("validate configured Project namespaces", "error", err)
		os.Exit(1)
	}
	resourceAccess := controlplane.TenancyAccessController{Access: access, Verifier: verifier, Namespaces: configuredNamespaces}
	resources := &controlplane.KubernetesResourceService{Client: kubeClient}
	transcripts, closeTranscripts, err := transcriptStoreFromEnvironment(context.Background(), log)
	if err != nil {
		log.Error("initialize transcript store", "error", err)
		os.Exit(1)
	}
	defer closeTranscripts()
	streamLifecycle, cancelStreams := context.WithCancel(context.Background())
	defer cancelStreams()
	server := &http.Server{
		Addr: *address,
		Handler: controlplane.NewServer(log, controlplane.ServerOptions{
			Access:                resourceAccess,
			Sessions:              access,
			Resources:             resources,
			Runs:                  controlplane.KubernetesRunResolver{Client: kubeClient},
			TranscriptStore:       transcripts,
			TerminalDialer:        controlplane.KubernetesTerminalDialer{Client: kubeClient},
			ConsoleAssets:         consoleui.Assets(),
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

func parseTenancyNamespaces(value string) (map[string]struct{}, error) {
	namespaces := make(map[string]struct{})
	if strings.TrimSpace(value) == "" {
		return namespaces, nil
	}
	for _, raw := range strings.Split(value, ",") {
		namespace := strings.TrimSpace(raw)
		if namespace == "" {
			return nil, errors.New("SWE_TENANCY_NAMESPACES contains a blank namespace")
		}
		if _, duplicate := namespaces[namespace]; duplicate {
			return nil, fmt.Errorf("SWE_TENANCY_NAMESPACES contains duplicate namespace %q", namespace)
		}
		namespaces[namespace] = struct{}{}
	}
	return namespaces, nil
}

func transcriptStoreFromEnvironment(ctx context.Context, log *slog.Logger) (controlplane.TranscriptStore, func(), error) {
	databaseURL := strings.TrimSpace(os.Getenv("SWE_POSTGRES_URL"))
	if databaseURL == "" {
		log.Warn("using development-only process-local transcript store; set SWE_POSTGRES_URL for durable transcripts")
		return controlplane.NewMemoryTranscriptStore(controlplane.DefaultMemoryTranscriptStoreOptions()), func() {}, nil
	}
	options := controlplane.DefaultPostgresTranscriptStoreOptions()
	values := []struct {
		name   string
		target *int
	}{
		{"SWE_TRANSCRIPT_MAX_EVENTS_PER_RUN", &options.MaxEventsPerRun},
		{"SWE_TRANSCRIPT_MAX_BYTES_PER_RUN", &options.MaxBytesPerRun},
		{"SWE_TRANSCRIPT_MAX_REPLAY_EVENTS", &options.MaxReplayEvents},
		{"SWE_TRANSCRIPT_MAX_SUBSCRIBERS", &options.MaxSubscribers},
		{"SWE_TRANSCRIPT_SUBSCRIBER_BUFFER", &options.SubscriberBuffer},
	}
	for _, value := range values {
		raw := strings.TrimSpace(os.Getenv(value.name))
		if raw == "" {
			continue
		}
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return nil, nil, fmt.Errorf("%s must be a positive integer", value.name)
		}
		*value.target = parsed
	}
	if raw := strings.TrimSpace(os.Getenv("SWE_TRANSCRIPT_POLL_INTERVAL")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			return nil, nil, errors.New("SWE_TRANSCRIPT_POLL_INTERVAL must be a positive duration")
		}
		options.PollInterval = parsed
	}
	store, err := controlplane.NewPostgresTranscriptStore(ctx, databaseURL, options)
	if err != nil {
		return nil, nil, err
	}
	log.Info("using durable PostgreSQL transcript store")
	return store, store.Close, nil
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
