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

	"github.com/prometheus/client_golang/prometheus"
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
	metricsAddress := flag.String("metrics-bind-address", "127.0.0.1:8082", "Address for the internal Prometheus metrics endpoint")
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
	transcripts, sessions, closeStores, err := controlPlaneStoresFromEnvironment(context.Background(), log)
	if err != nil {
		log.Error("initialize control-plane stores", "error", err)
		os.Exit(1)
	}
	defer closeStores()
	metricsRegistry := prometheus.NewRegistry()
	metricsRegistry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	metrics := controlplane.NewMetrics(metricsRegistry)
	access := controlplane.KubernetesAccessController{
		Client:         clientset,
		BootstrapToken: bootstrapToken,
		Audience:       os.Getenv("SWE_TOKEN_AUDIENCE"),
		Sessions:       sessions,
		Metrics:        metrics,
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
	streamLifecycle, cancelStreams := context.WithCancel(context.Background())
	defer cancelStreams()
	apiServer := &http.Server{
		Addr: *address,
		Handler: controlplane.NewServer(log, controlplane.ServerOptions{
			Access:                resourceAccess,
			Sessions:              access,
			Resources:             resources,
			Runs:                  controlplane.KubernetesRunResolver{Client: kubeClient},
			TranscriptStore:       transcripts,
			TerminalDialer:        controlplane.KubernetesTerminalDialer{Client: kubeClient, Metrics: metrics},
			ConsoleAssets:         consoleui.Assets(),
			TrustProxy:            strings.EqualFold(os.Getenv("SWE_TRUST_PROXY_HEADERS"), "true"),
			AllowInsecureSessions: strings.EqualFold(os.Getenv("SWE_ALLOW_INSECURE_SESSIONS"), "true"),
			StreamLifecycle:       streamLifecycle,
			Metrics:               metrics,
		}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	metricsServer := &http.Server{
		Addr:              *metricsAddress,
		Handler:           controlplane.MetricsHandler(metricsRegistry),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	apiListener, err := net.Listen("tcp", *address)
	if err != nil {
		log.Error("listen for control-plane API", "address", *address, "error", err)
		os.Exit(1)
	}
	metricsListener, err := net.Listen("tcp", *metricsAddress)
	if err != nil {
		_ = apiListener.Close()
		log.Error("listen for control-plane metrics", "address", *metricsAddress, "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Info("starting control-plane", "apiAddress", *address, "metricsAddress", *metricsAddress)
	if err := runHTTPServers(ctx, log, []httpServerListener{
		{name: "API", server: apiServer, listener: apiListener, cancelStreams: cancelStreams},
		{name: "metrics", server: metricsServer, listener: metricsListener},
	}, shutdownTimeout); err != nil {
		log.Error("control-plane stopped", "error", err)
		os.Exit(1)
	}
}

type httpServerListener struct {
	name          string
	server        *http.Server
	listener      net.Listener
	cancelStreams context.CancelFunc
}

func runHTTPServers(ctx context.Context, log *slog.Logger, servers []httpServerListener, drainTimeout time.Duration) error {
	serveContext, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		name string
		err  error
	}
	results := make(chan result, len(servers))
	for _, configured := range servers {
		configured := configured
		cancelStreams := configured.cancelStreams
		if cancelStreams == nil {
			cancelStreams = func() {}
		}
		go func() {
			results <- result{configured.name, runHTTPServer(serveContext, log, configured.server, configured.listener, drainTimeout, cancelStreams)}
		}()
	}
	first := <-results
	cancel()
	var firstErr error
	if first.err != nil {
		firstErr = fmt.Errorf("%s server: %w", first.name, first.err)
	}
	for range len(servers) - 1 {
		completed := <-results
		if firstErr == nil && completed.err != nil {
			firstErr = fmt.Errorf("%s server: %w", completed.name, completed.err)
		}
	}
	return firstErr
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

func controlPlaneStoresFromEnvironment(ctx context.Context, log *slog.Logger) (controlplane.TranscriptStore, controlplane.SessionStore, func(), error) {
	backend := strings.TrimSpace(os.Getenv("SWE_SESSION_BACKEND"))
	if backend != "memory" && backend != "postgres" {
		return nil, nil, nil, errors.New("SWE_SESSION_BACKEND must be memory or postgres")
	}
	databaseURL := strings.TrimSpace(os.Getenv("SWE_POSTGRES_URL"))
	keyringFile := strings.TrimSpace(os.Getenv("SWE_SESSION_KEYRING_FILE"))
	if backend == "postgres" && databaseURL == "" {
		return nil, nil, nil, errors.New("SWE_POSTGRES_URL is required when SWE_SESSION_BACKEND=postgres")
	}
	if backend == "postgres" && keyringFile == "" {
		return nil, nil, nil, errors.New("SWE_SESSION_KEYRING_FILE is required when SWE_SESSION_BACKEND=postgres")
	}
	if backend == "memory" && keyringFile != "" {
		return nil, nil, nil, errors.New("SWE_SESSION_KEYRING_FILE must be empty when SWE_SESSION_BACKEND=memory")
	}
	transcriptOptions, err := postgresTranscriptOptionsFromEnvironment()
	if err != nil {
		return nil, nil, nil, err
	}

	var database *controlplane.PostgresDatabase
	if databaseURL != "" {
		database, err = controlplane.NewPostgresDatabase(ctx, databaseURL)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	closeStores := func() {
		if database != nil {
			database.Close()
		}
	}
	fail := func(err error) (controlplane.TranscriptStore, controlplane.SessionStore, func(), error) {
		closeStores()
		return nil, nil, nil, err
	}

	var transcripts controlplane.TranscriptStore
	if database == nil {
		log.Warn("using development-only process-local transcript store; set SWE_POSTGRES_URL for durable transcripts")
		transcripts = controlplane.NewMemoryTranscriptStore(controlplane.DefaultMemoryTranscriptStoreOptions())
	} else {
		transcripts, err = controlplane.NewPostgresTranscriptStoreWithDatabase(ctx, database, transcriptOptions)
		if err != nil {
			return fail(err)
		}
		log.Info("using durable PostgreSQL transcript store")
	}

	var sessions controlplane.SessionStore
	if backend == "memory" {
		sessions = controlplane.NewMemorySessionStore(controlplane.MemorySessionStoreOptions{})
		log.Warn("using development-only process-local browser session store")
	} else {
		keyring, err := controlplane.LoadSessionKeyring(keyringFile)
		if err != nil {
			return fail(err)
		}
		sessions, err = controlplane.NewPostgresSessionStore(ctx, database, keyring, controlplane.PostgresSessionStoreOptions{})
		if err != nil {
			return fail(err)
		}
		log.Info("using durable PostgreSQL browser session store")
	}
	return transcripts, sessions, closeStores, nil
}

func postgresTranscriptOptionsFromEnvironment() (controlplane.PostgresTranscriptStoreOptions, error) {
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
			return options, fmt.Errorf("%s must be a positive integer", value.name)
		}
		*value.target = parsed
	}
	if raw := strings.TrimSpace(os.Getenv("SWE_TRANSCRIPT_POLL_INTERVAL")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			return options, errors.New("SWE_TRANSCRIPT_POLL_INTERVAL must be a positive duration")
		}
		options.PollInterval = parsed
	}
	return options, nil
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
