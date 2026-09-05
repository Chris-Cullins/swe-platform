package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	controllermetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/adapters/amp"
	"github.com/Chris-Cullins/swe-platform/internal/adapters/claudecode"
	"github.com/Chris-Cullins/swe-platform/internal/adapters/codex"
	"github.com/Chris-Cullins/swe-platform/internal/adapters/pi"
	"github.com/Chris-Cullins/swe-platform/internal/agent"
	"github.com/Chris-Cullins/swe-platform/internal/controllers"
	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
	"github.com/Chris-Cullins/swe-platform/internal/repositorycredential"
	"github.com/Chris-Cullins/swe-platform/internal/repositorycredential/githubapp"
	"github.com/Chris-Cullins/swe-platform/internal/sandboxclient"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
	"github.com/Chris-Cullins/swe-platform/internal/transcriptclient"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(platformv1alpha1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var probeAddr string
	var leaderElect bool
	var controlPlaneNamespace string
	var controlPlaneName string
	var controlPlaneInstance string
	var transcriptURL string
	var transcriptTokenFile string
	var githubAppClientID string
	var githubAppPrivateKeyFile string
	var tenancyModeValue string
	var installationNamespace string
	var installationName string
	var tenancyNamespaces stringListFlag
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&leaderElect, "leader-elect", false, "Enable leader election for the operator.")
	flag.StringVar(&controlPlaneNamespace, "control-plane-namespace", "", "Required system namespace containing the operator/control-plane pods allowed to reach sandboxd.")
	flag.StringVar(&controlPlaneName, "control-plane-name", "swe-platform", "app.kubernetes.io/name label of the control plane.")
	flag.StringVar(&controlPlaneInstance, "control-plane-instance", "swe-platform", "app.kubernetes.io/instance label of the control plane.")
	flag.StringVar(&transcriptURL, "transcript-url", "", "Control-plane base URL for adapter transcript events (disabled when empty).")
	flag.StringVar(&transcriptTokenFile, "transcript-token-file", "", "Projected service-account token used to append adapter transcript events.")
	flag.StringVar(&githubAppClientID, "github-app-client-id", "", "GitHub App client ID (requires private key file).")
	flag.StringVar(&githubAppPrivateKeyFile, "github-app-private-key-file", "", "GitHub App PEM private key file.")
	flag.StringVar(&tenancyModeValue, "tenancy-mode", "", "Required tenancy mode: scoped or trusted-admin.")
	flag.StringVar(&installationNamespace, "installation-namespace", "", "System namespace containing this installation's Installation object.")
	flag.StringVar(&installationName, "installation-name", "", "Name of this installation's Installation object.")
	flag.Var(&tenancyNamespaces, "tenancy-namespace", "Claimed Project namespace watched in scoped mode; repeat for each namespace.")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mode, err := tenancy.ParseMode(tenancyModeValue)
	if err != nil {
		setupLog.Error(err, "invalid tenancy configuration")
		os.Exit(1)
	}
	if installationNamespace == "" || installationName == "" {
		setupLog.Error(nil, "installation-namespace and installation-name are required")
		os.Exit(1)
	}
	if controlPlaneNamespace == "" {
		setupLog.Error(nil, "control-plane-namespace is required")
		os.Exit(1)
	}
	if mode == tenancy.ModeTrustedAdmin && len(tenancyNamespaces) != 0 {
		setupLog.Error(nil, "tenancy-namespace cannot be set in trusted-admin mode")
		os.Exit(1)
	}
	restConfig := ctrl.GetConfigOrDie()
	directClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		setupLog.Error(err, "create direct Kubernetes client")
		os.Exit(1)
	}
	identity, installation, err := tenancy.LoadInstallation(context.Background(), directClient, types.NamespacedName{Namespace: installationNamespace, Name: installationName})
	if err != nil {
		setupLog.Error(err, "load installation identity")
		os.Exit(1)
	}
	if err := tenancy.PrepareCatalogSources(context.Background(), directClient, installation); err != nil {
		setupLog.Error(err, "prepare installation catalog")
		os.Exit(1)
	}
	verifier := &tenancy.Verifier{Reader: directClient, Installation: identity, Mode: mode}
	if err := verifier.ValidateConfiguredNamespaces(context.Background(), tenancyNamespaces); err != nil {
		setupLog.Error(err, "validate configured Project namespaces")
		os.Exit(1)
	}
	cacheOptions := operatorCacheOptions(mode, tenancyNamespaces, installationNamespace)
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme: scheme,
		Cache:  cacheOptions,
		// Secret reads are exact-name lookups and deliberately bypass the shared
		// cache so the operator does not require cluster-wide list/watch access.
		Client: client.Options{Cache: &client.CacheOptions{
			DisableFor: []client.Object{&corev1.Secret{}},
		}},
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       installationLeaderElectionID(identity.UID),
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}
	verifier.Reader = mgr.GetAPIReader()
	scope := &tenancy.ReconcileScope{Verifier: verifier}
	guardedClient := tenancy.GuardedClient{Client: mgr.GetClient(), Verifier: verifier}
	if err := (&controllers.InstallationIsolationReconciler{
		Client:       mgr.GetClient(),
		APIReader:    mgr.GetAPIReader(),
		Installation: identity,
		Mode:         mode,
		Namespaces:   []string(tenancyNamespaces),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "InstallationIsolation")
		os.Exit(1)
	}
	processConnections := sandboxclient.NewProcessConnectionPool(mgr.GetAPIReader())
	if err := mgr.Add(processConnections); err != nil {
		setupLog.Error(err, "unable to register sandboxd process connection pool")
		os.Exit(1)
	}
	connector := sandboxclient.Connector{Reader: mgr.GetAPIReader(), ProcessPool: processConnections}
	adapters := registeredAdapters()
	operatorMetrics := controllers.NewOperatorMetrics(controllermetrics.Registry, adapters)
	if mode == tenancy.ModeScoped && len(tenancyNamespaces) == 0 {
		setupLog.Info("scoped installation has no configured Project namespaces; catalog is ready for onboarding and workload controllers are disabled")
	} else if err := (&controllers.EnvironmentReconciler{
		Client:                        guardedClient,
		APIReader:                     mgr.GetAPIReader(),
		Scheme:                        mgr.GetScheme(),
		Scope:                         scope,
		Metrics:                       operatorMetrics,
		ControlPlaneNamespace:         controlPlaneNamespace,
		ControlPlaneName:              controlPlaneName,
		ControlPlaneInstance:          controlPlaneInstance,
		EnvironmentServiceAccountName: tenancy.EnvironmentServiceAccount,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Environment")
		os.Exit(1)
	}
	if !(mode == tenancy.ModeScoped && len(tenancyNamespaces) == 0) {
		if err := (&controllers.ServiceObservationReconciler{Client: guardedClient, APIReader: mgr.GetAPIReader(), Scope: scope, Observer: connector}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "ServiceObservation")
			os.Exit(1)
		}
		if err := (&controllers.DeclaredServiceReconciler{
			Client: guardedClient, APIReader: mgr.GetAPIReader(), Scope: scope, Connector: connector,
			Routes: controlplaneclient.RotatingPortalResolver{BaseURL: transcriptURL, TokenFile: transcriptTokenFile, HTTP: &http.Client{Timeout: 15 * time.Second}},
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "DeclaredService")
			os.Exit(1)
		}
	}
	var eventSink agent.AdapterEventSink
	var changesSink controllers.RunChangesSink
	var transcriptCleanup *transcriptclient.Client
	var repositoryCredentials repositorycredential.Provider
	if (githubAppClientID == "") != (githubAppPrivateKeyFile == "") {
		setupLog.Error(nil, "github-app-client-id and github-app-private-key-file must be set together")
		os.Exit(1)
	}
	if githubAppClientID != "" {
		privateKey, readErr := os.ReadFile(githubAppPrivateKeyFile)
		if readErr != nil {
			setupLog.Error(readErr, "read GitHub App private key")
			os.Exit(1)
		}
		repositoryCredentials, err = githubapp.New(githubAppClientID, privateKey, nil)
		clear(privateKey)
		if err != nil {
			setupLog.Error(err, "configure GitHub App")
			os.Exit(1)
		}
	}
	if transcriptURL != "" {
		if transcriptTokenFile == "" {
			setupLog.Error(nil, "transcript-token-file is required when transcript-url is set")
			os.Exit(1)
		}
		transcriptCleanup = &transcriptclient.Client{
			BaseURL: transcriptURL, TokenFile: transcriptTokenFile,
			HTTP: &http.Client{Timeout: 15 * time.Second},
		}
		changesSink = transcriptclient.Client{BaseURL: transcriptURL, TokenFile: transcriptTokenFile, HTTP: &http.Client{Timeout: 30 * time.Second}}
		eventSink = transcriptCleanup
	}
	if !(mode == tenancy.ModeScoped && len(tenancyNamespaces) == 0) {
		if err := (&controllers.RunReconciler{
			Client:                  guardedClient,
			APIReader:               mgr.GetAPIReader(),
			Scheme:                  mgr.GetScheme(),
			Scope:                   scope,
			Adapters:                adapters,
			EventSink:               eventSink,
			Changes:                 changesSink,
			TranscriptCleanup:       transcriptCleanup,
			TranscriptsDisabled:     transcriptURL == "",
			Connector:               connector,
			Metrics:                 operatorMetrics,
			RepositoryCredentials:   repositoryCredentials,
			RepositoryCanonicalizer: githubapp.Canonicalizer{},
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "Run")
			os.Exit(1)
		}
		if err := (&controllers.WarmPoolReconciler{
			Client: guardedClient,
			Scheme: mgr.GetScheme(),
			Scope:  scope,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "WarmPool")
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

type stringListFlag []string

func operatorCacheOptions(mode tenancy.Mode, projectNamespaces []string, installationNamespace string) cache.Options {
	systemNamespaces := map[string]cache.Config{installationNamespace: {}}
	options := cache.Options{ByObject: map[client.Object]cache.ByObject{
		// These watches and their RBAC are intentionally system-namespace-only.
		&corev1.ConfigMap{}:              {Namespaces: systemNamespaces},
		&platformv1alpha1.Installation{}: {Namespaces: systemNamespaces},
	}}
	if mode != tenancy.ModeScoped {
		return options
	}
	options.DefaultNamespaces = make(map[string]cache.Config, len(projectNamespaces))
	for _, namespace := range projectNamespaces {
		options.DefaultNamespaces[namespace] = cache.Config{}
	}
	// An initial scoped install has no Project namespaces until onboarding.
	// Keep its otherwise-unused default cache restricted to the system
	// namespace; Installation and ConfigMap watches are restricted above even
	// after explicitly configured Project namespaces replace this default.
	if len(options.DefaultNamespaces) == 0 {
		options.DefaultNamespaces[installationNamespace] = cache.Config{}
	}
	return options
}

func (f *stringListFlag) String() string { return strings.Join(*f, ",") }

func (f *stringListFlag) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("namespace must not be blank")
	}
	*f = append(*f, value)
	return nil
}

func installationLeaderElectionID(uid types.UID) string {
	sum := sha256.Sum256([]byte(uid))
	return fmt.Sprintf("swe-platform-operator-%x", sum[:16])
}

func registeredAdapters() map[string]agent.AdapterLifecycle {
	return map[string]agent.AdapterLifecycle{
		"amp":         &amp.Adapter{},
		"claude-code": &claudecode.Adapter{},
		"codex":       &codex.Adapter{},
		"pi":          &pi.Adapter{},
	}
}
