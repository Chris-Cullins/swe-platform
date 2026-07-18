package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
)

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
	kubeClient, err := client.New(config.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		log.Error("create Kubernetes client", "error", err)
		os.Exit(1)
	}
	server := &http.Server{
		Addr:              *address,
		Handler:           controlplane.NewServer(log, controlplane.KubernetesTerminalDialer{Client: kubeClient}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	log.Info("starting control-plane API", "address", *address)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("control-plane API stopped", "error", err)
		os.Exit(1)
	}
}
