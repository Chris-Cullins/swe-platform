package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
)

func main() {
	address := flag.String("listen-address", ":8080", "Address for the control-plane HTTP API")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	server := &http.Server{
		Addr:              *address,
		Handler:           controlplane.NewServer(log).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	log.Info("starting control-plane API", "address", *address)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("control-plane API stopped", "error", err)
		os.Exit(1)
	}
}
