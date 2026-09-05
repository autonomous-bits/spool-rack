package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/autonomous-bits/spool-rack/internal/server/gateway"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
)

const (
	defaultPort = "8080"
	version     = "0.1.0-mvp"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	var opts []gateway.Option

	// Keep CAS opt-in so zero-config startup preserves today's safe 501 push
	// behavior unless an operator explicitly chooses a storage root.
	casRoot := os.Getenv("CAS_ROOT")
	if casRoot != "" {
		driver, err := cas.NewLocalDriver(casRoot)
		if err != nil {
			log.Fatalf("Failed to initialize CAS driver at %q: %v", casRoot, err)
		}
		opts = append(opts, gateway.WithCASDriver(driver))
	}

	postgresDSN := os.Getenv("POSTGRES_DSN")
	if postgresDSN != "" {
		openCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		store, err := postgres.Open(openCtx, postgresDSN)
		cancel()
		if err != nil {
			log.Fatalf("Failed to open Postgres store: %v", err)
		}
		// This simple main still uses blocking ListenAndServe without graceful
		// shutdown; defer is sufficient for fatal/panic exit paths.
		defer store.Close()
		opts = append(opts, gateway.WithBranchStore(store))
	}

	gw := gateway.New(opts...)
	addr := fmt.Sprintf(":%s", port)
	log.Printf("Starting Spool Rack server v%s on %s...", version, addr)
	if casRoot != "" && postgresDSN != "" {
		log.Printf("Push endpoint enabled (CAS_ROOT=%s)", casRoot)
	} else {
		log.Printf("Push endpoint disabled: CAS_ROOT/POSTGRES_DSN not both configured")
	}

	if err := http.ListenAndServe(addr, gw.Routes()); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed to listen: %v", err)
	}
}
