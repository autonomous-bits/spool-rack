package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/autonomous-bits/spool-rack/internal/server/gateway"
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

	gw := gateway.New()
	addr := fmt.Sprintf(":%s", port)
	log.Printf("Starting Spool Rack server v%s on %s...", version, addr)

	if err := http.ListenAndServe(addr, gw.Routes()); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed to listen: %v", err)
	}
}
