package gateway

import (
	"encoding/json"
	"net/http"
)

// Gateway routes incoming requests, extracts tenant context, and manages endpoints.
type Gateway struct {
	mux *http.ServeMux
}

// New constructs an initialized API Gateway.
func New() *Gateway {
	g := &Gateway{
		mux: http.NewServeMux(),
	}
	g.registerRoutes()
	return g
}

// Routes returns the configured http.Handler.
func (g *Gateway) Routes() http.Handler {
	return g.mux
}

func (g *Gateway) registerRoutes() {
	g.mux.HandleFunc("GET /healthz", g.handleHealthz)
}

func (g *Gateway) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
	})
}
