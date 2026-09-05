package gateway

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
)

// Gateway routes incoming requests, extracts tenant context, and manages endpoints.
type Gateway struct {
	logger   *slog.Logger
	verifier auth.Verifier
	mux      *http.ServeMux
}

// Option configures a Gateway during construction.
type Option func(*Gateway)

// WithLogger overrides the Gateway's structured logger (defaults to a JSON
// handler writing to os.Stderr when not provided).
func WithLogger(logger *slog.Logger) Option {
	return func(g *Gateway) { g.logger = logger }
}

// WithVerifier overrides the Gateway's auth verifier used by Authenticate.
func WithVerifier(verifier auth.Verifier) Option {
	return func(g *Gateway) { g.verifier = verifier }
}

// New constructs an initialized API Gateway. When no verifier is provided,
// it defaults to auth.PermissiveVerifier{} for zero-config local development
// and MVP wiring; this MUST be overridden with WithVerifier before any
// non-development deployment.
func New(opts ...Option) *Gateway {
	g := &Gateway{
		mux: http.NewServeMux(),
	}
	for _, opt := range opts {
		opt(g)
	}
	if g.verifier == nil {
		g.verifier = auth.PermissiveVerifier{}
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
	g.mux.Handle("GET /v1/whoami", Authenticate(g.logger, g.verifier)(http.HandlerFunc(g.handleWhoami)))
	g.mux.Handle("GET /api/v1/repos/{repo}/whoami", Authenticate(g.logger, g.verifier)(RequireRepoScope(g.logger)(http.HandlerFunc(g.handleRepoWhoami))))
}

func (g *Gateway) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
	})
}

func (g *Gateway) handleWhoami(w http.ResponseWriter, r *http.Request) {
	tenantID, _ := TenantIDFromContext(r.Context())
	claims, _ := ClaimsFromContext(r.Context())

	resp := map[string]string{"tenantID": tenantID}
	if claims != nil {
		resp["role"] = string(claims.Role)
		resp["subject"] = claims.Subject
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (g *Gateway) handleRepoWhoami(w http.ResponseWriter, r *http.Request) {
	tenantID, _ := TenantIDFromContext(r.Context())
	scope, _ := ScopeFromContext(r.Context())
	claims, _ := ClaimsFromContext(r.Context())

	resp := map[string]string{"tenantID": tenantID, "repoID": scope.RepoID()}
	if claims != nil {
		resp["role"] = string(claims.Role)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
