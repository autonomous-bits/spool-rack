package gateway

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"regexp"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	serversync "github.com/autonomous-bits/spool-rack/internal/server/sync"
)

// Gateway routes incoming requests, extracts tenant context, and manages endpoints.
type Gateway struct {
	logger      *slog.Logger
	verifier    auth.Verifier
	casDriver   cas.Driver
	branchStore serversync.BranchStore
	pushEngine  *serversync.PushEngine
	mux         *http.ServeMux
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

// WithCASDriver configures the Gateway's CAS driver for push handling.
func WithCASDriver(driver cas.Driver) Option {
	return func(g *Gateway) { g.casDriver = driver }
}

// WithBranchStore configures the Gateway's branch store for push handling.
func WithBranchStore(store serversync.BranchStore) Option {
	return func(g *Gateway) { g.branchStore = store }
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
	if g.casDriver != nil && g.branchStore != nil {
		g.pushEngine = serversync.NewPushEngine(g.casDriver, g.branchStore)
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
	g.mux.Handle("POST /api/v1/repos/{repo}/push",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handlePush)),
			),
		),
	)
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

type pushMetadata struct {
	Branch       string                    `json:"branch"`
	BaseCommit   string                    `json:"baseCommit"`
	TargetCommit string                    `json:"targetCommit"`
	PackHash     string                    `json:"packHash"`
	Commits      []serversync.CommitRecord `json:"commits,omitempty"`
}

var pushPackHashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func (g *Gateway) handlePush(w http.ResponseWriter, r *http.Request) {
	if g.pushEngine == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "push is not configured on this server")
		return
	}

	tenantID, _ := TenantIDFromContext(r.Context())
	scope, _ := ScopeFromContext(r.Context())

	mr, err := r.MultipartReader()
	if err != nil {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "request must be multipart/form-data")
		return
	}

	var (
		meta            pushMetadata
		sawMetadataPart bool
	)

	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "request must include metadata and pack parts")
			return
		}
		if err != nil {
			writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "failed to read multipart body")
			return
		}

		switch part.FormName() {
		case "metadata":
			dec := json.NewDecoder(part)
			if err := dec.Decode(&meta); err != nil {
				_ = part.Close()
				writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "metadata part must contain valid JSON")
				return
			}
			if !pushPackHashPattern.MatchString(meta.PackHash) {
				_ = part.Close()
				writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "packHash must be 64 lowercase hex characters")
				return
			}
			sawMetadataPart = true
			_ = part.Close()
		case "pack":
			// The metadata part must precede the pack part so the handler can pass
			// the multipart stream directly into PushEngine without buffering.
			if !sawMetadataPart {
				_ = part.Close()
				writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "metadata part must precede pack part")
				return
			}

			req := serversync.PushRequest{
				TenantID:     tenantID,
				RepoID:       scope.RepoID(),
				Branch:       meta.Branch,
				BaseCommit:   meta.BaseCommit,
				TargetCommit: meta.TargetCommit,
				Commits:      meta.Commits,
				PackHash:     meta.PackHash,
				PackStream:   part,
			}
			err := g.pushEngine.HandlePush(r.Context(), req)
			_ = part.Close()
			if err != nil {
				var nffErr *serversync.NonFastForwardError
				if errors.As(err, &nffErr) {
					writeJSONErrorEnvelope(w, r, g.logger, http.StatusConflict, errorEnvelope{
						Error:       ErrorCodeConflict,
						Message:     nffErr.Guidance,
						CurrentHead: nffErr.ActualHead,
					})
					return
				}

				writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, err.Error())
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"branch":     meta.Branch,
				"headCommit": meta.TargetCommit,
			})
			return
		default:
			_ = part.Close()
		}
	}
}
