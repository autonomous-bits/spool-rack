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
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	serversync "github.com/autonomous-bits/spool-rack/internal/server/sync"
)

// Gateway routes incoming requests, extracts tenant context, and manages endpoints.
type Gateway struct {
	logger      *slog.Logger
	verifier    auth.Verifier
	casDriver   cas.Driver
	branchStore serversync.BranchStore
	pushEngine  *serversync.PushEngine
	pullEngine  *serversync.PullEngine
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
		g.pullEngine = serversync.NewPullEngine(g.casDriver, g.branchStore)
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
	g.mux.Handle("GET /api/v1/repos/{repo}/pull",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleViewer)(http.HandlerFunc(g.handlePull)),
			),
		),
	)
}

func (g *Gateway) handlePull(w http.ResponseWriter, r *http.Request) {
	if g.pullEngine == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "pull is not configured on this server")
		return
	}

	branch := r.URL.Query().Get("branch")
	knownCommit := r.URL.Query().Get("knownCommit")
	if branch == "" {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "branch query parameter is required")
		return
	}

	tenantID, _ := TenantIDFromContext(r.Context())
	scope, _ := ScopeFromContext(r.Context())
	req := serversync.PullRequest{
		TenantID:    tenantID,
		RepoID:      scope.RepoID(),
		Branch:      branch,
		KnownCommit: knownCommit,
	}

	plan, err := g.pullEngine.PreparePull(r.Context(), req)
	if errors.Is(err, serversync.UpToDateError) {
		w.Header().Set("X-Spool-Head-Commit", plan.Head)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if errors.Is(err, serversync.ErrPullDiverged) {
		w.Header().Del("Content-Encoding")
		var divergence *serversync.PullDivergedError
		if errors.As(err, &divergence) {
			writeJSONErrorEnvelope(w, r, g.logger, http.StatusConflict, errorEnvelope{
				Error:       ErrorCodeConflict,
				Message:     "known commit is not an ancestor of the remote branch head",
				CurrentHead: divergence.CurrentHead,
			})
			return
		}
	}
	if errors.Is(err, postgres.ErrBranchNotFound) {
		writeJSONError(w, r, g.logger, http.StatusNotFound, ErrorCodeNotFound, "branch not found")
		return
	}
	if err != nil {
		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to prepare pull")
		return
	}
	if err := g.pullEngine.ValidatePullPacks(r.Context(), plan); err != nil {
		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to open pull pack")
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Encoding", "zstd")
	w.Header().Set("X-Spool-Head-Commit", plan.Head)
	if err := g.pullEngine.StreamPull(r.Context(), plan, w); err != nil {
		logger := g.logger
		if logger == nil {
			logger = defaultLogger
		}
		logger.Error("pull stream failed", "path", r.URL.Path, "error", err)
	}
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
