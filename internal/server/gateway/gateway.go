package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/autonomous-bits/spool/graphcontract"

	"github.com/autonomous-bits/spool-rack/internal/server/asset"
	"github.com/autonomous-bits/spool-rack/internal/server/audit"
	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/review"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	serversync "github.com/autonomous-bits/spool-rack/internal/server/sync"
)

// Gateway routes incoming requests, extracts tenant context, and manages endpoints.
type Gateway struct {
	logger        *slog.Logger
	verifier      auth.Verifier
	casDriver     cas.Driver
	branchStore   serversync.BranchStore
	pushEngine    *serversync.PushEngine
	pullEngine    *serversync.PullEngine
	previewEngine review.Engine
	mergeEngine   review.Finalizer
	auditLogger   *audit.Logger
	assetService  *asset.Service
	mux           *http.ServeMux

	branchLifecycleStore BranchLifecycleStore
	tenantWorkspaceStore TenantWorkspaceStore

	packFormatWindow         VersionWindow
	packIndexFormatWindow    VersionWindow
	packManifestFormatWindow VersionWindow
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

// WithAssetService configures the Gateway's remote asset management service.
func WithAssetService(service *asset.Service) Option {
	return func(g *Gateway) { g.assetService = service }
}

// WithBranchStore configures the Gateway's branch store for push handling.
func WithBranchStore(store serversync.BranchStore) Option {
	return func(g *Gateway) { g.branchStore = store }
}

// WithPreviewEngine configures the Gateway's merge preview engine.
func WithPreviewEngine(engine review.Engine) Option {
	return func(g *Gateway) { g.previewEngine = engine }
}

// WithMergeEngine configures the write-side merge lease and apply engine.
func WithMergeEngine(engine review.Finalizer) Option {
	return func(g *Gateway) { g.mergeEngine = engine }
}

// WithAuditLogger overrides the Gateway's audit event logger (defaults to a
// non-blocking in-memory logger when not provided; see comp-audit-event-logger).
func WithAuditLogger(logger *audit.Logger) Option {
	return func(g *Gateway) { g.auditLogger = logger }
}

// WithBranchLifecycleStore overrides the Gateway's branch lifecycle store
// directly. It is normally inferred automatically from WithBranchStore (when
// that store also implements BranchLifecycleStore, as postgres.Store does),
// but this override lets callers configure branch lifecycle management
// independently — for example, tests that only need this narrow slice of
// postgres.Store without a full serversync.BranchStore.
func WithBranchLifecycleStore(store BranchLifecycleStore) Option {
	return func(g *Gateway) { g.branchLifecycleStore = store }
}

// WithTenantWorkspaceStore overrides the Gateway's tenant and workspace store
// directly. It is normally inferred automatically from WithBranchStore (when
// that store also implements TenantWorkspaceStore, as postgres.Store does).
func WithTenantWorkspaceStore(store TenantWorkspaceStore) Option {
	return func(g *Gateway) { g.tenantWorkspaceStore = store }
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
	if g.auditLogger == nil {
		g.auditLogger = audit.NewLogger(audit.NewInMemorySink(), g.logger)
	}
	if g.packFormatWindow == (VersionWindow{}) {
		g.packFormatWindow = defaultPackFormatWindow()
	}
	if g.packIndexFormatWindow == (VersionWindow{}) {
		g.packIndexFormatWindow = defaultPackIndexFormatWindow()
	}
	if g.packManifestFormatWindow == (VersionWindow{}) {
		g.packManifestFormatWindow = defaultPackManifestFormatWindow()
	}
	if g.casDriver != nil && g.branchStore != nil {
		g.pushEngine = serversync.NewPushEngine(g.casDriver, g.branchStore, func(data []byte) error {
			_, err := review.DecodeSnapshotCBOR(data)
			return err
		})
		g.pushEngine.SetMinPackFormatVersion(g.packFormatWindow.Min)
		g.pullEngine = serversync.NewPullEngine(g.casDriver, g.branchStore)
		if g.previewEngine == nil {
			if metadataStore, ok := g.branchStore.(review.MetadataStore); ok {
				g.previewEngine = review.NewPreviewEngine(g.casDriver, metadataStore)
			}
		}
		if g.mergeEngine == nil {
			if mergeStore, ok := g.branchStore.(review.MergeStore); ok {
				g.mergeEngine = review.NewFinalizeEngine(g.casDriver, mergeStore)
			}
		}
	}
	if g.branchLifecycleStore == nil && g.branchStore != nil {
		if lifecycleStore, ok := g.branchStore.(BranchLifecycleStore); ok {
			g.branchLifecycleStore = lifecycleStore
		}
	}
	if g.tenantWorkspaceStore == nil && g.branchStore != nil {
		if wsStore, ok := g.branchStore.(TenantWorkspaceStore); ok {
			g.tenantWorkspaceStore = wsStore
		}
	}
	if g.assetService == nil && g.casDriver != nil && g.branchStore != nil {
		if assetStore, ok := g.branchStore.(asset.Store); ok {
			g.assetService = asset.NewService(g.casDriver, assetStore)
		}
	}
	g.registerRoutes()
	return g
}

// Routes returns the configured http.Handler, wrapped with correlation ID
// extraction/generation (see CorrelationID) so every route — including
// unauthenticated ones — echoes or assigns X-Correlation-Id.
func (g *Gateway) Routes() http.Handler {
	return CorrelationID()(g.mux)
}

func (g *Gateway) registerRoutes() {
	g.mux.HandleFunc("GET /healthz", g.handleHealthz)
	g.mux.Handle("GET /v1/whoami", Authenticate(g.logger, g.verifier)(http.HandlerFunc(g.handleWhoami)))
	// Tenant administration routes
	g.mux.Handle("POST /api/v1/tenants",
		Authenticate(g.logger, g.verifier)(
			RequireRole(g.logger, auth.RoleAdmin)(http.HandlerFunc(g.handleCreateTenant)),
		),
	)
	g.mux.Handle("GET /api/v1/tenants",
		Authenticate(g.logger, g.verifier)(
			RequireRole(g.logger, auth.RoleViewer)(http.HandlerFunc(g.handleListTenants)),
		),
	)
	g.mux.Handle("GET /api/v1/tenants/{tenant}",
		Authenticate(g.logger, g.verifier)(
			RequireRole(g.logger, auth.RoleViewer)(http.HandlerFunc(g.handleGetTenant)),
		),
	)

	// Workspace management routes (scoped to authenticated caller's tenant)
	g.mux.Handle("POST /api/v1/workspaces",
		Authenticate(g.logger, g.verifier)(
			RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handleCreateWorkspace)),
		),
	)
	g.mux.Handle("GET /api/v1/workspaces",
		Authenticate(g.logger, g.verifier)(
			RequireRole(g.logger, auth.RoleViewer)(http.HandlerFunc(g.handleListWorkspaces)),
		),
	)
	g.mux.Handle("GET /api/v1/workspaces/{workspace}",
		Authenticate(g.logger, g.verifier)(
			RequireRole(g.logger, auth.RoleViewer)(http.HandlerFunc(g.handleGetWorkspace)),
		),
	)

	// Repository routes (preserved for backwards compatibility)
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
	g.mux.Handle("GET /api/v1/repos/{repo}/clone",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleViewer)(http.HandlerFunc(g.handleClone)),
			),
		),
	)
	g.mux.Handle("POST /api/v1/repos/{repo}/merge/preview",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleViewer)(http.HandlerFunc(g.handleMergePreview)),
			),
		),
	)
	g.mux.Handle("POST /api/v1/repos/{repo}/merge/lease",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handleMergeLease)),
			),
		),
	)
	g.mux.Handle("DELETE /api/v1/repos/{repo}/merge/lease",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handleMergeLeaseRelease)),
			),
		),
	)
	g.mux.Handle("POST /api/v1/repos/{repo}/merge/apply",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handleMergeApply)),
			),
		),
	)
	g.mux.Handle("POST /api/v1/repos/{repo}/branches",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handleCreateBranch)),
			),
		),
	)
	g.mux.Handle("GET /api/v1/repos/{repo}/branches",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleViewer)(http.HandlerFunc(g.handleListBranches)),
			),
		),
	)
	g.mux.Handle("GET /api/v1/repos/{repo}/branches/default",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleViewer)(http.HandlerFunc(g.handleDefaultBranch)),
			),
		),
	)
	g.mux.Handle("DELETE /api/v1/repos/{repo}/branches/{name}",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handleDeleteBranch)),
			),
		),
	)

	// Mirrored first-class workspace routes
	g.mux.Handle("GET /api/v1/workspaces/{workspace}/whoami", Authenticate(g.logger, g.verifier)(RequireRepoScope(g.logger)(http.HandlerFunc(g.handleRepoWhoami))))
	g.mux.Handle("POST /api/v1/workspaces/{workspace}/push",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handlePush)),
			),
		),
	)
	g.mux.Handle("GET /api/v1/workspaces/{workspace}/pull",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleViewer)(http.HandlerFunc(g.handlePull)),
			),
		),
	)
	g.mux.Handle("GET /api/v1/workspaces/{workspace}/clone",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleViewer)(http.HandlerFunc(g.handleClone)),
			),
		),
	)
	g.mux.Handle("POST /api/v1/workspaces/{workspace}/merge/preview",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleViewer)(http.HandlerFunc(g.handleMergePreview)),
			),
		),
	)
	g.mux.Handle("POST /api/v1/workspaces/{workspace}/merge/lease",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handleMergeLease)),
			),
		),
	)
	g.mux.Handle("DELETE /api/v1/workspaces/{workspace}/merge/lease",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handleMergeLeaseRelease)),
			),
		),
	)
	g.mux.Handle("POST /api/v1/workspaces/{workspace}/merge/apply",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handleMergeApply)),
			),
		),
	)
	g.mux.Handle("POST /api/v1/workspaces/{workspace}/branches",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handleCreateBranch)),
			),
		),
	)
	g.mux.Handle("GET /api/v1/workspaces/{workspace}/branches",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleViewer)(http.HandlerFunc(g.handleListBranches)),
			),
		),
	)
	g.mux.Handle("GET /api/v1/workspaces/{workspace}/branches/default",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleViewer)(http.HandlerFunc(g.handleDefaultBranch)),
			),
		),
	)
	g.mux.Handle("DELETE /api/v1/workspaces/{workspace}/branches/{name}",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handleDeleteBranch)),
			),
		),
	)

	// Contextual asset reference routes (repository endpoints)
	g.mux.Handle("POST /api/v1/repos/{repo}/assets/negotiate",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handleAssetNegotiate)),
			),
		),
	)
	g.mux.Handle("PUT /api/v1/repos/{repo}/assets/blobs/{hash}",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handleAssetUpload)),
			),
		),
	)
	g.mux.Handle("GET /api/v1/repos/{repo}/assets/blobs/{hash}",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleViewer)(http.HandlerFunc(g.handleAssetStream)),
			),
		),
	)

	// Contextual asset reference routes (workspace endpoints)
	g.mux.Handle("POST /api/v1/workspaces/{workspace}/assets/negotiate",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handleAssetNegotiate)),
			),
		),
	)
	g.mux.Handle("PUT /api/v1/workspaces/{workspace}/assets/blobs/{hash}",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleContributor)(http.HandlerFunc(g.handleAssetUpload)),
			),
		),
	)
	g.mux.Handle("GET /api/v1/workspaces/{workspace}/assets/blobs/{hash}",
		Authenticate(g.logger, g.verifier)(
			RequireRepoScope(g.logger)(
				RequireRole(g.logger, auth.RoleViewer)(http.HandlerFunc(g.handleAssetStream)),
			),
		),
	)
}

type mergePreviewRequest struct {
	SourceBranch *string `json:"sourceBranch"`
	TargetBranch *string `json:"targetBranch"`
}

type mergeLeaseRequest struct {
	SourceBranch *string `json:"sourceBranch"`
	TargetBranch *string `json:"targetBranch"`
}

type mergeApplyRequest struct {
	SourceBranch *string             `json:"sourceBranch"`
	TargetBranch *string             `json:"targetBranch"`
	LeaseToken   string              `json:"leaseToken,omitempty"`
	Resolutions  []review.Resolution `json:"resolutions"`
	Author       *string             `json:"author"`
	Message      *string             `json:"message"`
}

type mergeLeaseReleaseRequest struct {
	TargetBranch *string `json:"targetBranch"`
	LeaseToken   *string `json:"leaseToken"`
}

func (g *Gateway) handleMergeLease(w http.ResponseWriter, r *http.Request) {
	if g.mergeEngine == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "merge finalization is not configured on this server")
		return
	}
	var request mergeLeaseRequest
	if !decodeSingleJSON(w, r, g.logger, &request) {
		return
	}
	if request.SourceBranch == nil || *request.SourceBranch == "" || request.TargetBranch == nil || *request.TargetBranch == "" {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "sourceBranch and targetBranch are required")
		return
	}
	tenantID, _ := TenantIDFromContext(r.Context())
	scope, _ := ScopeFromContext(r.Context())
	claims, _ := ClaimsFromContext(r.Context())
	lease, err := g.mergeEngine.AcquireLease(r.Context(), tenantID, scope.RepoID(), review.LeaseRequest{
		SourceBranch: *request.SourceBranch, TargetBranch: *request.TargetBranch, Subject: claims.Subject,
	})
	if err != nil {
		g.writeMergeError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(lease)
}

func (g *Gateway) handleMergeApply(w http.ResponseWriter, r *http.Request) {
	if g.mergeEngine == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "merge finalization is not configured on this server")
		return
	}
	var request mergeApplyRequest
	if !decodeSingleJSON(w, r, g.logger, &request) {
		return
	}
	if request.SourceBranch == nil || *request.SourceBranch == "" || request.TargetBranch == nil || *request.TargetBranch == "" ||
		request.Author == nil || *request.Author == "" || request.Message == nil || *request.Message == "" || request.Resolutions == nil {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "sourceBranch, targetBranch, resolutions, author, and message are required")
		return
	}
	tenantID, _ := TenantIDFromContext(r.Context())
	scope, _ := ScopeFromContext(r.Context())
	claims, _ := ClaimsFromContext(r.Context())
	result, err := g.mergeEngine.Apply(r.Context(), tenantID, scope.RepoID(), review.ApplyRequest{
		SourceBranch: *request.SourceBranch, TargetBranch: *request.TargetBranch, Subject: claims.Subject,
		LeaseToken: request.LeaseToken, Resolutions: request.Resolutions, Author: *request.Author, Message: *request.Message,
	})
	if err != nil {
		event := g.newAuditEvent(r, tenantID, scope.RepoID(), *request.TargetBranch, "merge.apply", "rejected")
		event.Detail = "merge apply rejected"
		g.auditLogger.Emit(event)

		g.writeMergeError(w, r, err)
		return
	}

	successEvent := g.newAuditEvent(r, tenantID, scope.RepoID(), *request.TargetBranch, "merge.apply", "success")
	successEvent.NewRef = result.HeadCommit
	g.auditLogger.Emit(successEvent)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(result)
}

func (g *Gateway) handleMergeLeaseRelease(w http.ResponseWriter, r *http.Request) {
	if g.mergeEngine == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "merge finalization is not configured on this server")
		return
	}
	var request mergeLeaseReleaseRequest
	if !decodeSingleJSON(w, r, g.logger, &request) {
		return
	}
	if request.TargetBranch == nil || *request.TargetBranch == "" || request.LeaseToken == nil || *request.LeaseToken == "" {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "targetBranch and leaseToken are required")
		return
	}
	tenantID, _ := TenantIDFromContext(r.Context())
	scope, _ := ScopeFromContext(r.Context())
	claims, _ := ClaimsFromContext(r.Context())
	if err := g.mergeEngine.ReleaseLease(r.Context(), tenantID, scope.RepoID(), *request.TargetBranch, claims.Subject, *request.LeaseToken); err != nil {
		g.writeMergeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func decodeSingleJSON(w http.ResponseWriter, r *http.Request, logger *slog.Logger, destination any) bool {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		writeJSONError(w, r, logger, http.StatusBadRequest, ErrorCodeBadRequest, "request must contain valid JSON")
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeJSONError(w, r, logger, http.StatusBadRequest, ErrorCodeBadRequest, "request must contain exactly one JSON object")
		return false
	}
	return true
}

// newAuditEvent constructs an audit.Event pre-populated with the
// request-scoped identity, scope, and correlation metadata shared by every
// audit emission site: the authenticated actor, tenant, repository, branch,
// current graphcontract pack format version, and correlation ID. Callers
// should set PreviousRef/NewRef/Detail/ContractVersion (when a more
// specific version applies) before emitting.
func (g *Gateway) newAuditEvent(r *http.Request, tenantID, repoID, branch, action, outcome string) audit.Event {
	var actor string
	if claims, ok := ClaimsFromContext(r.Context()); ok && claims != nil {
		actor = claims.Subject
	}
	correlationID, _ := CorrelationIDFromContext(r.Context())

	return audit.Event{
		CorrelationID:   correlationID,
		Actor:           actor,
		TenantID:        tenantID,
		RepoID:          repoID,
		Branch:          branch,
		Action:          action,
		Outcome:         outcome,
		ContractVersion: graphcontract.PackFormatVersion,
	}
}

func (g *Gateway) writeMergeError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, postgres.ErrBranchNotFound):
		writeJSONError(w, r, g.logger, http.StatusNotFound, ErrorCodeNotFound, "branch not found")
	case errors.Is(err, postgres.ErrMergeLeaseHeld), errors.Is(err, postgres.ErrNonFastForward), errors.Is(err, postgres.ErrMergeLeaseMismatch):
		writeJSONError(w, r, g.logger, http.StatusConflict, ErrorCodeConflict, "merge branch state changed or is currently leased")
	case errors.Is(err, postgres.ErrMergeLeaseNotFound), errors.Is(err, postgres.ErrMergeLeaseOwnership), errors.Is(err, postgres.ErrMergeLeaseExpired):
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "merge lease is invalid, expired, or not held by this caller")
	case errors.Is(err, review.ErrInvalidMergeRequest):
		logRejection(g.logger, r, "rejected invalid merge request", err)
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "invalid merge request")
	default:
		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to finalize merge")
	}
}

func (g *Gateway) handleMergePreview(w http.ResponseWriter, r *http.Request) {
	if g.previewEngine == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "merge preview is not configured on this server")
		return
	}

	var request mergePreviewRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "request must contain valid JSON")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "request must contain exactly one JSON object")
		return
	}
	if request.SourceBranch == nil || *request.SourceBranch == "" || request.TargetBranch == nil || *request.TargetBranch == "" {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "sourceBranch and targetBranch are required")
		return
	}

	tenantID, _ := TenantIDFromContext(r.Context())
	scope, _ := ScopeFromContext(r.Context())
	preview, err := g.previewEngine.PreviewMerge(r.Context(), tenantID, scope.RepoID(), *request.SourceBranch, *request.TargetBranch)
	if errors.Is(err, postgres.ErrBranchNotFound) {
		writeJSONError(w, r, g.logger, http.StatusNotFound, ErrorCodeNotFound, "branch not found")
		return
	}
	if errors.Is(err, review.ErrInvalidPreviewRequest) {
		logRejection(g.logger, r, "rejected invalid merge preview request", err)
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "invalid merge preview request")
		return
	}
	if err != nil {
		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to preview merge")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(preview)
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

	// packFormat is optional: an omitted value preserves today's behavior for
	// clients that predate version negotiation. When present, it must be
	// within the accepted pack format window advertised by GET /healthz (see
	// spec-cli-rack-contract-and-metadata-migrations).
	if rawPackFormat := r.URL.Query().Get("packFormat"); rawPackFormat != "" {
		packFormat, err := strconv.ParseUint(rawPackFormat, 10, 32)
		if err != nil || !g.packFormatWindow.Accepts(uint32(packFormat)) {
			writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeUnsupportedContractVersion,
				fmt.Sprintf("pack format %q is outside the versions this server currently accepts [%d,%d]", rawPackFormat, g.packFormatWindow.Min, g.packFormatWindow.Max))
			return
		}
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
	if errors.Is(err, serversync.ErrUpToDate) {
		event := g.newAuditEvent(r, tenantID, scope.RepoID(), branch, "pull", "up_to_date")
		event.PreviousRef = knownCommit
		event.NewRef = plan.Head
		g.auditLogger.Emit(event)

		w.Header().Set("X-Spool-Head-Commit", plan.Head)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if errors.Is(err, serversync.ErrPullDiverged) {
		w.Header().Del("Content-Encoding")
		var divergence *serversync.PullDivergedError
		if errors.As(err, &divergence) {
			event := g.newAuditEvent(r, tenantID, scope.RepoID(), branch, "pull", "rejected")
			event.PreviousRef = knownCommit
			event.NewRef = divergence.CurrentHead
			event.Detail = "known commit is not an ancestor of the remote branch head"
			g.auditLogger.Emit(event)

			writeJSONErrorEnvelope(w, r, g.logger, http.StatusConflict, errorEnvelope{
				Error:       ErrorCodeConflict,
				Message:     "known commit is not an ancestor of the remote branch head",
				CurrentHead: divergence.CurrentHead,
			})
			return
		}
	}
	if errors.Is(err, postgres.ErrBranchNotFound) {
		event := g.newAuditEvent(r, tenantID, scope.RepoID(), branch, "pull", "rejected")
		event.PreviousRef = knownCommit
		event.Detail = "branch not found"
		g.auditLogger.Emit(event)

		writeJSONError(w, r, g.logger, http.StatusNotFound, ErrorCodeNotFound, "branch not found")
		return
	}
	if err != nil {
		logRejection(g.logger, r, "pull preparation failed", err)
		event := g.newAuditEvent(r, tenantID, scope.RepoID(), branch, "pull", "error")
		event.PreviousRef = knownCommit
		g.auditLogger.Emit(event)

		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to prepare pull")
		return
	}
	manifest, err := g.pullEngine.BuildPullManifest(r.Context(), plan)
	if err != nil {
		logRejection(g.logger, r, "pull manifest build failed", err)
		event := g.newAuditEvent(r, tenantID, scope.RepoID(), branch, "pull", "error")
		event.PreviousRef = knownCommit
		event.NewRef = plan.Head
		g.auditLogger.Emit(event)

		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to open pull pack")
		return
	}

	w.Header().Set("Content-Type", "application/vnd.spool-rack.pull-envelope")
	w.Header().Set("Content-Encoding", "zstd")
	w.Header().Set("X-Spool-Pull-Format", "2")
	w.Header().Set("X-Spool-Head-Commit", plan.Head)

	successEvent := g.newAuditEvent(r, tenantID, scope.RepoID(), branch, "pull", "success")
	successEvent.PreviousRef = knownCommit
	successEvent.NewRef = plan.Head
	g.auditLogger.Emit(successEvent)

	if err := g.pullEngine.StreamPullWithManifest(r.Context(), plan, manifest, w); err != nil {
		logger := g.logger
		if logger == nil {
			logger = defaultLogger
		}
		logger.Error("pull stream failed", "path", r.URL.Path, "error", err)
	}
}

func (g *Gateway) handleClone(w http.ResponseWriter, r *http.Request) {
	if g.pullEngine == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "clone is not configured on this server")
		return
	}

	// packFormat is optional: an omitted value preserves today's behavior for
	// clients that predate version negotiation. When present, it must be
	// within the accepted pack format window advertised by GET /healthz.
	if rawPackFormat := r.URL.Query().Get("packFormat"); rawPackFormat != "" {
		packFormat, err := strconv.ParseUint(rawPackFormat, 10, 32)
		if err != nil || !g.packFormatWindow.Accepts(uint32(packFormat)) {
			writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeUnsupportedContractVersion,
				fmt.Sprintf("pack format %q is outside the versions this server currently accepts [%d,%d]", rawPackFormat, g.packFormatWindow.Min, g.packFormatWindow.Max))
			return
		}
	}

	tenantID, _ := TenantIDFromContext(r.Context())
	scope, _ := ScopeFromContext(r.Context())

	branch := r.URL.Query().Get("branch")
	defaultBranchName := ""

	if g.branchLifecycleStore != nil {
		ctx, err := g.branchLifecycleStore.SetTenantContext(r.Context(), tenantID)
		if err == nil {
			defaultRef, defErr := g.branchLifecycleStore.GetDefaultBranch(ctx, scope.RepoID())
			if defErr == nil {
				defaultBranchName = defaultRef.Name
			} else if errors.Is(defErr, postgres.ErrDefaultBranchNotSet) && branch == "" {
				// Empty workspace with no branches yet
				event := g.newAuditEvent(r, tenantID, scope.RepoID(), "main", "clone", "empty")
				g.auditLogger.Emit(event)

				w.Header().Set("X-Spool-Empty", "true")
				w.Header().Set("X-Spool-Default-Branch", "main")
				w.Header().Set("X-Spool-Branch", "main")
				w.WriteHeader(http.StatusOK)
				return
			}
		}
	}

	if branch == "" {
		if defaultBranchName != "" {
			branch = defaultBranchName
		} else {
			branch = "main"
		}
	}
	if defaultBranchName == "" {
		defaultBranchName = branch
	}

	req := serversync.PullRequest{
		TenantID:    tenantID,
		RepoID:      scope.RepoID(),
		Branch:      branch,
		KnownCommit: "", // clone always fetches complete history from root
	}

	plan, err := g.pullEngine.PreparePull(r.Context(), req)
	if errors.Is(err, postgres.ErrBranchNotFound) {
		event := g.newAuditEvent(r, tenantID, scope.RepoID(), branch, "clone", "rejected")
		event.Detail = "branch not found"
		g.auditLogger.Emit(event)

		writeJSONError(w, r, g.logger, http.StatusNotFound, ErrorCodeNotFound, "branch not found")
		return
	}
	if err != nil {
		logRejection(g.logger, r, "clone preparation failed", err)
		event := g.newAuditEvent(r, tenantID, scope.RepoID(), branch, "clone", "error")
		g.auditLogger.Emit(event)

		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to prepare clone")
		return
	}

	manifest, err := g.pullEngine.BuildPullManifest(r.Context(), plan)
	if err != nil {
		logRejection(g.logger, r, "clone manifest build failed", err)
		event := g.newAuditEvent(r, tenantID, scope.RepoID(), branch, "clone", "error")
		event.NewRef = plan.Head
		g.auditLogger.Emit(event)

		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to open clone pack")
		return
	}

	w.Header().Set("Content-Type", "application/vnd.spool-rack.pull-envelope")
	w.Header().Set("Content-Encoding", "zstd")
	w.Header().Set("X-Spool-Pull-Format", "2")
	w.Header().Set("X-Spool-Head-Commit", plan.Head)
	w.Header().Set("X-Spool-Branch", branch)
	w.Header().Set("X-Spool-Default-Branch", defaultBranchName)

	successEvent := g.newAuditEvent(r, tenantID, scope.RepoID(), branch, "clone", "success")
	successEvent.NewRef = plan.Head
	g.auditLogger.Emit(successEvent)

	if err := g.pullEngine.StreamPullWithManifest(r.Context(), plan, manifest, w); err != nil {
		logger := g.logger
		if logger == nil {
			logger = defaultLogger
		}
		logger.Error("clone stream failed", "path", r.URL.Path, "error", err)
	}
}

// healthzGraphContract reports the graphcontract pack format versions this
// server understands, so clients (e.g. the Spool CLI) can negotiate
// compatibility before push/pull. The Min/Max fields advertise the accepted
// version range for a rolling deploy (see
// spec-cli-rack-contract-and-metadata-migrations); the plain *Version fields
// are kept for existing clients that only compare against the current
// version.
type healthzGraphContract struct {
	PackFormatVersion            uint32 `json:"packFormatVersion"`
	PackFormatMinVersion         uint32 `json:"packFormatMinVersion"`
	PackFormatMaxVersion         uint32 `json:"packFormatMaxVersion"`
	PackIndexFormatVersion       uint32 `json:"packIndexFormatVersion"`
	PackIndexFormatMinVersion    uint32 `json:"packIndexFormatMinVersion"`
	PackIndexFormatMaxVersion    uint32 `json:"packIndexFormatMaxVersion"`
	PackManifestFormatVersion    uint32 `json:"packManifestFormatVersion"`
	PackManifestFormatMinVersion uint32 `json:"packManifestFormatMinVersion"`
	PackManifestFormatMaxVersion uint32 `json:"packManifestFormatMaxVersion"`
}

type healthzResponse struct {
	Status        string               `json:"status"`
	GraphContract healthzGraphContract `json:"graphcontract"`
}

func (g *Gateway) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(healthzResponse{
		Status: "healthy",
		GraphContract: healthzGraphContract{
			PackFormatVersion:            serversync.PackFormatV3,
			PackFormatMinVersion:         g.packFormatWindow.Min,
			PackFormatMaxVersion:         g.packFormatWindow.Max,
			PackIndexFormatVersion:       graphcontract.PackIndexFormatVersion,
			PackIndexFormatMinVersion:    g.packIndexFormatWindow.Min,
			PackIndexFormatMaxVersion:    g.packIndexFormatWindow.Max,
			PackManifestFormatVersion:    graphcontract.PackManifestFormatVersion,
			PackManifestFormatMinVersion: g.packManifestFormatWindow.Min,
			PackManifestFormatMaxVersion: g.packManifestFormatWindow.Max,
		},
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
	PackFormat   uint32                    `json:"packFormat,omitempty"`
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
				PackFormat:   meta.PackFormat,
				PackStream:   part,
			}
			err := g.pushEngine.HandlePush(r.Context(), req)
			_ = part.Close()
			if err != nil {
				var nffErr *serversync.NonFastForwardError
				if errors.As(err, &nffErr) {
					event := g.newAuditEvent(r, tenantID, scope.RepoID(), meta.Branch, "push", "rejected")
					event.PreviousRef = meta.BaseCommit
					event.NewRef = nffErr.ActualHead
					event.ContractVersion = meta.PackFormat
					event.Detail = "non-fast-forward push"
					g.auditLogger.Emit(event)

					writeJSONErrorEnvelope(w, r, g.logger, http.StatusConflict, errorEnvelope{
						Error:       ErrorCodeConflict,
						Message:     nffErr.Guidance,
						CurrentHead: nffErr.ActualHead,
					})
					return
				}

				logRejection(g.logger, r, "rejected invalid push", err)
				detail := "invalid push pack or metadata"
				if errors.Is(err, serversync.ErrUnsupportedContractVersion) {
					detail = "unsupported contract version"
				}
				event := g.newAuditEvent(r, tenantID, scope.RepoID(), meta.Branch, "push", "rejected")
				event.PreviousRef = meta.BaseCommit
				event.NewRef = meta.TargetCommit
				event.ContractVersion = meta.PackFormat
				event.Detail = detail
				g.auditLogger.Emit(event)

				if errors.Is(err, serversync.ErrUnsupportedContractVersion) {
					writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeUnsupportedContractVersion, err.Error())
					return
				}
				if errors.Is(err, serversync.ErrInvalidFrame) || errors.Is(err, serversync.ErrInvalidCanonicalFrame) {
					writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "invalid or malformed push pack")
					return
				}
				writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "push request could not be processed")
				return
			}

			verifiedEvent := g.newAuditEvent(r, tenantID, scope.RepoID(), meta.Branch, "push", "verified")
			verifiedEvent.PreviousRef = meta.BaseCommit
			verifiedEvent.NewRef = meta.TargetCommit
			verifiedEvent.ContractVersion = meta.PackFormat
			g.auditLogger.Emit(verifiedEvent)

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

func (g *Gateway) handleAssetNegotiate(w http.ResponseWriter, r *http.Request) {
	if g.assetService == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "asset service is not configured on this server")
		return
	}

	var req asset.NegotiationRequest
	if !decodeSingleJSON(w, r, g.logger, &req) {
		return
	}

	scope, ok := ScopeFromContext(r.Context())
	if !ok {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "missing repository scope")
		return
	}

	result, err := g.assetService.Negotiate(r.Context(), scope, req)
	if err != nil {
		if errors.Is(err, asset.ErrQuotaExceeded) {
			writeJSONError(w, r, g.logger, http.StatusForbidden, "QUOTA_EXCEEDED", err.Error())
			return
		}
		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(result)
}

func (g *Gateway) handleAssetUpload(w http.ResponseWriter, r *http.Request) {
	if g.assetService == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "asset service is not configured on this server")
		return
	}

	hash := r.PathValue("hash")
	if hash == "" {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "asset hash is required")
		return
	}

	scope, ok := ScopeFromContext(r.Context())
	if !ok {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "missing repository scope")
		return
	}

	size := r.ContentLength
	contentType := r.Header.Get("Content-Type")

	written, err := g.assetService.Upload(r.Context(), scope, hash, size, contentType, r.Body)
	if err != nil {
		if errors.Is(err, asset.ErrQuotaExceeded) {
			writeJSONError(w, r, g.logger, http.StatusForbidden, "QUOTA_EXCEEDED", err.Error())
			return
		}
		if errors.Is(err, cas.ErrHashMismatch) {
			writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, err.Error())
			return
		}
		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"hash":   hash,
		"size":   written,
		"status": "stored",
	})
}

func (g *Gateway) handleAssetStream(w http.ResponseWriter, r *http.Request) {
	if g.assetService == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "asset service is not configured on this server")
		return
	}

	hash := r.PathValue("hash")
	if hash == "" {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "asset hash is required")
		return
	}

	scope, ok := ScopeFromContext(r.Context())
	if !ok {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "missing repository scope")
		return
	}

	rc, size, mimeType, err := g.assetService.Open(r.Context(), scope, hash)
	if err != nil {
		if errors.Is(err, asset.ErrNotFound) {
			writeJSONError(w, r, g.logger, http.StatusNotFound, ErrorCodeNotFound, "asset blob not found")
			return
		}
		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, err.Error())
		return
	}
	defer func() {
		if err := rc.Close(); err != nil {
			g.logger.Error("close asset reader", "error", err)
		}
	}()

	w.Header().Set("Content-Type", mimeType)
	w.Header().Set("Accept-Ranges", "bytes")

	if seeker, ok := rc.(io.ReadSeeker); ok {
		http.ServeContent(w, r, hash, time.Time{}, seeker)
		return
	}

	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}
