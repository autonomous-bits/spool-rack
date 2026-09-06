package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
)

// BranchLifecycleStore is the narrow slice of postgres.Store that the branch
// lifecycle handlers depend on, kept separate so unit tests can supply a
// lightweight fake instead of a real PostgreSQL-backed Store — the same
// pattern serversync.BranchStore uses for push/pull.
type BranchLifecycleStore interface {
	SetTenantContext(ctx context.Context, tenantID string) (context.Context, error)
	CreateBranch(ctx context.Context, repoID, name, headCommitID string) error
	GetBranchRef(ctx context.Context, repoID, branch string) (string, error)
	ListBranchRefs(ctx context.Context, repoID string) ([]postgres.BranchRef, error)
	DeleteBranch(ctx context.Context, repoID, name string) error
	GetDefaultBranch(ctx context.Context, repoID string) (postgres.BranchRef, error)
}

var _ BranchLifecycleStore = (postgres.Store)(nil)

type createBranchRequest struct {
	Name         *string `json:"name"`
	SourceBranch *string `json:"sourceBranch"`
	SourceCommit *string `json:"sourceCommit"`
}

type branchResponse struct {
	Name       string `json:"name"`
	HeadCommit string `json:"headCommit"`
}

type branchListEntry struct {
	Name       string `json:"name"`
	HeadCommit string `json:"headCommit"`
	Default    bool   `json:"default"`
}

type branchListResponse struct {
	Branches []branchListEntry `json:"branches"`
}

func (g *Gateway) handleCreateBranch(w http.ResponseWriter, r *http.Request) {
	if g.branchLifecycleStore == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "branch lifecycle management is not configured on this server")
		return
	}

	var request createBranchRequest
	if !decodeSingleJSON(w, r, g.logger, &request) {
		return
	}
	if request.Name == nil || *request.Name == "" {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "name is required")
		return
	}
	hasSourceBranch := request.SourceBranch != nil && *request.SourceBranch != ""
	hasSourceCommit := request.SourceCommit != nil && *request.SourceCommit != ""
	if hasSourceBranch == hasSourceCommit {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "exactly one of sourceBranch or sourceCommit is required")
		return
	}

	tenantID, _ := TenantIDFromContext(r.Context())
	scope, _ := ScopeFromContext(r.Context())
	ctx, err := g.branchLifecycleStore.SetTenantContext(r.Context(), tenantID)
	if err != nil {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "invalid tenant context")
		return
	}

	headCommitID := ""
	if hasSourceBranch {
		headCommitID, err = g.branchLifecycleStore.GetBranchRef(ctx, scope.RepoID(), *request.SourceBranch)
		if errors.Is(err, postgres.ErrBranchNotFound) {
			writeJSONError(w, r, g.logger, http.StatusNotFound, ErrorCodeBranchSourceNotFound, "source branch not found")
			return
		}
		if err != nil {
			writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to resolve source branch")
			return
		}
	} else {
		headCommitID = *request.SourceCommit
	}

	if err := g.branchLifecycleStore.CreateBranch(ctx, scope.RepoID(), *request.Name, headCommitID); err != nil {
		switch {
		case errors.Is(err, postgres.ErrBranchAlreadyExists):
			writeJSONError(w, r, g.logger, http.StatusConflict, ErrorCodeBranchAlreadyExists, "branch already exists")
		case errors.Is(err, postgres.ErrCommitNotFound):
			writeJSONError(w, r, g.logger, http.StatusNotFound, ErrorCodeBranchSourceNotFound, "source commit not found")
		default:
			writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to create branch")
		}
		return
	}

	successEvent := g.newAuditEvent(r, tenantID, scope.RepoID(), *request.Name, "branch.create", "success")
	successEvent.NewRef = headCommitID
	g.auditLogger.Emit(successEvent)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(branchResponse{Name: *request.Name, HeadCommit: headCommitID})
}

func (g *Gateway) handleListBranches(w http.ResponseWriter, r *http.Request) {
	if g.branchLifecycleStore == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "branch lifecycle management is not configured on this server")
		return
	}

	tenantID, _ := TenantIDFromContext(r.Context())
	scope, _ := ScopeFromContext(r.Context())
	ctx, err := g.branchLifecycleStore.SetTenantContext(r.Context(), tenantID)
	if err != nil {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "invalid tenant context")
		return
	}

	refs, err := g.branchLifecycleStore.ListBranchRefs(ctx, scope.RepoID())
	if err != nil {
		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to list branches")
		return
	}

	defaultName := ""
	defaultRef, err := g.branchLifecycleStore.GetDefaultBranch(ctx, scope.RepoID())
	if err == nil {
		defaultName = defaultRef.Name
	} else if !errors.Is(err, postgres.ErrDefaultBranchNotSet) {
		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to resolve default branch")
		return
	}

	entries := make([]branchListEntry, 0, len(refs))
	for _, ref := range refs {
		if ref.Deleted {
			continue
		}
		entries = append(entries, branchListEntry{
			Name:       ref.Name,
			HeadCommit: ref.HeadCommitID,
			Default:    ref.Name == defaultName,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(branchListResponse{Branches: entries})
}

func (g *Gateway) handleDefaultBranch(w http.ResponseWriter, r *http.Request) {
	if g.branchLifecycleStore == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "branch lifecycle management is not configured on this server")
		return
	}

	tenantID, _ := TenantIDFromContext(r.Context())
	scope, _ := ScopeFromContext(r.Context())
	ctx, err := g.branchLifecycleStore.SetTenantContext(r.Context(), tenantID)
	if err != nil {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "invalid tenant context")
		return
	}

	ref, err := g.branchLifecycleStore.GetDefaultBranch(ctx, scope.RepoID())
	if errors.Is(err, postgres.ErrDefaultBranchNotSet) {
		writeJSONError(w, r, g.logger, http.StatusNotFound, ErrorCodeBranchNotFound, "repository has no default branch")
		return
	}
	if err != nil {
		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to resolve default branch")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(branchResponse{Name: ref.Name, HeadCommit: ref.HeadCommitID})
}

func (g *Gateway) handleDeleteBranch(w http.ResponseWriter, r *http.Request) {
	if g.branchLifecycleStore == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "branch lifecycle management is not configured on this server")
		return
	}

	name := r.PathValue("name")
	if name == "" {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "branch name is required")
		return
	}

	tenantID, _ := TenantIDFromContext(r.Context())
	scope, _ := ScopeFromContext(r.Context())
	ctx, err := g.branchLifecycleStore.SetTenantContext(r.Context(), tenantID)
	if err != nil {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "invalid tenant context")
		return
	}

	if err := g.branchLifecycleStore.DeleteBranch(ctx, scope.RepoID(), name); err != nil {
		switch {
		case errors.Is(err, postgres.ErrDefaultBranchProtected):
			event := g.newAuditEvent(r, tenantID, scope.RepoID(), name, "branch.delete", "rejected")
			event.Detail = "cannot delete default branch"
			g.auditLogger.Emit(event)
			writeJSONError(w, r, g.logger, http.StatusConflict, ErrorCodeBranchProtected, "cannot delete the repository's default branch")
		case errors.Is(err, postgres.ErrBranchNotFound):
			writeJSONError(w, r, g.logger, http.StatusNotFound, ErrorCodeBranchNotFound, "branch not found")
		default:
			writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to delete branch")
		}
		return
	}

	successEvent := g.newAuditEvent(r, tenantID, scope.RepoID(), name, "branch.delete", "success")
	g.auditLogger.Emit(successEvent)

	w.WriteHeader(http.StatusNoContent)
}
