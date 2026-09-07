package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
)

// TenantWorkspaceStore defines the metadata store operations needed for
// tenant and workspace administration.
type TenantWorkspaceStore interface {
	SetTenantContext(ctx context.Context, tenantID string) (context.Context, error)
	CreateTenant(ctx context.Context, name string) (string, error)
	ListTenants(ctx context.Context) ([]postgres.TenantSummary, error)
	GetTenant(ctx context.Context, idOrName string) (postgres.TenantSummary, error)
	CreateWorkspace(ctx context.Context, name string) (string, error)
	ListWorkspaces(ctx context.Context) ([]postgres.WorkspaceSummary, error)
	GetWorkspace(ctx context.Context, idOrName string) (postgres.WorkspaceSummary, error)
}

var _ TenantWorkspaceStore = (postgres.Store)(nil)

type createTenantRequest struct {
	Name *string `json:"name"`
}

type createTenantResponse struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
}

type tenantListResponse struct {
	Tenants []postgres.TenantSummary `json:"tenants"`
}

type createWorkspaceRequest struct {
	Name *string `json:"name"`
}

type createWorkspaceResponse struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenantId"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
}

type workspaceListResponse struct {
	Workspaces []postgres.WorkspaceSummary `json:"workspaces"`
}

func (g *Gateway) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	if g.tenantWorkspaceStore == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "tenant management is not configured on this server")
		return
	}

	var req createTenantRequest
	if !decodeSingleJSON(w, r, g.logger, &req) {
		return
	}
	if req.Name == nil || *req.Name == "" {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "name is required")
		return
	}

	id, err := g.tenantWorkspaceStore.CreateTenant(r.Context(), *req.Name)
	if err != nil {
		if errors.Is(err, postgres.ErrTenantNameConflict) {
			writeJSONError(w, r, g.logger, http.StatusConflict, ErrorCodeConflict, "tenant name already exists")
			return
		}
		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to create tenant")
		return
	}

	tenant, err := g.tenantWorkspaceStore.GetTenant(r.Context(), id)
	if err != nil {
		// Fallback response with now as CreatedAt if fetching fails
		tenant = postgres.TenantSummary{ID: id, Name: *req.Name, CreatedAt: time.Now().UTC()}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(createTenantResponse{
		ID:        tenant.ID,
		Name:      tenant.Name,
		CreatedAt: tenant.CreatedAt,
	})
}

func (g *Gateway) handleListTenants(w http.ResponseWriter, r *http.Request) {
	if g.tenantWorkspaceStore == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "tenant management is not configured on this server")
		return
	}

	tenants, err := g.tenantWorkspaceStore.ListTenants(r.Context())
	if err != nil {
		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to list tenants")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(tenantListResponse{Tenants: tenants})
}

func (g *Gateway) handleGetTenant(w http.ResponseWriter, r *http.Request) {
	if g.tenantWorkspaceStore == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "tenant management is not configured on this server")
		return
	}

	tenantParam := r.PathValue("tenant")
	if tenantParam == "" {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "tenant identifier is required")
		return
	}

	tenant, err := g.tenantWorkspaceStore.GetTenant(r.Context(), tenantParam)
	if err != nil {
		if errors.Is(err, postgres.ErrTenantNotFound) {
			writeJSONError(w, r, g.logger, http.StatusNotFound, ErrorCodeNotFound, "tenant not found")
			return
		}
		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to get tenant")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(tenant)
}

func (g *Gateway) handleCreateWorkspace(w http.ResponseWriter, r *http.Request) {
	if g.tenantWorkspaceStore == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "workspace management is not configured on this server")
		return
	}

	tenantID, ok := TenantIDFromContext(r.Context())
	if !ok || tenantID == "" {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "missing tenant context")
		return
	}

	ctx, err := g.tenantWorkspaceStore.SetTenantContext(r.Context(), tenantID)
	if err != nil {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "invalid tenant context")
		return
	}

	var req createWorkspaceRequest
	if !decodeSingleJSON(w, r, g.logger, &req) {
		return
	}
	if req.Name == nil || *req.Name == "" {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "name is required")
		return
	}

	workspaceID, err := g.tenantWorkspaceStore.CreateWorkspace(ctx, *req.Name)
	if err != nil {
		if errors.Is(err, postgres.ErrWorkspaceNameConflict) {
			writeJSONError(w, r, g.logger, http.StatusConflict, ErrorCodeConflict, "workspace name already exists in this tenant")
			return
		}
		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to create workspace")
		return
	}

	summary, err := g.tenantWorkspaceStore.GetWorkspace(ctx, workspaceID)
	if err != nil {
		summary = postgres.WorkspaceSummary{
			ID:        workspaceID,
			TenantID:  tenantID,
			Name:      *req.Name,
			CreatedAt: time.Now().UTC(),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(createWorkspaceResponse{
		ID:        summary.ID,
		TenantID:  summary.TenantID,
		Name:      summary.Name,
		CreatedAt: summary.CreatedAt,
	})
}

func (g *Gateway) handleListWorkspaces(w http.ResponseWriter, r *http.Request) {
	if g.tenantWorkspaceStore == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "workspace management is not configured on this server")
		return
	}

	tenantID, ok := TenantIDFromContext(r.Context())
	if !ok || tenantID == "" {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "missing tenant context")
		return
	}

	ctx, err := g.tenantWorkspaceStore.SetTenantContext(r.Context(), tenantID)
	if err != nil {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "invalid tenant context")
		return
	}

	workspaces, err := g.tenantWorkspaceStore.ListWorkspaces(ctx)
	if err != nil {
		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to list workspaces")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(workspaceListResponse{Workspaces: workspaces})
}

func (g *Gateway) handleGetWorkspace(w http.ResponseWriter, r *http.Request) {
	if g.tenantWorkspaceStore == nil {
		writeJSONError(w, r, g.logger, http.StatusNotImplemented, ErrorCodeNotImplemented, "workspace management is not configured on this server")
		return
	}

	tenantID, ok := TenantIDFromContext(r.Context())
	if !ok || tenantID == "" {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "missing tenant context")
		return
	}

	ctx, err := g.tenantWorkspaceStore.SetTenantContext(r.Context(), tenantID)
	if err != nil {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "invalid tenant context")
		return
	}

	workspaceParam := r.PathValue("workspace")
	if workspaceParam == "" {
		workspaceParam = r.PathValue("repo")
	}
	if workspaceParam == "" {
		writeJSONError(w, r, g.logger, http.StatusBadRequest, ErrorCodeBadRequest, "workspace identifier is required")
		return
	}

	workspace, err := g.tenantWorkspaceStore.GetWorkspace(ctx, workspaceParam)
	if err != nil {
		if errors.Is(err, postgres.ErrWorkspaceNotFound) {
			writeJSONError(w, r, g.logger, http.StatusNotFound, ErrorCodeNotFound, "workspace not found")
			return
		}
		writeJSONError(w, r, g.logger, http.StatusInternalServerError, ErrorCodeInternal, "failed to get workspace")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(workspace)
}
