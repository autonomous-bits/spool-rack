package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
)

type fakeTenantWorkspaceStore struct {
	mu         sync.RWMutex
	tenants    map[string]postgres.TenantSummary
	workspaces map[string]map[string]postgres.WorkspaceSummary // tenantID -> (id/name -> summary)
}

func newFakeTenantWorkspaceStore() *fakeTenantWorkspaceStore {
	return &fakeTenantWorkspaceStore{
		tenants:    make(map[string]postgres.TenantSummary),
		workspaces: make(map[string]map[string]postgres.WorkspaceSummary),
	}
}

func (f *fakeTenantWorkspaceStore) SetTenantContext(ctx context.Context, tenantID string) (context.Context, error) {
	return context.WithValue(ctx, tenantContextKey{}, tenantID), nil
}

type tenantContextKey struct{}

func (f *fakeTenantWorkspaceStore) tenantFromContext(ctx context.Context) (string, error) {
	val := ctx.Value(tenantContextKey{})
	if val == nil {
		return "", postgres.ErrMissingTenantContext
	}
	return val.(string), nil
}

func (f *fakeTenantWorkspaceStore) CreateTenant(_ context.Context, name string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, t := range f.tenants {
		if t.Name == name {
			return "", postgres.ErrTenantNameConflict
		}
	}
	id := fmt.Sprintf("tenant-%d", len(f.tenants)+1)
	f.tenants[id] = postgres.TenantSummary{
		ID:        id,
		Name:      name,
		CreatedAt: time.Now().UTC(),
	}
	return id, nil
}

func (f *fakeTenantWorkspaceStore) ListTenants(_ context.Context) ([]postgres.TenantSummary, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var result []postgres.TenantSummary
	for _, t := range f.tenants {
		result = append(result, t)
	}
	if result == nil {
		result = []postgres.TenantSummary{}
	}
	return result, nil
}

func (f *fakeTenantWorkspaceStore) GetTenant(_ context.Context, idOrName string) (postgres.TenantSummary, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if t, ok := f.tenants[idOrName]; ok {
		return t, nil
	}
	for _, t := range f.tenants {
		if t.Name == idOrName {
			return t, nil
		}
	}
	return postgres.TenantSummary{}, postgres.ErrTenantNotFound
}

func (f *fakeTenantWorkspaceStore) CreateWorkspace(ctx context.Context, name string) (string, error) {
	tenantID, err := f.tenantFromContext(ctx)
	if err != nil {
		return "", err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	wsMap := f.workspaces[tenantID]
	if wsMap == nil {
		wsMap = make(map[string]postgres.WorkspaceSummary)
		f.workspaces[tenantID] = wsMap
	}

	for _, w := range wsMap {
		if w.Name == name {
			return "", postgres.ErrWorkspaceNameConflict
		}
	}

	id := fmt.Sprintf("ws-%d", len(wsMap)+1)
	summary := postgres.WorkspaceSummary{
		ID:        id,
		TenantID:  tenantID,
		Name:      name,
		CreatedAt: time.Now().UTC(),
	}
	wsMap[id] = summary
	return id, nil
}

func (f *fakeTenantWorkspaceStore) ListWorkspaces(ctx context.Context) ([]postgres.WorkspaceSummary, error) {
	tenantID, err := f.tenantFromContext(ctx)
	if err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	var result []postgres.WorkspaceSummary
	if wsMap, ok := f.workspaces[tenantID]; ok {
		for _, w := range wsMap {
			result = append(result, w)
		}
	}
	if result == nil {
		result = []postgres.WorkspaceSummary{}
	}
	return result, nil
}

func (f *fakeTenantWorkspaceStore) GetWorkspace(ctx context.Context, idOrName string) (postgres.WorkspaceSummary, error) {
	tenantID, err := f.tenantFromContext(ctx)
	if err != nil {
		return postgres.WorkspaceSummary{}, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	if wsMap, ok := f.workspaces[tenantID]; ok {
		if w, exists := wsMap[idOrName]; exists {
			return w, nil
		}
		for _, w := range wsMap {
			if w.Name == idOrName {
				return w, nil
			}
		}
	}
	return postgres.WorkspaceSummary{}, postgres.ErrWorkspaceNotFound
}

func newTenantWorkspaceGateway(store *fakeTenantWorkspaceStore) *Gateway {
	return New(
		WithTenantWorkspaceStore(store),
		WithVerifier(auth.PermissiveVerifier{}),
	)
}

func TestCreateTenant(t *testing.T) {
	store := newFakeTenantWorkspaceStore()
	gw := newTenantWorkspaceGateway(store)

	body := []byte(`{"name":"acme-corp"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tenants", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer admin-token")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp createTenantResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Name != "acme-corp" || resp.ID == "" {
		t.Fatalf("unexpected tenant response: %+v", resp)
	}

	// Duplicate tenant returns 409 Conflict
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/tenants", bytes.NewReader(body))
	req2.Header.Set("Authorization", "Bearer admin-token")
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict, got %d", rec2.Code)
	}
}

func TestListAndGetTenant(t *testing.T) {
	store := newFakeTenantWorkspaceStore()
	_, _ = store.CreateTenant(context.Background(), "first-tenant")
	gw := newTenantWorkspaceGateway(store)

	// List tenants
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tenants", nil)
	req.Header.Set("Authorization", "Bearer any-token")
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var listResp tenantListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(listResp.Tenants) != 1 || listResp.Tenants[0].Name != "first-tenant" {
		t.Fatalf("unexpected list response: %+v", listResp)
	}

	// Get tenant by name
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/first-tenant", nil)
	getReq.Header.Set("Authorization", "Bearer any-token")
	getRec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", getRec.Code)
	}

	// Get nonexistent tenant
	notFoundReq := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/nonexistent", nil)
	notFoundReq.Header.Set("Authorization", "Bearer any-token")
	notFoundRec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(notFoundRec, notFoundReq)
	if notFoundRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found, got %d", notFoundRec.Code)
	}
}

func TestCreateAndListWorkspaces(t *testing.T) {
	store := newFakeTenantWorkspaceStore()
	gw := newTenantWorkspaceGateway(store)

	tenantID := "tenant-alpha"

	// Create workspace without tenant context fails
	noTenantReq := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces", bytes.NewReader([]byte(`{"name":"frontend"}`)))
	noTenantReq.Header.Set("Authorization", "Bearer dev-token")
	noTenantRec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(noTenantRec, noTenantReq)
	if noTenantRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", noTenantRec.Code)
	}

	// Create workspace with X-Tenant-ID
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces", bytes.NewReader([]byte(`{"name":"frontend"}`)))
	req.Header.Set("Authorization", "Bearer dev-token")
	req.Header.Set(HeaderTenantID, tenantID)
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", rec.Code, rec.Body.String())
	}

	var created createWorkspaceResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if created.Name != "frontend" || created.TenantID != tenantID || created.ID == "" {
		t.Fatalf("unexpected created workspace: %+v", created)
	}

	// List workspaces for tenant-alpha
	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces", nil)
	listReq.Header.Set("Authorization", "Bearer dev-token")
	listReq.Header.Set(HeaderTenantID, tenantID)
	listRec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", listRec.Code)
	}

	var listResp workspaceListResponse
	if err := json.Unmarshal(listRec.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listResp.Workspaces) != 1 || listResp.Workspaces[0].Name != "frontend" {
		t.Fatalf("unexpected list response: %+v", listResp)
	}

	// Get workspace by name
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces/frontend", nil)
	getReq.Header.Set("Authorization", "Bearer dev-token")
	getReq.Header.Set(HeaderTenantID, tenantID)
	getRec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", getRec.Code)
	}
}

func TestMirroredWorkspaceRoutes(t *testing.T) {
	store := newFakeTenantWorkspaceStore()
	branchStore := newFakeBranchLifecycleStore()
	branchStore.seedBranch("ws-1", "main", "c1", true)

	gw := New(
		WithTenantWorkspaceStore(store),
		WithBranchLifecycleStore(branchStore),
		WithVerifier(auth.PermissiveVerifier{}),
	)

	// Test GET /api/v1/workspaces/{workspace}/whoami
	whoamiReq := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces/ws-1/whoami", nil)
	whoamiReq.Header.Set("Authorization", "Bearer test-user")
	whoamiReq.Header.Set(HeaderTenantID, "tenant-1")
	whoamiRec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(whoamiRec, whoamiReq)
	if whoamiRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on workspace whoami, got %d: %s", whoamiRec.Code, whoamiRec.Body.String())
	}

	// Test GET /api/v1/workspaces/{workspace}/branches
	branchesReq := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces/ws-1/branches", nil)
	branchesReq.Header.Set("Authorization", "Bearer test-user")
	branchesReq.Header.Set(HeaderTenantID, "tenant-1")
	branchesRec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(branchesRec, branchesReq)
	if branchesRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on workspace branches, got %d: %s", branchesRec.Code, branchesRec.Body.String())
	}

	var branchList branchListResponse
	if err := json.Unmarshal(branchesRec.Body.Bytes(), &branchList); err != nil {
		t.Fatalf("decode branches: %v", err)
	}
	if len(branchList.Branches) != 1 || branchList.Branches[0].Name != "main" {
		t.Fatalf("unexpected branches list: %+v", branchList)
	}
}
