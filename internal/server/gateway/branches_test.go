package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
)

// fakeBranchLifecycleStore is a lightweight in-memory double for
// BranchLifecycleStore, kept separate from fakeGatewayBranchStore (used by
// push/pull tests) since it exercises a different narrow interface slice of
// postgres.Store.
type fakeBranchLifecycleStore struct {
	branches      map[string]map[string]postgres.BranchRef // repoID -> name -> ref
	defaultBranch map[string]string                        // repoID -> name

	createBranchErr     error
	getBranchRefErr     error
	listBranchRefsErr   error
	deleteBranchErr     error
	getDefaultBranchErr error

	createCalls int
	deleteCalls int
}

func newFakeBranchLifecycleStore() *fakeBranchLifecycleStore {
	return &fakeBranchLifecycleStore{
		branches:      map[string]map[string]postgres.BranchRef{},
		defaultBranch: map[string]string{},
	}
}

func (f *fakeBranchLifecycleStore) SetTenantContext(ctx context.Context, _ string) (context.Context, error) {
	return ctx, nil
}

func (f *fakeBranchLifecycleStore) seedBranch(repoID, name, headCommitID string, asDefault bool) {
	if f.branches[repoID] == nil {
		f.branches[repoID] = map[string]postgres.BranchRef{}
	}
	f.branches[repoID][name] = postgres.BranchRef{Name: name, HeadCommitID: headCommitID}
	if asDefault {
		f.defaultBranch[repoID] = name
	}
}

func (f *fakeBranchLifecycleStore) CreateBranch(_ context.Context, repoID, name, headCommitID string) error {
	f.createCalls++
	if f.createBranchErr != nil {
		return f.createBranchErr
	}
	if f.branches[repoID] == nil {
		f.branches[repoID] = map[string]postgres.BranchRef{}
	}
	if _, exists := f.branches[repoID][name]; exists {
		return postgres.ErrBranchAlreadyExists
	}
	f.branches[repoID][name] = postgres.BranchRef{Name: name, HeadCommitID: headCommitID}
	if _, hasDefault := f.defaultBranch[repoID]; !hasDefault {
		f.defaultBranch[repoID] = name
	}
	return nil
}

func (f *fakeBranchLifecycleStore) GetBranchRef(_ context.Context, repoID, branch string) (string, error) {
	if f.getBranchRefErr != nil {
		return "", f.getBranchRefErr
	}
	ref, ok := f.branches[repoID][branch]
	if !ok {
		return "", postgres.ErrBranchNotFound
	}
	return ref.HeadCommitID, nil
}

func (f *fakeBranchLifecycleStore) ListBranchRefs(_ context.Context, repoID string) ([]postgres.BranchRef, error) {
	if f.listBranchRefsErr != nil {
		return nil, f.listBranchRefsErr
	}
	refs := make([]postgres.BranchRef, 0, len(f.branches[repoID]))
	for _, ref := range f.branches[repoID] {
		refs = append(refs, ref)
	}
	return refs, nil
}

func (f *fakeBranchLifecycleStore) DeleteBranch(_ context.Context, repoID, name string) error {
	f.deleteCalls++
	if f.deleteBranchErr != nil {
		return f.deleteBranchErr
	}
	if f.defaultBranch[repoID] == name {
		return postgres.ErrDefaultBranchProtected
	}
	ref, ok := f.branches[repoID][name]
	if !ok || ref.Deleted {
		return postgres.ErrBranchNotFound
	}
	ref.Deleted = true
	f.branches[repoID][name] = ref
	return nil
}

func (f *fakeBranchLifecycleStore) GetDefaultBranch(_ context.Context, repoID string) (postgres.BranchRef, error) {
	if f.getDefaultBranchErr != nil {
		return postgres.BranchRef{}, f.getDefaultBranchErr
	}
	name, ok := f.defaultBranch[repoID]
	if !ok {
		return postgres.BranchRef{}, postgres.ErrDefaultBranchNotSet
	}
	return f.branches[repoID][name], nil
}

var _ BranchLifecycleStore = (*fakeBranchLifecycleStore)(nil)

func newBranchLifecycleGateway(store *fakeBranchLifecycleStore) *Gateway {
	return New(
		WithBranchLifecycleStore(store),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"contributor-token": {Role: auth.RoleContributor, Subject: "u1"},
			"viewer-token":      {Role: auth.RoleViewer, Subject: "u2"},
		})),
	)
}

func newBranchLifecycleRequest(method, path, token, tenantID string, body any) *http.Request {
	var req *http.Request
	if body != nil {
		payload, _ := json.Marshal(body)
		req = httptest.NewRequest(method, path, bytes.NewReader(payload))
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set(HeaderTenantID, tenantID)
	return req
}

func TestHandleCreateBranch_FromSourceBranch(t *testing.T) {
	t.Parallel()

	store := newFakeBranchLifecycleStore()
	store.seedBranch("repo-1", "main", "commit-1", true)
	gw := newBranchLifecycleGateway(store)

	req := newBranchLifecycleRequest(http.MethodPost, "/api/v1/repos/repo-1/branches", "contributor-token", "tenant-1",
		map[string]string{"name": "feature", "sourceBranch": "main"})
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusCreated)
	var resp branchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Name != "feature" || resp.HeadCommit != "commit-1" {
		t.Fatalf("response = %+v, want name=feature headCommit=commit-1", resp)
	}
	if store.createCalls != 1 {
		t.Fatalf("CreateBranch calls = %d, want 1", store.createCalls)
	}
}

func TestHandleCreateBranch_FromSourceCommit(t *testing.T) {
	t.Parallel()

	store := newFakeBranchLifecycleStore()
	store.seedBranch("repo-1", "main", "commit-1", true)
	gw := newBranchLifecycleGateway(store)

	req := newBranchLifecycleRequest(http.MethodPost, "/api/v1/repos/repo-1/branches", "contributor-token", "tenant-1",
		map[string]string{"name": "feature", "sourceCommit": "commit-9"})
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusCreated)
	var resp branchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.HeadCommit != "commit-9" {
		t.Fatalf("response = %+v, want headCommit=commit-9", resp)
	}
}

func TestHandleCreateBranch_RequiresExactlyOneSource(t *testing.T) {
	t.Parallel()

	store := newFakeBranchLifecycleStore()
	gw := newBranchLifecycleGateway(store)

	cases := []map[string]string{
		{"name": "feature"},
		{"name": "feature", "sourceBranch": "main", "sourceCommit": "commit-9"},
	}
	for _, body := range cases {
		req := newBranchLifecycleRequest(http.MethodPost, "/api/v1/repos/repo-1/branches", "contributor-token", "tenant-1", body)
		rec := httptest.NewRecorder()
		gw.Routes().ServeHTTP(rec, req)

		assertStatus(t, rec, http.StatusBadRequest)
		assertErrorCode(t, rec, ErrorCodeBadRequest)
	}
}

func TestHandleCreateBranch_SourceBranchNotFound(t *testing.T) {
	t.Parallel()

	store := newFakeBranchLifecycleStore()
	gw := newBranchLifecycleGateway(store)

	req := newBranchLifecycleRequest(http.MethodPost, "/api/v1/repos/repo-1/branches", "contributor-token", "tenant-1",
		map[string]string{"name": "feature", "sourceBranch": "missing"})
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusNotFound)
	assertErrorCode(t, rec, ErrorCodeBranchSourceNotFound)
}

func TestHandleCreateBranch_NameAlreadyExists(t *testing.T) {
	t.Parallel()

	store := newFakeBranchLifecycleStore()
	store.seedBranch("repo-1", "main", "commit-1", true)
	gw := newBranchLifecycleGateway(store)

	req := newBranchLifecycleRequest(http.MethodPost, "/api/v1/repos/repo-1/branches", "contributor-token", "tenant-1",
		map[string]string{"name": "main", "sourceBranch": "main"})
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusConflict)
	assertErrorCode(t, rec, ErrorCodeBranchAlreadyExists)
}

func TestHandleCreateBranch_ForbiddenForViewer(t *testing.T) {
	t.Parallel()

	store := newFakeBranchLifecycleStore()
	store.seedBranch("repo-1", "main", "commit-1", true)
	gw := newBranchLifecycleGateway(store)

	req := newBranchLifecycleRequest(http.MethodPost, "/api/v1/repos/repo-1/branches", "viewer-token", "tenant-1",
		map[string]string{"name": "feature", "sourceBranch": "main"})
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusForbidden)
	if store.createCalls != 0 {
		t.Fatalf("CreateBranch calls = %d, want 0", store.createCalls)
	}
}

func TestHandleListBranches(t *testing.T) {
	t.Parallel()

	store := newFakeBranchLifecycleStore()
	store.seedBranch("repo-1", "main", "commit-1", true)
	store.seedBranch("repo-1", "feature", "commit-2", false)
	store.branches["repo-1"]["stale"] = postgres.BranchRef{Name: "stale", HeadCommitID: "commit-3", Deleted: true}
	gw := newBranchLifecycleGateway(store)

	req := newBranchLifecycleRequest(http.MethodGet, "/api/v1/repos/repo-1/branches", "viewer-token", "tenant-1", nil)
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusOK)
	var resp branchListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Branches) != 2 {
		t.Fatalf("Branches = %+v, want 2 entries (deleted branch excluded)", resp.Branches)
	}
	byName := map[string]branchListEntry{}
	for _, entry := range resp.Branches {
		byName[entry.Name] = entry
	}
	if !byName["main"].Default {
		t.Fatalf("main entry = %+v, want default=true", byName["main"])
	}
	if byName["feature"].Default {
		t.Fatalf("feature entry = %+v, want default=false", byName["feature"])
	}
	if _, ok := byName["stale"]; ok {
		t.Fatalf("Branches = %+v, want soft-deleted branch excluded", resp.Branches)
	}
}

func TestHandleDefaultBranch(t *testing.T) {
	t.Parallel()

	store := newFakeBranchLifecycleStore()
	store.seedBranch("repo-1", "main", "commit-1", true)
	gw := newBranchLifecycleGateway(store)

	req := newBranchLifecycleRequest(http.MethodGet, "/api/v1/repos/repo-1/branches/default", "viewer-token", "tenant-1", nil)
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusOK)
	var resp branchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Name != "main" || resp.HeadCommit != "commit-1" {
		t.Fatalf("response = %+v, want name=main headCommit=commit-1", resp)
	}
}

func TestHandleDefaultBranch_NotSet(t *testing.T) {
	t.Parallel()

	store := newFakeBranchLifecycleStore()
	gw := newBranchLifecycleGateway(store)

	req := newBranchLifecycleRequest(http.MethodGet, "/api/v1/repos/repo-1/branches/default", "viewer-token", "tenant-1", nil)
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusNotFound)
	assertErrorCode(t, rec, ErrorCodeBranchNotFound)
}

func TestHandleDeleteBranch_Success(t *testing.T) {
	t.Parallel()

	store := newFakeBranchLifecycleStore()
	store.seedBranch("repo-1", "main", "commit-1", true)
	store.seedBranch("repo-1", "feature", "commit-2", false)
	gw := newBranchLifecycleGateway(store)

	req := newBranchLifecycleRequest(http.MethodDelete, "/api/v1/repos/repo-1/branches/feature", "contributor-token", "tenant-1", nil)
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusNoContent)
	if store.deleteCalls != 1 {
		t.Fatalf("DeleteBranch calls = %d, want 1", store.deleteCalls)
	}
}

func TestHandleDeleteBranch_ProtectsDefaultBranch(t *testing.T) {
	t.Parallel()

	store := newFakeBranchLifecycleStore()
	store.seedBranch("repo-1", "main", "commit-1", true)
	gw := newBranchLifecycleGateway(store)

	req := newBranchLifecycleRequest(http.MethodDelete, "/api/v1/repos/repo-1/branches/main", "contributor-token", "tenant-1", nil)
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusConflict)
	assertErrorCode(t, rec, ErrorCodeBranchProtected)
}

func TestHandleDeleteBranch_NotFound(t *testing.T) {
	t.Parallel()

	store := newFakeBranchLifecycleStore()
	store.seedBranch("repo-1", "main", "commit-1", true)
	gw := newBranchLifecycleGateway(store)

	req := newBranchLifecycleRequest(http.MethodDelete, "/api/v1/repos/repo-1/branches/missing", "contributor-token", "tenant-1", nil)
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusNotFound)
	assertErrorCode(t, rec, ErrorCodeBranchNotFound)
}

func TestHandleDeleteBranch_ForbiddenForViewer(t *testing.T) {
	t.Parallel()

	store := newFakeBranchLifecycleStore()
	store.seedBranch("repo-1", "main", "commit-1", true)
	store.seedBranch("repo-1", "feature", "commit-2", false)
	gw := newBranchLifecycleGateway(store)

	req := newBranchLifecycleRequest(http.MethodDelete, "/api/v1/repos/repo-1/branches/feature", "viewer-token", "tenant-1", nil)
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusForbidden)
	if store.deleteCalls != 0 {
		t.Fatalf("DeleteBranch calls = %d, want 0", store.deleteCalls)
	}
}

func TestHandleBranchLifecycle_NotConfigured(t *testing.T) {
	t.Parallel()

	gw := New(
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"contributor-token": {Role: auth.RoleContributor, Subject: "u1"},
		})),
	)

	req := newBranchLifecycleRequest(http.MethodPost, "/api/v1/repos/repo-1/branches", "contributor-token", "tenant-1",
		map[string]string{"name": "feature", "sourceBranch": "main"})
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusNotImplemented)
	assertErrorCode(t, rec, ErrorCodeNotImplemented)
}
