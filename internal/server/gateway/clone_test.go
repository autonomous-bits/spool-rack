package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	serversync "github.com/autonomous-bits/spool-rack/internal/server/sync"
)

func TestCloneStreamsFullHistoryWithDefaultBranch(t *testing.T) {
	t.Parallel()

	driver, store, _, _, head := pullFixture(t)
	lifecycleStore := newFakeBranchLifecycleStore()
	lifecycleStore.defaultBranch["repo-1"] = "main"
	lifecycleStore.branches["repo-1"] = map[string]postgres.BranchRef{
		"main": {Name: "main", HeadCommitID: head},
	}

	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithBranchLifecycleStore(lifecycleStore),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"viewer-token": {Role: auth.RoleViewer, Subject: "viewer"},
		})),
	)

	for _, path := range []string{
		"/api/v1/repos/repo-1/clone",
		"/api/v1/workspaces/repo-1/clone",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer viewer-token")
		req.Header.Set(HeaderTenantID, "tenant-1")

		rec := httptest.NewRecorder()
		gw.Routes().ServeHTTP(rec, req)

		assertStatus(t, rec, http.StatusOK)
		if got := rec.Header().Get("Content-Type"); got != "application/vnd.spool-rack.pull-envelope" {
			t.Fatalf("path %s Content-Type = %q, want application/vnd.spool-rack.pull-envelope", path, got)
		}
		if got := rec.Header().Get("Content-Encoding"); got != "zstd" {
			t.Fatalf("path %s Content-Encoding = %q, want zstd", path, got)
		}
		if got := rec.Header().Get("X-Spool-Pull-Format"); got != "2" {
			t.Fatalf("path %s X-Spool-Pull-Format = %q, want 2", path, got)
		}
		if got := rec.Header().Get("X-Spool-Head-Commit"); got != head {
			t.Fatalf("path %s X-Spool-Head-Commit = %q, want %q", path, got, head)
		}
		if got := rec.Header().Get("X-Spool-Branch"); got != "main" {
			t.Fatalf("path %s X-Spool-Branch = %q, want main", path, got)
		}
		if got := rec.Header().Get("X-Spool-Default-Branch"); got != "main" {
			t.Fatalf("path %s X-Spool-Default-Branch = %q, want main", path, got)
		}

		manifest, packs := decodePullEnvelope(t, rec.Body.Bytes())
		if manifest.Version != serversync.PullEnvelopeFormatV2 || manifest.Head != head {
			t.Fatalf("manifest = %+v, want v2 head %q", manifest, head)
		}
		if len(packs) != 3 {
			t.Fatalf("pack count = %d, want 3", len(packs))
		}
	}
}

func TestCloneExplicitBranch(t *testing.T) {
	t.Parallel()

	driver, store, _, _, head := pullFixture(t)
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"viewer-token": {Role: auth.RoleViewer, Subject: "viewer"},
		})),
	)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces/repo-1/clone?branch=main", nil)
	req.Header.Set("Authorization", "Bearer viewer-token")
	req.Header.Set(HeaderTenantID, "tenant-1")

	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusOK)
	if got := rec.Header().Get("X-Spool-Branch"); got != "main" {
		t.Fatalf("X-Spool-Branch = %q, want main", got)
	}
	manifest, packs := decodePullEnvelope(t, rec.Body.Bytes())
	if manifest.Head != head || len(packs) != 3 {
		t.Fatalf("manifest head = %q, packs = %d; want %q and 3", manifest.Head, len(packs), head)
	}
}

func TestCloneEmptyWorkspace(t *testing.T) {
	t.Parallel()

	driver, store, _, _, _ := pullFixture(t)
	lifecycleStore := newFakeBranchLifecycleStore()
	// No default branch configured for repo-empty

	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithBranchLifecycleStore(lifecycleStore),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"viewer-token": {Role: auth.RoleViewer, Subject: "viewer"},
		})),
	)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces/repo-empty/clone", nil)
	req.Header.Set("Authorization", "Bearer viewer-token")
	req.Header.Set(HeaderTenantID, "tenant-1")

	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusOK)
	if got := rec.Header().Get("X-Spool-Empty"); got != "true" {
		t.Fatalf("X-Spool-Empty = %q, want true", got)
	}
	if got := rec.Header().Get("X-Spool-Default-Branch"); got != "main" {
		t.Fatalf("X-Spool-Default-Branch = %q, want main", got)
	}
}

func TestCloneRejectsMissingBranch(t *testing.T) {
	t.Parallel()

	driver, store, _, _, _ := pullFixture(t)
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"viewer-token": {Role: auth.RoleViewer, Subject: "viewer"},
		})),
	)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces/repo-1/clone?branch=non-existent", nil)
	req.Header.Set("Authorization", "Bearer viewer-token")
	req.Header.Set(HeaderTenantID, "tenant-1")

	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusNotFound)
}

func TestCloneRequiresAuthentication(t *testing.T) {
	t.Parallel()

	driver, store, _, _, _ := pullFixture(t)
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
	)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces/repo-1/clone", nil)
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusUnauthorized)
}

func TestCloneReturnsNotImplementedWhenUnconfigured(t *testing.T) {
	t.Parallel()

	gw := New(
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"viewer-token": {Role: auth.RoleViewer, Subject: "viewer"},
		})),
	)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces/repo-1/clone", nil)
	req.Header.Set("Authorization", "Bearer viewer-token")
	req.Header.Set(HeaderTenantID, "tenant-1")

	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusNotImplemented)
}
