package postgres

import (
	"context"
	_ "embed"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	defaultTestPostgresAdminDSN = "postgres://spool:spoolpassword@localhost:5432/spool_rack?sslmode=disable"
	defaultTestPostgresAppDSN   = "postgres://spool_app:spool_app_dev_password@localhost:5432/spool_rack?sslmode=disable"
)

//go:embed schema.sql
var schemaSQL string

func newTestStore(t *testing.T) (*PGStore, context.Context) {
	t.Helper()

	adminDSN := getenvDefault("TEST_POSTGRES_ADMIN_DSN", defaultTestPostgresAdminDSN)
	appDSN := getenvDefault("TEST_POSTGRES_APP_DSN", defaultTestPostgresAppDSN)

	connectCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	adminConn, err := pgx.Connect(connectCtx, adminDSN)
	if err != nil {
		t.Skipf("postgres not reachable at %q: %v; run `docker compose up -d postgres` to enable this test", adminDSN, err)
	}
	t.Cleanup(func() {
		_ = adminConn.Close(context.Background())
	})

	if err := applySchema(t, adminConn); err != nil {
		t.Fatalf("apply schema: %v", err)
	}

	resetDatabase(t, adminConn)
	t.Cleanup(func() {
		resetDatabase(t, adminConn)
	})

	openCtx, openCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer openCancel()

	store, err := Open(openCtx, appDSN)
	if err != nil {
		t.Fatalf("Open(%q): %v", appDSN, err)
	}
	t.Cleanup(store.Close)

	return store, context.Background()
}

func applySchema(t *testing.T, conn *pgx.Conn) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := conn.Exec(ctx, schemaSQL)
	return err
}

func resetDatabase(t *testing.T, conn *pgx.Conn) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := conn.Exec(ctx, `TRUNCATE branches, commits, repositories, tenants CASCADE`); err != nil {
		t.Fatalf("reset database: %v", err)
	}
}

func getenvDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func mustTenantContext(t *testing.T, store *PGStore, ctx context.Context, tenantID string) context.Context {
	t.Helper()

	tenantCtx, err := store.SetTenantContext(ctx, tenantID)
	if err != nil {
		t.Fatalf("SetTenantContext(%q): %v", tenantID, err)
	}
	return tenantCtx
}

func mustCreateTenant(t *testing.T, store *PGStore, ctx context.Context, name string) string {
	t.Helper()

	tenantID, err := store.CreateTenant(ctx, name)
	if err != nil {
		t.Fatalf("CreateTenant(%q): %v", name, err)
	}
	return tenantID
}

func mustCreateRepository(t *testing.T, store *PGStore, ctx context.Context, name string) string {
	t.Helper()

	repoID, err := store.CreateRepository(ctx, name)
	if err != nil {
		t.Fatalf("CreateRepository(%q): %v", name, err)
	}
	return repoID
}

func mustPutCommit(t *testing.T, store *PGStore, ctx context.Context, repoID, parentCommitID, snapshotRoot, author, message string) string {
	t.Helper()

	commitID, err := store.PutCommit(ctx, repoID, parentCommitID, snapshotRoot, author, message)
	if err != nil {
		t.Fatalf("PutCommit(repo=%q, parent=%q): %v", repoID, parentCommitID, err)
	}
	return commitID
}

func mustCreateBranch(t *testing.T, store *PGStore, ctx context.Context, repoID, name, headCommitID string) {
	t.Helper()

	if err := store.CreateBranch(ctx, repoID, name, headCommitID); err != nil {
		t.Fatalf("CreateBranch(repo=%q, name=%q, head=%q): %v", repoID, name, headCommitID, err)
	}
}

func TestCrossTenantIsolation(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantAID := mustCreateTenant(t, store, ctx, "tenant-a")
	tenantBID := mustCreateTenant(t, store, ctx, "tenant-b")

	tenantACtx := mustTenantContext(t, store, ctx, tenantAID)
	tenantBCtx := mustTenantContext(t, store, ctx, tenantBID)

	repoAID := mustCreateRepository(t, store, tenantACtx, "repo-a")
	commitAID := mustPutCommit(t, store, tenantACtx, repoAID, "", "snap-a", "alice", "initial commit")
	mustCreateBranch(t, store, tenantACtx, repoAID, "main", commitAID)

	gotCommitID, err := store.GetBranchRef(tenantBCtx, repoAID, "main")
	if !errors.Is(err, ErrBranchNotFound) {
		t.Fatalf("GetBranchRef for foreign tenant: expected ErrBranchNotFound, got commit %q and err %v", gotCommitID, err)
	}
	if gotCommitID != "" {
		t.Fatalf("GetBranchRef for foreign tenant: got commit %q, want empty string", gotCommitID)
	}

	if err := store.CreateBranch(tenantBCtx, repoAID, "foreign-main", commitAID); err == nil {
		t.Fatal("CreateBranch for foreign tenant unexpectedly succeeded")
	}
}

func TestCompareAndSwapBranchRef_ConcurrentRace(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-race")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-race")
	commitA := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-a", "alice", "commit a")
	commitB := mustPutCommit(t, store, tenantCtx, repoID, commitA, "snap-b", "alice", "commit b")
	mustCreateBranch(t, store, tenantCtx, repoID, "main", commitA)

	const workers = 8
	errs := make([]error, workers)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = store.CompareAndSwapBranchRef(tenantCtx, repoID, "main", commitA, commitB)
		}(i)
	}
	wg.Wait()

	var successCount int
	for i, err := range errs {
		switch {
		case err == nil:
			successCount++
		case errors.Is(err, ErrNonFastForward):
		default:
			t.Fatalf("CompareAndSwapBranchRef worker %d: unexpected error %v", i, err)
		}
	}

	if successCount != 1 {
		t.Fatalf("CompareAndSwapBranchRef success count = %d, want 1", successCount)
	}

	gotHead, err := store.GetBranchRef(tenantCtx, repoID, "main")
	if err != nil {
		t.Fatalf("GetBranchRef after CAS race: %v", err)
	}
	if gotHead != commitB {
		t.Fatalf("GetBranchRef after CAS race = %q, want %q", gotHead, commitB)
	}
}

func TestCompareAndSwapBranchRef_NotFound(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-cas-not-found")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-cas-not-found")
	commitID := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-a", "alice", "initial")

	err := store.CompareAndSwapBranchRef(tenantCtx, repoID, "missing", commitID, commitID)
	if !errors.Is(err, ErrBranchNotFound) {
		t.Fatalf("CompareAndSwapBranchRef missing branch: expected ErrBranchNotFound, got %v", err)
	}
}

func TestGetBranchRef_NotFound(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-get-not-found")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-get-not-found")

	gotHead, err := store.GetBranchRef(tenantCtx, repoID, "missing")
	if !errors.Is(err, ErrBranchNotFound) {
		t.Fatalf("GetBranchRef missing branch: expected ErrBranchNotFound, got head %q and err %v", gotHead, err)
	}
	if gotHead != "" {
		t.Fatalf("GetBranchRef missing branch: got head %q, want empty string", gotHead)
	}
}

func TestCreateRepository_RequiresTenantContext(t *testing.T) {
	store, _ := newTestStore(t)

	_, err := store.CreateRepository(context.Background(), "repo-without-tenant")
	if !errors.Is(err, ErrMissingTenantContext) {
		t.Fatalf("CreateRepository without tenant context: expected ErrMissingTenantContext, got %v", err)
	}
}

func TestCreateTenant_DuplicateName(t *testing.T) {
	store, ctx := newTestStore(t)

	if _, err := store.CreateTenant(ctx, "duplicate-tenant"); err != nil {
		t.Fatalf("CreateTenant first call: %v", err)
	}

	_, err := store.CreateTenant(ctx, "duplicate-tenant")
	if !errors.Is(err, ErrTenantNameConflict) {
		t.Fatalf("CreateTenant duplicate: expected ErrTenantNameConflict, got %v", err)
	}
}
