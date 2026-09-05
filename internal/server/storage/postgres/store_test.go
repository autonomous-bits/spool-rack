package postgres

import (
	"context"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"lukechampine.com/blake3"
)

const (
	defaultTestPostgresAdminDSN = "postgres://spool:spoolpassword@localhost:5432/spool_rack?sslmode=disable"
	defaultTestPostgresAppDSN   = "postgres://spool_app:spool_app_dev_password@localhost:5432/spool_rack?sslmode=disable"
)

//go:embed schema.sql
var schemaSQL string

var putCommitCounter uint64

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

	commitID := testCommitID(t.Name(), repoID, parentCommitID, snapshotRoot, author, message)
	if err := store.PutCommit(ctx, repoID, commitID, parentCommitID, snapshotRoot, author, message); err != nil {
		t.Fatalf("PutCommit(repo=%q, parent=%q): %v", repoID, parentCommitID, err)
	}
	return commitID
}

func testCommitID(parts ...string) string {
	seq := atomic.AddUint64(&putCommitCounter, 1)
	sum := blake3.Sum256([]byte(fmt.Sprintf("%s\x00%d", joinWithNUL(parts...), seq)))
	return hex.EncodeToString(sum[:])
}

func joinWithNUL(parts ...string) string {
	if len(parts) == 0 {
		return ""
	}
	out := parts[0]
	for _, part := range parts[1:] {
		out += "\x00" + part
	}
	return out
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

func TestPutCommit_Idempotent(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-put-commit-idempotent")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-put-commit-idempotent")

	commitID := testCommitID(t.Name(), "commit")
	if err := store.PutCommit(tenantCtx, repoID, commitID, "", "snap-a", "alice", "initial"); err != nil {
		t.Fatalf("PutCommit first call: %v", err)
	}
	if err := store.PutCommit(tenantCtx, repoID, commitID, "ignored-parent", "snap-b", "bob", "replayed"); err != nil {
		t.Fatalf("PutCommit second call: %v", err)
	}

	var (
		parent       *string
		snapshotRoot string
		author       string
		message      string
		count        int
	)
	if err := store.withTenantTx(tenantCtx, func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `
			SELECT parent_commit_id, snapshot_root, author, message
			FROM commits
			WHERE repo_id = $1 AND id = $2
		`, repoID, commitID).Scan(&parent, &snapshotRoot, &author, &message); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM commits WHERE repo_id = $1 AND id = $2`, repoID, commitID).Scan(&count); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("query commit row: %v", err)
	}

	if count != 1 {
		t.Fatalf("commit row count = %d, want 1", count)
	}
	if parent != nil {
		t.Fatalf("parent_commit_id = %q, want NULL", *parent)
	}
	if snapshotRoot != "snap-a" || author != "alice" || message != "initial" {
		t.Fatalf("stored metadata = (%q, %q, %q), want (%q, %q, %q)", snapshotRoot, author, message, "snap-a", "alice", "initial")
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

func TestIsAncestor(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-is-ancestor")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-is-ancestor")

	rootCommit := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-root", "alice", "root")
	childCommit := mustPutCommit(t, store, tenantCtx, repoID, rootCommit, "snap-child", "alice", "child")
	grandchildCommit := mustPutCommit(t, store, tenantCtx, repoID, childCommit, "snap-grandchild", "alice", "grandchild")
	sideRootCommit := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-side-root", "alice", "side root")

	tests := []struct {
		name           string
		ancestorCommit string
		commit         string
		want           bool
	}{
		{
			name:           "direct parent",
			ancestorCommit: childCommit,
			commit:         grandchildCommit,
			want:           true,
		},
		{
			name:           "multi hop ancestor",
			ancestorCommit: rootCommit,
			commit:         grandchildCommit,
			want:           true,
		},
		{
			name:           "self",
			ancestorCommit: grandchildCommit,
			commit:         grandchildCommit,
			want:           true,
		},
		{
			name:           "unrelated commit",
			ancestorCommit: sideRootCommit,
			commit:         grandchildCommit,
			want:           false,
		},
		{
			name:           "missing start commit",
			ancestorCommit: rootCommit,
			commit:         testCommitID(t.Name(), "missing-start-commit"),
			want:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.IsAncestor(tenantCtx, repoID, tt.ancestorCommit, tt.commit)
			if err != nil {
				t.Fatalf("IsAncestor(%q, %q): %v", tt.ancestorCommit, tt.commit, err)
			}
			if got != tt.want {
				t.Fatalf("IsAncestor(%q, %q) = %t, want %t", tt.ancestorCommit, tt.commit, got, tt.want)
			}
		})
	}
}

func TestFindLowestCommonAncestor(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-find-lca")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-find-lca")

	root := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-root", "alice", "root")
	base := mustPutCommit(t, store, tenantCtx, repoID, root, "snap-base", "alice", "base")
	source := mustPutCommit(t, store, tenantCtx, repoID, base, "snap-source", "alice", "source")
	target := mustPutCommit(t, store, tenantCtx, repoID, base, "snap-target", "bob", "target")

	got, err := store.FindLowestCommonAncestor(tenantCtx, repoID, source, target)
	if err != nil {
		t.Fatalf("FindLowestCommonAncestor: %v", err)
	}
	if got != base {
		t.Fatalf("FindLowestCommonAncestor = %q, want %q", got, base)
	}

	got, err = store.FindLowestCommonAncestor(tenantCtx, repoID, source, source)
	if err != nil {
		t.Fatalf("FindLowestCommonAncestor(self): %v", err)
	}
	if got != source {
		t.Fatalf("FindLowestCommonAncestor(self) = %q, want %q", got, source)
	}

	unrelated := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-unrelated", "bob", "unrelated")
	got, err = store.FindLowestCommonAncestor(tenantCtx, repoID, source, unrelated)
	if !errors.Is(err, ErrNoCommonAncestor) {
		t.Fatalf("FindLowestCommonAncestor(unrelated): expected ErrNoCommonAncestor, got ancestor %q and err %v", got, err)
	}

	tenantBID := mustCreateTenant(t, store, ctx, "tenant-find-lca-foreign")
	tenantBCtx := mustTenantContext(t, store, ctx, tenantBID)
	got, err = store.FindLowestCommonAncestor(tenantBCtx, repoID, source, target)
	if !errors.Is(err, ErrCommitNotFound) {
		t.Fatalf("FindLowestCommonAncestor(foreign tenant): expected ErrCommitNotFound, got ancestor %q and err %v", got, err)
	}

	_, err = store.FindLowestCommonAncestor(context.Background(), repoID, source, target)
	if !errors.Is(err, ErrMissingTenantContext) {
		t.Fatalf("FindLowestCommonAncestor(no tenant context): expected ErrMissingTenantContext, got %v", err)
	}
}

func TestFindLowestCommonAncestor_HistoryErrorsAndBound(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-lca-history-errors")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-lca-history-errors")
	root := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-root", "alice", "root")

	_, err := store.FindLowestCommonAncestor(tenantCtx, repoID, testCommitID(t.Name(), "missing"), root)
	if !errors.Is(err, ErrCommitNotFound) {
		t.Fatalf("FindLowestCommonAncestor(missing): expected ErrCommitNotFound, got %v", err)
	}

	otherRepoID := mustCreateRepository(t, store, tenantCtx, "repo-other-history")
	foreignParent := mustPutCommit(t, store, tenantCtx, otherRepoID, "", "snap-foreign", "alice", "foreign")
	incomplete := mustPutCommit(t, store, tenantCtx, repoID, foreignParent, "snap-incomplete", "alice", "incomplete")
	_, err = store.FindLowestCommonAncestor(tenantCtx, repoID, incomplete, incomplete)
	if !errors.Is(err, ErrCommitHistoryIncomplete) {
		t.Fatalf("FindLowestCommonAncestor(incomplete): expected ErrCommitHistoryIncomplete, got %v", err)
	}

	tip := root
	for i := 1; i < maxMergeAncestryCommits; i++ {
		tip = mustPutCommit(t, store, tenantCtx, repoID, tip, fmt.Sprintf("snap-%d", i), "alice", fmt.Sprintf("commit-%d", i))
	}
	if got, err := store.FindLowestCommonAncestor(tenantCtx, repoID, tip, root); err != nil || got != root {
		t.Fatalf("FindLowestCommonAncestor(%d commits) = (%q, %v), want (%q, nil)", maxMergeAncestryCommits, got, err, root)
	}

	tooDeep := mustPutCommit(t, store, tenantCtx, repoID, tip, "snap-too-deep", "alice", "too deep")
	_, err = store.FindLowestCommonAncestor(tenantCtx, repoID, tooDeep, root)
	if !errors.Is(err, ErrCommitHistoryTooDeep) {
		t.Fatalf("FindLowestCommonAncestor(over limit): expected ErrCommitHistoryTooDeep, got %v", err)
	}
}

func TestGetCommitSnapshotRoot(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantAID := mustCreateTenant(t, store, ctx, "tenant-snapshot-a")
	tenantBID := mustCreateTenant(t, store, ctx, "tenant-snapshot-b")
	tenantACtx := mustTenantContext(t, store, ctx, tenantAID)
	tenantBCtx := mustTenantContext(t, store, ctx, tenantBID)
	repoID := mustCreateRepository(t, store, tenantACtx, "repo-snapshot")
	commitID := mustPutCommit(t, store, tenantACtx, repoID, "", "snap-root", "alice", "root")

	got, err := store.GetCommitSnapshotRoot(tenantACtx, repoID, commitID)
	if err != nil {
		t.Fatalf("GetCommitSnapshotRoot: %v", err)
	}
	if got != "snap-root" {
		t.Fatalf("GetCommitSnapshotRoot = %q, want %q", got, "snap-root")
	}

	got, err = store.GetCommitSnapshotRoot(tenantACtx, repoID, testCommitID(t.Name(), "missing"))
	if !errors.Is(err, ErrCommitNotFound) {
		t.Fatalf("GetCommitSnapshotRoot(missing): expected ErrCommitNotFound, got root %q and err %v", got, err)
	}

	got, err = store.GetCommitSnapshotRoot(tenantBCtx, repoID, commitID)
	if !errors.Is(err, ErrCommitNotFound) {
		t.Fatalf("GetCommitSnapshotRoot(foreign tenant): expected ErrCommitNotFound, got root %q and err %v", got, err)
	}

	_, err = store.GetCommitSnapshotRoot(context.Background(), repoID, commitID)
	if !errors.Is(err, ErrMissingTenantContext) {
		t.Fatalf("GetCommitSnapshotRoot(no tenant context): expected ErrMissingTenantContext, got %v", err)
	}
}

func TestGetPackRanges(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-pack-ranges")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-pack-ranges")
	root := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-root", "alice", "root")
	middle := mustPutCommit(t, store, tenantCtx, repoID, root, "snap-middle", "alice", "middle")
	head := mustPutCommit(t, store, tenantCtx, repoID, middle, "snap-head", "alice", "head")

	rootPack := testCommitID(t.Name(), "root-pack")
	middlePack := testCommitID(t.Name(), "middle-pack")
	headPack := testCommitID(t.Name(), "head-pack")
	for _, pack := range []struct {
		hash   string
		base   string
		target string
	}{
		{hash: rootPack, base: "", target: root},
		{hash: middlePack, base: root, target: middle},
		{hash: headPack, base: middle, target: head},
	} {
		if err := store.PutPackRange(tenantCtx, repoID, pack.hash, pack.base, pack.target); err != nil {
			t.Fatalf("PutPackRange(%q): %v", pack.hash, err)
		}
	}

	ranges, err := store.GetPackRanges(tenantCtx, repoID, head, middle)
	if err != nil {
		t.Fatalf("GetPackRanges(delta): %v", err)
	}
	if len(ranges) != 1 || ranges[0].PackHash != headPack {
		t.Fatalf("GetPackRanges(delta) = %+v, want only head pack", ranges)
	}

	ranges, err = store.GetPackRanges(tenantCtx, repoID, head, "")
	if err != nil {
		t.Fatalf("GetPackRanges(full): %v", err)
	}
	if len(ranges) != 3 {
		t.Fatalf("GetPackRanges(full) length = %d, want 3", len(ranges))
	}
	for i, want := range []string{headPack, middlePack, rootPack} {
		if ranges[i].PackHash != want {
			t.Fatalf("GetPackRanges(full)[%d] = %q, want %q", i, ranges[i].PackHash, want)
		}
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
