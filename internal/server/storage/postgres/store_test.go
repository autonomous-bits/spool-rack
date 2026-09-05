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
var postgresTestDatabaseMu sync.Mutex

func newTestStore(t *testing.T) (*PGStore, context.Context) {
	t.Helper()

	// The integration tests share one database and schema bootstrap truncates
	// every tenant table. Keep a test's setup, execution, and cleanup together
	// even when another caller marks its test parallel.
	postgresTestDatabaseMu.Lock()
	t.Cleanup(postgresTestDatabaseMu.Unlock)

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

func TestCompareAndSwapBranchRef_RespectsTargetBranchMergeLease(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-cas-merge-lease")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-cas-merge-lease")
	base := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-base", "alice", "base")
	target := mustPutCommit(t, store, tenantCtx, repoID, base, "snap-target", "alice", "target")
	next := mustPutCommit(t, store, tenantCtx, repoID, target, "snap-next", "alice", "next")
	source := mustPutCommit(t, store, tenantCtx, repoID, base, "snap-source", "bob", "source")
	mustCreateBranch(t, store, tenantCtx, repoID, "main", target)

	_, err := store.AcquireMergeLease(tenantCtx, MergeLeaseRequest{
		RepoID: repoID, TargetBranch: "main", Subject: "alice",
		SourceCommitID: source, TargetCommitID: target, BaseCommitID: base, Duration: time.Minute,
	})
	if err != nil {
		t.Fatalf("AcquireMergeLease active: %v", err)
	}
	if err := store.CompareAndSwapBranchRef(tenantCtx, repoID, "main", target, next); !errors.Is(err, ErrMergeLeaseHeld) {
		t.Fatalf("CompareAndSwapBranchRef active lease: expected ErrMergeLeaseHeld, got %v", err)
	}
	head, err := store.GetBranchRef(tenantCtx, repoID, "main")
	if err != nil || head != target {
		t.Fatalf("GetBranchRef after rejected CAS = (%q, %v), want (%q, nil)", head, err, target)
	}

	mustCreateBranch(t, store, tenantCtx, repoID, "expired", target)
	_, err = store.AcquireMergeLease(tenantCtx, MergeLeaseRequest{
		RepoID: repoID, TargetBranch: "expired", Subject: "alice",
		SourceCommitID: source, TargetCommitID: target, BaseCommitID: base, Duration: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("AcquireMergeLease expired: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	if err := store.CompareAndSwapBranchRef(tenantCtx, repoID, "expired", target, next); err != nil {
		t.Fatalf("CompareAndSwapBranchRef expired lease: %v", err)
	}
	head, err = store.GetBranchRef(tenantCtx, repoID, "expired")
	if err != nil || head != next {
		t.Fatalf("GetBranchRef after expired lease CAS = (%q, %v), want (%q, nil)", head, err, next)
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
	if err := store.PutCommit(tenantCtx, repoID, commitID, "", "snap-a", "alice", "initial"); err != nil {
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
	if err := store.PutCommit(tenantCtx, repoID, testCommitID(t.Name(), "cross-repo-parent"), foreignParent, "snap-incomplete", "alice", "incomplete"); err == nil {
		t.Fatal("PutCommit(cross-repository parent) error = nil, want scoped foreign-key rejection")
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

func TestCommitIdentityIsRepositoryScopedAndImmutable(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-scoped-v2-identity")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoA := mustCreateRepository(t, store, tenantCtx, "repo-v2-a")
	repoB := mustCreateRepository(t, store, tenantCtx, "repo-v2-b")
	commitID := testCommitID(t.Name(), "same-v2-frame")
	if err := store.PutCommitWithFormat(tenantCtx, repoA, commitID, "", "snapshot", "Ada", "same frame", 2); err != nil {
		t.Fatalf("PutCommitWithFormat(repo A): %v", err)
	}
	if err := store.PutCommitWithFormat(tenantCtx, repoB, commitID, "", "snapshot", "Ada", "same frame", 2); err != nil {
		t.Fatalf("PutCommitWithFormat(repo B): %v", err)
	}
	if err := store.PutCommitWithFormat(tenantCtx, repoA, commitID, "", "snapshot", "Mallory", "poisoned frame", 1); !errors.Is(err, ErrImmutableMetadataMismatch) {
		t.Fatalf("PutCommitWithFormat(conflicting ID) error = %v, want immutable metadata mismatch", err)
	}
	for _, repoID := range []string{repoA, repoB} {
		metadata, err := store.GetCommitMetadata(tenantCtx, repoID, commitID)
		if err != nil || metadata.Format != 2 || metadata.SnapshotRoot != "snapshot" {
			t.Fatalf("GetCommitMetadata(%s) = %+v, %v; want scoped v2 commit", repoID, metadata, err)
		}
	}
}

func TestSnapshotMigrationFreezesWritersMapsMergeDAGAndRollsBack(t *testing.T) {
	store, ctx := newTestStore(t)
	tenantAID := mustCreateTenant(t, store, ctx, "tenant-snapshot-migration-a")
	tenantBID := mustCreateTenant(t, store, ctx, "tenant-snapshot-migration-b")
	tenantACtx := mustTenantContext(t, store, ctx, tenantAID)
	tenantBCtx := mustTenantContext(t, store, ctx, tenantBID)
	repoID := mustCreateRepository(t, store, tenantACtx, "repo-snapshot-migration")

	root := mustPutCommit(t, store, tenantACtx, repoID, "", "legacy-snapshot-root", "Ada", "root")
	target := mustPutCommit(t, store, tenantACtx, repoID, root, "legacy-snapshot-target", "Ada", "target")
	source := mustPutCommit(t, store, tenantACtx, repoID, root, "legacy-snapshot-source", "Ada", "source")
	merge := mustPutCommit(t, store, tenantACtx, repoID, target, "legacy-snapshot-merge", "Ada", "merge")
	if err := store.withTenantTx(tenantACtx, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO commit_parents (tenant_id, repo_id, commit_id, parent_position, parent_commit_id)
			VALUES ($1, $2, $3, 2, $4)
		`, tenantAID, repoID, merge, source)
		return err
	}); err != nil {
		t.Fatalf("add merge second parent: %v", err)
	}
	mustCreateBranch(t, store, tenantACtx, repoID, "main", merge)

	packs := []struct{ hash, base, target string }{
		{"legacy-pack-root", "", root},
		{"legacy-pack-target", root, target},
		{"legacy-pack-source", root, source},
		{"legacy-pack-merge", target, merge},
	}
	for _, pack := range packs {
		if err := store.PutPackRange(tenantACtx, repoID, pack.hash, pack.base, pack.target); err != nil {
			t.Fatalf("PutPackRange(%s): %v", pack.hash, err)
		}
	}

	migration, err := store.BeginSnapshotMigration(tenantACtx, repoID)
	if err != nil {
		t.Fatalf("BeginSnapshotMigration() error = %v", err)
	}
	resumed, err := store.BeginSnapshotMigration(tenantACtx, repoID)
	if err != nil || migration.Generation == "" || resumed.Generation != migration.Generation || migration.Status != "frozen" {
		t.Fatalf("migration/resume = %+v/%+v/%v", migration, resumed, err)
	}
	if err := store.PutCommit(tenantACtx, repoID, testCommitID(t.Name(), "blocked"), merge, "blocked", "Ada", "blocked"); !errors.Is(err, ErrRepositoryMigrationFrozen) {
		t.Fatalf("PutCommit while frozen error = %v, want writer freeze", err)
	}

	legacyRoots := map[string]string{root: "legacy-snapshot-root", target: "legacy-snapshot-target", source: "legacy-snapshot-source", merge: "legacy-snapshot-merge"}
	v2IDs := map[string]string{root: testCommitID(t.Name(), "v2-root"), target: testCommitID(t.Name(), "v2-target"), source: testCommitID(t.Name(), "v2-source"), merge: testCommitID(t.Name(), "v2-merge")}
	for legacyID, legacyRoot := range legacyRoots {
		cborRoot := "cbor-" + legacyRoot
		if err := store.StageSnapshotObjectMapping(tenantACtx, repoID, legacyRoot, cborRoot); err != nil {
			t.Fatal(err)
		}
		if err := store.StageSnapshotCommitMapping(tenantACtx, repoID, legacyID, v2IDs[legacyID], legacyRoot, cborRoot); err != nil {
			t.Fatal(err)
		}
	}
	for _, pack := range packs {
		v2Base := ""
		if pack.base != "" {
			v2Base = v2IDs[pack.base]
		}
		if err := store.StageSnapshotPackMapping(tenantACtx, repoID, pack.hash, "v2-"+pack.hash, pack.base, pack.target, v2Base, v2IDs[pack.target]); err != nil {
			t.Fatal(err)
		}
	}

	if err := store.withTenantTx(tenantBCtx, func(ctx context.Context, tx pgx.Tx) error {
		var mappings int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM review_snapshot_commit_mappings WHERE repo_id = $1`, repoID).Scan(&mappings); err != nil {
			return err
		}
		if mappings != 0 {
			return fmt.Errorf("foreign tenant saw %d migration mappings", mappings)
		}
		return nil
	}); err != nil {
		t.Fatalf("migration ledger RLS: %v", err)
	}

	if err := store.CompleteSnapshotMigration(tenantACtx, repoID); err != nil {
		t.Fatalf("CompleteSnapshotMigration() error = %v", err)
	}
	head, err := store.GetBranchRef(tenantACtx, repoID, "main")
	if err != nil || head != v2IDs[merge] {
		t.Fatalf("v2 branch head = %q, %v; want %q", head, err, v2IDs[merge])
	}
	if ancestor, err := store.IsAncestor(tenantACtx, repoID, v2IDs[source], v2IDs[merge]); err != nil || !ancestor {
		t.Fatalf("v2 merge second parent ancestry = %t, %v; want true, nil", ancestor, err)
	}
	metadata, err := store.GetCommitMetadata(tenantACtx, repoID, v2IDs[merge])
	if err != nil || metadata.Format != 2 || metadata.SnapshotRoot != "cbor-legacy-snapshot-merge" {
		t.Fatalf("v2 metadata = %+v, %v", metadata, err)
	}
	ranges, err := store.GetPackRanges(tenantACtx, repoID, v2IDs[merge], v2IDs[target])
	if err != nil || len(ranges) != 1 || ranges[0].PackHash != "v2-legacy-pack-merge" || ranges[0].Format != 2 {
		t.Fatalf("v2 pack range = %+v, %v", ranges, err)
	}
	if err := store.RollbackSnapshotMigration(tenantACtx, repoID); err != nil {
		t.Fatalf("RollbackSnapshotMigration() error = %v", err)
	}
	if head, err := store.GetBranchRef(tenantACtx, repoID, "main"); err != nil || head != merge {
		t.Fatalf("rolled back branch head = %q, %v; want %q", head, err, merge)
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

func TestMergeLease_ContentionOwnershipAndExpiry(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-merge-lease")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-merge-lease")
	base := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-base", "alice", "base")
	target := mustPutCommit(t, store, tenantCtx, repoID, base, "snap-target", "alice", "target")
	source := mustPutCommit(t, store, tenantCtx, repoID, base, "snap-source", "bob", "source")
	mustCreateBranch(t, store, tenantCtx, repoID, "main", target)

	request := MergeLeaseRequest{
		RepoID: repoID, TargetBranch: "main", Subject: "alice",
		SourceCommitID: source, TargetCommitID: target, BaseCommitID: base,
		Duration: time.Second,
	}
	lease, err := store.AcquireMergeLease(tenantCtx, request)
	if err != nil {
		t.Fatalf("AcquireMergeLease: %v", err)
	}
	if lease.Token == "" || lease.ExpiresAt.Before(time.Now()) {
		t.Fatalf("AcquireMergeLease returned invalid lease: %+v", lease)
	}

	request.Subject = "bob"
	if _, err := store.AcquireMergeLease(tenantCtx, request); !errors.Is(err, ErrMergeLeaseHeld) {
		t.Fatalf("competing AcquireMergeLease: expected ErrMergeLeaseHeld, got %v", err)
	}
	if _, err := store.ValidateMergeLease(tenantCtx, repoID, "main", "bob", lease.Token); !errors.Is(err, ErrMergeLeaseOwnership) {
		t.Fatalf("ValidateMergeLease foreign subject: expected ErrMergeLeaseOwnership, got %v", err)
	}
	validated, err := store.ValidateMergeLease(tenantCtx, repoID, "main", "alice", lease.Token)
	if err != nil {
		t.Fatalf("ValidateMergeLease owner: %v", err)
	}
	if validated.SourceCommitID != source || validated.TargetCommitID != target || validated.BaseCommitID != base {
		t.Fatalf("ValidateMergeLease identities = %+v, want source=%q target=%q base=%q", validated, source, target, base)
	}

	shortRequest := MergeLeaseRequest{
		RepoID: repoID, TargetBranch: "main", Subject: "alice",
		SourceCommitID: source, TargetCommitID: target, BaseCommitID: base,
		Duration: 20 * time.Millisecond,
	}
	// A separate branch avoids waiting for the deliberately active lease.
	mustCreateBranch(t, store, tenantCtx, repoID, "release", target)
	shortRequest.TargetBranch = "release"
	expiring, err := store.AcquireMergeLease(tenantCtx, shortRequest)
	if err != nil {
		t.Fatalf("AcquireMergeLease short: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	if _, err := store.ValidateMergeLease(tenantCtx, repoID, "release", "alice", expiring.Token); !errors.Is(err, ErrMergeLeaseExpired) {
		t.Fatalf("ValidateMergeLease expired: expected ErrMergeLeaseExpired, got %v", err)
	}
	shortRequest.Subject = "bob"
	replacement, err := store.AcquireMergeLease(tenantCtx, shortRequest)
	if err != nil {
		t.Fatalf("AcquireMergeLease replacement: %v", err)
	}
	if replacement.Token == expiring.Token || replacement.Subject != "bob" {
		t.Fatalf("replacement lease = %+v, want a distinct bob-owned token", replacement)
	}
}

func TestMergeLease_ConcurrentContendersHaveOneWinner(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-merge-lease-race")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-merge-lease-race")
	base := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-base", "alice", "base")
	target := mustPutCommit(t, store, tenantCtx, repoID, base, "snap-target", "alice", "target")
	source := mustPutCommit(t, store, tenantCtx, repoID, base, "snap-source", "bob", "source")
	mustCreateBranch(t, store, tenantCtx, repoID, "main", target)

	const contenders = 8
	start := make(chan struct{})
	errs := make([]error, contenders)
	var wg sync.WaitGroup
	wg.Add(contenders)
	for i := 0; i < contenders; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = store.AcquireMergeLease(tenantCtx, MergeLeaseRequest{
				RepoID: repoID, TargetBranch: "main", Subject: fmt.Sprintf("reviewer-%d", i),
				SourceCommitID: source, TargetCommitID: target, BaseCommitID: base, Duration: time.Minute,
			})
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	for i, err := range errs {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, ErrMergeLeaseHeld):
		default:
			t.Fatalf("AcquireMergeLease contender %d: unexpected error %v", i, err)
		}
	}
	if winners != 1 {
		t.Fatalf("AcquireMergeLease winners = %d, want 1", winners)
	}
}

func TestReleaseMergeLease_RequiresMatchingOwner(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-release-merge-lease")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-release-merge-lease")
	base := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-base", "alice", "base")
	target := mustPutCommit(t, store, tenantCtx, repoID, base, "snap-target", "alice", "target")
	source := mustPutCommit(t, store, tenantCtx, repoID, base, "snap-source", "bob", "source")
	mustCreateBranch(t, store, tenantCtx, repoID, "main", target)
	lease, err := store.AcquireMergeLease(tenantCtx, MergeLeaseRequest{
		RepoID: repoID, TargetBranch: "main", Subject: "alice",
		SourceCommitID: source, TargetCommitID: target, BaseCommitID: base, Duration: time.Minute,
	})
	if err != nil {
		t.Fatalf("AcquireMergeLease: %v", err)
	}

	if err := store.ReleaseMergeLease(tenantCtx, repoID, "main", "bob", lease.Token); !errors.Is(err, ErrMergeLeaseOwnership) {
		t.Fatalf("ReleaseMergeLease wrong subject: expected ErrMergeLeaseOwnership, got %v", err)
	}
	if err := store.ReleaseMergeLease(tenantCtx, repoID, "main", "alice", "wrong-token"); !errors.Is(err, ErrMergeLeaseOwnership) {
		t.Fatalf("ReleaseMergeLease wrong token: expected ErrMergeLeaseOwnership, got %v", err)
	}
	if _, err := store.ValidateMergeLease(tenantCtx, repoID, "main", "alice", lease.Token); err != nil {
		t.Fatalf("lease after rejected releases: %v", err)
	}

	if err := store.ReleaseMergeLease(tenantCtx, repoID, "main", "alice", lease.Token); err != nil {
		t.Fatalf("ReleaseMergeLease owner: %v", err)
	}
	if _, err := store.ValidateMergeLease(tenantCtx, repoID, "main", "alice", lease.Token); !errors.Is(err, ErrMergeLeaseNotFound) {
		t.Fatalf("released lease validation: expected ErrMergeLeaseNotFound, got %v", err)
	}
	if err := store.ReleaseMergeLease(tenantCtx, repoID, "main", "alice", lease.Token); !errors.Is(err, ErrMergeLeaseNotFound) {
		t.Fatalf("ReleaseMergeLease absent: expected ErrMergeLeaseNotFound, got %v", err)
	}
	if err := store.ReleaseMergeLease(context.Background(), repoID, "main", "alice", lease.Token); !errors.Is(err, ErrMissingTenantContext) {
		t.Fatalf("ReleaseMergeLease no tenant context: expected ErrMissingTenantContext, got %v", err)
	}
}

func TestApplyMerge_RegistersDAGAndConsumesOwnedLease(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-apply-merge")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-apply-merge")
	root := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-root", "alice", "root")
	base := mustPutCommit(t, store, tenantCtx, repoID, root, "snap-base", "alice", "base")
	target := mustPutCommit(t, store, tenantCtx, repoID, base, "snap-target", "alice", "target")
	source := mustPutCommit(t, store, tenantCtx, repoID, base, "snap-source", "bob", "source")
	mustCreateBranch(t, store, tenantCtx, repoID, "main", target)

	lease, err := store.AcquireMergeLease(tenantCtx, MergeLeaseRequest{
		RepoID: repoID, TargetBranch: "main", Subject: "alice",
		SourceCommitID: source, TargetCommitID: target, BaseCommitID: base, Duration: time.Minute,
	})
	if err != nil {
		t.Fatalf("AcquireMergeLease: %v", err)
	}
	result := testCommitID(t.Name(), "merge-result")
	apply := ApplyMergeRequest{
		RepoID: repoID, TargetBranch: "main", Subject: "alice", LeaseToken: lease.Token,
		SourceCommitID: source, TargetCommitID: target, BaseCommitID: base,
		ResultCommitID: result, SnapshotRoot: "snap-merge", Author: "alice",
		Message: "merge source", PackHash: testCommitID(t.Name(), "merge-pack"),
	}
	if err := store.ApplyMerge(tenantCtx, apply); err != nil {
		t.Fatalf("ApplyMerge: %v", err)
	}

	head, err := store.GetBranchRef(tenantCtx, repoID, "main")
	if err != nil || head != result {
		t.Fatalf("GetBranchRef after ApplyMerge = (%q, %v), want (%q, nil)", head, err, result)
	}
	if _, err := store.ValidateMergeLease(tenantCtx, repoID, "main", "alice", lease.Token); !errors.Is(err, ErrMergeLeaseNotFound) {
		t.Fatalf("ValidateMergeLease consumed lease: expected ErrMergeLeaseNotFound, got %v", err)
	}
	var (
		legacyParent string
		parents      []string
		packBase     string
		packTarget   string
	)
	if err := store.withTenantTx(tenantCtx, func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT parent_commit_id FROM commits WHERE repo_id = $1 AND id = $2`, repoID, result).Scan(&legacyParent); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT parent_commit_id FROM commit_parents
			WHERE repo_id = $1 AND commit_id = $2
			ORDER BY parent_position
		`, repoID, result)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var parent string
			if err := rows.Scan(&parent); err != nil {
				return err
			}
			parents = append(parents, parent)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `
			SELECT base_commit_id, target_commit_id
			FROM pack_ranges WHERE repo_id = $1 AND pack_hash = $2
		`, repoID, apply.PackHash).Scan(&packBase, &packTarget)
	}); err != nil {
		t.Fatalf("query ApplyMerge records: %v", err)
	}
	if legacyParent != target {
		t.Fatalf("legacy merge parent = %q, want target %q", legacyParent, target)
	}
	if len(parents) != 2 || parents[0] != target || parents[1] != source {
		t.Fatalf("ordered merge parents = %q, want [%q %q]", parents, target, source)
	}
	if packBase != target || packTarget != result {
		t.Fatalf("merge pack range = (%q, %q), want (%q, %q)", packBase, packTarget, target, result)
	}

	for _, check := range []struct {
		ancestor string
		commit   string
	}{
		{source, result},
		{target, result},
		{root, result},
	} {
		got, err := store.IsAncestor(tenantCtx, repoID, check.ancestor, check.commit)
		if err != nil || !got {
			t.Fatalf("IsAncestor(%q, %q) = (%t, %v), want (true, nil)", check.ancestor, check.commit, got, err)
		}
	}
	got, err := store.FindLowestCommonAncestor(tenantCtx, repoID, result, source)
	if err != nil || got != source {
		t.Fatalf("FindLowestCommonAncestor(merge, source) = (%q, %v), want (%q, nil)", got, err, source)
	}
}

func TestApplyMerge_RejectsWrongOwnerWithoutConsumingLease(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-merge-wrong-owner")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-merge-wrong-owner")
	base := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-base", "alice", "base")
	target := mustPutCommit(t, store, tenantCtx, repoID, base, "snap-target", "alice", "target")
	source := mustPutCommit(t, store, tenantCtx, repoID, base, "snap-source", "bob", "source")
	mustCreateBranch(t, store, tenantCtx, repoID, "main", target)
	lease, err := store.AcquireMergeLease(tenantCtx, MergeLeaseRequest{
		RepoID: repoID, TargetBranch: "main", Subject: "alice",
		SourceCommitID: source, TargetCommitID: target, BaseCommitID: base, Duration: time.Minute,
	})
	if err != nil {
		t.Fatalf("AcquireMergeLease: %v", err)
	}
	err = store.ApplyMerge(tenantCtx, ApplyMergeRequest{
		RepoID: repoID, TargetBranch: "main", Subject: "mallory", LeaseToken: lease.Token,
		SourceCommitID: source, TargetCommitID: target, BaseCommitID: base,
		ResultCommitID: testCommitID(t.Name(), "result"), SnapshotRoot: "snap-merge",
		Author: "mallory", Message: "unauthorized", PackHash: testCommitID(t.Name(), "pack"),
	})
	if !errors.Is(err, ErrMergeLeaseOwnership) {
		t.Fatalf("ApplyMerge wrong owner: expected ErrMergeLeaseOwnership, got %v", err)
	}
	if _, err := store.ValidateMergeLease(tenantCtx, repoID, "main", "alice", lease.Token); err != nil {
		t.Fatalf("owner lease after rejected ApplyMerge: %v", err)
	}
}
