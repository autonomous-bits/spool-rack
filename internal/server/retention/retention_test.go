package retention

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
)

type fakeTenantRepoKey struct {
	tenant string
	repo   string
}

type ctxTenantKey struct{}

// fakeStore is an in-memory Store used to unit test Collector's reachability
// and collection logic in isolation from PostgreSQL. Every collection is
// keyed by (tenantID, repoID) so a test can assert that a Collect run scoped
// to one tenant never observes or mutates another tenant's data.
type fakeStore struct {
	branches     map[fakeTenantRepoKey][]postgres.BranchRef
	ancestry     map[fakeTenantRepoKey]map[string][]string
	activeTxns   map[fakeTenantRepoKey][]postgres.ActiveTransaction
	auditRecords map[fakeTenantRepoKey][]postgres.AuditRecord
	packs        map[fakeTenantRepoKey][]postgres.NativePackSummary
	objectToPack map[fakeTenantRepoKey]map[string]string
	deletedPacks map[fakeTenantRepoKey][]string
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		branches:     map[fakeTenantRepoKey][]postgres.BranchRef{},
		ancestry:     map[fakeTenantRepoKey]map[string][]string{},
		activeTxns:   map[fakeTenantRepoKey][]postgres.ActiveTransaction{},
		auditRecords: map[fakeTenantRepoKey][]postgres.AuditRecord{},
		packs:        map[fakeTenantRepoKey][]postgres.NativePackSummary{},
		objectToPack: map[fakeTenantRepoKey]map[string]string{},
		deletedPacks: map[fakeTenantRepoKey][]string{},
	}
}

func (f *fakeStore) SetTenantContext(ctx context.Context, tenantID string) (context.Context, error) {
	if tenantID == "" {
		return nil, errors.New("fakeStore: empty tenant id")
	}
	return context.WithValue(ctx, ctxTenantKey{}, tenantID), nil
}

func tenantFromCtx(ctx context.Context) string {
	tenantID, _ := ctx.Value(ctxTenantKey{}).(string)
	return tenantID
}

func (f *fakeStore) ListBranchRefs(ctx context.Context, repoID string) ([]postgres.BranchRef, error) {
	key := fakeTenantRepoKey{tenantFromCtx(ctx), repoID}
	return append([]postgres.BranchRef(nil), f.branches[key]...), nil
}

func (f *fakeStore) CommitAncestryIDs(ctx context.Context, repoID, commitID string) ([]string, error) {
	key := fakeTenantRepoKey{tenantFromCtx(ctx), repoID}
	perCommit, ok := f.ancestry[key]
	if !ok {
		return nil, postgres.ErrCommitNotFound
	}
	ids, ok := perCommit[commitID]
	if !ok {
		return nil, postgres.ErrCommitNotFound
	}
	return append([]string(nil), ids...), nil
}

func (f *fakeStore) ListActiveTransactions(ctx context.Context, repoID string) ([]postgres.ActiveTransaction, error) {
	key := fakeTenantRepoKey{tenantFromCtx(ctx), repoID}
	return append([]postgres.ActiveTransaction(nil), f.activeTxns[key]...), nil
}

func (f *fakeStore) ListAuditRecords(ctx context.Context, repoID string) ([]postgres.AuditRecord, error) {
	key := fakeTenantRepoKey{tenantFromCtx(ctx), repoID}
	return append([]postgres.AuditRecord(nil), f.auditRecords[key]...), nil
}

func (f *fakeStore) ListNativePacks(ctx context.Context, repoID string) ([]postgres.NativePackSummary, error) {
	key := fakeTenantRepoKey{tenantFromCtx(ctx), repoID}
	return append([]postgres.NativePackSummary(nil), f.packs[key]...), nil
}

func (f *fakeStore) ListNativePackObjectIDs(ctx context.Context, repoID string) (map[string]string, error) {
	key := fakeTenantRepoKey{tenantFromCtx(ctx), repoID}
	out := make(map[string]string, len(f.objectToPack[key]))
	for objectID, packID := range f.objectToPack[key] {
		out[objectID] = packID
	}
	return out, nil
}

func (f *fakeStore) DeleteNativePack(ctx context.Context, repoID, packID string) error {
	key := fakeTenantRepoKey{tenantFromCtx(ctx), repoID}
	packs := f.packs[key]
	filtered := packs[:0]
	found := false
	for _, pack := range packs {
		if pack.PackID == packID {
			found = true
			continue
		}
		filtered = append(filtered, pack)
	}
	if !found {
		return errors.New("fakeStore: delete native pack: not found")
	}
	f.packs[key] = filtered
	f.deletedPacks[key] = append(f.deletedPacks[key], packID)
	return nil
}

var _ Store = (*fakeStore)(nil)

type deletedPackCall struct {
	tenantID string
	repoID   string
	hash     string
}

// fakeDriver is a minimal cas.Driver that also implements PackDeleter, so
// tests can assert exactly which CAS pack hashes a Collect run physically
// removed.
type fakeDriver struct {
	deletes []deletedPackCall
}

func (f *fakeDriver) Put(context.Context, cas.Scope, string, []byte) error { return nil }
func (f *fakeDriver) Get(context.Context, cas.Scope, string) ([]byte, error) {
	return nil, errors.New("fakeDriver: Get not supported")
}
func (f *fakeDriver) Exists(context.Context, cas.Scope, string) (bool, error) { return false, nil }
func (f *fakeDriver) OpenPack(context.Context, cas.Scope, string) (io.ReadCloser, error) {
	return nil, errors.New("fakeDriver: OpenPack not supported")
}
func (f *fakeDriver) WritePack(context.Context, cas.Scope, string, io.Reader) error { return nil }
func (f *fakeDriver) WriteAsset(context.Context, cas.Scope, string, io.Reader) (int64, error) {
	return 0, nil
}
func (f *fakeDriver) OpenAsset(context.Context, cas.Scope, string) (io.ReadCloser, int64, error) {
	return nil, 0, errors.New("fakeDriver: OpenAsset not supported")
}
func (f *fakeDriver) AssetExists(context.Context, cas.Scope, string) (bool, error) { return false, nil }
func (f *fakeDriver) DeletePack(_ context.Context, scope cas.Scope, hash string) error {
	f.deletes = append(f.deletes, deletedPackCall{tenantID: scope.TenantID(), repoID: scope.RepoID(), hash: hash})
	return nil
}

var (
	_ cas.Driver  = (*fakeDriver)(nil)
	_ PackDeleter = (*fakeDriver)(nil)
)

func retainedPackIDs(report Report) []string {
	ids := make([]string, len(report.Retained))
	for i, r := range report.Retained {
		ids[i] = r.PackID
	}
	return ids
}

func collectedPackIDs(report Report) []string {
	ids := make([]string, len(report.Collected))
	for i, c := range report.Collected {
		ids[i] = c.PackID
	}
	return ids
}

func assertStringSlice(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// TestCollect_RetainsBranchReachableHistory proves an object whose only root
// is commit history reachable from a live branch head is retained.
func TestCollect_RetainsBranchReachableHistory(t *testing.T) {
	store := newFakeStore()
	key := fakeTenantRepoKey{"tenant-1", "repo-1"}
	store.branches[key] = []postgres.BranchRef{{Name: "main", HeadCommitID: "c3"}}
	store.ancestry[key] = map[string][]string{"c3": {"c1", "c2", "c3"}}
	store.packs[key] = []postgres.NativePackSummary{{PackID: "p1", CASPackHash: "h1", CommitID: "c1"}}

	collector := NewCollector(store, &fakeDriver{})
	report, err := collector.Collect(context.Background(), "tenant-1", "repo-1", false)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	assertStringSlice(t, retainedPackIDs(report), []string{"p1"})
	assertStringSlice(t, collectedPackIDs(report), nil)
	if len(store.packs[key]) != 1 {
		t.Fatalf("store.packs after collect = %+v, want pack retained", store.packs[key])
	}
}

// TestCollect_RetainsDeletedBranchHistory proves a soft-deleted branch's
// history still roots retention, per
// adr-immutable-commit-retention-on-branch-deletion.
func TestCollect_RetainsDeletedBranchHistory(t *testing.T) {
	store := newFakeStore()
	key := fakeTenantRepoKey{"tenant-1", "repo-1"}
	store.branches[key] = []postgres.BranchRef{{Name: "stale", HeadCommitID: "c2", Deleted: true}}
	store.ancestry[key] = map[string][]string{"c2": {"c1", "c2"}}
	store.packs[key] = []postgres.NativePackSummary{{PackID: "p1", CASPackHash: "h1", CommitID: "c1"}}

	collector := NewCollector(store, &fakeDriver{})
	report, err := collector.Collect(context.Background(), "tenant-1", "repo-1", false)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	assertStringSlice(t, retainedPackIDs(report), []string{"p1"})
	assertStringSlice(t, collectedPackIDs(report), nil)
}

// TestCollect_RetainsActiveTransactionCommitRoot proves a pack linked to a
// commit anchored only by an in-flight active transaction (not yet
// reachable from any branch) is retained.
func TestCollect_RetainsActiveTransactionCommitRoot(t *testing.T) {
	store := newFakeStore()
	key := fakeTenantRepoKey{"tenant-1", "repo-1"}
	store.ancestry[key] = map[string][]string{"c9": {"c9"}}
	store.activeTxns[key] = []postgres.ActiveTransaction{{TransactionID: "tx-1", CommitID: "c9"}}
	store.packs[key] = []postgres.NativePackSummary{{PackID: "p1", CASPackHash: "h1", CommitID: "c9"}}

	collector := NewCollector(store, &fakeDriver{})
	report, err := collector.Collect(context.Background(), "tenant-1", "repo-1", false)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	assertStringSlice(t, retainedPackIDs(report), []string{"p1"})
}

// TestCollect_RetainsActiveTransactionObjectRoot proves a pack that has not
// yet been linked to any commit (commit_id empty, e.g. mid-upload) is still
// retained when one of its objects is anchored by an active transaction.
func TestCollect_RetainsActiveTransactionObjectRoot(t *testing.T) {
	store := newFakeStore()
	key := fakeTenantRepoKey{"tenant-1", "repo-1"}
	store.activeTxns[key] = []postgres.ActiveTransaction{{TransactionID: "tx-1", ObjectID: "obj-1"}}
	store.objectToPack[key] = map[string]string{"obj-1": "p1"}
	store.packs[key] = []postgres.NativePackSummary{{PackID: "p1", CASPackHash: "h1"}}

	collector := NewCollector(store, &fakeDriver{})
	report, err := collector.Collect(context.Background(), "tenant-1", "repo-1", false)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	assertStringSlice(t, retainedPackIDs(report), []string{"p1"})
}

// TestCollect_RetainsAuditRecordCommitRoot proves a pack linked to a commit
// referenced only by an audit record is retained.
func TestCollect_RetainsAuditRecordCommitRoot(t *testing.T) {
	store := newFakeStore()
	key := fakeTenantRepoKey{"tenant-1", "repo-1"}
	store.ancestry[key] = map[string][]string{"c9": {"c9"}}
	store.auditRecords[key] = []postgres.AuditRecord{{ID: "audit-1", EventType: "push", Subject: "alice", CommitID: "c9"}}
	store.packs[key] = []postgres.NativePackSummary{{PackID: "p1", CASPackHash: "h1", CommitID: "c9"}}

	collector := NewCollector(store, &fakeDriver{})
	report, err := collector.Collect(context.Background(), "tenant-1", "repo-1", false)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	assertStringSlice(t, retainedPackIDs(report), []string{"p1"})
}

// TestCollect_RetainsAuditRecordObjectRoot proves an unlinked pack is
// retained when one of its objects is anchored directly by an audit record.
func TestCollect_RetainsAuditRecordObjectRoot(t *testing.T) {
	store := newFakeStore()
	key := fakeTenantRepoKey{"tenant-1", "repo-1"}
	store.auditRecords[key] = []postgres.AuditRecord{{ID: "audit-1", EventType: "access", Subject: "bob", ObjectID: "obj-1"}}
	store.objectToPack[key] = map[string]string{"obj-1": "p1"}
	store.packs[key] = []postgres.NativePackSummary{{PackID: "p1", CASPackHash: "h1"}}

	collector := NewCollector(store, &fakeDriver{})
	report, err := collector.Collect(context.Background(), "tenant-1", "repo-1", false)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	assertStringSlice(t, retainedPackIDs(report), []string{"p1"})
}

// TestCollect_DryRunReportsWithoutMutating proves dry-run mode reports an
// unreachable pack as collectible without deleting its index rows or its
// CAS blob.
func TestCollect_DryRunReportsWithoutMutating(t *testing.T) {
	store := newFakeStore()
	key := fakeTenantRepoKey{"tenant-1", "repo-1"}
	store.packs[key] = []postgres.NativePackSummary{{PackID: "p1", CASPackHash: "h1"}}

	driver := &fakeDriver{}
	collector := NewCollector(store, driver)
	report, err := collector.Collect(context.Background(), "tenant-1", "repo-1", true)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if !report.DryRun {
		t.Fatal("report.DryRun = false, want true")
	}
	assertStringSlice(t, retainedPackIDs(report), nil)
	if len(report.Collected) != 1 || report.Collected[0].PackID != "p1" || !report.Collected[0].CASBlobCollected {
		t.Fatalf("report.Collected = %+v, want p1 reported as collectible", report.Collected)
	}
	if len(store.packs[key]) != 1 {
		t.Fatalf("store.packs after dry run = %+v, want unchanged", store.packs[key])
	}
	if len(driver.deletes) != 0 {
		t.Fatalf("driver.deletes after dry run = %+v, want none", driver.deletes)
	}
}

// TestCollect_RealRunRemovesUnreachablePackAndCASBlob proves a real
// (non-dry-run) collection actually removes an unreachable pack's index
// rows and its CAS blob.
func TestCollect_RealRunRemovesUnreachablePackAndCASBlob(t *testing.T) {
	store := newFakeStore()
	key := fakeTenantRepoKey{"tenant-1", "repo-1"}
	store.packs[key] = []postgres.NativePackSummary{{PackID: "p1", CASPackHash: "h1"}}

	driver := &fakeDriver{}
	collector := NewCollector(store, driver)
	report, err := collector.Collect(context.Background(), "tenant-1", "repo-1", false)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	assertStringSlice(t, collectedPackIDs(report), []string{"p1"})
	if !report.Collected[0].CASBlobCollected {
		t.Fatalf("report.Collected[0] = %+v, want CASBlobCollected", report.Collected[0])
	}
	if len(store.packs[key]) != 0 {
		t.Fatalf("store.packs after collect = %+v, want empty", store.packs[key])
	}
	want := []deletedPackCall{{tenantID: "tenant-1", repoID: "repo-1", hash: "h1"}}
	if len(driver.deletes) != 1 || driver.deletes[0] != want[0] {
		t.Fatalf("driver.deletes = %+v, want %+v", driver.deletes, want)
	}
}

// TestCollect_SharedCASHashKeptWhenSiblingPackRetained proves that when an
// unreachable pack shares its CAS pack hash with a retained pack, the CAS
// blob is not deleted even though the unreachable pack's own index rows
// are.
func TestCollect_SharedCASHashKeptWhenSiblingPackRetained(t *testing.T) {
	store := newFakeStore()
	key := fakeTenantRepoKey{"tenant-1", "repo-1"}
	store.branches[key] = []postgres.BranchRef{{Name: "main", HeadCommitID: "c1"}}
	store.ancestry[key] = map[string][]string{"c1": {"c1"}}
	store.packs[key] = []postgres.NativePackSummary{
		{PackID: "p1", CASPackHash: "hshared", CommitID: "c1"},
		{PackID: "p2", CASPackHash: "hshared"},
	}

	driver := &fakeDriver{}
	collector := NewCollector(store, driver)
	report, err := collector.Collect(context.Background(), "tenant-1", "repo-1", false)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	assertStringSlice(t, retainedPackIDs(report), []string{"p1"})
	assertStringSlice(t, collectedPackIDs(report), []string{"p2"})
	if report.Collected[0].CASBlobCollected {
		t.Fatalf("report.Collected[0] = %+v, want CAS blob kept (still referenced by retained p1)", report.Collected[0])
	}
	if len(driver.deletes) != 0 {
		t.Fatalf("driver.deletes = %+v, want none (hash still referenced)", driver.deletes)
	}
	if len(store.packs[key]) != 1 || store.packs[key][0].PackID != "p1" {
		t.Fatalf("store.packs after collect = %+v, want only p1 remaining", store.packs[key])
	}
}

// TestCollect_AllFourRootsAndOneUnreachablePackCombined proves each of the
// four rooting categories retains its pack simultaneously, while a pack
// rooted by none of them is collected in the same run.
func TestCollect_AllFourRootsAndOneUnreachablePackCombined(t *testing.T) {
	store := newFakeStore()
	key := fakeTenantRepoKey{"tenant-1", "repo-1"}
	store.branches[key] = []postgres.BranchRef{
		{Name: "main", HeadCommitID: "c1"},
		{Name: "deleted-branch", HeadCommitID: "c2", Deleted: true},
	}
	store.ancestry[key] = map[string][]string{
		"c1": {"c1"},
		"c2": {"c2"},
		"c9": {"c9"},
		"c8": {"c8"},
	}
	store.activeTxns[key] = []postgres.ActiveTransaction{{TransactionID: "tx-1", CommitID: "c9"}}
	store.auditRecords[key] = []postgres.AuditRecord{{ID: "audit-1", EventType: "push", Subject: "alice", CommitID: "c8"}}
	store.packs[key] = []postgres.NativePackSummary{
		{PackID: "p-history", CASPackHash: "h1", CommitID: "c1"},
		{PackID: "p-deleted-branch", CASPackHash: "h2", CommitID: "c2"},
		{PackID: "p-active-tx", CASPackHash: "h3", CommitID: "c9"},
		{PackID: "p-audit", CASPackHash: "h4", CommitID: "c8"},
		{PackID: "p-unreachable", CASPackHash: "h5"},
	}

	collector := NewCollector(store, &fakeDriver{})
	report, err := collector.Collect(context.Background(), "tenant-1", "repo-1", false)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	assertStringSlice(t, retainedPackIDs(report), []string{"p-active-tx", "p-audit", "p-deleted-branch", "p-history"})
	assertStringSlice(t, collectedPackIDs(report), []string{"p-unreachable"})
}

// TestCollect_TenantIsolation proves a Collect run scoped to one tenant
// never inspects or mutates another tenant's rows, even when both tenants
// use the same repoID string.
func TestCollect_TenantIsolation(t *testing.T) {
	store := newFakeStore()
	keyA := fakeTenantRepoKey{"tenant-a", "repo-1"}
	keyB := fakeTenantRepoKey{"tenant-b", "repo-1"}
	store.packs[keyA] = []postgres.NativePackSummary{{PackID: "p-a", CASPackHash: "ha"}}
	store.packs[keyB] = []postgres.NativePackSummary{{PackID: "p-b", CASPackHash: "hb"}}

	driver := &fakeDriver{}
	collector := NewCollector(store, driver)
	report, err := collector.Collect(context.Background(), "tenant-a", "repo-1", false)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	assertStringSlice(t, collectedPackIDs(report), []string{"p-a"})
	if len(store.packs[keyA]) != 0 {
		t.Fatalf("store.packs[tenant-a] = %+v, want empty", store.packs[keyA])
	}
	if len(store.packs[keyB]) != 1 || store.packs[keyB][0].PackID != "p-b" {
		t.Fatalf("store.packs[tenant-b] = %+v, want untouched", store.packs[keyB])
	}
	for _, deleted := range driver.deletes {
		if deleted.tenantID != "tenant-a" {
			t.Fatalf("driver.deletes = %+v, want only tenant-a CAS deletions", driver.deletes)
		}
	}
}

func TestCollect_RequiresTenantAndRepoID(t *testing.T) {
	store := newFakeStore()
	collector := NewCollector(store, &fakeDriver{})

	if _, err := collector.Collect(context.Background(), "", "repo-1", false); err == nil {
		t.Fatal("Collect with empty tenant ID: expected error, got nil")
	}
	if _, err := collector.Collect(context.Background(), "tenant-1", "", false); err == nil {
		t.Fatal("Collect with empty repo ID: expected error, got nil")
	}
}

func TestCollect_WithoutPackDeleterSkipsCASDeletion(t *testing.T) {
	store := newFakeStore()
	key := fakeTenantRepoKey{"tenant-1", "repo-1"}
	store.packs[key] = []postgres.NativePackSummary{{PackID: "p1", CASPackHash: "h1"}}

	// A driver that does not implement PackDeleter must still let index-row
	// collection proceed; it simply cannot reclaim the CAS blob.
	collector := NewCollector(store, noopDriver{})
	report, err := collector.Collect(context.Background(), "tenant-1", "repo-1", false)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(report.Collected) != 1 || report.Collected[0].CASBlobCollected {
		t.Fatalf("report.Collected = %+v, want CAS blob left in place", report.Collected)
	}
	if len(store.packs[key]) != 0 {
		t.Fatalf("store.packs after collect = %+v, want index rows removed", store.packs[key])
	}
}

type noopDriver struct{}

func (noopDriver) Put(context.Context, cas.Scope, string, []byte) error { return nil }
func (noopDriver) Get(context.Context, cas.Scope, string) ([]byte, error) {
	return nil, errors.New("noopDriver: Get not supported")
}
func (noopDriver) Exists(context.Context, cas.Scope, string) (bool, error) { return false, nil }
func (noopDriver) OpenPack(context.Context, cas.Scope, string) (io.ReadCloser, error) {
	return nil, errors.New("noopDriver: OpenPack not supported")
}
func (noopDriver) WritePack(context.Context, cas.Scope, string, io.Reader) error { return nil }
func (noopDriver) WriteAsset(context.Context, cas.Scope, string, io.Reader) (int64, error) {
	return 0, nil
}
func (noopDriver) OpenAsset(context.Context, cas.Scope, string) (io.ReadCloser, int64, error) {
	return nil, 0, errors.New("noopDriver: OpenAsset not supported")
}
func (noopDriver) AssetExists(context.Context, cas.Scope, string) (bool, error) { return false, nil }

var _ cas.Driver = noopDriver{}
