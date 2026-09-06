package postgres

import (
	"errors"
	"sort"
	"testing"
)

func TestDeleteBranch_SoftDeletesAndRetainsHead(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-branch-delete")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-branch-delete")

	commitID := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-1", "author", "initial")
	// "main" is created first so it becomes the repo's default branch,
	// leaving "feature" free to be deleted in this test.
	mustCreateBranch(t, store, tenantCtx, repoID, "main", commitID)
	mustCreateBranch(t, store, tenantCtx, repoID, "feature", commitID)

	if err := store.DeleteBranch(tenantCtx, repoID, "feature"); err != nil {
		t.Fatalf("DeleteBranch: %v", err)
	}

	refs, err := store.ListBranchRefs(tenantCtx, repoID)
	if err != nil {
		t.Fatalf("ListBranchRefs: %v", err)
	}
	var featureRef *BranchRef
	for i := range refs {
		if refs[i].Name == "feature" {
			featureRef = &refs[i]
		}
	}
	if featureRef == nil {
		t.Fatalf("ListBranchRefs = %+v, want %q retained", refs, "feature")
	}
	if featureRef.HeadCommitID != commitID || !featureRef.Deleted {
		t.Fatalf("ListBranchRefs feature entry = %+v, want deleted branch with head %q retained", featureRef, commitID)
	}

	// Deleting again must fail: the branch is already gone, not resurrect-able
	// by a second call.
	if err := store.DeleteBranch(tenantCtx, repoID, "feature"); !errors.Is(err, ErrBranchNotFound) {
		t.Fatalf("DeleteBranch second call: expected ErrBranchNotFound, got %v", err)
	}
}

func TestDeleteBranch_NotFound(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-branch-delete-missing")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-branch-delete-missing")

	if err := store.DeleteBranch(tenantCtx, repoID, "does-not-exist"); !errors.Is(err, ErrBranchNotFound) {
		t.Fatalf("DeleteBranch: expected ErrBranchNotFound, got %v", err)
	}
}

func TestListBranchRefs_IncludesLiveAndDeleted(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-branch-refs")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-branch-refs")

	commitID := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-1", "author", "initial")
	mustCreateBranch(t, store, tenantCtx, repoID, "main", commitID)
	mustCreateBranch(t, store, tenantCtx, repoID, "stale", commitID)
	if err := store.DeleteBranch(tenantCtx, repoID, "stale"); err != nil {
		t.Fatalf("DeleteBranch: %v", err)
	}

	refs, err := store.ListBranchRefs(tenantCtx, repoID)
	if err != nil {
		t.Fatalf("ListBranchRefs: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("ListBranchRefs = %+v, want 2 branches (one live, one deleted)", refs)
	}
	byName := map[string]BranchRef{}
	for _, ref := range refs {
		byName[ref.Name] = ref
	}
	if byName["main"].Deleted {
		t.Fatalf("branch %q: want live, got deleted", "main")
	}
	if !byName["stale"].Deleted {
		t.Fatalf("branch %q: want deleted, got live", "stale")
	}
}

func TestCommitAncestryIDs_ReturnsFullHistory(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-ancestry")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-ancestry")

	root := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-0", "author", "root")
	mid := mustPutCommit(t, store, tenantCtx, repoID, root, "snap-1", "author", "mid")
	tip := mustPutCommit(t, store, tenantCtx, repoID, mid, "snap-2", "author", "tip")

	ids, err := store.CommitAncestryIDs(tenantCtx, repoID, tip)
	if err != nil {
		t.Fatalf("CommitAncestryIDs: %v", err)
	}
	want := []string{root, mid, tip}
	sort.Strings(want)
	if len(ids) != len(want) {
		t.Fatalf("CommitAncestryIDs = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("CommitAncestryIDs = %v, want %v", ids, want)
		}
	}
}

func TestCommitAncestryIDs_NotFound(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-ancestry-missing")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-ancestry-missing")

	if _, err := store.CommitAncestryIDs(tenantCtx, repoID, "does-not-exist"); !errors.Is(err, ErrCommitNotFound) {
		t.Fatalf("CommitAncestryIDs: expected ErrCommitNotFound, got %v", err)
	}
}

func TestActiveTransaction_PutListDelete(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-active-tx")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-active-tx")

	if err := store.PutActiveTransaction(tenantCtx, repoID, "tx-1", "commit-a", ""); err != nil {
		t.Fatalf("PutActiveTransaction: %v", err)
	}
	if err := store.PutActiveTransaction(tenantCtx, repoID, "tx-2", "", "object-b"); err != nil {
		t.Fatalf("PutActiveTransaction: %v", err)
	}

	txns, err := store.ListActiveTransactions(tenantCtx, repoID)
	if err != nil {
		t.Fatalf("ListActiveTransactions: %v", err)
	}
	if len(txns) != 2 {
		t.Fatalf("ListActiveTransactions = %+v, want 2 rows", txns)
	}
	byID := map[string]ActiveTransaction{}
	for _, txn := range txns {
		byID[txn.TransactionID] = txn
	}
	if byID["tx-1"].CommitID != "commit-a" || byID["tx-1"].ObjectID != "" {
		t.Fatalf("tx-1 = %+v, want CommitID=commit-a ObjectID=\"\"", byID["tx-1"])
	}
	if byID["tx-2"].ObjectID != "object-b" || byID["tx-2"].CommitID != "" {
		t.Fatalf("tx-2 = %+v, want ObjectID=object-b CommitID=\"\"", byID["tx-2"])
	}

	if err := store.DeleteActiveTransaction(tenantCtx, repoID, "tx-1"); err != nil {
		t.Fatalf("DeleteActiveTransaction: %v", err)
	}
	txns, err = store.ListActiveTransactions(tenantCtx, repoID)
	if err != nil {
		t.Fatalf("ListActiveTransactions after delete: %v", err)
	}
	if len(txns) != 1 || txns[0].TransactionID != "tx-2" {
		t.Fatalf("ListActiveTransactions after delete = %+v, want only tx-2", txns)
	}

	if err := store.DeleteActiveTransaction(tenantCtx, repoID, "tx-1"); !errors.Is(err, ErrActiveTransactionNotFound) {
		t.Fatalf("DeleteActiveTransaction second call: expected ErrActiveTransactionNotFound, got %v", err)
	}
}

func TestPutActiveTransaction_RequiresCommitOrObject(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-active-tx-invalid")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-active-tx-invalid")

	if err := store.PutActiveTransaction(tenantCtx, repoID, "tx-empty", "", ""); err == nil {
		t.Fatalf("PutActiveTransaction with neither commit nor object ID: expected error, got nil")
	}
}

func TestAuditRecord_PutList(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-audit")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-audit")

	if err := store.PutAuditRecord(tenantCtx, repoID, "push", "alice", "commit-x", ""); err != nil {
		t.Fatalf("PutAuditRecord: %v", err)
	}
	if err := store.PutAuditRecord(tenantCtx, repoID, "access", "bob", "", "object-y"); err != nil {
		t.Fatalf("PutAuditRecord: %v", err)
	}

	records, err := store.ListAuditRecords(tenantCtx, repoID)
	if err != nil {
		t.Fatalf("ListAuditRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("ListAuditRecords = %+v, want 2 records", records)
	}
	bySubject := map[string]AuditRecord{}
	for _, record := range records {
		bySubject[record.Subject] = record
	}
	if bySubject["alice"].CommitID != "commit-x" || bySubject["alice"].EventType != "push" {
		t.Fatalf("alice record = %+v, want CommitID=commit-x EventType=push", bySubject["alice"])
	}
	if bySubject["bob"].ObjectID != "object-y" || bySubject["bob"].EventType != "access" {
		t.Fatalf("bob record = %+v, want ObjectID=object-y EventType=access", bySubject["bob"])
	}
}

func TestListNativePacksAndDeleteNativePack(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantID := mustCreateTenant(t, store, ctx, "tenant-native-pack-list")
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)
	repoID := mustCreateRepository(t, store, tenantCtx, "repo-native-pack-list")

	commitID := mustPutCommit(t, store, tenantCtx, repoID, "", "snap-1", "author", "initial")
	entries := testNativeEntries(t, 2)
	packID := "cccc1111cccc1111cccc1111cccc1111"
	casPackHash := testCommitID(t.Name(), "cas-pack-hash")
	if err := store.PutNativePack(tenantCtx, repoID, packID, casPackHash, commitID, entries); err != nil {
		t.Fatalf("PutNativePack: %v", err)
	}

	packs, err := store.ListNativePacks(tenantCtx, repoID)
	if err != nil {
		t.Fatalf("ListNativePacks: %v", err)
	}
	if len(packs) != 1 || packs[0].PackID != packID || packs[0].CASPackHash != casPackHash || packs[0].CommitID != commitID {
		t.Fatalf("ListNativePacks = %+v, want single pack %q linked to commit %q", packs, packID, commitID)
	}

	objects, err := store.ListNativePackObjectIDs(tenantCtx, repoID)
	if err != nil {
		t.Fatalf("ListNativePackObjectIDs: %v", err)
	}
	if len(objects) != len(entries) {
		t.Fatalf("ListNativePackObjectIDs = %+v, want %d entries", objects, len(entries))
	}
	for _, entry := range entries {
		if objects[string(entry.Object)] != packID {
			t.Fatalf("ListNativePackObjectIDs[%s] = %q, want %q", entry.Object, objects[string(entry.Object)], packID)
		}
	}

	if err := store.DeleteNativePack(tenantCtx, repoID, packID); err != nil {
		t.Fatalf("DeleteNativePack: %v", err)
	}
	packs, err = store.ListNativePacks(tenantCtx, repoID)
	if err != nil {
		t.Fatalf("ListNativePacks after delete: %v", err)
	}
	if len(packs) != 0 {
		t.Fatalf("ListNativePacks after delete = %+v, want none", packs)
	}
	for _, entry := range entries {
		if _, err := store.GetNativeObjectLocation(tenantCtx, repoID, string(entry.Object)); !errors.Is(err, ErrNativeObjectNotFound) {
			t.Fatalf("GetNativeObjectLocation(%s) after DeleteNativePack: expected ErrNativeObjectNotFound, got %v", entry.Object, err)
		}
	}
}

func TestRetentionStore_CrossTenantIsolation(t *testing.T) {
	store, ctx := newTestStore(t)

	tenantA := mustCreateTenant(t, store, ctx, "tenant-retention-a")
	tenantACtx := mustTenantContext(t, store, ctx, tenantA)
	repoA := mustCreateRepository(t, store, tenantACtx, "repo-retention-a")
	commitA := mustPutCommit(t, store, tenantACtx, repoA, "", "snap-a", "author", "a")
	mustCreateBranch(t, store, tenantACtx, repoA, "main", commitA)
	if err := store.PutActiveTransaction(tenantACtx, repoA, "tx-a", commitA, ""); err != nil {
		t.Fatalf("PutActiveTransaction: %v", err)
	}
	if err := store.PutAuditRecord(tenantACtx, repoA, "push", "alice", commitA, ""); err != nil {
		t.Fatalf("PutAuditRecord: %v", err)
	}

	tenantB := mustCreateTenant(t, store, ctx, "tenant-retention-b")
	tenantBCtx := mustTenantContext(t, store, ctx, tenantB)

	if refs, err := store.ListBranchRefs(tenantBCtx, repoA); err != nil || len(refs) != 0 {
		t.Fatalf("ListBranchRefs cross-tenant = (%+v, %v), want (empty, nil)", refs, err)
	}
	if txns, err := store.ListActiveTransactions(tenantBCtx, repoA); err != nil || len(txns) != 0 {
		t.Fatalf("ListActiveTransactions cross-tenant = (%+v, %v), want (empty, nil)", txns, err)
	}
	if records, err := store.ListAuditRecords(tenantBCtx, repoA); err != nil || len(records) != 0 {
		t.Fatalf("ListAuditRecords cross-tenant = (%+v, %v), want (empty, nil)", records, err)
	}
	if _, err := store.CommitAncestryIDs(tenantBCtx, repoA, commitA); !errors.Is(err, ErrCommitNotFound) {
		t.Fatalf("CommitAncestryIDs cross-tenant: expected ErrCommitNotFound, got %v", err)
	}
}
