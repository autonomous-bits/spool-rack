// Package retention implements tenant-scoped reachability retention and
// garbage collection for native Spool pack objects indexed by
// internal/server/nativeindex (see the native_packs and native_pack_objects
// tables in internal/server/storage/postgres/schema.sql).
//
// A native pack is retained when it is reachable from any of four roots:
//
//  1. Commit history: the pack's linked commit is an ancestor (inclusive) of
//     any branch head, including a branch that has been soft-deleted per
//     adr-immutable-commit-retention-on-branch-deletion — deleting a branch
//     must never make its history collectible.
//  2. Active transactions: an in-flight upload (most importantly a
//     concurrent push) that has registered a commit or object root, so its
//     objects can never be collected out from under it before it completes.
//  3. Audit records: a persisted audit trail entry that refers to a commit
//     or object, satisfying req-tenant-audit-and-access-logging independent
//     of current branch state.
//
// Anything not reachable from one of the above may be collected: its
// native_packs/native_pack_objects rows, and — once no remaining retained
// pack still references the same CAS pack hash — the underlying CAS blob.
// Collection is always scoped to a single tenant/repository and never
// inspects or mutates another tenant's rows.
package retention

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
)

// ErrNotInitialized indicates a Collector was used without a Store, which
// can only happen if it was constructed by hand instead of via NewCollector.
var ErrNotInitialized = errors.New("retention: collector is not initialized")

// Store is the narrow slice of postgres.Store that Collector depends on,
// kept separate so unit tests can supply a lightweight fake instead of a
// real PostgreSQL-backed store.
type Store interface {
	SetTenantContext(ctx context.Context, tenantID string) (context.Context, error)
	ListBranchRefs(ctx context.Context, repoID string) ([]postgres.BranchRef, error)
	CommitAncestryIDs(ctx context.Context, repoID, commitID string) ([]string, error)
	ListActiveTransactions(ctx context.Context, repoID string) ([]postgres.ActiveTransaction, error)
	ListAuditRecords(ctx context.Context, repoID string) ([]postgres.AuditRecord, error)
	ListNativePacks(ctx context.Context, repoID string) ([]postgres.NativePackSummary, error)
	ListNativePackObjectIDs(ctx context.Context, repoID string) (map[string]string, error)
	DeleteNativePack(ctx context.Context, repoID, packID string) error
}

var _ Store = (postgres.Store)(nil)

// PackDeleter is an optional capability a cas.Driver may additionally
// implement to support physically reclaiming packfile data during
// collection. It is deliberately not part of the cas.Driver interface: a
// Collector type-asserts for it at call time, so a Driver implementation
// without physical deletion support still builds and simply leaves
// unreferenced CAS blobs in place instead of failing.
type PackDeleter interface {
	DeletePack(ctx context.Context, scope cas.Scope, packHash string) error
}

// RetainedPack identifies one native pack a Collect run kept because it is
// reachable from history, an active transaction, or an audit record.
type RetainedPack struct {
	PackID      string
	CASPackHash string
}

// CollectedPack identifies one native pack a Collect run removed (or, in
// dry-run mode, would remove) because it is unreachable from every
// retention root. CASBlobCollected reports whether the underlying CAS blob
// was (or, in dry-run mode, would be) deleted — false when another retained
// pack still shares the same CAS pack hash, or when the configured driver
// does not support PackDeleter.
type CollectedPack struct {
	PackID           string
	CASPackHash      string
	CASBlobCollected bool
}

// Report describes the outcome of one Collect run.
type Report struct {
	TenantID  string
	RepoID    string
	DryRun    bool
	Retained  []RetainedPack
	Collected []CollectedPack
}

// Collector computes tenant-scoped reachability and collects unreachable
// native pack objects.
type Collector struct {
	store  Store
	driver cas.Driver
}

// NewCollector constructs a Collector backed by store's native pack index
// and driver's CAS storage. driver may be nil or may not implement
// PackDeleter; either way, Collect still removes unreachable index rows and
// simply skips physical CAS blob deletion.
func NewCollector(store Store, driver cas.Driver) *Collector {
	return &Collector{store: store, driver: driver}
}

// Collect computes every native pack reachable from history, active
// transactions, or audit records for tenantID's repoID, then either reports
// (dryRun true) or actually removes (dryRun false) every other pack's index
// rows and, once no retained pack still references it, its CAS blob.
func (c *Collector) Collect(ctx context.Context, tenantID, repoID string, dryRun bool) (Report, error) {
	if c == nil || c.store == nil {
		return Report{}, ErrNotInitialized
	}
	if tenantID == "" {
		return Report{}, errors.New("retention: collect: tenant ID is required")
	}
	if repoID == "" {
		return Report{}, errors.New("retention: collect: repo ID is required")
	}

	var scope cas.Scope
	if c.driver != nil {
		var err error
		scope, err = cas.NewScope(tenantID, repoID)
		if err != nil {
			return Report{}, fmt.Errorf("retention: collect: invalid CAS scope: %w", err)
		}
	}

	tenantCtx, err := c.store.SetTenantContext(ctx, tenantID)
	if err != nil {
		return Report{}, fmt.Errorf("retention: collect: set tenant context: %w", err)
	}

	retainedCommits := make(map[string]struct{})
	addCommitRoot := func(commitID string) error {
		if commitID == "" {
			return nil
		}
		retainedCommits[commitID] = struct{}{}
		ancestry, err := c.store.CommitAncestryIDs(tenantCtx, repoID, commitID)
		if err != nil {
			// A root that names a commit ID which isn't actually a
			// registered commit (e.g. an active transaction anchoring a
			// bare object before any commit references it) still roots
			// that literal ID; it just has no further history to expand.
			if errors.Is(err, postgres.ErrCommitNotFound) {
				return nil
			}
			return err
		}
		for _, id := range ancestry {
			retainedCommits[id] = struct{}{}
		}
		return nil
	}

	branches, err := c.store.ListBranchRefs(tenantCtx, repoID)
	if err != nil {
		return Report{}, fmt.Errorf("retention: collect: list branch refs: %w", err)
	}
	for _, branch := range branches {
		// Live and soft-deleted branches are treated identically: per
		// adr-immutable-commit-retention-on-branch-deletion, a deleted
		// branch's history remains a valid reachability root.
		if err := addCommitRoot(branch.HeadCommitID); err != nil {
			return Report{}, fmt.Errorf("retention: collect: branch %q history: %w", branch.Name, err)
		}
	}

	rootedObjectIDs := make(map[string]struct{})

	activeTxns, err := c.store.ListActiveTransactions(tenantCtx, repoID)
	if err != nil {
		return Report{}, fmt.Errorf("retention: collect: list active transactions: %w", err)
	}
	for _, txn := range activeTxns {
		if err := addCommitRoot(txn.CommitID); err != nil {
			return Report{}, fmt.Errorf("retention: collect: active transaction %q history: %w", txn.TransactionID, err)
		}
		if txn.ObjectID != "" {
			rootedObjectIDs[txn.ObjectID] = struct{}{}
		}
	}

	auditRecords, err := c.store.ListAuditRecords(tenantCtx, repoID)
	if err != nil {
		return Report{}, fmt.Errorf("retention: collect: list audit records: %w", err)
	}
	for _, record := range auditRecords {
		if err := addCommitRoot(record.CommitID); err != nil {
			return Report{}, fmt.Errorf("retention: collect: audit record %q history: %w", record.ID, err)
		}
		if record.ObjectID != "" {
			rootedObjectIDs[record.ObjectID] = struct{}{}
		}
	}

	objectToPack, err := c.store.ListNativePackObjectIDs(tenantCtx, repoID)
	if err != nil {
		return Report{}, fmt.Errorf("retention: collect: list native pack object IDs: %w", err)
	}
	rootedPackIDs := make(map[string]struct{})
	for objectID := range rootedObjectIDs {
		if packID, ok := objectToPack[objectID]; ok {
			rootedPackIDs[packID] = struct{}{}
		}
	}

	packs, err := c.store.ListNativePacks(tenantCtx, repoID)
	if err != nil {
		return Report{}, fmt.Errorf("retention: collect: list native packs: %w", err)
	}

	report := Report{TenantID: tenantID, RepoID: repoID, DryRun: dryRun}
	retainedHashes := make(map[string]struct{})
	var candidates []postgres.NativePackSummary
	for _, pack := range packs {
		_, retainedByCommit := retainedCommits[pack.CommitID]
		_, retainedByObject := rootedPackIDs[pack.PackID]
		if pack.CommitID != "" && retainedByCommit || retainedByObject {
			retainedHashes[pack.CASPackHash] = struct{}{}
			report.Retained = append(report.Retained, RetainedPack{PackID: pack.PackID, CASPackHash: pack.CASPackHash})
			continue
		}
		candidates = append(candidates, pack)
	}

	deleter, canDeleteCAS := c.driver.(PackDeleter)
	handledHashes := make(map[string]struct{})
	for _, pack := range candidates {
		if !dryRun {
			if err := c.store.DeleteNativePack(tenantCtx, repoID, pack.PackID); err != nil {
				return Report{}, fmt.Errorf("retention: collect: delete native pack %q: %w", pack.PackID, err)
			}
		}

		collected := CollectedPack{PackID: pack.PackID, CASPackHash: pack.CASPackHash}
		if _, stillRetained := retainedHashes[pack.CASPackHash]; !stillRetained {
			if _, alreadyHandled := handledHashes[pack.CASPackHash]; !alreadyHandled {
				handledHashes[pack.CASPackHash] = struct{}{}
				if canDeleteCAS {
					if !dryRun {
						if err := deleter.DeletePack(tenantCtx, scope, pack.CASPackHash); err != nil {
							return Report{}, fmt.Errorf("retention: collect: delete CAS pack %q: %w", pack.CASPackHash, err)
						}
					}
					collected.CASBlobCollected = true
				}
			}
		}
		report.Collected = append(report.Collected, collected)
	}

	sort.Slice(report.Retained, func(i, j int) bool { return report.Retained[i].PackID < report.Retained[j].PackID })
	sort.Slice(report.Collected, func(i, j int) bool { return report.Collected[i].PackID < report.Collected[j].PackID })

	return report, nil
}
