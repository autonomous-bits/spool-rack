package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/autonomous-bits/spool/graphcontract"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrTenantNotFound indicates the requested tenant does not exist.
	ErrTenantNotFound = errors.New("tenant not found")
	// ErrRepositoryNotFound indicates the target repository was not found.
	ErrRepositoryNotFound = errors.New("repository not found")
	// ErrBranchNotFound indicates the requested branch ref was not found.
	ErrBranchNotFound = errors.New("branch not found")
	// ErrNonFastForward indicates a push cannot be fast-forwarded.
	ErrNonFastForward = errors.New("non-fast-forward ref update rejected")
	// ErrCommitNotFound indicates that a requested commit is not visible in the
	// repository scoped by the current tenant context.
	ErrCommitNotFound = errors.New("commit not found")
	// ErrImmutableMetadataMismatch indicates a caller attempted to reuse an
	// immutable commit or pack ID with different metadata.
	ErrImmutableMetadataMismatch = errors.New("immutable metadata does not match existing content ID")
	// ErrNoCommonAncestor indicates that two complete commit histories do not
	// share an ancestor.
	ErrNoCommonAncestor = errors.New("no common ancestor")
	// ErrCommitHistoryIncomplete indicates that an ancestry chain references a
	// parent which is not available in the requested repository.
	ErrCommitHistoryIncomplete = errors.New("commit history is incomplete")
	// ErrCommitHistoryTooDeep indicates that ancestry could not be resolved
	// within the bounded merge-history traversal limit.
	ErrCommitHistoryTooDeep = errors.New("commit history exceeds traversal limit")
	// ErrMissingTenantContext indicates a caller attempted a tenant-scoped operation
	// without first attaching a validated tenant identifier to the request context.
	ErrMissingTenantContext = errors.New("postgres: no tenant context set")
	// ErrTenantNameConflict indicates an attempted tenant creation reused an
	// existing globally-unique tenant name.
	ErrTenantNameConflict = errors.New("postgres: tenant name already exists")
	// ErrMergeLeaseHeld indicates another unexpired lease owns the target branch.
	ErrMergeLeaseHeld = errors.New("postgres: target branch merge lease is held")
	// ErrMergeLeaseNotFound indicates no lease exists for a target branch.
	ErrMergeLeaseNotFound = errors.New("postgres: target branch merge lease not found")
	// ErrMergeLeaseOwnership indicates a lease token is not owned by its subject.
	ErrMergeLeaseOwnership = errors.New("postgres: target branch merge lease is not owned by subject")
	// ErrMergeLeaseExpired indicates an otherwise matching lease is expired.
	ErrMergeLeaseExpired = errors.New("postgres: target branch merge lease has expired")
	// ErrMergeLeaseMismatch indicates a merge apply no longer matches its lease.
	ErrMergeLeaseMismatch = errors.New("postgres: merge apply does not match lease")
	// ErrInvalidMergeLease indicates invalid lease acquisition input.
	ErrInvalidMergeLease = errors.New("postgres: invalid target branch merge lease")
	// ErrNativeObjectNotFound indicates the requested native pack object is
	// not indexed for the tenant carried by ctx's repository. It is
	// deliberately returned for both an absent object and one that exists
	// only under a different tenant.
	ErrNativeObjectNotFound = errors.New("postgres: native pack object not found")
	errNilContext           = errors.New("postgres: nil context")

	uuidV4Pattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)

type contextKey int

const tenantIDContextKey contextKey = iota

const maxMergeAncestryCommits = 100

// Store defines the metadata store operations for multi-tenant repositories.
type Store interface {
	// SetTenantContext returns a child context carrying the validated tenant ID
	// that later database operations must project into PostgreSQL's RLS session
	// setting before reading or mutating tenant-scoped rows.
	SetTenantContext(ctx context.Context, tenantID string) (context.Context, error)
	// CreateTenant creates a new tenant row and returns its generated UUID.
	CreateTenant(ctx context.Context, name string) (string, error)
	// CreateRepository creates a repository within the tenant carried by ctx and
	// returns its generated UUID.
	CreateRepository(ctx context.Context, name string) (string, error)
	// CreateBranch creates a branch ref within the tenant carried by ctx pointing
	// at the supplied head commit.
	CreateBranch(ctx context.Context, repoID, name, headCommitID string) error
	// PutCommit registers a canonical Spool commit and its complete ordered
	// parent collection. It is idempotent only for identical immutable metadata.
	PutCommit(ctx context.Context, repoID string, commitID graphcontract.ObjectID, commit graphcontract.Commit) error
	// PutPackRange registers the immutable CAS pack created by a successful
	// push and the contiguous commit range that it contains.
	PutPackRange(ctx context.Context, repoID, packHash, baseCommitID, targetCommitID string) error
	// GetBranchRef resolves the commit identifier for a named branch.
	GetBranchRef(ctx context.Context, repoID, branch string) (string, error)
	// IsAncestor reports whether ancestorCommit is reachable through either
	// ordered parent starting at commit (inclusive — a commit is considered
	// its own ancestor), scoped to repoID within the tenant
	// carried by ctx. Used to verify fast-forward reachability during push,
	// per req-ancestry-verified-remote-fast-forward. If commit does not exist
	// in the repository, IsAncestor returns false, nil.
	IsAncestor(ctx context.Context, repoID, ancestorCommit, commit string) (bool, error)
	// FindLowestCommonAncestor finds the nearest commit shared by sourceCommitID
	// and targetCommitID, traversing no more than 100 commits from either tip.
	// It returns ErrCommitNotFound for a missing tip, ErrCommitHistoryIncomplete
	// for a missing parent in an otherwise-present history,
	// ErrCommitHistoryTooDeep when the bounded traversal is exhausted, and
	// ErrNoCommonAncestor when complete histories have no shared ancestor.
	FindLowestCommonAncestor(ctx context.Context, repoID, sourceCommitID, targetCommitID string) (string, error)
	// GetCommitSnapshotRoot returns the immutable snapshot root recorded for a
	// commit, or ErrCommitNotFound when that commit is not visible in repoID.
	GetCommitSnapshotRoot(ctx context.Context, repoID, commitID string) (string, error)
	// GetCommitMetadata returns the immutable snapshot root and framing format
	// for a commit visible in the tenant-scoped repository.
	GetCommitMetadata(ctx context.Context, repoID, commitID string) (CommitMetadata, error)
	// GetPackRanges returns the pack ranges required to reconstruct the
	// history from headCommitID back to knownCommitID. Ranges are returned
	// newest-first so callers can validate the chain and stream it in reverse.
	GetPackRanges(ctx context.Context, repoID, headCommitID, knownCommitID string) ([]PackRange, error)
	// CompareAndSwapBranchRef updates a branch ref only if current matches expected.
	CompareAndSwapBranchRef(ctx context.Context, repoID, branch, expectedCommit, newCommit string) error
	// AcquireMergeLease exclusively reserves a target branch for a merge. The
	// lease is owned by Subject and may replace only an expired lease.
	AcquireMergeLease(ctx context.Context, request MergeLeaseRequest) (MergeLease, error)
	// ValidateMergeLease returns a lease only when its token is currently owned
	// by subject and it has not expired.
	ValidateMergeLease(ctx context.Context, repoID, targetBranch, subject, token string) (MergeLease, error)
	// ReleaseMergeLease abandons a lease only when subject owns token.
	ReleaseMergeLease(ctx context.Context, repoID, targetBranch, subject, token string) error
	// ApplyMerge atomically registers a CAS payload that has already been
	// written by the caller, creates the merge commit and its two parents,
	// advances the target ref with a compare-and-swap, and consumes its lease.
	ApplyMerge(ctx context.Context, request ApplyMergeRequest) error
	// PutNativePack registers a verified native Spool pack (graphcontract's
	// binary pack container format) and the location of every object it
	// carries, scoped to the tenant carried by ctx. Re-registering the same
	// packID is idempotent only for identical immutable metadata; entries
	// sharing an already-indexed object ID are left pointing at their first
	// recorded location, since an object ID is a content hash and any two
	// verified entries sharing one must carry identical bytes.
	PutNativePack(ctx context.Context, repoID, packID, casPackHash, commitID string, entries []graphcontract.PackIndexEntry) error
	// GetNativeObjectLocation resolves a verified native pack object's
	// storage location scoped to the tenant carried by ctx, returning
	// ErrNativeObjectNotFound when the object is not indexed for this
	// tenant's repository — including when it exists only for a different
	// tenant, which RLS and the repo scope keep indistinguishable from an
	// absent object.
	GetNativeObjectLocation(ctx context.Context, repoID, objectID string) (NativeObjectLocation, error)
}

// PackRange identifies one immutable pack and the commit interval it contains.
type PackRange struct {
	PackHash       string
	BaseCommitID   string
	TargetCommitID string
	Format         uint32
}

// CommitMetadata is the storage identity returned for a commit without
// exposing mutable branch state.
type CommitMetadata struct {
	ID           string
	SnapshotRoot string
	Format       uint32
}

// MergeLeaseRequest identifies the merge preview that is reserving a target
// branch. Duration is evaluated by PostgreSQL when the lease is acquired.
type MergeLeaseRequest struct {
	RepoID         string
	TargetBranch   string
	Subject        string
	SourceCommitID string
	TargetCommitID string
	BaseCommitID   string
	Duration       time.Duration
}

// MergeLease is a persisted, target-branch-exclusive merge reservation.
// Token is generated by the store and must be retained by the caller to apply
// the merge.
type MergeLease struct {
	RepoID         string
	TargetBranch   string
	Subject        string
	Token          string
	SourceCommitID string
	TargetCommitID string
	BaseCommitID   string
	ExpiresAt      time.Time
}

// ApplyMergeRequest supplies all metadata needed to register one merge after
// its immutable CAS pack has already been durably written.
type ApplyMergeRequest struct {
	RepoID         string
	TargetBranch   string
	Subject        string
	LeaseToken     string
	SourceCommitID string
	TargetCommitID string
	BaseCommitID   string
	ResultCommitID string
	SnapshotRoot   string
	Author         string
	Message        string
	PackHash       string
	CommitFormat   uint32
	PackFormat     uint32
	CommitTime     time.Time
}

// NativeObjectLocation identifies where one verified native pack object's
// compressed bytes live: inside the CAS pack named by CASPackHash, at
// [Offset, Offset+CompressedSize).
type NativeObjectLocation struct {
	PackID           string
	CASPackHash      string
	Offset           uint64
	CompressedSize   uint64
	UncompressedSize uint64
	CRC32            uint32
}

// PGStore is a PostgreSQL-backed implementation of Store.
type PGStore struct {
	pool *pgxpool.Pool
}

var _ Store = (*PGStore)(nil)

// Open connects to PostgreSQL, verifies the connection is usable, and returns
// a store backed by a pgx connection pool.
func Open(ctx context.Context, dsn string) (*PGStore, error) {
	if ctx == nil {
		return nil, errNilContext
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	return &PGStore{pool: pool}, nil
}

// Close releases the underlying PostgreSQL connection pool.
func (s *PGStore) Close() {
	if s == nil || s.pool == nil {
		return
	}
	s.pool.Close()
}

// SetTenantContext validates tenantID and returns a child context carrying it
// for later tenant-scoped PostgreSQL operations.
func (s *PGStore) SetTenantContext(ctx context.Context, tenantID string) (context.Context, error) {
	if ctx == nil {
		return nil, errNilContext
	}
	if !uuidV4Pattern.MatchString(tenantID) {
		return nil, fmt.Errorf("postgres: invalid tenant id %q", tenantID)
	}
	return context.WithValue(ctx, tenantIDContextKey, tenantID), nil
}

// CreateTenant inserts a new tenant row and returns its generated UUID.
func (s *PGStore) CreateTenant(ctx context.Context, name string) (string, error) {
	tenantID, err := newUUIDv4()
	if err != nil {
		return "", fmt.Errorf("postgres: create tenant %q: %w", name, err)
	}

	if err := s.withConfiguredTenantTx(ctx, tenantID, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $2)`, tenantID, name); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("postgres: create tenant %q: %w", name, ErrTenantNameConflict)
			}
			return fmt.Errorf("postgres: create tenant %q: insert tenant: %w", name, err)
		}
		return nil
	}); err != nil {
		return "", err
	}

	return tenantID, nil
}

// CreateRepository inserts a repository row scoped to the tenant carried by ctx.
func (s *PGStore) CreateRepository(ctx context.Context, name string) (string, error) {
	tenantID, err := requireTenantID(ctx)
	if err != nil {
		return "", fmt.Errorf("postgres: create repository %q: %w", name, err)
	}

	repoID, err := newUUIDv4()
	if err != nil {
		return "", fmt.Errorf("postgres: create repository %q: %w", name, err)
	}

	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO repositories (id, tenant_id, name) VALUES ($1, $2, $3)`, repoID, tenantID, name); err != nil {
			return fmt.Errorf("postgres: create repository %q: insert repository: %w", name, err)
		}
		return nil
	}); err != nil {
		return "", err
	}

	return repoID, nil
}

// CreateBranch inserts a branch ref scoped to the tenant carried by ctx.
func (s *PGStore) CreateBranch(ctx context.Context, repoID, name, headCommitID string) error {
	tenantID, err := requireTenantID(ctx)
	if err != nil {
		return fmt.Errorf("postgres: create branch %q for repo %s: %w", name, repoID, err)
	}

	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO branches (tenant_id, repo_id, name, head_commit_id) VALUES ($1, $2, $3, $4)`, tenantID, repoID, name, headCommitID); err != nil {
			return fmt.Errorf("postgres: create branch %q for repo %s: insert branch: %w", name, repoID, err)
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// PutCommit inserts a content-addressed commit row scoped to the tenant
// carried by ctx. Re-registering the same commitID is a no-op.
func (s *PGStore) PutCommit(ctx context.Context, repoID string, commitID graphcontract.ObjectID, commit graphcontract.Commit) error {
	return s.putCommit(ctx, repoID, commitID, commit, 1)
}

// PutCommitWithFormat records a commit using its explicit frame format.
func (s *PGStore) PutCommitWithFormat(ctx context.Context, repoID string, commitID graphcontract.ObjectID, commit graphcontract.Commit, format uint32) error {
	if !validObjectFormat(format) {
		return fmt.Errorf("postgres: put commit for repo %s: unsupported object format %d", repoID, format)
	}
	return s.putCommit(ctx, repoID, commitID, commit, normalizeObjectFormat(format))
}

func (s *PGStore) putCommit(ctx context.Context, repoID string, commitID graphcontract.ObjectID, commit graphcontract.Commit, format uint32) error {
	tenantID, err := requireTenantID(ctx)
	if err != nil {
		return fmt.Errorf("postgres: put commit for repo %s: %w", repoID, err)
	}
	normalized, err := commit.Normalize()
	if err != nil {
		return fmt.Errorf("postgres: put commit for repo %s: canonical commit: %w", repoID, err)
	}

	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		result, err := tx.Exec(ctx, `
			INSERT INTO commits (id, tenant_id, repo_id, snapshot_root, object_format, author, message, commit_time)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (tenant_id, repo_id, id) DO NOTHING
		`, string(commitID), tenantID, repoID, string(normalized.Snapshot), format, normalized.Author, normalized.Message, normalized.Time)
		if err != nil {
			return fmt.Errorf("postgres: put commit for repo %s: insert commit: %w", repoID, err)
		}
		existingCommit := result.RowsAffected() == 0
		if existingCommit {
			var existing struct {
				snapshotRoot string
				format       uint32
				author       string
				message      string
				time         time.Time
			}
			if err := tx.QueryRow(ctx, `
				SELECT snapshot_root, object_format, author, message, commit_time
				FROM commits
				WHERE tenant_id = $1 AND repo_id = $2 AND id = $3
				FOR KEY SHARE
			`, tenantID, repoID, string(commitID)).Scan(
				&existing.snapshotRoot, &existing.format, &existing.author, &existing.message, &existing.time,
			); err != nil {
				return fmt.Errorf("postgres: put commit for repo %s: read existing commit: %w", repoID, err)
			}
			if existing.snapshotRoot != string(normalized.Snapshot) || existing.format != format ||
				existing.author != normalized.Author || existing.message != normalized.Message ||
				!existing.time.Equal(normalized.Time) {
				return ErrImmutableMetadataMismatch
			}
		}

		existingParents, err := commitParentIDs(ctx, tx, repoID, string(commitID))
		if err != nil {
			return fmt.Errorf("postgres: put commit for repo %s: read existing parents: %w", repoID, err)
		}
		if existingCommit {
			if len(existingParents) != len(normalized.Parents) {
				return ErrImmutableMetadataMismatch
			}
			for i, parent := range normalized.Parents {
				if existingParents[i] != string(parent) {
					return ErrImmutableMetadataMismatch
				}
			}
			return nil
		}
		if len(normalized.Parents) == 0 {
			return nil
		}
		parentIDs := make([]string, len(normalized.Parents))
		for i, parent := range normalized.Parents {
			parentIDs[i] = string(parent)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO commit_parents (tenant_id, repo_id, commit_id, parent_position, parent_commit_id)
			SELECT $1, $2, $3, parent_position, parent_commit_id
			FROM unnest($4::text[]) WITH ORDINALITY AS parent(parent_commit_id, parent_position)
		`, tenantID, repoID, string(commitID), parentIDs); err != nil {
			return fmt.Errorf("postgres: put commit for repo %s: insert commit parents: %w", repoID, err)
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// PutPackRange records the commit range carried by an immutable CAS pack.
func (s *PGStore) PutPackRange(ctx context.Context, repoID, packHash, baseCommitID, targetCommitID string) error {
	return s.putPackRange(ctx, repoID, packHash, baseCommitID, targetCommitID, 1)
}

// PutPackRangeWithFormat records an explicitly framed v2 pack range.
func (s *PGStore) PutPackRangeWithFormat(ctx context.Context, repoID, packHash, baseCommitID, targetCommitID string, format uint32) error {
	if !validObjectFormat(format) {
		return fmt.Errorf("postgres: put pack range for repo %s: unsupported object format %d", repoID, format)
	}
	return s.putPackRange(ctx, repoID, packHash, baseCommitID, targetCommitID, normalizeObjectFormat(format))
}

func (s *PGStore) putPackRange(ctx context.Context, repoID, packHash, baseCommitID, targetCommitID string, format uint32) error {
	tenantID, err := requireTenantID(ctx)
	if err != nil {
		return fmt.Errorf("postgres: put pack range for repo %s: %w", repoID, err)
	}

	var base any
	if baseCommitID != "" {
		base = baseCommitID
	}

	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		result, err := tx.Exec(ctx, `
			INSERT INTO pack_ranges (tenant_id, repo_id, pack_hash, object_format, base_commit_id, target_commit_id)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (tenant_id, repo_id, pack_hash) DO NOTHING
		`, tenantID, repoID, packHash, format, base, targetCommitID)
		if err != nil {
			return fmt.Errorf("postgres: put pack range for repo %s: insert pack range: %w", repoID, err)
		}
		if result.RowsAffected() == 0 {
			var existing struct {
				base   *string
				target string
				format uint32
			}
			if err := tx.QueryRow(ctx, `
				SELECT base_commit_id, target_commit_id, object_format
				FROM pack_ranges
				WHERE tenant_id = $1 AND repo_id = $2 AND pack_hash = $3
				FOR KEY SHARE
			`, tenantID, repoID, packHash).Scan(&existing.base, &existing.target, &existing.format); err != nil {
				return fmt.Errorf("postgres: put pack range for repo %s: read existing pack range: %w", repoID, err)
			}
			existingBase := ""
			if existing.base != nil {
				existingBase = *existing.base
			}
			if existingBase != baseCommitID || existing.target != targetCommitID || existing.format != format {
				return ErrImmutableMetadataMismatch
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// AcquireMergeLease creates a target-branch-exclusive merge lease. Locking the
// branch makes the supplied target commit identity stable while the lease is
// created, and PostgreSQL's conflict lock makes concurrent contenders observe
// the unexpired winner.
func (s *PGStore) AcquireMergeLease(ctx context.Context, request MergeLeaseRequest) (MergeLease, error) {
	tenantID, err := requireTenantID(ctx)
	if err != nil {
		return MergeLease{}, fmt.Errorf("postgres: acquire merge lease: %w", err)
	}
	if request.RepoID == "" || request.TargetBranch == "" || request.Subject == "" ||
		request.SourceCommitID == "" || request.TargetCommitID == "" || request.BaseCommitID == "" ||
		request.Duration <= 0 {
		return MergeLease{}, fmt.Errorf("postgres: acquire merge lease: %w", ErrInvalidMergeLease)
	}

	token, err := newOpaqueToken()
	if err != nil {
		return MergeLease{}, fmt.Errorf("postgres: acquire merge lease: %w", err)
	}
	lease := MergeLease{
		RepoID: request.RepoID, TargetBranch: request.TargetBranch, Subject: request.Subject,
		Token: token, SourceCommitID: request.SourceCommitID, TargetCommitID: request.TargetCommitID,
		BaseCommitID: request.BaseCommitID,
	}
	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		head, err := branchHeadForUpdate(ctx, tx, request.RepoID, request.TargetBranch)
		if err != nil {
			return err
		}
		if head != request.TargetCommitID {
			return ErrNonFastForward
		}

		err = tx.QueryRow(ctx, `
			INSERT INTO target_branch_merge_leases (
				tenant_id, repo_id, target_branch, subject, lease_token,
				source_commit_id, target_commit_id, base_commit_id, expires_at
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now() + $9::interval)
			ON CONFLICT (tenant_id, repo_id, target_branch) DO UPDATE
			SET subject = EXCLUDED.subject,
				lease_token = EXCLUDED.lease_token,
				source_commit_id = EXCLUDED.source_commit_id,
				target_commit_id = EXCLUDED.target_commit_id,
				base_commit_id = EXCLUDED.base_commit_id,
				expires_at = EXCLUDED.expires_at,
				created_at = now()
			WHERE target_branch_merge_leases.expires_at <= now()
			RETURNING expires_at
		`, tenantID, request.RepoID, request.TargetBranch, request.Subject, token,
			request.SourceCommitID, request.TargetCommitID, request.BaseCommitID, request.Duration.String()).Scan(&lease.ExpiresAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrMergeLeaseHeld
		}
		if err != nil {
			return fmt.Errorf("insert lease: %w", err)
		}
		return nil
	}); err != nil {
		return MergeLease{}, fmt.Errorf("postgres: acquire merge lease: %w", err)
	}
	return lease, nil
}

// ValidateMergeLease validates that subject still owns an unexpired lease.
// ApplyMerge repeats this validation under a row lock, so this method is safe
// to use before the caller writes its immutable CAS object.
func (s *PGStore) ValidateMergeLease(ctx context.Context, repoID, targetBranch, subject, token string) (MergeLease, error) {
	if _, err := requireTenantID(ctx); err != nil {
		return MergeLease{}, fmt.Errorf("postgres: validate merge lease: %w", err)
	}
	var lease MergeLease
	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var expiresAt time.Time
		err := tx.QueryRow(ctx, `
			SELECT subject, lease_token, source_commit_id, target_commit_id, base_commit_id, expires_at
			FROM target_branch_merge_leases
			WHERE repo_id = $1 AND target_branch = $2
		`, repoID, targetBranch).Scan(&lease.Subject, &lease.Token, &lease.SourceCommitID,
			&lease.TargetCommitID, &lease.BaseCommitID, &expiresAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrMergeLeaseNotFound
		}
		if err != nil {
			return fmt.Errorf("query lease: %w", err)
		}
		if lease.Subject != subject || lease.Token != token {
			return ErrMergeLeaseOwnership
		}
		if !expiresAt.After(time.Now()) {
			return ErrMergeLeaseExpired
		}
		lease.RepoID, lease.TargetBranch, lease.ExpiresAt = repoID, targetBranch, expiresAt
		return nil
	}); err != nil {
		return MergeLease{}, fmt.Errorf("postgres: validate merge lease: %w", err)
	}
	return lease, nil
}

// ReleaseMergeLease deletes a lease only after checking its ownership under a
// row lock. Expired leases may also be released, allowing callers to clean up
// a preview that they no longer intend to apply.
func (s *PGStore) ReleaseMergeLease(ctx context.Context, repoID, targetBranch, subject, token string) error {
	if _, err := requireTenantID(ctx); err != nil {
		return fmt.Errorf("postgres: release merge lease: %w", err)
	}
	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var (
			leaseSubject string
			leaseToken   string
		)
		err := tx.QueryRow(ctx, `
			SELECT subject, lease_token
			FROM target_branch_merge_leases
			WHERE repo_id = $1 AND target_branch = $2
			FOR UPDATE
		`, repoID, targetBranch).Scan(&leaseSubject, &leaseToken)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrMergeLeaseNotFound
		}
		if err != nil {
			return fmt.Errorf("lock lease: %w", err)
		}
		if leaseSubject != subject || leaseToken != token {
			return ErrMergeLeaseOwnership
		}
		result, err := tx.Exec(ctx, `
			DELETE FROM target_branch_merge_leases
			WHERE repo_id = $1 AND target_branch = $2 AND subject = $3 AND lease_token = $4
		`, repoID, targetBranch, subject, token)
		if err != nil {
			return fmt.Errorf("delete lease: %w", err)
		}
		if result.RowsAffected() != 1 {
			return ErrMergeLeaseNotFound
		}
		return nil
	}); err != nil {
		return fmt.Errorf("postgres: release merge lease: %w", err)
	}
	return nil
}

// ApplyMerge performs the database half of applying a merge. The caller owns
// writing the pack to CAS before this call; this method never writes CAS.
func (s *PGStore) ApplyMerge(ctx context.Context, request ApplyMergeRequest) error {
	tenantID, err := requireTenantID(ctx)
	if err != nil {
		return fmt.Errorf("postgres: apply merge: %w", err)
	}
	if request.RepoID == "" || request.TargetBranch == "" || request.Subject == "" ||
		request.LeaseToken == "" || request.SourceCommitID == "" || request.TargetCommitID == "" ||
		request.BaseCommitID == "" || request.ResultCommitID == "" || request.SnapshotRoot == "" ||
		request.Author == "" || request.Message == "" || request.PackHash == "" ||
		!validObjectFormat(request.CommitFormat) || !validObjectFormat(request.PackFormat) {
		return fmt.Errorf("postgres: apply merge: %w", ErrInvalidMergeLease)
	}

	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		head, err := branchHeadForUpdate(ctx, tx, request.RepoID, request.TargetBranch)
		if err != nil {
			return err
		}
		if head != request.TargetCommitID {
			return ErrNonFastForward
		}

		var lease MergeLease
		err = tx.QueryRow(ctx, `
			SELECT subject, lease_token, source_commit_id, target_commit_id, base_commit_id, expires_at
			FROM target_branch_merge_leases
			WHERE repo_id = $1 AND target_branch = $2
			FOR UPDATE
		`, request.RepoID, request.TargetBranch).Scan(&lease.Subject, &lease.Token,
			&lease.SourceCommitID, &lease.TargetCommitID, &lease.BaseCommitID, &lease.ExpiresAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrMergeLeaseNotFound
		}
		if err != nil {
			return fmt.Errorf("lock lease: %w", err)
		}
		if lease.Subject != request.Subject || lease.Token != request.LeaseToken {
			return ErrMergeLeaseOwnership
		}
		if !lease.ExpiresAt.After(time.Now()) {
			return ErrMergeLeaseExpired
		}
		if lease.SourceCommitID != request.SourceCommitID ||
			lease.TargetCommitID != request.TargetCommitID ||
			lease.BaseCommitID != request.BaseCommitID {
			return ErrMergeLeaseMismatch
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO commits (id, tenant_id, repo_id, snapshot_root, object_format, author, message, commit_time)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, request.ResultCommitID, tenantID, request.RepoID, request.SnapshotRoot,
			normalizeObjectFormat(request.CommitFormat), request.Author, request.Message, request.CommitTime.UTC().Truncate(time.Second)); err != nil {
			return fmt.Errorf("insert merge commit: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO commit_parents (tenant_id, repo_id, commit_id, parent_position, parent_commit_id)
			VALUES ($1, $2, $3, 1, $4), ($1, $2, $3, 2, $5)
		`, tenantID, request.RepoID, request.ResultCommitID, request.TargetCommitID, request.SourceCommitID); err != nil {
			return fmt.Errorf("insert merge parents: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO pack_ranges (tenant_id, repo_id, pack_hash, object_format, base_commit_id, target_commit_id)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, tenantID, request.RepoID, request.PackHash, normalizeObjectFormat(request.PackFormat), request.TargetCommitID, request.ResultCommitID); err != nil {
			return fmt.Errorf("insert merge pack range: %w", err)
		}

		result, err := tx.Exec(ctx, `
			UPDATE branches
			SET head_commit_id = $1, updated_at = now()
			WHERE repo_id = $2 AND name = $3 AND head_commit_id = $4
		`, request.ResultCommitID, request.RepoID, request.TargetBranch, request.TargetCommitID)
		if err != nil {
			return fmt.Errorf("cas target branch: %w", err)
		}
		if result.RowsAffected() != 1 {
			return ErrNonFastForward
		}
		result, err = tx.Exec(ctx, `
			DELETE FROM target_branch_merge_leases
			WHERE repo_id = $1 AND target_branch = $2 AND subject = $3 AND lease_token = $4
				AND source_commit_id = $5 AND target_commit_id = $6 AND base_commit_id = $7
		`, request.RepoID, request.TargetBranch, request.Subject, request.LeaseToken,
			request.SourceCommitID, request.TargetCommitID, request.BaseCommitID)
		if err != nil {
			return fmt.Errorf("consume lease: %w", err)
		}
		if result.RowsAffected() != 1 {
			return ErrMergeLeaseMismatch
		}
		return nil
	}); err != nil {
		return fmt.Errorf("postgres: apply merge: %w", err)
	}
	return nil
}

func branchHeadForUpdate(ctx context.Context, tx pgx.Tx, repoID, branch string) (string, error) {
	var head string
	if err := tx.QueryRow(ctx, `
		SELECT head_commit_id
		FROM branches
		WHERE repo_id = $1 AND name = $2
		FOR UPDATE
	`, repoID, branch).Scan(&head); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrBranchNotFound
		}
		return "", fmt.Errorf("lock branch: %w", err)
	}
	return head, nil
}

// GetBranchRef resolves the current head commit for branch in repoID.
func (s *PGStore) GetBranchRef(ctx context.Context, repoID, branch string) (string, error) {
	if _, err := requireTenantID(ctx); err != nil {
		return "", fmt.Errorf("postgres: get branch ref: %w", err)
	}

	var headCommitID string
	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT head_commit_id FROM branches WHERE repo_id = $1 AND name = $2`, repoID, branch).Scan(&headCommitID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("postgres: get branch ref: %w", ErrBranchNotFound)
			}
			return fmt.Errorf("postgres: get branch ref: query branch ref: %w", err)
		}
		return nil
	}); err != nil {
		return "", err
	}

	return headCommitID, nil
}

// IsAncestor reports whether ancestorCommit is reachable from commit through
// either ordered parent within repoID for the tenant carried by ctx. A commit
// is considered its own ancestor. If commit does not exist in the repository,
// IsAncestor returns false, nil.
func (s *PGStore) IsAncestor(ctx context.Context, repoID, ancestorCommit, commit string) (bool, error) {
	if _, err := requireTenantID(ctx); err != nil {
		return false, fmt.Errorf("postgres: is ancestor: %w", err)
	}

	var found bool
	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `
				WITH RECURSIVE ancestry AS (
					SELECT id, ARRAY[id]::text[] AS path
					FROM commits
					WHERE repo_id = $1 AND id = $2
					UNION ALL
					SELECT c.id, a.path || c.id
					FROM ancestry a
					JOIN commit_parents p ON p.commit_id = a.id
					JOIN commits c ON c.id = p.parent_commit_id
					WHERE p.repo_id = $1 AND c.repo_id = $1
					AND NOT c.id = ANY(a.path)
			)
			SELECT EXISTS (SELECT 1 FROM ancestry WHERE id = $3)
		`, repoID, commit, ancestorCommit).Scan(&found); err != nil {
			return fmt.Errorf("postgres: is ancestor: query ancestry: %w", err)
		}
		return nil
	}); err != nil {
		return false, err
	}

	return found, nil
}

// FindLowestCommonAncestor returns the nearest shared ancestor of source and
// target. Merge preview only needs a bounded history window, so each recursive
// query is capped at maxMergeAncestryCommits commits.
func (s *PGStore) FindLowestCommonAncestor(ctx context.Context, repoID, sourceCommitID, targetCommitID string) (string, error) {
	if _, err := requireTenantID(ctx); err != nil {
		return "", fmt.Errorf("postgres: find lowest common ancestor: %w", err)
	}

	var ancestor string
	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		sourceHistory, err := collectMergeAncestry(ctx, tx, repoID, sourceCommitID)
		if err != nil {
			return fmt.Errorf("postgres: find lowest common ancestor: source history: %w", err)
		}
		targetHistory, err := collectMergeAncestry(ctx, tx, repoID, targetCommitID)
		if err != nil {
			return fmt.Errorf("postgres: find lowest common ancestor: target history: %w", err)
		}

		bestDistance := -1
		bestMaximumDepth := -1
		for commitID, sourceDepth := range sourceHistory {
			targetDepth, ok := targetHistory[commitID]
			if !ok {
				continue
			}

			distance := sourceDepth + targetDepth
			maximumDepth := max(sourceDepth, targetDepth)
			if bestDistance == -1 ||
				distance < bestDistance ||
				(distance == bestDistance && maximumDepth < bestMaximumDepth) ||
				(distance == bestDistance && maximumDepth == bestMaximumDepth && commitID < ancestor) {
				ancestor = commitID
				bestDistance = distance
				bestMaximumDepth = maximumDepth
			}
		}
		if ancestor == "" {
			return ErrNoCommonAncestor
		}
		return nil
	}); err != nil {
		return "", err
	}

	return ancestor, nil
}

// GetCommitSnapshotRoot resolves a commit's snapshot root without exposing
// metadata belonging to another tenant or repository.
func (s *PGStore) GetCommitSnapshotRoot(ctx context.Context, repoID, commitID string) (string, error) {
	if _, err := requireTenantID(ctx); err != nil {
		return "", fmt.Errorf("postgres: get commit snapshot root: %w", err)
	}

	var snapshotRoot string
	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `
			SELECT snapshot_root
			FROM commits
			WHERE repo_id = $1 AND id = $2
		`, repoID, commitID).Scan(&snapshotRoot); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("postgres: get commit snapshot root: %w", ErrCommitNotFound)
			}
			return fmt.Errorf("postgres: get commit snapshot root: query commit: %w", err)
		}
		return nil
	}); err != nil {
		return "", err
	}

	return snapshotRoot, nil
}

// GetCommitMetadata resolves the immutable snapshot root and framing version
// of a visible commit. It is optional for older store fakes, keeping existing
// HTTP preview contracts compatible while allowing v2 frames to retain parent
// identity formats.
func (s *PGStore) GetCommitMetadata(ctx context.Context, repoID, commitID string) (CommitMetadata, error) {
	if _, err := requireTenantID(ctx); err != nil {
		return CommitMetadata{}, fmt.Errorf("postgres: get commit metadata: %w", err)
	}

	metadata := CommitMetadata{ID: commitID}
	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `
			SELECT snapshot_root, object_format
			FROM commits
			WHERE repo_id = $1 AND id = $2
		`, repoID, commitID).Scan(&metadata.SnapshotRoot, &metadata.Format); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("postgres: get commit metadata: %w", ErrCommitNotFound)
			}
			return fmt.Errorf("postgres: get commit metadata: query commit: %w", err)
		}
		return nil
	}); err != nil {
		return CommitMetadata{}, err
	}
	return metadata, nil
}

func collectMergeAncestry(ctx context.Context, tx pgx.Tx, repoID, commitID string) (map[string]int, error) {
	type queuedCommit struct {
		id    string
		depth int
	}

	history := make(map[string]int, maxMergeAncestryCommits)
	queue := []queuedCommit{{id: commitID, depth: 0}}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if _, seen := history[current.id]; seen {
			continue
		}
		if len(history) == maxMergeAncestryCommits {
			return nil, ErrCommitHistoryTooDeep
		}

		var exists bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM commits WHERE repo_id = $1 AND id = $2)
		`, repoID, current.id).Scan(&exists); err != nil {
			return nil, fmt.Errorf("query ancestry commit: %w", err)
		}
		if !exists {
			if len(history) == 0 {
				return nil, ErrCommitNotFound
			}
			return nil, ErrCommitHistoryIncomplete
		}
		history[current.id] = current.depth

		parents, err := commitParentIDs(ctx, tx, repoID, current.id)
		if err != nil {
			return nil, err
		}
		for _, parentID := range parents {
			if _, seen := history[parentID]; !seen {
				queue = append(queue, queuedCommit{id: parentID, depth: current.depth + 1})
			}
		}
	}
	return history, nil
}

func commitParentIDs(ctx context.Context, tx pgx.Tx, repoID, commitID string) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT parent_commit_id
		FROM commit_parents
		WHERE repo_id = $1 AND commit_id = $2
		ORDER BY parent_position
	`, repoID, commitID)
	if err != nil {
		return nil, fmt.Errorf("query commit parents: %w", err)
	}
	defer rows.Close()

	parents := make([]string, 0)
	for rows.Next() {
		var parentID string
		if err := rows.Scan(&parentID); err != nil {
			return nil, fmt.Errorf("scan commit parent: %w", err)
		}
		parents = append(parents, parentID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate commit parents: %w", err)
	}
	return parents, nil
}

func (s *PGStore) GetPackRanges(ctx context.Context, repoID, headCommitID, knownCommitID string) ([]PackRange, error) {
	if _, err := requireTenantID(ctx); err != nil {
		return nil, fmt.Errorf("postgres: get pack ranges: %w", err)
	}

	ranges := make([]PackRange, 0)
	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			WITH RECURSIVE pack_chain AS (
				SELECT pack_hash, base_commit_id, target_commit_id, object_format, 0 AS depth
				FROM pack_ranges
				WHERE repo_id = $1 AND target_commit_id = $2
				UNION ALL
				SELECT p.pack_hash, p.base_commit_id, p.target_commit_id, p.object_format, c.depth + 1
				FROM pack_ranges p
				JOIN pack_chain c ON p.target_commit_id = c.base_commit_id
				WHERE p.repo_id = $1 AND c.base_commit_id IS NOT NULL AND c.base_commit_id <> $3
			)
			SELECT pack_hash, COALESCE(base_commit_id, ''), target_commit_id, object_format
			FROM pack_chain
			ORDER BY depth ASC
		`, repoID, headCommitID, knownCommitID)
		if err != nil {
			return fmt.Errorf("postgres: get pack ranges: query pack chain: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var pack PackRange
			if err := rows.Scan(&pack.PackHash, &pack.BaseCommitID, &pack.TargetCommitID, &pack.Format); err != nil {
				return fmt.Errorf("postgres: get pack ranges: scan pack range: %w", err)
			}
			ranges = append(ranges, pack)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("postgres: get pack ranges: iterate pack ranges: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return ranges, nil
}

// CompareAndSwapBranchRef updates a branch head only when it still points at
// expectedCommit and no unexpired merge lease owns the branch. ApplyMerge is
// the sole path that can advance a leased target because it consumes its lease
// in the same transaction.
func (s *PGStore) CompareAndSwapBranchRef(ctx context.Context, repoID, branch, expectedCommit, newCommit string) error {
	if _, err := requireTenantID(ctx); err != nil {
		return fmt.Errorf("postgres: cas branch ref: %w", err)
	}

	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		head, err := branchHeadForUpdate(ctx, tx, repoID, branch)
		if err != nil {
			return err
		}
		if head != expectedCommit {
			return ErrNonFastForward
		}

		var leaseHeld bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM target_branch_merge_leases
				WHERE repo_id = $1 AND target_branch = $2 AND expires_at > now()
			)
		`, repoID, branch).Scan(&leaseHeld); err != nil {
			return fmt.Errorf("postgres: cas branch ref: check merge lease: %w", err)
		}
		if leaseHeld {
			return ErrMergeLeaseHeld
		}

		result, err := tx.Exec(ctx, `
			UPDATE branches
			SET head_commit_id = $1, updated_at = now()
			WHERE repo_id = $2 AND name = $3 AND head_commit_id = $4
		`, newCommit, repoID, branch, expectedCommit)
		if err != nil {
			return fmt.Errorf("postgres: cas branch ref: update branch ref: %w", err)
		}
		if result.RowsAffected() == 1 {
			return nil
		}
		return ErrNonFastForward
	}); err != nil {
		return err
	}

	return nil
}

// PutNativePack registers a verified native Spool pack and the location of
// every object it carries, scoped to the tenant carried by ctx. Callers must
// only pass entries that have already passed graphcontract's pack, header,
// entry-bounds, and per-object integrity verification: this method trusts
// its input and only enforces immutability and tenant isolation.
func (s *PGStore) PutNativePack(ctx context.Context, repoID, packID, casPackHash, commitID string, entries []graphcontract.PackIndexEntry) error {
	tenantID, err := requireTenantID(ctx)
	if err != nil {
		return fmt.Errorf("postgres: put native pack for repo %s: %w", repoID, err)
	}
	if packID == "" || casPackHash == "" {
		return fmt.Errorf("postgres: put native pack for repo %s: pack ID and CAS pack hash are required", repoID)
	}

	var commit any
	if commitID != "" {
		commit = commitID
	}

	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		result, err := tx.Exec(ctx, `
			INSERT INTO native_packs (tenant_id, repo_id, pack_id, cas_pack_hash, commit_id, object_count)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (tenant_id, repo_id, pack_id) DO NOTHING
		`, tenantID, repoID, packID, casPackHash, commit, len(entries))
		if err != nil {
			return fmt.Errorf("postgres: put native pack for repo %s: insert native pack: %w", repoID, err)
		}
		if result.RowsAffected() == 0 {
			var existing struct {
				casPackHash string
				commitID    *string
				objectCount int
			}
			if err := tx.QueryRow(ctx, `
				SELECT cas_pack_hash, commit_id, object_count
				FROM native_packs
				WHERE tenant_id = $1 AND repo_id = $2 AND pack_id = $3
				FOR KEY SHARE
			`, tenantID, repoID, packID).Scan(&existing.casPackHash, &existing.commitID, &existing.objectCount); err != nil {
				return fmt.Errorf("postgres: put native pack for repo %s: read existing native pack: %w", repoID, err)
			}
			existingCommit := ""
			if existing.commitID != nil {
				existingCommit = *existing.commitID
			}
			if existing.casPackHash != casPackHash || existingCommit != commitID || existing.objectCount != len(entries) {
				return ErrImmutableMetadataMismatch
			}
		}

		for _, entry := range entries {
			if _, err := tx.Exec(ctx, `
				INSERT INTO native_pack_objects (tenant_id, repo_id, pack_id, object_id, pack_offset, compressed_size, uncompressed_size, crc32)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				ON CONFLICT (tenant_id, repo_id, object_id) DO NOTHING
			`, tenantID, repoID, packID, string(entry.Object), int64(entry.Offset), int64(entry.CompressedSize), int64(entry.UncompressedSize), int64(entry.CRC32)); err != nil {
				return fmt.Errorf("postgres: put native pack for repo %s: insert native pack object %s: %w", repoID, entry.Object, err)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// GetNativeObjectLocation resolves a verified native pack object's storage
// location scoped to the tenant carried by ctx and repoID. It returns
// ErrNativeObjectNotFound when no row is visible — which RLS and the repo
// scope make indistinguishable between an absent object and one that exists
// only for a different tenant or repository.
func (s *PGStore) GetNativeObjectLocation(ctx context.Context, repoID, objectID string) (NativeObjectLocation, error) {
	if _, err := requireTenantID(ctx); err != nil {
		return NativeObjectLocation{}, fmt.Errorf("postgres: get native object location: %w", err)
	}

	var loc NativeObjectLocation
	var offset, compressedSize, uncompressedSize, crc32Value int64
	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			SELECT o.pack_id, p.cas_pack_hash, o.pack_offset, o.compressed_size, o.uncompressed_size, o.crc32
			FROM native_pack_objects o
			JOIN native_packs p
				ON p.tenant_id = o.tenant_id AND p.repo_id = o.repo_id AND p.pack_id = o.pack_id
			WHERE o.repo_id = $1 AND o.object_id = $2
		`, repoID, objectID).Scan(&loc.PackID, &loc.CASPackHash, &offset, &compressedSize, &uncompressedSize, &crc32Value)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNativeObjectNotFound
			}
			return fmt.Errorf("postgres: get native object location: query object %s: %w", objectID, err)
		}
		return nil
	}); err != nil {
		return NativeObjectLocation{}, err
	}
	loc.Offset = uint64(offset)
	loc.CompressedSize = uint64(compressedSize)
	loc.UncompressedSize = uint64(uncompressedSize)
	loc.CRC32 = uint32(crc32Value)
	return loc, nil
}

func (s *PGStore) withTenantTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error {
	tenantID, _ := tenantIDFromContext(ctx)
	return s.withConfiguredTenantTx(ctx, tenantID, fn)
}

func validObjectFormat(format uint32) bool {
	return format == 0 || format == 1 || format == 2
}

func normalizeObjectFormat(format uint32) uint32 {
	if format == 0 {
		return 1
	}
	return format
}

func (s *PGStore) withConfiguredTenantTx(ctx context.Context, tenantID string, fn func(ctx context.Context, tx pgx.Tx) error) error {
	if s == nil || s.pool == nil {
		return errors.New("postgres: store is not open")
	}
	if ctx == nil {
		return errNilContext
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if tenantID != "" {
		if err := setTenantLocal(ctx, tx, tenantID); err != nil {
			return err
		}
	}
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit tx: %w", err)
	}
	return nil
}

// setTenantLocal projects tenantID into the current transaction's local GUC so
// PostgreSQL Row-Level Security policies can enforce tenant isolation.
//
// It uses set_config(..., true) instead of `SET LOCAL app.current_tenant_id =
// $1` because PostgreSQL does not allow bind parameters in plain SET syntax at
// all. set_config is the standard parameterized equivalent, and its third
// argument being true makes the value transaction-scoped, matching SET LOCAL
// semantics and automatically reverting it at commit or rollback.
func setTenantLocal(ctx context.Context, tx pgx.Tx, tenantID string) error {
	if _, err := tx.Exec(ctx, `SELECT set_config('app.current_tenant_id', $1, true)`, tenantID); err != nil {
		return fmt.Errorf("postgres: set tenant context: %w", err)
	}
	return nil
}

func tenantIDFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}

	tenantID, ok := ctx.Value(tenantIDContextKey).(string)
	if !ok || tenantID == "" {
		return "", false
	}
	return tenantID, true
}

func requireTenantID(ctx context.Context) (string, error) {
	tenantID, ok := tenantIDFromContext(ctx)
	if !ok {
		return "", fmt.Errorf("postgres: tenant context: %w", ErrMissingTenantContext)
	}
	return tenantID, nil
}

func newUUIDv4() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate uuid: %w", err)
	}

	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80

	var formatted [36]byte
	hex.Encode(formatted[0:8], raw[0:4])
	formatted[8] = '-'
	hex.Encode(formatted[9:13], raw[4:6])
	formatted[13] = '-'
	hex.Encode(formatted[14:18], raw[6:8])
	formatted[18] = '-'
	hex.Encode(formatted[19:23], raw[8:10])
	formatted[23] = '-'
	hex.Encode(formatted[24:36], raw[10:16])

	return string(formatted[:]), nil
}

func newOpaqueToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate lease token: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
