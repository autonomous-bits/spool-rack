package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"

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
	// ErrMissingTenantContext indicates a caller attempted a tenant-scoped operation
	// without first attaching a validated tenant identifier to the request context.
	ErrMissingTenantContext = errors.New("postgres: no tenant context set")
	// ErrTenantNameConflict indicates an attempted tenant creation reused an
	// existing globally-unique tenant name.
	ErrTenantNameConflict = errors.New("postgres: tenant name already exists")
	errNilContext         = errors.New("postgres: nil context")

	uuidV4Pattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)

type contextKey int

const tenantIDContextKey contextKey = iota

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
	// PutCommit inserts a content-addressed commit row (commitID is a caller-
	// supplied BLAKE3 hex hash, not server-generated) scoped to the tenant
	// carried by ctx. It is idempotent: re-registering an already-known
	// commitID (e.g. a retried push) is a no-op rather than an error.
	PutCommit(ctx context.Context, repoID, commitID, parentCommitID, snapshotRoot, author, message string) error
	// PutPackRange registers the immutable CAS pack created by a successful
	// push and the contiguous commit range that it contains.
	PutPackRange(ctx context.Context, repoID, packHash, baseCommitID, targetCommitID string) error
	// GetBranchRef resolves the commit identifier for a named branch.
	GetBranchRef(ctx context.Context, repoID, branch string) (string, error)
	// IsAncestor reports whether ancestorCommit is reachable by walking the
	// parent_commit_id chain starting at commit (inclusive — a commit is
	// considered its own ancestor), scoped to repoID within the tenant
	// carried by ctx. Used to verify fast-forward reachability during push,
	// per req-ancestry-verified-remote-fast-forward. If commit does not exist
	// in the repository, IsAncestor returns false, nil.
	IsAncestor(ctx context.Context, repoID, ancestorCommit, commit string) (bool, error)
	// GetPackRanges returns the pack ranges required to reconstruct the
	// history from headCommitID back to knownCommitID. Ranges are returned
	// newest-first so callers can validate the chain and stream it in reverse.
	GetPackRanges(ctx context.Context, repoID, headCommitID, knownCommitID string) ([]PackRange, error)
	// CompareAndSwapBranchRef updates a branch ref only if current matches expected.
	CompareAndSwapBranchRef(ctx context.Context, repoID, branch, expectedCommit, newCommit string) error
}

// PackRange identifies one immutable pack and the commit interval it contains.
type PackRange struct {
	PackHash       string
	BaseCommitID   string
	TargetCommitID string
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
func (s *PGStore) PutCommit(ctx context.Context, repoID, commitID, parentCommitID, snapshotRoot, author, message string) error {
	tenantID, err := requireTenantID(ctx)
	if err != nil {
		return fmt.Errorf("postgres: put commit for repo %s: %w", repoID, err)
	}

	var parent any
	if parentCommitID != "" {
		parent = parentCommitID
	}

	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO commits (id, tenant_id, repo_id, parent_commit_id, snapshot_root, author, message)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (tenant_id, repo_id, id) DO NOTHING
		`, commitID, tenantID, repoID, parent, snapshotRoot, author, message); err != nil {
			return fmt.Errorf("postgres: put commit for repo %s: insert commit: %w", repoID, err)
		}
		// MVP note: if the same commitID is re-registered with different metadata,
		// PostgreSQL keeps the first row and ignores the later insert.
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// PutPackRange records the commit range carried by an immutable CAS pack.
func (s *PGStore) PutPackRange(ctx context.Context, repoID, packHash, baseCommitID, targetCommitID string) error {
	tenantID, err := requireTenantID(ctx)
	if err != nil {
		return fmt.Errorf("postgres: put pack range for repo %s: %w", repoID, err)
	}

	var base any
	if baseCommitID != "" {
		base = baseCommitID
	}

	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO pack_ranges (tenant_id, repo_id, pack_hash, base_commit_id, target_commit_id)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (tenant_id, repo_id, pack_hash) DO NOTHING
		`, tenantID, repoID, packHash, base, targetCommitID); err != nil {
			return fmt.Errorf("postgres: put pack range for repo %s: insert pack range: %w", repoID, err)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
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

// IsAncestor reports whether ancestorCommit is reachable from commit by walking
// the parent_commit_id chain within repoID for the tenant carried by ctx. A
// commit is considered its own ancestor. If commit does not exist in the
// repository, IsAncestor returns false, nil.
func (s *PGStore) IsAncestor(ctx context.Context, repoID, ancestorCommit, commit string) (bool, error) {
	if _, err := requireTenantID(ctx); err != nil {
		return false, fmt.Errorf("postgres: is ancestor: %w", err)
	}

	var found bool
	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `
			WITH RECURSIVE ancestry AS (
				SELECT id, parent_commit_id
				FROM commits
				WHERE repo_id = $1 AND id = $2
				UNION ALL
				SELECT c.id, c.parent_commit_id
				FROM commits c
				JOIN ancestry a ON c.id = a.parent_commit_id
				WHERE c.repo_id = $1
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

// GetPackRanges traverses packs from a branch head toward a known ancestor.
func (s *PGStore) GetPackRanges(ctx context.Context, repoID, headCommitID, knownCommitID string) ([]PackRange, error) {
	if _, err := requireTenantID(ctx); err != nil {
		return nil, fmt.Errorf("postgres: get pack ranges: %w", err)
	}

	ranges := make([]PackRange, 0)
	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			WITH RECURSIVE pack_chain AS (
				SELECT pack_hash, base_commit_id, target_commit_id, 0 AS depth
				FROM pack_ranges
				WHERE repo_id = $1 AND target_commit_id = $2
				UNION ALL
				SELECT p.pack_hash, p.base_commit_id, p.target_commit_id, c.depth + 1
				FROM pack_ranges p
				JOIN pack_chain c ON p.target_commit_id = c.base_commit_id
				WHERE p.repo_id = $1 AND c.base_commit_id IS NOT NULL AND c.base_commit_id <> $3
			)
			SELECT pack_hash, COALESCE(base_commit_id, ''), target_commit_id
			FROM pack_chain
			ORDER BY depth ASC
		`, repoID, headCommitID, knownCommitID)
		if err != nil {
			return fmt.Errorf("postgres: get pack ranges: query pack chain: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var pack PackRange
			if err := rows.Scan(&pack.PackHash, &pack.BaseCommitID, &pack.TargetCommitID); err != nil {
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
// expectedCommit, returning ErrBranchNotFound or ErrNonFastForward when the
// update cannot be applied.
func (s *PGStore) CompareAndSwapBranchRef(ctx context.Context, repoID, branch, expectedCommit, newCommit string) error {
	if _, err := requireTenantID(ctx); err != nil {
		return fmt.Errorf("postgres: cas branch ref: %w", err)
	}

	if err := s.withTenantTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
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

		var actualHead string
		if err := tx.QueryRow(ctx, `SELECT head_commit_id FROM branches WHERE repo_id = $1 AND name = $2`, repoID, branch).Scan(&actualHead); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("postgres: cas branch ref: %w", ErrBranchNotFound)
			}
			return fmt.Errorf("postgres: cas branch ref: query branch ref: %w", err)
		}
		return fmt.Errorf("postgres: cas branch ref: %w", ErrNonFastForward)
	}); err != nil {
		return err
	}

	return nil
}

func (s *PGStore) withTenantTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error {
	tenantID, _ := tenantIDFromContext(ctx)
	return s.withConfiguredTenantTx(ctx, tenantID, fn)
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

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
