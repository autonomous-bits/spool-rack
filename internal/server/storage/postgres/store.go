package postgres

import (
	"context"
	"errors"
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
)

// Store defines the metadata store operations for multi-tenant repositories.
type Store interface {
	// SetTenantContext sets the PostgreSQL session tenant context for Row-Level Security.
	SetTenantContext(ctx context.Context, tenantID string) (context.Context, error)
	// GetBranchRef resolves the commit hash for a named branch.
	GetBranchRef(ctx context.Context, repoID, branch string) (string, error)
	// CompareAndSwapBranchRef updates a branch ref only if current matches expected.
	CompareAndSwapBranchRef(ctx context.Context, repoID, branch, expectedCommit, newCommit string) error
}
