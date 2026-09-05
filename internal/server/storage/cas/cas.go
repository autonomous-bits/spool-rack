package cas

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
)

// ErrInvalidScope indicates a Scope's tenant or repository identifier is
// empty, contains unsafe characters, or could otherwise be used to escape
// its partitioned storage path (e.g. directory traversal).
var ErrInvalidScope = errors.New("cas: invalid scope")

// scopeSegmentPattern restricts tenant and repository identifiers to a safe
// allowlist of characters that can never form a path traversal sequence,
// hidden file, or filesystem-reserved name when used as a directory
// component.
var scopeSegmentPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// Scope fences all CAS operations to a single tenant's single repository.
// It can only be constructed via NewScope, which validates both
// identifiers, so a valid Scope value can always be trusted by a Driver
// implementation to derive a safe, isolated storage path.
type Scope struct {
	tenantID string
	repoID   string
}

// NewScope validates tenantID and repoID and returns a Scope fencing
// storage operations to that tenant/repository partition. Both identifiers
// must be non-empty, must not be "." or "..", and may only contain
// alphanumerics, '.', '_', and '-' (never '/' or '\', which would allow
// escaping the partitioned storage root).
func NewScope(tenantID, repoID string) (Scope, error) {
	if err := validateScopeSegment(tenantID); err != nil {
		return Scope{}, fmt.Errorf("%w: tenant ID %q: %v", ErrInvalidScope, tenantID, err)
	}
	if err := validateScopeSegment(repoID); err != nil {
		return Scope{}, fmt.Errorf("%w: repo ID %q: %v", ErrInvalidScope, repoID, err)
	}
	return Scope{tenantID: tenantID, repoID: repoID}, nil
}

// TenantID returns the validated tenant identifier.
func (s Scope) TenantID() string { return s.tenantID }

// RepoID returns the validated repository identifier.
func (s Scope) RepoID() string { return s.repoID }

func validateScopeSegment(seg string) error {
	if seg == "" {
		return errors.New("must not be empty")
	}
	if seg == "." || seg == ".." {
		return errors.New("must not be a relative path segment")
	}
	if !scopeSegmentPattern.MatchString(seg) {
		return errors.New("must match ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
	}
	return nil
}

// Driver represents a pluggable content-addressed storage tier for Spool
// objects and packfiles. Every operation is fenced to a caller-supplied
// Scope, and implementations must guarantee that data written or read under
// one Scope is never visible to, or derivable from, another Scope.
type Driver interface {
	// Put writes an immutable object identified by its BLAKE3 hex hash, scoped to a single tenant/repository.
	Put(ctx context.Context, scope Scope, hash string, data []byte) error
	// Get reads an object by its BLAKE3 hex hash, scoped to a single tenant/repository.
	Get(ctx context.Context, scope Scope, hash string) ([]byte, error)
	// Exists checks if an object exists in storage, scoped to a single tenant/repository.
	Exists(ctx context.Context, scope Scope, hash string) (bool, error)
	// OpenPack streams a packfile by packfile hash, scoped to a single tenant/repository.
	OpenPack(ctx context.Context, scope Scope, packHash string) (io.ReadCloser, error)
	// WritePack persists a validated packfile stream, scoped to a single tenant/repository.
	WritePack(ctx context.Context, scope Scope, packHash string, r io.Reader) error
}
