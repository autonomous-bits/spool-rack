package sync

import (
	"context"
	"errors"
	"fmt"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
)

// ErrNonFastForward indicates a push was rejected because advancing the remote
// branch would not be a fast-forward update.
var ErrNonFastForward = errors.New("sync: non-fast-forward push rejected")

// BranchStore is the narrow slice of postgres.Store that HandlePush depends on,
// kept separate so unit tests can supply a lightweight fake instead of a real
// PostgreSQL-backed Store.
type BranchStore interface {
	PutCommit(ctx context.Context, repoID, commitID, parentCommitID, snapshotRoot, author, message string) error
	GetBranchRef(ctx context.Context, repoID, branch string) (string, error)
	IsAncestor(ctx context.Context, repoID, ancestorCommit, commit string) (bool, error)
	CompareAndSwapBranchRef(ctx context.Context, repoID, branch, expectedCommit, newCommit string) error
}

var _ BranchStore = (postgres.Store)(nil)

// NonFastForwardError carries branch-head context and caller guidance for
// rejected non-fast-forward pushes.
type NonFastForwardError struct {
	ActualHead string
	Guidance   string
}

func (e *NonFastForwardError) Error() string {
	if e == nil {
		return ErrNonFastForward.Error()
	}
	if e.ActualHead == "" {
		return fmt.Sprintf("%s: %s", ErrNonFastForward, e.Guidance)
	}
	return fmt.Sprintf("%s: actual head %s: %s", ErrNonFastForward, e.ActualHead, e.Guidance)
}

func (e *NonFastForwardError) Unwrap() error {
	return ErrNonFastForward
}

// PushEngine persists incoming packs and advances branch refs once the push is
// proven to be a fast-forward.
type PushEngine struct {
	driver cas.Driver
	store  BranchStore
}

// NewPushEngine constructs a PushEngine backed by CAS and branch metadata
// storage.
func NewPushEngine(driver cas.Driver, store BranchStore) *PushEngine {
	return &PushEngine{
		driver: driver,
		store:  store,
	}
}

// HandlePush persists the immutable pack payload before reading or mutating any
// branch metadata, then registers pushed commit metadata, and only then checks
// and advances the branch ref. This ordering makes it structurally impossible
// for a failed CAS write to be followed by any branch-ref update path.
func (e *PushEngine) HandlePush(ctx context.Context, req PushRequest) error {
	if err := validatePushRequest(req); err != nil {
		return err
	}

	scope, err := cas.NewScope(req.TenantID, req.RepoID)
	if err != nil {
		return fmt.Errorf("sync: push: invalid CAS scope: %w", err)
	}

	if err := e.driver.WritePack(ctx, scope, req.PackHash, req.PackStream); err != nil {
		return fmt.Errorf("sync: push: write pack: %w", err)
	}

	for _, c := range req.Commits {
		if err := e.store.PutCommit(ctx, req.RepoID, c.ID, c.ParentID, c.SnapshotRoot, c.Author, c.Message); err != nil {
			return fmt.Errorf("sync: push: register commit %s: %w", c.ID, err)
		}
	}

	actualHead, err := e.store.GetBranchRef(ctx, req.RepoID, req.Branch)
	if err != nil {
		return fmt.Errorf("sync: push: get branch ref: %w", err)
	}

	if actualHead != req.BaseCommit {
		return e.diagnoseNonFastForward(ctx, req, actualHead)
	}

	if err := e.store.CompareAndSwapBranchRef(ctx, req.RepoID, req.Branch, req.BaseCommit, req.TargetCommit); err != nil {
		if errors.Is(err, postgres.ErrNonFastForward) {
			return &NonFastForwardError{
				ActualHead: actualHead,
				Guidance:   "remote branch changed concurrently; pull the latest changes and retry your push",
			}
		}
		return fmt.Errorf("sync: push: advance branch ref: %w", err)
	}

	return nil
}

func validatePushRequest(req PushRequest) error {
	switch {
	case req.Branch == "":
		return fmt.Errorf("sync: push: branch is required")
	case req.TargetCommit == "":
		return fmt.Errorf("sync: push: target commit is required")
	case req.BaseCommit == "":
		return fmt.Errorf("sync: push: base commit is required")
	case req.PackHash == "":
		return fmt.Errorf("sync: push: pack hash is required")
	default:
		return nil
	}
}

func (e *PushEngine) diagnoseNonFastForward(ctx context.Context, req PushRequest, actualHead string) error {
	isAncestor, err := e.store.IsAncestor(ctx, req.RepoID, req.BaseCommit, actualHead)
	if err != nil {
		return fmt.Errorf("sync: push: check branch ancestry: %w", err)
	}
	if isAncestor {
		return &NonFastForwardError{
			ActualHead: actualHead,
			Guidance:   "remote branch has advanced since your base commit; pull the latest changes and reapply your commit before pushing again",
		}
	}
	return &NonFastForwardError{
		ActualHead: actualHead,
		Guidance:   "base commit not found in the remote branch's history; branches have diverged — pull and manually reconcile before pushing",
	}
}
