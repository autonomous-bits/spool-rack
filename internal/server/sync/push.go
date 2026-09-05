package sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

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
	SetTenantContext(ctx context.Context, tenantID string) (context.Context, error)
	PutCommit(ctx context.Context, repoID, commitID, parentCommitID, snapshotRoot, author, message string) error
	PutPackRange(ctx context.Context, repoID, packHash, baseCommitID, targetCommitID string) error
	GetCommitMetadata(ctx context.Context, repoID, commitID string) (postgres.CommitMetadata, error)
	GetBranchRef(ctx context.Context, repoID, branch string) (string, error)
	IsAncestor(ctx context.Context, repoID, ancestorCommit, commit string) (bool, error)
	GetPackRanges(ctx context.Context, repoID, headCommitID, knownCommitID string) ([]postgres.PackRange, error)
	CompareAndSwapBranchRef(ctx context.Context, repoID, branch, expectedCommit, newCommit string) error
}

var _ BranchStore = (postgres.Store)(nil)

type formattedCommitStore interface {
	PutCommitWithFormat(context.Context, string, string, string, string, string, string, uint32) error
}

type formattedPackStore interface {
	PutPackRangeWithFormat(context.Context, string, string, string, string, uint32) error
}

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
	driver            cas.Driver
	store             BranchStore
	snapshotValidator SnapshotValidator
}

// NewPushEngine constructs a PushEngine backed by CAS and branch metadata
// storage.
func NewPushEngine(driver cas.Driver, store BranchStore, validators ...SnapshotValidator) *PushEngine {
	var snapshotValidator SnapshotValidator
	if len(validators) != 0 {
		snapshotValidator = validators[0]
	}
	return &PushEngine{driver: driver, store: store, snapshotValidator: snapshotValidator}
}

// HandlePush persists the immutable pack payload before reading or mutating any
// branch metadata, then registers pushed commit metadata, and only then checks
// and advances the branch ref. This ordering makes it structurally impossible
// for a failed CAS write to be followed by any branch-ref update path.
func (e *PushEngine) HandlePush(ctx context.Context, req PushRequest) error {
	if err := validatePushRequest(req); err != nil {
		return err
	}
	ctx, err := e.store.SetTenantContext(ctx, req.TenantID)
	if err != nil {
		return fmt.Errorf("sync: push: set tenant context: %w", err)
	}

	scope, err := cas.NewScope(req.TenantID, req.RepoID)
	if err != nil {
		return fmt.Errorf("sync: push: invalid CAS scope: %w", err)
	}

	frame, err := e.writePack(ctx, scope, req)
	if err != nil {
		return fmt.Errorf("sync: push: write pack: %w", err)
	}
	if frame != nil {
		if err := e.validateV2BaseFormat(ctx, req.RepoID, *frame); err != nil {
			return err
		}
	}

	for _, c := range req.Commits {
		if err := validateCommitRecord(c); err != nil {
			return err
		}
		if formatted, ok := e.store.(formattedCommitStore); ok && c.Identity != nil {
			if err := formatted.PutCommitWithFormat(ctx, req.RepoID, c.ID, c.ParentID, c.SnapshotRoot, c.Author, c.Message, c.Identity.Format); err != nil {
				return fmt.Errorf("sync: push: register commit %s: %w", c.ID, err)
			}
			continue
		}
		if err := e.store.PutCommit(ctx, req.RepoID, c.ID, c.ParentID, c.SnapshotRoot, c.Author, c.Message); err != nil {
			return fmt.Errorf("sync: push: register commit %s: %w", c.ID, err)
		}
	}
	if formatted, ok := e.store.(formattedPackStore); ok && normalizePackFormat(req.PackFormat) == PackFormatV2 {
		if err := formatted.PutPackRangeWithFormat(ctx, req.RepoID, req.PackHash, req.BaseCommit, req.TargetCommit, PackFormatV2); err != nil {
			return fmt.Errorf("sync: push: register pack range: %w", err)
		}
	} else if err := e.store.PutPackRange(ctx, req.RepoID, req.PackHash, req.BaseCommit, req.TargetCommit); err != nil {
		return fmt.Errorf("sync: push: register pack range: %w", err)
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
	packFormat := normalizePackFormat(req.PackFormat)
	switch {
	case req.Branch == "":
		return fmt.Errorf("sync: push: branch is required")
	case req.TargetCommit == "":
		return fmt.Errorf("sync: push: target commit is required")
	case req.BaseCommit == "":
		return fmt.Errorf("sync: push: base commit is required")
	case req.PackHash == "":
		return fmt.Errorf("sync: push: pack hash is required")
	case packFormat != PackFormatLegacy && packFormat != PackFormatV2:
		return fmt.Errorf("sync: push: unsupported pack format %d", req.PackFormat)
	}
	for _, commit := range req.Commits {
		if err := validateCommitRecord(commit); err != nil {
			return err
		}
		if packFormat == PackFormatLegacy && commit.Identity != nil && commit.Identity.Format == CommitFormatV2 {
			return fmt.Errorf("%w: v2 commits require a v2 pack frame", ErrInvalidFrame)
		}
	}
	return nil
}

func (e *PushEngine) writePack(ctx context.Context, scope cas.Scope, req PushRequest) (*PackFrameV2, error) {
	if normalizePackFormat(req.PackFormat) != PackFormatV2 {
		return nil, e.driver.WritePack(ctx, scope, req.PackHash, req.PackStream)
	}
	if req.PackStream == nil {
		return nil, fmt.Errorf("v2 pack stream is required")
	}
	if e.snapshotValidator == nil {
		return nil, fmt.Errorf("%w: v2 snapshot validator is required", ErrInvalidFrame)
	}
	data, err := io.ReadAll(io.LimitReader(req.PackStream, MaxV2PackBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read v2 pack: %w", err)
	}
	if int64(len(data)) > MaxV2PackBytes {
		return nil, fmt.Errorf("v2 pack exceeds %d byte limit", MaxV2PackBytes)
	}
	if ContentID(data) != req.PackHash {
		return nil, fmt.Errorf("v2 pack content ID does not match pack hash")
	}
	frame, err := UnmarshalPackFrameV2(data)
	if err != nil {
		return nil, err
	}
	if frame.Base.ID != req.BaseCommit || frame.Target.ID != req.TargetCommit {
		return nil, fmt.Errorf("%w: pack frame base/target does not match push metadata", ErrInvalidFrame)
	}
	if len(frame.SupplementalCommits) != 0 {
		return nil, fmt.Errorf("%w: client v2 pushes cannot include supplemental merge commits", ErrInvalidFrame)
	}
	if err := validateV2CommitMetadata(req.Commits, frame.Commits); err != nil {
		return nil, err
	}
	for _, object := range frame.Objects {
		if err := e.driver.Put(ctx, scope, object.ID, object.Data); err != nil {
			return nil, fmt.Errorf("write v2 object %s: %w", object.ID, err)
		}
	}
	for _, commit := range frame.Commits {
		snapshot, err := e.driver.Get(ctx, scope, commit.SnapshotRoot)
		if err != nil {
			return nil, fmt.Errorf("read v2 snapshot %s: %w", commit.SnapshotRoot, err)
		}
		if ContentID(snapshot) != commit.SnapshotRoot {
			return nil, fmt.Errorf("%w: v2 snapshot %s has an invalid content ID", ErrInvalidFrame, commit.SnapshotRoot)
		}
		if err := e.snapshotValidator(snapshot); err != nil {
			return nil, fmt.Errorf("%w: v2 snapshot %s: %v", ErrInvalidFrame, commit.SnapshotRoot, err)
		}
	}
	if err := e.driver.WritePack(ctx, scope, req.PackHash, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	return &frame, nil
}

func (e *PushEngine) validateV2BaseFormat(ctx context.Context, repoID string, frame PackFrameV2) error {
	metadata, err := e.store.GetCommitMetadata(ctx, repoID, frame.Base.ID)
	if err != nil {
		return fmt.Errorf("sync: push: resolve v2 pack base %s: %w", frame.Base.ID, err)
	}
	if metadata.Format != frame.Base.Format {
		return fmt.Errorf("%w: v2 pack base %s has format %d, frame declares %d", ErrInvalidFrame, frame.Base.ID, metadata.Format, frame.Base.Format)
	}
	return nil
}

func validateCommitRecord(record CommitRecord) error {
	if record.Identity == nil {
		return nil
	}
	if record.Identity.ID != record.ID {
		return fmt.Errorf("%w: commit record identity does not match ID", ErrInvalidFrame)
	}
	if err := record.Identity.validate(); err != nil {
		return err
	}
	return nil
}

func validateV2CommitMetadata(records []CommitRecord, frames []CommitFrameV2) error {
	if len(records) != len(frames) {
		return fmt.Errorf("%w: v2 pack and metadata must contain the same commits", ErrInvalidFrame)
	}
	for i, frame := range frames {
		identity, err := frame.Identity()
		if err != nil {
			return err
		}
		record := records[i]
		if record.Identity == nil || record.Identity.Format != CommitFormatV2 || *record.Identity != identity ||
			record.ID != identity.ID || record.SnapshotRoot != frame.SnapshotRoot ||
			record.Author != frame.Author || record.Message != frame.Message {
			return fmt.Errorf("%w: commit metadata %d does not match its v2 frame", ErrInvalidFrame, i)
		}
		expectedParent := ""
		if len(frame.Parents) > 0 {
			expectedParent = frame.Parents[0].ID
		}
		if record.ParentID != expectedParent {
			return fmt.Errorf("%w: commit metadata %d has the wrong first parent", ErrInvalidFrame, i)
		}
		if len(frame.Parents) > 1 {
			return fmt.Errorf("%w: v2 push metadata cannot represent merge commit %d", ErrInvalidFrame, i)
		}
	}
	return nil
}

func normalizePackFormat(format uint32) uint32 {
	if format == 0 {
		return PackFormatLegacy
	}
	return format
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
