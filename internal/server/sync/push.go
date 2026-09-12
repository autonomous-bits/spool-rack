package sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	"github.com/autonomous-bits/spool/graphcontract"
	"github.com/fxamacker/cbor/v2"
)

// ErrNonFastForward indicates a push was rejected because advancing the remote
// branch would not be a fast-forward update.
var ErrNonFastForward = errors.New("sync: non-fast-forward push rejected")

// ErrMissingAsset indicates a commit snapshot references an asset blob not found in CAS.
var ErrMissingAsset = errors.New("sync: push: referenced asset blob missing from storage")

// ErrUnsupportedContractVersion indicates a client declared a graphcontract
// pack format version outside the range this server currently accepts. It is
// distinct from ErrInvalidFrame (malformed pack bytes) so callers can return
// a specific, actionable error instead of a generic bad-request.
var ErrUnsupportedContractVersion = errors.New("sync: unsupported contract version")

// BranchStore is the narrow slice of postgres.Store that HandlePush depends on,
// kept separate so unit tests can supply a lightweight fake instead of a real
// PostgreSQL-backed Store.
type BranchStore interface {
	SetTenantContext(ctx context.Context, tenantID string) (context.Context, error)
	PutCommit(ctx context.Context, repoID string, commitID graphcontract.ObjectID, commit graphcontract.Commit) error
	PutPackRange(ctx context.Context, repoID, packHash, baseCommitID, targetCommitID string) error
	GetCommitMetadata(ctx context.Context, repoID, commitID string) (postgres.CommitMetadata, error)
	GetBranchRef(ctx context.Context, repoID, branch string) (string, error)
	IsAncestor(ctx context.Context, repoID, ancestorCommit, commit string) (bool, error)
	GetPackRanges(ctx context.Context, repoID, headCommitID, knownCommitID string) ([]postgres.PackRange, error)
	CompareAndSwapBranchRef(ctx context.Context, repoID, branch, expectedCommit, newCommit string) error
}

var _ BranchStore = (postgres.Store)(nil)

type formattedCommitStore interface {
	PutCommitWithFormat(context.Context, string, graphcontract.ObjectID, graphcontract.Commit, uint32) error
}

type formattedPackStore interface {
	PutPackRangeWithFormat(context.Context, string, string, string, string, uint32) error
}

type branchCreator interface {
	CreateBranch(context.Context, string, string, string) error
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
	// minPackFormatVersion is the lowest client-declared PackFormat this
	// engine accepts. It defaults to PackFormatV2 (today's only implemented
	// wire format), preserving existing strict behavior unless a caller
	// explicitly widens it via SetMinPackFormatVersion for a rolling deploy
	// that must keep serving the immediately-prior compatible client version
	// (see spec-cli-rack-contract-and-metadata-migrations).
	minPackFormatVersion uint32
}

// NewPushEngine constructs a PushEngine backed by CAS and branch metadata
// storage.
func NewPushEngine(driver cas.Driver, store BranchStore, validators ...SnapshotValidator) *PushEngine {
	var snapshotValidator SnapshotValidator
	if len(validators) != 0 {
		snapshotValidator = validators[0]
	}
	return &PushEngine{driver: driver, store: store, snapshotValidator: snapshotValidator, minPackFormatVersion: PackFormatV2}
}

// SetMinPackFormatVersion widens (or restores) the lowest client-declared
// PackFormat this engine accepts. Passing 0 or a value above PackFormatV2 is
// ignored, keeping the current setting.
func (e *PushEngine) SetMinPackFormatVersion(min uint32) {
	if min == 0 || min > PackFormatV2 {
		return
	}
	e.minPackFormatVersion = min
}

// HandlePush persists the immutable pack payload before reading or mutating any
// branch metadata, then registers pushed commit metadata, and only then checks
// and advances the branch ref. This ordering makes it structurally impossible
// for a failed CAS write to be followed by any branch-ref update path.
func (e *PushEngine) HandlePush(ctx context.Context, req PushRequest) error {
	if err := validatePushRequest(req, e.minPackFormatVersion); err != nil {
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

	base, err := e.writePack(ctx, scope, req)
	if err != nil {
		return fmt.Errorf("sync: push: write pack: %w", err)
	}
	if base != nil && req.BaseCommit != "" {
		if err := e.validateV2BaseFormat(ctx, req.RepoID, *base); err != nil {
			return err
		}
	}

	packFormat := normalizePackFormat(req.PackFormat)
	if packFormat == PackFormatV3 {
		known := make(map[string]struct{}, len(req.Commits)+1)
		if req.BaseCommit != "" {
			known[req.BaseCommit] = struct{}{}
		}
		for _, c := range req.Commits {
			for _, parent := range c.Commit.Parents {
				parentID := string(parent)
				if _, ok := known[parentID]; ok {
					continue
				}
				if _, err := e.store.GetCommitMetadata(ctx, req.RepoID, parentID); err != nil {
					if errors.Is(err, postgres.ErrCommitNotFound) {
						return fmt.Errorf("%w: commit %s references unknown parent %s", ErrInvalidFrame, c.ID, parentID)
					}
					return fmt.Errorf("sync: push: resolve parent %s: %w", parentID, err)
				}
				known[parentID] = struct{}{}
			}
			known[string(c.ID)] = struct{}{}
		}
	}

	for _, c := range req.Commits {
		if err := validateCommitRecord(c); err != nil {
			return err
		}
		if formatted, ok := e.store.(formattedCommitStore); ok {
			if err := formatted.PutCommitWithFormat(ctx, req.RepoID, c.ID, c.Commit, CommitFormatV2); err != nil {
				return fmt.Errorf("sync: push: register commit %s: %w", c.ID, err)
			}
			continue
		}
		if err := e.store.PutCommit(ctx, req.RepoID, c.ID, c.Commit); err != nil {
			return fmt.Errorf("sync: push: register commit %s: %w", c.ID, err)
		}
	}
	if formatted, ok := e.store.(formattedPackStore); ok {
		if err := formatted.PutPackRangeWithFormat(ctx, req.RepoID, req.PackHash, req.BaseCommit, req.TargetCommit, packFormat); err != nil {
			return fmt.Errorf("sync: push: register pack range: %w", err)
		}
	} else if err := e.store.PutPackRange(ctx, req.RepoID, req.PackHash, req.BaseCommit, req.TargetCommit); err != nil {
		return fmt.Errorf("sync: push: register pack range: %w", err)
	}

	actualHead, err := e.store.GetBranchRef(ctx, req.RepoID, req.Branch)
	if err != nil {
		if errors.Is(err, postgres.ErrBranchNotFound) && req.BaseCommit == "" {
			if creator, ok := e.store.(branchCreator); ok {
				if err := creator.CreateBranch(ctx, req.RepoID, req.Branch, req.TargetCommit); err != nil {
					return fmt.Errorf("sync: push: create branch: %w", err)
				}
				return nil
			}
			return fmt.Errorf("sync: push: branch creation not supported by store")
		}
		return fmt.Errorf("sync: push: get branch ref: %w", err)
	}

	expectedCommit := req.BaseCommit
	if actualHead != req.BaseCommit {
		if req.BaseCommit == "" {
			if actualHead == req.TargetCommit {
				return nil
			}
			isAncestor, err := e.store.IsAncestor(ctx, req.RepoID, actualHead, req.TargetCommit)
			if err == nil && isAncestor {
				expectedCommit = actualHead
			} else {
				return e.diagnoseNonFastForward(ctx, req, actualHead)
			}
		} else {
			return e.diagnoseNonFastForward(ctx, req, actualHead)
		}
	}

	if err := e.store.CompareAndSwapBranchRef(ctx, req.RepoID, req.Branch, expectedCommit, req.TargetCommit); err != nil {
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

func validatePushRequest(req PushRequest, minPackFormatVersion uint32) error {
	packFormat := normalizePackFormat(req.PackFormat)
	switch {
	case req.Branch == "":
		return fmt.Errorf("sync: push: branch is required")
	case req.TargetCommit == "":
		return fmt.Errorf("sync: push: target commit is required")
	case req.PackHash == "":
		return fmt.Errorf("sync: push: pack hash is required")
	case packFormat < minPackFormatVersion || packFormat > PackFormatV3:
		return fmt.Errorf("%w: pack format %d is outside the versions this server currently accepts [%d,%d]", ErrUnsupportedContractVersion, packFormat, minPackFormatVersion, PackFormatV3)
	}
	for _, commit := range req.Commits {
		if err := validateCommitRecord(commit); err != nil {
			return err
		}
	}
	return nil
}

func (e *PushEngine) writePack(ctx context.Context, scope cas.Scope, req PushRequest) (*CommitIdentity, error) {
	if req.PackStream == nil {
		return nil, fmt.Errorf("pack stream is required")
	}
	if e.snapshotValidator == nil {
		return nil, fmt.Errorf("%w: snapshot validator is required", ErrInvalidFrame)
	}
	data, err := io.ReadAll(io.LimitReader(req.PackStream, MaxV2PackBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read pack: %w", err)
	}
	if int64(len(data)) > MaxV2PackBytes {
		return nil, fmt.Errorf("pack exceeds %d byte limit", MaxV2PackBytes)
	}
	if ContentID(data) != req.PackHash {
		return nil, fmt.Errorf("pack content ID does not match pack hash")
	}

	var (
		base    CommitIdentity
		target  CommitIdentity
		commits []CommitFrameV2
		objects []PackObjectV2
	)

	packFormat := normalizePackFormat(req.PackFormat)
	if packFormat == PackFormatV3 {
		frame, err := UnmarshalPackFrameV3(data)
		if err != nil {
			return nil, err
		}
		base = frame.Base
		target = frame.Target
		commits = frame.Commits
		objects = frame.Objects
	} else {
		frame, err := UnmarshalPackFrameV2(data)
		if err != nil {
			return nil, err
		}
		base = frame.Base
		target = frame.Target
		commits = frame.Commits
		objects = frame.Objects
	}

	if base.ID != req.BaseCommit || target.ID != req.TargetCommit {
		return nil, fmt.Errorf("%w: pack frame base/target does not match push metadata", ErrInvalidFrame)
	}
	if err := validateV2CommitMetadata(req.Commits, commits); err != nil {
		return nil, err
	}
	for _, object := range objects {
		if err := e.driver.Put(ctx, scope, object.ID, object.Data); err != nil {
			return nil, fmt.Errorf("write object %s: %w", object.ID, err)
		}
	}
	for _, commit := range commits {
		snapshot, err := e.driver.Get(ctx, scope, commit.SnapshotRoot)
		if err != nil {
			return nil, fmt.Errorf("read snapshot %s: %w", commit.SnapshotRoot, err)
		}
		if ContentID(snapshot) != commit.SnapshotRoot {
			return nil, fmt.Errorf("%w: snapshot %s has an invalid content ID", ErrInvalidFrame, commit.SnapshotRoot)
		}
		if err := e.snapshotValidator(snapshot); err != nil {
			return nil, fmt.Errorf("%w: snapshot %s: %v", ErrInvalidFrame, commit.SnapshotRoot, err)
		}
		if err := e.verifySnapshotAssets(ctx, scope, snapshot); err != nil {
			return nil, err
		}
	}
	if err := e.driver.WritePack(ctx, scope, req.PackHash, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	return &base, nil
}

type snapshotAssetScan struct {
	Nodes map[string]graphcontract.Node `cbor:"3,keyasint"`
}

func (e *PushEngine) verifySnapshotAssets(ctx context.Context, scope cas.Scope, snapshotData []byte) error {
	var scan snapshotAssetScan
	if err := cbor.Unmarshal(snapshotData, &scan); err != nil {
		// Non-CBOR or legacy/test data without valid node envelope
		return nil
	}
	if len(scan.Nodes) == 0 {
		return nil
	}

	for _, node := range scan.Nodes {
		prop, ok := node.Properties["assetUri"]
		if !ok || prop.Kind != graphcontract.PropertyString {
			continue
		}
		uri := strings.TrimSpace(prop.String)
		hash := strings.TrimPrefix(uri, "spool://assets/")
		hash = strings.ToLower(strings.TrimSpace(hash))
		if hash == "" {
			continue
		}

		exists, err := e.driver.AssetExists(ctx, scope, hash)
		if err != nil {
			return fmt.Errorf("sync: push: check asset %s: %w", hash, err)
		}
		if !exists {
			return fmt.Errorf("%w: node %s references missing asset blob %s", ErrMissingAsset, node.ID, hash)
		}
	}
	return nil
}

func (e *PushEngine) validateV2BaseFormat(ctx context.Context, repoID string, base CommitIdentity) error {
	if base.Format != CommitFormatV2 {
		return fmt.Errorf("%w: native pack base must use v2 framing", ErrInvalidFrame)
	}
	metadata, err := e.store.GetCommitMetadata(ctx, repoID, base.ID)
	if err != nil {
		return fmt.Errorf("sync: push: resolve pack base %s: %w", base.ID, err)
	}
	if metadata.Format != CommitFormatV2 {
		return fmt.Errorf("%w: pack base %s has format %d", ErrInvalidFrame, base.ID, metadata.Format)
	}
	return nil
}

func validateCommitRecord(record CommitRecord) error {
	identity, err := CommitObjectID(record.Commit)
	if err != nil {
		return fmt.Errorf("%w: commit record: %v", ErrInvalidFrame, err)
	}
	if record.ID != identity {
		return fmt.Errorf("%w: commit record identity does not match ID", ErrInvalidFrame)
	}
	return nil
}

func validateV2CommitMetadata(records []CommitRecord, frames []CommitFrameV2) error {
	if len(records) != len(frames) {
		return fmt.Errorf("%w: v2 pack and metadata must contain the same commits", ErrInvalidFrame)
	}
	for i, frame := range frames {
		commit, err := frame.Commit()
		if err != nil {
			return err
		}
		identity, err := CommitObjectID(commit)
		if err != nil {
			return err
		}
		record := records[i]
		if record.ID != identity || !record.Commit.Equal(commit) {
			return fmt.Errorf("%w: commit metadata %d does not match its v2 frame", ErrInvalidFrame, i)
		}
	}
	return nil
}

func normalizePackFormat(format uint32) uint32 {
	if format == 0 {
		return PackFormatV2
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
