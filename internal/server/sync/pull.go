package sync

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	"github.com/klauspost/compress/zstd"
)

// ErrPullDiverged indicates the caller's known commit is not an ancestor of
// the requested remote branch head.
var ErrPullDiverged = errors.New("sync: pull history diverged")

// PullDivergedError supplies the current remote branch head for reconciliation.
type PullDivergedError struct {
	CurrentHead string
}

func (e *PullDivergedError) Error() string {
	if e == nil || e.CurrentHead == "" {
		return ErrPullDiverged.Error()
	}
	return fmt.Sprintf("%s: remote head %s", ErrPullDiverged, e.CurrentHead)
}

func (e *PullDivergedError) Unwrap() error {
	return ErrPullDiverged
}

// ErrUpToDate is a sentinel result allowing HTTP to return 204 cleanly.
var ErrUpToDate = errors.New("sync: already up to date")

// PullEngine negotiates and streams tenant-fenced pack ranges from CAS.
type PullEngine struct {
	driver cas.Driver
	store  BranchStore
}

// PullPlan is the fully negotiated, tenant-fenced set of ranges to stream.
type PullPlan struct {
	Head   string
	Scope  cas.Scope
	Ranges []postgres.PackRange
}

// NewPullEngine constructs a PullEngine backed by CAS and branch metadata.
func NewPullEngine(driver cas.Driver, store BranchStore) *PullEngine {
	return &PullEngine{driver: driver, store: store}
}

// PreparePull verifies the supplied history and returns the selected ranges.
// Callers can use the resulting head before committing HTTP response headers.
func (e *PullEngine) PreparePull(ctx context.Context, req PullRequest) (PullPlan, error) {
	if req.Branch == "" {
		return PullPlan{}, fmt.Errorf("sync: pull: branch is required")
	}

	ctx, err := e.store.SetTenantContext(ctx, req.TenantID)
	if err != nil {
		return PullPlan{}, fmt.Errorf("sync: pull: set tenant context: %w", err)
	}
	head, err := e.store.GetBranchRef(ctx, req.RepoID, req.Branch)
	if err != nil {
		return PullPlan{}, fmt.Errorf("sync: pull: get branch ref: %w", err)
	}
	if req.KnownCommit == head {
		return PullPlan{Head: head}, ErrUpToDate
	}
	if req.KnownCommit != "" {
		isAncestor, err := e.store.IsAncestor(ctx, req.RepoID, req.KnownCommit, head)
		if err != nil {
			return PullPlan{}, fmt.Errorf("sync: pull: check known commit ancestry: %w", err)
		}
		if !isAncestor {
			return PullPlan{Head: head}, &PullDivergedError{CurrentHead: head}
		}
	}

	ranges, err := e.store.GetPackRanges(ctx, req.RepoID, head, req.KnownCommit)
	if err != nil {
		return PullPlan{}, fmt.Errorf("sync: pull: get pack ranges: %w", err)
	}
	if err := validatePackChain(ranges, head, req.KnownCommit); err != nil {
		return PullPlan{}, err
	}

	scope, err := cas.NewScope(req.TenantID, req.RepoID)
	if err != nil {
		return PullPlan{}, fmt.Errorf("sync: pull: invalid CAS scope: %w", err)
	}
	return PullPlan{Head: head, Scope: scope, Ranges: ranges}, nil
}

// HandlePull verifies the supplied history, then streams selected ranges
// oldest-to-newest into one zstd stream without buffering pack payloads.
func (e *PullEngine) HandlePull(ctx context.Context, req PullRequest, w io.Writer) (string, error) {
	if w == nil {
		return "", fmt.Errorf("sync: pull: response writer is required")
	}
	plan, err := e.PreparePull(ctx, req)
	if err != nil {
		return plan.Head, err
	}
	if err := e.StreamPull(ctx, plan, w); err != nil {
		return "", err
	}
	return plan.Head, nil
}

// StreamPull writes a prepared pull plan as one zstd HTTP payload.
func (e *PullEngine) StreamPull(ctx context.Context, plan PullPlan, w io.Writer) error {
	encoder, err := zstd.NewWriter(w)
	if err != nil {
		return fmt.Errorf("sync: pull: create zstd encoder: %w", err)
	}
	for i := len(plan.Ranges) - 1; i >= 0; i-- {
		pack, err := e.driver.OpenPack(ctx, plan.Scope, plan.Ranges[i].PackHash)
		if err != nil {
			_ = encoder.Close()
			return fmt.Errorf("sync: pull: open pack %s: %w", plan.Ranges[i].PackHash, err)
		}
		_, copyErr := io.Copy(encoder, pack)
		closeErr := pack.Close()
		if copyErr != nil {
			_ = encoder.Close()
			return fmt.Errorf("sync: pull: stream pack %s: %w", plan.Ranges[i].PackHash, copyErr)
		}
		if closeErr != nil {
			_ = encoder.Close()
			return fmt.Errorf("sync: pull: close pack %s: %w", plan.Ranges[i].PackHash, closeErr)
		}
	}
	if err := encoder.Close(); err != nil {
		return fmt.Errorf("sync: pull: finalize zstd stream: %w", err)
	}
	return nil
}

// ValidatePullPacks confirms every selected CAS pack is readable before an
// HTTP handler commits a successful response header.
func (e *PullEngine) ValidatePullPacks(ctx context.Context, plan PullPlan) error {
	for _, packRange := range plan.Ranges {
		pack, err := e.driver.OpenPack(ctx, plan.Scope, packRange.PackHash)
		if err != nil {
			return fmt.Errorf("sync: pull: open pack %s: %w", packRange.PackHash, err)
		}
		if err := pack.Close(); err != nil {
			return fmt.Errorf("sync: pull: close pack %s: %w", packRange.PackHash, err)
		}
	}
	return nil
}

func validatePackChain(ranges []postgres.PackRange, head, knownCommit string) error {
	if len(ranges) == 0 {
		return fmt.Errorf("sync: pull: no pack range found for branch head %s", head)
	}
	expectedTarget := head
	for _, pack := range ranges {
		if pack.TargetCommitID != expectedTarget {
			return fmt.Errorf("sync: pull: incomplete pack range chain at commit %s", expectedTarget)
		}
		expectedTarget = pack.BaseCommitID
	}
	if expectedTarget != knownCommit {
		return fmt.Errorf("sync: pull: pack range chain does not reach requested commit")
	}
	return nil
}
