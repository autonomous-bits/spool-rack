package review

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	serversync "github.com/autonomous-bits/spool-rack/internal/server/sync"
)

var (
	// ErrMigrationUnavailable indicates an incomplete migrator configuration.
	ErrMigrationUnavailable = errors.New("review: snapshot migration dependencies are unavailable")
	// ErrMigrationPreflight indicates legacy data cannot be converted without
	// loss. No metadata cutover occurs after this error.
	ErrMigrationPreflight = errors.New("review: snapshot migration preflight failed")
)

// SnapshotMigrationStore is the tenant-scoped metadata protocol used by
// SnapshotMigrator. PostgreSQL owns the writer freeze, mapping ledger, and
// atomic ref cutover; CAS remains the immutable object tier.
type SnapshotMigrationStore interface {
	SetTenantContext(context.Context, string) (context.Context, error)
	BeginSnapshotMigration(context.Context, string) (postgres.SnapshotMigration, error)
	AbortSnapshotMigration(context.Context, string) error
	ListSnapshotMigrationCommits(context.Context, string) ([]postgres.SnapshotMigrationCommit, error)
	ListSnapshotMigrationPacks(context.Context, string) ([]postgres.SnapshotMigrationPack, error)
	StageSnapshotObjectMapping(context.Context, string, string, string) error
	StageSnapshotCommitMapping(context.Context, string, string, string, string, string) error
	StageSnapshotPackMapping(context.Context, string, string, string, string, string, string, string) error
	CompleteSnapshotMigration(context.Context, string) error
	RollbackSnapshotMigration(context.Context, string) error
}

// SnapshotMigrationResult reports the durable generation and staged artifact
// counts. A returned result with nil error has already cut over all refs.
type SnapshotMigrationResult struct {
	Generation string
	Snapshots  int
	Commits    int
	Packs      int
}

// SnapshotMigrator moves a single tenant/repository from legacy canonical JSON
// snapshots to Rack's canonical CBOR envelope. It intentionally leaves all
// v1 CAS objects, packs, commits, and ledger entries in place for rollback.
type SnapshotMigrator struct {
	objects cas.Driver
	store   SnapshotMigrationStore
}

// NewSnapshotMigrator constructs a migration coordinator.
func NewSnapshotMigrator(objects cas.Driver, store SnapshotMigrationStore) *SnapshotMigrator {
	return &SnapshotMigrator{objects: objects, store: store}
}

// Migrate freezes writers, performs a lossless preflight, writes tenant-scoped
// shadow objects and v2 pack frames, records every mapping, then atomically
// repoints the metadata DAG. Repeating a call after interruption is safe:
// immutable writes and mapping inserts are deterministic and idempotent.
func (m *SnapshotMigrator) Migrate(ctx context.Context, tenantID, repoID string) (SnapshotMigrationResult, error) {
	if m == nil || m.objects == nil || m.store == nil {
		return SnapshotMigrationResult{}, ErrMigrationUnavailable
	}
	if ctx == nil || tenantID == "" || repoID == "" {
		return SnapshotMigrationResult{}, fmt.Errorf("%w: tenant and repository are required", ErrMigrationPreflight)
	}
	tenantCtx, err := m.store.SetTenantContext(ctx, tenantID)
	if err != nil {
		return SnapshotMigrationResult{}, fmt.Errorf("review: set migration tenant context: %w", err)
	}
	migration, err := m.store.BeginSnapshotMigration(tenantCtx, repoID)
	if err != nil {
		return SnapshotMigrationResult{}, fmt.Errorf("review: begin snapshot migration: %w", err)
	}
	scope, err := cas.NewScope(tenantID, repoID)
	if err != nil {
		return SnapshotMigrationResult{}, fmt.Errorf("review: create migration CAS scope: %w", err)
	}
	commits, err := m.store.ListSnapshotMigrationCommits(tenantCtx, repoID)
	if err != nil {
		return SnapshotMigrationResult{}, fmt.Errorf("review: list migration commits: %w", err)
	}
	packs, err := m.store.ListSnapshotMigrationPacks(tenantCtx, repoID)
	if err != nil {
		return SnapshotMigrationResult{}, fmt.Errorf("review: list migration packs: %w", err)
	}

	plan, err := m.preflight(tenantCtx, scope, commits, packs)
	if err != nil {
		if abortErr := m.store.AbortSnapshotMigration(tenantCtx, repoID); abortErr != nil {
			return SnapshotMigrationResult{}, fmt.Errorf("%w; additionally failed to release writer freeze: %v", err, abortErr)
		}
		return SnapshotMigrationResult{}, err
	}
	if err := m.stage(tenantCtx, scope, plan); err != nil {
		return SnapshotMigrationResult{}, err
	}
	if err := m.store.CompleteSnapshotMigration(tenantCtx, repoID); err != nil {
		return SnapshotMigrationResult{}, fmt.Errorf("review: atomically cut over snapshot migration: %w", err)
	}
	return SnapshotMigrationResult{
		Generation: migration.Generation,
		Snapshots:  len(plan.snapshots),
		Commits:    len(plan.commits),
		Packs:      len(plan.packs),
	}, nil
}

// Rollback restores all branch refs to their recorded legacy commit IDs. The
// store rejects rollback once a branch has advanced after cutover, preventing
// an implicit discard of newly reachable data.
func (m *SnapshotMigrator) Rollback(ctx context.Context, tenantID, repoID string) error {
	if m == nil || m.store == nil {
		return ErrMigrationUnavailable
	}
	if ctx == nil || tenantID == "" || repoID == "" {
		return fmt.Errorf("%w: tenant and repository are required", ErrMigrationPreflight)
	}
	tenantCtx, err := m.store.SetTenantContext(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("review: set rollback tenant context: %w", err)
	}
	if err := m.store.RollbackSnapshotMigration(tenantCtx, repoID); err != nil {
		return fmt.Errorf("review: rollback snapshot migration: %w", err)
	}
	return nil
}

type migrationPlan struct {
	snapshots map[string]stagedSnapshot
	commits   map[string]stagedCommit
	packs     []stagedPack
}

type stagedSnapshot struct {
	legacyRoot string
	cborRoot   string
	data       []byte
}

type stagedCommit struct {
	legacy postgres.SnapshotMigrationCommit
	v2ID   string
}

type stagedPack struct {
	legacy postgres.SnapshotMigrationPack
	v2Hash string
	baseID string
	target string
}

func (m *SnapshotMigrator) preflight(ctx context.Context, scope cas.Scope, commits []postgres.SnapshotMigrationCommit, packs []postgres.SnapshotMigrationPack) (migrationPlan, error) {
	plan := migrationPlan{
		snapshots: make(map[string]stagedSnapshot),
		commits:   make(map[string]stagedCommit, len(commits)),
		packs:     make([]stagedPack, 0, len(packs)),
	}
	byID := make(map[string]postgres.SnapshotMigrationCommit, len(commits))
	for _, commit := range commits {
		if commit.ID == "" || commit.SnapshotRoot == "" || commit.Author == "" || commit.Message == "" {
			return migrationPlan{}, fmt.Errorf("%w: commit metadata is incomplete", ErrMigrationPreflight)
		}
		if _, duplicate := byID[commit.ID]; duplicate {
			return migrationPlan{}, fmt.Errorf("%w: duplicate legacy commit %q", ErrMigrationPreflight, commit.ID)
		}
		byID[commit.ID] = commit
		if _, alreadyRead := plan.snapshots[commit.SnapshotRoot]; alreadyRead {
			continue
		}
		data, err := m.objects.Get(ctx, scope, commit.SnapshotRoot)
		if err != nil {
			return migrationPlan{}, fmt.Errorf("%w: load legacy snapshot %s: %v", ErrMigrationPreflight, commit.SnapshotRoot, err)
		}
		if len(bytes.TrimSpace(data)) == 0 || bytes.TrimSpace(data)[0] != '{' {
			return migrationPlan{}, fmt.Errorf("%w: snapshot %s is not a legacy JSON object", ErrMigrationPreflight, commit.SnapshotRoot)
		}
		snapshot, err := DecodeSnapshotJSON(data)
		if err != nil {
			return migrationPlan{}, fmt.Errorf("%w: decode snapshot %s: %v", ErrMigrationPreflight, commit.SnapshotRoot, err)
		}
		cborData, err := MarshalSnapshotCBOR(snapshot)
		if err != nil {
			return migrationPlan{}, fmt.Errorf("%w: convert snapshot %s: %v", ErrMigrationPreflight, commit.SnapshotRoot, err)
		}
		plan.snapshots[commit.SnapshotRoot] = stagedSnapshot{
			legacyRoot: commit.SnapshotRoot,
			cborRoot:   serversync.ContentID(cborData),
			data:       cborData,
		}
	}

	visiting := make(map[string]bool, len(commits))
	var stageCommit func(string) error
	stageCommit = func(legacyID string) error {
		if _, done := plan.commits[legacyID]; done {
			return nil
		}
		if visiting[legacyID] {
			return fmt.Errorf("%w: cycle in legacy commit DAG at %q", ErrMigrationPreflight, legacyID)
		}
		legacy, exists := byID[legacyID]
		if !exists {
			return fmt.Errorf("%w: legacy parent %q is not available", ErrMigrationPreflight, legacyID)
		}
		visiting[legacyID] = true
		parents := make([]serversync.CommitIdentity, len(legacy.Parents))
		for i, parentID := range legacy.Parents {
			if err := stageCommit(parentID); err != nil {
				return err
			}
			parents[i] = serversync.V2CommitIdentity(plan.commits[parentID].v2ID)
		}
		snapshot, exists := plan.snapshots[legacy.SnapshotRoot]
		if !exists {
			return fmt.Errorf("%w: snapshot mapping for commit %q is missing", ErrMigrationPreflight, legacyID)
		}
		identity, err := (serversync.CommitFrameV2{
			Version:      serversync.CommitFormatV2,
			Parents:      parents,
			SnapshotRoot: snapshot.cborRoot,
			Author:       legacy.Author,
			Message:      legacy.Message,
		}).Identity()
		if err != nil {
			return fmt.Errorf("%w: frame commit %s: %v", ErrMigrationPreflight, legacyID, err)
		}
		plan.commits[legacyID] = stagedCommit{legacy: legacy, v2ID: identity.ID}
		delete(visiting, legacyID)
		return nil
	}
	for _, commit := range commits {
		if err := stageCommit(commit.ID); err != nil {
			return migrationPlan{}, err
		}
	}

	for _, legacy := range packs {
		frame, baseID, targetID, err := m.migrationPackFrame(ctx, scope, plan, legacy)
		if err != nil {
			return migrationPlan{}, err
		}
		plan.packs = append(plan.packs, stagedPack{
			legacy: legacy, v2Hash: serversync.ContentID(frame), baseID: baseID, target: targetID,
		})
	}
	return plan, nil
}

func (m *SnapshotMigrator) stage(ctx context.Context, scope cas.Scope, plan migrationPlan) error {
	for _, snapshot := range plan.snapshots {
		if err := m.objects.Put(ctx, scope, snapshot.cborRoot, snapshot.data); err != nil {
			return fmt.Errorf("review: write shadow snapshot %s: %w", snapshot.cborRoot, err)
		}
		if err := m.store.StageSnapshotObjectMapping(ctx, scope.RepoID(), snapshot.legacyRoot, snapshot.cborRoot); err != nil {
			return fmt.Errorf("review: record snapshot mapping: %w", err)
		}
	}
	for _, commit := range plan.commits {
		snapshot := plan.snapshots[commit.legacy.SnapshotRoot]
		if err := m.store.StageSnapshotCommitMapping(ctx, scope.RepoID(), commit.legacy.ID, commit.v2ID, snapshot.legacyRoot, snapshot.cborRoot); err != nil {
			return fmt.Errorf("review: record commit mapping: %w", err)
		}
	}
	for _, pack := range plan.packs {
		frame, _, _, err := m.migrationPackFrame(ctx, scope, plan, pack.legacy)
		if err != nil {
			return err
		}
		if serversync.ContentID(frame) != pack.v2Hash {
			return fmt.Errorf("review: migration pack %s changed after preflight", pack.legacy.Hash)
		}
		if err := m.objects.WritePack(ctx, scope, pack.v2Hash, bytes.NewReader(frame)); err != nil {
			return fmt.Errorf("review: write shadow pack %s: %w", pack.v2Hash, err)
		}
		if err := m.store.StageSnapshotPackMapping(ctx, scope.RepoID(), pack.legacy.Hash, pack.v2Hash,
			pack.legacy.BaseID, pack.legacy.TargetID, pack.baseID, pack.target); err != nil {
			return fmt.Errorf("review: record pack mapping: %w", err)
		}
	}
	return nil
}

func (m *SnapshotMigrator) migrationPackFrame(ctx context.Context, scope cas.Scope, plan migrationPlan, legacy postgres.SnapshotMigrationPack) ([]byte, string, string, error) {
	if legacy.Hash == "" || legacy.TargetID == "" {
		return nil, "", "", fmt.Errorf("%w: pack metadata is incomplete", ErrMigrationPreflight)
	}
	target, exists := plan.commits[legacy.TargetID]
	if !exists {
		return nil, "", "", fmt.Errorf("%w: pack %s target %s has no migration mapping", ErrMigrationPreflight, legacy.Hash, legacy.TargetID)
	}
	var base serversync.CommitIdentity
	baseID := ""
	if legacy.BaseID != "" {
		mapped, exists := plan.commits[legacy.BaseID]
		if !exists {
			return nil, "", "", fmt.Errorf("%w: pack %s base %s has no migration mapping", ErrMigrationPreflight, legacy.Hash, legacy.BaseID)
		}
		base = serversync.V2CommitIdentity(mapped.v2ID)
		baseID = mapped.v2ID
	}
	payload, err := m.readLegacyPack(ctx, scope, legacy.Hash)
	if err != nil {
		return nil, "", "", err
	}
	path, err := migrationPackPath(plan.commits, legacy)
	if err != nil {
		return nil, "", "", err
	}
	supplemental, err := migrationSupplementalCommits(plan.commits, path, legacy.BaseID)
	if err != nil {
		return nil, "", "", err
	}
	primaryIDs := make(map[string]struct{}, len(path))
	for _, staged := range path {
		primaryIDs[staged.legacy.ID] = struct{}{}
	}
	frames := make([]serversync.CommitFrameV2, 0, len(path))
	supplementalFrames := make([]serversync.CommitFrameV2, 0, len(supplemental))
	objects := make([]serversync.PackObjectV2, 0, len(path)+len(supplemental))
	seenObjects := make(map[string]struct{}, len(path)+len(supplemental))
	stagedCommits := make([]stagedCommit, 0, len(path)+len(supplemental))
	stagedCommits = append(stagedCommits, path...)
	stagedCommits = append(stagedCommits, supplemental...)
	for _, staged := range stagedCommits {
		parents := make([]serversync.CommitIdentity, len(staged.legacy.Parents))
		for i, parentID := range staged.legacy.Parents {
			parent, exists := plan.commits[parentID]
			if !exists {
				return nil, "", "", fmt.Errorf("%w: commit %s parent %s has no mapping", ErrMigrationPreflight, staged.legacy.ID, parentID)
			}
			parents[i] = serversync.V2CommitIdentity(parent.v2ID)
		}
		frame := serversync.CommitFrameV2{
			Version:      serversync.CommitFormatV2,
			Parents:      parents,
			SnapshotRoot: plan.snapshots[staged.legacy.SnapshotRoot].cborRoot,
			Author:       staged.legacy.Author,
			Message:      staged.legacy.Message,
		}
		identity, err := frame.Identity()
		if err != nil || identity.ID != staged.v2ID {
			return nil, "", "", fmt.Errorf("%w: commit %s frame identity changed", ErrMigrationPreflight, staged.legacy.ID)
		}
		if _, primary := primaryIDs[staged.legacy.ID]; primary {
			frames = append(frames, frame)
		} else {
			supplementalFrames = append(supplementalFrames, frame)
		}
		snapshot := plan.snapshots[staged.legacy.SnapshotRoot]
		if _, exists := seenObjects[snapshot.cborRoot]; !exists {
			objects = append(objects, serversync.PackObjectV2{ID: snapshot.cborRoot, Data: snapshot.data})
			seenObjects[snapshot.cborRoot] = struct{}{}
		}
	}
	frame, err := serversync.MarshalPackFrameV2(serversync.PackFrameV2{
		Version:             serversync.PackFormatV2,
		Base:                base,
		Target:              serversync.V2CommitIdentity(target.v2ID),
		Commits:             frames,
		Objects:             objects,
		LegacyPackID:        legacy.Hash,
		LegacyPayload:       payload,
		SupplementalCommits: supplementalFrames,
	})
	if err != nil {
		return nil, "", "", fmt.Errorf("%w: frame legacy pack %s: %v", ErrMigrationPreflight, legacy.Hash, err)
	}

	return frame, baseID, target.v2ID, nil
}

func (m *SnapshotMigrator) readLegacyPack(ctx context.Context, scope cas.Scope, hash string) ([]byte, error) {
	pack, err := m.objects.OpenPack(ctx, scope, hash)
	if err != nil {
		return nil, fmt.Errorf("%w: open legacy pack %s: %v", ErrMigrationPreflight, hash, err)
	}
	payload, readErr := io.ReadAll(io.LimitReader(pack, serversync.MaxV2PackBytes+1))
	closeErr := pack.Close()
	if readErr != nil {
		return nil, fmt.Errorf("%w: read legacy pack %s: %v", ErrMigrationPreflight, hash, readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("%w: close legacy pack %s: %v", ErrMigrationPreflight, hash, closeErr)
	}
	if int64(len(payload)) > serversync.MaxV2PackBytes {
		return nil, fmt.Errorf("%w: legacy pack %s exceeds %d byte limit", ErrMigrationPreflight, hash, serversync.MaxV2PackBytes)
	}
	if serversync.ContentID(payload) != hash {
		return nil, fmt.Errorf("%w: legacy pack %s fails BLAKE3 verification", ErrMigrationPreflight, hash)
	}
	return payload, nil
}

func migrationPackPath(commits map[string]stagedCommit, pack postgres.SnapshotMigrationPack) ([]stagedCommit, error) {
	path := make([]stagedCommit, 0)
	seen := make(map[string]struct{})
	current := pack.TargetID
	for current != pack.BaseID {
		if _, duplicate := seen[current]; duplicate {
			return nil, fmt.Errorf("%w: pack %s has a cyclic first-parent path", ErrMigrationPreflight, pack.Hash)
		}
		seen[current] = struct{}{}
		commit, exists := commits[current]
		if !exists {
			return nil, fmt.Errorf("%w: pack %s target path commit %s has no mapping", ErrMigrationPreflight, pack.Hash, current)
		}
		path = append(path, commit)
		if len(commit.legacy.Parents) == 0 {
			if pack.BaseID != "" {
				return nil, fmt.Errorf("%w: pack %s does not reach base %s", ErrMigrationPreflight, pack.Hash, pack.BaseID)
			}
			break
		}
		current = commit.legacy.Parents[0]
	}
	if len(path) == 0 {
		return nil, fmt.Errorf("%w: pack %s does not advance its commit range", ErrMigrationPreflight, pack.Hash)
	}
	for left, right := 0, len(path)-1; left < right; left, right = left+1, right-1 {
		path[left], path[right] = path[right], path[left]
	}
	return path, nil
}

func migrationSupplementalCommits(commits map[string]stagedCommit, primary []stagedCommit, baseID string) ([]stagedCommit, error) {
	baseHistory := make(map[string]struct{})
	var markBaseHistory func(string) error
	markBaseHistory = func(id string) error {
		if id == "" {
			return nil
		}
		if _, exists := baseHistory[id]; exists {
			return nil
		}
		commit, exists := commits[id]
		if !exists {
			return fmt.Errorf("%w: pack base history commit %s has no mapping", ErrMigrationPreflight, id)
		}
		baseHistory[id] = struct{}{}
		for _, parentID := range commit.legacy.Parents {
			if err := markBaseHistory(parentID); err != nil {
				return err
			}
		}
		return nil
	}
	if err := markBaseHistory(baseID); err != nil {
		return nil, err
	}

	included := make(map[string]struct{}, len(primary))
	for _, commit := range primary {
		included[commit.legacy.ID] = struct{}{}
	}
	supplemental := make([]stagedCommit, 0)
	var include func(string) error
	include = func(id string) error {
		if _, baseHistoryContains := baseHistory[id]; baseHistoryContains {
			return nil
		}
		if _, alreadyIncluded := included[id]; alreadyIncluded {
			return nil
		}
		commit, exists := commits[id]
		if !exists {
			return fmt.Errorf("%w: supplemental commit %s has no mapping", ErrMigrationPreflight, id)
		}
		included[id] = struct{}{}
		for _, parentID := range commit.legacy.Parents {
			if err := include(parentID); err != nil {
				return err
			}
		}
		supplemental = append(supplemental, commit)
		return nil
	}
	for _, commit := range primary {
		if len(commit.legacy.Parents) < 2 {
			continue
		}
		for _, parentID := range commit.legacy.Parents[1:] {
			if err := include(parentID); err != nil {
				return nil, err
			}
		}
	}
	return supplemental, nil
}
