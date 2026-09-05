package sync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	"github.com/klauspost/compress/zstd"
	"lukechampine.com/blake3"
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

const (
	// PullEnvelopeFormatV2 identifies the versioned outer pull response
	// envelope. Its payload is a canonical PullManifestV2 followed by the
	// packs in manifest order.
	PullEnvelopeFormatV2   uint32 = 2
	pullEnvelopeMagic             = "SRPL"
	pullEnvelopeHeaderSize        = 16
)

// ErrInvalidPullEnvelope indicates malformed, non-canonical, or tampered
// multi-pack pull data.
var ErrInvalidPullEnvelope = errors.New("sync: invalid pull envelope")

// PullPackManifestV2 describes one raw pack in a pull envelope. Packs are
// ordered oldest-to-newest and their declared lengths make each boundary
// unambiguous.
type PullPackManifestV2 struct {
	Hash   string `json:"hash" cbor:"1,keyasint"`
	Format uint32 `json:"format" cbor:"2,keyasint"`
	Length uint64 `json:"length" cbor:"3,keyasint"`
}

// PullManifestV2 is the canonical manifest at the start of every version 2
// pull envelope.
type PullManifestV2 struct {
	Version uint32               `json:"version" cbor:"1,keyasint"`
	Head    string               `json:"head" cbor:"2,keyasint"`
	Packs   []PullPackManifestV2 `json:"packs" cbor:"3,keyasint"`
}

// MarshalPullManifestV2 validates and returns the canonical encoding of a
// pull manifest.
func MarshalPullManifestV2(manifest PullManifestV2) ([]byte, error) {
	if err := manifest.validate(); err != nil {
		return nil, err
	}
	data, err := frameCanonicalCBOR.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("%w: encode manifest: %v", ErrInvalidPullEnvelope, err)
	}
	return data, nil
}

// UnmarshalPullManifestV2 verifies a canonical pull manifest.
func UnmarshalPullManifestV2(data []byte) (PullManifestV2, error) {
	var manifest PullManifestV2
	if err := frameCBORDecoder.Unmarshal(data, &manifest); err != nil {
		return PullManifestV2{}, fmt.Errorf("%w: decode manifest: %v", ErrInvalidPullEnvelope, err)
	}
	canonical, err := MarshalPullManifestV2(manifest)
	if err != nil {
		return PullManifestV2{}, err
	}
	if !bytes.Equal(data, canonical) {
		return PullManifestV2{}, fmt.Errorf("%w: manifest is not canonically encoded", ErrInvalidPullEnvelope)
	}
	return manifest, nil
}

func (m PullManifestV2) validate() error {
	if m.Version != PullEnvelopeFormatV2 {
		return fmt.Errorf("%w: unsupported manifest version %d", ErrInvalidPullEnvelope, m.Version)
	}
	if m.Head == "" {
		return fmt.Errorf("%w: head is required", ErrInvalidPullEnvelope)
	}
	if len(m.Packs) == 0 {
		return fmt.Errorf("%w: at least one pack is required", ErrInvalidPullEnvelope)
	}
	for i, pack := range m.Packs {
		if !validContentID(pack.Hash) {
			return fmt.Errorf("%w: pack %d has invalid hash", ErrInvalidPullEnvelope, i)
		}
		if pack.Format != PackFormatV2 {
			return fmt.Errorf("%w: pack %d is not canonical v2", ErrInvalidPullEnvelope, i)
		}
	}
	return nil
}

// UnmarshalPullEnvelopeV2 verifies a complete decompressed pull envelope and
// returns the manifest plus its separately bounded, hash-verified packs.
func UnmarshalPullEnvelopeV2(data []byte) (PullManifestV2, [][]byte, error) {
	if len(data) < pullEnvelopeHeaderSize || string(data[:4]) != pullEnvelopeMagic {
		return PullManifestV2{}, nil, fmt.Errorf("%w: missing envelope header", ErrInvalidPullEnvelope)
	}
	if version := binary.BigEndian.Uint32(data[4:8]); version != PullEnvelopeFormatV2 {
		return PullManifestV2{}, nil, fmt.Errorf("%w: unsupported envelope version %d", ErrInvalidPullEnvelope, version)
	}
	manifestLength := binary.BigEndian.Uint64(data[8:pullEnvelopeHeaderSize])
	if manifestLength > uint64(len(data)-pullEnvelopeHeaderSize) {
		return PullManifestV2{}, nil, fmt.Errorf("%w: manifest length exceeds envelope", ErrInvalidPullEnvelope)
	}
	manifestEnd := pullEnvelopeHeaderSize + int(manifestLength)
	manifest, err := UnmarshalPullManifestV2(data[pullEnvelopeHeaderSize:manifestEnd])
	if err != nil {
		return PullManifestV2{}, nil, err
	}

	packs := make([][]byte, len(manifest.Packs))
	frames := make([]PackFrameV2, len(manifest.Packs))
	commits := make(map[CommitIdentity]struct{})
	offset := manifestEnd
	for i, pack := range manifest.Packs {
		if pack.Length > uint64(MaxV2PackBytes) {
			return PullManifestV2{}, nil, fmt.Errorf("%w: pack %d exceeds %d byte limit", ErrInvalidPullEnvelope, i, MaxV2PackBytes)
		}
		if pack.Length > uint64(len(data)-offset) {
			return PullManifestV2{}, nil, fmt.Errorf("%w: pack %d length exceeds envelope", ErrInvalidPullEnvelope, i)
		}
		end := offset + int(pack.Length)
		packs[i] = data[offset:end]
		if ContentID(packs[i]) != pack.Hash {
			return PullManifestV2{}, nil, fmt.Errorf("%w: pack %d hash mismatch", ErrInvalidPullEnvelope, i)
		}
		frame, err := UnmarshalPackFrameV2(packs[i])
		if err != nil {
			return PullManifestV2{}, nil, fmt.Errorf("%w: pack %d is not a canonical v2 frame: %v", ErrInvalidPullEnvelope, i, err)
		}
		frames[i] = frame
		for _, commit := range frame.Commits {
			identity, err := commit.Identity()
			if err != nil {
				return PullManifestV2{}, nil, fmt.Errorf("%w: identify pack %d commit: %v", ErrInvalidPullEnvelope, i, err)
			}
			if _, duplicate := commits[identity]; duplicate {
				return PullManifestV2{}, nil, fmt.Errorf("%w: commit %s appears in multiple packs", ErrInvalidPullEnvelope, identity.ID)
			}
			commits[identity] = struct{}{}
		}
		offset = end
	}
	if offset != len(data) {
		return PullManifestV2{}, nil, fmt.Errorf("%w: trailing bytes after packs", ErrInvalidPullEnvelope)
	}
	for _, frame := range frames {
		for _, commit := range frame.Commits {
			if len(commit.Parents) < 2 {
				continue
			}
			for _, parent := range commit.Parents[1:] {
				if _, found := commits[parent]; !found {
					return PullManifestV2{}, nil, fmt.Errorf("%w: merge parent %s is not supplied by the envelope", ErrInvalidPullEnvelope, parent.ID)
				}
			}
		}
	}
	return manifest, packs, nil
}

// PullEngine negotiates and streams tenant-fenced pack ranges from CAS.
type PullEngine struct {
	driver cas.Driver
	store  BranchStore
}

// PullPlan is the fully negotiated, tenant-fenced set of ranges to stream in
// dependency order, so every referenced parent pack precedes its child.
type PullPlan struct {
	Head   string
	Scope  cas.Scope
	Ranges []postgres.PackRange
}

type plannedPullPack struct {
	range_ postgres.PackRange
	frame  PackFrameV2
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
	ranges, err = e.planPullDAG(ctx, scope, req.RepoID, req.KnownCommit, ranges)
	if err != nil {
		return PullPlan{}, err
	}
	return PullPlan{Head: head, Scope: scope, Ranges: ranges}, nil
}

// HandlePull verifies the supplied history, then streams its framed,
// oldest-to-newest pack envelope without buffering pack payloads.
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

// BuildPullManifest validates every selected, tenant-fenced CAS pack and
// returns their canonical dependency-ordered pull manifest.
func (e *PullEngine) BuildPullManifest(ctx context.Context, plan PullPlan) (PullManifestV2, error) {
	manifest := PullManifestV2{
		Version: PullEnvelopeFormatV2,
		Head:    plan.Head,
		Packs:   make([]PullPackManifestV2, 0, len(plan.Ranges)),
	}
	for _, packRange := range plan.Ranges {
		if packRange.Format != PackFormatV2 {
			return PullManifestV2{}, fmt.Errorf("%w: pack %s is not canonical v2", ErrInvalidPullEnvelope, packRange.PackHash)
		}
		if !validContentID(packRange.PackHash) {
			return PullManifestV2{}, fmt.Errorf("%w: pack %s has invalid hash", ErrInvalidPullEnvelope, packRange.PackHash)
		}
		pack, err := e.driver.OpenPack(ctx, plan.Scope, packRange.PackHash)
		if err != nil {
			return PullManifestV2{}, fmt.Errorf("sync: pull: open pack %s: %w", packRange.PackHash, err)
		}
		hasher := blake3.New(32, nil)
		length, copyErr := io.Copy(hasher, pack)
		closeErr := pack.Close()
		if copyErr != nil {
			return PullManifestV2{}, fmt.Errorf("sync: pull: read pack %s: %w", packRange.PackHash, copyErr)
		}
		if closeErr != nil {
			return PullManifestV2{}, fmt.Errorf("sync: pull: close pack %s: %w", packRange.PackHash, closeErr)
		}
		if got := fmt.Sprintf("%x", hasher.Sum(nil)); got != packRange.PackHash {
			return PullManifestV2{}, fmt.Errorf("%w: pack %s hash mismatch", ErrInvalidPullEnvelope, packRange.PackHash)
		}
		if length > MaxV2PackBytes {
			return PullManifestV2{}, fmt.Errorf("%w: pack %s exceeds %d byte limit", ErrInvalidPullEnvelope, packRange.PackHash, MaxV2PackBytes)
		}
		manifest.Packs = append(manifest.Packs, PullPackManifestV2{
			Hash: packRange.PackHash, Format: PackFormatV2, Length: uint64(length),
		})
	}
	if _, err := MarshalPullManifestV2(manifest); err != nil {
		return PullManifestV2{}, err
	}
	return manifest, nil
}

// StreamPull writes a prepared pull plan as one zstd-compressed, framed
// multi-pack payload.
func (e *PullEngine) StreamPull(ctx context.Context, plan PullPlan, w io.Writer) error {
	manifest, err := e.BuildPullManifest(ctx, plan)
	if err != nil {
		return err
	}
	return e.StreamPullWithManifest(ctx, plan, manifest, w)
}

// StreamPullWithManifest writes a previously verified manifest and its
// corresponding packs. The packs are re-checked while being streamed so CAS
// data cannot be substituted between manifest creation and emission.
func (e *PullEngine) StreamPullWithManifest(ctx context.Context, plan PullPlan, manifest PullManifestV2, w io.Writer) error {
	if err := pullManifestMatchesPlan(manifest, plan); err != nil {
		return err
	}
	encoder, err := zstd.NewWriter(w)
	if err != nil {
		return fmt.Errorf("sync: pull: create zstd encoder: %w", err)
	}
	manifestData, err := MarshalPullManifestV2(manifest)
	if err != nil {
		_ = encoder.Close()
		return err
	}
	var header [pullEnvelopeHeaderSize]byte
	copy(header[:4], pullEnvelopeMagic)
	binary.BigEndian.PutUint32(header[4:8], PullEnvelopeFormatV2)
	binary.BigEndian.PutUint64(header[8:], uint64(len(manifestData)))
	if _, err := encoder.Write(header[:]); err != nil {
		_ = encoder.Close()
		return fmt.Errorf("sync: pull: write envelope header: %w", err)
	}
	if _, err := encoder.Write(manifestData); err != nil {
		_ = encoder.Close()
		return fmt.Errorf("sync: pull: write envelope manifest: %w", err)
	}

	for i, manifestPack := range manifest.Packs {
		packRange := plan.Ranges[i]
		pack, err := e.driver.OpenPack(ctx, plan.Scope, packRange.PackHash)
		if err != nil {
			_ = encoder.Close()
			return fmt.Errorf("sync: pull: open pack %s: %w", packRange.PackHash, err)
		}
		hasher := blake3.New(32, nil)
		length, copyErr := io.Copy(io.MultiWriter(encoder, hasher), pack)
		closeErr := pack.Close()
		if copyErr != nil {
			_ = encoder.Close()
			return fmt.Errorf("sync: pull: stream pack %s: %w", packRange.PackHash, copyErr)
		}
		if closeErr != nil {
			_ = encoder.Close()
			return fmt.Errorf("sync: pull: close pack %s: %w", packRange.PackHash, closeErr)
		}
		if uint64(length) != manifestPack.Length || fmt.Sprintf("%x", hasher.Sum(nil)) != manifestPack.Hash {
			_ = encoder.Close()
			return fmt.Errorf("%w: pack %s changed while streaming", ErrInvalidPullEnvelope, packRange.PackHash)
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
	_, err := e.BuildPullManifest(ctx, plan)
	return err
}

func pullManifestMatchesPlan(manifest PullManifestV2, plan PullPlan) error {
	if _, err := MarshalPullManifestV2(manifest); err != nil {
		return err
	}
	if manifest.Head != plan.Head || len(manifest.Packs) != len(plan.Ranges) {
		return fmt.Errorf("%w: manifest does not match pull plan", ErrInvalidPullEnvelope)
	}
	for i, pack := range manifest.Packs {
		packRange := plan.Ranges[i]
		if pack.Hash != packRange.PackHash || pack.Format != PackFormatV2 || packRange.Format != PackFormatV2 {
			return fmt.Errorf("%w: manifest pack %d does not match pull plan", ErrInvalidPullEnvelope, i)
		}
	}
	return nil
}

// planPullDAG expands first-parent pull ranges with every non-first-parent
// history required by a v2 merge, then topologically orders the bounded packs.
func (e *PullEngine) planPullDAG(ctx context.Context, scope cas.Scope, repoID, knownCommit string, initial []postgres.PackRange) ([]postgres.PackRange, error) {
	selected := make(map[string]plannedPullPack, len(initial))
	pending := append([]postgres.PackRange(nil), initial...)
	frames := make(map[string]PackFrameV2, len(initial))
	commitPacks := make(map[CommitIdentity]string)
	expandedParents := make(map[CommitIdentity]struct{})

	for {
		for len(pending) != 0 {
			packRange := pending[0]
			pending = pending[1:]
			if _, exists := selected[packRange.PackHash]; exists {
				continue
			}
			frame, err := e.readV2PackFrame(ctx, scope, packRange)
			if err != nil {
				return nil, err
			}
			selected[packRange.PackHash] = plannedPullPack{range_: packRange, frame: frame}
			frames[packRange.PackHash] = frame
			for _, commit := range frame.Commits {
				identity, err := commit.Identity()
				if err != nil {
					return nil, err
				}
				if other, exists := commitPacks[identity]; exists && other != packRange.PackHash {
					return nil, fmt.Errorf("%w: commit %s appears in multiple packs", ErrInvalidPullEnvelope, identity.ID)
				}
				commitPacks[identity] = packRange.PackHash
			}
		}

		missingParents := make(map[CommitIdentity]struct{})
		for _, frame := range frames {
			for _, commit := range frame.Commits {
				if len(commit.Parents) < 2 {
					continue
				}
				for _, parent := range commit.Parents[1:] {
					if parent.Format != CommitFormatV2 {
						return nil, fmt.Errorf("%w: merge parent %s is not a v2 commit", ErrInvalidPullEnvelope, parent.ID)
					}
					if _, supplied := commitPacks[parent]; supplied || parent.ID == knownCommit {
						continue
					}
					if knownCommit != "" {
						knownContainsParent, err := e.store.IsAncestor(ctx, repoID, parent.ID, knownCommit)
						if err != nil {
							return nil, fmt.Errorf("sync: pull: check known merge-parent ancestry: %w", err)
						}
						if knownContainsParent {
							continue
						}
					}
					missingParents[parent] = struct{}{}
				}
			}
		}
		if len(missingParents) == 0 {
			break
		}
		parentIDs := make([]string, 0, len(missingParents))
		for parent := range missingParents {
			parentIDs = append(parentIDs, parent.ID)
		}
		sort.Strings(parentIDs)
		for _, parentID := range parentIDs {
			for parent := range missingParents {
				if parent.ID != parentID {
					continue
				}
				if _, expanded := expandedParents[parent]; expanded {
					return nil, fmt.Errorf("%w: merge parent %s is not supplied by its history packs", ErrInvalidPullEnvelope, parent.ID)
				}
				expandedParents[parent] = struct{}{}
				break
			}
			ranges, err := e.store.GetPackRanges(ctx, repoID, parentID, "")
			if err != nil {
				return nil, fmt.Errorf("sync: pull: get merge parent pack ranges for %s: %w", parentID, err)
			}
			if err := validatePackChain(ranges, parentID, ""); err != nil {
				return nil, err
			}
			pending = append(pending, ranges...)
		}
	}

	return orderPullPacks(selected, commitPacks)
}

func (e *PullEngine) readV2PackFrame(ctx context.Context, scope cas.Scope, packRange postgres.PackRange) (PackFrameV2, error) {
	if packRange.Format != PackFormatV2 {
		return PackFrameV2{}, fmt.Errorf("%w: pack %s is not canonical v2", ErrInvalidPullEnvelope, packRange.PackHash)
	}
	pack, err := e.driver.OpenPack(ctx, scope, packRange.PackHash)
	if err != nil {
		return PackFrameV2{}, fmt.Errorf("sync: pull: open pack %s: %w", packRange.PackHash, err)
	}
	data, readErr := io.ReadAll(io.LimitReader(pack, MaxV2PackBytes+1))
	closeErr := pack.Close()
	if readErr != nil {
		return PackFrameV2{}, fmt.Errorf("sync: pull: read pack %s: %w", packRange.PackHash, readErr)
	}
	if closeErr != nil {
		return PackFrameV2{}, fmt.Errorf("sync: pull: close pack %s: %w", packRange.PackHash, closeErr)
	}
	if int64(len(data)) > MaxV2PackBytes || ContentID(data) != packRange.PackHash {
		return PackFrameV2{}, fmt.Errorf("%w: pack %s has invalid content", ErrInvalidPullEnvelope, packRange.PackHash)
	}
	frame, err := UnmarshalPackFrameV2(data)
	if err != nil {
		return PackFrameV2{}, err
	}
	if frame.Base.ID != packRange.BaseCommitID || frame.Target.ID != packRange.TargetCommitID {
		return PackFrameV2{}, fmt.Errorf("%w: pack %s does not match its metadata range", ErrInvalidPullEnvelope, packRange.PackHash)
	}
	if frame.Base.ID != "" && frame.Base.Format != CommitFormatV2 {
		return PackFrameV2{}, fmt.Errorf("%w: pack %s has a non-v2 base", ErrInvalidPullEnvelope, packRange.PackHash)
	}
	return frame, nil
}

func orderPullPacks(packs map[string]plannedPullPack, commitPacks map[CommitIdentity]string) ([]postgres.PackRange, error) {
	dependencies := make(map[string]map[string]struct{}, len(packs))
	dependents := make(map[string][]string, len(packs))
	for hash, pack := range packs {
		dependencies[hash] = make(map[string]struct{})
		for _, commit := range pack.frame.Commits {
			for _, parent := range commit.Parents {
				if parentPack, found := commitPacks[parent]; found && parentPack != hash {
					dependencies[hash][parentPack] = struct{}{}
				}
			}
		}
		for parentPack := range dependencies[hash] {
			dependents[parentPack] = append(dependents[parentPack], hash)
		}
	}
	ready := make([]string, 0, len(packs))
	for hash, dependencies := range dependencies {
		if len(dependencies) == 0 {
			ready = append(ready, hash)
		}
	}
	sort.Strings(ready)
	ordered := make([]postgres.PackRange, 0, len(packs))
	for len(ready) != 0 {
		hash := ready[0]
		ready = ready[1:]
		ordered = append(ordered, packs[hash].range_)
		for _, dependent := range dependents[hash] {
			delete(dependencies[dependent], hash)
			if len(dependencies[dependent]) == 0 {
				ready = append(ready, dependent)
			}
		}
		sort.Strings(ready)
	}
	if len(ordered) != len(packs) {
		return nil, fmt.Errorf("%w: pack dependency cycle", ErrInvalidPullEnvelope)
	}
	return ordered, nil
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
