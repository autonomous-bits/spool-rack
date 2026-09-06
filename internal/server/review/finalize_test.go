package review

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/autonomous-bits/spool/graphcontract"
	"time"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	serversync "github.com/autonomous-bits/spool-rack/internal/server/sync"
	"github.com/klauspost/compress/zstd"
)

func TestFinalizeEngineRequiresExactConflictResolutions(t *testing.T) {
	t.Parallel()
	engine, _, _ := finalizationFixture(t, false)
	lease, err := engine.AcquireLease(context.Background(), "tenant-1", "repo-1", LeaseRequest{
		SourceBranch: "feature", TargetBranch: "main", Subject: "alice",
	})
	if err != nil {
		t.Fatalf("AcquireLease() error = %v", err)
	}
	_, err = engine.Apply(context.Background(), "tenant-1", "repo-1", ApplyRequest{
		SourceBranch: "feature", TargetBranch: "main", Subject: "alice", LeaseToken: lease.Token,
		Resolutions: []Resolution{}, Author: "alice", Message: "merge",
	})
	if !errors.Is(err, ErrInvalidMergeRequest) {
		t.Fatalf("Apply() error = %v, want invalid request", err)
	}
}

func TestFinalizeEngineFinalizesResolvedMergeAfterCASWrites(t *testing.T) {
	t.Parallel()
	engine, store, driver := finalizationFixture(t, false)
	lease, err := engine.AcquireLease(context.Background(), "tenant-1", "repo-1", LeaseRequest{
		SourceBranch: "feature", TargetBranch: "main", Subject: "alice",
	})
	if err != nil {
		t.Fatalf("AcquireLease() error = %v", err)
	}
	result, err := engine.Apply(context.Background(), "tenant-1", "repo-1", ApplyRequest{
		SourceBranch: "feature", TargetBranch: "main", Subject: "alice", LeaseToken: lease.Token,
		Resolutions: []Resolution{{Token: "property:node:n:name", Choice: "source"}},
		Author:      "alice", Message: "merge feature",
	})
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if result.FastForward || result.HeadCommit == "" || result.SnapshotRoot == "" || result.PackHash == "" {
		t.Fatalf("Apply() result = %+v, want synthesized merge metadata", result)
	}
	if store.apply.PackHash != result.PackHash || store.apply.ResultCommitID != result.HeadCommit {
		t.Fatalf("ApplyMerge() request = %+v, want result metadata", store.apply)
	}
	scope, _ := cas.NewScope("tenant-1", "repo-1")
	snapshotData, err := driver.Get(context.Background(), scope, result.SnapshotRoot)
	if err != nil {
		t.Fatalf("merged snapshot was not written before store apply: %v", err)
	}
	if _, err := DecodeSnapshotCBOR(snapshotData); err != nil {
		t.Fatalf("merged snapshot must use canonical CBOR: %v", err)
	}
	pack, err := driver.OpenPack(context.Background(), scope, result.PackHash)
	if err != nil {
		t.Fatalf("merged pack was not written before store apply: %v", err)
	}
	packData, readErr := io.ReadAll(pack)
	closeErr := pack.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read merged pack: %v / %v", readErr, closeErr)
	}
	frame, err := serversync.UnmarshalPackFrameV2(packData)
	if err != nil {
		t.Fatalf("merged pack must use a canonical v2 frame: %v", err)
	}
	if frame.Target.ID != result.HeadCommit {
		t.Fatalf("merged frame target = %q, want commit %q", frame.Target.ID, result.HeadCommit)
	}
	if len(frame.Commits) != 1 || len(frame.Objects) != 1 {
		t.Fatalf("merged frame has %d commits and %d objects, want one bounded merge slice", len(frame.Commits), len(frame.Objects))
	}
}

func TestFinalizeEngineFastForwardsWithoutLease(t *testing.T) {
	t.Parallel()
	engine, store, _ := finalizationFixture(t, true)
	result, err := engine.Apply(context.Background(), "tenant-1", "repo-1", ApplyRequest{
		SourceBranch: "feature", TargetBranch: "main", Subject: "alice",
		Resolutions: []Resolution{}, Author: "alice", Message: "fast-forward",
	})
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if !result.FastForward || result.HeadCommit != store.heads["feature"] || store.casNew != store.heads["feature"] {
		t.Fatalf("Apply() result/store = %+v/%q, want fast-forward source %q", result, store.casNew, store.heads["feature"])
	}
}

func TestFinalizeEnginePullsTargetOnlyMergeWithOrderedParentPacks(t *testing.T) {
	t.Parallel()

	engine, store, driver := finalizationFixture(t, false)
	lease, err := engine.AcquireLease(context.Background(), "tenant-1", "repo-1", LeaseRequest{
		SourceBranch: "feature", TargetBranch: "main", Subject: "alice",
	})
	if err != nil {
		t.Fatal(err)
	}
	target := store.heads["main"]
	result, err := engine.Apply(context.Background(), "tenant-1", "repo-1", ApplyRequest{
		SourceBranch: "feature", TargetBranch: "main", Subject: "alice", LeaseToken: lease.Token,
		Resolutions: []Resolution{{Token: "property:node:n:name", Choice: "source"}},
		Author:      "alice", Message: "merge feature",
	})
	if err != nil {
		t.Fatal(err)
	}
	var response bytes.Buffer
	head, err := serversync.NewPullEngine(driver, store).HandlePull(context.Background(), serversync.PullRequest{
		TenantID: "tenant-1", RepoID: "repo-1", Branch: "main", KnownCommit: target,
	}, &response)
	if err != nil {
		t.Fatalf("HandlePull() error = %v", err)
	}
	if head != result.HeadCommit {
		t.Fatalf("pull head = %q, want %q", head, result.HeadCommit)
	}
	decoder, err := zstd.NewReader(bytes.NewReader(response.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := io.ReadAll(decoder)
	decoder.Close()
	if err != nil {
		t.Fatalf("decode pull response: %v", err)
	}
	manifest, packs, err := serversync.UnmarshalPullEnvelopeV2(envelope)
	if err != nil {
		t.Fatalf("UnmarshalPullEnvelopeV2() error = %v", err)
	}
	if manifest.Head != result.HeadCommit || len(packs) != 3 {
		t.Fatalf("target-only pull = head %q packs %d, want merge head and three dependency-ordered packs", manifest.Head, len(packs))
	}
	frames := make([]serversync.PackFrameV2, len(packs))
	for i, pack := range packs {
		frames[i], err = serversync.UnmarshalPackFrameV2(pack)
		if err != nil {
			t.Fatalf("pull pack %d is not canonical v2: %v", i, err)
		}
	}
	scope, err := cas.NewScope("tenant-1", "repo-1")
	if err != nil {
		t.Fatal(err)
	}
	targetPack, err := driver.OpenPack(context.Background(), scope, store.ranges[target][0].PackHash)
	if err != nil {
		t.Fatal(err)
	}
	targetPackData, err := io.ReadAll(targetPack)
	closeErr := targetPack.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("read target recipient history: %v / %v", err, closeErr)
	}
	targetFrame, err := serversync.UnmarshalPackFrameV2(targetPackData)
	if err != nil {
		t.Fatal(err)
	}
	frames = append(frames, targetFrame)
	verifyPullableMergePack(t, frames, result.HeadCommit)
}

func finalizationFixture(t *testing.T, fastForward bool) (*FinalizeEngine, *fakeFinalizeStore, *cas.LocalDriver) {
	t.Helper()
	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := testSnapshot(testNode("n", testProperty("name", `"base"`)))
	source := testSnapshot(testNode("n", testProperty("name", `"source"`)))
	target := testSnapshot(testNode("n", testProperty("name", `"target"`)))
	scope, _ := cas.NewScope("tenant-1", "repo-1")
	snapshots := []Snapshot{base, source, target}
	snapshotData := make([][]byte, len(snapshots))
	roots := make([]string, len(snapshots))
	for i, snapshot := range snapshots {
		data, err := MarshalSnapshot(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		snapshotData[i] = data
		roots[i] = hashBytes(data)
		if err := driver.Put(context.Background(), scope, roots[i], data); err != nil {
			t.Fatal(err)
		}
	}
	baseFrame := serversync.CommitFrameV2{
		Version: serversync.CommitFormatV2, Parents: []serversync.CommitIdentity{},
		SnapshotRoot: roots[0], Author: "Ada", Message: "base",
	}
	baseID, err := baseFrame.Identity()
	if err != nil {
		t.Fatal(err)
	}
	sourceFrame := serversync.CommitFrameV2{
		Version: serversync.CommitFormatV2, Parents: []serversync.CommitIdentity{baseID},
		SnapshotRoot: roots[1], Author: "Ada", Message: "source",
	}
	sourceID, err := sourceFrame.Identity()
	if err != nil {
		t.Fatal(err)
	}
	targetFrame := serversync.CommitFrameV2{
		Version: serversync.CommitFormatV2, Parents: []serversync.CommitIdentity{baseID},
		SnapshotRoot: roots[2], Author: "Ada", Message: "target",
	}
	targetID, err := targetFrame.Identity()
	if err != nil {
		t.Fatal(err)
	}
	writePack := func(base, target serversync.CommitIdentity, commits []serversync.CommitFrameV2, objects []serversync.PackObjectV2) postgres.PackRange {
		t.Helper()
		packData, err := serversync.MarshalPackFrameV2(serversync.PackFrameV2{
			Version: serversync.PackFormatV2, Base: base, Target: target, Commits: commits, Objects: objects,
		})
		if err != nil {
			t.Fatal(err)
		}
		packHash := serversync.ContentID(packData)
		if err := driver.WritePack(context.Background(), scope, packHash, bytes.NewReader(packData)); err != nil {
			t.Fatal(err)
		}
		return postgres.PackRange{PackHash: packHash, BaseCommitID: base.ID, TargetCommitID: target.ID, Format: serversync.PackFormatV2}
	}
	baseRange := writePack(serversync.CommitIdentity{}, baseID, []serversync.CommitFrameV2{baseFrame},
		[]serversync.PackObjectV2{{ID: roots[0], Data: snapshotData[0]}})
	sourceRange := writePack(baseID, sourceID, []serversync.CommitFrameV2{sourceFrame},
		[]serversync.PackObjectV2{{ID: roots[1], Data: snapshotData[1]}})
	targetRange := writePack(baseID, targetID, []serversync.CommitFrameV2{targetFrame},
		[]serversync.PackObjectV2{{ID: roots[2], Data: snapshotData[2]}})
	store := &fakeFinalizeStore{
		heads:   map[string]string{"feature": sourceID.ID, "main": targetID.ID},
		roots:   map[string]string{baseID.ID: roots[0], sourceID.ID: roots[1], targetID.ID: roots[2]},
		formats: map[string]uint32{baseID.ID: serversync.CommitFormatV2, sourceID.ID: serversync.CommitFormatV2, targetID.ID: serversync.CommitFormatV2},
		ranges: map[string][]postgres.PackRange{
			sourceID.ID: {sourceRange, baseRange},
			targetID.ID: {targetRange, baseRange},
		},
		base:        baseID.ID,
		fastForward: fastForward,
	}
	return NewFinalizeEngine(driver, store), store, driver
}

type fakeFinalizeStore struct {
	heads       map[string]string
	roots       map[string]string
	formats     map[string]uint32
	ranges      map[string][]postgres.PackRange
	base        string
	fastForward bool
	lease       postgres.MergeLease
	apply       postgres.ApplyMergeRequest
	casNew      string
}

func (s *fakeFinalizeStore) SetTenantContext(ctx context.Context, tenantID string) (context.Context, error) {
	if tenantID != "tenant-1" {
		return nil, errors.New("incorrect tenant")
	}
	return ctx, nil
}
func (s *fakeFinalizeStore) GetBranchRef(_ context.Context, _ string, branch string) (string, error) {
	return s.heads[branch], nil
}
func (s *fakeFinalizeStore) IsAncestor(_ context.Context, _ string, ancestor, commit string) (bool, error) {
	return s.fastForward || (ancestor == s.apply.TargetCommitID && commit == s.apply.ResultCommitID), nil
}
func (s *fakeFinalizeStore) FindLowestCommonAncestor(context.Context, string, string, string) (string, error) {
	return s.base, nil
}
func (s *fakeFinalizeStore) GetCommitSnapshotRoot(_ context.Context, _ string, commit string) (string, error) {
	return s.roots[commit], nil
}
func (s *fakeFinalizeStore) GetCommitMetadata(_ context.Context, _ string, commit string) (postgres.CommitMetadata, error) {
	return postgres.CommitMetadata{ID: commit, SnapshotRoot: s.roots[commit], Format: s.formats[commit]}, nil
}
func (s *fakeFinalizeStore) GetPackRanges(_ context.Context, _ string, head, known string) ([]postgres.PackRange, error) {
	if known == "" {
		return append([]postgres.PackRange(nil), s.ranges[head]...), nil
	}
	if head == s.apply.ResultCommitID && known == s.apply.TargetCommitID {
		return append([]postgres.PackRange(nil), s.ranges[head]...), nil
	}
	return nil, errors.New("unexpected pull range")
}
func (s *fakeFinalizeStore) PutCommit(context.Context, string, graphcontract.ObjectID, graphcontract.Commit) error {
	return errors.New("unexpected push commit")
}
func (s *fakeFinalizeStore) PutPackRange(context.Context, string, string, string, string) error {
	return errors.New("unexpected push pack range")
}
func (s *fakeFinalizeStore) AcquireMergeLease(_ context.Context, request postgres.MergeLeaseRequest) (postgres.MergeLease, error) {
	s.lease = postgres.MergeLease{RepoID: request.RepoID, TargetBranch: request.TargetBranch, Subject: request.Subject, Token: "lease",
		SourceCommitID: request.SourceCommitID, TargetCommitID: request.TargetCommitID, BaseCommitID: request.BaseCommitID, ExpiresAt: time.Now().Add(time.Minute)}
	return s.lease, nil
}
func (s *fakeFinalizeStore) ValidateMergeLease(_ context.Context, _, _, _, token string) (postgres.MergeLease, error) {
	if token != s.lease.Token {
		return postgres.MergeLease{}, postgres.ErrMergeLeaseOwnership
	}
	return s.lease, nil
}
func (s *fakeFinalizeStore) ReleaseMergeLease(context.Context, string, string, string, string) error {
	return nil
}
func (s *fakeFinalizeStore) ApplyMerge(_ context.Context, request postgres.ApplyMergeRequest) error {
	s.apply = request
	s.heads[request.TargetBranch] = request.ResultCommitID
	s.ranges[request.ResultCommitID] = []postgres.PackRange{{
		PackHash: request.PackHash, BaseCommitID: request.TargetCommitID,
		TargetCommitID: request.ResultCommitID, Format: serversync.PackFormatV2,
	}}
	return nil
}
func (s *fakeFinalizeStore) CompareAndSwapBranchRef(_ context.Context, _, _, _, newCommit string) error {
	s.casNew = newCommit
	return nil
}

func verifyPullableMergePack(t *testing.T, frames []serversync.PackFrameV2, target string) {
	t.Helper()
	commits := make(map[string]serversync.CommitFrameV2)
	objects := make(map[string][]byte)
	for _, frame := range frames {
		for _, commit := range frame.Commits {
			identity, err := commit.Identity()
			if err != nil {
				t.Fatalf("commit identity: %v", err)
			}
			commits[identity.ID] = commit
		}
		for _, object := range frame.Objects {
			objects[object.ID] = object.Data
		}
	}
	seen := map[string]bool{}
	var verify func(string)
	verify = func(id string) {
		if seen[id] {
			return
		}
		seen[id] = true
		commit, found := commits[id]
		if !found {
			t.Fatalf("pull recipient cannot verify commit %s because its frame is absent", id)
		}
		snapshot, found := objects[commit.SnapshotRoot]
		if !found {
			t.Fatalf("pull recipient cannot verify commit %s because snapshot %s is absent", id, commit.SnapshotRoot)
		}
		if _, err := DecodeSnapshotCBOR(snapshot); err != nil {
			t.Fatalf("pull recipient cannot decode snapshot %s: %v", commit.SnapshotRoot, err)
		}
		for _, parent := range commit.Parents {
			verify(parent.ID)
		}
	}
	verify(target)
}

func TestManualResolutionDecodesTypedValue(t *testing.T) {
	t.Parallel()
	value, err := json.Marshal(testProperty("name", `"manual"`))
	if err != nil {
		t.Fatal(err)
	}
	property, exists, err := resolutionProperty(Snapshot{}, Snapshot{}, ConflictToken{Property: "name"}, Resolution{Choice: "manual", Value: value})
	if err != nil || !exists || string(property.Value) != `"manual"` {
		t.Fatalf("resolutionProperty() = %+v, %t, %v", property, exists, err)
	}
}

func TestMaterializeMergeAppliesElementBeforePropertyResolution(t *testing.T) {
	t.Parallel()
	base := testSnapshot(Node{ID: "n", Labels: []string{"base"}, Properties: []Property{testProperty("name", `"base"`)}})
	source := testSnapshot(Node{ID: "n", Labels: []string{"source"}, Properties: []Property{testProperty("name", `"source"`)}})
	target := testSnapshot(Node{ID: "n", Labels: []string{"target"}, Properties: []Property{testProperty("name", `"target"`)}})
	preview := mergeSnapshots(base, source, target)
	if len(preview.Conflicts) != 2 {
		t.Fatalf("conflicts = %+v, want structural and property conflicts", preview.Conflicts)
	}
	merged, err := materializeMerge(base, source, target, &preview, []Resolution{
		{Token: "property:node:n:name", Choice: "target"},
		{Token: "structural:node:n", Choice: "source"},
	})
	if err != nil {
		t.Fatalf("materializeMerge() error = %v", err)
	}
	node := merged.Nodes[0]
	if node.Labels[0] != "source" || string(node.Properties[0].Value) != `"target"` {
		t.Fatalf("resolved node = %+v, want source structure with target property", node)
	}
}
