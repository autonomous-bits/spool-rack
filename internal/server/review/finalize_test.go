package review

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	serversync "github.com/autonomous-bits/spool-rack/internal/server/sync"
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
	if frame.Target.ID != result.HeadCommit || frame.Objects[0].ID != result.SnapshotRoot {
		t.Fatalf("merged frame = %+v, want commit %q and snapshot %q", frame, result.HeadCommit, result.SnapshotRoot)
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
	if !result.FastForward || result.HeadCommit != "source" || store.casNew != "source" {
		t.Fatalf("Apply() result/store = %+v/%q, want fast-forward source", result, store.casNew)
	}
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
	roots := map[string]string{}
	for id, snapshot := range map[string]Snapshot{"base": base, "source": source, "target": target} {
		data, err := MarshalSnapshotJSON(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		roots[id] = hashBytes(data)
		if err := driver.Put(context.Background(), scope, roots[id], data); err != nil {
			t.Fatal(err)
		}
	}
	store := &fakeFinalizeStore{
		heads: map[string]string{"feature": "source", "main": "target"},
		roots: roots, fastForward: fastForward,
	}
	return NewFinalizeEngine(driver, store), store, driver
}

type fakeFinalizeStore struct {
	heads       map[string]string
	roots       map[string]string
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
func (s *fakeFinalizeStore) IsAncestor(context.Context, string, string, string) (bool, error) {
	return s.fastForward, nil
}
func (s *fakeFinalizeStore) FindLowestCommonAncestor(context.Context, string, string, string) (string, error) {
	return "base", nil
}
func (s *fakeFinalizeStore) GetCommitSnapshotRoot(_ context.Context, _ string, commit string) (string, error) {
	return s.roots[commit], nil
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
	return nil
}
func (s *fakeFinalizeStore) CompareAndSwapBranchRef(_ context.Context, _, _, _, newCommit string) error {
	s.casNew = newCommit
	return nil
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
