package sync

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	"github.com/autonomous-bits/spool/graphcontract"
	"lukechampine.com/blake3"
)

func TestPushEngineHandlePushRejectsLegacyPack(t *testing.T) {
	t.Parallel()

	baseCommit := hashString("commit-a")
	targetCommit := hashString("commit-c")
	store := &fakeBranchStore{
		branchHeads: map[string]string{"main": baseCommit},
	}
	err := NewPushEngine(&fakeCASDriver{}, store).HandlePush(context.Background(), PushRequest{
		TenantID: "tenant-123", RepoID: "repo-456", Branch: "main",
		BaseCommit: baseCommit, TargetCommit: targetCommit, PackHash: hashString("legacy pack"),
		PackFormat: 1, PackStream: bytes.NewReader([]byte("legacy pack")),
	})
	if !errors.Is(err, ErrUnsupportedContractVersion) {
		t.Fatalf("HandlePush(legacy) error = %v, want unsupported contract version", err)
	}
	if len(store.putCommitCalls) != 0 || store.getBranchRefCalls != 0 || store.compareAndSwapCalls != 0 {
		t.Fatalf("legacy push mutated metadata: commits=%d reads=%d updates=%d", len(store.putCommitCalls), store.getBranchRefCalls, store.compareAndSwapCalls)
	}
}

func TestPushEngineDefaultsOmittedFormatToCanonicalV2(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := hashString("v2-base")
	store := &fakeBranchStore{
		branchHeads:  map[string]string{"main": base},
		commitFormat: map[string]uint32{base: CommitFormatV2},
	}
	req := nativePushRequest(t, base, "default v2")
	if req.PackFormat != 0 {
		t.Fatalf("test request format = %d, want omitted format", req.PackFormat)
	}
	if err := NewPushEngine(driver, store, func([]byte) error { return nil }).HandlePush(context.Background(), req); err != nil {
		t.Fatalf("HandlePush() error = %v", err)
	}
	if store.branchHeads["main"] != req.TargetCommit {
		t.Fatalf("branch head = %q, want %q", store.branchHeads["main"], req.TargetCommit)
	}
}

func TestPushEngineEmptyBaseCommitFastForwardsIfActualHeadIsAncestor(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rootObject := []byte("root snapshot object")
	rootCommit := CommitFrameV2{
		Version:      CommitFormatV2,
		Parents:      nil,
		SnapshotRoot: ContentID(rootObject),
		Author:       "Ada",
		Message:      "root commit",
	}
	rootIdentity, err := rootCommit.Identity()
	if err != nil {
		t.Fatal(err)
	}

	childObject := []byte("child snapshot object")
	childCommit := CommitFrameV2{
		Version:      CommitFormatV2,
		Parents:      []CommitIdentity{rootIdentity},
		SnapshotRoot: ContentID(childObject),
		Author:       "Ada",
		Message:      "child commit",
	}
	target, err := childCommit.Identity()
	if err != nil {
		t.Fatal(err)
	}

	packData, err := MarshalPackFrameV2(PackFrameV2{
		Version: PackFormatV2,
		Base:    CommitIdentity{},
		Target:  target,
		Commits: []CommitFrameV2{rootCommit, childCommit},
		Objects: []PackObjectV2{
			{ID: ContentID(rootObject), Data: rootObject},
			{ID: ContentID(childObject), Data: childObject},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeBranchStore{
		branchHeads: map[string]string{"main": rootIdentity.ID},
		parentOf:    map[string]string{target.ID: rootIdentity.ID},
	}
	req := PushRequest{
		TenantID: "tenant-123", RepoID: "repo-456", Branch: "main",
		BaseCommit: "", TargetCommit: target.ID,
		Commits:  []CommitRecord{mustCommitRecord(t, rootCommit), mustCommitRecord(t, childCommit)},
		PackHash: ContentID(packData), PackFormat: PackFormatV2, PackStream: bytes.NewReader(packData),
	}
	if err := NewPushEngine(driver, store, func([]byte) error { return nil }).HandlePush(context.Background(), req); err != nil {
		t.Fatalf("HandlePush() error = %v", err)
	}
	if store.branchHeads["main"] != target.ID {
		t.Fatalf("branch head = %q, want %q", store.branchHeads["main"], target.ID)
	}
}

func TestPushEngineHandlePushAcceptsOnlyCanonicalV2PackFrames(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := hashString("v2-base")
	object := []byte("canonical v2 snapshot object")
	frameCommit := CommitFrameV2{
		Version:      CommitFormatV2,
		Parents:      []CommitIdentity{V2CommitIdentity(base)},
		SnapshotRoot: ContentID(object),
		Author:       "Ada",
		Message:      "v2 push",
	}
	target, err := frameCommit.Identity()
	if err != nil {
		t.Fatal(err)
	}
	packData, err := MarshalPackFrameV2(PackFrameV2{
		Version: PackFormatV2,
		Base:    V2CommitIdentity(base),
		Target:  target,
		Commits: []CommitFrameV2{frameCommit},
		Objects: []PackObjectV2{{ID: ContentID(object), Data: object}},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeBranchStore{branchHeads: map[string]string{"main": base}, commitFormat: map[string]uint32{base: CommitFormatV2}}
	req := PushRequest{
		TenantID: "tenant-123", RepoID: "repo-456", Branch: "main",
		BaseCommit: base, TargetCommit: target.ID,
		Commits:  []CommitRecord{mustCommitRecord(t, frameCommit)},
		PackHash: ContentID(packData), PackFormat: PackFormatV2, PackStream: bytes.NewReader(packData),
	}
	validator := func(data []byte) error {
		if !bytes.Equal(data, object) {
			return errors.New("invalid canonical snapshot")
		}
		return nil
	}
	if err := NewPushEngine(driver, store, validator).HandlePush(context.Background(), req); err != nil {
		t.Fatalf("HandlePush(v2) error = %v", err)
	}
	scope, _ := cas.NewScope(req.TenantID, req.RepoID)
	pack, err := driver.OpenPack(context.Background(), scope, req.PackHash)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pack.Close() }()
	stored, err := io.ReadAll(pack)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := UnmarshalPackFrameV2(stored); err != nil {
		t.Fatalf("stored v2 frame failed canonical verification: %v", err)
	}
	if storedObject, err := driver.Get(context.Background(), scope, ContentID(object)); err != nil || !bytes.Equal(storedObject, object) {
		t.Fatalf("v2 frame object was not persisted: %q, %v", storedObject, err)
	}

	req.Commits[0].Commit.Message = "tampered metadata"
	req.PackStream = bytes.NewReader(packData)
	err = NewPushEngine(driver, &fakeBranchStore{branchHeads: map[string]string{"main": base}, commitFormat: map[string]uint32{base: CommitFormatV2}}, validator).HandlePush(context.Background(), req)
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("HandlePush(tampered v2 metadata) error = %v, want frame error", err)
	}
	req.Commits[0].Commit.Message = "v2 push"

	noncanonical := append([]byte{0xb8, 0x07}, packData[1:]...)
	req.PackHash = ContentID(noncanonical)
	req.PackStream = bytes.NewReader(noncanonical)
	store = &fakeBranchStore{branchHeads: map[string]string{"main": base}}
	err = NewPushEngine(driver, store, validator).HandlePush(context.Background(), req)
	if !errors.Is(err, ErrInvalidCanonicalFrame) {
		t.Fatalf("HandlePush(noncanonical v2) error = %v, want canonical frame error", err)
	}
}

func TestPushEngineHandlePushRejectsMismatchedMergeParentMetadataAtomically(t *testing.T) {
	t.Parallel()

	base := hashString("merge base")
	side := hashString("merge side")
	snapshot := []byte("merge snapshot")
	frameCommit := CommitFrameV2{
		Version:      CommitFormatV2,
		Parents:      []CommitIdentity{V2CommitIdentity(base), V2CommitIdentity(side)},
		SnapshotRoot: ContentID(snapshot),
		Author:       "Ada",
		Message:      "native merge",
	}
	target, err := frameCommit.Identity()
	if err != nil {
		t.Fatal(err)
	}
	pack, err := MarshalPackFrameV2(PackFrameV2{
		Version: PackFormatV2, Base: V2CommitIdentity(base), Target: target,
		Commits: []CommitFrameV2{frameCommit},
		Objects: []PackObjectV2{{ID: ContentID(snapshot), Data: snapshot}},
	})
	if err != nil {
		t.Fatal(err)
	}
	canonical := mustCommitRecord(t, frameCommit)
	for _, test := range []struct {
		name    string
		parents []graphcontract.ObjectID
	}{
		{name: "reordered", parents: []graphcontract.ObjectID{graphcontract.ObjectID(side), graphcontract.ObjectID(base)}},
		{name: "missing", parents: []graphcontract.ObjectID{graphcontract.ObjectID(base)}},
		{name: "extra", parents: []graphcontract.ObjectID{graphcontract.ObjectID(base), graphcontract.ObjectID(side), graphcontract.ObjectID(hashString("extra parent"))}},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := canonical
			record.Commit = record.Commit.Clone()
			record.Commit.Parents = test.parents
			store := &fakeBranchStore{
				branchHeads:  map[string]string{"main": base},
				commitFormat: map[string]uint32{base: CommitFormatV2},
			}
			err := NewPushEngine(&fakeCASDriver{}, store, func([]byte) error { return nil }).HandlePush(context.Background(), PushRequest{
				TenantID: "tenant-123", RepoID: "repo-456", Branch: "main",
				BaseCommit: base, TargetCommit: target.ID, Commits: []CommitRecord{record},
				PackHash: ContentID(pack), PackFormat: PackFormatV2, PackStream: bytes.NewReader(pack),
			})
			if !errors.Is(err, ErrInvalidFrame) {
				t.Fatalf("HandlePush(%s parent metadata) error = %v, want invalid frame", test.name, err)
			}
			if len(store.putCommitCalls) != 0 || store.compareAndSwapCalls != 0 {
				t.Fatalf("%s parent metadata mutated state: commits=%d branch updates=%d", test.name, len(store.putCommitCalls), store.compareAndSwapCalls)
			}
			if got := store.branchHeads["main"]; got != base {
				t.Fatalf("%s parent metadata advanced branch to %q, want %q", test.name, got, base)
			}
		})
	}
}

func TestPushEngineHandlePushRejectsInvalidV2Snapshot(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := hashString("v2-base")
	object := []byte("not a Rack CBOR snapshot")
	commit := CommitFrameV2{
		Version:      CommitFormatV2,
		Parents:      []CommitIdentity{V2CommitIdentity(base)},
		SnapshotRoot: ContentID(object),
		Author:       "Ada",
		Message:      "invalid snapshot",
	}
	target, err := commit.Identity()
	if err != nil {
		t.Fatal(err)
	}
	pack, err := MarshalPackFrameV2(PackFrameV2{
		Version: PackFormatV2, Base: V2CommitIdentity(base), Target: target,
		Commits: []CommitFrameV2{commit}, Objects: []PackObjectV2{{ID: ContentID(object), Data: object}},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeBranchStore{branchHeads: map[string]string{"main": base}, commitFormat: map[string]uint32{base: CommitFormatV2}}
	req := PushRequest{
		TenantID: "tenant-123", RepoID: "repo-456", Branch: "main", BaseCommit: base, TargetCommit: target.ID,
		Commits:  []CommitRecord{mustCommitRecord(t, commit)},
		PackHash: ContentID(pack), PackFormat: PackFormatV2, PackStream: bytes.NewReader(pack),
	}
	err = NewPushEngine(driver, store, func([]byte) error {
		return errors.New("not a canonical Rack CBOR snapshot")
	}).HandlePush(context.Background(), req)
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("HandlePush(invalid v2 snapshot) error = %v, want invalid frame", err)
	}
	if len(store.putCommitCalls) != 0 || store.compareAndSwapCalls != 0 {
		t.Fatalf("invalid snapshot mutated metadata: commits=%d branch updates=%d", len(store.putCommitCalls), store.compareAndSwapCalls)
	}
}

func TestPushEngineHandlePushRejectsV2BaseFormatMismatch(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := hashString("legacy-base")
	object := []byte("canonical v2 snapshot object")
	commit := CommitFrameV2{
		Version:      CommitFormatV2,
		Parents:      []CommitIdentity{V2CommitIdentity(base)},
		SnapshotRoot: ContentID(object),
		Author:       "Ada",
		Message:      "mismatched parent format",
	}
	target, err := commit.Identity()
	if err != nil {
		t.Fatal(err)
	}
	pack, err := MarshalPackFrameV2(PackFrameV2{
		Version: PackFormatV2, Base: V2CommitIdentity(base), Target: target,
		Commits: []CommitFrameV2{commit}, Objects: []PackObjectV2{{ID: ContentID(object), Data: object}},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeBranchStore{branchHeads: map[string]string{"main": base}}
	req := PushRequest{
		TenantID: "tenant-123", RepoID: "repo-456", Branch: "main", BaseCommit: base, TargetCommit: target.ID,
		Commits:  []CommitRecord{mustCommitRecord(t, commit)},
		PackHash: ContentID(pack), PackFormat: PackFormatV2, PackStream: bytes.NewReader(pack),
	}
	err = NewPushEngine(driver, store, func([]byte) error { return nil }).HandlePush(context.Background(), req)
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("HandlePush(v2 base format mismatch) error = %v, want invalid frame", err)
	}
	if len(store.putCommitCalls) != 0 || store.compareAndSwapCalls != 0 {
		t.Fatalf("mismatched base format mutated metadata: commits=%d branch updates=%d", len(store.putCommitCalls), store.compareAndSwapCalls)
	}
}

func TestPushEngineHandlePushRejectsV2CommitInLegacyPack(t *testing.T) {
	t.Parallel()

	base := hashString("legacy-base")
	target := V2CommitIdentity(hashString("v2-target"))
	store := &fakeBranchStore{branchHeads: map[string]string{"main": base}}
	driver := &fakeCASDriver{}
	err := NewPushEngine(driver, store).HandlePush(context.Background(), PushRequest{
		TenantID: "tenant-123", RepoID: "repo-456", Branch: "main", BaseCommit: base, TargetCommit: target.ID,
		Commits:  []CommitRecord{{ID: graphcontract.ObjectID(target.ID)}},
		PackHash: hashString("opaque legacy pack"), PackStream: bytes.NewReader([]byte("opaque legacy pack")),
	})
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("HandlePush(v2 identity in legacy pack) error = %v, want invalid frame", err)
	}
	if driver.writePackCalls != 0 || len(store.putCommitCalls) != 0 || store.compareAndSwapCalls != 0 {
		t.Fatalf("downgrade attempt mutated state: pack=%d commits=%d branch updates=%d", driver.writePackCalls, len(store.putCommitCalls), store.compareAndSwapCalls)
	}
}

func TestPushEngineHandlePush_DivergentPushRejected(t *testing.T) {
	t.Parallel()

	rootDir := testWorkspaceDir(t)
	driver, err := cas.NewLocalDriver(rootDir)
	if err != nil {
		t.Fatalf("NewLocalDriver() error = %v", err)
	}

	baseCommit := hashString("commit-a")
	remoteParent := hashString("commit-b")
	actualHead := hashString("commit-c")
	store := &fakeBranchStore{
		branchHeads:  map[string]string{"main": actualHead},
		parentOf:     map[string]string{actualHead: remoteParent},
		commitFormat: map[string]uint32{baseCommit: CommitFormatV2},
	}
	engine := NewPushEngine(driver, store, func([]byte) error { return nil })
	req := nativePushRequest(t, baseCommit, "divergent push")

	err = engine.HandlePush(context.Background(), req)
	if err == nil {
		t.Fatal("HandlePush() error = nil, want non-fast-forward error")
	}
	if !errors.Is(err, ErrNonFastForward) {
		t.Fatalf("errors.Is(err, ErrNonFastForward) = false, err = %v", err)
	}

	var nffErr *NonFastForwardError
	if !errors.As(err, &nffErr) {
		t.Fatalf("errors.As(err, *NonFastForwardError) = false, err = %v", err)
	}
	if nffErr.ActualHead != actualHead {
		t.Fatalf("NonFastForwardError.ActualHead = %q, want %q", nffErr.ActualHead, actualHead)
	}
	if !strings.Contains(nffErr.Guidance, "branches have diverged") {
		t.Fatalf("NonFastForwardError.Guidance = %q, want divergence guidance", nffErr.Guidance)
	}

	if store.getBranchRefCalls != 1 {
		t.Fatalf("GetBranchRef() calls = %d, want 1", store.getBranchRefCalls)
	}
	if store.isAncestorCalls != 1 {
		t.Fatalf("IsAncestor() calls = %d, want 1", store.isAncestorCalls)
	}
	if store.compareAndSwapCalls != 0 {
		t.Fatalf("CompareAndSwapBranchRef() calls = %d, want 0", store.compareAndSwapCalls)
	}
}

func TestPushEngineHandlePush_CASWriteFailureStopsBeforeBranchLookup(t *testing.T) {
	t.Parallel()

	baseCommit := hashString("commit-a")
	store := &fakeBranchStore{
		branchHeads:  map[string]string{"main": baseCommit},
		commitFormat: map[string]uint32{baseCommit: CommitFormatV2},
	}
	driver := &fakeCASDriver{writePackErr: errors.New("disk full")}
	engine := NewPushEngine(driver, store, func([]byte) error { return nil })
	req := nativePushRequest(t, baseCommit, "write failure")

	err := engine.HandlePush(context.Background(), req)
	if err == nil {
		t.Fatal("HandlePush() error = nil, want CAS write failure")
	}
	if driver.writePackCalls != 1 {
		t.Fatalf("WritePack() calls = %d, want 1", driver.writePackCalls)
	}
	if store.getBranchRefCalls != 0 {
		t.Fatalf("GetBranchRef() calls = %d, want 0", store.getBranchRefCalls)
	}
	if len(store.putCommitCalls) != 0 {
		t.Fatalf("PutCommit() calls = %d, want 0", len(store.putCommitCalls))
	}
	if store.compareAndSwapCalls != 0 {
		t.Fatalf("CompareAndSwapBranchRef() calls = %d, want 0", store.compareAndSwapCalls)
	}
}

func TestPushEngineHandlePush_ValidationFailureStopsBeforeDependencies(t *testing.T) {
	t.Parallel()

	baseCommit := hashString("commit-a")
	targetCommit := hashString("commit-b")
	store := &fakeBranchStore{
		branchHeads: map[string]string{"main": baseCommit},
	}
	driver := &fakeCASDriver{}
	engine := NewPushEngine(driver, store)

	req := PushRequest{
		TenantID:     "tenant-123",
		RepoID:       "repo-456",
		Branch:       "main",
		BaseCommit:   baseCommit,
		TargetCommit: targetCommit,
		PackStream:   bytes.NewReader([]byte("pack payload")),
	}

	err := engine.HandlePush(context.Background(), req)
	if err == nil {
		t.Fatal("HandlePush() error = nil, want validation error")
	}
	if !strings.Contains(err.Error(), "pack hash is required") {
		t.Fatalf("HandlePush() error = %v, want missing pack hash validation", err)
	}
	if driver.writePackCalls != 0 {
		t.Fatalf("WritePack() calls = %d, want 0", driver.writePackCalls)
	}
	if store.getBranchRefCalls != 0 {
		t.Fatalf("GetBranchRef() calls = %d, want 0", store.getBranchRefCalls)
	}
	if len(store.putCommitCalls) != 0 {
		t.Fatalf("PutCommit() calls = %d, want 0", len(store.putCommitCalls))
	}
	if store.compareAndSwapCalls != 0 {
		t.Fatalf("CompareAndSwapBranchRef() calls = %d, want 0", store.compareAndSwapCalls)
	}
}

func TestPushEngineHandlePush_CommitRegistrationFailureStopsBeforeBranchLookup(t *testing.T) {
	t.Parallel()

	baseCommit := hashString("commit-a")
	store := &fakeBranchStore{
		branchHeads:  map[string]string{"main": baseCommit},
		commitFormat: map[string]uint32{baseCommit: CommitFormatV2},
		putCommitErr: errors.New("register failed"),
	}
	driver := &fakeCASDriver{}
	engine := NewPushEngine(driver, store, func([]byte) error { return nil })
	req := nativePushRequest(t, baseCommit, "registration failure")
	store.putCommitErrFor = req.TargetCommit

	err := engine.HandlePush(context.Background(), req)
	if err == nil {
		t.Fatal("HandlePush() error = nil, want commit registration failure")
	}
	if !strings.Contains(err.Error(), "register commit "+req.TargetCommit) {
		t.Fatalf("HandlePush() error = %v, want commit registration context", err)
	}
	if driver.writePackCalls != 1 {
		t.Fatalf("WritePack() calls = %d, want 1", driver.writePackCalls)
	}
	if len(store.putCommitCalls) != 1 {
		t.Fatalf("PutCommit() calls = %d, want 1", len(store.putCommitCalls))
	}
	if store.getBranchRefCalls != 0 {
		t.Fatalf("GetBranchRef() calls = %d, want 0", store.getBranchRefCalls)
	}
	if store.compareAndSwapCalls != 0 {
		t.Fatalf("CompareAndSwapBranchRef() calls = %d, want 0", store.compareAndSwapCalls)
	}
}

type fakeBranchStore struct {
	branchHeads  map[string]string
	parentOf     map[string]string
	commitFormat map[string]uint32

	putCommitErr        error
	putCommitErrFor     string
	getBranchRefErr     error
	isAncestorErr       error
	compareAndSwapErr   error
	putCommitCalls      []putCommitCall
	getBranchRefCalls   int
	isAncestorCalls     int
	compareAndSwapCalls int
	compareAndSwapArgs  casCallArgs
	callOrder           []string
}

func (f *fakeBranchStore) SetTenantContext(ctx context.Context, _ string) (context.Context, error) {
	return ctx, nil
}

type casCallArgs struct {
	repoID         string
	branch         string
	expectedCommit string
	newCommit      string
}

type putCommitCall struct {
	repoID   string
	commitID graphcontract.ObjectID
	commit   graphcontract.Commit
}

func (f *fakeBranchStore) PutCommit(_ context.Context, repoID string, commitID graphcontract.ObjectID, commit graphcontract.Commit) error {
	f.putCommitCalls = append(f.putCommitCalls, putCommitCall{
		repoID:   repoID,
		commitID: commitID,
		commit:   commit,
	})
	f.callOrder = append(f.callOrder, "put:"+string(commitID))
	if f.putCommitErr != nil && (f.putCommitErrFor == "" || f.putCommitErrFor == string(commitID)) {
		return f.putCommitErr
	}
	return nil
}

func (f *fakeBranchStore) PutPackRange(context.Context, string, string, string, string) error {
	f.callOrder = append(f.callOrder, "pack")
	return nil
}

func (f *fakeBranchStore) GetCommitMetadata(_ context.Context, _ string, commitID string) (postgres.CommitMetadata, error) {
	format := CommitFormatLegacy
	if f.commitFormat != nil {
		format = f.commitFormat[commitID]
	}
	return postgres.CommitMetadata{ID: commitID, Format: format}, nil
}

func (f *fakeBranchStore) GetPackRanges(context.Context, string, string, string) ([]postgres.PackRange, error) {
	panic("unexpected GetPackRanges call")
}

func (f *fakeBranchStore) GetBranchRef(_ context.Context, _ string, branch string) (string, error) {
	f.getBranchRefCalls++
	f.callOrder = append(f.callOrder, "get")
	if f.getBranchRefErr != nil {
		return "", f.getBranchRefErr
	}
	head, ok := f.branchHeads[branch]
	if !ok {
		return "", postgres.ErrBranchNotFound
	}
	return head, nil
}

func (f *fakeBranchStore) IsAncestor(_ context.Context, _ string, ancestorCommit, commit string) (bool, error) {
	f.isAncestorCalls++
	if f.isAncestorErr != nil {
		return false, f.isAncestorErr
	}

	current := commit
	for current != "" {
		if current == ancestorCommit {
			return true, nil
		}
		next, ok := f.parentOf[current]
		if !ok {
			return false, nil
		}
		current = next
	}

	return false, nil
}

func (f *fakeBranchStore) CompareAndSwapBranchRef(_ context.Context, repoID, branch, expectedCommit, newCommit string) error {
	f.compareAndSwapCalls++
	f.callOrder = append(f.callOrder, "cas")
	f.compareAndSwapArgs = casCallArgs{
		repoID:         repoID,
		branch:         branch,
		expectedCommit: expectedCommit,
		newCommit:      newCommit,
	}
	if f.compareAndSwapErr != nil {
		return f.compareAndSwapErr
	}

	actualHead, ok := f.branchHeads[branch]
	if !ok {
		return postgres.ErrBranchNotFound
	}
	if actualHead != expectedCommit {
		return postgres.ErrNonFastForward
	}

	f.branchHeads[branch] = newCommit
	return nil
}

type fakeCASDriver struct {
	writePackErr   error
	writePackCalls int
	objects        map[string][]byte
}

func (f *fakeCASDriver) Put(_ context.Context, _ cas.Scope, hash string, data []byte) error {
	if f.objects == nil {
		f.objects = make(map[string][]byte)
	}
	f.objects[hash] = append([]byte(nil), data...)
	return nil
}

func (f *fakeCASDriver) Get(_ context.Context, _ cas.Scope, hash string) ([]byte, error) {
	data, ok := f.objects[hash]
	if !ok {
		return nil, cas.ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

func (f *fakeCASDriver) Exists(context.Context, cas.Scope, string) (bool, error) {
	panic("unexpected Exists call")
}

func (f *fakeCASDriver) OpenPack(context.Context, cas.Scope, string) (io.ReadCloser, error) {
	panic("unexpected OpenPack call")
}

func (f *fakeCASDriver) WritePack(_ context.Context, _ cas.Scope, _ string, _ io.Reader) error {
	f.writePackCalls++
	return f.writePackErr
}

func testWorkspaceDir(t *testing.T) string {
	t.Helper()

	rootDir, err := os.MkdirTemp(".", "push-engine-test-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(rootDir); err != nil {
			t.Fatalf("RemoveAll(%q) error = %v", rootDir, err)
		}
	})

	return rootDir
}

func hashBytes(data []byte) string {
	sum := blake3.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func nativePushRequest(t *testing.T, base, message string) PushRequest {
	t.Helper()

	snapshot := []byte(message + " snapshot")
	commit := CommitFrameV2{
		Version: CommitFormatV2, Parents: []CommitIdentity{V2CommitIdentity(base)},
		SnapshotRoot: ContentID(snapshot), Author: "alice", Message: message,
	}
	target, err := commit.Identity()
	if err != nil {
		t.Fatal(err)
	}
	pack, err := MarshalPackFrameV2(PackFrameV2{
		Version: PackFormatV2, Base: V2CommitIdentity(base), Target: target,
		Commits: []CommitFrameV2{commit},
		Objects: []PackObjectV2{{ID: ContentID(snapshot), Data: snapshot}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return PushRequest{
		TenantID: "tenant-123", RepoID: "repo-456", Branch: "main",
		BaseCommit: base, TargetCommit: target.ID,
		Commits:  []CommitRecord{mustCommitRecord(t, commit)},
		PackHash: ContentID(pack), PackStream: bytes.NewReader(pack),
	}
}

func mustCommitRecord(t *testing.T, frame CommitFrameV2) CommitRecord {
	t.Helper()
	commit, err := frame.Commit()
	if err != nil {
		t.Fatalf("CommitFrameV2.Commit() error = %v", err)
	}
	id, err := CommitObjectID(commit)
	if err != nil {
		t.Fatalf("CommitObjectID() error = %v", err)
	}
	return CommitRecord{ID: id, Commit: commit}
}

func hashString(data string) string {
	return hashBytes([]byte(data))
}
