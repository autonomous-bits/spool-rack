package sync

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	"lukechampine.com/blake3"
)

func TestPushEngineHandlePush_FastForwardSuccess(t *testing.T) {
	t.Parallel()

	rootDir := testWorkspaceDir(t)
	driver, err := cas.NewLocalDriver(rootDir)
	if err != nil {
		t.Fatalf("NewLocalDriver() error = %v", err)
	}

	baseCommit := hashString("commit-a")
	targetParent := hashString("commit-b")
	targetCommit := hashString("commit-c")
	store := &fakeBranchStore{
		branchHeads: map[string]string{"main": baseCommit},
	}
	engine := NewPushEngine(driver, store)

	packData := []byte("pack payload for fast-forward push")
	packHash := hashBytes(packData)
	req := PushRequest{
		TenantID:     "tenant-123",
		RepoID:       "repo-456",
		Branch:       "main",
		BaseCommit:   baseCommit,
		TargetCommit: targetCommit,
		Commits: []CommitRecord{
			{ID: targetParent, ParentID: baseCommit, SnapshotRoot: hashString("snap-b"), Author: "alice", Message: "commit b"},
			{ID: targetCommit, ParentID: targetParent, SnapshotRoot: hashString("snap-c"), Author: "alice", Message: "commit c"},
		},
		PackHash:   packHash,
		PackStream: bytes.NewReader(packData),
	}

	if err := engine.HandlePush(context.Background(), req); err != nil {
		t.Fatalf("HandlePush() error = %v", err)
	}

	if store.getBranchRefCalls != 1 {
		t.Fatalf("GetBranchRef() calls = %d, want 1", store.getBranchRefCalls)
	}
	if len(store.putCommitCalls) != len(req.Commits) {
		t.Fatalf("PutCommit() calls = %d, want %d", len(store.putCommitCalls), len(req.Commits))
	}
	for i, got := range store.putCommitCalls {
		want := req.Commits[i]
		if got != (putCommitCall{
			repoID:       req.RepoID,
			commitID:     want.ID,
			parentID:     want.ParentID,
			snapshotRoot: want.SnapshotRoot,
			author:       want.Author,
			message:      want.Message,
		}) {
			t.Fatalf("PutCommit() call %d = %+v, want %+v", i, got, putCommitCall{
				repoID:       req.RepoID,
				commitID:     want.ID,
				parentID:     want.ParentID,
				snapshotRoot: want.SnapshotRoot,
				author:       want.Author,
				message:      want.Message,
			})
		}
	}
	wantOrder := []string{
		"put:" + targetParent,
		"put:" + targetCommit,
		"pack",
		"get",
		"cas",
	}
	if strings.Join(store.callOrder, ",") != strings.Join(wantOrder, ",") {
		t.Fatalf("call order = %v, want %v", store.callOrder, wantOrder)
	}
	if store.isAncestorCalls != 0 {
		t.Fatalf("IsAncestor() calls = %d, want 0", store.isAncestorCalls)
	}
	if store.compareAndSwapCalls != 1 {
		t.Fatalf("CompareAndSwapBranchRef() calls = %d, want 1", store.compareAndSwapCalls)
	}
	if got := store.compareAndSwapArgs; got != (casCallArgs{repoID: req.RepoID, branch: req.Branch, expectedCommit: req.BaseCommit, newCommit: req.TargetCommit}) {
		t.Fatalf("CompareAndSwapBranchRef() args = %+v, want repo=%q branch=%q expected=%q new=%q", got, req.RepoID, req.Branch, req.BaseCommit, req.TargetCommit)
	}

	scope, err := cas.NewScope(req.TenantID, req.RepoID)
	if err != nil {
		t.Fatalf("NewScope() error = %v", err)
	}
	rc, err := driver.OpenPack(context.Background(), scope, packHash)
	if err != nil {
		t.Fatalf("OpenPack() error = %v", err)
	}
	defer func() { _ = rc.Close() }()

	gotPackData, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(OpenPack()) error = %v", err)
	}
	if !bytes.Equal(gotPackData, packData) {
		t.Fatalf("OpenPack() data mismatch: got %q want %q", gotPackData, packData)
	}

	if !packExistsUnderScopedLayout(t, rootDir, req.TenantID, req.RepoID) {
		t.Fatalf("expected pack file somewhere under the scoped pack storage layout for tenant %q repo %q", req.TenantID, req.RepoID)
	}
}

func TestPushEngineHandlePushAcceptsOnlyCanonicalV2PackFrames(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := hashString("legacy-base")
	object := []byte("canonical v2 snapshot object")
	frameCommit := CommitFrameV2{
		Version:      CommitFormatV2,
		Parents:      []CommitIdentity{LegacyCommitIdentity(base)},
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
		Base:    LegacyCommitIdentity(base),
		Target:  target,
		Commits: []CommitFrameV2{frameCommit},
		Objects: []PackObjectV2{{ID: ContentID(object), Data: object}},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeBranchStore{branchHeads: map[string]string{"main": base}}
	req := PushRequest{
		TenantID: "tenant-123", RepoID: "repo-456", Branch: "main",
		BaseCommit: base, TargetCommit: target.ID,
		Commits: []CommitRecord{{
			ID: target.ID, Identity: &target, ParentID: base, SnapshotRoot: ContentID(object), Author: "Ada", Message: "v2 push",
		}},
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

	req.Commits[0].Message = "tampered metadata"
	req.PackStream = bytes.NewReader(packData)
	err = NewPushEngine(driver, &fakeBranchStore{branchHeads: map[string]string{"main": base}}, validator).HandlePush(context.Background(), req)
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("HandlePush(tampered v2 metadata) error = %v, want frame error", err)
	}
	req.Commits[0].Message = "v2 push"

	noncanonical := append([]byte{0xb8, 0x07}, packData[1:]...)
	req.PackHash = ContentID(noncanonical)
	req.PackStream = bytes.NewReader(noncanonical)
	store = &fakeBranchStore{branchHeads: map[string]string{"main": base}}
	err = NewPushEngine(driver, store, validator).HandlePush(context.Background(), req)
	if !errors.Is(err, ErrInvalidCanonicalFrame) {
		t.Fatalf("HandlePush(noncanonical v2) error = %v, want canonical frame error", err)
	}
}

func TestPushEngineHandlePushRejectsInvalidV2Snapshot(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := hashString("legacy-base")
	object := []byte("not a Rack CBOR snapshot")
	commit := CommitFrameV2{
		Version:      CommitFormatV2,
		Parents:      []CommitIdentity{LegacyCommitIdentity(base)},
		SnapshotRoot: ContentID(object),
		Author:       "Ada",
		Message:      "invalid snapshot",
	}
	target, err := commit.Identity()
	if err != nil {
		t.Fatal(err)
	}
	pack, err := MarshalPackFrameV2(PackFrameV2{
		Version: PackFormatV2, Base: LegacyCommitIdentity(base), Target: target,
		Commits: []CommitFrameV2{commit}, Objects: []PackObjectV2{{ID: ContentID(object), Data: object}},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeBranchStore{branchHeads: map[string]string{"main": base}}
	req := PushRequest{
		TenantID: "tenant-123", RepoID: "repo-456", Branch: "main", BaseCommit: base, TargetCommit: target.ID,
		Commits:  []CommitRecord{{ID: target.ID, Identity: &target, ParentID: base, SnapshotRoot: ContentID(object), Author: "Ada", Message: "invalid snapshot"}},
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
		Commits:  []CommitRecord{{ID: target.ID, Identity: &target, ParentID: base, SnapshotRoot: ContentID(object), Author: "Ada", Message: "mismatched parent format"}},
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
		Commits:  []CommitRecord{{ID: target.ID, Identity: &target, ParentID: base, SnapshotRoot: hashString("snapshot"), Author: "Ada", Message: "downgrade"}},
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
	targetCommit := hashString("commit-d")
	store := &fakeBranchStore{
		branchHeads: map[string]string{"main": actualHead},
		parentOf:    map[string]string{actualHead: remoteParent},
	}
	engine := NewPushEngine(driver, store)

	packData := []byte("divergent pack payload")
	req := PushRequest{
		TenantID:     "tenant-123",
		RepoID:       "repo-456",
		Branch:       "main",
		BaseCommit:   baseCommit,
		TargetCommit: targetCommit,
		PackHash:     hashBytes(packData),
		PackStream:   bytes.NewReader(packData),
	}

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
	targetCommit := hashString("commit-b")
	store := &fakeBranchStore{
		branchHeads: map[string]string{"main": baseCommit},
	}
	driver := &fakeCASDriver{writePackErr: errors.New("disk full")}
	engine := NewPushEngine(driver, store)

	req := PushRequest{
		TenantID:     "tenant-123",
		RepoID:       "repo-456",
		Branch:       "main",
		BaseCommit:   baseCommit,
		TargetCommit: targetCommit,
		PackHash:     strings.Repeat("a", 64),
		PackStream:   bytes.NewReader([]byte("pack payload")),
	}

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
	targetCommit := hashString("commit-b")
	commitRecord := CommitRecord{
		ID:           targetCommit,
		ParentID:     baseCommit,
		SnapshotRoot: hashString("snap-b"),
		Author:       "alice",
		Message:      "commit b",
	}
	store := &fakeBranchStore{
		branchHeads:     map[string]string{"main": baseCommit},
		putCommitErr:    errors.New("register failed"),
		putCommitErrFor: targetCommit,
	}
	driver := &fakeCASDriver{}
	engine := NewPushEngine(driver, store)

	req := PushRequest{
		TenantID:     "tenant-123",
		RepoID:       "repo-456",
		Branch:       "main",
		BaseCommit:   baseCommit,
		TargetCommit: targetCommit,
		Commits:      []CommitRecord{commitRecord},
		PackHash:     strings.Repeat("b", 64),
		PackStream:   bytes.NewReader([]byte("pack payload")),
	}

	err := engine.HandlePush(context.Background(), req)
	if err == nil {
		t.Fatal("HandlePush() error = nil, want commit registration failure")
	}
	if !strings.Contains(err.Error(), "register commit "+targetCommit) {
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
	repoID       string
	commitID     string
	parentID     string
	snapshotRoot string
	author       string
	message      string
}

func (f *fakeBranchStore) PutCommit(_ context.Context, repoID, commitID, parentCommitID, snapshotRoot, author, message string) error {
	f.putCommitCalls = append(f.putCommitCalls, putCommitCall{
		repoID:       repoID,
		commitID:     commitID,
		parentID:     parentCommitID,
		snapshotRoot: snapshotRoot,
		author:       author,
		message:      message,
	})
	f.callOrder = append(f.callOrder, "put:"+commitID)
	if f.putCommitErr != nil && (f.putCommitErrFor == "" || f.putCommitErrFor == commitID) {
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
}

func (f *fakeCASDriver) Put(context.Context, cas.Scope, string, []byte) error {
	panic("unexpected Put call")
}

func (f *fakeCASDriver) Get(context.Context, cas.Scope, string) ([]byte, error) {
	panic("unexpected Get call")
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

func hashString(data string) string {
	return hashBytes([]byte(data))
}

func packExistsUnderScopedLayout(t *testing.T, rootDir, tenantID, repoID string) bool {
	t.Helper()

	expectedFragment := filepath.Join("tenants", hashString(tenantID), "repos", hashString(repoID), "packs") + string(filepath.Separator)
	found := false
	err := filepath.WalkDir(rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(rootDir, path)
		if err != nil {
			return err
		}
		if strings.Contains(rel, expectedFragment) {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir(%q) error = %v", rootDir, err)
	}

	return found
}
