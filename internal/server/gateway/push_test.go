package gateway

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/review"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	serversync "github.com/autonomous-bits/spool-rack/internal/server/sync"
	"github.com/autonomous-bits/spool/graphcontract"
	"lukechampine.com/blake3"
)

func TestPushRejectsLegacyPack(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver() error = %v", err)
	}

	baseCommit := hashCommitString("commit-a")
	targetCommit := hashCommitString("commit-c")
	store := &fakeGatewayBranchStore{
		branchHeads: map[gatewayBranchKey]string{
			{repoID: "repo-1", branch: "main"}: baseCommit,
		},
	}
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"contributor-token": {Role: auth.RoleContributor, Subject: "u1"},
		})),
	)

	packData := []byte("legacy push payload")
	req := newPushRequest(t, pushRequestFixture{
		token:    "contributor-token",
		tenantID: "tenant-1",
		repoID:   "repo-1",
		metadata: pushMetadata{
			Branch:       "main",
			BaseCommit:   baseCommit,
			TargetCommit: targetCommit,
			PackHash:     hashPackBytes(packData),
			PackFormat:   1,
		},
		packData: packData,
	})
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorCode(t, rec, ErrorCodeBadRequest)
	if len(store.putCommitCalls) != 0 || store.compareAndSwapCalls != 0 {
		t.Fatalf("legacy push mutated metadata: commits=%d updates=%d", len(store.putCommitCalls), store.compareAndSwapCalls)
	}
}

func TestPushForbiddenForViewer(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver() error = %v", err)
	}

	baseCommit := hashCommitString("commit-a")
	targetCommit := hashCommitString("commit-b")
	store := &fakeGatewayBranchStore{
		branchHeads: map[gatewayBranchKey]string{
			{repoID: "repo-1", branch: "main"}: baseCommit,
		},
	}
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"viewer-token": {Role: auth.RoleViewer, Subject: "u2"},
		})),
	)

	packData := []byte("viewer cannot push")
	req := newPushRequest(t, pushRequestFixture{
		token:    "viewer-token",
		tenantID: "tenant-1",
		repoID:   "repo-1",
		metadata: pushMetadata{
			Branch:       "main",
			BaseCommit:   baseCommit,
			TargetCommit: targetCommit,
			PackHash:     hashPackBytes(packData),
		},
		packData: packData,
	})
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusForbidden)
	assertErrorCode(t, rec, ErrorCodeForbidden)
	if store.compareAndSwapCalls != 0 {
		t.Fatalf("CompareAndSwapBranchRef() calls = %d, want 0", store.compareAndSwapCalls)
	}
}

func TestPushRejectsOmittedLegacyPackFormat(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver() error = %v", err)
	}

	baseCommit := hashCommitString("commit-a")
	remoteParent := hashCommitString("commit-b")
	actualHead := hashCommitString("commit-c")
	targetCommit := hashCommitString("commit-d")
	store := &fakeGatewayBranchStore{
		branchHeads: map[gatewayBranchKey]string{
			{repoID: "repo-1", branch: "main"}: actualHead,
		},
		parentOf: map[string]string{
			actualHead: remoteParent,
		},
	}
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"contributor-token": {Role: auth.RoleContributor, Subject: "u3"},
		})),
	)

	packData := []byte("conflicting push payload")
	req := newPushRequest(t, pushRequestFixture{
		token:    "contributor-token",
		tenantID: "tenant-1",
		repoID:   "repo-1",
		metadata: pushMetadata{
			Branch:       "main",
			BaseCommit:   baseCommit,
			TargetCommit: targetCommit,
			PackHash:     hashPackBytes(packData),
		},
		packData: packData,
	})
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorCode(t, rec, ErrorCodeBadRequest)
}

func TestPushRejectsNonMultipartBody(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver() error = %v", err)
	}

	baseCommit := hashCommitString("commit-a")
	store := &fakeGatewayBranchStore{
		branchHeads: map[gatewayBranchKey]string{
			{repoID: "repo-1", branch: "main"}: baseCommit,
		},
	}
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
	)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/repos/repo-1/push", bytes.NewBufferString(`{"branch":"main"}`))
	req.Header.Set("Authorization", "Bearer anytoken")
	req.Header.Set(HeaderTenantID, "tenant-1")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorCode(t, rec, ErrorCodeBadRequest)
}

func TestPushRejectsInvalidPackHash(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver() error = %v", err)
	}

	baseCommit := hashCommitString("commit-a")
	store := &fakeGatewayBranchStore{
		branchHeads: map[gatewayBranchKey]string{
			{repoID: "repo-1", branch: "main"}: baseCommit,
		},
	}
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"contributor-token": {Role: auth.RoleContributor, Subject: "u4"},
		})),
	)

	req := newPushRequest(t, pushRequestFixture{
		token:    "contributor-token",
		tenantID: "tenant-1",
		repoID:   "repo-1",
		metadata: pushMetadata{
			Branch:       "main",
			BaseCommit:   baseCommit,
			TargetCommit: hashCommitString("commit-b"),
			PackHash:     "../etc/passwd",
		},
		packData: []byte("invalid pack hash should never be written"),
	})
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorCode(t, rec, ErrorCodeBadRequest)
	if len(store.putCommitCalls) != 0 {
		t.Fatalf("PutCommit() calls = %d, want 0", len(store.putCommitCalls))
	}
	if store.getBranchRefCalls != 0 {
		t.Fatalf("GetBranchRef() calls = %d, want 0", store.getBranchRefCalls)
	}
	if store.compareAndSwapCalls != 0 {
		t.Fatalf("CompareAndSwapBranchRef() calls = %d, want 0", store.compareAndSwapCalls)
	}
}

func TestPushReturnsNotImplementedWhenUnconfigured(t *testing.T) {
	t.Parallel()

	gw := New()
	packData := []byte("unconfigured push payload")
	baseCommit := hashCommitString("commit-a")
	targetCommit := hashCommitString("commit-b")
	req := newPushRequest(t, pushRequestFixture{
		token:    "anytoken",
		tenantID: "tenant-1",
		repoID:   "repo-1",
		metadata: pushMetadata{
			Branch:       "main",
			BaseCommit:   baseCommit,
			TargetCommit: targetCommit,
			PackHash:     hashPackBytes(packData),
		},
		packData: packData,
	})
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusNotImplemented)
	assertErrorCode(t, rec, ErrorCodeNotImplemented)
}

func TestPushRejectsNonCanonicalV2Snapshot(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := hashCommitString("v2-base")
	snapshot := []byte("not a Rack canonical CBOR snapshot")
	commit := serversync.CommitFrameV2{
		Version:      serversync.CommitFormatV2,
		Parents:      []serversync.CommitIdentity{serversync.V2CommitIdentity(base)},
		SnapshotRoot: serversync.ContentID(snapshot),
		Author:       "Ada",
		Message:      "reject invalid snapshot",
	}
	target, err := commit.Identity()
	if err != nil {
		t.Fatal(err)
	}
	pack, err := serversync.MarshalPackFrameV2(serversync.PackFrameV2{
		Version: serversync.PackFormatV2,
		Base:    serversync.V2CommitIdentity(base),
		Target:  target,
		Commits: []serversync.CommitFrameV2{commit},
		Objects: []serversync.PackObjectV2{{ID: serversync.ContentID(snapshot), Data: snapshot}},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeGatewayBranchStore{
		branchHeads:  map[gatewayBranchKey]string{{repoID: "repo-1", branch: "main"}: base},
		commitFormat: map[string]uint32{base: serversync.CommitFormatV2},
	}
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"contributor-token": {Role: auth.RoleContributor, Subject: "u1"},
		})),
	)
	req := newPushRequest(t, pushRequestFixture{
		token: "contributor-token", tenantID: "tenant-1", repoID: "repo-1",
		metadata: pushMetadata{
			Branch: "main", BaseCommit: base, TargetCommit: target.ID, PackHash: serversync.ContentID(pack),
			PackFormat: serversync.PackFormatV2,
			Commits:    []serversync.CommitRecord{mustGatewayCommitRecord(t, commit)},
		},
		packData: pack,
	})
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorCode(t, rec, ErrorCodeBadRequest)
	if len(store.putCommitCalls) != 0 || store.compareAndSwapCalls != 0 {
		t.Fatalf("invalid v2 snapshot mutated metadata: commits=%d branch updates=%d", len(store.putCommitCalls), store.compareAndSwapCalls)
	}
}

func TestPushAcceptsCanonicalV2Snapshot(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := hashCommitString("v2-base")
	snapshot, err := review.MarshalSnapshotCBOR(review.Snapshot{
		Version: review.SnapshotVersion,
		Schema: review.Schema{
			NodeLabels: []review.LabelRule{}, EdgeLabels: []review.LabelRule{}, Cardinalities: []review.CardinalityRule{},
		},
		Nodes: []review.Node{}, Edges: []review.Edge{},
	})
	if err != nil {
		t.Fatal(err)
	}
	commit := serversync.CommitFrameV2{
		Version:      serversync.CommitFormatV2,
		Parents:      []serversync.CommitIdentity{serversync.V2CommitIdentity(base)},
		SnapshotRoot: serversync.ContentID(snapshot),
		Author:       "Ada",
		Message:      "accept canonical snapshot",
	}
	target, err := commit.Identity()
	if err != nil {
		t.Fatal(err)
	}
	pack, err := serversync.MarshalPackFrameV2(serversync.PackFrameV2{
		Version: serversync.PackFormatV2,
		Base:    serversync.V2CommitIdentity(base),
		Target:  target,
		Commits: []serversync.CommitFrameV2{commit},
		Objects: []serversync.PackObjectV2{{ID: serversync.ContentID(snapshot), Data: snapshot}},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeGatewayBranchStore{
		branchHeads:  map[gatewayBranchKey]string{{repoID: "repo-1", branch: "main"}: base},
		commitFormat: map[string]uint32{base: serversync.CommitFormatV2},
	}
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"contributor-token": {Role: auth.RoleContributor, Subject: "u1"},
		})),
	)
	req := newPushRequest(t, pushRequestFixture{
		token: "contributor-token", tenantID: "tenant-1", repoID: "repo-1",
		metadata: pushMetadata{
			Branch: "main", BaseCommit: base, TargetCommit: target.ID, PackHash: serversync.ContentID(pack),
			PackFormat: serversync.PackFormatV2,
			Commits:    []serversync.CommitRecord{mustGatewayCommitRecord(t, commit)},
		},
		packData: pack,
	})
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusOK)
	if got := store.branchHeads[gatewayBranchKey{repoID: "repo-1", branch: "main"}]; got != target.ID {
		t.Fatalf("branch head = %s, want %s", got, target.ID)
	}
}

type pushRequestFixture struct {
	token    string
	tenantID string
	repoID   string
	metadata pushMetadata
	packData []byte
}

func newPushRequest(t *testing.T, fixture pushRequestFixture) *http.Request {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	metaWriter, err := writer.CreateFormField("metadata")
	if err != nil {
		t.Fatalf("CreateFormField(metadata) error = %v", err)
	}
	metaJSON, err := json.Marshal(fixture.metadata)
	if err != nil {
		t.Fatalf("Marshal(metadata) error = %v", err)
	}
	if _, err := metaWriter.Write(metaJSON); err != nil {
		t.Fatalf("Write(metadata) error = %v", err)
	}

	packWriter, err := writer.CreateFormFile("pack", "test.spack")
	if err != nil {
		t.Fatalf("CreateFormFile(pack) error = %v", err)
	}
	if _, err := packWriter.Write(fixture.packData); err != nil {
		t.Fatalf("Write(pack) error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("multipart writer Close() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/repos/"+fixture.repoID+"/push", &body)
	req.Header.Set("Authorization", "Bearer "+fixture.token)
	req.Header.Set(HeaderTenantID, fixture.tenantID)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return req
}

type gatewayBranchKey struct {
	repoID string
	branch string
}

type gatewayCASArgs struct {
	repoID         string
	branch         string
	expectedCommit string
	newCommit      string
}

type fakeGatewayBranchStore struct {
	branchHeads  map[gatewayBranchKey]string
	parentOf     map[string]string
	commitFormat map[string]uint32

	putCommitErr        error
	getBranchRefErr     error
	isAncestorErr       error
	getPackRangesErr    error
	compareAndSwapErr   error
	packRanges          []postgres.PackRange
	putCommitCalls      []gatewayPutCommitCall
	getBranchRefCalls   int
	isAncestorCalls     int
	compareAndSwapCalls int
	compareAndSwapArgs  gatewayCASArgs
}

func (f *fakeGatewayBranchStore) SetTenantContext(ctx context.Context, _ string) (context.Context, error) {
	return ctx, nil
}

type gatewayPutCommitCall struct {
	repoID   string
	commitID graphcontract.ObjectID
	commit   graphcontract.Commit
}

func (f *fakeGatewayBranchStore) PutCommit(_ context.Context, repoID string, commitID graphcontract.ObjectID, commit graphcontract.Commit) error {
	f.putCommitCalls = append(f.putCommitCalls, gatewayPutCommitCall{
		repoID:   repoID,
		commitID: commitID,
		commit:   commit,
	})
	return f.putCommitErr
}

func (f *fakeGatewayBranchStore) PutPackRange(context.Context, string, string, string, string) error {
	return nil
}

func (f *fakeGatewayBranchStore) GetCommitMetadata(_ context.Context, _ string, commitID string) (postgres.CommitMetadata, error) {
	format := serversync.CommitFormatLegacy
	if f.commitFormat != nil {
		format = f.commitFormat[commitID]
	}
	return postgres.CommitMetadata{ID: commitID, Format: format}, nil
}

func (f *fakeGatewayBranchStore) GetPackRanges(_ context.Context, _ string, _ string, knownCommit string) ([]postgres.PackRange, error) {
	if f.getPackRangesErr != nil {
		return nil, f.getPackRangesErr
	}
	for i, pack := range f.packRanges {
		if pack.BaseCommitID == knownCommit {
			return f.packRanges[:i+1], nil
		}
	}
	return f.packRanges, nil
}

func (f *fakeGatewayBranchStore) GetBranchRef(_ context.Context, repoID, branch string) (string, error) {
	f.getBranchRefCalls++
	if f.getBranchRefErr != nil {
		return "", f.getBranchRefErr
	}

	head, ok := f.branchHeads[gatewayBranchKey{repoID: repoID, branch: branch}]
	if !ok {
		return "", postgres.ErrBranchNotFound
	}
	return head, nil
}

func (f *fakeGatewayBranchStore) IsAncestor(_ context.Context, _ string, ancestorCommit, commit string) (bool, error) {
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

func (f *fakeGatewayBranchStore) CompareAndSwapBranchRef(_ context.Context, repoID, branch, expectedCommit, newCommit string) error {
	f.compareAndSwapCalls++
	f.compareAndSwapArgs = gatewayCASArgs{
		repoID:         repoID,
		branch:         branch,
		expectedCommit: expectedCommit,
		newCommit:      newCommit,
	}
	if f.compareAndSwapErr != nil {
		return f.compareAndSwapErr
	}

	key := gatewayBranchKey{repoID: repoID, branch: branch}
	actualHead, ok := f.branchHeads[key]
	if !ok {
		return postgres.ErrBranchNotFound
	}
	if actualHead != expectedCommit {
		return postgres.ErrNonFastForward
	}

	f.branchHeads[key] = newCommit
	return nil
}

var _ serversync.BranchStore = (*fakeGatewayBranchStore)(nil)

func mustGatewayCommitRecord(t *testing.T, frame serversync.CommitFrameV2) serversync.CommitRecord {
	t.Helper()
	commit, err := frame.Commit()
	if err != nil {
		t.Fatalf("CommitFrameV2.Commit() error = %v", err)
	}
	id, err := serversync.CommitObjectID(commit)
	if err != nil {
		t.Fatalf("CommitObjectID() error = %v", err)
	}
	return serversync.CommitRecord{ID: id, Commit: commit}
}

func hashPackBytes(data []byte) string {
	sum := blake3.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hashCommitString(data string) string {
	return hashPackBytes([]byte(data))
}
