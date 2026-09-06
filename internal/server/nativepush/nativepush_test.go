package nativepush

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"sync"
	"testing"
	"time"

	"github.com/autonomous-bits/spool-rack/internal/server/nativeindex"
	"github.com/autonomous-bits/spool-rack/internal/server/review"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	serversync "github.com/autonomous-bits/spool-rack/internal/server/sync"
	"github.com/autonomous-bits/spool/graphcontract"
	"github.com/fxamacker/cbor/v2"
	"github.com/klauspost/compress/zstd"
	"lukechampine.com/blake3"
)

// --- native pack construction helpers (mirrors internal/server/nativeindex's
// unexported test helpers, since neither package's helpers are shared). ---

type packObjectEnvelope struct {
	Type string `cbor:"1,keyasint"`
	Data []byte `cbor:"2,keyasint"`
}

var testCanonicalCBOR, _ = cbor.CanonicalEncOptions().EncMode()

type testObject struct {
	objectType string
	data       []byte
}

type builtPack struct {
	packID   graphcontract.PackID
	manifest graphcontract.PackManifest
	data     []byte
	entries  []graphcontract.PackIndexEntry
	objectID []graphcontract.ObjectID
}

func buildNativePack(t *testing.T, packID graphcontract.PackID, objects []testObject) builtPack {
	t.Helper()

	entries := make([]graphcontract.PackIndexEntry, len(objects))
	objectIDs := make([]graphcontract.ObjectID, len(objects))
	var body bytes.Buffer
	offset := uint64(graphcontract.PackHeaderSize)

	for i, obj := range objects {
		objectID := graphcontract.ObjectIDForEncoded(obj.objectType, obj.data)
		envelope := packObjectEnvelope{Type: obj.objectType, Data: obj.data}
		encoded, err := testCanonicalCBOR.Marshal(envelope)
		if err != nil {
			t.Fatalf("marshal object envelope %d: %v", i, err)
		}

		var compressedBuf bytes.Buffer
		zw, err := zstd.NewWriter(&compressedBuf)
		if err != nil {
			t.Fatalf("create zstd writer for object %d: %v", i, err)
		}
		if _, err := zw.Write(encoded); err != nil {
			t.Fatalf("compress object %d: %v", i, err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("close zstd writer for object %d: %v", i, err)
		}
		compressed := compressedBuf.Bytes()

		entries[i] = graphcontract.PackIndexEntry{
			Object:           objectID,
			Offset:           offset,
			CompressedSize:   uint64(len(compressed)),
			UncompressedSize: uint64(len(encoded)),
			CRC32:            crc32.ChecksumIEEE(compressed),
		}
		objectIDs[i] = objectID
		offset += uint64(len(compressed))
		body.Write(compressed)
	}

	var header [12]byte
	copy(header[:4], graphcontract.PackMagic)
	binary.BigEndian.PutUint32(header[4:8], graphcontract.PackFormatVersion)
	binary.BigEndian.PutUint32(header[8:12], uint32(len(objects)))

	packData := append(append([]byte(nil), header[:]...), body.Bytes()...)

	manifest := graphcontract.PackManifest{
		Version: graphcontract.PackManifestFormatVersion,
		Packs: []graphcontract.PackMetadata{{
			ID:          packID,
			Version:     graphcontract.PackFormatVersion,
			Compression: graphcontract.PackCompressionZstd,
			ObjectCount: uint32(len(objects)),
		}},
	}

	return builtPack{packID: packID, manifest: manifest, data: packData, entries: entries, objectID: objectIDs}
}

func testPackID(name string) graphcontract.PackID {
	sum := blake3.Sum256([]byte(name))
	return graphcontract.PackID(hex.EncodeToString(sum[:16]))
}

// buildSnapshotBytes constructs a native "snapshot" object's canonical bytes
// directly (bypassing review.MarshalSnapshotCBOR's own validation) so tests
// can exercise schema-invalid graph state that a real producer would never
// intentionally emit.
type testSnapshotEnvelope struct {
	Version uint32                        `cbor:"1,keyasint"`
	Schema  graphcontract.SchemaSnapshot  `cbor:"2,keyasint"`
	Nodes   map[string]graphcontract.Node `cbor:"3,keyasint"`
	Edges   map[string]graphcontract.Edge `cbor:"4,keyasint"`
}

func buildSnapshotBytes(t *testing.T, schema graphcontract.SchemaSnapshot, nodes map[string]graphcontract.Node, edges map[string]graphcontract.Edge) []byte {
	t.Helper()
	data, err := testCanonicalCBOR.Marshal(testSnapshotEnvelope{
		Version: review.SnapshotEnvelopeVersion, Schema: schema, Nodes: nodes, Edges: edges,
	})
	if err != nil {
		t.Fatalf("marshal test snapshot envelope: %v", err)
	}
	return data
}

func validSnapshotBytes(t *testing.T) []byte {
	t.Helper()
	data, err := review.MarshalSnapshotCBOR(review.Snapshot{
		Version: review.SnapshotVersion,
		Schema:  graphcontract.BuiltinSchemaSnapshot(),
		Nodes:   map[string]graphcontract.Node{},
		Edges:   map[string]graphcontract.Edge{},
	})
	if err != nil {
		t.Fatalf("marshal valid snapshot: %v", err)
	}
	return data
}

// --- fake nativeindex.Store, mirroring nativeindex's own in-memory fake. ---

type tenantContextKey struct{}

type fakeIndexStore struct {
	mu      sync.Mutex
	packs   map[string]bool
	objects map[string]postgres.NativeObjectLocation
}

func newFakeIndexStore() *fakeIndexStore {
	return &fakeIndexStore{packs: make(map[string]bool), objects: make(map[string]postgres.NativeObjectLocation)}
}

func (f *fakeIndexStore) SetTenantContext(ctx context.Context, tenantID string) (context.Context, error) {
	if tenantID == "" {
		return nil, errors.New("fakeIndexStore: empty tenant ID")
	}
	return context.WithValue(ctx, tenantContextKey{}, tenantID), nil
}

func (f *fakeIndexStore) PutNativePack(ctx context.Context, repoID, packID, casPackHash, commitID string, entries []graphcontract.PackIndexEntry) error {
	tenantID, ok := ctx.Value(tenantContextKey{}).(string)
	if !ok || tenantID == "" {
		return errors.New("fakeIndexStore: missing tenant context")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.packs[tenantID+"|"+repoID+"|"+packID] = true
	for _, entry := range entries {
		key := tenantID + "|" + repoID + "|" + string(entry.Object)
		if _, exists := f.objects[key]; exists {
			continue
		}
		f.objects[key] = postgres.NativeObjectLocation{
			PackID: packID, CASPackHash: casPackHash,
			Offset: entry.Offset, CompressedSize: entry.CompressedSize,
			UncompressedSize: entry.UncompressedSize, CRC32: entry.CRC32,
		}
	}
	return nil
}

func (f *fakeIndexStore) GetNativeObjectLocation(ctx context.Context, repoID, objectID string) (postgres.NativeObjectLocation, error) {
	tenantID, ok := ctx.Value(tenantContextKey{}).(string)
	if !ok || tenantID == "" {
		return postgres.NativeObjectLocation{}, errors.New("fakeIndexStore: missing tenant context")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	loc, ok := f.objects[tenantID+"|"+repoID+"|"+objectID]
	if !ok {
		return postgres.NativeObjectLocation{}, postgres.ErrNativeObjectNotFound
	}
	return loc, nil
}

// --- fake serversync.BranchStore. ---

type fakeBranchStore struct {
	branchHeads  map[string]string
	commitFormat map[string]uint32

	putCommitCalls      []graphcontract.ObjectID
	getBranchRefCalls   int
	compareAndSwapCalls int
}

func (f *fakeBranchStore) SetTenantContext(ctx context.Context, _ string) (context.Context, error) {
	return ctx, nil
}

func (f *fakeBranchStore) PutCommit(_ context.Context, _ string, commitID graphcontract.ObjectID, _ graphcontract.Commit) error {
	f.putCommitCalls = append(f.putCommitCalls, commitID)
	if f.commitFormat == nil {
		f.commitFormat = make(map[string]uint32)
	}
	f.commitFormat[string(commitID)] = 1
	return nil
}

func (f *fakeBranchStore) PutPackRange(context.Context, string, string, string, string) error {
	panic("unexpected PutPackRange call")
}

func (f *fakeBranchStore) GetCommitMetadata(_ context.Context, _ string, commitID string) (postgres.CommitMetadata, error) {
	format, ok := f.commitFormat[commitID]
	if !ok {
		return postgres.CommitMetadata{}, postgres.ErrCommitNotFound
	}
	return postgres.CommitMetadata{ID: commitID, Format: format}, nil
}

func (f *fakeBranchStore) GetPackRanges(context.Context, string, string, string) ([]postgres.PackRange, error) {
	panic("unexpected GetPackRanges call")
}

func (f *fakeBranchStore) GetBranchRef(_ context.Context, _ string, branch string) (string, error) {
	f.getBranchRefCalls++
	head, ok := f.branchHeads[branch]
	if !ok {
		return "", postgres.ErrBranchNotFound
	}
	return head, nil
}

func (f *fakeBranchStore) IsAncestor(_ context.Context, _ string, ancestorCommit, commit string) (bool, error) {
	return ancestorCommit == commit, nil
}

func (f *fakeBranchStore) CompareAndSwapBranchRef(_ context.Context, _, branch, expectedCommit, newCommit string) error {
	f.compareAndSwapCalls++
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

var _ serversync.BranchStore = (*fakeBranchStore)(nil)

// --- fixture assembling a single, valid native push. ---

type pushFixture struct {
	engine      *Engine
	branchStore *fakeBranchStore
	indexStore  *fakeIndexStore
	req         PushRequest
	commitID    graphcontract.ObjectID
}

func newPushFixture(t *testing.T) pushFixture {
	t.Helper()

	base := hex.EncodeToString(make([]byte, 32))

	snapshotBytes := validSnapshotBytes(t)
	snapshotID := graphcontract.ObjectIDForEncoded(ObjectTypeSnapshot, snapshotBytes)

	commit, err := graphcontract.NewCommit(snapshotID, []graphcontract.ObjectID{graphcontract.ObjectID(base)}, "alice", "first native commit", time.Now())
	if err != nil {
		t.Fatalf("NewCommit: %v", err)
	}
	commitBytes, err := graphcontract.MarshalCommit(commit)
	if err != nil {
		t.Fatalf("MarshalCommit: %v", err)
	}
	commitID := graphcontract.ObjectIDForEncoded(ObjectTypeCommit, commitBytes)

	pack := buildNativePack(t, testPackID(t.Name()), []testObject{
		{objectType: ObjectTypeSnapshot, data: snapshotBytes},
		{objectType: ObjectTypeCommit, data: commitBytes},
	})

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver: %v", err)
	}
	indexStore := newFakeIndexStore()
	indexer := nativeindex.NewIndexer(driver, indexStore)
	branchStore := &fakeBranchStore{
		branchHeads:  map[string]string{"main": base},
		commitFormat: map[string]uint32{base: CommitFormatNative},
	}
	engine := NewEngine(indexer, branchStore)

	req := PushRequest{
		TenantID: "tenant-a", RepoID: "repo-a", Branch: "main",
		BaseCommit: base, TargetCommit: string(commitID),
		PackID: pack.packID, PackManifest: pack.manifest, PackData: pack.data, PackEntries: pack.entries,
		Commits: []CommitRecord{{ID: commitID, Commit: commit}},
	}

	return pushFixture{engine: engine, branchStore: branchStore, indexStore: indexStore, req: req, commitID: commitID}
}

func TestHandlePushValidPushAdvancesRefExactlyOnce(t *testing.T) {
	t.Parallel()
	fx := newPushFixture(t)

	if err := fx.engine.HandlePush(context.Background(), fx.req); err != nil {
		t.Fatalf("HandlePush() error = %v", err)
	}
	if got := fx.branchStore.branchHeads["main"]; got != string(fx.commitID) {
		t.Fatalf("branch head = %q, want %q", got, fx.commitID)
	}
	if fx.branchStore.compareAndSwapCalls != 1 {
		t.Fatalf("CompareAndSwapBranchRef calls = %d, want 1", fx.branchStore.compareAndSwapCalls)
	}
	if len(fx.branchStore.putCommitCalls) != 1 || fx.branchStore.putCommitCalls[0] != fx.commitID {
		t.Fatalf("PutCommit calls = %v, want exactly [%s]", fx.branchStore.putCommitCalls, fx.commitID)
	}
}

func TestHandlePushRetryAfterSuccessIsRejectedNonFastForward(t *testing.T) {
	t.Parallel()
	fx := newPushFixture(t)
	ctx := context.Background()

	if err := fx.engine.HandlePush(ctx, fx.req); err != nil {
		t.Fatalf("HandlePush() first call error = %v", err)
	}
	head := fx.branchStore.branchHeads["main"]
	casCallsAfterFirst := fx.branchStore.compareAndSwapCalls

	err := fx.engine.HandlePush(ctx, fx.req)
	var nffErr *serversync.NonFastForwardError
	if !errors.As(err, &nffErr) {
		t.Fatalf("HandlePush() retry error = %v, want NonFastForwardError", err)
	}
	if fx.branchStore.branchHeads["main"] != head {
		t.Fatalf("branch head changed on retry: got %q, want unchanged %q", fx.branchStore.branchHeads["main"], head)
	}
	if fx.branchStore.compareAndSwapCalls != casCallsAfterFirst {
		t.Fatalf("CompareAndSwapBranchRef called again on retry: calls = %d, want unchanged %d", fx.branchStore.compareAndSwapCalls, casCallsAfterFirst)
	}
}

func TestHandlePushCorruptPackLeavesRefUnchanged(t *testing.T) {
	t.Parallel()
	fx := newPushFixture(t)

	corrupted := append([]byte(nil), fx.req.PackData...)
	corrupted[len(corrupted)-1] ^= 0xFF
	fx.req.PackData = corrupted

	err := fx.engine.HandlePush(context.Background(), fx.req)
	if err == nil {
		t.Fatal("HandlePush() error = nil, want a rejection for a corrupt pack")
	}
	if fx.branchStore.branchHeads["main"] != fx.req.BaseCommit {
		t.Fatalf("branch head = %q, want unchanged base %q", fx.branchStore.branchHeads["main"], fx.req.BaseCommit)
	}
	if fx.branchStore.getBranchRefCalls != 0 || fx.branchStore.compareAndSwapCalls != 0 || len(fx.branchStore.putCommitCalls) != 0 {
		t.Fatalf("corrupt pack mutated metadata: getBranchRef=%d cas=%d putCommit=%d",
			fx.branchStore.getBranchRefCalls, fx.branchStore.compareAndSwapCalls, len(fx.branchStore.putCommitCalls))
	}
}

func TestHandlePushDisconnectedMissingParentRejected(t *testing.T) {
	t.Parallel()
	fx := newPushFixture(t)

	unknownParent := graphcontract.ObjectID("unknown-parent-0000000000000000000000000000000000000000000000000")
	commit := fx.req.Commits[0].Commit
	commit.Parents = append(commit.Parents, unknownParent)
	commitBytes, err := graphcontract.MarshalCommit(commit)
	if err != nil {
		t.Fatal(err)
	}
	commitID := graphcontract.ObjectIDForEncoded(ObjectTypeCommit, commitBytes)

	snapshotBytes := validSnapshotBytes(t)
	pack := buildNativePack(t, testPackID(t.Name()), []testObject{
		{objectType: ObjectTypeSnapshot, data: snapshotBytes},
		{objectType: ObjectTypeCommit, data: commitBytes},
	})
	fx.req.PackID = pack.packID
	fx.req.PackManifest = pack.manifest
	fx.req.PackData = pack.data
	fx.req.PackEntries = pack.entries
	fx.req.TargetCommit = string(commitID)
	fx.req.Commits = []CommitRecord{{ID: commitID, Commit: commit}}

	err = fx.engine.HandlePush(context.Background(), fx.req)
	if !errors.Is(err, ErrDisconnectedPush) {
		t.Fatalf("HandlePush() error = %v, want ErrDisconnectedPush", err)
	}
	if fx.branchStore.branchHeads["main"] != fx.req.BaseCommit {
		t.Fatalf("branch head = %q, want unchanged base %q", fx.branchStore.branchHeads["main"], fx.req.BaseCommit)
	}
	if len(fx.branchStore.putCommitCalls) != 0 {
		t.Fatalf("PutCommit calls = %d, want 0", len(fx.branchStore.putCommitCalls))
	}
}

func TestHandlePushDisconnectedMissingSnapshotRejected(t *testing.T) {
	t.Parallel()
	fx := newPushFixture(t)

	missingSnapshot := graphcontract.ObjectID("missing-snapshot-000000000000000000000000000000000000000000000000")
	base := fx.req.BaseCommit
	commit, err := graphcontract.NewCommit(missingSnapshot, []graphcontract.ObjectID{graphcontract.ObjectID(base)}, "alice", "dangling snapshot", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	commitBytes, err := graphcontract.MarshalCommit(commit)
	if err != nil {
		t.Fatal(err)
	}
	commitID := graphcontract.ObjectIDForEncoded(ObjectTypeCommit, commitBytes)

	// Only the commit object is packed; its referenced snapshot is absent
	// from both this pack and the (empty) native object index.
	pack := buildNativePack(t, testPackID(t.Name()), []testObject{
		{objectType: ObjectTypeCommit, data: commitBytes},
	})
	fx.req.PackID = pack.packID
	fx.req.PackManifest = pack.manifest
	fx.req.PackData = pack.data
	fx.req.PackEntries = pack.entries
	fx.req.TargetCommit = string(commitID)
	fx.req.Commits = []CommitRecord{{ID: commitID, Commit: commit}}

	err = fx.engine.HandlePush(context.Background(), fx.req)
	if !errors.Is(err, ErrDisconnectedPush) {
		t.Fatalf("HandlePush() error = %v, want ErrDisconnectedPush", err)
	}
	if fx.branchStore.branchHeads["main"] != fx.req.BaseCommit {
		t.Fatalf("branch head = %q, want unchanged base %q", fx.branchStore.branchHeads["main"], fx.req.BaseCommit)
	}
	if len(fx.branchStore.putCommitCalls) != 0 {
		t.Fatalf("PutCommit calls = %d, want 0", len(fx.branchStore.putCommitCalls))
	}
}

func TestHandlePushSchemaInvalidSnapshotRejected(t *testing.T) {
	t.Parallel()
	fx := newPushFixture(t)

	schema := graphcontract.SchemaSnapshot{
		Version: 2,
		NodeRules: []graphcontract.NodeLabelRule{{
			Label: "Task",
			Properties: []graphcontract.PropertyRule{
				{Key: "title", Required: true, Types: []graphcontract.PropertyKind{graphcontract.PropertyString}},
			},
		}},
	}
	normalizedSchema, err := schema.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	node, err := graphcontract.NewNode("task-1", "", []string{"Task"}, map[string]graphcontract.PropertyValue{})
	if err != nil {
		t.Fatal(err)
	}
	invalidSnapshotBytes := buildSnapshotBytes(t, normalizedSchema, map[string]graphcontract.Node{"task-1": node}, map[string]graphcontract.Edge{})
	snapshotID := graphcontract.ObjectIDForEncoded(ObjectTypeSnapshot, invalidSnapshotBytes)

	base := fx.req.BaseCommit
	commit, err := graphcontract.NewCommit(snapshotID, []graphcontract.ObjectID{graphcontract.ObjectID(base)}, "alice", "schema violation", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	commitBytes, err := graphcontract.MarshalCommit(commit)
	if err != nil {
		t.Fatal(err)
	}
	commitID := graphcontract.ObjectIDForEncoded(ObjectTypeCommit, commitBytes)

	pack := buildNativePack(t, testPackID(t.Name()), []testObject{
		{objectType: ObjectTypeSnapshot, data: invalidSnapshotBytes},
		{objectType: ObjectTypeCommit, data: commitBytes},
	})
	fx.req.PackID = pack.packID
	fx.req.PackManifest = pack.manifest
	fx.req.PackData = pack.data
	fx.req.PackEntries = pack.entries
	fx.req.TargetCommit = string(commitID)
	fx.req.Commits = []CommitRecord{{ID: commitID, Commit: commit}}

	err = fx.engine.HandlePush(context.Background(), fx.req)
	if !errors.Is(err, ErrInvalidNativePush) || !errors.Is(err, graphcontract.ErrSchemaValidation) {
		t.Fatalf("HandlePush() error = %v, want ErrInvalidNativePush wrapping ErrSchemaValidation", err)
	}
	if fx.branchStore.branchHeads["main"] != fx.req.BaseCommit {
		t.Fatalf("branch head = %q, want unchanged base %q", fx.branchStore.branchHeads["main"], fx.req.BaseCommit)
	}
	if len(fx.branchStore.putCommitCalls) != 0 {
		t.Fatalf("PutCommit calls = %d, want 0", len(fx.branchStore.putCommitCalls))
	}
}

func TestHandlePushNonFastForwardRejected(t *testing.T) {
	t.Parallel()
	fx := newPushFixture(t)

	otherHead := hex.EncodeToString(bytes.Repeat([]byte{0x11}, 32))
	fx.branchStore.branchHeads["main"] = otherHead
	fx.branchStore.commitFormat[otherHead] = CommitFormatNative

	err := fx.engine.HandlePush(context.Background(), fx.req)
	var nffErr *serversync.NonFastForwardError
	if !errors.As(err, &nffErr) {
		t.Fatalf("HandlePush() error = %v, want NonFastForwardError", err)
	}
	if fx.branchStore.branchHeads["main"] != otherHead {
		t.Fatalf("branch head = %q, want unchanged %q", fx.branchStore.branchHeads["main"], otherHead)
	}
	if len(fx.branchStore.putCommitCalls) != 0 {
		t.Fatalf("PutCommit calls = %d, want 0 (metadata must not be published for a rejected push)", len(fx.branchStore.putCommitCalls))
	}
	if fx.branchStore.compareAndSwapCalls != 0 {
		t.Fatalf("CompareAndSwapBranchRef calls = %d, want 0", fx.branchStore.compareAndSwapCalls)
	}
}

func TestHandlePushBaseCommitWrongFormatRejected(t *testing.T) {
	t.Parallel()
	fx := newPushFixture(t)
	fx.branchStore.commitFormat[fx.req.BaseCommit] = 2 // legacy v2 CBOR framing, not native

	err := fx.engine.HandlePush(context.Background(), fx.req)
	if !errors.Is(err, ErrInvalidNativePush) {
		t.Fatalf("HandlePush() error = %v, want ErrInvalidNativePush", err)
	}
	if len(fx.branchStore.putCommitCalls) != 0 {
		t.Fatalf("PutCommit calls = %d, want 0", len(fx.branchStore.putCommitCalls))
	}
}

func TestHandlePushCommitRecordMismatchRejected(t *testing.T) {
	t.Parallel()
	fx := newPushFixture(t)

	tampered := fx.req.Commits[0].Commit
	tampered.Message = "not what was actually packed"
	fx.req.Commits[0].Commit = tampered

	err := fx.engine.HandlePush(context.Background(), fx.req)
	if !errors.Is(err, ErrInvalidNativePush) {
		t.Fatalf("HandlePush() error = %v, want ErrInvalidNativePush", err)
	}
	if len(fx.branchStore.putCommitCalls) != 0 {
		t.Fatalf("PutCommit calls = %d, want 0", len(fx.branchStore.putCommitCalls))
	}
}
