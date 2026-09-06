package nativeindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"sync"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	"github.com/autonomous-bits/spool/graphcontract"
	"github.com/fxamacker/cbor/v2"
	"github.com/klauspost/compress/zstd"
	"lukechampine.com/blake3"
)

// packObjectEnvelope mirrors the unexported envelope shape that
// graphcontract.DecodePackedObjectEnvelope expects: canonical CBOR with
// integer keys 1 (type) and 2 (data). CBOR encoding only depends on these
// tags, not the Go type name, so this local copy round-trips identically.
type packObjectEnvelope struct {
	Type string `cbor:"1,keyasint"`
	Data []byte `cbor:"2,keyasint"`
}

var testCanonicalCBOR, _ = cbor.CanonicalEncOptions().EncMode()

// testObject describes one object to embed in a hand-built native pack.
type testObject struct {
	objectType string
	data       []byte
}

// builtPack is a from-scratch, fully valid native pack (per graphcontract's
// format) plus the metadata needed to index it.
type builtPack struct {
	packID   graphcontract.PackID
	manifest graphcontract.PackManifest
	data     []byte
	entries  []graphcontract.PackIndexEntry
	objectID []graphcontract.ObjectID // in the same order as objects passed in
}

// buildNativePack constructs a valid native Spool pack byte-for-byte
// following graphcontract/pack.go's format: a 12-byte big-endian header
// (magic "IDGP", version 2, object count), followed by each object's
// zstd-compressed canonical CBOR envelope, referenced by a caller-usable
// index of PackIndexEntry values.
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

// testPackID returns a well-formed, unique 32-character lowercase hex pack
// ID derived from name.
func testPackID(name string) graphcontract.PackID {
	sum := blake3.Sum256([]byte(name))
	return graphcontract.PackID(hex.EncodeToString(sum[:16]))
}

type tenantContextKey struct{}

// fakeStore is a minimal in-memory Store used to unit test Indexer without a
// real PostgreSQL instance. It enforces the same tenant partitioning
// contract as postgres.PGStore: objects are keyed by (tenantID, repoID,
// objectID), so a lookup under the wrong tenant is indistinguishable from an
// absent object.
type fakeStore struct {
	mu      sync.Mutex
	packs   map[string]fakePack
	objects map[string]postgres.NativeObjectLocation
}

type fakePack struct {
	casPackHash string
	commitID    string
	objectCount int
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		packs:   make(map[string]fakePack),
		objects: make(map[string]postgres.NativeObjectLocation),
	}
}

func (f *fakeStore) SetTenantContext(ctx context.Context, tenantID string) (context.Context, error) {
	if tenantID == "" {
		return nil, errors.New("fakeStore: empty tenant ID")
	}
	return context.WithValue(ctx, tenantContextKey{}, tenantID), nil
}

func (f *fakeStore) PutNativePack(ctx context.Context, repoID, packID, casPackHash, commitID string, entries []graphcontract.PackIndexEntry) error {
	tenantID, ok := ctx.Value(tenantContextKey{}).(string)
	if !ok || tenantID == "" {
		return errors.New("fakeStore: missing tenant context")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	packKey := tenantID + "|" + repoID + "|" + packID
	if existing, ok := f.packs[packKey]; ok {
		if existing.casPackHash != casPackHash || existing.commitID != commitID || existing.objectCount != len(entries) {
			return postgres.ErrImmutableMetadataMismatch
		}
		return nil
	}
	f.packs[packKey] = fakePack{casPackHash: casPackHash, commitID: commitID, objectCount: len(entries)}

	for _, entry := range entries {
		objectKey := tenantID + "|" + repoID + "|" + string(entry.Object)
		if _, exists := f.objects[objectKey]; exists {
			continue
		}
		f.objects[objectKey] = postgres.NativeObjectLocation{
			PackID:           packID,
			CASPackHash:      casPackHash,
			Offset:           entry.Offset,
			CompressedSize:   entry.CompressedSize,
			UncompressedSize: entry.UncompressedSize,
			CRC32:            entry.CRC32,
		}
	}
	return nil
}

func (f *fakeStore) GetNativeObjectLocation(ctx context.Context, repoID, objectID string) (postgres.NativeObjectLocation, error) {
	tenantID, ok := ctx.Value(tenantContextKey{}).(string)
	if !ok || tenantID == "" {
		return postgres.NativeObjectLocation{}, errors.New("fakeStore: missing tenant context")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	loc, ok := f.objects[tenantID+"|"+repoID+"|"+objectID]
	if !ok {
		return postgres.NativeObjectLocation{}, postgres.ErrNativeObjectNotFound
	}
	return loc, nil
}

func (f *fakeStore) objectCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.objects)
}

func newTestIndexer(t *testing.T) (*Indexer, *fakeStore, cas.Driver) {
	t.Helper()
	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver: %v", err)
	}
	store := newFakeStore()
	return NewIndexer(driver, store), store, driver
}

func TestIndexPack_RoundTrip(t *testing.T) {
	idx, _, _ := newTestIndexer(t)
	ctx := context.Background()

	pack := buildNativePack(t, testPackID(t.Name()), []testObject{
		{objectType: "blob", data: []byte("hello world")},
		{objectType: "tree", data: []byte("tree-entries")},
		{objectType: "blob", data: []byte("goodbye world")},
	})

	if err := idx.IndexPack(ctx, "tenant-a", "repo-1", pack.packID, pack.manifest, pack.data, pack.entries, ""); err != nil {
		t.Fatalf("IndexPack: %v", err)
	}

	for i, id := range pack.objectID {
		gotType, gotData, err := idx.ResolveObject(ctx, "tenant-a", "repo-1", id)
		if err != nil {
			t.Fatalf("ResolveObject(%s): %v", id, err)
		}
		want := []testObject{
			{objectType: "blob", data: []byte("hello world")},
			{objectType: "tree", data: []byte("tree-entries")},
			{objectType: "blob", data: []byte("goodbye world")},
		}[i]
		if gotType != want.objectType || !bytes.Equal(gotData, want.data) {
			t.Fatalf("ResolveObject(%s) = (%q, %q), want (%q, %q)", id, gotType, gotData, want.objectType, want.data)
		}
	}
}

func TestIndexPack_RejectsBadMagic(t *testing.T) {
	idx, store, _ := newTestIndexer(t)
	ctx := context.Background()

	pack := buildNativePack(t, testPackID(t.Name()), []testObject{{objectType: "blob", data: []byte("data")}})
	pack.data[0] = 'X'

	err := idx.IndexPack(ctx, "tenant-a", "repo-1", pack.packID, pack.manifest, pack.data, pack.entries, "")
	if !errors.Is(err, ErrInvalidPack) {
		t.Fatalf("IndexPack(bad magic) = %v, want ErrInvalidPack", err)
	}
	if store.objectCount() != 0 {
		t.Fatalf("IndexPack(bad magic) indexed %d objects, want 0", store.objectCount())
	}
}

func TestIndexPack_RejectsUnsupportedVersion(t *testing.T) {
	idx, store, _ := newTestIndexer(t)
	ctx := context.Background()

	pack := buildNativePack(t, testPackID(t.Name()), []testObject{{objectType: "blob", data: []byte("data")}})
	binary.BigEndian.PutUint32(pack.data[4:8], 99)

	err := idx.IndexPack(ctx, "tenant-a", "repo-1", pack.packID, pack.manifest, pack.data, pack.entries, "")
	if !errors.Is(err, ErrInvalidPack) {
		t.Fatalf("IndexPack(bad version) = %v, want ErrInvalidPack", err)
	}
	if store.objectCount() != 0 {
		t.Fatalf("IndexPack(bad version) indexed %d objects, want 0", store.objectCount())
	}
}

func TestIndexPack_RejectsTamperedObjectBytes(t *testing.T) {
	idx, store, driver := newTestIndexer(t)
	ctx := context.Background()

	pack := buildNativePack(t, testPackID(t.Name()), []testObject{
		{objectType: "blob", data: []byte("hello world")},
		{objectType: "blob", data: []byte("a second object")},
	})
	// Flip a byte inside the first entry's compressed region so its CRC32
	// (and ultimately its decompressed content hash) no longer verifies.
	firstEntry := pack.entries[0]
	pack.data[firstEntry.Offset] ^= 0xFF

	err := idx.IndexPack(ctx, "tenant-a", "repo-1", pack.packID, pack.manifest, pack.data, pack.entries, "")
	if !errors.Is(err, ErrInvalidPack) {
		t.Fatalf("IndexPack(tampered object) = %v, want ErrInvalidPack", err)
	}
	if store.objectCount() != 0 {
		t.Fatalf("IndexPack(tampered object) indexed %d objects, want 0", store.objectCount())
	}
	scope, err := cas.NewScope("tenant-a", "repo-1")
	if err != nil {
		t.Fatalf("NewScope: %v", err)
	}
	casHash := contentHash(pack.data)
	if exists, _ := driver.Exists(ctx, scope, casHash); exists {
		t.Fatal("IndexPack(tampered object) unexpectedly wrote an object to CAS")
	}
}

func TestIndexPack_RejectsManifestObjectCountMismatch(t *testing.T) {
	idx, store, _ := newTestIndexer(t)
	ctx := context.Background()

	pack := buildNativePack(t, testPackID(t.Name()), []testObject{{objectType: "blob", data: []byte("data")}})
	pack.manifest.Packs[0].ObjectCount = 42

	err := idx.IndexPack(ctx, "tenant-a", "repo-1", pack.packID, pack.manifest, pack.data, pack.entries, "")
	if !errors.Is(err, ErrInvalidPack) {
		t.Fatalf("IndexPack(manifest mismatch) = %v, want ErrInvalidPack", err)
	}
	if store.objectCount() != 0 {
		t.Fatalf("IndexPack(manifest mismatch) indexed %d objects, want 0", store.objectCount())
	}
}

func TestIndexPack_RejectsManifestMissingPack(t *testing.T) {
	idx, _, _ := newTestIndexer(t)
	ctx := context.Background()

	pack := buildNativePack(t, testPackID(t.Name()), []testObject{{objectType: "blob", data: []byte("data")}})
	pack.manifest.Packs[0].ID = testPackID(t.Name() + "-different")

	if err := idx.IndexPack(ctx, "tenant-a", "repo-1", pack.packID, pack.manifest, pack.data, pack.entries, ""); !errors.Is(err, ErrInvalidPack) {
		t.Fatalf("IndexPack(manifest missing pack) = %v, want ErrInvalidPack", err)
	}
}

func TestIndexPack_RejectsInvalidPackID(t *testing.T) {
	idx, _, _ := newTestIndexer(t)
	ctx := context.Background()

	pack := buildNativePack(t, testPackID(t.Name()), []testObject{{objectType: "blob", data: []byte("data")}})

	if err := idx.IndexPack(ctx, "tenant-a", "repo-1", "not-a-valid-pack-id", pack.manifest, pack.data, pack.entries, ""); !errors.Is(err, ErrInvalidPack) {
		t.Fatalf("IndexPack(invalid pack ID) = %v, want ErrInvalidPack", err)
	}
}

func TestIndexPack_RejectsEntryCountMismatch(t *testing.T) {
	idx, store, _ := newTestIndexer(t)
	ctx := context.Background()

	pack := buildNativePack(t, testPackID(t.Name()), []testObject{
		{objectType: "blob", data: []byte("one")},
		{objectType: "blob", data: []byte("two")},
	})
	// Header declares 2 objects but only 1 entry is supplied: the entries no
	// longer cover the pack contiguously, so ValidatePackEntries must reject
	// this before any object is trusted.
	truncatedEntries := pack.entries[:1]

	err := idx.IndexPack(ctx, "tenant-a", "repo-1", pack.packID, pack.manifest, pack.data, truncatedEntries, "")
	if !errors.Is(err, ErrInvalidPack) {
		t.Fatalf("IndexPack(entry count mismatch) = %v, want ErrInvalidPack", err)
	}
	if store.objectCount() != 0 {
		t.Fatalf("IndexPack(entry count mismatch) indexed %d objects, want 0", store.objectCount())
	}
}

func TestResolveObject_NotFound(t *testing.T) {
	idx, _, _ := newTestIndexer(t)
	ctx := context.Background()

	_, _, err := idx.ResolveObject(ctx, "tenant-a", "repo-1", graphcontract.ObjectID(hex.EncodeToString(make([]byte, 32))))
	if !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("ResolveObject(absent) = %v, want ErrObjectNotFound", err)
	}
}

func TestResolveObject_RejectsCrossTenantLookup(t *testing.T) {
	idx, _, _ := newTestIndexer(t)
	ctx := context.Background()

	pack := buildNativePack(t, testPackID(t.Name()), []testObject{{objectType: "blob", data: []byte("tenant-a-secret")}})
	if err := idx.IndexPack(ctx, "tenant-a", "repo-1", pack.packID, pack.manifest, pack.data, pack.entries, ""); err != nil {
		t.Fatalf("IndexPack: %v", err)
	}

	// A different tenant querying the same repo ID and object ID must see it
	// as absent, not merely denied.
	if _, _, err := idx.ResolveObject(ctx, "tenant-b", "repo-1", pack.objectID[0]); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("ResolveObject(foreign tenant) = %v, want ErrObjectNotFound", err)
	}

	// Sanity check: the owning tenant can still resolve it.
	gotType, gotData, err := idx.ResolveObject(ctx, "tenant-a", "repo-1", pack.objectID[0])
	if err != nil || gotType != "blob" || !bytes.Equal(gotData, []byte("tenant-a-secret")) {
		t.Fatalf("ResolveObject(owning tenant) = (%q, %q, %v), want (\"blob\", \"tenant-a-secret\", nil)", gotType, gotData, err)
	}
}
