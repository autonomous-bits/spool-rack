package nativepull

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"testing"

	"github.com/autonomous-bits/spool/graphcontract"
	"github.com/fxamacker/cbor/v2"
	"github.com/klauspost/compress/zstd"
	"lukechampine.com/blake3"
)

// packObjectEnvelope mirrors the unexported envelope shape that
// graphcontract.DecodePackedObjectEnvelope expects: canonical CBOR with
// integer keys 1 (type) and 2 (data).
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
// format) plus the metadata needed to frame it.
type builtPack struct {
	packID   graphcontract.PackID
	manifest graphcontract.PackManifest
	data     []byte
	entries  []graphcontract.PackIndexEntry
}

// buildNativePack constructs a valid native Spool pack byte-for-byte
// following graphcontract/pack.go's format, mirroring the equivalent helper
// in nativeindex_test.go: a 12-byte big-endian header (magic "IDGP",
// version 2, object count), followed by each object's zstd-compressed
// canonical CBOR envelope, referenced by a caller-usable index of
// PackIndexEntry values.
func buildNativePack(t *testing.T, packID graphcontract.PackID, objects []testObject) builtPack {
	t.Helper()

	entries := make([]graphcontract.PackIndexEntry, len(objects))
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
		offset += uint64(len(compressed))
		body.Write(compressed)
	}

	var header [graphcontract.PackHeaderSize]byte
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

	return builtPack{packID: packID, manifest: manifest, data: packData, entries: entries}
}

// testPackID returns a well-formed, unique 32-character lowercase hex pack
// ID derived from name (graphcontract.ValidPackID requires exactly 32 hex
// characters).
func testPackID(name string) graphcontract.PackID {
	sum := blake3.Sum256([]byte(name))
	return graphcontract.PackID(hex.EncodeToString(sum[:16]))
}

// testCommitID returns a well-formed, unique 64-character lowercase hex
// commit ID derived from name.
func testCommitID(name string) string {
	sum := blake3.Sum256([]byte("commit:" + name))
	return hex.EncodeToString(sum[:])
}

func twoPackSources(t *testing.T) ([]NativePackSource, string) {
	t.Helper()
	pack1 := buildNativePack(t, testPackID("pack-1"), []testObject{
		{objectType: "node", data: []byte("alpha")},
		{objectType: "node", data: []byte("beta")},
	})
	pack2 := buildNativePack(t, testPackID("pack-2"), []testObject{
		{objectType: "edge", data: []byte("gamma")},
	})
	commit1 := testCommitID("commit-1")
	commit2 := testCommitID("commit-2")
	sources := []NativePackSource{
		{PackID: string(pack1.packID), CommitID: commit1, Manifest: pack1.manifest, Entries: pack1.entries, PackData: pack1.data},
		{PackID: string(pack2.packID), CommitID: commit2, Manifest: pack2.manifest, Entries: pack2.entries, PackData: pack2.data},
	}
	return sources, commit2
}

func TestEncodeDecodeNativePullResponse_RoundTrip(t *testing.T) {
	sources, head := twoPackSources(t)

	data, err := EncodeNativePullResponse(head, sources)
	if err != nil {
		t.Fatalf("EncodeNativePullResponse: %v", err)
	}

	decodedHead, packs, err := DecodeNativePullResponse(data)
	if err != nil {
		t.Fatalf("DecodeNativePullResponse: %v", err)
	}
	if decodedHead != head {
		t.Fatalf("head = %q, want %q", decodedHead, head)
	}
	if len(packs) != len(sources) {
		t.Fatalf("got %d packs, want %d", len(packs), len(sources))
	}
	for i, src := range sources {
		got := packs[i]
		if string(got.PackID) != src.PackID {
			t.Errorf("pack %d: PackID = %q, want %q", i, got.PackID, src.PackID)
		}
		if got.CommitID != src.CommitID {
			t.Errorf("pack %d: CommitID = %q, want %q", i, got.CommitID, src.CommitID)
		}
		if !bytes.Equal(got.Data, src.PackData) {
			t.Errorf("pack %d: Data mismatch (boundary not preserved)", i)
		}
		if len(got.Entries) != len(src.Entries) {
			t.Errorf("pack %d: got %d index entries, want %d", i, len(got.Entries), len(src.Entries))
		}
		for j, entry := range src.Entries {
			if got.Entries[j] != entry {
				t.Errorf("pack %d entry %d: got %+v, want %+v", i, j, got.Entries[j], entry)
			}
		}
		if len(got.Manifest.Packs) != 1 || got.Manifest.Packs[0].ID != src.Manifest.Packs[0].ID {
			t.Errorf("pack %d: manifest identity not preserved: got %+v", i, got.Manifest)
		}
	}
}

func TestEncodeNativePullResponse_HeadMustMatchTerminalCommit(t *testing.T) {
	sources, _ := twoPackSources(t)
	if _, err := EncodeNativePullResponse(testCommitID("not-the-head"), sources); !errors.Is(err, ErrInvalidNativePullFrame) {
		t.Fatalf("expected ErrInvalidNativePullFrame for mismatched head, got %v", err)
	}
}

func TestDecodeNativePullResponse_RejectsTruncatedHeader(t *testing.T) {
	_, _, err := DecodeNativePullResponse([]byte("short"))
	if !errors.Is(err, ErrInvalidNativePullFrame) {
		t.Fatalf("expected ErrInvalidNativePullFrame for truncated header, got %v", err)
	}
}

func TestDecodeNativePullResponse_RejectsBadMagic(t *testing.T) {
	sources, head := twoPackSources(t)
	data, err := EncodeNativePullResponse(head, sources)
	if err != nil {
		t.Fatalf("EncodeNativePullResponse: %v", err)
	}
	tampered := append([]byte(nil), data...)
	copy(tampered[:4], "XXXX")
	if _, _, err := DecodeNativePullResponse(tampered); !errors.Is(err, ErrInvalidNativePullFrame) {
		t.Fatalf("expected ErrInvalidNativePullFrame for bad magic, got %v", err)
	}
}

func TestDecodeNativePullResponse_RejectsUnsupportedVersion(t *testing.T) {
	sources, head := twoPackSources(t)
	data, err := EncodeNativePullResponse(head, sources)
	if err != nil {
		t.Fatalf("EncodeNativePullResponse: %v", err)
	}
	tampered := append([]byte(nil), data...)
	binary.BigEndian.PutUint32(tampered[4:8], 99)
	if _, _, err := DecodeNativePullResponse(tampered); !errors.Is(err, ErrInvalidNativePullFrame) {
		t.Fatalf("expected ErrInvalidNativePullFrame for unsupported version, got %v", err)
	}
}

func TestDecodeNativePullResponse_RejectsManifestLengthOverflow(t *testing.T) {
	sources, head := twoPackSources(t)
	data, err := EncodeNativePullResponse(head, sources)
	if err != nil {
		t.Fatalf("EncodeNativePullResponse: %v", err)
	}
	tampered := append([]byte(nil), data...)
	binary.BigEndian.PutUint64(tampered[8:nativePullHeaderSize], uint64(len(tampered)*10))
	if _, _, err := DecodeNativePullResponse(tampered); !errors.Is(err, ErrInvalidNativePullFrame) {
		t.Fatalf("expected ErrInvalidNativePullFrame for manifest length overflow, got %v", err)
	}
}

func TestDecodeNativePullResponse_RejectsTruncatedPackStream(t *testing.T) {
	sources, head := twoPackSources(t)
	data, err := EncodeNativePullResponse(head, sources)
	if err != nil {
		t.Fatalf("EncodeNativePullResponse: %v", err)
	}
	// Drop the final pack's trailing bytes so its declared length exceeds
	// what remains in the stream.
	truncated := data[:len(data)-5]
	if _, _, err := DecodeNativePullResponse(truncated); !errors.Is(err, ErrInvalidNativePullFrame) {
		t.Fatalf("expected ErrInvalidNativePullFrame for truncated pack stream, got %v", err)
	}
}

func TestDecodeNativePullResponse_RejectsTrailingBytes(t *testing.T) {
	sources, head := twoPackSources(t)
	data, err := EncodeNativePullResponse(head, sources)
	if err != nil {
		t.Fatalf("EncodeNativePullResponse: %v", err)
	}
	withTrailer := append(append([]byte(nil), data...), 0x01, 0x02, 0x03)
	if _, _, err := DecodeNativePullResponse(withTrailer); !errors.Is(err, ErrInvalidNativePullFrame) {
		t.Fatalf("expected ErrInvalidNativePullFrame for trailing bytes, got %v", err)
	}
}

func TestDecodeNativePullResponse_RejectsPackHashMismatch(t *testing.T) {
	sources, head := twoPackSources(t)
	data, err := EncodeNativePullResponse(head, sources)
	if err != nil {
		t.Fatalf("EncodeNativePullResponse: %v", err)
	}
	// Flip a byte inside the first pack's raw data (well after the header
	// and manifest) without changing any declared length, so the pack
	// content-hash check must be what catches the tamper.
	tampered := append([]byte(nil), data...)
	tampered[len(tampered)-1] ^= 0xFF
	if _, _, err := DecodeNativePullResponse(tampered); !errors.Is(err, ErrInvalidNativePullFrame) {
		t.Fatalf("expected ErrInvalidNativePullFrame for pack hash mismatch, got %v", err)
	}
}

func TestDecodeNativePullResponse_RejectsNonCanonicalManifest(t *testing.T) {
	sources, head := twoPackSources(t)
	manifest := NativePullManifestV1{
		Version: NativePullFormatV1,
		Head:    head,
	}
	for _, src := range sources {
		manifest.Packs = append(manifest.Packs, NativePackFrame{
			PackID:   src.PackID,
			CommitID: src.CommitID,
			PackHash: contentHash(src.PackData),
			Length:   uint64(len(src.PackData)),
			Manifest: src.Manifest,
			Entries:  src.Entries,
		})
	}
	canonical, err := MarshalNativePullManifestV1(manifest)
	if err != nil {
		t.Fatalf("MarshalNativePullManifestV1: %v", err)
	}
	// Re-encode with the non-canonical (default) CBOR options so map/struct
	// key ordering may differ from the canonical form, simulating a
	// tampered-but-still-decodable manifest.
	nonCanonical, err := cbor.Marshal(manifest)
	if err != nil {
		t.Fatalf("cbor.Marshal: %v", err)
	}
	if bytes.Equal(canonical, nonCanonical) {
		t.Skip("default and canonical encodings coincidentally matched; cannot exercise non-canonical rejection")
	}
	if _, err := UnmarshalNativePullManifestV1(nonCanonical); !errors.Is(err, ErrInvalidNativePullFrame) {
		t.Fatalf("expected ErrInvalidNativePullFrame for non-canonical manifest, got %v", err)
	}
}

func TestNativePullManifestV1Validate_RejectsEmptyPacks(t *testing.T) {
	manifest := NativePullManifestV1{Version: NativePullFormatV1, Head: testCommitID("head")}
	if _, err := MarshalNativePullManifestV1(manifest); !errors.Is(err, ErrInvalidNativePullFrame) {
		t.Fatalf("expected ErrInvalidNativePullFrame for empty packs, got %v", err)
	}
}

func TestNativePullManifestV1Validate_RejectsDuplicatePackID(t *testing.T) {
	sources, _ := twoPackSources(t)
	pack := buildNativePack(t, testPackID("pack-1"), []testObject{{objectType: "node", data: []byte("delta")}})
	dupHead := testCommitID("dup-head")
	manifest := NativePullManifestV1{
		Version: NativePullFormatV1,
		Head:    dupHead,
		Packs: []NativePackFrame{
			{PackID: sources[0].PackID, CommitID: sources[0].CommitID, PackHash: contentHash(sources[0].PackData), Length: uint64(len(sources[0].PackData)), Manifest: sources[0].Manifest, Entries: sources[0].Entries},
			{PackID: sources[0].PackID, CommitID: dupHead, PackHash: contentHash(pack.data), Length: uint64(len(pack.data)), Manifest: pack.manifest, Entries: pack.entries},
		},
	}
	if _, err := MarshalNativePullManifestV1(manifest); !errors.Is(err, ErrInvalidNativePullFrame) {
		t.Fatalf("expected ErrInvalidNativePullFrame for duplicate pack ID, got %v", err)
	}
}

func TestEncodeNativePullResponse_RejectsManifestEntryMismatch(t *testing.T) {
	pack := buildNativePack(t, testPackID("mismatch"), []testObject{
		{objectType: "node", data: []byte("alpha")},
		{objectType: "node", data: []byte("beta")},
	})
	src := NativePackSource{
		PackID:   string(pack.packID),
		CommitID: testCommitID("mismatch"),
		Manifest: pack.manifest,
		Entries:  pack.entries[:1], // drop one entry so it disagrees with the pack header's object count
		PackData: pack.data,
	}
	if _, err := EncodeNativePullResponse(src.CommitID, []NativePackSource{src}); !errors.Is(err, ErrInvalidNativePullFrame) {
		t.Fatalf("expected ErrInvalidNativePullFrame for entry/header mismatch, got %v", err)
	}
}
