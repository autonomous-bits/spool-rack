// Package nativepull defines the versioned wire framing for a Rack pull
// response that carries multiple native Spool packs (the binary pack
// container format defined by graphcontract/pack.go and indexed by
// internal/server/nativeindex) in a single stream.
//
// This is distinct from, and does not modify, Rack's existing CBOR-framed
// PackFrameV2 push/pull envelope in the sync package: that framing carries
// Rack's own commit/object CBOR frames, while this framing carries opaque
// native pack binaries plus the per-pack index material (a
// graphcontract.PackManifest and its graphcontract.PackIndexEntry list) a
// receiver needs to verify and resolve each pack's objects — for example by
// handing a decoded pack straight to nativeindex.Indexer.IndexPack.
//
// The package is intentionally storage-agnostic: it only encodes and
// decodes in-memory byte slices and structs, with no CAS or PostgreSQL
// dependency and no transfer-budget policy, so it can serve as a stable
// contract for both the engine that assembles pull responses from
// already-indexed packs and the client that installs them.
package nativepull

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/autonomous-bits/spool-rack/internal/server/nativeindex"
	"github.com/autonomous-bits/spool/graphcontract"
	"github.com/fxamacker/cbor/v2"
	"lukechampine.com/blake3"
)

const (
	// NativePullFormatV1 identifies the first versioned Rack native
	// multi-pack pull response framing.
	NativePullFormatV1 uint32 = 1

	nativePullMagic      = "SRNP"
	nativePullHeaderSize = 16 // magic(4) + version(4) + manifest length(8)

	// MaxNativePullPacks bounds how many packs a single pull response
	// manifest may declare, so a hostile or corrupt manifest cannot force
	// unbounded allocation before any pack bytes have even been read.
	MaxNativePullPacks = 4096

	// MaxNativePackBytes bounds every individual native pack carried by a
	// pull response, matching nativeindex's own limit so framing and
	// indexing agree on the largest pack either will ever accept.
	MaxNativePackBytes = nativeindex.MaxNativePackBytes
)

// ErrInvalidNativePullFrame indicates malformed, inconsistent, tampered, or
// unsupported native multi-pack pull framing.
var ErrInvalidNativePullFrame = errors.New("nativepull: invalid native pull frame")

var (
	canonicalCBOR, _ = cbor.CanonicalEncOptions().EncMode()
	strictCBOR, _    = cbor.DecOptions{
		DupMapKey:         cbor.DupMapKeyEnforcedAPF,
		IndefLength:       cbor.IndefLengthForbidden,
		TagsMd:            cbor.TagsForbidden,
		ExtraReturnErrors: cbor.ExtraDecErrorUnknownField,
	}.DecMode()
)

// NativePackFrame identifies and describes one native pack within a
// multi-pack pull response manifest: PackID and CommitID give the pack's
// identity, PackHash and Length bound its raw bytes within the envelope
// stream, and Manifest/Entries carry the index material needed to verify
// and resolve its objects once deframed.
type NativePackFrame struct {
	PackID   string                         `json:"packId" cbor:"1,keyasint"`
	CommitID string                         `json:"commitId" cbor:"2,keyasint"`
	PackHash string                         `json:"packHash" cbor:"3,keyasint"`
	Length   uint64                         `json:"length" cbor:"4,keyasint"`
	Manifest graphcontract.PackManifest     `json:"manifest" cbor:"5,keyasint"`
	Entries  []graphcontract.PackIndexEntry `json:"entries" cbor:"6,keyasint"`
}

// NativePullManifestV1 is the canonical manifest at the start of every
// version 1 native multi-pack pull envelope. Packs are ordered
// oldest-to-newest; Head names the commit the response advances the caller
// to and must equal the terminal pack's CommitID.
type NativePullManifestV1 struct {
	Version uint32            `json:"version" cbor:"1,keyasint"`
	Head    string            `json:"head" cbor:"2,keyasint"`
	Packs   []NativePackFrame `json:"packs" cbor:"3,keyasint"`
}

// MarshalNativePullManifestV1 validates and returns the canonical encoding
// of manifest.
func MarshalNativePullManifestV1(manifest NativePullManifestV1) ([]byte, error) {
	if err := manifest.validate(); err != nil {
		return nil, err
	}
	data, err := canonicalCBOR.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("%w: encode manifest: %v", ErrInvalidNativePullFrame, err)
	}
	return data, nil
}

// UnmarshalNativePullManifestV1 verifies that data is a canonical,
// self-consistent v1 native pull manifest.
func UnmarshalNativePullManifestV1(data []byte) (NativePullManifestV1, error) {
	var manifest NativePullManifestV1
	if err := strictCBOR.Unmarshal(data, &manifest); err != nil {
		return NativePullManifestV1{}, fmt.Errorf("%w: decode manifest: %v", ErrInvalidNativePullFrame, err)
	}
	canonical, err := MarshalNativePullManifestV1(manifest)
	if err != nil {
		return NativePullManifestV1{}, err
	}
	if !bytes.Equal(data, canonical) {
		return NativePullManifestV1{}, fmt.Errorf("%w: manifest is not canonically encoded", ErrInvalidNativePullFrame)
	}
	return manifest, nil
}

func (m NativePullManifestV1) validate() error {
	if m.Version != NativePullFormatV1 {
		return fmt.Errorf("%w: unsupported manifest version %d", ErrInvalidNativePullFrame, m.Version)
	}
	if len(m.Packs) == 0 {
		return fmt.Errorf("%w: at least one pack is required", ErrInvalidNativePullFrame)
	}
	if len(m.Packs) > MaxNativePullPacks {
		return fmt.Errorf("%w: manifest declares %d packs, exceeding the %d limit", ErrInvalidNativePullFrame, len(m.Packs), MaxNativePullPacks)
	}
	if !validContentID(m.Head) {
		return fmt.Errorf("%w: head must be a valid BLAKE3-256 content ID", ErrInvalidNativePullFrame)
	}
	seenPacks := make(map[string]struct{}, len(m.Packs))
	seenCommits := make(map[string]struct{}, len(m.Packs))
	for i, pack := range m.Packs {
		if err := pack.validate(); err != nil {
			return fmt.Errorf("%w: pack %d: %v", ErrInvalidNativePullFrame, i, err)
		}
		if _, dup := seenPacks[pack.PackID]; dup {
			return fmt.Errorf("%w: pack ID %q appears more than once", ErrInvalidNativePullFrame, pack.PackID)
		}
		seenPacks[pack.PackID] = struct{}{}
		if _, dup := seenCommits[pack.CommitID]; dup {
			return fmt.Errorf("%w: commit %q appears in multiple packs", ErrInvalidNativePullFrame, pack.CommitID)
		}
		seenCommits[pack.CommitID] = struct{}{}
	}
	if m.Packs[len(m.Packs)-1].CommitID != m.Head {
		return fmt.Errorf("%w: head %s is not the terminal pack's commit", ErrInvalidNativePullFrame, m.Head)
	}
	return nil
}

func (f NativePackFrame) validate() error {
	if !graphcontract.ValidPackID(graphcontract.PackID(f.PackID)) {
		return fmt.Errorf("invalid pack ID %q", f.PackID)
	}
	if !validContentID(f.PackHash) {
		return fmt.Errorf("pack %s: invalid pack hash", f.PackID)
	}
	if !validContentID(f.CommitID) {
		return fmt.Errorf("pack %s: invalid commit ID", f.PackID)
	}
	if f.Length == 0 {
		return fmt.Errorf("pack %s: length must be nonzero", f.PackID)
	}
	if f.Length > uint64(MaxNativePackBytes) {
		return fmt.Errorf("pack %s: length %d exceeds %d byte limit", f.PackID, f.Length, MaxNativePackBytes)
	}
	if err := graphcontract.ValidatePackManifest(f.Manifest); err != nil {
		return fmt.Errorf("pack %s: %v", f.PackID, err)
	}
	if err := manifestListsPack(f.Manifest, graphcontract.PackID(f.PackID), len(f.Entries)); err != nil {
		return fmt.Errorf("pack %s: %v", f.PackID, err)
	}
	seen := make(map[graphcontract.ObjectID]struct{}, len(f.Entries))
	for i, entry := range f.Entries {
		if err := graphcontract.ValidatePackIndexEntry(graphcontract.PackID(f.PackID), entry); err != nil {
			return fmt.Errorf("pack %s: entry %d: %v", f.PackID, i, err)
		}
		if _, dup := seen[entry.Object]; dup {
			return fmt.Errorf("pack %s: duplicate object %s in index", f.PackID, entry.Object)
		}
		seen[entry.Object] = struct{}{}
	}
	return nil
}

// manifestListsPack requires that manifest declare exactly packID with a
// matching object count and the required zstd compression, mirroring
// nativeindex's own manifest/pack agreement check so a frame cannot claim
// index material that disagrees with its own declared manifest metadata.
func manifestListsPack(manifest graphcontract.PackManifest, packID graphcontract.PackID, objectCount int) error {
	for _, metadata := range manifest.Packs {
		if metadata.ID != packID {
			continue
		}
		if metadata.ObjectCount != uint32(objectCount) {
			return fmt.Errorf("manifest object count %d does not match %d supplied entries", metadata.ObjectCount, objectCount)
		}
		if metadata.Compression != graphcontract.PackCompressionZstd {
			return fmt.Errorf("manifest declares unsupported compression %q for pack %s", metadata.Compression, packID)
		}
		return nil
	}
	return fmt.Errorf("manifest does not list pack %s", packID)
}

// NativePackSource supplies one pack's identity, index material, and raw
// bytes to EncodeNativePullResponse. PackHash and Length are always derived
// from PackData so a caller cannot desynchronize the declared framing from
// the bytes actually served.
type NativePackSource struct {
	PackID   string
	CommitID string
	Manifest graphcontract.PackManifest
	Entries  []graphcontract.PackIndexEntry
	PackData []byte
}

// EncodeNativePullResponse builds a complete, versioned native multi-pack
// pull envelope: a header, a canonical NativePullManifestV1 derived from
// sources, and each source's raw pack bytes concatenated in order. head
// must equal the final source's CommitID.
func EncodeNativePullResponse(head string, sources []NativePackSource) ([]byte, error) {
	manifest := NativePullManifestV1{
		Version: NativePullFormatV1,
		Head:    head,
		Packs:   make([]NativePackFrame, len(sources)),
	}
	packs := make([][]byte, len(sources))
	for i, src := range sources {
		if int64(len(src.PackData)) > MaxNativePackBytes {
			return nil, fmt.Errorf("%w: pack %s exceeds %d byte limit", ErrInvalidNativePullFrame, src.PackID, MaxNativePackBytes)
		}
		if err := validateNativePackBytes(graphcontract.PackID(src.PackID), src.PackData, len(src.Entries)); err != nil {
			return nil, err
		}
		manifest.Packs[i] = NativePackFrame{
			PackID:   src.PackID,
			CommitID: src.CommitID,
			PackHash: contentHash(src.PackData),
			Length:   uint64(len(src.PackData)),
			Manifest: src.Manifest,
			Entries:  src.Entries,
		}
		packs[i] = src.PackData
	}
	manifestData, err := MarshalNativePullManifestV1(manifest)
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer
	var header [nativePullHeaderSize]byte
	copy(header[:4], nativePullMagic)
	binary.BigEndian.PutUint32(header[4:8], NativePullFormatV1)
	binary.BigEndian.PutUint64(header[8:], uint64(len(manifestData)))
	out.Write(header[:])
	out.Write(manifestData)
	for _, pack := range packs {
		out.Write(pack)
	}
	return out.Bytes(), nil
}

// DecodedNativePack is one pack recovered from a native pull envelope,
// with fields that map 1:1 onto nativeindex.Indexer.IndexPack's parameters
// so a receiver can hand it straight to the indexer.
type DecodedNativePack struct {
	PackID   graphcontract.PackID
	CommitID string
	Manifest graphcontract.PackManifest
	Entries  []graphcontract.PackIndexEntry
	Data     []byte
}

// DecodeNativePullResponse verifies data is a well-formed, canonically
// encoded v1 native pull envelope and splits it back into its individually
// bounded, hash-verified packs, preserving each pack's boundary, identity,
// and index material. It rejects truncated streams, malformed per-pack
// length prefixes, unsupported versions, trailing bytes, and any pack whose
// content no longer matches its declared hash.
func DecodeNativePullResponse(data []byte) (head string, packs []DecodedNativePack, err error) {
	if len(data) < nativePullHeaderSize || string(data[:4]) != nativePullMagic {
		return "", nil, fmt.Errorf("%w: missing envelope header", ErrInvalidNativePullFrame)
	}
	if version := binary.BigEndian.Uint32(data[4:8]); version != NativePullFormatV1 {
		return "", nil, fmt.Errorf("%w: unsupported envelope version %d", ErrInvalidNativePullFrame, version)
	}
	manifestLength := binary.BigEndian.Uint64(data[8:nativePullHeaderSize])
	if manifestLength > uint64(len(data)-nativePullHeaderSize) {
		return "", nil, fmt.Errorf("%w: manifest length exceeds envelope", ErrInvalidNativePullFrame)
	}
	manifestEnd := nativePullHeaderSize + int(manifestLength)
	manifest, err := UnmarshalNativePullManifestV1(data[nativePullHeaderSize:manifestEnd])
	if err != nil {
		return "", nil, err
	}

	decoded := make([]DecodedNativePack, len(manifest.Packs))
	offset := manifestEnd
	for i, pack := range manifest.Packs {
		if pack.Length > uint64(MaxNativePackBytes) {
			return "", nil, fmt.Errorf("%w: pack %d exceeds %d byte limit", ErrInvalidNativePullFrame, i, MaxNativePackBytes)
		}
		if pack.Length > uint64(len(data)-offset) {
			return "", nil, fmt.Errorf("%w: pack %d length exceeds envelope", ErrInvalidNativePullFrame, i)
		}
		end := offset + int(pack.Length)
		packData := data[offset:end]
		if contentHash(packData) != pack.PackHash {
			return "", nil, fmt.Errorf("%w: pack %d hash mismatch", ErrInvalidNativePullFrame, i)
		}
		if err := validateNativePackBytes(graphcontract.PackID(pack.PackID), packData, len(pack.Entries)); err != nil {
			return "", nil, err
		}
		decoded[i] = DecodedNativePack{
			PackID:   graphcontract.PackID(pack.PackID),
			CommitID: pack.CommitID,
			Manifest: pack.Manifest,
			Entries:  pack.Entries,
			Data:     packData,
		}
		offset = end
	}
	if offset != len(data) {
		return "", nil, fmt.Errorf("%w: trailing bytes after packs", ErrInvalidNativePullFrame)
	}
	return manifest.Head, decoded, nil
}

// validateNativePackBytes performs the cheap structural checks a framing
// layer owns: the pack's header parses and matches graphcontract's format
// and declares the same object count as the supplied index entries. Full
// per-object cryptographic verification (CRC32, decompression, content
// hash) remains nativeindex's job once a pack has been deframed.
func validateNativePackBytes(packID graphcontract.PackID, packData []byte, entryCount int) error {
	header, err := graphcontract.ReadPackHeader(bytes.NewReader(packData))
	if err != nil {
		return fmt.Errorf("%w: pack %s: read pack header: %v", ErrInvalidNativePullFrame, packID, err)
	}
	if err := graphcontract.ValidatePackHeader(header); err != nil {
		return fmt.Errorf("%w: pack %s: %v", ErrInvalidNativePullFrame, packID, err)
	}
	if int(header.ObjectCount) != entryCount {
		return fmt.Errorf("%w: pack %s: header declares %d objects but %d entries were supplied", ErrInvalidNativePullFrame, packID, header.ObjectCount, entryCount)
	}
	return nil
}

// contentHash returns the lowercase BLAKE3-256 content ID used to address
// raw native pack bytes, matching the hashing scheme nativeindex uses for
// its own CAS pack writes.
func contentHash(data []byte) string {
	sum := blake3.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// validContentID reports whether id is a well-formed lowercase BLAKE3-256
// content ID (64 hex characters), the convention used throughout Rack for
// both commit and pack identifiers.
func validContentID(id string) bool {
	if len(id) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(id)
	return err == nil && hex.EncodeToString(decoded) == id
}
