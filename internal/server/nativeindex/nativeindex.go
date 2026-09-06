// Package nativeindex builds and serves a tenant-scoped index over verified
// native Spool packs (the binary pack container format exposed by
// graphcontract/pack.go), so Rack can resolve every object reachable from an
// uploaded commit by object ID. It is distinct from, and does not modify,
// Rack's existing CBOR-framed PackFrameV2 push/pull path in the sync
// package.
package nativeindex

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	"github.com/autonomous-bits/spool/graphcontract"
	"lukechampine.com/blake3"
)

// MaxNativePackBytes bounds every native pack this indexer accepts, matching
// Rack's existing v2 pack size limit so a hostile or corrupt upload cannot
// force unbounded memory use while it is parsed and verified.
const MaxNativePackBytes = 64 << 20

var (
	// ErrInvalidPack indicates a native pack failed graphcontract's format,
	// manifest, or per-object integrity verification. No CAS write or index
	// row is ever produced when this error is returned.
	ErrInvalidPack = errors.New("nativeindex: invalid native pack")
	// ErrObjectNotFound indicates the requested object is not indexed for
	// the caller's tenant-scoped repository — including when it exists only
	// for a different tenant, which is deliberately indistinguishable from
	// an absent object so a lookup can never leak cross-tenant existence.
	ErrObjectNotFound = errors.New("nativeindex: object not found")
)

// Store is the narrow slice of postgres.Store that Indexer depends on, kept
// separate so unit tests can supply a lightweight fake instead of a real
// PostgreSQL-backed store.
type Store interface {
	SetTenantContext(ctx context.Context, tenantID string) (context.Context, error)
	PutNativePack(ctx context.Context, repoID, packID, casPackHash, commitID string, entries []graphcontract.PackIndexEntry) error
	GetNativeObjectLocation(ctx context.Context, repoID, objectID string) (postgres.NativeObjectLocation, error)
}

var _ Store = (postgres.Store)(nil)

// Indexer verifies and indexes native Spool packs so their objects can later
// be resolved by object ID, strictly scoped to the uploading tenant.
type Indexer struct {
	driver cas.Driver
	store  Store
}

// NewIndexer constructs an Indexer backed by CAS and native pack metadata
// storage.
func NewIndexer(driver cas.Driver, store Store) *Indexer {
	return &Indexer{driver: driver, store: store}
}

// IndexPack fully verifies packData against manifest, packID, and entries
// using graphcontract's native pack contract — header, manifest, index
// bounds, and every individual object's CRC32/decompression/canonical
// envelope/content hash — and only once every object has been proven valid
// does it write the raw pack bytes to CAS and register each object's
// location. commitID may be empty when the pack is not yet associated with a
// registered commit.
func (idx *Indexer) IndexPack(
	ctx context.Context,
	tenantID, repoID string,
	packID graphcontract.PackID,
	manifest graphcontract.PackManifest,
	packData []byte,
	entries []graphcontract.PackIndexEntry,
	commitID string,
) error {
	if int64(len(packData)) > MaxNativePackBytes {
		return fmt.Errorf("%w: pack exceeds %d byte limit", ErrInvalidPack, MaxNativePackBytes)
	}
	if !graphcontract.ValidPackID(packID) {
		return fmt.Errorf("%w: invalid pack ID %q", ErrInvalidPack, packID)
	}
	if err := graphcontract.ValidatePackManifest(manifest); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPack, err)
	}
	if err := validateManifestListsPack(manifest, packID, len(entries)); err != nil {
		return err
	}

	header, err := graphcontract.ReadPackHeader(bytes.NewReader(packData))
	if err != nil {
		return fmt.Errorf("%w: read pack header: %v", ErrInvalidPack, err)
	}
	if err := graphcontract.ValidatePackHeader(header); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPack, err)
	}

	sorted := make([]graphcontract.PackIndexEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Offset < sorted[j].Offset })

	for _, entry := range sorted {
		if err := graphcontract.ValidatePackIndexEntry(packID, entry); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidPack, err)
		}
	}
	if err := graphcontract.ValidatePackEntries(packID, header, uint64(len(packData)), sorted); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPack, err)
	}

	// Every object is fully verified — CRC32, zstd decompression, canonical
	// CBOR envelope, and content-hash match — before anything is persisted,
	// so a corrupt or tampered pack can never partially pollute the index.
	for _, entry := range sorted {
		compressed := packData[entry.Offset : entry.Offset+entry.CompressedSize]
		if _, _, err := graphcontract.DecompressPackedObject(entry, compressed); err != nil {
			return fmt.Errorf("%w: verify object %s: %v", ErrInvalidPack, entry.Object, err)
		}
	}

	scope, err := cas.NewScope(tenantID, repoID)
	if err != nil {
		return fmt.Errorf("nativeindex: index pack: invalid CAS scope: %w", err)
	}
	casPackHash := contentHash(packData)
	if err := idx.driver.WritePack(ctx, scope, casPackHash, bytes.NewReader(packData)); err != nil {
		return fmt.Errorf("nativeindex: index pack: write CAS pack: %w", err)
	}

	tenantCtx, err := idx.store.SetTenantContext(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("nativeindex: index pack: set tenant context: %w", err)
	}
	if err := idx.store.PutNativePack(tenantCtx, repoID, string(packID), casPackHash, commitID, sorted); err != nil {
		return fmt.Errorf("nativeindex: index pack: register native pack: %w", err)
	}
	return nil
}

// ResolveObject looks up objectID scoped to tenantID and repoID and returns
// its fully verified type and canonical bytes. It returns ErrObjectNotFound
// both when the object has never been indexed and when it exists only for a
// different tenant or repository.
func (idx *Indexer) ResolveObject(ctx context.Context, tenantID, repoID string, objectID graphcontract.ObjectID) (objectType string, objectData []byte, err error) {
	tenantCtx, err := idx.store.SetTenantContext(ctx, tenantID)
	if err != nil {
		return "", nil, fmt.Errorf("nativeindex: resolve object: set tenant context: %w", err)
	}

	loc, err := idx.store.GetNativeObjectLocation(tenantCtx, repoID, string(objectID))
	if err != nil {
		if errors.Is(err, postgres.ErrNativeObjectNotFound) {
			return "", nil, fmt.Errorf("%w: %s", ErrObjectNotFound, objectID)
		}
		return "", nil, fmt.Errorf("nativeindex: resolve object: lookup location: %w", err)
	}

	scope, err := cas.NewScope(tenantID, repoID)
	if err != nil {
		return "", nil, fmt.Errorf("nativeindex: resolve object: invalid CAS scope: %w", err)
	}

	packReader, err := idx.driver.OpenPack(ctx, scope, loc.CASPackHash)
	if err != nil {
		return "", nil, fmt.Errorf("nativeindex: resolve object: open pack %s: %w", loc.CASPackHash, err)
	}
	defer func() { _ = packReader.Close() }()

	packData, err := io.ReadAll(io.LimitReader(packReader, MaxNativePackBytes+1))
	if err != nil {
		return "", nil, fmt.Errorf("nativeindex: resolve object: read pack %s: %w", loc.CASPackHash, err)
	}
	if int64(len(packData)) > MaxNativePackBytes {
		return "", nil, fmt.Errorf("nativeindex: resolve object: pack %s exceeds %d byte limit", loc.CASPackHash, MaxNativePackBytes)
	}
	if loc.Offset+loc.CompressedSize > uint64(len(packData)) {
		return "", nil, fmt.Errorf("nativeindex: resolve object: object %s entry extends beyond pack data", objectID)
	}

	entry := graphcontract.PackIndexEntry{
		Object:           objectID,
		Offset:           loc.Offset,
		CompressedSize:   loc.CompressedSize,
		UncompressedSize: loc.UncompressedSize,
		CRC32:            loc.CRC32,
	}
	compressed := packData[loc.Offset : loc.Offset+loc.CompressedSize]
	return graphcontract.DecompressPackedObject(entry, compressed)
}

// validateManifestListsPack requires that manifest declare exactly packID
// with a matching object count and the required zstd compression, so a
// caller cannot index a pack whose declared manifest metadata disagrees with
// what was actually verified.
func validateManifestListsPack(manifest graphcontract.PackManifest, packID graphcontract.PackID, objectCount int) error {
	for _, metadata := range manifest.Packs {
		if metadata.ID != packID {
			continue
		}
		if metadata.ObjectCount != uint32(objectCount) {
			return fmt.Errorf("%w: manifest object count %d does not match %d supplied entries", ErrInvalidPack, metadata.ObjectCount, objectCount)
		}
		if metadata.Compression != graphcontract.PackCompressionZstd {
			return fmt.Errorf("%w: manifest declares unsupported compression %q for pack %s", ErrInvalidPack, metadata.Compression, packID)
		}
		return nil
	}
	return fmt.Errorf("%w: manifest does not list pack %s", ErrInvalidPack, packID)
}

// contentHash returns the lowercase BLAKE3-256 content ID used to address
// the raw native pack bytes within Rack's CAS, matching the hashing scheme
// used elsewhere for CAS objects and packs.
func contentHash(data []byte) string {
	sum := blake3.Sum256(data)
	return hex.EncodeToString(sum[:])
}
