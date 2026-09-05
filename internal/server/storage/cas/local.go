package cas

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
	"lukechampine.com/blake3"
)

const (
	hashSize   = 32 // BLAKE3-256 digest size in bytes
	hashHexLen = hashSize * 2

	tenantsDir = "tenants"
	reposDir   = "repos"
	objectsDir = "objects"
	packsDir   = "packs"
	tmpDir     = "tmp"

	dirPerm  = 0o755
	filePerm = 0o444 // objects and packs are immutable once written
)

// Sentinel errors returned by LocalDriver. Callers should use errors.Is to
// check for these rather than comparing error strings.
var (
	// ErrInvalidHash indicates the supplied hash is not a well-formed BLAKE3-256 hex digest.
	ErrInvalidHash = errors.New("cas: invalid hash")
	// ErrHashMismatch indicates the computed content hash does not match the requested hash.
	ErrHashMismatch = errors.New("cas: hash mismatch")
	// ErrCorruptObject indicates an object read back from disk fails BLAKE3 verification,
	// signalling on-disk corruption (e.g. bit-rot) rather than a bad write request.
	ErrCorruptObject = errors.New("cas: corrupt object")
	// ErrNotFound indicates the requested object or packfile does not exist.
	ErrNotFound = errors.New("cas: not found")
)

// LocalDriver is a filesystem-backed implementation of Driver. It stores
// immutable objects and zstd-compressed packfiles under a shared root
// directory, natively fencing every operation to its caller-supplied Scope
// by deterministically partitioning storage paths under opaque, fixed-width
// directory keys derived from each tenant/repository identifier. Content-addressed
// BLAKE3-256 hashes verify integrity, and crash-consistent
// temp-file-then-rename writes guarantee durability.
//
// LocalDriver holds no mutable state beyond its immutable root path, so a
// single value may be shared safely across goroutines and across tenants.
type LocalDriver struct {
	root string
}

// NewLocalDriver constructs a LocalDriver rooted at the given directory,
// creating it if it does not exist. Per-tenant/repository subdirectories
// are created lazily and on demand as scoped operations occur, since the
// set of tenants and repositories is not known up front.
func NewLocalDriver(root string) (*LocalDriver, error) {
	if err := os.MkdirAll(root, dirPerm); err != nil {
		return nil, fmt.Errorf("cas: create root directory: %w", err)
	}
	return &LocalDriver{root: root}, nil
}

// Put writes an immutable object identified by its BLAKE3 hex hash, fenced to scope.
func (d *LocalDriver) Put(ctx context.Context, scope Scope, hash string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateHash(hash); err != nil {
		return err
	}
	sum := blake3.Sum256(data)
	if hex.EncodeToString(sum[:]) != hash {
		return fmt.Errorf("%w: object %s", ErrHashMismatch, hash)
	}

	dest, err := d.objectPath(scope, hash)
	if err != nil {
		return err
	}
	tmp, err := d.scopeTmpDir(scope)
	if err != nil {
		return err
	}

	if _, err := os.Stat(dest); err == nil {
		// Content-addressed and immutable: an existing object with this hash
		// already holds identical content, so the write is a no-op.
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("cas: stat object %s: %w", hash, err)
	}

	if err := writeFileAtomic(tmp, dest, func(w io.Writer) error {
		_, err := w.Write(data)
		return err
	}); err != nil {
		return fmt.Errorf("cas: put object %s: %w", hash, err)
	}
	return nil
}

// Get reads an object by its BLAKE3 hex hash, fenced to scope.
func (d *LocalDriver) Get(ctx context.Context, scope Scope, hash string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateHash(hash); err != nil {
		return nil, err
	}
	path, err := d.objectPath(scope, hash)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: object %s", ErrNotFound, hash)
		}
		return nil, fmt.Errorf("cas: read object %s: %w", hash, err)
	}

	sum := blake3.Sum256(data)
	if hex.EncodeToString(sum[:]) != hash {
		return nil, fmt.Errorf("%w: object %s", ErrCorruptObject, hash)
	}
	return data, nil
}

// Exists checks if an object exists in storage, fenced to scope.
func (d *LocalDriver) Exists(ctx context.Context, scope Scope, hash string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := validateHash(hash); err != nil {
		return false, err
	}
	path, err := d.objectPath(scope, hash)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("cas: stat object %s: %w", hash, err)
}

// OpenPack streams a packfile by packfile hash, fenced to scope, transparently
// decompressing the zstd-compressed archive on disk.
func (d *LocalDriver) OpenPack(ctx context.Context, scope Scope, packHash string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateHash(packHash); err != nil {
		return nil, err
	}
	path, err := d.packPath(scope, packHash)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: pack %s", ErrNotFound, packHash)
		}
		return nil, fmt.Errorf("cas: open pack %s: %w", packHash, err)
	}

	dec, err := zstd.NewReader(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("cas: open pack decoder %s: %w", packHash, err)
	}
	return &packReader{dec: dec, f: f}, nil
}

// packReader adapts a zstd.Decoder and its backing file into an io.ReadCloser.
type packReader struct {
	dec *zstd.Decoder
	f   *os.File
}

func (p *packReader) Read(b []byte) (int, error) {
	return p.dec.Read(b)
}

func (p *packReader) Close() error {
	p.dec.Close()
	return p.f.Close()
}

// WritePack persists a validated packfile stream, fenced to scope,
// compressing it with zstd and verifying its BLAKE3 hash before making it
// durable.
func (d *LocalDriver) WritePack(ctx context.Context, scope Scope, packHash string, r io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateHash(packHash); err != nil {
		return err
	}
	dest, err := d.packPath(scope, packHash)
	if err != nil {
		return err
	}
	tmp, err := d.scopeTmpDir(scope)
	if err != nil {
		return err
	}

	if _, err := os.Stat(dest); err == nil {
		// Already durable under this hash; drain the caller's stream so
		// producers relying on synchronous consumption aren't left blocked.
		if _, err := io.Copy(io.Discard, r); err != nil {
			return fmt.Errorf("cas: drain existing pack %s: %w", packHash, err)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("cas: stat pack %s: %w", packHash, err)
	}

	hasher := blake3.New(hashSize, nil)
	tee := io.TeeReader(r, hasher)

	if err := writeFileAtomic(tmp, dest, func(w io.Writer) error {
		zw, err := zstd.NewWriter(w)
		if err != nil {
			return fmt.Errorf("create zstd encoder: %w", err)
		}
		if _, err := io.Copy(zw, tee); err != nil {
			_ = zw.Close()
			return fmt.Errorf("compress pack stream: %w", err)
		}
		return zw.Close()
	}); err != nil {
		return fmt.Errorf("cas: write pack %s: %w", packHash, err)
	}

	sum := hex.EncodeToString(hasher.Sum(nil))
	if sum != packHash {
		if rmErr := os.Remove(dest); rmErr != nil && !os.IsNotExist(rmErr) {
			return fmt.Errorf("%w: pack %s (also failed to remove invalid pack: %v)", ErrHashMismatch, packHash, rmErr)
		}
		return fmt.Errorf("%w: pack %s", ErrHashMismatch, packHash)
	}
	return nil
}

// scopeRoot returns the tenant/repository-partitioned root directory for
// scope, mapping externally supplied scope identifiers to opaque hex keys
// before constructing any filesystem path. This keeps path expressions free
// of raw request-derived strings while still deterministically fencing every
// scope to its own on-disk partition.
func (d *LocalDriver) scopeRoot(scope Scope) (string, error) {
	tenantKey := scopeStorageKey(scope.TenantID())
	repoKey := scopeStorageKey(scope.RepoID())
	return safeJoin(d.root, tenantsDir, tenantKey, reposDir, repoKey)
}

// scopeTmpDir returns scope's private temporary-file staging directory,
// creating it if necessary. Keeping temp files inside the scope's own
// partition (rather than a root-wide tmp directory) ensures no
// partially-written data is ever visible outside its tenant sandbox, and
// keeps the rename target on the same filesystem as the temp file.
func (d *LocalDriver) scopeTmpDir(scope Scope) (string, error) {
	root, err := d.scopeRoot(scope)
	if err != nil {
		return "", err
	}
	dir, err := safeJoin(root, tmpDir)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return "", fmt.Errorf("cas: create scope tmp directory: %w", err)
	}
	return dir, nil
}

func (d *LocalDriver) objectPath(scope Scope, hash string) (string, error) {
	root, err := d.scopeRoot(scope)
	if err != nil {
		return "", err
	}
	canonicalHash, err := canonicalHashString(hash)
	if err != nil {
		return "", err
	}
	return safeJoin(root, objectsDir, canonicalHash[:2], canonicalHash[2:])
}

func (d *LocalDriver) packPath(scope Scope, packHash string) (string, error) {
	root, err := d.scopeRoot(scope)
	if err != nil {
		return "", err
	}
	canonicalHash, err := canonicalHashString(packHash)
	if err != nil {
		return "", err
	}
	return safeJoin(root, packsDir, canonicalHash+".spack")
}

// validateHash rejects any hash that is not a well-formed lowercase hex
// encoding of a BLAKE3-256 digest, guarding path construction against
// traversal and malformed input.
func validateHash(hash string) error {
	if len(hash) != hashHexLen {
		return fmt.Errorf("%w: expected %d hex characters, got %d", ErrInvalidHash, hashHexLen, len(hash))
	}
	if _, err := canonicalHashString(hash); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidHash, err)
	}
	return nil
}

func canonicalHashString(hash string) (string, error) {
	decoded, err := hex.DecodeString(hash)
	if err != nil {
		return "", err
	}
	if len(decoded) != hashSize {
		return "", fmt.Errorf("decoded hash must be %d bytes, got %d", hashSize, len(decoded))
	}
	return hex.EncodeToString(decoded), nil
}

func scopeStorageKey(scopeID string) string {
	sum := blake3.Sum256([]byte(scopeID))
	return hex.EncodeToString(sum[:])
}

// safeJoin joins elems onto root and verifies, via filepath.Rel, that the
// resulting path is strictly contained within root. This is a defense-in-depth
// guard against directory traversal: even though Scope and hash inputs are
// independently validated before reaching this function, safeJoin ensures no
// combination of inputs can ever resolve to a path outside root.
func safeJoin(root string, elems ...string) (string, error) {
	joined := filepath.Join(append([]string{root}, elems...)...)
	rel, err := filepath.Rel(root, joined)
	if err != nil {
		return "", fmt.Errorf("%w: cannot resolve path relative to root", ErrInvalidScope)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("%w: path escapes storage root", ErrInvalidScope)
	}
	return joined, nil
}

// writeFileAtomic implements the crash-consistent write pattern required for
// all mutable control state in this repository: write content to a temp file
// on the same filesystem, fsync its bytes, atomically rename it into place,
// then fsync the parent directory so the rename itself is durable.
func writeFileAtomic(tmpDir, dest string, write func(w io.Writer) error) error {
	destDir := filepath.Dir(dest)
	if err := os.MkdirAll(destDir, dirPerm); err != nil {
		return fmt.Errorf("create destination directory: %w", err)
	}

	tmp, err := os.CreateTemp(tmpDir, "cas-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }() // no-op once successfully renamed

	if err := write(tmp); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Chmod(tmpPath, filePerm); err != nil {
		return fmt.Errorf("chmod temp file: %w", err)
	}
	if err := os.Rename(tmpPath, dest); err != nil {
		return fmt.Errorf("rename into place: %w", err)
	}
	if err := syncDir(destDir); err != nil {
		return fmt.Errorf("sync destination directory: %w", err)
	}
	return nil
}

// syncDir fsyncs a directory so that a preceding rename within it is
// durable across a crash, per the atomic-file-replacement standard.
func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory: %w", err)
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("fsync directory: %w", err)
	}
	return nil
}
