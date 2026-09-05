package cas

import (
	"context"
	"io"
)

// Driver represents a pluggable content-addressed storage tier for Spool objects and packfiles.
type Driver interface {
	// Put writes an immutable object identified by its BLAKE3 hex hash.
	Put(ctx context.Context, hash string, data []byte) error
	// Get reads an object by its BLAKE3 hex hash.
	Get(ctx context.Context, hash string) ([]byte, error)
	// Exists checks if an object exists in storage.
	Exists(ctx context.Context, hash string) (bool, error)
	// OpenPack streams a packfile by packfile hash.
	OpenPack(ctx context.Context, packHash string) (io.ReadCloser, error)
	// WritePack persists a validated packfile stream.
	WritePack(ctx context.Context, packHash string, r io.Reader) error
}
