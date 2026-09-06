package cas

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
)

// TestDeletePack_RemovesAndIsIdempotent proves LocalDriver.DeletePack (used
// by retention/GC to reclaim CAS blobs no longer referenced by any retained
// pack) actually removes the on-disk pack and that removing an already
// absent pack is a safe no-op rather than an error.
func TestDeletePack_RemovesAndIsIdempotent(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	scope := testScope(t)
	payload := bytes.Repeat([]byte("gc candidate pack payload "), 100)
	hash := hashOf(payload)

	if err := d.WritePack(ctx, scope, hash, bytes.NewReader(payload)); err != nil {
		t.Fatalf("WritePack: %v", err)
	}

	packPath, err := d.packPath(scope, hash)
	if err != nil {
		t.Fatalf("packPath: %v", err)
	}
	if _, err := os.Stat(packPath); err != nil {
		t.Fatalf("expected pack to exist before delete, stat err = %v", err)
	}

	if err := d.DeletePack(ctx, scope, hash); err != nil {
		t.Fatalf("DeletePack: %v", err)
	}
	if _, err := os.Stat(packPath); !os.IsNotExist(err) {
		t.Fatalf("expected pack file removed, stat err = %v", err)
	}

	if _, err := d.OpenPack(ctx, scope, hash); !errors.Is(err, ErrNotFound) {
		t.Fatalf("OpenPack after DeletePack: expected ErrNotFound, got %v", err)
	}

	// Deleting an already-absent pack must be a no-op, not an error, since
	// retention runs may retry after a partial failure.
	if err := d.DeletePack(ctx, scope, hash); err != nil {
		t.Fatalf("DeletePack on absent pack: expected nil, got %v", err)
	}
}

func TestDeletePack_InvalidHashRejected(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	scope := testScope(t)

	if err := d.DeletePack(ctx, scope, "not-a-hash"); !errors.Is(err, ErrInvalidHash) {
		t.Fatalf("expected ErrInvalidHash, got %v", err)
	}
}

// TestDeletePack_TenantIsolation proves deleting a pack under one tenant's
// scope never affects a pack of the same hash stored under another tenant's
// scope, matching Driver's tenant-fencing contract.
func TestDeletePack_TenantIsolation(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	payload := bytes.Repeat([]byte("shared hash payload "), 50)
	hash := hashOf(payload)

	scopeA, err := NewScope("tenant-a", "repo-a")
	if err != nil {
		t.Fatalf("NewScope tenant-a: %v", err)
	}
	scopeB, err := NewScope("tenant-b", "repo-b")
	if err != nil {
		t.Fatalf("NewScope tenant-b: %v", err)
	}

	if err := d.WritePack(ctx, scopeA, hash, bytes.NewReader(payload)); err != nil {
		t.Fatalf("WritePack scopeA: %v", err)
	}
	if err := d.WritePack(ctx, scopeB, hash, bytes.NewReader(payload)); err != nil {
		t.Fatalf("WritePack scopeB: %v", err)
	}

	if err := d.DeletePack(ctx, scopeA, hash); err != nil {
		t.Fatalf("DeletePack scopeA: %v", err)
	}

	if _, err := d.OpenPack(ctx, scopeA, hash); !errors.Is(err, ErrNotFound) {
		t.Fatalf("OpenPack scopeA after delete: expected ErrNotFound, got %v", err)
	}
	if _, err := d.OpenPack(ctx, scopeB, hash); err != nil {
		t.Fatalf("OpenPack scopeB after scopeA delete: expected pack to remain, got %v", err)
	}
}
