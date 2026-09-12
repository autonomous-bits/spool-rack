package cas

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

func TestLocalDriver_AssetOperations(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	scopeA := testScope(t)

	scopeB, err := NewScope("tenant-beta", "repo-beta")
	if err != nil {
		t.Fatalf("NewScope: %v", err)
	}

	payload := []byte("hello contextual asset blob content")
	hash := hashOf(payload)

	// 1. AssetExists before write should return false
	exists, err := d.AssetExists(ctx, scopeA, hash)
	if err != nil {
		t.Fatalf("AssetExists: %v", err)
	}
	if exists {
		t.Fatal("expected AssetExists to be false before write")
	}

	// 2. OpenAsset before write should return ErrNotFound
	if _, _, err := d.OpenAsset(ctx, scopeA, hash); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound from OpenAsset, got: %v", err)
	}

	// 3. WriteAsset with mismatched hash should fail and not leave file behind
	badHash := hashOf([]byte("some other payload"))
	if _, err := d.WriteAsset(ctx, scopeA, badHash, bytes.NewReader(payload)); !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("expected ErrHashMismatch, got: %v", err)
	}
	exists, _ = d.AssetExists(ctx, scopeA, badHash)
	if exists {
		t.Fatal("bad hash file should not exist on disk")
	}

	// 4. WriteAsset with matching hash should succeed
	written, err := d.WriteAsset(ctx, scopeA, hash, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("WriteAsset: %v", err)
	}
	if written != int64(len(payload)) {
		t.Fatalf("expected written %d, got %d", len(payload), written)
	}

	// 5. AssetExists should now return true
	exists, err = d.AssetExists(ctx, scopeA, hash)
	if err != nil {
		t.Fatalf("AssetExists: %v", err)
	}
	if !exists {
		t.Fatal("expected AssetExists to be true after write")
	}

	// 6. OpenAsset should stream the exact payload
	rc, size, err := d.OpenAsset(ctx, scopeA, hash)
	if err != nil {
		t.Fatalf("OpenAsset: %v", err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			t.Errorf("close asset reader: %v", err)
		}
	}()
	if size != int64(len(payload)) {
		t.Fatalf("expected size %d, got %d", len(payload), size)
	}
	readBytes, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read asset: %v", err)
	}
	if !bytes.Equal(readBytes, payload) {
		t.Fatalf("payload mismatch: got %q, want %q", string(readBytes), string(payload))
	}

	// 7. WriteAsset deduplication: writing again should succeed and return size
	written2, err := d.WriteAsset(ctx, scopeA, hash, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("WriteAsset duplicate: %v", err)
	}
	if written2 != int64(len(payload)) {
		t.Fatalf("expected duplicate written %d, got %d", len(payload), written2)
	}

	// 8. Tenant isolation: scopeB should NOT see the asset written under scopeA
	existsB, err := d.AssetExists(ctx, scopeB, hash)
	if err != nil {
		t.Fatalf("AssetExists scopeB: %v", err)
	}
	if existsB {
		t.Fatal("tenant isolation violated: scopeB should not see scopeA asset")
	}
	if _, _, err := d.OpenAsset(ctx, scopeB, hash); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tenant isolation violated: OpenAsset scopeB expected ErrNotFound, got: %v", err)
	}
}
