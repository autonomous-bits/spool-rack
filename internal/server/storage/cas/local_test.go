package cas

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"lukechampine.com/blake3"
)

func hashOf(data []byte) string {
	sum := blake3.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func newTestDriver(t *testing.T) *LocalDriver {
	t.Helper()
	d, err := NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver: %v", err)
	}
	return d
}

func mustScope(t *testing.T, tenantID, repoID string) Scope {
	t.Helper()
	s, err := NewScope(tenantID, repoID)
	if err != nil {
		t.Fatalf("NewScope(%q, %q): %v", tenantID, repoID, err)
	}
	return s
}

func testScope(t *testing.T) Scope {
	t.Helper()
	return mustScope(t, "tenant-a", "repo-a")
}

func TestNewScopeValidation(t *testing.T) {
	valid := []struct{ tenantID, repoID string }{
		{"tenant-a", "repo-a"},
		{"t1", "r1"},
		{"tenant.with.dots", "repo_with_underscores"},
		{"A", "B"},
	}
	for _, tc := range valid {
		if _, err := NewScope(tc.tenantID, tc.repoID); err != nil {
			t.Errorf("NewScope(%q, %q): expected success, got %v", tc.tenantID, tc.repoID, err)
		}
	}

	invalid := []struct{ tenantID, repoID string }{
		{"", "repo"},
		{"tenant", ""},
		{".", "repo"},
		{"tenant", ".."},
		{"..", "repo"},
		{"../../etc", "repo"},
		{"tenant/with/slash", "repo"},
		{"tenant", "repo/with/slash"},
		{"tenant\\with\\backslash", "repo"},
		{"tenant with space", "repo"},
		{strings.Repeat("a", 200), "repo"},
	}
	for _, tc := range invalid {
		if _, err := NewScope(tc.tenantID, tc.repoID); !errors.Is(err, ErrInvalidScope) {
			t.Errorf("NewScope(%q, %q): expected ErrInvalidScope, got %v", tc.tenantID, tc.repoID, err)
		}
	}
}

func TestPutGetRoundTrip(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	scope := testScope(t)
	data := []byte("hello content-addressed world")
	hash := hashOf(data)

	if err := d.Put(ctx, scope, hash, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := d.Get(ctx, scope, hash)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("round trip mismatch: got %q, want %q", got, data)
	}

	exists, err := d.Exists(ctx, scope, hash)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Fatal("expected object to exist after Put")
	}
}

func TestPutHashMismatchRejected(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	scope := testScope(t)
	data := []byte("some payload")
	wrongHash := hashOf([]byte("different payload"))

	err := d.Put(ctx, scope, wrongHash, data)
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("expected ErrHashMismatch, got %v", err)
	}

	exists, err := d.Exists(ctx, scope, wrongHash)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Fatal("object must not be persisted after a hash mismatch")
	}
}

func TestGetCorruptPayloadDetected(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	scope := testScope(t)
	data := []byte("integrity checked content")
	hash := hashOf(data)

	if err := d.Put(ctx, scope, hash, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Simulate on-disk bit-rot by overwriting the stored object directly.
	// Objects are written read-only, so restore write permission first.
	objPath, err := d.objectPath(scope, hash)
	if err != nil {
		t.Fatalf("objectPath: %v", err)
	}
	if err := os.Chmod(objPath, 0o644); err != nil {
		t.Fatalf("chmod object file for tampering: %v", err)
	}
	if err := os.WriteFile(objPath, []byte("corrupted bytes!!"), 0o644); err != nil {
		t.Fatalf("tamper with object file: %v", err)
	}

	_, err = d.Get(ctx, scope, hash)
	if !errors.Is(err, ErrCorruptObject) {
		t.Fatalf("expected ErrCorruptObject, got %v", err)
	}
}

func TestGetNotFound(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	scope := testScope(t)
	hash := hashOf([]byte("never written"))

	_, err := d.Get(ctx, scope, hash)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	exists, err := d.Exists(ctx, scope, hash)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Fatal("expected Exists to report false for unwritten hash")
	}
}

func TestInvalidHashRejected(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	scope := testScope(t)

	if err := d.Put(ctx, scope, "not-a-hash", []byte("data")); !errors.Is(err, ErrInvalidHash) {
		t.Fatalf("expected ErrInvalidHash from Put, got %v", err)
	}
	if _, err := d.Get(ctx, scope, "short"); !errors.Is(err, ErrInvalidHash) {
		t.Fatalf("expected ErrInvalidHash from Get, got %v", err)
	}
}

// TestScopeTenantIsolation verifies that two distinct scopes never see each
// other's objects, even when writing byte-identical content under the same
// hash, and that each scope's data is physically partitioned on disk under
// its own tenant/repository directory.
func TestScopeTenantIsolation(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	scopeA := mustScope(t, "tenant-a", "repo-1")
	scopeB := mustScope(t, "tenant-b", "repo-1")
	data := []byte("shared content, isolated tenants")
	hash := hashOf(data)

	if err := d.Put(ctx, scopeA, hash, data); err != nil {
		t.Fatalf("Put scopeA: %v", err)
	}

	existsB, err := d.Exists(ctx, scopeB, hash)
	if err != nil {
		t.Fatalf("Exists scopeB: %v", err)
	}
	if existsB {
		t.Fatal("object written under scopeA must not be visible under scopeB")
	}
	if _, err := d.Get(ctx, scopeB, hash); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound reading scopeA's object via scopeB, got %v", err)
	}

	if err := d.Put(ctx, scopeB, hash, data); err != nil {
		t.Fatalf("Put scopeB: %v", err)
	}
	gotA, err := d.Get(ctx, scopeA, hash)
	if err != nil {
		t.Fatalf("Get scopeA: %v", err)
	}
	gotB, err := d.Get(ctx, scopeB, hash)
	if err != nil {
		t.Fatalf("Get scopeB: %v", err)
	}
	if !bytes.Equal(gotA, data) || !bytes.Equal(gotB, data) {
		t.Fatal("both scopes should independently retrieve their own copy of the object")
	}

	pathA, err := d.objectPath(scopeA, hash)
	if err != nil {
		t.Fatalf("objectPath scopeA: %v", err)
	}
	pathB, err := d.objectPath(scopeB, hash)
	if err != nil {
		t.Fatalf("objectPath scopeB: %v", err)
	}
	if pathA == pathB {
		t.Fatal("scopes must resolve to distinct on-disk paths")
	}
	if !strings.Contains(pathA, filepath.Join("tenants", "tenant-a", "repos", "repo-1")) {
		t.Fatalf("scopeA path %q does not contain expected tenant/repo partition", pathA)
	}
	if !strings.Contains(pathB, filepath.Join("tenants", "tenant-b", "repos", "repo-1")) {
		t.Fatalf("scopeB path %q does not contain expected tenant/repo partition", pathB)
	}
}

func TestWritePackOpenPackRoundTrip(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	scope := testScope(t)
	payload := bytes.Repeat([]byte("packfile content payload "), 1000)
	hash := hashOf(payload)

	if err := d.WritePack(ctx, scope, hash, bytes.NewReader(payload)); err != nil {
		t.Fatalf("WritePack: %v", err)
	}

	rc, err := d.OpenPack(ctx, scope, hash)
	if err != nil {
		t.Fatalf("OpenPack: %v", err)
	}
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read pack: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("decompressed pack content does not match original payload")
	}

	// Confirm the stored file is actually zstd-compressed on disk.
	packPath, err := d.packPath(scope, hash)
	if err != nil {
		t.Fatalf("packPath: %v", err)
	}
	raw, err := os.ReadFile(packPath)
	if err != nil {
		t.Fatalf("read raw pack file: %v", err)
	}
	if !bytes.HasPrefix(raw, []byte{0x28, 0xb5, 0x2f, 0xfd}) {
		t.Fatal("stored pack file does not have zstd magic bytes")
	}
	if len(raw) >= len(payload) {
		t.Fatalf("expected compressed pack (%d bytes) to be smaller than payload (%d bytes)", len(raw), len(payload))
	}
}

func TestWritePackHashMismatchRejected(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	scope := testScope(t)
	payload := []byte("pack stream data")
	wrongHash := hashOf([]byte("not the pack data"))

	err := d.WritePack(ctx, scope, wrongHash, bytes.NewReader(payload))
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("expected ErrHashMismatch, got %v", err)
	}

	packPath, err := d.packPath(scope, wrongHash)
	if err != nil {
		t.Fatalf("packPath: %v", err)
	}
	if _, err := os.Stat(packPath); !os.IsNotExist(err) {
		t.Fatalf("expected no pack file left behind, stat err = %v", err)
	}
}

func TestOpenPackNotFound(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	scope := testScope(t)
	hash := hashOf([]byte("never packed"))

	_, err := d.OpenPack(ctx, scope, hash)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// TestConcurrentWrites exercises Put and WritePack from many goroutines
// concurrently, mixing distinct hashes with duplicate hashes racing to
// write the same content. Run with -race to catch data races.
func TestConcurrentWrites(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	scope := testScope(t)

	const numObjects = 50
	const writersPerObject = 4

	type object struct {
		hash string
		data []byte
	}
	objects := make([]object, numObjects)
	for i := range objects {
		data := []byte(filepath.Join("object-payload", string(rune('a'+i%26)), string(rune(i))))
		objects[i] = object{hash: hashOf(data), data: data}
	}

	var wg sync.WaitGroup
	for _, obj := range objects {
		for w := 0; w < writersPerObject; w++ {
			wg.Add(1)
			go func(obj object) {
				defer wg.Done()
				if err := d.Put(ctx, scope, obj.hash, obj.data); err != nil {
					t.Errorf("concurrent Put(%s): %v", obj.hash, err)
				}
			}(obj)
		}
	}
	wg.Wait()

	for _, obj := range objects {
		got, err := d.Get(ctx, scope, obj.hash)
		if err != nil {
			t.Fatalf("Get(%s) after concurrent writes: %v", obj.hash, err)
		}
		if !bytes.Equal(got, obj.data) {
			t.Fatalf("Get(%s) returned wrong data after concurrent writes", obj.hash)
		}
	}
}

// TestConcurrentWritePack exercises WritePack racing on the same pack hash.
func TestConcurrentWritePack(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	scope := testScope(t)
	payload := bytes.Repeat([]byte("shared pack payload "), 500)
	hash := hashOf(payload)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := d.WritePack(ctx, scope, hash, bytes.NewReader(payload)); err != nil {
				t.Errorf("concurrent WritePack: %v", err)
			}
		}()
	}
	wg.Wait()

	rc, err := d.OpenPack(ctx, scope, hash)
	if err != nil {
		t.Fatalf("OpenPack: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read pack: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("pack content mismatch after concurrent WritePack")
	}
}

// TestConcurrentWritesAcrossScopes exercises Put concurrently across many
// distinct tenant scopes to ensure per-scope directory creation and atomic
// writes are race-safe when multiple tenants are provisioned simultaneously.
func TestConcurrentWritesAcrossScopes(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	data := []byte("cross-tenant concurrent payload")
	hash := hashOf(data)

	const numScopes = 20
	var wg sync.WaitGroup
	for i := 0; i < numScopes; i++ {
		scope := mustScope(t, "tenant-"+string(rune('a'+i%26)), "repo-1")
		wg.Add(1)
		go func(scope Scope) {
			defer wg.Done()
			if err := d.Put(ctx, scope, hash, data); err != nil {
				t.Errorf("concurrent Put across scopes: %v", err)
			}
		}(scope)
	}
	wg.Wait()

	for i := 0; i < numScopes; i++ {
		scope := mustScope(t, "tenant-"+string(rune('a'+i%26)), "repo-1")
		got, err := d.Get(ctx, scope, hash)
		if err != nil {
			t.Fatalf("Get after concurrent cross-scope writes: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Fatal("cross-scope concurrent write returned wrong data")
		}
	}
}
