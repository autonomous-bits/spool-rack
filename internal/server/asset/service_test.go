package asset

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
)

type fakeStore struct {
	assets   map[string]postgres.AssetRecord
	quota    int64
	used     int64
	admitErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		assets: make(map[string]postgres.AssetRecord),
	}
}

func (f *fakeStore) AdmitAssetUpload(_ context.Context, _ string, hash string, sizeBytes int64) error {
	if f.admitErr != nil {
		return f.admitErr
	}
	if _, exists := f.assets[hash]; exists {
		return nil
	}
	if f.quota > 0 && (f.used+sizeBytes) > f.quota {
		return ErrQuotaExceeded
	}
	return nil
}

func (f *fakeStore) RegisterAsset(_ context.Context, repoID, hash string, sizeBytes int64, mimeType string) error {
	if _, exists := f.assets[hash]; exists {
		return nil
	}
	if f.quota > 0 && (f.used+sizeBytes) > f.quota {
		return ErrQuotaExceeded
	}
	f.assets[hash] = postgres.AssetRecord{
		RepoID:    repoID,
		Hash:      hash,
		SizeBytes: sizeBytes,
		MIMEType:  mimeType,
	}
	f.used += sizeBytes
	return nil
}

func (f *fakeStore) GetAssetMetadata(_ context.Context, _ string, hash string) (postgres.AssetRecord, error) {
	rec, ok := f.assets[hash]
	if !ok {
		return postgres.AssetRecord{}, postgres.ErrAssetNotFound
	}
	return rec, nil
}

func (f *fakeStore) ListMissingAssets(_ context.Context, _ string, candidateHashes []string) ([]string, error) {
	var missing []string
	for _, h := range candidateHashes {
		if _, ok := f.assets[h]; !ok {
			missing = append(missing, h)
		}
	}
	return missing, nil
}

type fakeCASDriver struct {
	blobs map[string][]byte
}

func newFakeCASDriver() *fakeCASDriver {
	return &fakeCASDriver{blobs: make(map[string][]byte)}
}

func (f *fakeCASDriver) Put(context.Context, cas.Scope, string, []byte) error    { return nil }
func (f *fakeCASDriver) Get(context.Context, cas.Scope, string) ([]byte, error)  { return nil, nil }
func (f *fakeCASDriver) Exists(context.Context, cas.Scope, string) (bool, error) { return false, nil }
func (f *fakeCASDriver) OpenPack(context.Context, cas.Scope, string) (io.ReadCloser, error) {
	return nil, nil
}
func (f *fakeCASDriver) WritePack(context.Context, cas.Scope, string, io.Reader) error { return nil }

func (f *fakeCASDriver) WriteAsset(_ context.Context, _ cas.Scope, hash string, r io.Reader) (int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	f.blobs[hash] = data
	return int64(len(data)), nil
}

func (f *fakeCASDriver) OpenAsset(_ context.Context, _ cas.Scope, hash string) (io.ReadCloser, int64, error) {
	data, ok := f.blobs[hash]
	if !ok {
		return nil, 0, cas.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

func (f *fakeCASDriver) AssetExists(_ context.Context, _ cas.Scope, hash string) (bool, error) {
	_, ok := f.blobs[hash]
	return ok, nil
}

func TestAssetService_NegotiateAndUpload(t *testing.T) {
	ctx := context.Background()
	driver := newFakeCASDriver()
	store := newFakeStore()
	store.quota = 1000
	svc := NewService(driver, store)

	scope, err := cas.NewScope("tenant-1", "repo-1")
	if err != nil {
		t.Fatalf("NewScope: %v", err)
	}

	hash1 := "1111111111111111111111111111111111111111111111111111111111111111"
	hash2 := "2222222222222222222222222222222222222222222222222222222222222222"

	// 1. Initial negotiation: both hashes are missing
	res, err := svc.Negotiate(ctx, scope, NegotiationRequest{
		Hashes: []string{hash1, hash2},
		Sizes:  map[string]int64{hash1: 400, hash2: 300},
	})
	if err != nil {
		t.Fatalf("Negotiate: %v", err)
	}
	if len(res.Missing) != 2 || !res.UploadAuthorized {
		t.Fatalf("unexpected negotiation result: %+v", res)
	}

	// 2. Upload hash1
	payload1 := []byte("payload for asset 1")
	written, err := svc.Upload(ctx, scope, hash1, int64(len(payload1)), "text/plain", bytes.NewReader(payload1))
	if err != nil {
		t.Fatalf("Upload hash1: %v", err)
	}
	if written != int64(len(payload1)) {
		t.Fatalf("expected written %d, got %d", len(payload1), written)
	}

	// 3. Second negotiation: hash1 exists, hash2 is missing
	res, err = svc.Negotiate(ctx, scope, NegotiationRequest{
		Hashes: []string{hash1, hash2},
	})
	if err != nil {
		t.Fatalf("Negotiate second: %v", err)
	}
	if len(res.Missing) != 1 || res.Missing[0] != hash2 {
		t.Fatalf("expected [hash2] missing, got %v", res.Missing)
	}
	if len(res.Existing) != 1 || res.Existing[0] != hash1 {
		t.Fatalf("expected [hash1] existing, got %v", res.Existing)
	}

	// 4. Open hash1: should stream content and return metadata
	rc, size, mimeType, err := svc.Open(ctx, scope, hash1)
	if err != nil {
		t.Fatalf("Open hash1: %v", err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			t.Errorf("close asset reader: %v", err)
		}
	}()
	if size != int64(len(payload1)) || mimeType != "text/plain" {
		t.Fatalf("unexpected open result: size=%d, mimeType=%s", size, mimeType)
	}

	// 5. Open hash2 (not uploaded): should return ErrNotFound
	if _, _, _, err := svc.Open(ctx, scope, hash2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for hash2, got: %v", err)
	}

	// 6. Quota check in negotiation: quota 1000. Asking for 1100 exceeds quota
	_, err = svc.Negotiate(ctx, scope, NegotiationRequest{
		Hashes: []string{hash2},
		Sizes:  map[string]int64{hash2: 1100},
	})
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got: %v", err)
	}
}
