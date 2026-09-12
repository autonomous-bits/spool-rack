package postgres

import (
	"errors"
	"testing"
)

func TestPGStore_AssetManagementAndQuota(t *testing.T) {
	store, ctx := newTestStore(t)
	defer store.Close()

	tenantID, err := store.CreateTenant(ctx, "asset-test-tenant")
	if err != nil {
		t.Fatalf("CreateTenant failed: %v", err)
	}
	tenantCtx := mustTenantContext(t, store, ctx, tenantID)

	repoID, err := store.CreateRepository(tenantCtx, "asset-test-repo")
	if err != nil {
		t.Fatalf("CreateRepository failed: %v", err)
	}

	// Set tenant quota to 1000 bytes
	if err := store.SetTenantStorageQuota(tenantCtx, tenantID, 1000); err != nil {
		t.Fatalf("SetTenantStorageQuota failed: %v", err)
	}

	hash1 := "1111111111111111111111111111111111111111111111111111111111111111"
	hash2 := "2222222222222222222222222222222222222222222222222222222222222222"
	hash3 := "3333333333333333333333333333333333333333333333333333333333333333"

	// 1. Initial missing check: all candidate hashes should be missing
	missing, err := store.ListMissingAssets(tenantCtx, repoID, []string{hash1, hash2, hash3})
	if err != nil {
		t.Fatalf("ListMissingAssets failed: %v", err)
	}
	if len(missing) != 3 {
		t.Fatalf("expected 3 missing assets, got %d", len(missing))
	}

	// 2. Admit upload within quota: 600 bytes <= 1000 bytes
	if err := store.AdmitAssetUpload(tenantCtx, repoID, hash1, 600); err != nil {
		t.Fatalf("AdmitAssetUpload failed: %v", err)
	}

	// 3. Register asset1
	if err := store.RegisterAsset(tenantCtx, repoID, hash1, 600, "text/plain"); err != nil {
		t.Fatalf("RegisterAsset failed: %v", err)
	}

	// 4. Retrieve asset1 metadata
	meta, err := store.GetAssetMetadata(tenantCtx, repoID, hash1)
	if err != nil {
		t.Fatalf("GetAssetMetadata failed: %v", err)
	}
	if meta.Hash != hash1 || meta.SizeBytes != 600 || meta.MIMEType != "text/plain" {
		t.Fatalf("unexpected asset metadata: %+v", meta)
	}

	// 5. Deduplication: admitting or registering identical asset hash again should succeed without consuming extra quota
	if err := store.AdmitAssetUpload(tenantCtx, repoID, hash1, 600); err != nil {
		t.Fatalf("AdmitAssetUpload on duplicate failed: %v", err)
	}
	if err := store.RegisterAsset(tenantCtx, repoID, hash1, 600, "text/plain"); err != nil {
		t.Fatalf("RegisterAsset on duplicate failed: %v", err)
	}

	// 6. Missing check should now only return hash2 and hash3
	missing, err = store.ListMissingAssets(tenantCtx, repoID, []string{hash1, hash2, hash3})
	if err != nil {
		t.Fatalf("ListMissingAssets failed: %v", err)
	}
	if len(missing) != 2 || missing[0] != hash2 || missing[1] != hash3 {
		t.Fatalf("expected [hash2, hash3] missing, got %v", missing)
	}

	// 7. Quota exceeded: currently 600 bytes used. Adding 500 bytes exceeds 1000 limit
	if err := store.AdmitAssetUpload(tenantCtx, repoID, hash2, 500); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got: %v", err)
	}
	if err := store.RegisterAsset(tenantCtx, repoID, hash2, 500, "application/octet-stream"); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded on RegisterAsset, got: %v", err)
	}

	// 8. Non-existent asset lookup should return ErrAssetNotFound
	if _, err := store.GetAssetMetadata(tenantCtx, repoID, hash2); !errors.Is(err, ErrAssetNotFound) {
		t.Fatalf("expected ErrAssetNotFound, got: %v", err)
	}
}
