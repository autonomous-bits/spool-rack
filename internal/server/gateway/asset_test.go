package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/asset"
	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	"lukechampine.com/blake3"
)

type fakeGatewayAssetStore struct {
	assets map[string]postgres.AssetRecord
	quota  int64
	used   int64
}

func newFakeGatewayAssetStore() *fakeGatewayAssetStore {
	return &fakeGatewayAssetStore{
		assets: make(map[string]postgres.AssetRecord),
	}
}

func (f *fakeGatewayAssetStore) AdmitAssetUpload(_ context.Context, _ string, hash string, sizeBytes int64) error {
	if _, ok := f.assets[hash]; ok {
		return nil
	}
	if f.quota > 0 && (f.used+sizeBytes) > f.quota {
		return postgres.ErrQuotaExceeded
	}
	return nil
}

func (f *fakeGatewayAssetStore) RegisterAsset(_ context.Context, repoID, hash string, sizeBytes int64, mimeType string) error {
	if _, ok := f.assets[hash]; ok {
		return nil
	}
	if f.quota > 0 && (f.used+sizeBytes) > f.quota {
		return postgres.ErrQuotaExceeded
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

func (f *fakeGatewayAssetStore) GetAssetMetadata(_ context.Context, _ string, hash string) (postgres.AssetRecord, error) {
	rec, ok := f.assets[hash]
	if !ok {
		return postgres.AssetRecord{}, postgres.ErrAssetNotFound
	}
	return rec, nil
}

func (f *fakeGatewayAssetStore) ListMissingAssets(_ context.Context, _ string, candidateHashes []string) ([]string, error) {
	var missing []string
	for _, h := range candidateHashes {
		if _, ok := f.assets[h]; !ok {
			missing = append(missing, h)
		}
	}
	return missing, nil
}

func TestGateway_AssetLifecycle(t *testing.T) {
	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver: %v", err)
	}
	store := newFakeGatewayAssetStore()
	store.quota = 2000
	assetSvc := asset.NewService(driver, store)

	verifier := auth.NewStaticVerifier(map[string]auth.Claims{
		"contributor-token": {Role: auth.RoleContributor, Subject: "writer"},
		"viewer-token":      {Role: auth.RoleViewer, Subject: "reader"},
	})

	gw := New(
		WithCASDriver(driver),
		WithAssetService(assetSvc),
		WithVerifier(verifier),
	)

	payload := []byte("hello asset world! this is a test specification asset.")
	sum := blake3.Sum256(payload)
	hash := hexEncode(sum[:])

	// 1. Negotiate assets with contributor token
	body, _ := json.Marshal(asset.NegotiationRequest{
		Hashes: []string{hash},
		Sizes:  map[string]int64{hash: int64(len(payload))},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/repos/repo-1/assets/negotiate", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer contributor-token")
	req.Header.Set("X-Tenant-Id", "tenant-1")
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("negotiate returned %d, body: %s", rec.Code, rec.Body.String())
	}
	var negResult asset.NegotiationResult
	if err := json.Unmarshal(rec.Body.Bytes(), &negResult); err != nil {
		t.Fatalf("unmarshal negotiation result: %v", err)
	}
	if len(negResult.Missing) != 1 || negResult.Missing[0] != hash {
		t.Fatalf("expected missing [%s], got %v", hash, negResult.Missing)
	}

	// 2. Upload asset with viewer token should be rejected (403)
	upReqViewer := httptest.NewRequest(http.MethodPut, "/api/v1/repos/repo-1/assets/blobs/"+hash, bytes.NewReader(payload))
	upReqViewer.Header.Set("Authorization", "Bearer viewer-token")
	upReqViewer.Header.Set("X-Tenant-Id", "tenant-1")
	upReqViewer.Header.Set("Content-Type", "text/plain")
	recViewer := httptest.NewRecorder()
	gw.Routes().ServeHTTP(recViewer, upReqViewer)
	if recViewer.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for viewer upload, got %d", recViewer.Code)
	}

	// 3. Upload asset with contributor token should succeed (201)
	upReq := httptest.NewRequest(http.MethodPut, "/api/v1/repos/repo-1/assets/blobs/"+hash, bytes.NewReader(payload))
	upReq.Header.Set("Authorization", "Bearer contributor-token")
	upReq.Header.Set("X-Tenant-Id", "tenant-1")
	upReq.Header.Set("Content-Type", "text/plain")
	recUp := httptest.NewRecorder()
	gw.Routes().ServeHTTP(recUp, upReq)
	if recUp.Code != http.StatusCreated {
		t.Fatalf("upload returned %d, body: %s", recUp.Code, recUp.Body.String())
	}

	// 4. Stream asset with viewer token: full read
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/repos/repo-1/assets/blobs/"+hash, nil)
	getReq.Header.Set("Authorization", "Bearer viewer-token")
	getReq.Header.Set("X-Tenant-Id", "tenant-1")
	recGet := httptest.NewRecorder()
	gw.Routes().ServeHTTP(recGet, getReq)

	if recGet.Code != http.StatusOK {
		t.Fatalf("stream returned %d, body: %s", recGet.Code, recGet.Body.String())
	}
	if recGet.Header().Get("Content-Type") != "text/plain" {
		t.Fatalf("expected Content-Type text/plain, got %s", recGet.Header().Get("Content-Type"))
	}
	if !bytes.Equal(recGet.Body.Bytes(), payload) {
		t.Fatalf("streamed content mismatch: got %q, want %q", recGet.Body.String(), string(payload))
	}

	// 5. Stream asset with HTTP Range request
	rangeReq := httptest.NewRequest(http.MethodGet, "/api/v1/repos/repo-1/assets/blobs/"+hash, nil)
	rangeReq.Header.Set("Authorization", "Bearer viewer-token")
	rangeReq.Header.Set("X-Tenant-Id", "tenant-1")
	rangeReq.Header.Set("Range", "bytes=0-10")
	recRange := httptest.NewRecorder()
	gw.Routes().ServeHTTP(recRange, rangeReq)

	if recRange.Code != http.StatusPartialContent {
		t.Fatalf("expected 206 Partial Content, got %d", recRange.Code)
	}
	if !bytes.Equal(recRange.Body.Bytes(), payload[:11]) {
		t.Fatalf("range content mismatch: got %q, want %q", recRange.Body.String(), string(payload[:11]))
	}

	// 6. Mirrored workspace route: GET /api/v1/workspaces/repo-1/assets/blobs/{hash}
	wsReq := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces/repo-1/assets/blobs/"+hash, nil)
	wsReq.Header.Set("Authorization", "Bearer viewer-token")
	wsReq.Header.Set("X-Tenant-Id", "tenant-1")
	recWs := httptest.NewRecorder()
	gw.Routes().ServeHTTP(recWs, wsReq)
	if recWs.Code != http.StatusOK {
		t.Fatalf("workspace stream returned %d", recWs.Code)
	}

	// 7. Non-existent asset returns 404
	badHash := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	badReq := httptest.NewRequest(http.MethodGet, "/api/v1/repos/repo-1/assets/blobs/"+badHash, nil)
	badReq.Header.Set("Authorization", "Bearer viewer-token")
	badReq.Header.Set("X-Tenant-Id", "tenant-1")
	recBad := httptest.NewRecorder()
	gw.Routes().ServeHTTP(recBad, badReq)
	if recBad.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing asset, got %d", recBad.Code)
	}
}

func hexEncode(b []byte) string {
	const hextable = "0123456789abcdef"
	dst := make([]byte, len(b)*2)
	for i, v := range b {
		dst[i*2] = hextable[v>>4]
		dst[i*2+1] = hextable[v&0x0f]
	}
	return string(dst)
}
