package gateway

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	"github.com/klauspost/compress/zstd"
)

func TestPullStreamsFullHistoryForViewer(t *testing.T) {
	t.Parallel()

	driver, store, _, _, head := pullFixture(t)
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"viewer-token": {Role: auth.RoleViewer, Subject: "viewer"},
		})),
	)
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, newPullRequest("viewer-token", "tenant-1", "repo-1", "main", ""))

	assertStatus(t, rec, http.StatusOK)
	if got := rec.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("Content-Type = %q, want application/octet-stream", got)
	}
	if got := rec.Header().Get("Content-Encoding"); got != "zstd" {
		t.Fatalf("Content-Encoding = %q, want zstd", got)
	}
	if got := rec.Header().Get("X-Spool-Head-Commit"); got != head {
		t.Fatalf("X-Spool-Head-Commit = %q, want %q", got, head)
	}
	if got := decodeZstd(t, rec.Body.Bytes()); string(got) != "root-packmiddle-packhead-pack" {
		t.Fatalf("decoded pull payload = %q, want full ordered history", got)
	}
	if store.getBranchRefCalls != 1 || store.isAncestorCalls != 0 {
		t.Fatalf("metadata calls = get:%d ancestor:%d, want get:1 ancestor:0", store.getBranchRefCalls, store.isAncestorCalls)
	}
}

func TestPullStreamsDeltaFromKnownAncestor(t *testing.T) {
	t.Parallel()

	driver, store, _, middle, head := pullFixture(t)
	gw := New(WithCASDriver(driver), WithBranchStore(store))
	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, newPullRequest("any-token", "tenant-1", "repo-1", "main", middle))

	assertStatus(t, rec, http.StatusOK)
	if got := decodeZstd(t, rec.Body.Bytes()); string(got) != "head-pack" {
		t.Fatalf("decoded delta pull payload = %q, want head pack", got)
	}
	if got := rec.Header().Get("X-Spool-Head-Commit"); got != head {
		t.Fatalf("X-Spool-Head-Commit = %q, want %q", got, head)
	}
	if store.isAncestorCalls != 1 {
		t.Fatalf("IsAncestor() calls = %d, want 1", store.isAncestorCalls)
	}
}

func TestPullNegotiatesUpToDateAndDiverged(t *testing.T) {
	t.Parallel()

	driver, store, _, _, head := pullFixture(t)
	gw := New(WithCASDriver(driver), WithBranchStore(store))

	upToDate := httptest.NewRecorder()
	gw.Routes().ServeHTTP(upToDate, newPullRequest("any-token", "tenant-1", "repo-1", "main", head))
	assertStatus(t, upToDate, http.StatusNoContent)
	if got := upToDate.Header().Get("X-Spool-Head-Commit"); got != head {
		t.Fatalf("up-to-date head = %q, want %q", got, head)
	}

	diverged := httptest.NewRecorder()
	gw.Routes().ServeHTTP(diverged, newPullRequest("any-token", "tenant-1", "repo-1", "main", hashCommitString("unrelated")))
	assertStatus(t, diverged, http.StatusConflict)
	assertErrorCode(t, diverged, ErrorCodeConflict)
	assertBodyValue(t, decodeBody(t, diverged), "currentHead", head)
}

func TestPullRejectsInsufficientRoleAndMissingBranch(t *testing.T) {
	t.Parallel()

	driver, store, _, _, _ := pullFixture(t)
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"invalid-role": {Role: "unknown", Subject: "unknown"},
			"viewer-token": {Role: auth.RoleViewer, Subject: "viewer"},
		})),
	)

	forbidden := httptest.NewRecorder()
	gw.Routes().ServeHTTP(forbidden, newPullRequest("invalid-role", "tenant-1", "repo-1", "main", ""))
	assertStatus(t, forbidden, http.StatusForbidden)

	missingBranch := httptest.NewRecorder()
	req := newPullRequest("viewer-token", "tenant-1", "repo-1", "", "")
	gw.Routes().ServeHTTP(missingBranch, req)
	assertStatus(t, missingBranch, http.StatusBadRequest)
}

func TestPullReturnsNotImplementedWhenUnconfigured(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	New().Routes().ServeHTTP(rec, newPullRequest("any-token", "tenant-1", "repo-1", "main", ""))
	assertStatus(t, rec, http.StatusNotImplemented)
	assertErrorCode(t, rec, ErrorCodeNotImplemented)
}

func TestPullFailsWhenSelectedPackIsMissing(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver() error = %v", err)
	}
	head := hashCommitString("head")
	store := &fakeGatewayBranchStore{
		branchHeads: map[gatewayBranchKey]string{{repoID: "repo-1", branch: "main"}: head},
		packRanges: []postgres.PackRange{{
			PackHash:       strings.Repeat("a", 64),
			TargetCommitID: head,
		}},
	}
	rec := httptest.NewRecorder()
	New(WithCASDriver(driver), WithBranchStore(store)).Routes().ServeHTTP(
		rec,
		newPullRequest("any-token", "tenant-1", "repo-1", "main", ""),
	)

	assertStatus(t, rec, http.StatusInternalServerError)
	assertErrorCode(t, rec, ErrorCodeInternal)
}

func pullFixture(t *testing.T) (*cas.LocalDriver, *fakeGatewayBranchStore, string, string, string) {
	t.Helper()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver() error = %v", err)
	}
	root := hashCommitString("root")
	middle := hashCommitString("middle")
	head := hashCommitString("head")
	rootPack := []byte("root-pack")
	middlePack := []byte("middle-pack")
	headPack := []byte("head-pack")
	scope, err := cas.NewScope("tenant-1", "repo-1")
	if err != nil {
		t.Fatalf("NewScope() error = %v", err)
	}
	for _, pack := range [][]byte{rootPack, middlePack, headPack} {
		if err := driver.WritePack(context.Background(), scope, hashPackBytes(pack), bytes.NewReader(pack)); err != nil {
			t.Fatalf("WritePack() error = %v", err)
		}
	}
	return driver, &fakeGatewayBranchStore{
		branchHeads: map[gatewayBranchKey]string{{repoID: "repo-1", branch: "main"}: head},
		parentOf:    map[string]string{head: middle, middle: root},
		packRanges: []postgres.PackRange{
			{PackHash: hashPackBytes(headPack), BaseCommitID: middle, TargetCommitID: head},
			{PackHash: hashPackBytes(middlePack), BaseCommitID: root, TargetCommitID: middle},
			{PackHash: hashPackBytes(rootPack), BaseCommitID: "", TargetCommitID: root},
		},
	}, root, middle, head
}

func newPullRequest(token, tenantID, repoID, branch, knownCommit string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/repos/"+repoID+"/pull?branch="+branch+"&knownCommit="+knownCommit, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set(HeaderTenantID, tenantID)
	return req
}

func decodeZstd(t *testing.T, compressed []byte) []byte {
	t.Helper()

	decoder, err := zstd.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer decoder.Close()
	decoded, err := io.ReadAll(decoder)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	return decoded
}
