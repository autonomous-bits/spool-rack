package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	serversync "github.com/autonomous-bits/spool-rack/internal/server/sync"
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
	if got := rec.Header().Get("Content-Type"); got != "application/vnd.spool-rack.pull-envelope" {
		t.Fatalf("Content-Type = %q, want application/vnd.spool-rack.pull-envelope", got)
	}
	if got := rec.Header().Get("Content-Encoding"); got != "zstd" {
		t.Fatalf("Content-Encoding = %q, want zstd", got)
	}
	if got := rec.Header().Get("X-Spool-Pull-Format"); got != "2" {
		t.Fatalf("X-Spool-Pull-Format = %q, want 2", got)
	}
	if got := rec.Header().Get("X-Spool-Head-Commit"); got != head {
		t.Fatalf("X-Spool-Head-Commit = %q, want %q", got, head)
	}
	manifest, packs := decodePullEnvelope(t, rec.Body.Bytes())
	if manifest.Version != serversync.PullEnvelopeFormatV2 || manifest.Head != head {
		t.Fatalf("manifest = %+v, want v2 head %q", manifest, head)
	}
	if len(packs) != 3 {
		t.Fatalf("pack count = %d, want 3", len(packs))
	}
	for i, pack := range packs {
		if _, err := serversync.UnmarshalPackFrameV2(pack); err != nil {
			t.Fatalf("pack %d is not canonical v2: %v", i, err)
		}
		if manifest.Packs[i].Hash != hashPackBytes(pack) ||
			manifest.Packs[i].Format != serversync.PackFormatV2 ||
			manifest.Packs[i].Length != uint64(len(pack)) {
			t.Fatalf("manifest pack %d = %+v, want v2 hash and length", i, manifest.Packs[i])
		}
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
	manifest, packs := decodePullEnvelope(t, rec.Body.Bytes())
	if manifest.Head != head || len(packs) != 1 {
		t.Fatalf("delta envelope = manifest:%+v packs:%q, want one head pack", manifest, packs)
	}
	frame, err := serversync.UnmarshalPackFrameV2(packs[0])
	if err != nil || frame.Target.ID != head {
		t.Fatalf("delta pack = %+v, %v; want canonical v2 pack for head %q", frame, err, head)
	}
	if manifest.Packs[0].Hash != hashPackBytes(packs[0]) ||
		manifest.Packs[0].Format != serversync.PackFormatV2 ||
		manifest.Packs[0].Length != uint64(len(packs[0])) {
		t.Fatalf("delta manifest pack = %+v, want head pack metadata", manifest.Packs[0])
	}
	if got := rec.Header().Get("X-Spool-Head-Commit"); got != head {
		t.Fatalf("X-Spool-Head-Commit = %q, want %q", got, head)
	}
	if store.isAncestorCalls != 1 {
		t.Fatalf("IsAncestor() calls = %d, want 1", store.isAncestorCalls)
	}
}

func TestPullPreservesV2PackFormatInManifest(t *testing.T) {
	t.Parallel()

	object := []byte("v2 snapshot")
	commit := serversync.CommitFrameV2{
		Version: serversync.CommitFormatV2, SnapshotRoot: serversync.ContentID(object),
		Author: "Ada", Message: "initial v2 commit",
	}
	target, err := commit.Identity()
	if err != nil {
		t.Fatalf("CommitFrameV2.Identity() error = %v", err)
	}
	packData, err := serversync.MarshalPackFrameV2(serversync.PackFrameV2{
		Version: serversync.PackFormatV2, Target: target, Commits: []serversync.CommitFrameV2{commit},
		Objects: []serversync.PackObjectV2{{ID: serversync.ContentID(object), Data: object}},
	})
	if err != nil {
		t.Fatalf("MarshalPackFrameV2() error = %v", err)
	}
	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver() error = %v", err)
	}
	scope, err := cas.NewScope("tenant-1", "repo-1")
	if err != nil {
		t.Fatalf("NewScope() error = %v", err)
	}
	packHash := hashPackBytes(packData)
	if err := driver.WritePack(context.Background(), scope, packHash, bytes.NewReader(packData)); err != nil {
		t.Fatalf("WritePack() error = %v", err)
	}
	store := &fakeGatewayBranchStore{
		branchHeads: map[gatewayBranchKey]string{{repoID: "repo-1", branch: "main"}: target.ID},
		packRanges: []postgres.PackRange{{
			PackHash: packHash, TargetCommitID: target.ID, Format: serversync.PackFormatV2,
		}},
	}
	rec := httptest.NewRecorder()
	New(WithCASDriver(driver), WithBranchStore(store)).Routes().ServeHTTP(
		rec,
		newPullRequest("any-token", "tenant-1", "repo-1", "main", ""),
	)

	assertStatus(t, rec, http.StatusOK)
	manifest, packs := decodePullEnvelope(t, rec.Body.Bytes())
	if len(manifest.Packs) != 1 || manifest.Packs[0].Format != serversync.PackFormatV2 ||
		manifest.Packs[0].Hash != packHash || !bytes.Equal(packs[0], packData) {
		t.Fatalf("v2 envelope = manifest:%+v packs:%q, want the v2 pack", manifest, packs)
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

func TestPullEnvelopeRejectsTamperedPack(t *testing.T) {
	t.Parallel()

	driver, store, _, _, _ := pullFixture(t)
	rec := httptest.NewRecorder()
	New(WithCASDriver(driver), WithBranchStore(store)).Routes().ServeHTTP(
		rec,
		newPullRequest("any-token", "tenant-1", "repo-1", "main", ""),
	)
	assertStatus(t, rec, http.StatusOK)

	envelope := decodeZstd(t, rec.Body.Bytes())
	envelope[len(envelope)-1] ^= 0xff
	if _, _, err := serversync.UnmarshalPullEnvelopeV2(envelope); !errors.Is(err, serversync.ErrInvalidPullEnvelope) {
		t.Fatalf("UnmarshalPullEnvelopeV2(tampered) error = %v, want invalid pull envelope", err)
	}
}

func pullFixture(t *testing.T) (*cas.LocalDriver, *fakeGatewayBranchStore, string, string, string) {
	t.Helper()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver() error = %v", err)
	}
	scope, err := cas.NewScope("tenant-1", "repo-1")
	if err != nil {
		t.Fatalf("NewScope() error = %v", err)
	}
	makePack := func(base serversync.CommitIdentity, label string) (serversync.CommitIdentity, []byte) {
		t.Helper()
		snapshot := []byte(label + " snapshot")
		parents := []serversync.CommitIdentity{}
		if base.ID != "" {
			parents = []serversync.CommitIdentity{base}
		}
		commit := serversync.CommitFrameV2{
			Version: serversync.CommitFormatV2, Parents: parents,
			SnapshotRoot: serversync.ContentID(snapshot), Author: "Ada", Message: label,
		}
		target, err := commit.Identity()
		if err != nil {
			t.Fatal(err)
		}
		pack, err := serversync.MarshalPackFrameV2(serversync.PackFrameV2{
			Version: serversync.PackFormatV2, Base: base, Target: target,
			Commits: []serversync.CommitFrameV2{commit},
			Objects: []serversync.PackObjectV2{{ID: serversync.ContentID(snapshot), Data: snapshot}},
		})
		if err != nil {
			t.Fatal(err)
		}
		return target, pack
	}
	rootIdentity, rootPack := makePack(serversync.CommitIdentity{}, "root")
	middleIdentity, middlePack := makePack(rootIdentity, "middle")
	headIdentity, headPack := makePack(middleIdentity, "head")
	for _, pack := range [][]byte{rootPack, middlePack, headPack} {
		if err := driver.WritePack(context.Background(), scope, hashPackBytes(pack), bytes.NewReader(pack)); err != nil {
			t.Fatalf("WritePack() error = %v", err)
		}
	}
	root, middle, head := rootIdentity.ID, middleIdentity.ID, headIdentity.ID
	return driver, &fakeGatewayBranchStore{
		branchHeads: map[gatewayBranchKey]string{{repoID: "repo-1", branch: "main"}: head},
		parentOf:    map[string]string{head: middle, middle: root},
		packRanges: []postgres.PackRange{
			{PackHash: hashPackBytes(headPack), BaseCommitID: middle, TargetCommitID: head, Format: serversync.PackFormatV2},
			{PackHash: hashPackBytes(middlePack), BaseCommitID: root, TargetCommitID: middle, Format: serversync.PackFormatV2},
			{PackHash: hashPackBytes(rootPack), BaseCommitID: "", TargetCommitID: root, Format: serversync.PackFormatV2},
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

func decodePullEnvelope(t *testing.T, compressed []byte) (serversync.PullManifestV2, [][]byte) {
	t.Helper()

	manifest, packs, err := serversync.UnmarshalPullEnvelopeV2(decodeZstd(t, compressed))
	if err != nil {
		t.Fatalf("UnmarshalPullEnvelopeV2() error = %v", err)
	}
	return manifest, packs
}
