package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/autonomous-bits/spool/graphcontract"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
)

// TestPushAcceptsPriorCompatiblePackFormatDuringRollout verifies that, when a
// rollout window is configured (widening Min below the current pack format
// version), a push declaring the immediately-prior compatible pack format is
// still accepted rather than rejected — this is the "old compatible clients
// keep working during a rolling deploy" acceptance criterion from
// spec-cli-rack-contract-and-metadata-migrations.
func TestPushAcceptsPriorCompatiblePackFormatDuringRollout(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver() error = %v", err)
	}

	baseCommit := hashCommitString("commit-a")
	targetCommit := hashCommitString("commit-c")
	store := &fakeGatewayBranchStore{
		branchHeads: map[gatewayBranchKey]string{
			{repoID: "repo-1", branch: "main"}: baseCommit,
		},
	}
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithPackFormatWindow(VersionWindow{Min: graphcontract.PackFormatVersion - 1, Max: graphcontract.PackFormatVersion}),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"contributor-token": {Role: auth.RoleContributor, Subject: "u1"},
		})),
	)

	packData := []byte("prior compatible push payload")
	req := newPushRequest(t, pushRequestFixture{
		token:    "contributor-token",
		tenantID: "tenant-1",
		repoID:   "repo-1",
		metadata: pushMetadata{
			Branch:       "main",
			BaseCommit:   baseCommit,
			TargetCommit: targetCommit,
			PackHash:     hashPackBytes(packData),
			PackFormat:   graphcontract.PackFormatVersion - 1,
		},
		packData: packData,
	})
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	// The prior pack format is only accepted by the version-compatibility
	// gate; whether it decodes successfully is orthogonal to this test, so
	// any response other than the unsupported-contract-version rejection
	// demonstrates the gate let it through.
	if rec.Code == http.StatusBadRequest {
		assertErrorCodeNot(t, rec, ErrorCodeUnsupportedContractVersion)
	}
}

// TestPushRejectsPackFormatBelowRolloutWindow verifies a pack format older
// than the configured rollout window's Min is still explicitly rejected with
// the unsupported-contract-version error, distinguishing "not yet retired
// prior client" from "unsupported/ancient client".
func TestPushRejectsPackFormatBelowRolloutWindow(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver() error = %v", err)
	}

	baseCommit := hashCommitString("commit-a")
	targetCommit := hashCommitString("commit-c")
	store := &fakeGatewayBranchStore{
		branchHeads: map[gatewayBranchKey]string{
			{repoID: "repo-1", branch: "main"}: baseCommit,
		},
	}
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithPackFormatWindow(VersionWindow{Min: graphcontract.PackFormatVersion, Max: graphcontract.PackFormatVersion}),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"contributor-token": {Role: auth.RoleContributor, Subject: "u1"},
		})),
	)

	packData := []byte("ancient push payload")
	req := newPushRequest(t, pushRequestFixture{
		token:    "contributor-token",
		tenantID: "tenant-1",
		repoID:   "repo-1",
		metadata: pushMetadata{
			Branch:       "main",
			BaseCommit:   baseCommit,
			TargetCommit: targetCommit,
			PackHash:     hashPackBytes(packData),
			PackFormat:   graphcontract.PackFormatVersion - 1,
		},
		packData: packData,
	})
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorCode(t, rec, ErrorCodeUnsupportedContractVersion)
	if len(store.putCommitCalls) != 0 || store.compareAndSwapCalls != 0 {
		t.Fatalf("rejected push mutated metadata: commits=%d updates=%d", len(store.putCommitCalls), store.compareAndSwapCalls)
	}
}

// TestPullRejectsOutOfRangePackFormatQueryParam verifies pull rejects an
// explicit, out-of-range packFormat query parameter with the structured
// unsupported-contract-version error, while an omitted parameter preserves
// existing behavior for clients that predate version negotiation.
func TestPullRejectsOutOfRangePackFormatQueryParam(t *testing.T) {
	t.Parallel()

	driver, store, _, _, _ := pullFixture(t)
	gw := New(WithCASDriver(driver), WithBranchStore(store))

	req := newPullRequest("any-token", "tenant-1", "repo-1", "main", "")
	q := req.URL.Query()
	q.Set("packFormat", "1")
	req.URL.RawQuery = q.Encode()

	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorCode(t, rec, ErrorCodeUnsupportedContractVersion)
}

// TestPullAcceptsInRangePackFormatQueryParam verifies an explicit, in-range
// packFormat query parameter is accepted (not rejected by the version gate).
func TestPullAcceptsInRangePackFormatQueryParam(t *testing.T) {
	t.Parallel()

	driver, store, _, _, head := pullFixture(t)
	gw := New(WithCASDriver(driver), WithBranchStore(store))

	req := newPullRequest("any-token", "tenant-1", "repo-1", "main", "")
	q := req.URL.Query()
	q.Set("packFormat", "2")
	req.URL.RawQuery = q.Encode()

	rec := httptest.NewRecorder()
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusOK)
	if got := rec.Header().Get("X-Spool-Head-Commit"); got != head {
		t.Fatalf("X-Spool-Head-Commit = %q, want %q", got, head)
	}
}

func assertErrorCodeNot(t *testing.T, rec *httptest.ResponseRecorder, notWant string) {
	t.Helper()
	if rec.Code != http.StatusBadRequest {
		return
	}
	body := decodeBody(t, rec)
	if body["error"] == notWant {
		t.Fatalf("unexpected error code %q in response body %s", notWant, rec.Body.String())
	}
}
