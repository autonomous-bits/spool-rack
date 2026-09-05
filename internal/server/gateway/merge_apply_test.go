package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/review"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
)

func TestMergeLeaseRequiresContributorAndUsesPathScope(t *testing.T) {
	t.Parallel()
	engine := &fakeMergeFinalizer{}
	gateway := New(WithMergeEngine(engine), WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
		"writer": {Role: auth.RoleContributor, Subject: "alice"},
		"viewer": {Role: auth.RoleViewer, Subject: "viewer"},
	})))
	for _, test := range []struct {
		token string
		want  int
	}{{"viewer", http.StatusForbidden}, {"writer", http.StatusCreated}} {
		rec := httptest.NewRecorder()
		request := mergeJSONRequest(test.token, "repo-path", "/merge/lease", `{"sourceBranch":"feature","targetBranch":"main"}`)
		gateway.Routes().ServeHTTP(rec, request)
		assertStatus(t, rec, test.want)
	}
	if engine.lease.RepoID != "repo-path" || engine.lease.Subject != "alice" {
		t.Fatalf("lease = %+v, want URL repo and authenticated subject", engine.lease)
	}
}

func TestMergeApplyStrictlyValidatesAndReturnsResult(t *testing.T) {
	t.Parallel()
	engine := &fakeMergeFinalizer{result: review.ApplyResult{Branch: "main", HeadCommit: "merged", SnapshotRoot: "snapshot", PackHash: "pack"}}
	gateway := New(WithMergeEngine(engine), WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
		"writer": {Role: auth.RoleContributor, Subject: "alice"},
	})))
	rec := httptest.NewRecorder()
	gateway.Routes().ServeHTTP(rec, mergeJSONRequest("writer", "repo-path", "/merge/apply",
		`{"sourceBranch":"feature","targetBranch":"main","leaseToken":"lease","resolutions":[],"author":"alice","message":"merge"}`))
	assertStatus(t, rec, http.StatusOK)
	var result review.ApplyResult
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.HeadCommit != "merged" || engine.apply.Subject != "alice" || engine.repoID != "repo-path" {
		t.Fatalf("result/apply scope = %+v/%+v/%q", result, engine.apply, engine.repoID)
	}

	invalid := httptest.NewRecorder()
	gateway.Routes().ServeHTTP(invalid, mergeJSONRequest("writer", "repo-path", "/merge/apply",
		`{"sourceBranch":"feature","targetBranch":"main","author":"alice","message":"merge","unknown":true}`))
	assertStatus(t, invalid, http.StatusBadRequest)
}

func mergeJSONRequest(token, repoID, suffix, body string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/api/v1/repos/"+repoID+suffix, bytes.NewBufferString(body))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set(HeaderTenantID, "tenant-1")
	request.Header.Set("Content-Type", "application/json")
	return request
}

type fakeMergeFinalizer struct {
	lease  postgres.MergeLease
	apply  review.ApplyRequest
	repoID string
	result review.ApplyResult
}

func (f *fakeMergeFinalizer) AcquireLease(_ context.Context, _, repoID string, request review.LeaseRequest) (postgres.MergeLease, error) {
	f.repoID = repoID
	f.lease = postgres.MergeLease{RepoID: repoID, TargetBranch: request.TargetBranch, Subject: request.Subject, Token: "lease", ExpiresAt: time.Now().Add(time.Minute)}
	return f.lease, nil
}

func (f *fakeMergeFinalizer) Apply(_ context.Context, _, repoID string, request review.ApplyRequest) (review.ApplyResult, error) {
	f.repoID = repoID
	f.apply = request
	return f.result, nil
}

func (f *fakeMergeFinalizer) ReleaseLease(context.Context, string, string, string, string, string) error {
	return nil
}
