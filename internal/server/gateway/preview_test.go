package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/review"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
)

func TestMergePreviewAllowsViewerAndUsesContextScope(t *testing.T) {
	t.Parallel()

	engine := &fakePreviewEngine{preview: &review.MergePreview{
		SourceBranch:   "feature",
		TargetBranch:   "main",
		CanFastForward: true,
		CleanChanges:   []review.Change{},
		Conflicts:      []review.ConflictToken{},
		Resolutions:    []review.ResolutionRequirement{},
	}}
	gw := New(
		WithPreviewEngine(engine),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"viewer-token": {Role: auth.RoleViewer, Subject: "viewer"},
		})),
	)
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, newPreviewRequest("viewer-token", "tenant-1", "repo-from-path", `{"sourceBranch":"feature","targetBranch":"main"}`))

	assertStatus(t, rec, http.StatusOK)
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if engine.tenantID != "tenant-1" || engine.repoID != "repo-from-path" {
		t.Fatalf("engine scope = tenant %q repo %q, want tenant-1/repo-from-path", engine.tenantID, engine.repoID)
	}
	if engine.sourceBranch != "feature" || engine.targetBranch != "main" {
		t.Fatalf("engine branches = %q/%q, want feature/main", engine.sourceBranch, engine.targetBranch)
	}
	var preview review.MergePreview
	if err := json.NewDecoder(rec.Body).Decode(&preview); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !preview.CanFastForward || preview.SourceBranch != "feature" || preview.TargetBranch != "main" {
		t.Fatalf("preview = %+v, want structured engine result", preview)
	}
}

func TestMergePreviewRejectsUnauthorizedAndInvalidRequests(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		token string
		body  string
		want  int
	}{
		{name: "insufficient role", token: "unknown-role", body: `{"sourceBranch":"feature","targetBranch":"main"}`, want: http.StatusForbidden},
		{name: "missing source branch", token: "viewer-token", body: `{"targetBranch":"main"}`, want: http.StatusBadRequest},
		{name: "unknown field", token: "viewer-token", body: `{"sourceBranch":"feature","targetBranch":"main","repoID":"other"}`, want: http.StatusBadRequest},
		{name: "malformed JSON", token: "viewer-token", body: `{"sourceBranch":"feature","targetBranch":"main"`, want: http.StatusBadRequest},
		{name: "multiple JSON values", token: "viewer-token", body: `{"sourceBranch":"feature","targetBranch":"main"} {}`, want: http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine := &fakePreviewEngine{preview: &review.MergePreview{}}
			gw := New(
				WithPreviewEngine(engine),
				WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
					"viewer-token": {Role: auth.RoleViewer, Subject: "viewer"},
					"unknown-role": {Role: "unknown", Subject: "unknown"},
				})),
			)
			rec := httptest.NewRecorder()

			gw.Routes().ServeHTTP(rec, newPreviewRequest(tc.token, "tenant-1", "repo-1", tc.body))

			assertStatus(t, rec, tc.want)
			if tc.want == http.StatusBadRequest {
				assertErrorCode(t, rec, ErrorCodeBadRequest)
			}
			if engine.calls != 0 {
				t.Fatalf("PreviewMerge() calls = %d, want 0", engine.calls)
			}
		})
	}
}

func TestMergePreviewMapsEngineErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want int
		code string
	}{
		{name: "missing branch", err: errors.Join(errors.New("resolve source"), postgres.ErrBranchNotFound), want: http.StatusNotFound, code: ErrorCodeNotFound},
		{name: "invalid engine request", err: review.ErrInvalidPreviewRequest, want: http.StatusBadRequest, code: ErrorCodeBadRequest},
		{name: "unexpected failure", err: errors.New("database unavailable"), want: http.StatusInternalServerError, code: ErrorCodeInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			New(WithPreviewEngine(&fakePreviewEngine{err: tc.err})).Routes().ServeHTTP(
				rec,
				newPreviewRequest("any-token", "tenant-1", "repo-1", `{"sourceBranch":"feature","targetBranch":"main"}`),
			)

			assertStatus(t, rec, tc.want)
			assertErrorCode(t, rec, tc.code)
		})
	}
}

func TestMergePreviewReturnsNotImplementedWhenUnconfigured(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	New().Routes().ServeHTTP(rec, newPreviewRequest("any-token", "tenant-1", "repo-1", `{"sourceBranch":"feature","targetBranch":"main"}`))

	assertStatus(t, rec, http.StatusNotImplemented)
	assertErrorCode(t, rec, ErrorCodeNotImplemented)
}

func newPreviewRequest(token, tenantID, repoID, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/repos/"+repoID+"/merge/preview", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set(HeaderTenantID, tenantID)
	req.Header.Set("Content-Type", "application/json")
	return req
}

type fakePreviewEngine struct {
	preview                    *review.MergePreview
	err                        error
	calls                      int
	tenantID, repoID           string
	sourceBranch, targetBranch string
}

func (e *fakePreviewEngine) PreviewMerge(_ context.Context, tenantID, repoID, sourceBranch, targetBranch string) (*review.MergePreview, error) {
	e.calls++
	e.tenantID = tenantID
	e.repoID = repoID
	e.sourceBranch = sourceBranch
	e.targetBranch = targetBranch
	return e.preview, e.err
}
