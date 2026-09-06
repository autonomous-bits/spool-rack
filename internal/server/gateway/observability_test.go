package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/audit"
	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
)

var uuidV4Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestCorrelationIDEchoedOnSuccess(t *testing.T) {
	t.Parallel()

	gw := New()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set(HeaderCorrelationID, "caller-supplied-id")
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusOK)
	if got := rec.Header().Get(HeaderCorrelationID); got != "caller-supplied-id" {
		t.Fatalf("X-Correlation-Id = %q, want echoed caller-supplied-id", got)
	}
}

func TestCorrelationIDGeneratedWhenAbsentOnSuccess(t *testing.T) {
	t.Parallel()

	gw := New()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusOK)
	got := rec.Header().Get(HeaderCorrelationID)
	if !uuidV4Pattern.MatchString(got) {
		t.Fatalf("X-Correlation-Id = %q, want a generated UUIDv4", got)
	}
}

func TestCorrelationIDEchoedOnErrorResponse(t *testing.T) {
	t.Parallel()

	gw := New()
	req := httptest.NewRequest(http.MethodGet, "/v1/whoami", nil)
	req.Header.Set(HeaderCorrelationID, "caller-supplied-id")
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusUnauthorized)
	if got := rec.Header().Get(HeaderCorrelationID); got != "caller-supplied-id" {
		t.Fatalf("X-Correlation-Id header = %q, want echoed caller-supplied-id", got)
	}
	body := decodeBody(t, rec)
	if got := body["correlationId"]; got != "caller-supplied-id" {
		t.Fatalf("correlationId body field = %q, want caller-supplied-id", got)
	}
}

func TestCorrelationIDGeneratedOnErrorResponse(t *testing.T) {
	t.Parallel()

	gw := New()
	req := httptest.NewRequest(http.MethodGet, "/v1/whoami", nil)
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusUnauthorized)
	headerID := rec.Header().Get(HeaderCorrelationID)
	if !uuidV4Pattern.MatchString(headerID) {
		t.Fatalf("X-Correlation-Id header = %q, want a generated UUIDv4", headerID)
	}
	body := decodeBody(t, rec)
	if body["correlationId"] != headerID {
		t.Fatalf("correlationId body field = %q, want it to match response header %q", body["correlationId"], headerID)
	}
}

func TestRequireRepoScopeSanitizesErrorDetail(t *testing.T) {
	t.Parallel()

	gw := New()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/repos/repo!bad/whoami", nil)
	req.Header.Set("Authorization", "Bearer anytoken")
	req.Header.Set(HeaderTenantID, "tenant-1")
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorCode(t, rec, ErrorCodeBadRequest)
	body := decodeBody(t, rec)
	message := body["message"]
	if message != "invalid tenant or repository identifier" {
		t.Fatalf("message = %q, want a stable sanitized message", message)
	}
	if strings.Contains(message, "repo!bad") || strings.Contains(message, cas.ErrInvalidScope.Error()) {
		t.Fatalf("message leaked internal validation detail: %q", message)
	}
}

func TestPushInvalidFrameSanitizesErrorDetail(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver() error = %v", err)
	}
	baseCommit := hashCommitString("commit-a")
	targetCommit := hashCommitString("commit-b")
	store := &fakeGatewayBranchStore{
		branchHeads: map[gatewayBranchKey]string{
			{repoID: "repo-1", branch: "main"}: baseCommit,
		},
	}
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"contributor-token": {Role: auth.RoleContributor, Subject: "u1"},
		})),
	)

	packData := []byte("not a v2 frame")
	req := newPushRequest(t, pushRequestFixture{
		token:    "contributor-token",
		tenantID: "tenant-1",
		repoID:   "repo-1",
		metadata: pushMetadata{
			Branch:       "main",
			BaseCommit:   baseCommit,
			TargetCommit: targetCommit,
			PackHash:     hashPackBytes(packData),
			PackFormat:   2,
		},
		packData: packData,
	})
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorCode(t, rec, ErrorCodeBadRequest)
	body := decodeBody(t, rec)
	message := body["message"]
	if message != "invalid or malformed push pack" {
		t.Fatalf("message = %q, want a stable sanitized message", message)
	}
	if strings.Contains(message, "sync:") {
		t.Fatalf("message leaked internal error detail: %q", message)
	}
}

func TestAuditLoggerRecordsPushEvents(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver() error = %v", err)
	}
	baseCommit := hashCommitString("commit-a")
	targetCommit := hashCommitString("commit-b")
	store := &fakeGatewayBranchStore{
		branchHeads: map[gatewayBranchKey]string{
			{repoID: "repo-1", branch: "main"}: baseCommit,
		},
	}
	sink := audit.NewInMemorySink()
	auditLogger := audit.NewLogger(sink, nil)
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithAuditLogger(auditLogger),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"contributor-token": {Role: auth.RoleContributor, Subject: "pusher-1"},
		})),
	)

	packData := []byte("not a v2 frame")
	req := newPushRequest(t, pushRequestFixture{
		token:    "contributor-token",
		tenantID: "tenant-1",
		repoID:   "repo-1",
		metadata: pushMetadata{
			Branch:       "main",
			BaseCommit:   baseCommit,
			TargetCommit: targetCommit,
			PackHash:     hashPackBytes(packData),
			PackFormat:   2,
		},
		packData: packData,
	})
	req.Header.Set(HeaderCorrelationID, "push-correlation-1")
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)
	assertStatus(t, rec, http.StatusBadRequest)

	auditLogger.Close()
	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 audit event, got %d", len(events))
	}

	got := events[0]
	if got.Action != "push" {
		t.Fatalf("Action = %q, want push", got.Action)
	}
	if got.Outcome != "rejected" {
		t.Fatalf("Outcome = %q, want rejected", got.Outcome)
	}
	if got.Actor != "pusher-1" {
		t.Fatalf("Actor = %q, want pusher-1", got.Actor)
	}
	if got.TenantID != "tenant-1" {
		t.Fatalf("TenantID = %q, want tenant-1", got.TenantID)
	}
	if got.RepoID != "repo-1" {
		t.Fatalf("RepoID = %q, want repo-1", got.RepoID)
	}
	if got.Branch != "main" {
		t.Fatalf("Branch = %q, want main", got.Branch)
	}
	if got.CorrelationID != "push-correlation-1" {
		t.Fatalf("CorrelationID = %q, want push-correlation-1", got.CorrelationID)
	}
	if got.PreviousRef != baseCommit || got.NewRef != targetCommit {
		t.Fatalf("ref transition = (%q -> %q), want (%q -> %q)", got.PreviousRef, got.NewRef, baseCommit, targetCommit)
	}
}

func TestAuditLoggerNonBlockingWhenSinkErrors(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDriver() error = %v", err)
	}
	baseCommit := hashCommitString("commit-a")
	targetCommit := hashCommitString("commit-b")
	store := &fakeGatewayBranchStore{
		branchHeads: map[gatewayBranchKey]string{
			{repoID: "repo-1", branch: "main"}: baseCommit,
		},
	}
	auditLogger := audit.NewLogger(erroringAuditSink{}, nil)
	gw := New(
		WithCASDriver(driver),
		WithBranchStore(store),
		WithAuditLogger(auditLogger),
		WithVerifier(auth.NewStaticVerifier(map[string]auth.Claims{
			"contributor-token": {Role: auth.RoleContributor, Subject: "u1"},
		})),
	)

	packData := []byte("not a v2 frame")
	req := newPushRequest(t, pushRequestFixture{
		token:    "contributor-token",
		tenantID: "tenant-1",
		repoID:   "repo-1",
		metadata: pushMetadata{
			Branch:       "main",
			BaseCommit:   baseCommit,
			TargetCommit: targetCommit,
			PackHash:     hashPackBytes(packData),
			PackFormat:   2,
		},
		packData: packData,
	})
	rec := httptest.NewRecorder()

	// The wired audit Logger's Sink always fails; the request must still
	// complete with its ordinary sanitized error response, proving audit
	// emission never blocks or fails the request path.
	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorCode(t, rec, ErrorCodeBadRequest)
	auditLogger.Close()
}

type erroringAuditSink struct{}

func (erroringAuditSink) Record(context.Context, audit.Event) error {
	return errAuditSinkFailure
}

var errAuditSinkFailure = errors.New("simulated audit sink failure")
