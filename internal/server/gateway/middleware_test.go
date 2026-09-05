package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
)

func TestWhoamiMissingCredential(t *testing.T) {
	gw := New()
	req := httptest.NewRequest(http.MethodGet, "/v1/whoami", nil)
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusUnauthorized)
	assertErrorCode(t, rec, ErrorCodeUnauthorized)
}

func TestWhoamiWithBearerCredential(t *testing.T) {
	gw := New()
	req := httptest.NewRequest(http.MethodGet, "/v1/whoami", nil)
	req.Header.Set("Authorization", "Bearer anytoken")
	req.Header.Set(HeaderTenantID, "tenant-1")
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusOK)
	body := decodeBody(t, rec)
	assertBodyValue(t, body, "tenantID", "tenant-1")
	assertBodyValue(t, body, "role", string(auth.RoleAdmin))
	assertBodyValue(t, body, "subject", "anytoken")
}

func TestWhoamiWithAPIKeyCredential(t *testing.T) {
	gw := New()
	req := httptest.NewRequest(http.MethodGet, "/v1/whoami", nil)
	req.Header.Set("X-Api-Key", "anykey")
	req.Header.Set(HeaderTenantID, "tenant-1")
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusOK)
	body := decodeBody(t, rec)
	assertBodyValue(t, body, "tenantID", "tenant-1")
	assertBodyValue(t, body, "role", string(auth.RoleAdmin))
	assertBodyValue(t, body, "subject", "anykey")
}

func TestWhoamiAllowsMissingTenantHeader(t *testing.T) {
	gw := New()
	req := httptest.NewRequest(http.MethodGet, "/v1/whoami", nil)
	req.Header.Set("Authorization", "Bearer anytoken")
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusOK)
	body := decodeBody(t, rec)
	assertBodyValue(t, body, "tenantID", "")
	assertBodyValue(t, body, "role", string(auth.RoleAdmin))
	assertBodyValue(t, body, "subject", "anytoken")
}

func TestRepoWhoamiResolvesScopeFromPath(t *testing.T) {
	gw := New()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/repos/repo-1/whoami", nil)
	req.Header.Set("Authorization", "Bearer repo-token")
	req.Header.Set(HeaderTenantID, "tenant-1")
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusOK)
	body := decodeBody(t, rec)
	assertBodyValue(t, body, "tenantID", "tenant-1")
	assertBodyValue(t, body, "repoID", "repo-1")
	assertBodyValue(t, body, "role", string(auth.RoleAdmin))
}

func TestRepoWhoamiRejectsUnsafeRepoSegment(t *testing.T) {
	gw := New()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/repos/repo!bad/whoami", nil)
	req.Header.Set("Authorization", "Bearer repo-token")
	req.Header.Set(HeaderTenantID, "tenant-1")
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorCode(t, rec, ErrorCodeBadRequest)
}

func TestRepoWhoamiRequiresTenantContext(t *testing.T) {
	gw := New()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/repos/repo-1/whoami", nil)
	req.Header.Set("Authorization", "Bearer repo-token")
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorCode(t, rec, ErrorCodeBadRequest)
}

func TestRepoWhoamiMissingCredential(t *testing.T) {
	gw := New()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/repos/repo-1/whoami", nil)
	req.Header.Set(HeaderTenantID, "tenant-1")
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	assertStatus(t, rec, http.StatusUnauthorized)
	assertErrorCode(t, rec, ErrorCodeUnauthorized)
}

func TestRequireRole(t *testing.T) {
	t.Run("rejects insufficient role", func(t *testing.T) {
		h := Authenticate(nil, auth.NewStaticVerifier(map[string]auth.Claims{
			"viewer-token": {Role: auth.RoleViewer, Subject: "u1"},
		}))(RequireRole(nil, auth.RoleContributor)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})))

		req := httptest.NewRequest(http.MethodGet, "/protected", nil)
		req.Header.Set("Authorization", "Bearer viewer-token")
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		assertStatus(t, rec, http.StatusForbidden)
		assertErrorCode(t, rec, ErrorCodeForbidden)
	})

	t.Run("allows satisfied role", func(t *testing.T) {
		h := Authenticate(nil, auth.NewStaticVerifier(map[string]auth.Claims{
			"contributor-token": {Role: auth.RoleContributor, Subject: "u2"},
		}))(RequireRole(nil, auth.RoleContributor)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})))

		req := httptest.NewRequest(http.MethodGet, "/protected", nil)
		req.Header.Set("Authorization", "Bearer contributor-token")
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		assertStatus(t, rec, http.StatusOK)
	})
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]string {
	t.Helper()

	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}

	return body
}

func assertStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()

	if rec.Code != want {
		t.Fatalf("expected status %d, got %d", want, rec.Code)
	}
}

func assertErrorCode(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()

	body := decodeBody(t, rec)
	assertBodyValue(t, body, "error", want)
}

func assertBodyValue(t *testing.T, body map[string]string, key, want string) {
	t.Helper()

	if got := body[key]; got != want {
		t.Fatalf("expected %s %q, got %q", key, want, got)
	}
}
