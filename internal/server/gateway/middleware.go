package gateway

import (
	"crypto/rand"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
)

// HeaderTenantID is the request header carrying the caller's tenant identifier.
const HeaderTenantID = "X-Tenant-ID"

// HeaderCorrelationID is the request/response header carrying the
// correlation ID used to tie a Spool CLI operation to its Rack audit
// events, per spec-cli-rack-operational-error-and-audit-contract.
const HeaderCorrelationID = "X-Correlation-Id"

// CorrelationID returns middleware that extracts the caller-supplied
// X-Correlation-Id request header, or generates a new UUIDv4 when absent,
// stores it in the request context (see CorrelationIDFromContext), and sets
// it on the response header for both success and error responses. It must
// be mounted outermost so every route — including unauthenticated ones —
// reports a correlation ID.
func CorrelationID() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			correlationID := r.Header.Get(HeaderCorrelationID)
			if correlationID == "" {
				correlationID = newCorrelationID()
			}

			w.Header().Set(HeaderCorrelationID, correlationID)
			ctx := withCorrelationID(r.Context(), correlationID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// newCorrelationID generates a random UUIDv4 (RFC 4122) string.
func newCorrelationID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand.Read on the standard reader does not fail in practice;
		// this is an intentionally inert fallback rather than a panic.
		return "00000000-0000-4000-8000-000000000000"
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// Authenticate returns middleware that extracts the caller's credential,
// verifies it statelessly via verifier, and injects the resulting Claims
// and tenant identity into the request context for downstream handlers. It
// responds 401 Unauthorized when no credential is present, the credential
// is malformed, or verification fails. It does not resolve repository
// scope — see RequireRepoScope for that, per adr-url-path-repo-scoping.
func Authenticate(logger *slog.Logger, verifier auth.Verifier) func(http.Handler) http.Handler {
	if verifier == nil {
		verifier = auth.PermissiveVerifier{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cred, err := auth.ExtractCredential(r)
			if err != nil {
				writeJSONError(w, r, logger, http.StatusUnauthorized, ErrorCodeUnauthorized, "missing or malformed credential")
				return
			}

			claims, err := verifier.VerifyToken(r.Context(), cred.Token)
			if err != nil {
				writeJSONError(w, r, logger, http.StatusUnauthorized, ErrorCodeUnauthorized, "credential verification failed")
				return
			}

			tenantID := r.Header.Get(HeaderTenantID)
			claimsCopy := *claims
			if tenantID != "" {
				claimsCopy.TenantID = tenantID
			} else if claimsCopy.TenantID != "" {
				tenantID = claimsCopy.TenantID
			}

			ctx := withCredential(r.Context(), cred)
			ctx = withClaims(ctx, &claimsCopy)
			ctx = withTenantID(ctx, tenantID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireRepoScope returns middleware that resolves the repository segment
// of the URL path (registered as a "{repo}" wildcard, e.g.
// "/api/v1/repos/{repo}/...") together with the tenant ID already injected
// by Authenticate, constructing a validated cas.Scope. It must be mounted
// on routes registered with a "{repo}" path parameter, downstream of
// Authenticate. It responds 400 Bad Request when the tenant ID is missing
// from context (Authenticate wasn't applied) or when cas.NewScope rejects
// the tenant/repo identifiers (empty, or containing path-traversal-unsafe
// characters).
func RequireRepoScope(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tenantID, ok := TenantIDFromContext(r.Context())
			if !ok || tenantID == "" {
				writeJSONError(w, r, logger, http.StatusBadRequest, ErrorCodeBadRequest, "missing tenant context")
				return
			}

			repoID := r.PathValue("repo")
			scope, err := cas.NewScope(tenantID, repoID)
			if err != nil {
				logRejection(logger, r, "rejected invalid repo scope", err)
				writeJSONError(w, r, logger, http.StatusBadRequest, ErrorCodeBadRequest, "invalid tenant or repository identifier")
				return
			}

			next.ServeHTTP(w, r.WithContext(withScope(r.Context(), scope)))
		})
	}
}

// RequireRole returns middleware that rejects requests whose verified Claims
// role does not satisfy requiredRole, per adr-three-tier-rbac-model
// (Viewer < Contributor < Admin). It must be mounted downstream of
// Authenticate. Responds 403 Forbidden when the role is insufficient, and
// 401 Unauthorized when no Claims are present in context (Authenticate
// wasn't applied).
func RequireRole(logger *slog.Logger, requiredRole auth.Role) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			role, ok := RoleFromContext(r.Context())
			if !ok {
				writeJSONError(w, r, logger, http.StatusUnauthorized, ErrorCodeUnauthorized, "missing authenticated claims")
				return
			}

			if !role.Satisfies(requiredRole) {
				writeJSONError(w, r, logger, http.StatusForbidden, ErrorCodeForbidden, "insufficient role")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
