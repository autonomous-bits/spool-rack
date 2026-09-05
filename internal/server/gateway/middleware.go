package gateway

import (
	"log/slog"
	"net/http"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
)

// HeaderTenantID is the request header carrying the caller's tenant identifier.
const HeaderTenantID = "X-Tenant-ID"

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
				writeJSONError(w, r, logger, http.StatusBadRequest, ErrorCodeBadRequest, err.Error())
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
