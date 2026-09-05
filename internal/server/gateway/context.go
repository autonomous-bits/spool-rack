package gateway

import (
	"context"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
)

type contextKey int

const (
	tenantIDContextKey contextKey = iota
	scopeContextKey
	credentialContextKey
	claimsContextKey
)

// TenantIDFromContext returns the tenant identity extracted at ingress and
// propagated through context.Context for downstream Postgres and CAS consumers
// following the tenant-context-propagation pattern.
func TenantIDFromContext(ctx context.Context) (string, bool) {
	tenantID, ok := ctx.Value(tenantIDContextKey).(string)
	return tenantID, ok
}

// ScopeFromContext returns the validated CAS scope extracted at ingress and
// propagated through context.Context for downstream Postgres and CAS consumers
// following the tenant-context-propagation pattern.
func ScopeFromContext(ctx context.Context) (cas.Scope, bool) {
	scope, ok := ctx.Value(scopeContextKey).(cas.Scope)
	return scope, ok
}

// CredentialFromContext returns the caller credential extracted at ingress and
// propagated through context.Context for downstream Postgres and CAS consumers
// following the tenant-context-propagation pattern.
func CredentialFromContext(ctx context.Context) (auth.Credential, bool) {
	cred, ok := ctx.Value(credentialContextKey).(auth.Credential)
	return cred, ok
}

// ClaimsFromContext returns the verified caller Claims injected by Authenticate.
func ClaimsFromContext(ctx context.Context) (*auth.Claims, bool) {
	claims, ok := ctx.Value(claimsContextKey).(*auth.Claims)
	return claims, ok
}

// RoleFromContext returns the caller's Role from the verified Claims, if present.
func RoleFromContext(ctx context.Context) (auth.Role, bool) {
	claims, ok := ClaimsFromContext(ctx)
	if !ok || claims == nil {
		return "", false
	}

	return claims.Role, true
}

func withTenantID(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantIDContextKey, tenantID)
}

func withScope(ctx context.Context, scope cas.Scope) context.Context {
	return context.WithValue(ctx, scopeContextKey, scope)
}

func withCredential(ctx context.Context, cred auth.Credential) context.Context {
	return context.WithValue(ctx, credentialContextKey, cred)
}

func withClaims(ctx context.Context, claims *auth.Claims) context.Context {
	return context.WithValue(ctx, claimsContextKey, claims)
}
