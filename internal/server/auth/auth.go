// Package auth implements Spool Rack's stateless OIDC/JWT resource-server
// authentication layer. It extracts credentials from inbound requests,
// verifies them without database lookups or IdP round-trips, and produces
// Claims describing the authenticated caller's tenant, role, and subject.
//
// Verification is stateless per adr-stateless-resource-server-auth: JWT
// verifiers validate cryptographic signatures against known keys, while
// configured tenant/API-key verifiers check an in-memory credential map.
// This package does not store credentials and does not function as an IdP.
package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

// CredentialKind identifies the extracted credential transport type.
type CredentialKind string

const (
	// CredentialKindBearer identifies an Authorization bearer token.
	CredentialKindBearer CredentialKind = "bearer"
	// CredentialKindAPIKey identifies an X-Api-Key token.
	CredentialKindAPIKey CredentialKind = "api_key"
)

// Role identifies a caller's permission tier under the three-tier RBAC model
// (adr-three-tier-rbac-model): Viewer < Contributor < Admin.
type Role string

const (
	// RoleViewer identifies callers with read-only access.
	RoleViewer Role = "viewer"
	// RoleContributor identifies callers allowed to modify tenant resources.
	RoleContributor Role = "contributor"
	// RoleAdmin identifies callers with the highest tenant privilege tier.
	RoleAdmin Role = "admin"
)

// roleRank orders roles from least to most privileged.
var roleRank = map[Role]int{
	RoleViewer:      0,
	RoleContributor: 1,
	RoleAdmin:       2,
}

// Credential carries an extracted credential token and its transport kind.
type Credential struct {
	Kind  CredentialKind
	Token string
}

// Claims carries the authenticated caller's identity extracted from a
// verified credential: tenant, role, and subject (the caller's stable
// identifier — an OIDC subject claim or an agent token identifier).
type Claims struct {
	TenantID string
	Role     Role
	Subject  string
}

var (
	// ErrMissingCredential indicates no Authorization bearer token and no
	// X-Api-Key header were present on the request.
	ErrMissingCredential = errors.New("auth: missing credential")
	// ErrMalformedCredential indicates a credential-bearing header was present
	// but not well-formed, such as a non-bearer scheme or an
	// empty/whitespace-only token value.
	ErrMalformedCredential = errors.New("auth: malformed credential")
	// ErrInvalidToken indicates credential verification failed.
	ErrInvalidToken = errors.New("auth: invalid token")
)

// Verifier statelessly verifies a raw credential token (a JWT bearer token
// or a configured tenant API key) and returns the caller's Claims. A
// Verifier implementation must not perform any database or IdP round-trip;
// JWT verifiers check a cryptographic signature against known keys, and
// static/API-key verifiers check against an in-memory configured set —
// both are stateless per adr-stateless-resource-server-auth.
type Verifier interface {
	VerifyToken(ctx context.Context, rawToken string) (*Claims, error)
}

// StaticVerifier verifies tokens against a fixed, in-memory map of raw
// token -> Claims. It is intended for local development, tests, and as
// the zero-configuration default until real JWT/JWKS verification is
// wired in; it performs no I/O and is safe for concurrent use.
type StaticVerifier struct {
	tokens map[string]Claims
}

// PermissiveVerifier accepts any non-empty token and assigns RoleAdmin to
// its Subject equal to the raw token. It exists ONLY as the zero-config
// default for early MVP wiring/local development where no real IdP or
// static token map has been configured yet, and MUST be replaced by a
// real Verifier (JWT/JWKS-backed or a configured StaticVerifier) before
// any non-development deployment. It performs no real authentication.
type PermissiveVerifier struct{}

// Satisfies reports whether role r meets or exceeds the required role. An
// unrecognized role never satisfies any requirement.
func (r Role) Satisfies(required Role) bool {
	rr, ok := roleRank[r]
	if !ok {
		return false
	}

	reqRank, ok := roleRank[required]
	if !ok {
		return false
	}

	return rr >= reqRank
}

// NewStaticVerifier constructs a StaticVerifier from a map of raw token to
// Claims. A nil or empty map is valid (every VerifyToken call then fails
// with ErrInvalidToken).
func NewStaticVerifier(tokens map[string]Claims) *StaticVerifier {
	cp := make(map[string]Claims, len(tokens))
	for k, v := range tokens {
		cp[k] = v
	}

	return &StaticVerifier{tokens: cp}
}

// VerifyToken returns the claims mapped to rawToken or ErrInvalidToken when
// the token is not configured.
func (v *StaticVerifier) VerifyToken(_ context.Context, rawToken string) (*Claims, error) {
	claims, ok := v.tokens[rawToken]
	if !ok {
		return nil, ErrInvalidToken
	}

	claimsCopy := claims
	return &claimsCopy, nil
}

// VerifyToken accepts any non-empty token and returns administrative claims
// whose subject is the raw token.
func (PermissiveVerifier) VerifyToken(_ context.Context, rawToken string) (*Claims, error) {
	if rawToken == "" {
		return nil, ErrInvalidToken
	}

	return &Claims{Subject: rawToken, Role: RoleAdmin}, nil
}

var (
	_ Verifier = (*StaticVerifier)(nil)
	_ Verifier = (*PermissiveVerifier)(nil)
)

// ExtractCredential extracts a bearer token from Authorization or an API key
// from X-Api-Key, returning Authorization bearer in preference to
// X-Api-Key when both headers are present.
func ExtractCredential(r *http.Request) (Credential, error) {
	if values := r.Header.Values("Authorization"); len(values) > 0 {
		authorization := strings.TrimSpace(values[0])
		if authorization == "" {
			return Credential{}, ErrMalformedCredential
		}

		scheme, token, ok := strings.Cut(authorization, " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") {
			return Credential{}, ErrMalformedCredential
		}

		token = strings.TrimSpace(token)
		if token == "" {
			return Credential{}, ErrMalformedCredential
		}

		return Credential{Kind: CredentialKindBearer, Token: token}, nil
	}

	if values := r.Header.Values("X-Api-Key"); len(values) > 0 {
		token := strings.TrimSpace(values[0])
		if token == "" {
			return Credential{}, ErrMalformedCredential
		}
		return Credential{Kind: CredentialKindAPIKey, Token: token}, nil
	}

	return Credential{}, ErrMissingCredential
}
