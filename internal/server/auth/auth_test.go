package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestRoleSatisfies(t *testing.T) {
	testCases := []struct {
		name     string
		role     Role
		required Role
		want     bool
	}{
		{name: "viewer satisfies viewer", role: RoleViewer, required: RoleViewer, want: true},
		{name: "viewer does not satisfy contributor", role: RoleViewer, required: RoleContributor, want: false},
		{name: "viewer does not satisfy admin", role: RoleViewer, required: RoleAdmin, want: false},
		{name: "contributor satisfies viewer", role: RoleContributor, required: RoleViewer, want: true},
		{name: "contributor satisfies contributor", role: RoleContributor, required: RoleContributor, want: true},
		{name: "contributor does not satisfy admin", role: RoleContributor, required: RoleAdmin, want: false},
		{name: "admin satisfies viewer", role: RoleAdmin, required: RoleViewer, want: true},
		{name: "admin satisfies contributor", role: RoleAdmin, required: RoleContributor, want: true},
		{name: "admin satisfies admin", role: RoleAdmin, required: RoleAdmin, want: true},
		{name: "unrecognized role satisfies nothing", role: Role("mystery"), required: RoleViewer, want: false},
		{name: "recognized role does not satisfy unrecognized requirement", role: RoleAdmin, required: Role("mystery"), want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.role.Satisfies(tc.required); got != tc.want {
				t.Fatalf("Satisfies(%q, %q) = %t, want %t", tc.role, tc.required, got, tc.want)
			}
		})
	}
}

func TestExtractCredentialBearer(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer token-123")

	cred, err := ExtractCredential(req)
	if err != nil {
		t.Fatalf("ExtractCredential returned error: %v", err)
	}

	want := Credential{Kind: CredentialKindBearer, Token: "token-123"}
	if cred != want {
		t.Fatalf("expected credential %+v, got %+v", want, cred)
	}
}

func TestExtractCredentialAPIKey(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Api-Key", "  api-key-123  ")

	cred, err := ExtractCredential(req)
	if err != nil {
		t.Fatalf("ExtractCredential returned error: %v", err)
	}

	want := Credential{Kind: CredentialKindAPIKey, Token: "api-key-123"}
	if cred != want {
		t.Fatalf("expected credential %+v, got %+v", want, cred)
	}
}

func TestExtractCredentialMissing(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	_, err := ExtractCredential(req)
	if !errors.Is(err, ErrMissingCredential) {
		t.Fatalf("expected ErrMissingCredential, got %v", err)
	}
}

func TestExtractCredentialMalformedAuthorization(t *testing.T) {
	testCases := []struct {
		name          string
		authorization string
	}{
		{name: "wrong scheme", authorization: "Token abc"},
		{name: "missing token", authorization: "Bearer"},
		{name: "whitespace token", authorization: "Bearer   "},
		{name: "whitespace only header", authorization: "   "},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Authorization", tc.authorization)

			_, err := ExtractCredential(req)
			if !errors.Is(err, ErrMalformedCredential) {
				t.Fatalf("expected ErrMalformedCredential, got %v", err)
			}
		})
	}
}

func TestExtractCredentialMalformedAPIKey(t *testing.T) {
	testCases := []struct {
		name   string
		apiKey string
	}{
		{name: "empty", apiKey: ""},
		{name: "whitespace", apiKey: "   "},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("X-Api-Key", tc.apiKey)

			_, err := ExtractCredential(req)
			if !errors.Is(err, ErrMalformedCredential) {
				t.Fatalf("expected ErrMalformedCredential, got %v", err)
			}
		})
	}
}

func TestExtractCredentialAuthorizationPrecedence(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer bearer-token")
	req.Header.Set("X-Api-Key", "api-key-123")

	cred, err := ExtractCredential(req)
	if err != nil {
		t.Fatalf("ExtractCredential returned error: %v", err)
	}

	want := Credential{Kind: CredentialKindBearer, Token: "bearer-token"}
	if cred != want {
		t.Fatalf("expected credential %+v, got %+v", want, cred)
	}
}

func TestExtractCredentialBearerSchemeCaseInsensitive(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "bearer token-123")

	cred, err := ExtractCredential(req)
	if err != nil {
		t.Fatalf("ExtractCredential returned error: %v", err)
	}

	want := Credential{Kind: CredentialKindBearer, Token: "token-123"}
	if cred != want {
		t.Fatalf("expected credential %+v, got %+v", want, cred)
	}
}

func TestStaticVerifierVerifyToken(t *testing.T) {
	verifier := NewStaticVerifier(map[string]Claims{
		"known-token": {
			TenantID: "tenant-123",
			Role:     RoleContributor,
			Subject:  "subject-123",
		},
	})

	got, err := verifier.VerifyToken(context.Background(), "known-token")
	if err != nil {
		t.Fatalf("VerifyToken returned error: %v", err)
	}

	want := &Claims{
		TenantID: "tenant-123",
		Role:     RoleContributor,
		Subject:  "subject-123",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected claims %+v, got %+v", want, got)
	}
}

func TestStaticVerifierUnknownToken(t *testing.T) {
	verifier := NewStaticVerifier(nil)

	_, err := verifier.VerifyToken(context.Background(), "missing-token")
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestStaticVerifierDefensiveCopy(t *testing.T) {
	source := map[string]Claims{
		"original-token": {
			Role:    RoleViewer,
			Subject: "original-subject",
		},
	}
	verifier := NewStaticVerifier(source)

	source["original-token"] = Claims{
		TenantID: "tenant-overridden",
		Role:     RoleAdmin,
		Subject:  "mutated-subject",
	}
	source["added-later"] = Claims{
		Role:    RoleAdmin,
		Subject: "added-later",
	}

	got, err := verifier.VerifyToken(context.Background(), "original-token")
	if err != nil {
		t.Fatalf("VerifyToken returned error: %v", err)
	}

	want := &Claims{
		Role:    RoleViewer,
		Subject: "original-subject",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected copied claims %+v, got %+v", want, got)
	}

	_, err = verifier.VerifyToken(context.Background(), "added-later")
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected added-later to be unknown, got %v", err)
	}
}

func TestPermissiveVerifierVerifyToken(t *testing.T) {
	var verifier PermissiveVerifier

	got, err := verifier.VerifyToken(context.Background(), "dev-token")
	if err != nil {
		t.Fatalf("VerifyToken returned error: %v", err)
	}

	want := &Claims{
		Role:    RoleAdmin,
		Subject: "dev-token",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected claims %+v, got %+v", want, got)
	}
}

func TestPermissiveVerifierRejectsEmptyToken(t *testing.T) {
	var verifier PermissiveVerifier

	_, err := verifier.VerifyToken(context.Background(), "")
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}
