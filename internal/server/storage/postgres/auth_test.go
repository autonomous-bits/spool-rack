package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

type mockCredential struct {
	token string
	err   error
	calls int
	scope string
}

func (m *mockCredential) GetToken(_ context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	m.calls++
	if len(options.Scopes) > 0 {
		m.scope = options.Scopes[0]
	}
	if m.err != nil {
		return azcore.AccessToken{}, m.err
	}
	return azcore.AccessToken{
		Token:     m.token,
		ExpiresOn: time.Now().Add(1 * time.Hour),
	}, nil
}

func TestTokenFunc(t *testing.T) {
	expectedToken := "test-token-123"
	fn := TokenFunc(func(_ context.Context) (string, error) {
		return expectedToken, nil
	})

	token, err := fn.GetToken(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != expectedToken {
		t.Fatalf("got %q, want %q", token, expectedToken)
	}

	expectedErr := errors.New("token failure")
	errFn := TokenFunc(func(_ context.Context) (string, error) {
		return "", expectedErr
	})
	_, err = errFn.GetToken(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("got err %v, want %v", err, expectedErr)
	}
}

func TestAzureTokenProvider_DefaultsAndCustomScope(t *testing.T) {
	mock := &mockCredential{token: "mock-azure-token"}
	provider, err := NewAzureTokenProvider(AzureTokenProviderOptions{
		TokenCredential: mock,
	})
	if err != nil {
		t.Fatalf("NewAzureTokenProvider: %v", err)
	}

	token, err := provider.GetToken(context.Background())
	if err != nil {
		t.Fatalf("GetToken: %v", err)
	}
	if token != "mock-azure-token" {
		t.Fatalf("got %q, want %q", token, "mock-azure-token")
	}
	if mock.scope != DefaultAzurePostgresScope {
		t.Fatalf("got scope %q, want %q", mock.scope, DefaultAzurePostgresScope)
	}

	// Custom scope test
	customScope := "https://ossrdbms-aad.database.chinacloudapi.cn/.default"
	customMock := &mockCredential{token: "china-token"}
	customProvider, err := NewAzureTokenProvider(AzureTokenProviderOptions{
		Scope:           customScope,
		TokenCredential: customMock,
	})
	if err != nil {
		t.Fatalf("NewAzureTokenProvider: %v", err)
	}
	token, err = customProvider.GetToken(context.Background())
	if err != nil {
		t.Fatalf("GetToken: %v", err)
	}
	if token != "china-token" {
		t.Fatalf("got %q, want %q", token, "china-token")
	}
	if customMock.scope != customScope {
		t.Fatalf("got scope %q, want %q", customMock.scope, customScope)
	}
}

func TestAzureTokenProvider_ErrorHandling(t *testing.T) {
	mock := &mockCredential{err: errors.New("network timeout")}
	provider, err := NewAzureTokenProvider(AzureTokenProviderOptions{
		TokenCredential: mock,
	})
	if err != nil {
		t.Fatalf("NewAzureTokenProvider: %v", err)
	}

	_, err = provider.GetToken(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "network timeout") {
		t.Fatalf("unexpected error message: %v", err)
	}

	var nilProvider *AzureTokenProvider
	_, err = nilProvider.GetToken(context.Background())
	if err == nil {
		t.Fatal("expected error for nil provider, got nil")
	}
}

func TestNewPoolConfig_OptionsAndTokenRefresh(t *testing.T) {
	tokenCalls := 0
	tokenVal := "initial-secret-token"
	tp := TokenFunc(func(_ context.Context) (string, error) {
		tokenCalls++
		return tokenVal, nil
	})

	dsn := "postgres://initialuser@localhost:5432/spool_rack?sslmode=disable"
	opts := []Option{
		WithTokenProvider(tp),
		WithUser("managed-identity-user"),
		WithMaxConnLifetime(20 * time.Minute),
	}

	cfg, err := newPoolConfig(context.Background(), dsn, opts...)
	if err != nil {
		t.Fatalf("newPoolConfig: %v", err)
	}

	if cfg.ConnConfig.User != "managed-identity-user" {
		t.Fatalf("user = %q, want %q", cfg.ConnConfig.User, "managed-identity-user")
	}
	if cfg.ConnConfig.Password != "initial-secret-token" {
		t.Fatalf("initial password = %q, want %q", cfg.ConnConfig.Password, "initial-secret-token")
	}
	if cfg.MaxConnLifetime != 20*time.Minute {
		t.Fatalf("lifetime = %v, want 20m", cfg.MaxConnLifetime)
	}
	if cfg.BeforeConnect == nil {
		t.Fatal("expected BeforeConnect hook to be installed")
	}

	testConnConfig := cfg.ConnConfig.Copy()
	testConnConfig.Password = ""
	if err := cfg.BeforeConnect(context.Background(), testConnConfig); err != nil {
		t.Fatalf("BeforeConnect failed: %v", err)
	}
	if testConnConfig.Password != "initial-secret-token" {
		t.Fatalf("password after BeforeConnect = %q, want %q", testConnConfig.Password, "initial-secret-token")
	}
	if testConnConfig.User != "managed-identity-user" {
		t.Fatalf("user after BeforeConnect = %q, want %q", testConnConfig.User, "managed-identity-user")
	}

	// Dynamic token refresh: change token returned by provider
	tokenVal = "refreshed-token-456"
	testConnConfig2 := cfg.ConnConfig.Copy()
	testConnConfig2.Password = ""
	if err := cfg.BeforeConnect(context.Background(), testConnConfig2); err != nil {
		t.Fatalf("BeforeConnect 2 failed: %v", err)
	}
	if testConnConfig2.Password != "refreshed-token-456" {
		t.Fatalf("password = %q, want %q", testConnConfig2.Password, "refreshed-token-456")
	}

	// Verify default MaxConnLifetime is 45m when tokenProvider is set and no lifetime is given
	cfgDefaultLifetime, err := newPoolConfig(context.Background(), dsn, WithTokenProvider(tp))
	if err != nil {
		t.Fatalf("newPoolConfig with default lifetime: %v", err)
	}
	if cfgDefaultLifetime.MaxConnLifetime != 45*time.Minute {
		t.Fatalf("default lifetime = %v, want 45m", cfgDefaultLifetime.MaxConnLifetime)
	}
}

func TestOpen_TokenProviderError(t *testing.T) {
	errTP := TokenFunc(func(_ context.Context) (string, error) {
		return "", errors.New("auth failure from STS")
	})

	dsn := "postgres://user@localhost:5432/spool_rack?sslmode=disable"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := Open(ctx, dsn, WithTokenProvider(errTP))
	if err == nil {
		t.Fatal("expected Open to fail when TokenProvider fails, got nil")
	}
	if !strings.Contains(err.Error(), "auth failure from STS") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestMigrate_TokenProviderError(t *testing.T) {
	errTP := TokenFunc(func(_ context.Context) (string, error) {
		return "", errors.New("migrate auth failure from STS")
	})

	dsn := "postgres://user@localhost:5432/spool_rack?sslmode=disable"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := Migrate(ctx, dsn, WithMigrateTokenProvider(errTP))
	if err == nil {
		t.Fatal("expected Migrate to fail when TokenProvider fails, got nil")
	}
	if !strings.Contains(err.Error(), "migrate auth failure from STS") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestConfirmRetirement_TokenProviderError(t *testing.T) {
	errTP := TokenFunc(func(_ context.Context) (string, error) {
		return "", errors.New("confirm retirement auth failure from STS")
	})

	dsn := "postgres://user@localhost:5432/spool_rack?sslmode=disable"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := ConfirmRetirement(ctx, dsn, "test-name", "test-note", WithMigrateTokenProvider(errTP))
	if err == nil {
		t.Fatal("expected ConfirmRetirement to fail when TokenProvider fails, got nil")
	}
	if !strings.Contains(err.Error(), "confirm retirement auth failure from STS") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestConnect_TokenProviderError(t *testing.T) {
	errTP := TokenFunc(func(_ context.Context) (string, error) {
		return "", errors.New("connect auth failure from STS")
	})

	dsn := "postgres://user@localhost:5432/spool_rack?sslmode=disable"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := Connect(ctx, dsn, WithMigrateTokenProvider(errTP))
	if err == nil {
		t.Fatal("expected Connect to fail when TokenProvider fails, got nil")
	}
	if !strings.Contains(err.Error(), "connect auth failure from STS") {
		t.Fatalf("unexpected error message: %v", err)
	}
}
