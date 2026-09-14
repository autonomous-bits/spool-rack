package postgres

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

const (
	// DefaultAzurePostgresScope is the default OAuth2 scope for Azure Database for PostgreSQL Flexible Server.
	DefaultAzurePostgresScope = "https://ossrdbms-aad.database.windows.net/.default"
)

// TokenProvider defines an interface for acquiring dynamic database authentication tokens.
type TokenProvider interface {
	GetToken(ctx context.Context) (string, error)
}

// TokenFunc is a convenience adapter allowing bare functions to implement TokenProvider.
type TokenFunc func(ctx context.Context) (string, error)

// GetToken calls f(ctx).
func (f TokenFunc) GetToken(ctx context.Context) (string, error) {
	return f(ctx)
}

// AzureTokenProviderOptions configures the AzureTokenProvider.
type AzureTokenProviderOptions struct {
	// ClientID is the client ID of a user-assigned managed identity or application registration.
	// If empty, the default credential resolution chain is used (including AZURE_CLIENT_ID from environment).
	ClientID string

	// TenantID is the Azure Active Directory tenant ID. Optional.
	TenantID string

	// Scope is the OAuth2 resource scope to request. If empty, DefaultAzurePostgresScope is used.
	Scope string

	// TokenCredential allows injecting a custom azcore.TokenCredential (useful for testing or specialized credentials).
	TokenCredential azcore.TokenCredential
}

// AzureTokenProvider implements TokenProvider using Azure Identity.
type AzureTokenProvider struct {
	cred  azcore.TokenCredential
	scope string
}

// NewAzureTokenProvider creates a new AzureTokenProvider configured with the given options.
// It uses azidentity.NewDefaultAzureCredential under the hood, supporting Azure Workload Identity,
// Managed Identity, Environment variables, and Azure CLI credentials.
func NewAzureTokenProvider(opts AzureTokenProviderOptions) (*AzureTokenProvider, error) {
	scope := opts.Scope
	if scope == "" {
		scope = DefaultAzurePostgresScope
	}

	cred := opts.TokenCredential
	if cred == nil {
		if opts.ClientID != "" {
			var sources []azcore.TokenCredential
			if wic, err := azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
				ClientID: opts.ClientID,
				TenantID: opts.TenantID,
			}); err == nil {
				sources = append(sources, wic)
			}
			if mic, err := azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{
				ID: azidentity.ClientID(opts.ClientID),
			}); err == nil {
				sources = append(sources, mic)
			}
			if clicred, err := azidentity.NewAzureCLICredential(&azidentity.AzureCLICredentialOptions{
				TenantID: opts.TenantID,
			}); err == nil {
				sources = append(sources, clicred)
			}
			if len(sources) == 0 {
				return nil, fmt.Errorf("postgres: no azure credentials available for client id %q", opts.ClientID)
			}
			chainedCred, err := azidentity.NewChainedTokenCredential(sources, nil)
			if err != nil {
				return nil, fmt.Errorf("postgres: create chained azure credential: %w", err)
			}
			cred = chainedCred
		} else {
			var dacOpts azidentity.DefaultAzureCredentialOptions
			if opts.TenantID != "" {
				dacOpts.TenantID = opts.TenantID
			}
			defaultCred, err := azidentity.NewDefaultAzureCredential(&dacOpts)
			if err != nil {
				return nil, fmt.Errorf("postgres: create default azure credential: %w", err)
			}
			cred = defaultCred
		}
	}

	return &AzureTokenProvider{
		cred:  cred,
		scope: scope,
	}, nil
}

// GetToken acquires a fresh or cached access token from Azure Entra ID.
func (p *AzureTokenProvider) GetToken(ctx context.Context) (string, error) {
	if p == nil || p.cred == nil {
		return "", fmt.Errorf("postgres: azure token provider not initialized")
	}
	token, err := p.cred.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{p.scope},
	})
	if err != nil {
		return "", fmt.Errorf("postgres: acquire azure token: %w", err)
	}
	return token.Token, nil
}
