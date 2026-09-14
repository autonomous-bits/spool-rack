package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/gateway"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	"github.com/jackc/pgx/v5"
)

const (
	defaultPort = "8080"
)

var (
	version = "0.3.0"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	var opts []gateway.Option

	// Keep CAS opt-in so zero-config startup preserves today's safe 501 push
	// behavior unless an operator explicitly chooses a storage root.
	casRoot := os.Getenv("CAS_ROOT")
	if casRoot != "" {
		driver, err := cas.NewLocalDriver(casRoot)
		if err != nil {
			log.Fatalf("Failed to initialize CAS driver at %q: %v", casRoot, err)
		}
		opts = append(opts, gateway.WithCASDriver(driver))
	}

	postgresDSN := os.Getenv("POSTGRES_DSN")
	migrationsDSN := os.Getenv("POSTGRES_MIGRATIONS_DSN")
	if postgresDSN != "" || migrationsDSN != "" {
		if migrationsDSN == "" {
			migrationsDSN = postgresDSN
		}

		authType := os.Getenv("POSTGRES_AUTH_TYPE")
		if authType == "" && (os.Getenv("POSTGRES_AZURE_AUTH") == "true" || os.Getenv("POSTGRES_AZURE_AUTH") == "1") {
			authType = "azure"
		}

		migrationsAuthType := os.Getenv("POSTGRES_MIGRATIONS_AUTH_TYPE")
		if migrationsAuthType == "" {
			if migrationsDSN != postgresDSN && hasPasswordInDSN(migrationsDSN) {
				migrationsAuthType = "password"
			} else {
				migrationsAuthType = authType
			}
		}

		var azureTokenProvider *postgres.AzureTokenProvider
		if isAzureAuth(authType) || isAzureAuth(migrationsAuthType) {
			azureClientID := os.Getenv("POSTGRES_AZURE_CLIENT_ID")
			if azureClientID == "" {
				azureClientID = os.Getenv("AZURE_CLIENT_ID")
			}
			azureScope := os.Getenv("POSTGRES_AZURE_SCOPE")
			if azureScope == "" {
				azureScope = postgres.DefaultAzurePostgresScope
			}
			tp, err := postgres.NewAzureTokenProvider(postgres.AzureTokenProviderOptions{
				ClientID: azureClientID,
				Scope:    azureScope,
			})
			if err != nil {
				log.Fatalf("Failed to initialize Azure token provider: %v", err)
			}
			azureTokenProvider = tp
		}

		dbUser := os.Getenv("POSTGRES_USER")
		if dbUser == "" {
			dbUser = os.Getenv("POSTGRES_AZURE_USER")
		}

		var migrateOpts []postgres.MigrateOption
		if isAzureAuth(migrationsAuthType) {
			migrateOpts = append(migrateOpts, postgres.WithMigrateTokenProvider(azureTokenProvider))
		}
		migrationsUser := resolveMigrationUser(os.Getenv("POSTGRES_MIGRATIONS_USER"), dbUser, migrationsDSN, postgresDSN)
		if migrationsUser != "" {
			migrateOpts = append(migrateOpts, postgres.WithMigrateUser(migrationsUser))
		}

		runMigrations := true
		if val := os.Getenv("POSTGRES_RUN_MIGRATIONS"); val == "false" || val == "0" || val == "no" {
			runMigrations = false
		}
		if val := os.Getenv("POSTGRES_MIGRATIONS_ENABLED"); val == "false" || val == "0" || val == "no" {
			runMigrations = false
		}

		if runMigrations && migrationsDSN != "" {
			migrateCtx, migrateCancel := context.WithTimeout(context.Background(), 30*time.Second)
			result, err := postgres.Migrate(migrateCtx, migrationsDSN, migrateOpts...)
			migrateCancel()
			if err != nil {
				log.Fatalf("Failed to apply Postgres migrations: %v", err)
			}
			if len(result.Applied) > 0 {
				log.Printf("Applied %d Postgres migration(s): %v", len(result.Applied), result.Applied)
			}
		} else if !runMigrations {
			log.Printf("Postgres startup migrations skipped (POSTGRES_RUN_MIGRATIONS=false)")
		}

		if os.Getenv("POSTGRES_MIGRATE_ONLY") == "true" || os.Getenv("POSTGRES_MIGRATE_ONLY") == "1" {
			devTenantID := os.Getenv("DEV_TENANT_ID")
			devRepoID := os.Getenv("DEV_REPO_ID")
			if devTenantID != "" && devRepoID != "" && migrationsDSN != "" {
				seedCtx, seedCancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := seedDevTenant(seedCtx, migrationsDSN, devTenantID, devRepoID, migrateOpts...); err != nil {
					log.Printf("Warning: failed to seed dev tenant/repo: %v", err)
				}
				seedCancel()
			}
			log.Printf("POSTGRES_MIGRATE_ONLY completed successfully; exiting.")
			return
		}

		if postgresDSN != "" {
			var openOpts []postgres.Option
			if isAzureAuth(authType) {
				openOpts = append(openOpts, postgres.WithTokenProvider(azureTokenProvider))
			}
			if dbUser != "" {
				openOpts = append(openOpts, postgres.WithUser(dbUser))
			}
			if lifetimeStr := os.Getenv("POSTGRES_MAX_CONN_LIFETIME"); lifetimeStr != "" {
				d, err := time.ParseDuration(lifetimeStr)
				if err != nil {
					log.Fatalf("Invalid POSTGRES_MAX_CONN_LIFETIME %q: %v", lifetimeStr, err)
				}
				openOpts = append(openOpts, postgres.WithMaxConnLifetime(d))
			}

			openCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			store, err := postgres.Open(openCtx, postgresDSN, openOpts...)
			cancel()
			if err != nil {
				log.Fatalf("Failed to open Postgres store: %v", err)
			}
			// This simple main still uses blocking ListenAndServe without graceful
			// shutdown; defer is sufficient for fatal/panic exit paths.
			defer store.Close()
			opts = append(opts, gateway.WithBranchStore(store), gateway.WithTenantWorkspaceStore(store))
		}
	}

	devTenantID := os.Getenv("DEV_TENANT_ID")
	if devTenantID != "" {
		opts = append(opts, gateway.WithVerifier(devTenantVerifier{tenantID: devTenantID}))
		devRepoID := os.Getenv("DEV_REPO_ID")
		migrationsDSN := os.Getenv("POSTGRES_MIGRATIONS_DSN")
		if migrationsDSN == "" {
			migrationsDSN = postgresDSN
		}
		if devRepoID != "" && migrationsDSN != "" {
			seedCtx, seedCancel := context.WithTimeout(context.Background(), 10*time.Second)

			authType := os.Getenv("POSTGRES_AUTH_TYPE")
			if authType == "" && (os.Getenv("POSTGRES_AZURE_AUTH") == "true" || os.Getenv("POSTGRES_AZURE_AUTH") == "1") {
				authType = "azure"
			}
			migrationsAuthType := os.Getenv("POSTGRES_MIGRATIONS_AUTH_TYPE")
			if migrationsAuthType == "" {
				if migrationsDSN != postgresDSN && hasPasswordInDSN(migrationsDSN) {
					migrationsAuthType = "password"
				} else {
					migrationsAuthType = authType
				}
			}

			var seedOpts []postgres.MigrateOption
			if isAzureAuth(migrationsAuthType) {
				azureClientID := os.Getenv("POSTGRES_AZURE_CLIENT_ID")
				if azureClientID == "" {
					azureClientID = os.Getenv("AZURE_CLIENT_ID")
				}
				azureScope := os.Getenv("POSTGRES_AZURE_SCOPE")
				if azureScope == "" {
					azureScope = postgres.DefaultAzurePostgresScope
				}
				tp, err := postgres.NewAzureTokenProvider(postgres.AzureTokenProviderOptions{
					ClientID: azureClientID,
					Scope:    azureScope,
				})
				if err != nil {
					log.Printf("Warning: failed to initialize Azure token provider for seeding: %v", err)
				} else {
					seedOpts = append(seedOpts, postgres.WithMigrateTokenProvider(tp))
				}
			}
			appUser := os.Getenv("POSTGRES_USER")
			if appUser == "" {
				appUser = os.Getenv("POSTGRES_AZURE_USER")
			}
			seedUser := resolveMigrationUser(os.Getenv("POSTGRES_MIGRATIONS_USER"), appUser, migrationsDSN, postgresDSN)
			if seedUser != "" {
				seedOpts = append(seedOpts, postgres.WithMigrateUser(seedUser))
			}

			if err := seedDevTenant(seedCtx, migrationsDSN, devTenantID, devRepoID, seedOpts...); err != nil {
				log.Printf("Warning: failed to seed dev tenant/repo: %v", err)
			}
			seedCancel()
		}
	}

	gw := gateway.New(opts...)
	addr := fmt.Sprintf(":%s", port)
	log.Printf("Starting Spool Rack server v%s on %s...", version, addr)
	if casRoot != "" && postgresDSN != "" {
		log.Printf("Push and pull endpoints enabled (CAS_ROOT=%s)", casRoot)
	} else {
		log.Printf("Push and pull endpoints disabled: CAS_ROOT/POSTGRES_DSN not both configured")
	}

	if err := http.ListenAndServe(addr, gw.Routes()); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed to listen: %v", err)
	}
}

func resolveMigrationUser(migrationsUser, appUser, migrationsDSN, postgresDSN string) string {
	if migrationsUser != "" {
		return migrationsUser
	}
	if migrationsDSN == postgresDSN {
		return appUser
	}
	return ""
}

func isAzureAuth(authType string) bool {
	switch strings.ToLower(strings.TrimSpace(authType)) {
	case "azure", "azure-identity", "azure-ad", "azure_ad", "entra", "workload-identity":
		return true
	default:
		return false
	}
}

func hasPasswordInDSN(dsn string) bool {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return false
	}
	return cfg.Password != ""
}

type devTenantVerifier struct {
	tenantID string
}

func (v devTenantVerifier) VerifyToken(_ context.Context, rawToken string) (*auth.Claims, error) {
	if rawToken == "" {
		return nil, auth.ErrInvalidToken
	}
	return &auth.Claims{
		Subject:  rawToken,
		Role:     auth.RoleAdmin,
		TenantID: v.tenantID,
	}, nil
}

func seedDevTenant(ctx context.Context, dsn, tenantID, repoID string, opts ...postgres.MigrateOption) error {
	conn, err := postgres.Connect(ctx, dsn, opts...)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()

	if _, err := conn.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, 'default') ON CONFLICT (id) DO NOTHING`, tenantID); err != nil {
		return fmt.Errorf("seed tenant: %w", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO repositories (id, tenant_id, name) VALUES ($1, $2, 'default') ON CONFLICT (id) DO NOTHING`, repoID, tenantID); err != nil {
		return fmt.Errorf("seed repository: %w", err)
	}
	return nil
}
