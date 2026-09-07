package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/autonomous-bits/spool-rack/internal/server/auth"
	"github.com/autonomous-bits/spool-rack/internal/server/gateway"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	"github.com/jackc/pgx/v5"
)

const (
	defaultPort = "8080"
	version     = "0.1.0-mvp"
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
	if postgresDSN != "" {
		// POSTGRES_MIGRATIONS_DSN lets operators point schema migrations at a
		// privileged role while POSTGRES_DSN keeps the server itself on the
		// restricted "spool_app" role (see migrations/0001_baseline.sql).
		// Falling back to POSTGRES_DSN keeps single-DSN local/dev setups
		// working unchanged.
		migrationsDSN := os.Getenv("POSTGRES_MIGRATIONS_DSN")
		if migrationsDSN == "" {
			migrationsDSN = postgresDSN
		}
		migrateCtx, migrateCancel := context.WithTimeout(context.Background(), 30*time.Second)
		result, err := postgres.Migrate(migrateCtx, migrationsDSN)
		migrateCancel()
		if err != nil {
			log.Fatalf("Failed to apply Postgres migrations: %v", err)
		}
		if len(result.Applied) > 0 {
			log.Printf("Applied %d Postgres migration(s): %v", len(result.Applied), result.Applied)
		}

		openCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		store, err := postgres.Open(openCtx, postgresDSN)
		cancel()
		if err != nil {
			log.Fatalf("Failed to open Postgres store: %v", err)
		}
		// This simple main still uses blocking ListenAndServe without graceful
		// shutdown; defer is sufficient for fatal/panic exit paths.
		defer store.Close()
		opts = append(opts, gateway.WithBranchStore(store), gateway.WithTenantWorkspaceStore(store))
	}

	devTenantID := os.Getenv("DEV_TENANT_ID")
	if devTenantID != "" {
		opts = append(opts, gateway.WithVerifier(devTenantVerifier{tenantID: devTenantID}))
		devRepoID := os.Getenv("DEV_REPO_ID")
		if devRepoID != "" && postgresDSN != "" {
			seedCtx, seedCancel := context.WithTimeout(context.Background(), 10*time.Second)
			migrationsDSN := os.Getenv("POSTGRES_MIGRATIONS_DSN")
			if migrationsDSN == "" {
				migrationsDSN = postgresDSN
			}
			if err := seedDevTenant(seedCtx, migrationsDSN, devTenantID, devRepoID); err != nil {
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

func seedDevTenant(ctx context.Context, dsn, tenantID, repoID string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, 'default') ON CONFLICT (id) DO NOTHING`, tenantID); err != nil {
		return fmt.Errorf("seed tenant: %w", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO repositories (id, tenant_id, name) VALUES ($1, $2, 'default') ON CONFLICT (id) DO NOTHING`, repoID, tenantID); err != nil {
		return fmt.Errorf("seed repository: %w", err)
	}
	return nil
}
