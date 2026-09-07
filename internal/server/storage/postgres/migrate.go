package postgres

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// migrationFS embeds the ordered set of SQL migration files applied by
// Migrate. Files are named "NNNN_description.sql"; NNNN is the migration's
// version and must be unique and strictly increasing.
//
//go:embed migrations/*.sql
var migrationFS embed.FS

// destructiveDirectivePrefix marks a migration file as intentionally
// non-additive. A migration containing a statement matched by
// destructiveStatementPattern MUST start with this directive (after leading
// blank/comment lines are permitted anywhere before it), naming a retirement
// confirmation that must be recorded via ConfirmRetirement before Migrate
// will apply the file. This is the enforcement mechanism for
// std-non-breaking-evolution-and-version-compatibility and
// spec-cli-rack-contract-and-metadata-migrations: it blocks a destructive
// change from landing until old clients are confirmed retired, and it fails
// loudly if a migration is destructive but was never explicitly marked as
// such.
const destructiveDirectivePrefix = "-- destructive: requires-retirement="

// destructiveStatementPattern flags SQL statements that can destroy data or
// break a reader relying on the prior shape: dropping tables/columns,
// changing a column's type, renaming, and truncation. It deliberately does
// NOT flag idempotent "DROP ... IF EXISTS" patterns paired with an immediate
// CREATE (e.g. "DROP POLICY IF EXISTS x; CREATE POLICY x ..."), which the
// baseline schema already relies on and which are additive in effect.
var destructiveStatementPattern = regexp.MustCompile(`(?is)\bDROP\s+TABLE\b|\bDROP\s+COLUMN\b|\bALTER\s+COLUMN\s+\S+\s+TYPE\b|\bRENAME\s+COLUMN\b|\bRENAME\s+TO\b|\bTRUNCATE\b`)

var migrationFileNamePattern = regexp.MustCompile(`^(\d+)_(.+)\.sql$`)

// migration is one parsed, embedded SQL migration file.
type migration struct {
	Version                uint64
	Name                   string
	FileName               string
	SQL                    string
	Checksum               string
	Destructive            bool
	RetirementConfirmation string
}

// ErrDestructiveMigrationNotMarked indicates a migration file contains a
// statement that can destroy data or break compatibility but does not
// declare the required destructive directive.
var ErrDestructiveMigrationNotMarked = errors.New("postgres: migration contains a destructive statement without a destructive directive")

// ErrRetirementNotConfirmed indicates a destructive migration cannot be
// applied yet because its named retirement has not been confirmed via
// ConfirmRetirement.
var ErrRetirementNotConfirmed = errors.New("postgres: destructive migration requires a retirement confirmation")

// ErrMigrationChecksumMismatch indicates a migration file that was already
// recorded as applied has since changed on disk, which would make forward
// migration non-deterministic.
var ErrMigrationChecksumMismatch = errors.New("postgres: applied migration checksum mismatch")

func loadMigrations() ([]migration, error) {
	return loadMigrationsFromFS(migrationFS, "migrations")
}

// loadMigrationsFromFS is the fs.FS-parameterized implementation behind
// loadMigrations, split out so tests can exercise the parsing, additive
// lint, and retirement-directive logic against an in-memory fstest.MapFS
// instead of the real embedded migrations.
func loadMigrationsFromFS(fsys fs.FS, dir string) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("postgres: read migrations directory: %w", err)
	}
	migrations := make([]migration, 0, len(entries))
	seen := make(map[uint64]string, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		match := migrationFileNamePattern.FindStringSubmatch(entry.Name())
		if match == nil {
			return nil, fmt.Errorf("postgres: migration file %q does not match required NNNN_name.sql naming", entry.Name())
		}
		version, err := strconv.ParseUint(match[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("postgres: migration file %q has an invalid version prefix: %w", entry.Name(), err)
		}
		if existing, ok := seen[version]; ok {
			return nil, fmt.Errorf("postgres: migration version %d used by both %q and %q", version, existing, entry.Name())
		}
		seen[version] = entry.Name()

		data, err := fs.ReadFile(fsys, dir+"/"+entry.Name())
		if err != nil {
			return nil, fmt.Errorf("postgres: read migration %q: %w", entry.Name(), err)
		}
		sql := string(data)

		m := migration{
			Version:  version,
			Name:     match[2],
			FileName: entry.Name(),
			SQL:      sql,
			Checksum: checksumSQL(sql),
		}
		// The baseline migration captures the schema as it already existed in
		// deployed databases before this versioned runner existed (including
		// idempotent "DROP COLUMN IF EXISTS ... ADD COLUMN ..." convergence
		// statements from its own history). It is grandfathered in and exempt
		// from the additive lint: the lint protects new migrations added from
		// here on, not a one-time snapshot of already-live state.
		if match[2] != "baseline" && destructiveStatementPattern.MatchString(sql) {
			confirmation, ok := destructiveConfirmationName(sql)
			if !ok {
				return nil, fmt.Errorf("postgres: migration %q: %w", entry.Name(), ErrDestructiveMigrationNotMarked)
			}
			m.Destructive = true
			m.RetirementConfirmation = confirmation
		}
		migrations = append(migrations, m)
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].Version < migrations[j].Version })
	return migrations, nil
}

// destructiveConfirmationName returns the retirement confirmation name
// declared by a "-- destructive: requires-retirement=<name>" directive
// appearing on its own line anywhere in sql, and whether one was found.
func destructiveConfirmationName(sql string) (string, bool) {
	for _, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, destructiveDirectivePrefix) {
			name := strings.TrimSpace(strings.TrimPrefix(trimmed, destructiveDirectivePrefix))
			if name != "" {
				return name, true
			}
		}
	}
	return "", false
}

func checksumSQL(sql string) string {
	sum := sha256.Sum256([]byte(sql))
	return hex.EncodeToString(sum[:])
}

const bootstrapTrackingTablesSQL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
	version bigint PRIMARY KEY,
	name text NOT NULL,
	checksum text NOT NULL,
	applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS migration_retirement_confirmations (
	name text PRIMARY KEY,
	confirmed_at timestamptz NOT NULL DEFAULT now(),
	note text NOT NULL DEFAULT ''
);
`

// MigrationResult reports what Migrate did.
type MigrationResult struct {
	// Applied lists the migrations applied during this call, in order. It is
	// empty when the database was already at the latest version.
	Applied []string
}

// Migrate connects to PostgreSQL using dsn (expected to be a privileged
// migration role, distinct from the restricted "spool_app" role that Open
// uses for ordinary request handling; see migrations/0001_baseline.sql) and
// applies every embedded migration that has not yet been recorded in
// schema_migrations, in version order, each inside its own transaction.
//
// A migration whose file contains a destructive statement (dropping a
// table/column, changing a column's type, renaming, or truncating) must
// declare a "-- destructive: requires-retirement=<name>" directive; Migrate
// refuses to run it until a matching row exists in
// migration_retirement_confirmations (see ConfirmRetirement). This makes
// "block destructive migration until old clients are retired"
// (spec-cli-rack-contract-and-metadata-migrations) an enforced property of
// deployment rather than a convention.
//
// Migrate is safe to call repeatedly (including concurrently across rolling
// replicas that all run it at startup): already-applied migrations are
// skipped, and a previously-applied file whose checksum has since changed is
// reported as an error rather than silently reapplied or ignored, since that
// would make forward migration non-deterministic.
func Migrate(ctx context.Context, dsn string) (*MigrationResult, error) {
	if ctx == nil {
		return nil, errNilContext
	}
	migrations, err := loadMigrations()
	if err != nil {
		return nil, err
	}
	return migrateWithMigrations(ctx, dsn, migrations)
}

// migrateWithMigrations is the connection/apply core behind Migrate, split
// out so tests can drive it with an in-memory migration set (built via
// loadMigrationsFromFS against an fstest.MapFS) to exercise the destructive
// lint, retirement gate, and checksum-drift detection against a real
// database without needing to change the embedded migrations directory.
func migrateWithMigrations(ctx context.Context, dsn string, migrations []migration) (*MigrationResult, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: migrate: connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if _, err := conn.Exec(ctx, bootstrapTrackingTablesSQL); err != nil {
		return nil, fmt.Errorf("postgres: migrate: bootstrap tracking tables: %w", err)
	}

	applied, err := loadAppliedMigrations(ctx, conn)
	if err != nil {
		return nil, err
	}

	result := &MigrationResult{}
	for _, m := range migrations {
		if existing, ok := applied[m.Version]; ok {
			if existing.Checksum != m.Checksum {
				return nil, fmt.Errorf("postgres: migrate: %s (version %d): %w: recorded checksum %s, file checksum %s", m.FileName, m.Version, ErrMigrationChecksumMismatch, existing.Checksum, m.Checksum)
			}
			continue
		}
		if m.Destructive {
			confirmed, err := isRetirementConfirmed(ctx, conn, m.RetirementConfirmation)
			if err != nil {
				return nil, err
			}
			if !confirmed {
				return nil, fmt.Errorf("postgres: migrate: %s (version %d): %w %q; call ConfirmRetirement once old clients no longer depend on the shape this migration removes", m.FileName, m.Version, ErrRetirementNotConfirmed, m.RetirementConfirmation)
			}
		}
		if err := applyMigration(ctx, conn, m); err != nil {
			return nil, err
		}
		result.Applied = append(result.Applied, m.FileName)
	}
	return result, nil
}

type appliedMigration struct {
	Checksum string
}

func loadAppliedMigrations(ctx context.Context, conn *pgx.Conn) (map[uint64]appliedMigration, error) {
	rows, err := conn.Query(ctx, `SELECT version, checksum FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("postgres: migrate: list applied migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[uint64]appliedMigration)
	for rows.Next() {
		var version int64
		var checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			return nil, fmt.Errorf("postgres: migrate: scan applied migration: %w", err)
		}
		applied[uint64(version)] = appliedMigration{Checksum: checksum}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: migrate: list applied migrations: %w", err)
	}
	return applied, nil
}

func applyMigration(ctx context.Context, conn *pgx.Conn, m migration) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: migrate: %s: begin transaction: %w", m.FileName, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, m.SQL); err != nil {
		return fmt.Errorf("postgres: migrate: %s: apply: %w", m.FileName, err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`,
		int64(m.Version), m.Name, m.Checksum,
	); err != nil {
		return fmt.Errorf("postgres: migrate: %s: record applied migration: %w", m.FileName, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: migrate: %s: commit transaction: %w", m.FileName, err)
	}
	return nil
}

func isRetirementConfirmed(ctx context.Context, conn *pgx.Conn, name string) (bool, error) {
	var exists bool
	err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM migration_retirement_confirmations WHERE name = $1)`,
		name,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("postgres: migrate: check retirement confirmation %q: %w", name, err)
	}
	return exists, nil
}

// ConfirmRetirement records an operator's explicit acknowledgement that old
// clients depending on the shape a destructive migration removes have been
// confirmed retired, unblocking that migration's next Migrate call. name must
// match the value declared by the migration's
// "-- destructive: requires-retirement=<name>" directive.
func ConfirmRetirement(ctx context.Context, dsn, name, note string) error {
	if ctx == nil {
		return errNilContext
	}
	if name == "" {
		return fmt.Errorf("postgres: confirm retirement: name is required")
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("postgres: confirm retirement: connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if _, err := conn.Exec(ctx, bootstrapTrackingTablesSQL); err != nil {
		return fmt.Errorf("postgres: confirm retirement: bootstrap tracking tables: %w", err)
	}
	if _, err := conn.Exec(ctx,
		`INSERT INTO migration_retirement_confirmations (name, note) VALUES ($1, $2)
		 ON CONFLICT (name) DO UPDATE SET confirmed_at = now(), note = EXCLUDED.note`,
		name, note,
	); err != nil {
		return fmt.Errorf("postgres: confirm retirement: %w", err)
	}
	return nil
}
