package postgres

import (
	"context"
	"errors"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
)

// newMigrateTestDSN connects with the same admin DSN used by the rest of the
// integration suite, skipping the test if PostgreSQL is not reachable.
func newMigrateTestDSN(t *testing.T) string {
	t.Helper()
	dsn := getenvDefault("TEST_POSTGRES_ADMIN_DSN", defaultTestPostgresAdminDSN)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Skipf("postgres not reachable at %q: %v; run `docker compose up -d postgres` to enable this test", dsn, err)
	}
	_ = conn.Close(ctx)
	return dsn
}

// dropMigrationTestArtifacts removes the tables a test's ad hoc migration set
// created plus the tracking tables, so each test starts from a clean slate
// regardless of execution order.
func dropMigrationTestArtifacts(t *testing.T, dsn string, extraTables ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	tables := append([]string{"schema_migrations", "migration_retirement_confirmations"}, extraTables...)
	for _, table := range tables {
		if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+table+" CASCADE"); err != nil {
			t.Fatalf("drop %s: %v", table, err)
		}
	}
}

func TestMigrate_AppliesInOrderAndIsIdempotent(t *testing.T) {
	dsn := newMigrateTestDSN(t)
	dropMigrationTestArtifacts(t, dsn, "migrate_test_widgets")
	t.Cleanup(func() { dropMigrationTestArtifacts(t, dsn, "migrate_test_widgets") })

	fsys := fstest.MapFS{
		"migrations/0001_create_widgets.sql": &fstest.MapFile{Data: []byte(
			`CREATE TABLE IF NOT EXISTS migrate_test_widgets (id bigserial PRIMARY KEY);`,
		)},
		"migrations/0002_add_widget_name.sql": &fstest.MapFile{Data: []byte(
			`ALTER TABLE migrate_test_widgets ADD COLUMN IF NOT EXISTS name text NOT NULL DEFAULT '';`,
		)},
	}
	migrations, err := loadMigrationsFromFS(fsys, "migrations")
	if err != nil {
		t.Fatalf("loadMigrationsFromFS: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := migrateWithMigrations(ctx, dsn, migrations)
	if err != nil {
		t.Fatalf("migrateWithMigrations: %v", err)
	}
	if len(result.Applied) != 2 {
		t.Fatalf("Applied = %v, want 2 migrations", result.Applied)
	}

	// Re-running must be a no-op: nothing new applied, no error.
	result, err = migrateWithMigrations(ctx, dsn, migrations)
	if err != nil {
		t.Fatalf("migrateWithMigrations (rerun): %v", err)
	}
	if len(result.Applied) != 0 {
		t.Fatalf("Applied on rerun = %v, want none", result.Applied)
	}
}

func TestMigrate_ChecksumDriftIsRejected(t *testing.T) {
	dsn := newMigrateTestDSN(t)
	dropMigrationTestArtifacts(t, dsn, "migrate_test_drift")
	t.Cleanup(func() { dropMigrationTestArtifacts(t, dsn, "migrate_test_drift") })

	original := fstest.MapFS{
		"migrations/0001_create_drift.sql": &fstest.MapFile{Data: []byte(
			`CREATE TABLE IF NOT EXISTS migrate_test_drift (id bigserial PRIMARY KEY);`,
		)},
	}
	migrations, err := loadMigrationsFromFS(original, "migrations")
	if err != nil {
		t.Fatalf("loadMigrationsFromFS: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := migrateWithMigrations(ctx, dsn, migrations); err != nil {
		t.Fatalf("initial migrateWithMigrations: %v", err)
	}

	changed := fstest.MapFS{
		"migrations/0001_create_drift.sql": &fstest.MapFile{Data: []byte(
			`CREATE TABLE IF NOT EXISTS migrate_test_drift (id bigserial PRIMARY KEY, extra text);`,
		)},
	}
	changedMigrations, err := loadMigrationsFromFS(changed, "migrations")
	if err != nil {
		t.Fatalf("loadMigrationsFromFS (changed): %v", err)
	}

	_, err = migrateWithMigrations(ctx, dsn, changedMigrations)
	if err == nil {
		t.Fatal("migrateWithMigrations: expected checksum drift error, got nil")
	}
	if !errors.Is(err, ErrMigrationChecksumMismatch) {
		t.Fatalf("error = %v, want wrapping ErrMigrationChecksumMismatch", err)
	}
}

func TestLoadMigrations_DestructiveStatementRequiresDirective(t *testing.T) {
	fsys := fstest.MapFS{
		"migrations/0001_drop_widgets.sql": &fstest.MapFile{Data: []byte(
			`DROP TABLE widgets;`,
		)},
	}
	if _, err := loadMigrationsFromFS(fsys, "migrations"); !errors.Is(err, ErrDestructiveMigrationNotMarked) {
		t.Fatalf("error = %v, want ErrDestructiveMigrationNotMarked", err)
	}
}

// TestLoadMigrations_RejectsVersionPrefixOverflowingInt64 guards against
// CodeQL's "incorrect conversion between integer types" finding: schema_
// migrations.version is a Postgres bigint (signed 64-bit) and applyMigration
// stores a migration's parsed version via int64(m.Version), so a filename
// version prefix that fits in uint64 but overflows int64 must be rejected up
// front instead of silently wrapping into a negative value on insert.
func TestLoadMigrations_RejectsVersionPrefixOverflowingInt64(t *testing.T) {
	fsys := fstest.MapFS{
		"migrations/18446744073709551615_overflow.sql": &fstest.MapFile{Data: []byte(
			`CREATE TABLE IF NOT EXISTS migrate_test_overflow (id bigserial PRIMARY KEY);`,
		)},
	}
	_, err := loadMigrationsFromFS(fsys, "migrations")
	if err == nil {
		t.Fatal("loadMigrationsFromFS: expected an error for a version prefix exceeding math.MaxInt64, got nil")
	}
}

func TestMigrate_DestructiveMigrationBlockedUntilRetirementConfirmed(t *testing.T) {
	dsn := newMigrateTestDSN(t)
	dropMigrationTestArtifacts(t, dsn, "migrate_test_legacy")
	t.Cleanup(func() { dropMigrationTestArtifacts(t, dsn, "migrate_test_legacy") })

	fsys := fstest.MapFS{
		"migrations/0001_create_legacy.sql": &fstest.MapFile{Data: []byte(
			`CREATE TABLE IF NOT EXISTS migrate_test_legacy (id bigserial PRIMARY KEY, legacy_col text);`,
		)},
		"migrations/0002_drop_legacy_col.sql": &fstest.MapFile{Data: []byte(
			"-- destructive: requires-retirement=migrate-test-legacy-col\n" +
				`ALTER TABLE migrate_test_legacy DROP COLUMN legacy_col;`,
		)},
	}
	migrations, err := loadMigrationsFromFS(fsys, "migrations")
	if err != nil {
		t.Fatalf("loadMigrationsFromFS: %v", err)
	}
	if !migrations[1].Destructive || migrations[1].RetirementConfirmation != "migrate-test-legacy-col" {
		t.Fatalf("migrations[1] = %+v, want destructive with confirmation name", migrations[1])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = migrateWithMigrations(ctx, dsn, migrations)
	if !errors.Is(err, ErrRetirementNotConfirmed) {
		t.Fatalf("error = %v, want ErrRetirementNotConfirmed before confirmation", err)
	}

	if err := ConfirmRetirement(ctx, dsn, "migrate-test-legacy-col", "no clients read legacy_col anymore"); err != nil {
		t.Fatalf("ConfirmRetirement: %v", err)
	}

	result, err := migrateWithMigrations(ctx, dsn, migrations)
	if err != nil {
		t.Fatalf("migrateWithMigrations after confirmation: %v", err)
	}
	if len(result.Applied) != 1 || result.Applied[0] != "0002_drop_legacy_col.sql" {
		t.Fatalf("Applied = %v, want only 0002 to apply now that 0001 already applied", result.Applied)
	}
}
