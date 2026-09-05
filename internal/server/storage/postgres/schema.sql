-- schema.sql provisions the PostgreSQL control-plane metadata store used by
-- Spool Rack's multi-tenant server implementation.
--
-- The schema models tenants, repositories, commits, and branch heads, and it
-- is intentionally designed around PostgreSQL Row-Level Security (RLS) rather
-- than relying on every query to remember to add "WHERE tenant_id = ...".
-- Each application connection is expected to set
-- "app.current_tenant_id" for the current request, and the database then
-- becomes the last line of defence that prevents cross-tenant reads or writes
-- even if an application query is accidentally too broad.
--
-- RLS only helps if the application connects as a role that is actually
-- subject to it. PostgreSQL table owners bypass RLS by default, so this file
-- also provisions a restricted application role, "spool_app", and assumes
-- migrations are applied by some separate, more privileged role. The app
-- should connect as "spool_app", not as the table owner, so the policies below
-- are genuinely enforced at runtime.
--
-- Every statement in this file is written to be idempotent and safe to
-- re-apply. That matters for bootstrap flows, repeated local development
-- setup, and deployment systems that may run the same schema migration more
-- than once while converging a database to its desired shape.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- spool_app is a deliberately constrained login role for local development and
-- test environments. The password literal is fixed so repeatable local setup
-- does not depend on an external secret bootstrap step; production deployments
-- should override this with externally managed credentials instead of reusing
-- the development value below.
DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1
		FROM pg_catalog.pg_roles
		WHERE rolname = 'spool_app'
	) THEN
		CREATE ROLE spool_app LOGIN PASSWORD 'spool_app_dev_password';
	END IF;
END
$$;

-- Tenants sit at the root of the metadata graph because every repository,
-- commit, and branch belongs to exactly one tenant. A unique tenant name is
-- reasonable here because this store models control-plane identities rather
-- than end-user display labels.
CREATE TABLE IF NOT EXISTS tenants (
	id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	name text NOT NULL UNIQUE,
	created_at timestamptz NOT NULL DEFAULT now()
);

-- Repositories are namespaced within a tenant. The composite uniqueness on
-- (tenant_id, name) allows different tenants to use the same repository name
-- while preventing duplicates inside one tenant's namespace. The extra
-- (tenant_id, id) uniqueness looks redundant next to the primary key, but it
-- gives child tables a cheap way to assert that their denormalised tenant_id
-- still agrees with the repository they point at.
CREATE TABLE IF NOT EXISTS repositories (
	id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	name text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	UNIQUE (tenant_id, id),
	UNIQUE (tenant_id, name)
);

-- Commits belong to both a tenant and a repository. Their content-addressed ID
-- is unique only inside that scope: the same canonical frame may legitimately
-- exist in two repositories. Storing tenant_id directly lets RLS decisions
-- remain local to this table instead of requiring a join through repositories
-- to prove tenancy for every read or write.
CREATE TABLE IF NOT EXISTS commits (
	id text NOT NULL,
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL REFERENCES repositories(id),
	parent_commit_id text,
	snapshot_root text NOT NULL,
	object_format smallint NOT NULL DEFAULT 1 CHECK (object_format IN (1, 2)),
	author text NOT NULL,
	message text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (tenant_id, repo_id, id),
	CONSTRAINT commits_repository_scope_fk
		FOREIGN KEY (tenant_id, repo_id)
		REFERENCES repositories(tenant_id, id),
	CONSTRAINT commits_parent_scope_fk
		FOREIGN KEY (tenant_id, repo_id, parent_commit_id)
		REFERENCES commits(tenant_id, repo_id, id)
);

CREATE INDEX IF NOT EXISTS commits_repo_id_idx ON commits (repo_id);

-- Pack ranges connect the immutable CAS payload created by a push to the
-- commit interval it contains. Pull traverses these ranges backwards from a
-- branch head, then streams them oldest-to-newest.
CREATE TABLE IF NOT EXISTS pack_ranges (
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL REFERENCES repositories(id),
	pack_hash text NOT NULL,
	object_format smallint NOT NULL DEFAULT 1 CHECK (object_format IN (1, 2)),
	base_commit_id text,
	target_commit_id text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT pack_ranges_repository_scope_fk
		FOREIGN KEY (tenant_id, repo_id)
		REFERENCES repositories(tenant_id, id),
	CONSTRAINT pack_ranges_target_scope_fk
		FOREIGN KEY (tenant_id, repo_id, target_commit_id)
		REFERENCES commits(tenant_id, repo_id, id),
	CONSTRAINT pack_ranges_base_scope_fk
		FOREIGN KEY (tenant_id, repo_id, base_commit_id)
		REFERENCES commits(tenant_id, repo_id, id),
	UNIQUE (tenant_id, repo_id, pack_hash),
	UNIQUE (tenant_id, repo_id, target_commit_id)
);

CREATE INDEX IF NOT EXISTS pack_ranges_repo_target_idx ON pack_ranges (repo_id, target_commit_id);

-- Branch heads are mutable refs pointing at immutable commits. The primary key
-- is scoped by repository because branch names only need to be unique within a
-- single repository, not globally or per tenant. Extra scoped foreign keys
-- make sure a branch head cannot accidentally target a commit from some other
-- repository or tenant.
CREATE TABLE IF NOT EXISTS branches (
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL REFERENCES repositories(id),
	name text NOT NULL,
	head_commit_id text NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT branches_repository_scope_fk
		FOREIGN KEY (tenant_id, repo_id)
		REFERENCES repositories(tenant_id, id),
	CONSTRAINT branches_head_commit_scope_fk
		FOREIGN KEY (tenant_id, repo_id, head_commit_id)
		REFERENCES commits(tenant_id, repo_id, id),
	PRIMARY KEY (repo_id, name)
);

-- parent_commit_id remains the legacy first-parent representation.  The
-- normalized table records an ordered parent list so a merge commit can retain
-- both its target (position 1) and source (position 2) histories.
CREATE TABLE IF NOT EXISTS commit_parents (
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL REFERENCES repositories(id),
	commit_id text NOT NULL,
	parent_position smallint NOT NULL CHECK (parent_position BETWEEN 1 AND 2),
	parent_commit_id text NOT NULL,
	CONSTRAINT commit_parents_commit_scope_fk
		FOREIGN KEY (tenant_id, repo_id, commit_id)
		REFERENCES commits(tenant_id, repo_id, id),
	CONSTRAINT commit_parents_parent_scope_fk
		FOREIGN KEY (tenant_id, repo_id, parent_commit_id)
		REFERENCES commits(tenant_id, repo_id, id),
	CONSTRAINT commit_parents_repository_scope_fk
		FOREIGN KEY (tenant_id, repo_id)
		REFERENCES repositories(tenant_id, id),
	PRIMARY KEY (tenant_id, repo_id, commit_id, parent_position)
);

CREATE INDEX IF NOT EXISTS commit_parents_parent_idx ON commit_parents (repo_id, parent_commit_id);

-- Backfill pre-existing linear commits.  The conflict clause makes repeated
-- schema application safe and preserves any already-recorded merge parent.
INSERT INTO commit_parents (tenant_id, repo_id, commit_id, parent_position, parent_commit_id)
SELECT tenant_id, repo_id, id, 1, parent_commit_id
FROM commits
WHERE parent_commit_id IS NOT NULL
ON CONFLICT (tenant_id, repo_id, commit_id, parent_position) DO NOTHING;

CREATE TABLE IF NOT EXISTS target_branch_merge_leases (
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL REFERENCES repositories(id),
	target_branch text NOT NULL,
	subject text NOT NULL,
	lease_token text NOT NULL,
	source_commit_id text NOT NULL,
	target_commit_id text NOT NULL,
	base_commit_id text NOT NULL,
	expires_at timestamptz NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT target_branch_merge_leases_repository_scope_fk
		FOREIGN KEY (tenant_id, repo_id)
		REFERENCES repositories(tenant_id, id),
	CONSTRAINT target_branch_merge_leases_branch_scope_fk
		FOREIGN KEY (tenant_id, repo_id, target_branch)
		REFERENCES branches(tenant_id, repo_id, name),
	CONSTRAINT target_branch_merge_leases_source_scope_fk
		FOREIGN KEY (tenant_id, repo_id, source_commit_id)
		REFERENCES commits(tenant_id, repo_id, id),
	CONSTRAINT target_branch_merge_leases_target_scope_fk
		FOREIGN KEY (tenant_id, repo_id, target_commit_id)
		REFERENCES commits(tenant_id, repo_id, id),
	CONSTRAINT target_branch_merge_leases_base_scope_fk
		FOREIGN KEY (tenant_id, repo_id, base_commit_id)
		REFERENCES commits(tenant_id, repo_id, id),
	PRIMARY KEY (tenant_id, repo_id, target_branch),
	UNIQUE (lease_token)
);

CREATE INDEX IF NOT EXISTS target_branch_merge_leases_expiry_idx
	ON target_branch_merge_leases (expires_at);

-- Canonical v2 commit IDs are intentionally reusable in independent
-- repositories, so convert the original globally keyed commit table to a
-- repository-scoped key. The surrounding transaction prevents the temporary
-- foreign-key removal required by PostgreSQL from becoming externally visible.
BEGIN;

-- Older Rack schemas allowed a global commit ID to be used as a parent in a
-- different repository. A scoped key cannot safely represent that invalid
-- topology. Refuse the upgrade before changing any schema so operators can
-- repair the affected history with the retained legacy artifacts.
DO $$
BEGIN
	IF EXISTS (
		SELECT 1 FROM commits child
		WHERE child.parent_commit_id IS NOT NULL
			AND NOT EXISTS (
				SELECT 1 FROM commits parent
				WHERE parent.tenant_id = child.tenant_id
					AND parent.repo_id = child.repo_id
					AND parent.id = child.parent_commit_id
			)
	) OR EXISTS (
		SELECT 1 FROM commit_parents child
		WHERE NOT EXISTS (
			SELECT 1 FROM commits parent
			WHERE parent.tenant_id = child.tenant_id
				AND parent.repo_id = child.repo_id
				AND parent.id = child.parent_commit_id
		)
	) OR EXISTS (
		SELECT 1 FROM pack_ranges child
		WHERE (child.base_commit_id IS NOT NULL AND NOT EXISTS (
				SELECT 1 FROM commits parent
				WHERE parent.tenant_id = child.tenant_id
					AND parent.repo_id = child.repo_id
					AND parent.id = child.base_commit_id
			))
			OR NOT EXISTS (
				SELECT 1 FROM commits parent
				WHERE parent.tenant_id = child.tenant_id
					AND parent.repo_id = child.repo_id
					AND parent.id = child.target_commit_id
			)
	) OR EXISTS (
		SELECT 1 FROM branches child
		WHERE NOT EXISTS (
			SELECT 1 FROM commits parent
			WHERE parent.tenant_id = child.tenant_id
				AND parent.repo_id = child.repo_id
				AND parent.id = child.head_commit_id
		)
	) OR EXISTS (
		SELECT 1 FROM target_branch_merge_leases child
		WHERE NOT EXISTS (
				SELECT 1 FROM commits parent
				WHERE parent.tenant_id = child.tenant_id
					AND parent.repo_id = child.repo_id
					AND parent.id = child.source_commit_id
			)
			OR NOT EXISTS (
				SELECT 1 FROM commits parent
				WHERE parent.tenant_id = child.tenant_id
					AND parent.repo_id = child.repo_id
					AND parent.id = child.target_commit_id
			)
			OR NOT EXISTS (
				SELECT 1 FROM commits parent
				WHERE parent.tenant_id = child.tenant_id
					AND parent.repo_id = child.repo_id
					AND parent.id = child.base_commit_id
			)
	) THEN
		RAISE EXCEPTION 'cannot scope commit IDs while cross-repository ancestry exists; repair legacy topology before applying this migration';
	END IF;
END
$$;

ALTER TABLE branches DROP CONSTRAINT IF EXISTS branches_head_commit_id_fkey;
ALTER TABLE branches DROP CONSTRAINT IF EXISTS branches_head_commit_scope_fk;
ALTER TABLE commits DROP CONSTRAINT IF EXISTS commits_parent_commit_id_fkey;
ALTER TABLE commits DROP CONSTRAINT IF EXISTS commits_parent_scope_fk;
ALTER TABLE pack_ranges DROP CONSTRAINT IF EXISTS pack_ranges_base_commit_id_fkey;
ALTER TABLE pack_ranges DROP CONSTRAINT IF EXISTS pack_ranges_target_commit_id_fkey;
ALTER TABLE pack_ranges DROP CONSTRAINT IF EXISTS pack_ranges_base_scope_fk;
ALTER TABLE pack_ranges DROP CONSTRAINT IF EXISTS pack_ranges_target_scope_fk;
ALTER TABLE commit_parents DROP CONSTRAINT IF EXISTS commit_parents_parent_commit_id_fkey;
ALTER TABLE commit_parents DROP CONSTRAINT IF EXISTS commit_parents_commit_scope_fk;
ALTER TABLE commit_parents DROP CONSTRAINT IF EXISTS commit_parents_parent_scope_fk;
ALTER TABLE target_branch_merge_leases DROP CONSTRAINT IF EXISTS target_branch_merge_leases_source_commit_id_fkey;
ALTER TABLE target_branch_merge_leases DROP CONSTRAINT IF EXISTS target_branch_merge_leases_target_commit_id_fkey;
ALTER TABLE target_branch_merge_leases DROP CONSTRAINT IF EXISTS target_branch_merge_leases_base_commit_id_fkey;
ALTER TABLE target_branch_merge_leases DROP CONSTRAINT IF EXISTS target_branch_merge_leases_source_scope_fk;
ALTER TABLE target_branch_merge_leases DROP CONSTRAINT IF EXISTS target_branch_merge_leases_target_scope_fk;
ALTER TABLE target_branch_merge_leases DROP CONSTRAINT IF EXISTS target_branch_merge_leases_base_scope_fk;

ALTER TABLE commits ALTER COLUMN id DROP DEFAULT;
ALTER TABLE commits ALTER COLUMN id TYPE text USING id::text;
ALTER TABLE commits ALTER COLUMN parent_commit_id TYPE text USING parent_commit_id::text;
ALTER TABLE branches ALTER COLUMN head_commit_id TYPE text USING head_commit_id::text;
ALTER TABLE pack_ranges ALTER COLUMN pack_hash TYPE text USING pack_hash::text;
ALTER TABLE pack_ranges ALTER COLUMN base_commit_id TYPE text USING base_commit_id::text;
ALTER TABLE pack_ranges ALTER COLUMN target_commit_id TYPE text USING target_commit_id::text;
ALTER TABLE commit_parents ALTER COLUMN commit_id TYPE text USING commit_id::text;
ALTER TABLE commit_parents ALTER COLUMN parent_commit_id TYPE text USING parent_commit_id::text;
ALTER TABLE target_branch_merge_leases ALTER COLUMN source_commit_id TYPE text USING source_commit_id::text;
ALTER TABLE target_branch_merge_leases ALTER COLUMN target_commit_id TYPE text USING target_commit_id::text;
ALTER TABLE target_branch_merge_leases ALTER COLUMN base_commit_id TYPE text USING base_commit_id::text;
ALTER TABLE commits ADD COLUMN IF NOT EXISTS object_format smallint NOT NULL DEFAULT 1;
ALTER TABLE pack_ranges ADD COLUMN IF NOT EXISTS object_format smallint NOT NULL DEFAULT 1;
ALTER TABLE commits DROP CONSTRAINT IF EXISTS commits_object_format_check;
ALTER TABLE commits ADD CONSTRAINT commits_object_format_check CHECK (object_format IN (1, 2));
ALTER TABLE pack_ranges DROP CONSTRAINT IF EXISTS pack_ranges_object_format_check;
ALTER TABLE pack_ranges ADD CONSTRAINT pack_ranges_object_format_check CHECK (object_format IN (1, 2));

DO $$
BEGIN
	IF EXISTS (
		SELECT 1
		FROM pg_constraint
		WHERE conrelid = 'commits'::regclass
			AND contype = 'p'
			AND pg_get_constraintdef(oid) = 'PRIMARY KEY (id)'
	) THEN
		ALTER TABLE commits DROP CONSTRAINT commits_pkey;
		ALTER TABLE commits ADD PRIMARY KEY (tenant_id, repo_id, id);
	END IF;
END
$$;

DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'commits_parent_scope_fk') THEN
		ALTER TABLE commits ADD CONSTRAINT commits_parent_scope_fk
			FOREIGN KEY (tenant_id, repo_id, parent_commit_id)
			REFERENCES commits(tenant_id, repo_id, id);
	END IF;
	IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pack_ranges_target_scope_fk') THEN
		ALTER TABLE pack_ranges ADD CONSTRAINT pack_ranges_target_scope_fk
			FOREIGN KEY (tenant_id, repo_id, target_commit_id)
			REFERENCES commits(tenant_id, repo_id, id);
	END IF;
	IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pack_ranges_base_scope_fk') THEN
		ALTER TABLE pack_ranges ADD CONSTRAINT pack_ranges_base_scope_fk
			FOREIGN KEY (tenant_id, repo_id, base_commit_id)
			REFERENCES commits(tenant_id, repo_id, id);
	END IF;
	IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'branches_head_commit_scope_fk') THEN
		ALTER TABLE branches ADD CONSTRAINT branches_head_commit_scope_fk
			FOREIGN KEY (tenant_id, repo_id, head_commit_id)
			REFERENCES commits(tenant_id, repo_id, id);
	END IF;
	IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'commit_parents_parent_scope_fk') THEN
		ALTER TABLE commit_parents ADD CONSTRAINT commit_parents_parent_scope_fk
			FOREIGN KEY (tenant_id, repo_id, parent_commit_id)
			REFERENCES commits(tenant_id, repo_id, id);
	END IF;
	IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'target_branch_merge_leases_source_scope_fk') THEN
		ALTER TABLE target_branch_merge_leases ADD CONSTRAINT target_branch_merge_leases_source_scope_fk
			FOREIGN KEY (tenant_id, repo_id, source_commit_id)
			REFERENCES commits(tenant_id, repo_id, id);
	END IF;
	IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'target_branch_merge_leases_target_scope_fk') THEN
		ALTER TABLE target_branch_merge_leases ADD CONSTRAINT target_branch_merge_leases_target_scope_fk
			FOREIGN KEY (tenant_id, repo_id, target_commit_id)
			REFERENCES commits(tenant_id, repo_id, id);
	END IF;
	IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'target_branch_merge_leases_base_scope_fk') THEN
		ALTER TABLE target_branch_merge_leases ADD CONSTRAINT target_branch_merge_leases_base_scope_fk
			FOREIGN KEY (tenant_id, repo_id, base_commit_id)
			REFERENCES commits(tenant_id, repo_id, id);
	END IF;
END
$$;

COMMIT;

-- A review snapshot migration is tenant/repository scoped. "frozen" keeps
-- writers out while immutable CBOR objects and v2 frames are staged; both
-- legacy artifacts and every old-to-new mapping remain in place after either
-- cutover or rollback.
CREATE TABLE IF NOT EXISTS review_snapshot_migrations (
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL REFERENCES repositories(id),
	status text NOT NULL CHECK (status IN ('frozen', 'cutover', 'rolled_back')),
	generation uuid NOT NULL DEFAULT gen_random_uuid(),
	created_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now(),
	cutover_at timestamptz,
	PRIMARY KEY (tenant_id, repo_id),
	CONSTRAINT review_snapshot_migrations_repository_scope_fk
		FOREIGN KEY (tenant_id, repo_id)
		REFERENCES repositories(tenant_id, id)
);

CREATE TABLE IF NOT EXISTS review_snapshot_object_mappings (
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL REFERENCES repositories(id),
	legacy_snapshot_root text NOT NULL,
	cbor_snapshot_root text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT review_snapshot_object_mappings_repository_scope_fk
		FOREIGN KEY (tenant_id, repo_id)
		REFERENCES repositories(tenant_id, id),
	PRIMARY KEY (tenant_id, repo_id, legacy_snapshot_root),
	UNIQUE (tenant_id, repo_id, cbor_snapshot_root)
);

CREATE TABLE IF NOT EXISTS review_snapshot_commit_mappings (
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL REFERENCES repositories(id),
	legacy_commit_id text NOT NULL,
	v2_commit_id text NOT NULL,
	legacy_snapshot_root text NOT NULL,
	cbor_snapshot_root text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT review_snapshot_commit_mappings_repository_scope_fk
		FOREIGN KEY (tenant_id, repo_id)
		REFERENCES repositories(tenant_id, id),
	CONSTRAINT review_snapshot_commit_mappings_legacy_scope_fk
		FOREIGN KEY (tenant_id, repo_id, legacy_commit_id)
		REFERENCES commits(tenant_id, repo_id, id),
	PRIMARY KEY (tenant_id, repo_id, legacy_commit_id),
	UNIQUE (tenant_id, repo_id, v2_commit_id)
);

CREATE TABLE IF NOT EXISTS review_snapshot_pack_mappings (
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL REFERENCES repositories(id),
	legacy_pack_hash text NOT NULL,
	v2_pack_hash text NOT NULL,
	legacy_base_commit_id text,
	legacy_target_commit_id text NOT NULL,
	v2_base_commit_id text,
	v2_target_commit_id text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT review_snapshot_pack_mappings_repository_scope_fk
		FOREIGN KEY (tenant_id, repo_id)
		REFERENCES repositories(tenant_id, id),
	PRIMARY KEY (tenant_id, repo_id, legacy_pack_hash),
	UNIQUE (tenant_id, repo_id, v2_pack_hash)
);

ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE repositories ENABLE ROW LEVEL SECURITY;
ALTER TABLE commits ENABLE ROW LEVEL SECURITY;
ALTER TABLE commit_parents ENABLE ROW LEVEL SECURITY;
ALTER TABLE pack_ranges ENABLE ROW LEVEL SECURITY;
ALTER TABLE branches ENABLE ROW LEVEL SECURITY;
ALTER TABLE target_branch_merge_leases ENABLE ROW LEVEL SECURITY;
ALTER TABLE review_snapshot_migrations ENABLE ROW LEVEL SECURITY;
ALTER TABLE review_snapshot_object_mappings ENABLE ROW LEVEL SECURITY;
ALTER TABLE review_snapshot_commit_mappings ENABLE ROW LEVEL SECURITY;
ALTER TABLE review_snapshot_pack_mappings ENABLE ROW LEVEL SECURITY;

-- current_setting(..., true) returns NULL instead of raising if the tenant
-- context was never set on the session. That fail-closed behaviour is
-- important: an unset tenant should silently see zero rows rather than
-- accidentally bypass isolation or crash unrelated code paths.
DROP POLICY IF EXISTS tenant_isolation ON tenants;
CREATE POLICY tenant_isolation ON tenants
	FOR ALL
	USING (id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON repositories;
CREATE POLICY tenant_isolation ON repositories
	FOR ALL
	USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON commits;
CREATE POLICY tenant_isolation ON commits
	FOR ALL
	USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON commit_parents;
CREATE POLICY tenant_isolation ON commit_parents
	FOR ALL
	USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON pack_ranges;
CREATE POLICY tenant_isolation ON pack_ranges
	FOR ALL
	USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON branches;
CREATE POLICY tenant_isolation ON branches
	FOR ALL
	USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON target_branch_merge_leases;
CREATE POLICY tenant_isolation ON target_branch_merge_leases
	FOR ALL
	USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON review_snapshot_migrations;
CREATE POLICY tenant_isolation ON review_snapshot_migrations
	FOR ALL
	USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON review_snapshot_object_mappings;
CREATE POLICY tenant_isolation ON review_snapshot_object_mappings
	FOR ALL
	USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON review_snapshot_commit_mappings;
CREATE POLICY tenant_isolation ON review_snapshot_commit_mappings
	FOR ALL
	USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON review_snapshot_pack_mappings;
CREATE POLICY tenant_isolation ON review_snapshot_pack_mappings
	FOR ALL
	USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

-- Grant the application only the privileges needed for ordinary CRUD access to
-- metadata rows. Schema changes, ownership, and any RLS-bypass capability
-- remain with the migration/admin role, which keeps the blast radius of an
-- application credential compromise as small as this control plane allows.
GRANT USAGE ON SCHEMA public TO spool_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON tenants, repositories, commits, commit_parents, pack_ranges, branches, target_branch_merge_leases, review_snapshot_migrations, review_snapshot_object_mappings, review_snapshot_commit_mappings, review_snapshot_pack_mappings TO spool_app;
