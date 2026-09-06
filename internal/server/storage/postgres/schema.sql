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
	snapshot_root text NOT NULL,
	object_format smallint NOT NULL DEFAULT 1 CHECK (object_format IN (1, 2, 3)),
	author text NOT NULL,
	message text NOT NULL,
	commit_time timestamptz NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (tenant_id, repo_id, id),
	CONSTRAINT commits_repository_scope_fk
		FOREIGN KEY (tenant_id, repo_id)
		REFERENCES repositories(tenant_id, id)
);

-- object_format 3 identifies a commit registered from a verified native
-- Spool pack (internal/server/nativepush), distinct from the legacy opaque
-- format (1) and Rack's own v2 CBOR pack framing (2). Widen the constraint on
-- databases created before native pushes were supported.
ALTER TABLE commits DROP CONSTRAINT IF EXISTS commits_object_format_check;
ALTER TABLE commits ADD CONSTRAINT commits_object_format_check CHECK (object_format IN (1, 2, 3));

CREATE INDEX IF NOT EXISTS commits_repo_id_idx ON commits (repo_id);

-- Supersede the legacy denormalized first-parent column on databases created
-- before the canonical graphcontract parent collection was adopted.
ALTER TABLE commits DROP CONSTRAINT IF EXISTS commits_parent_scope_fk;
ALTER TABLE commits DROP COLUMN IF EXISTS parent_commit_id;
ALTER TABLE commits ADD COLUMN IF NOT EXISTS commit_time timestamptz NOT NULL DEFAULT 'epoch';

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

-- Branch deletion is a soft delete: per
-- adr-immutable-commit-retention-on-branch-deletion, removing a branch must
-- never delete the historical commits or audit trail it referenced. Keeping
-- the row (with its head_commit_id intact) instead of deleting it lets
-- retention/GC keep treating a deleted branch's history as a valid
-- reachability root exactly like a live one.
ALTER TABLE branches ADD COLUMN IF NOT EXISTS deleted_at timestamptz;

-- Leases reference a branch together with its tenant and repository scope.
-- Keep this candidate key separate and before the lease table so fresh schema
-- bootstrap can create that foreign key. The conventional constraint-backed
-- index name lets this remain a no-op for databases created by earlier
-- schema versions that already have the equivalent unique constraint.
CREATE UNIQUE INDEX IF NOT EXISTS branches_tenant_id_repo_id_name_key
	ON branches (tenant_id, repo_id, name);

-- The normalized table is the sole ordered parent representation. A canonical
-- graphcontract.Commit permits zero or more parents.
CREATE TABLE IF NOT EXISTS commit_parents (
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL REFERENCES repositories(id),
	commit_id text NOT NULL,
	parent_position integer NOT NULL CHECK (parent_position >= 1),
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
ALTER TABLE commit_parents DROP CONSTRAINT IF EXISTS commit_parents_parent_position_check;
ALTER TABLE commit_parents ALTER COLUMN parent_position TYPE integer;
ALTER TABLE commit_parents ADD CONSTRAINT commit_parents_parent_position_check CHECK (parent_position >= 1);

-- Native packs are the immutable, verified binary pack containers described
-- by graphcontract/pack.go (distinct from the legacy CBOR PackFrameV2 used by
-- the sync engine). Each row records the pack's client-supplied PackID
-- alongside the content hash under which its raw bytes are stored in CAS, and
-- optionally the commit the pack was uploaded to make reachable.
CREATE TABLE IF NOT EXISTS native_packs (
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL REFERENCES repositories(id),
	pack_id text NOT NULL,
	cas_pack_hash text NOT NULL,
	commit_id text,
	object_count integer NOT NULL CHECK (object_count >= 0),
	created_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT native_packs_repository_scope_fk
		FOREIGN KEY (tenant_id, repo_id)
		REFERENCES repositories(tenant_id, id),
	CONSTRAINT native_packs_commit_scope_fk
		FOREIGN KEY (tenant_id, repo_id, commit_id)
		REFERENCES commits(tenant_id, repo_id, id),
	PRIMARY KEY (tenant_id, repo_id, pack_id)
);

-- Native pack objects index every verified object reachable through a native
-- pack by its content-derived object ID, scoped strictly to one tenant's
-- repository. The unique constraint on (tenant_id, repo_id, object_id) is a
-- deliberate content-addressed dedup: since an object ID is a hash of its
-- canonical bytes, any two verified entries sharing an object ID must carry
-- identical content, so only the first indexed location needs to be kept.
CREATE TABLE IF NOT EXISTS native_pack_objects (
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL REFERENCES repositories(id),
	pack_id text NOT NULL,
	object_id text NOT NULL,
	pack_offset bigint NOT NULL CHECK (pack_offset >= 0),
	compressed_size bigint NOT NULL CHECK (compressed_size > 0),
	uncompressed_size bigint NOT NULL CHECK (uncompressed_size > 0),
	crc32 bigint NOT NULL CHECK (crc32 >= 0 AND crc32 <= 4294967295),
	created_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT native_pack_objects_pack_scope_fk
		FOREIGN KEY (tenant_id, repo_id, pack_id)
		REFERENCES native_packs(tenant_id, repo_id, pack_id),
	PRIMARY KEY (tenant_id, repo_id, object_id)
);

CREATE INDEX IF NOT EXISTS native_pack_objects_pack_idx ON native_pack_objects (repo_id, pack_id);

-- Active transactions root retention for in-flight work that has not yet
-- become reachable through any branch ref — most importantly a concurrent
-- push in progress, whose objects must never be collected out from under it.
-- A row may anchor a commit_id, an object_id, or both; deliberately no
-- foreign keys are placed on either column, since a transaction may need to
-- reserve an identifier before any other table has a row for it yet.
CREATE TABLE IF NOT EXISTS repo_active_transactions (
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL REFERENCES repositories(id),
	transaction_id text NOT NULL,
	commit_id text,
	object_id text,
	expires_at timestamptz,
	created_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT repo_active_transactions_repository_scope_fk
		FOREIGN KEY (tenant_id, repo_id)
		REFERENCES repositories(tenant_id, id),
	CONSTRAINT repo_active_transactions_root_check
		CHECK (commit_id IS NOT NULL OR object_id IS NOT NULL),
	PRIMARY KEY (tenant_id, repo_id, transaction_id)
);

-- Audit records satisfy req-tenant-audit-and-access-logging generally, and
-- specifically anchor retention for any commit or object a compliance trail
-- must keep referring to independent of current branch state. Like
-- repo_active_transactions, commit_id/object_id are intentionally
-- unconstrained by foreign keys.
CREATE TABLE IF NOT EXISTS audit_records (
	id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL REFERENCES repositories(id),
	event_type text NOT NULL,
	subject text NOT NULL,
	commit_id text,
	object_id text,
	created_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT audit_records_repository_scope_fk
		FOREIGN KEY (tenant_id, repo_id)
		REFERENCES repositories(tenant_id, id)
);

CREATE INDEX IF NOT EXISTS audit_records_repo_idx ON audit_records (repo_id, created_at);

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

ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE repositories ENABLE ROW LEVEL SECURITY;
ALTER TABLE commits ENABLE ROW LEVEL SECURITY;
ALTER TABLE commit_parents ENABLE ROW LEVEL SECURITY;
ALTER TABLE pack_ranges ENABLE ROW LEVEL SECURITY;
ALTER TABLE branches ENABLE ROW LEVEL SECURITY;
ALTER TABLE target_branch_merge_leases ENABLE ROW LEVEL SECURITY;
ALTER TABLE native_packs ENABLE ROW LEVEL SECURITY;
ALTER TABLE native_pack_objects ENABLE ROW LEVEL SECURITY;
ALTER TABLE repo_active_transactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_records ENABLE ROW LEVEL SECURITY;

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

DROP POLICY IF EXISTS tenant_isolation ON native_packs;
CREATE POLICY tenant_isolation ON native_packs
	FOR ALL
	USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON native_pack_objects;
CREATE POLICY tenant_isolation ON native_pack_objects
	FOR ALL
	USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON repo_active_transactions;
CREATE POLICY tenant_isolation ON repo_active_transactions
	FOR ALL
	USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON audit_records;
CREATE POLICY tenant_isolation ON audit_records
	FOR ALL
	USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

-- Grant the application only the privileges needed for ordinary CRUD access to
-- metadata rows. Schema changes, ownership, and any RLS-bypass capability
-- remain with the migration/admin role, which keeps the blast radius of an
-- application credential compromise as small as this control plane allows.
GRANT USAGE ON SCHEMA public TO spool_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON tenants, repositories, commits, commit_parents, pack_ranges, branches, target_branch_merge_leases, native_packs, native_pack_objects, repo_active_transactions, audit_records TO spool_app;
