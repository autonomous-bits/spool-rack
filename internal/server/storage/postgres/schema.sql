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

-- Commits belong to both a tenant and a repository. Storing tenant_id directly
-- lets RLS decisions remain local to this table instead of requiring a join
-- through repositories to prove tenancy for every read or write. The scoped
-- foreign key and composite uniqueness keep that denormalised tenant_id honest
-- so a commit cannot claim one tenant while pointing at another tenant's repo.
CREATE TABLE IF NOT EXISTS commits (
	id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL REFERENCES repositories(id),
	parent_commit_id uuid REFERENCES commits(id),
	snapshot_root text NOT NULL,
	author text NOT NULL,
	message text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	UNIQUE (tenant_id, repo_id, id),
	CONSTRAINT commits_repository_scope_fk
		FOREIGN KEY (tenant_id, repo_id)
		REFERENCES repositories(tenant_id, id)
);

CREATE INDEX IF NOT EXISTS commits_repo_id_idx ON commits (repo_id);

-- Branch heads are mutable refs pointing at immutable commits. The primary key
-- is scoped by repository because branch names only need to be unique within a
-- single repository, not globally or per tenant. Extra scoped foreign keys
-- make sure a branch head cannot accidentally target a commit from some other
-- repository or tenant.
CREATE TABLE IF NOT EXISTS branches (
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL REFERENCES repositories(id),
	name text NOT NULL,
	head_commit_id uuid NOT NULL REFERENCES commits(id),
	updated_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT branches_repository_scope_fk
		FOREIGN KEY (tenant_id, repo_id)
		REFERENCES repositories(tenant_id, id),
	CONSTRAINT branches_head_commit_scope_fk
		FOREIGN KEY (tenant_id, repo_id, head_commit_id)
		REFERENCES commits(tenant_id, repo_id, id),
	PRIMARY KEY (repo_id, name)
);

ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE repositories ENABLE ROW LEVEL SECURITY;
ALTER TABLE commits ENABLE ROW LEVEL SECURITY;
ALTER TABLE branches ENABLE ROW LEVEL SECURITY;

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

DROP POLICY IF EXISTS tenant_isolation ON branches;
CREATE POLICY tenant_isolation ON branches
	FOR ALL
	USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

-- Grant the application only the privileges needed for ordinary CRUD access to
-- metadata rows. Schema changes, ownership, and any RLS-bypass capability
-- remain with the migration/admin role, which keeps the blast radius of an
-- application credential compromise as small as this control plane allows.
GRANT USAGE ON SCHEMA public TO spool_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON tenants, repositories, commits, branches TO spool_app;
