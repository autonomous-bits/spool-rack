-- Add storage quota and usage tracking columns to tenants table.
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS storage_quota_bytes bigint NOT NULL DEFAULT 10737418240;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS storage_used_bytes bigint NOT NULL DEFAULT 0;

-- Assets table records contextual reference assets stored in content-addressable storage.
CREATE TABLE IF NOT EXISTS assets (
	tenant_id uuid NOT NULL REFERENCES tenants(id),
	repo_id uuid NOT NULL,
	hash text NOT NULL,
	size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
	mime_type text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT assets_repository_scope_fk
		FOREIGN KEY (tenant_id, repo_id)
		REFERENCES repositories(tenant_id, id),
	PRIMARY KEY (tenant_id, repo_id, hash)
);

CREATE INDEX IF NOT EXISTS assets_repo_idx ON assets (repo_id);

ALTER TABLE assets ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON assets;
CREATE POLICY tenant_isolation ON assets
	FOR ALL
	USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
	WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

GRANT SELECT, INSERT, UPDATE, DELETE ON assets TO spool_app;
