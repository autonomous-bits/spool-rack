-- workspaces view provides a first-class workspace projection over the
-- repositories table so Spool multi-repo workspaces map naturally to the
-- control plane while preserving existing repository table references, foreign
-- keys, and RLS policies.
CREATE OR REPLACE VIEW workspaces AS
	SELECT id, tenant_id, name, created_at
	FROM repositories;
