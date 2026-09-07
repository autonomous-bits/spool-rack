-- migration_retirement_confirmations records an operator's explicit,
-- auditable acknowledgement that old clients speaking a prior graphcontract
-- or wire-protocol version have been confirmed retired. The migration runner
-- (see postgres.Migrate) refuses to apply a migration file marked with a
-- "-- destructive:" directive until a matching row exists here, so a
-- non-additive schema change cannot land mid-rollout while an old client
-- might still depend on the shape it removes.
--
-- This table is also bootstrapped defensively by the runner itself (so the
-- gate works even before this migration has run), but it is tracked here too
-- so it participates in ordinary backup/restore and appears in schema
-- introspection like every other control-plane table.
CREATE TABLE IF NOT EXISTS migration_retirement_confirmations (
	name text PRIMARY KEY,
	confirmed_at timestamptz NOT NULL DEFAULT now(),
	note text NOT NULL DEFAULT ''
);
