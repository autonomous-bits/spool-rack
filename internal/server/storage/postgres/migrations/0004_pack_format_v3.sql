-- Allow pack_ranges to record canonical v3 pack frames supporting general DAG commit histories.
ALTER TABLE pack_ranges DROP CONSTRAINT IF EXISTS pack_ranges_object_format_check;
ALTER TABLE pack_ranges ADD CONSTRAINT pack_ranges_object_format_check CHECK (object_format IN (1, 2, 3));
