-- MANUAL v1/v2 -> v3 reviewed-OWNER migration; never runtime DDL.
-- Receipt format/dry_run remain immutable legacy request hints. No backfill.
-- The console/other consumers need a separately reviewed compatibility adapter.
BEGIN;
SET LOCAL ROLE qh_owner;
LOCK TABLE query_history.executions IN ACCESS EXCLUSIVE MODE;
ALTER TABLE query_history.executions
 ADD COLUMN effective_format text,
 ADD COLUMN effective_dry_run boolean,
 ADD CONSTRAINT executions_effective_v3 CHECK (
  (effective_format IS NULL AND effective_dry_run IS NULL) OR
  (effective_format IS NOT NULL AND effective_dry_run IS NOT NULL AND (
   (operation='sql' AND (effective_format='records' OR (effective_format='ndjson' AND NOT effective_dry_run))) OR
   (operation='explain' AND effective_format='records' AND effective_dry_run) OR
   (operation='shard' AND effective_format='arrow' AND NOT effective_dry_run)
  ))
 );
CREATE FUNCTION query_history.immutable_effective_v3() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF OLD.effective_format IS NOT NULL AND
    ROW(NEW.effective_format,NEW.effective_dry_run) IS DISTINCT FROM
    ROW(OLD.effective_format,OLD.effective_dry_run) THEN
  RAISE EXCEPTION 'immutable history effective execution' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION query_history.immutable_effective_v3() FROM PUBLIC;
CREATE TRIGGER immutable_effective_v3 BEFORE UPDATE ON query_history.executions FOR EACH ROW EXECUTE FUNCTION query_history.immutable_effective_v3();
-- Existing receipt/terminal triggers, row bounds, v1/v2 constraints, RLS,
-- reader/writer grants and original expiry are deliberately unchanged.
COMMIT;
