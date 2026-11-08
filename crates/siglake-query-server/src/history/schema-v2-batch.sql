-- MANUAL v1 -> v2 reviewed-OWNER migration. Never runtime writer DDL.
-- Apply schema.sql first on a fresh install. Existing v1 data is neither
-- relabeled nor backfilled. Incompatible constraints/data abort atomically.
BEGIN;
SET LOCAL ROLE qh_owner;
LOCK TABLE query_history.executions IN ACCESS EXCLUSIVE MODE;
DO $$
DECLARE original_name text;
BEGIN
 SELECT conname INTO STRICT original_name FROM pg_constraint
 WHERE conrelid='query_history.executions'::regclass
 AND contype='c' AND convalidated
 AND pg_get_constraintdef(oid) = 'CHECK (((observation_state = ''terminal''::text) OR ((duration_ms IS NULL) AND (cache_state = ''unknown''::text) AND (truncated IS NULL) AND (job_id IS NULL) AND (estimated_bytes_scanned IS NULL) AND (estimated_rows_processed IS NULL) AND (measured_bytes_scanned IS NULL) AND (measured_rows_scanned IS NULL) AND (returned_rows IS NULL))))';
 EXECUTE format('ALTER TABLE query_history.executions DROP CONSTRAINT %I', original_name);
END $$;
-- Exactly the v1 invariant, except job_id may now carry a queued association.
ALTER TABLE query_history.executions ADD CONSTRAINT executions_nonterminal_evidence_v2
 CHECK (observation_state = 'terminal' OR (duration_ms IS NULL AND cache_state='unknown' AND truncated IS NULL AND estimated_bytes_scanned IS NULL AND estimated_rows_processed IS NULL AND measured_bytes_scanned IS NULL AND measured_rows_scanned IS NULL AND returned_rows IS NULL));
ALTER TABLE query_history.executions ADD CONSTRAINT executions_batch_job_v2
 CHECK (job_id IS NULL OR (operation='sql' AND priority='batch' AND format='records' AND NOT dry_run));
CREATE UNIQUE INDEX executions_one_parent_per_job_v2 ON query_history.executions(job_id) WHERE job_id IS NOT NULL;
CREATE FUNCTION query_history.immutable_job_association_v2() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF OLD.job_id IS NOT NULL AND NEW.job_id IS DISTINCT FROM OLD.job_id THEN
 RAISE EXCEPTION 'immutable history job association' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION query_history.immutable_job_association_v2() FROM PUBLIC;
CREATE TRIGGER immutable_job_association_v2 BEFORE UPDATE ON query_history.executions FOR EACH ROW EXECUTE FUNCTION query_history.immutable_job_association_v2();
COMMIT;
