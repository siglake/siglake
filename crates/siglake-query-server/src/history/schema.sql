-- MANUAL reviewed-owner migration; never invoked by runtime code.
-- Provision qh_owner NOLOGIN, qh_writer LOGIN, qh_reader LOGIN separately.
-- All three must be distinct, without memberships; writer/reader own no objects.
-- Run as a reviewed administrator allowed to SET ROLE qh_owner, on catalog DB.
BEGIN;
SET LOCAL ROLE qh_owner;
CREATE SCHEMA query_history AUTHORIZATION qh_owner;
REVOKE ALL ON SCHEMA query_history FROM PUBLIC;
GRANT USAGE ON SCHEMA query_history TO qh_writer, qh_reader;

CREATE TABLE query_history.executions (
 execution_id uuid PRIMARY KEY CHECK (execution_id <> '00000000-0000-0000-0000-000000000000'),
 tenant text NOT NULL CHECK (octet_length(tenant) BETWEEN 1 AND 128 AND tenant ~ '^[A-Za-z0-9_-]+$' AND lower(tenant) NOT IN ('active','sealed','processing','committed','consumers')),
 issuer text CHECK (octet_length(issuer) BETWEEN 1 AND 2048),
 client_id text CHECK (octet_length(client_id) BETWEEN 1 AND 128),
 principal_kind text NOT NULL CHECK (principal_kind IN ('provider','unknown','internal')),
 received_at timestamptz NOT NULL,
 expires_at timestamptz NOT NULL,
 owner_uuid uuid NOT NULL CHECK (owner_uuid <> '00000000-0000-0000-0000-000000000000'),
 admitted_at timestamptz,
 finished_at timestamptz,
 admission_observed_at timestamptz,
 finished_observed_at timestamptz,
 observation_state text NOT NULL DEFAULT 'pending' CHECK (observation_state IN ('pending','terminal','indeterminate')),
 execution_admitted boolean, -- NULL means admission is genuinely unknown
 operation text NOT NULL CHECK (operation IN ('sql','explain','shard')),
 format text NOT NULL CHECK (format IN ('records','ndjson','csv','arrow','parquet')),
 priority text NOT NULL CHECK (priority IN ('interactive','batch')),
 dry_run boolean NOT NULL,
 redacted_sql text CHECK (redacted_sql IS NULL),
 redaction_version smallint NOT NULL CHECK (redaction_version = 1),
 redaction_status text NOT NULL CHECK (redaction_status IN ('omitted','input_limit')),
 outcome text CHECK (outcome IN ('success','rejected','failed','timed_out','cancelled','disconnected')),
 error_code text CHECK (error_code IN ('invalid_sql','validation','planning','admission','execution','timeout','cancelled','disconnected')),
 job_id uuid CHECK (job_id <> '00000000-0000-0000-0000-000000000000'),
 duration_ms bigint CHECK (duration_ms >= 0),
 cache_state text NOT NULL DEFAULT 'unknown' CHECK (cache_state IN ('hit','miss','bypass','unknown')),
 truncated boolean,
 estimated_bytes_scanned bigint CHECK (estimated_bytes_scanned >= 0),
 estimated_rows_processed bigint CHECK (estimated_rows_processed >= 0),
 measured_bytes_scanned bigint CHECK (measured_bytes_scanned >= 0),
 measured_rows_scanned bigint CHECK (measured_rows_scanned >= 0),
 returned_rows bigint CHECK (returned_rows >= 0),
 CHECK (expires_at = received_at + interval '720 hours'),
 CHECK ((principal_kind = 'provider' AND issuer IS NOT NULL AND client_id IS NOT NULL) OR (principal_kind <> 'provider' AND issuer IS NULL AND client_id IS NULL)),
 CHECK ((execution_admitted IS TRUE) = (admission_observed_at IS NOT NULL)),
 CHECK (admitted_at IS NULL OR execution_admitted IS TRUE),
 CHECK (admission_observed_at IS NULL OR admission_observed_at >= received_at),
 CHECK (finished_observed_at IS NULL OR finished_observed_at >= COALESCE(admission_observed_at,received_at)),
 CHECK (admitted_at IS NULL OR admitted_at >= received_at),
 CHECK (finished_at IS NULL OR finished_at >= COALESCE(admitted_at, received_at)),
 CHECK ((observation_state = 'terminal' AND outcome IS NOT NULL AND finished_observed_at IS NOT NULL AND execution_admitted IS NOT NULL) OR
        (observation_state <> 'terminal' AND outcome IS NULL AND finished_at IS NULL AND finished_observed_at IS NULL)),
 CHECK ((outcome IS NULL AND error_code IS NULL) OR (outcome = 'success' AND error_code IS NULL) OR
        (outcome IS NOT NULL AND error_code IS NOT NULL AND (
         (outcome = 'rejected' AND error_code IN ('invalid_sql','validation','planning','admission')) OR (outcome = 'failed' AND error_code = 'execution') OR
         (outcome = 'timed_out' AND error_code = 'timeout') OR (outcome = 'cancelled' AND error_code = 'cancelled') OR
         (outcome = 'disconnected' AND error_code = 'disconnected')))),
 CHECK (outcome IS DISTINCT FROM 'success' OR execution_admitted IS TRUE),
 CHECK (outcome IS DISTINCT FROM 'rejected' OR execution_admitted IS FALSE),
 CHECK (truncated IS DISTINCT FROM true OR outcome IS NOT DISTINCT FROM 'success'),
 CHECK (observation_state = 'terminal' OR (duration_ms IS NULL AND cache_state='unknown' AND truncated IS NULL AND job_id IS NULL AND estimated_bytes_scanned IS NULL AND estimated_rows_processed IS NULL AND measured_bytes_scanned IS NULL AND measured_rows_scanned IS NULL AND returned_rows IS NULL))
);
CREATE INDEX executions_tenant_time ON query_history.executions (tenant, received_at DESC, execution_id DESC);
CREATE INDEX executions_principal_time ON query_history.executions (tenant, issuer, client_id, received_at DESC, execution_id DESC);
CREATE INDEX executions_owner ON query_history.executions (owner_uuid);
CREATE INDEX executions_expiry ON query_history.executions (expires_at, execution_id);

-- Protect original identity/receipt across every retry and recovery update.
CREATE FUNCTION query_history.immutable_receipt() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
 IF octet_length(to_jsonb(NEW)::text) > 8192 THEN
 RAISE EXCEPTION 'history payload limit' USING ERRCODE = '23514';
 END IF;
 IF TG_OP = 'INSERT' THEN RETURN NEW; END IF;
 IF OLD.admission_observed_at IS NOT NULL AND NEW.admission_observed_at IS DISTINCT FROM OLD.admission_observed_at THEN
 RAISE EXCEPTION 'immutable history admission observation' USING ERRCODE = '23514';
 END IF;
 IF OLD.admitted_at IS NOT NULL AND NEW.admitted_at IS DISTINCT FROM OLD.admitted_at THEN
 RAISE EXCEPTION 'immutable history admission' USING ERRCODE = '23514';
 END IF;
 IF ROW(NEW.execution_id,NEW.tenant,NEW.issuer,NEW.client_id,NEW.principal_kind,NEW.received_at,NEW.expires_at,NEW.owner_uuid,NEW.operation,NEW.format,NEW.priority,NEW.dry_run,NEW.redacted_sql,NEW.redaction_version,NEW.redaction_status)
 IS DISTINCT FROM ROW(OLD.execution_id,OLD.tenant,OLD.issuer,OLD.client_id,OLD.principal_kind,OLD.received_at,OLD.expires_at,OLD.owner_uuid,OLD.operation,OLD.format,OLD.priority,OLD.dry_run,OLD.redacted_sql,OLD.redaction_version,OLD.redaction_status) THEN
 RAISE EXCEPTION 'immutable history receipt' USING ERRCODE = '23514';
 END IF;
 IF OLD.observation_state = 'terminal' AND NEW IS DISTINCT FROM OLD THEN
 RAISE EXCEPTION 'immutable terminal history' USING ERRCODE = '23514';
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION query_history.immutable_receipt() FROM PUBLIC;
CREATE TRIGGER immutable_receipt BEFORE INSERT OR UPDATE ON query_history.executions FOR EACH ROW EXECUTE FUNCTION query_history.immutable_receipt();

CREATE TABLE query_history.owners (
 owner_uuid uuid PRIMARY KEY CHECK (owner_uuid <> '00000000-0000-0000-0000-000000000000'),
 heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 coverage_version smallint NOT NULL CHECK (coverage_version = 1)
);
CREATE INDEX owners_heartbeat ON query_history.owners (heartbeat_at,owner_uuid);
-- Minimal future lifecycle gap storage, not a telemetry/event ingestion service.
CREATE TABLE query_history.coverage (
 gap_id uuid PRIMARY KEY CHECK (gap_id <> '00000000-0000-0000-0000-000000000000'),
 tenant text NOT NULL CHECK (octet_length(tenant) BETWEEN 1 AND 128 AND tenant ~ '^[A-Za-z0-9_-]+$' AND lower(tenant) NOT IN ('active','sealed','processing','committed','consumers')),
 received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 expires_at timestamptz NOT NULL,
 started_at timestamptz NOT NULL,
 ended_at timestamptz,
 reason text NOT NULL CHECK (reason IN ('store_unavailable','queue_full','unknown_crash')),
 CHECK (expires_at = received_at + interval '720 hours'),
 CHECK (started_at <= received_at),
 CHECK (ended_at IS NULL OR ended_at >= started_at)
);
CREATE FUNCTION query_history.immutable_gap_receipt() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
 IF ROW(NEW.gap_id,NEW.tenant,NEW.received_at,NEW.expires_at) IS DISTINCT FROM ROW(OLD.gap_id,OLD.tenant,OLD.received_at,OLD.expires_at) THEN
 RAISE EXCEPTION 'immutable history gap receipt' USING ERRCODE = '23514';
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION query_history.immutable_gap_receipt() FROM PUBLIC;
CREATE TRIGGER immutable_gap_receipt BEFORE UPDATE ON query_history.coverage FOR EACH ROW EXECUTE FUNCTION query_history.immutable_gap_receipt();
CREATE INDEX coverage_expiry ON query_history.coverage (expires_at, gap_id);
ALTER TABLE query_history.executions ENABLE ROW LEVEL SECURITY;
ALTER TABLE query_history.executions FORCE ROW LEVEL SECURITY;
ALTER TABLE query_history.coverage ENABLE ROW LEVEL SECURITY;
ALTER TABLE query_history.coverage FORCE ROW LEVEL SECURITY;
CREATE POLICY writer ON query_history.executions TO qh_writer USING (true) WITH CHECK (expires_at > clock_timestamp());
CREATE POLICY writer ON query_history.coverage TO qh_writer USING (true) WITH CHECK (expires_at > clock_timestamp());
CREATE POLICY reader ON query_history.executions FOR SELECT TO qh_reader USING (tenant = nullif(current_setting('query_history.tenant',true),'') AND expires_at > clock_timestamp());
CREATE POLICY reader ON query_history.coverage FOR SELECT TO qh_reader USING (tenant = nullif(current_setting('query_history.tenant',true),'') AND expires_at > clock_timestamp());
REVOKE ALL ON ALL TABLES IN SCHEMA query_history FROM PUBLIC;
GRANT SELECT,INSERT,UPDATE,DELETE ON query_history.executions,query_history.coverage,query_history.owners TO qh_writer;
GRANT SELECT ON query_history.executions,query_history.coverage TO qh_reader;
COMMIT;
