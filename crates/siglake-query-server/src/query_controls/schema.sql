-- MANUAL reviewed-owner schema v1. NEVER applied by the engine.
-- Separately provision qp_owner NOLOGIN, qp_reader/qp_manager LOGIN, all
-- NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT,
-- with no memberships. Reader/manager own nothing. Revoke PUBLIC database
-- CREATE/TEMP and unrelated schema/table/column/function privileges first.
-- No cross-database foreign keys/cascades, automatic credential rebinding,
-- retention duration, cleanup job, or production activation is authorized.
BEGIN;
SET LOCAL ROLE qp_owner;
CREATE SCHEMA query_policy AUTHORIZATION qp_owner;
REVOKE ALL ON SCHEMA query_policy FROM PUBLIC;
GRANT USAGE ON SCHEMA query_policy TO qp_reader, qp_manager;

CREATE TABLE query_policy.agent_limits (
 tenant text COLLATE "C" NOT NULL CHECK (octet_length(tenant) BETWEEN 1 AND 128 AND tenant ~ '^[A-Za-z0-9_-]+$' AND lower(tenant) NOT IN ('active','sealed','processing','committed','consumers')),
 verified_issuer text COLLATE "C" NOT NULL CHECK (octet_length(verified_issuer) BETWEEN 1 AND 2048 AND verified_issuer !~ '[[:cntrl:]]'),
 verified_client_id text COLLATE "C" NOT NULL CHECK (octet_length(verified_client_id) BETWEEN 1 AND 128 AND verified_client_id !~ '[[:cntrl:]]'),
 org_uuid uuid NOT NULL CHECK (org_uuid <> '00000000-0000-0000-0000-000000000000'),
 credential_uuid uuid NOT NULL CHECK (credential_uuid <> '00000000-0000-0000-0000-000000000000'),
 schema_version smallint NOT NULL CHECK (schema_version = 1),
 revision bigint NOT NULL CHECK (revision > 0),
 timeout_seconds bigint CHECK (timeout_seconds > 0 AND timeout_seconds <= 9223372036854775),
 scan_bytes_warning bigint CHECK (scan_bytes_warning > 0),
 updated_at timestamptz NOT NULL,
 PRIMARY KEY (tenant, verified_issuer, verified_client_id),
 UNIQUE (org_uuid, credential_uuid)
);
CREATE TABLE query_policy.edits (
 tenant text COLLATE "C" NOT NULL,
 org_uuid uuid NOT NULL,
 credential_uuid uuid NOT NULL,
 schema_version smallint NOT NULL CHECK (schema_version = 1),
 revision bigint NOT NULL CHECK (revision > 0),
 actor_sub text NOT NULL CHECK (octet_length(actor_sub) BETWEEN 1 AND 256 AND actor_sub !~ '[[:cntrl:]]'),
 edited_at timestamptz NOT NULL,
 old_timeout_seconds bigint CHECK (old_timeout_seconds > 0 AND old_timeout_seconds <= 9223372036854775),
 old_scan_bytes_warning bigint CHECK (old_scan_bytes_warning > 0),
 new_timeout_seconds bigint CHECK (new_timeout_seconds > 0 AND new_timeout_seconds <= 9223372036854775),
 new_scan_bytes_warning bigint CHECK (new_scan_bytes_warning > 0),
 PRIMARY KEY (org_uuid, credential_uuid, revision)
);
ALTER TABLE query_policy.agent_limits ENABLE ROW LEVEL SECURITY;
ALTER TABLE query_policy.agent_limits FORCE ROW LEVEL SECURITY;
ALTER TABLE query_policy.edits ENABLE ROW LEVEL SECURITY;
ALTER TABLE query_policy.edits FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON query_policy.agent_limits USING (tenant = current_setting('query_policy.tenant', true)) WITH CHECK (tenant = current_setting('query_policy.tenant', true));
CREATE POLICY tenant_scope ON query_policy.edits USING (tenant = current_setting('query_policy.tenant', true)) WITH CHECK (tenant = current_setting('query_policy.tenant', true));

CREATE FUNCTION query_policy.immutable_binding() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
 IF (NEW.tenant, NEW.verified_issuer, NEW.verified_client_id, NEW.org_uuid, NEW.credential_uuid, NEW.schema_version)
    IS DISTINCT FROM (OLD.tenant, OLD.verified_issuer, OLD.verified_client_id, OLD.org_uuid, OLD.credential_uuid, OLD.schema_version)
    OR OLD.revision = 9223372036854775807 OR NEW.revision <> OLD.revision + 1 THEN
  RAISE EXCEPTION USING ERRCODE='QP002', MESSAGE='policy binding rejected';
 END IF;
 RETURN NEW;
END
$$;
CREATE TRIGGER immutable_binding BEFORE UPDATE ON query_policy.agent_limits FOR EACH ROW EXECUTE FUNCTION query_policy.immutable_binding();

CREATE FUNCTION query_policy.save(p_tenant text, p_issuer text, p_client text, p_org uuid, p_credential uuid, p_actor text, p_expected bigint, p_timeout bigint, p_warning bigint)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
 prior query_policy.agent_limits%ROWTYPE;
 inserted boolean := false;
 saved bigint;
 stamp timestamptz := pg_catalog.clock_timestamp();
BEGIN
 IF current_setting('query_policy.tenant', true) COLLATE "C" IS DISTINCT FROM p_tenant COLLATE "C"
    OR p_tenant IS NULL OR octet_length(p_tenant) NOT BETWEEN 1 AND 128 OR p_tenant !~ '^[A-Za-z0-9_-]+$'
    OR lower(p_tenant) IN ('active','sealed','processing','committed','consumers')
    OR p_issuer IS NULL OR octet_length(p_issuer) NOT BETWEEN 1 AND 2048 OR p_issuer ~ '[[:cntrl:]]'
    OR p_client IS NULL OR octet_length(p_client) NOT BETWEEN 1 AND 128 OR p_client ~ '[[:cntrl:]]'
    OR p_org IS NULL OR p_org = '00000000-0000-0000-0000-000000000000'
    OR p_credential IS NULL OR p_credential = '00000000-0000-0000-0000-000000000000'
    OR p_actor IS NULL OR octet_length(p_actor) NOT BETWEEN 1 AND 256 OR p_actor ~ '[[:cntrl:]]'
    OR p_expected IS NULL OR p_expected < 0 OR p_expected = 9223372036854775807
    OR (p_timeout IS NOT NULL AND (p_timeout <= 0 OR p_timeout > 9223372036854775))
    OR (p_warning IS NOT NULL AND p_warning <= 0) THEN
  RAISE EXCEPTION USING ERRCODE='QP003', MESSAGE='policy input rejected';
 END IF;
 IF p_expected = 0 THEN
  INSERT INTO query_policy.agent_limits VALUES (p_tenant,p_issuer,p_client,p_org,p_credential,1,1,p_timeout,p_warning,stamp)
   ON CONFLICT DO NOTHING RETURNING true INTO inserted;
 END IF;
 IF inserted IS TRUE THEN
  saved := 1;
 ELSE
  SELECT * INTO prior FROM query_policy.agent_limits
   WHERE tenant=p_tenant AND verified_issuer=p_issuer AND verified_client_id=p_client FOR UPDATE;
  IF NOT FOUND THEN
   IF p_expected = 0 THEN
    RAISE EXCEPTION USING ERRCODE='QP002', MESSAGE='policy binding rejected';
   END IF;
   RAISE EXCEPTION USING ERRCODE='QP001', MESSAGE='policy revision conflict';
  END IF;
  IF prior.org_uuid <> p_org OR prior.credential_uuid <> p_credential THEN
   RAISE EXCEPTION USING ERRCODE='QP002', MESSAGE='policy binding rejected';
  END IF;
  IF prior.revision <> p_expected THEN
   RAISE EXCEPTION USING ERRCODE='QP001', MESSAGE='policy revision conflict';
  END IF;
  saved := prior.revision + 1;
  UPDATE query_policy.agent_limits SET revision=saved, timeout_seconds=p_timeout, scan_bytes_warning=p_warning, updated_at=stamp
   WHERE tenant=p_tenant AND verified_issuer=p_issuer AND verified_client_id=p_client;
 END IF;
 INSERT INTO query_policy.edits VALUES (p_tenant,p_org,p_credential,1,saved,p_actor,stamp,prior.timeout_seconds,prior.scan_bytes_warning,p_timeout,p_warning);
 RETURN saved;
END
$$;
REVOKE ALL ON ALL TABLES IN SCHEMA query_policy FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA query_policy FROM PUBLIC;
GRANT SELECT ON query_policy.agent_limits TO qp_reader, qp_manager;
GRANT EXECUTE ON FUNCTION query_policy.save(text,text,text,uuid,uuid,text,bigint,bigint,bigint) TO qp_manager;
COMMIT;
