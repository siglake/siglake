-- MANUAL REVIEWED LOCAL-ONLY artifact. NOT run by JobStore startup.
-- Apply only to an isolated fixture after the existing job schema. No grants.
-- Trusted schema/admin/job role assumed. GUC is a mixed-version lifecycle
-- compatibility fence, NOT security against a compromised SQL role or DDL.
BEGIN;
ALTER TABLE public.siglake_query_jobs ADD COLUMN controls_version smallint
    CHECK (controls_version IS NULL OR controls_version = 1);
CREATE TABLE public.siglake_query_job_controls (
    job_id text PRIMARY KEY REFERENCES public.siglake_query_jobs(job_id) ON DELETE CASCADE,
    context_json jsonb NOT NULL CHECK (jsonb_typeof(context_json) = 'object'),
    execution_json jsonb
);
CREATE FUNCTION public.siglake_guard_controlled_job() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.controls_version IS DISTINCT FROM OLD.controls_version THEN
        RAISE EXCEPTION 'immutable job controls version';
    END IF;
    IF NEW.controls_version IS NOT NULL AND NEW.status = 'running' THEN
        IF current_setting('siglake.controlled_start', true) IS DISTINCT FROM
            (NEW.job_id || '|' || NEW.owner || '|1') OR
            NOT EXISTS (SELECT 1 FROM public.siglake_query_job_controls c
                WHERE c.job_id=NEW.job_id AND c.context_json->>'version'='1'
                  AND c.context_json->>'owner'=NEW.owner) THEN
            RAISE EXCEPTION 'controlled job start rejected';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER siglake_controlled_job_version BEFORE UPDATE ON public.siglake_query_jobs
FOR EACH ROW WHEN (NEW.controls_version IS DISTINCT FROM OLD.controls_version)
EXECUTE FUNCTION public.siglake_guard_controlled_job();
CREATE TRIGGER siglake_controlled_job_start BEFORE INSERT OR UPDATE OF status, started_at
ON public.siglake_query_jobs FOR EACH ROW EXECUTE FUNCTION public.siglake_guard_controlled_job();
CREATE FUNCTION public.siglake_guard_job_context() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF EXISTS (SELECT 1 FROM public.siglake_query_jobs WHERE job_id=OLD.job_id) THEN
            RAISE EXCEPTION 'immutable job context';
        END IF;
        RETURN OLD;
    END IF;
    IF NEW.job_id IS DISTINCT FROM OLD.job_id OR NEW.context_json IS DISTINCT FROM OLD.context_json THEN
        RAISE EXCEPTION 'immutable job context';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER siglake_controlled_context_immutable BEFORE UPDATE OR DELETE
ON public.siglake_query_job_controls FOR EACH ROW EXECUTE FUNCTION public.siglake_guard_job_context();
REVOKE ALL ON FUNCTION public.siglake_guard_controlled_job() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.siglake_guard_job_context() FROM PUBLIC;
REVOKE ALL ON public.siglake_query_job_controls FROM PUBLIC;
COMMIT;
