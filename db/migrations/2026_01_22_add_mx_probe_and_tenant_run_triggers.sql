BEGIN;

-- companies.attrs (present in live)
ALTER TABLE public.companies
  ADD COLUMN IF NOT EXISTS attrs text;

-- mx_probe_stats (present in live)
CREATE TABLE IF NOT EXISTS public.mx_probe_stats (
  id BIGSERIAL PRIMARY KEY,
  mx_host text NOT NULL,
  ts text NOT NULL DEFAULT CURRENT_TIMESTAMP,
  code integer,
  category text,
  error_kind text,
  elapsed_ms integer
);

CREATE INDEX IF NOT EXISTS idx_mx_probe_host_ts
  ON public.mx_probe_stats (mx_host, ts);

-- ------------------------------------------------------------
-- Trigger function: fill emails.tenant_id/run_id from company_id
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.fill_emails_tenant_run()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  t text;
  r text;
BEGIN
  IF NEW.company_id IS NOT NULL THEN
    SELECT c.tenant_id, c.run_id INTO t, r
    FROM public.companies c
    WHERE c.id = NEW.company_id;

    IF (NEW.tenant_id IS NULL OR NEW.tenant_id = '') AND t IS NOT NULL THEN
      NEW.tenant_id := t;
    END IF;

    IF (NEW.run_id IS NULL OR NEW.run_id = '') AND r IS NOT NULL THEN
      NEW.run_id := r;
    END IF;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_fill_emails_tenant_run ON public.emails;
CREATE TRIGGER trg_fill_emails_tenant_run
BEFORE INSERT OR UPDATE OF company_id ON public.emails
FOR EACH ROW
EXECUTE FUNCTION public.fill_emails_tenant_run();

-- ------------------------------------------------------------
-- Trigger function: fill verification_results.tenant_id/run_id from email_id
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.fill_vr_tenant_run()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  SELECT e.tenant_id, e.run_id
  INTO NEW.tenant_id, NEW.run_id
  FROM public.emails e
  WHERE e.id = NEW.email_id;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_fill_vr_tenant_run ON public.verification_results;
CREATE TRIGGER trg_fill_vr_tenant_run
BEFORE INSERT ON public.verification_results
FOR EACH ROW
EXECUTE FUNCTION public.fill_vr_tenant_run();

-- ------------------------------------------------------------
-- Trigger function: roll up run_metrics from latest VR rows per email
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.upsert_run_metrics_from_vr()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  rid text;
  tid text;
BEGIN
  rid := NEW.run_id;
  IF rid IS NULL OR rid = '' THEN
    RETURN NEW;
  END IF;

  tid := COALESCE(NULLIF(NEW.tenant_id, ''), (SELECT tenant_id FROM public.runs WHERE id = rid));
  IF tid IS NULL OR tid = '' THEN
    RETURN NEW;
  END IF;

  WITH latest AS (
    SELECT DISTINCT ON (vr.email_id)
      vr.email_id,
      vr.verify_status
    FROM public.verification_results vr
    WHERE vr.run_id = rid
    ORDER BY vr.email_id, vr.id DESC
  ),
  c AS (
    SELECT
      COUNT(*) FILTER (WHERE verify_status = 'valid') AS emails_valid,
      COUNT(*) FILTER (WHERE verify_status = 'invalid') AS emails_invalid,
      COUNT(*) FILTER (WHERE verify_status = 'risky_catch_all') AS emails_risky_catch_all,
      COUNT(*) FILTER (WHERE verify_status IN ('valid','invalid','risky_catch_all')) AS emails_verified
    FROM latest
  )
  INSERT INTO public.run_metrics (
    run_id, tenant_id,
    emails_valid, emails_invalid, emails_risky_catch_all, emails_verified
  )
  SELECT
    rid, tid,
    emails_valid, emails_invalid, emails_risky_catch_all, emails_verified
  FROM c
  ON CONFLICT (run_id) DO UPDATE SET
    tenant_id = EXCLUDED.tenant_id,
    emails_valid = EXCLUDED.emails_valid,
    emails_invalid = EXCLUDED.emails_invalid,
    emails_risky_catch_all = EXCLUDED.emails_risky_catch_all,
    emails_verified = EXCLUDED.emails_verified;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_upsert_run_metrics_from_vr ON public.verification_results;
CREATE TRIGGER trg_upsert_run_metrics_from_vr
AFTER INSERT OR UPDATE OF verify_status, run_id, verified_at ON public.verification_results
FOR EACH ROW
EXECUTE FUNCTION public.upsert_run_metrics_from_vr();

COMMIT;
