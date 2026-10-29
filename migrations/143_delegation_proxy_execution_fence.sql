CREATE TABLE delegation_proxy_quarantines (
 instance_id UUID PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 first_observed_at TIMESTAMPTZ NOT NULL DEFAULT (clock_timestamp())
);
-- Relays retain their identity independently of editable metadata and cannot run natively.
CREATE INDEX idx_task_instances_delegation_proxy ON task_instances(id) WHERE jsonb_typeof(metadata)='object' AND metadata ? 'orch8_delegation_proxy';
CREATE INDEX idx_delegation_publications_proxy_instances ON delegation_publications(instance_id) WHERE NOT COALESCE(
   jsonb_typeof(record->'version')='number' AND record->>'version'='1'
   AND replace(lower(record#>>'{task_owner,continuity_id}'),'-','') ~ '^[0-9a-f]{32}$'
   AND replace(lower(record#>>'{task_owner,continuity_id}'),'-','') = replace(lower(record#>>'{delegation,parent_continuity_id}'),'-',''),false);
CREATE VIEW delegation_proxy_instances AS
 SELECT id AS instance_id FROM task_instances WHERE jsonb_typeof(metadata)='object' AND metadata ? 'orch8_delegation_proxy'
 UNION ALL
 SELECT instance_id FROM delegation_publications WHERE NOT COALESCE(
   jsonb_typeof(record->'version')='number' AND record->>'version'='1'
   AND replace(lower(record#>>'{task_owner,continuity_id}'),'-','') ~ '^[0-9a-f]{32}$'
   AND replace(lower(record#>>'{task_owner,continuity_id}'),'-','') = replace(lower(record#>>'{delegation,parent_continuity_id}'),'-',''),false)
 UNION ALL SELECT instance_id FROM delegation_proxy_quarantines;
CREATE VIEW delegation_proxy_native_history AS
 SELECT instance_id FROM delegation_proxy_quarantines
 UNION ALL
 SELECT id AS instance_id FROM task_instances WHERE CASE
 WHEN EXISTS(SELECT 1 FROM delegation_proxy_instances relay WHERE relay.instance_id=task_instances.id)
 THEN state='running' OR context#>>'{runtime,run_id}' IS NOT NULL OR context#>>'{runtime,started_at}' IS NOT NULL
   OR context#>>'{runtime,current_step}' IS NOT NULL OR context#>>'{runtime,current_step_started_at}' IS NOT NULL
   OR EXISTS(SELECT 1 FROM execution_tree n WHERE n.instance_id=task_instances.id)
 ELSE false END;
CREATE FUNCTION fence_delegation_proxy_instance() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' THEN
 IF jsonb_typeof(OLD.metadata)='object' AND OLD.metadata ? 'orch8_delegation_proxy'
    AND (OLD.metadata->'orch8_delegation_proxy')::text IS DISTINCT FROM (NEW.metadata->'orch8_delegation_proxy')::text THEN
  RAISE EXCEPTION 'delegation relay identity is immutable';
 END IF;
 IF NOT (jsonb_typeof(OLD.metadata)='object' AND OLD.metadata ? 'orch8_delegation_proxy') AND (jsonb_typeof(NEW.metadata)='object' AND NEW.metadata ? 'orch8_delegation_proxy')
    AND NOT EXISTS(SELECT 1 FROM delegation_proxy_instances WHERE instance_id=OLD.id) THEN
  RAISE EXCEPTION 'native instance cannot become a delegation relay';
 END IF;
 IF EXISTS(SELECT 1 FROM delegation_proxy_instances WHERE instance_id=OLD.id) AND (
   OLD.context#>>'{runtime,run_id}' IS DISTINCT FROM NEW.context#>>'{runtime,run_id}'
   OR OLD.context#>>'{runtime,started_at}' IS DISTINCT FROM NEW.context#>>'{runtime,started_at}'
   OR OLD.context#>>'{runtime,current_step}' IS DISTINCT FROM NEW.context#>>'{runtime,current_step}'
   OR OLD.context#>>'{runtime,current_step_started_at}' IS DISTINCT FROM NEW.context#>>'{runtime,current_step_started_at}') THEN
  RAISE EXCEPTION 'delegation relay native runtime history is immutable';
 END IF;
 END IF;
 IF NEW.state='running' AND ((jsonb_typeof(NEW.metadata)='object' AND NEW.metadata ? 'orch8_delegation_proxy')
    OR EXISTS(SELECT 1 FROM delegation_proxy_instances WHERE instance_id=NEW.id)) THEN
  RAISE EXCEPTION 'delegation relay cannot enter native execution';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER delegation_proxy_instance_fence BEFORE INSERT OR UPDATE ON task_instances
 FOR EACH ROW EXECUTE FUNCTION fence_delegation_proxy_instance();
INSERT INTO delegation_proxy_quarantines(instance_id,tenant_id)
 SELECT id,tenant_id FROM task_instances WHERE EXISTS(SELECT 1 FROM delegation_proxy_native_history history WHERE history.instance_id=task_instances.id) ON CONFLICT DO NOTHING;
CREATE FUNCTION protect_delegation_proxy_quarantine() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'delegation relay quarantine is immutable'; END $$;
CREATE TRIGGER delegation_proxy_quarantine_immutable BEFORE UPDATE OR DELETE ON delegation_proxy_quarantines
 FOR EACH ROW EXECUTE FUNCTION protect_delegation_proxy_quarantine();
CREATE FUNCTION capture_delegation_proxy_quarantine() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target_id UUID;
BEGIN
 target_id := CASE WHEN TG_OP='DELETE' THEN OLD.id ELSE NEW.id END;
 INSERT INTO delegation_proxy_quarantines(instance_id,tenant_id)
 SELECT id,tenant_id FROM task_instances WHERE id=target_id
 AND EXISTS(SELECT 1 FROM delegation_proxy_native_history history WHERE history.instance_id=target_id)
 ON CONFLICT DO NOTHING;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER delegation_proxy_quarantine_capture_insert AFTER INSERT ON task_instances
 FOR EACH ROW EXECUTE FUNCTION capture_delegation_proxy_quarantine();
CREATE TRIGGER delegation_proxy_quarantine_capture_delete BEFORE DELETE ON task_instances
 FOR EACH ROW EXECUTE FUNCTION capture_delegation_proxy_quarantine();
-- Quiesce pre-upgrade native claims while preserving context/tree/effect evidence.
UPDATE task_instances SET state=CASE WHEN state='running'
 OR EXISTS(SELECT 1 FROM delegation_proxy_native_history history WHERE history.instance_id=task_instances.id)
 THEN 'paused' ELSE 'waiting' END,
 claimed_at=NULL,next_fire_at=NULL,updated_at=clock_timestamp()
 WHERE state IN ('running','scheduled') AND EXISTS(SELECT 1 FROM delegation_proxy_instances relay WHERE relay.instance_id=task_instances.id);
