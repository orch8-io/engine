CREATE TABLE IF NOT EXISTS delegation_proxy_quarantines (
 instance_id TEXT PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 first_observed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_task_instances_delegation_proxy ON task_instances(id) WHERE json_type(metadata,'$.orch8_delegation_proxy') IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_delegation_publications_proxy_instances ON delegation_publications(instance_id) WHERE NOT COALESCE(
   json_type(record,'$.version')='integer' AND json_extract(record,'$.version')=1
   AND length(replace(lower(json_extract(record,'$.task_owner.continuity_id')),'-',''))=32
   AND replace(lower(json_extract(record,'$.task_owner.continuity_id')),'-','') NOT GLOB '*[^0-9a-f]*'
   AND replace(lower(json_extract(record,'$.task_owner.continuity_id')),'-','')=replace(lower(json_extract(record,'$.delegation.parent_continuity_id')),'-',''),0);
CREATE VIEW IF NOT EXISTS delegation_proxy_instances AS
 SELECT id AS instance_id FROM task_instances INDEXED BY idx_task_instances_delegation_proxy WHERE json_type(metadata,'$.orch8_delegation_proxy') IS NOT NULL
 UNION ALL
 SELECT instance_id FROM delegation_publications WHERE NOT COALESCE(
   json_type(record,'$.version')='integer' AND json_extract(record,'$.version')=1
   AND length(replace(lower(json_extract(record,'$.task_owner.continuity_id')),'-',''))=32
   AND replace(lower(json_extract(record,'$.task_owner.continuity_id')),'-','') NOT GLOB '*[^0-9a-f]*'
   AND replace(lower(json_extract(record,'$.task_owner.continuity_id')),'-','')=replace(lower(json_extract(record,'$.delegation.parent_continuity_id')),'-',''),0)
 UNION ALL SELECT instance_id FROM delegation_proxy_quarantines;
CREATE VIEW IF NOT EXISTS delegation_proxy_native_history AS
 SELECT instance_id FROM delegation_proxy_quarantines
 UNION ALL
 SELECT id AS instance_id FROM task_instances WHERE CASE
 WHEN EXISTS(SELECT 1 FROM delegation_proxy_instances relay WHERE relay.instance_id=task_instances.id)
 THEN state='running' OR (json_type(context,'$.runtime.run_id') IS NOT NULL AND json_type(context,'$.runtime.run_id')!='null')
  OR (json_type(context,'$.runtime.started_at') IS NOT NULL AND json_type(context,'$.runtime.started_at')!='null')
  OR (json_type(context,'$.runtime.current_step') IS NOT NULL AND json_type(context,'$.runtime.current_step')!='null')
  OR (json_type(context,'$.runtime.current_step_started_at') IS NOT NULL AND json_type(context,'$.runtime.current_step_started_at')!='null')
  OR EXISTS(SELECT 1 FROM execution_tree n WHERE n.instance_id=task_instances.id)
 ELSE 0 END;
CREATE TRIGGER IF NOT EXISTS delegation_proxy_instance_insert BEFORE INSERT ON task_instances
 WHEN NEW.state='running' AND (json_type(NEW.metadata,'$.orch8_delegation_proxy') IS NOT NULL
  OR EXISTS(SELECT 1 FROM delegation_proxy_instances WHERE instance_id=NEW.id))
 BEGIN SELECT RAISE(ABORT,'delegation relay cannot enter native execution'); END;
CREATE TRIGGER IF NOT EXISTS delegation_proxy_instance_update BEFORE UPDATE ON task_instances
 WHEN NEW.state='running' AND EXISTS(SELECT 1 FROM delegation_proxy_instances WHERE instance_id=NEW.id)
 BEGIN SELECT RAISE(ABORT,'delegation relay cannot enter native execution'); END;
CREATE TRIGGER IF NOT EXISTS delegation_proxy_metadata_running BEFORE UPDATE OF metadata ON task_instances
 WHEN NEW.state='running' AND json_type(NEW.metadata,'$.orch8_delegation_proxy') IS NOT NULL
 BEGIN SELECT RAISE(ABORT,'delegation relay cannot enter native execution'); END;
CREATE TRIGGER IF NOT EXISTS delegation_proxy_identity_update BEFORE UPDATE OF metadata ON task_instances
 WHEN json_type(OLD.metadata,'$.orch8_delegation_proxy') IS NOT NULL AND (
  EXISTS(SELECT fullkey,type,atom FROM json_tree(OLD.metadata,'$.orch8_delegation_proxy') EXCEPT SELECT fullkey,type,atom FROM json_tree(NEW.metadata,'$.orch8_delegation_proxy'))
  OR EXISTS(SELECT fullkey,type,atom FROM json_tree(NEW.metadata,'$.orch8_delegation_proxy') EXCEPT SELECT fullkey,type,atom FROM json_tree(OLD.metadata,'$.orch8_delegation_proxy')))
 BEGIN SELECT RAISE(ABORT,'delegation relay identity is immutable'); END;
CREATE TRIGGER IF NOT EXISTS delegation_proxy_identity_add BEFORE UPDATE OF metadata ON task_instances
 WHEN json_type(OLD.metadata,'$.orch8_delegation_proxy') IS NULL
  AND json_type(NEW.metadata,'$.orch8_delegation_proxy') IS NOT NULL
  AND NOT EXISTS(SELECT 1 FROM delegation_proxy_instances WHERE instance_id=OLD.id)
 BEGIN SELECT RAISE(ABORT,'native instance cannot become a delegation relay'); END;
CREATE TRIGGER IF NOT EXISTS delegation_proxy_runtime_update BEFORE UPDATE OF context ON task_instances
 WHEN EXISTS(SELECT 1 FROM delegation_proxy_instances WHERE instance_id=OLD.id) AND (
   json_extract(OLD.context,'$.runtime.run_id') IS NOT json_extract(NEW.context,'$.runtime.run_id')
   OR json_extract(OLD.context,'$.runtime.started_at') IS NOT json_extract(NEW.context,'$.runtime.started_at')
   OR json_extract(OLD.context,'$.runtime.current_step') IS NOT json_extract(NEW.context,'$.runtime.current_step')
   OR json_extract(OLD.context,'$.runtime.current_step_started_at') IS NOT json_extract(NEW.context,'$.runtime.current_step_started_at'))
 BEGIN SELECT RAISE(ABORT,'delegation relay native runtime history is immutable'); END;

INSERT OR IGNORE INTO delegation_proxy_quarantines(instance_id,tenant_id)
 SELECT id,tenant_id FROM task_instances WHERE EXISTS(SELECT 1 FROM delegation_proxy_native_history history WHERE history.instance_id=task_instances.id);
CREATE TRIGGER IF NOT EXISTS delegation_proxy_quarantine_update BEFORE UPDATE ON delegation_proxy_quarantines
 BEGIN SELECT RAISE(ABORT,'delegation relay quarantine is immutable'); END;
CREATE TRIGGER IF NOT EXISTS delegation_proxy_quarantine_delete BEFORE DELETE ON delegation_proxy_quarantines
 BEGIN SELECT RAISE(ABORT,'delegation relay quarantine is immutable'); END;
CREATE TRIGGER IF NOT EXISTS delegation_proxy_quarantine_capture_insert AFTER INSERT ON task_instances
 WHEN EXISTS(SELECT 1 FROM delegation_proxy_native_history history WHERE history.instance_id=NEW.id)
 BEGIN INSERT OR IGNORE INTO delegation_proxy_quarantines(instance_id,tenant_id) VALUES(NEW.id,NEW.tenant_id); END;
CREATE TRIGGER IF NOT EXISTS delegation_proxy_quarantine_capture_delete BEFORE DELETE ON task_instances
 WHEN EXISTS(SELECT 1 FROM delegation_proxy_native_history history WHERE history.instance_id=OLD.id)
 BEGIN INSERT OR IGNORE INTO delegation_proxy_quarantines(instance_id,tenant_id) VALUES(OLD.id,OLD.tenant_id); END;
