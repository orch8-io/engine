CREATE TABLE wasm_artifact_retirements (
    tenant_id TEXT NOT NULL,
    digest TEXT NOT NULL CHECK (length(digest) = 64),
    unreferenced_at TIMESTAMPTZ NOT NULL,
    retired BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (tenant_id, digest)
);

CREATE FUNCTION orch8_guard_wasm_artifact_references() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    roots jsonb;
BEGIN
    IF TG_TABLE_NAME = 'sequences' THEN
        roots := NEW.definition;
    ELSIF TG_TABLE_NAME = 'plugins' THEN
        roots := jsonb_build_array(NEW.source);
    ELSE
        roots := NEW.metadata->'_injected_blocks';
    END IF;
    IF EXISTS (
        SELECT 1 FROM wasm_artifact_retirements r
        WHERE r.tenant_id = NEW.tenant_id AND r.retired AND jsonb_path_exists(
            roots, '$.** ? (@ == $source || @ == $handler)',
            jsonb_build_object('source', 'wasm-artifact:sha256:' || r.digest,
                               'handler', 'wasm://wasm-artifact:sha256:' || r.digest))
    ) THEN
        RAISE EXCEPTION 'WASM artifact was retired; upload it before referencing it'
            USING ERRCODE = '23514';
    END IF;
    DELETE FROM wasm_artifact_retirements r
    WHERE r.tenant_id = NEW.tenant_id AND NOT r.retired AND jsonb_path_exists(
        roots, '$.** ? (@ == $source || @ == $handler)',
        jsonb_build_object('source', 'wasm-artifact:sha256:' || r.digest,
                           'handler', 'wasm://wasm-artifact:sha256:' || r.digest));
    RETURN NEW;
END;
$$;

CREATE TRIGGER guard_sequence_wasm_artifacts
BEFORE INSERT OR UPDATE OF definition, tenant_id ON sequences
FOR EACH ROW EXECUTE FUNCTION orch8_guard_wasm_artifact_references();

CREATE TRIGGER guard_plugin_wasm_artifacts
BEFORE INSERT OR UPDATE OF source, tenant_id ON plugins
FOR EACH ROW EXECUTE FUNCTION orch8_guard_wasm_artifact_references();

CREATE TRIGGER guard_injected_wasm_artifacts
BEFORE INSERT OR UPDATE OF metadata, tenant_id ON task_instances
FOR EACH ROW EXECUTE FUNCTION orch8_guard_wasm_artifact_references();
