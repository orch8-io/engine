-- Rollback of 156: Remove the WASM artifact retirement table and reference guards.
DROP TRIGGER IF EXISTS guard_injected_wasm_artifacts ON task_instances;
DROP TRIGGER IF EXISTS guard_plugin_wasm_artifacts ON plugins;
DROP TRIGGER IF EXISTS guard_sequence_wasm_artifacts ON sequences;
DROP FUNCTION IF EXISTS orch8_guard_wasm_artifact_references();
DROP TABLE IF EXISTS wasm_artifact_retirements;
