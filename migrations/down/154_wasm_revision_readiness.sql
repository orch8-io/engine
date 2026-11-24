-- Rollback of 154: Remove persisted WASM revision readiness records.
DROP TABLE IF EXISTS wasm_revision_readiness;
