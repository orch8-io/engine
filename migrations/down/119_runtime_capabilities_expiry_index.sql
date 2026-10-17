-- Revert the runtime-capability expiry sweep index.
DROP INDEX IF EXISTS idx_runtime_capabilities_expiry;
