-- Capability-scoped tenant principals. Existing keys retain compatibility;
-- newly minted keys default to operator unless scopes are supplied explicitly.
ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS capabilities_json TEXT NOT NULL
    DEFAULT '["operator","worker","device","publisher","approver","auditor"]';
