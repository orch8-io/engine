-- Undo 092: drop approval action tokens.
DROP INDEX IF EXISTS idx_approval_action_tokens_expiry;
DROP INDEX IF EXISTS idx_approval_action_tokens_gate;
DROP TABLE IF EXISTS approval_action_tokens;
