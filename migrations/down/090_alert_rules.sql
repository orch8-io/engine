-- Undo 093: drop alert rules and evaluator state.
DROP TABLE IF EXISTS alert_rule_state;
DROP INDEX IF EXISTS idx_alert_rules_tenant;
DROP TABLE IF EXISTS alert_rules;
