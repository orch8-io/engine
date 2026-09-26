//! Built-in operational alert rules and their delivery destinations.
//!
//! A rule pairs one [`AlertCondition`] (evaluated periodically by the
//! engine) with one [`AlertDestination`]. Transitions (ok → firing,
//! firing → resolved) are enqueued on the durable webhook outbox, so a
//! destination outage never loses an alert. Secrets (Slack webhook URLs,
//! `PagerDuty` routing keys, webhook signing secrets) are always credential
//! references resolved at delivery time — never stored in the rule.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ids::TenantId;

/// Minimum cooldown between two firings of the same rule.
pub const MIN_COOLDOWN_SECS: u64 = 60;
pub const DEFAULT_COOLDOWN_SECS: u64 = 900;

/// What the evaluator watches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AlertCondition {
    /// Failed (dead-lettered) instances in the tenant. Fires when the total
    /// reaches `threshold`, or when at least `growth` new failures land
    /// within `window_secs`. At least one of the two must be set.
    DlqGrowth {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        threshold: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        growth: Option<u64>,
        #[serde(default = "default_window_secs")]
        window_secs: u64,
    },
    /// Any circuit breaker (or the named handler's) for the tenant is open.
    CircuitBreakerOpened {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        handler: Option<String>,
    },
    /// Instances paused by budget enforcement (`paused_reason =
    /// budget_exceeded`) reach `min_instances` (default 1).
    BudgetBreach {
        #[serde(default = "default_one")]
        min_instances: u64,
    },
    /// No worker for `handler` (optionally on `queue`) has polled within
    /// `seen_within_secs`.
    WorkerPoolEmpty {
        handler: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        queue: Option<String>,
        #[serde(default = "default_seen_within")]
        seen_within_secs: u64,
    },
}

const fn default_window_secs() -> u64 {
    300
}
const fn default_one() -> u64 {
    1
}
const fn default_seen_within() -> u64 {
    120
}

impl AlertCondition {
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::DlqGrowth { .. } => "dlq_growth",
            Self::CircuitBreakerOpened { .. } => "circuit_breaker_opened",
            Self::BudgetBreach { .. } => "budget_breach",
            Self::WorkerPoolEmpty { .. } => "worker_pool_empty",
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::DlqGrowth {
                threshold,
                growth,
                window_secs,
            } => {
                if threshold.is_none() && growth.is_none() {
                    return Err("dlq_growth needs `threshold` and/or `growth`".into());
                }
                if threshold == &Some(0) || growth == &Some(0) {
                    return Err("dlq_growth thresholds must be > 0".into());
                }
                if !(60..=86_400).contains(window_secs) {
                    return Err("dlq_growth `window_secs` must be 60..=86400".into());
                }
            }
            Self::CircuitBreakerOpened { handler } => {
                if handler.as_ref().is_some_and(String::is_empty) {
                    return Err("circuit_breaker_opened `handler` must not be empty".into());
                }
            }
            Self::BudgetBreach { min_instances } => {
                if *min_instances == 0 {
                    return Err("budget_breach `min_instances` must be > 0".into());
                }
            }
            Self::WorkerPoolEmpty {
                handler,
                seen_within_secs,
                ..
            } => {
                if handler.is_empty() {
                    return Err("worker_pool_empty needs `handler`".into());
                }
                if !(10..=86_400).contains(seen_within_secs) {
                    return Err("worker_pool_empty `seen_within_secs` must be 10..=86400".into());
                }
            }
        }
        Ok(())
    }
}

/// Where a transition is delivered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AlertDestination {
    /// Slack incoming webhook; `url_ref` is `credentials://id[/field]`.
    Slack { url_ref: String },
    /// `PagerDuty` Events API v2. `routing_key_ref` is a credential reference.
    /// Firing sends `trigger`, recovery sends `resolve` with the same
    /// `dedup_key`.
    Pagerduty {
        routing_key_ref: String,
        #[serde(default = "default_severity")]
        severity: String,
    },
    /// Generic JSON webhook, HMAC-signed (`X-Orch8-Signature`) with the
    /// secret behind `secret_ref`.
    Webhook { url: String, secret_ref: String },
}

fn default_severity() -> String {
    "error".into()
}

impl AlertDestination {
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Slack { .. } => "slack",
            Self::Pagerduty { .. } => "pagerduty",
            Self::Webhook { .. } => "webhook",
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        let is_ref = |r: &str| r.starts_with("credentials://") && r.len() > "credentials://".len();
        match self {
            Self::Slack { url_ref } => {
                if !is_ref(url_ref) {
                    return Err("slack `url_ref` must be a credentials:// reference".into());
                }
            }
            Self::Pagerduty {
                routing_key_ref,
                severity,
            } => {
                if !is_ref(routing_key_ref) {
                    return Err(
                        "pagerduty `routing_key_ref` must be a credentials:// reference".into(),
                    );
                }
                if !matches!(severity.as_str(), "critical" | "error" | "warning" | "info") {
                    return Err(
                        "pagerduty `severity` must be critical, error, warning, or info".into(),
                    );
                }
            }
            Self::Webhook { url, secret_ref } => {
                // Full parsing + SSRF checks happen at delivery time.
                let ok = (url.starts_with("https://") || url.starts_with("http://"))
                    && url.len() <= 2048
                    && !url.chars().any(|c| c.is_whitespace() || c.is_control());
                if !ok {
                    return Err("webhook `url` must be an http(s) URL".into());
                }
                if !is_ref(secret_ref) {
                    return Err("webhook `secret_ref` must be a credentials:// reference".into());
                }
            }
        }
        Ok(())
    }
}

/// A tenant-scoped alert rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AlertRule {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub name: String,
    #[serde(default = "crate::serde_defaults::yes")]
    pub enabled: bool,
    pub condition: AlertCondition,
    pub destination: AlertDestination,
    #[serde(default = "default_cooldown")]
    pub cooldown_secs: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

const fn default_cooldown() -> u64 {
    DEFAULT_COOLDOWN_SECS
}

impl AlertRule {
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() || self.name.len() > 200 {
            return Err("`name` must be 1..=200 characters".into());
        }
        if self.cooldown_secs < MIN_COOLDOWN_SECS || self.cooldown_secs > 7 * 86_400 {
            return Err(format!(
                "`cooldown_secs` must be {MIN_COOLDOWN_SECS}..=604800"
            ));
        }
        self.condition.validate()?;
        self.destination.validate()
    }

    /// Stable `PagerDuty` / receiver dedup key for this rule.
    #[must_use]
    pub fn dedup_key(&self) -> String {
        format!("orch8-alert-{}", self.id)
    }
}

/// Evaluator bookkeeping for one rule, updated with a version CAS so that
/// exactly one engine node emits each transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct AlertRuleState {
    pub rule_id: Uuid,
    pub firing: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_fired_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_resolved_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_evaluated_at: Option<DateTime<Utc>>,
    /// Last observed metric value (DLQ count, open breakers, …).
    #[serde(default)]
    pub last_value: f64,
    /// Rolling samples `(unix_secs, value)` for rate conditions.
    #[serde(default)]
    pub samples: Vec<(i64, f64)>,
    /// Monotonic CAS version (0 = never stored).
    pub version: i64,
}

impl AlertRuleState {
    #[must_use]
    pub const fn new(rule_id: Uuid) -> Self {
        Self {
            rule_id,
            firing: false,
            last_fired_at: None,
            last_resolved_at: None,
            last_evaluated_at: None,
            last_value: 0.0,
            samples: Vec::new(),
            version: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rule(condition: AlertCondition, destination: AlertDestination) -> AlertRule {
        AlertRule {
            id: Uuid::now_v7(),
            tenant_id: TenantId::unchecked("t"),
            name: "r".into(),
            enabled: true,
            condition,
            destination,
            cooldown_secs: 300,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn tagged_json_shapes() {
        let c: AlertCondition =
            serde_json::from_value(json!({"kind": "dlq_growth", "growth": 5})).unwrap();
        assert_eq!(
            c,
            AlertCondition::DlqGrowth {
                threshold: None,
                growth: Some(5),
                window_secs: 300
            }
        );
        let d: AlertDestination = serde_json::from_value(
            json!({"type": "pagerduty", "routing_key_ref": "credentials://pd"}),
        )
        .unwrap();
        assert_eq!(d.kind(), "pagerduty");
    }

    #[test]
    fn validation_rejects_literal_secrets_and_empty_conditions() {
        let slack = AlertDestination::Slack {
            url_ref: "https://hooks.slack.com/x".into(),
        };
        assert!(slack.validate().is_err());
        let dlq = AlertCondition::DlqGrowth {
            threshold: None,
            growth: None,
            window_secs: 300,
        };
        assert!(dlq.validate().is_err());
        let ok = rule(
            AlertCondition::BudgetBreach { min_instances: 1 },
            AlertDestination::Webhook {
                url: "https://x.example/h".into(),
                secret_ref: "credentials://s".into(),
            },
        );
        assert!(ok.validate().is_ok());
        let mut bad = ok.clone();
        bad.cooldown_secs = 1;
        assert!(bad.validate().is_err());
    }
}
