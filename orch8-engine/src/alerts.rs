//! Built-in operational alerts.
//!
//! Every engine node runs [`run_alert_loop`]. Each tick evaluates all
//! enabled rules (API-managed rows plus `[alerts]` config rules) and turns
//! *transitions* into durable deliveries on the webhook outbox:
//!
//! * ok → firing (respecting `cooldown_secs` since the last firing),
//! * firing → resolved.
//!
//! While a rule stays firing nothing is re-sent (dedup). Evaluator state is
//! written with a version compare-and-swap before a transition is enqueued,
//! so exactly one node emits each transition. Delivery (retries, parking,
//! redelivery, attempt history) is the existing outbox machinery; alert rows
//! carry the provider-shaped body plus the destination's *credential
//! references*, which are resolved only at send time — secrets never land in
//! the outbox table.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;

use orch8_storage::StorageBackend;
use orch8_types::alert::{AlertCondition, AlertDestination, AlertRule, AlertRuleState};
use orch8_types::config::AlertsConfig;
use orch8_types::filter::InstanceFilter;
use orch8_types::ids::TenantId;
use orch8_types::instance::InstanceState;
use orch8_types::webhook_outbox::{WebhookOutboxEntry, WebhookOutboxStatus};

use crate::webhooks::WebhookEvent;

/// Key inside `WebhookEvent.data` marking an alert delivery.
pub(crate) const ALERT_MARKER: &str = "_orch8_alert";
const MAX_SAMPLES: usize = 512;

/// What the outbox row carries for an alert (no secrets).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AlertDelivery {
    pub tenant_id: String,
    pub destination: AlertDestination,
    /// Provider-shaped body. For `PagerDuty` the `routing_key` is injected at
    /// send time.
    pub body: Value,
}

impl AlertDelivery {
    pub(crate) fn from_event(event: &WebhookEvent) -> Option<Self> {
        serde_json::from_value(event.data.get(ALERT_MARKER)?.clone()).ok()
    }

    pub(crate) const fn signed(&self) -> bool {
        matches!(self.destination, AlertDestination::Webhook { .. })
    }
}

/// Stable id for a config-declared rule (so its evaluator state persists
/// across restarts).
fn config_rule_id(tenant: &str, name: &str) -> Uuid {
    let digest = Sha256::digest(format!("orch8-config-alert\0{tenant}\0{name}").as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Builder::from_custom_bytes(bytes).into_uuid()
}

fn config_rules(config: &AlertsConfig) -> Vec<AlertRule> {
    let epoch = DateTime::<Utc>::UNIX_EPOCH;
    config
        .rules
        .iter()
        .filter_map(|r| {
            let rule = AlertRule {
                id: config_rule_id(&r.tenant_id, &r.name),
                tenant_id: TenantId::unchecked(r.tenant_id.clone()),
                name: r.name.clone(),
                enabled: r.enabled,
                condition: r.condition.clone(),
                destination: r.destination.clone(),
                cooldown_secs: r.cooldown_secs,
                created_at: epoch,
                updated_at: epoch,
            };
            match rule.validate() {
                Ok(()) => Some(rule),
                Err(e) => {
                    warn!(rule = %r.name, error = %e, "ignoring invalid configured alert rule");
                    None
                }
            }
        })
        .collect()
}

/// Validate config-declared rules at startup (fail fast on typos).
pub fn validate_config(config: &AlertsConfig) -> Result<(), String> {
    for r in &config.rules {
        let rule = AlertRule {
            id: Uuid::nil(),
            tenant_id: TenantId::unchecked(r.tenant_id.clone()),
            name: r.name.clone(),
            enabled: r.enabled,
            condition: r.condition.clone(),
            destination: r.destination.clone(),
            cooldown_secs: r.cooldown_secs,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        rule.validate()
            .map_err(|e| format!("alerts.rules `{}`: {e}", r.name))?;
    }
    Ok(())
}

/// One measurement: `(firing, value, human summary)`.
#[allow(clippy::too_many_lines)]
async fn measure(
    storage: &dyn StorageBackend,
    rule: &AlertRule,
    state: &mut AlertRuleState,
    now: DateTime<Utc>,
) -> Result<(bool, f64, String), orch8_types::error::StorageError> {
    let tenant = Some(rule.tenant_id.clone());
    match &rule.condition {
        AlertCondition::DlqGrowth {
            threshold,
            growth,
            window_secs,
        } => {
            let count = storage
                .count_instances(&InstanceFilter {
                    tenant_id: tenant,
                    states: Some(vec![InstanceState::Failed]),
                    ..InstanceFilter::default()
                })
                .await?;
            #[allow(clippy::cast_precision_loss)]
            let value = count as f64;
            let horizon = now.timestamp() - i64::try_from(*window_secs).unwrap_or(i64::MAX);
            state.samples.retain(|(t, _)| *t >= horizon);
            let baseline = state.samples.first().map_or(value, |(_, v)| *v);
            state.samples.push((now.timestamp(), value));
            if state.samples.len() > MAX_SAMPLES {
                let excess = state.samples.len() - MAX_SAMPLES;
                state.samples.drain(..excess);
            }
            let delta = (value - baseline).max(0.0);
            #[allow(clippy::cast_precision_loss)]
            let over_threshold = threshold.is_some_and(|t| value >= t as f64);
            #[allow(clippy::cast_precision_loss)]
            let over_growth = growth.is_some_and(|g| delta >= g as f64);
            Ok((
                over_threshold || over_growth,
                value,
                format!(
                    "{count} failed instances in the DLQ (+{delta} in the last {window_secs}s)"
                ),
            ))
        }
        AlertCondition::CircuitBreakerOpened { handler } => {
            let open: Vec<_> = storage
                .list_open_circuit_breakers()
                .await?
                .into_iter()
                .filter(|b| b.tenant_id == rule.tenant_id)
                .filter(|b| b.state == orch8_types::circuit_breaker::BreakerState::Open)
                .filter(|b| handler.as_ref().is_none_or(|h| &b.handler == h))
                .collect();
            let names: Vec<_> = open.iter().map(|b| b.handler.as_str()).collect();
            #[allow(clippy::cast_precision_loss)]
            Ok((
                !open.is_empty(),
                open.len() as f64,
                if open.is_empty() {
                    "all circuit breakers closed".into()
                } else {
                    format!("circuit breaker open: {}", names.join(", "))
                },
            ))
        }
        AlertCondition::BudgetBreach { min_instances } => {
            let count = storage
                .count_instances(&InstanceFilter {
                    tenant_id: tenant,
                    states: Some(vec![InstanceState::Paused]),
                    metadata_filter: Some(json!({"paused_reason": "budget_exceeded"})),
                    ..InstanceFilter::default()
                })
                .await?;
            #[allow(clippy::cast_precision_loss)]
            Ok((
                count >= *min_instances,
                count as f64,
                format!("{count} instance(s) paused by budget enforcement"),
            ))
        }
        AlertCondition::WorkerPoolEmpty {
            handler,
            queue,
            seen_within_secs,
        } => {
            let live = storage
                .list_worker_registrations(Some(
                    i64::try_from(*seen_within_secs).unwrap_or(i64::MAX),
                ))
                .await?
                .into_iter()
                .filter(|w| &w.handler_name == handler)
                .filter(|w| queue.is_none() || w.queue_name == *queue)
                // Unscoped workers serve every tenant.
                .filter(|w| {
                    w.tenant_id
                        .as_deref()
                        .is_none_or(|t| t == rule.tenant_id.as_str())
                })
                .count();
            #[allow(clippy::cast_precision_loss)]
            Ok((
                live == 0,
                live as f64,
                if live == 0 {
                    format!(
                        "no live workers for handler `{handler}` in the last {seen_within_secs}s"
                    )
                } else {
                    format!("{live} live worker(s) for handler `{handler}`")
                },
            ))
        }
    }
}

/// Provider-shaped body for a transition.
fn render(rule: &AlertRule, firing: bool, value: f64, summary: &str, now: DateTime<Utc>) -> Value {
    let status = if firing { "firing" } else { "resolved" };
    match &rule.destination {
        AlertDestination::Slack { .. } => {
            let n = crate::handlers::notify::Notification {
                title: Some(format!(
                    "{} {}: {}",
                    if firing { "🔴" } else { "✅" },
                    if firing { "Alert" } else { "Resolved" },
                    rule.name
                )),
                text: summary.to_string(),
                fields: vec![
                    ("Tenant".into(), rule.tenant_id.as_str().to_string()),
                    ("Condition".into(), rule.condition.kind().to_string()),
                    ("Status".into(), status.to_string()),
                ],
                link: None,
                color: None,
            };
            crate::handlers::notify::render_slack(&n, None)
        }
        AlertDestination::Pagerduty { severity, .. } => {
            if firing {
                json!({
                    "event_action": "trigger",
                    "dedup_key": rule.dedup_key(),
                    "payload": {
                        "summary": format!("[orch8] {}: {summary}", rule.name),
                        "source": format!("orch8/{}", rule.tenant_id.as_str()),
                        "severity": severity,
                        "component": rule.condition.kind(),
                        "timestamp": now.to_rfc3339(),
                        "custom_details": {"tenant_id": rule.tenant_id.as_str(), "rule_id": rule.id, "value": value},
                    },
                })
            } else {
                json!({"event_action": "resolve", "dedup_key": rule.dedup_key()})
            }
        }
        AlertDestination::Webhook { .. } => json!({
            "event": format!("alert.{status}"),
            "rule": {"id": rule.id, "name": rule.name, "condition": rule.condition.kind()},
            "tenant_id": rule.tenant_id.as_str(),
            "status": status,
            "value": value,
            "summary": summary,
            "dedup_key": rule.dedup_key(),
            "timestamp": now.to_rfc3339(),
        }),
    }
}

/// Display/target URL stored on the outbox row: never a secret.
fn outbox_url(rule: &AlertRule, config: &AlertsConfig) -> String {
    match &rule.destination {
        AlertDestination::Slack { url_ref } => url_ref.clone(),
        AlertDestination::Pagerduty { .. } => config.pagerduty_events_url.clone(),
        AlertDestination::Webhook { url, .. } => url.clone(),
    }
}

fn delivery_entry(
    rule: &AlertRule,
    config: &AlertsConfig,
    firing: bool,
    value: f64,
    summary: &str,
    now: DateTime<Utc>,
) -> WebhookOutboxEntry {
    let event_type = if firing {
        "alert.firing"
    } else {
        "alert.resolved"
    };
    let delivery = AlertDelivery {
        tenant_id: rule.tenant_id.as_str().to_string(),
        destination: rule.destination.clone(),
        body: render(rule, firing, value, summary, now),
    };
    let event = WebhookEvent {
        event_type: event_type.into(),
        instance_id: None,
        timestamp: now.to_rfc3339(),
        data: json!({ ALERT_MARKER: delivery, "rule_id": rule.id }),
    };
    WebhookOutboxEntry {
        id: Uuid::now_v7(),
        url: outbox_url(rule, config),
        event_type: event_type.into(),
        instance_id: None,
        payload: serde_json::to_value(&event).unwrap_or(Value::Null),
        attempts: 0,
        last_error: None,
        created_at: now,
        delivery_id: Some(Uuid::now_v7()),
        status: WebhookOutboxStatus::Pending,
        next_attempt_at: None,
        claimed_at: None,
    }
}

/// Evaluate every rule once. Returns the number of transitions enqueued.
pub async fn evaluate_once(
    storage: &dyn StorageBackend,
    config: &AlertsConfig,
    now: DateTime<Utc>,
) -> Result<usize, orch8_types::error::StorageError> {
    let mut rules = storage.list_alert_rules(None, 1000).await?;
    rules.extend(config_rules(config));
    let mut transitions = 0;
    for rule in rules.into_iter().filter(|r| r.enabled) {
        let prior = storage
            .get_alert_rule_state(rule.id)
            .await?
            .unwrap_or_else(|| AlertRuleState::new(rule.id));
        let mut next = prior.clone();
        let (firing, value, summary) = match measure(storage, &rule, &mut next, now).await {
            Ok(m) => m,
            Err(e) => {
                warn!(rule_id = %rule.id, error = %e, "alert rule evaluation failed");
                continue;
            }
        };
        next.last_value = value;
        next.last_evaluated_at = Some(now);
        let cooled = prior.last_fired_at.is_none_or(|t| {
            (now - t).num_seconds() >= i64::try_from(rule.cooldown_secs).unwrap_or(i64::MAX)
        });
        let transition = match (prior.firing, firing) {
            (false, true) if cooled => {
                next.firing = true;
                next.last_fired_at = Some(now);
                Some(true)
            }
            (true, false) => {
                next.firing = false;
                next.last_resolved_at = Some(now);
                Some(false)
            }
            _ => None,
        };
        next.version = prior.version + 1;
        // CAS first: only the node that wins the version race emits.
        if !storage.cas_alert_rule_state(&next, prior.version).await? {
            debug!(rule_id = %rule.id, "alert state CAS lost to another node");
            continue;
        }
        if let Some(is_firing) = transition {
            let entry = delivery_entry(&rule, config, is_firing, value, &summary, now);
            if let Err(e) = storage.park_webhook(&entry).await {
                warn!(rule_id = %rule.id, error = %e, "failed to enqueue alert delivery");
            } else {
                transitions += 1;
                crate::metrics::inc(crate::metrics::ALERTS_EMITTED);
            }
        }
    }
    Ok(transitions)
}

/// Periodic evaluator; exits on `cancel`.
pub async fn run_alert_loop(
    storage: Arc<dyn StorageBackend>,
    config: AlertsConfig,
    cancel: CancellationToken,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(config.eval_interval_secs.max(5)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            _ = ticker.tick() => {
                if let Err(e) = evaluate_once(storage.as_ref(), &config, Utc::now()).await {
                    warn!(error = %e, "alert evaluation pass failed");
                }
            }
        }
    }
}

/// Resolve a `credentials://…` reference to a non-empty string in the
/// tenant's scope.
async fn resolve_ref(
    storage: &dyn StorageBackend,
    tenant: &str,
    reference: &str,
) -> Result<zeroize_string::Secret, String> {
    let mut v = Value::String(reference.to_string());
    crate::credentials::resolve_in_value(storage, tenant, &mut v)
        .await
        .map_err(|e| format!("credential resolution failed: {e}"))?;
    match v {
        Value::String(s) if !s.is_empty() => Ok(zeroize_string::Secret(s)),
        _ => {
            Err("credential must resolve to a non-empty string (use credentials://id/field)".into())
        }
    }
}

/// Tiny wrapper so resolved secrets are never `Debug`-printed.
mod zeroize_string {
    pub(crate) struct Secret(pub String);
    impl std::fmt::Debug for Secret {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("<redacted>")
        }
    }
}

/// Send one alert delivery. `Ok(status)` for any HTTP response; `Err` for
/// transport/config errors (never containing secrets or secret URLs).
pub(crate) async fn send_alert(
    storage: &dyn StorageBackend,
    target_url: &str,
    delivery: &AlertDelivery,
    timeout: Duration,
) -> Result<u16, String> {
    let (url, body, headers): (String, Vec<u8>, Vec<(&str, String)>) = match &delivery.destination {
        AlertDestination::Slack { url_ref } => {
            let url = resolve_ref(storage, &delivery.tenant_id, url_ref).await?;
            if !crate::handlers::builtin::is_url_safe(&url.0).await {
                return Err("blocked: Slack URL targets a private/internal network address".into());
            }
            let body = serde_json::to_vec(&delivery.body).map_err(|e| e.to_string())?;
            (url.0, body, Vec::new())
        }
        AlertDestination::Pagerduty {
            routing_key_ref, ..
        } => {
            let key = resolve_ref(storage, &delivery.tenant_id, routing_key_ref).await?;
            let mut body = delivery.body.clone();
            body["routing_key"] = Value::String(key.0);
            let bytes = serde_json::to_vec(&body).map_err(|e| e.to_string())?;
            (target_url.to_string(), bytes, Vec::new())
        }
        AlertDestination::Webhook { url, secret_ref } => {
            if !crate::handlers::builtin::is_url_safe(url).await {
                return Err(
                    "blocked: webhook URL targets a private/internal network address".into(),
                );
            }
            let secret = resolve_ref(storage, &delivery.tenant_id, secret_ref).await?;
            let body = serde_json::to_vec(&delivery.body).map_err(|e| e.to_string())?;
            let ts = Utc::now().timestamp();
            let sig = crate::webhooks::sign(&secret.0, ts, &body);
            (
                url.clone(),
                body,
                vec![
                    ("X-Orch8-Timestamp", ts.to_string()),
                    ("X-Orch8-Signature", format!("sha256={sig}")),
                ],
            )
        }
    };
    // PagerDuty's endpoint is operator-configured; tenant-supplied targets go
    // through the SSRF-hardened client.
    let client = if matches!(delivery.destination, AlertDestination::Pagerduty { .. }) {
        crate::webhooks::operator_client()
    } else {
        crate::handlers::llm::http_client()
    };
    let mut req = client
        .post(&url)
        .timeout(timeout)
        .header("Content-Type", "application/json")
        .body(body);
    for (k, v) in headers {
        req = req.header(k, v);
    }
    let resp = req.send().await.map_err(|e| e.without_url().to_string())?;
    Ok(resp.status().as_u16())
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_storage::{AdminStore, WorkerStore};
    use orch8_types::alert::AlertCondition;

    fn rule(tenant: &str, condition: AlertCondition, destination: AlertDestination) -> AlertRule {
        AlertRule {
            id: Uuid::now_v7(),
            tenant_id: TenantId::unchecked(tenant),
            name: "test rule".into(),
            enabled: true,
            condition,
            destination,
            cooldown_secs: 600,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn pd() -> AlertDestination {
        AlertDestination::Pagerduty {
            routing_key_ref: "credentials://pd/routing_key".into(),
            severity: "critical".into(),
        }
    }

    async fn pending_alerts(storage: &dyn StorageBackend) -> Vec<WebhookOutboxEntry> {
        storage
            .claim_due_webhook_outbox(Utc::now(), 100)
            .await
            .unwrap()
            .into_iter()
            .filter(|e| e.event_type.starts_with("alert."))
            .collect()
    }

    #[test]
    fn config_rule_ids_are_stable() {
        assert_eq!(config_rule_id("t", "a"), config_rule_id("t", "a"));
        assert_ne!(config_rule_id("t", "a"), config_rule_id("t", "b"));
    }

    #[test]
    fn pagerduty_trigger_and_resolve_share_dedup_key() {
        let r = rule("t", AlertCondition::BudgetBreach { min_instances: 1 }, pd());
        let fire = render(&r, true, 1.0, "x", Utc::now());
        let resolve = render(&r, false, 0.0, "x", Utc::now());
        assert_eq!(fire["event_action"], "trigger");
        assert_eq!(resolve["event_action"], "resolve");
        assert_eq!(fire["dedup_key"], resolve["dedup_key"]);
        assert_eq!(fire["payload"]["severity"], "critical");
        assert!(
            fire.get("routing_key").is_none(),
            "routing key injected only at send time"
        );
    }

    #[tokio::test]
    async fn worker_pool_empty_fires_once_then_resolves() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let r = rule(
            "acme",
            AlertCondition::WorkerPoolEmpty {
                handler: "charge".into(),
                queue: None,
                seen_within_secs: 60,
            },
            pd(),
        );
        storage.create_alert_rule(&r).await.unwrap();
        let cfg = AlertsConfig::default();

        // No workers: fires once.
        assert_eq!(evaluate_once(&storage, &cfg, Utc::now()).await.unwrap(), 1);
        // Still firing: deduplicated, nothing new.
        assert_eq!(evaluate_once(&storage, &cfg, Utc::now()).await.unwrap(), 0);
        let rows = pending_alerts(&storage).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_type, "alert.firing");
        assert_eq!(rows[0].url, cfg.pagerduty_events_url);
        assert!(
            !rows[0].payload.to_string().contains("\"routing_key\":"),
            "no secrets in outbox: {}",
            rows[0].payload
        );

        // A worker polls: resolves.
        storage
            .upsert_worker_registration(&orch8_types::worker::WorkerRegistration {
                worker_id: "w1".into(),
                handler_name: "charge".into(),
                queue_name: None,
                version: None,
                tenant_id: Some("acme".into()),
                last_seen_at: Utc::now(),
            })
            .await
            .unwrap();
        assert_eq!(evaluate_once(&storage, &cfg, Utc::now()).await.unwrap(), 1);
        let st = storage.get_alert_rule_state(r.id).await.unwrap().unwrap();
        assert!(!st.firing);
        assert!(st.last_resolved_at.is_some());
    }

    #[tokio::test]
    async fn cooldown_suppresses_refiring_and_config_rules_evaluate() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let mut cfg = AlertsConfig::default();
        cfg.rules.push(orch8_types::config::ConfiguredAlertRule {
            name: "pool".into(),
            tenant_id: "acme".into(),
            enabled: true,
            condition: AlertCondition::WorkerPoolEmpty {
                handler: "h".into(),
                queue: None,
                seen_within_secs: 60,
            },
            destination: AlertDestination::Slack {
                url_ref: "credentials://slack/url".into(),
            },
            cooldown_secs: 3600,
        });
        let t0 = Utc::now();
        assert_eq!(evaluate_once(&storage, &cfg, t0).await.unwrap(), 1, "fires");
        // Force a resolve by flipping state manually, then re-fire inside cooldown.
        let id = config_rule_id("acme", "pool");
        let mut st = storage.get_alert_rule_state(id).await.unwrap().unwrap();
        let v = st.version;
        st.firing = false;
        st.version = v + 1;
        assert!(storage.cas_alert_rule_state(&st, v).await.unwrap());
        assert_eq!(
            evaluate_once(&storage, &cfg, t0 + chrono::Duration::seconds(60))
                .await
                .unwrap(),
            0,
            "cooldown"
        );
        assert_eq!(
            evaluate_once(&storage, &cfg, t0 + chrono::Duration::seconds(3700))
                .await
                .unwrap(),
            1,
            "after cooldown"
        );
    }

    async fn mock_once() -> (String, tokio::sync::oneshot::Receiver<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 8192];
            loop {
                let n = sock.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                let t = String::from_utf8_lossy(&buf);
                if let Some(i) = t.find("\r\n\r\n") {
                    let len = t[..i]
                        .lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse::<usize>().ok())?
                        })
                        .unwrap_or(0);
                    if buf.len() >= i + 4 + len {
                        break;
                    }
                }
            }
            let _ = sock
                .write_all(
                    b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await;
            let _ = tx.send(String::from_utf8_lossy(&buf).to_string());
        });
        (format!("http://127.0.0.1:{}/v2/enqueue", addr.port()), rx)
    }

    async fn store_cred(storage: &SqliteStorage, id: &str, value: &str) {
        storage
            .create_credential(&orch8_types::credential::CredentialDef {
                id: id.into(),
                tenant_id: "acme".into(),
                name: id.into(),
                kind: orch8_types::credential::CredentialKind::ApiKey,
                value: orch8_types::config::SecretString::new(value.into()),
                expires_at: None,
                refresh_url: None,
                refresh_token: None,
                enabled: true,
                description: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pagerduty_delivery_injects_routing_key_at_send_time() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        store_cred(&storage, "pd", r#"{"routing_key":"R0UT1NGKEY"}"#).await;
        let (url, rx) = mock_once().await;
        let cfg = AlertsConfig {
            pagerduty_events_url: url.clone(),
            ..AlertsConfig::default()
        };
        let r = rule(
            "acme",
            AlertCondition::BudgetBreach { min_instances: 1 },
            pd(),
        );
        let entry = delivery_entry(&r, &cfg, true, 2.0, "2 paused", Utc::now());
        let event: WebhookEvent = serde_json::from_value(entry.payload.clone()).unwrap();
        let alert = AlertDelivery::from_event(&event).unwrap();
        let status = send_alert(&storage, &entry.url, &alert, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(status, 202);
        let raw = rx.await.unwrap();
        let body: Value = serde_json::from_str(&raw[raw.find("\r\n\r\n").unwrap() + 4..]).unwrap();
        assert_eq!(body["routing_key"], "R0UT1NGKEY");
        assert_eq!(body["event_action"], "trigger");
        assert_eq!(body["dedup_key"], r.dedup_key());
    }

    #[tokio::test]
    async fn webhook_delivery_is_signed_with_credential_secret() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        store_cred(&storage, "hook", "hook-secret").await;
        let (url, rx) = mock_once().await;
        crate::handlers::builtin::mark_url_safe_for_test(&url).await;
        let r = rule(
            "acme",
            AlertCondition::CircuitBreakerOpened { handler: None },
            AlertDestination::Webhook {
                url: url.clone(),
                secret_ref: "credentials://hook".into(),
            },
        );
        let entry = delivery_entry(
            &r,
            &AlertsConfig::default(),
            false,
            0.0,
            "closed",
            Utc::now(),
        );
        let event: WebhookEvent = serde_json::from_value(entry.payload.clone()).unwrap();
        let alert = AlertDelivery::from_event(&event).unwrap();
        assert!(alert.signed());
        send_alert(&storage, &entry.url, &alert, Duration::from_secs(5))
            .await
            .unwrap();
        let raw = rx.await.unwrap();
        let lower = raw.to_ascii_lowercase();
        let ts = lower
            .lines()
            .find_map(|l| l.strip_prefix("x-orch8-timestamp: "))
            .unwrap()
            .trim()
            .parse::<i64>()
            .unwrap();
        let body = &raw[raw.find("\r\n\r\n").unwrap() + 4..];
        let expected = crate::webhooks::sign("hook-secret", ts, body.as_bytes());
        assert!(lower.contains(&format!("x-orch8-signature: sha256={expected}")));
        let parsed: Value = serde_json::from_str(body).unwrap();
        assert_eq!(parsed["event"], "alert.resolved");
        assert!(!raw.contains("hook-secret"));
    }

    #[tokio::test]
    async fn dlq_growth_uses_rolling_window() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let r = rule(
            "acme",
            AlertCondition::DlqGrowth {
                threshold: None,
                growth: Some(2),
                window_secs: 300,
            },
            pd(),
        );
        let mut st = AlertRuleState::new(r.id);
        let now = Utc::now();
        let (f, v, _) = measure(&storage, &r, &mut st, now).await.unwrap();
        assert!(!f);
        assert!(v.abs() < f64::EPSILON);
        // Simulate an older sample with a lower count than "now".
        st.samples = vec![(now.timestamp() - 100, -3.0)];
        let (f, _, summary) = measure(&storage, &r, &mut st, now).await.unwrap();
        assert!(f, "growth of 3 within window fires: {summary}");
        // Samples older than the window are dropped.
        st.samples = vec![(now.timestamp() - 1000, -3.0)];
        let (f, _, _) = measure(&storage, &r, &mut st, now).await.unwrap();
        assert!(!f);
    }
}
