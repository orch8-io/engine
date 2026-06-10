//! `ActivePieces` polling triggers.
//!
//! Triggers of type [`TriggerType::ActivepiecesPoll`] periodically ask the
//! `@orch8/activepieces-worker` Node sidecar to run a piece trigger's poll
//! (Stripe "payment failed", Typeform "new submission", ...). Each item the
//! sidecar returns creates one instance of the trigger's sequence, exactly
//! like `POST /triggers/{slug}/fire` does — the item is the instance's
//! initial `context.data`.
//!
//! # Trigger config shape (`TriggerDef::config`)
//!
//! ```json
//! {
//!   "piece": "stripe",
//!   "trigger": "new_failed_payment",
//!   "auth": "credentials://stripe-prod",
//!   "props": { "account": "acct_123" },
//!   "interval_secs": 60
//! }
//! ```
//!
//! Either `interval_secs` (default 60) or `cron` (standard 5-field or the
//! `cron` crate's 6/7-field shape, evaluated in UTC) selects the poll
//! schedule — not both. `auth` and `props` may contain `credentials://`
//! references; they are resolved tenant-scoped on every poll so rotated
//! credentials take effect without re-registering the trigger.
//!
//! # Sidecar protocol (`POST /poll`)
//!
//! Request: `{ piece, trigger, auth, props, state, slug }` where `state` is
//! the opaque cursor blob returned by the previous successful poll (`null`
//! on the first poll). Response on success:
//! `{ "ok": true, "items": [...], "state": { ... } }` — `items` are the new
//! events since the cursor, `state` is the next cursor. Failures use the
//! same envelope as `/execute`:
//! `{ "ok": false, "error": { "type", "message", "details" } }`.
//!
//! # Failure semantics
//!
//! A failing poll records `last_error` / increments `consecutive_failures`
//! on the trigger's [`TriggerPollState`] row and retries on the next
//! schedule — the listener never exits on poll errors. The cursor is only
//! advanced after every returned item produced an instance, so delivery is
//! at-least-once: a partial failure re-delivers the whole batch.

use std::env;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use orch8_storage::StorageBackend;
use orch8_types::trigger::{TriggerDef, TriggerPollState, TriggerType};

const DEFAULT_SIDECAR_POLL_URL: &str = "http://127.0.0.1:50052/poll";

/// Default poll cadence when the trigger config sets neither
/// `interval_secs` nor `cron`.
pub const DEFAULT_POLL_INTERVAL_SECS: u64 = 60;

/// Floor for both `interval_secs` and computed cron gaps — protects the
/// sidecar (and the polled `SaaS` API) from sub-second hot loops.
const MIN_POLL_DELAY: Duration = Duration::from_secs(1);

/// Shared HTTP client for sidecar poll calls. Mirrors the action handler's
/// client (`handlers::activepieces`): pooled connections, generous ceiling
/// so we don't race the sidecar's own per-piece timeout.
static POLL_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(75))
        .build()
        .unwrap_or_else(|e| {
            warn!(error = %e, "failed to build activepieces poll HTTP client, using default");
            reqwest::Client::new()
        })
});

/// Validated view of an `activepieces_poll` trigger's `config` JSON.
#[derive(Debug, Clone)]
pub struct ApPollConfig {
    /// Piece name (`stripe`, `typeform`, ...) — same charset rules as the
    /// sidecar's loader: lowercase alphanumerics and hyphens.
    pub piece: String,
    /// Trigger name on the piece (`new_failed_payment`, ...).
    pub trigger: String,
    /// Piece auth — passed verbatim to the sidecar after `credentials://`
    /// resolution.
    pub auth: Value,
    /// Trigger props — passed verbatim after `credentials://` resolution.
    pub props: Value,
    /// Fixed poll interval. Mutually exclusive with `cron`.
    pub interval_secs: Option<u64>,
    /// Cron schedule (UTC). Mutually exclusive with `interval_secs`.
    pub cron: Option<String>,
}

/// Parse and validate the `config` blob of an `activepieces_poll` trigger.
///
/// # Errors
/// Returns a human-readable message when `config` is not an object, when
/// `piece`/`trigger` are missing, malformed, or empty, when `interval_secs`
/// is zero/negative, when `cron` does not parse, or when both schedule
/// fields are set.
pub fn parse_config(config: &Value) -> Result<ApPollConfig, String> {
    let obj = config
        .as_object()
        .ok_or_else(|| "config must be a JSON object".to_string())?;

    let piece = obj
        .get("piece")
        .and_then(Value::as_str)
        .ok_or_else(|| "config.piece (string) is required".to_string())?;
    if piece.is_empty()
        || !piece
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        || piece.starts_with('-')
    {
        return Err(format!(
            "config.piece '{piece}' is invalid: expected lowercase alphanumerics and hyphens"
        ));
    }

    let trigger = obj
        .get("trigger")
        .and_then(Value::as_str)
        .ok_or_else(|| "config.trigger (string) is required".to_string())?;
    if trigger.is_empty() {
        return Err("config.trigger must be non-empty".to_string());
    }

    let interval_secs = match obj.get("interval_secs") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let n = v
                .as_u64()
                .filter(|n| *n >= 1)
                .ok_or_else(|| "config.interval_secs must be a positive integer".to_string())?;
            Some(n)
        }
    };

    let cron = match obj.get("cron") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let expr = v
                .as_str()
                .ok_or_else(|| "config.cron must be a string".to_string())?;
            crate::cron::validate_cron_expr(expr)
                .map_err(|e| format!("config.cron is invalid: {e}"))?;
            Some(expr.to_string())
        }
    };

    if interval_secs.is_some() && cron.is_some() {
        return Err("config must set either interval_secs or cron, not both".to_string());
    }

    Ok(ApPollConfig {
        piece: piece.to_string(),
        trigger: trigger.to_string(),
        auth: obj.get("auth").cloned().unwrap_or(Value::Null),
        props: obj.get("props").cloned().unwrap_or_else(|| json!({})),
        interval_secs,
        cron,
    })
}

/// Derive the sidecar poll URL from the configured execute URL. The
/// `ORCH8_ACTIVEPIECES_URL` env var traditionally points at `/execute`;
/// the poll endpoint lives next to it.
fn derive_poll_url(execute_url: &str) -> String {
    execute_url.strip_suffix("/execute").map_or_else(
        || format!("{}/poll", execute_url.trim_end_matches('/')),
        |base| format!("{base}/poll"),
    )
}

/// Sidecar poll URL resolved once at first use, mirroring
/// `handlers::activepieces::AP_URL`.
static POLL_URL: LazyLock<String> = LazyLock::new(|| {
    env::var("ORCH8_ACTIVEPIECES_URL").map_or_else(
        |_| DEFAULT_SIDECAR_POLL_URL.into(),
        |url| derive_poll_url(&url),
    )
});

/// Run a poll listener for one `activepieces_poll` trigger until cancelled.
/// Sidecar URL is taken from `ORCH8_ACTIVEPIECES_URL` (default
/// `http://127.0.0.1:50052/execute` → poll endpoint `/poll`).
pub async fn run_ap_poll_listener(
    storage: Arc<dyn StorageBackend>,
    trigger: TriggerDef,
    cancel: CancellationToken,
) {
    run_ap_poll_listener_with_url(storage, trigger, POLL_URL.clone(), cancel).await;
}

/// Like [`run_ap_poll_listener`] but with an explicit sidecar poll URL —
/// the testable entry point.
pub async fn run_ap_poll_listener_with_url(
    storage: Arc<dyn StorageBackend>,
    trigger: TriggerDef,
    url: String,
    cancel: CancellationToken,
) {
    debug_assert_eq!(trigger.trigger_type, TriggerType::ActivepiecesPoll);

    let config = match parse_config(&trigger.config) {
        Ok(c) => c,
        Err(e) => {
            // Invalid configs are rejected at the API layer; reaching this
            // point means a row was edited out-of-band. Record the problem
            // where GET /triggers/{slug} surfaces it, then park until the
            // definition changes (the trigger sync loop restarts us).
            error!(slug = %trigger.slug, error = %e, "activepieces poll trigger has invalid config");
            record_poll_failure(
                storage.as_ref(),
                &trigger.slug,
                &format!("invalid config: {e}"),
            )
            .await;
            cancel.cancelled().await;
            return;
        }
    };

    info!(
        slug = %trigger.slug,
        piece = %config.piece,
        trigger_name = %config.trigger,
        url = %url,
        "starting activepieces poll listener"
    );

    loop {
        match poll_once(storage.as_ref(), &trigger, &config, &url).await {
            Ok(0) => {}
            Ok(n) => {
                info!(
                    slug = %trigger.slug,
                    items = n,
                    "activepieces poll created instances"
                );
            }
            Err(e) => {
                warn!(slug = %trigger.slug, error = %e, "activepieces poll failed");
                crate::metrics::inc(crate::metrics::AP_POLL_ERRORS);
                record_poll_failure(storage.as_ref(), &trigger.slug, &e).await;
            }
        }

        let delay = next_poll_delay(&config);
        tokio::select! {
            () = cancel.cancelled() => {
                info!(slug = %trigger.slug, "activepieces poll listener shutting down");
                return;
            }
            () = tokio::time::sleep(delay) => {}
        }
    }
}

/// Time until the next poll: cron expression (UTC) if configured, otherwise
/// the fixed interval (default [`DEFAULT_POLL_INTERVAL_SECS`]). Clamped to
/// [`MIN_POLL_DELAY`] so a misconfigured schedule can't hot-loop.
fn next_poll_delay(config: &ApPollConfig) -> Duration {
    if let Some(expr) = &config.cron {
        if let Some(next) = crate::cron::next_fire_from_expr(expr) {
            let delta = next - chrono::Utc::now();
            let delay = delta.to_std().unwrap_or(MIN_POLL_DELAY);
            return delay.max(MIN_POLL_DELAY);
        }
        // Validated at registration, so this means "no future fire" —
        // fall back to the default interval rather than spinning.
        warn!(cron = %expr, "cron expression has no upcoming fire, using default interval");
    }
    Duration::from_secs(config.interval_secs.unwrap_or(DEFAULT_POLL_INTERVAL_SECS))
        .max(MIN_POLL_DELAY)
}

/// Sidecar `/poll` response envelope.
#[derive(Debug, Deserialize)]
struct PollResponse {
    ok: bool,
    #[serde(default)]
    items: Option<Vec<Value>>,
    #[serde(default)]
    state: Option<Value>,
    #[serde(default)]
    error: Option<PollError>,
}

#[derive(Debug, Deserialize)]
struct PollError {
    #[serde(rename = "type")]
    #[allow(dead_code)] // parsed for forward-compat with the /execute envelope
    kind: Option<String>,
    message: String,
}

/// Execute one poll round-trip: load the cursor, resolve credentials, call
/// the sidecar, create one instance per returned item, persist the new
/// cursor. Returns the number of instances created.
async fn poll_once(
    storage: &dyn StorageBackend,
    trigger: &TriggerDef,
    config: &ApPollConfig,
    url: &str,
) -> Result<usize, String> {
    let prev = storage
        .get_trigger_poll_state(&trigger.slug)
        .await
        .map_err(|e| format!("failed to load poll state: {e}"))?
        .unwrap_or_else(|| TriggerPollState::empty(trigger.slug.clone()));

    // Resolve credentials:// references fresh on every poll so credential
    // rotation takes effect without re-registering the trigger.
    let mut auth = config.auth.clone();
    let mut props = config.props.clone();
    crate::credentials::resolve_in_value(storage, trigger.tenant_id.as_str(), &mut auth)
        .await
        .map_err(|e| format!("auth credential resolution failed: {e}"))?;
    crate::credentials::resolve_in_value(storage, trigger.tenant_id.as_str(), &mut props)
        .await
        .map_err(|e| format!("props credential resolution failed: {e}"))?;

    let body = json!({
        "piece": config.piece,
        "trigger": config.trigger,
        "auth": auth,
        "props": props,
        "state": prev.state,
        "slug": trigger.slug,
    });

    debug!(
        slug = %trigger.slug,
        piece = %config.piece,
        trigger_name = %config.trigger,
        url = %url,
        "polling activepieces sidecar"
    );

    let response = POLL_CLIENT
        .post(url)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("sidecar unreachable at {url}: {e}"))?;

    let status = response.status().as_u16();
    let text = response
        .text()
        .await
        .map_err(|e| format!("failed to read sidecar response: {e}"))?;

    let parsed: PollResponse = serde_json::from_str(&text)
        .map_err(|_| format!("sidecar returned HTTP {status} with non-envelope body"))?;

    if !parsed.ok {
        let msg = parsed
            .error
            .map_or_else(|| format!("sidecar error (HTTP {status})"), |e| e.message);
        return Err(msg);
    }

    let items = parsed.items.unwrap_or_default();
    let event_meta = json!({
        "source": "activepieces_poll",
        "piece": config.piece,
        "trigger": config.trigger,
    });

    for item in &items {
        crate::triggers::create_trigger_instance(
            storage,
            trigger,
            item.clone(),
            event_meta.clone(),
            None,
        )
        .await
        .map_err(|e| format!("failed to create instance for polled item: {e}"))?;
    }
    crate::metrics::inc_by(crate::metrics::AP_POLL_ITEMS, items.len() as u64);

    let now = chrono::Utc::now();
    storage
        .upsert_trigger_poll_state(&TriggerPollState {
            slug: trigger.slug.clone(),
            // Keep the previous cursor when the sidecar omits `state`.
            state: parsed.state.unwrap_or(prev.state),
            last_poll_at: Some(now),
            last_error: None,
            consecutive_failures: 0,
            updated_at: now,
        })
        .await
        .map_err(|e| format!("failed to persist poll state: {e}"))?;

    Ok(items.len())
}

/// Record a failed poll on the trigger's state row: keep the cursor,
/// remember the error, bump the consecutive-failure counter. Best-effort —
/// a storage error here is logged, never propagated (the loop must go on).
async fn record_poll_failure(storage: &dyn StorageBackend, slug: &str, message: &str) {
    let prev = match storage.get_trigger_poll_state(slug).await {
        Ok(state) => state.unwrap_or_else(|| TriggerPollState::empty(slug)),
        Err(e) => {
            error!(slug, error = %e, "failed to load poll state while recording failure");
            TriggerPollState::empty(slug)
        }
    };
    let now = chrono::Utc::now();
    let next = TriggerPollState {
        slug: prev.slug,
        state: prev.state,
        last_poll_at: Some(now),
        last_error: Some(message.to_string()),
        consecutive_failures: prev.consecutive_failures.saturating_add(1),
        updated_at: now,
    };
    if let Err(e) = storage.upsert_trigger_poll_state(&next).await {
        error!(slug, error = %e, "failed to persist poll failure state");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use orch8_storage::sqlite::SqliteStorage;
    use orch8_storage::{AdminStore, InstanceStore, SequenceStore};
    use orch8_types::ids::{Namespace, SequenceId, TenantId};
    use orch8_types::instance::InstanceState;
    use orch8_types::sequence::{SequenceDefinition, SequenceStatus};

    use super::*;

    // ── config parsing ───────────────────────────────────────────────────

    #[test]
    fn parse_config_minimal() {
        let c = parse_config(&json!({"piece": "stripe", "trigger": "new_payment"})).unwrap();
        assert_eq!(c.piece, "stripe");
        assert_eq!(c.trigger, "new_payment");
        assert!(c.auth.is_null());
        assert_eq!(c.props, json!({}));
        assert!(c.interval_secs.is_none());
        assert!(c.cron.is_none());
    }

    #[test]
    fn parse_config_full() {
        let c = parse_config(&json!({
            "piece": "google-sheets",
            "trigger": "new_row",
            "auth": "credentials://sheets",
            "props": {"sheet": "s1"},
            "interval_secs": 30,
        }))
        .unwrap();
        assert_eq!(c.piece, "google-sheets");
        assert_eq!(c.interval_secs, Some(30));
        assert_eq!(c.auth, json!("credentials://sheets"));
        assert_eq!(c.props, json!({"sheet": "s1"}));
    }

    #[test]
    fn parse_config_rejects_bad_shapes() {
        assert!(parse_config(&Value::Null).is_err());
        assert!(parse_config(&json!({"trigger": "t"})).is_err());
        assert!(parse_config(&json!({"piece": "p"})).is_err());
        assert!(parse_config(&json!({"piece": "", "trigger": "t"})).is_err());
        assert!(parse_config(&json!({"piece": "Bad_Name", "trigger": "t"})).is_err());
        assert!(parse_config(&json!({"piece": "-bad", "trigger": "t"})).is_err());
        assert!(parse_config(&json!({"piece": "p", "trigger": ""})).is_err());
        assert!(parse_config(&json!({"piece": "p", "trigger": "t", "interval_secs": 0})).is_err());
        assert!(parse_config(&json!({"piece": "p", "trigger": "t", "interval_secs": -5})).is_err());
        assert!(parse_config(&json!({"piece": "p", "trigger": "t", "cron": "nonsense"})).is_err());
        assert!(parse_config(
            &json!({"piece": "p", "trigger": "t", "cron": "* * * * *", "interval_secs": 5})
        )
        .is_err());
    }

    #[test]
    fn parse_config_accepts_cron() {
        let c =
            parse_config(&json!({"piece": "p", "trigger": "t", "cron": "*/5 * * * *"})).unwrap();
        assert_eq!(c.cron.as_deref(), Some("*/5 * * * *"));
        assert!(c.interval_secs.is_none());
    }

    #[test]
    fn derive_poll_url_variants() {
        assert_eq!(
            derive_poll_url("http://127.0.0.1:50052/execute"),
            "http://127.0.0.1:50052/poll"
        );
        assert_eq!(
            derive_poll_url("http://ap.internal:9999"),
            "http://ap.internal:9999/poll"
        );
        assert_eq!(
            derive_poll_url("http://ap.internal:9999/"),
            "http://ap.internal:9999/poll"
        );
    }

    #[test]
    fn next_poll_delay_interval_and_default() {
        let mut c = parse_config(&json!({"piece": "p", "trigger": "t"})).unwrap();
        assert_eq!(
            next_poll_delay(&c),
            Duration::from_secs(DEFAULT_POLL_INTERVAL_SECS)
        );
        c.interval_secs = Some(7);
        assert_eq!(next_poll_delay(&c), Duration::from_secs(7));
    }

    #[test]
    fn next_poll_delay_cron_is_bounded() {
        let c =
            parse_config(&json!({"piece": "p", "trigger": "t", "cron": "* * * * * * *"})).unwrap();
        let d = next_poll_delay(&c);
        assert!(d >= MIN_POLL_DELAY);
        assert!(d <= Duration::from_secs(2), "every-second cron, got {d:?}");
    }

    // ── poll round-trips against a mock sidecar ──────────────────────────

    /// Read one HTTP/1.1 request (headers + Content-Length body) from `stream`.
    async fn read_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1024);
        let mut tmp = [0u8; 1024];
        let mut header_end = None;
        loop {
            let n = stream.read(&mut tmp).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if header_end.is_none() {
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    header_end = Some(pos + 4);
                }
            }
            if let Some(end) = header_end {
                let headers = std::str::from_utf8(&buf[..end]).unwrap_or("");
                let cl: usize = headers
                    .lines()
                    .find_map(|l| {
                        let l = l.to_ascii_lowercase();
                        l.strip_prefix("content-length:")
                            .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                    })
                    .unwrap_or(0);
                if buf.len() >= end + cl {
                    break;
                }
            }
        }
        buf
    }

    /// Spawn a mock sidecar that answers each request with the next response
    /// in `responses` (status, JSON body), recording received request bodies.
    /// Once responses run out, the last one repeats.
    async fn spawn_mock_sidecar(
        responses: Vec<(u16, String)>,
    ) -> (
        String,
        Arc<tokio::sync::Mutex<Vec<Value>>>,
        Arc<AtomicUsize>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}/poll");
        let bodies = Arc::new(tokio::sync::Mutex::new(Vec::<Value>::new()));
        let hits = Arc::new(AtomicUsize::new(0));
        let bodies_srv = bodies.clone();
        let hits_srv = hits.clone();

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let i = hits_srv.fetch_add(1, Ordering::SeqCst);
                let (status, body) = responses
                    .get(i.min(responses.len().saturating_sub(1)))
                    .cloned()
                    .unwrap_or((500, "{}".into()));
                let req = read_request(&mut stream).await;
                if let Some(pos) = req.windows(4).position(|w| w == b"\r\n\r\n") {
                    if let Ok(v) = serde_json::from_slice::<Value>(&req[pos + 4..]) {
                        bodies_srv.lock().await.push(v);
                    }
                }
                let reason = if status == 200 { "OK" } else { "Error" };
                let resp = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len(),
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });

        (url, bodies, hits)
    }

    async fn seed(storage: &SqliteStorage, seq_name: &str) -> SequenceId {
        let id = SequenceId::new();
        storage
            .create_sequence(&SequenceDefinition {
                id,
                tenant_id: TenantId::unchecked("t1"),
                namespace: Namespace::new("default"),
                name: seq_name.into(),
                version: 1,
                deprecated: false,
                status: SequenceStatus::default(),
                blocks: vec![],
                interceptors: None,
                created_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        id
    }

    async fn list_t1_instances(
        storage: &SqliteStorage,
    ) -> Vec<orch8_types::instance::TaskInstance> {
        storage
            .list_instances(
                &orch8_types::filter::InstanceFilter {
                    tenant_id: Some(TenantId::unchecked("t1")),
                    ..Default::default()
                },
                &orch8_types::filter::Pagination::default(),
            )
            .await
            .unwrap()
    }

    fn mk_poll_trigger(slug: &str, seq_name: &str, config: Value) -> TriggerDef {
        let now = chrono::Utc::now();
        TriggerDef {
            slug: slug.into(),
            sequence_name: seq_name.into(),
            version: None,
            tenant_id: TenantId::unchecked("t1"),
            namespace: "default".into(),
            enabled: true,
            secret: None,
            trigger_type: TriggerType::ActivepiecesPoll,
            config,
            created_at: now,
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn poll_once_creates_one_instance_per_item_and_persists_state() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let seq_id = seed(&storage, "on-payment").await;
        let trigger = mk_poll_trigger(
            "stripe-poll",
            "on-payment",
            json!({"piece": "stripe", "trigger": "failed_payment"}),
        );
        let config = parse_config(&trigger.config).unwrap();

        let (url, bodies, _) = spawn_mock_sidecar(vec![(
            200,
            json!({
                "ok": true,
                "items": [{"id": "pi_1", "amount": 100}, {"id": "pi_2", "amount": 250}],
                "state": {"lastPoll": 1234},
            })
            .to_string(),
        )])
        .await;

        let n = poll_once(&storage, &trigger, &config, &url).await.unwrap();
        assert_eq!(n, 2);

        // Two instances, each carrying one item as context.data.
        let instances = list_t1_instances(&storage).await;
        assert_eq!(instances.len(), 2);
        let mut ids: Vec<String> = instances
            .iter()
            .map(|i| i.context.data["id"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["pi_1", "pi_2"]);
        for inst in &instances {
            assert_eq!(inst.sequence_id, seq_id);
            assert_eq!(inst.state, InstanceState::Scheduled);
            assert_eq!(inst.metadata["_trigger"], "stripe-poll");
            assert_eq!(inst.metadata["_trigger_type"], "activepieces_poll");
            assert_eq!(inst.metadata["_trigger_event"]["piece"], "stripe");
        }

        // First poll sent a null cursor; new cursor persisted afterwards.
        let sent = bodies.lock().await;
        assert_eq!(sent[0]["piece"], "stripe");
        assert_eq!(sent[0]["trigger"], "failed_payment");
        assert!(sent[0]["state"].is_null());

        let state = storage
            .get_trigger_poll_state("stripe-poll")
            .await
            .unwrap()
            .expect("state row persisted");
        assert_eq!(state.state, json!({"lastPoll": 1234}));
        assert!(state.last_error.is_none());
        assert_eq!(state.consecutive_failures, 0);
        assert!(state.last_poll_at.is_some());
    }

    #[tokio::test]
    async fn poll_once_sends_persisted_cursor_on_next_poll() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        seed(&storage, "seq").await;
        let trigger = mk_poll_trigger("p", "seq", json!({"piece": "x", "trigger": "t"}));
        let config = parse_config(&trigger.config).unwrap();

        let (url, bodies, _) = spawn_mock_sidecar(vec![
            (
                200,
                json!({"ok": true, "items": [], "state": {"cursor": "abc"}}).to_string(),
            ),
            (
                200,
                json!({"ok": true, "items": [], "state": {"cursor": "def"}}).to_string(),
            ),
        ])
        .await;

        poll_once(&storage, &trigger, &config, &url).await.unwrap();
        poll_once(&storage, &trigger, &config, &url).await.unwrap();

        let sent = bodies.lock().await;
        assert!(sent[0]["state"].is_null(), "first poll has no cursor");
        assert_eq!(
            sent[1]["state"],
            json!({"cursor": "abc"}),
            "second poll must send the cursor persisted by the first"
        );
        let state = storage.get_trigger_poll_state("p").await.unwrap().unwrap();
        assert_eq!(state.state, json!({"cursor": "def"}));
    }

    #[tokio::test]
    async fn poll_error_records_last_error_and_keeps_cursor() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        seed(&storage, "seq").await;
        let trigger = mk_poll_trigger("p", "seq", json!({"piece": "x", "trigger": "t"}));
        let config = parse_config(&trigger.config).unwrap();

        let (url, _, _) = spawn_mock_sidecar(vec![
            (
                200,
                json!({"ok": true, "items": [], "state": {"cursor": 1}}).to_string(),
            ),
            (
                502,
                json!({"ok": false, "error": {"type": "retryable", "message": "stripe 503"}})
                    .to_string(),
            ),
        ])
        .await;

        poll_once(&storage, &trigger, &config, &url).await.unwrap();
        let err = poll_once(&storage, &trigger, &config, &url)
            .await
            .unwrap_err();
        assert!(err.contains("stripe 503"), "got: {err}");
        record_poll_failure(&storage, &trigger.slug, &err).await;

        let state = storage.get_trigger_poll_state("p").await.unwrap().unwrap();
        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(state.last_error.as_deref(), Some("stripe 503"));
        assert_eq!(
            state.state,
            json!({"cursor": 1}),
            "cursor must survive failures"
        );

        // Another failure bumps the counter.
        record_poll_failure(&storage, &trigger.slug, "boom").await;
        let state = storage.get_trigger_poll_state("p").await.unwrap().unwrap();
        assert_eq!(state.consecutive_failures, 2);
        assert_eq!(state.last_error.as_deref(), Some("boom"));
    }

    #[tokio::test]
    async fn poll_success_resets_failure_counter() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        seed(&storage, "seq").await;
        let trigger = mk_poll_trigger("p", "seq", json!({"piece": "x", "trigger": "t"}));
        let config = parse_config(&trigger.config).unwrap();

        record_poll_failure(&storage, "p", "first failure").await;
        record_poll_failure(&storage, "p", "second failure").await;

        let (url, _, _) = spawn_mock_sidecar(vec![(
            200,
            json!({"ok": true, "items": [], "state": null}).to_string(),
        )])
        .await;
        poll_once(&storage, &trigger, &config, &url).await.unwrap();

        let state = storage.get_trigger_poll_state("p").await.unwrap().unwrap();
        assert_eq!(state.consecutive_failures, 0);
        assert!(state.last_error.is_none());
    }

    #[tokio::test]
    async fn poll_once_resolves_credentials_in_auth() {
        use orch8_types::credential::{CredentialDef, CredentialKind};

        let storage = SqliteStorage::in_memory().await.unwrap();
        seed(&storage, "seq").await;
        let now = chrono::Utc::now();
        orch8_storage::AdminStore::create_credential(
            &storage,
            &CredentialDef {
                id: "stripe-key".into(),
                tenant_id: "t1".into(),
                name: "stripe-key".into(),
                kind: CredentialKind::ApiKey,
                value: orch8_types::config::SecretString::new(
                    r#"{"access_token": "sk_test_123"}"#.into(),
                ),
                expires_at: None,
                refresh_url: None,
                refresh_token: None,
                enabled: true,
                description: None,
                created_at: now,
                updated_at: now,
            },
        )
        .await
        .unwrap();

        let trigger = mk_poll_trigger(
            "p",
            "seq",
            json!({"piece": "stripe", "trigger": "t", "auth": "credentials://stripe-key"}),
        );
        let config = parse_config(&trigger.config).unwrap();

        let (url, bodies, _) = spawn_mock_sidecar(vec![(
            200,
            json!({"ok": true, "items": [], "state": null}).to_string(),
        )])
        .await;
        poll_once(&storage, &trigger, &config, &url).await.unwrap();

        let sent = bodies.lock().await;
        assert_eq!(
            sent[0]["auth"],
            json!({"access_token": "sk_test_123"}),
            "credentials:// reference must be resolved before hitting the sidecar"
        );
    }

    #[tokio::test]
    async fn listener_loop_polls_and_survives_errors_until_cancelled() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        seed(&storage, "seq").await;
        let trigger = mk_poll_trigger(
            "loop-trigger",
            "seq",
            json!({"piece": "x", "trigger": "t", "interval_secs": 1}),
        );

        // First response errors, second succeeds with one item — the loop
        // must keep going after the failure.
        let (url, _, hits) = spawn_mock_sidecar(vec![
            (
                500,
                json!({"ok": false, "error": {"message": "kaput"}}).to_string(),
            ),
            (
                200,
                json!({"ok": true, "items": [{"n": 1}], "state": {"c": 2}}).to_string(),
            ),
        ])
        .await;

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(run_ap_poll_listener_with_url(
            Arc::clone(&storage) as Arc<dyn StorageBackend>,
            trigger,
            url.clone(),
            cancel.clone(),
        ));

        // Wait until at least two polls happened (error + success).
        for _ in 0..100 {
            if hits.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        cancel.cancel();
        handle.await.unwrap();

        assert!(
            hits.load(Ordering::SeqCst) >= 2,
            "loop must poll repeatedly"
        );
        let instances = list_t1_instances(&storage).await;
        assert_eq!(
            instances.len(),
            1,
            "second (successful) poll created the instance"
        );
        let state = storage
            .get_trigger_poll_state("loop-trigger")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state.state, json!({"c": 2}));
        assert_eq!(
            state.consecutive_failures, 0,
            "success after failure resets counter"
        );
    }

    #[tokio::test]
    async fn listener_with_invalid_config_records_error_and_parks() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let trigger = mk_poll_trigger("bad-config", "seq", json!({"piece": "x"}));

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(run_ap_poll_listener_with_url(
            Arc::clone(&storage) as Arc<dyn StorageBackend>,
            trigger,
            "http://127.0.0.1:9/poll".to_string(),
            cancel.clone(),
        ));

        // Wait for the failure record to land.
        let mut state = None;
        for _ in 0..100 {
            state = storage.get_trigger_poll_state("bad-config").await.unwrap();
            if state.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let state = state.expect("invalid config must be recorded as poll failure");
        assert!(state
            .last_error
            .as_deref()
            .unwrap()
            .contains("invalid config"));

        cancel.cancel();
        handle.await.unwrap();
    }
}
