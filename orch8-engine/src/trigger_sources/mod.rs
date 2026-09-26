//! Message-source triggers: Kafka, AWS SQS, GCP Pub/Sub, Redis Streams and
//! Postgres row changes.
//!
//! Each source lives in its own module. The config parsing/validation and
//! the message → instance mapping are plain functions compiled into every
//! build (so the API can validate configs and the mapping is unit-testable);
//! the network client and listener loop of each source sit behind its own
//! cargo feature (`kafka`, `sqs`, `pubsub`, `redis-streams`,
//! `postgres-rows`) so the default engine build stays lean.
//!
//! # Delivery contract (all sources)
//!
//! * **Ack after durable create.** A message is acknowledged (Kafka/Postgres:
//!   cursor persisted; SQS: `DeleteMessage`; Pub/Sub: `acknowledge`; Redis:
//!   `XACK`) only after the instance row is committed.
//! * **Idempotent by message id.** Every instance is created with a
//!   deterministic, tenant-scoped idempotency key derived from the trigger
//!   slug and the source's message id, so a redelivery (crash between create
//!   and ack, visibility timeout, consumer rebalance, a second engine node)
//!   is a no-op that is acked, never a duplicate run.
//! * **No ack on failure.** If the create fails the message is left
//!   unacknowledged and redelivered by the broker (or re-read from the
//!   persisted cursor) after a backoff.

use std::sync::LazyLock;
use std::time::Duration;

use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use orch8_storage::StorageBackend;
use orch8_types::ids::InstanceId;
use orch8_types::trigger::{TriggerDef, TriggerPollState, TriggerType};

use crate::error::EngineError;

pub mod kafka;
pub mod pg_rows;
pub mod pubsub;
pub mod redis_streams;
pub mod sqs;

/// Idempotency keys longer than this are hashed.
const MAX_RAW_KEY_LEN: usize = 200;

/// Identity of this process when holding per-trigger consumer leases.
#[cfg_attr(
    not(any(
        feature = "kafka",
        feature = "postgres-rows",
        feature = "redis-streams"
    )),
    allow(dead_code)
)]
pub(crate) static LEASE_OWNER: LazyLock<String> =
    LazyLock::new(|| format!("orch8-{}", uuid::Uuid::now_v7()));

/// Outcome of delivering one message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivered {
    /// A new instance was created.
    Created(InstanceId),
    /// The message was already delivered earlier (idempotency hit).
    Duplicate,
}

/// Decode a message body: JSON when it parses, otherwise a UTF-8 (lossy)
/// string — the same rule the NATS trigger applies.
#[must_use]
pub fn decode_payload(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(bytes).into_owned()))
}

/// Deterministic, tenant-scoped idempotency key for a message.
#[must_use]
pub fn idempotency_key(slug: &str, source_id: &str) -> String {
    let raw = format!("trigger:{slug}:{source_id}");
    if raw.len() <= MAX_RAW_KEY_LEN {
        return raw;
    }
    let digest = Sha256::digest(raw.as_bytes());
    let mut hex = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(hex, "{b:02x}");
    }
    format!("trigger:{slug}:sha256:{hex}")
}

/// Create the instance for one message, idempotently.
pub async fn deliver(
    storage: &dyn StorageBackend,
    trigger: &TriggerDef,
    data: Value,
    meta: Value,
    source_id: &str,
) -> Result<Delivered, EngineError> {
    let key = idempotency_key(&trigger.slug, source_id);
    match crate::triggers::create_trigger_instance_idempotent(storage, trigger, data, meta, key)
        .await?
    {
        Some(id) => Ok(Delivered::Created(id)),
        None => Ok(Delivered::Duplicate),
    }
}

/// Validate a trigger config for the message-source types. Other trigger
/// types are accepted unchanged.
pub fn validate_config(trigger_type: &TriggerType, config: &Value) -> Result<(), String> {
    match trigger_type {
        TriggerType::Kafka => kafka::KafkaConfig::parse(config).map(|_| ()),
        TriggerType::Sqs => sqs::SqsConfig::parse(config).map(|_| ()),
        TriggerType::PubSub => pubsub::PubSubConfig::parse(config).map(|_| ()),
        TriggerType::RedisStreams => redis_streams::RedisStreamsConfig::parse(config).map(|_| ()),
        TriggerType::PostgresRows => pg_rows::PgRowsConfig::parse(config).map(|_| ()),
        _ => Ok(()),
    }
}

/// Whether this build can run a listener for `trigger_type`.
#[must_use]
// Arms collapse to constants per feature set; keep the explicit table.
#[allow(clippy::match_like_matches_macro)]
pub const fn is_compiled_in(trigger_type: &TriggerType) -> bool {
    match trigger_type {
        TriggerType::Kafka => cfg!(feature = "kafka"),
        TriggerType::Sqs => cfg!(feature = "sqs"),
        TriggerType::PubSub => cfg!(feature = "pubsub"),
        TriggerType::RedisStreams => cfg!(feature = "redis-streams"),
        TriggerType::PostgresRows => cfg!(feature = "postgres-rows"),
        _ => true,
    }
}

/// The trigger config with every `credentials://` reference resolved
/// (tenant-scoped), so secrets never have to be stored inline.
pub async fn resolved_config(
    storage: &dyn StorageBackend,
    trigger: &TriggerDef,
) -> Result<Value, EngineError> {
    let mut config = trigger.config.clone();
    crate::credentials::resolve_in_value(storage, trigger.tenant_id.as_str(), &mut config)
        .await
        .map_err(|e| EngineError::InvalidConfig(format!("credential resolution failed: {e:?}")))?;
    Ok(config)
}

/// Acquire or renew the per-trigger consumer lease for this process. Used by
/// sources without broker-side consumer coordination (Kafka via `rskafka`,
/// Postgres rows) so exactly one engine node consumes a trigger. Fails closed.
#[cfg_attr(
    not(any(feature = "kafka", feature = "postgres-rows")),
    allow(dead_code)
)]
pub(crate) async fn acquire_lease(storage: &dyn StorageBackend, slug: &str, ttl: Duration) -> bool {
    let now = chrono::Utc::now();
    let ttl = chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::minutes(1));
    let until = now.checked_add_signed(ttl).unwrap_or(now);
    match storage
        .try_acquire_trigger_poll_lease(slug, &LEASE_OWNER, now, until)
        .await
    {
        Ok(held) => {
            if !held {
                debug!(slug, "trigger consumer lease held by another node");
            }
            held
        }
        Err(e) => {
            warn!(slug, error = %e, "failed to acquire trigger consumer lease");
            false
        }
    }
}

/// Load a source's persisted cursor (`trigger_poll_state.state`).
#[cfg_attr(
    not(any(feature = "kafka", feature = "postgres-rows")),
    allow(dead_code)
)]
pub(crate) async fn load_cursor(
    storage: &dyn StorageBackend,
    slug: &str,
) -> Result<Option<Value>, EngineError> {
    Ok(storage
        .get_trigger_poll_state(slug)
        .await?
        .map(|s| s.state)
        .filter(|v| !v.is_null()))
}

/// Persist a source's cursor. Keeps the lease columns untouched.
#[cfg_attr(
    not(any(feature = "kafka", feature = "postgres-rows")),
    allow(dead_code)
)]
pub(crate) async fn save_cursor(
    storage: &dyn StorageBackend,
    slug: &str,
    cursor: Value,
    last_error: Option<String>,
) -> Result<(), EngineError> {
    let now = chrono::Utc::now();
    storage
        .upsert_trigger_poll_state(&TriggerPollState {
            slug: slug.to_string(),
            state: cursor,
            last_poll_at: Some(now),
            consecutive_failures: i32::from(last_error.is_some()),
            last_error,
            updated_at: now,
        })
        .await?;
    Ok(())
}

/// Exponential backoff for a failing listener: 1s, 2s, 4s … capped at 60s.
#[must_use]
#[cfg_attr(
    not(any(
        feature = "kafka",
        feature = "sqs",
        feature = "pubsub",
        feature = "redis-streams",
        feature = "postgres-rows"
    )),
    allow(dead_code)
)]
pub(crate) fn failure_backoff(consecutive_failures: u32) -> Duration {
    Duration::from_secs(1u64 << consecutive_failures.min(6)).min(Duration::from_secs(60))
}

/// Sleep for `dur` unless cancelled first. Returns `true` when cancelled.
#[cfg_attr(
    not(any(
        feature = "kafka",
        feature = "sqs",
        feature = "pubsub",
        feature = "redis-streams",
        feature = "postgres-rows"
    )),
    allow(dead_code)
)]
pub(crate) async fn sleep_or_cancel(cancel: &CancellationToken, dur: Duration) -> bool {
    tokio::select! {
        () = cancel.cancelled() => true,
        () = tokio::time::sleep(dur) => false,
    }
}

/// Read an optional string field.
pub(crate) fn opt_str(config: &Value, key: &str) -> Result<Option<String>, String> {
    match config.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s.trim().is_empty() => Err(format!("'{key}' must not be empty")),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(format!("'{key}' must be a string")),
    }
}

/// Read a required string field.
pub(crate) fn req_str(config: &Value, key: &str) -> Result<String, String> {
    opt_str(config, key)?.ok_or_else(|| format!("'{key}' is required"))
}

/// Read an optional unsigned integer field bounded to `[min, max]`.
pub(crate) fn opt_u64(
    config: &Value,
    key: &str,
    default: u64,
    min: u64,
    max: u64,
) -> Result<u64, String> {
    match config.get(key) {
        None | Some(Value::Null) => Ok(default),
        Some(v) => {
            let n = v
                .as_u64()
                .ok_or_else(|| format!("'{key}' must be a non-negative integer"))?;
            if n < min || n > max {
                return Err(format!("'{key}' must be between {min} and {max}"));
            }
            Ok(n)
        }
    }
}

/// Reject a config that is not a JSON object.
pub(crate) fn require_object(config: &Value, kind: &str) -> Result<(), String> {
    if config.is_object() {
        Ok(())
    } else {
        Err(format!("{kind} trigger config must be a JSON object"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_storage::{InstanceStore, SequenceStore};
    use orch8_types::ids::{Namespace, SequenceId, TenantId};
    use serde_json::json;

    #[test]
    fn decode_payload_prefers_json_then_string() {
        assert_eq!(decode_payload(br#"{"a":1}"#), json!({"a": 1}));
        assert_eq!(decode_payload(b"plain text"), json!("plain text"));
        assert_eq!(decode_payload(&[0xff, 0x41]), json!("\u{fffd}A"));
    }

    #[test]
    fn idempotency_key_is_deterministic_and_bounded() {
        assert_eq!(idempotency_key("s", "m1"), "trigger:s:m1");
        assert_eq!(idempotency_key("s", "m1"), idempotency_key("s", "m1"));
        assert_ne!(idempotency_key("s", "m1"), idempotency_key("s2", "m1"));
        let long = "x".repeat(500);
        let k = idempotency_key("s", &long);
        assert!(k.len() < 100, "{k}");
        assert!(k.starts_with("trigger:s:sha256:"));
        assert_ne!(k, idempotency_key("s", &"y".repeat(500)));
    }

    #[test]
    fn validate_config_dispatches_per_type() {
        assert!(validate_config(&TriggerType::Webhook, &Value::Null).is_ok());
        assert!(validate_config(&TriggerType::Kafka, &json!({})).is_err());
        assert!(validate_config(&TriggerType::Sqs, &json!({})).is_err());
        assert!(validate_config(&TriggerType::PubSub, &json!({})).is_err());
        assert!(validate_config(&TriggerType::RedisStreams, &json!({})).is_err());
        assert!(validate_config(&TriggerType::PostgresRows, &json!({})).is_err());
    }

    #[test]
    fn failure_backoff_is_capped() {
        assert_eq!(failure_backoff(0), Duration::from_secs(1));
        assert_eq!(failure_backoff(3), Duration::from_secs(8));
        assert_eq!(failure_backoff(100), Duration::from_secs(60));
    }

    #[tokio::test]
    async fn deliver_creates_once_then_reports_duplicate() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let seq = orch8_types::sequence::SequenceDefinition {
            schema: None,
            schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
            id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t1"),
            namespace: Namespace::new("default"),
            name: "on-msg".into(),
            version: 1,
            deprecated: false,
            status: orch8_types::sequence::SequenceStatus::default(),
            blocks: vec![],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            created_at: chrono::Utc::now(),
        };
        storage.create_sequence(&seq).await.unwrap();
        let now = chrono::Utc::now();
        let trigger = TriggerDef {
            slug: "orders".into(),
            sequence_name: "on-msg".into(),
            version: None,
            tenant_id: TenantId::unchecked("t1"),
            namespace: "default".into(),
            enabled: true,
            secret: None,
            trigger_type: TriggerType::Sqs,
            config: Value::Null,
            created_at: now,
            updated_at: now,
        };
        let first = deliver(
            &storage,
            &trigger,
            json!({"a": 1}),
            json!({"m": 1}),
            "msg-1",
        )
        .await
        .unwrap();
        let Delivered::Created(id) = first else {
            panic!("first delivery must create");
        };
        let again = deliver(
            &storage,
            &trigger,
            json!({"a": 1}),
            json!({"m": 1}),
            "msg-1",
        )
        .await
        .unwrap();
        assert_eq!(again, Delivered::Duplicate);
        let inst = storage.get_instance(id).await.unwrap().unwrap();
        assert_eq!(inst.context.data, json!({"a": 1}));
        assert_eq!(inst.metadata["_trigger_type"], "sqs");
        assert_eq!(
            inst.idempotency_key.as_deref(),
            Some("trigger:orders:msg-1")
        );
        // A different message id creates a new instance.
        assert!(matches!(
            deliver(&storage, &trigger, json!({}), json!({}), "msg-2")
                .await
                .unwrap(),
            Delivered::Created(_)
        ));
    }
}
