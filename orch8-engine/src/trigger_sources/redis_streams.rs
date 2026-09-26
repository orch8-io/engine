//! Redis Streams trigger (`trigger_type: "redis_streams"`, engine feature
//! `redis-streams`).
//!
//! Consumes a stream through a consumer group (`XREADGROUP`), so several
//! engine nodes share the work. Each entry creates one instance (idempotency
//! key = `stream/entry-id`) and is `XACK`ed only after the create commits. An
//! entry whose create fails stays in the group's pending list; the listener
//! re-reads its own pending entries on (re)start and periodically
//! `XAUTOCLAIM`s entries idle longer than `claim_idle_ms` from crashed
//! consumers.
//!
//! ```json
//! {
//!   "url": "credentials://redis-url",   // redis:// or rediss:// (TLS)
//!   "stream": "orders",
//!   "group": "orch8",                // default: orch8-<slug>
//!   "start_id": "$",                 // group creation only: "$" new entries, "0" backlog
//!   "payload_field": "payload",      // field holding the JSON body
//!   "batch_size": 50,
//!   "block_ms": 5000,
//!   "claim_idle_ms": 60000
//! }
//! ```

use std::collections::BTreeMap;

use serde_json::{Value, json};

use super::{opt_str, opt_u64, req_str, require_object};

/// Validated `redis_streams` trigger config.
#[derive(Clone, PartialEq, Eq)]
pub struct RedisStreamsConfig {
    pub url: String,
    pub stream: String,
    pub group: Option<String>,
    pub start_id: String,
    pub payload_field: String,
    pub batch_size: usize,
    pub block_ms: usize,
    pub claim_idle_ms: usize,
}

impl std::fmt::Debug for RedisStreamsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStreamsConfig")
            .field("url", &"<redacted>")
            .field("stream", &self.stream)
            .field("group", &self.group)
            .field("start_id", &self.start_id)
            .finish_non_exhaustive()
    }
}

impl RedisStreamsConfig {
    pub fn parse(config: &Value) -> Result<Self, String> {
        require_object(config, "redis_streams")?;
        let url = req_str(config, "url")?;
        if !(url.starts_with("redis://")
            || url.starts_with("rediss://")
            || url.starts_with("credentials://"))
        {
            return Err(
                "'url' must be a redis:// or rediss:// URL or a credentials:// reference".into(),
            );
        }
        let stream = req_str(config, "stream")?;
        let start_id = opt_str(config, "start_id")?.unwrap_or_else(|| "$".into());
        let valid_start = start_id == "$"
            || start_id
                .split_once('-')
                .map_or(start_id.parse::<u64>().is_ok(), |(ms, seq)| {
                    ms.parse::<u64>().is_ok() && seq.parse::<u64>().is_ok()
                });
        if !valid_start {
            return Err("'start_id' must be '$', '0' or a stream id like 1700000000000-0".into());
        }
        let as_usize = |n: u64| usize::try_from(n).map_err(|_| "value out of range".to_string());
        Ok(Self {
            url,
            stream,
            group: opt_str(config, "group")?,
            start_id,
            payload_field: opt_str(config, "payload_field")?.unwrap_or_else(|| "payload".into()),
            batch_size: as_usize(opt_u64(config, "batch_size", 50, 1, 1000)?)?,
            block_ms: as_usize(opt_u64(config, "block_ms", 5000, 0, 60_000)?)?,
            claim_idle_ms: as_usize(opt_u64(config, "claim_idle_ms", 60_000, 1000, 86_400_000)?)?,
        })
    }

    /// Consumer group name for a trigger.
    #[must_use]
    pub fn group_for(&self, slug: &str) -> String {
        self.group
            .clone()
            .unwrap_or_else(|| format!("orch8-{slug}"))
    }
}

/// Map one stream entry (fields already converted to strings) to
/// `(data, meta, source_id)`. When `payload_field` is present its value is
/// the payload (parsed as JSON when possible); otherwise the whole field map
/// is the payload.
#[must_use]
pub fn map_entry(
    stream: &str,
    id: &str,
    fields: &BTreeMap<String, String>,
    payload_field: &str,
) -> (Value, Value, String) {
    let data = match fields.get(payload_field) {
        Some(v) => super::decode_payload(v.as_bytes()),
        None => Value::Object(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                .collect(),
        ),
    };
    let meta = json!({
        "stream": stream,
        "id": id,
        "fields": fields,
    });
    (data, meta, format!("{stream}/{id}"))
}

#[cfg(feature = "redis-streams")]
pub use listener::run;

#[cfg(feature = "redis-streams")]
mod listener {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use redis::AsyncCommands;
    use redis::streams::{
        StreamAutoClaimOptions, StreamAutoClaimReply, StreamId, StreamReadOptions, StreamReadReply,
    };
    use tokio_util::sync::CancellationToken;
    use tracing::{error, info, warn};

    use orch8_storage::StorageBackend;
    use orch8_types::trigger::TriggerDef;

    use super::RedisStreamsConfig;
    use crate::error::EngineError;
    use crate::trigger_sources::{
        LEASE_OWNER, deliver, failure_backoff, resolved_config, sleep_or_cancel,
    };

    fn fields_as_strings(entry: &StreamId) -> BTreeMap<String, String> {
        entry
            .map
            .iter()
            .map(|(k, v)| {
                let s =
                    redis::from_redis_value_ref::<String>(v).unwrap_or_else(|_| format!("{v:?}"));
                (k.clone(), s)
            })
            .collect()
    }

    /// Deliver entries and XACK the ones whose instance was created (or
    /// already existed). Returns how many entries were handled.
    async fn process(
        storage: &dyn StorageBackend,
        trigger: &TriggerDef,
        cfg: &RedisStreamsConfig,
        group: &str,
        con: &mut redis::aio::MultiplexedConnection,
        entries: &[StreamId],
    ) -> Result<usize, redis::RedisError> {
        let mut acks: Vec<&str> = Vec::new();
        for entry in entries {
            let fields = fields_as_strings(entry);
            let (data, meta, source_id) =
                super::map_entry(&cfg.stream, &entry.id, &fields, &cfg.payload_field);
            match deliver(storage, trigger, data, meta, &source_id).await {
                Ok(_) => acks.push(entry.id.as_str()),
                Err(e) => {
                    error!(slug = %trigger.slug, id = %entry.id, error = %e, "redis stream entry not delivered; left pending");
                }
            }
        }
        if !acks.is_empty() {
            let _: usize = con.xack(&cfg.stream, group, &acks).await?;
        }
        Ok(entries.len())
    }

    /// Run the Redis Streams listener until cancelled.
    // One reconnect/read/claim state machine kept in one place.
    #[allow(clippy::too_many_lines)]
    pub async fn run(
        storage: Arc<dyn StorageBackend>,
        trigger: TriggerDef,
        cancel: CancellationToken,
    ) -> Result<(), EngineError> {
        let config = resolved_config(storage.as_ref(), &trigger).await?;
        let cfg = RedisStreamsConfig::parse(&config)
            .map_err(|e| EngineError::InvalidConfig(format!("redis_streams: {e}")))?;
        if rustls::crypto::CryptoProvider::get_default().is_none() {
            // rediss:// needs a process-level rustls provider; both ring and
            // aws-lc-rs are linked, so pick one explicitly.
            let _ = rustls::crypto::ring::default_provider().install_default();
        }
        let client = redis::Client::open(cfg.url.as_str())
            .map_err(|e| EngineError::InvalidConfig(format!("redis_streams: invalid url: {e}")))?;
        let group = cfg.group_for(&trigger.slug);
        let consumer = LEASE_OWNER.clone();
        let slug = trigger.slug.clone();
        let mut failures = 0u32;

        'connect: loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            let mut con = match client.get_multiplexed_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    failures = failures.saturating_add(1);
                    warn!(slug, error = %e, "redis connect failed");
                    if sleep_or_cancel(&cancel, failure_backoff(failures)).await {
                        return Ok(());
                    }
                    continue;
                }
            };
            // Create the group (and stream) if missing; BUSYGROUP means it exists.
            let created: redis::RedisResult<()> = con
                .xgroup_create_mkstream(&cfg.stream, &group, &cfg.start_id)
                .await;
            if let Err(e) = created
                && e.code() != Some("BUSYGROUP")
            {
                failures = failures.saturating_add(1);
                warn!(slug, error = %e, "redis XGROUP CREATE failed");
                if sleep_or_cancel(&cancel, failure_backoff(failures)).await {
                    return Ok(());
                }
                continue;
            }
            info!(slug, stream = %cfg.stream, group, consumer, "redis streams trigger listener active");

            // Re-read this consumer's own pending entries first ("0"), then
            // switch to new entries (">").
            let mut read_id = "0";
            let mut last_claim = Instant::now()
                .checked_sub(Duration::from_millis(cfg.claim_idle_ms as u64))
                .unwrap_or_else(Instant::now);
            loop {
                if cancel.is_cancelled() {
                    info!(slug, "redis streams trigger listener shutting down");
                    return Ok(());
                }
                if last_claim.elapsed() >= Duration::from_millis(cfg.claim_idle_ms as u64) {
                    last_claim = Instant::now();
                    let claimed: redis::RedisResult<StreamAutoClaimReply> = con
                        .xautoclaim_options(
                            &cfg.stream,
                            &group,
                            &consumer,
                            cfg.claim_idle_ms,
                            "0-0",
                            StreamAutoClaimOptions::default().count(cfg.batch_size),
                        )
                        .await;
                    match claimed {
                        Ok(reply) if !reply.claimed.is_empty() => {
                            info!(
                                slug,
                                claimed = reply.claimed.len(),
                                "redis: reclaimed idle pending entries"
                            );
                            if let Err(e) = process(
                                storage.as_ref(),
                                &trigger,
                                &cfg,
                                &group,
                                &mut con,
                                &reply.claimed,
                            )
                            .await
                            {
                                warn!(slug, error = %e, "redis XACK failed");
                                continue 'connect;
                            }
                        }
                        Ok(_) => {}
                        Err(e) => warn!(slug, error = %e, "redis XAUTOCLAIM failed"),
                    }
                }
                let mut opts = StreamReadOptions::default()
                    .group(&group, &consumer)
                    .count(cfg.batch_size);
                if read_id == ">" {
                    opts = opts.block(cfg.block_ms);
                }
                let keys = [cfg.stream.as_str()];
                let ids = [read_id];
                let read: redis::RedisResult<Option<StreamReadReply>> = tokio::select! {
                    () = cancel.cancelled() => return Ok(()),
                    r = con.xread_options(&keys, &ids, &opts) => r,
                };
                let entries: Vec<StreamId> = match read {
                    Ok(reply) => {
                        failures = 0;
                        reply
                            .map(|r| r.keys.into_iter().flat_map(|k| k.ids).collect())
                            .unwrap_or_default()
                    }
                    Err(e) => {
                        failures = failures.saturating_add(1);
                        warn!(slug, error = %e, "redis XREADGROUP failed");
                        if sleep_or_cancel(&cancel, failure_backoff(failures)).await {
                            return Ok(());
                        }
                        continue 'connect;
                    }
                };
                if entries.is_empty() {
                    if read_id == "0" {
                        read_id = ">";
                    }
                    continue;
                }
                if let Err(e) =
                    process(storage.as_ref(), &trigger, &cfg, &group, &mut con, &entries).await
                {
                    warn!(slug, error = %e, "redis XACK failed");
                    continue 'connect;
                }
                // Pending entries that failed again stay pending; move on to
                // new entries and let XAUTOCLAIM retry them after the idle
                // window instead of hot-looping on a poison entry.
                if read_id == "0" {
                    read_id = ">";
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_defaults_and_overrides() {
        let c = RedisStreamsConfig::parse(&json!({"url": "redis://localhost:6379", "stream": "s"}))
            .unwrap();
        assert_eq!(c.start_id, "$");
        assert_eq!(c.payload_field, "payload");
        assert_eq!(c.batch_size, 50);
        assert_eq!(c.group_for("orders"), "orch8-orders");
        let c = RedisStreamsConfig::parse(&json!({
            "url": "rediss://h:6380", "stream": "s", "group": "g", "start_id": "0",
            "payload_field": "body", "batch_size": 5, "block_ms": 0, "claim_idle_ms": 5000
        }))
        .unwrap();
        assert_eq!(c.group_for("x"), "g");
        assert_eq!(c.start_id, "0");
        assert!(!format!("{c:?}").contains("rediss://h"));
        assert!(
            RedisStreamsConfig::parse(
                &json!({"url": "redis://h", "stream": "s", "start_id": "1700000000000-3"})
            )
            .is_ok()
        );
        assert!(
            RedisStreamsConfig::parse(&json!({"url": "credentials://redis-url", "stream": "s"}))
                .is_ok()
        );
    }

    #[test]
    fn rejects_invalid_configs() {
        for bad in [
            json!({}),
            json!({"url": "http://h", "stream": "s"}),
            json!({"url": "redis://h"}),
            json!({"url": "redis://h", "stream": "s", "start_id": "latest"}),
            json!({"url": "redis://h", "stream": "s", "batch_size": 0}),
            json!({"url": "redis://h", "stream": "s", "claim_idle_ms": 10}),
        ] {
            assert!(RedisStreamsConfig::parse(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn maps_entry_with_and_without_payload_field() {
        let mut fields = BTreeMap::new();
        fields.insert("payload".to_string(), r#"{"order": 1}"#.to_string());
        fields.insert("source".to_string(), "web".to_string());
        let (data, meta, id) = map_entry("orders", "1-0", &fields, "payload");
        assert_eq!(data, json!({"order": 1}));
        assert_eq!(meta["fields"]["source"], "web");
        assert_eq!(id, "orders/1-0");

        let mut flat = BTreeMap::new();
        flat.insert("a".to_string(), "1".to_string());
        let (data, _, _) = map_entry("s", "2-0", &flat, "payload");
        assert_eq!(data, json!({"a": "1"}));
    }
}
