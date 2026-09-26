//! Kafka trigger (`trigger_type: "kafka"`, engine feature `kafka`).
//!
//! Uses the pure-Rust `rskafka` client. `rskafka` has no consumer-group
//! coordination, so offsets are owned by the engine: they are persisted per
//! partition in `trigger_poll_state` (committed only after every record up to
//! that offset produced a durable instance), and a per-trigger lease makes a
//! single engine node consume the trigger at a time. Each record's
//! idempotency key is `topic/partition/offset`, so re-reading after a crash
//! never duplicates a run.
//!
//! ```json
//! {
//!   "brokers": ["kafka-1:9092", "kafka-2:9092"],
//!   "topic": "orders",
//!   "partitions": [0, 1],            // optional, default: all partitions
//!   "start_from": "latest",          // or "earliest" (first run only)
//!   "tls": false,
//!   "sasl": {"mechanism": "plain", "username": "u", "password": "credentials://kafka-pw"},
//!   "max_wait_ms": 500,
//!   "max_batch_bytes": 1048576
//! }
//! ```
//!
//! Supported record compression: none, gzip, snappy (lz4/zstd need C
//! libraries and are not compiled in).

use serde_json::{Value, json};

use super::{opt_str, opt_u64, req_str, require_object};

/// Where to begin when a partition has no persisted offset yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartFrom {
    Earliest,
    Latest,
}

/// SASL mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

/// SASL credentials.
#[derive(Clone, PartialEq, Eq)]
pub struct KafkaSasl {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for KafkaSasl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSasl")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// Validated `kafka` trigger config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    pub partitions: Option<Vec<i32>>,
    pub start_from: StartFrom,
    pub tls: bool,
    pub sasl: Option<KafkaSasl>,
    pub max_wait_ms: i32,
    pub max_batch_bytes: i32,
}

impl KafkaConfig {
    // Flat field-by-field validation; each block is independent.
    #[allow(clippy::too_many_lines)]
    pub fn parse(config: &Value) -> Result<Self, String> {
        require_object(config, "kafka")?;
        let brokers: Vec<String> = match config.get("brokers") {
            Some(Value::String(s)) => s
                .split(',')
                .map(str::trim)
                .filter(|b| !b.is_empty())
                .map(ToString::to_string)
                .collect(),
            Some(Value::Array(items)) => items
                .iter()
                .map(|b| {
                    b.as_str()
                        .map(|s| s.trim().to_string())
                        .ok_or_else(|| "'brokers' entries must be strings".to_string())
                })
                .collect::<Result<_, _>>()?,
            _ => return Err("'brokers' is required (array or comma-separated string)".into()),
        };
        if brokers.is_empty() {
            return Err("'brokers' must list at least one host:port".into());
        }
        for b in &brokers {
            let valid = b
                .rsplit_once(':')
                .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok());
            if !valid {
                return Err(format!("broker '{b}' must be host:port"));
            }
        }
        let topic = req_str(config, "topic")?;
        if topic.len() > 249
            || !topic
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        {
            return Err("'topic' must be 1-249 chars of [a-zA-Z0-9._-]".into());
        }
        let partitions = match config.get("partitions") {
            None | Some(Value::Null) => None,
            Some(Value::Array(items)) => {
                let parts = items
                    .iter()
                    .map(|p| {
                        p.as_i64()
                            .and_then(|n| i32::try_from(n).ok())
                            .filter(|n| *n >= 0)
                            .ok_or_else(|| "'partitions' must be non-negative integers".to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if parts.is_empty() {
                    return Err("'partitions' must not be empty when provided".into());
                }
                Some(parts)
            }
            Some(_) => return Err("'partitions' must be an array".into()),
        };
        let start_from = match opt_str(config, "start_from")?.as_deref() {
            None | Some("latest") => StartFrom::Latest,
            Some("earliest") => StartFrom::Earliest,
            Some(other) => {
                return Err(format!(
                    "'start_from' must be 'earliest' or 'latest', got '{other}'"
                ));
            }
        };
        let tls = match config.get("tls") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(b)) => *b,
            Some(_) => return Err("'tls' must be a boolean".into()),
        };
        let sasl = match config.get("sasl") {
            None | Some(Value::Null) => None,
            Some(s @ Value::Object(_)) => {
                let mechanism = match req_str(s, "mechanism")?.to_ascii_lowercase().as_str() {
                    "plain" => SaslMechanism::Plain,
                    "scram-sha-256" => SaslMechanism::ScramSha256,
                    "scram-sha-512" => SaslMechanism::ScramSha512,
                    other => {
                        return Err(format!(
                            "sasl.mechanism must be plain, scram-sha-256 or scram-sha-512, got '{other}'"
                        ));
                    }
                };
                Some(KafkaSasl {
                    mechanism,
                    username: req_str(s, "username").map_err(|e| format!("sasl: {e}"))?,
                    password: req_str(s, "password").map_err(|e| format!("sasl: {e}"))?,
                })
            }
            Some(_) => return Err("'sasl' must be an object".into()),
        };
        let max_wait_ms = i32::try_from(opt_u64(config, "max_wait_ms", 500, 10, 30_000)?)
            .map_err(|_| "'max_wait_ms' out of range".to_string())?;
        let max_batch_bytes = i32::try_from(opt_u64(
            config,
            "max_batch_bytes",
            1_048_576,
            1024,
            52_428_800,
        )?)
        .map_err(|_| "'max_batch_bytes' out of range".to_string())?;
        Ok(Self {
            brokers,
            topic,
            partitions,
            start_from,
            tls,
            sasl,
            max_wait_ms,
            max_batch_bytes,
        })
    }
}

/// A fetched Kafka record, decoupled from the client crate for testing.
#[derive(Debug, Clone)]
pub struct KafkaRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Map a record to `(data, event_meta, source_id)`.
#[must_use]
pub fn map_record(record: &KafkaRecord) -> (Value, Value, String) {
    let data = record
        .value
        .as_deref()
        .map_or(Value::Null, super::decode_payload);
    let headers: serde_json::Map<String, Value> = record
        .headers
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                Value::String(String::from_utf8_lossy(v).into_owned()),
            )
        })
        .collect();
    let meta = json!({
        "topic": record.topic,
        "partition": record.partition,
        "offset": record.offset,
        "key": record.key.as_deref().map(|k| String::from_utf8_lossy(k).into_owned()),
        "headers": headers,
        "timestamp": record.timestamp.to_rfc3339(),
    });
    let source_id = format!("{}/{}/{}", record.topic, record.partition, record.offset);
    (data, meta, source_id)
}

/// Next offset to read per partition, as persisted in `trigger_poll_state`.
/// A cursor written for a different topic (the trigger's config was edited)
/// is ignored, so the new topic starts from `start_from`.
#[must_use]
pub fn offsets_from_cursor(
    cursor: Option<&Value>,
    topic: &str,
) -> std::collections::BTreeMap<i32, i64> {
    cursor
        .filter(|c| c.get("topic").and_then(Value::as_str) == Some(topic))
        .and_then(|c| c.get("offsets"))
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(p, o)| Some((p.parse::<i32>().ok()?, o.as_i64()?)))
                .collect()
        })
        .unwrap_or_default()
}

/// Serialize per-partition offsets into the persisted cursor shape.
#[must_use]
pub fn cursor_from_offsets(topic: &str, offsets: &std::collections::BTreeMap<i32, i64>) -> Value {
    let map: serde_json::Map<String, Value> = offsets
        .iter()
        .map(|(p, o)| (p.to_string(), json!(o)))
        .collect();
    json!({ "topic": topic, "offsets": map })
}

#[cfg(feature = "kafka")]
pub use listener::run;

#[cfg(feature = "kafka")]
mod listener {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use rskafka::client::partition::{OffsetAt, PartitionClient, UnknownTopicHandling};
    use rskafka::client::{Client, ClientBuilder, Credentials, SaslConfig};
    use tokio_util::sync::CancellationToken;
    use tracing::{error, info, warn};

    use orch8_storage::StorageBackend;
    use orch8_types::trigger::TriggerDef;

    use super::{KafkaConfig, KafkaRecord, SaslMechanism, StartFrom};
    use crate::error::EngineError;
    use crate::trigger_sources::{
        acquire_lease, deliver, failure_backoff, load_cursor, resolved_config, save_cursor,
        sleep_or_cancel,
    };

    const LEASE_HOLD: Duration = Duration::from_secs(60);
    const IDLE_WAIT: Duration = Duration::from_millis(250);

    fn cfg_err(e: impl std::fmt::Display) -> EngineError {
        EngineError::InvalidConfig(format!("kafka: {e}"))
    }

    async fn connect(cfg: &KafkaConfig) -> Result<Client, EngineError> {
        let mut builder = ClientBuilder::new(cfg.brokers.clone()).client_id("orch8-trigger");
        if cfg.tls {
            let roots = rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            };
            let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(cfg_err)?
            .with_root_certificates(roots)
            .with_no_client_auth();
            builder = builder.tls_config(Arc::new(tls));
        }
        if let Some(sasl) = &cfg.sasl {
            let creds = Credentials::new(sasl.username.clone(), sasl.password.clone());
            builder = builder.sasl_config(match sasl.mechanism {
                SaslMechanism::Plain => SaslConfig::Plain(creds),
                SaslMechanism::ScramSha256 => SaslConfig::ScramSha256(creds),
                SaslMechanism::ScramSha512 => SaslConfig::ScramSha512(creds),
            });
        }
        builder.build().await.map_err(cfg_err)
    }

    async fn partitions(client: &Client, cfg: &KafkaConfig) -> Result<Vec<i32>, EngineError> {
        if let Some(p) = &cfg.partitions {
            return Ok(p.clone());
        }
        let topics = client.list_topics().await.map_err(cfg_err)?;
        let topic = topics
            .into_iter()
            .find(|t| t.name == cfg.topic)
            .ok_or_else(|| cfg_err(format!("topic '{}' not found", cfg.topic)))?;
        Ok(topic.partitions.into_iter().collect())
    }

    /// Run the Kafka listener until cancelled or a fatal config error.
    // One reconnect/consume state machine; splitting it would scatter the
    // lease/offset invariants across helpers.
    #[allow(clippy::too_many_lines)]
    pub async fn run(
        storage: Arc<dyn StorageBackend>,
        trigger: TriggerDef,
        cancel: CancellationToken,
    ) -> Result<(), EngineError> {
        let config = resolved_config(storage.as_ref(), &trigger).await?;
        let cfg = KafkaConfig::parse(&config).map_err(cfg_err)?;
        let slug = trigger.slug.clone();
        let mut failures = 0u32;

        'reconnect: loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            // Only the lease holder connects and consumes.
            if !acquire_lease(storage.as_ref(), &slug, LEASE_HOLD).await {
                if sleep_or_cancel(&cancel, LEASE_HOLD / 4).await {
                    return Ok(());
                }
                continue;
            }
            let client = match connect(&cfg).await {
                Ok(c) => c,
                Err(e) => {
                    warn!(slug, error = %e, "kafka connect failed, retrying");
                    failures = failures.saturating_add(1);
                    if sleep_or_cancel(&cancel, failure_backoff(failures)).await {
                        return Ok(());
                    }
                    continue;
                }
            };
            let parts = match partitions(&client, &cfg).await {
                Ok(p) => p,
                Err(e) => {
                    warn!(slug, error = %e, "kafka partition discovery failed, retrying");
                    failures = failures.saturating_add(1);
                    if sleep_or_cancel(&cancel, failure_backoff(failures)).await {
                        return Ok(());
                    }
                    continue;
                }
            };
            let mut clients: Vec<PartitionClient> = Vec::with_capacity(parts.len());
            for p in &parts {
                match client
                    .partition_client(cfg.topic.clone(), *p, UnknownTopicHandling::Retry)
                    .await
                {
                    Ok(pc) => clients.push(pc),
                    Err(e) => {
                        warn!(slug, partition = p, error = %e, "kafka partition client failed");
                        failures = failures.saturating_add(1);
                        if sleep_or_cancel(&cancel, failure_backoff(failures)).await {
                            return Ok(());
                        }
                        continue 'reconnect;
                    }
                }
            }
            let mut offsets = super::offsets_from_cursor(
                load_cursor(storage.as_ref(), &slug).await?.as_ref(),
                &cfg.topic,
            );
            for pc in &clients {
                if offsets.contains_key(&pc.partition()) {
                    continue;
                }
                let at = match cfg.start_from {
                    StartFrom::Earliest => OffsetAt::Earliest,
                    StartFrom::Latest => OffsetAt::Latest,
                };
                match pc.get_offset(at).await {
                    Ok(o) => {
                        offsets.insert(pc.partition(), o);
                    }
                    Err(e) => {
                        warn!(slug, error = %e, "kafka initial offset lookup failed");
                        failures = failures.saturating_add(1);
                        if sleep_or_cancel(&cancel, failure_backoff(failures)).await {
                            return Ok(());
                        }
                        continue 'reconnect;
                    }
                }
            }
            save_cursor(
                storage.as_ref(),
                &slug,
                super::cursor_from_offsets(&cfg.topic, &offsets),
                None,
            )
            .await?;
            info!(slug, topic = %cfg.topic, partitions = ?parts, "kafka trigger listener active");

            loop {
                if cancel.is_cancelled() {
                    info!(slug, "kafka trigger listener shutting down");
                    return Ok(());
                }
                if !acquire_lease(storage.as_ref(), &slug, LEASE_HOLD).await {
                    warn!(slug, "kafka consumer lease lost, pausing consumption");
                    continue 'reconnect;
                }
                let mut consumed = 0usize;
                for pc in &clients {
                    match consume_partition(storage.as_ref(), &trigger, &cfg, pc, &mut offsets)
                        .await
                    {
                        Ok(n) => consumed += n,
                        Err(e) => {
                            error!(slug, partition = pc.partition(), error = %e, "kafka consume failed");
                            let _ = save_cursor(
                                storage.as_ref(),
                                &slug,
                                super::cursor_from_offsets(&cfg.topic, &offsets),
                                Some(e.to_string()),
                            )
                            .await;
                            failures = failures.saturating_add(1);
                            if sleep_or_cancel(&cancel, failure_backoff(failures)).await {
                                return Ok(());
                            }
                            continue 'reconnect;
                        }
                    }
                }
                failures = 0;
                if consumed > 0 {
                    save_cursor(
                        storage.as_ref(),
                        &slug,
                        super::cursor_from_offsets(&cfg.topic, &offsets),
                        None,
                    )
                    .await?;
                } else if sleep_or_cancel(&cancel, IDLE_WAIT).await {
                    return Ok(());
                }
            }
        }
    }

    /// Fetch one batch from a partition and deliver it in offset order. The
    /// in-memory offset advances only past records whose instance is durably
    /// created; the caller persists it after the batch.
    async fn consume_partition(
        storage: &dyn StorageBackend,
        trigger: &TriggerDef,
        cfg: &KafkaConfig,
        pc: &PartitionClient,
        offsets: &mut BTreeMap<i32, i64>,
    ) -> Result<usize, EngineError> {
        let partition = pc.partition();
        let offset = offsets.get(&partition).copied().unwrap_or(0);
        let fetched = pc
            .fetch_records(offset, 1..cfg.max_batch_bytes, cfg.max_wait_ms)
            .await;
        let (records, _high_watermark) = match fetched {
            Ok(r) => r,
            Err(e) if e.to_string().contains("OffsetOutOfRange") => {
                // Retention removed our position: resume at the earliest kept record.
                let earliest = pc.get_offset(OffsetAt::Earliest).await.map_err(cfg_err)?;
                warn!(slug = %trigger.slug, partition, offset, earliest, "kafka offset out of range, resetting to earliest");
                offsets.insert(partition, earliest);
                return Ok(0);
            }
            Err(e) => return Err(cfg_err(e)),
        };
        let mut delivered = 0usize;
        for rec in records {
            if rec.offset < offset {
                continue;
            }
            let record = KafkaRecord {
                topic: cfg.topic.clone(),
                partition,
                offset: rec.offset,
                key: rec.record.key,
                value: rec.record.value,
                headers: rec.record.headers.into_iter().collect(),
                timestamp: rec.record.timestamp,
            };
            let (data, meta, source_id) = super::map_record(&record);
            deliver(storage, trigger, data, meta, &source_id).await?;
            offsets.insert(partition, rec.offset + 1);
            delivered += 1;
        }
        Ok(delivered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_and_full_configs() {
        let c =
            KafkaConfig::parse(&json!({"brokers": "a:9092, b:9093", "topic": "orders"})).unwrap();
        assert_eq!(c.brokers, vec!["a:9092", "b:9093"]);
        assert_eq!(c.start_from, StartFrom::Latest);
        assert!(!c.tls);
        assert!(c.sasl.is_none());
        assert_eq!(c.max_wait_ms, 500);

        let c = KafkaConfig::parse(&json!({
            "brokers": ["k:9092"], "topic": "t.v1", "partitions": [0, 2],
            "start_from": "earliest", "tls": true,
            "sasl": {"mechanism": "SCRAM-SHA-512", "username": "u", "password": "p"},
            "max_wait_ms": 1000, "max_batch_bytes": 2048
        }))
        .unwrap();
        assert_eq!(c.partitions, Some(vec![0, 2]));
        assert_eq!(c.start_from, StartFrom::Earliest);
        assert_eq!(c.sasl.unwrap().mechanism, SaslMechanism::ScramSha512);
    }

    #[test]
    fn rejects_invalid_configs() {
        for bad in [
            json!(null),
            json!({"topic": "t"}),
            json!({"brokers": [], "topic": "t"}),
            json!({"brokers": ["nohost"], "topic": "t"}),
            json!({"brokers": ["h:99999"], "topic": "t"}),
            json!({"brokers": ["h:1"]}),
            json!({"brokers": ["h:1"], "topic": "bad topic"}),
            json!({"brokers": ["h:1"], "topic": "t", "partitions": [-1]}),
            json!({"brokers": ["h:1"], "topic": "t", "partitions": []}),
            json!({"brokers": ["h:1"], "topic": "t", "start_from": "middle"}),
            json!({"brokers": ["h:1"], "topic": "t", "tls": "yes"}),
            json!({"brokers": ["h:1"], "topic": "t", "sasl": {"mechanism": "gssapi", "username": "u", "password": "p"}}),
            json!({"brokers": ["h:1"], "topic": "t", "sasl": {"mechanism": "plain", "username": "u"}}),
            json!({"brokers": ["h:1"], "topic": "t", "max_wait_ms": 0}),
        ] {
            assert!(KafkaConfig::parse(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn maps_record_to_instance_input() {
        let rec = KafkaRecord {
            topic: "orders".into(),
            partition: 3,
            offset: 42,
            key: Some(b"order-9".to_vec()),
            value: Some(br#"{"id": 9}"#.to_vec()),
            headers: vec![("trace".into(), b"abc".to_vec())],
            timestamp: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        };
        let (data, meta, id) = map_record(&rec);
        assert_eq!(data, json!({"id": 9}));
        assert_eq!(meta["topic"], "orders");
        assert_eq!(meta["partition"], 3);
        assert_eq!(meta["offset"], 42);
        assert_eq!(meta["key"], "order-9");
        assert_eq!(meta["headers"]["trace"], "abc");
        assert_eq!(id, "orders/3/42");

        let tombstone = KafkaRecord {
            value: None,
            key: None,
            ..rec
        };
        let (data, meta, _) = map_record(&tombstone);
        assert_eq!(data, Value::Null);
        assert_eq!(meta["key"], Value::Null);
    }

    #[test]
    fn cursor_round_trips() {
        let mut offsets = std::collections::BTreeMap::new();
        offsets.insert(0, 10);
        offsets.insert(7, 3);
        let cursor = cursor_from_offsets("t", &offsets);
        assert_eq!(offsets_from_cursor(Some(&cursor), "t"), offsets);
        assert!(offsets_from_cursor(Some(&cursor), "other-topic").is_empty());
        assert!(offsets_from_cursor(None, "t").is_empty());
        assert!(
            offsets_from_cursor(Some(&json!({"topic": "t", "offsets": {"x": 1}})), "t").is_empty()
        );
    }
}
