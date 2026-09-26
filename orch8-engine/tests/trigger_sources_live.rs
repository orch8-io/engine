//! Live integration tests for the message-source triggers.
//!
//! Each test needs a real broker and is `#[ignore]`d; run one with its
//! feature and env var, e.g.
//!
//! ```text
//! docker run -d -p 6379:6379 redis:7
//! ORCH8_TEST_REDIS_URL=redis://127.0.0.1:6379 \
//!   cargo test -p orch8-engine --features redis-streams --test trigger_sources_live -- --ignored
//! ```
//!
//! | test | feature | env |
//! |---|---|---|
//! | `redis_streams_*` | `redis-streams` | `ORCH8_TEST_REDIS_URL` |
//! | `kafka_*` | `kafka` | `ORCH8_TEST_KAFKA_BROKERS` (e.g. redpanda `127.0.0.1:9092`) |
//! | `sqs_*` | `sqs` | `ORCH8_TEST_SQS_QUEUE_URL` (+ `AWS_*`; `LocalStack`/`ElasticMQ` work) |
//! | `pubsub_*` | `pubsub` | `PUBSUB_EMULATOR_HOST` + `ORCH8_TEST_PUBSUB_PROJECT` |
#![allow(dead_code, unused_imports)]

use std::sync::Arc;
use std::time::Duration;

use orch8_storage::{StorageBackend, sqlite::SqliteStorage};
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{Namespace, SequenceId, TenantId};
use orch8_types::instance::TaskInstance;
use orch8_types::trigger::{TriggerDef, TriggerType};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

async fn setup(
    tt: TriggerType,
    slug: &str,
    config: Value,
) -> (Arc<dyn StorageBackend>, TriggerDef) {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
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
        slug: slug.into(),
        sequence_name: "on-msg".into(),
        version: None,
        tenant_id: TenantId::unchecked("t1"),
        namespace: "default".into(),
        enabled: true,
        secret: None,
        trigger_type: tt,
        config,
        created_at: now,
        updated_at: now,
    };
    storage.create_trigger(&trigger).await.unwrap();
    (storage, trigger)
}

async fn instances(storage: &Arc<dyn StorageBackend>) -> Vec<TaskInstance> {
    storage
        .list_instances(
            &InstanceFilter {
                tenant_id: Some(TenantId::unchecked("t1")),
                ..InstanceFilter::default()
            },
            &Pagination {
                offset: 0,
                limit: 1000,
                sort_ascending: true,
            },
        )
        .await
        .unwrap()
}

async fn wait_for(storage: &Arc<dyn StorageBackend>, n: usize, secs: u64) -> Vec<TaskInstance> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let found = instances(storage).await;
        if found.len() >= n || tokio::time::Instant::now() > deadline {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::now_v7().simple())
}

#[cfg(feature = "redis-streams")]
#[tokio::test]
#[ignore = "needs a Redis server: set ORCH8_TEST_REDIS_URL"]
async fn redis_streams_delivers_acks_and_survives_restart() {
    use redis::AsyncCommands;
    let url = std::env::var("ORCH8_TEST_REDIS_URL").expect("ORCH8_TEST_REDIS_URL");
    let stream = unique("orch8-test-stream");
    let (storage, trigger) = setup(
        TriggerType::RedisStreams,
        "redis-t",
        json!({"url": url, "stream": stream, "start_id": "0", "block_ms": 200}),
    )
    .await;
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();
    let _: String = con
        .xadd(&stream, "*", &[("payload", r#"{"order": 1}"#)])
        .await
        .unwrap();

    let cancel = CancellationToken::new();
    let task = tokio::spawn(orch8_engine::trigger_sources::redis_streams::run(
        Arc::clone(&storage),
        trigger.clone(),
        cancel.clone(),
    ));
    let got = wait_for(&storage, 1, 10).await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].context.data, json!({"order": 1}));
    cancel.cancel();
    task.await.unwrap().unwrap();

    // Produced while the listener is down → picked up after restart.
    let _: String = con
        .xadd(&stream, "*", &[("payload", r#"{"order": 2}"#)])
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(orch8_engine::trigger_sources::redis_streams::run(
        Arc::clone(&storage),
        trigger,
        cancel.clone(),
    ));
    let got = wait_for(&storage, 2, 10).await;
    assert_eq!(got.len(), 2, "no loss and no duplicates across restart");
    // Everything acked: nothing pending for the group.
    let pending: redis::streams::StreamPendingReply =
        con.xpending(&stream, "orch8-redis-t").await.unwrap();
    assert_eq!(pending.count(), 0);
    cancel.cancel();
    task.await.unwrap().unwrap();
    let _: () = con.del(&stream).await.unwrap();
}

#[cfg(feature = "kafka")]
#[tokio::test]
#[ignore = "needs a Kafka/Redpanda broker: set ORCH8_TEST_KAFKA_BROKERS"]
async fn kafka_delivers_from_earliest_and_resumes_from_persisted_offset() {
    use rskafka::client::ClientBuilder;
    use rskafka::client::partition::{Compression, UnknownTopicHandling};
    use rskafka::record::Record;
    let brokers = std::env::var("ORCH8_TEST_KAFKA_BROKERS").expect("ORCH8_TEST_KAFKA_BROKERS");
    let topic = unique("orch8-test");
    let client = ClientBuilder::new(vec![brokers.clone()])
        .build()
        .await
        .unwrap();
    client
        .controller_client()
        .unwrap()
        .create_topic(&topic, 1, 1, 5_000)
        .await
        .unwrap();
    let partition = client
        .partition_client(&topic, 0, UnknownTopicHandling::Retry)
        .await
        .unwrap();
    let record = |n: i32| Record {
        key: None,
        value: Some(json!({"n": n}).to_string().into_bytes()),
        headers: std::collections::BTreeMap::default(),
        timestamp: chrono::Utc::now(),
    };
    partition
        .produce(vec![record(1), record(2)], Compression::NoCompression)
        .await
        .unwrap();

    let (storage, trigger) = setup(
        TriggerType::Kafka,
        "kafka-t",
        json!({"brokers": [brokers], "topic": topic, "start_from": "earliest", "max_wait_ms": 100}),
    )
    .await;
    let cancel = CancellationToken::new();
    let task = tokio::spawn(orch8_engine::trigger_sources::kafka::run(
        Arc::clone(&storage),
        trigger.clone(),
        cancel.clone(),
    ));
    assert_eq!(wait_for(&storage, 2, 15).await.len(), 2);
    cancel.cancel();
    task.await.unwrap().unwrap();

    partition
        .produce(vec![record(3)], Compression::NoCompression)
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(orch8_engine::trigger_sources::kafka::run(
        Arc::clone(&storage),
        trigger,
        cancel.clone(),
    ));
    let got = wait_for(&storage, 3, 15).await;
    assert_eq!(
        got.len(),
        3,
        "resumes at the persisted offset without replays"
    );
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[cfg(feature = "sqs")]
#[tokio::test]
#[ignore = "needs SQS or LocalStack/ElasticMQ: set ORCH8_TEST_SQS_QUEUE_URL and AWS_* credentials"]
async fn sqs_delivers_and_deletes_messages() {
    use orch8_engine::trigger_sources::sqs::{SigV4Request, SqsConfig, sigv4_authorization};
    let queue_url = std::env::var("ORCH8_TEST_SQS_QUEUE_URL").expect("ORCH8_TEST_SQS_QUEUE_URL");
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let config = json!({"queue_url": queue_url, "region": region, "wait_time_seconds": 1});
    let cfg = SqsConfig::parse(&config).unwrap();
    let creds = cfg.effective_credentials().expect("AWS_* credentials");

    // SendMessage with the same signer the listener uses.
    let body =
        serde_json::to_vec(&json!({"QueueUrl": queue_url, "MessageBody": "{\"k\": 1}"})).unwrap();
    let now = chrono::Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let headers = [
        ("content-type", "application/x-amz-json-1.0"),
        ("host", cfg.host.as_str()),
        ("x-amz-date", amz_date.as_str()),
        ("x-amz-target", "AmazonSQS.SendMessage"),
    ];
    let auth = sigv4_authorization(
        &SigV4Request {
            method: "POST",
            path: "/",
            query: "",
            headers: &headers,
            body: &body,
            region: &cfg.region,
            service: "sqs",
            time: now,
        },
        &creds,
    );
    reqwest::Client::new()
        .post(format!("{}/", cfg.endpoint))
        .header("content-type", "application/x-amz-json-1.0")
        .header("x-amz-date", &amz_date)
        .header("x-amz-target", "AmazonSQS.SendMessage")
        .header("authorization", auth)
        .body(body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let (storage, trigger) = setup(TriggerType::Sqs, "sqs-t", config).await;
    let cancel = CancellationToken::new();
    let task = tokio::spawn(orch8_engine::trigger_sources::sqs::run(
        Arc::clone(&storage),
        trigger,
        cancel.clone(),
    ));
    let got = wait_for(&storage, 1, 15).await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].context.data, json!({"k": 1}));
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[cfg(feature = "pubsub")]
#[tokio::test]
#[ignore = "needs the Pub/Sub emulator: set PUBSUB_EMULATOR_HOST and ORCH8_TEST_PUBSUB_PROJECT"]
async fn pubsub_emulator_delivers_and_acks() {
    use base64::Engine as _;
    let host = std::env::var("PUBSUB_EMULATOR_HOST").expect("PUBSUB_EMULATOR_HOST");
    let project = std::env::var("ORCH8_TEST_PUBSUB_PROJECT").unwrap_or_else(|_| "test".into());
    let base = format!("http://{host}/v1/projects/{project}");
    let topic = unique("t");
    let sub = unique("s");
    let http = reqwest::Client::new();
    http.put(format!("{base}/topics/{topic}"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    http.put(format!("{base}/subscriptions/{sub}"))
        .json(&json!({"topic": format!("projects/{project}/topics/{topic}")}))
        .send()
        .await
        .unwrap();
    let data = base64::engine::general_purpose::STANDARD.encode(br#"{"p": 1}"#);
    http.post(format!("{base}/topics/{topic}:publish"))
        .json(&json!({"messages": [{"data": data, "attributes": {"a": "b"}}]}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let (storage, trigger) = setup(
        TriggerType::PubSub,
        "pubsub-t",
        json!({"subscription": format!("projects/{project}/subscriptions/{sub}"), "endpoint": format!("http://{host}")}),
    )
    .await;
    let cancel = CancellationToken::new();
    let task = tokio::spawn(orch8_engine::trigger_sources::pubsub::run(
        Arc::clone(&storage),
        trigger,
        cancel.clone(),
    ));
    let got = wait_for(&storage, 1, 15).await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].context.data, json!({"p": 1}));
    assert_eq!(got[0].metadata["_trigger_event"]["attributes"]["a"], "b");
    cancel.cancel();
    task.await.unwrap().unwrap();
}
