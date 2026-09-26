//! `POST /triggers` validates message-source trigger configs.

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::json;

#[tokio::test]
async fn message_source_trigger_configs_are_validated_on_create() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    client
        .post(format!("{base}/sequences"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "id": uuid::Uuid::now_v7(), "created_at": chrono::Utc::now().to_rfc3339(),
            "tenant_id": "t1", "namespace": "default", "name": "on-msg", "version": 1,
            "blocks": [{"type": "step", "id": "s1", "handler": "noop", "params": {}}]
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let cases = [
        (
            "kafka",
            json!({"topic": "t"}),
            json!({"brokers": ["k:9092"], "topic": "orders"}),
        ),
        (
            "sqs",
            json!({"queue_url": "nope"}),
            json!({"queue_url": "https://sqs.us-east-1.amazonaws.com/1/q"}),
        ),
        (
            "pubsub",
            json!({"subscription": "s"}),
            json!({"subscription": "projects/p/subscriptions/s"}),
        ),
        (
            "redis_streams",
            json!({"url": "http://x", "stream": "s"}),
            json!({"url": "redis://localhost:6379", "stream": "orders"}),
        ),
        (
            "postgres_rows",
            json!({"database_url": "postgres://x", "table": "bad name"}),
            json!({"database_url": "postgres://u@h/db", "table": "public.orders", "events": ["insert"]}),
        ),
    ];
    for (i, (kind, bad, good)) in cases.into_iter().enumerate() {
        let resp = client
            .post(format!("{base}/triggers"))
            .header("X-Tenant-Id", "t1")
            .json(&json!({
                "slug": format!("bad-{i}"), "sequence_name": "on-msg", "tenant_id": "t1",
                "trigger_type": kind, "config": bad
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{kind} bad config");

        let resp = client
            .post(format!("{base}/triggers"))
            .header("X-Tenant-Id", "t1")
            .json(&json!({
                "slug": format!("good-{i}"), "sequence_name": "on-msg", "tenant_id": "t1",
                "trigger_type": kind, "config": good
            }))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        assert_eq!(status, StatusCode::CREATED, "{kind} good config: {body}");
        assert_eq!(body["trigger_type"], kind);
    }
}
