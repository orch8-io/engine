//! Functional tests for error-budget auto-rollback logic.
//!
//! Verifies that rollback checks respect thresholds, cooldown hysteresis, and
//! handle concurrent error ingestion correctly.

use orch8_api::test_harness::spawn_test_server;
use orch8_storage::AdminStore;

const TENANT: &str = "default";

/// Helper: create a rollback policy for a given sequence under the default tenant.
async fn create_policy(
    client: &reqwest::Client,
    base_url: &str,
    seq: &str,
    error_rate_threshold: f64,
    time_window_secs: u32,
) {
    let resp = client
        .post(format!("{base_url}/rollback-policies"))
        .json(&serde_json::json!({
            "sequence_name": seq,
            "error_rate_threshold": error_rate_threshold,
            "time_window_secs": time_window_secs
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "policy creation failed for {seq}");
}

/// Helper: ingest a batch of success events.
async fn ingest_success_batch(client: &reqwest::Client, base_url: &str, seq: &str, count: usize) {
    let events: Vec<serde_json::Value> = (0..count)
        .map(|i| {
            serde_json::json!({
                "event_type": "InstanceCompleted",
                "payload": serde_json::json!({"sequence_name": seq}).to_string(),
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "device": {
                    "device_id": format!("d-{i}"),
                    "os_name": "iOS",
                    "os_version": "17",
                    "app_version": "1.0",
                    "sdk_version": "0.1"
                }
            })
        })
        .collect();
    let resp = client
        .post(format!("{base_url}/telemetry/mobile"))
        .json(&serde_json::json!({ "events": events }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["accepted"], count, "not all events were accepted");
}

/// Helper: ingest a single error event.
async fn ingest_error(client: &reqwest::Client, base_url: &str, seq: &str, msg: &str) {
    let resp = client
        .post(format!("{base_url}/telemetry/mobile/errors"))
        .json(&serde_json::json!({
            "error_type": "RuntimeError",
            "message": msg,
            "device": {
                "device_id": "d-err",
                "os_name": "iOS",
                "os_version": "17",
                "app_version": "1.0",
                "sdk_version": "0.1"
            },
            "sequence_name": seq
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
}

#[tokio::test]
async fn error_rate_below_threshold_does_not_trigger_rollback() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq = "below-threshold-seq";

    create_policy(&client, &server.base_url, seq, 0.1, 300).await;

    // 100 successes + 5 errors = 5% error rate (below 10% threshold).
    ingest_success_batch(&client, &server.base_url, seq, 100).await;
    for i in 0..5 {
        ingest_error(&client, &server.base_url, seq, &format!("error {i}")).await;
    }

    // Verify error rate was computed correctly.
    let rate = server
        .storage
        .query_error_rate(TENANT, seq, 300)
        .await
        .unwrap();
    assert!(
        rate.is_some(),
        "error rate should be computable with events present"
    );
    let rate = rate.unwrap();
    assert!(
        rate < 0.1,
        "error rate {rate} should be below 0.1 threshold"
    );

    let history = server
        .storage
        .list_rollback_history(None, None, 100)
        .await
        .unwrap();
    assert!(
        history.is_empty(),
        "rollback should NOT trigger at {rate:.1}% error rate (threshold 10%)"
    );
}

#[tokio::test]
async fn cooldown_prevents_duplicate_rollback_triggers() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq = "cooldown-seq";

    // 0% threshold = any error triggers; default cooldown prevents re-trigger.
    create_policy(&client, &server.base_url, seq, 0.0, 3600).await;

    // First error triggers rollback.
    ingest_error(&client, &server.base_url, seq, "first error").await;

    let history = server
        .storage
        .list_rollback_history(None, None, 100)
        .await
        .unwrap();
    assert_eq!(history.len(), 1, "first error should trigger rollback");

    // Second error immediately after — cooldown should prevent re-trigger.
    ingest_error(&client, &server.base_url, seq, "second error").await;

    let history = server
        .storage
        .list_rollback_history(None, None, 100)
        .await
        .unwrap();
    assert_eq!(
        history.len(),
        1,
        "second error within cooldown must NOT trigger another rollback"
    );
}

#[tokio::test]
async fn sequential_errors_above_threshold_trigger_single_rollback() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq = "threshold-breach-seq";

    // 50% threshold.
    create_policy(&client, &server.base_url, seq, 0.5, 3600).await;

    // Ingest 500 success events as baseline.
    ingest_success_batch(&client, &server.base_url, seq, 500).await;

    // Ingest enough errors to breach the 50% threshold.
    // 500 successes + 600 errors = ~54.5% error rate.
    for i in 0..600 {
        ingest_error(&client, &server.base_url, seq, &format!("error {i}")).await;
    }

    // Verify error rate is above threshold.
    let rate = server
        .storage
        .query_error_rate(TENANT, seq, 3600)
        .await
        .unwrap();
    assert!(
        rate.is_some_and(|r| r >= 0.5),
        "error rate should be >= 50% after 600 errors on 500 successes, got {rate:?}"
    );

    // Exactly one rollback: first error that breaches threshold triggers it,
    // cooldown prevents all subsequent errors from triggering again.
    let history = server
        .storage
        .list_rollback_history(None, None, 100)
        .await
        .unwrap();
    assert_eq!(
        history.len(),
        1,
        "exactly one rollback should trigger despite 600 errors (cooldown)"
    );
}
