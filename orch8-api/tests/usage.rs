//! E2E tests for the Usage API cost estimates.
//!
//! Records usage events directly through the storage handle (the same path
//! `llm_call`/`agent` use in production) and asserts the `GET /usage`
//! response carries per-entry `cost_usd`, a window-wide `total_cost_usd` and
//! the `cost_is_estimate` marker.

use chrono::Utc;
use orch8_api::test_harness::spawn_test_server;
use orch8_storage::{TelemetryStore, UsageEvent};
use reqwest::StatusCode;

fn usage_event(tenant: &str, model: &str, input_tokens: i64, output_tokens: i64) -> UsageEvent {
    UsageEvent {
        tenant_id: tenant.into(),
        instance_id: None,
        block_id: Some("llm".into()),
        kind: "llm_tokens".into(),
        model: model.into(),
        input_tokens,
        output_tokens,
        created_at: Utc::now(),
    }
}

#[tokio::test]
async fn get_usage_includes_cost_estimates() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    // gpt-4o: 1M in + 1M out = $2.50 + $10.00 = $12.50 (two events).
    srv.storage
        .record_usage_event(&usage_event("t1", "gpt-4o", 600_000, 400_000))
        .await
        .unwrap();
    srv.storage
        .record_usage_event(&usage_event("t1", "gpt-4o", 400_000, 600_000))
        .await
        .unwrap();
    // Versioned model name resolves by prefix: claude-sonnet-4-6 →
    // claude-sonnet-4 at $3/$15 per 1M → 0.5M/0.1M = $1.50 + $1.50 = $3.00.
    srv.storage
        .record_usage_event(&usage_event("t1", "claude-sonnet-4-6", 500_000, 100_000))
        .await
        .unwrap();
    // Unknown model → cost_usd null, excluded from the total.
    srv.storage
        .record_usage_event(&usage_event("t1", "mystery-model", 1_000_000, 1_000_000))
        .await
        .unwrap();
    // Another tenant — must not leak into t1's report.
    srv.storage
        .record_usage_event(&usage_event("t2", "gpt-4o", 999_999, 999_999))
        .await
        .unwrap();

    let resp = client
        .get(format!("{}/usage", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(body["tenant"], "t1");
    assert_eq!(body["cost_is_estimate"], true);

    let usage = body["usage"].as_array().unwrap();
    assert_eq!(usage.len(), 3, "grouped by (kind, model)");

    let entry = |model: &str| {
        usage
            .iter()
            .find(|u| u["model"] == model)
            .unwrap_or_else(|| panic!("no usage entry for {model}"))
    };

    let gpt = entry("gpt-4o");
    assert_eq!(gpt["events"], 2);
    assert_eq!(gpt["input_tokens"], 1_000_000);
    assert_eq!(gpt["output_tokens"], 1_000_000);
    assert_eq!(gpt["cost_usd"], 12.5);

    let claude = entry("claude-sonnet-4-6");
    assert_eq!(claude["cost_usd"], 3.0);

    let unknown = entry("mystery-model");
    assert!(unknown["cost_usd"].is_null(), "unknown model → null cost");

    // Total covers only the priceable entries: 12.50 + 3.00.
    assert_eq!(body["total_cost_usd"], 15.5);
}

#[tokio::test]
async fn get_usage_empty_window_has_zero_total() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/usage", srv.v1_url()))
        .header("X-Tenant-Id", "t-empty")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(body["usage"].as_array().unwrap().len(), 0);
    assert_eq!(body["total_cost_usd"], 0.0);
    assert_eq!(body["cost_is_estimate"], true);
}
