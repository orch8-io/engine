//! Extensive e2e tests for the webhook delivery inspector:
//! `GET /webhooks/deliveries` (summaries + filters + limit),
//! `GET /webhooks/deliveries/{id}` (attempt timeline),
//! `GET /webhooks/outbox/{id}/redeliver-preview`, and the existing
//! outbox list/discard endpoints.

use chrono::{DateTime, Duration, Utc};
use orch8_api::test_harness::{TestServer, spawn_test_server};
use orch8_storage::WorkerStore;
use orch8_types::webhook_delivery::{DeliveryErrorClass, WebhookDeliveryAttempt};
use orch8_types::webhook_outbox::WebhookOutboxEntry;
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

const DEFAULT_URL: &str = "https://hooks.example.com/a";
const DEFAULT_EVENT: &str = "instance.failed";

fn mk_attempt(delivery: Uuid, n: i32, success: bool, at: DateTime<Utc>) -> WebhookDeliveryAttempt {
    WebhookDeliveryAttempt {
        id: Uuid::now_v7(),
        delivery_id: delivery,
        url: DEFAULT_URL.into(),
        event_type: DEFAULT_EVENT.into(),
        instance_id: None,
        attempt_number: n,
        attempted_at: at,
        duration_ms: 10,
        success,
        status_code: None,
        error_class: None,
        error_excerpt: None,
        signed: false,
    }
}

/// A failed attempt classified as `http 503`.
fn failed_attempt(delivery: Uuid, n: i32, at: DateTime<Utc>) -> WebhookDeliveryAttempt {
    let mut a = mk_attempt(delivery, n, false, at);
    a.status_code = Some(503);
    a.set_error("http 503", Some(503));
    a
}

async fn record(srv: &TestServer, attempt: &WebhookDeliveryAttempt) {
    srv.storage.record_webhook_attempt(attempt).await.unwrap();
}

/// Record a whole delivery group: `fails` failed attempts, optionally
/// followed by a success. Returns the delivery id.
async fn record_group(srv: &TestServer, base: DateTime<Utc>, fails: i32, then_success: bool) -> Uuid {
    let delivery = Uuid::now_v7();
    for n in 1..=fails {
        record(srv, &failed_attempt(delivery, n, base + Duration::seconds(i64::from(n)))).await;
    }
    if then_success {
        let n = fails + 1;
        record(srv, &mk_attempt(delivery, n, true, base + Duration::seconds(i64::from(n)))).await;
    }
    delivery
}

async fn list_deliveries(srv: &TestServer, query: &[(&str, &str)]) -> Vec<Value> {
    let resp = reqwest::Client::new()
        .get(format!("{}/webhooks/deliveries", srv.v1_url()))
        .query(query)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json::<Value>().await.unwrap().as_array().unwrap().clone()
}

async fn deliveries_status(srv: &TestServer, query: &[(&str, &str)]) -> StatusCode {
    reqwest::Client::new()
        .get(format!("{}/webhooks/deliveries", srv.v1_url()))
        .query(query)
        .send()
        .await
        .unwrap()
        .status()
}

async fn get_timeline(srv: &TestServer, delivery: Uuid) -> Vec<Value> {
    let resp = reqwest::Client::new()
        .get(format!("{}/webhooks/deliveries/{delivery}", srv.v1_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json::<Value>().await.unwrap().as_array().unwrap().clone()
}

fn mk_parked(url: &str, delivery_id: Option<Uuid>) -> WebhookOutboxEntry {
    WebhookOutboxEntry {
        id: Uuid::now_v7(),
        url: url.into(),
        event_type: DEFAULT_EVENT.into(),
        instance_id: Some(Uuid::now_v7()),
        payload: json!({
            "event_type": DEFAULT_EVENT,
            "timestamp": "2026-07-01T00:00:00Z",
            "data": {"reason": "boom"}
        }),
        attempts: 4,
        last_error: Some("http 503".into()),
        created_at: Utc::now(),
        delivery_id,
    }
}

async fn get_preview(srv: &TestServer, id: Uuid) -> (StatusCode, Value) {
    let resp = reqwest::Client::new()
        .get(format!("{}/webhooks/outbox/{id}/redeliver-preview", srv.v1_url()))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.json::<Value>().await.unwrap_or(Value::Null);
    (status, body)
}

// ---------------------------------------------------------------------------
// Summary aggregation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn empty_delivery_list_returns_empty_array() {
    let srv = spawn_test_server().await;
    assert!(list_deliveries(&srv, &[]).await.is_empty());
}

#[tokio::test]
async fn single_failed_attempt_summary_fields() {
    let srv = spawn_test_server().await;
    let delivery = record_group(&srv, Utc::now(), 1, false).await;

    let rows = list_deliveries(&srv, &[]).await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["delivery_id"], delivery.to_string().as_str());
    assert_eq!(row["url"], DEFAULT_URL);
    assert_eq!(row["event_type"], DEFAULT_EVENT);
    assert_eq!(row["attempts"], 1);
    assert_eq!(row["delivered"], false);
    assert_eq!(row["last_error_class"], "http_status");
}

#[tokio::test]
async fn single_success_summary_has_no_error_class() {
    let srv = spawn_test_server().await;
    record_group(&srv, Utc::now(), 0, true).await;

    let rows = list_deliveries(&srv, &[]).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["delivered"], true);
    assert!(
        rows[0].get("last_error_class").is_none(),
        "successful delivery must omit last_error_class"
    );
}

#[tokio::test]
async fn retries_then_success_aggregate_into_one_summary() {
    let srv = spawn_test_server().await;
    record_group(&srv, Utc::now(), 2, true).await;

    let rows = list_deliveries(&srv, &[]).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["attempts"], 3);
    assert_eq!(rows[0]["delivered"], true);
}

#[tokio::test]
async fn exhausted_retries_summary_uses_final_attempt_class() {
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    let base = Utc::now();

    let mut first = mk_attempt(delivery, 1, false, base);
    first.set_error("connection refused", None);
    record(&srv, &first).await;
    let mut second = mk_attempt(delivery, 2, false, base + Duration::seconds(1));
    second.set_error("operation timed out", None);
    record(&srv, &second).await;

    let rows = list_deliveries(&srv, &[]).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["attempts"], 2);
    assert_eq!(rows[0]["delivered"], false);
    // The class of the *final* attempt (attempt_number 2) wins.
    assert_eq!(rows[0]["last_error_class"], "timeout");
}

#[tokio::test]
async fn delivered_is_true_if_any_attempt_succeeded_even_mid_group() {
    // Documents aggregation semantics: delivered = MAX(success) over the
    // group, while last_error_class follows the final attempt.
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    let base = Utc::now();
    record(&srv, &mk_attempt(delivery, 1, true, base)).await;
    record(&srv, &failed_attempt(delivery, 2, base + Duration::seconds(1))).await;

    let rows = list_deliveries(&srv, &[]).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["delivered"], true);
    assert_eq!(rows[0]["last_error_class"], "http_status");
}

#[tokio::test]
async fn summary_first_and_last_timestamps_span_the_group() {
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    let base = Utc::now();
    let first_at = base;
    let last_at = base + Duration::seconds(30);
    record(&srv, &failed_attempt(delivery, 1, first_at)).await;
    record(&srv, &failed_attempt(delivery, 2, base + Duration::seconds(10))).await;
    record(&srv, &failed_attempt(delivery, 3, last_at)).await;

    let rows = list_deliveries(&srv, &[]).await;
    let parse = |v: &Value| {
        DateTime::parse_from_rfc3339(v.as_str().unwrap())
            .unwrap()
            .with_timezone(&Utc)
    };
    let got_first = parse(&rows[0]["first_attempt_at"]);
    let got_last = parse(&rows[0]["last_attempt_at"]);
    assert_eq!(got_first, first_at);
    assert_eq!(got_last, last_at);
    assert!(got_first < got_last);
}

#[tokio::test]
async fn one_summary_per_delivery_group_with_correct_counts() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    let g1 = record_group(&srv, base, 1, false).await;
    let g2 = record_group(&srv, base + Duration::seconds(100), 3, false).await;
    let g3 = record_group(&srv, base + Duration::seconds(200), 2, true).await;

    let rows = list_deliveries(&srv, &[]).await;
    assert_eq!(rows.len(), 3);
    let by_id = |id: Uuid| {
        rows.iter()
            .find(|r| r["delivery_id"] == id.to_string().as_str())
            .unwrap()
            .clone()
    };
    assert_eq!(by_id(g1)["attempts"], 1);
    assert_eq!(by_id(g2)["attempts"], 3);
    assert_eq!(by_id(g3)["attempts"], 3); // 2 failures + 1 success
    assert_eq!(by_id(g3)["delivered"], true);
}

#[tokio::test]
async fn summaries_are_ordered_newest_first() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    // Record in a scrambled order; ordering must follow last attempt time.
    let middle = record_group(&srv, base + Duration::seconds(100), 1, false).await;
    let newest = record_group(&srv, base + Duration::seconds(200), 1, false).await;
    let oldest = record_group(&srv, base, 1, false).await;

    let rows = list_deliveries(&srv, &[]).await;
    let ids: Vec<String> = rows
        .iter()
        .map(|r| r["delivery_id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        ids,
        vec![newest.to_string(), middle.to_string(), oldest.to_string()]
    );
}

#[tokio::test]
async fn newest_first_uses_last_attempt_not_first() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    // Group A starts first but its retry lands last.
    let a = Uuid::now_v7();
    record(&srv, &failed_attempt(a, 1, base)).await;
    record(&srv, &failed_attempt(a, 2, base + Duration::seconds(300))).await;
    // Group B lives entirely in between.
    let b = Uuid::now_v7();
    record(&srv, &failed_attempt(b, 1, base + Duration::seconds(100))).await;

    let rows = list_deliveries(&srv, &[]).await;
    assert_eq!(rows[0]["delivery_id"], a.to_string().as_str());
    assert_eq!(rows[1]["delivery_id"], b.to_string().as_str());
}

#[tokio::test]
async fn summaries_never_leak_payload_or_error_excerpt() {
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    let mut a = mk_attempt(delivery, 1, false, Utc::now());
    a.set_error("SENTINEL-EXCERPT-DO-NOT-LEAK", None);
    record(&srv, &a).await;

    let resp = reqwest::Client::new()
        .get(format!("{}/webhooks/deliveries", srv.v1_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = resp.text().await.unwrap();
    assert!(
        !text.contains("SENTINEL-EXCERPT-DO-NOT-LEAK"),
        "summary list must not include attempt error excerpts"
    );
    let rows: Vec<Value> = serde_json::from_str(&text).unwrap();
    for row in &rows {
        let obj = row.as_object().unwrap();
        assert!(!obj.contains_key("payload"));
        assert!(!obj.contains_key("error_excerpt"));
    }
}

// ---------------------------------------------------------------------------
// Filters: url / event_type / delivered
// ---------------------------------------------------------------------------

#[tokio::test]
async fn filter_by_url_matches_exact_url() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    let target = Uuid::now_v7();
    let mut a = failed_attempt(target, 1, base);
    a.url = "https://hooks.example.com/special".into();
    record(&srv, &a).await;
    record_group(&srv, base + Duration::seconds(10), 1, false).await;

    let rows = list_deliveries(&srv, &[("url", "https://hooks.example.com/special")]).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["delivery_id"], target.to_string().as_str());
}

#[tokio::test]
async fn filter_by_url_without_match_returns_empty() {
    let srv = spawn_test_server().await;
    record_group(&srv, Utc::now(), 1, false).await;
    let rows = list_deliveries(&srv, &[("url", "https://nowhere.example.com/x")]).await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn filter_by_url_is_exact_not_substring() {
    let srv = spawn_test_server().await;
    record_group(&srv, Utc::now(), 1, false).await; // url = DEFAULT_URL
    let rows = list_deliveries(&srv, &[("url", "hooks.example.com")]).await;
    assert!(rows.is_empty(), "substring of the URL must not match");
}

#[tokio::test]
async fn filter_by_event_type_matches() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    let target = Uuid::now_v7();
    let mut a = failed_attempt(target, 1, base);
    a.event_type = "instance.completed".into();
    record(&srv, &a).await;
    record_group(&srv, base + Duration::seconds(10), 1, false).await;

    let rows = list_deliveries(&srv, &[("event_type", "instance.completed")]).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["delivery_id"], target.to_string().as_str());
    assert_eq!(rows[0]["event_type"], "instance.completed");
}

#[tokio::test]
async fn filter_by_event_type_without_match_returns_empty() {
    let srv = spawn_test_server().await;
    record_group(&srv, Utc::now(), 1, false).await;
    let rows = list_deliveries(&srv, &[("event_type", "instance.started")]).await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn filter_delivered_true_returns_only_delivered() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    let ok = record_group(&srv, base, 0, true).await;
    record_group(&srv, base + Duration::seconds(10), 2, false).await;

    let rows = list_deliveries(&srv, &[("delivered", "true")]).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["delivery_id"], ok.to_string().as_str());
    assert_eq!(rows[0]["delivered"], true);
}

#[tokio::test]
async fn filter_delivered_false_returns_only_failed() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    record_group(&srv, base, 0, true).await;
    let failed = record_group(&srv, base + Duration::seconds(10), 2, false).await;

    let rows = list_deliveries(&srv, &[("delivered", "false")]).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["delivery_id"], failed.to_string().as_str());
    assert_eq!(rows[0]["delivered"], false);
}

#[tokio::test]
async fn filter_delivered_splits_mixed_population() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    for i in 0..3 {
        record_group(&srv, base + Duration::seconds(i * 10), 0, true).await;
    }
    for i in 3..5 {
        record_group(&srv, base + Duration::seconds(i * 10), 1, false).await;
    }
    assert_eq!(list_deliveries(&srv, &[("delivered", "true")]).await.len(), 3);
    assert_eq!(list_deliveries(&srv, &[("delivered", "false")]).await.len(), 2);
    assert_eq!(list_deliveries(&srv, &[]).await.len(), 5);
}

// ---------------------------------------------------------------------------
// Filters: error_class (every value)
// ---------------------------------------------------------------------------

/// One test per error class value: only the group whose *final* attempt
/// carries the class is returned.
macro_rules! filter_by_error_class {
    ($name:ident, $class:ident, $decoy:ident) => {
        #[tokio::test]
        async fn $name() {
            let srv = spawn_test_server().await;
            let base = Utc::now();

            let target = Uuid::now_v7();
            let mut a = mk_attempt(target, 1, false, base);
            a.error_class = Some(DeliveryErrorClass::$class);
            a.error_excerpt = Some("boom".into());
            record(&srv, &a).await;

            let decoy = Uuid::now_v7();
            let mut b = mk_attempt(decoy, 1, false, base + Duration::seconds(1));
            b.error_class = Some(DeliveryErrorClass::$decoy);
            b.error_excerpt = Some("boom".into());
            record(&srv, &b).await;

            let label = DeliveryErrorClass::$class.as_str();
            let rows = list_deliveries(&srv, &[("error_class", label)]).await;
            assert_eq!(rows.len(), 1, "filter error_class={label}");
            assert_eq!(rows[0]["delivery_id"], target.to_string().as_str());
            assert_eq!(rows[0]["last_error_class"], label);
        }
    };
}

filter_by_error_class!(filter_error_class_dns, Dns, Other);
filter_by_error_class!(filter_error_class_connect, Connect, Dns);
filter_by_error_class!(filter_error_class_tls, Tls, Dns);
filter_by_error_class!(filter_error_class_timeout, Timeout, Dns);
filter_by_error_class!(filter_error_class_redirect_rejected, RedirectRejected, Dns);
filter_by_error_class!(filter_error_class_http_status, HttpStatus, Dns);
filter_by_error_class!(
    filter_error_class_signature_configuration,
    SignatureConfiguration,
    Dns
);
filter_by_error_class!(filter_error_class_response_too_large, ResponseTooLarge, Dns);
filter_by_error_class!(filter_error_class_internal_policy, InternalPolicy, Dns);
filter_by_error_class!(filter_error_class_serialization, Serialization, Dns);
filter_by_error_class!(filter_error_class_aborted, Aborted, Dns);
filter_by_error_class!(filter_error_class_other, Other, Dns);

#[tokio::test]
async fn filter_error_class_uses_final_attempt_class() {
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    let base = Utc::now();
    let mut first = mk_attempt(delivery, 1, false, base);
    first.set_error("connection refused", None);
    record(&srv, &first).await;
    let mut second = mk_attempt(delivery, 2, false, base + Duration::seconds(1));
    second.set_error("operation timed out", None);
    record(&srv, &second).await;

    // The group's final class is timeout, so a connect filter misses it.
    assert!(list_deliveries(&srv, &[("error_class", "connect")]).await.is_empty());
    assert_eq!(list_deliveries(&srv, &[("error_class", "timeout")]).await.len(), 1);
}

#[tokio::test]
async fn filter_error_class_excludes_delivered_groups() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    record_group(&srv, base, 0, true).await; // no error class at all
    let failed = Uuid::now_v7();
    let mut a = mk_attempt(failed, 1, false, base + Duration::seconds(1));
    a.set_error("operation timed out", None);
    record(&srv, &a).await;

    let rows = list_deliveries(&srv, &[("error_class", "timeout")]).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["delivery_id"], failed.to_string().as_str());
}

// ---------------------------------------------------------------------------
// Combined filters
// ---------------------------------------------------------------------------

#[tokio::test]
async fn combined_url_and_event_type_filters() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    let target = Uuid::now_v7();
    let mut a = failed_attempt(target, 1, base);
    a.url = "https://combo.example.com/hook".into();
    a.event_type = "instance.completed".into();
    record(&srv, &a).await;
    // Same URL, different event.
    let mut b = failed_attempt(Uuid::now_v7(), 1, base + Duration::seconds(1));
    b.url = "https://combo.example.com/hook".into();
    record(&srv, &b).await;
    // Same event, different URL.
    let mut c = failed_attempt(Uuid::now_v7(), 1, base + Duration::seconds(2));
    c.event_type = "instance.completed".into();
    record(&srv, &c).await;

    let rows = list_deliveries(
        &srv,
        &[
            ("url", "https://combo.example.com/hook"),
            ("event_type", "instance.completed"),
        ],
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["delivery_id"], target.to_string().as_str());
}

#[tokio::test]
async fn combined_url_and_delivered_filters() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    let url = "https://combo2.example.com/hook";
    // Delivered group on the target URL.
    let ok = Uuid::now_v7();
    let mut a = mk_attempt(ok, 1, true, base);
    a.url = url.into();
    record(&srv, &a).await;
    // Failed group on the target URL.
    let mut b = failed_attempt(Uuid::now_v7(), 1, base + Duration::seconds(1));
    b.url = url.into();
    record(&srv, &b).await;
    // Delivered group elsewhere.
    record_group(&srv, base + Duration::seconds(2), 0, true).await;

    let rows = list_deliveries(&srv, &[("url", url), ("delivered", "true")]).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["delivery_id"], ok.to_string().as_str());
}

#[tokio::test]
async fn combined_event_type_and_error_class_filters() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    let target = Uuid::now_v7();
    let mut a = mk_attempt(target, 1, false, base);
    a.event_type = "instance.completed".into();
    a.set_error("tls handshake eof", None);
    record(&srv, &a).await;
    // Same class, wrong event.
    let mut b = mk_attempt(Uuid::now_v7(), 1, false, base + Duration::seconds(1));
    b.set_error("tls handshake eof", None);
    record(&srv, &b).await;
    // Same event, wrong class.
    let mut c = mk_attempt(Uuid::now_v7(), 1, false, base + Duration::seconds(2));
    c.event_type = "instance.completed".into();
    c.set_error("operation timed out", None);
    record(&srv, &c).await;

    let rows = list_deliveries(
        &srv,
        &[("event_type", "instance.completed"), ("error_class", "tls")],
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["delivery_id"], target.to_string().as_str());
}

#[tokio::test]
async fn combined_all_four_filters() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    let url = "https://all.example.com/hook";
    let target = Uuid::now_v7();
    let mut a = mk_attempt(target, 1, false, base);
    a.url = url.into();
    a.event_type = "instance.completed".into();
    a.set_error("dns error", None);
    record(&srv, &a).await;
    // Decoys, each off by one dimension.
    let mut b = mk_attempt(Uuid::now_v7(), 1, false, base + Duration::seconds(1));
    b.url = url.into();
    b.event_type = "instance.completed".into();
    b.set_error("operation timed out", None); // wrong class
    record(&srv, &b).await;
    let mut c = mk_attempt(Uuid::now_v7(), 1, true, base + Duration::seconds(2));
    c.url = url.into();
    c.event_type = "instance.completed".into(); // delivered
    record(&srv, &c).await;

    let rows = list_deliveries(
        &srv,
        &[
            ("url", url),
            ("event_type", "instance.completed"),
            ("delivered", "false"),
            ("error_class", "dns"),
        ],
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["delivery_id"], target.to_string().as_str());
}

#[tokio::test]
async fn mismatched_filter_combination_returns_empty() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    // Group 1: special URL, default event.
    let mut a = failed_attempt(Uuid::now_v7(), 1, base);
    a.url = "https://only-here.example.com/hook".into();
    record(&srv, &a).await;
    // Group 2: default URL, special event.
    let mut b = failed_attempt(Uuid::now_v7(), 1, base + Duration::seconds(1));
    b.event_type = "instance.completed".into();
    record(&srv, &b).await;

    // Cross-product that exists in no single group.
    let rows = list_deliveries(
        &srv,
        &[
            ("url", "https://only-here.example.com/hook"),
            ("event_type", "instance.completed"),
        ],
    )
    .await;
    assert!(rows.is_empty());
}

// ---------------------------------------------------------------------------
// Limit handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn limit_zero_clamps_to_one_newest() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    record_group(&srv, base, 1, false).await;
    record_group(&srv, base + Duration::seconds(10), 1, false).await;
    let newest = record_group(&srv, base + Duration::seconds(20), 1, false).await;

    let rows = list_deliveries(&srv, &[("limit", "0")]).await;
    assert_eq!(rows.len(), 1, "limit=0 must clamp to 1");
    assert_eq!(rows[0]["delivery_id"], newest.to_string().as_str());
}

#[tokio::test]
async fn limit_one_returns_single_newest() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    record_group(&srv, base, 1, false).await;
    let newest = record_group(&srv, base + Duration::seconds(10), 1, false).await;

    let rows = list_deliveries(&srv, &[("limit", "1")]).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["delivery_id"], newest.to_string().as_str());
}

#[tokio::test]
async fn limit_truncates_to_newest_n() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    let oldest = record_group(&srv, base, 1, false).await;
    let mid = record_group(&srv, base + Duration::seconds(10), 1, false).await;
    let newest = record_group(&srv, base + Duration::seconds(20), 1, false).await;

    let rows = list_deliveries(&srv, &[("limit", "2")]).await;
    let ids: Vec<&str> = rows.iter().map(|r| r["delivery_id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec![newest.to_string(), mid.to_string()]);
    assert!(!ids.contains(&oldest.to_string().as_str()));
}

#[tokio::test]
async fn limit_above_max_clamps_to_1000_and_succeeds() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    for i in 0..3 {
        record_group(&srv, base + Duration::seconds(i * 10), 1, false).await;
    }
    // 4 billion clamps to 1000 server-side; the request must still succeed.
    let rows = list_deliveries(&srv, &[("limit", "4000000000")]).await;
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn limit_negative_is_bad_request() {
    let srv = spawn_test_server().await;
    assert_eq!(
        deliveries_status(&srv, &[("limit", "-1")]).await,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn limit_non_numeric_is_bad_request() {
    let srv = spawn_test_server().await;
    assert_eq!(
        deliveries_status(&srv, &[("limit", "many")]).await,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn default_limit_returns_all_small_populations() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    for i in 0..7 {
        record_group(&srv, base + Duration::seconds(i * 10), 1, false).await;
    }
    assert_eq!(list_deliveries(&srv, &[]).await.len(), 7);
}

// ---------------------------------------------------------------------------
// Invalid error_class values
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unknown_error_class_is_bad_request() {
    let srv = spawn_test_server().await;
    assert_eq!(
        deliveries_status(&srv, &[("error_class", "gremlins")]).await,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn wrong_case_error_class_is_bad_request() {
    let srv = spawn_test_server().await;
    assert_eq!(
        deliveries_status(&srv, &[("error_class", "HTTP_STATUS")]).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        deliveries_status(&srv, &[("error_class", "Timeout")]).await,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn empty_error_class_is_bad_request() {
    let srv = spawn_test_server().await;
    assert_eq!(
        deliveries_status(&srv, &[("error_class", "")]).await,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn every_valid_error_class_is_accepted() {
    let srv = spawn_test_server().await;
    for label in [
        "dns",
        "connect",
        "tls",
        "timeout",
        "redirect_rejected",
        "http_status",
        "signature_configuration",
        "response_too_large",
        "internal_policy",
        "serialization",
        "aborted",
        "other",
    ] {
        assert_eq!(
            deliveries_status(&srv, &[("error_class", label)]).await,
            StatusCode::OK,
            "error_class={label} must be accepted"
        );
    }
}

// ---------------------------------------------------------------------------
// Attempt timeline
// ---------------------------------------------------------------------------

#[tokio::test]
async fn timeline_orders_by_attempt_number_regardless_of_insert_order() {
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    let base = Utc::now();
    for n in [3, 1, 2] {
        record(
            &srv,
            &failed_attempt(delivery, n, base + Duration::seconds(i64::from(n))),
        )
        .await;
    }

    let attempts = get_timeline(&srv, delivery).await;
    let numbers: Vec<i64> = attempts.iter().map(|a| a["attempt_number"].as_i64().unwrap()).collect();
    assert_eq!(numbers, vec![1, 2, 3]);
}

#[tokio::test]
async fn timeline_ordering_ignores_attempted_at() {
    // Attempt 1 carries the *latest* wall-clock time; attempt_number still
    // dictates the timeline order.
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    let base = Utc::now();
    record(&srv, &failed_attempt(delivery, 1, base + Duration::seconds(100))).await;
    record(&srv, &failed_attempt(delivery, 2, base)).await;

    let attempts = get_timeline(&srv, delivery).await;
    assert_eq!(attempts[0]["attempt_number"], 1);
    assert_eq!(attempts[1]["attempt_number"], 2);
}

#[tokio::test]
async fn timeline_unknown_delivery_is_404() {
    let srv = spawn_test_server().await;
    let resp = reqwest::Client::new()
        .get(format!("{}/webhooks/deliveries/{}", srv.v1_url(), Uuid::now_v7()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn timeline_malformed_delivery_id_is_400() {
    let srv = spawn_test_server().await;
    let resp = reqwest::Client::new()
        .get(format!("{}/webhooks/deliveries/not-a-uuid", srv.v1_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn timeline_failed_http_attempt_has_full_error_fields() {
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    record(&srv, &failed_attempt(delivery, 1, Utc::now())).await;

    let attempts = get_timeline(&srv, delivery).await;
    assert_eq!(attempts.len(), 1);
    let a = &attempts[0];
    assert_eq!(a["success"], false);
    assert_eq!(a["status_code"], 503);
    assert_eq!(a["error_class"], "http_status");
    assert_eq!(a["error_excerpt"], "http 503");
}

#[tokio::test]
async fn timeline_transport_error_attempt_omits_status_code() {
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    let mut a = mk_attempt(delivery, 1, false, Utc::now());
    a.set_error("connection refused", None);
    record(&srv, &a).await;

    let attempts = get_timeline(&srv, delivery).await;
    let obj = attempts[0].as_object().unwrap();
    assert!(!obj.contains_key("status_code"), "transport errors carry no status");
    assert_eq!(attempts[0]["error_class"], "connect");
    assert_eq!(attempts[0]["error_excerpt"], "connection refused");
}

#[tokio::test]
async fn timeline_success_attempt_has_no_error_fields() {
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    record(&srv, &mk_attempt(delivery, 1, true, Utc::now())).await;

    let attempts = get_timeline(&srv, delivery).await;
    let obj = attempts[0].as_object().unwrap();
    assert_eq!(attempts[0]["success"], true);
    assert!(!obj.contains_key("error_class"));
    assert!(!obj.contains_key("error_excerpt"));
    assert!(!obj.contains_key("status_code"));
}

#[tokio::test]
async fn timeline_reports_signed_true() {
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    let mut a = mk_attempt(delivery, 1, true, Utc::now());
    a.signed = true;
    record(&srv, &a).await;

    let attempts = get_timeline(&srv, delivery).await;
    assert_eq!(attempts[0]["signed"], true);
}

#[tokio::test]
async fn timeline_reports_signed_false() {
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    record(&srv, &mk_attempt(delivery, 1, true, Utc::now())).await;

    let attempts = get_timeline(&srv, delivery).await;
    assert_eq!(attempts[0]["signed"], false);
}

#[tokio::test]
async fn timeline_preserves_per_attempt_durations() {
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    let base = Utc::now();
    for (n, duration) in [(1, 0_i64), (2, 1500), (3, 42)] {
        let mut a = failed_attempt(delivery, n, base + Duration::seconds(i64::from(n)));
        a.duration_ms = duration;
        record(&srv, &a).await;
    }

    let attempts = get_timeline(&srv, delivery).await;
    let durations: Vec<i64> = attempts.iter().map(|a| a["duration_ms"].as_i64().unwrap()).collect();
    assert_eq!(durations, vec![0, 1500, 42]);
}

#[tokio::test]
async fn timeline_includes_instance_id_when_present() {
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    let instance = Uuid::now_v7();
    let mut a = mk_attempt(delivery, 1, true, Utc::now());
    a.instance_id = Some(instance);
    record(&srv, &a).await;

    let attempts = get_timeline(&srv, delivery).await;
    assert_eq!(attempts[0]["instance_id"], instance.to_string().as_str());
}

#[tokio::test]
async fn timeline_omits_instance_id_when_absent() {
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    record(&srv, &mk_attempt(delivery, 1, true, Utc::now())).await;

    let attempts = get_timeline(&srv, delivery).await;
    assert!(!attempts[0].as_object().unwrap().contains_key("instance_id"));
}

#[tokio::test]
async fn timeline_is_isolated_per_delivery_group() {
    let srv = spawn_test_server().await;
    let base = Utc::now();
    let a = Uuid::now_v7();
    let b = Uuid::now_v7();
    record(&srv, &failed_attempt(a, 1, base)).await;
    record(&srv, &failed_attempt(a, 2, base + Duration::seconds(1))).await;
    record(&srv, &failed_attempt(b, 1, base + Duration::seconds(2))).await;

    let attempts_a = get_timeline(&srv, a).await;
    assert_eq!(attempts_a.len(), 2);
    for attempt in &attempts_a {
        assert_eq!(attempt["delivery_id"], a.to_string().as_str());
    }
    assert_eq!(get_timeline(&srv, b).await.len(), 1);
}

#[tokio::test]
async fn timeline_echoes_url_and_event_type() {
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    let mut a = mk_attempt(delivery, 1, false, Utc::now());
    a.url = "https://echo.example.com/hook".into();
    a.event_type = "instance.completed".into();
    a.set_error("gremlins", None);
    record(&srv, &a).await;

    let attempts = get_timeline(&srv, delivery).await;
    assert_eq!(attempts[0]["url"], "https://echo.example.com/hook");
    assert_eq!(attempts[0]["event_type"], "instance.completed");
    assert_eq!(attempts[0]["error_class"], "other");
}

// ---------------------------------------------------------------------------
// Redeliver preview
// ---------------------------------------------------------------------------

#[tokio::test]
async fn preview_reports_payload_bytes_and_unsigned_in_tests() {
    let srv = spawn_test_server().await;
    let entry = mk_parked("https://preview.example.com/hook", None);
    srv.storage.park_webhook(&entry).await.unwrap();

    let (status, preview) = get_preview(&srv, entry.id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(preview["url"], "https://preview.example.com/hook");
    assert_eq!(preview["event_type"], DEFAULT_EVENT);
    // Exact serialized size of the payload.
    let expected = serde_json::to_vec(&entry.payload).unwrap().len();
    assert_eq!(preview["payload_bytes"].as_u64().unwrap(), expected as u64);
    // The harness configures no webhook secret.
    assert_eq!(preview["will_be_signed"], false);
}

#[tokio::test]
async fn preview_includes_previous_delivery_id_when_linked() {
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    let entry = mk_parked("https://linked.example.com/hook", Some(delivery));
    srv.storage.park_webhook(&entry).await.unwrap();

    let (status, preview) = get_preview(&srv, entry.id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(preview["previous_delivery_id"], delivery.to_string().as_str());
}

#[tokio::test]
async fn preview_omits_previous_delivery_id_when_unlinked() {
    let srv = spawn_test_server().await;
    let entry = mk_parked("https://unlinked.example.com/hook", None);
    srv.storage.park_webhook(&entry).await.unwrap();

    let (status, preview) = get_preview(&srv, entry.id).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        !preview.as_object().unwrap().contains_key("previous_delivery_id"),
        "unlinked rows must omit previous_delivery_id"
    );
}

#[tokio::test]
async fn preview_reports_previous_attempts_and_last_error() {
    let srv = spawn_test_server().await;
    let mut entry = mk_parked("https://err.example.com/hook", None);
    entry.attempts = 7;
    entry.last_error = Some("operation timed out".into());
    srv.storage.park_webhook(&entry).await.unwrap();

    let (_, preview) = get_preview(&srv, entry.id).await;
    assert_eq!(preview["previous_attempts"], 7);
    assert_eq!(preview["last_error"], "operation timed out");
}

#[tokio::test]
async fn preview_omits_last_error_when_none() {
    let srv = spawn_test_server().await;
    let mut entry = mk_parked("https://noerr.example.com/hook", None);
    entry.last_error = None;
    srv.storage.park_webhook(&entry).await.unwrap();

    let (status, preview) = get_preview(&srv, entry.id).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!preview.as_object().unwrap().contains_key("last_error"));
}

#[tokio::test]
async fn preview_unknown_id_is_404() {
    let srv = spawn_test_server().await;
    let (status, _) = get_preview(&srv, Uuid::now_v7()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn preview_is_non_destructive() {
    let srv = spawn_test_server().await;
    let entry = mk_parked("https://keep.example.com/hook", Some(Uuid::now_v7()));
    srv.storage.park_webhook(&entry).await.unwrap();

    let (status, _) = get_preview(&srv, entry.id).await;
    assert_eq!(status, StatusCode::OK);

    // Row is still parked (storage) and still listed (API).
    assert!(srv.storage.get_webhook_outbox(entry.id).await.unwrap().is_some());
    let resp = reqwest::Client::new()
        .get(format!("{}/webhooks/outbox", srv.v1_url()))
        .send()
        .await
        .unwrap();
    let rows: Value = resp.json().await.unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1);
    assert_eq!(rows[0]["id"], entry.id.to_string().as_str());
}

#[tokio::test]
async fn repeated_previews_are_stable() {
    let srv = spawn_test_server().await;
    let entry = mk_parked("https://stable.example.com/hook", Some(Uuid::now_v7()));
    srv.storage.park_webhook(&entry).await.unwrap();

    let (s1, p1) = get_preview(&srv, entry.id).await;
    let (s2, p2) = get_preview(&srv, entry.id).await;
    let (s3, p3) = get_preview(&srv, entry.id).await;
    assert_eq!(s1, StatusCode::OK);
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(s3, StatusCode::OK);
    assert_eq!(p1, p2);
    assert_eq!(p2, p3);
}

#[tokio::test]
async fn preview_links_to_inspectable_delivery_timeline() {
    // End-to-end linkage: the preview's previous_delivery_id can be fed to
    // the delivery inspector to see the attempts that parked the entry.
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    let base = Utc::now();
    record(&srv, &failed_attempt(delivery, 1, base)).await;
    record(&srv, &failed_attempt(delivery, 2, base + Duration::seconds(1))).await;

    let mut entry = mk_parked("https://story.example.com/hook", Some(delivery));
    entry.attempts = 2;
    srv.storage.park_webhook(&entry).await.unwrap();

    let (_, preview) = get_preview(&srv, entry.id).await;
    let linked = Uuid::parse_str(preview["previous_delivery_id"].as_str().unwrap()).unwrap();
    let attempts = get_timeline(&srv, linked).await;
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0]["attempt_number"], 1);
    assert_eq!(attempts[1]["attempt_number"], 2);
}

// ---------------------------------------------------------------------------
// Outbox list / discard (with delivery_id linkage)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn outbox_empty_list_returns_empty_array() {
    let srv = spawn_test_server().await;
    let resp = reqwest::Client::new()
        .get(format!("{}/webhooks/outbox", srv.v1_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let rows: Value = resp.json().await.unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn outbox_lists_entries_with_and_without_delivery_id() {
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    let linked = mk_parked("https://linked.example.com/hook", Some(delivery));
    let legacy = mk_parked("https://legacy.example.com/hook", None);
    srv.storage.park_webhook(&linked).await.unwrap();
    srv.storage.park_webhook(&legacy).await.unwrap();

    let resp = reqwest::Client::new()
        .get(format!("{}/webhooks/outbox", srv.v1_url()))
        .send()
        .await
        .unwrap();
    let rows: Value = resp.json().await.unwrap();
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    let find = |id: Uuid| {
        arr.iter()
            .find(|r| r["id"] == id.to_string().as_str())
            .unwrap()
            .clone()
    };
    let linked_row = find(linked.id);
    assert_eq!(linked_row["delivery_id"], delivery.to_string().as_str());
    let legacy_row = find(legacy.id);
    assert!(
        !legacy_row.as_object().unwrap().contains_key("delivery_id"),
        "legacy rows must omit delivery_id"
    );
}

#[tokio::test]
async fn outbox_entry_fields_round_trip_through_api() {
    let srv = spawn_test_server().await;
    let mut entry = mk_parked("https://fields.example.com/hook", None);
    entry.attempts = 9;
    entry.last_error = Some("connection refused".into());
    srv.storage.park_webhook(&entry).await.unwrap();

    let resp = reqwest::Client::new()
        .get(format!("{}/webhooks/outbox", srv.v1_url()))
        .send()
        .await
        .unwrap();
    let rows: Value = resp.json().await.unwrap();
    let row = &rows[0];
    assert_eq!(row["url"], "https://fields.example.com/hook");
    assert_eq!(row["event_type"], DEFAULT_EVENT);
    assert_eq!(row["attempts"], 9);
    assert_eq!(row["last_error"], "connection refused");
    assert_eq!(row["instance_id"], entry.instance_id.unwrap().to_string().as_str());
    assert_eq!(row["payload"], entry.payload);
}

#[tokio::test]
async fn outbox_discard_removes_only_target_row() {
    let srv = spawn_test_server().await;
    let keep = mk_parked("https://keep.example.com/hook", None);
    let drop_me = mk_parked("https://drop.example.com/hook", Some(Uuid::now_v7()));
    srv.storage.park_webhook(&keep).await.unwrap();
    srv.storage.park_webhook(&drop_me).await.unwrap();

    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{}/webhooks/outbox/{}", srv.v1_url(), drop_me.id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let rows: Value = client
        .get(format!("{}/webhooks/outbox", srv.v1_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["id"], keep.id.to_string().as_str());
}

#[tokio::test]
async fn outbox_discard_unknown_id_is_no_content() {
    let srv = spawn_test_server().await;
    let resp = reqwest::Client::new()
        .delete(format!("{}/webhooks/outbox/{}", srv.v1_url(), Uuid::now_v7()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn outbox_list_respects_limit() {
    let srv = spawn_test_server().await;
    for i in 0..3 {
        srv.storage
            .park_webhook(&mk_parked(&format!("https://n{i}.example.com/hook"), None))
            .await
            .unwrap();
    }
    let resp = reqwest::Client::new()
        .get(format!("{}/webhooks/outbox", srv.v1_url()))
        .query(&[("limit", "1")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let rows: Value = resp.json().await.unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn discarding_outbox_row_keeps_delivery_history() {
    // The attempt history outlives the parked row: an operator can discard
    // the outbox entry and still audit what happened.
    let srv = spawn_test_server().await;
    let delivery = Uuid::now_v7();
    record(&srv, &failed_attempt(delivery, 1, Utc::now())).await;
    let entry = mk_parked("https://audit.example.com/hook", Some(delivery));
    srv.storage.park_webhook(&entry).await.unwrap();

    let resp = reqwest::Client::new()
        .delete(format!("{}/webhooks/outbox/{}", srv.v1_url(), entry.id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let attempts = get_timeline(&srv, delivery).await;
    assert_eq!(attempts.len(), 1);
}
