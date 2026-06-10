//! E2E tests for `ActivePieces` polling-trigger registrations
//! (`trigger_type: "activepieces_poll"`) through the `/triggers` API:
//! creation + config validation, poll-state surfacing on GET, tenant
//! isolation, and delete cleanup.

use orch8_api::test_harness::spawn_test_server;
use orch8_storage::AdminStore;
use orch8_types::trigger::TriggerPollState;
use reqwest::StatusCode;
use serde_json::json;

fn poll_trigger_body(slug: &str, tenant: &str) -> serde_json::Value {
    json!({
        "slug": slug,
        "sequence_name": "payment-recovery",
        "tenant_id": tenant,
        "trigger_type": "activepieces_poll",
        "config": {
            "piece": "stripe",
            "trigger": "new_failed_payment",
            "auth": "credentials://stripe-prod",
            "props": {"account": "acct_1"},
            "interval_secs": 30
        }
    })
}

#[tokio::test]
async fn create_and_get_activepieces_poll_trigger_round_trip() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/triggers", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&poll_trigger_body("stripe-failed", "t1"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(created["trigger_type"], "activepieces_poll");
    assert_eq!(created["config"]["piece"], "stripe");

    let resp = client
        .get(format!("{}/triggers/stripe-failed", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(got["slug"], "stripe-failed");
    assert_eq!(got["trigger_type"], "activepieces_poll");
    assert_eq!(got["config"]["trigger"], "new_failed_payment");
    assert_eq!(got["config"]["interval_secs"], 30);
    // Poll-state field is present (null before the first poll).
    assert!(
        got.as_object().unwrap().contains_key("poll_state"),
        "activepieces_poll GET must carry poll_state: {got}"
    );
    assert!(got["poll_state"].is_null());

    // Non-poll triggers do NOT carry the field.
    let resp = client
        .post(format!("{}/triggers", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "slug": "plain-webhook",
            "sequence_name": "seq",
            "tenant_id": "t1"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let got: serde_json::Value = client
        .get(format!("{}/triggers/plain-webhook", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!got.as_object().unwrap().contains_key("poll_state"));
}

#[tokio::test]
async fn create_rejects_invalid_poll_configs() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let cases = [
        // missing piece
        json!({"trigger": "t"}),
        // missing trigger
        json!({"piece": "stripe"}),
        // bad piece charset
        json!({"piece": "Not_Valid", "trigger": "t"}),
        // zero interval
        json!({"piece": "stripe", "trigger": "t", "interval_secs": 0}),
        // unparseable cron
        json!({"piece": "stripe", "trigger": "t", "cron": "banana"}),
        // both schedules
        json!({"piece": "stripe", "trigger": "t", "cron": "* * * * *", "interval_secs": 5}),
        // not an object
        json!(null),
    ];

    for (i, config) in cases.iter().enumerate() {
        let resp = client
            .post(format!("{}/triggers", srv.v1_url()))
            .header("X-Tenant-Id", "t1")
            .json(&json!({
                "slug": format!("bad-{i}"),
                "sequence_name": "seq",
                "tenant_id": "t1",
                "trigger_type": "activepieces_poll",
                "config": config
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "config case {i} must be rejected: {config}"
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert!(
            body["error"]
                .as_str()
                .unwrap_or_default()
                .contains("activepieces_poll"),
            "error should mention the trigger kind: {body}"
        );
    }

    // The same configs are fine for non-poll trigger types (config is opaque there).
    let resp = client
        .post(format!("{}/triggers", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "slug": "webhook-any-config",
            "sequence_name": "seq",
            "tenant_id": "t1",
            "trigger_type": "webhook",
            "config": {"piece": "Not_Valid"}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn get_surfaces_poll_state_written_by_the_engine() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/triggers", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&poll_trigger_body("with-state", "t1"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Simulate the engine's poll loop persisting a cursor + failure info.
    let now = chrono::Utc::now();
    srv.storage
        .upsert_trigger_poll_state(&TriggerPollState {
            slug: "with-state".into(),
            state: json!({"lastPoll": 1_717_171_717}),
            last_poll_at: Some(now),
            last_error: Some("stripe 503".into()),
            consecutive_failures: 3,
            updated_at: now,
        })
        .await
        .unwrap();

    let got: serde_json::Value = client
        .get(format!("{}/triggers/with-state", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        got["poll_state"]["state"],
        json!({"lastPoll": 1_717_171_717})
    );
    assert_eq!(got["poll_state"]["last_error"], "stripe 503");
    assert_eq!(got["poll_state"]["consecutive_failures"], 3);
    assert!(got["poll_state"]["last_poll_at"].is_string());
}

#[tokio::test]
async fn tenant_isolation_for_poll_triggers() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Tenant header mismatching body tenant_id → forbidden.
    let resp = client
        .post(format!("{}/triggers", srv.v1_url()))
        .header("X-Tenant-Id", "tenant-b")
        .json(&poll_trigger_body("a-poll", "tenant-a"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // Create as tenant-a.
    let resp = client
        .post(format!("{}/triggers", srv.v1_url()))
        .header("X-Tenant-Id", "tenant-a")
        .json(&poll_trigger_body("a-poll", "tenant-a"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Cross-tenant GET is a 404 (existence must not leak).
    let resp = client
        .get(format!("{}/triggers/a-poll", srv.v1_url()))
        .header("X-Tenant-Id", "tenant-b")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Cross-tenant DELETE likewise.
    let resp = client
        .delete(format!("{}/triggers/a-poll", srv.v1_url()))
        .header("X-Tenant-Id", "tenant-b")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Owner still sees it.
    let resp = client
        .get(format!("{}/triggers/a-poll", srv.v1_url()))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn delete_removes_trigger_and_poll_state() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/triggers", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&poll_trigger_body("doomed", "t1"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let now = chrono::Utc::now();
    srv.storage
        .upsert_trigger_poll_state(&TriggerPollState {
            slug: "doomed".into(),
            state: json!({"cursor": 9}),
            last_poll_at: Some(now),
            last_error: None,
            consecutive_failures: 0,
            updated_at: now,
        })
        .await
        .unwrap();

    let resp = client
        .delete(format!("{}/triggers/doomed", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Trigger gone…
    let resp = client
        .get(format!("{}/triggers/doomed", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    // …and the poll-state row was cleaned up with it.
    let state = srv.storage.get_trigger_poll_state("doomed").await.unwrap();
    assert!(state.is_none(), "delete_trigger must remove poll state");
}

#[tokio::test]
async fn list_includes_poll_triggers() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/triggers", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&poll_trigger_body("listed-poll", "t1"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let got: serde_json::Value = client
        .get(format!("{}/triggers", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = got.as_array().unwrap();
    assert!(arr
        .iter()
        .any(|t| t["slug"] == "listed-poll" && t["trigger_type"] == "activepieces_poll"));
}
