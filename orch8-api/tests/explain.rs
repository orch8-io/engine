//! E2E tests for `GET /instances/{id}/explain`.

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

async fn waiting_instance(base: &str, client: &reqwest::Client) -> String {
    let seq_id = Uuid::now_v7().to_string();
    let resp = client
        .post(format!("{base}/sequences"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "id": seq_id,
            "tenant_id": "t1",
            "namespace": "default",
            "name": format!("explain-{seq_id}"),
            "version": 1,
            "blocks": [{"type": "step", "id": "a", "handler": "noop", "params": {}}],
            "created_at": chrono::Utc::now().to_rfc3339(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let fire_at = chrono::Utc::now() + chrono::Duration::hours(6);
    let resp = client
        .post(format!("{base}/instances"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "tenant_id": "t1",
            "sequence_id": seq_id,
            "namespace": "default",
            "next_fire_at": fire_at.to_rfc3339(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let v: Value = resp.json().await.unwrap();
    v["id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn explain_renders_template_explanation_for_waiting_instance() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let inst = waiting_instance(&base, &client).await;

    let resp = client
        .get(format!("{base}/instances/{inst}/explain"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let e: Value = resp.json().await.unwrap();
    assert_eq!(e["mode"], "template");
    assert_eq!(e["code"], "WAITING_UNTIL");
    assert_eq!(e["error_code"], "ORCH8-D003");
    assert_eq!(e["docs_url"], "https://orch8.io/docs/errors#ORCH8-D003");
    assert_eq!(
        e["headline"],
        "The instance is waiting for a scheduled time (this is expected)."
    );
    assert!(e["likely_cause"].as_str().unwrap().len() > 10);
    assert!(!e["commands"].as_array().unwrap().is_empty());
    assert!(e.get("narrative").is_none());
}

#[tokio::test]
async fn explain_llm_without_configured_key_degrades_to_template() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let inst = waiting_instance(&base, &client).await;

    // With no key configured, `llm_call` key resolution fails before any
    // network call and the template explanation is returned unchanged.
    let resp = client
        .get(format!("{base}/instances/{inst}/explain?llm=true"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let e: Value = resp.json().await.unwrap();
    assert_eq!(e["code"], "WAITING_UNTIL");
    if std::env::var_os("OPENAI_API_KEY").is_none()
        && std::env::var_os("ORCH8_EXPLAIN_LLM_API_KEY").is_none()
    {
        assert_eq!(e["mode"], "template");
        assert!(e["llm_error"].as_str().unwrap().contains("API key"), "{e}");
    }
}

#[tokio::test]
async fn explain_is_tenant_isolated_and_404s_unknown_instances() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let inst = waiting_instance(&base, &client).await;

    let resp = client
        .get(format!("{base}/instances/{inst}/explain"))
        .header("X-Tenant-Id", "intruder")
        .send()
        .await
        .unwrap();
    assert_ne!(resp.status(), StatusCode::OK);

    let resp = client
        .get(format!("{base}/instances/{}/explain", Uuid::now_v7()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
