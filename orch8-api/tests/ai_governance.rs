//! E2E: prompt registry, tenant budgets, and the usage endpoint's budget
//! status + cache-savings reporting.
#![allow(clippy::too_many_lines)]

use chrono::Utc;
use orch8_api::test_harness::spawn_test_server;
use orch8_storage::{TelemetryStore, UsageEvent};
use reqwest::StatusCode;
use serde_json::{Value, json};

fn usage_event(tenant: &str, kind: &str, model: &str, input: i64, output: i64) -> UsageEvent {
    UsageEvent {
        tenant_id: tenant.into(),
        instance_id: None,
        block_id: Some("llm".into()),
        kind: kind.into(),
        model: model.into(),
        input_tokens: input,
        output_tokens: output,
        created_at: Utc::now(),
    }
}

#[tokio::test]
async fn prompt_registry_push_label_resolve_and_isolation() {
    let srv = spawn_test_server().await;
    let c = reqwest::Client::new();
    let api = srv.v1_url();
    let push = |tenant: &'static str, text: &'static str, label: Option<&'static str>| {
        let c = c.clone();
        let api = api.clone();
        async move {
            c.post(format!("{api}/prompts"))
                .header("X-Tenant-Id", tenant)
                .json(&json!({
                    "name": "triage",
                    "system": "Classify {{ product }} tickets.",
                    "messages": [{"role": "user", "content": text}],
                    "model_params": {"model": "gpt-4o", "temperature": 0},
                    "label": label,
                }))
                .send()
                .await
                .unwrap()
        }
    };

    let r = push("t1", "v1: {{ ticket }}", Some("production")).await;
    assert_eq!(r.status(), StatusCode::CREATED);
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["prompt"]["version"], 1);
    assert_eq!(body["prompt"]["variables"], json!(["product", "ticket"]));
    assert_eq!(body["label"]["label"], "production");

    // Identical content is idempotent (200, same version).
    let r = push("t1", "v1: {{ ticket }}", None).await;
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.json::<Value>().await.unwrap()["created"], false);

    let r = push("t1", "v2: {{ ticket }}", None).await;
    assert_eq!(r.json::<Value>().await.unwrap()["prompt"]["version"], 2);

    // Canary label: 20% of executions to v2.
    let r = c
        .put(format!("{api}/prompts/triage/labels/production"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"version": 1, "canary_version": 2, "canary_percent": 20}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.json::<Value>().await.unwrap()["canary"]["percent"], 20);

    // Unknown version → 404; bad name → 400.
    let r = c
        .put(format!("{api}/prompts/triage/labels/production"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"version": 99}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
    let r = c
        .post(format!("{api}/prompts"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"name": "bad name", "system": "x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);

    let list: Value = c
        .get(format!("{api}/prompts"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list[0]["name"], "triage");
    assert_eq!(list[0]["latest_version"], 2);
    assert_eq!(list[0]["versions"], 2);
    assert_eq!(list[0]["labels"][0]["version"], 1);

    let resolved: Value = c
        .get(format!("{api}/prompts/triage/resolve?label=production"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resolved["version"], 1);
    let v2: Value = c
        .get(format!("{api}/prompts/triage/versions/2"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v2["messages"][0]["content"], "v2: {{ ticket }}");

    // Tenant isolation: t2 sees nothing of t1's registry.
    let r = c
        .get(format!("{api}/prompts/triage"))
        .header("X-Tenant-Id", "t2")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
    let empty: Value = c
        .get(format!("{api}/prompts"))
        .header("X-Tenant-Id", "t2")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(empty, json!([]));

    let r = c
        .delete(format!("{api}/prompts/triage/labels/production"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn budgets_crud_status_alerts_and_usage_report() {
    let srv = spawn_test_server().await;
    let c = reqwest::Client::new();
    let api = srv.v1_url();

    let r = c
        .post(format!("{api}/budgets"))
        .json(&json!({"tenant_id": "b1", "period": "monthly", "limit_usd": 0.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::BAD_REQUEST,
        "limit must be positive"
    );

    let r = c
        .post(format!("{api}/budgets"))
        .json(
            &json!({"tenant_id": "b1", "model": "gpt-4o", "period": "monthly",
                      "limit_usd": 2.0, "thresholds": [80, 50]}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CREATED);
    let budget: Value = r.json().await.unwrap();
    assert_eq!(budget["thresholds"], json!([50, 80]), "sorted");
    assert_eq!(budget["hard_cap"], true);
    let id = budget["id"].as_str().unwrap().to_string();

    // gpt-4o: 400k input = $1.00 (50%); a cache hit saving $1.00 is not spend.
    srv.storage
        .record_usage_event(&usage_event("b1", "llm_tokens", "gpt-4o", 400_000, 0))
        .await
        .unwrap();
    srv.storage
        .record_usage_event(&usage_event("b1", "llm_cache_hit", "gpt-4o", 400_000, 0))
        .await
        .unwrap();

    let statuses: Value = c
        .get(format!("{api}/budgets"))
        .header("X-Tenant-Id", "b1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(statuses[0]["spend_usd"], 1.0);
    assert_eq!(statuses[0]["state"], "warning");
    assert_eq!(statuses[0]["blocking"], false);

    let usage: Value = c
        .get(format!("{api}/usage"))
        .header("X-Tenant-Id", "b1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        usage["total_cost_usd"], 1.0,
        "cache hits excluded from cost"
    );
    assert_eq!(usage["cache_savings"]["hits"], 1);
    assert_eq!(usage["cache_savings"]["saved_usd"], 1.0);
    assert_eq!(usage["budgets"][0]["budget"]["id"], id.as_str());
    assert_eq!(usage["budgets"][0]["percent_used"], 50.0);

    // Update: raise the limit.
    let r = c
        .put(format!("{api}/budgets/{id}"))
        .json(
            &json!({"tenant_id": "b1", "model": "gpt-4o", "period": "monthly",
                      "limit_usd": 10.0, "hard_cap": false}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.json::<Value>().await.unwrap()["hard_cap"], false);

    // Another tenant can't see or delete it.
    let other: Value = c
        .get(format!("{api}/budgets"))
        .header("X-Tenant-Id", "b2")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(other, json!([]));
    let r = c
        .delete(format!("{api}/budgets/{id}"))
        .header("X-Tenant-Id", "b2")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);

    let alerts: Value = c
        .get(format!("{api}/budgets/alerts?tenant_id=b1"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        alerts.as_array().unwrap().is_empty(),
        "alerts are emitted by llm_call"
    );

    let r = c
        .delete(format!("{api}/budgets/{id}?tenant_id=b1"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);

    let r = c
        .delete(format!("{api}/llm-cache"))
        .header("X-Tenant-Id", "b1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.json::<Value>().await.unwrap()["deleted"], 0);
}
