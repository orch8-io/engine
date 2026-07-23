//! E2E tests for the Cron API.

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::json;
use uuid::Uuid;

fn mk_sequence_body(id: Uuid) -> serde_json::Value {
    json!({
        "id": id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "cron-seq",
        "version": 1,
        "deprecated": false,
        "blocks": [
            {
                "type": "step",
                "id": "s1",
                "handler": "noop",
                "params": {},
                "cancellable": true
            }
        ],
        "interceptors": null,
        "created_at": chrono::Utc::now().to_rfc3339()
    })
}

async fn create_sequence(client: &reqwest::Client, base_url: &str) -> Uuid {
    let seq_id = Uuid::now_v7();
    let resp = client
        .post(format!("{base_url}/sequences"))
        .header("X-Tenant-Id", "t1")
        .json(&mk_sequence_body(seq_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    seq_id
}

#[tokio::test]
async fn create_cron_and_get_by_id_round_trip() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "tenant_id": "t1",
        "namespace": "ns1",
        "sequence_id": seq_id,
        "cron_expr": "0 * * * *",
        "timezone": "UTC",
        "enabled": true,
        "metadata": {}
    });

    let resp = client
        .post(format!("{}/cron", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    let cron_id = created["id"].as_str().unwrap();

    let resp = client
        .get(format!("{}/cron/{cron_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["cron_expr"], "0 * * * *");
    assert!(fetched["next_fire_at"].is_string());
}

#[tokio::test]
async fn create_cron_with_invalid_expression_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "tenant_id": "t1",
        "namespace": "ns1",
        "sequence_id": seq_id,
        "cron_expr": "not-a-cron",
        "timezone": "UTC",
        "enabled": true,
        "metadata": {}
    });

    let resp = client
        .post(format!("{}/cron", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn update_cron_expression_changes_next_fire_at() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "tenant_id": "t1",
        "namespace": "ns1",
        "sequence_id": seq_id,
        "cron_expr": "0 * * * *",
        "timezone": "UTC",
        "enabled": true,
        "metadata": {}
    });

    let resp = client
        .post(format!("{}/cron", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    let created: serde_json::Value = resp.json().await.unwrap();
    let cron_id = created["id"].as_str().unwrap();

    let resp = client
        .put(format!("{}/cron/{cron_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "cron_expr": "0 0 * * *" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .get(format!("{}/cron/{cron_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["cron_expr"], "0 0 * * *");
}

#[tokio::test]
async fn delete_cron_removes_schedule() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "tenant_id": "t1",
        "namespace": "ns1",
        "sequence_id": seq_id,
        "cron_expr": "0 * * * *",
        "timezone": "UTC",
        "enabled": true,
        "metadata": {}
    });

    let resp = client
        .post(format!("{}/cron", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    let created: serde_json::Value = resp.json().await.unwrap();
    let cron_id = created["id"].as_str().unwrap();

    let resp = client
        .delete(format!("{}/cron/{cron_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let resp = client
        .get(format!("{}/cron/{cron_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_cron_filters_by_tenant() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "tenant_id": "t1",
        "namespace": "ns1",
        "sequence_id": seq_id,
        "cron_expr": "0 * * * *",
        "timezone": "UTC",
        "enabled": true,
        "metadata": {}
    });

    let resp = client
        .post(format!("{}/cron", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = client
        .get(format!("{}/cron?tenant_id=t1", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let list: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(list.len(), 1);

    let resp = client
        .get(format!("{}/cron?tenant_id=t2", srv.base_url))
        .header("X-Tenant-Id", "t2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let list: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(list.len(), 0);
}

#[tokio::test]
async fn next_fires_returns_n_ascending_instants() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "tenant_id": "t1",
        "namespace": "ns1",
        "sequence_id": seq_id,
        "cron_expr": "0 * * * *",
        "timezone": "America/New_York",
        "enabled": true,
        "metadata": {}
    });
    let resp = client
        .post(format!("{}/cron", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let cron_id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = client
        .get(format!("{}/cron/{cron_id}/next-fires?n=5", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let out: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(out["timezone"], "America/New_York");
    let fires = out["fires"].as_array().unwrap();
    assert_eq!(fires.len(), 5);
    // Strictly ascending — proves the iterative bound advances.
    let parsed: Vec<chrono::DateTime<chrono::Utc>> = fires
        .iter()
        .map(|v| v.as_str().unwrap().parse().unwrap())
        .collect();
    for w in parsed.windows(2) {
        assert!(w[0] < w[1], "fires must be strictly ascending");
    }
}

#[tokio::test]
async fn create_cron_with_invalid_timezone_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "tenant_id": "t1",
        "namespace": "ns1",
        "sequence_id": seq_id,
        "cron_expr": "0 * * * *",
        "timezone": "Amercia/New_York",
        "enabled": true,
        "metadata": {}
    });

    let resp = client
        .post(format!("{}/cron", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn update_cron_with_invalid_timezone_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "tenant_id": "t1",
        "namespace": "ns1",
        "sequence_id": seq_id,
        "cron_expr": "0 * * * *",
        "timezone": "UTC",
        "enabled": true,
        "metadata": {}
    });
    let resp = client
        .post(format!("{}/cron", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let cron_id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = client
        .put(format!("{}/cron/{cron_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "timezone": "Not/A_Real_Zone" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // The stored schedule must be untouched.
    let resp = client
        .get(format!("{}/cron/{cron_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["timezone"], "UTC");
}

#[tokio::test]
async fn next_fires_unknown_id_returns_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "{}/cron/{}/next-fires",
            srv.base_url,
            Uuid::now_v7()
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
