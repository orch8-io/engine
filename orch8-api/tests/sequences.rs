//! E2E tests for the Sequences API.

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn create_sequence_and_get_by_id_round_trip() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let seq_id = Uuid::now_v7();
    let body = json!({
        "id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "test-seq",
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
    });

    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = client
        .get(format!("{}/sequences/{seq_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["name"], "test-seq");
    assert_eq!(fetched["version"], 1);
}

#[tokio::test]
async fn create_sequence_with_duplicate_block_ids_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let body = json!({
        "id": Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "dup-seq",
        "version": 1,
        "deprecated": false,
        "blocks": [
            {
                "type": "step",
                "id": "s1",
                "handler": "noop",
                "params": {},
                "cancellable": true
            },
            {
                "type": "step",
                "id": "s1",
                "handler": "log",
                "params": {},
                "cancellable": true
            }
        ],
        "interceptors": null,
        "created_at": chrono::Utc::now().to_rfc3339()
    });

    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_sequence_by_name_returns_latest_version() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let seq_id_v1 = Uuid::now_v7();
    let seq_id_v2 = Uuid::now_v7();

    for (id, version) in [(seq_id_v1, 1), (seq_id_v2, 2)] {
        let body = json!({
            "id": id,
            "tenant_id": "t1",
            "namespace": "ns1",
            "name": "versioned-seq",
            "version": version,
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
        });
        let resp = client
            .post(format!("{}/sequences", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    let resp = client
        .get(format!(
            "{}/sequences/by-name?tenant_id=t1&namespace=ns1&name=versioned-seq",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["version"], 2);
}

#[tokio::test]
async fn deprecate_sequence_hides_from_by_name_lookup() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let seq_id = Uuid::now_v7();
    let body = json!({
        "id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "dep-seq",
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
    });

    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = client
        .post(format!("{}/sequences/{seq_id}/deprecate", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let resp = client
        .get(format!(
            "{}/sequences/by-name?tenant_id=t1&namespace=ns1&name=dep-seq",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    // No non-deprecated version exists → 404.
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_sequence_versions_returns_all_versions() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let seq_id_v1 = Uuid::now_v7();
    let seq_id_v2 = Uuid::now_v7();

    for (id, version) in [(seq_id_v1, 1), (seq_id_v2, 2)] {
        let body = json!({
            "id": id,
            "tenant_id": "t1",
            "namespace": "ns1",
            "name": "list-seq",
            "version": version,
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
        });
        let resp = client
            .post(format!("{}/sequences", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    let resp = client
        .get(format!(
            "{}/sequences/versions?tenant_id=t1&namespace=ns1&name=list-seq",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let versions: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(versions.len(), 2);
}

#[tokio::test]
async fn delete_sequence_returns_404_for_unknown_id() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .delete(format!("{}/sequences/{}", srv.base_url, Uuid::now_v7()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_sequence_returns_404_for_unknown_id() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/sequences/{}", srv.base_url, Uuid::now_v7()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// `Stab#14`: `migrate_instance` used to fetch the target sequence and
/// immediately discard it — never comparing tenants. That let tenant A
/// rebind their instance onto tenant B's sequence, so the scheduler would
/// execute A's instance against B's blocks (crossing the isolation
/// boundary). The fix rejects cross-tenant target sequences with 403.
#[tokio::test]
async fn migrate_instance_rejects_cross_tenant_target_sequence() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Sequence owned by tenant "t1".
    let seq_t1 = Uuid::now_v7();
    let body_t1 = json!({
        "id": seq_t1,
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "seq-t1",
        "version": 1,
        "deprecated": false,
        "blocks": [
            { "type": "step", "id": "s1", "handler": "noop", "params": {}, "cancellable": true }
        ],
        "interceptors": null,
        "created_at": chrono::Utc::now().to_rfc3339()
    });
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body_t1)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Sequence owned by tenant "t2" — the cross-tenant migration target.
    let seq_t2 = Uuid::now_v7();
    let body_t2 = json!({
        "id": seq_t2,
        "tenant_id": "t2",
        "namespace": "ns1",
        "name": "seq-t2",
        "version": 1,
        "deprecated": false,
        "blocks": [
            { "type": "step", "id": "s1", "handler": "noop", "params": {}, "cancellable": true }
        ],
        "interceptors": null,
        "created_at": chrono::Utc::now().to_rfc3339()
    });
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t2")
        .json(&body_t2)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Create a t1 instance.
    let inst_body = json!({
        "sequence_id": seq_t1,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": {}, "config": {}, "audit": [] }
    });
    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&inst_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    let inst_id = created["id"].as_str().unwrap().to_string();

    // Attempt to migrate t1's instance onto t2's sequence — must be
    // rejected with 403 (isolation breach), not silently accepted.
    let migrate_body = json!({
        "instance_id": inst_id,
        "target_sequence_id": seq_t2,
    });
    let resp = client
        .post(format!("{}/sequences/migrate-instance", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&migrate_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}
