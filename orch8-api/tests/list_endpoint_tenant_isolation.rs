//! Exercise tenant list scoping through the real router and authentication layers.

use std::sync::Arc;

use orch8_api::test_harness::{BackendTestServer, spawn_test_server_on};
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::api_key;
use reqwest::StatusCode;
use serde_json::{Value, json};

const ROOT_KEY: &str = "list-isolation-root-key";

async fn setup() -> (BackendTestServer, String) {
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let key = api_key::mint("acme", "list-isolation", None);
    orch8_storage::AdminStore::create_api_key(storage.as_ref(), &key.record)
        .await
        .unwrap();
    let server = spawn_test_server_on(storage, Some(ROOT_KEY)).await;
    let client = reqwest::Client::new();
    for tenant in ["acme", "foreign"] {
        client
            .post(format!("{}/sequences", server.v1_url()))
            .header("x-api-key", ROOT_KEY)
            .json(&json!({
                "id": uuid::Uuid::now_v7(),
                "created_at": chrono::Utc::now(),
                "tenant_id": tenant,
                "namespace": "default",
                "name": format!("{tenant}-sequence"),
                "version": 1,
                "blocks": [{"type": "step", "id": "s1", "handler": "noop", "params": {}}]
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        client
            .post(format!("{}/triggers", server.v1_url()))
            .header("x-api-key", ROOT_KEY)
            .json(&json!({
                "slug": format!("{tenant}-trigger"),
                "sequence_name": format!("{tenant}-sequence"),
                "tenant_id": tenant
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
    (server, key.secret)
}

#[tokio::test]
async fn list_endpoints_reject_missing_and_unknown_credentials_even_with_a_tenant_query() {
    let (server, _) = setup().await;
    let client = reqwest::Client::new();
    for prefix in ["", "/api/v1"] {
        for route in [
            "/sequences",
            "/sequences.json",
            "/triggers",
            "/changes",
            "/changes/stream",
            "/credentials",
            "/cron",
            "/instances/dlq/groups",
            "/events",
            "/plugins",
            "/pools",
            "/queues/dispatch",
            "/routing-rules",
            "/releases",
            "/workers/tasks",
            "/alerts/rules",
        ] {
            for secret in [None, Some("unknown-key")] {
                let url = format!("{}{prefix}{route}?tenant_id=foreign", server.base_url);
                let mut request = client.get(&url);
                if let Some(secret) = secret {
                    request = request.header("x-api-key", secret);
                }
                assert_eq!(
                    request.send().await.unwrap().status(),
                    StatusCode::UNAUTHORIZED,
                    "{url}, key={secret:?}"
                );
            }
        }
    }
}

#[tokio::test]
async fn tenant_queries_cannot_override_key_identity_and_root_can_filter_tenants() {
    let (server, tenant_key) = setup().await;
    let client = reqwest::Client::new();
    for prefix in ["", "/api/v1"] {
        for route in ["/sequences", "/triggers"] {
            for (secret, query, expected) in [
                (tenant_key.as_str(), "", vec!["acme"]),
                (tenant_key.as_str(), "?tenant_id=foreign", vec!["acme"]),
                (tenant_key.as_str(), "?tenant_id=", vec!["acme"]),
                (ROOT_KEY, "?tenant_id=foreign", vec!["foreign"]),
                (ROOT_KEY, "", vec!["acme", "foreign"]),
            ] {
                let url = format!("{}{prefix}{route}{query}", server.base_url);
                let response: Value = client
                    .get(&url)
                    .header("x-api-key", secret)
                    .send()
                    .await
                    .unwrap()
                    .error_for_status()
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                let items = response
                    .as_array()
                    .or_else(|| response["items"].as_array())
                    .unwrap();
                let mut tenants: Vec<_> = items
                    .iter()
                    .map(|item| item["tenant_id"].as_str().unwrap())
                    .collect();
                tenants.sort_unstable();
                assert_eq!(tenants, expected, "{url}");
            }
        }
    }
}

#[tokio::test]
async fn a_key_authenticated_by_one_server_is_rejected_by_an_independent_server() {
    let (first, key) = setup().await;
    let second = spawn_test_server_on(
        Arc::new(SqliteStorage::in_memory().await.unwrap()),
        Some(ROOT_KEY),
    )
    .await;
    let client = reqwest::Client::new();
    let first_status = client
        .get(format!("{}/triggers", first.v1_url()))
        .header("x-api-key", &key)
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(first_status, StatusCode::OK);

    let second_status = client
        .get(format!("{}/triggers?tenant_id=acme", second.v1_url()))
        .header("x-api-key", &key)
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(second_status, StatusCode::UNAUTHORIZED);
}
