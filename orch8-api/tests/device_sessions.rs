//! Device-session minting and the operator-key signal for mobile SDKs, end
//! to end through the real auth middleware (root API key configured). Runs
//! on `SQLite` and, when `DATABASE_URL` is set, on Postgres. The full scope
//! matrix (every refused action) lives in `orch8-e2e/tests/distributed_api.rs`.

use std::sync::Arc;

use orch8_api::test_harness::{BackendTestServer, TestServerOptions, spawn_test_server_with};
use orch8_storage::StorageBackend;
use orch8_storage::postgres::PostgresStorage;
use orch8_storage::sqlite::SqliteStorage;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

const ROOT: &str = "root-key-for-device-session-tests";
const SCOPE_HEADER: &str = "x-orch8-principal-scope";

fn options() -> TestServerOptions {
    TestServerOptions {
        root_api_key: Some(ROOT.into()),
        mobile_sync_enabled: true,
    }
}

async fn servers() -> Vec<(&'static str, BackendTestServer)> {
    let mut out = vec![(
        "sqlite",
        spawn_test_server_with(
            Arc::new(SqliteStorage::in_memory().await.unwrap()) as Arc<dyn StorageBackend>,
            options(),
        )
        .await,
    )];
    if let Ok(url) = std::env::var("DATABASE_URL") {
        let pg = PostgresStorage::new(&url, 5, None).await.unwrap();
        pg.run_migrations().await.unwrap();
        out.push((
            "postgres",
            spawn_test_server_with(Arc::new(pg) as Arc<dyn StorageBackend>, options()).await,
        ));
    }
    out
}

async fn call(
    client: &Client,
    server: &BackendTestServer,
    key: &str,
    tenant: &str,
    path: &str,
    body: &Value,
) -> reqwest::Response {
    client
        .post(format!("{}{path}", server.v1_url()))
        .header("x-api-key", key)
        .header("X-Tenant-Id", tenant)
        .json(body)
        .send()
        .await
        .unwrap()
}

async fn mint_key(
    client: &Client,
    server: &BackendTestServer,
    tenant: &str,
    capabilities: &[&str],
) -> String {
    let response = call(
        client,
        server,
        ROOT,
        tenant,
        "/api-keys",
        &json!({"tenant_id": tenant, "name": "k", "capabilities": capabilities}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    response.json::<Value>().await.unwrap()["secret"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn minting_is_validated_and_bound_to_the_device_tenant() {
    let client = Client::new();
    for (backend, server) in servers().await {
        let tenant = format!("ds-{}", Uuid::now_v7().simple());
        let runtime = Uuid::now_v7().to_string();
        let operator = mint_key(&client, &server, &tenant, &["operator"]).await;

        let response = call(
            &client,
            &server,
            &operator,
            &tenant,
            "/runtimes/device-sessions",
            &json!({"device_id": "phone-1", "runtime_id": runtime, "handlers": ["scan", "scan"]}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED, "{backend}");
        let session: Value = response.json().await.unwrap();
        assert!(session["token"].as_str().unwrap().starts_with("dst_"));
        assert_eq!(session["handlers"], json!(["scan"]), "{backend}: deduped");
        let ttl = chrono::DateTime::parse_from_rfc3339(session["expires_at"].as_str().unwrap())
            .unwrap()
            .signed_duration_since(chrono::Utc::now());
        assert!(
            ttl > chrono::Duration::minutes(59) && ttl <= chrono::Duration::hours(1),
            "{backend}: default ttl is an hour: {ttl}"
        );

        for (body, why) in [
            (
                json!({"device_id": "", "runtime_id": runtime}),
                "empty device",
            ),
            (
                json!({"device_id": "phone-1", "runtime_id": runtime, "ttl_secs": 86_401}),
                "ttl above a day",
            ),
            (
                json!({"device_id": "phone-1", "runtime_id": runtime, "ttl_secs": 0}),
                "zero ttl",
            ),
            (
                json!({"device_id": "phone-1", "runtime_id": runtime, "handlers": [" "]}),
                "blank handler",
            ),
        ] {
            let response = call(
                &client,
                &server,
                &operator,
                &tenant,
                "/runtimes/device-sessions",
                &body,
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{backend}: {why}"
            );
        }

        // A device registered to another tenant cannot be bound here.
        let other = format!("ds-other-{}", Uuid::now_v7().simple());
        let taken = format!("phone-taken-{}", Uuid::now_v7().simple());
        let response = call(
            &client,
            &server,
            ROOT,
            &other,
            "/mobile/devices/register",
            &json!({"device_id": taken, "platform": "ios"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED, "{backend}");
        let response = call(
            &client,
            &server,
            &operator,
            &tenant,
            "/runtimes/device-sessions",
            &json!({"device_id": taken, "runtime_id": runtime}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT, "{backend}");
    }
}

#[tokio::test]
async fn mobile_routes_flag_operator_keys_but_not_scoped_credentials() {
    let client = Client::new();
    for (backend, server) in servers().await {
        let tenant = format!("ds-{}", Uuid::now_v7().simple());
        let suffix = Uuid::now_v7().simple().to_string();
        let device = |name: &str| format!("{name}-{suffix}");
        let register = |device: &str| json!({"device_id": device, "platform": "android"});
        let operator = mint_key(&client, &server, &tenant, &["operator"]).await;
        let device_key = mint_key(&client, &server, &tenant, &["worker", "device"]).await;
        let response = call(
            &client,
            &server,
            ROOT,
            &tenant,
            "/runtimes/device-sessions",
            &json!({"device_id": device("phone-s"), "runtime_id": Uuid::now_v7()}),
        )
        .await;
        let session: Value = response.json().await.unwrap();
        let session = session["token"].as_str().unwrap().to_owned();

        for (key, device, scope) in [
            (ROOT, device("phone-r"), Some("root")),
            (operator.as_str(), device("phone-o"), Some("operator")),
            (device_key.as_str(), device("phone-d"), None),
            (session.as_str(), device("phone-s"), None),
        ] {
            let response = call(
                &client,
                &server,
                key,
                &tenant,
                "/mobile/devices/register",
                &register(&device),
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::CREATED,
                "{backend}: {device}"
            );
            assert_eq!(
                response
                    .headers()
                    .get(SCOPE_HEADER)
                    .and_then(|value| value.to_str().ok()),
                scope,
                "{backend}: {device}"
            );
        }
    }
}
