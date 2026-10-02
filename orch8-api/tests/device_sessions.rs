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
        mobile_sync_resolve_credentials: false,
        ..TestServerOptions::default()
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

fn runtime(
    id: Uuid,
    kind: orch8_types::continuity::RuntimeKind,
    handlers: &[&str],
) -> orch8_types::continuity::RuntimeCapabilities {
    use orch8_types::continuity::{RuntimeCapabilities, RuntimeConnectivity, RuntimeTrustLevel};
    let now = chrono::Utc::now();
    RuntimeCapabilities {
        runtime_id: orch8_types::continuity::RuntimeId::from_uuid(id),
        kind,
        trust: RuntimeTrustLevel::Attested,
        handlers: handlers
            .iter()
            .map(|handler| (*handler).to_owned())
            .collect(),
        plugins: vec!["ocr".into()],
        credentials: vec!["stripe-live".into()],
        regions: vec!["eu-west-1".into()],
        hardware: vec!["gpu".into()],
        offline_capable: true,
        connectivity: Some(RuntimeConnectivity::Wifi),
        battery_percent: Some(40),
        estimated_cost_microunits: Some(7),
        estimated_latency_ms: Some(12),
        draining: false,
        capsule_signing_public_key: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".into()),
        labels: [("residency".to_owned(), "eu".to_owned())].into(),
        observed_at: now,
        expires_at: now + chrono::Duration::minutes(5),
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one scenario per backend, asserted inline
async fn device_sessions_see_only_live_delegation_destinations_reduced_to_matching_facts() {
    use orch8_types::continuity::RuntimeKind;
    const DELEGATION: &str = "orch8.delegation";

    let client = Client::new();
    for (backend, server) in servers().await {
        let tenant = format!("ds-{}", Uuid::now_v7().simple());
        let tenant_id = orch8_types::ids::TenantId::new(tenant.clone()).unwrap();
        let own = Uuid::now_v7();
        let destination = Uuid::now_v7();
        let draining = Uuid::now_v7();
        let no_delegation = Uuid::now_v7();
        let unverified = Uuid::now_v7();
        let expired = Uuid::now_v7();

        let mut rows = vec![
            runtime(own, RuntimeKind::Mobile, &[DELEGATION, "scan"]),
            runtime(destination, RuntimeKind::Server, &[DELEGATION, "scan"]),
            runtime(no_delegation, RuntimeKind::Server, &["scan"]),
        ];
        let mut row = runtime(draining, RuntimeKind::Server, &[DELEGATION, "scan"]);
        row.draining = true;
        rows.push(row);
        let mut row = runtime(unverified, RuntimeKind::Edge, &[DELEGATION, "scan"]);
        row.trust = orch8_types::continuity::RuntimeTrustLevel::Unverified;
        rows.push(row);
        let mut row = runtime(expired, RuntimeKind::Server, &[DELEGATION, "scan"]);
        row.observed_at = chrono::Utc::now() - chrono::Duration::minutes(10);
        row.expires_at = chrono::Utc::now() - chrono::Duration::minutes(5);
        rows.push(row);
        for row in &rows {
            server
                .storage
                .upsert_runtime_capabilities(&tenant_id, row)
                .await
                .unwrap();
        }

        let response = call(
            &client,
            &server,
            ROOT,
            &tenant,
            "/runtimes/device-sessions",
            &json!({"device_id": "phone-1", "runtime_id": own, "handlers": ["scan"]}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED, "{backend}");
        let session: Value = response.json().await.unwrap();
        let session = session["token"].as_str().unwrap().to_owned();

        let list = |key: String| {
            let client = client.clone();
            let url = format!("{}/runtimes?tenant_id={tenant}", server.v1_url());
            let tenant = tenant.clone();
            async move {
                let response = client
                    .get(url)
                    .header("x-api-key", key)
                    .header("X-Tenant-Id", tenant)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                response.json::<Vec<Value>>().await.unwrap()
            }
        };

        // The operator sees the tenant's full live inventory.
        let full = list(ROOT.to_owned()).await;
        let ids: Vec<&str> = full
            .iter()
            .map(|runtime| runtime["runtime_id"].as_str().unwrap())
            .collect();
        for id in [own, destination, draining, no_delegation, unverified] {
            assert!(ids.contains(&id.to_string().as_str()), "{backend}: {id}");
        }
        assert!(
            !ids.contains(&expired.to_string().as_str()),
            "{backend}: expired"
        );
        assert!(
            full.iter()
                .all(|runtime| runtime["regions"] == json!(["eu-west-1"])
                    && runtime["labels"] == json!({"residency": "eu"})),
            "{backend}: operators keep every fact"
        );

        // The device session sees one destination and only matching facts.
        let visible = list(session).await;
        assert_eq!(visible.len(), 1, "{backend}: {visible:?}");
        let mut seen = visible[0].clone();
        let object = seen.as_object_mut().unwrap();
        let expires_at = object.remove("expires_at").unwrap();
        let observed_at = object.remove("observed_at").unwrap();
        assert!(
            chrono::DateTime::parse_from_rfc3339(expires_at.as_str().unwrap()).unwrap()
                > chrono::Utc::now(),
            "{backend}"
        );
        assert!(observed_at.is_string(), "{backend}");
        assert_eq!(
            seen,
            json!({
                "runtime_id": destination.to_string(),
                "kind": "server",
                "trust": "registered",
                "handlers": [DELEGATION, "scan"],
                "offline_capable": false,
                "draining": false,
            }),
            "{backend}"
        );
        // Still the shape the mobile SDK deserializes.
        serde_json::from_value::<Vec<orch8_types::continuity::RuntimeCapabilities>>(json!(visible))
            .unwrap();
    }
}

/// `/mobile/sync` `step_delegations` resolve `credentials://` secrets and
/// return the plaintext to the caller. A stored `device` key ships inside
/// an app, so that is opt-in (`ORCH8_MOBILE_SYNC_RESOLVE_CREDENTIALS`).
#[tokio::test]
#[allow(clippy::too_many_lines)] // one scenario per backend, asserted inline
async fn stored_device_keys_get_resolved_credentials_only_when_opted_in() {
    let client = Client::new();
    for resolve in [false, true] {
        let mut servers = vec![(
            "sqlite",
            spawn_test_server_with(
                Arc::new(SqliteStorage::in_memory().await.unwrap()) as Arc<dyn StorageBackend>,
                TestServerOptions {
                    mobile_sync_resolve_credentials: resolve,
                    ..options()
                },
            )
            .await,
        )];
        if let Ok(url) = std::env::var("DATABASE_URL") {
            let pg = PostgresStorage::new(&url, 5, None).await.unwrap();
            pg.run_migrations().await.unwrap();
            servers.push((
                "postgres",
                spawn_test_server_with(
                    Arc::new(pg) as Arc<dyn StorageBackend>,
                    TestServerOptions {
                        mobile_sync_resolve_credentials: resolve,
                        ..options()
                    },
                )
                .await,
            ));
        }
        for (backend, server) in servers {
            let tenant = format!("ds-{}", Uuid::now_v7().simple());
            let device = format!("phone-{}", Uuid::now_v7().simple());
            let credential = format!("cred-{}", Uuid::now_v7().simple());
            let secret = format!("sk_live_{}", Uuid::now_v7().simple());
            let response = call(
                &client,
                &server,
                ROOT,
                &tenant,
                "/credentials",
                &json!({"id": credential, "name": "c", "kind": "api_key",
                        "value": secret, "tenant_id": tenant}),
            )
            .await;
            assert!(response.status().is_success(), "{backend}: credential");
            let device_key = mint_key(&client, &server, &tenant, &["device"]).await;
            let response = call(
                &client,
                &server,
                &device_key,
                &tenant,
                "/mobile/devices/register",
                &json!({"device_id": device, "platform": "ios"}),
            )
            .await;
            assert_eq!(response.status(), StatusCode::CREATED, "{backend}");

            let response = call(
                &client,
                &server,
                &device_key,
                &tenant,
                "/mobile/sync",
                &json!({"device_id": device, "step_delegations": [{
                    "request_id": "r1", "instance_id": "i1", "block_id": "b1",
                    "handler": "http_request",
                    "params": {"auth": format!("credentials://{credential}")},
                }]}),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK, "{backend}/{resolve}");
            let response = call(
                &client,
                &server,
                &device_key,
                &tenant,
                "/mobile/sync",
                &json!({"device_id": device}),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK, "{backend}/{resolve}");
            let body: Value = response.json().await.unwrap();
            let commands = body["commands"].as_array().unwrap();
            assert_eq!(commands.len(), 1, "{backend}/{resolve}: {body}");
            assert_eq!(commands[0]["type"], "step_result", "{backend}/{resolve}");
            let payload = &commands[0]["payload"];
            if resolve {
                assert_eq!(payload["success"], true, "{backend}: {payload}");
                assert_eq!(
                    payload["resolved_params"]["auth"],
                    json!(secret),
                    "{backend}"
                );
            } else {
                assert_eq!(payload["success"], false, "{backend}: {payload}");
                assert!(
                    payload["error"]
                        .as_str()
                        .unwrap()
                        .contains("ORCH8_MOBILE_SYNC_RESOLVE_CREDENTIALS"),
                    "{backend}: {payload}"
                );
                assert!(!body.to_string().contains(&secret), "{backend}: leaked");
            }
        }
    }
}
