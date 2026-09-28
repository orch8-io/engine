//! Browser-session tokens and runtime identity binding, end to end through
//! the real auth middleware (root API key configured). Runs on `SQLite` and,
//! when `DATABASE_URL` is set, on Postgres.
#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use chrono::Utc;
use orch8_api::test_harness::{BackendTestServer, spawn_test_server_on};
use orch8_storage::StorageBackend;
use orch8_storage::postgres::PostgresStorage;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::continuity::CapsuleRequirements;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::worker::{WorkerTask, WorkerTaskState};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

const ROOT: &str = "root-key-for-browser-session-tests";

async fn servers() -> Vec<(&'static str, BackendTestServer)> {
    let mut out = vec![(
        "sqlite",
        spawn_test_server_on(
            Arc::new(SqliteStorage::in_memory().await.unwrap()) as Arc<dyn StorageBackend>,
            Some(ROOT),
        )
        .await,
    )];
    if let Ok(url) = std::env::var("DATABASE_URL") {
        let pg = PostgresStorage::new(&url, 5, None).await.unwrap();
        pg.run_migrations().await.unwrap();
        out.push((
            "postgres",
            spawn_test_server_on(Arc::new(pg) as Arc<dyn StorageBackend>, Some(ROOT)).await,
        ));
    }
    out
}

fn tenant() -> String {
    format!("bs-{}", Uuid::now_v7().simple())
}

async fn seed_task(
    server: &BackendTestServer,
    client: &Client,
    tenant: &str,
    handler: &str,
) -> WorkerTask {
    let sequence_id = Uuid::now_v7();
    let response = client
        .post(format!("{}/sequences", server.v1_url()))
        .header("x-api-key", ROOT)
        .header("X-Tenant-Id", tenant)
        .json(&json!({
            "id": sequence_id, "tenant_id": tenant, "namespace": "browser",
            "name": format!("browser-{sequence_id}"), "version": 1, "deprecated": false,
            "blocks": [{"type": "step", "id": "read", "handler": handler, "params": {}, "cancellable": true}],
            "interceptors": null, "created_at": Utc::now().to_rfc3339()
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = client
        .post(format!("{}/instances", server.v1_url()))
        .header("x-api-key", ROOT)
        .header("X-Tenant-Id", tenant)
        .json(&json!({
            "sequence_id": sequence_id, "tenant_id": tenant, "namespace": "browser",
            "context": {"data": {"page": "checkout"}, "config": {"stripe": "sk_live_x"}, "audit": []}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let instance: Uuid = response.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let now = Utc::now();
    let task = WorkerTask {
        id: Uuid::now_v7(),
        instance_id: InstanceId::from_uuid(instance),
        block_id: BlockId::new("read"),
        handler_name: handler.into(),
        queue_name: None,
        requirements: CapsuleRequirements::default(),
        params: json!({"selector": "#total"}),
        context: json!({"data": {"page": "checkout"}, "config": {"stripe": "sk_live_x"}}),
        attempt: 0,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        claim_epoch: 0,
        resume_checkpoint: None,
        checkpoint_seq: 0,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: now,
        effect_id: None,
        continuity_epoch: None,
        lease_secs: None,
        carries_credentials: false,
        claimed_runtime_kind: None,
    };
    server.storage.create_worker_task(&task).await.unwrap();
    task
}

async fn mint(
    server: &BackendTestServer,
    client: &Client,
    tenant: &str,
    body: Value,
) -> reqwest::Response {
    client
        .post(format!("{}/runtimes/browser-sessions", server.v1_url()))
        .header("x-api-key", ROOT)
        .header("X-Tenant-Id", tenant)
        .json(&body)
        .send()
        .await
        .unwrap()
}

fn caps(runtime_id: &str, kind: &str, handler: &str) -> Value {
    let now = Utc::now();
    json!({
        "runtime_id": runtime_id, "kind": kind, "trust": "registered",
        "handlers": [handler], "offline_capable": false, "connectivity": "wifi",
        "observed_at": now.to_rfc3339(),
        "expires_at": (now + chrono::Duration::seconds(240)).to_rfc3339()
    })
}

#[tokio::test]
async fn browser_session_token_is_scoped_to_the_lease_protocol() {
    let client = Client::new();
    for (backend, server) in servers().await {
        let tenant = tenant();
        let handler = format!("read_dom_{}", Uuid::now_v7().simple());
        let task = seed_task(&server, &client, &tenant, &handler).await;

        let minted = mint(&server, &client, &tenant, json!({"handlers": [handler]})).await;
        assert_eq!(minted.status(), StatusCode::CREATED, "{backend}");
        let minted: Value = minted.json().await.unwrap();
        let token = minted["token"].as_str().unwrap().to_owned();
        let runtime_id = minted["runtime_id"].as_str().unwrap().to_owned();
        assert!(token.starts_with("bst_"), "{backend}");
        assert!(
            chrono::DateTime::parse_from_rfc3339(minted["expires_at"].as_str().unwrap()).is_ok(),
            "{backend}: RFC 3339 expiry"
        );

        // Nothing outside the lease protocol.
        for (method, path) in [
            (reqwest::Method::GET, "/workers/tasks".to_owned()),
            (reqwest::Method::GET, "/workers/tasks/stats".to_owned()),
            (reqwest::Method::POST, "/workers/commands".to_owned()),
            (reqwest::Method::GET, "/instances".to_owned()),
            (
                reqwest::Method::POST,
                "/runtimes/browser-sessions".to_owned(),
            ),
        ] {
            let response = client
                .request(method.clone(), format!("{}{path}", server.v1_url()))
                .header("x-api-key", &token)
                .json(&json!({"handlers": [handler]}))
                .send()
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::FORBIDDEN,
                "{backend}: {method} {path}"
            );
        }

        let poll = |body: Value| {
            client
                .post(format!("{}/workers/tasks/poll", server.v1_url()))
                .header("x-api-key", &token)
                .json(&body)
                .send()
        };
        // Identity conflicts with the binding are refused.
        let other_runtime = Uuid::now_v7().to_string();
        for body in [
            json!({"handler_name": "charge_card", "worker_id": runtime_id}),
            json!({"handler_name": handler, "worker_id": other_runtime}),
            json!({"handler_name": handler, "worker_id": runtime_id,
                   "capabilities": caps(&runtime_id, "server", &handler)}),
        ] {
            assert_eq!(
                poll(body).await.unwrap().status(),
                StatusCode::FORBIDDEN,
                "{backend}"
            );
        }

        // A matching poll claims as a browser: filtered context, 30s lease.
        let response = poll(json!({
            "handler_name": handler, "worker_id": runtime_id,
            "capabilities": caps(&runtime_id, "browser", &handler)
        }))
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{backend}");
        let body: Value = response.json().await.unwrap();
        let claimed = &body["tasks"][0];
        assert_eq!(claimed["id"], task.id.to_string(), "{backend}");
        assert_eq!(claimed["lease_secs"], 30, "{backend}");
        assert!(claimed["context"].get("config").is_none(), "{backend}");
        let epoch = claimed["claim_epoch"].as_u64().unwrap();

        // Oversized page data is refused; lease mutations for another
        // runtime identity are refused; the bound runtime completes.
        let too_big = "x".repeat(orch8_api::DEFAULT_BROWSER_OUTPUT_MAX_BYTES + 1);
        let complete = |worker: &str, output: Value| {
            client
                .post(format!(
                    "{}/workers/tasks/{}/complete",
                    server.v1_url(),
                    task.id
                ))
                .header("authorization", format!("Bearer {token}"))
                .json(&json!({"worker_id": worker, "claim_epoch": epoch, "output": output}))
                .send()
        };
        assert_eq!(
            complete(&runtime_id, json!({"html": too_big}))
                .await
                .unwrap()
                .status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "{backend}"
        );
        assert_eq!(
            complete(&other_runtime, json!({"total": 42}))
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN,
            "{backend}"
        );
        assert_eq!(
            complete(&runtime_id, json!({"total": 42}))
                .await
                .unwrap()
                .status(),
            StatusCode::OK,
            "{backend}: Bearer is accepted too"
        );

        // Provenance (runtime kind + id) is recorded next to the output.
        let audit = server
            .storage
            .list_audit_log(task.instance_id, 100)
            .await
            .unwrap();
        let provenance = audit
            .iter()
            .find(|entry| entry.event_type == "worker_output_provenance")
            .expect("provenance recorded");
        assert_eq!(provenance.details["runtime_kind"], "browser", "{backend}");
        assert_eq!(provenance.details["runtime_id"], runtime_id, "{backend}");
    }
}

#[tokio::test]
async fn minting_requires_an_operator_and_bounded_ttl() {
    let client = Client::new();
    for (backend, server) in servers().await {
        let tenant = tenant();
        assert_eq!(
            mint(
                &server,
                &client,
                &tenant,
                json!({"handlers": ["h"], "ttl_secs": 3601})
            )
            .await
            .status(),
            StatusCode::BAD_REQUEST,
            "{backend}"
        );
        assert_eq!(
            mint(&server, &client, &tenant, json!({"handlers": []}))
                .await
                .status(),
            StatusCode::BAD_REQUEST,
            "{backend}"
        );
        // A worker-only key cannot mint (the route is operator-only).
        let key: Value = client
            .post(format!("{}/api-keys", server.v1_url()))
            .header("x-api-key", ROOT)
            .json(&json!({"tenant_id": tenant, "name": "w", "capabilities": ["worker"]}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let worker_key = key["secret"].as_str().unwrap();
        let response = client
            .post(format!("{}/runtimes/browser-sessions", server.v1_url()))
            .header("x-api-key", worker_key)
            .json(&json!({"handlers": ["h"]}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{backend}");
        // The reserved capability can't be granted to a stored key.
        let response = client
            .post(format!("{}/api-keys", server.v1_url()))
            .header("x-api-key", ROOT)
            .json(&json!({"tenant_id": tenant, "capabilities": ["browser_worker"]}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{backend}");
    }
}

#[tokio::test]
async fn expired_or_forged_tokens_are_unauthorized() {
    let client = Client::new();
    for (backend, server) in servers().await {
        let tenant = tenant();
        let minted: Value = mint(
            &server,
            &client,
            &tenant,
            json!({"handlers": ["h"], "ttl_secs": 1}),
        )
        .await
        .json()
        .await
        .unwrap();
        let token = minted["token"].as_str().unwrap().to_owned();
        let runtime_id = minted["runtime_id"].as_str().unwrap().to_owned();
        tokio::time::sleep(std::time::Duration::from_millis(2_100)).await;
        let response = client
            .post(format!("{}/workers/tasks/poll", server.v1_url()))
            .header("x-api-key", &token)
            .json(&json!({"handler_name": "h", "worker_id": runtime_id}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{backend}: expired"
        );
        let forged = format!("{}x", &token[..token.len() - 1]);
        let response = client
            .post(format!("{}/workers/tasks/poll", server.v1_url()))
            .header("x-api-key", forged)
            .json(&json!({"handler_name": "h", "worker_id": runtime_id}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{backend}: forged"
        );
    }
}
