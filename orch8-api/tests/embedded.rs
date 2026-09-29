//! Sub-tenants, pooled limits, metering, scoped embed tokens, theme,
//! per-sub-tenant rollouts and soft license enforcement — end to end through
//! the real auth middleware (root API key configured). Runs on `SQLite` and,
//! when `DATABASE_URL` is set, on Postgres.
#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use ed25519_dalek::{Signer, SigningKey};
use orch8_api::embed::{EmbedSigner, EmbeddedRuntime};
use orch8_api::license::{License, SoftEnforcer};
use orch8_api::test_harness::{BackendTestServer, TestServerOptions, spawn_test_server_with};
use orch8_storage::StorageBackend;
use orch8_storage::postgres::PostgresStorage;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::instance::InstanceState;
use reqwest::{Client, RequestBuilder, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

const ROOT: &str = "root-key-for-embedded-tests";
const SECRET: &str = "5f1e0c3a9b7d24e68f0a1c2b3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60";

struct Case {
    name: &'static str,
    server: BackendTestServer,
    client: Client,
}

impl Case {
    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.server.v1_url())
    }

    fn get(&self, path: &str, tenant: &str) -> RequestBuilder {
        admin(self.client.get(self.url(path)), tenant)
    }

    fn post(&self, path: &str, tenant: &str) -> RequestBuilder {
        admin(self.client.post(self.url(path)), tenant)
    }

    fn put(&self, path: &str, tenant: &str) -> RequestBuilder {
        admin(self.client.put(self.url(path)), tenant)
    }

    fn embed_get(&self, path: &str, token: &str) -> RequestBuilder {
        self.client.get(self.url(path)).bearer_auth(token)
    }

    fn embed_post(&self, path: &str, token: &str) -> RequestBuilder {
        self.client.post(self.url(path)).bearer_auth(token)
    }
}

fn admin(builder: RequestBuilder, tenant: &str) -> RequestBuilder {
    builder
        .header("x-api-key", ROOT)
        .header("x-tenant-id", tenant)
}

fn runtime(license: License) -> EmbeddedRuntime {
    let mut runtime = EmbeddedRuntime::with_signer(EmbedSigner::from_hex(SECRET).unwrap(), license);
    runtime.enforcer = SoftEnforcer::with_ttl(std::time::Duration::ZERO);
    runtime
}

async fn cases_with(embedded: Option<EmbeddedRuntime>) -> Vec<Case> {
    let embedded = embedded.map(Arc::new);
    let options = TestServerOptions {
        root_api_key: Some(ROOT.into()),
        embedded: embedded.clone(),
        ..TestServerOptions::default()
    };
    let mut out = vec![Case {
        name: "sqlite",
        server: spawn_test_server_with(
            Arc::new(SqliteStorage::in_memory().await.unwrap()) as Arc<dyn StorageBackend>,
            options.clone(),
        )
        .await,
        client: Client::new(),
    }];
    if let Ok(url) = std::env::var("DATABASE_URL") {
        let pg = PostgresStorage::new(&url, 5, None).await.unwrap();
        pg.run_migrations().await.unwrap();
        out.push(Case {
            name: "postgres",
            server: spawn_test_server_with(Arc::new(pg) as Arc<dyn StorageBackend>, options).await,
            client: Client::new(),
        });
    }
    out
}

async fn cases() -> Vec<Case> {
    cases_with(Some(runtime(License::unlicensed()))).await
}

fn tenant() -> String {
    format!("emb-{}", Uuid::now_v7().simple())
}

/// Create a sequence; returns (id, name).
async fn sequence(
    case: &Case,
    tenant: &str,
    sub: Option<&str>,
    namespace: &str,
    extra: Value,
) -> (Uuid, String) {
    let id = Uuid::now_v7();
    let name = format!("seq-{}", id.simple());
    let mut body = json!({
        "id": id, "tenant_id": tenant, "namespace": namespace, "name": name, "version": 1,
        "created_at": Utc::now(),
        "blocks": [
            {"type": "step", "id": "fetch", "handler": "noop",
             "params": {"url": "https://internal.vendor.example/secret"}},
            {"type": "step", "id": "review", "handler": "noop", "params": {},
             "wait_for_input": {"prompt": "Ship it?", "choices": [
                {"label": "Approve", "value": "approve"}, {"label": "Reject", "value": "reject"}]}},
            {"type": "step", "id": "summary", "handler": "noop", "params": {}}
        ]
    });
    if let (Some(obj), Some(extra)) = (body.as_object_mut(), extra.as_object()) {
        for (k, v) in extra {
            obj.insert(k.clone(), v.clone());
        }
    }
    let mut request = case.post("/sequences", tenant).json(&body);
    if let Some(sub) = sub {
        request = request.header("x-orch8-sub-tenant", sub);
    }
    let response = request.send().await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED, "{}", case.name);
    (id, name)
}

async fn start(case: &Case, tenant: &str, seq: Uuid, sub: Option<&str>) -> reqwest::Response {
    let mut request = case.post("/instances", tenant).json(&json!({
        "sequence_id": seq, "tenant_id": tenant, "namespace": "default",
        "context": {"data": {"email": "secret@customer.example"}}
    }));
    if let Some(sub) = sub {
        request = request.header("x-orch8-sub-tenant", sub);
    }
    request.send().await.unwrap()
}

async fn start_ok(case: &Case, tenant: &str, seq: Uuid, sub: Option<&str>) -> Uuid {
    let response = start(case, tenant, seq, sub).await;
    assert_eq!(response.status(), StatusCode::CREATED, "{}", case.name);
    response.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

async fn mint(case: &Case, tenant: &str, sub: &str, scopes: &[&str], seqs: Value) -> String {
    let response = case
        .post("/embed/tokens", tenant)
        .json(&json!({"sub_tenant": sub, "scopes": scopes, "sequences": seqs, "ttl_seconds": 600}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED, "{}", case.name);
    let body: Value = response.json().await.unwrap();
    assert!(body["expires_at"].is_string());
    body["token"].as_str().unwrap().to_string()
}

async fn error_code(response: reqwest::Response) -> String {
    response.json::<Value>().await.unwrap()["error"]["code"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

#[tokio::test]
async fn sub_tenant_header_scopes_instances_and_lists() {
    for case in cases().await {
        let t = tenant();
        let (seq, _) = sequence(&case, &t, None, "default", json!({})).await;
        let acme = start_ok(&case, &t, seq, Some("acme")).await;
        start_ok(&case, &t, seq, Some("globex")).await;
        start_ok(&case, &t, seq, None).await;

        let listed: Value = case
            .get("/instances?sub_tenant=acme", &t)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let items = listed["items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "{}", case.name);
        assert_eq!(items[0]["sub_tenant"], "acme", "{}", case.name);

        // The header scopes lists and single reads.
        let listed: Value = case
            .get("/instances?sub_tenant=acme", &t)
            .header("x-orch8-sub-tenant", "globex")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(listed["items"][0]["sub_tenant"], "globex", "{}", case.name);
        let foreign = case
            .get(&format!("/instances/{acme}"), &t)
            .header("x-orch8-sub-tenant", "globex")
            .send()
            .await
            .unwrap();
        assert_eq!(foreign.status(), StatusCode::NOT_FOUND, "{}", case.name);
        let own = case
            .get(&format!("/instances/{acme}"), &t)
            .header("x-orch8-sub-tenant", "acme")
            .send()
            .await
            .unwrap();
        assert_eq!(own.status(), StatusCode::OK, "{}", case.name);

        let all: Value = case
            .get("/instances", &t)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(all["items"].as_array().unwrap().len(), 3, "{}", case.name);

        // Malformed header and header/body mismatch.
        let bad = start(&case, &t, seq, Some("a b")).await;
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST, "{}", case.name);
        let mismatch = case
            .post("/instances", &t)
            .header("x-orch8-sub-tenant", "acme")
            .json(
                &json!({"sequence_id": seq, "tenant_id": t, "namespace": "default",
                "sub_tenant": "globex"}),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(mismatch.status(), StatusCode::FORBIDDEN, "{}", case.name);

        // Batches target one sub-tenant.
        let batch = case
            .post("/instances/batch", &t)
            .header("x-orch8-sub-tenant", "acme")
            .json(&json!({"instances": [
                {"sequence_id": seq, "tenant_id": t, "namespace": "default"},
                {"sequence_id": seq, "tenant_id": t, "namespace": "default"}]}))
            .send()
            .await
            .unwrap();
        assert_eq!(batch.status(), StatusCode::CREATED, "{}", case.name);
        let mixed = case
            .post("/instances/batch", &t)
            .json(&json!({"instances": [
                {"sequence_id": seq, "tenant_id": t, "namespace": "default", "sub_tenant": "a"},
                {"sequence_id": seq, "tenant_id": t, "namespace": "default", "sub_tenant": "b"}]}))
            .send()
            .await
            .unwrap();
        assert_eq!(mixed.status(), StatusCode::BAD_REQUEST, "{}", case.name);
    }
}

#[tokio::test]
async fn sub_tenant_caps_reject_with_their_own_code_and_usage_is_metered() {
    for case in cases().await {
        let t = tenant();
        let (seq, _) = sequence(&case, &t, None, "default", json!({})).await;
        let limits = case
            .put("/sub-tenants/acme/limits", &t)
            .json(&json!({"max_concurrent": 1, "max_executions_per_month": null}))
            .send()
            .await
            .unwrap();
        assert_eq!(limits.status(), StatusCode::OK, "{}", case.name);
        let got: Value = case
            .get("/sub-tenants/acme/limits", &t)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(got["max_concurrent"], 1, "{}", case.name);
        assert!(got["max_executions_per_month"].is_null(), "{}", case.name);
        let unknown_field = case
            .put("/sub-tenants/acme/limits", &t)
            .json(&json!({"max": 1}))
            .send()
            .await
            .unwrap();
        assert!(unknown_field.status().is_client_error(), "{}", case.name);

        start_ok(&case, &t, seq, Some("acme")).await;
        let rejected = start(&case, &t, seq, Some("acme")).await;
        assert_eq!(
            rejected.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "{}",
            case.name
        );
        assert_eq!(
            error_code(rejected).await,
            "sub_tenant_quota_exceeded",
            "{}",
            case.name
        );
        // Other sub-tenants and tenant-level runs are unaffected.
        start_ok(&case, &t, seq, Some("globex")).await;
        start_ok(&case, &t, seq, None).await;

        case.put("/sub-tenants/initech/limits", &t)
            .json(&json!({"max_executions_per_month": 1}))
            .send()
            .await
            .unwrap();
        start_ok(&case, &t, seq, Some("initech")).await;
        let monthly = start(&case, &t, seq, Some("initech")).await;
        assert_eq!(
            monthly.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "{}",
            case.name
        );

        let usage: Value = case
            .get("/usage/sub-tenants", &t)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(usage["active_sub_tenants"], 3, "{}: {usage}", case.name);
        let items = usage["items"].as_array().unwrap();
        let acme = items.iter().find(|i| i["sub_tenant"] == "acme").unwrap();
        assert_eq!(acme["executions_started"], 1, "{}", case.name);
        assert!(acme["last_active_at"].is_string(), "{}", case.name);
        let bad_window = case
            .get(
                "/usage/sub-tenants?from=2026-09-02T00:00:00Z&to=2026-09-01T00:00:00Z",
                &t,
            )
            .send()
            .await
            .unwrap();
        assert_eq!(
            bad_window.status(),
            StatusCode::BAD_REQUEST,
            "{}",
            case.name
        );

        // Another tenant sees none of it.
        let other: Value = case
            .get("/usage/sub-tenants", &tenant())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(other["active_sub_tenants"], 0, "{}", case.name);
    }
}

#[tokio::test]
async fn embed_routes_are_404_without_a_secret() {
    for case in cases_with(None).await {
        let t = tenant();
        let mint = case
            .post("/embed/tokens", &t)
            .json(&json!({"sub_tenant": "acme", "scopes": ["runs:read"]}))
            .send()
            .await
            .unwrap();
        assert_eq!(mint.status(), StatusCode::NOT_FOUND, "{}", case.name);
        let runs = case
            .embed_get("/embed/runs", "o8e1.e30.AAAA")
            .send()
            .await
            .unwrap();
        assert_eq!(runs.status(), StatusCode::NOT_FOUND, "{}", case.name);
    }
}

#[tokio::test]
async fn embed_tokens_are_bound_to_tenant_sub_tenant_and_scopes() {
    for case in cases().await {
        let t = tenant();
        let (seq, name) = sequence(
            &case,
            &t,
            None,
            "default",
            json!({"embed": {"visible_outputs": ["summary"]}}),
        )
        .await;
        let acme_run = start_ok(&case, &t, seq, Some("acme")).await;
        let globex_run = start_ok(&case, &t, seq, Some("globex")).await;
        start_ok(&case, &t, seq, None).await;

        let token = mint(&case, &t, "acme", &["runs:read", "runs:start"], Value::Null).await;
        assert!(token.starts_with("o8e1."));

        let runs: Value = case
            .embed_get("/embed/runs", &token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let items = runs["items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "{}: {runs}", case.name);
        assert_eq!(items[0]["id"], acme_run.to_string(), "{}", case.name);
        assert_eq!(items[0]["sequence"], name, "{}", case.name);
        assert!(items[0].get("context").is_none(), "{}", case.name);

        // Another sub-tenant's run is indistinguishable from a missing one.
        let foreign = case
            .embed_get(&format!("/embed/runs/{globex_run}"), &token)
            .send()
            .await
            .unwrap();
        assert_eq!(foreign.status(), StatusCode::NOT_FOUND, "{}", case.name);

        // Detail: no context, outputs only for opted-in steps.
        let storage = &case.server.storage;
        for (block, output) in [
            ("fetch", json!({"secret": "hunter2"})),
            ("summary", json!({"done": true})),
        ] {
            storage
                .save_block_output(&orch8_types::output::BlockOutput {
                    id: Uuid::now_v7(),
                    instance_id: InstanceId::from_uuid(acme_run),
                    block_id: BlockId::new(block.to_string()),
                    output,
                    output_ref: None,
                    output_size: 16,
                    attempt: 0,
                    created_at: Utc::now(),
                })
                .await
                .unwrap();
        }
        let detail: Value = case
            .embed_get(&format!("/embed/runs/{acme_run}"), &token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let text = detail.to_string();
        assert!(
            !text.contains("secret@customer.example"),
            "{}: {text}",
            case.name
        );
        assert!(!text.contains("hunter2"), "{}: {text}", case.name);
        let steps = detail["steps"].as_array().unwrap();
        let summary = steps.iter().find(|s| s["id"] == "summary").unwrap();
        assert_eq!(summary["output"], json!({"done": true}), "{}", case.name);
        let fetch = steps.iter().find(|s| s["id"] == "fetch").unwrap();
        assert!(fetch.get("output").is_none(), "{}", case.name);

        // Start a run through the embed surface: stamped with the sub-tenant.
        let started = case
            .embed_post("/embed/runs", &token)
            .json(&json!({"sequence": name, "input": {"plan": "pro"}}))
            .send()
            .await
            .unwrap();
        assert_eq!(started.status(), StatusCode::CREATED, "{}", case.name);
        let id: Uuid = started.json::<Value>().await.unwrap()["id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let instance = storage
            .get_instance(InstanceId::from_uuid(id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            instance.sub_tenant.as_deref(),
            Some("acme"),
            "{}",
            case.name
        );

        // Scope and sequence allowlist enforcement.
        let read_only = mint(&case, &t, "acme", &["runs:read"], Value::Null).await;
        let denied = case
            .embed_post("/embed/runs", &read_only)
            .json(&json!({"sequence": name}))
            .send()
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN, "{}", case.name);
        assert_eq!(
            error_code(denied).await,
            "embed_scope_denied",
            "{}",
            case.name
        );
        let narrowed = mint(&case, &t, "acme", &["runs:read"], json!(["other-seq"])).await;
        let hidden: Value = case
            .embed_get("/embed/runs", &narrowed)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(
            hidden["items"].as_array().unwrap().is_empty(),
            "{}",
            case.name
        );

        // Another tenant's token sees nothing of this tenant.
        let other_tenant = mint(&case, &tenant(), "acme", &["runs:read"], Value::Null).await;
        let cross = case
            .embed_get(&format!("/embed/runs/{acme_run}"), &other_tenant)
            .send()
            .await
            .unwrap();
        assert_eq!(cross.status(), StatusCode::NOT_FOUND, "{}", case.name);

        // Tampered, garbage and missing tokens are 401.
        let (head, sig) = token.rsplit_once('.').unwrap();
        let (_, payload) = head.split_once('.').unwrap();
        let mut claims: Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).unwrap()).unwrap();
        claims["sub"] = json!("globex");
        let forged = format!(
            "o8e1.{}.{sig}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
        );
        for bad in [forged.as_str(), "o8e1.garbage.garbage"] {
            let response = case.embed_get("/embed/runs", bad).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{}", case.name);
        }
        let anonymous = case
            .client
            .get(case.url("/embed/runs"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            anonymous.status(),
            StatusCode::UNAUTHORIZED,
            "{}",
            case.name
        );

        // An embed token is not a credential for the management API, and an
        // API key is not a credential for embed-token routes.
        let mgmt = case.embed_get("/instances", &token).send().await.unwrap();
        assert_eq!(mgmt.status(), StatusCode::UNAUTHORIZED, "{}", case.name);
        let mint_with_token = case
            .embed_post("/embed/tokens", &token)
            .json(&json!({"sub_tenant": "acme", "scopes": ["runs:read"]}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            mint_with_token.status(),
            StatusCode::UNAUTHORIZED,
            "{}",
            case.name
        );
        let key_on_embed = case.get("/embed/runs", &t).send().await.unwrap();
        assert_eq!(
            key_on_embed.status(),
            StatusCode::UNAUTHORIZED,
            "{}",
            case.name
        );

        // Mint validation.
        for body in [
            json!({"sub_tenant": "a b", "scopes": ["runs:read"]}),
            json!({"sub_tenant": "acme", "scopes": []}),
            json!({"sub_tenant": "acme", "scopes": ["runs:read"], "ttl_seconds": 3601}),
            json!({"sub_tenant": "acme", "scopes": ["runs:delete"]}),
        ] {
            let response = case
                .post("/embed/tokens", &t)
                .json(&body)
                .send()
                .await
                .unwrap();
            assert!(response.status().is_client_error(), "{}: {body}", case.name);
        }
    }
}

#[tokio::test]
async fn embed_approvals_list_resolve_and_conflict() {
    for case in cases().await {
        let t = tenant();
        let (seq, _) = sequence(&case, &t, None, "default", json!({})).await;
        let run = start_ok(&case, &t, seq, Some("acme")).await;
        let other = start_ok(&case, &t, seq, Some("globex")).await;
        let storage = &case.server.storage;
        storage
            .save_block_output(&orch8_types::output::BlockOutput {
                id: Uuid::now_v7(),
                instance_id: InstanceId::from_uuid(run),
                block_id: BlockId::new("fetch".to_string()),
                output: json!({}),
                output_ref: None,
                output_size: 2,
                attempt: 0,
                created_at: Utc::now(),
            })
            .await
            .unwrap();
        for id in [run, other] {
            storage
                .update_instance_state(InstanceId::from_uuid(id), InstanceState::Waiting, None)
                .await
                .unwrap();
        }

        let token = mint(&case, &t, "acme", &["approvals:resolve"], Value::Null).await;
        let list: Value = case
            .embed_get("/embed/approvals", &token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let items = list["items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "{}: {list}", case.name);
        let approval = &items[0];
        assert_eq!(approval["instance_id"], run.to_string(), "{}", case.name);
        assert_eq!(approval["step_id"], "review", "{}", case.name);
        assert_eq!(approval["prompt"], "Ship it?", "{}", case.name);
        assert_eq!(approval["choices"][0]["value"], "approve", "{}", case.name);
        let approval_id = approval["id"].as_str().unwrap();

        let bad_choice = case
            .embed_post(&format!("/embed/approvals/{approval_id}"), &token)
            .json(&json!({"choice": "maybe"}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            bad_choice.status(),
            StatusCode::BAD_REQUEST,
            "{}",
            case.name
        );

        let resolved = case
            .embed_post(&format!("/embed/approvals/{approval_id}"), &token)
            .json(&json!({"choice": "approve", "comment": "lgtm"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resolved.status(), StatusCode::ACCEPTED, "{}", case.name);
        let signals = storage
            .get_pending_signals(InstanceId::from_uuid(run))
            .await
            .unwrap();
        assert_eq!(signals.len(), 1, "{}", case.name);
        assert_eq!(signals[0].payload["value"], "approve", "{}", case.name);
        assert_eq!(
            signals[0].payload["decided_by"]["sub_tenant"], "acme",
            "{}",
            case.name
        );

        // Once the gate is no longer pending the widget gets 409.
        storage
            .update_instance_state(InstanceId::from_uuid(run), InstanceState::Running, None)
            .await
            .unwrap();
        let again = case
            .embed_post(&format!("/embed/approvals/{approval_id}"), &token)
            .json(&json!({"choice": "approve"}))
            .send()
            .await
            .unwrap();
        assert_eq!(again.status(), StatusCode::CONFLICT, "{}", case.name);

        // Another sub-tenant's approval id is a 404, even with a valid token.
        let globex_list: Value = case
            .embed_get(
                "/embed/approvals",
                &mint(&case, &t, "globex", &["approvals:resolve"], Value::Null).await,
            )
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let globex_id = globex_list["items"][0]["id"].as_str().unwrap().to_string();
        let cross = case
            .embed_post(&format!("/embed/approvals/{globex_id}"), &token)
            .json(&json!({"choice": "approve"}))
            .send()
            .await
            .unwrap();
        assert_eq!(cross.status(), StatusCode::NOT_FOUND, "{}", case.name);
    }
}

#[tokio::test]
async fn embed_sequences_visibility_builder_and_gallery() {
    for case in cases().await {
        let t = tenant();
        let (_, shared) = sequence(&case, &t, None, "default", json!({})).await;
        let (_, owned) = sequence(&case, &t, Some("acme"), "default", json!({})).await;
        let (_, foreign) = sequence(&case, &t, Some("globex"), "default", json!({})).await;
        let (_, gallery) = sequence(
            &case,
            &t,
            None,
            "embed-gallery",
            json!({"embed": {"gallery": true, "title": "Onboarding", "template": "onboarding-v1"}}),
        )
        .await;

        let narrowed = mint(&case, &t, "acme", &["sequences:read"], json!([owned])).await;
        let list: Value = case
            .embed_get("/embed/sequences", &narrowed)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let names: Vec<&str> = list["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&owned.as_str()), "{}: {list}", case.name);
        assert!(names.contains(&gallery.as_str()), "{}: {list}", case.name);
        assert!(
            !names.contains(&shared.as_str()),
            "allowlist narrows tenant-level"
        );
        assert!(!names.contains(&foreign.as_str()), "{}", case.name);
        assert!(
            list["handlers"].as_array().unwrap().is_empty(),
            "no builder scope"
        );
        let g = list["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|i| i["name"] == gallery.as_str())
            .unwrap();
        assert_eq!(g["gallery"], true, "{}", case.name);
        assert_eq!(g["title"], "Onboarding", "{}", case.name);
        assert_eq!(g["owned"], false, "{}", case.name);

        let builder = mint(
            &case,
            &t,
            "acme",
            &["sequences:read", "builder:edit"],
            Value::Null,
        )
        .await;
        let list: Value = case
            .embed_get("/embed/sequences", &builder)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(
            !list["handlers"].as_array().unwrap().is_empty(),
            "{}",
            case.name
        );

        // Tenant-level definitions are redacted; owned ones are not.
        let shared_def: Value = case
            .embed_get(&format!("/embed/sequences/{shared}"), &builder)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(shared_def["redacted"], true, "{}", case.name);
        assert!(
            !shared_def.to_string().contains("internal.vendor.example"),
            "{}",
            case.name
        );
        let owned_def: Value = case
            .embed_get(&format!("/embed/sequences/{owned}"), &builder)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(owned_def["owned"], true, "{}", case.name);
        assert!(
            owned_def["definition"]["blocks"].is_array(),
            "{}",
            case.name
        );
        let foreign_def = case
            .embed_get(&format!("/embed/sequences/{foreign}"), &builder)
            .send()
            .await
            .unwrap();
        assert_eq!(foreign_def.status(), StatusCode::NOT_FOUND, "{}", case.name);

        // Builder writes: new owned names and new versions of owned ones.
        let blocks = json!([{"type": "step", "id": "a", "handler": "noop", "params": {}}]);
        let created = case
            .client
            .put(case.url("/embed/sequences/my-flow"))
            .bearer_auth(&builder)
            .json(&json!({"blocks": blocks, "tenant_id": "someone-else", "sub_tenant": "globex"}))
            .send()
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED, "{}", case.name);
        let v2 = case
            .client
            .put(case.url("/embed/sequences/my-flow"))
            .bearer_auth(&builder)
            .json(&json!({"blocks": blocks}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            v2.json::<Value>().await.unwrap()["version"],
            2,
            "{}",
            case.name
        );
        let stored: Value = case
            .embed_get("/embed/sequences/my-flow", &builder)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(stored["definition"]["tenant_id"], t, "{}", case.name);
        assert_eq!(stored["definition"]["sub_tenant"], "acme", "{}", case.name);

        for (path, expected) in [
            (format!("/embed/sequences/{shared}"), StatusCode::CONFLICT),
            (format!("/embed/sequences/{foreign}"), StatusCode::CONFLICT),
            (
                format!("/embed/sequences/{gallery}?namespace=embed-gallery"),
                StatusCode::FORBIDDEN,
            ),
        ] {
            let response = case
                .client
                .put(case.url(&path))
                .bearer_auth(&builder)
                .json(&json!({"blocks": blocks}))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "{}: {path}", case.name);
        }
        let reader = mint(&case, &t, "acme", &["sequences:read"], Value::Null).await;
        let no_builder = case
            .client
            .put(case.url("/embed/sequences/another"))
            .bearer_auth(&reader)
            .json(&json!({"blocks": blocks}))
            .send()
            .await
            .unwrap();
        assert_eq!(no_builder.status(), StatusCode::FORBIDDEN, "{}", case.name);
    }
}

fn license_key(signing: &SigningKey, features: &[&str]) -> String {
    let now = Utc::now().timestamp();
    let payload = json!({"v": 1, "licensee": "Vendor", "edition": "embedded",
        "features": features, "max_sub_tenants": null,
        "issued_at": now, "expires_at": now + 86_400});
    let b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap());
    let signed = format!("o8l1.{b64}");
    let sig = signing.sign(signed.as_bytes());
    format!("{signed}.{}", URL_SAFE_NO_PAD.encode(sig.to_bytes()))
}

#[tokio::test]
async fn theme_is_normalised_and_badge_needs_white_label() {
    let signing = SigningKey::from_bytes(&rand::random::<[u8; 32]>());
    let licensed = License::verify(
        &license_key(&signing, &["white_label"]),
        &signing.verifying_key(),
    );
    for (licensed, cases) in [
        (false, cases().await),
        (true, cases_with(Some(runtime(licensed.clone()))).await),
    ] {
        for case in cases {
            let t = tenant();
            let put = case
                .put("/embed/theme", &t)
                .json(
                    &json!({"css_vars": {"--orch8-accent": "#0af", "radius": "6px"},
                    "logo_url": "https://cdn.example.com/logo.svg", "hide_badge": true}),
                )
                .send()
                .await
                .unwrap();
            assert_eq!(put.status(), StatusCode::OK, "{}", case.name);
            let token = mint(&case, &t, "acme", &["runs:read"], Value::Null).await;
            let theme: Value = case
                .embed_get("/embed/theme", &token)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(
                theme["css_vars"],
                json!({"accent": "#0af", "radius": "6px"})
            );
            assert_eq!(theme["hide_badge"], licensed, "{}", case.name);
            let injection = case
                .put("/embed/theme", &t)
                .json(&json!({"css_vars": {"accent": "red;} body{display:none"}}))
                .send()
                .await
                .unwrap();
            assert_eq!(injection.status(), StatusCode::BAD_REQUEST, "{}", case.name);
            let license: Value = case
                .get("/license", &t)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(
                license["status"],
                if licensed { "licensed" } else { "unlicensed" },
                "{}",
                case.name
            );
        }
    }
}

#[tokio::test]
async fn unlicensed_sub_tenant_use_is_flagged_but_never_blocked() {
    for case in cases().await {
        let t = tenant();
        let (seq, _) = sequence(&case, &t, None, "default", json!({})).await;
        for sub in ["s1", "s2", "s3", "s4"] {
            start_ok(&case, &t, seq, Some(sub)).await;
        }
        let response = case.get("/license", &t).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{}", case.name);
        assert_eq!(
            response.headers().get("x-orch8-license").unwrap(),
            "unlicensed",
            "{}",
            case.name
        );
        // Still admitted.
        start_ok(&case, &t, seq, Some("s5")).await;
    }
}

#[tokio::test]
async fn release_target_routes_by_sub_tenant() {
    for case in cases().await {
        let t = tenant();
        let (baseline, name) = sequence(&case, &t, None, "default", json!({})).await;
        let candidate = Uuid::now_v7();
        let response = case
            .post("/sequences", &t)
            .json(
                &json!({"id": candidate, "tenant_id": t, "namespace": "default", "name": name,
                "version": 2, "created_at": Utc::now(),
                "blocks": [{"type": "step", "id": "only", "handler": "noop", "params": {}}]}),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED, "{}", case.name);

        let bad = case
            .post("/releases", &t)
            .json(&json!({"tenant_id": t, "baseline_sequence_id": baseline,
                "candidate_sequence_id": candidate, "target": {"percentage": 101}}))
            .send()
            .await
            .unwrap();
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST, "{}", case.name);

        let release: Value = case
            .post("/releases", &t)
            .json(&json!({"tenant_id": t, "baseline_sequence_id": baseline,
                "candidate_sequence_id": candidate,
                "target": {"sub_tenants": ["design-partner"], "percentage": 0}}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let id = release["id"].as_str().unwrap().to_string();
        assert_eq!(release["target"]["sub_tenants"][0], "design-partner");
        case.post(&format!("/releases/{id}/validate"), &t)
            .json(&json!({"skip": true}))
            .send()
            .await
            .unwrap();
        let canary = case
            .post(&format!("/releases/{id}/canary"), &t)
            .json(&json!({"percent": 100}))
            .send()
            .await
            .unwrap();
        assert_eq!(canary.status(), StatusCode::OK, "{}", case.name);

        let storage = &case.server.storage;
        let sequence_of = |run: Uuid| {
            let storage = storage.clone();
            async move {
                storage
                    .get_instance(InstanceId::from_uuid(run))
                    .await
                    .unwrap()
                    .unwrap()
                    .sequence_id
                    .into_uuid()
            }
        };
        // The canary is at 100%, but with a target only the listed
        // sub-tenant gets the candidate; unlisted sub-tenants and tenant-level
        // runs stay on the baseline.
        let partner = start_ok(&case, &t, baseline, Some("design-partner")).await;
        assert_eq!(sequence_of(partner).await, candidate, "{}", case.name);
        let other = start_ok(&case, &t, baseline, Some("acme")).await;
        assert_eq!(sequence_of(other).await, baseline, "{}", case.name);
        let plain = start_ok(&case, &t, baseline, None).await;
        assert_eq!(sequence_of(plain).await, baseline, "{}", case.name);

        // Retarget: 100% of sub-tenants.
        let retarget = case
            .put(&format!("/releases/{id}/target"), &t)
            .json(&json!({"percentage": 100}))
            .send()
            .await
            .unwrap();
        assert_eq!(retarget.status(), StatusCode::OK, "{}", case.name);
        let acme = start_ok(&case, &t, baseline, Some("acme")).await;
        assert_eq!(sequence_of(acme).await, candidate, "{}", case.name);
        let decisions: Value = case
            .get(&format!("/releases/{id}/decisions"), &t)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(
            decisions
                .as_array()
                .unwrap()
                .iter()
                .any(|d| d["reason"].as_str().unwrap().contains("rollout target")),
            "{}",
            case.name
        );

        // Clearing the target returns to per-instance cohorts (100% canary).
        case.put(&format!("/releases/{id}/target"), &t)
            .json(&Value::Null)
            .send()
            .await
            .unwrap();
        let plain = start_ok(&case, &t, baseline, None).await;
        assert_eq!(sequence_of(plain).await, candidate, "{}", case.name);
    }
}
