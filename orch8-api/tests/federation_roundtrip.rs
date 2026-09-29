//! Two-engine federation round trip over real HTTP.
//!
//! Engine A (tenant `acme`) runs a `federate` step that starts sequence `kyc`
//! at engine B (tenant `globex`) through B's signature-authenticated inbound
//! route, parks on its `wait_for_input` gate, and resumes with B's declared
//! outputs. Also covers disclosure minimization, idempotent restarts and
//! envelope replays, mutual allowlists, and cancel propagation for a
//! cross-cluster child.

use std::sync::Arc;
use std::time::Duration;

use orch8_api::test_harness::{TestServer, spawn_federation_test_server};
use orch8_engine::Engine;
use orch8_engine::federation::{FederationClient, identity_for, sign_message};
use orch8_engine::handlers::HandlerRegistry;
use orch8_storage::StorageBackend;
use orch8_types::config::SchedulerConfig;
use orch8_types::federation::{
    FEDERATION_PROTOCOL_VERSION, FederationCallState, FederationRequest, FederationRequestKind,
    derive_call_id,
};
use orch8_types::ids::{BlockId, InstanceId, TenantId};
use orch8_types::instance::InstanceState;
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const KEY_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const KEY_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

struct Node {
    server: TestServer,
    storage: Arc<dyn StorageBackend>,
    identity: Value,
    tenant: &'static str,
    cancel: CancellationToken,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

fn signing_key(master: &str) -> ed25519_dalek::SigningKey {
    orch8_api::ContinuityCrypto::from_master_key(master)
        .unwrap()
        .signing_key
}

async fn start_node(master: &str, tenant: &'static str, http: &reqwest::Client) -> Node {
    let server = spawn_federation_test_server(master).await;
    let storage: Arc<dyn StorageBackend> = server.storage.clone();
    let cancel = CancellationToken::new();

    let mut handlers = HandlerRegistry::new();
    orch8_engine::handlers::builtin::register_builtins(&mut handlers);
    let config = SchedulerConfig {
        tick_interval_ms: 25,
        ..SchedulerConfig::default()
    };
    let engine = Engine::new(Arc::clone(&storage), config, handlers, cancel.clone());
    tokio::spawn(async move {
        let _ = engine.run().await;
    });
    let client = Arc::new(FederationClient::new(signing_key(master), true).unwrap());
    tokio::spawn(orch8_engine::federation::run_poller(
        Arc::clone(&storage),
        client,
        Duration::from_millis(50),
        cancel.clone(),
    ));

    let identity: Value = http
        .get(format!("{}/federation/identity", server.v1_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    Node {
        server,
        storage,
        identity,
        tenant,
        cancel,
    }
}

async fn register(http: &reqwest::Client, on: &Node, peer: &Node, body: Value) -> StatusCode {
    let mut body = body;
    body["peer_id"] = peer.identity["peer_id"].clone();
    body["public_key"] = peer.identity["public_key"].clone();
    body["endpoint"] = json!(peer.server.base_url);
    body["remote_tenant_id"] = json!(peer.tenant);
    http.post(format!("{}/federation/peers", on.server.v1_url()))
        .header("X-Tenant-Id", on.tenant)
        .json(&body)
        .send()
        .await
        .unwrap()
        .status()
}

async fn create_sequence(http: &reqwest::Client, node: &Node, name: &str, blocks: Value) -> Uuid {
    let id = Uuid::now_v7();
    let resp = http
        .post(format!("{}/sequences", node.server.v1_url()))
        .header("X-Tenant-Id", node.tenant)
        .json(&json!({
            "id": id,
            "tenant_id": node.tenant,
            "namespace": "default",
            "name": name,
            "version": 1,
            "deprecated": false,
            "blocks": blocks,
            "created_at": chrono::Utc::now().to_rfc3339(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "{}",
        resp.text().await.unwrap()
    );
    id
}

async fn start_instance(http: &reqwest::Client, node: &Node, seq: Uuid, data: Value) -> InstanceId {
    let resp = http
        .post(format!("{}/instances", node.server.v1_url()))
        .header("X-Tenant-Id", node.tenant)
        .json(&json!({
            "sequence_id": seq,
            "tenant_id": node.tenant,
            "namespace": "default",
            "context": { "data": data, "config": {}, "audit": [] }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body: Value = resp.json().await.unwrap();
    InstanceId::from_uuid(body["id"].as_str().unwrap().parse().unwrap())
}

async fn wait_for_state(
    storage: &Arc<dyn StorageBackend>,
    id: InstanceId,
    want: &[InstanceState],
) -> InstanceState {
    for _ in 0..400 {
        let state = storage.get_instance(id).await.unwrap().unwrap().state;
        if want.contains(&state) {
            return state;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!(
        "instance {id} never reached {want:?}; last state {:?}",
        storage.get_instance(id).await.unwrap().unwrap().state
    );
}

fn federate_step(peer: &str, sequence: &str) -> Value {
    json!({
        "type": "step",
        "id": "remote",
        "handler": "federate",
        "params": {
            "peer": peer,
            "sequence": sequence,
            "input": {
                "customer_id": "{{ context.data.customer_id }}",
                "ssn": "{{ context.data.ssn }}"
            }
        },
        "wait_for_input": { "prompt": "waiting for the peer", "timeout": 60_000 },
        "cancellable": true
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)] // one end-to-end scenario, asserted step by step
async fn two_engines_federate_a_sequence_and_return_only_declared_outputs() {
    let http = reqwest::Client::new();
    let a = start_node(KEY_A, "acme", &http).await;
    let b = start_node(KEY_B, "globex", &http).await;
    assert_ne!(a.identity["peer_id"], b.identity["peer_id"]);

    assert_eq!(
        register(
            &http,
            &a,
            &b,
            json!({
                "name": "globex",
                "outbound": { "sequences": ["kyc"], "disclosed_fields": ["customer_id"] }
            })
        )
        .await,
        StatusCode::CREATED
    );
    assert_eq!(
        register(
            &http,
            &b,
            &a,
            json!({
                "name": "acme",
                "inbound": { "sequences": ["kyc"], "returned_outputs": ["verdict"] }
            })
        )
        .await,
        StatusCode::CREATED
    );

    create_sequence(&http, &b, "kyc", json!([
        { "type": "step", "id": "verdict", "handler": "transform",
          "params": { "approved": true, "customer": "{{ context.data.customer_id }}" }, "cancellable": true },
        { "type": "step", "id": "internal_notes", "handler": "transform",
          "params": { "risk_model": "v7-private" }, "cancellable": true }
    ]))
    .await;
    let onboard = create_sequence(
        &http,
        &a,
        "onboard",
        json!([federate_step("globex", "kyc")]),
    )
    .await;
    let instance = start_instance(
        &http,
        &a,
        onboard,
        json!({"customer_id": "c-42", "ssn": "123-45-6789"}),
    )
    .await;

    wait_for_state(&a.storage, instance, &[InstanceState::Completed]).await;

    // A: the block output carries only B's declared output.
    let output = a
        .storage
        .get_block_output(instance, &BlockId::new("remote"))
        .await
        .unwrap()
        .unwrap()
        .output;
    assert_eq!(output["state"], "completed");
    assert_eq!(output["outputs"]["verdict"]["approved"], true);
    assert_eq!(output["outputs"]["verdict"]["customer"], "c-42");
    assert!(
        output["outputs"].get("internal_notes").is_none(),
        "undeclared output leaked: {output}"
    );
    assert_eq!(output["withheld_fields"], 1);

    // B: exactly one federated instance; the undeclared field never arrived.
    let remote_id = InstanceId::from_uuid(
        output["remote_instance_id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap(),
    );
    let remote = b.storage.get_instance(remote_id).await.unwrap().unwrap();
    assert_eq!(remote.tenant_id.as_str(), "globex");
    assert_eq!(remote.context.data, json!({"customer_id": "c-42"}));
    assert_eq!(
        remote.metadata["federation"]["peer_id"],
        a.identity["peer_id"]
    );
    assert!(
        remote.metadata["federation"].get("parent").is_none(),
        "organization peers get no parent link"
    );

    // Idempotency: re-sending `start` for the same call (a fresh envelope, as
    // a poller retry would) and replaying one envelope verbatim both map to
    // the same remote instance.
    let tenant = TenantId::new("acme").unwrap();
    let call_id = derive_call_id(&tenant, instance, &BlockId::new("remote"));
    let call = a
        .storage
        .get_federation_call(&tenant, call_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(call.state, FederationCallState::Completed);
    assert!(call.notified);
    let key = signing_key(KEY_A);
    let a_id = identity_for(&key).peer_id;
    let b_id = identity_for(&signing_key(KEY_B)).peer_id;
    let request = FederationRequest {
        v: FEDERATION_PROTOCOL_VERSION,
        kind: FederationRequestKind::Start,
        call_id,
        sequence: Some("kyc".into()),
        sequence_version: None,
        input: Some(json!({"customer_id": "c-42"})),
        parent: None,
    };
    let payload = serde_json::to_vec(&request).unwrap();
    let message = sign_message(
        &key,
        a_id,
        b_id,
        TenantId::new("globex").unwrap(),
        call_id,
        &payload,
        chrono::Utc::now(),
    );
    for _ in 0..2 {
        let resp = http
            .post(format!("{}/api/v1/federation/inbound", b.server.base_url))
            .json(&message)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
    let fresh = sign_message(
        &key,
        a_id,
        b_id,
        TenantId::new("globex").unwrap(),
        call_id,
        &payload,
        chrono::Utc::now(),
    );
    assert_eq!(
        http.post(format!("{}/api/v1/federation/inbound", b.server.base_url))
            .json(&fresh)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let globex = TenantId::new("globex").unwrap();
    let started = b
        .storage
        .list_instances(
            &orch8_types::filter::InstanceFilter {
                tenant_id: Some(globex.clone()),
                ..Default::default()
            },
            &orch8_types::filter::Pagination::default(),
        )
        .await
        .unwrap();
    assert_eq!(started.len(), 1, "duplicate remote runs: {started:?}");

    // Tampering or an unknown signer is refused uniformly.
    let mut tampered = message.clone();
    tampered.payload_base64 = base64_encode(br#"{"v":1,"kind":"start"}"#);
    let stranger = sign_message(
        &signing_key(&"cc".repeat(32)),
        identity_for(&signing_key(&"cc".repeat(32))).peer_id,
        b_id,
        globex,
        call_id,
        &payload,
        chrono::Utc::now(),
    );
    for bad in [tampered, stranger] {
        let resp = http
            .post(format!("{}/api/v1/federation/inbound", b.server.base_url))
            .json(&bad)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn callee_allowlist_is_enforced_even_when_the_caller_allows_it() {
    let http = reqwest::Client::new();
    let a = start_node(KEY_A, "acme", &http).await;
    let b = start_node(KEY_B, "globex", &http).await;
    register(
        &http,
        &a,
        &b,
        json!({
            "name": "globex",
            "outbound": { "sequences": ["payroll"], "disclosed_fields": ["customer_id"] }
        }),
    )
    .await;
    register(
        &http,
        &b,
        &a,
        json!({
            "name": "acme",
            "inbound": { "sequences": ["kyc"], "returned_outputs": [] }
        }),
    )
    .await;
    create_sequence(
        &http,
        &b,
        "payroll",
        json!([
            { "type": "step", "id": "pay", "handler": "noop", "params": {}, "cancellable": true }
        ]),
    )
    .await;
    let seq = create_sequence(
        &http,
        &a,
        "try-payroll",
        json!([federate_step("globex", "payroll")]),
    )
    .await;
    let instance = start_instance(&http, &a, seq, json!({"customer_id": "c-1"})).await;
    wait_for_state(&a.storage, instance, &[InstanceState::Failed]).await;
    let started = b
        .storage
        .list_instances(
            &orch8_types::filter::InstanceFilter::default(),
            &orch8_types::filter::Pagination::default(),
        )
        .await
        .unwrap();
    assert!(
        started.is_empty(),
        "callee started a sequence outside its allowlist"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_the_parent_cancels_the_cross_cluster_child() {
    let http = reqwest::Client::new();
    let a = start_node(KEY_A, "acme", &http).await;
    let b = start_node(KEY_B, "acme", &http).await;
    register(
        &http,
        &a,
        &b,
        json!({
            "name": "eu-cluster",
            "relationship": "cluster",
            "outbound": { "sequences": ["long-job"], "disclosed_fields": ["*"] }
        }),
    )
    .await;
    register(
        &http,
        &b,
        &a,
        json!({
            "name": "us-cluster",
            "relationship": "cluster",
            "inbound": { "sequences": ["long-job"], "returned_outputs": ["*"] }
        }),
    )
    .await;
    // The child parks on an approval that never arrives.
    create_sequence(
        &http,
        &b,
        "long-job",
        json!([
            { "type": "step", "id": "hold", "handler": "noop", "params": {},
              "wait_for_input": { "prompt": "hold", "timeout": 600_000 }, "cancellable": true }
        ]),
    )
    .await;
    let seq = create_sequence(
        &http,
        &a,
        "parent",
        json!([federate_step("eu-cluster", "long-job")]),
    )
    .await;
    let parent = start_instance(&http, &a, seq, json!({"customer_id": "c-9", "ssn": "x"})).await;

    // Wait until the child exists and is parked.
    let tenant = TenantId::new("acme").unwrap();
    let call_id = derive_call_id(&tenant, parent, &BlockId::new("remote"));
    let mut child = None;
    for _ in 0..400 {
        if let Some(call) = a
            .storage
            .get_federation_call(&tenant, call_id)
            .await
            .unwrap()
            && let Some(remote) = call.remote_instance_id
        {
            child = Some(InstanceId::from_uuid(remote));
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let child = child.expect("child was never started");
    wait_for_state(&b.storage, child, &[InstanceState::Waiting]).await;
    let child_row = b.storage.get_instance(child).await.unwrap().unwrap();
    // Same organization: "*" disclosure and the parent link are honoured.
    assert_eq!(child_row.context.data["ssn"], "x");
    assert_eq!(
        child_row.metadata["federation"]["parent"]["instance_id"],
        json!(parent)
    );

    // Cancel the parent through the public API.
    let resp = http
        .patch(format!("{}/instances/{parent}/state", a.server.v1_url()))
        .header("X-Tenant-Id", "acme")
        .json(&json!({ "state": "cancelled" }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "{}", resp.text().await.unwrap());

    wait_for_state(&b.storage, child, &[InstanceState::Cancelled]).await;
    let call = a
        .storage
        .get_federation_call(&tenant, call_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(call.state, FederationCallState::Cancelled);
}
