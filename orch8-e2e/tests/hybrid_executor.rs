//! Hybrid mode end to end: the engine (API + scheduler + lease reaper +
//! gRPC) runs as "Orch8 Cloud" behind one HTTPS endpoint, and real
//! `orch8-server` processes run as remote executors in the "customer VPC",
//! started from a real `o8x1` join token with **no database, no API key and
//! no encryption key** — only `ORCH8_JOIN_TOKEN` plus executor-local
//! settings.
//!
//! Proven, on `SQLite` and (with `DATABASE_URL`) Postgres:
//!
//! 1. a residency-placed step runs on the executor, an unplaced step runs in
//!    the engine;
//! 2. the placed step's `credentials://` reference is resolved on the
//!    executor from its local credentials directory — the engine holds a
//!    decoy under the same id that is never used — and the secret appears in
//!    no engine table;
//! 3. with a BYOK vault on the executor, the large output field is sealed
//!    into the customer bucket, the engine stores only a reference, and a
//!    later placed step receives the plaintext on the executor;
//! 4. a tenant policy (`match.tag = vpc` → `require.labels.site = vpc`)
//!    routes tagged instances to executors and leaves untagged ones in the
//!    engine;
//! 5. a managed `drain` command makes an executor withdraw, release its
//!    in-flight task (effect `unknown`) and exit; the retry runs on an
//!    executor using the HTTP transport;
//! 6. SIGKILL of an executor mid-task: the engine's reaper reclaims the lease
//!    and another executor finishes the step; the effect ledger shows one
//!    `unknown` and one `committed` attempt, never two commits.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use orch8_e2e::{Backend, Cloud, CloudOptions, ExecutorProcess, ROOT_KEY, TlsFront, backends};
use orch8_storage::encrypting::ExternalPayloadVault as _;
use orch8_types::continuity::EffectState;
use orch8_types::join_token::JoinToken;
use serde_json::{Value, json};
use uuid::Uuid;

/// The value only the executor holds (its local credentials directory).
const LOCAL_SECRET: &str = "Bearer local-secret-7f3a91c2";
/// What the engine holds under the same credential id. Never used.
const CLOUD_DECOY: &str = "Bearer cloud-decoy-5b0e";
/// Tag the BYOK step sends (it is a step param, so the engine sees it).
const BYOK_TAG: &str = "byok-step";
/// Plaintext only the internal API produces; for the BYOK step it must stay
/// in the customer bucket.
const INTERNAL_REPORT: &str = "internal-plaintext-only-in-vpc-3c8e";
const BYOK_KEY_HEX: &str = "8f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0";

// ---------------------------------------------------------------------------
// The customer's internal API (reachable only from the "VPC")
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Delivery {
    path: String,
    authorization: String,
    idempotency_key: String,
}

#[derive(Default)]
struct ApiState {
    deliveries: Mutex<Vec<Delivery>>,
    applied: Mutex<BTreeMap<String, u32>>,
    gate: tokio::sync::Mutex<()>,
    gate_open: std::sync::atomic::AtomicBool,
    opened: tokio::sync::Notify,
}

struct InternalApi {
    base: String,
    state: Arc<ApiState>,
}

async fn deliver(state: &ApiState, path: &str, headers: &HeaderMap) -> (StatusCode, Json<Value>) {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    };
    let delivery = Delivery {
        path: path.to_owned(),
        authorization: header("authorization"),
        idempotency_key: header("idempotency-key"),
    };
    state
        .deliveries
        .lock()
        .expect("deliveries")
        .push(delivery.clone());
    if delivery.authorization != LOCAL_SECRET {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "bad credential"})),
        );
    }
    if path == "/slow" {
        loop {
            let notified = state.opened.notified();
            if state.gate_open.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            notified.await;
        }
        let _serial = state.gate.lock().await;
    }
    let duplicate = {
        let mut applied = state.applied.lock().expect("applied");
        let count = applied.entry(delivery.idempotency_key.clone()).or_insert(0);
        *count += 1;
        *count > 1
    };
    (
        StatusCode::OK,
        Json(json!({
            "charged": true,
            "deduplicated": duplicate,
            "report": format!("{}:{INTERNAL_REPORT}:{}", header("x-report-tag"), "x".repeat(96)),
        })),
    )
}

impl InternalApi {
    fn start(cloud: &Cloud) -> Self {
        let state = Arc::new(ApiState::default());
        let app = Router::new()
            .route(
                "/charge",
                post(|State(s): State<Arc<ApiState>>, h: HeaderMap| async move {
                    deliver(&s, "/charge", &h).await
                }),
            )
            .route(
                "/slow",
                post(|State(s): State<Arc<ApiState>>, h: HeaderMap| async move {
                    deliver(&s, "/slow", &h).await
                }),
            )
            .with_state(Arc::clone(&state));
        let listener = cloud
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .expect("bind internal api");
        let base = format!("http://{}", listener.local_addr().expect("addr"));
        cloud.handle().spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { base, state }
    }

    fn deliveries(&self, path: &str) -> Vec<Delivery> {
        self.state
            .deliveries
            .lock()
            .expect("deliveries")
            .iter()
            .filter(|d| d.path == path)
            .cloned()
            .collect()
    }

    fn set_gate(&self, open: bool) {
        self.state
            .gate_open
            .store(open, std::sync::atomic::Ordering::Release);
        if open {
            self.state.opened.notify_waiters();
        }
    }

    fn applied(&self, key: &str) -> u32 {
        self.state
            .applied
            .lock()
            .expect("applied")
            .get(key)
            .copied()
            .unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn wait_until<T>(what: &str, limit: Duration, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(value) = probe() {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {limit:?} waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn retry_policy() -> Value {
    json!({"max_attempts": 4, "initial_backoff": 100, "max_backoff": 400, "backoff_multiplier": 2.0})
}

fn create_tagged_instance(cloud: &Cloud, sequence: Uuid, data: &Value, tags: &[&str]) -> Uuid {
    let (status, body) = cloud.root(
        "POST",
        "/instances",
        Some(&json!({
            "sequence_id": sequence, "tenant_id": cloud.tenant, "namespace": "e2e",
            "context": {"data": data, "config": {}, "audit": []},
            "metadata": {"tags": tags},
        })),
    );
    assert_eq!(status, 201, "create instance: {body}");
    body["id"].as_str().expect("id").parse().expect("uuid")
}

fn replica(token: &JoinToken, host: &str) -> String {
    orch8_engine::remote_executor::replica_runtime_id(token.runtime_id, &token.worker_id(host))
        .to_string()
}

/// Wait until the executor `host` advertised itself (capability row with
/// the token's placement labels) — i.e. it joined over the worker protocol.
fn wait_registered(cloud: &Cloud, token: &JoinToken, host: &str) {
    let id = replica(token, host);
    wait_until(
        &format!("executor {host} to register"),
        Duration::from_secs(30),
        || {
            cloud
                .runtimes()
                .into_iter()
                .find(|r| r.runtime_id.to_string() == id && !r.draining)
                .filter(|r| r.labels.get("residency").map(String::as_str) == Some("eu-vpc"))
                .map(|_| ())
        },
    );
}

fn block_task(cloud: &Cloud, instance: Uuid, block: &str) -> Vec<orch8_types::worker::WorkerTask> {
    cloud
        .tasks(instance)
        .into_iter()
        .filter(|task| task.block_id.as_str() == block)
        .collect()
}

// ---------------------------------------------------------------------------
// The suite
// ---------------------------------------------------------------------------

#[test]
fn hybrid_remote_executors_end_to_end() {
    for backend in backends() {
        run(&backend);
    }
}

#[allow(clippy::too_many_lines)]
fn run(backend: &Backend) {
    let started = Instant::now();
    let cloud = Cloud::start_with(
        backend,
        |_| {},
        CloudOptions {
            scheduler: orch8_types::config::SchedulerConfig {
                tick_interval_ms: 50,
                worker_reaper_tick_secs: 1,
                worker_reaper_stale_secs: 4,
                ..orch8_types::config::SchedulerConfig::default()
            },
            grpc_auth: true,
        },
    );
    let front = TlsFront::start(&cloud);
    let api = InternalApi::start(&cloud);
    let dir = tempfile::tempdir().expect("tempdir");

    // Executor-local credentials (a mounted secret directory).
    let creds = dir.path().join("credentials");
    std::fs::create_dir_all(&creds).expect("credentials dir");
    std::fs::write(
        creds.join("vpc-api"),
        json!({"header": LOCAL_SECRET}).to_string(),
    )
    .expect("local credential");
    // The engine holds a *different* value under the same id: if it ever
    // resolved the reference itself, the internal API would see the decoy.
    let (status, body) = cloud.root(
        "POST",
        "/credentials",
        Some(
            &json!({"id": "vpc-api", "name": "decoy", "tenant_id": cloud.tenant,
                     "value": json!({"header": CLOUD_DECOY}).to_string()}),
        ),
    );
    assert!(status == 201 || status == 200, "decoy credential: {body}");

    // Cloud mints a dedicated worker key and a join token.
    let key = cloud.mint_key(&["worker"]);
    let token = JoinToken {
        v: 1,
        endpoint: front.endpoint.clone(),
        api_key: key,
        tenant_id: cloud.tenant.clone(),
        runtime_id: Uuid::now_v7(),
        worker_id_prefix: "acme-vpc".into(),
        labels: BTreeMap::from([
            ("residency".into(), "eu-vpc".into()),
            ("site".into(), "vpc".into()),
        ]),
        region: Some("eu-central-1".into()),
    };
    let encoded = token.encode();
    let bucket = dir.path().join("customer-bucket");
    let base_env = |extra: &[(&str, &str)]| -> Vec<(&'static str, String)> {
        let mut env: Vec<(&'static str, String)> = vec![
            (
                "ORCH8_EXECUTOR_CA_CERT",
                orch8_e2e::tls_fixture("ca.pem").display().to_string(),
            ),
            ("ORCH8_CREDENTIALS_DIR", creds.display().to_string()),
            ("ORCH8_ALLOWED_INTERNAL_CIDRS", "127.0.0.1/32".into()),
            ("ORCH8_EXECUTOR_POLL_INTERVAL_MS", "100".into()),
            ("ORCH8_EXECUTOR_HEARTBEAT_SECS", "1".into()),
            ("ORCH8_EXECUTOR_DRAIN_TIMEOUT_SECS", "1".into()),
        ];
        for (k, v) in extra {
            let key: &'static str = Box::leak((*k).to_owned().into_boxed_str());
            env.push((key, (*v).to_owned()));
        }
        env
    };
    let byok_env = [
        ("ORCH8_BYOK_LOCAL_PATH", bucket.to_str().expect("utf-8")),
        ("ORCH8_BYOK_STATIC_KEY", BYOK_KEY_HEX),
        ("ORCH8_BYOK_STATIC_KEY_ID", "customer-kms-key"),
        ("ORCH8_EXECUTOR_EXTERNALIZE_BYTES", "48"),
        ("ORCH8_EXECUTOR_TRANSPORT", "grpc"),
    ];

    // ---- 1-3: placement, executor-local credentials, BYOK (gRPC stream) ----
    let mut e1 = ExecutorProcess::spawn("e1", &encoded, &base_env(&byok_env), dir.path());
    wait_registered(&cloud, &token, "e1");
    let e1_id = replica(&token, "e1");

    let order = cloud.create_sequence(
        "hybrid-order",
        &json!([
            {"type": "step", "id": "cloud_prep", "handler": "transform",
             "params": {"order": "{{context.data.order}}"}},
            {"type": "step", "id": "vpc_charge", "handler": "http_request",
             "placement": {"residency": "eu-vpc"}, "retry": retry_policy(),
             "params": {
                "url": format!("{}/charge", api.base), "method": "POST",
                "headers": {
                    "Authorization": "credentials://vpc-api/header",
                    "Idempotency-Key": "{{context.data.order}}-charge",
                    "X-Report-Tag": BYOK_TAG,
                },
                "body": "order={{context.data.order}}"}},
            {"type": "step", "id": "vpc_digest", "handler": "transform",
             "placement": {"residency": "eu-vpc"},
             "params": {"report": "{{outputs.vpc_charge.body}}", "order": "{{context.data.order}}"}}
        ]),
    );
    let instance = cloud.create_instance(order, &json!({"order": "A-1"}));
    cloud.wait_state(instance, "completed", Duration::from_secs(60));

    // The unplaced step ran in the engine: output, no worker task.
    assert!(block_task(&cloud, instance, "cloud_prep").is_empty());
    assert_eq!(
        cloud.block_outputs(instance, "cloud_prep")[0]["output"]["order"],
        "A-1"
    );
    // The placed steps ran on e1, which resolved the credential locally.
    let charge = block_task(&cloud, instance, "vpc_charge");
    assert_eq!(charge.len(), 1, "one attempt");
    assert_eq!(charge[0].worker_id.as_deref(), Some(e1_id.as_str()));
    assert_eq!(
        charge[0].params["headers"]["Authorization"], "credentials://vpc-api/header",
        "the engine stores and ships the reference, never the value"
    );
    assert_eq!(
        charge[0].requirements.credentials,
        vec!["vpc-api".to_string()]
    );
    assert_eq!(charge[0].requirements.residency.as_deref(), Some("eu-vpc"));
    let deliveries = api.deliveries("/charge");
    assert_eq!(deliveries.len(), 1);
    assert_eq!(
        deliveries[0].authorization, LOCAL_SECRET,
        "executor-local value used"
    );
    let digest = block_task(&cloud, instance, "vpc_digest");
    assert_eq!(digest[0].worker_id.as_deref(), Some(e1_id.as_str()));

    // BYOK: the engine holds references; the plaintext is in the bucket.
    let charge_out = cloud.block_outputs(instance, "vpc_charge")[0]["output"].clone();
    assert_eq!(charge_out["status"], 200, "small fields stay readable");
    assert!(
        orch8_storage::encrypting::is_vault_reference(&charge_out["body"]),
        "{charge_out}"
    );
    assert_eq!(
        charge_out["body"]["_o8vault"]["kid"], "customer-kms-key",
        "wrapped by the customer's key"
    );
    let digest_out = cloud.block_outputs(instance, "vpc_digest")[0]["output"].clone();
    assert!(orch8_storage::encrypting::is_vault_reference(
        &digest_out["report"]
    ));
    // Open the resealed digest with the customer's vault: the second step
    // received the plaintext on the executor.
    let vault = orch8_storage::vault::PayloadVault::local(
        bucket.to_str().expect("utf-8"),
        "orch8",
        Arc::new(
            orch8_storage::vault::StaticKeyProvider::from_hex("customer-kms-key", BYOK_KEY_HEX)
                .expect("key"),
        ),
    )
    .expect("vault");
    let ref_key = digest_out["report"]["_o8vault"]["ref"]
        .as_str()
        .expect("ref key")
        .to_owned();
    let report = cloud
        .block_on(vault.open(
            orch8_types::ids::InstanceId::from_uuid(instance),
            &ref_key,
            &digest_out["report"],
        ))
        .expect("open sealed report");
    assert!(
        report.as_str().expect("report").contains(INTERNAL_REPORT),
        "{report}"
    );

    // Nothing the customer kept local reached any engine table.
    for needle in ["local-secret-7f3a91c2", INTERNAL_REPORT] {
        let hits = cloud.tables_containing(needle);
        assert!(
            hits.is_empty(),
            "`{needle}` found in engine tables {hits:?}"
        );
    }
    // The decoy lives only in the engine's credential table.
    let decoy = cloud.tables_containing("cloud-decoy-5b0e");
    assert!(
        decoy.iter().all(|table| table.contains("credential")),
        "{decoy:?}"
    );
    // The executor advertised credential names, never values.
    let e1_caps = cloud
        .runtimes()
        .into_iter()
        .find(|r| r.runtime_id.to_string() == e1_id)
        .expect("e1 runtime");
    assert_eq!(e1_caps.credentials, vec!["vpc-api".to_string()]);
    assert_eq!(e1_caps.regions, vec!["eu-central-1".to_string()]);
    assert!(e1_caps.hardware.contains(&"host:acme-vpc-e1".to_string()));
    eprintln!(
        "[{}] placement/credentials/BYOK ok ({:?})",
        backend.name(),
        started.elapsed()
    );

    // ---- 4-5: tenant policy, managed drain, HTTP transport --------------
    let (status, body) = cloud.root(
        "PUT",
        "/placement/policies",
        Some(
            &json!({"items": [{"name": "vpc-tagged", "match": {"tag": "vpc"},
                                "require": {"labels": {"site": "vpc"}}}]}),
        ),
    );
    assert_eq!(status, 200, "policies: {body}");
    let slow = cloud.create_sequence(
        "hybrid-slow",
        &json!([{"type": "step", "id": "vpc_slow", "handler": "http_request",
                 "retry": retry_policy(),
                 "params": {"url": format!("{}/slow", api.base), "method": "POST",
                            "headers": {"Authorization": "credentials://vpc-api/header",
                                        "Idempotency-Key": "{{context.data.order}}-slow",
                                        "X-Report-Tag": "open"},
                            "timeout_ms": 60000}}]),
    );
    api.set_gate(false);
    let drained = create_tagged_instance(&cloud, slow, &json!({"order": "D-1"}), &["vpc"]);
    wait_until("e1 to hold the drain task", Duration::from_secs(30), || {
        (api.deliveries("/slow").len() == 1).then_some(())
    });
    let held = block_task(&cloud, drained, "vpc_slow");
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].worker_id.as_deref(), Some(e1_id.as_str()));
    let held_id = held[0].id;
    // Operator (or Cloud) drains e1 through the managed-control channel.
    let (status, body) = cloud.root(
        "POST",
        "/workers/commands",
        Some(
            &json!({"worker_id": token.worker_id("e1"), "tenant_id": cloud.tenant,
                     "command": "drain"}),
        ),
    );
    assert_eq!(status, 201, "drain command: {body}");
    let exit = e1
        .wait_exit(Duration::from_secs(30))
        .expect("e1 exits after drain");
    assert!(exit.success(), "drained executor exits cleanly: {exit:?}");
    let e1_log = e1.log_text();
    assert!(e1_log.contains("draining"), "{e1_log}");
    let e1_caps = cloud
        .runtimes()
        .into_iter()
        .find(|r| r.runtime_id.to_string() == e1_id)
        .expect("e1 runtime");
    assert!(e1_caps.draining, "drain withdrew placement capability");
    // The in-flight attempt was released as started: effect unknown.
    let released = wait_until(
        "release of the drained attempt",
        Duration::from_secs(20),
        || {
            let receipts = cloud.block_receipts(drained, "vpc_slow");
            receipts
                .into_iter()
                .find(|r| r.state == EffectState::Unknown)
        },
    );

    // The retry runs on an executor over plain HTTP polling.
    api.set_gate(true);
    let h1 = ExecutorProcess::spawn(
        "h1",
        &encoded,
        &base_env(&[("ORCH8_EXECUTOR_TRANSPORT", "http")]),
        dir.path(),
    );
    let h1_id = replica(&token, "h1");
    cloud.wait_state(drained, "completed", Duration::from_secs(60));
    // The released attempt's lease history names e1 and the release reason;
    // the retry (a new task) was completed by h1 over HTTP.
    let events = cloud.attempt_events(held_id);
    assert!(
        events
            .iter()
            .any(|e| e.worker_id.as_deref() == Some(e1_id.as_str())
                && e.reason.as_deref().is_some_and(|r| r.contains("released"))),
        "{events:?}"
    );
    let tasks = block_task(&cloud, drained, "vpc_slow");
    let last = tasks.last().expect("retry task");
    assert_ne!(last.id, held_id);
    assert_eq!(last.worker_id.as_deref(), Some(h1_id.as_str()));
    let receipts = cloud.block_receipts(drained, "vpc_slow");
    let committed: Vec<_> = receipts
        .iter()
        .filter(|r| r.state == EffectState::Committed)
        .collect();
    assert_eq!(committed.len(), 1, "{receipts:?}");
    assert_ne!(committed[0].attempt, released.attempt);
    // Both attempts reached the provider; the first connection was dropped
    // mid-request (drain), so only the retry was applied.
    assert_eq!(
        api.deliveries("/slow")
            .iter()
            .filter(|d| d.idempotency_key == "D-1-slow")
            .count(),
        2
    );
    assert_eq!(api.applied("D-1-slow"), 1);

    // Policy: tagged instances go to executors, untagged stay in the engine.
    let plain = cloud.create_sequence(
        "hybrid-plain",
        &json!([{"type": "step", "id": "t", "handler": "transform", "params": {"v": 1}}]),
    );
    let tagged = create_tagged_instance(&cloud, plain, &json!({}), &["vpc"]);
    let untagged = create_tagged_instance(&cloud, plain, &json!({}), &[]);
    cloud.wait_state(tagged, "completed", Duration::from_secs(60));
    cloud.wait_state(untagged, "completed", Duration::from_secs(60));
    let tagged_tasks = block_task(&cloud, tagged, "t");
    assert_eq!(tagged_tasks.len(), 1);
    assert_eq!(tagged_tasks[0].worker_id.as_deref(), Some(h1_id.as_str()));
    assert_eq!(
        tagged_tasks[0]
            .requirements
            .labels
            .get("site")
            .map(String::as_str),
        Some("vpc")
    );
    assert!(block_task(&cloud, untagged, "t").is_empty());
    eprintln!(
        "[{}] policy/drain/http ok ({:?})",
        backend.name(),
        started.elapsed()
    );

    // ---- 6: SIGKILL → lease expiry → another executor finishes ----------
    drop(h1);
    let mut victim = ExecutorProcess::spawn(
        "k1",
        &encoded,
        &base_env(&[("ORCH8_EXECUTOR_TRANSPORT", "grpc")]),
        dir.path(),
    );
    wait_registered(&cloud, &token, "k1");
    let victim_id = replica(&token, "k1");
    api.set_gate(false);
    let killed = create_tagged_instance(&cloud, slow, &json!({"order": "K-1"}), &["vpc"]);
    wait_until(
        "the victim to hold the task",
        Duration::from_secs(30),
        || {
            api.deliveries("/slow")
                .iter()
                .any(|d| d.idempotency_key == "K-1-slow")
                .then_some(())
        },
    );
    let held = block_task(&cloud, killed, "vpc_slow");
    assert_eq!(held[0].worker_id.as_deref(), Some(victim_id.as_str()));
    let held_id = held[0].id;
    let survivor = ExecutorProcess::spawn(
        "s1",
        &encoded,
        &base_env(&[("ORCH8_EXECUTOR_TRANSPORT", "grpc")]),
        dir.path(),
    );
    wait_registered(&cloud, &token, "s1");
    let survivor_id = replica(&token, "s1");
    let killed_at = Instant::now();
    victim.kill();
    api.set_gate(true);
    let reclaimed = wait_until("the reaper to reclaim", Duration::from_secs(30), || {
        cloud
            .block_receipts(killed, "vpc_slow")
            .into_iter()
            .find(|r| r.state == EffectState::Unknown)
            .map(|_| killed_at.elapsed())
    });
    cloud.wait_state(killed, "completed", Duration::from_secs(60));
    let recovered = killed_at.elapsed();
    let events = cloud.attempt_events(held_id);
    assert!(
        events.iter().any(|e| e
            .reason
            .as_deref()
            .is_some_and(|r| r.contains("lease expired"))),
        "reclaimed by the reaper: {events:?}"
    );
    let tasks = block_task(&cloud, killed, "vpc_slow");
    let last = tasks.last().expect("retry task");
    assert_ne!(last.id, held_id);
    assert_eq!(last.worker_id.as_deref(), Some(survivor_id.as_str()));
    let receipts = cloud.block_receipts(killed, "vpc_slow");
    let ledger: Vec<(u32, EffectState)> = receipts.iter().map(|r| (r.attempt, r.state)).collect();
    assert_eq!(
        receipts
            .iter()
            .filter(|r| r.state == EffectState::Committed)
            .count(),
        1,
        "never two commits: {ledger:?}"
    );
    let mut attempts: Vec<u32> = receipts.iter().map(|r| r.attempt).collect();
    attempts.sort_unstable();
    attempts.dedup();
    assert_eq!(
        attempts.len(),
        receipts.len(),
        "one receipt per attempt: {ledger:?}"
    );
    // Both attempts reached the provider; the first connection was dropped
    // mid-request (kill), so only the retry was applied.
    assert_eq!(
        api.deliveries("/slow")
            .iter()
            .filter(|d| d.idempotency_key == "K-1-slow")
            .count(),
        2
    );
    assert_eq!(api.applied("K-1-slow"), 1);
    eprintln!(
        "[{}] kill-executor: detection {:?}, recovery {:?}, ledger {ledger:?} (total {:?})",
        backend.name(),
        reclaimed,
        recovered,
        started.elapsed()
    );
    drop(survivor);
    let _ = ROOT_KEY;
}
