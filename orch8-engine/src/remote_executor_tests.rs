use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::json;

use super::*;
use crate::remote_worker::lease::ConfirmedLease;

/// In-memory lease transport: hands out queued tasks once, records every
/// mutation.
#[derive(Default)]
struct FakeTransport {
    queue: Mutex<Vec<RemoteTask>>,
    calls: Mutex<Vec<(String, uuid::Uuid, Value)>>,
    advertisements: Mutex<Vec<RuntimeCapabilities>>,
    /// `complete` answers `Retry` this many times before accepting.
    retry_completes: AtomicUsize,
    /// Override for every `heartbeat` answer (default `Accepted`).
    heartbeat_response: Mutex<Option<LeaseResponse>>,
    ignore_capacity: std::sync::atomic::AtomicBool,
    claim_calls: AtomicUsize,
    release_gate: Mutex<Option<Arc<Semaphore>>>,
    releases_active: AtomicUsize,
    peak_releases: AtomicUsize,
    heartbeat_gate: Mutex<Option<Arc<Semaphore>>>,
    complete_gate: Mutex<Option<Arc<Semaphore>>>,
}

impl FakeTransport {
    fn push(&self, task: RemoteTask) {
        self.queue.lock().unwrap().push(task);
    }

    fn calls(&self, verb: &str) -> Vec<(uuid::Uuid, Value)> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(v, _, _)| v == verb)
            .map(|(_, id, body)| (*id, body.clone()))
            .collect()
    }

    fn record(&self, verb: &str, task: &ClaimedTask, body: Value) {
        self.calls
            .lock()
            .unwrap()
            .push((verb.to_string(), task.task.id, body));
    }
}

#[async_trait]
impl LeaseTransport for FakeTransport {
    fn name(&self) -> &'static str {
        "fake"
    }

    async fn advertise(&self, capabilities: &RuntimeCapabilities) -> Result<(), String> {
        self.advertisements
            .lock()
            .unwrap()
            .push(capabilities.clone());
        Ok(())
    }

    async fn claim(
        &self,
        handlers: &[String],
        capacity: u32,
        _capabilities: &RuntimeCapabilities,
    ) -> Result<Claimed, String> {
        self.claim_calls.fetch_add(1, Ordering::SeqCst);
        let mut queue = self.queue.lock().unwrap();
        let mut tasks = Vec::new();
        let limit = if self.ignore_capacity.load(Ordering::SeqCst) {
            usize::MAX
        } else {
            capacity as usize
        };
        while tasks.len() < limit {
            let Some(index) = queue
                .iter()
                .position(|task| handlers.contains(&task.handler_name))
            else {
                break;
            };
            tasks.push(ClaimedTask {
                task: queue.remove(index),
                session: 0,
            });
        }
        Ok(Claimed {
            tasks,
            poll_after: None,
            heartbeat: Some(Duration::from_millis(50)),
        })
    }

    async fn heartbeat(&self, task: &ClaimedTask) -> LeaseResponse {
        self.record("heartbeat", task, Value::Null);
        let gate = self.heartbeat_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            let _permit = gate.acquire().await.unwrap();
        }
        self.heartbeat_response
            .lock()
            .unwrap()
            .unwrap_or(LeaseResponse::Accepted)
    }

    async fn complete(&self, task: &ClaimedTask, output: &Value) -> LeaseResponse {
        self.record("complete", task, output.clone());
        let gate = self.complete_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            let _permit = gate.acquire().await.unwrap();
        }
        if self
            .retry_completes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_ok()
        {
            return LeaseResponse::Retry;
        }
        LeaseResponse::Accepted
    }

    async fn fail(&self, task: &ClaimedTask, message: &str, retryable: bool) -> LeaseResponse {
        self.record(
            "fail",
            task,
            json!({"message": message, "retryable": retryable}),
        );
        LeaseResponse::Accepted
    }

    async fn release(&self, task: &ClaimedTask, started: bool) -> LeaseResponse {
        struct Active<'a>(&'a AtomicUsize);
        impl Drop for Active<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let active = self.releases_active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_releases.fetch_max(active, Ordering::SeqCst);
        let _active = Active(&self.releases_active);
        self.record(
            "release",
            task,
            json!({"started": started, "claim_epoch": task.task.claim_epoch}),
        );
        let gate = self.release_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            let _permit = gate.acquire().await.unwrap();
        }
        LeaseResponse::Accepted
    }
}

/// Vault double: seals into a map keyed by `(instance, ref_key)`.
#[derive(Default)]
struct MemoryVault {
    objects: Mutex<HashMap<(InstanceId, String), Value>>,
}

#[async_trait]
impl ExternalPayloadVault for MemoryVault {
    async fn seal(
        &self,
        instance_id: InstanceId,
        ref_key: &str,
        value: &Value,
    ) -> Result<Value, orch8_types::error::StorageError> {
        self.objects
            .lock()
            .unwrap()
            .insert((instance_id, ref_key.to_string()), value.clone());
        Ok(
            json!({ VAULT_REF_KEY: { "v": 1, "object": format!("obj/{ref_key}"), "kid": "k", "dek": "d", "alg": "A256GCM" } }),
        )
    }

    async fn open(
        &self,
        instance_id: InstanceId,
        ref_key: &str,
        _reference: &Value,
    ) -> Result<Value, orch8_types::error::StorageError> {
        self.objects
            .lock()
            .unwrap()
            .get(&(instance_id, ref_key.to_string()))
            .cloned()
            .ok_or_else(|| orch8_types::error::StorageError::Backend("no such object".into()))
    }
}

#[allow(clippy::needless_pass_by_value)]
fn task(handler: &str, params: Value) -> RemoteTask {
    serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(), "instance_id": uuid::Uuid::now_v7(),
        "block_id": "b1", "handler_name": handler, "params": params,
        "context": {"data": {}}, "attempt": 1, "claim_epoch": 7,
    }))
    .unwrap()
}

async fn executor(
    transport: Arc<FakeTransport>,
    credentials: LocalCredentials,
    vault: Option<Arc<dyn ExternalPayloadVault>>,
    drain_timeout: Duration,
) -> Arc<RemoteExecutor> {
    let identity = ExecutorIdentity {
        runtime_id: replica_runtime_id(uuid::Uuid::now_v7(), "acme-dc1-host"),
        host: "acme-dc1-host".into(),
        tenant_id: "acme".into(),
        labels: BTreeMap::from([("residency".into(), "eu".into())]),
        regions: vec!["eu-west-1".into()],
        handlers: vec!["transform".into(), "sleep".into(), "fail".into()],
        credentials: credentials.names(),
    };
    let names = identity.handlers.clone();
    RemoteExecutor::new(RemoteExecutorParts {
        identity,
        settings: RemoteExecutorSettings {
            poll_interval: Duration::from_millis(20),
            drain_timeout,
            externalize_bytes: 16,
            ..RemoteExecutorSettings::default()
        },
        transport,
        registry: executor_registry(&names),
        credentials,
        vault,
        scratch: Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        ),
    })
    .unwrap()
}

async fn wait_until(what: &str, mut probe: impl FnMut() -> bool) {
    for _ in 0..400 {
        if probe() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

#[test]
fn replica_runtime_ids_are_stable_and_distinct_per_replica() {
    let token = uuid::Uuid::now_v7();
    assert_eq!(
        replica_runtime_id(token, "acme-a"),
        replica_runtime_id(token, "acme-a")
    );
    assert_ne!(
        replica_runtime_id(token, "acme-a"),
        replica_runtime_id(token, "acme-b")
    );
    assert_ne!(
        replica_runtime_id(token, "acme-a"),
        replica_runtime_id(uuid::Uuid::now_v7(), "acme-a")
    );
}

#[test]
fn registry_serves_only_remote_executable_builtins() {
    let all = executor_registry(&[]);
    assert!(all.contains("http_request"));
    assert!(all.contains("transform"));
    assert!(!all.contains("set_state"));
    assert!(!all.contains("send_signal"));
    let one = executor_registry(&["http_request".to_string()]);
    assert!(one.contains("http_request"));
    assert!(!one.contains("transform"));
}

#[test]
fn capabilities_carry_placement_facts_and_credential_names_only() {
    let identity = ExecutorIdentity {
        runtime_id: RuntimeId::new(),
        host: "acme-dc1-host".into(),
        tenant_id: "acme".into(),
        labels: BTreeMap::from([("residency".into(), "eu".into())]),
        regions: vec!["eu-west-1".into()],
        handlers: vec!["http_request".into()],
        credentials: vec!["stripe".into()],
    };
    let caps = identity.capabilities(true);
    assert!(caps.draining);
    assert_eq!(caps.kind, RuntimeKind::Server);
    assert_eq!(caps.labels.get("residency").map(String::as_str), Some("eu"));
    assert_eq!(caps.hardware, vec!["host:acme-dc1-host".to_string()]);
    assert_eq!(caps.credentials, vec!["stripe".to_string()]);
    assert!(caps.expires_at - caps.observed_at <= chrono::Duration::minutes(5));
    assert_eq!(identity.worker_id(), identity.runtime_id.to_string());
}

#[tokio::test]
async fn runs_tasks_with_local_credentials_and_reports_outcomes() {
    let transport = Arc::new(FakeTransport::default());
    let ok = task(
        "transform",
        json!({"token": "credentials://api/token", "n": 1}),
    );
    let missing = task("transform", json!({"token": "credentials://absent"}));
    let failing = task("fail", json!({"message": "boom"}));
    for t in [ok.clone(), missing.clone(), failing.clone()] {
        transport.push(t);
    }
    let credentials = LocalCredentials::from_vars(
        None,
        [(
            "ORCH8_CREDENTIAL_api".to_string(),
            r#"{"token":"tok-local"}"#.to_string(),
        )],
    );
    let exec = executor(
        Arc::clone(&transport),
        credentials,
        None,
        Duration::from_secs(5),
    )
    .await;
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(Arc::clone(&exec).run(shutdown.clone()));
    wait_until("three outcomes", || {
        transport.calls("complete").len() + transport.calls("fail").len() == 3
    })
    .await;
    shutdown.cancel();
    let stats = handle.await.unwrap();

    let completes = transport.calls("complete");
    assert_eq!(completes.len(), 1);
    assert_eq!(completes[0].0, ok.id);
    assert_eq!(
        completes[0].1["token"], "tok-local",
        "resolved on the executor"
    );
    let fails = transport.calls("fail");
    let missing_fail = fails.iter().find(|(id, _)| *id == missing.id).unwrap();
    assert_eq!(missing_fail.1["retryable"], false);
    assert!(
        missing_fail.1["message"]
            .as_str()
            .unwrap()
            .contains("no local credential 'absent'")
    );
    assert!(fails.iter().any(|(id, _)| *id == failing.id));
    assert_eq!(stats.claimed, 3);
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.failed, 2);
    let ads = transport.advertisements.lock().unwrap();
    assert!(!ads.first().unwrap().draining);
    assert!(ads.last().unwrap().draining, "drain withdraws capability");
    assert_eq!(ads[0].credentials, vec!["api".to_string()]);
}

#[tokio::test]
async fn drain_releases_tasks_that_outlive_the_window() {
    let transport = Arc::new(FakeTransport::default());
    let slow = task("sleep", json!({"duration_ms": 60_000}));
    transport.push(slow.clone());
    let exec = executor(
        Arc::clone(&transport),
        LocalCredentials::default(),
        None,
        Duration::from_millis(200),
    )
    .await;
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(Arc::clone(&exec).run(shutdown.clone()));
    wait_until("the slow task to start", || exec.in_flight() == 1).await;
    wait_until("a heartbeat", || !transport.calls("heartbeat").is_empty()).await;
    shutdown.cancel();
    let stats = handle.await.unwrap();
    let releases = transport.calls("release");
    assert_eq!(releases.len(), 1);
    assert_eq!(releases[0].0, slow.id);
    assert_eq!(releases[0].1["started"], true);
    assert!(transport.calls("complete").is_empty());
    assert_eq!(stats.released, 1);
}

#[tokio::test]
async fn byok_seals_large_output_fields_and_opens_them_for_later_steps() {
    let vault = Arc::new(MemoryVault::default());
    let instance = InstanceId::new();
    let output = json!({"status": 200, "body": {"rows": ["a", "b", "c", "d", "e"]}});
    let sealed = seal_output(vault.as_ref(), instance, "fetch", output.clone(), 16)
        .await
        .unwrap();
    assert_eq!(sealed["status"], 200, "small fields stay readable");
    assert!(is_vault_reference(&sealed["body"]));
    assert_eq!(
        sealed["body"][VAULT_REF_KEY][VAULT_REF_KEY_FIELD],
        "remote-output/fetch/body"
    );
    assert!(!sealed.to_string().contains("rows"), "no plaintext");

    let mut next_params = json!({"input": sealed["body"].clone(), "x": 1});
    open_vault_references(vault.as_ref(), instance, &mut next_params)
        .await
        .unwrap();
    assert_eq!(next_params["input"], output["body"]);

    // A reference from another instance does not open.
    let mut foreign = json!({"input": sealed["body"].clone()});
    assert!(
        open_vault_references(vault.as_ref(), InstanceId::new(), &mut foreign)
            .await
            .is_err()
    );

    // Everything sealed with threshold 0; scalars sealed whole when large.
    let all = seal_output(vault.as_ref(), instance, "b", json!({"a": 1}), 0)
        .await
        .unwrap();
    assert!(is_vault_reference(&all["a"]));
    let scalar = seal_output(vault.as_ref(), instance, "c", json!("x".repeat(64)), 16)
        .await
        .unwrap();
    assert!(is_vault_reference(&scalar));
}

#[tokio::test]
async fn executor_with_vault_reports_references_not_plaintext() {
    let transport = Arc::new(FakeTransport::default());
    let t = task(
        "transform",
        json!({"secret_report": "confidential-payload-0123456789", "ok": true}),
    );
    transport.push(t.clone());
    let vault: Arc<dyn ExternalPayloadVault> = Arc::new(MemoryVault::default());
    let exec = executor(
        Arc::clone(&transport),
        LocalCredentials::default(),
        Some(vault),
        Duration::from_secs(5),
    )
    .await;
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(Arc::clone(&exec).run(shutdown.clone()));
    wait_until("completion", || !transport.calls("complete").is_empty()).await;
    shutdown.cancel();
    handle.await.unwrap();
    let (_, output) = &transport.calls("complete")[0];
    assert!(!output.to_string().contains("confidential-payload"));
    assert!(is_vault_reference(&output["secret_report"]));
    assert_eq!(output["ok"], true);
}

/// L5-01: a control-plane blip longer than a few attempts must not drop a
/// finished outcome. Delivery keeps retrying, renewing the lease, until the
/// engine accepts it.
#[tokio::test]
async fn settle_retries_through_a_control_plane_outage_and_keeps_the_lease() {
    let transport = Arc::new(FakeTransport::default());
    // About 10 s of `Retry` with the capped back-off.
    transport.retry_completes.store(6, Ordering::Relaxed);
    let exec = executor(
        Arc::clone(&transport),
        LocalCredentials::default(),
        None,
        Duration::from_secs(5),
    )
    .await;
    tokio::time::pause();
    let claimed = ClaimedTask {
        task: task("transform", json!({})),
        session: 0,
    };
    let started = tokio::time::Instant::now();
    exec.execute_inner(&claimed, Duration::from_secs(2), &CancellationToken::new())
        .await;
    assert!(started.elapsed() >= Duration::from_secs(3));
    assert_eq!(
        transport.calls("complete").len(),
        7,
        "retried until accepted"
    );
    assert!(
        !transport.calls("heartbeat").is_empty(),
        "the lease was renewed while reporting"
    );
    assert_eq!(exec.stats().completed, 1);
}

/// Delivery stops once the lease would lapse instead of retrying forever.
#[tokio::test]
async fn settle_gives_up_when_the_lease_runs_out() {
    let transport = Arc::new(FakeTransport::default());
    transport
        .retry_completes
        .store(usize::MAX, Ordering::Relaxed);
    let exec = executor(
        Arc::clone(&transport),
        LocalCredentials::default(),
        None,
        Duration::from_secs(5),
    )
    .await;
    tokio::time::pause();
    let mut short = task("transform", json!({}));
    short.lease_secs = Some(6);
    let claimed = ClaimedTask {
        task: short,
        session: 0,
    };
    let started = tokio::time::Instant::now();
    let lease = ConfirmedLease::acquire(claimed.task.lease_secs, Duration::from_secs(2), || {
        transport.heartbeat(&claimed)
    })
    .await
    .unwrap();
    *transport.heartbeat_response.lock().unwrap() = Some(LeaseResponse::Retry);
    assert!(
        lease
            .run(
                Duration::from_secs(2),
                exec.deliver(&claimed, Outcome::Complete(json!({}))),
                || transport.heartbeat(&claimed)
            )
            .await
            .is_err()
    );
    assert!(
        started.elapsed() <= Duration::from_millis(6010),
        "elapsed: {:?}",
        started.elapsed()
    );
    assert!(transport.calls("complete").len() > 4);
    assert_eq!(exec.stats().completed, 0);

    // A heartbeat answering LostOwnership ends delivery at once.
    *transport.heartbeat_response.lock().unwrap() = Some(LeaseResponse::LostOwnership);
    exec.execute_inner(&claimed, Duration::from_secs(2), &CancellationToken::new())
        .await;
    assert_eq!(exec.stats().lost, 1);
}

/// T1-GAP-236: a handler that panics must not leave its task in the
/// `running` map, or `in_flight()` (drain accounting) stays inflated.
#[tokio::test]
async fn panicking_handler_does_not_leak_an_in_flight_slot() {
    let mut registry = HandlerRegistry::new();
    registry.register("boom", |_ctx| async { panic!("handler panicked") });
    let exec = RemoteExecutor::new(RemoteExecutorParts {
        identity: ExecutorIdentity {
            runtime_id: replica_runtime_id(uuid::Uuid::now_v7(), "acme-dc1-host"),
            host: "acme-dc1-host".into(),
            tenant_id: "acme".into(),
            labels: BTreeMap::new(),
            regions: vec![],
            handlers: vec!["boom".into()],
            credentials: vec![],
        },
        settings: RemoteExecutorSettings::default(),
        transport: Arc::new(FakeTransport::default()),
        registry,
        credentials: LocalCredentials::default(),
        vault: None,
        scratch: Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        ),
    })
    .unwrap();
    let claimed = ClaimedTask {
        task: task("boom", json!({})),
        session: 0,
    };
    let mut tasks = JoinSet::new();
    exec.spawn_task(&mut tasks, claimed, Duration::from_secs(30));
    let joined = tasks.join_next().await.unwrap();
    assert!(joined.unwrap_err().is_panic());
    assert_eq!(exec.in_flight(), 0);
}

/// Claimed work must be returned before unrelated handler polls can fail or
/// consume its lease while the executor has not started heartbeating it yet.
#[tokio::test]
async fn http_claim_returns_owned_work_before_polling_another_handler() {
    use axum::{Json, Router, http::StatusCode};
    let polls = Arc::new(Mutex::new(Vec::<String>::new()));
    let captured = Arc::clone(&polls);
    let queued = task("transform", json!({}));
    let task_id = queued.id;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let app = Router::new().fallback(move |Json(body): Json<Value>| {
            let polls = Arc::clone(&captured);
            let task = queued.clone();
            async move {
                let handler = body["handler_name"].as_str().unwrap().to_owned();
                polls.lock().unwrap().push(handler.clone());
                if handler == "transform" {
                    (
                        StatusCode::OK,
                        Json(json!({"tasks":[{"id":task.id,"instance_id":task.instance_id,"block_id":task.block_id,"handler_name":task.handler_name,"claim_epoch":task.claim_epoch}], "lease_secs":1})),
                    )
                } else {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({"error":"other handler unavailable"})),
                    )
                }
            }
        });
        axum::serve(listener, app).await.unwrap();
    });
    let lease = HttpLeaseClient::new(reqwest::Client::new(), &base, Vec::new(), "worker".into());
    let transport = HttpLeaseTransport::new(lease, "acme".into(), "test".into());
    let identity = ExecutorIdentity {
        runtime_id: RuntimeId::new(),
        host: "worker".into(),
        tenant_id: "acme".into(),
        labels: BTreeMap::new(),
        regions: Vec::new(),
        handlers: vec!["transform".into(), "unavailable".into()],
        credentials: Vec::new(),
    };
    let result = transport
        .claim(&identity.handlers, 2, &identity.capabilities(false))
        .await;
    let first_polls = polls.lock().unwrap().clone();
    let next = transport
        .claim(&identity.handlers, 1, &identity.capabilities(false))
        .await;
    server.abort();
    let claimed = result.expect("a later poll must not discard an already claimed task");
    assert_eq!(claimed.tasks.len(), 1);
    assert_eq!(claimed.tasks[0].task.id, task_id);
    assert_eq!(first_polls, ["transform"]);
    assert!(next.is_err());
    assert_eq!(
        *polls.lock().unwrap(),
        ["transform", "unavailable"],
        "the next call advances fairly to the next handler"
    );
}

async fn gated_executor(
    capacity: u32,
) -> (
    Arc<RemoteExecutor>,
    Arc<FakeTransport>,
    Arc<Semaphore>,
    Arc<AtomicUsize>,
) {
    let transport = Arc::new(FakeTransport::default());
    let mut exec = executor(
        Arc::clone(&transport),
        LocalCredentials::default(),
        None,
        Duration::from_secs(1),
    )
    .await;
    let gate = Arc::new(Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_gate = Arc::clone(&gate);
    let handler_calls = Arc::clone(&calls);
    let inner = Arc::get_mut(&mut exec).unwrap();
    inner.settings.max_concurrent_tasks = capacity;
    inner.slots = Arc::new(Semaphore::new(usize::try_from(capacity).unwrap()));
    inner.registry.register("transform", move |_| {
        let gate = Arc::clone(&handler_gate);
        let calls = Arc::clone(&handler_calls);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            let _permit = gate.acquire().await.unwrap();
            Ok(json!({"done":true}))
        }
    });
    (exec, transport, gate, calls)
}

#[tokio::test]
async fn duplicate_delivery_with_free_slots_invokes_the_callback_once() {
    let (exec, transport, gate, calls) = gated_executor(2).await;
    let original = task("transform", json!({}));
    let mut tasks = JoinSet::new();
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: original.clone(),
            session: 1,
        },
        Duration::from_secs(30),
    );
    wait_until("the original invocation", || {
        calls.load(Ordering::SeqCst) == 1
    })
    .await;
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: original,
            session: 2,
        },
        Duration::from_secs(30),
    );
    gate.add_permits(2);
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "reconnecting streams cannot replay a live callback"
    );
    assert_eq!(transport.calls("complete").len(), 1);
    assert!(transport.calls("release").is_empty());
    assert_eq!(exec.in_flight(), 0);
}

#[tokio::test]
async fn duplicate_delivery_when_full_does_not_release_the_original_lease() {
    let (exec, transport, gate, calls) = gated_executor(1).await;
    let original = task("transform", json!({}));
    let mut tasks = JoinSet::new();
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: original.clone(),
            session: 1,
        },
        Duration::from_secs(30),
    );
    wait_until("the original invocation", || {
        calls.load(Ordering::SeqCst) == 1
    })
    .await;
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: original,
            session: 2,
        },
        Duration::from_secs(30),
    );
    gate.add_permits(1);
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    assert!(
        transport.calls("release").is_empty(),
        "a duplicate must not revoke the live claim merely because slots are full"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn stale_delivery_cannot_run_alongside_a_newer_active_generation() {
    let (exec, transport, gate, calls) = gated_executor(2).await;
    let original = task("transform", json!({}));
    let mut stale = original.clone();
    stale.claim_epoch -= 1;
    let mut tasks = JoinSet::new();
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: original,
            session: 2,
        },
        Duration::from_secs(30),
    );
    wait_until("the current invocation", || {
        calls.load(Ordering::SeqCst) == 1
    })
    .await;
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: stale,
            session: 1,
        },
        Duration::from_secs(30),
    );
    gate.add_permits(2);
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "an older generation must not enter a second callback"
    );
    assert!(transport.calls("release").is_empty());
}

#[tokio::test]
async fn surplus_release_fanout_is_bounded_and_backpressures_new_claims() {
    let (mut exec, transport, handler_gate, calls) = gated_executor(1).await;
    Arc::get_mut(&mut exec).unwrap().settings.advertise_interval = Duration::from_millis(10);
    let network_gate = Arc::new(Semaphore::new(0));
    *transport.release_gate.lock().unwrap() = Some(Arc::clone(&network_gate));
    transport.ignore_capacity.store(true, Ordering::SeqCst);
    for _ in 0..101 {
        transport.push(task("transform", json!({})));
    }
    let shutdown = CancellationToken::new();
    let runner = Arc::clone(&exec);
    let stop = shutdown.clone();
    let running = tokio::spawn(async move { runner.run(stop).await });
    wait_until("eight pending releases", || {
        transport.releases_active.load(Ordering::SeqCst) >= 8
    })
    .await;
    wait_until("the accepted callback", || {
        calls.load(Ordering::SeqCst) == 1
    })
    .await;
    handler_gate.add_permits(1);
    wait_until("the accepted completion", || {
        transport.calls("complete").len() == 1
    })
    .await;
    let ads = transport.advertisements.lock().unwrap().len();
    wait_until("the controller servicing its next advertisement", || {
        transport.advertisements.lock().unwrap().len() > ads
    })
    .await;
    let peak = transport.peak_releases.load(Ordering::SeqCst);
    let polls = transport.claim_calls.load(Ordering::SeqCst);
    shutdown.cancel();
    network_gate.add_permits(101);
    let stats = running.await.unwrap();
    assert!(
        peak <= 8,
        "overdelivery started {peak} concurrent release requests"
    );
    assert_eq!(
        polls, 1,
        "a pending surplus batch must backpressure additional claims"
    );
    assert_eq!(stats.released, 100);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(transport.releases_active.load(Ordering::SeqCst), 0);
    assert_eq!(exec.in_flight(), 0);
}

#[tokio::test]
async fn pending_unstarted_release_prevents_reentering_the_same_lease() {
    let (exec, transport, gate, calls) = gated_executor(1).await;
    let network_gate = Arc::new(Semaphore::new(0));
    *transport.release_gate.lock().unwrap() = Some(Arc::clone(&network_gate));
    let first = task("transform", json!({}));
    let surplus = task("transform", json!({}));
    let mut tasks = JoinSet::new();
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: first,
            session: 0,
        },
        Duration::from_secs(30),
    );
    wait_until("the first callback", || calls.load(Ordering::SeqCst) == 1).await;
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: surplus.clone(),
            session: 0,
        },
        Duration::from_secs(30),
    );
    wait_until("the pending surplus release", || {
        transport.releases_active.load(Ordering::SeqCst) == 1
    })
    .await;
    gate.add_permits(1);
    wait_until("the first completion", || {
        transport.calls("complete").len() == 1
    })
    .await;
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: surplus,
            session: 1,
        },
        Duration::from_secs(30),
    );
    network_gate.add_permits(1);
    exec.drain(tasks).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a late unstarted release must not race a new callback for the same lease"
    );
    assert_eq!(transport.calls("release").len(), 1);
}

#[tokio::test]
async fn newer_delivery_cancels_its_predecessor_and_defers_the_new_callback() {
    let (exec, transport, gate, calls) = gated_executor(2).await;
    let first = task("transform", json!({}));
    let mut newer = first.clone();
    newer.claim_epoch += 1;
    let mut tasks = JoinSet::new();
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: first,
            session: 0,
        },
        Duration::from_secs(30),
    );
    wait_until("the predecessor", || calls.load(Ordering::SeqCst) == 1).await;
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: newer.clone(),
            session: 1,
        },
        Duration::from_secs(30),
    );
    gate.add_permits(2);
    while let Some(joined) = tasks.join_next().await {
        joined.unwrap();
    }
    let first_phase_calls = calls.load(Ordering::SeqCst);
    let deferred = transport
        .calls("release")
        .iter()
        .any(|(_, body)| body["claim_epoch"] == newer.claim_epoch && body["started"] == false);
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: newer,
            session: 1,
        },
        Duration::from_secs(30),
    );
    while let Some(joined) = tasks.join_next().await {
        joined.unwrap();
    }
    assert_eq!(
        first_phase_calls, 1,
        "a replacement generation must not overlap its predecessor"
    );
    assert!(
        deferred,
        "the replacement must be returned unstarted while its predecessor is live"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(exec.in_flight(), 0);
}

#[tokio::test]
async fn cancelling_a_queued_callback_frees_its_claim_and_slot() {
    let (exec, _transport, _gate, calls) = gated_executor(1).await;
    let mut tasks = JoinSet::new();
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: task("transform", json!({})),
            session: 0,
        },
        Duration::from_secs(30),
    );
    assert_eq!(
        exec.in_flight(),
        1,
        "ownership must exist before the task is first polled"
    );
    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        assert!(result.unwrap_err().is_cancelled());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(exec.in_flight(), 0);
    assert_eq!(exec.slots.available_permits(), 1);
}

#[tokio::test]
async fn stopping_with_a_blocked_release_backlog_clears_owned_resources() {
    let (exec, transport, _handler_gate, calls) = gated_executor(1).await;
    *transport.release_gate.lock().unwrap() = Some(Arc::new(Semaphore::new(0)));
    transport.ignore_capacity.store(true, Ordering::SeqCst);
    for _ in 0..101 {
        transport.push(task("transform", json!({})));
    }
    tokio::time::pause();
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let runner = Arc::clone(&exec);
    let running = tokio::spawn(async move { runner.run(stop).await });
    wait_until("the callback and release backlog", || {
        calls.load(Ordering::SeqCst) == 1 && transport.releases_active.load(Ordering::SeqCst) == 8
    })
    .await;
    let start = tokio::time::Instant::now();
    shutdown.cancel();
    running.await.unwrap();
    assert!(
        start.elapsed() <= Duration::from_secs(12),
        "cleanup exceeded drain plus release/abort grace"
    );
    assert!(!exec.has_pending_releases());
    assert_eq!(transport.releases_active.load(Ordering::SeqCst), 0);
    assert_eq!(exec.in_flight(), 0);
    assert_eq!(exec.slots.available_permits(), 1);
    assert_eq!(exec.release_slots.available_permits(), 8);
}

#[tokio::test]
async fn already_cancelled_controller_never_advertises_or_claims() {
    let (exec, transport, _gate, calls) = gated_executor(1).await;
    let stop = CancellationToken::new();
    stop.cancel();
    exec.run(stop).await;
    assert!(
        transport.advertisements.lock().unwrap().is_empty(),
        "a cancelled start must not revive or withdraw capability"
    );
    assert_eq!(transport.claim_calls.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancelling_a_queued_controller_does_not_drain_the_active_controller() {
    let (exec, transport, gate, calls) = gated_executor(1).await;
    transport.push(task("transform", json!({})));
    let first_stop = CancellationToken::new();
    let worker = Arc::clone(&exec);
    let stop = first_stop.clone();
    let first = tokio::spawn(async move { worker.run(stop).await });
    wait_until("the active controller's callback", || {
        calls.load(Ordering::SeqCst) == 1
    })
    .await;
    let queued_stop = CancellationToken::new();
    let stop = queued_stop.clone();
    let worker = Arc::clone(&exec);
    let (entered, ready) = tokio::sync::oneshot::channel();
    let second = tokio::spawn(async move {
        entered.send(()).unwrap();
        worker.run(stop).await
    });
    ready.await.unwrap();
    queued_stop.cancel();
    second.await.unwrap();
    let advertisements = transport.advertisements.lock().unwrap().clone();
    let still_active = !first.is_finished();
    first_stop.cancel();
    gate.add_permits(1);
    first.await.unwrap();
    assert_eq!(
        advertisements.len(),
        1,
        "a queued controller must not duplicate or withdraw the active owner's advertisement"
    );
    assert!(!advertisements[0].draining);
    assert!(still_active);
}

#[tokio::test]
async fn queued_controller_can_restart_after_its_predecessor_drains() {
    let (exec, transport, gate, calls) = gated_executor(1).await;
    transport.push(task("transform", json!({})));
    let first_stop = CancellationToken::new();
    let worker = Arc::clone(&exec);
    let stop = first_stop.clone();
    let first = tokio::spawn(async move { worker.run(stop).await });
    wait_until("the first controller", || calls.load(Ordering::SeqCst) == 1).await;
    let second_stop = CancellationToken::new();
    let worker = Arc::clone(&exec);
    let stop = second_stop.clone();
    let second = tokio::spawn(async move { worker.run(stop).await });
    transport.push(task("transform", json!({})));
    first_stop.cancel();
    gate.add_permits(1);
    first.await.unwrap();
    wait_until("the successor controller", || {
        calls.load(Ordering::SeqCst) == 2
    })
    .await;
    second_stop.cancel();
    gate.add_permits(1);
    second.await.unwrap();
    let advertisements = transport.advertisements.lock().unwrap();
    let draining: Vec<_> = advertisements.iter().map(|a| a.draining).collect();
    assert_eq!(draining, vec![false, true, false, true]);
    assert_eq!(exec.in_flight(), 0);
    assert_eq!(exec.slots.available_permits(), 1);
}

#[tokio::test]
async fn stale_delivery_is_checked_before_a_fast_callback_can_run() {
    let (exec, transport, gate, calls) = gated_executor(1).await;
    *transport.heartbeat_response.lock().unwrap() = Some(LeaseResponse::LostOwnership);
    gate.add_permits(1);
    let mut tasks = JoinSet::new();
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: task("transform", json!({})),
            session: 0,
        },
        Duration::from_millis(50),
    );
    while tasks.join_next().await.is_some() {}
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "a stale delivery must not invoke even a fast callback"
    );
    assert_eq!(exec.stats().lost, 1);
    assert!(transport.calls("complete").is_empty());
}

#[tokio::test]
async fn failed_renewals_stop_the_callback_at_the_confirmed_deadline() {
    let (exec, transport, _gate, calls) = gated_executor(1).await;
    tokio::time::pause();
    let mut short = task("transform", json!({}));
    short.lease_secs = Some(1);
    let mut tasks = JoinSet::new();
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: short,
            session: 0,
        },
        Duration::from_millis(50),
    );
    wait_until("callback start", || calls.load(Ordering::SeqCst) == 1).await;
    *transport.heartbeat_response.lock().unwrap() = Some(LeaseResponse::Retry);
    tokio::time::advance(Duration::from_secs(2)).await;
    wait_until("expired callback cleanup", || exec.in_flight() == 0).await;
    assert!(transport.calls("complete").is_empty());
    assert_eq!(transport.calls("release")[0].1["started"], true);
}

#[tokio::test]
async fn blocked_renewal_does_not_block_callback_progress() {
    let (exec, transport, gate, calls) = gated_executor(1).await;
    tokio::time::pause();
    let mut tasks = JoinSet::new();
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: task("transform", json!({})),
            session: 0,
        },
        Duration::from_millis(50),
    );
    wait_until("callback start", || calls.load(Ordering::SeqCst) == 1).await;
    let before = transport.calls("heartbeat").len();
    *transport.heartbeat_gate.lock().unwrap() = Some(Arc::new(Semaphore::new(0)));
    wait_until("blocked renewal", || {
        transport.calls("heartbeat").len() > before
    })
    .await;
    gate.add_permits(1);
    wait_until("callback completes during renewal", || {
        exec.stats().completed == 1
    })
    .await;
    while tasks.join_next().await.is_some() {}
}

#[tokio::test]
async fn stalled_settlement_cannot_keep_the_execution_alive_past_its_lease() {
    let (exec, transport, gate, _calls) = gated_executor(1).await;
    gate.add_permits(1);
    *transport.complete_gate.lock().unwrap() = Some(Arc::new(Semaphore::new(0)));
    tokio::time::pause();
    let mut short = task("transform", json!({}));
    short.lease_secs = Some(1);
    let mut tasks = JoinSet::new();
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: short,
            session: 0,
        },
        Duration::from_millis(50),
    );
    wait_until("settlement RPC", || !transport.calls("complete").is_empty()).await;
    *transport.heartbeat_response.lock().unwrap() = Some(LeaseResponse::Retry);
    tokio::time::advance(Duration::from_secs(2)).await;
    wait_until("settlement deadline cleanup", || exec.in_flight() == 0).await;
    assert_eq!(exec.stats().completed, 0);
}

#[tokio::test]
async fn cancellation_drops_the_callback_before_waiting_for_release() {
    struct Active(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for Active {
        fn drop(&mut self) {
            self.0.store(false, Ordering::SeqCst);
        }
    }
    let (mut exec, transport, _gate, _calls) = gated_executor(1).await;
    let alive = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let active = Arc::clone(&alive);
    let mut registry = HandlerRegistry::new();
    registry.register("transform", move |_ctx| {
        let active = Arc::clone(&active);
        async move {
            active.store(true, Ordering::SeqCst);
            let _active = Active(active);
            std::future::pending::<Result<Value, StepError>>().await
        }
    });
    Arc::get_mut(&mut exec).unwrap().registry = registry;
    *transport.release_gate.lock().unwrap() = Some(Arc::new(Semaphore::new(0)));
    let mut tasks = JoinSet::new();
    exec.spawn_task(
        &mut tasks,
        ClaimedTask {
            task: task("transform", json!({})),
            session: 0,
        },
        Duration::from_millis(50),
    );
    wait_until("live callback", || alive.load(Ordering::SeqCst)).await;
    exec.claims.cancellations()[0].cancel();
    wait_until("blocked cleanup release", || {
        !transport.calls("release").is_empty()
    })
    .await;
    assert!(
        !alive.load(Ordering::SeqCst),
        "cleanup I/O must not retain a cancelled callback"
    );
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    assert_eq!(exec.in_flight(), 0);
}
