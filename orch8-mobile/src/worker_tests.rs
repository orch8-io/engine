use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

use serde_json::json;
use tokio::sync::Semaphore;

use super::*;
use crate::handlers::{StepHandler, register_foreign_handler};
use crate::node::{Advertisement, NodeCapabilities};
use crate::test_support::{MockControlPlane, Route, spawn_control_plane};
use orch8_types::continuity::RuntimeId;

struct Recording {
    inputs: Arc<Mutex<Vec<String>>>,
    delay: Duration,
    result: Result<String, crate::HandlerError>,
}

impl StepHandler for Recording {
    fn execute(&self, _step: String, input: String) -> Result<String, crate::HandlerError> {
        self.inputs.lock().unwrap().push(input);
        std::thread::sleep(self.delay);
        match &self.result {
            Ok(v) => Ok(v.clone()),
            Err(crate::HandlerError::Retryable { detail: message }) => {
                Err(crate::HandlerError::Retryable {
                    detail: message.clone(),
                })
            }
            Err(crate::HandlerError::Permanent { detail: message }) => {
                Err(crate::HandlerError::Permanent {
                    detail: message.clone(),
                })
            }
        }
    }
}

fn task_json(id: uuid::Uuid) -> Value {
    json!({
        "id": id,
        "instance_id": uuid::Uuid::new_v4(),
        "block_id": "scan-step",
        "handler_name": "scan",
        "params": { "sku": "A-1" },
        "context": {},
        "attempt": 1,
        "claim_epoch": 7,
        "state": "claimed",
        "created_at": "2026-01-01T00:00:00Z",
        "effect_id": "eff-42",
        "continuity_epoch": 2,
        "lease_secs": 3
    })
}

/// Poll hands out `tasks` once, then nothing; heartbeats answer
/// `heartbeat_status`, every other lease endpoint `settle_status`.
fn route_with(tasks: Vec<Value>, heartbeat_status: u16, settle_status: u16) -> Route {
    let queue = Arc::new(Mutex::new(tasks));
    Arc::new(move |_method, path, _body| {
        if path.ends_with("/workers/tasks/poll") {
            let next: Vec<Value> = queue.lock().unwrap().drain(..).collect();
            return (
                200,
                json!({ "tasks": next, "lease_secs": 3, "heartbeat_interval_secs": 1, "poll_after_ms": 0 }),
            );
        }
        if path.ends_with("/heartbeat") {
            return (heartbeat_status, json!({}));
        }
        (settle_status, json!({}))
    })
}

struct Harness {
    pool: sqlx::SqlitePool,
    worker: Arc<Worker>,
    client: Arc<NodeClient>,
    store: ClaimStore,
    inputs: Arc<Mutex<Vec<String>>>,
    foreground: Arc<AtomicBool>,
    power: Arc<AtomicU8>,
}

async fn harness_with(server: &MockControlPlane, handler: Recording, idle_ms: u64) -> Harness {
    let sqlite = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    );
    let pool = sqlite.pool().clone();
    let store = ClaimStore::new(pool.clone());
    store.init_tables().await.unwrap();
    let inputs = Arc::clone(&handler.inputs);
    let mut registry = HandlerRegistry::new();
    let stragglers = Arc::new(crate::stragglers::Stragglers::default());
    register_foreign_handler(
        &mut registry,
        "scan",
        Arc::new(handler),
        Duration::from_secs(10),
        Arc::clone(&stragglers),
    );
    let client = NodeClient::new_unchecked(
        server.base.clone(),
        crate::credential::Credential::new("key".into()),
        "device-1".into(),
        RuntimeId::new(),
        Advertisement {
            handlers: vec!["scan".into()],
            caps: NodeCapabilities::default(),
            draining: false,
        },
    );
    let foreground = Arc::new(AtomicBool::new(true));
    let power = Arc::new(AtomicU8::new(0));
    let worker = Worker::new(
        WorkerDeps {
            client: Arc::clone(&client),
            store: store.clone(),
            registry: Arc::new(registry),
            foreign_handlers: std::iter::once("scan".to_string()).collect(),
            storage: sqlite,
            signals: HostSignals {
                foreground: Arc::clone(&foreground),
                power_state: Arc::clone(&power),
            },
            stragglers,
        },
        WorkerOptions {
            max_concurrent_tasks: 1,
            idle_poll_interval_ms: idle_ms,
            version: Some("1.2.3".into()),
        },
    )
    .unwrap();
    Harness {
        pool,
        worker,
        client,
        store,
        inputs,
        foreground,
        power,
    }
}

async fn harness(server: &MockControlPlane, handler: Recording) -> Harness {
    harness_with(server, handler, 250).await
}

fn ok_handler(delay: Duration) -> Recording {
    Recording {
        inputs: Arc::new(Mutex::new(Vec::new())),
        delay,
        result: Ok(r#"{"scanned":true}"#.into()),
    }
}

async fn wait_for(mut done: impl FnMut() -> bool) {
    for _ in 0..400 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("condition not met in time");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_runs_task_with_effect_id_heartbeats_and_completes() {
    let task_id = uuid::Uuid::new_v4();
    let server = spawn_control_plane(route_with(vec![task_json(task_id)], 200, 200)).await;
    let h = harness(&server, ok_handler(Duration::from_millis(1300))).await;
    h.worker.spawn(&tokio::runtime::Handle::current());

    wait_for(|| server.count(&format!("{task_id}/complete")) == 1).await;
    h.worker.stop();

    // Poll: kind mobile, worker_id == runtime_id, handler advertised.
    let poll = &server.bodies("/workers/tasks/poll")[0];
    assert_eq!(poll["handler_name"], "scan");
    assert_eq!(poll["worker_id"], h.client.worker_id());
    assert_eq!(poll["capabilities"]["runtime_id"], h.client.worker_id());
    assert_eq!(poll["capabilities"]["kind"], "mobile");
    assert_eq!(poll["capabilities"]["handlers"], json!(["scan"]));
    assert_eq!(poll["version"], "1.2.3");

    // Handler input: original params + reserved __orch8 metadata.
    let input: Value = serde_json::from_str(&h.inputs.lock().unwrap()[0]).unwrap();
    assert_eq!(input["sku"], "A-1");
    assert_eq!(input["__orch8"]["effect_id"], "eff-42");
    assert_eq!(input["__orch8"]["task_id"], json!(task_id));
    assert_eq!(input["__orch8"]["continuity_epoch"], 2);
    assert_eq!(input["__orch8"]["runtime_id"], h.client.worker_id());

    // At least one heartbeat during the 1.3 s handler (1 s cadence).
    let beats = server.bodies(&format!("{task_id}/heartbeat"));
    assert!(!beats.is_empty(), "expected a heartbeat");
    assert_eq!(beats[0]["claim_epoch"], 7);

    let complete = &server.bodies(&format!("{task_id}/complete"))[0];
    assert_eq!(complete["claim_epoch"], 7);
    assert_eq!(complete["worker_id"], h.client.worker_id());
    assert_eq!(complete["output"], json!({ "scanned": true }));

    wait_for(|| h.worker.stats().in_flight == 0).await;
    assert_eq!(
        h.store.count().await.unwrap(),
        0,
        "settled claim must leave the journal"
    );
    let stats = h.worker.stats();
    assert_eq!((stats.claimed, stats.completed), (1, 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retryable_handler_error_is_reported_as_retryable_fail() {
    let task_id = uuid::Uuid::new_v4();
    let server = spawn_control_plane(route_with(vec![task_json(task_id)], 200, 200)).await;
    let h = harness(
        &server,
        Recording {
            inputs: Arc::new(Mutex::new(Vec::new())),
            delay: Duration::ZERO,
            result: Err(crate::HandlerError::Retryable {
                detail: "camera busy".into(),
            }),
        },
    )
    .await;
    h.worker.spawn(&tokio::runtime::Handle::current());
    wait_for(|| server.count(&format!("{task_id}/fail")) == 1).await;
    h.worker.stop();
    let fail = &server.bodies(&format!("{task_id}/fail"))[0];
    assert_eq!(fail["retryable"], true);
    assert_eq!(fail["message"], "camera busy");
    assert_eq!(server.count(&format!("{task_id}/complete")), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_lease_abandons_the_task_without_settling() {
    let task_id = uuid::Uuid::new_v4();
    let server = spawn_control_plane(route_with(vec![task_json(task_id)], 409, 200)).await;
    let h = harness(&server, ok_handler(Duration::from_millis(1500))).await;
    h.worker.spawn(&tokio::runtime::Handle::current());
    wait_for(|| h.worker.stats().lost == 1).await;
    h.worker.stop();
    tokio::time::sleep(Duration::from_millis(1600)).await;
    assert_eq!(server.count(&format!("{task_id}/complete")), 0);
    assert_eq!(h.store.count().await.unwrap(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backgrounded_or_critical_battery_worker_does_not_claim() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    h.foreground.store(false, Ordering::Release);
    h.worker.spawn(&tokio::runtime::Handle::current());
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        server.count("/workers/tasks/poll"),
        0,
        "paused app must not claim"
    );

    h.foreground.store(true, Ordering::Release);
    h.power.store(3, Ordering::Release); // CriticalBattery
    h.worker.wake();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        server.count("/workers/tasks/poll"),
        0,
        "critical battery must not claim"
    );

    h.power.store(1, Ordering::Release); // Unplugged
    h.worker.wake();
    wait_for(|| server.count("/workers/tasks/poll") > 0).await;
    h.worker.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_wake_polls_immediately() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let h = harness_with(&server, ok_handler(Duration::ZERO), 60_000).await;
    h.worker.spawn(&tokio::runtime::Handle::current());
    wait_for(|| server.count("/workers/tasks/poll") == 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        server.count("/workers/tasks/poll"),
        1,
        "idle worker waits for its interval"
    );
    h.worker.wake();
    wait_for(|| server.count("/workers/tasks/poll") == 2).await;
    h.worker.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_window_runs_until_idle() {
    let task_id = uuid::Uuid::new_v4();
    let server = spawn_control_plane(route_with(vec![task_json(task_id)], 200, 200)).await;
    let h = harness(&server, ok_handler(Duration::from_millis(100))).await;
    h.foreground.store(false, Ordering::Release);
    h.worker.spawn(&tokio::runtime::Handle::current());
    let result = h.worker.run_window(Duration::from_secs(10)).await;
    assert_eq!(result.claimed, 1);
    assert_eq!(result.completed, 1);
    assert_eq!(result.still_running, 0);
    assert!(!result.budget_exhausted);
    h.worker.stop();
}

async fn seed(
    store: &ClaimStore,
    client: &NodeClient,
    started: bool,
    outcome: Option<Outcome>,
) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    let task: RemoteTask = serde_json::from_value(task_json(id)).unwrap();
    let key = ClaimKey::for_task(&task, &client.worker_id(), client.api_base());
    store
        .insert(&task, &client.worker_id(), client.api_base())
        .await
        .unwrap();
    if started {
        store.mark_started(&key).await.unwrap();
    }
    if let Some(outcome) = outcome {
        store.record_outcome(&key, &outcome).await.unwrap();
    }
    id
}

#[tokio::test]
async fn restart_drain_releases_or_redelivers_every_orphaned_claim() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    let unstarted = seed(&h.store, &h.client, false, None).await;
    let started = seed(&h.store, &h.client, true, None).await;
    let finished = seed(
        &h.store,
        &h.client,
        true,
        Some(Outcome::Complete {
            output: json!({"n": 1}),
        }),
    )
    .await;

    let settled = drain_orphans(&h.client, &h.store, None).await;
    assert_eq!(settled, 3);
    assert_eq!(h.store.count().await.unwrap(), 0);

    let release_a = &server.bodies(&format!("{unstarted}/release"))[0];
    assert_eq!(release_a["started"], false);
    assert_eq!(release_a["claim_epoch"], 7);
    assert_eq!(release_a["worker_id"], h.client.worker_id());
    assert_eq!(
        server.bodies(&format!("{started}/release"))[0]["started"],
        true
    );
    assert_eq!(server.count(&format!("{finished}/release")), 0);
    assert_eq!(
        server.bodies(&format!("{finished}/complete"))[0]["output"],
        json!({"n": 1})
    );
}

#[tokio::test]
async fn release_falls_back_to_retryable_fail_on_pre_contract_servers() {
    let route: Route = Arc::new(|_m, path, _b| {
        if path.ends_with("/release") {
            (404, json!({}))
        } else {
            (200, json!({}))
        }
    });
    let server = spawn_control_plane(route).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    let id = seed(&h.store, &h.client, true, None).await;
    assert_eq!(drain_orphans(&h.client, &h.store, None).await, 1);
    let fail = &server.bodies(&format!("{id}/fail"))[0];
    assert_eq!(fail["retryable"], true);
    assert_eq!(h.store.count().await.unwrap(), 0);
}

#[tokio::test]
async fn unreachable_control_plane_keeps_claims_for_the_next_drain() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    let route: Route = Arc::new(move |_m, _p, _b| {
        seen.fetch_add(1, Ordering::Relaxed);
        (503, json!({}))
    });
    let server = spawn_control_plane(route).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    seed(&h.store, &h.client, false, None).await;
    // In-flight claims are never touched by the drain.
    let busy = seed(&h.store, &h.client, false, None).await;
    let busy_key = h
        .store
        .list()
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.key.task_id == busy)
        .unwrap()
        .key;
    let _live = h.store.reserve(&busy_key).unwrap();
    assert_eq!(drain_orphans(&h.client, &h.store, None).await, 0);
    assert_eq!(h.store.count().await.unwrap(), 2);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "only the idle claim is attempted"
    );
    assert_eq!(server.count(&format!("{busy}/release")), 0);
}

#[test]
fn metadata_is_injected_only_into_object_params() {
    let task: RemoteTask = serde_json::from_value(task_json(uuid::Uuid::new_v4())).unwrap();
    let mut object = json!({"a": 1});
    inject_task_metadata(&mut object, &task, "rt");
    assert_eq!(object["__orch8"]["effect_id"], "eff-42");
    assert_eq!(object["a"], 1);
    let mut scalar = json!("raw");
    inject_task_metadata(&mut scalar, &task, "rt");
    assert_eq!(scalar, json!("raw"));
}

#[test]
fn heartbeat_follows_the_tightest_lease() {
    assert_eq!(
        heartbeat_interval(None, None, None),
        Duration::from_secs(40)
    );
    assert_eq!(
        heartbeat_interval(Some(20), Some(60), None),
        Duration::from_secs(20)
    );
    assert_eq!(
        heartbeat_interval(Some(20), Some(60), Some(30)),
        Duration::from_secs(10)
    );
    assert_eq!(
        heartbeat_interval(None, Some(60), Some(120)),
        Duration::from_secs(40)
    );
    assert_eq!(
        heartbeat_interval(Some(0), Some(0), Some(1)),
        Duration::from_secs(1)
    );
}

#[tokio::test]
async fn worker_rejects_unregistered_advertised_handlers() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    h.client
        .update_advertisement(|ad| ad.handlers = vec!["missing".into()]);
    let sqlite = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    );
    let result = Worker::new(
        WorkerDeps {
            client: Arc::clone(&h.client),
            store: h.store.clone(),
            registry: Arc::new(HandlerRegistry::new()),
            foreign_handlers: HashSet::new(),
            storage: sqlite,
            signals: HostSignals {
                foreground: Arc::new(AtomicBool::new(true)),
                power_state: Arc::new(AtomicU8::new(0)),
            },
            stragglers: Arc::new(crate::stragglers::Stragglers::default()),
        },
        WorkerOptions::default(),
    );
    assert!(matches!(result, Err(MobileError::InvalidInput { .. })));
}

/// A device-side timeout is an ambiguous outcome, not a failure: the native
/// handler keeps running, so the worker releases the task as `started`
/// (server: receipt unknown → retry policy) and claims nothing new for that
/// handler until the timed-out call returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn device_timeout_releases_as_started_and_quarantines_the_handler() {
    let task_id = uuid::Uuid::new_v4();
    let mut task = task_json(task_id);
    task["timeout_ms"] = json!(200);
    let server = spawn_control_plane(route_with(vec![task], 200, 204)).await;
    let h = harness_with(&server, ok_handler(Duration::from_millis(1500)), 50).await;
    h.worker.spawn(&tokio::runtime::Handle::current());

    wait_for(|| server.count(&format!("{task_id}/release")) == 1).await;
    let release = &server.bodies(&format!("{task_id}/release"))[0];
    assert_eq!(release["started"], true);
    assert_eq!(release["claim_epoch"], 7);
    assert_eq!(server.count(&format!("{task_id}/fail")), 0);
    assert_eq!(server.count(&format!("{task_id}/complete")), 0);

    // While the native call is still running, no new claim for "scan".
    let polls_at_release = server.count("/workers/tasks/poll");
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        server.count("/workers/tasks/poll"),
        polls_at_release,
        "a handler with a running timed-out invocation must not claim new work"
    );

    // Once it returns, polling resumes; its late result is never reported.
    wait_for(|| server.count("/workers/tasks/poll") > polls_at_release).await;
    h.worker.stop();
    assert_eq!(server.count(&format!("{task_id}/complete")), 0);
    assert_eq!(h.store.count().await.unwrap(), 0);
    assert_eq!(h.worker.stats().released, 1);
}

#[tokio::test]
async fn handler_claim_loss_clears_journal_without_reporting_an_outcome() {
    let task_id = uuid::Uuid::new_v4();
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let mut h = harness(&server, ok_handler(Duration::ZERO)).await;
    let mut registry = HandlerRegistry::new();
    registry.register("scan", |_| async { Err(StepError::ClaimLost) });
    Arc::get_mut(&mut h.worker).unwrap().deps.registry = Arc::new(registry);
    let task = serde_json::from_value(task_json(task_id)).unwrap();
    h.worker.execute(task, Duration::from_secs(1)).await;
    assert_eq!(h.worker.stats().lost, 1);
    assert_eq!(h.store.count().await.unwrap(), 0);
    assert_eq!(server.count(&format!("{task_id}/complete")), 0);
    assert_eq!(server.count(&format!("{task_id}/fail")), 0);
    assert_eq!(server.count(&format!("{task_id}/release")), 0);
}

fn successor(h: &Harness) -> Arc<Worker> {
    Worker::new(
        WorkerDeps {
            client: Arc::clone(&h.client),
            store: h.store.clone(),
            registry: Arc::clone(&h.worker.deps.registry),
            foreign_handlers: h.worker.deps.foreign_handlers.clone(),
            storage: Arc::clone(&h.worker.deps.storage),
            signals: h.worker.deps.signals.clone(),
            stragglers: Arc::clone(&h.worker.deps.stragglers),
        },
        WorkerOptions::default(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restarted_worker_does_not_recover_a_predecessors_live_claim() {
    let id = uuid::Uuid::new_v4();
    let server = spawn_control_plane(route_with(vec![task_json(id)], 200, 200)).await;
    let mut h = harness(&server, ok_handler(Duration::ZERO)).await;
    let entered = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let started = Arc::clone(&entered);
    let release = Arc::clone(&finish);
    let mut registry = HandlerRegistry::new();
    registry.register("scan", move |_| {
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        async move {
            started.notify_one();
            release.notified().await;
            Ok(json!({"scanned": true}))
        }
    });
    Arc::get_mut(&mut h.worker).unwrap().deps.registry = Arc::new(registry);
    h.worker.spawn(&tokio::runtime::Handle::current());
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    h.worker.stop();
    let next = successor(&h);
    let recovered = drain_orphans(&next.deps.client, &next.deps.store, None).await;
    let releases = server.count(&format!("{id}/release"));
    finish.notify_one();
    wait_for(|| server.count(&format!("{id}/complete")) == 1).await;
    next.stop();
    assert_eq!(recovered, 0, "a live predecessor claim is not an orphan");
    assert_eq!(releases, 0, "restart must not revoke in-process work");
}

#[tokio::test]
async fn stale_journal_cleanup_does_not_delete_a_newer_claim_generation() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    let id = uuid::Uuid::new_v4();
    let old: RemoteTask = serde_json::from_value(task_json(id)).unwrap();
    let mut new = old.clone();
    new.claim_epoch += 1;
    h.store
        .insert(&old, &h.client.worker_id(), h.client.api_base())
        .await
        .unwrap();
    h.store
        .insert(&new, &h.client.worker_id(), h.client.api_base())
        .await
        .unwrap();
    h.store
        .delete(&ClaimKey::for_task(
            &old,
            &h.client.worker_id(),
            h.client.api_base(),
        ))
        .await;
    let rows = h.store.list().await.unwrap();
    assert!(
        rows.iter()
            .any(|row| row.key.claim_epoch == new.claim_epoch),
        "settling an old epoch must preserve the new epoch's crash journal"
    );
}

#[tokio::test]
async fn duplicate_claim_preserves_its_recorded_outcome() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    let task: RemoteTask = serde_json::from_value(task_json(uuid::Uuid::new_v4())).unwrap();
    let outcome = Outcome::Complete {
        output: json!({"receipt": "already-performed"}),
    };
    h.store
        .insert(&task, &h.client.worker_id(), h.client.api_base())
        .await
        .unwrap();
    let key = ClaimKey::for_task(&task, &h.client.worker_id(), h.client.api_base());
    h.store.mark_started(&key).await.unwrap();
    h.store.record_outcome(&key, &outcome).await.unwrap();
    h.store
        .insert(&task, &h.client.worker_id(), h.client.api_base())
        .await
        .unwrap();
    let rows = h.store.list().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].started, "redelivery must preserve the start marker");
    assert_eq!(rows[0].outcome.as_ref(), Some(&outcome));
}

#[tokio::test]
async fn independent_control_planes_keep_claims_with_the_same_task_id() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    let task: RemoteTask = serde_json::from_value(task_json(uuid::Uuid::new_v4())).unwrap();
    h.store
        .insert(&task, &h.client.worker_id(), h.client.api_base())
        .await
        .unwrap();
    h.store
        .insert(&task, &h.client.worker_id(), "https://other.example/api/v1")
        .await
        .unwrap();
    assert_eq!(
        h.store.count().await.unwrap(),
        2,
        "a task ID from another control plane must not replace recovery evidence"
    );
}

#[tokio::test]
async fn recovery_does_not_submit_another_workers_outcome() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    let task: RemoteTask = serde_json::from_value(task_json(uuid::Uuid::new_v4())).unwrap();
    h.store
        .insert(&task, "another-runtime", h.client.api_base())
        .await
        .unwrap();
    let key = ClaimKey::for_task(&task, "another-runtime", h.client.api_base());
    h.store.mark_started(&key).await.unwrap();
    h.store
        .record_outcome(
            &key,
            &Outcome::Complete {
                output: json!({"private": "other-runtime"}),
            },
        )
        .await
        .unwrap();
    let settled = drain_orphans(&h.client, &h.store, None).await;
    assert_eq!(settled, 0, "recovery must use the original worker identity");
    assert_eq!(server.count(&format!("{}/complete", task.id)), 0);
    assert_eq!(h.store.count().await.unwrap(), 1);
}

#[tokio::test]
async fn earlier_epoch_mutations_do_not_mark_or_overwrite_a_newer_epoch() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    let old: RemoteTask = serde_json::from_value(task_json(uuid::Uuid::new_v4())).unwrap();
    let mut new = old.clone();
    new.claim_epoch += 1;
    h.store
        .insert(&old, &h.client.worker_id(), h.client.api_base())
        .await
        .unwrap();
    h.store
        .insert(&new, &h.client.worker_id(), h.client.api_base())
        .await
        .unwrap();
    let old_key = ClaimKey::for_task(&old, &h.client.worker_id(), h.client.api_base());
    assert!(h.store.mark_started(&old_key).await.unwrap());
    assert!(
        h.store
            .record_outcome(
                &old_key,
                &Outcome::Complete {
                    output: json!({"old":true})
                }
            )
            .await
            .unwrap()
    );
    let new_key = ClaimKey::for_task(&new, &h.client.worker_id(), h.client.api_base());
    let row = h.store.get(&new_key).await.unwrap().unwrap();
    assert!(!row.started);
    assert!(
        row.outcome.is_none(),
        "an older invocation cannot manufacture the new epoch's outcome"
    );
}

#[tokio::test]
async fn panicking_invocation_releases_local_reservations_for_crash_recovery() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let mut h = harness(&server, ok_handler(Duration::ZERO)).await;
    let mut registry = HandlerRegistry::new();
    registry.register("scan", |_| async {
        panic!("application callback panicked")
    });
    Arc::get_mut(&mut h.worker).unwrap().deps.registry = Arc::new(registry);
    let task: RemoteTask = serde_json::from_value(task_json(uuid::Uuid::new_v4())).unwrap();
    let id = task.id;
    let worker = Arc::clone(&h.worker);
    let result =
        tokio::spawn(async move { worker.execute(task, Duration::from_secs(1)).await }).await;
    assert!(result.unwrap_err().is_panic());
    assert_eq!(h.worker.stats().in_flight, 0);
    assert_eq!(drain_orphans(&h.client, &h.store, None).await, 1);
    assert_eq!(server.bodies(&format!("{id}/release"))[0]["started"], true);
}

/// Lease loss must quarantine a live native callback even when the server
/// supplied an additional timeout around the foreign handler wrapper.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_loss_with_a_remote_timeout_quarantines_the_live_native_callback() {
    struct Gated {
        entered: Arc<AtomicBool>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl StepHandler for Gated {
        fn execute(&self, _step: String, _input: String) -> Result<String, crate::HandlerError> {
            self.entered.store(true, Ordering::Release);
            let _ = self
                .release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5));
            Ok("{}".into())
        }
    }
    let entered = Arc::new(AtomicBool::new(false));
    let started = Arc::clone(&entered);
    let route: Route = Arc::new(move |_, path, _| {
        let status = if path.ends_with("/heartbeat") && started.load(Ordering::Acquire) {
            409
        } else {
            200
        };
        (status, json!({}))
    });
    let server = spawn_control_plane(route).await;
    let mut h = harness(&server, ok_handler(Duration::ZERO)).await;
    let (release, receiver) = std::sync::mpsc::channel();
    let mut registry = HandlerRegistry::new();
    register_foreign_handler(
        &mut registry,
        "scan",
        Arc::new(Gated {
            entered,
            release: Mutex::new(receiver),
        }),
        Duration::from_secs(60),
        Arc::clone(&h.worker.deps.stragglers),
    );
    Arc::get_mut(&mut h.worker).unwrap().deps.registry = Arc::new(registry);
    let id = uuid::Uuid::new_v4();
    let mut task = task_json(id);
    task["timeout_ms"] = json!(60_000);
    h.worker
        .execute(
            serde_json::from_value(task).unwrap(),
            Duration::from_millis(10),
        )
        .await;
    let quarantined = h.worker.deps.stragglers.blocks("scan");
    release.send(()).unwrap();
    wait_for(|| !h.worker.deps.stragglers.blocks("scan")).await;
    assert!(
        quarantined,
        "lease loss must not detach a live native callback without quarantine"
    );
    assert_eq!(h.worker.stats().lost, 1);
    assert_eq!(h.store.count().await.unwrap(), 0);
    assert_eq!(server.count(&format!("{id}/complete")), 0);
    assert_eq!(server.count(&format!("{id}/fail")), 0);
}

#[tokio::test]
async fn cancelling_a_background_window_revokes_its_claim_permission() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 204)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    h.foreground.store(false, Ordering::Release);
    let worker = Arc::clone(&h.worker);
    let window = tokio::spawn(async move { worker.run_window(Duration::from_secs(60)).await });
    wait_for(|| h.worker.may_claim()).await;
    window.abort();
    assert!(window.await.unwrap_err().is_cancelled());
    assert!(
        !h.worker.may_claim(),
        "cancelled windows cannot leave background claims enabled"
    );
}

#[tokio::test]
async fn overlapping_background_windows_retain_independent_claim_permission() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 204)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    h.foreground.store(false, Ordering::Release);
    let first_worker = Arc::clone(&h.worker);
    let first =
        tokio::spawn(async move { first_worker.run_window(Duration::from_millis(100)).await });
    wait_for(|| h.worker.may_claim()).await;
    let second_worker = Arc::clone(&h.worker);
    let (entered, ready) = tokio::sync::oneshot::channel();
    let second = tokio::spawn(async move {
        let window = second_worker.run_window(Duration::from_secs(60));
        tokio::pin!(window);
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(window.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        entered.send(()).unwrap();
        window.await
    });
    ready.await.unwrap();
    first.await.unwrap();
    let still_allowed = h.worker.may_claim();
    second.abort();
    assert!(second.await.unwrap_err().is_cancelled());
    assert!(
        still_allowed,
        "one window ending cannot revoke another active window"
    );
    assert!(!h.worker.may_claim());
}

#[tokio::test]
async fn stopped_worker_releases_a_late_poll_claim_without_starting_it() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 204)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    h.worker.stop();
    let id = uuid::Uuid::new_v4();
    h.worker
        .execute(
            serde_json::from_value(task_json(id)).unwrap(),
            Duration::from_secs(1),
        )
        .await;
    assert!(
        h.inputs.lock().unwrap().is_empty(),
        "a stopped worker must not start a late delivery"
    );
    assert_eq!(server.bodies(&format!("{id}/release"))[0]["started"], false);
    assert_eq!(h.store.count().await.unwrap(), 0);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renewal_outage_quarantines_the_live_native_callback_at_lease_expiry() {
    struct Gated {
        entered: Arc<AtomicBool>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl StepHandler for Gated {
        fn execute(&self, _step: String, _input: String) -> Result<String, crate::HandlerError> {
            self.entered.store(true, Ordering::Release);
            let _ = self
                .release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5));
            Ok("{}".into())
        }
    }
    let entered = Arc::new(AtomicBool::new(false));
    let started = Arc::clone(&entered);
    let route: Route = Arc::new(move |_, path, _| {
        let status = if path.ends_with("/heartbeat") && started.load(Ordering::Acquire) {
            503
        } else {
            200
        };
        (status, json!({}))
    });
    let server = spawn_control_plane(route).await;
    let mut h = harness(&server, ok_handler(Duration::ZERO)).await;
    let (release, receiver) = std::sync::mpsc::channel();
    let mut registry = HandlerRegistry::new();
    register_foreign_handler(
        &mut registry,
        "scan",
        Arc::new(Gated {
            entered,
            release: Mutex::new(receiver),
        }),
        Duration::from_secs(60),
        Arc::clone(&h.worker.deps.stragglers),
    );
    Arc::get_mut(&mut h.worker).unwrap().deps.registry = Arc::new(registry);
    let id = uuid::Uuid::new_v4();
    let mut task = task_json(id);
    task["timeout_ms"] = json!(60_000);
    task["lease_secs"] = json!(1);
    let start = tokio::time::Instant::now();
    h.worker
        .execute(
            serde_json::from_value(task).unwrap(),
            Duration::from_millis(10),
        )
        .await;
    let elapsed = start.elapsed();
    let quarantined = h.worker.deps.stragglers.blocks("scan");
    let _ = release.send(());
    wait_for(|| !h.worker.deps.stragglers.blocks("scan")).await;
    assert!(
        quarantined,
        "lease loss must not detach a live native callback without quarantine"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "renewal failure must not wait for physical callback completion"
    );
    assert_eq!(server.bodies(&format!("{id}/release"))[0]["started"], true);
    assert_eq!(h.store.count().await.unwrap(), 0);
    assert_eq!(server.count(&format!("{id}/complete")), 0);
    assert_eq!(server.count(&format!("{id}/fail")), 0);
}

#[tokio::test]
async fn stale_mobile_delivery_is_checked_before_native_invocation() {
    let server = spawn_control_plane(route_with(Vec::new(), 409, 204)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    let id = uuid::Uuid::new_v4();
    h.worker
        .execute(
            serde_json::from_value(task_json(id)).unwrap(),
            Duration::from_secs(1),
        )
        .await;
    assert!(
        h.inputs.lock().unwrap().is_empty(),
        "a fenced claim must not invoke native code"
    );
    assert_eq!(h.worker.stats().lost, 1);
    assert_eq!(h.store.count().await.unwrap(), 0);
    assert_eq!(server.count(&format!("{id}/complete")), 0);
}

#[tokio::test]
async fn expiry_retains_a_known_outcome_for_recovery_without_reinvoking_the_handler() {
    let delivering = Arc::new(AtomicBool::new(false));
    let restored = Arc::new(AtomicBool::new(false));
    let settling = Arc::clone(&delivering);
    let healthy = Arc::clone(&restored);
    let server = spawn_control_plane(Arc::new(move |_, path, _| {
        if path.ends_with("/complete") {
            settling.store(true, Ordering::Release);
            return (
                if healthy.load(Ordering::Acquire) {
                    200
                } else {
                    503
                },
                json!({}),
            );
        }
        if path.ends_with("/heartbeat")
            && settling.load(Ordering::Acquire)
            && !healthy.load(Ordering::Acquire)
        {
            return (503, json!({}));
        }
        (200, json!({}))
    }))
    .await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    let id = uuid::Uuid::new_v4();
    let mut task = task_json(id);
    task["lease_secs"] = json!(1);
    let start = tokio::time::Instant::now();
    h.worker
        .execute(
            serde_json::from_value(task).unwrap(),
            Duration::from_millis(100),
        )
        .await;
    assert!(
        start.elapsed() < Duration::from_millis(1450),
        "settlement must share the confirmed lease deadline"
    );
    let claims = h.store.list().await.unwrap();
    assert_eq!(claims.len(), 1);
    assert!(claims[0].started);
    assert!(
        matches!(&claims[0].outcome, Some(Outcome::Complete { output }) if output == &json!({"scanned":true}))
    );
    assert_eq!(
        server.count(&format!("{id}/release")),
        0,
        "a known outcome must not become an unknown release"
    );
    restored.store(true, Ordering::Release);
    assert_eq!(drain_orphans(&h.client, &h.store, None).await, 1);
    assert_eq!(h.store.count().await.unwrap(), 0);
    assert_eq!(h.inputs.lock().unwrap().len(), 1);
}

async fn admission_harness(
    server: &MockControlPlane,
) -> (Harness, Arc<Semaphore>, Arc<AtomicUsize>) {
    let mut h = harness(server, ok_handler(Duration::ZERO)).await;
    let gate = Arc::new(Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::clone(&gate);
    let entered = Arc::clone(&calls);
    let mut registry = HandlerRegistry::new();
    registry.register("scan", move |_| {
        let release = Arc::clone(&release);
        let entered = Arc::clone(&entered);
        async move {
            entered.fetch_add(1, Ordering::SeqCst);
            let _permit = release.acquire().await.unwrap();
            Ok(json!({"scanned": true}))
        }
    });
    Arc::get_mut(&mut h.worker).unwrap().deps.registry = Arc::new(registry);
    (h, gate, calls)
}

#[tokio::test]
async fn restarted_worker_keeps_predecessor_capacity_and_reports_its_live_work() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let (h, gate, calls) = admission_harness(&server).await;
    let first = uuid::Uuid::new_v4();
    h.worker.spawn_task(
        serde_json::from_value(task_json(first)).unwrap(),
        Duration::from_millis(100),
    );
    wait_for(|| calls.load(Ordering::SeqCst) == 1).await;
    h.worker.stop();
    let next = successor(&h);
    let inherited = next.stats().in_flight;
    let second = uuid::Uuid::new_v4();
    next.spawn_task(
        serde_json::from_value(task_json(second)).unwrap(),
        Duration::from_millis(100),
    );
    wait_for(|| {
        calls.load(Ordering::SeqCst) == 2 || server.count(&format!("{second}/release")) > 0
    })
    .await;
    let observed = calls.load(Ordering::SeqCst);
    gate.add_permits(2);
    wait_for(|| server.count(&format!("{first}/complete")) == 1).await;
    assert_eq!(
        observed, 1,
        "restart must not grant a second independent concurrency budget"
    );
    assert_eq!(
        inherited, 1,
        "window/stats accounting must include draining predecessors"
    );
    assert_eq!(
        server.bodies(&format!("{second}/release"))[0]["started"],
        false
    );
    wait_for(|| next.stats().in_flight == 0).await;
}

#[tokio::test]
async fn duplicate_mobile_delivery_at_capacity_does_not_release_the_active_claim() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let (h, gate, calls) = admission_harness(&server).await;
    let id = uuid::Uuid::new_v4();
    let task: RemoteTask = serde_json::from_value(task_json(id)).unwrap();
    h.worker
        .spawn_task(task.clone(), Duration::from_millis(100));
    wait_for(|| calls.load(Ordering::SeqCst) == 1).await;
    h.worker.spawn_task(task, Duration::from_millis(100));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let releases = server.count(&format!("{id}/release"));
    gate.add_permits(1);
    wait_for(|| h.worker.stats().in_flight == 0).await;
    assert_eq!(
        releases, 0,
        "a duplicate must not revoke a live claim because its slot is full"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn newer_mobile_claim_cancels_and_defers_while_the_predecessor_is_live() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let (h, gate, calls) = admission_harness(&server).await;
    let id = uuid::Uuid::new_v4();
    let old: RemoteTask = serde_json::from_value(task_json(id)).unwrap();
    let mut new = old.clone();
    new.claim_epoch += 1;
    let worker = Arc::clone(&h.worker);
    let first = tokio::spawn(async move { worker.execute(old, Duration::from_millis(100)).await });
    wait_for(|| calls.load(Ordering::SeqCst) == 1).await;
    let worker = Arc::clone(&h.worker);
    let second = tokio::spawn(async move { worker.execute(new, Duration::from_millis(100)).await });
    wait_for(|| calls.load(Ordering::SeqCst) == 2 || server.count(&format!("{id}/release")) > 0)
        .await;
    let observed = calls.load(Ordering::SeqCst);
    gate.add_permits(2);
    first.await.unwrap();
    second.await.unwrap();
    assert_eq!(
        observed, 1,
        "a newer generation must not overlap a live predecessor"
    );
    assert_eq!(
        h.worker.stats().lost,
        1,
        "the superseded wrapper must stop promptly"
    );
    let releases = server.bodies(&format!("{id}/release"));
    assert_eq!(releases.len(), 1);
    assert_eq!(releases[0]["claim_epoch"], 8);
    assert_eq!(releases[0]["started"], false);
}

#[tokio::test]
async fn replacement_identity_defers_the_same_task_even_with_spare_capacity() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let (h, gate, calls) = admission_harness(&server).await;
    let client = NodeClient::new_unchecked(
        server.base.clone(),
        crate::credential::Credential::new("replacement-key".into()),
        "replacement-device".into(),
        RuntimeId::new(),
        Advertisement {
            handlers: vec!["scan".into()],
            caps: NodeCapabilities::default(),
            draining: false,
        },
    );
    let next = Worker::new(
        WorkerDeps {
            client: Arc::clone(&client),
            store: h.store.clone(),
            registry: Arc::clone(&h.worker.deps.registry),
            foreign_handlers: h.worker.deps.foreign_handlers.clone(),
            storage: Arc::clone(&h.worker.deps.storage),
            signals: h.worker.deps.signals.clone(),
            stragglers: Arc::clone(&h.worker.deps.stragglers),
        },
        WorkerOptions {
            max_concurrent_tasks: 2,
            ..WorkerOptions::default()
        },
    )
    .unwrap();
    assert_ne!(client.worker_id(), h.client.worker_id());
    let id = uuid::Uuid::new_v4();
    let old: RemoteTask = serde_json::from_value(task_json(id)).unwrap();
    let mut new = old.clone();
    new.claim_epoch += 1;
    let worker = Arc::clone(&h.worker);
    let first = tokio::spawn(async move { worker.execute(old, Duration::from_millis(100)).await });
    wait_for(|| calls.load(Ordering::SeqCst) == 1).await;
    let second = tokio::spawn(async move { next.execute(new, Duration::from_millis(100)).await });
    wait_for(|| calls.load(Ordering::SeqCst) == 2 || server.count(&format!("{id}/release")) > 0)
        .await;
    let observed = calls.load(Ordering::SeqCst);
    gate.add_permits(2);
    first.await.unwrap();
    second.await.unwrap();
    assert_eq!(
        observed, 1,
        "worker identity changes must not duplicate invocation"
    );
    let releases = server.bodies(&format!("{id}/release"));
    assert_eq!(releases.len(), 1);
    assert_eq!(releases[0]["worker_id"], client.worker_id());
    assert_eq!(releases[0]["claim_epoch"], 8);
    assert_eq!(releases[0]["started"], false);
}

#[tokio::test]
async fn surplus_mobile_releases_are_bounded_and_block_additional_polls() {
    let release_gate = Arc::new(Semaphore::new(0));
    let tasks = (0..101).map(|_| task_json(uuid::Uuid::new_v4())).collect();
    let server = crate::test_support::spawn_control_plane_gated(
        route_with(tasks, 200, 200),
        Some(("/release".into(), Arc::clone(&release_gate))),
    )
    .await;
    let (h, handler_gate, calls) = admission_harness(&server).await;
    h.worker.spawn(&tokio::runtime::Handle::current());
    wait_for(|| calls.load(Ordering::SeqCst) == 1 && server.count("/release") >= 8).await;
    handler_gate.add_permits(1);
    wait_for(|| server.count("/complete") == 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let active_releases = server.count("/release");
    let polls = server.count("/workers/tasks/poll");
    h.worker.stop();
    release_gate.add_permits(100);
    wait_for(|| server.count("/release") == 100).await;
    assert!(
        active_releases <= 8,
        "observed {active_releases} simultaneous blocked release requests"
    );
    assert_eq!(
        polls, 1,
        "a pending cleanup batch must backpressure claiming even after the callback ends"
    );
}

#[tokio::test]
async fn surplus_delivery_preserves_a_previously_recorded_outcome() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let (h, gate, calls) = admission_harness(&server).await;
    let active = uuid::Uuid::new_v4();
    h.worker.spawn_task(
        serde_json::from_value(task_json(active)).unwrap(),
        Duration::from_millis(100),
    );
    wait_for(|| calls.load(Ordering::SeqCst) == 1).await;
    let known: RemoteTask = serde_json::from_value(task_json(uuid::Uuid::new_v4())).unwrap();
    h.store
        .insert(&known, &h.client.worker_id(), h.client.api_base())
        .await
        .unwrap();
    let key = ClaimKey::for_task(&known, &h.client.worker_id(), h.client.api_base());
    h.store.mark_started(&key).await.unwrap();
    let outcome = Outcome::Complete {
        output: json!({"private": "already-performed"}),
    };
    h.store.record_outcome(&key, &outcome).await.unwrap();
    h.worker
        .spawn_task(known.clone(), Duration::from_millis(100));
    wait_for(|| h.worker.admission.available() == 0).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.add_permits(1);
    wait_for(|| server.count(&format!("{active}/complete")) == 1).await;
    wait_for(|| h.worker.admission.available() == 1).await;
    assert_eq!(
        server.count(&format!("{}/release", known.id)),
        0,
        "capacity must not replace a known outcome with an unstarted release"
    );
    assert_eq!(
        h.store.get(&key).await.unwrap().unwrap().outcome,
        Some(outcome)
    );
    assert_eq!(drain_orphans(&h.client, &h.store, None).await, 1);
    assert_eq!(server.count(&format!("{}/complete", known.id)), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn pending_mobile_release_fences_redelivery_after_callback_capacity_returns() {
    let release_gate = Arc::new(Semaphore::new(0));
    let server = crate::test_support::spawn_control_plane_gated(
        route_with(Vec::new(), 200, 200),
        Some(("/release".into(), Arc::clone(&release_gate))),
    )
    .await;
    let (h, gate, calls) = admission_harness(&server).await;
    let active = uuid::Uuid::new_v4();
    h.worker.spawn_task(
        serde_json::from_value(task_json(active)).unwrap(),
        Duration::from_millis(100),
    );
    wait_for(|| calls.load(Ordering::SeqCst) == 1).await;
    let pending: RemoteTask = serde_json::from_value(task_json(uuid::Uuid::new_v4())).unwrap();
    h.worker
        .spawn_task(pending.clone(), Duration::from_millis(100));
    wait_for(|| server.count("/release") == 1).await;
    gate.add_permits(1);
    wait_for(|| server.count("/complete") == 1).await;
    h.worker.spawn_task(pending, Duration::from_millis(100));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let observed = calls.load(Ordering::SeqCst);
    release_gate.add_permits(1);
    wait_for(|| h.worker.admission.available() == 1).await;
    assert_eq!(observed, 1);
    assert_eq!(server.count("/release"), 1);
}

#[tokio::test]
async fn unpolled_mobile_task_future_owns_and_returns_shared_capacity() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let (h, _gate, calls) = admission_harness(&server).await;
    let task = serde_json::from_value(task_json(uuid::Uuid::new_v4())).unwrap();
    let future = h
        .worker
        .task_future(task, Duration::from_millis(100))
        .unwrap();
    assert_eq!(h.worker.stats().in_flight, 1);
    assert_eq!(h.worker.admission.available(), 0);
    drop(future);
    assert_eq!(h.worker.stats().in_flight, 0);
    assert_eq!(h.worker.admission.available(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(h.store.count().await.unwrap(), 0);
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn replacement_worker_does_not_overlap_its_predecessors_pending_poll() {
    let poll_gate = Arc::new(Semaphore::new(0));
    let server = crate::test_support::spawn_control_plane_gated(
        route_with(vec![task_json(uuid::Uuid::new_v4())], 200, 200),
        Some(("/workers/tasks/poll".into(), Arc::clone(&poll_gate))),
    )
    .await;
    let (h, _gate, calls) = admission_harness(&server).await;
    h.worker.spawn(&tokio::runtime::Handle::current());
    wait_for(|| server.count("/workers/tasks/poll") == 1).await;
    h.worker.stop();
    let next = successor(&h);
    next.spawn(&tokio::runtime::Handle::current());
    tokio::time::sleep(Duration::from_millis(100)).await;
    let pending_polls = server.count("/workers/tasks/poll");
    poll_gate.add_permits(1);
    wait_for(|| server.count("/workers/tasks/poll") >= 2).await;
    next.stop();
    wait_for(|| server.count("/release") == 1).await;
    assert_eq!(pending_polls, 1);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the stopped predecessor must release its late response unstarted"
    );
}

#[tokio::test]
async fn unreadable_start_evidence_cannot_be_released_as_unstarted() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    let task: RemoteTask = serde_json::from_value(task_json(uuid::Uuid::new_v4())).unwrap();
    h.store
        .insert(&task, &h.client.worker_id(), h.client.api_base())
        .await
        .unwrap();
    let key = ClaimKey::for_task(&task, &h.client.worker_id(), h.client.api_base());
    h.store.mark_started(&key).await.unwrap();
    let outcome = Outcome::Complete {
        output: json!({"receipt": "already-performed"}),
    };
    h.store.record_outcome(&key, &outcome).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_claim_insert BEFORE INSERT ON mobile_worker_claims BEGIN SELECT RAISE(FAIL, 'injected journal failure'); END").execute(&h.pool).await.unwrap();
    h.worker
        .execute(task.clone(), Duration::from_millis(100))
        .await;
    assert_eq!(
        server.count(&format!("{}/release", task.id)),
        0,
        "a journal error does not prove the prior effect never started"
    );
    assert!(h.inputs.lock().unwrap().is_empty());
    assert_eq!(
        h.store.get(&key).await.unwrap().unwrap().outcome,
        Some(outcome)
    );
    assert_eq!(drain_orphans(&h.client, &h.store, None).await, 1);
}

#[tokio::test]
async fn concurrent_recovery_drains_share_the_cleanup_io_bound() {
    let release_gate = Arc::new(Semaphore::new(0));
    let server = crate::test_support::spawn_control_plane_gated(
        route_with(Vec::new(), 200, 200),
        Some(("/release".into(), Arc::clone(&release_gate))),
    )
    .await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    for _ in 0..20 {
        seed(&h.store, &h.client, false, None).await;
    }
    let mut drains = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let client = Arc::clone(&h.client);
        let store = h.store.clone();
        drains.spawn(async move { drain_orphans(&client, &store, None).await });
    }
    wait_for(|| server.count("/release") >= 8).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let simultaneous = server.count("/release");
    release_gate.add_permits(20);
    let mut resolved = 0;
    while let Some(result) = drains.join_next().await {
        resolved += result.unwrap();
    }
    assert!(
        simultaneous <= 8,
        "observed {simultaneous} concurrent recovery release requests"
    );
    assert_eq!(resolved, 20);
    assert_eq!(server.count("/release"), 20);
    assert_eq!(h.store.count().await.unwrap(), 0);
}

#[tokio::test]
async fn rejected_recovery_keeps_all_claim_evidence_until_authority_resolves_it() {
    for rejected in [400, 401, 403, 405, 422, 429, 503] {
        let status = Arc::new(std::sync::atomic::AtomicU16::new(rejected));
        let current = Arc::clone(&status);
        let server = spawn_control_plane(Arc::new(move |_, _, _| {
            (current.load(Ordering::Acquire), json!({}))
        }))
        .await;
        let h = harness(&server, ok_handler(Duration::ZERO)).await;
        seed(&h.store, &h.client, false, None).await;
        seed(&h.store, &h.client, true, None).await;
        seed(
            &h.store,
            &h.client,
            true,
            Some(Outcome::Complete {
                output: json!({"receipt": "performed"}),
            }),
        )
        .await;
        seed(
            &h.store,
            &h.client,
            true,
            Some(Outcome::Fail {
                message: "known failure".into(),
                retryable: false,
            }),
        )
        .await;
        assert_eq!(
            drain_orphans(&h.client, &h.store, None).await,
            0,
            "status {rejected} does not prove the claim ended"
        );
        assert_eq!(h.store.count().await.unwrap(), 4);
        status.store(200, Ordering::Release);
        assert_eq!(drain_orphans(&h.client, &h.store, None).await, 4);
        assert_eq!(h.store.count().await.unwrap(), 0);
        assert!(h.inputs.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn rejected_legacy_release_fallback_retains_started_evidence() {
    for rejected in [400, 401, 403, 405, 503] {
        let status = Arc::new(std::sync::atomic::AtomicU16::new(rejected));
        let current = Arc::clone(&status);
        let server = spawn_control_plane(Arc::new(move |_, path, _| {
            (
                if path.ends_with("/release") {
                    404
                } else {
                    current.load(Ordering::Acquire)
                },
                json!({}),
            )
        }))
        .await;
        let h = harness(&server, ok_handler(Duration::ZERO)).await;
        let id = seed(&h.store, &h.client, true, None).await;
        assert_eq!(
            drain_orphans(&h.client, &h.store, None).await,
            0,
            "fallback status {rejected} is not a settlement acknowledgement"
        );
        assert!(h.store.list().await.unwrap()[0].started);
        status.store(200, Ordering::Release);
        assert_eq!(drain_orphans(&h.client, &h.store, None).await, 1);
        assert_eq!(server.bodies(&format!("{id}/release"))[1]["started"], true);
        assert!(
            server.bodies(&format!("{id}/fail"))[1]["message"]
                .as_str()
                .unwrap()
                .contains("unknown")
        );
    }
}

#[tokio::test]
async fn rejected_live_result_is_recovered_without_reinvoking_the_native_handler() {
    for rejected in [400, 401, 403, 405] {
        let status = Arc::new(std::sync::atomic::AtomicU16::new(rejected));
        let current = Arc::clone(&status);
        let server = spawn_control_plane(Arc::new(move |_, path, _| {
            (
                if path.ends_with("/heartbeat") {
                    200
                } else {
                    current.load(Ordering::Acquire)
                },
                json!({}),
            )
        }))
        .await;
        let h = harness(&server, ok_handler(Duration::ZERO)).await;
        let id = uuid::Uuid::new_v4();
        let task = serde_json::from_value(task_json(id)).unwrap();
        h.worker.execute(task, Duration::from_millis(100)).await;
        let rows = h.store.list().await.unwrap();
        assert_eq!(
            rows.len(),
            1,
            "status {rejected} must retain the known outcome"
        );
        assert!(
            matches!(&rows[0].outcome, Some(Outcome::Complete { output }) if output == &json!({"scanned":true}))
        );
        assert_eq!(h.worker.stats().completed, 0);
        status.store(200, Ordering::Release);
        assert_eq!(
            drain_orphans(&h.client, &h.store, Some(&h.worker.counters)).await,
            1
        );
        assert_eq!(h.worker.stats().completed, 1);
        assert_eq!(h.inputs.lock().unwrap().len(), 1);
        assert_eq!(server.count(&format!("{id}/release")), 0);
    }
}

#[tokio::test]
async fn recovery_does_not_forget_old_claims_without_server_authority() {
    let status = Arc::new(std::sync::atomic::AtomicU16::new(503));
    let current = Arc::clone(&status);
    let server = spawn_control_plane(Arc::new(move |_, _, _| {
        (current.load(Ordering::Acquire), json!({}))
    }))
    .await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    seed(&h.store, &h.client, false, None).await;
    seed(&h.store, &h.client, true, None).await;
    seed(
        &h.store,
        &h.client,
        true,
        Some(Outcome::Complete {
            output: json!({"receipt":"old-but-known"}),
        }),
    )
    .await;
    sqlx::query("UPDATE mobile_worker_claims SET claimed_at = '2010-01-01T00:00:00Z'")
        .execute(&h.pool)
        .await
        .unwrap();
    assert_eq!(drain_orphans(&h.client, &h.store, None).await, 0);
    assert_eq!(
        h.store.count().await.unwrap(),
        3,
        "age alone does not prove that the server ended these attempts"
    );
    status.store(200, Ordering::Release);
    assert_eq!(drain_orphans(&h.client, &h.store, None).await, 3);
    assert_eq!(
        server.bodies("/complete")[1]["output"],
        json!({"receipt":"old-but-known"})
    );
}

#[tokio::test]
async fn recovery_preserves_malformed_or_future_outcomes_instead_of_releasing_them() {
    for raw in [
        "{private-secret-invalid",
        r#"{"kind":"future-outcome","private":"private-secret"}"#,
    ] {
        let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
        let h = harness(&server, ok_handler(Duration::ZERO)).await;
        let bad = seed(&h.store, &h.client, true, None).await;
        sqlx::query("UPDATE mobile_worker_claims SET outcome = ? WHERE task_id = ?")
            .bind(raw)
            .bind(bad.to_string())
            .execute(&h.pool)
            .await
            .unwrap();
        seed(
            &h.store,
            &h.client,
            true,
            Some(Outcome::Complete {
                output: json!({"healthy":true}),
            }),
        )
        .await;
        assert_eq!(
            drain_orphans(&h.client, &h.store, None).await,
            1,
            "unknown evidence must not become an unknown-effect release"
        );
        assert_eq!(server.count(&format!("{bad}/release")), 0);
        let preserved: String =
            sqlx::query_scalar("SELECT outcome FROM mobile_worker_claims WHERE task_id = ?")
                .bind(bad.to_string())
                .fetch_one(&h.pool)
                .await
                .unwrap();
        assert_eq!(preserved, raw);
        let key = ClaimKey {
            task_id: bad,
            claim_epoch: 7,
            worker_id: h.client.worker_id(),
            api_base: h.client.api_base().into(),
        };
        let error = h.store.get(&key).await.unwrap_err().to_string();
        assert!(
            !error.contains("private-secret"),
            "diagnostics must not include stored outcome content"
        );
    }
}

#[tokio::test]
async fn recovery_reads_only_its_identity_even_when_another_outcome_is_corrupt() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    seed(
        &h.store,
        &h.client,
        true,
        Some(Outcome::Complete {
            output: json!({"healthy":true}),
        }),
    )
    .await;
    let foreign: RemoteTask = serde_json::from_value(task_json(uuid::Uuid::new_v4())).unwrap();
    h.store
        .insert(&foreign, "foreign-runtime", h.client.api_base())
        .await
        .unwrap();
    sqlx::query("UPDATE mobile_worker_claims SET started = 1, outcome = x'ff' WHERE worker_id = 'foreign-runtime'").execute(&h.pool).await.unwrap();
    assert_eq!(drain_orphans(&h.client, &h.store, None).await, 1);
    assert_eq!(h.store.count().await.unwrap(), 1);
    assert_eq!(server.count(&format!("{}/release", foreign.id)), 0);
    assert_eq!(server.count(&format!("{}/complete", foreign.id)), 0);
}

#[tokio::test]
async fn recovery_paging_keeps_every_epoch_when_prior_pages_are_deleted() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    for id in [uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2)] {
        for epoch in 1..=65 {
            let mut task: RemoteTask = serde_json::from_value(task_json(id)).unwrap();
            task.claim_epoch = epoch;
            h.store
                .insert(&task, &h.client.worker_id(), h.client.api_base())
                .await
                .unwrap();
        }
    }
    assert_eq!(drain_orphans(&h.client, &h.store, None).await, 130);
    assert_eq!(h.store.count().await.unwrap(), 0);
    let unique: HashSet<_> = server
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, _)| path.ends_with("/release"))
        .map(|(path, body)| (path.clone(), body["claim_epoch"].as_u64().unwrap()))
        .collect();
    assert_eq!(unique.len(), 130);
}

#[tokio::test]
async fn recovery_advances_past_a_full_page_of_invalid_identity_keys() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    for index in 0..65 {
        sqlx::query("INSERT INTO mobile_worker_claims (task_id,claim_epoch,worker_id,handler_name,api_base,started,claimed_at) VALUES (?,7,?,'scan',?,1,'2026-10-24T00:00:00Z')")
            .bind(format!("!not-a-task-{index:03}"))
            .bind(h.client.worker_id()).bind(h.client.api_base())
            .execute(&h.pool).await.unwrap();
    }
    seed(
        &h.store,
        &h.client,
        true,
        Some(Outcome::Complete {
            output: json!({"healthy":true}),
        }),
    )
    .await;
    assert_eq!(drain_orphans(&h.client, &h.store, None).await, 1);
    assert_eq!(h.store.count().await.unwrap(), 65);
    assert_eq!(server.count("/complete"), 1);
    assert_eq!(server.count("/release"), 0);
}

#[tokio::test]
async fn recovery_clears_claims_when_the_server_explicitly_fences_them() {
    for status in [404, 409, 410] {
        let server = spawn_control_plane(route_with(Vec::new(), status, status)).await;
        let h = harness(&server, ok_handler(Duration::ZERO)).await;
        seed(&h.store, &h.client, false, None).await;
        seed(&h.store, &h.client, true, None).await;
        seed(
            &h.store,
            &h.client,
            true,
            Some(Outcome::Complete {
                output: json!({"receipt":"performed"}),
            }),
        )
        .await;
        assert_eq!(drain_orphans(&h.client, &h.store, None).await, 3);
        assert_eq!(h.store.count().await.unwrap(), 0);
        assert!(h.inputs.lock().unwrap().is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn denied_renewal_stops_owned_native_work_but_retains_started_evidence() {
    let handler = ok_handler(Duration::from_secs(2));
    let entered = Arc::clone(&handler.inputs);
    let restored = Arc::new(AtomicBool::new(false));
    let healthy = Arc::clone(&restored);
    let server = spawn_control_plane(Arc::new(move |_, _, _| {
        let denied = !entered.lock().unwrap().is_empty() && !healthy.load(Ordering::Acquire);
        (if denied { 403 } else { 200 }, json!({}))
    }))
    .await;
    let h = harness(&server, handler).await;
    let id = uuid::Uuid::new_v4();
    let task = serde_json::from_value(task_json(id)).unwrap();
    let start = tokio::time::Instant::now();
    h.worker.execute(task, Duration::from_millis(50)).await;
    let elapsed = start.elapsed();
    let quarantined = h.worker.deps.stragglers.blocks("scan");
    restored.store(true, Ordering::Release);
    let rows = h.store.list().await.unwrap();
    wait_for(|| !h.worker.deps.stragglers.blocks("scan")).await;
    assert!(
        elapsed < Duration::from_secs(1),
        "denied renewal must stop owned work immediately, elapsed: {elapsed:?}"
    );
    assert!(
        quarantined,
        "the physical native call must remain quarantined"
    );
    assert_eq!(rows.len(), 1);
    assert!(rows[0].started);
    assert_eq!(rows[0].outcome, None);
    assert_eq!(
        h.worker.stats().lost,
        0,
        "authorization denial is not proof of ownership loss"
    );
    assert_eq!(drain_orphans(&h.client, &h.store, None).await, 1);
    assert!(
        server
            .bodies(&format!("{id}/release"))
            .iter()
            .all(|body| body["started"] == true)
    );
    assert_eq!(server.count(&format!("{id}/complete")), 0);
    assert_eq!(h.inputs.lock().unwrap().len(), 1);
}

#[path = "worker/worker_tests/prepared_admission.rs"]
mod prepared_admission;

#[path = "worker/worker_tests/process_ownership.rs"]
mod process_ownership;
