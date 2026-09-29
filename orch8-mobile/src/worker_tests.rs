use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

use serde_json::json;

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
            Err(crate::HandlerError::Retryable { message }) => {
                Err(crate::HandlerError::Retryable {
                    message: message.clone(),
                })
            }
            Err(crate::HandlerError::Permanent { message }) => {
                Err(crate::HandlerError::Permanent {
                    message: message.clone(),
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
    let store = ClaimStore::new(sqlite.pool().clone());
    store.init_tables().await.unwrap();
    let inputs = Arc::clone(&handler.inputs);
    let mut registry = HandlerRegistry::new();
    register_foreign_handler(
        &mut registry,
        "scan",
        Arc::new(handler),
        Duration::from_secs(10),
        Arc::new(crate::stragglers::Stragglers::default()),
    );
    let client = NodeClient::new_unchecked(
        server.base.clone(),
        "key".into(),
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
            stragglers: Arc::new(crate::stragglers::Stragglers::default()),
        },
        WorkerOptions {
            max_concurrent_tasks: 1,
            idle_poll_interval_ms: idle_ms,
            version: Some("1.2.3".into()),
        },
    )
    .unwrap();
    Harness {
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
                message: "camera busy".into(),
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
    base: &str,
    started: bool,
    outcome: Option<Outcome>,
) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    let task: RemoteTask = serde_json::from_value(task_json(id)).unwrap();
    store.insert(&task, "w", base).await.unwrap();
    if started {
        store.mark_started(id).await.unwrap();
    }
    if let Some(outcome) = outcome {
        store.record_outcome(id, &outcome).await.unwrap();
    }
    id
}

#[tokio::test]
async fn restart_drain_releases_or_redelivers_every_orphaned_claim() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    let unstarted = seed(&h.store, &server.base, false, None).await;
    let started = seed(&h.store, &server.base, true, None).await;
    let finished = seed(
        &h.store,
        &server.base,
        true,
        Some(Outcome::Complete {
            output: json!({"n": 1}),
        }),
    )
    .await;

    let none = StdMutex::new(HashSet::new());
    let settled = drain_orphans(&h.client, &h.store, &none, None).await;
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
    let id = seed(&h.store, &server.base, true, None).await;
    let none = StdMutex::new(HashSet::new());
    assert_eq!(drain_orphans(&h.client, &h.store, &none, None).await, 1);
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
    seed(&h.store, &server.base, false, None).await;
    // In-flight claims are never touched by the drain.
    let busy = seed(&h.store, &server.base, false, None).await;
    let in_flight = StdMutex::new(std::iter::once(busy).collect());
    assert_eq!(
        drain_orphans(&h.client, &h.store, &in_flight, None).await,
        0
    );
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
