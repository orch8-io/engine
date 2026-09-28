//! Engine-level tests for crash recovery, builtins, and the runtime-node /
//! worker `UniFFI` surface.

use std::sync::atomic::AtomicUsize;
use std::sync::{Condvar, Mutex as StdMutex};

use serde_json::json;

use super::*;
use crate::test_support::{Route, spawn_control_plane};
use orch8_storage::InstanceStore as _;

fn sequence(name: &str, blocks: &serde_json::Value) -> String {
    json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "tenant_id": "mobile",
        "namespace": "default",
        "name": name,
        "version": 1,
        "deprecated": false,
        "blocks": blocks,
        "created_at": chrono::Utc::now().to_rfc3339()
    })
    .to_string()
}

fn step(id: &str, handler: &str) -> serde_json::Value {
    json!({ "type": "step", "id": id, "handler": handler, "params": {}, "cancellable": true })
}

fn wait_terminal(engine: &MobileEngine, id: &str) -> InstanceStateKind {
    for _ in 0..200 {
        let state = engine.get_instance(id.to_string()).unwrap().state;
        if matches!(
            state,
            InstanceStateKind::Completed | InstanceStateKind::Failed | InstanceStateKind::Cancelled
        ) {
            return state;
        }
        let _ = engine.tick_once();
        std::thread::sleep(Duration::from_millis(20));
    }
    engine.get_instance(id.to_string()).unwrap().state
}

/// Blocks inside `execute` until the gate opens.
struct GatedHandler {
    entered: Arc<AtomicUsize>,
    gate: Arc<(StdMutex<bool>, Condvar)>,
}

impl StepHandler for GatedHandler {
    fn execute(&self, _step: String, _input: String) -> Result<String, HandlerError> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        let (lock, cvar) = &*self.gate;
        let mut open = lock.lock().unwrap();
        while !*open {
            open = cvar.wait(open).unwrap();
        }
        Ok(r#"{"from":"killed-process"}"#.into())
    }
}

struct CountingHandler(Arc<AtomicUsize>);

impl StepHandler for CountingHandler {
    fn execute(&self, _step: String, _input: String) -> Result<String, HandlerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(r#"{"from":"restarted-process"}"#.into())
    }
}

struct CompletionCounter(Arc<AtomicUsize>);

impl EngineListener for CompletionCounter {
    fn on_instance_completed(&self, _instance_id: String, _output: String) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn on_instance_failed(&self, _instance_id: String, _error: String) {}
    fn on_step_pending(&self, _instance_id: String, _step_name: String, _handler: String) {}
}

fn raw_state_and_outputs(path: &str, instance_id: &str) -> (InstanceState, usize, usize) {
    use orch8_storage::OutputStore as _;
    let rt = runtime::MobileRuntime::new(1).unwrap();
    rt.block_on(async {
        let storage = orch8_storage::sqlite::SqliteStorage::file_mobile(path)
            .await
            .unwrap();
        let id = parse_instance_id(instance_id).unwrap();
        let outputs = storage.get_all_outputs(id).await.unwrap();
        let sentinels = outputs
            .iter()
            .filter(|o| o.output.get("_sentinel").is_some())
            .count();
        let state = storage.get_instance(id).await.unwrap().unwrap().state;
        (state, outputs.len() - sentinels, sentinels)
    })
}

fn wait_until(mut done: impl FnMut() -> bool) {
    for _ in 0..300 {
        if done() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("condition not met in time");
}

/// P0: the OS kills the app while a (replay-safe) step runs. The next
/// `MobileEngine::new` on the same database must recover the instance — no
/// dirty `pause` ever happened — and finish it exactly once.
#[test]
fn reopen_after_kill_mid_step_recovers_and_completes_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("engine.db").to_string_lossy().to_string();

    let instance_id = {
        let engine = MobileEngine::new(path.clone(), MobileEngineConfig::default()).unwrap();
        engine
            .load_sequence_from_json(sequence(
                "crashy",
                &json!([
                    { "type": "step", "id": "s1", "handler": "sleep",
                      "params": { "duration_ms": 1500 }, "cancellable": true },
                    step("s2", "log")
                ]),
            ))
            .unwrap();
        let id = engine.start("crashy".into(), "{}".into(), None).unwrap();
        // The foreground loop runs ticks on the engine runtime, like an app.
        engine.resume();
        wait_until(|| raw_state_and_outputs(&path, &id).2 == 1);
        // "Kill": drop the engine mid-step without pause()/shutdown(). The
        // runtime goes away with the step future, exactly like process death.
        drop(engine);
        id
    };

    let (state, real_outputs, _) = raw_state_and_outputs(&path, &instance_id);
    assert_eq!(
        state,
        InstanceState::Running,
        "kill leaves the instance Running"
    );
    assert_eq!(real_outputs, 0, "the killed step never produced output");

    let completions = Arc::new(AtomicUsize::new(0));
    let engine = MobileEngine::new(path.clone(), MobileEngineConfig::default()).unwrap();
    assert_eq!(
        engine.get_instance(instance_id.clone()).unwrap().state,
        InstanceStateKind::Scheduled,
        "startup recovery must reschedule the orphaned instance"
    );
    engine.set_listener(Arc::new(CompletionCounter(Arc::clone(&completions))));
    assert_eq!(
        wait_terminal(&engine, &instance_id),
        InstanceStateKind::Completed
    );
    for _ in 0..5 {
        let _ = engine.tick_once();
    }
    assert_eq!(
        completions.load(Ordering::SeqCst),
        1,
        "instance completes exactly once"
    );
    let (_, real_outputs, _) = raw_state_and_outputs(&path, &instance_id);
    assert_eq!(real_outputs, 2, "each step produced exactly one output");
}

/// A kill in the middle of an app-native (side-effecting) handler must not
/// leave the instance hanging in `Running` either — but it must not blindly
/// re-run the native effect: the engine's at-most-once effect guard turns the
/// ambiguous dispatch into a terminal failure the host is told about once.
#[test]
fn reopen_after_kill_mid_native_step_never_replays_the_effect() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("engine.db").to_string_lossy().to_string();
    let entered = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new((StdMutex::new(false), Condvar::new()));

    let instance_id = {
        let engine = MobileEngine::new(path.clone(), MobileEngineConfig::default()).unwrap();
        engine
            .register_handler(
                "charge".into(),
                Arc::new(GatedHandler {
                    entered: Arc::clone(&entered),
                    gate: Arc::clone(&gate),
                }),
            )
            .unwrap();
        engine
            .load_sequence_from_json(sequence("pay", &json!([step("s1", "charge")])))
            .unwrap();
        let id = engine.start("pay".into(), "{}".into(), None).unwrap();
        engine.resume();
        wait_until(|| entered.load(Ordering::SeqCst) == 1);
        // The native callback is a thread the OS would freeze with the
        // process; here it returns only after the runtime is gone.
        let opener = Arc::clone(&gate);
        let unblock = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            *opener.0.lock().unwrap() = true;
            opener.1.notify_all();
        });
        drop(engine);
        unblock.join().unwrap();
        id
    };
    assert_eq!(
        raw_state_and_outputs(&path, &instance_id).0,
        InstanceState::Running
    );

    let calls = Arc::new(AtomicUsize::new(0));
    let failures = Arc::new(AtomicUsize::new(0));
    let engine = MobileEngine::new(path, MobileEngineConfig::default()).unwrap();
    engine
        .register_handler(
            "charge".into(),
            Arc::new(CountingHandler(Arc::clone(&calls))),
        )
        .unwrap();
    engine.set_listener(Arc::new(FailureCounter(Arc::clone(&failures))));
    assert_eq!(
        wait_terminal(&engine, &instance_id),
        InstanceStateKind::Failed
    );
    for _ in 0..5 {
        let _ = engine.tick_once();
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "ambiguous native effect is not replayed"
    );
    assert_eq!(
        failures.load(Ordering::SeqCst),
        1,
        "host is told exactly once"
    );
}

struct Sign(Arc<StdMutex<Vec<String>>>);

impl StepHandler for Sign {
    fn execute(&self, _step: String, input: String) -> Result<String, HandlerError> {
        self.0.lock().unwrap().push(input);
        Ok(r#"{"signed":true}"#.into())
    }
}

struct FailureCounter(Arc<AtomicUsize>);

impl EngineListener for FailureCounter {
    fn on_instance_completed(&self, _instance_id: String, _output: String) {}
    fn on_instance_failed(&self, _instance_id: String, _error: String) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn on_step_pending(&self, _instance_id: String, _step_name: String, _handler: String) {}
}

#[test]
fn pure_builtins_run_without_host_handlers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("engine.db").to_string_lossy().to_string();
    let engine = MobileEngine::new(path, MobileEngineConfig::default()).unwrap();
    engine
        .load_sequence_from_json(sequence(
            "pure",
            &json!([step("a", "noop"), step("b", "log")]),
        ))
        .unwrap();
    let id = engine.start("pure".into(), "{}".into(), None).unwrap();
    assert_eq!(wait_terminal(&engine, &id), InstanceStateKind::Completed);
}

#[test]
fn network_builtins_are_opt_in_and_server_only_builtins_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("engine.db").to_string_lossy().to_string();
    let engine = MobileEngine::new(path, MobileEngineConfig::default()).unwrap();
    let has = |name: &str| engine.handlers.read().unwrap().contains(name);
    assert!(has("log"));
    assert!(!has("http_request"));
    engine.enable_builtin("http_request".into()).unwrap();
    assert!(has("http_request"));
    for server_only in ["email", "llm_call", "blob_put", "send_signal"] {
        assert!(
            engine.enable_builtin(server_only.into()).is_err(),
            "{server_only}"
        );
    }
    // Host handlers override a builtin of the same name.
    let calls = Arc::new(AtomicUsize::new(0));
    engine
        .register_handler("log".into(), Arc::new(CountingHandler(Arc::clone(&calls))))
        .unwrap();
    engine
        .load_sequence_from_json(sequence("override", &json!([step("a", "log")])))
        .unwrap();
    let id = engine.start("override".into(), "{}".into(), None).unwrap();
    assert_eq!(wait_terminal(&engine, &id), InstanceStateKind::Completed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        has("http_request"),
        "enabled builtins survive handler registration"
    );
}

#[test]
fn register_node_requires_sync_credentials_and_https() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("engine.db").to_string_lossy().to_string();
    let engine = MobileEngine::new(path, MobileEngineConfig::default()).unwrap();
    assert!(matches!(
        engine.register_node(NodeCapabilities::default()),
        Err(MobileError::InvalidInput { .. })
    ));
    let insecure = NodeCapabilities {
        api_base_url: Some("http://api.example.com/api/v1".into()),
        ..NodeCapabilities::default()
    };
    // Still missing device id / key.
    assert!(engine.register_node(insecure).is_err());
    assert!(matches!(
        engine.start_worker(WorkerOptions::default()),
        Err(MobileError::InvalidInput { .. })
    ));
    assert!(engine.run_worker_window(10).is_err());
    assert_eq!(engine.worker_stats().claimed, 0);
}

#[test]
fn runtime_id_is_stable_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("engine.db").to_string_lossy().to_string();
    let first = {
        let engine = MobileEngine::new(path.clone(), MobileEngineConfig::default()).unwrap();
        let id = engine.node_runtime_id().unwrap();
        assert_eq!(id, engine.node_runtime_id().unwrap());
        engine.shutdown();
        id
    };
    let engine = MobileEngine::new(path, MobileEngineConfig::default()).unwrap();
    assert_eq!(engine.node_runtime_id().unwrap(), first);
    assert!(uuid::Uuid::parse_str(&first).is_ok());
}

#[test]
fn push_wake_envelope_is_filtered_by_runtime_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("engine.db").to_string_lossy().to_string();
    let engine = MobileEngine::new(path, MobileEngineConfig::default()).unwrap();
    let own = engine.node_runtime_id().unwrap();
    assert!(engine.on_push_wake(json!({ "runtime_id": own, "reason": "task" }).to_string()));
    assert!(engine.on_push_wake(json!({ "task_id": "t1" }).to_string()));
    assert!(
        !engine.on_push_wake(json!({ "runtime_id": uuid::Uuid::new_v4().to_string() }).to_string())
    );
    assert!(!engine.on_push_wake("not json".into()));
}

/// Full UniFFI-surface flow against a mock control plane: register the
/// device + runtime, start the worker, receive a task, run the app-native
/// handler with `effect_id`, complete it.
#[test]
fn registered_node_worker_executes_remote_task_end_to_end() {
    let server_rt = tokio::runtime::Runtime::new().unwrap();
    let task_id = uuid::Uuid::new_v4();
    let handed_out = Arc::new(AtomicBool::new(false));
    let handed = Arc::clone(&handed_out);
    let route: Route = Arc::new(move |_m, path, _b| {
        if path.ends_with("/workers/tasks/poll") {
            let tasks = if handed.swap(true, Ordering::SeqCst) {
                json!([])
            } else {
                json!([{
                    "id": task_id, "instance_id": uuid::Uuid::new_v4(), "block_id": "b",
                    "handler_name": "sign", "params": {"doc": 1}, "context": {},
                    "attempt": 1, "claim_epoch": 1, "effect_id": "eff-e2e", "lease_secs": 120
                }])
            };
            return (
                200,
                json!({ "tasks": tasks, "lease_secs": 120, "poll_after_ms": 0 }),
            );
        }
        if path.ends_with("/mobile/devices/register") || path.contains("/runtime") {
            return (201, json!({}));
        }
        (200, json!({}))
    });
    let server = server_rt.block_on(spawn_control_plane(route));

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("engine.db").to_string_lossy().to_string();
    let config = MobileEngineConfig {
        device_id: "phone-1".into(),
        ..MobileEngineConfig::default()
    };
    let engine = MobileEngine::new(path, config).unwrap();
    let inputs = Arc::new(StdMutex::new(Vec::<String>::new()));
    engine
        .register_handler("sign".into(), Arc::new(Sign(Arc::clone(&inputs))))
        .unwrap();

    let runtime_id = engine.node_runtime_id().unwrap();
    let client = node::NodeClient::new_unchecked(
        server.base.clone(),
        "key".into(),
        "phone-1".into(),
        orch8_types::continuity::RuntimeId::from_uuid(uuid::Uuid::parse_str(&runtime_id).unwrap()),
        node::Advertisement {
            handlers: engine.advertised_handlers(&NodeCapabilities::default()),
            caps: NodeCapabilities::default(),
            draining: false,
        },
    );
    let registration = engine.register_with_client(client).unwrap();
    assert_eq!(registration.runtime_id, runtime_id);
    assert_eq!(registration.handlers, vec!["sign".to_string()]);

    let advertised = &server.bodies("/mobile/devices/phone-1/runtime")[0];
    assert_eq!(advertised["capabilities"]["kind"], "mobile");
    assert_eq!(advertised["capabilities"]["runtime_id"], runtime_id);
    assert_eq!(
        server.bodies("/mobile/devices/register")[0]["device_id"],
        "phone-1"
    );

    engine.start_worker(WorkerOptions::default()).unwrap();
    assert!(
        engine.start_worker(WorkerOptions::default()).is_err(),
        "single worker"
    );
    for _ in 0..200 {
        if server.count(&format!("{task_id}/complete")) == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let complete = &server.bodies(&format!("{task_id}/complete"))[0];
    assert_eq!(complete["output"], json!({ "signed": true }));
    assert_eq!(complete["worker_id"], runtime_id);
    let input: serde_json::Value = serde_json::from_str(&inputs.lock().unwrap()[0]).unwrap();
    assert_eq!(input["__orch8"]["effect_id"], "eff-e2e");
    assert_eq!(input["doc"], 1);

    // Handler registration is frozen while the worker holds the registry.
    assert!(
        engine
            .register_handler("late".into(), Arc::new(Sign(Arc::clone(&inputs))))
            .is_err()
    );

    engine.unregister_node();
    let last_ad = server.bodies("/mobile/devices/phone-1/runtime");
    assert_eq!(last_ad.last().unwrap()["capabilities"]["draining"], true);
    assert!(!engine.worker_stats().running);
    engine.shutdown();
}
