use super::*;
use std::sync::atomic::AtomicUsize;

fn engine() -> (Arc<MobileEngine>, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let engine = MobileEngine::new(
        directory
            .path()
            .join("shutdown.db")
            .to_string_lossy()
            .into_owned(),
        MobileEngineConfig::default(),
    )
    .unwrap();
    (engine, directory)
}

struct RetainingCallback {
    engine: Arc<StdMutex<Option<Arc<MobileEngine>>>>,
    dropped: Arc<AtomicBool>,
}

impl Drop for RetainingCallback {
    fn drop(&mut self) {
        let retained = self.engine.lock().unwrap().take();
        drop(retained);
        self.dropped.store(true, Ordering::Release);
    }
}

impl TokenProvider for RetainingCallback {
    fn current_token(&self) -> String {
        "dst_cached".into()
    }
    fn refresh_token(&self) -> Result<String, MobileError> {
        Ok("dst_new".into())
    }
}

impl StepHandler for RetainingCallback {
    fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
        Ok("{}".into())
    }
}

impl EngineListener for RetainingCallback {
    fn on_instance_completed(&self, _: String, _: String) {}
    fn on_instance_failed(&self, _: String, _: String) {}
    fn on_step_pending(&self, _: String, _: String, _: String) {}
}

#[test]
fn shutdown_breaks_provider_handler_and_listener_engine_cycles() {
    let mut results = Vec::new();
    for role in ["provider", "handler", "listener"] {
        let (engine, _directory) = engine();
        let weak = Arc::downgrade(&engine);
        let holder = Arc::new(StdMutex::new(Some(Arc::clone(&engine))));
        let dropped = Arc::new(AtomicBool::new(false));
        let callback = Arc::new(RetainingCallback {
            engine: Arc::clone(&holder),
            dropped: Arc::clone(&dropped),
        });
        match role {
            "provider" => engine.set_token_provider(callback),
            "handler" => engine.register_handler("native".into(), callback).unwrap(),
            _ => engine.set_listener(callback),
        }
        engine.shutdown();
        engine.shutdown();
        let retired = dropped.load(Ordering::Acquire);
        drop(engine);
        let released = weak.upgrade().is_none();
        // The external holder lets the failing baseline release its retain
        // cycle before asserting, without leaking executors across cases.
        let retained = holder.lock().unwrap().take();
        drop(retained);
        results.push((role, retired, released));
    }
    assert!(
        results
            .iter()
            .all(|(_, retired, released)| *retired && *released),
        "{results:?}"
    );
}

#[test]
fn shutdown_is_terminal_for_common_storage_operations() {
    let (engine, _directory) = engine();
    engine.shutdown();
    assert!(matches!(
        engine.node_runtime_id(),
        Err(MobileError::Shutdown)
    ));
}

struct CountedProvider(Arc<AtomicUsize>);
impl TokenProvider for CountedProvider {
    fn current_token(&self) -> String {
        self.0.fetch_add(1, Ordering::SeqCst);
        "dst_late".into()
    }
    fn refresh_token(&self) -> Result<String, MobileError> {
        Ok("dst_refresh".into())
    }
}

#[test]
fn shutdown_refuses_new_host_credentials_without_invoking_them() {
    let (engine, _directory) = engine();
    engine.shutdown();
    let calls = Arc::new(AtomicUsize::new(0));
    engine.set_token_provider(Arc::new(CountedProvider(Arc::clone(&calls))));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(engine.credential.current().is_empty());
    assert!(engine.credential.request_snapshot().is_none());
}

struct InitialGate {
    entered: std::sync::mpsc::Sender<()>,
    gate: StdMutex<std::sync::mpsc::Receiver<()>>,
}
impl TokenProvider for InitialGate {
    fn current_token(&self) -> String {
        self.entered.send(()).unwrap();
        let _ = self.gate.lock().unwrap().recv();
        "dst_initial_after_shutdown".into()
    }
    fn refresh_token(&self) -> Result<String, MobileError> {
        Ok("dst_refresh".into())
    }
}

#[test]
fn shutdown_fences_an_initial_token_callback_already_in_progress() {
    let (engine, _directory) = engine();
    let (entered, started) = std::sync::mpsc::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let installing = Arc::clone(&engine);
    let thread = std::thread::spawn(move || {
        installing.set_token_provider(Arc::new(InitialGate {
            entered,
            gate: StdMutex::new(gate),
        }));
    });
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    engine.shutdown();
    release.send(()).unwrap();
    thread.join().unwrap();
    assert!(engine.credential.current().is_empty());
    assert!(engine.credential.request_snapshot().is_none());
}

#[test]
fn shutdown_revokes_existing_clients_without_building_or_sending_payloads() {
    use orch8_engine::remote_worker::{LeaseRequestError, LeaseResponse};
    let (engine, _directory) = engine();
    engine.set_token_provider(Arc::new(CountedProvider(Arc::new(AtomicUsize::new(0)))));
    engine.runtime.block_on(async {
        let server = crate::test_support::spawn_control_plane(Arc::new(|_, _, _| {
            (200, serde_json::json!({}))
        }))
        .await;
        let node = node::NodeClient::new_unchecked(
            server.base.clone(),
            Arc::clone(&engine.credential),
            "shutdown-device".into(),
            orch8_types::continuity::RuntimeId::new(),
            node::Advertisement {
                handlers: Vec::new(),
                caps: NodeCapabilities::default(),
                draining: false,
            },
        );
        engine.shutdown();
        let built = AtomicBool::new(false);
        let result = engine
            .credential
            .send(|token| {
                built.store(true, Ordering::SeqCst);
                reqwest::Client::new()
                    .post(&server.base)
                    .header("x-api-key", token)
            })
            .await;
        assert!(matches!(
            result,
            Err(LeaseRequestError::AuthorizationUnavailable)
        ));
        assert!(!built.load(Ordering::SeqCst));
        assert_eq!(
            node.heartbeat(uuid::Uuid::new_v4(), 1).await,
            LeaseResponse::Rejected
        );
        assert_eq!(server.count(""), 0);
    });
}

struct RefreshGate {
    entered: std::sync::mpsc::Sender<()>,
    gate: StdMutex<std::sync::mpsc::Receiver<()>>,
    finished: std::sync::mpsc::Sender<()>,
}
impl TokenProvider for RefreshGate {
    fn current_token(&self) -> String {
        "dst_cached".into()
    }
    fn refresh_token(&self) -> Result<String, MobileError> {
        self.entered.send(()).unwrap();
        let _ = self.gate.lock().unwrap().recv();
        Ok("dst_refresh_after_shutdown".into())
    }
}
impl Drop for RefreshGate {
    fn drop(&mut self) {
        let _ = self.finished.send(());
    }
}

#[test]
fn shutdown_cancels_refresh_waiters_but_keeps_running_host_ownership() {
    let (engine, _directory) = engine();
    let (entered, started) = std::sync::mpsc::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let (finished, retired) = std::sync::mpsc::channel();
    let provider = Arc::new(RefreshGate {
        entered,
        gate: StdMutex::new(gate),
        finished,
    });
    let weak = Arc::downgrade(&provider);
    engine.set_token_provider(provider);
    let credential = Arc::clone(&engine.credential);
    let generation = credential.snapshot().generation;
    let (completed, result) = std::sync::mpsc::channel();
    drop(engine.runtime.handle().spawn(async move {
        let ok = credential.refresh_generation(&generation).await;
        let _ = completed.send(ok);
    }));
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    engine.shutdown();
    let cancelled_before_host_returns = result.recv_timeout(Duration::from_secs(5));
    let host_still_owned = weak.upgrade().is_some();
    release.send(()).unwrap();
    retired.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(cancelled_before_host_returns, Ok(false));
    assert!(host_still_owned);
    assert!(weak.upgrade().is_none());
    assert!(engine.credential.current().is_empty());
}

#[test]
fn shutdown_never_polls_new_work_and_cancels_an_admitted_operation() {
    let (engine, _directory) = engine();
    let (entered, started) = std::sync::mpsc::channel();
    let (finished, result) = std::sync::mpsc::channel();
    let active = Arc::clone(&engine);
    let operation = std::thread::spawn(move || {
        let r: Result<(), MobileError> = active.run_with_timeout(async {
            entered.send(()).unwrap();
            std::future::pending().await
        });
        let _ = finished.send(matches!(r, Err(MobileError::Shutdown)));
    });
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    engine.shutdown();
    operation.join().unwrap();
    assert_eq!(result.recv_timeout(Duration::from_secs(5)), Ok(true));
    let polled = AtomicBool::new(false);
    let r: Result<(), MobileError> = engine.run_with_timeout(async {
        polled.store(true, Ordering::SeqCst);
        Ok(())
    });
    assert!(matches!(r, Err(MobileError::Shutdown)));
    assert!(!polled.load(Ordering::SeqCst));
}

struct ReentrantCallback {
    engine: std::sync::Weak<MobileEngine>,
    result: std::sync::mpsc::Sender<bool>,
}
impl Drop for ReentrantCallback {
    fn drop(&mut self) {
        let engine = self.engine.upgrade().unwrap();
        let unlocked =
            engine.handlers.try_read().is_ok() && engine.foreign_handlers.try_lock().is_ok();
        let _token = engine.credential.current();
        let _listener = engine.notifier.listener();
        let allowed = if engine.cancel.is_cancelled() {
            matches!(engine.node_runtime_id(), Err(MobileError::Shutdown))
        } else {
            engine.node_runtime_id().is_ok()
        };
        let _ = self.result.send(unlocked && allowed);
    }
}
impl TokenProvider for ReentrantCallback {
    fn current_token(&self) -> String {
        "dst_reentrant".into()
    }
    fn refresh_token(&self) -> Result<String, MobileError> {
        Ok("dst_fresh".into())
    }
}
impl StepHandler for ReentrantCallback {
    fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
        Ok("{}".into())
    }
}
impl EngineListener for ReentrantCallback {
    fn on_instance_completed(&self, _: String, _: String) {}
    fn on_instance_failed(&self, _: String, _: String) {}
    fn on_step_pending(&self, _: String, _: String, _: String) {}
}

#[test]
fn foreign_destruction_can_reenter_after_replacement_and_shutdown() {
    for role in ["provider", "handler", "listener"] {
        let (engine, _directory) = engine();
        let (finished, result) = std::sync::mpsc::channel();
        let owner = Arc::clone(&engine);
        let thread = std::thread::spawn(move || {
            for _ in 0..2 {
                let callback = Arc::new(ReentrantCallback {
                    engine: Arc::downgrade(&owner),
                    result: finished.clone(),
                });
                match role {
                    "provider" => owner.set_token_provider(callback),
                    "handler" => owner
                        .register_handler("reentrant".into(), callback)
                        .unwrap(),
                    _ => owner.set_listener(callback),
                }
            }
            owner.shutdown();
        });
        assert_eq!(
            result.recv_timeout(Duration::from_secs(5)),
            Ok(true),
            "{role} replacement"
        );
        assert_eq!(
            result.recv_timeout(Duration::from_secs(5)),
            Ok(true),
            "{role} shutdown"
        );
        thread.join().unwrap();
    }
}

#[test]
fn failed_handler_replacement_preserves_advertised_handler_metadata() {
    let (engine, _directory) = engine();
    let holder = Arc::new(StdMutex::new(None));
    let dropped = Arc::new(AtomicBool::new(false));
    engine
        .register_handler(
            "native".into(),
            Arc::new(RetainingCallback {
                engine: holder,
                dropped: Arc::clone(&dropped),
            }),
        )
        .unwrap();
    let shared = engine.handlers.read().unwrap().clone();
    let (finished, retired) = std::sync::mpsc::channel();
    assert!(
        engine
            .register_handler(
                "native".into(),
                Arc::new(ReentrantCallback {
                    engine: Arc::downgrade(&engine),
                    result: finished,
                })
            )
            .is_err()
    );
    assert_eq!(retired.recv_timeout(Duration::from_secs(5)), Ok(true));
    assert_eq!(
        engine.advertised_handlers(&NodeCapabilities::default()),
        vec!["native"]
    );
    assert!(!dropped.load(Ordering::Acquire));
    drop(shared);
    engine.shutdown();
    assert!(dropped.load(Ordering::Acquire));
}

#[test]
fn shutdown_refuses_handlers_listeners_and_worker_restart() {
    let (engine, _directory) = engine();
    engine.shutdown();
    let dropped = Arc::new(AtomicBool::new(false));
    let callback = Arc::new(RetainingCallback {
        engine: Arc::new(StdMutex::new(None)),
        dropped: Arc::clone(&dropped),
    });
    assert!(matches!(
        engine.register_handler("late".into(), callback.clone()),
        Err(MobileError::Shutdown)
    ));
    engine.set_listener(callback.clone());
    assert!(engine.notifier.listener().is_none());
    drop(callback);
    assert!(dropped.load(Ordering::Acquire));
    assert!(matches!(
        engine.enable_builtin("http_request".into()),
        Err(MobileError::Shutdown)
    ));
    assert!(matches!(
        engine.start_worker(WorkerOptions::default()),
        Err(MobileError::Shutdown)
    ));
    assert!(matches!(
        engine.start_delegation(DelegationOptions {
            tenant_id: "mobile".into(),
            poll_interval_ms: 2000,
            ttl_secs: 600
        }),
        Err(MobileError::Shutdown)
    ));
    assert!(matches!(
        engine.run_worker_window(1),
        Err(MobileError::Shutdown)
    ));
    engine.runtime.block_on(async {
        // Closed lifecycle calls must not enter another synchronous runtime
        // bridge when invoked from an engine-owned callback.
        engine.pause();
        engine.resume();
        engine.on_push_received();
        assert!(!engine.on_push_wake("{}".into()));
    });
    assert!(engine.current_worker().is_none());
    assert!(engine.current_delegation().is_none());
}

#[test]
fn enabling_an_existing_builtin_is_idempotent_with_a_shared_registry() {
    let (engine, _directory) = engine();
    let shared = engine.handlers.read().unwrap().clone();
    engine
        .enable_builtin(builtins::DEFAULT_BUILTINS[0].to_string())
        .unwrap();
    drop(shared);
    engine.shutdown();
    assert!(matches!(
        engine.enable_builtin(builtins::DEFAULT_BUILTINS[0].to_string()),
        Err(MobileError::Shutdown)
    ));
}
