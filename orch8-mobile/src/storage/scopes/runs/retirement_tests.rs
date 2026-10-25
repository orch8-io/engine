//! Runtime-local cleanup must own foreign destruction independently of callers.
use super::tests::{fixture, scope};
use crate::{HandlerError, StepHandler, handlers::scoped::CatalogHandlers};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

struct DropGate {
    entered: std::sync::mpsc::Sender<()>,
    held: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl StepHandler for DropGate {
    fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
        Ok("{}".into())
    }
}
impl Drop for DropGate {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        let _ = self
            .held
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10));
    }
}

struct Gone(Arc<tokio::sync::Notify>);
impl StepHandler for Gone {
    fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
        Ok("{}".into())
    }
}
impl Drop for Gone {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
#[test]
fn dropping_router_does_not_run_a_foreign_destructor_on_its_caller() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (storage, sqlite) = runtime.block_on(fixture());
    let owner = runtime.block_on(scope(&storage, "alice", "one"));
    let hub = CatalogHandlers::isolated(storage, sqlite);
    let (entered, started) = std::sync::mpsc::channel();
    let (release, held) = std::sync::mpsc::channel();
    runtime
        .block_on(hub.bind(
            &owner,
            "held",
            Arc::new(DropGate {
                entered,
                held: Mutex::new(held),
            }),
            Duration::from_secs(5),
        ))
        .unwrap();
    let (finished, result) = std::sync::mpsc::channel();
    let caller = std::thread::spawn(move || {
        drop(hub);
        finished.send(()).unwrap();
    });
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    let returned_while_host_held = result.recv_timeout(Duration::from_secs(3)).is_ok();
    release.send(()).unwrap();
    caller.join().unwrap();
    assert!(
        returned_while_host_held,
        "router drop ran the foreign destructor on its caller"
    );
}

#[test]
fn shutdown_releases_idle_host_references_even_when_the_router_is_retained() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (storage, sqlite) = runtime.block_on(fixture());
    let owner = runtime.block_on(scope(&storage, "alice", "one"));
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite);
    let (entered, started) = std::sync::mpsc::channel();
    let (release, held) = std::sync::mpsc::channel();
    hub_shutdown_bind(&runtime, &hub, &owner, entered, held);
    hub.shutdown();
    let released_while_retained = started.recv_timeout(Duration::from_secs(3)).is_ok();
    release.send(()).unwrap();
    drop(hub);
    runtime
        .block_on(storage.catalog_metadata(&owner, "still-active"))
        .unwrap();
    assert!(
        released_while_retained,
        "shutdown retained every registered host reference"
    );
}

fn hub_shutdown_bind(
    runtime: &tokio::runtime::Runtime,
    hub: &CatalogHandlers,
    owner: &crate::storage::scopes::CatalogScope,
    entered: std::sync::mpsc::Sender<()>,
    held: std::sync::mpsc::Receiver<()>,
) {
    runtime
        .block_on(hub.bind(
            owner,
            "held",
            Arc::new(DropGate {
                entered,
                held: Mutex::new(held),
            }),
            Duration::from_secs(5),
        ))
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_owns_pending_references_and_prevents_late_registration_publication() {
    for retire_account in [false, true] {
        let (storage, sqlite) = fixture().await;
        let owner = scope(&storage, "alice", "one").await;
        let hub = CatalogHandlers::isolated(storage.clone(), sqlite);
        let (entered, release) = hub.hold_next_bind();
        let dropped = Arc::new(tokio::sync::Notify::new());
        let handler = Arc::new(Gone(dropped.clone()));
        let weak = Arc::downgrade(&handler);
        let caller = hub.clone();
        let target = owner.clone();
        let pending = tokio::spawn(async move {
            caller
                .bind(&target, "late", handler, Duration::from_secs(5))
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), entered)
            .await
            .unwrap()
            .unwrap();
        if retire_account {
            storage.retire_account_scope(&owner.account).await.unwrap();
            hub.release_retired(&owner).await.unwrap().await.unwrap();
        } else {
            for receipt in hub.close() {
                receipt.await.unwrap();
            }
        }
        tokio::time::timeout(Duration::from_secs(5), dropped.notified())
            .await
            .unwrap();
        assert!(
            weak.upgrade().is_none(),
            "pending storage observation retained the host object"
        );
        release.send(()).unwrap();
        assert!(matches!(
            pending.await.unwrap(),
            Err(orch8_types::error::StepError::ClaimLost)
        ));
        if !retire_account {
            storage.catalog_metadata(&owner, "active").await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_registration_does_not_drop_host_code_on_the_async_worker() {
    let (storage, sqlite) = fixture().await;
    let owner = scope(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage, sqlite);
    let (entered, release_read) = hub.hold_next_bind();
    let (dropping, observed) = std::sync::mpsc::channel();
    let (release_drop, held) = std::sync::mpsc::channel();
    let caller = hub.clone();
    let target = owner.clone();
    let pending = tokio::spawn(async move {
        caller
            .bind(
                &target,
                "pending",
                Arc::new(DropGate {
                    entered: dropping,
                    held: Mutex::new(held),
                }),
                Duration::from_secs(5),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .unwrap()
        .unwrap();
    pending.abort();
    let logical = tokio::time::timeout(Duration::from_secs(5), pending).await;
    observed.recv_timeout(Duration::from_secs(5)).unwrap();
    release_drop.send(()).unwrap();
    let _ = release_read.send(());
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
    assert!(matches!(logical, Ok(Err(error)) if error.is_cancelled()));
}

#[tokio::test]
async fn foreign_destruction_can_reenter_close_without_holding_the_registry_lock() {
    struct Reenter {
        owner: std::sync::Weak<CatalogHandlers>,
        result: std::sync::mpsc::Sender<bool>,
    }
    impl StepHandler for Reenter {
        fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
            Ok("{}".into())
        }
    }
    impl Drop for Reenter {
        fn drop(&mut self) {
            let owner = self.owner.upgrade().unwrap();
            let unlocked = owner.registry_unlocked();
            drop(owner.close());
            let _ = self.result.send(unlocked);
        }
    }
    let (storage, sqlite) = fixture().await;
    let owner = scope(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage, sqlite);
    let (result, observed) = std::sync::mpsc::channel();
    hub.bind(
        &owner,
        "reenter",
        Arc::new(Reenter {
            owner: Arc::downgrade(&hub),
            result,
        }),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let first = hub.close();
    let repeated = hub.close();
    for receipt in first.into_iter().chain(repeated) {
        tokio::time::timeout(Duration::from_secs(5), receipt)
            .await
            .unwrap()
            .unwrap();
    }
    assert!(observed.recv_timeout(Duration::from_secs(5)).unwrap());
}

#[test]
fn mobile_engine_lifecycle_owns_scoped_cleanup_without_retiring_the_account() {
    use crate::{MobileEngine, MobileEngineConfig};
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _serial = runtime.block_on(CatalogHandlers::shared_test_gate().lock_owned());
    for final_drop in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let engine = MobileEngine::new(
            root.path().join("engine.db").to_string_lossy().into_owned(),
            MobileEngineConfig::default(),
        )
        .unwrap();
        let hub = engine.catalog_handlers.clone();
        let storage = hub.test_storage();
        let owner = runtime.block_on(scope(&storage, "alice", "one"));
        let (entered, observed) = std::sync::mpsc::channel();
        let (release, held) = std::sync::mpsc::channel();
        hub_shutdown_bind(&runtime, &hub, &owner, entered, held);
        if final_drop {
            drop(engine);
        } else {
            engine.shutdown();
            drop(engine);
        }
        observed.recv_timeout(Duration::from_secs(5)).unwrap();
        release.send(()).unwrap();
        runtime.block_on(async {
            for receipt in hub.close() {
                tokio::time::timeout(Duration::from_secs(5), receipt)
                    .await
                    .unwrap()
                    .unwrap();
            }
            let inspector = crate::storage::MobileStorage::new(Arc::new(
                orch8_storage::sqlite::SqliteStorage::file_mobile(
                    root.path().join("engine.db").to_str().unwrap(),
                )
                .await
                .unwrap(),
            ));
            inspector
                .catalog_metadata(&owner, "still-active")
                .await
                .unwrap();
        });
    }
}

struct BodyGate {
    entered: std::sync::mpsc::Sender<()>,
    held: Mutex<std::sync::mpsc::Receiver<()>>,
    dropped: std::sync::mpsc::Sender<()>,
}
impl StepHandler for BodyGate {
    fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
        self.entered.send(()).unwrap();
        let _ = self
            .held
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10));
        Ok("{}".into())
    }
}
impl Drop for BodyGate {
    fn drop(&mut self) {
        let _ = self.dropped.send(());
    }
}

#[test]
fn queued_local_cleanup_survives_its_async_runtime_and_keeps_live_callback_ownership() {
    use super::callback_tests::{make_registry, running};
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (storage, sqlite) = runtime.block_on(fixture());
    let owner = runtime.block_on(scope(&storage, "alice", "one"));
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    let (entered, started) = std::sync::mpsc::channel();
    let (release, held) = std::sync::mpsc::channel();
    let (dropped, gone) = std::sync::mpsc::channel();
    let handler = Arc::new(BodyGate {
        entered,
        held: Mutex::new(held),
        dropped,
    });
    let weak = Arc::downgrade(&handler);
    runtime
        .block_on(hub.bind(&owner, "same", handler, Duration::from_secs(60)))
        .unwrap();
    let registry = make_registry(&hub);
    let ctx = runtime.block_on(running(&storage, &sqlite, &owner, "runtime-gone"));
    let task = runtime.spawn(registry.get("same").unwrap()(ctx));
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    let receipts = hub.close();
    task.abort();
    runtime.block_on(async {
        assert!(task.await.unwrap_err().is_cancelled());
    });
    let gate = hub.account_gate(&owner).unwrap();
    assert_eq!(gate.available_permits(), 0);
    assert!(weak.upgrade().is_some());
    drop(registry);
    drop(hub);
    runtime.shutdown_background();
    release.send(()).unwrap();
    gone.recv_timeout(Duration::from_secs(5)).unwrap();
    let observer = tokio::runtime::Runtime::new().unwrap();
    observer.block_on(async {
        for receipt in receipts {
            tokio::time::timeout(Duration::from_secs(5), receipt)
                .await
                .unwrap()
                .unwrap();
        }
    });
    assert_eq!(gate.available_permits(), 1);
    assert!(weak.upgrade().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_account_waiting_for_cleanup_cannot_block_another_accounts_native_work() {
    use super::callback_tests::{make_registry, running};
    let (storage, sqlite) = fixture().await;
    let owner = scope(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    let (entered, started) = std::sync::mpsc::channel();
    let (release, held) = std::sync::mpsc::channel();
    let (dropped, gone) = std::sync::mpsc::channel();
    hub.bind(
        &owner,
        "same",
        Arc::new(BodyGate {
            entered,
            held: Mutex::new(held),
            dropped,
        }),
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    let registry = make_registry(&hub);
    let ctx = running(&storage, &sqlite, &owner, "held-old").await;
    let task = tokio::spawn(registry.get("same").unwrap()(ctx));
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    let receipts = hub.close();
    let successor = hub.sibling();
    let bob = scope(&storage, "bob", "one").await;
    successor
        .bind(
            &bob,
            "same",
            Arc::new(Gone(Arc::new(tokio::sync::Notify::new()))),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let active = make_registry(&successor);
    let ctx = running(&storage, &sqlite, &bob, "other-account").await;
    let progress = active.get("same").unwrap()(ctx).await;
    release.send(()).unwrap();
    task.await.unwrap().unwrap();
    for receipt in receipts.into_iter().chain(successor.close()) {
        tokio::time::timeout(Duration::from_secs(5), receipt)
            .await
            .unwrap()
            .unwrap();
    }
    gone.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        progress.is_ok(),
        "a physically held account blocked unrelated progress: {progress:?}"
    );
}

#[tokio::test]
async fn closing_does_not_wait_for_a_pending_bind_to_get_a_database_connection() {
    use std::{future::Future, task::Poll};
    let (storage, sqlite) = fixture().await;
    let owner = scope(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    assert_eq!(sqlite.pool().options().get_max_connections(), 1);
    let connection = sqlite.pool().acquire().await.unwrap();
    let dropped = Arc::new(tokio::sync::Notify::new());
    let handler = Arc::new(Gone(dropped.clone()));
    let weak = Arc::downgrade(&handler);
    let mut pending = Box::pin(hub.bind(&owner, "waiting-for-db", handler, Duration::from_secs(5)));
    std::future::poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let receipts = hub.close();
    for receipt in receipts {
        tokio::time::timeout(Duration::from_secs(5), receipt)
            .await
            .unwrap()
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), dropped.notified())
        .await
        .unwrap();
    assert!(
        weak.upgrade().is_none(),
        "a blocked DB read retained the accepted foreign reference"
    );
    drop(connection);
    assert!(matches!(
        pending.await,
        Err(orch8_types::error::StepError::ClaimLost)
    ));
    storage
        .catalog_metadata(&owner, "still-active")
        .await
        .unwrap();
}

#[tokio::test]
async fn panicking_panic_payload_cannot_kill_retirement_or_strand_remaining_references() {
    struct Payload;
    impl Drop for Payload {
        fn drop(&mut self) {
            panic!("panic payload destructor");
        }
    }
    struct Host;
    impl StepHandler for Host {
        fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
            Ok("{}".into())
        }
    }
    impl Drop for Host {
        fn drop(&mut self) {
            std::panic::panic_any(Payload);
        }
    }
    let (storage, sqlite) = fixture().await;
    let owner = scope(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage, sqlite);
    let gone = Arc::new(tokio::sync::Notify::new());
    hub.bind(&owner, "panic", Arc::new(Host), Duration::from_secs(5))
        .await
        .unwrap();
    hub.bind(
        &owner,
        "ordinary",
        Arc::new(Gone(gone.clone())),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    for receipt in hub.close() {
        tokio::time::timeout(Duration::from_secs(5), receipt)
            .await
            .unwrap()
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), gone.notified())
        .await
        .unwrap();
    let successor = hub.sibling();
    successor
        .bind(
            &owner,
            "after-panic",
            Arc::new(Gone(Arc::new(tokio::sync::Notify::new()))),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    for receipt in successor.close() {
        tokio::time::timeout(Duration::from_secs(5), receipt)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_account_cleanup_queues_once_and_cannot_detach_reopened_epoch_bindings() {
    use super::callback_tests::{make_registry, running};
    let (storage, sqlite) = fixture().await;
    let owner = scope(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    let (entered, started) = std::sync::mpsc::channel();
    let (release, held) = std::sync::mpsc::channel();
    let (dropped, gone) = std::sync::mpsc::channel();
    hub.bind(
        &owner,
        "same",
        Arc::new(BodyGate {
            entered,
            held: Mutex::new(held),
            dropped,
        }),
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    let registry = make_registry(&hub);
    let old = running(&storage, &sqlite, &owner, "retired-live").await;
    let task = tokio::spawn(registry.get("same").unwrap()(old));
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    storage.retire_account_scope(&owner.account).await.unwrap();
    let first = hub.close_retired(&owner).await.unwrap();
    let second = hub.close_retired(&owner).await.unwrap();
    let reopened = scope(&storage, "alice", "one").await;
    hub.bind(
        &reopened,
        "same",
        Arc::new(Gone(Arc::new(tokio::sync::Notify::new()))),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let next = running(&storage, &sqlite, &reopened, "new-epoch").await;
    let before = registry.get("same").unwrap()(next.clone()).await;
    release.send(()).unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(orch8_types::error::StepError::ClaimLost)
    ));
    for receipt in [first, second] {
        tokio::time::timeout(Duration::from_secs(5), receipt)
            .await
            .unwrap()
            .unwrap();
    }
    gone.recv_timeout(Duration::from_secs(5)).unwrap();
    hub.close_retired(&owner).await.unwrap().await.unwrap();
    assert!(hub.names(&reopened).await.unwrap().contains("same"));
    assert!(registry.get("same").unwrap()(next).await.is_ok());
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
    assert!(
        matches!(before, Err(orch8_types::error::StepError::Retryable { .. })),
        "old physical work was bypassed: {before:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callback_panic_payload_stays_inside_physical_admission_until_its_drop_returns() {
    use super::callback_tests::{make_registry, running};
    struct Payload {
        entered: std::sync::mpsc::Sender<()>,
        held: Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            let _ = self.entered.send(());
            let _ = self
                .held
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10));
        }
    }
    struct Host(Mutex<Option<Payload>>);
    impl StepHandler for Host {
        fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
            let payload = self.0.lock().unwrap().take().unwrap();
            std::panic::panic_any(payload);
        }
    }
    let (storage, sqlite) = fixture().await;
    let owner = scope(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    let (entered, observed) = std::sync::mpsc::channel();
    let (release, held) = std::sync::mpsc::channel();
    hub.bind(
        &owner,
        "same",
        Arc::new(Host(Mutex::new(Some(Payload {
            entered,
            held: Mutex::new(held),
        })))),
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    let registry = make_registry(&hub);
    let ctx = running(&storage, &sqlite, &owner, "panic-payload").await;
    let task = tokio::spawn(registry.get("same").unwrap()(ctx));
    observed.recv_timeout(Duration::from_secs(5)).unwrap();
    let remained_owned = hub.account_gate(&owner).unwrap().available_permits() == 0;
    release.send(()).unwrap();
    let result = task.await.unwrap();
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
    assert!(
        remained_owned,
        "a host panic payload outlived its physical admission"
    );
    assert!(matches!(
        result,
        Err(orch8_types::error::StepError::Permanent { .. })
    ));
}
