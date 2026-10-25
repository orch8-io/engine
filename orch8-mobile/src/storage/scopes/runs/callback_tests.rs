use super::{
    tests::{fixture, limits, native_fixture, request, scope},
    *,
};
use crate::error::HandlerError;
use crate::handlers::{StepHandler, scoped::CatalogHandlers};
use orch8_engine::handlers::{
    HandlerRegistry, StepContext,
    step::{StepExecParams, execute_step_dry},
};
use orch8_storage::{InstanceStore, StorageBackend, sqlite::SqliteStorage};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Answer(&'static str, Arc<AtomicUsize>);
impl StepHandler for Answer {
    fn execute(&self, _name: String, _input: String) -> Result<String, HandlerError> {
        self.1.fetch_add(1, Ordering::SeqCst);
        Ok(serde_json::json!({"owner":self.0}).to_string())
    }
}
struct Held {
    entered: Arc<tokio::sync::Notify>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl StepHandler for Held {
    fn execute(&self, _name: String, _input: String) -> Result<String, HandlerError> {
        self.entered.notify_one();
        let _ = self
            .release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10));
        Ok("{}".into())
    }
}
fn held() -> (
    Arc<Held>,
    Arc<tokio::sync::Notify>,
    std::sync::mpsc::Sender<()>,
) {
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release, receiver) = std::sync::mpsc::channel();
    (
        Arc::new(Held {
            entered: entered.clone(),
            release: Mutex::new(receiver),
        }),
        entered,
        release,
    )
}
async fn running(
    storage: &MobileStorage,
    sqlite: &Arc<SqliteStorage>,
    scope: &CatalogScope,
    key: &str,
) -> StepContext {
    let run = storage
        .prepare_catalog_run(scope, request(Some(key), "{}"), limits())
        .await
        .unwrap();
    let native = native_fixture(sqlite, &run).await;
    let claim = sqlite
        .claim_due_instances(chrono::Utc::now(), 32, 0)
        .await
        .unwrap()
        .into_iter()
        .find(|i| i.id == run.id)
        .unwrap();
    StepContext {
        effect_admission: None,
        claim_token: claim.claimed_at,
        instance_id: run.id,
        tenant_id: native.tenant_id,
        block_id: orch8_types::ids::BlockId::new("work"),
        params: serde_json::json!({}),
        context: Arc::new(orch8_types::context::ExecutionContext::default()),
        attempt: 0,
        storage: sqlite.clone(),
        wait_for_input: None,
    }
}
fn exec(ctx: &StepContext) -> StepExecParams {
    StepExecParams {
        claim_token: ctx.claim_token,
        instance_id: ctx.instance_id,
        tenant_id: ctx.tenant_id.clone(),
        block_id: ctx.block_id.clone(),
        handler_name: "same".into(),
        params: ctx.params.clone(),
        context: ctx.context.as_ref().clone(),
        attempt: 0,
        timeout: None,
        externalize_threshold: 0,
        wait_for_input: None,
        cache_key: None,
        output_schema: None,
    }
}
fn make_registry(hub: &Arc<CatalogHandlers>) -> HandlerRegistry {
    let mut registry = HandlerRegistry::new();
    hub.register(&mut registry, "same");
    registry
}
async fn call_until_available(registry: &HandlerRegistry, ctx: &StepContext) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match registry.get("same").unwrap()(ctx.clone()).await {
                Ok(value) => break value,
                Err(orch8_types::error::StepError::Retryable { .. }) => {
                    tokio::task::yield_now().await;
                }
                other => panic!("unexpected callback result: {other:?}"),
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn frozen_bindings_route_four_scopes_through_one_real_step_registry() {
    let (storage, sqlite) = fixture().await;
    let backend: Arc<dyn StorageBackend> = sqlite.clone();
    let hub = CatalogHandlers::isolated(storage.clone(), backend.clone());
    let registry = make_registry(&hub);
    for (account, catalog, label) in [
        ("alice", "one", "alice/one"),
        ("alice", "two", "alice/two"),
        ("bob", "one", "bob/one"),
        ("bob", "two", "bob/two"),
    ] {
        let owner = scope(&storage, account, catalog).await;
        let count = Arc::new(AtomicUsize::new(0));
        hub.bind(
            &owner,
            "same",
            Arc::new(Answer(label, count.clone())),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert!(
            hub.bind(
                &owner,
                "same",
                Arc::new(Answer("replacement", count.clone())),
                Duration::from_secs(5)
            )
            .await
            .is_err()
        );
        let ctx = running(&storage, &sqlite, &owner, "operation").await;
        let result = execute_step_dry(&backend, &registry, exec(&ctx))
            .await
            .unwrap();
        assert_eq!(result.output["owner"], label);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(
            hub.names(&owner).await.unwrap(),
            std::collections::HashSet::from(["same".into()])
        );
    }
}

#[tokio::test]
async fn foreign_binding_and_backend_collision_cannot_dispatch() {
    let (storage, sqlite) = fixture().await;
    let backend: Arc<dyn StorageBackend> = sqlite.clone();
    let hub = CatalogHandlers::isolated(storage.clone(), backend);
    let alice = scope(&storage, "alice", "one").await;
    let bob = scope(&storage, "bob", "one").await;
    let calls = Arc::new(AtomicUsize::new(0));
    hub.bind(
        &alice,
        "same",
        Arc::new(Answer("alice", calls.clone())),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let registry = make_registry(&hub);
    let bob_ctx = running(&storage, &sqlite, &bob, "bob").await;
    assert!(registry.get("same").unwrap()(bob_ctx).await.is_err());
    assert!(hub.names(&bob).await.unwrap().is_empty());
    let mut ctx = running(&storage, &sqlite, &alice, "alice").await;
    let foreign = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let native = sqlite.get_instance(ctx.instance_id).await.unwrap().unwrap();
    foreign.create_instance(&native).await.unwrap();
    assert!(
        foreign
            .verify_instance_claim(ctx.instance_id, &ctx.tenant_id, ctx.claim_token.unwrap())
            .await
            .unwrap(),
        "the other database deliberately has a colliding valid legacy claim"
    );
    ctx.storage = foreign;
    assert!(matches!(
        registry.get("same").unwrap()(ctx).await,
        Err(orch8_types::error::StepError::ClaimLost)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_account_slots_survive_retirement_reopen_and_independent_pools() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("callbacks.sqlite");
    let sqlite = Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    );
    let storage = Arc::new(MobileStorage::new(sqlite.clone()));
    let backend: Arc<dyn StorageBackend> = sqlite.clone();
    let hub = CatalogHandlers::new(storage.clone(), backend);
    let registry = make_registry(&hub);
    let alice = scope(&storage, "alice", "one").await;
    let alice_other = scope(&storage, "alice", "two").await;
    let bob = scope(&storage, "bob", "one").await;
    let (handler, entered, release) = held();
    hub.bind(&alice, "same", handler, Duration::from_secs(60))
        .await
        .unwrap();
    let other_calls = Arc::new(AtomicUsize::new(0));
    let bob_calls = Arc::new(AtomicUsize::new(0));
    hub.bind(
        &alice_other,
        "same",
        Arc::new(Answer("other", other_calls.clone())),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    hub.bind(
        &bob,
        "same",
        Arc::new(Answer("bob", bob_calls.clone())),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let first = tokio::spawn(registry.get("same").unwrap()(
        running(&storage, &sqlite, &alice, "old").await,
    ));
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(
        registry.get("same").unwrap()(running(&storage, &sqlite, &alice_other, "other").await)
            .await
            .is_err()
    );
    assert_eq!(
        registry.get("same").unwrap()(running(&storage, &sqlite, &bob, "bob").await)
            .await
            .unwrap()["owner"],
        "bob"
    );
    storage.retire_account_scope(&alice.account).await.unwrap();
    let fresh = scope(&storage, "alice", "one").await;
    let ctx = running(&storage, &sqlite, &fresh, "fresh").await;
    let independent = Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    );
    let new_storage = Arc::new(MobileStorage::new(independent.clone()));
    let new_backend: Arc<dyn StorageBackend> = independent;
    let successor = CatalogHandlers::new(new_storage, new_backend.clone());
    let fresh_calls = Arc::new(AtomicUsize::new(0));
    successor
        .bind(
            &fresh,
            "same",
            Arc::new(Answer("fresh", fresh_calls.clone())),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let successor_registry = make_registry(&successor);
    let mut ctx = ctx;
    ctx.storage = new_backend;
    assert!(
        successor_registry.get("same").unwrap()(ctx.clone())
            .await
            .is_err(),
        "another hub/pool cannot bypass the live account slot"
    );
    release.send(()).unwrap();
    assert_eq!(
        call_until_available(&successor_registry, &ctx).await["owner"],
        "fresh"
    );
    assert_eq!(fresh_calls.load(Ordering::SeqCst), 1);
    assert_eq!(bob_calls.load(Ordering::SeqCst), 1);
    assert_eq!(other_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn global_physical_limit_counts_detached_calls() {
    let (storage, sqlite) = fixture().await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    let registry = make_registry(&hub);
    let mut tasks = Vec::new();
    let mut releases = Vec::new();
    for account in ["alice", "bob"] {
        let owner = scope(&storage, account, "one").await;
        let (handler, entered, release) = held();
        hub.bind(&owner, "same", handler, Duration::from_secs(60))
            .await
            .unwrap();
        let task = tokio::spawn(registry.get("same").unwrap()(
            running(&storage, &sqlite, &owner, account).await,
        ));
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        tasks.push(task);
        releases.push(release);
    }
    let carol = scope(&storage, "carol", "one").await;
    let calls = Arc::new(AtomicUsize::new(0));
    hub.bind(
        &carol,
        "same",
        Arc::new(Answer("carol", calls.clone())),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let ctx = running(&storage, &sqlite, &carol, "carol").await;
    assert!(registry.get("same").unwrap()(ctx.clone()).await.is_err());
    let first = tasks.remove(0);
    first.abort();
    let _ = first.await;
    assert!(registry.get("same").unwrap()(ctx.clone()).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    releases.remove(0).send(()).unwrap();
    call_until_available(&registry, &ctx).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    releases.remove(0).send(()).unwrap();
    tasks.remove(0).await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn physical_permits_cover_foreign_handler_destruction() {
    struct Drops {
        entered: Arc<tokio::sync::Notify>,
        body: Mutex<std::sync::mpsc::Receiver<()>>,
        dropping: Arc<tokio::sync::Notify>,
        destructor: Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl StepHandler for Drops {
        fn execute(&self, _name: String, _input: String) -> Result<String, HandlerError> {
            self.entered.notify_one();
            let _ = self
                .body
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10));
            Ok("{}".into())
        }
    }
    impl Drop for Drops {
        fn drop(&mut self) {
            self.dropping.notify_one();
            let _ = self
                .destructor
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10));
        }
    }
    let (storage, sqlite) = fixture().await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    let registry = make_registry(&hub);
    let owner = scope(&storage, "alice", "one").await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropping = Arc::new(tokio::sync::Notify::new());
    let (body_tx, body_rx) = std::sync::mpsc::channel();
    let (drop_tx, drop_rx) = std::sync::mpsc::channel();
    hub.bind(
        &owner,
        "same",
        Arc::new(Drops {
            entered: entered.clone(),
            body: Mutex::new(body_rx),
            dropping: dropping.clone(),
            destructor: Mutex::new(drop_rx),
        }),
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    let task = tokio::spawn(registry.get("same").unwrap()(
        running(&storage, &sqlite, &owner, "drop").await,
    ));
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let gate = hub.account_gate(&owner).unwrap();
    task.abort();
    let _ = task.await;
    drop(registry);
    drop(hub);
    body_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), dropping.notified())
        .await
        .unwrap();
    assert_eq!(
        gate.available_permits(),
        0,
        "foreign destruction is still physical native work"
    );
    drop_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while gate.available_permits() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn scoped_callback_queued_before_retirement_cannot_enter_host() {
    use std::{future::Future, task::Poll};
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (storage, sqlite) = runtime.block_on(fixture());
    let owner = runtime.block_on(scope(&storage, "alice", "one"));
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    runtime
        .block_on(hub.bind(
            &owner,
            "same",
            Arc::new(Answer("alice", calls.clone())),
            Duration::from_secs(60),
        ))
        .unwrap();
    let registry = make_registry(&hub);
    let ctx = runtime.block_on(running(&storage, &sqlite, &owner, "queued"));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocker = runtime.spawn_blocking(move || {
        entered_tx.send(()).unwrap();
        let _ = release_rx.recv_timeout(Duration::from_secs(10));
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    runtime.block_on(async {
        let mut pending = Box::pin(registry.get("same").unwrap()(ctx));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                std::future::poll_fn(|cx| {
                    assert!(pending.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                if hub
                    .account_gate(&owner)
                    .is_some_and(|gate| gate.available_permits() == 0)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        storage.retire_account_scope(&owner.account).await.unwrap();
        release_tx.send(()).unwrap();
        blocker.await.unwrap();
        assert!(matches!(
            pending.await,
            Err(orch8_types::error::StepError::ClaimLost)
        ));
        tokio::task::spawn_blocking(|| ()).await.unwrap();
    });
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn decorator_identity_and_failed_preflight_remain_scope_bound() {
    use orch8_storage::testing::faulty::FaultStorage;
    let (storage, sqlite) = fixture().await;
    let (backend, faults) = FaultStorage::wrap(sqlite.clone());
    let hub = CatalogHandlers::isolated(storage.clone(), backend.clone());
    let owner = scope(&storage, "alice", "one").await;
    let calls = Arc::new(AtomicUsize::new(0));
    hub.bind(
        &owner,
        "same",
        Arc::new(Answer("wrapped", calls.clone())),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let registry = make_registry(&hub);
    let mut ctx = running(&storage, &sqlite, &owner, "wrapped").await;
    ctx.storage = backend;
    faults.on("verify_instance_claim").times(1).fail_transient();
    assert!(registry.get("same").unwrap()(ctx.clone()).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        registry.get("same").unwrap()(ctx.clone()).await.unwrap()["owner"],
        "wrapped"
    );
    let (other, _) = FaultStorage::wrap(sqlite);
    ctx.storage = other;
    assert!(matches!(
        registry.get("same").unwrap()(ctx).await,
        Err(orch8_types::error::StepError::ClaimLost)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn registration_limits_remain_charged_after_account_reopen() {
    let (storage, sqlite) = fixture().await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    let owner = scope(&storage, "alice", "one").await;
    let calls = Arc::new(AtomicUsize::new(0));
    for index in 0..32 {
        hub.bind(
            &owner,
            &format!("handler-{index}"),
            Arc::new(Answer("alice", calls.clone())),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    }
    assert!(
        hub.bind(
            &owner,
            "overflow",
            Arc::new(Answer("alice", calls.clone())),
            Duration::from_secs(5)
        )
        .await
        .is_err()
    );
    storage.retire_account_scope(&owner.account).await.unwrap();
    let reopened = scope(&storage, "alice", "one").await;
    assert!(
        hub.bind(
            &reopened,
            "new-epoch",
            Arc::new(Answer("alice", calls.clone())),
            Duration::from_secs(5)
        )
        .await
        .is_err(),
        "retired references remain charged until explicit registry cleanup"
    );
    assert!(hub.names(&owner).await.is_err());
    assert!(hub.names(&reopened).await.unwrap().is_empty());
}

struct IdleDrop {
    entered: Arc<tokio::sync::Notify>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl StepHandler for IdleDrop {
    fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
        panic!("idle handler must never execute")
    }
}
impl Drop for IdleDrop {
    fn drop(&mut self) {
        self.entered.notify_one();
        let _ = self
            .release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_cleanup_retains_capacity_and_registration_charges_until_host_drop() {
    let (storage, sqlite) = fixture().await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    let owner = scope(&storage, "alice", "one").await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release, held) = std::sync::mpsc::channel();
    hub.bind(
        &owner,
        "idle",
        Arc::new(IdleDrop {
            entered: entered.clone(),
            release: Mutex::new(held),
        }),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    for index in 1..32 {
        hub.bind(
            &owner,
            &format!("idle-{index}"),
            Arc::new(Answer("alice", calls.clone())),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    }
    assert!(
        hub.release_retired(&owner).await.is_err(),
        "an active epoch cannot be detached"
    );
    storage.retire_account_scope(&owner.account).await.unwrap();
    let finished = hub.release_retired(&owner).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let gate = hub.account_gate(&owner).unwrap();
    assert_eq!(gate.available_permits(), 0);
    let reopened = scope(&storage, "alice", "one").await;
    assert!(
        hub.bind(
            &reopened,
            "new",
            Arc::new(Answer("new", calls.clone())),
            Duration::from_secs(5)
        )
        .await
        .is_err(),
        "detached host references still consume registration quota"
    );
    assert!(
        hub.release_retired(&owner).await.is_err(),
        "a second cleanup cannot bypass the live destructor"
    );
    // Another account remains usable while Alice's destructor is blocked.
    let bob = scope(&storage, "bob", "one").await;
    hub.bind(
        &bob,
        "same",
        Arc::new(Answer("bob", calls.clone())),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let registry = make_registry(&hub);
    registry.get("same").unwrap()(running(&storage, &sqlite, &bob, "bob-cleanup").await)
        .await
        .unwrap();
    drop(finished); // Losing the observer must not abort owned cleanup.
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while gate.available_permits() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    hub.bind(
        &reopened,
        "new",
        Arc::new(Answer("new", calls)),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(
        hub.names(&reopened).await.unwrap(),
        ["new".to_owned()].into()
    );
    hub.release_retired(&owner).await.unwrap().await.unwrap();
    assert!(
        hub.names(&reopened).await.unwrap().contains("new"),
        "late cleanup must not detach the new epoch"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn busy_retirement_cleanup_can_retry_after_physical_call_settles() {
    let (storage, sqlite) = fixture().await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    let owner = scope(&storage, "alice", "one").await;
    let (handler, entered, release) = held();
    let weak = Arc::downgrade(&handler);
    hub.bind(&owner, "same", handler, Duration::from_secs(60))
        .await
        .unwrap();
    let registry = make_registry(&hub);
    let task = tokio::spawn(registry.get("same").unwrap()(
        running(&storage, &sqlite, &owner, "retire-live").await,
    ));
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    storage.retire_account_scope(&owner.account).await.unwrap();
    assert!(hub.release_retired(&owner).await.is_err());
    task.abort();
    let _ = task.await;
    assert!(hub.release_retired(&owner).await.is_err());
    let gate = hub.account_gate(&owner).unwrap();
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while gate.available_permits() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    hub.release_retired(&owner).await.unwrap().await.unwrap();
    assert!(
        weak.upgrade().is_none(),
        "retry must release the original host object"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_drop_panic_does_not_strand_other_retired_bindings_or_capacity() {
    struct PanicDrop;
    impl StepHandler for PanicDrop {
        fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
            Ok("{}".into())
        }
    }
    impl Drop for PanicDrop {
        fn drop(&mut self) {
            panic!("host destructor panic");
        }
    }
    let (storage, sqlite) = fixture().await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite);
    let owner = scope(&storage, "alice", "one").await;
    let ordinary = Arc::new(Answer("ordinary", Arc::new(AtomicUsize::new(0))));
    let weak = Arc::downgrade(&ordinary);
    hub.bind(&owner, "panic", Arc::new(PanicDrop), Duration::from_secs(5))
        .await
        .unwrap();
    hub.bind(&owner, "ordinary", ordinary, Duration::from_secs(5))
        .await
        .unwrap();
    storage.retire_account_scope(&owner.account).await.unwrap();
    hub.release_retired(&owner).await.unwrap().await.unwrap();
    assert!(weak.upgrade().is_none());
    let reopened = scope(&storage, "alice", "one").await;
    hub.bind(
        &reopened,
        "same",
        Arc::new(Answer("new", Arc::new(AtomicUsize::new(0)))),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
}

#[test]
fn queued_retirement_keeps_cleanup_owned_after_runtime_shutdown() {
    struct DropSignal(std::sync::mpsc::Sender<()>);
    impl StepHandler for DropSignal {
        fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
            panic!("retired handler cannot execute")
        }
    }
    impl Drop for DropSignal {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (storage, sqlite) = runtime.block_on(fixture());
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite);
    let owner = runtime.block_on(scope(&storage, "alice", "one"));
    let (dropped, observed) = std::sync::mpsc::channel();
    runtime
        .block_on(hub.bind(
            &owner,
            "idle",
            Arc::new(DropSignal(dropped)),
            Duration::from_secs(5),
        ))
        .unwrap();
    runtime
        .block_on(storage.retire_account_scope(&owner.account))
        .unwrap();
    let (started, entered) = std::sync::mpsc::channel();
    let (release, held) = std::sync::mpsc::channel();
    runtime.spawn_blocking(move || {
        started.send(()).unwrap();
        let _ = held.recv_timeout(Duration::from_secs(10));
    });
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let receiver = runtime.block_on(hub.release_retired(&owner)).unwrap();
    let gate = hub.account_gate(&owner).unwrap();
    assert_eq!(gate.available_permits(), 0);
    drop(receiver);
    runtime.shutdown_background();
    release.send(()).unwrap();
    observed.recv_timeout(Duration::from_secs(5)).unwrap();
    // The cleanup frame needs no async runtime/registry lock for its drop path.
}
