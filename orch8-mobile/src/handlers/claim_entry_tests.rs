use super::*;
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
use orch8_storage::{InstanceStore, SequenceStore, sqlite::SqliteStorage};
use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
    task::Poll,
};

struct Counter(Arc<AtomicUsize>);
impl StepHandler for Counter {
    fn execute(&self, _name: String, _input: String) -> Result<String, HandlerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok("{}".into())
    }
}

async fn claimed_context() -> (Arc<SqliteStorage>, StepContext) {
    let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let sequence = SequenceBuilder::new("mobile")
        .step("step", "queued")
        .build();
    sqlite.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
    sqlite.create_instance(&instance).await.unwrap();
    let claimed = sqlite
        .claim_due_instances(chrono::Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let now = claimed.claimed_at.unwrap();
    let ctx = StepContext {
        effect_admission: None,
        claim_token: Some(now),
        instance_id: instance.id,
        tenant_id: instance.tenant_id,
        block_id: orch8_types::ids::BlockId::new("step"),
        params: serde_json::json!({}),
        context: Arc::new(orch8_types::context::ExecutionContext::default()),
        attempt: 1,
        storage: sqlite.clone(),
        wait_for_input: None,
    };
    (sqlite, ctx)
}

async fn change_claim(sqlite: &SqliteStorage, id: &str, case: &str) {
    match case {
        "valid" | "tenant" => {}
        "missing" => {
            sqlx::query("DELETE FROM task_instances WHERE id=?1")
                .bind(id)
                .execute(sqlite.pool())
                .await
                .unwrap();
        }
        "reclaimed" => {
            sqlx::query("UPDATE task_instances SET claimed_at=?2 WHERE id=?1")
                .bind(id)
                .bind((chrono::Utc::now() + chrono::Duration::seconds(1)).to_rfc3339())
                .execute(sqlite.pool())
                .await
                .unwrap();
        }
        state => {
            sqlx::query("UPDATE task_instances SET state=?2 WHERE id=?1")
                .bind(id)
                .bind(state)
                .execute(sqlite.pool())
                .await
                .unwrap();
        }
    }
}

#[test]
fn queued_native_entry_rechecks_running_claim_and_tenant() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    for case in [
        "valid",
        "scheduled",
        "waiting",
        "paused",
        "completed",
        "cancelled",
        "reclaimed",
        "missing",
        "tenant",
    ] {
        let (sqlite, mut ctx) = runtime.block_on(claimed_context());
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = runtime.spawn_blocking(move || {
            entered_tx.send(()).unwrap();
            let _ = release_rx.recv_timeout(Duration::from_secs(10));
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = HandlerRegistry::new();
        register_foreign_handler(
            &mut registry,
            "queued",
            Arc::new(Counter(calls.clone())),
            Duration::from_secs(5),
            Arc::new(Stragglers::default()),
        );
        let id = ctx.instance_id.to_string();
        if case == "tenant" {
            ctx.tenant_id = orch8_types::ids::TenantId::unchecked("foreign");
        }
        runtime.block_on(async {
            let mut result = Box::pin(registry.get("queued").unwrap()(ctx));
            std::future::poll_fn(|cx| {
                assert!(result.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            change_claim(&sqlite, &id, case).await;
            release_tx.send(()).unwrap();
            blocker.await.unwrap();
            let result = result.await;
            if case == "valid" {
                assert!(result.is_ok());
            } else {
                assert!(
                    matches!(result, Err(StepError::ClaimLost)),
                    "{case}: {result:?}"
                );
            }
            tokio::task::spawn_blocking(|| ()).await.unwrap();
        });
        assert_eq!(
            calls.load(Ordering::SeqCst),
            usize::from(case == "valid"),
            "{case}"
        );
    }
}

#[tokio::test]
async fn cancellation_and_timeout_during_verification_never_enter_host() {
    use orch8_storage::testing::faulty::FaultStorage;
    for timeout in [false, true] {
        let (_sqlite, mut ctx) = claimed_context().await;
        let (storage, faults) = FaultStorage::wrap(ctx.storage.clone());
        ctx.storage = storage;
        let retry = ctx.clone();
        let pause = faults.on("verify_instance_claim").times(1).pause();
        let calls = Arc::new(AtomicUsize::new(0));
        let stragglers = Arc::new(Stragglers::default());
        let mut registry = HandlerRegistry::new();
        register_foreign_handler(
            &mut registry,
            "queued",
            Arc::new(Counter(calls.clone())),
            Duration::from_secs(3600),
            stragglers.clone(),
        );
        let mut result = Box::pin(registry.get("queued").unwrap()(ctx));
        tokio::select! {
            ()=pause.entered()=>{},
            outcome=&mut result=>panic!("callback completed before its verification barrier: {outcome:?}"),
        }
        if timeout {
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(3601)).await;
            let error = (&mut result).await.unwrap_err();
            assert!(crate::stragglers::is_device_timeout(&error));
            tokio::time::resume();
        }
        drop(result);
        assert!(
            stragglers.blocks("queued"),
            "physical verification has not settled"
        );
        pause.release();
        tokio::time::timeout(Duration::from_secs(5), async {
            while stragglers.blocks("queued") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        registry.get("queued").unwrap()(retry).await.unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a clean retry remains usable"
        );
    }
}

#[tokio::test]
async fn storage_verification_failures_never_enter_host_and_keep_error_class() {
    use orch8_storage::testing::faulty::FaultStorage;
    for transient in [true, false] {
        let (_sqlite, mut ctx) = claimed_context().await;
        let (storage, faults) = FaultStorage::wrap(ctx.storage.clone());
        ctx.storage = storage;
        if transient {
            faults.on("verify_instance_claim").times(1).fail_transient();
        } else {
            faults.on("verify_instance_claim").times(1).fail_permanent();
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = HandlerRegistry::new();
        register_foreign_handler(
            &mut registry,
            "queued",
            Arc::new(Counter(calls.clone())),
            Duration::from_secs(5),
            Arc::new(Stragglers::default()),
        );
        let error = registry.get("queued").unwrap()(ctx).await.unwrap_err();
        assert!(if transient {
            matches!(error, StepError::Retryable { .. })
        } else {
            matches!(error, StepError::Permanent { .. })
        });
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detached_native_callback_retains_claim_backend_until_physical_completion() {
    struct Held {
        entered: Arc<tokio::sync::Notify>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
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
    let (sqlite, ctx) = claimed_context().await;
    let backend = Arc::downgrade(&sqlite);
    drop(sqlite);
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let stragglers = Arc::new(Stragglers::default());
    let mut registry = HandlerRegistry::new();
    register_foreign_handler(
        &mut registry,
        "held",
        Arc::new(Held {
            entered: entered.clone(),
            release: std::sync::Mutex::new(release_rx),
        }),
        Duration::from_secs(60),
        stragglers.clone(),
    );
    let task = tokio::spawn(registry.get("held").unwrap()(ctx));
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(stragglers.blocks("held"));
    assert!(
        backend.upgrade().is_some(),
        "physical work must retain its immutable claim/backend even after its caller disappears"
    );
    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while stragglers.blocks("held") || backend.strong_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

fn execution(ctx: &StepContext) -> orch8_engine::handlers::step::StepExecParams {
    orch8_engine::handlers::step::StepExecParams {
        claim_token: ctx.claim_token,
        instance_id: ctx.instance_id,
        tenant_id: ctx.tenant_id.clone(),
        block_id: ctx.block_id.clone(),
        handler_name: "queued".into(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_native_preflight_retries_through_the_real_effect_ledger() {
    use orch8_engine::handlers::step::execute_step_dry;
    use orch8_storage::testing::faulty::FaultStorage;
    let (_sqlite, mut ctx) = claimed_context().await;
    let (storage, faults) = FaultStorage::wrap(ctx.storage.clone());
    ctx.storage = storage.clone();
    let native_entry = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed_entry = native_entry.clone();
    let pause = faults
        .on("verify_instance_claim")
        .when(move |_| observed_entry.load(Ordering::SeqCst))
        .pause();
    let calls = Arc::new(AtomicUsize::new(0));
    let stragglers = Arc::new(Stragglers::default());
    let mut registry = HandlerRegistry::new();
    register_foreign_handler(
        &mut registry,
        "queued",
        Arc::new(Counter(calls.clone())),
        Duration::from_secs(60),
        stragglers.clone(),
    );
    // Observe adapter entry instead of counting generic/core preflight reads.
    let native = Arc::new(registry);
    let mut execution_registry = HandlerRegistry::new();
    execution_registry.register_with_deferred_effect_admission("queued", move |ctx| {
        native_entry.store(true, Ordering::SeqCst);
        native.get("queued").unwrap()(ctx)
    });
    let registry = Arc::new(execution_registry);
    let first = execution(&ctx);
    let first_store = storage.clone();
    let first_registry = registry.clone();
    let task =
        tokio::spawn(async move { execute_step_dry(&first_store, &first_registry, first).await });
    tokio::time::timeout(Duration::from_secs(5), pause.entered())
        .await
        .unwrap();
    let ledger = storage
        .get_continuity_execution_by_instance(&ctx.tenant_id, ctx.instance_id)
        .await
        .unwrap()
        .expect("native preflight must hold a prepared effect");
    assert_eq!(
        storage
            .list_effect_receipts(&ctx.tenant_id, ledger.continuity_id, 10)
            .await
            .unwrap()[0]
            .state,
        orch8_types::continuity::EffectState::Prepared
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    pause.release();
    tokio::time::timeout(Duration::from_secs(5), async {
        while stragglers.blocks("queued") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let retry = execute_step_dry(&storage, &registry, execution(&ctx)).await;
    assert!(
        retry.is_ok(),
        "a callback proved unstarted must not be blocked by the effect ledger: {retry:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let ledger = storage
        .get_continuity_execution_by_instance(&ctx.tenant_id, ctx.instance_id)
        .await
        .unwrap()
        .unwrap();
    let receipts = storage
        .list_effect_receipts(&ctx.tenant_id, ledger.continuity_id, 10)
        .await
        .unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(
        receipts[0].state,
        orch8_types::continuity::EffectState::Committed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admitted_native_failure_preserves_unknown_evidence_and_blocks_same_attempt() {
    use orch8_engine::{error::EngineError, handlers::step::execute_step_dry};
    struct Failed(Arc<AtomicUsize>);
    impl StepHandler for Failed {
        fn execute(&self, _name: String, _input: String) -> Result<String, HandlerError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(HandlerError::Retryable {
                message: "provider outcome is ambiguous".into(),
            })
        }
    }
    let (_sqlite, ctx) = claimed_context().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = HandlerRegistry::new();
    register_foreign_handler(
        &mut registry,
        "queued",
        Arc::new(Failed(calls.clone())),
        Duration::from_secs(5),
        Arc::new(Stragglers::default()),
    );
    assert!(matches!(
        execute_step_dry(&ctx.storage, &registry, execution(&ctx)).await,
        Err(EngineError::StepFailed { .. })
    ));
    assert!(matches!(
        execute_step_dry(&ctx.storage, &registry, execution(&ctx)).await,
        Err(EngineError::EffectBlocked {
            state: orch8_types::continuity::EffectState::Unknown,
            ..
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn native_policy_builtin_name_override_still_has_effect_evidence() {
    use orch8_engine::handlers::step::execute_step_dry;
    let (_sqlite, ctx) = claimed_context().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = HandlerRegistry::new();
    register_foreign_handler(
        &mut registry,
        "noop",
        Arc::new(Counter(calls.clone())),
        Duration::from_secs(5),
        Arc::new(Stragglers::default()),
    );
    let mut exec = execution(&ctx);
    exec.handler_name = "noop".into();
    execute_step_dry(&ctx.storage, &registry, exec)
        .await
        .unwrap();
    let ledger = ctx
        .storage
        .get_continuity_execution_by_instance(&ctx.tenant_id, ctx.instance_id)
        .await
        .unwrap()
        .expect("native callbacks need effect evidence even under a builtin name");
    assert_eq!(
        ctx.storage
            .list_effect_receipts(&ctx.tenant_id, ledger.continuity_id, 10)
            .await
            .unwrap()[0]
            .state,
        orch8_types::continuity::EffectState::Committed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn native_policy_dry_run_never_enters_host() {
    let (_sqlite, mut ctx) = claimed_context().await;
    Arc::make_mut(&mut ctx.context).runtime.dry_run = true;
    ctx.params = serde_json::json!({"credential": "DRY_RUN_SECRET"});
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = HandlerRegistry::new();
    register_foreign_handler(
        &mut registry,
        "queued",
        Arc::new(Counter(calls.clone())),
        Duration::from_secs(5),
        Arc::new(Stragglers::default()),
    );
    let result = registry.get("queued").unwrap()(ctx).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(result["dry_run"], true);
    assert!(!result.to_string().contains("DRY_RUN_SECRET"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_or_local_shutdown_during_admission_keeps_uncertain_dispatch_evidence() {
    use orch8_engine::{error::EngineError, handlers::step::execute_step_dry};
    use orch8_storage::testing::faulty::FaultStorage;
    for local_stop in [false, true] {
        let (_sqlite, mut ctx) = claimed_context().await;
        let (storage, faults) = FaultStorage::wrap(ctx.storage.clone());
        ctx.storage = storage.clone();
        // First CAS prepares; second begins the durable dispatch window.
        let pause = faults.on("cas_effect_receipt").nth(2).pause();
        let calls = Arc::new(AtomicUsize::new(0));
        let stragglers = Arc::new(Stragglers::default());
        let stop = tokio_util::sync::CancellationToken::new();
        let mut registry = HandlerRegistry::new();
        register_owned_foreign_handler(
            &mut registry,
            "queued",
            Arc::new(Counter(calls.clone())),
            Duration::from_secs(60),
            stragglers.clone(),
            stop.clone(),
        );
        let registry = Arc::new(registry);
        let exec = execution(&ctx);
        let s = storage.clone();
        let h = registry.clone();
        let first = tokio::spawn(async move { execute_step_dry(&s, &h, exec).await });
        tokio::time::timeout(Duration::from_secs(5), pause.entered())
            .await
            .unwrap();
        if local_stop {
            stop.cancel();
            pause.release();
            assert!(matches!(
                first.await.unwrap(),
                Err(EngineError::ClaimLost { .. })
            ));
        } else {
            first.abort();
            assert!(first.await.unwrap_err().is_cancelled());
            pause.release();
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while stragglers.blocks("queued") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            execute_step_dry(&storage, &registry, execution(&ctx)).await,
            Err(EngineError::EffectBlocked {
                state: orch8_types::continuity::EffectState::Unknown,
                ..
            })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_admission_storage_failures_preserve_type_and_commit_ambiguity() {
    use orch8_engine::{error::EngineError, handlers::step::execute_step_dry};
    use orch8_storage::testing::faulty::FaultStorage;
    for ambiguous in [false, true] {
        let (_sqlite, mut ctx) = claimed_context().await;
        let (storage, faults) = FaultStorage::wrap(ctx.storage.clone());
        ctx.storage = storage.clone();
        if ambiguous {
            faults.on("cas_effect_receipt").nth(2).fail_after_commit(
                orch8_types::error::StorageError::Connection("ambiguous native admission".into()),
            );
        } else {
            faults.on("cas_effect_receipt").nth(2).fail_transient();
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = HandlerRegistry::new();
        register_foreign_handler(
            &mut registry,
            "queued",
            Arc::new(Counter(calls.clone())),
            Duration::from_secs(5),
            Arc::new(Stragglers::default()),
        );
        let error = execute_step_dry(&storage, &registry, execution(&ctx))
            .await
            .unwrap_err();
        assert!(
            error.is_transient_storage(),
            "admission must preserve the core storage error: {error:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let retry = execute_step_dry(&storage, &registry, execution(&ctx)).await;
        if ambiguous {
            assert!(matches!(
                retry,
                Err(EngineError::EffectBlocked {
                    state: orch8_types::continuity::EffectState::Unknown,
                    ..
                })
            ));
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        } else {
            retry.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_shutdown_during_claim_preflight_refuses_host_entry() {
    use orch8_storage::testing::faulty::FaultStorage;
    let (sqlite, mut ctx) = claimed_context().await;
    let (storage, faults) = FaultStorage::wrap(ctx.storage.clone());
    ctx.storage = storage;
    let pause = faults.on("verify_instance_claim").pause();
    let stop = tokio_util::sync::CancellationToken::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = HandlerRegistry::new();
    register_owned_foreign_handler(
        &mut registry,
        "queued",
        Arc::new(Counter(calls.clone())),
        Duration::from_secs(60),
        Arc::new(Stragglers::default()),
        stop.clone(),
    );
    let callback = registry.get("queued").unwrap()(ctx.clone());
    let task = tokio::spawn(callback);
    tokio::time::timeout(Duration::from_secs(5), pause.entered())
        .await
        .unwrap();
    stop.cancel();
    pause.release();
    let result = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(StepError::ClaimLost)), "{result:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        sqlite
            .verify_instance_claim(ctx.instance_id, &ctx.tenant_id, ctx.claim_token.unwrap())
            .await
            .unwrap()
    );
}
