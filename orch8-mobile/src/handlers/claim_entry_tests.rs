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
