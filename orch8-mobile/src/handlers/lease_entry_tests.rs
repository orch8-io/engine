use super::*;
use orch8_engine::remote_worker::{
    LeaseResponse,
    lease::{ConfirmedLease, LeaseEnd},
};
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::{
    context::ExecutionContext,
    ids::{BlockId, InstanceId},
};
use std::{
    future::{Future, pending, ready},
    sync::atomic::{AtomicUsize, Ordering},
    task::Poll,
};
struct Counter(Arc<AtomicUsize>);
impl StepHandler for Counter {
    fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok("{}".into())
    }
}
async fn context() -> StepContext {
    StepContext {
        effect_admission: None,
        claim_token: None,
        instance_id: InstanceId::new(),
        tenant_id: crate::mobile_tenant_id(),
        block_id: BlockId::new("step"),
        params: serde_json::json!({}),
        context: Arc::new(ExecutionContext::default()),
        attempt: 1,
        storage: Arc::new(SqliteStorage::in_memory().await.unwrap()),
        wait_for_input: None,
    }
}
fn registry(calls: Arc<AtomicUsize>, slots: Arc<Semaphore>) -> HandlerRegistry {
    let mut registry = HandlerRegistry::new();
    register_foreign_handler_with_capacity(
        &mut registry,
        "native",
        Arc::new(Counter(calls)),
        Duration::from_secs(5),
        Arc::new(Stragglers::default()),
        slots,
    );
    registry
}
async fn lease() -> ConfirmedLease {
    ConfirmedLease::acquire(Some(1), Duration::from_millis(100), || {
        ready(LeaseResponse::Accepted)
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn unstarted_supervision_cannot_reserve_a_native_callback() {
    let calls = Arc::new(AtomicUsize::new(0));
    let slots = Arc::new(Semaphore::new(1));
    let registry = registry(calls.clone(), slots.clone());
    let lease = lease().await;
    assert!(matches!(
        registry
            .prepare_native_handler_with_lease("native", context().await, lease.authority())
            .unwrap(),
        Err(StepError::ClaimLost)
    ));
    assert_eq!(slots.available_permits(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn a_prepared_native_callback_cannot_outlive_its_supervisor() {
    let calls = Arc::new(AtomicUsize::new(0));
    let slots = Arc::new(Semaphore::new(1));
    let registry = registry(calls.clone(), slots.clone());
    let lease = lease().await;
    let authority = lease.authority();
    let ctx = context().await;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    lease
        .run(
            Duration::from_millis(100),
            async {
                let prepared = registry
                    .prepare_native_handler_with_lease("native", ctx, authority.clone())
                    .unwrap()
                    .unwrap();
                assert!(sender.send(prepared).is_ok());
            },
            || ready(LeaseResponse::Accepted),
        )
        .await
        .unwrap();
    let prepared = receiver.await.unwrap();
    assert_eq!(authority.check(), Err(LeaseEnd::Unconfirmed));
    assert!(matches!(prepared.await, Err(StepError::ClaimLost)));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(slots.available_permits(), 1);
}
#[test]
fn queued_physical_entry_checks_expiry_even_when_the_supervisor_is_not_polled() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    for expired in [false, true] {
        let ctx = runtime.block_on(context());
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = runtime.spawn_blocking(move || {
            entered_tx.send(()).unwrap();
            let _ = release_rx.recv_timeout(Duration::from_secs(10));
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let slots = Arc::new(Semaphore::new(1));
        let registry = registry(calls.clone(), slots.clone());
        runtime.block_on(async {
            let lease = lease().await;
            let authority = lease.authority();
            let entry_authority = authority.clone();
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let operation = async move {
                let prepared = registry
                    .prepare_native_handler_with_lease("native", ctx, entry_authority)
                    .unwrap()
                    .unwrap();
                assert!(sender.send(prepared).is_ok());
                pending::<()>().await;
            };
            let mut supervised = Box::pin(lease.run(Duration::from_millis(100), operation, || {
                pending::<LeaseResponse>()
            }));
            std::future::poll_fn(|cx| {
                assert!(supervised.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            let mut prepared = receiver.await.unwrap();
            std::future::poll_fn(|cx| {
                assert!(prepared.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            // Keep the supervisor future alive but deliberately stop polling it.
            // The physical callback must check time itself after its pool wait.
            if expired {
                tokio::time::sleep(Duration::from_millis(1100)).await;
            }
            release_tx.send(()).unwrap();
            blocker.await.unwrap();
            let result = prepared.await;
            if expired {
                assert!(matches!(result, Err(StepError::ClaimLost)));
                assert_eq!(authority.check(), Err(LeaseEnd::Expired));
            } else {
                assert!(result.is_ok());
                assert_eq!(authority.check(), Ok(()));
            }
            drop(supervised);
        });
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(!expired));
        assert_eq!(slots.available_permits(), 1);
    }
    runtime.shutdown_timeout(Duration::from_secs(1));
}

#[tokio::test]
async fn a_native_call_started_in_time_cannot_publish_success_after_lease_expiry() {
    struct Slow(Arc<std::sync::atomic::AtomicBool>);
    impl StepHandler for Slow {
        fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
            self.0.store(true, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(1100));
            Ok("{}".into())
        }
    }
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut registry = HandlerRegistry::new();
    let slots = Arc::new(Semaphore::new(1));
    register_foreign_handler_with_capacity(
        &mut registry,
        "native",
        Arc::new(Slow(entered.clone())),
        Duration::from_secs(5),
        Arc::new(Stragglers::default()),
        slots.clone(),
    );
    let ctx = context().await;
    let lease = lease().await;
    let authority = lease.authority();
    let entry_authority = authority.clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let operation = async move {
        let prepared = registry
            .prepare_native_handler_with_lease("native", ctx, entry_authority)
            .unwrap()
            .unwrap();
        assert!(sender.send(prepared).is_ok());
        pending::<()>().await;
    };
    let mut run = Box::pin(lease.run(Duration::from_millis(100), operation, || {
        pending::<LeaseResponse>()
    }));
    std::future::poll_fn(|cx| {
        assert!(run.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let prepared = receiver.await.unwrap();
    assert!(matches!(prepared.await, Err(StepError::ClaimLost)));
    assert!(entered.load(Ordering::SeqCst));
    assert_eq!(authority.check(), Err(LeaseEnd::Expired));
    assert_eq!(slots.available_permits(), 1);
    drop(run);
}
