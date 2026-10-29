use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;

#[tokio::test]
async fn native_preparation_reserves_without_host_entry_and_drop_returns_capacity() {
    let slots = Arc::new(Semaphore::new(1));
    let calls = Arc::new(AtomicUsize::new(0));
    let capacity = slots.clone();
    let invoked = calls.clone();
    let mut registry = HandlerRegistry::new();
    registry.register_prepared_with_deferred_effect_admission("native", move |_ctx| {
        let permit = capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| StepError::ClaimLost)?;
        let invoked = invoked.clone();
        Ok(async move {
            let _permit = permit;
            invoked.fetch_add(1, Ordering::SeqCst);
            Ok(serde_json::json!("native"))
        })
    });
    let ctx = mk_ctx(mk_test_storage().await);
    let prepared = registry
        .prepare_native_handler("native", ctx.clone())
        .unwrap()
        .unwrap();
    assert_eq!(slots.available_permits(), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(prepared);
    assert_eq!(slots.available_permits(), 1);
    let ordinary_entry = registry.get("native").unwrap()(ctx.clone());
    assert_eq!(slots.available_permits(), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(ordinary_entry);
    assert_eq!(slots.available_permits(), 1);
    registry.get("native").unwrap()(ctx).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(slots.available_permits(), 1);
}

#[tokio::test]
async fn mocks_replacement_and_retention_cannot_use_stale_native_preparation() {
    let preparations = Arc::new(AtomicUsize::new(0));
    let count = preparations.clone();
    let mut registry = HandlerRegistry::new();
    registry.register_prepared_with_deferred_effect_admission("native", move |_ctx| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(async { Ok(serde_json::json!("native")) })
    });
    let ctx = mk_ctx(mk_test_storage().await);
    registry.set_mock("native", serde_json::json!("mock"));
    assert!(
        registry
            .prepare_native_handler("native", ctx.clone())
            .is_none()
    );
    assert_eq!(
        registry.get("native").unwrap()(ctx.clone()).await.unwrap(),
        "mock"
    );
    assert_eq!(preparations.load(Ordering::SeqCst), 0);
    registry.clear_mock("native");
    let prepared = registry
        .prepare_native_handler("native", ctx.clone())
        .unwrap()
        .unwrap();
    assert_eq!(prepared.await.unwrap(), "native");
    assert_eq!(preparations.load(Ordering::SeqCst), 1);
    registry.retain_handlers(|_| false);
    assert!(
        registry
            .prepare_native_handler("native", ctx.clone())
            .is_none()
    );
    assert!(!registry.contains("native"));
    registry.register_prepared_with_deferred_effect_admission("native", |_| {
        Ok(async { Ok(serde_json::Value::Null) })
    });
    let constructions = Arc::new(AtomicUsize::new(0));
    let called = constructions.clone();
    registry.register("native", move |_| {
        called.fetch_add(1, Ordering::SeqCst);
        async { Ok(serde_json::Value::Null) }
    });
    assert!(
        registry
            .prepare_native_handler("native", ctx.clone())
            .is_none()
    );
    assert_eq!(
        constructions.load(Ordering::SeqCst),
        0,
        "ordinary handler constructors must not run at native preflight"
    );
    assert!(!registry.defers_effect_admission("native"));
    registry.get("native").unwrap()(ctx).await.unwrap();
    assert_eq!(constructions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn lease_aware_preparation_requires_live_supervision_and_keeps_local_api_compatible() {
    use crate::remote_worker::{LeaseResponse, lease::ConfirmedLease};
    use std::{future::ready, sync::Mutex, time::Duration};
    let seen = Arc::new(Mutex::new(Vec::new()));
    let observations = seen.clone();
    let mut registry = HandlerRegistry::new();
    registry.register_lease_aware_prepared_with_deferred_effect_admission(
        "native",
        move |_ctx, lease| {
            observations.lock().unwrap().push(lease.is_some());
            Ok(async move {
                if let Some(lease) = lease {
                    lease.check().map_err(|_| StepError::ClaimLost)?;
                }
                Ok(serde_json::Value::Null)
            })
        },
    );
    let ctx = mk_ctx(mk_test_storage().await);
    let lease = ConfirmedLease::acquire(Some(1), Duration::from_millis(100), || {
        ready(LeaseResponse::Accepted)
    })
    .await
    .unwrap();
    let authority = lease.authority();
    assert!(matches!(
        registry
            .prepare_native_handler_with_lease("native", ctx.clone(), authority.clone())
            .unwrap(),
        Err(StepError::ClaimLost)
    ));
    assert!(seen.lock().unwrap().is_empty());
    assert!(
        lease
            .run(
                Duration::from_millis(100),
                async {
                    registry
                        .prepare_native_handler_with_lease("native", ctx.clone(), authority)
                        .unwrap()
                        .unwrap()
                        .await
                },
                || ready(LeaseResponse::Accepted)
            )
            .await
            .unwrap()
            .is_ok()
    );
    assert!(
        registry
            .prepare_native_handler("native", ctx)
            .unwrap()
            .unwrap()
            .await
            .is_ok()
    );
    assert_eq!(*seen.lock().unwrap(), vec![true, false]);
}
