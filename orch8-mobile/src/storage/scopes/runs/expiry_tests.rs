//! Expiry point checks cannot invalidate another claim or terminal result.
use super::{
    callback_tests::running,
    tests::{fixture, scope},
};
use crate::handlers::scoped::lifetime::Lifetime;
use orch8_storage::InstanceStore;
use orch8_types::instance::InstanceState;

#[tokio::test]
async fn expiry_uses_original_claim_and_preserves_successors_and_terminal_states() {
    for mode in ["stale", "foreign", "terminal", "expired", "huge", "fresh"] {
        let (storage, sqlite) = fixture().await;
        let owner = scope(&storage, "alice", "one").await;
        let ctx = running(&storage, &sqlite, &owner, mode).await;
        let token = ctx.claim_token.unwrap();
        let old = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        if mode != "fresh" {
            for sql in [
                "UPDATE mobile_catalog_run_requests SET created_at=?1 WHERE id=?2",
                "UPDATE task_instances SET created_at=?1 WHERE id=?2",
            ] {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(&old)
                    .bind(ctx.instance_id.to_string())
                    .execute(storage.pool())
                    .await
                    .unwrap();
            }
        }
        if mode == "terminal" {
            sqlite
                .conditional_update_instance_state(
                    ctx.instance_id,
                    InstanceState::Running,
                    InstanceState::Completed,
                    None,
                )
                .await
                .unwrap();
        }
        let limit = if mode == "huge" { u64::MAX } else { 60 };
        let lifetime = Lifetime::new(storage, limit);
        let supplied_token = if mode == "stale" {
            token - chrono::Duration::seconds(1)
        } else {
            token
        };
        let tenant = if mode == "foreign" {
            orch8_types::ids::TenantId::new("foreign").unwrap()
        } else {
            ctx.tenant_id
        };
        let allowed = lifetime
            .verify(ctx.instance_id, &tenant, supplied_token)
            .await
            .unwrap();
        assert_eq!(allowed, matches!(mode, "huge" | "fresh"), "{mode}");
        let stored = sqlite.get_instance(ctx.instance_id).await.unwrap().unwrap();
        let expected = match mode {
            "terminal" => InstanceState::Completed,
            "expired" => InstanceState::Failed,
            _ => InstanceState::Running,
        };
        assert_eq!(stored.state, expected, "{mode}");
        if mode == "expired" {
            assert!(stored.claimed_at.is_none());
        }
    }
}

#[tokio::test]
async fn builtin_expiry_does_not_require_periodic_cleanup() {
    use super::{
        materialize_tests::{builtins, instance_limits, owner_with_handler, tick},
        tests::{limits, request},
    };
    use crate::handlers::scoped::CatalogHandlers;
    use orch8_engine::handlers::HandlerRegistry;
    use orch8_storage::StorageBackend;
    use orch8_types::clock::{ManualClock, SharedClock};
    use std::sync::Arc;
    let (storage, sqlite) = fixture().await;
    let owner = owner_with_handler(
        &storage,
        "alice",
        "one",
        serde_json::json!({"key":"expiry_canary","value":true}),
        "set_state",
    )
    .await;
    let backend: Arc<dyn StorageBackend> = sqlite;
    let clock = Arc::new(ManualClock::new(chrono::Utc::now()));
    let hub = CatalogHandlers::isolated_with_lifetime(
        storage.clone(),
        backend.clone(),
        60,
        SharedClock::from_arc(clock.clone()),
    );
    let run = storage
        .prepare_catalog_run(&owner, request(Some("builtin-expiry"), "{}"), limits())
        .await
        .unwrap();
    hub.materialize(owner.clone(), run.id, instance_limits(), builtins())
        .await
        .unwrap();
    clock.advance(chrono::Duration::hours(1));
    let mut registry = HandlerRegistry::new();
    hub.install_resolver(&mut registry, crate::DEFAULT_BUILTINS);
    tick(&backend, &Arc::new(registry)).await;
    assert_eq!(
        backend.get_instance(run.id).await.unwrap().unwrap().state,
        InstanceState::Failed
    );
    assert!(
        backend
            .get_instance_kv(run.id, "expiry_canary")
            .await
            .unwrap()
            .is_none()
    );
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
async fn native_result_after_expiry_keeps_dispatched_receipt_without_committing_output() {
    use super::{
        materialize_tests::{builtins, instance_limits, owner_with_handler, tick},
        tests::{limits, request},
    };
    use crate::{HandlerError, StepHandler, handlers::scoped::CatalogHandlers};
    use orch8_engine::handlers::HandlerRegistry;
    use orch8_storage::StorageBackend;
    use orch8_types::clock::{ManualClock, SharedClock};
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    struct Advance(Arc<ManualClock>, Arc<AtomicUsize>);
    impl StepHandler for Advance {
        fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
            self.1.fetch_add(1, Ordering::SeqCst);
            self.0.advance(chrono::Duration::hours(1));
            Ok("{\"expired_result\":true}".into())
        }
    }
    let (storage, sqlite) = fixture().await;
    let owner = owner_with_handler(&storage, "alice", "one", serde_json::json!({}), "native").await;
    let backend: Arc<dyn StorageBackend> = sqlite;
    let clock = Arc::new(ManualClock::new(chrono::Utc::now()));
    let calls = Arc::new(AtomicUsize::new(0));
    let hub = CatalogHandlers::isolated_with_lifetime(
        storage.clone(),
        backend.clone(),
        60,
        SharedClock::from_arc(clock.clone()),
    );
    hub.bind(
        &owner,
        "native",
        Arc::new(Advance(clock, calls.clone())),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let run = storage
        .prepare_catalog_run(&owner, request(Some("late-result"), "{}"), limits())
        .await
        .unwrap();
    hub.materialize(owner.clone(), run.id, instance_limits(), builtins())
        .await
        .unwrap();
    let tenant = super::super::workflows::owner(&owner).unwrap().0;
    let mut registry = HandlerRegistry::new();
    hub.install_resolver(&mut registry, crate::DEFAULT_BUILTINS);
    tick(&backend, &Arc::new(registry)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        backend.get_instance(run.id).await.unwrap().unwrap().state,
        InstanceState::Failed
    );
    let execution = backend
        .get_continuity_execution_by_instance(&tenant, run.id)
        .await
        .unwrap()
        .unwrap();
    let receipts = backend
        .list_effect_receipts(&tenant, execution.continuity_id, 10)
        .await
        .unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(
        receipts[0].state,
        orch8_types::continuity::EffectState::Dispatched
    );
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
async fn expiry_after_preparation_is_rechecked_before_flat_and_tree_native_dispatch() {
    for tree in [false, true] {
        native_preflight_expiry(tree, 0).await;
    }
}
#[tokio::test]
async fn expiry_during_last_claim_read_prevents_native_entry() {
    for tree in [false, true] {
        for read in [1, 2] {
            native_preflight_expiry(tree, read).await;
        }
    }
}
#[allow(clippy::too_many_lines)]
async fn native_preflight_expiry(tree: bool, after_claim: usize) {
    use super::{
        admission_tests::work_scope,
        materialize_tests::{Echo, tick},
        tests::{limits, native_fixture, request},
    };
    use crate::handlers::scoped::CatalogHandlers;
    use orch8_engine::handlers::HandlerRegistry;
    use orch8_storage::{StorageBackend, testing::faulty::FaultStorage};
    use orch8_types::{
        clock::{ManualClock, SharedClock},
        continuity::EffectState,
    };
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };
    {
        let (storage, sqlite) = fixture().await;
        let owner = work_scope(&storage, tree).await;
        let run = storage
            .prepare_catalog_run(&owner, request(Some("queued-expiry"), "{}"), limits())
            .await
            .unwrap();
        native_fixture(&sqlite, &run).await;
        let tenant = super::super::workflows::owner(&owner).unwrap().0;
        let (backend, faults) = FaultStorage::wrap(sqlite);
        let backend: Arc<dyn StorageBackend> = backend;
        let clock = Arc::new(ManualClock::new(chrono::Utc::now()));
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(AtomicBool::new(false));
        let advanced = Arc::new(AtomicBool::new(false));
        if after_claim != 0 {
            let (entered, advanced, clock) = (entered.clone(), advanced.clone(), clock.clone());
            let reads = AtomicUsize::new(0);
            faults
                .on("verify_instance_claim")
                .when(move |_| {
                    if entered.load(Ordering::SeqCst)
                        && reads.fetch_add(1, Ordering::SeqCst) + 1 == after_claim
                    {
                        advanced.store(true, Ordering::SeqCst);
                        clock.advance(chrono::Duration::hours(1));
                    }
                    false // Delay/clock injection; the underlying claim read still succeeds.
                })
                .fail_transient();
        }
        let hub = CatalogHandlers::isolated_with_lifetime(
            storage,
            backend.clone(),
            60,
            SharedClock::from_arc(clock.clone()),
        );
        hub.bind(
            &owner,
            "native",
            Arc::new(Echo(calls.clone())),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let mut adapter = HandlerRegistry::new();
        hub.register(&mut adapter, "native");
        let adapter = Arc::new(adapter);
        let mut registry = HandlerRegistry::new();
        // The core has already prepared its effect capability here. Advancing
        // only at adapter entry bypasses the earlier contextual selection check.
        registry.register_with_deferred_effect_admission("native", move |ctx| {
            entered.store(true, Ordering::SeqCst);
            if after_claim == 0 {
                clock.advance(chrono::Duration::hours(1));
            }
            adapter.get("native").unwrap()(ctx)
        });
        tick(&backend, &Arc::new(registry)).await;
        assert_eq!(advanced.load(Ordering::SeqCst), after_claim != 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0, "tree={tree}");
        assert_eq!(
            backend.get_instance(run.id).await.unwrap().unwrap().state,
            InstanceState::Failed
        );
        let receipts = backend
            .list_instance_effect_receipts(&tenant, run.id, 10)
            .await
            .unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            receipts[0].state,
            if after_claim == 2 {
                EffectState::Dispatched
            } else {
                EffectState::Prepared
            }
        );
        for receipt in hub.close() {
            receipt.await.unwrap();
        }
    }
}
