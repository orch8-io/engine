//! Admission contention and infrastructure faults are scheduler control outcomes,
//! independent of a workflow's business retry policy.
use super::{
    CatalogScope, MobileStorage,
    tests::{fixture, limits, native_fixture, request, scope},
};
use crate::{HandlerError, StepHandler, handlers::scoped::CatalogHandlers};
use orch8_engine::{
    handlers::HandlerRegistry, scheduler::tick_once, sequence_cache::SequenceCache,
};
use orch8_storage::{
    StorageBackend,
    testing::{faulty::FaultStorage, fixtures::SequenceBuilder},
};
use orch8_types::{
    clock::{ManualClock, SharedClock},
    config::SchedulerConfig,
    instance::InstanceState,
    sequence::{BlockDefinition, ParallelDef},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Native(Arc<AtomicUsize>);
impl StepHandler for Native {
    fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok("{}".into())
    }
}
async fn work_scope(storage: &MobileStorage, tree: bool) -> CatalogScope {
    let account = storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    let owner = storage
        .open_catalog_scope(&account, "one", "source", "authority")
        .await
        .unwrap();
    let step = orch8_storage::testing::fixtures::step_block("work", "native");
    let block = if tree {
        BlockDefinition::Parallel(Box::new(ParallelDef {
            id: orch8_types::ids::BlockId::new("parallel"),
            branches: vec![vec![step]],
        }))
    } else {
        step
    };
    let seq = SequenceBuilder::new("publisher")
        .name("flow")
        .block(block)
        .build();
    let digest = "a".repeat(64);
    storage
        .accept_catalog_manifest(&owner, &[], 1, &digest)
        .await
        .unwrap();
    storage
        .install_catalog_workflow(&owner, &serde_json::to_string(&seq).unwrap(), 1, &digest)
        .await
        .unwrap();
    storage
        .mark_catalog_reconciled(&owner, 1, &digest)
        .await
        .unwrap();
    owner
}

#[allow(clippy::too_many_lines)]
async fn scheduler_contract(tree: bool, global: bool, fault: bool) {
    let (storage, sqlite) = fixture().await;
    let owner = work_scope(&storage, tree).await;
    let run = storage
        .prepare_catalog_run(&owner, request(Some("admission"), "{}"), limits())
        .await
        .unwrap();
    native_fixture(&sqlite, &run).await;
    let (backend, faults) = FaultStorage::wrap(sqlite.clone());
    let hub = CatalogHandlers::isolated(storage.clone(), backend.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    hub.bind(
        &owner,
        "native",
        Arc::new(Native(calls.clone())),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let mut registry = HandlerRegistry::new();
    let mut occupied = Vec::new();
    let injected = Arc::new(AtomicBool::new(false));
    if fault {
        let entered = Arc::new(AtomicBool::new(false));
        let observed = entered.clone();
        let injection = injected.clone();
        faults
            .on("verify_instance_claim")
            .when(move |_| {
                observed.load(Ordering::SeqCst) && !injection.swap(true, Ordering::SeqCst)
            })
            .fail_transient();
        hub.register(&mut registry, "native");
        let native = Arc::new(registry);
        registry = HandlerRegistry::new();
        registry.register_with_deferred_effect_admission("native", move |ctx| {
            entered.store(true, Ordering::SeqCst);
            native.get("native").unwrap()(ctx)
        });
    } else {
        hub.install_resolver(&mut registry, crate::DEFAULT_BUILTINS);
        occupied.push(hub.occupy_native_for_test(&owner));
        if global {
            let other = scope(&storage, "bob", "one").await;
            occupied.push(hub.occupy_native_for_test(&other));
        }
    }
    let backend: Arc<dyn StorageBackend> = backend;
    let registry = Arc::new(registry);
    let clock = Arc::new(ManualClock::new(
        chrono::Utc::now() + chrono::Duration::seconds(1),
    ));
    let config = SchedulerConfig {
        clock: SharedClock::from_arc(clock.clone()),
        ..SchedulerConfig::default()
    };
    let semaphore = Arc::new(tokio::sync::Semaphore::new(4));
    let cache = Arc::new(SequenceCache::new(16, Duration::from_secs(30)));
    let cancel = tokio_util::sync::CancellationToken::new();
    let mut receipt = None;
    // More than the infrastructure-fault retry limit: resource contention must
    // neither consume that budget nor increment the workflow's business attempt.
    for _ in 0..if fault { 1 } else { 40 } {
        tick_once(&backend, &registry, &semaphore, &config, &cache, &cancel)
            .await
            .unwrap();
        let instance = backend.get_instance(run.id).await.unwrap().unwrap();
        assert_eq!(
            instance.state,
            InstanceState::Scheduled,
            "admission is not a business failure"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        if fault {
            assert!(
                injected.load(Ordering::SeqCst),
                "fault must fire after adapter entry"
            );
        } else {
            assert_eq!(
                instance.next_fire_at,
                Some(
                    orch8_types::clock::Clock::now(clock.as_ref())
                        + chrono::Duration::milliseconds(250)
                ),
                "admission must not use the growing infrastructure-fault backoff"
            );
        }
        let ledger = backend
            .get_continuity_execution_by_instance(&instance.tenant_id, run.id)
            .await
            .unwrap()
            .unwrap();
        let rows = backend
            .list_effect_receipts(&instance.tenant_id, ledger.continuity_id, 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].state,
            orch8_types::continuity::EffectState::Prepared
        );
        assert_eq!(rows[0].attempt, 0);
        if let Some(previous) = receipt {
            assert_eq!(rows[0].id, previous);
        } else {
            receipt = Some(rows[0].id);
        }
        clock.advance(chrono::Duration::seconds(1));
    }
    drop(occupied);
    for _ in 0..8 {
        tick_once(&backend, &registry, &semaphore, &config, &cache, &cancel)
            .await
            .unwrap();
        if backend.get_instance(run.id).await.unwrap().unwrap().state == InstanceState::Completed {
            break;
        }
        clock.advance(chrono::Duration::seconds(1));
    }
    assert_eq!(
        backend.get_instance(run.id).await.unwrap().unwrap().state,
        InstanceState::Completed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let tenant = backend
        .get_instance(run.id)
        .await
        .unwrap()
        .unwrap()
        .tenant_id;
    let ledger = backend
        .get_continuity_execution_by_instance(&tenant, run.id)
        .await
        .unwrap()
        .unwrap();
    let rows = backend
        .list_effect_receipts(&tenant, ledger.continuity_id, 10)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(Some(rows[0].id), receipt);
    assert_eq!(
        rows[0].state,
        orch8_types::continuity::EffectState::Committed
    );
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}
#[tokio::test]
async fn flat_account_contention_reschedules_without_business_retry() {
    scheduler_contract(false, false, false).await;
}
#[tokio::test]
async fn tree_account_contention_reschedules_without_business_retry() {
    scheduler_contract(true, false, false).await;
}
#[tokio::test]
async fn flat_global_contention_reschedules_without_business_retry() {
    scheduler_contract(false, true, false).await;
}
#[tokio::test]
async fn tree_global_contention_reschedules_without_business_retry() {
    scheduler_contract(true, true, false).await;
}
#[tokio::test]
async fn flat_native_preflight_storage_fault_preserves_infrastructure_recovery() {
    scheduler_contract(false, false, true).await;
}
#[tokio::test]
async fn tree_native_preflight_storage_fault_preserves_infrastructure_recovery() {
    scheduler_contract(true, false, true).await;
}
