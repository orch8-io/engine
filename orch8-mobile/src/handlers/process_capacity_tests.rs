use super::*;
use orch8_engine::{scheduler::tick_once, sequence_cache::SequenceCache};
use orch8_storage::{StorageBackend, testing::fixtures::SequenceBuilder};
use orch8_types::{
    clock::{ManualClock, SharedClock},
    config::SchedulerConfig,
    continuity::EffectState,
    instance::InstanceState,
    sequence::{BlockDefinition, ParallelDef},
};
use std::sync::atomic::{AtomicUsize, Ordering};

mod physical;

struct Counter(Arc<AtomicUsize>);
impl StepHandler for Counter {
    fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok("{}".into())
    }
}

#[allow(clippy::too_many_lines)]
async fn scheduler_capacity(tree: bool, straggler: bool) {
    let sqlite = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    );
    let backend: Arc<dyn StorageBackend> = sqlite;
    let native_step = orch8_storage::testing::fixtures::step_block("work", "native");
    let block = if tree {
        BlockDefinition::Parallel(Box::new(ParallelDef {
            id: orch8_types::ids::BlockId::new("parallel"),
            branches: vec![vec![native_step]],
        }))
    } else {
        native_step
    };
    let sequence = SequenceBuilder::new("mobile").block(block).build();
    backend.create_sequence(&sequence).await.unwrap();
    let instance = orch8_storage::testing::fixtures::InstanceBuilder::for_sequence(&sequence)
        .due_now()
        .build();
    backend.create_instance(&instance).await.unwrap();
    let capacity = Arc::new(Semaphore::new(1));
    let occupied = (!straggler).then(|| capacity.clone().try_acquire_owned().unwrap());
    let quarantine = Arc::new(Stragglers::default());
    if straggler {
        quarantine.begin("native");
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = HandlerRegistry::new();
    register_with_capacity(
        &mut registry,
        "native",
        Arc::new(Counter(calls.clone())),
        Duration::from_secs(5),
        quarantine.clone(),
        CancellationToken::new(),
        capacity,
    );
    let registry = Arc::new(registry);
    let clock = Arc::new(ManualClock::new(
        chrono::Utc::now() + chrono::Duration::seconds(1),
    ));
    let config = SchedulerConfig {
        clock: SharedClock::from_arc(clock.clone()),
        ..SchedulerConfig::default()
    };
    let semaphore = Arc::new(Semaphore::new(4));
    let cache = Arc::new(SequenceCache::new(16, Duration::from_secs(30)));
    let stop = CancellationToken::new();
    let mut receipt_id = None;
    // More than the infrastructure retry ceiling. Contention is never a
    // business attempt or a growing infrastructure-failure backoff.
    for _ in 0..40 {
        tick_once(&backend, &registry, &semaphore, &config, &cache, &stop)
            .await
            .unwrap();
        let current = backend.get_instance(instance.id).await.unwrap().unwrap();
        assert_eq!(
            current.state,
            InstanceState::Scheduled,
            "occupied native capacity must reschedule before dispatch"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            current.next_fire_at,
            Some(
                orch8_types::clock::Clock::now(clock.as_ref())
                    + chrono::Duration::milliseconds(250)
            )
        );
        let ledger = backend
            .get_continuity_execution_by_instance(&instance.tenant_id, instance.id)
            .await
            .unwrap()
            .unwrap();
        let receipts = backend
            .list_effect_receipts(&instance.tenant_id, ledger.continuity_id, 10)
            .await
            .unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].state, EffectState::Prepared);
        assert_eq!(receipts[0].attempt, 0);
        if let Some(previous) = receipt_id {
            assert_eq!(receipts[0].id, previous);
        } else {
            receipt_id = Some(receipts[0].id);
        }
        clock.advance(chrono::Duration::seconds(1));
    }
    drop(occupied);
    if straggler {
        quarantine.end("native");
    }
    for _ in 0..8 {
        tick_once(&backend, &registry, &semaphore, &config, &cache, &stop)
            .await
            .unwrap();
        if backend
            .get_instance(instance.id)
            .await
            .unwrap()
            .unwrap()
            .state
            == InstanceState::Completed
        {
            break;
        }
        clock.advance(chrono::Duration::seconds(1));
    }
    assert_eq!(
        backend
            .get_instance(instance.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Completed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let ledger = backend
        .get_continuity_execution_by_instance(&instance.tenant_id, instance.id)
        .await
        .unwrap()
        .unwrap();
    let receipts = backend
        .list_effect_receipts(&instance.tenant_id, ledger.continuity_id, 10)
        .await
        .unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(Some(receipts[0].id), receipt_id);
    assert_eq!(receipts[0].state, EffectState::Committed);
}

#[tokio::test]
async fn flat_process_contention_preserves_business_attempt_and_prepared_effect() {
    scheduler_capacity(false, false).await;
}

#[tokio::test]
async fn tree_process_contention_preserves_business_attempt_and_prepared_effect() {
    scheduler_capacity(true, false).await;
}

#[tokio::test]
async fn flat_straggler_contention_preserves_business_attempt_and_prepared_effect() {
    scheduler_capacity(false, true).await;
}
#[tokio::test]
async fn tree_straggler_contention_preserves_business_attempt_and_prepared_effect() {
    scheduler_capacity(true, true).await;
}
