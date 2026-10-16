//! Transient storage faults on the scheduler and cron paths, injected with
//! `orch8_storage::testing::faulty::FaultStorage` and driven through the real
//! `tick_once` / `process_cron_tick` (T1-GAP-001, T1-GAP-002 / L3-06,
//! T1-GAP-112 / L2-08).
//!
//! Pattern to copy: wrap a fresh `SQLite` backend, seed through the *inner*
//! backend (so arrange steps are neither counted nor failed), arm one rule,
//! tick, assert on the inner backend's rows.

use std::sync::Arc;

use chrono::{Duration as ChronoDuration, Utc};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use orch8_engine::clock::{Clock, ManualClock, SharedClock};
use orch8_engine::cron::process_cron_tick;
use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::scheduler::tick_once;
use orch8_storage::StorageBackend;
use orch8_storage::testing::faulty::{FaultHandle, FaultStorage};
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
use orch8_types::config::SchedulerConfig;
use orch8_types::cron::{CronSchedule, OverlapPolicy};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::InstanceState;
use orch8_types::sequence::{BlockDefinition, ParallelDef, SequenceDefinition};

mod common;
use common::{cache, default_config, mk_step, registry, semaphore};

struct Rig {
    /// What the code under test sees.
    storage: Arc<dyn StorageBackend>,
    /// The real backend, for arrange / assert without touching the counters.
    inner: Arc<dyn StorageBackend>,
    faults: FaultHandle,
    clock: Arc<ManualClock>,
    config: SchedulerConfig,
    handlers: Arc<HandlerRegistry>,
}

impl Rig {
    async fn new() -> Self {
        let inner = common::storage().await;
        let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
        let clock = Arc::new(ManualClock::new(Utc::now()));
        let config = SchedulerConfig {
            clock: SharedClock::from_arc(Arc::clone(&clock) as Arc<dyn Clock>),
            ..default_config()
        };
        Self {
            storage,
            inner,
            faults,
            clock,
            config,
            handlers: Arc::new(registry()),
        }
    }

    async fn seed(&self, seq: &SequenceDefinition) -> InstanceId {
        self.inner.create_sequence(seq).await.unwrap();
        let inst = InstanceBuilder::for_sequence(seq)
            .due_at(self.clock.now())
            .build();
        self.inner.create_instance(&inst).await.unwrap();
        inst.id
    }

    async fn tick(&self) {
        tick_once(
            &self.storage,
            &self.handlers,
            &semaphore(16),
            &self.config,
            &cache(),
            &CancellationToken::new(),
        )
        .await
        .expect("tick_once");
    }

    async fn state(&self, id: InstanceId) -> (InstanceState, Option<chrono::DateTime<Utc>>) {
        let inst = self
            .inner
            .get_instance(id)
            .await
            .unwrap()
            .expect("instance");
        (inst.state, inst.next_fire_at)
    }
}

/// T1-GAP-001 (L2-02 / L2-17 end to end): a transient error while the flat
/// step loop persists a step output reschedules the instance with the first
/// transient backoff (200 ms, +-20 % jitter) instead of failing it.
#[tokio::test]
async fn transient_storage_error_mid_step_reschedules_instead_of_failing() {
    let rig = Rig::new().await;
    let seq = SequenceBuilder::new("t")
        .step("a", "noop")
        .step("b", "noop")
        .build();
    let id = rig.seed(&seq).await;
    rig.faults.on("save_block_output").times(1).fail_transient();
    let tick_at = rig.clock.now();

    rig.tick().await;

    let (state, next_fire_at) = rig.state(id).await;
    assert_eq!(state, InstanceState::Scheduled);
    let backoff_ms = (next_fire_at.expect("rescheduled") - tick_at).num_milliseconds();
    assert!(
        (160..=240).contains(&backoff_ms),
        "first transient retry must back off ~200 ms, got {backoff_ms} ms"
    );
    assert_eq!(rig.faults.injected("save_block_output"), 1);
}

/// T1-GAP-001, second half: once storage recovers, the next tick after the
/// backoff completes the instance.
#[tokio::test]
async fn instance_completes_on_the_tick_after_a_transient_storage_error() {
    let rig = Rig::new().await;
    let seq = SequenceBuilder::new("t")
        .step("a", "noop")
        .step("b", "noop")
        .build();
    let id = rig.seed(&seq).await;
    rig.faults.on("save_block_output").times(1).fail_transient();
    rig.tick().await;

    rig.clock.advance(ChronoDuration::seconds(1));
    rig.tick().await;

    assert_eq!(rig.state(id).await.0, InstanceState::Completed);
}

/// A permanent error on the same write is a processing failure: the
/// instance is failed, not rescheduled (the control for the test above).
#[tokio::test]
async fn permanent_storage_error_mid_step_fails_the_instance() {
    let rig = Rig::new().await;
    let seq = SequenceBuilder::new("t").step("a", "noop").build();
    let id = rig.seed(&seq).await;
    rig.faults.on("save_block_output").times(1).fail_permanent();

    rig.tick().await;

    assert_eq!(rig.state(id).await.0, InstanceState::Failed);
}

fn parallel_sequence() -> SequenceDefinition {
    SequenceBuilder::new("t")
        .block(BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("fan"),
            branches: vec![
                vec![mk_step("left", "noop")],
                vec![mk_step("right", "noop")],
            ],
        })))
        .build()
}

/// T1-GAP-002 / L3-06: on the tree path (composite blocks), a transient
/// failure loading the execution tree reschedules the instance and leaves
/// the tree as it was.
#[tokio::test]
async fn tree_path_transient_error_reschedules_not_fails() {
    let rig = Rig::new().await;
    let id = rig.seed(&parallel_sequence()).await;
    rig.faults
        .on("get_execution_tree")
        .times(1)
        .fail_transient();

    rig.tick().await;

    assert_eq!(rig.state(id).await.0, InstanceState::Scheduled);
    assert_eq!(rig.faults.injected("get_execution_tree"), 1);
}

/// T1-GAP-002, second half: the next tick after the backoff runs the whole
/// parallel block to completion.
#[tokio::test]
async fn tree_path_instance_completes_after_transient_error_clears() {
    let rig = Rig::new().await;
    let id = rig.seed(&parallel_sequence()).await;
    rig.faults
        .on("get_execution_tree")
        .times(1)
        .fail_transient();
    rig.tick().await;

    rig.clock.advance(ChronoDuration::seconds(1));
    for _ in 0..5 {
        rig.tick().await;
    }

    assert_eq!(rig.state(id).await.0, InstanceState::Completed);
}

fn due_schedule(seq_id: SequenceId, due: chrono::DateTime<Utc>) -> CronSchedule {
    CronSchedule {
        id: Uuid::now_v7(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("default"),
        sequence_id: seq_id,
        cron_expr: "0 0 * * *".into(),
        timezone: "UTC".into(),
        enabled: true,
        metadata: json!({}),
        overlap_policy: OverlapPolicy::Allow,
        skipped_fires: 0,
        last_skipped_at: None,
        last_triggered_at: None,
        next_fire_at: Some(due),
        created_at: due,
        updated_at: due,
    }
}

async fn seed_cron(rig: &Rig) -> CronSchedule {
    let seq = SequenceBuilder::new("t").step("a", "noop").build();
    rig.inner.create_sequence(&seq).await.unwrap();
    let schedule = due_schedule(seq.id, rig.clock.now() - ChronoDuration::seconds(5));
    rig.inner.create_cron_schedule(&schedule).await.unwrap();
    schedule
}

/// Move the virtual clock past the cron claim lease so the occurrence can be
/// re-claimed.
fn expire_cron_lease(rig: &Rig) {
    rig.clock.advance(ChronoDuration::seconds(
        orch8_storage::CRON_CLAIM_LEASE_SECS + 1,
    ));
}

/// T1-GAP-112 / L2-08: a transient `create_instance` failure keeps the
/// occurrence (fire time not advanced), so nothing is lost.
#[tokio::test]
async fn cron_transient_create_error_keeps_the_occurrence() {
    let rig = Rig::new().await;
    let schedule = seed_cron(&rig).await;
    rig.faults.on("create_instance").times(1).fail_transient();

    process_cron_tick(&rig.storage, &rig.config.clock)
        .await
        .unwrap();

    let after = rig
        .inner
        .get_cron_schedule(schedule.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.next_fire_at, schedule.next_fire_at);
    assert!(after.last_triggered_at.is_none());
    let fired = rig
        .inner
        .active_instance_ids_for_cron(schedule.id, 10)
        .await
        .unwrap();
    assert!(fired.is_empty());
}

/// T1-GAP-112: once the claim lease expires the same occurrence refires and
/// creates exactly one instance, keyed on the original fire time.
#[tokio::test]
async fn cron_occurrence_refires_once_after_transient_create_error() {
    let rig = Rig::new().await;
    let schedule = seed_cron(&rig).await;
    rig.faults.on("create_instance").times(1).fail_transient();
    process_cron_tick(&rig.storage, &rig.config.clock)
        .await
        .unwrap();

    expire_cron_lease(&rig);
    process_cron_tick(&rig.storage, &rig.config.clock)
        .await
        .unwrap();

    let fired = rig
        .inner
        .active_instance_ids_for_cron(schedule.id, 10)
        .await
        .unwrap();
    assert_eq!(fired.len(), 1);
    let inst = rig.inner.get_instance(fired[0]).await.unwrap().unwrap();
    let expected_key = format!(
        "cron:{}:{}",
        schedule.id,
        schedule.next_fire_at.unwrap().to_rfc3339()
    );
    assert_eq!(inst.idempotency_key.as_deref(), Some(expected_key.as_str()));
}

/// Ambiguous commit: the first `create_instance` landed but reported a
/// transient error. The refire must hit the idempotency key, not duplicate,
/// and the schedule must then advance.
#[tokio::test]
async fn cron_refire_after_ambiguous_commit_does_not_duplicate_the_instance() {
    let rig = Rig::new().await;
    let schedule = seed_cron(&rig).await;
    rig.faults.on("create_instance").times(1).fail_after_commit(
        orch8_types::error::StorageError::Connection("reset after commit".into()),
    );
    process_cron_tick(&rig.storage, &rig.config.clock)
        .await
        .unwrap();

    expire_cron_lease(&rig);
    process_cron_tick(&rig.storage, &rig.config.clock)
        .await
        .unwrap();

    let fired = rig
        .inner
        .active_instance_ids_for_cron(schedule.id, 10)
        .await
        .unwrap();
    assert_eq!(fired.len(), 1, "exactly one instance for the occurrence");
    let after = rig
        .inner
        .get_cron_schedule(schedule.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        after.next_fire_at > schedule.next_fire_at,
        "schedule advanced"
    );
}
