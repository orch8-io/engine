//! Placement policies (docs/PLACEMENT.md): residency, labels, tenant
//! policies, sticky affinity, rate budgets, and the autoscaling backlog.
//!
//! Every scenario runs against `SQLite` and, when `DATABASE_URL` is set,
//! against Postgres too. Each scenario uses its own tenant and handler so
//! parallel tests sharing the Postgres tables (and the process-wide policy
//! cache) never observe each other.
#![allow(clippy::too_many_lines)]

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::Utc;
use serde_json::json;

use orch8_storage::StorageBackend;
use orch8_storage::postgres::PostgresStorage;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::continuity::{
    CapsuleRequirements, RuntimeCapabilities, RuntimeConnectivity, RuntimeId, RuntimeKind,
    RuntimeTrustLevel,
};
use orch8_types::filter::Pagination;
use orch8_types::ids::TenantId;
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::placement::{
    Affinity, PLACEMENT_UNSATISFIED, Placement, PlacementPolicies, PlacementPolicy,
    PlacementPreference, PolicyMatch, PolicyPreference, PolicyRequirement, RateBudget,
    RateBudgetCheck,
};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition};
use orch8_types::worker::{WorkerTask, WorkerTaskState};
use orch8_types::worker_filter::WorkerTaskFilter;

use common::{mk_instance, mk_sequence, mk_step, registry};

async fn backends() -> Vec<(&'static str, Arc<dyn StorageBackend>)> {
    let mut out: Vec<(&'static str, Arc<dyn StorageBackend>)> = vec![(
        "sqlite",
        Arc::new(SqliteStorage::in_memory().await.unwrap()),
    )];
    if let Ok(url) = std::env::var("DATABASE_URL") {
        let pg = PostgresStorage::new(&url, 5, None)
            .await
            .expect("connect to DATABASE_URL");
        pg.run_migrations().await.expect("run migrations");
        out.push(("postgres", Arc::new(pg)));
    } else {
        eprintln!("DATABASE_URL not set: running the sqlite leg only");
    }
    out
}

fn unique(prefix: &str) -> String {
    format!("{prefix}.{}", uuid::Uuid::now_v7().simple())
}

fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

fn placed_step(id: &str, handler: &str, placement: Placement) -> BlockDefinition {
    let mut block = mk_step(id, handler);
    if let BlockDefinition::Step(step) = &mut block {
        step.placement = Some(placement);
    }
    block
}

fn caps(handler: &str, runtime_labels: BTreeMap<String, String>) -> RuntimeCapabilities {
    let now = Utc::now();
    RuntimeCapabilities {
        runtime_id: RuntimeId::new(),
        kind: RuntimeKind::Server,
        trust: RuntimeTrustLevel::Registered,
        handlers: vec![handler.into()],
        plugins: Vec::new(),
        credentials: Vec::new(),
        regions: vec!["eu-west-1".into()],
        hardware: Vec::new(),
        offline_capable: false,
        connectivity: Some(RuntimeConnectivity::Ethernet),
        battery_percent: None,
        estimated_cost_microunits: None,
        estimated_latency_ms: None,
        draining: false,
        capsule_signing_public_key: None,
        labels: runtime_labels,
        observed_at: now,
        expires_at: now + chrono::Duration::minutes(4),
    }
}

/// Create the sequence + instance under `tenant` and evaluate until the
/// step is dispatched (or deferred).
async fn start(
    storage: &Arc<dyn StorageBackend>,
    tenant: &TenantId,
    mut seq: SequenceDefinition,
) -> (SequenceDefinition, TaskInstance) {
    seq.name = unique("placement");
    seq.tenant_id = tenant.clone();
    storage.create_sequence(&seq).await.unwrap();
    let mut inst = mk_instance(seq.id);
    inst.tenant_id = tenant.clone();
    storage.create_instance(&inst).await.unwrap();
    common::drive_n(storage, &registry(), inst.id, &seq, 5).await;
    let current = storage.get_instance(inst.id).await.unwrap().unwrap();
    if current.state == InstanceState::Running {
        storage
            .update_instance_state(inst.id, InstanceState::Waiting, None)
            .await
            .unwrap();
    }
    (seq, inst)
}

async fn tasks_of(storage: &Arc<dyn StorageBackend>, inst: &TaskInstance) -> Vec<WorkerTask> {
    storage
        .list_worker_tasks(
            &WorkerTaskFilter {
                instance_id: Some(inst.id),
                ..WorkerTaskFilter::default()
            },
            &Pagination::default(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn residency_waits_and_never_dispatches_to_a_non_matching_runtime() {
    for (backend, storage) in backends().await {
        let tenant = TenantId::unchecked(unique("tenant"));
        let handler = unique("ext.charge");
        let seq = mk_sequence(vec![placed_step(
            "charge",
            &handler,
            Placement {
                residency: Some("eu".into()),
                ..Placement::default()
            },
        )]);
        let (_, inst) = start(&storage, &tenant, seq).await;

        let tasks = tasks_of(&storage, &inst).await;
        assert_eq!(tasks.len(), 1, "{backend}: {tasks:?}");
        let task = &tasks[0];
        assert_eq!(
            task.requirements.residency.as_deref(),
            Some("eu"),
            "{backend}"
        );
        assert_eq!(task.state, WorkerTaskState::Pending);

        // Visible reason on the instance.
        let current = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(current.state, InstanceState::Waiting, "{backend}");
        assert_eq!(
            current.metadata["placement"]["status"], PLACEMENT_UNSATISFIED,
            "{backend}: {}",
            current.metadata
        );

        // A legacy (capability-less) poll never claims placed work.
        let legacy = storage
            .claim_worker_tasks(&handler, "legacy-worker", 10)
            .await
            .unwrap();
        assert!(
            legacy.is_empty(),
            "{backend}: legacy poll claimed {legacy:?}"
        );

        // A runtime in another residency zone never claims it.
        let us = caps(&handler, labels(&[("residency", "us")]));
        let claimed = storage
            .claim_worker_tasks_matching(
                &handler,
                &us.runtime_id.to_string(),
                Some(&tenant),
                None,
                &us,
                10,
            )
            .await
            .unwrap();
        assert!(
            claimed.is_empty(),
            "{backend}: us runtime claimed {claimed:?}"
        );

        // The backlog reports it as unsatisfied.
        let snapshot = orch8_engine::step_placement::backlog_snapshot(storage.as_ref())
            .await
            .unwrap();
        assert!(
            snapshot
                .unsatisfied
                .keys()
                .any(|(capability, _)| capability == &handler),
            "{backend}: {snapshot:?}"
        );
        assert!(
            snapshot.depth.iter().any(|(labels, count)| {
                labels.capability == handler && labels.priority_lane == "standard" && *count == 1
            }),
            "{backend}: {snapshot:?}"
        );

        // A matching runtime claims it; the status flips to `placed`.
        let eu = caps(&handler, labels(&[("residency", "eu")]));
        let claimed = storage
            .claim_worker_tasks_matching(
                &handler,
                &eu.runtime_id.to_string(),
                Some(&tenant),
                None,
                &eu,
                10,
            )
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1, "{backend}");
        orch8_engine::step_placement::record_placement_claimed(storage.as_ref(), &claimed[0]).await;
        let current = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(
            current.metadata["placement"]["status"], "placed",
            "{backend}"
        );
    }
}

#[tokio::test]
async fn tenant_policies_add_labels_and_preferences() {
    for (backend, storage) in backends().await {
        let tenant = TenantId::unchecked(unique("tenant"));
        let handler = unique("ext.render");
        storage
            .put_placement_policies(
                &tenant,
                &PlacementPolicies {
                    items: vec![PlacementPolicy {
                        name: "gpu".into(),
                        matcher: PolicyMatch {
                            handler: Some(handler.clone()),
                            ..PolicyMatch::default()
                        },
                        require: PolicyRequirement {
                            labels: labels(&[("gpu", "a100")]),
                            ..PolicyRequirement::default()
                        },
                        prefer: Some(PolicyPreference {
                            labels: labels(&[("tier", "fast")]),
                        }),
                    }],
                },
            )
            .await
            .unwrap();
        orch8_engine::step_placement::invalidate_policies(&tenant).await;

        let seq = mk_sequence(vec![mk_step("render", &handler)]);
        let (_, inst) = start(&storage, &tenant, seq).await;
        let tasks = tasks_of(&storage, &inst).await;
        assert_eq!(tasks.len(), 1, "{backend}: policy forces the worker queue");
        let task = &tasks[0];
        assert_eq!(
            task.requirements.labels,
            labels(&[("gpu", "a100")]),
            "{backend}"
        );
        let prefer = task.requirements.prefer.as_ref().expect("preference");
        assert_eq!(prefer.labels, labels(&[("tier", "fast")]), "{backend}");

        // Without the required label: never. With it but not preferred:
        // only after the bounded wait. Preferred: immediately.
        let plain = caps(&handler, labels(&[("tier", "fast")]));
        let slow = caps(&handler, labels(&[("gpu", "a100"), ("tier", "slow")]));
        let fast = caps(&handler, labels(&[("gpu", "a100"), ("tier", "fast")]));
        for (runtime, expect) in [(&plain, 0), (&slow, 0), (&fast, 1)] {
            let claimed = storage
                .claim_worker_tasks_matching(
                    &handler,
                    &runtime.runtime_id.to_string(),
                    Some(&tenant),
                    None,
                    runtime,
                    10,
                )
                .await
                .unwrap();
            assert_eq!(claimed.len(), expect, "{backend}: {:?}", runtime.labels);
        }
    }
}

fn pending_task(
    inst: &TaskInstance,
    handler: &str,
    requirements: CapsuleRequirements,
) -> WorkerTask {
    WorkerTask {
        id: uuid::Uuid::now_v7(),
        instance_id: inst.id,
        block_id: orch8_types::ids::BlockId::new("s"),
        handler_name: handler.into(),
        queue_name: None,
        requirements,
        params: json!({}),
        context: json!({}),
        attempt: 1,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        claim_epoch: 0,
        resume_checkpoint: None,
        checkpoint_seq: 0,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
        effect_id: None,
        continuity_epoch: None,
        lease_secs: None,
        carries_credentials: false,
        claimed_runtime_kind: None,
    }
}

#[tokio::test]
async fn sticky_affinity_prefers_the_previous_worker_then_falls_back() {
    for (backend, storage) in backends().await {
        let handler = unique("ext.affine");
        let mut seq = mk_sequence(vec![mk_step("s", &handler)]);
        seq.name = unique("affinity");
        storage.create_sequence(&seq).await.unwrap();
        let inst = mk_instance(seq.id);
        storage.create_instance(&inst).await.unwrap();
        storage
            .update_instance_state(inst.id, InstanceState::Waiting, None)
            .await
            .unwrap();

        // A completed step ran on `worker-a`.
        let mut done = pending_task(&inst, &handler, CapsuleRequirements::default());
        done.block_id = orch8_types::ids::BlockId::new("previous");
        storage.create_worker_task(&done).await.unwrap();
        let claimed = storage
            .claim_worker_tasks(&handler, "worker-a", 1)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1, "{backend}");
        assert!(
            storage
                .complete_worker_task(
                    done.id,
                    &orch8_types::worker::WorkerClaim::new("worker-a", claimed[0].claim_epoch),
                    &json!({}),
                )
                .await
                .unwrap()
        );

        // The next step's placement resolves the preference to worker-a.
        let mut step = orch8_types::sequence::StepDef::clone(match &seq.blocks[0] {
            BlockDefinition::Step(step) => step,
            _ => unreachable!(),
        });
        step.placement = Some(Placement {
            affinity: Some(Affinity::Instance),
            affinity_wait_ms: Some(60_000),
            ..Placement::default()
        });
        let mut params = json!({"x": 1});
        let resolved = orch8_engine::step_placement::apply_step_placement(
            storage.as_ref(),
            &inst,
            &step,
            &mut params,
            Utc::now(),
        )
        .await
        .unwrap()
        .unwrap()
        .expect("affinity applies");
        assert!(!resolved.has_hard_constraints());
        let requirements = orch8_types::worker::peek_runtime_requirements(&params).unwrap();
        let prefer = requirements.prefer.clone().expect("preference");
        assert_eq!(prefer.worker_id.as_deref(), Some("worker-a"), "{backend}");

        // Legacy polls honour the preference window in SQL.
        let task = pending_task(&inst, &handler, requirements);
        storage.create_worker_task(&task).await.unwrap();
        let other = storage
            .claim_worker_tasks(&handler, "worker-b", 10)
            .await
            .unwrap();
        assert!(
            other.is_empty(),
            "{backend}: worker-b claimed during the wait"
        );
        let preferred = storage
            .claim_worker_tasks(&handler, "worker-a", 10)
            .await
            .unwrap();
        assert_eq!(preferred.len(), 1, "{backend}");

        // After the bounded wait any worker may claim.
        let mut expired = pending_task(
            &inst,
            &handler,
            CapsuleRequirements {
                prefer: Some(PlacementPreference {
                    worker_id: Some("worker-a".into()),
                    labels: BTreeMap::new(),
                    until_ms: Utc::now().timestamp_millis() - 1_000,
                }),
                ..CapsuleRequirements::default()
            },
        );
        expired.block_id = orch8_types::ids::BlockId::new("after-wait");
        storage.create_worker_task(&expired).await.unwrap();
        let fallback = storage
            .claim_worker_tasks(&handler, "worker-b", 10)
            .await
            .unwrap();
        assert_eq!(
            fallback.len(),
            1,
            "{backend}: fallback after the wait: {:?}",
            tasks_of(&storage, &inst)
                .await
                .iter()
                .map(|t| (t.state, t.requirements.clone(), t.worker_id.clone()))
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn rate_budget_defers_instead_of_failing() {
    for (backend, storage) in backends().await {
        let tenant = TenantId::unchecked(unique("tenant"));
        let key = unique("stripe-api");
        let stored = storage
            .upsert_rate_budget(&RateBudget {
                tenant_id: tenant.as_str().into(),
                key: key.clone(),
                capacity: 1,
                refill_per_sec: 0.001,
                tokens: 1.0,
                updated_at: Utc::now(),
            })
            .await
            .unwrap();
        assert!(
            (stored.tokens - 1.0).abs() < 1e-9,
            "{backend}: created full"
        );

        let handler = unique("ext.stripe");
        let budgeted = || {
            let mut block = mk_step("charge", &handler);
            if let BlockDefinition::Step(step) = &mut block {
                step.rate_budget = Some(key.clone());
            }
            mk_sequence(vec![block])
        };
        let (_, first) = start(&storage, &tenant, budgeted()).await;
        assert_eq!(tasks_of(&storage, &first).await.len(), 1, "{backend}");

        let (_, second) = start(&storage, &tenant, budgeted()).await;
        let current = storage.get_instance(second.id).await.unwrap().unwrap();
        assert_eq!(
            current.state,
            InstanceState::Scheduled,
            "{backend}: deferred"
        );
        assert!(
            current.next_fire_at.expect("retry time") > Utc::now(),
            "{backend}"
        );
        assert!(tasks_of(&storage, &second).await.is_empty(), "{backend}");

        // The shared bucket is empty for every node.
        let check = storage
            .take_rate_budget_token(&tenant, &key, Utc::now())
            .await
            .unwrap();
        assert!(
            matches!(check, RateBudgetCheck::Deferred { .. }),
            "{backend}"
        );
        let unknown = storage
            .take_rate_budget_token(&tenant, "not-configured", Utc::now())
            .await
            .unwrap();
        assert_eq!(unknown, RateBudgetCheck::Unconfigured, "{backend}");

        // Reshaping keeps tokens clamped; listing and deleting work.
        let listed = storage.list_rate_budgets(&tenant).await.unwrap();
        assert_eq!(listed.len(), 1, "{backend}");
        assert!(storage.delete_rate_budget(&tenant, &key).await.unwrap());
        assert!(!storage.delete_rate_budget(&tenant, &key).await.unwrap());
    }
}

#[tokio::test]
async fn placement_policies_round_trip_and_sequence_placement_persists() {
    for (backend, storage) in backends().await {
        let tenant = TenantId::unchecked(unique("tenant"));
        assert!(
            storage
                .get_placement_policies(&tenant)
                .await
                .unwrap()
                .items
                .is_empty(),
            "{backend}"
        );
        let policies: PlacementPolicies = serde_json::from_value(json!({
            "items": [{"name": "eu", "match": {"sequence": "billing"},
                       "require": {"residency": "eu"}, "prefer": null}]
        }))
        .unwrap();
        storage
            .put_placement_policies(&tenant, &policies)
            .await
            .unwrap();
        assert_eq!(
            storage.get_placement_policies(&tenant).await.unwrap(),
            policies,
            "{backend}"
        );

        let mut seq = mk_sequence(vec![mk_step("a", &unique("ext.a"))]);
        seq.name = unique("persist");
        seq.tenant_id = tenant.clone();
        seq.placement = Some(Placement {
            residency: Some("eu".into()),
            priority_lane: Some(orch8_types::placement::PriorityLane::Premium),
            ..Placement::default()
        });
        storage.create_sequence(&seq).await.unwrap();
        let loaded = storage.get_sequence(seq.id).await.unwrap().unwrap();
        assert_eq!(loaded.placement, seq.placement, "{backend}");
    }
}
