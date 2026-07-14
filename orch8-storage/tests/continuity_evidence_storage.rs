use chrono::{Duration, Utc};
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::{AttentionStore, ContinuityStore, EvaluationStore, InvariantStore};
use orch8_types::continuity::{
    ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState, RuntimeId,
};
use orch8_types::continuity_advanced::{
    AttentionState, AttentionTask, AttentionTaskId, BudgetReservation, BudgetReservationId,
    EvaluationId, EvaluationScore, EvidenceStatus, InvariantId, InvariantResult, InvariantResultId,
    InvariantRule, ReservationState, WorkflowInvariant,
};
use orch8_types::ids::{InstanceId, SequenceId, TenantId};
use orch8_types::instance::BudgetUsage;

fn tenant() -> TenantId {
    TenantId::new("tenant-a").unwrap()
}

async fn setup() -> (SqliteStorage, TenantId, ContinuityId, SequenceId) {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let tenant = tenant();
    let continuity_id = ContinuityId::new();
    storage
        .create_continuity_execution(&ContinuityExecution {
            continuity_id,
            tenant_id: tenant.clone(),
            current_instance_id: InstanceId::new(),
            owner_runtime_id: RuntimeId::new(),
            epoch: ExecutionEpoch::initial(),
            state: OwnershipState::Owned,
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
    (storage, tenant, continuity_id, SequenceId::new())
}

#[tokio::test]
async fn invariant_and_evaluation_evidence_is_deduplicated() {
    let (storage, tenant, continuity_id, sequence_id) = setup().await;
    let now = Utc::now();
    let invariant = WorkflowInvariant {
        id: InvariantId::new(),
        tenant_id: tenant.clone(),
        sequence_id,
        sequence_version: Some(1),
        name: "effect once".into(),
        rule: InvariantRule::NoUnknownEffects,
        commit_guard: false,
        enabled: true,
        created_at: now,
    };
    storage.create_workflow_invariant(&invariant).await.unwrap();
    assert_eq!(
        storage
            .list_workflow_invariants(&tenant, sequence_id, 1, 10)
            .await
            .unwrap(),
        std::slice::from_ref(&invariant)
    );
    let result = InvariantResult {
        id: InvariantResultId::new(),
        invariant_id: invariant.id,
        continuity_id,
        epoch: ExecutionEpoch::initial(),
        status: EvidenceStatus::Fail,
        dedupe_key: "same-violation".into(),
        evidence_sha256: vec!["a".repeat(64)],
        summary: "unknown effect".into(),
        evaluated_at: now,
    };
    assert!(
        storage
            .append_invariant_result(&tenant, &result)
            .await
            .unwrap()
    );
    let mut replay = result.clone();
    replay.id = InvariantResultId::new();
    assert!(
        !storage
            .append_invariant_result(&tenant, &replay)
            .await
            .unwrap()
    );
    assert_eq!(
        storage
            .list_invariant_results(&tenant, continuity_id, 10)
            .await
            .unwrap(),
        [result]
    );

    let score = EvaluationScore {
        id: EvaluationId::new(),
        tenant_id: tenant.clone(),
        continuity_id,
        evaluator: "quality".into(),
        score_millipoints: 900,
        sample_size: 1,
        deferred: true,
        dedupe_key: "eval-one".into(),
        evidence_sha256: "b".repeat(64),
        created_at: now,
    };
    assert!(storage.append_evaluation_score(&score).await.unwrap());
    assert!(!storage.append_evaluation_score(&score).await.unwrap());
    assert_eq!(
        storage
            .list_evaluation_scores(&tenant, continuity_id, 10)
            .await
            .unwrap(),
        [score]
    );
}

#[tokio::test]
async fn attention_claim_is_single_winner() {
    let (storage, tenant, continuity_id, _) = setup().await;
    let now = Utc::now();
    let task = AttentionTask {
        id: AttentionTaskId::new(),
        tenant_id: tenant.clone(),
        continuity_id,
        required_skills: vec!["fraud".into()],
        classification: orch8_types::continuity::DataClassification::Confidential,
        allowed_regions: vec!["br".into()],
        priority: 2,
        deadline: now + Duration::minutes(5),
        estimated_attention_units: 2,
        state: AttentionState::Pending,
        assignee: None,
        lease_expires_at: None,
    };
    storage.create_attention_task(&task).await.unwrap();
    let mut assigned = task.clone();
    assigned.state = AttentionState::Assigned;
    assigned.assignee = Some("reviewer-a".into());
    assigned.lease_expires_at = Some(now + Duration::minutes(1));
    assert!(
        storage
            .claim_attention_task(&tenant, &task, &assigned, now)
            .await
            .unwrap()
    );
    assert!(
        !storage
            .claim_attention_task(&tenant, &task, &assigned, now)
            .await
            .unwrap()
    );
    assert_eq!(
        storage.get_attention_task(&tenant, task.id).await.unwrap(),
        Some(assigned)
    );
}

#[tokio::test]
async fn budget_settlement_is_single_winner_and_actual_usage_remains_cumulative() {
    let (storage, tenant, continuity_id, _) = setup().await;
    let now = Utc::now();
    let budget = orch8_types::instance::Budget {
        max_cost_microunits: Some(100),
        ..Default::default()
    };
    let mut reservation = BudgetReservation {
        id: BudgetReservationId::new(),
        tenant_id: tenant.clone(),
        continuity_id,
        epoch: ExecutionEpoch::initial(),
        requested: BudgetUsage {
            cost_microunits: 70,
            attention_units: 2,
            ..BudgetUsage::default()
        },
        actual: None,
        estimation_version: "prices-v1".into(),
        state: ReservationState::Reserved,
        created_at: now,
    };
    assert!(storage.reserve_budget(&reservation, &budget).await.unwrap());
    let first_id = reservation.id;
    let mut competing = reservation.clone();
    competing.id = BudgetReservationId::new();
    competing.requested.cost_microunits = 31;
    assert!(
        !storage.reserve_budget(&competing, &budget).await.unwrap(),
        "active reservations must be summed under one database write lock"
    );
    reservation.state = ReservationState::Reconciled;
    reservation.actual = Some(BudgetUsage {
        cost_microunits: 60,
        attention_units: 2,
        ..BudgetUsage::default()
    });
    assert!(
        storage
            .settle_budget_reservation(&reservation)
            .await
            .unwrap()
    );
    assert!(
        !storage
            .settle_budget_reservation(&reservation)
            .await
            .unwrap()
    );
    assert_eq!(
        storage
            .get_budget_reservation(&tenant, first_id)
            .await
            .unwrap(),
        Some(reservation.clone())
    );
    competing.id = BudgetReservationId::new();
    competing.requested.cost_microunits = 41;
    assert!(
        !storage.reserve_budget(&competing, &budget).await.unwrap(),
        "reconciled actual usage must remain part of the cumulative budget"
    );
    competing.id = BudgetReservationId::new();
    competing.requested.cost_microunits = 40;
    assert!(storage.reserve_budget(&competing, &budget).await.unwrap());
    assert_eq!(
        storage
            .list_budget_reservations(&tenant, continuity_id, 10)
            .await
            .unwrap()
            .len(),
        2
    );
}
