//! Safety decisions must use every receipt, while response evidence stays bounded.
use chrono::{Duration, Utc};
use orch8_api::test_harness::spawn_test_server_on;
use orch8_storage::{
    ContinuityStore, InstanceStore, InvariantStore, SequenceStore,
    sqlite::SqliteStorage,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    continuity::{
        ContinuityExecution, ContinuityId, EffectId, EffectKind, EffectReceipt, EffectState,
        ExecutionEpoch, OwnershipState, RuntimeCapabilities, RuntimeId,
    },
    continuity_advanced::{InvariantId, InvariantRule, WorkflowInvariant},
    ids::BlockId,
    instance::InstanceState,
};
use serde_json::{Value, json};
use std::sync::Arc;

async fn fixture(compensate: bool) -> (Arc<SqliteStorage>, ContinuityExecution, RuntimeId) {
    let states: Vec<_> = (0..10_022)
        .map(|ordinal| {
            if ordinal < 10_000 || ordinal == 10_021 {
                EffectState::Committed
            } else {
                EffectState::Unknown
            }
        })
        .collect();
    fixture_with_states(compensate, &states).await
}

#[allow(clippy::too_many_lines)] // one fixture binds ownership/definition to caller-selected durable evidence
async fn fixture_with_states(
    compensate: bool,
    states: &[EffectState],
) -> (Arc<SqliteStorage>, ContinuityExecution, RuntimeId) {
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let mut sequence = SequenceBuilder::new("complete-evidence")
        .step("charge", "noop")
        .build();
    if compensate {
        let orch8_types::sequence::BlockDefinition::Step(step) = &mut sequence.blocks[0] else {
            unreachable!()
        };
        step.compensation = Some(
            serde_json::from_value(json!({"handler":"refund", "params":{"receipt_backed":true}}))
                .unwrap(),
        );
    }
    storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Paused)
        .build();
    storage.create_instance(&instance).await.unwrap();
    let now = Utc::now();
    let owner = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: instance.tenant_id.clone(),
        current_instance_id: instance.id,
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: now,
    };
    storage.create_continuity_execution(&owner).await.unwrap();
    for rule in [
        InvariantRule::NoUnknownEffects,
        InvariantRule::EffectAtMostOnce {
            kind: EffectKind::Worker,
        },
    ] {
        storage
            .create_workflow_invariant(&WorkflowInvariant {
                id: InvariantId::new(),
                tenant_id: owner.tenant_id.clone(),
                sequence_id: sequence.id,
                sequence_version: Some(sequence.version),
                name: "whole history".into(),
                rule,
                commit_guard: false,
                enabled: true,
                created_at: now,
            })
            .await
            .unwrap();
    }
    let destination = RuntimeId::new();
    for runtime in [owner.owner_runtime_id, destination] {
        let capabilities: RuntimeCapabilities = serde_json::from_value(json!({
            "runtime_id":runtime, "kind":"server", "trust":"registered",
            "observed_at":now, "expires_at":now + Duration::hours(1)
        }))
        .unwrap();
        storage
            .upsert_runtime_capabilities(&owner.tenant_id, &capabilities)
            .await
            .unwrap();
    }
    for (ordinal, state) in (0_u128..).zip(states.iter().copied()) {
        let receipt = EffectReceipt {
            id: EffectId::from_uuid(uuid::Uuid::from_u128(ordinal + 1)),
            tenant_id: owner.tenant_id.clone(),
            continuity_id: owner.continuity_id,
            epoch: owner.epoch,
            instance_id: instance.id,
            block_id: BlockId::new("charge"),
            kind: EffectKind::Worker,
            state,
            destination_fingerprint: "payments".into(),
            request_sha256: if ordinal == 10_021 {
                "request-10000".into()
            } else {
                format!("request-{ordinal:05}")
            },
            idempotency_key: None,
            provider_receipt_id: None,
            attempt: 0,
            created_at: now,
            updated_at: now,
        };
        storage.create_effect_receipt(&receipt).await.unwrap();
    }
    (storage, owner, destination)
}

async fn resolve_unknowns(storage: &SqliteStorage, owner: &ContinuityExecution) {
    for ordinal in 10_000..10_021_u128 {
        let id = EffectId::from_uuid(uuid::Uuid::from_u128(ordinal + 1));
        let mut receipt = storage
            .get_effect_receipt(&owner.tenant_id, id)
            .await
            .unwrap()
            .unwrap();
        receipt
            .transition(EffectState::Committed, Utc::now())
            .unwrap();
        assert!(
            storage
                .cas_effect_receipt(&owner.tenant_id, id, EffectState::Unknown, &receipt)
                .await
                .unwrap()
        );
    }
}

#[tokio::test]
async fn manual_invariants_cannot_certify_a_safe_prefix_of_a_larger_ledger() {
    let (storage, owner, _) = fixture(false).await;
    let server = spawn_test_server_on(storage.clone(), None).await;
    let response = reqwest::Client::new()
        .post(format!(
            "{}/api/v1/continuity/executions/{}/invariants/evaluate",
            server.base_url, owner.continuity_id
        ))
        .json(&json!({"tenant_id":owner.tenant_id}))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let results: Vec<Value> = response.json().await.unwrap();
    assert_eq!(results.len(), 2);
    assert!(
        results.iter().all(|r| r["status"] == "fail"),
        "late unsafe evidence was omitted"
    );
    assert!(
        results
            .iter()
            .all(|r| r["evidence_sha256"].as_array().unwrap().len() <= 17)
    );
    // Resolving late uncertainty and compensating the actual duplicate restores
    // a Pass without dropping or shortening the complete receipt history.
    resolve_unknowns(&storage, &owner).await;
    let id = EffectId::from_uuid(uuid::Uuid::from_u128(10_022));
    let mut duplicate = storage
        .get_effect_receipt(&owner.tenant_id, id)
        .await
        .unwrap()
        .unwrap();
    duplicate
        .transition(EffectState::Compensated, Utc::now())
        .unwrap();
    assert!(
        storage
            .cas_effect_receipt(&owner.tenant_id, id, EffectState::Committed, &duplicate)
            .await
            .unwrap()
    );
    let results: Vec<Value> = reqwest::Client::new()
        .post(format!(
            "{}/api/v1/continuity/executions/{}/invariants/evaluate",
            server.base_url, owner.continuity_id
        ))
        .json(&json!({"tenant_id":owner.tenant_id}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| r["status"] == "pass"));
    assert!(
        results
            .iter()
            .all(|r| r["evidence_sha256"].as_array().unwrap().is_empty())
    );
    let mut unknown = duplicate;
    unknown.id = EffectId::new();
    unknown.state = EffectState::Unknown;
    unknown.request_sha256 = "only-unknown-request".into();
    storage.create_effect_receipt(&unknown).await.unwrap();
    let results: Vec<Value> = reqwest::Client::new()
        .post(format!(
            "{}/api/v1/continuity/executions/{}/invariants/evaluate",
            server.base_url, owner.continuity_id
        ))
        .json(&json!({"tenant_id":owner.tenant_id}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let failures: Vec<_> = results.iter().filter(|r| r["status"] == "fail").collect();
    assert_eq!(failures.len(), 1);
    assert_eq!(
        failures[0]["evidence_sha256"],
        json!(["only-unknown-request"])
    );
}

#[tokio::test]
async fn handoff_preview_rejects_late_unknown_effects_and_reports_the_complete_count() {
    let (storage, owner, destination) = fixture(false).await;
    let server = spawn_test_server_on(storage, None).await;
    let response = reqwest::Client::new()
        .post(format!(
            "{}/api/v1/continuity/executions/{}/handoff-preview",
            server.base_url, owner.continuity_id
        ))
        .json(&json!({"tenant_id":owner.tenant_id,"destination_runtime_id":destination}))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let preview: Value = response.json().await.unwrap();
    assert_eq!(
        preview["compatible"], false,
        "late unknown effects must prevent handoff"
    );
    assert_eq!(preview["unresolved_effect_count"], 21);
    assert_eq!(preview["unresolved_effects_truncated"], true);
    assert_eq!(preview["unresolved_effects"].as_array().unwrap().len(), 16);
}

#[tokio::test]
async fn handoff_admission_rechecks_late_uncertainty_after_a_valid_preview() {
    let (storage, owner, destination) = fixture(false).await;
    resolve_unknowns(&storage, &owner).await;
    let server = spawn_test_server_on(storage.clone(), None).await;
    let client = reqwest::Client::new();
    let preview: Value = client
        .post(format!(
            "{}/api/v1/continuity/executions/{}/handoff-preview",
            server.base_url, owner.continuity_id
        ))
        .json(&json!({"tenant_id":owner.tenant_id,"destination_runtime_id":destination}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(preview["compatible"], true);
    assert_eq!(preview["unresolved_effect_count"], 0);
    let mut late = storage
        .get_effect_receipt(
            &owner.tenant_id,
            EffectId::from_uuid(uuid::Uuid::from_u128(10_001)),
        )
        .await
        .unwrap()
        .unwrap();
    late.id = EffectId::new();
    late.state = EffectState::Unknown;
    late.created_at = Utc::now();
    late.updated_at = late.created_at;
    storage.create_effect_receipt(&late).await.unwrap();
    let refused = client.post(format!("{}/api/v1/continuity/handoffs", server.base_url))
        .json(&json!({"tenant_id":owner.tenant_id,"continuity_id":owner.continuity_id,
            "destination_runtime_id":destination,"placement_decision_id":preview["placement_decision"]["id"],
            "preview_sha256":preview["preview_sha256"]}))
        .send().await.unwrap();
    assert_eq!(refused.status(), reqwest::StatusCode::CONFLICT);
    assert!(refused.text().await.unwrap().contains("unresolved effects"));
    assert_eq!(
        storage
            .get_continuity_execution(&owner.tenant_id, owner.continuity_id)
            .await
            .unwrap()
            .unwrap(),
        owner
    );
}

#[tokio::test]
async fn compensation_plan_includes_late_commits_and_late_unknown_hazards() {
    let (storage, owner, _) = fixture(true).await;
    let server = spawn_test_server_on(storage, None).await;
    let plan: Value = reqwest::Client::new()
        .post(format!(
            "{}/api/v1/continuity/executions/{}/compensations/preview",
            server.base_url, owner.continuity_id
        ))
        .json(&json!({"tenant_id":owner.tenant_id}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        plan["steps"].as_array().unwrap().len(),
        10_001,
        "a late committed effect was omitted from compensation"
    );
    assert_eq!(
        plan["hazards"].as_array().unwrap().len(),
        21,
        "late uncertainty must remain visible"
    );
    assert_eq!(
        plan["steps"][0]["effect_id"],
        EffectId::from_uuid(uuid::Uuid::from_u128(10_022)).to_string()
    );
    let created: Value = reqwest::Client::new()
        .post(format!(
            "{}/api/v1/continuity/executions/{}/compensations",
            server.base_url, owner.continuity_id
        ))
        .json(&json!({"tenant_id":owner.tenant_id}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(created["steps"].as_array().unwrap().len(), 10_001);
    assert_eq!(created["residual_effects"].as_array().unwrap().len(), 21);
    let saved: Value = reqwest::Client::new()
        .get(format!(
            "{}/api/v1/continuity/compensations/{}",
            server.base_url,
            created["id"].as_str().unwrap()
        ))
        .query(&[("tenant_id", owner.tenant_id.as_str())])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(saved["steps"], created["steps"]);
    assert_eq!(saved["hazards"], plan["hazards"]);
}

async fn post_value(
    server: &orch8_api::test_harness::BackendTestServer,
    path: &str,
    body: &Value,
) -> Value {
    reqwest::Client::new()
        .post(format!("{}{path}", server.base_url))
        .json(body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn complete_single_rollback(
    server: &orch8_api::test_harness::BackendTestServer,
    owner: &ContinuityExecution,
    run: &Value,
) -> Value {
    let id = run["id"].as_str().unwrap();
    let claimed = post_value(
        server,
        &format!("/api/v1/continuity/compensations/{id}/claim"),
        &json!({"tenant_id":owner.tenant_id,"worker_id":"compensator","lease_seconds":60}),
    )
    .await;
    assert_eq!(
        claimed["plan"]["effect_id"],
        run["steps"][0]["plan"]["effect_id"]
    );
    post_value(
        server,
        &format!(
            "/api/v1/continuity/compensations/{id}/steps/{}/complete",
            claimed["plan"]["effect_id"].as_str().unwrap()
        ),
        &json!({"tenant_id":owner.tenant_id,"worker_id":"compensator"}),
    )
    .await
}

#[tokio::test]
async fn unsettled_work_prevents_clean_completion_and_can_be_compensated_after_settlement() {
    let (storage, owner, _) = fixture_with_states(
        true,
        &[
            EffectState::Committed,
            EffectState::Dispatched,
            EffectState::Planned,
            EffectState::Prepared,
        ],
    )
    .await;
    let server = spawn_test_server_on(storage, None).await;
    let path = format!(
        "/api/v1/continuity/executions/{}/compensations",
        owner.continuity_id
    );
    let scope = json!({"tenant_id":owner.tenant_id});
    let run = post_value(&server, &path, &scope).await;
    assert_eq!(run["steps"].as_array().unwrap().len(), 1);
    assert_eq!(run["hazards"].as_array().unwrap().len(), 3);
    let finished = complete_single_rollback(&server, &owner, &run).await;
    assert_eq!(finished["state"], "completed_with_residuals");
    assert_eq!(finished["residual_effects"].as_array().unwrap().len(), 3);
    // The backend later confirms the in-flight outcome and abandons work that
    // never dispatched. A new plan must compensate only the newly committed ID.
    for (id, state) in [
        (2, EffectState::Committed),
        (3, EffectState::Abandoned),
        (4, EffectState::Abandoned),
    ] {
        post_value(
            &server,
            &format!(
                "/api/v1/continuity/effects/{}/resolve",
                EffectId::from_uuid(uuid::Uuid::from_u128(id))
            ),
            &json!({"tenant_id":owner.tenant_id,"state":state}),
        )
        .await;
    }
    let resumed = post_value(&server, &path, &scope).await;
    assert_eq!(resumed["steps"].as_array().unwrap().len(), 1);
    assert_eq!(
        resumed["steps"][0]["plan"]["effect_id"],
        EffectId::from_uuid(uuid::Uuid::from_u128(2)).to_string()
    );
    assert!(resumed["hazards"].as_array().unwrap().is_empty());
    let complete = complete_single_rollback(&server, &owner, &resumed).await;
    assert_eq!(complete["state"], "completed");
    assert!(complete["residual_effects"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn compensation_start_durably_fences_forward_execution() {
    let (storage, owner, _) = fixture_with_states(true, &[EffectState::Committed]).await;
    storage
        .update_instance_state(owner.current_instance_id, InstanceState::Waiting, None)
        .await
        .unwrap();
    let server = spawn_test_server_on(storage.clone(), None).await;
    let path = format!(
        "/api/v1/continuity/executions/{}/compensations",
        owner.continuity_id
    );
    let run = post_value(&server, &path, &json!({"tenant_id":owner.tenant_id})).await;
    assert_eq!(run["state"], "planned");
    let frozen = storage
        .get_continuity_execution(&owner.tenant_id, owner.continuity_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(serde_json::to_value(frozen.state).unwrap(), "compensating");
    let instance = storage
        .get_instance(owner.current_instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(instance.state, InstanceState::Paused);
    assert!(instance.claimed_at.is_none());
    assert!(instance.next_fire_at.is_none());
    assert!(
        !storage
            .cas_continuity_owner(
                &owner.tenant_id,
                owner.continuity_id,
                owner.epoch,
                owner.owner_runtime_id,
                &owner
            )
            .await
            .unwrap()
    );
    let completed = complete_single_rollback(&server, &owner, &run).await;
    assert_eq!(completed["state"], "completed");
    assert_eq!(
        serde_json::to_value(
            storage
                .get_continuity_execution(&owner.tenant_id, owner.continuity_id)
                .await
                .unwrap()
                .unwrap()
                .state
        )
        .unwrap(),
        "compensating"
    );
}

#[tokio::test]
async fn compensation_start_retries_source_completion_during_admission() {
    let (storage, owner, _) = fixture_with_states(true, &[EffectState::Committed]).await;
    let (wrapped, faults) = orch8_storage::testing::faulty::FaultStorage::wrap(storage.clone());
    let gate = faults.on("begin_compensation_run").nth(1).pause();
    let server = spawn_test_server_on(wrapped, None).await;
    let url = format!(
        "{}/api/v1/continuity/executions/{}/compensations",
        server.base_url, owner.continuity_id
    );
    let body = json!({"tenant_id":owner.tenant_id});
    let request = tokio::spawn(async move {
        reqwest::Client::new()
            .post(url)
            .json(&body)
            .send()
            .await
            .unwrap()
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered())
        .await
        .unwrap();
    storage
        .update_instance_state(owner.current_instance_id, InstanceState::Completed, None)
        .await
        .unwrap();
    gate.release();
    let response = tokio::time::timeout(std::time::Duration::from_secs(5), request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 201);
    let run: Value = response.json().await.unwrap();
    assert_eq!(run["steps"].as_array().unwrap().len(), 1);
    assert_eq!(
        storage
            .get_instance(owner.current_instance_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Completed
    );
    assert_eq!(
        storage
            .get_continuity_execution(&owner.tenant_id, owner.continuity_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        OwnershipState::Compensating
    );
}
