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
async fn compensation_request_replays_after_lost_response_and_completion() {
    let (storage, owner, _) = fixture_with_states(true, &[EffectState::Committed]).await;
    let server = spawn_test_server_on(storage, None).await;
    let path = format!(
        "/api/v1/continuity/executions/{}/compensations",
        owner.continuity_id
    );
    let body = json!({"tenant_id":owner.tenant_id,"request_id":uuid::Uuid::new_v4()});
    let client = reqwest::Client::new();
    let first = client
        .post(format!("{}{path}", server.base_url))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 201);
    drop(first); // The caller never obtains the committed run's ID.
    let retry = client
        .post(format!("{}{path}", server.base_url))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(retry.status(), 200);
    let recovered: Value = retry.json().await.unwrap();
    let finished = complete_single_rollback(&server, &owner, &recovered).await;
    assert_eq!(finished["state"], "completed");
    let retry = client
        .post(format!("{}{path}", server.base_url))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(retry.status(), 200);
    let replay: Value = retry.json().await.unwrap();
    assert_eq!(replay["id"], recovered["id"]);
    assert_eq!(replay["steps"], finished["steps"]);
    assert_eq!(replay["state"], "completed");
}

#[tokio::test]
async fn compensation_request_concurrent_retries_share_one_run() {
    let (storage, owner, _) = fixture_with_states(true, &[EffectState::Committed]).await;
    let server = spawn_test_server_on(storage, None).await;
    let path = format!(
        "{}/api/v1/continuity/executions/{}/compensations",
        server.base_url, owner.continuity_id
    );
    let body = json!({"tenant_id":owner.tenant_id,"request_id":uuid::Uuid::new_v4()});
    let client = reqwest::Client::new();
    let (first, second) = tokio::join!(
        client.post(&path).json(&body).send(),
        client.post(&path).json(&body).send()
    );
    let first = first.unwrap();
    let second = second.unwrap();
    let mut statuses = [first.status().as_u16(), second.status().as_u16()];
    statuses.sort_unstable();
    assert_eq!(statuses, [200, 201]);
    let first: Value = first.json().await.unwrap();
    let second: Value = second.json().await.unwrap();
    assert_eq!(first["id"], second["id"]);
    assert_eq!(first["steps"], second["steps"]);
    let distinct = client
        .post(&path)
        .json(&json!({"tenant_id":owner.tenant_id,"request_id":uuid::Uuid::new_v4()}))
        .send()
        .await
        .unwrap();
    assert_eq!(distinct.status(), 409);
}

#[tokio::test]
async fn compensation_request_recovers_after_committed_storage_error() {
    let (storage, owner, _) = fixture_with_states(true, &[EffectState::Committed]).await;
    let (wrapped, faults) = orch8_storage::testing::faulty::FaultStorage::wrap(storage);
    faults
        .on("begin_compensation_run")
        .nth(1)
        .fail_after_commit(orch8_types::error::StorageError::Connection(
            "creation acknowledgement lost".into(),
        ));
    let server = spawn_test_server_on(wrapped, None).await;
    let path = format!(
        "{}/api/v1/continuity/executions/{}/compensations",
        server.base_url, owner.continuity_id
    );
    let body = json!({"tenant_id":owner.tenant_id,"request_id":uuid::Uuid::new_v4()});
    let client = reqwest::Client::new();
    let response = client.post(&path).json(&body).send().await.unwrap();
    assert_eq!(response.status(), 200);
    let recovered: Value = response.json().await.unwrap();
    let replay: Value = client
        .post(&path)
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(recovered["id"], replay["id"]);
    assert_eq!(faults.calls("begin_compensation_run"), 1);
}

#[tokio::test]
async fn compensation_active_lookup_recovers_legacy_creation_and_hides_terminal_runs() {
    let (storage, owner, _) = fixture_with_states(true, &[EffectState::Committed]).await;
    let server = spawn_test_server_on(storage, None).await;
    let path = format!(
        "{}/api/v1/continuity/executions/{}/compensations",
        server.base_url, owner.continuity_id
    );
    let client = reqwest::Client::new();
    let scope = json!({"tenant_id":owner.tenant_id});
    let response = client.post(&path).json(&scope).send().await.unwrap();
    assert_eq!(response.status(), 201);
    drop(response);
    assert_eq!(
        client
            .post(&path)
            .json(&scope)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    let active: Value = client
        .get(format!("{path}/active"))
        .query(&[("tenant_id", owner.tenant_id.as_str())])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(active["state"], "planned");
    assert_eq!(
        client
            .get(format!("{path}/active"))
            .query(&[("tenant_id", "foreign-tenant")])
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    let completed = complete_single_rollback(&server, &owner, &active).await;
    assert_eq!(completed["state"], "completed");
    assert_eq!(
        client
            .get(format!("{path}/active"))
            .query(&[("tenant_id", owner.tenant_id.as_str())])
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
}

#[tokio::test]
async fn compensation_legacy_creation_recovers_a_committed_storage_error() {
    let (storage, owner, _) = fixture_with_states(true, &[EffectState::Committed]).await;
    let (wrapped, faults) = orch8_storage::testing::faulty::FaultStorage::wrap(storage);
    faults
        .on("begin_compensation_run")
        .nth(1)
        .fail_after_commit(orch8_types::error::StorageError::Connection(
            "legacy acknowledgement lost".into(),
        ));
    let server = spawn_test_server_on(wrapped, None).await;
    let path = format!(
        "{}/api/v1/continuity/executions/{}/compensations",
        server.base_url, owner.continuity_id
    );
    let client = reqwest::Client::new();
    let response = client
        .post(&path)
        .json(&json!({"tenant_id":owner.tenant_id}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let recovered: Value = response.json().await.unwrap();
    let active: Value = client
        .get(format!("{path}/active"))
        .query(&[("tenant_id", owner.tenant_id.as_str())])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(recovered["id"], active["id"]);
    assert_eq!(faults.calls("begin_compensation_run"), 1);
}

#[tokio::test]
async fn compensation_request_rejects_invalid_or_misspelled_intent_keys_without_writing() {
    let (storage, owner, _) = fixture_with_states(true, &[EffectState::Committed]).await;
    let server = spawn_test_server_on(storage.clone(), None).await;
    let path = format!(
        "{}/api/v1/continuity/executions/{}/compensations",
        server.base_url, owner.continuity_id
    );
    for body in [
        json!({"tenant_id":owner.tenant_id,"request_id":"not-a-uuid"}),
        json!({"tenant_id":owner.tenant_id,"requestId":uuid::Uuid::new_v4()}),
    ] {
        assert_eq!(
            reqwest::Client::new()
                .post(&path)
                .json(&body)
                .send()
                .await
                .unwrap()
                .status(),
            422
        );
    }
    assert!(
        storage
            .get_active_compensation_run(&owner.tenant_id, owner.continuity_id)
            .await
            .unwrap()
            .is_none()
    );
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
async fn compensation_request_scope_and_authorization_precede_replay() {
    let (storage, owner, _) = fixture_with_states(true, &[EffectState::Committed]).await;
    let original = storage
        .get_instance(owner.current_instance_id)
        .await
        .unwrap()
        .unwrap();
    let sequence = storage
        .get_sequence(original.sequence_id)
        .await
        .unwrap()
        .unwrap();
    let other_instance = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Paused)
        .build();
    storage.create_instance(&other_instance).await.unwrap();
    let other = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        current_instance_id: other_instance.id,
        ..owner.clone()
    };
    storage.create_continuity_execution(&other).await.unwrap();
    let mut effect = storage
        .get_effect_receipt(
            &owner.tenant_id,
            EffectId::from_uuid(uuid::Uuid::from_u128(1)),
        )
        .await
        .unwrap()
        .unwrap();
    effect.id = EffectId::new();
    effect.continuity_id = other.continuity_id;
    effect.instance_id = other_instance.id;
    storage.create_effect_receipt(&effect).await.unwrap();
    let server = spawn_test_server_on(storage, None).await;
    let path = |id: ContinuityId| {
        format!(
            "{}/api/v1/continuity/executions/{id}/compensations",
            server.base_url
        )
    };
    let body = json!({"tenant_id":owner.tenant_id,"request_id":uuid::Uuid::new_v4()});
    let client = reqwest::Client::new();
    let first: Value = client
        .post(path(owner.continuity_id))
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let second: Value = client
        .post(path(other.continuity_id))
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_ne!(first["id"], second["id"]);
    assert_eq!(
        client
            .post(path(owner.continuity_id))
            .header("X-Tenant-Id", "foreign-tenant")
            .json(&body)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        client
            .post(path(owner.continuity_id))
            .json(&json!({"tenant_id":"foreign-tenant","request_id":body["request_id"]}))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
}

#[tokio::test]
async fn compensation_request_replays_its_saved_plan_after_evidence_changes() {
    let (storage, owner, _) = fixture_with_states(true, &[EffectState::Committed]).await;
    let server = spawn_test_server_on(storage.clone(), None).await;
    let path = format!(
        "/api/v1/continuity/executions/{}/compensations",
        owner.continuity_id
    );
    let body = json!({"tenant_id":owner.tenant_id,"request_id":uuid::Uuid::new_v4()});
    let created = post_value(&server, &path, &body).await;
    let mut late = storage
        .get_effect_receipt(
            &owner.tenant_id,
            EffectId::from_uuid(uuid::Uuid::from_u128(1)),
        )
        .await
        .unwrap()
        .unwrap();
    late.id = EffectId::new();
    late.state = EffectState::Unknown;
    storage.create_effect_receipt(&late).await.unwrap();
    let replay = post_value(&server, &path, &body).await;
    assert_eq!(replay["id"], created["id"]);
    assert_eq!(replay["steps"], created["steps"]);
    assert_eq!(replay["hazards"], created["hazards"]);
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

#[tokio::test]
async fn compensation_version_exhaustion_rejects_claim_without_writing() {
    let (storage, owner, _) = fixture_with_states(true, &[EffectState::Committed]).await;
    let server = spawn_test_server_on(storage.clone(), None).await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!(
            "{}/api/v1/continuity/executions/{}/compensations/preview",
            server.base_url, owner.continuity_id
        ))
        .json(&json!({"tenant_id":owner.tenant_id}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let plan: Value = response.json().await.unwrap();
    let now = Utc::now();
    // Model a restored run at the last version supported by both SQL backends.
    let run:orch8_types::continuity_advanced::CompensationRunRecord=serde_json::from_value(json!({
        "id":uuid::Uuid::new_v4(),"tenant_id":owner.tenant_id,"continuity_id":owner.continuity_id,
        "source_instance_id":owner.current_instance_id,"state":"planned","version":i64::MAX,
        "steps":[{"plan":plan["steps"][0],"state":"pending","attempt":0,"lease_owner":null,
          "lease_expires_at":null,"provider_receipt_id":null,"error":null,"updated_at":now}],
        "hazards":[],"residual_effects":[],"created_at":now,"updated_at":now,"completed_at":null
    })).unwrap();
    assert!(storage.create_compensation_run(&run).await.unwrap());
    let response = client
        .post(format!(
            "{}/api/v1/continuity/compensations/{}/claim",
            server.base_url, run.id
        ))
        .json(&json!({"tenant_id":owner.tenant_id,"worker_id":"exhausted-version-worker"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("compensation version exhausted")
    );
    let unchanged = storage
        .get_compensation_run(&owner.tenant_id, run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(unchanged).unwrap(),
        serde_json::to_value(run).unwrap()
    );
}

// A legacy task can outlive collected continuity history. Retirement must stop
// propagation without throwing away a valid result from its original lease.
orch8_storage::backend_test!(async fn retired_legacy_completion_keeps_outcome_without_forward_propagation(b) {
    let sequence = SequenceBuilder::new("retired-completion").step("charge", "noop").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let source = InstanceBuilder::for_sequence(&sequence).state(InstanceState::Waiting)
        .data(json!({"private":"source-context-must-stay-frozen"})).build();
    b.storage.create_instance(&source).await.unwrap();
    let now = Utc::now();
    let task: orch8_types::worker::WorkerTask = serde_json::from_value(json!({
        "id":uuid::Uuid::new_v4(), "instance_id":source.id, "block_id":"charge", "handler_name":"noop",
        "params":{}, "context":{}, "attempt":0, "state":"pending", "created_at":now
    })).unwrap();
    b.storage.create_worker_task(&task).await.unwrap();
    let mut claimed = b.storage.claim_worker_tasks("noop", "legacy-worker", 1).await.unwrap();
    assert_eq!(claimed.len(), 1);
    let task = claimed.pop().unwrap();
    // Only compact identity evidence remains, as after destination-history GC.
    if b.kind.is_postgres() {
        sqlx::query("INSERT INTO physical_instance_retirements VALUES($1,$2,$3,$4,$5)")
            .bind(source.id.into_uuid()).bind(source.tenant_id.as_str()).bind(uuid::Uuid::new_v4())
            .bind(uuid::Uuid::new_v4()).bind(now).execute(b.pg_pool()).await.unwrap();
    } else {
        sqlx::query("INSERT INTO physical_instance_retirements VALUES(?1,?2,?3,?4,?5)")
            .bind(source.id.to_string()).bind(source.tenant_id.as_str()).bind(uuid::Uuid::new_v4().to_string())
            .bind(uuid::Uuid::new_v4().to_string()).bind(now.to_rfc3339()).execute(b.sqlite_pool()).await.unwrap();
    }
    let quarantined = b.storage.get_instance(source.id).await.unwrap().unwrap();
    assert_eq!(quarantined.state,InstanceState::Paused);
    assert!(quarantined.claimed_at.is_none());
    assert!(b.storage.get_continuity_execution_touching_instance(&source.tenant_id,source.id).await.unwrap().is_none());
    let server = spawn_test_server_on(b.storage.clone(), None).await;
    let client = reqwest::Client::new();
    let url = format!("{}/api/v1/workers/tasks/{}/complete", server.base_url, task.id);
    for output in [json!({"settled":"durable-original"}),json!({"settled":"retry-must-not-replace"})] {
        let response = client.post(&url).header("X-Tenant-Id",source.tenant_id.as_str())
            .json(&json!({"worker_id":"legacy-worker","claim_epoch":task.claim_epoch,"output":output})).send().await.unwrap();
        assert_eq!(response.status(),reqwest::StatusCode::OK,"{}",response.text().await.unwrap());
        let recorded = b.storage.get_worker_task(task.id).await.unwrap().unwrap();
        assert_eq!(recorded.state,orch8_types::worker::WorkerTaskState::Completed);
        assert_eq!(recorded.output,Some(json!({"settled":"durable-original"})));
        let frozen = b.storage.get_instance(source.id).await.unwrap().unwrap();
        assert_eq!(frozen.state,quarantined.state);
        assert_eq!(frozen.context.data,source.context.data);
        assert!(b.storage.get_block_output(source.id,&BlockId::new("charge")).await.unwrap().is_none());
    }
});

#[tokio::test]
async fn effect_resolution_rolls_back_when_provenance_cannot_commit() {
    let (storage, owner, _) = fixture_with_states(false, &[EffectState::Unknown]).await;
    let id = EffectId::from_uuid(uuid::Uuid::from_u128(1));
    let original = storage
        .get_effect_receipt(&owner.tenant_id, id)
        .await
        .unwrap()
        .unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_resolution_provenance BEFORE INSERT ON provenance_entries BEGIN SELECT RAISE(ABORT,'injected provenance failure'); END;")
        .execute(storage.pool()).await.unwrap();
    let server = spawn_test_server_on(storage.clone(), None).await;
    let path = format!("/api/v1/continuity/effects/{id}/resolve");
    let body = json!({"tenant_id":owner.tenant_id,"state":"committed","provider_receipt_id":"provider-confirmed"});
    let response = reqwest::Client::new()
        .post(format!("{}{path}", server.base_url))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success());
    assert_eq!(
        storage
            .get_effect_receipt(&owner.tenant_id, id)
            .await
            .unwrap(),
        Some(original),
        "failed provenance must not commit an unaudited operator resolution"
    );
    sqlx::raw_sql("DROP TRIGGER reject_resolution_provenance")
        .execute(storage.pool())
        .await
        .unwrap();
    let response = reqwest::Client::new()
        .post(format!("{}{path}", server.base_url))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let receipt = storage
        .get_effect_receipt(&owner.tenant_id, id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.state, EffectState::Committed);
    assert_eq!(
        receipt.provider_receipt_id.as_deref(),
        Some("provider-confirmed")
    );
    let entries = storage
        .list_provenance(&owner.tenant_id, owner.continuity_id, 10)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].kind, "effect_resolved");
    assert!(
        orch8_engine::continuity::verify_provenance_chain(&entries, Some(&entries[0].entry_sha256))
            .valid
    );
}

#[tokio::test]
async fn effect_resolution_handles_a_receipt_timestamp_from_an_ahead_replica() {
    let (storage, owner, _) = fixture_with_states(false, &[EffectState::Unknown]).await;
    let id = EffectId::from_uuid(uuid::Uuid::from_u128(1));
    let mut receipt = storage
        .get_effect_receipt(&owner.tenant_id, id)
        .await
        .unwrap()
        .unwrap();
    receipt.updated_at = Utc::now() + Duration::hours(1);
    assert!(
        storage
            .cas_effect_receipt(&owner.tenant_id, id, EffectState::Unknown, &receipt)
            .await
            .unwrap()
    );
    let server = spawn_test_server_on(storage.clone(), None).await;
    let response = reqwest::Client::new()
        .post(format!(
            "{}/api/v1/continuity/effects/{id}/resolve",
            server.base_url
        ))
        .json(&json!({"tenant_id":owner.tenant_id,"state":"committed"}))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let settled = storage
        .get_effect_receipt(&owner.tenant_id, id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled.state, EffectState::Committed);
    assert!(settled.updated_at >= receipt.updated_at);
    let chain = storage
        .list_provenance(&owner.tenant_id, owner.continuity_id, 10)
        .await
        .unwrap();
    assert_eq!(chain.len(), 1);
    assert!(
        orch8_engine::continuity::verify_provenance_chain(&chain, Some(&chain[0].entry_sha256))
            .valid
    );
}
