//! HTTP recovery never retires a local source or accepts unauthenticated backups.
use chrono::{Duration, Utc};
use orch8_api::test_harness::spawn_test_server_on;
use orch8_storage::{
    StorageBackend,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    checkpoint::Checkpoint,
    continuity::{ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState, RuntimeId},
    continuity_advanced::{
        LiveMigrationPlan, LiveMigrationRecord, LiveMigrationState, MigrationDisposition,
        MigrationPlanId, StateTransform,
    },
    execution::{BlockType, ExecutionNode, NodeState},
    ids::{BlockId, ExecutionNodeId, InstanceId},
    instance::InstanceState,
    sequence::{BlockDefinition, SequenceDefinition},
    signal::{Signal, SignalType},
};
use reqwest::StatusCode;
use serde_json::json;

async fn fixture(storage: &dyn StorageBackend) -> (LiveMigrationRecord, ContinuityExecution) {
    fixture_shape(storage, false, false).await
}

fn recovery_sequences(branching: bool, moved: bool) -> (SequenceDefinition, SequenceDefinition) {
    let mut source = SequenceBuilder::new("local-http")
        .step("gate", "human_review")
        .build();
    if branching {
        let gate = source.blocks.remove(0);
        let other = SequenceBuilder::new("local-http")
            .step("other", "noop")
            .build()
            .blocks
            .remove(0);
        source.blocks.push(BlockDefinition::Parallel(Box::new(
            orch8_types::sequence::ParallelDef {
                id: BlockId::new("fanout"),
                branches: vec![vec![gate], vec![other]],
            },
        )));
    }
    let mut target = source.clone();
    target.id = orch8_types::ids::SequenceId::new();
    target.version = 2;
    if moved && let BlockDefinition::Parallel(parallel) = &mut target.blocks[0] {
        parallel.branches.reverse();
    }
    (source, target)
}

async fn seed_recovery_nodes(storage: &dyn StorageBackend, instance: InstanceId, branching: bool) {
    let parent = ExecutionNodeId::new();
    let mut nodes = vec![(
        "gate",
        BlockType::Step,
        branching.then_some(parent),
        branching.then_some(0),
        NodeState::Waiting,
    )];
    if branching {
        nodes.insert(
            0,
            (
                "fanout",
                BlockType::Parallel,
                None,
                None,
                NodeState::Running,
            ),
        );
        nodes.push((
            "other",
            BlockType::Step,
            Some(parent),
            Some(1),
            NodeState::Pending,
        ));
    }
    for (block, block_type, parent_id, branch_index, state) in nodes {
        storage
            .create_execution_node(&ExecutionNode {
                id: if block == "fanout" {
                    parent
                } else {
                    ExecutionNodeId::new()
                },
                instance_id: instance,
                block_id: BlockId::new(block),
                parent_id,
                block_type,
                branch_index,
                state,
                started_at: Some(Utc::now()),
                completed_at: None,
            })
            .await
            .unwrap();
    }
}

async fn fixture_shape(
    storage: &dyn StorageBackend,
    branching: bool,
    moved: bool,
) -> (LiveMigrationRecord, ContinuityExecution) {
    let (source, target) = recovery_sequences(branching, moved);
    storage.create_sequence(&source).await.unwrap();
    storage.create_sequence(&target).await.unwrap();
    let mut instance = InstanceBuilder::for_sequence(&source)
        .state(InstanceState::Waiting)
        .data(json!({"version":"old","secret":"private-local-recovery"}))
        .build();
    instance.context.runtime.current_step = Some(BlockId::new("gate"));
    instance.next_fire_at = Some(Utc::now() + Duration::hours(2));
    storage.create_instance(&instance).await.unwrap();
    seed_recovery_nodes(storage, instance.id, branching).await;
    storage
        .enqueue_signal(&Signal {
            id: uuid::Uuid::new_v4(),
            instance_id: instance.id,
            signal_type: SignalType::Custom("queued-input".into()),
            payload: json!({"preserve":true}),
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        })
        .await
        .unwrap();
    let instance = storage.get_instance(instance.id).await.unwrap().unwrap();
    let owner = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: instance.tenant_id.clone(),
        current_instance_id: instance.id,
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    storage.create_continuity_execution(&owner).await.unwrap();
    storage.save_checkpoint(&Checkpoint {id:uuid::Uuid::new_v4(),instance_id:instance.id,checkpoint_data:json!({"context_snapshot":instance.context.data,"replacement":{"version":"new","secret":"private-local-recovery"}}),created_at:Utc::now()}).await.unwrap();
    let checkpoint = storage
        .get_latest_checkpoint(instance.id)
        .await
        .unwrap()
        .unwrap();
    let record = LiveMigrationRecord {
        plan: LiveMigrationPlan {
            id: MigrationPlanId::new(),
            tenant_id: instance.tenant_id.clone(),
            continuity_id: owner.continuity_id,
            from_sequence_id: source.id,
            from_version: source.version,
            to_sequence_id: target.id,
            to_version: 2,
            disposition: MigrationDisposition::Automatic,
            transforms: vec![StateTransform {
                version: 1,
                from_path: "replacement".into(),
                to_path: "context_snapshot".into(),
                transform: "copy".into(),
            }],
            finding_codes: vec![],
            rollback_capsule_required: true,
            created_at: Utc::now(),
        },
        expected_epoch: owner.epoch,
        source_checkpoint: checkpoint,
        source_context: serde_json::to_value(&instance.context).unwrap(),
        source_state: instance.state,
        rollback_capsule: None,
        state: LiveMigrationState::Planned,
        applied_epoch: None,
        rollback_expires_at: Utc::now() + Duration::hours(1),
        applied_at: None,
        rolled_back_at: None,
    };
    storage.save_live_migration(&record).await.unwrap();
    (record, owner)
}
fn url(base: &str, id: MigrationPlanId, action: &str) -> String {
    format!("{base}/continuity/migrations/{id}/{action}")
}
async fn apply(
    client: &reqwest::Client,
    base: &str,
    record: &LiveMigrationRecord,
) -> LiveMigrationRecord {
    let response = client
        .post(url(base, record.plan.id, "apply"))
        .json(&json!({"tenant_id":record.plan.tenant_id,"approved":true}))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    serde_json::from_str(&body).unwrap()
}

orch8_storage::backend_test!(artifacts = true, async fn signed_local_recovery_restores_timer_and_keeps_nodes_and_inputs(b) {
    let storage=b.storage.clone();let (record,owner)=fixture(storage.as_ref()).await;
    let before=storage.get_instance(owner.current_instance_id).await.unwrap().unwrap();let nodes=storage.get_execution_tree(before.id).await.unwrap();
    let server=spawn_test_server_on(storage.clone(),None).await;let base=server.v1_url();let client=reqwest::Client::new();
    let applied=apply(&client,&base,&record).await;
    assert_eq!(applied.state,LiveMigrationState::Applied);
    let current=storage.get_continuity_execution(&owner.tenant_id,owner.continuity_id).await.unwrap().unwrap();
    assert_eq!(current.state,OwnershipState::Owned);assert_eq!(current.current_instance_id,before.id);assert_eq!(current.epoch,owner.epoch.checked_next().unwrap());
    assert!(!storage.is_physical_instance_retired(before.id).await.unwrap());assert_eq!(b.raw.count("capsule_source_cuts","source_instance_id",&before.id.to_string()).await,0);
    let paused=storage.get_instance(before.id).await.unwrap().unwrap();assert_eq!(paused.state,InstanceState::Paused);assert!(paused.next_fire_at.is_none());assert_eq!(paused.context.data["version"],"new");
    let backup=applied.rollback_capsule.as_ref().unwrap();let bytes=storage.get_artifact(&backup.payload_artifact.key).await.unwrap().unwrap();let envelope:serde_json::Value=serde_json::from_slice(&bytes).unwrap();
    assert_eq!(envelope["manifest"]["identity"]["purpose"],"orch8.local-migration-recovery/v1");
    assert!(!std::str::from_utf8(&bytes).unwrap().contains("private-local-recovery"));
    assert!(serde_json::from_slice::<orch8_publisher::capsule::SignedCapsuleManifest>(&bytes).is_err(),"local recovery cannot be a portable ownership grant");
    assert_eq!(storage.get_execution_tree(before.id).await.unwrap()[0].id,nodes[0].id);
    assert_eq!(storage.get_pending_signals(before.id).await.unwrap().len(),1);
    let response=client.post(url(&base,record.plan.id,"rollback")).json(&json!({"tenant_id":owner.tenant_id})).send().await.unwrap();let status=response.status();let body=response.text().await.unwrap();assert_eq!(status,StatusCode::OK,"{body}");
    let restored=storage.get_instance(before.id).await.unwrap().unwrap();assert_eq!(restored.state,InstanceState::Waiting);assert_eq!(restored.next_fire_at,before.next_fire_at);assert_eq!(restored.context.data,before.context.data);assert_eq!(restored.sequence_id,before.sequence_id);
    assert_eq!(storage.get_execution_tree(before.id).await.unwrap()[0].id,nodes[0].id);assert_eq!(storage.get_pending_signals(before.id).await.unwrap().len(),1);
});

orch8_storage::backend_test!(artifacts = true, async fn local_apply_rejects_changed_planning_context_without_transfer_side_effects(b) {
    let storage=b.storage.clone();let (record,owner)=fixture(storage.as_ref()).await;
    let mut changed=storage.get_instance(owner.current_instance_id).await.unwrap().unwrap().context;changed.data["version"]=json!("newer-owner-data");storage.update_instance_context(owner.current_instance_id,&changed).await.unwrap();
    let server=spawn_test_server_on(storage.clone(),None).await;let response=reqwest::Client::new().post(url(&server.v1_url(),record.plan.id,"apply")).json(&json!({"tenant_id":owner.tenant_id,"approved":true})).send().await.unwrap();
    assert_eq!(response.status(),StatusCode::CONFLICT);assert_eq!(storage.get_instance(owner.current_instance_id).await.unwrap().unwrap().context.data,changed.data);
    assert_eq!(storage.get_continuity_execution(&owner.tenant_id,owner.continuity_id).await.unwrap().unwrap().epoch,owner.epoch);assert!(!storage.is_physical_instance_retired(owner.current_instance_id).await.unwrap());
});

orch8_storage::backend_test!(artifacts = true, async fn local_rollback_rejects_newer_paused_target_context(b) {
    let storage=b.storage.clone();let (record,owner)=fixture(storage.as_ref()).await;let server=spawn_test_server_on(storage.clone(),None).await;let base=server.v1_url();let client=reqwest::Client::new();let applied=apply(&client,&base,&record).await;
    let mut changed=storage.get_instance(owner.current_instance_id).await.unwrap().unwrap().context;changed.data["after_apply"]=json!("retain me");storage.update_instance_context(owner.current_instance_id,&changed).await.unwrap();
    let response=client.post(url(&base,record.plan.id,"rollback")).json(&json!({"tenant_id":owner.tenant_id})).send().await.unwrap();assert_eq!(response.status(),StatusCode::CONFLICT);
    assert_eq!(storage.get_instance(owner.current_instance_id).await.unwrap().unwrap().context.data,changed.data);assert_eq!(storage.get_live_migration(&owner.tenant_id,record.plan.id).await.unwrap().unwrap().state,LiveMigrationState::Applied);assert_eq!(storage.get_continuity_execution(&owner.tenant_id,owner.continuity_id).await.unwrap().unwrap().epoch,applied.applied_epoch.unwrap());
});

orch8_storage::backend_test!(artifacts = true, async fn local_rollback_does_not_trust_modified_signature_metadata(b) {
    let storage=b.storage.clone();let (record,owner)=fixture(storage.as_ref()).await;let server=spawn_test_server_on(storage.clone(),None).await;let base=server.v1_url();let client=reqwest::Client::new();let applied=apply(&client,&base,&record).await;
    let mut forged=applied.clone();forged.rollback_capsule.as_mut().unwrap().signature="A".repeat(88);assert!(storage.cas_live_migration(&owner.tenant_id,&applied,&forged).await.unwrap());
    let response=client.post(url(&base,record.plan.id,"rollback")).json(&json!({"tenant_id":owner.tenant_id})).send().await.unwrap();assert_eq!(response.status(),StatusCode::CONFLICT);
    assert_eq!(storage.get_instance(owner.current_instance_id).await.unwrap().unwrap().sequence_id,record.plan.to_sequence_id);
    assert_eq!(storage.get_continuity_execution(&owner.tenant_id,owner.continuity_id).await.unwrap().unwrap().epoch,applied.applied_epoch.unwrap());
});

orch8_storage::backend_test!(artifacts = true, async fn local_migration_refuses_rebinding_retained_nodes_to_different_branches(b) {
    let storage=b.storage.clone();let(record,owner)=fixture_shape(storage.as_ref(),true,true).await;
    let before=storage.get_execution_tree(owner.current_instance_id).await.unwrap();
    let server=spawn_test_server_on(storage.clone(),None).await;
    let response=reqwest::Client::new().post(url(&server.v1_url(),record.plan.id,"apply")).json(&json!({"tenant_id":owner.tenant_id,"approved":true})).send().await.unwrap();
    let status=response.status();let body=response.text().await.unwrap();assert_eq!(status,StatusCode::CONFLICT,"{body}");assert!(body.contains("branch"),"{body}");
    assert_eq!(storage.get_continuity_execution(&owner.tenant_id,owner.continuity_id).await.unwrap().unwrap(),owner);
    assert_eq!(storage.get_live_migration(&owner.tenant_id,record.plan.id).await.unwrap().unwrap().state,LiveMigrationState::Planned);
    assert_eq!(serde_json::to_value(storage.get_execution_tree(owner.current_instance_id).await.unwrap()).unwrap(),serde_json::to_value(before).unwrap());
    assert_eq!(storage.get_pending_signals(owner.current_instance_id).await.unwrap().len(),1);
    assert!(!storage.is_physical_instance_retired(owner.current_instance_id).await.unwrap());
});

orch8_storage::backend_test!(artifacts = true, async fn local_migration_keeps_compatible_parallel_wait_nodes_and_can_roll_back(b) {
    let storage=b.storage.clone();let(record,owner)=fixture_shape(storage.as_ref(),true,false).await;
    let before=storage.get_execution_tree(owner.current_instance_id).await.unwrap();let original=serde_json::to_value(&before).unwrap();
    let server=spawn_test_server_on(storage.clone(),None).await;let client=reqwest::Client::new();let base=server.v1_url();
    apply(&client,&base,&record).await;
    assert_eq!(serde_json::to_value(storage.get_execution_tree(owner.current_instance_id).await.unwrap()).unwrap(),original);
    let response=client.post(url(&base,record.plan.id,"rollback")).json(&json!({"tenant_id":owner.tenant_id})).send().await.unwrap();let status=response.status();let body=response.text().await.unwrap();assert_eq!(status,StatusCode::OK,"{body}");
    assert_eq!(serde_json::to_value(storage.get_execution_tree(owner.current_instance_id).await.unwrap()).unwrap(),original);
    assert_eq!(storage.get_pending_signals(owner.current_instance_id).await.unwrap().len(),1);
});

orch8_storage::backend_test!(artifacts = true, async fn migration_planning_reports_retained_graph_incompatibility(b) {
    let storage=b.storage.clone();let server=spawn_test_server_on(storage.clone(),None).await;let client=reqwest::Client::new();
    for moved in [false,true] {
        let(record,owner)=fixture_shape(storage.as_ref(),true,moved).await;
        let response=client.post(format!("{}/continuity/migrations/plan",server.v1_url())).json(&json!({"tenant_id":owner.tenant_id,"continuity_id":owner.continuity_id,"to_sequence_id":record.plan.to_sequence_id,"to_version":2})).send().await.unwrap();
        let status=response.status();let body=response.text().await.unwrap();assert_eq!(status,StatusCode::OK,"{body}");
        let plan:LiveMigrationPlan=serde_json::from_str(&body).unwrap();
        assert_eq!(plan.finding_codes.iter().any(|c|c=="INCOMPATIBLE:EXECUTION_GRAPH"),moved,"{body}");
        if moved {assert_eq!(plan.disposition,MigrationDisposition::Incompatible);}
        assert_eq!(storage.get_continuity_execution(&owner.tenant_id,owner.continuity_id).await.unwrap().unwrap(),owner,"planning must not change authority");
    }
});
