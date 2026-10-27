//! Local recovery preparation must not turn an in-place migration into a transfer.
use chrono::{Duration, Utc};
use orch8_storage::{
    StorageBackend,
    local_migration::{LocalMigrationSnapshotCapture as Capture, LocalMigrationSnapshotRequest},
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    checkpoint::Checkpoint,
    continuity::{ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState, RuntimeId},
    continuity_advanced::{
        LiveMigrationPlan, LiveMigrationRecord, LiveMigrationState, MigrationDisposition,
        MigrationPlanId,
    },
    ids::{SequenceId, TenantId},
    instance::InstanceState,
};
use serde_json::json;

async fn fixture(s: &dyn StorageBackend) -> LocalMigrationSnapshotRequest {
    let sequence = SequenceBuilder::new("local-migration")
        .step("wait", "noop")
        .build();
    s.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Waiting)
        .data(json!({"secret":"local-recovery-only"}))
        .build();
    s.create_instance(&instance).await.unwrap();
    let owner = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: instance.tenant_id.clone(),
        current_instance_id: instance.id,
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    s.create_continuity_execution(&owner).await.unwrap();
    let checkpoint = Checkpoint {
        id: uuid::Uuid::new_v4(),
        instance_id: instance.id,
        checkpoint_data: json!({"context_snapshot":instance.context.data}),
        created_at: Utc::now(),
    };
    s.save_checkpoint(&checkpoint).await.unwrap();
    let record = LiveMigrationRecord {
        plan: LiveMigrationPlan {
            id: MigrationPlanId::new(),
            tenant_id: instance.tenant_id.clone(),
            continuity_id: owner.continuity_id,
            from_sequence_id: sequence.id,
            from_version: sequence.version,
            to_sequence_id: SequenceId::new(),
            to_version: 2,
            disposition: MigrationDisposition::Automatic,
            transforms: vec![],
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
    s.save_live_migration(&record).await.unwrap();
    let current = s.get_instance(instance.id).await.unwrap().unwrap();
    LocalMigrationSnapshotRequest {
        execution: owner,
        migration_id: record.plan.id,
        expected_instance_updated_at: current.updated_at,
        mobile_access: None,
    }
}

orch8_storage::backend_test!(async fn local_recovery_preserves_authority_and_reserves_expiry_cleanup(b) {
    let request=fixture(b.storage.as_ref()).await;
    let before=b.storage.get_instance(request.execution.current_instance_id).await.unwrap().unwrap();
    let Capture::Ready(snapshot)=b.storage.prepare_local_migration_snapshot(&request).await.unwrap() else {panic!("safe local recovery snapshot was refused")};
    assert_eq!(snapshot.source.instance.context.data,json!({"secret":"local-recovery-only"}));
    assert_eq!(snapshot.source.checkpoint.checkpoint_data,json!({"context_snapshot":{"secret":"local-recovery-only"}}));
    assert_eq!(snapshot.record.source_context,serde_json::to_value(&before.context).unwrap());
    assert_eq!(serde_json::to_value(b.storage.get_instance(before.id).await.unwrap().unwrap()).unwrap(),serde_json::to_value(&before).unwrap());
    assert_eq!(b.storage.get_continuity_execution(&request.execution.tenant_id,request.execution.continuity_id).await.unwrap().unwrap(),request.execution);
    assert!(!b.storage.is_physical_instance_retired(before.id).await.unwrap());
    assert_eq!(b.raw.count("capsule_source_cuts","source_instance_id",&before.id.to_string()).await,0);
    let Capture::Ready(retry)=b.storage.prepare_local_migration_snapshot(&request).await.unwrap() else {panic!("snapshot retry was refused")};
    assert_eq!(retry.artifact_key,snapshot.artifact_key);
    assert_eq!(retry.fingerprint,snapshot.fingerprint,"unchanged retry must retain its commit proof");
    assert_eq!(b.raw.count("capsule_artifact_retirements","artifact_key",&snapshot.artifact_key).await,1);
    assert!(b.storage.claim_retired_capsule_artifacts(Utc::now(),Utc::now()+Duration::minutes(5),10).await.unwrap().is_empty());
    let due=snapshot.record.rollback_expires_at+Duration::seconds(1);
    assert_eq!(b.storage.claim_retired_capsule_artifacts(due,due+Duration::minutes(5),10).await.unwrap(),vec![snapshot.artifact_key.clone()]);
});

orch8_storage::backend_test!(async fn local_recovery_refuses_foreign_or_stale_proofs_without_reserving_artifacts(b) {
    let request=fixture(b.storage.as_ref()).await;
    for variant in 0..6 {
        let mut wrong=request.clone();
        match variant {0=>wrong.execution.tenant_id=TenantId::unchecked("foreign"),1=>wrong.execution.owner_runtime_id=RuntimeId::new(),2=>wrong.execution.epoch=wrong.execution.epoch.checked_next().unwrap(),3=>wrong.migration_id=MigrationPlanId::new(),4=>wrong.expected_instance_updated_at+=Duration::seconds(1),_=>wrong.execution.state=OwnershipState::Compensating}
        assert!(matches!(b.storage.prepare_local_migration_snapshot(&wrong).await.unwrap(),Capture::Refused(_)),"unsafe proof variant {variant} was accepted");
    }
    let key=format!("{}/{}",request.execution.current_instance_id,request.migration_id);
    assert_eq!(b.raw.count("capsule_artifact_retirements","artifact_key",&key).await,0);
});

orch8_storage::backend_test!(async fn local_recovery_refuses_in_flight_workers_without_freezing_waiting_source(b) {
    let request=fixture(b.storage.as_ref()).await;
    let task=serde_json::from_value(json!({"id":uuid::Uuid::new_v4(),"instance_id":request.execution.current_instance_id,"block_id":"wait","handler_name":"worker","state":"pending","attempt":0,"params":{},"context":{},"created_at":Utc::now()})).unwrap();b.storage.create_worker_task(&task).await.unwrap();
    assert!(matches!(b.storage.prepare_local_migration_snapshot(&request).await.unwrap(),Capture::Refused(orch8_storage::capsule_export::CapsuleCutRefusal::WorkerTasksInFlight(1))));
    assert_eq!(b.storage.get_instance(request.execution.current_instance_id).await.unwrap().unwrap().state,InstanceState::Waiting);
    assert_eq!(b.storage.get_continuity_execution(&request.execution.tenant_id,request.execution.continuity_id).await.unwrap().unwrap().state,OwnershipState::Owned);
});

orch8_storage::backend_test!(async fn local_recovery_never_bypasses_a_real_transfer_cut(b) {
    let request=fixture(b.storage.as_ref()).await;
    let cut=orch8_storage::capsule_export::CapsuleCutRequest {owner:request.execution.clone(),destination_runtime_id:Some(RuntimeId::new()),options_sha256:"a".repeat(64),mobile_access:None,now:Utc::now(),expires_at:Utc::now()+Duration::minutes(5)};
    assert!(matches!(b.storage.capture_capsule_cut(&cut).await.unwrap(),orch8_storage::capsule_export::CapsuleCutCapture::Ready(_)));
    assert!(matches!(b.storage.prepare_local_migration_snapshot(&request).await.unwrap(),Capture::Refused(_)));
});

// Native updated_at alone cannot prove that child evidence is unchanged.
orch8_storage::backend_test!(async fn local_commit_rejects_changed_children_and_target_without_advancing_owner(b) {
    use orch8_types::{execution::{BlockType, ExecutionNode, NodeState}, ids::{BlockId, ExecutionNodeId}, output::BlockOutput};
    for mutation in 0..5 {
        let storage=b.storage.as_ref();
        let request=fixture(storage).await;
        if mutation == 4 {
            let old=storage.get_live_migration(&request.execution.tenant_id,request.migration_id).await.unwrap().unwrap();
            let mut expiring=old.clone();expiring.rollback_expires_at=Utc::now()+Duration::seconds(2);
            assert!(storage.cas_live_migration(&request.execution.tenant_id,&old,&expiring).await.unwrap());
        }
        let Capture::Ready(snapshot)=storage.prepare_local_migration_snapshot(&request).await.unwrap() else {panic!("snapshot refused")};
        let mut target=snapshot.source.sequence.clone();
        target.id=snapshot.record.plan.to_sequence_id;
        target.version=snapshot.record.plan.to_version;
        storage.create_sequence(&target).await.unwrap();
        let id=request.execution.current_instance_id;
        match mutation {
            0=>storage.save_block_output(&BlockOutput {id:uuid::Uuid::new_v4(),instance_id:id,block_id:BlockId::new("wait"),output:json!({"late":true}),output_ref:None,output_size:13,attempt:1,created_at:Utc::now()}).await.unwrap(),
            1=>storage.create_execution_node(&ExecutionNode {id:ExecutionNodeId::new(),instance_id:id,block_id:BlockId::new("wait"),parent_id:None,block_type:BlockType::Step,branch_index:None,state:NodeState::Waiting,started_at:Some(Utc::now()),completed_at:None}).await.unwrap(),
            2=>{let mut changed=snapshot.record.clone();changed.source_context=json!({"changed":"after preparation"});assert!(storage.cas_live_migration(&request.execution.tenant_id,&snapshot.record,&changed).await.unwrap());},
            3=>{let mut changed=target.clone();changed.version+=1;storage.replace_sequence(target.id,&changed).await.unwrap();}
            _=>{let until_expired=snapshot.record.rollback_expires_at-Utc::now()+Duration::milliseconds(10);if let Ok(delay)=until_expired.to_std(){tokio::time::sleep(delay).await;}}
        }
        let mut next=request.execution.clone();next.epoch=next.epoch.checked_next().unwrap();next.updated_at=Utc::now();
        let mut applied=snapshot.record.clone();applied.state=LiveMigrationState::Applied;applied.applied_epoch=Some(next.epoch);applied.applied_at=Some(next.updated_at);
        let checkpoint=Checkpoint {id:uuid::Uuid::new_v4(),instance_id:id,checkpoint_data:json!({"context_snapshot":{"must_not_commit":true}}),created_at:next.updated_at};
        let committed=storage.commit_live_migration_transition(orch8_storage::LiveMigrationTransition {
            tenant_id:&request.execution.tenant_id,expected_record:&snapshot.record,next_record:&applied,expected_execution:&request.execution,next_execution:&next,
            expected_instance_state:snapshot.source.instance.state,expected_instance_updated_at:request.expected_instance_updated_at,
            expected_checkpoint_id:snapshot.source.checkpoint.id,expected_source_fingerprint:&snapshot.fingerprint,expected_target_sequence:&target,
            next_instance_state:InstanceState::Paused,next_fire_at:None,next_sequence_id:target.id,next_context:&snapshot.source.instance.context,checkpoint:&checkpoint,forbid_effects_epoch:None
        }).await.unwrap();
        assert!(!committed,"stale evidence mutation {mutation} was accepted");
        assert_eq!(storage.get_continuity_execution(&request.execution.tenant_id,request.execution.continuity_id).await.unwrap().unwrap(),request.execution);
        let current=storage.get_instance(id).await.unwrap().unwrap();
        assert_eq!(current.sequence_id,snapshot.source.instance.sequence_id);
        assert_eq!(current.updated_at,request.expected_instance_updated_at,"child changes must exercise fingerprint, not just native timestamp");
        assert_eq!(storage.get_latest_checkpoint(id).await.unwrap().unwrap().id,snapshot.source.checkpoint.id);
        assert!(!storage.is_physical_instance_retired(id).await.unwrap());
    }
});

#[cfg(feature = "postgres")]
mod definition_races {
    use super::*;
    use orch8_storage::{LiveMigrationTransition, local_migration::LocalMigrationSnapshot};
    use orch8_types::{error::StorageError, sequence::SequenceDefinition};
    use std::time::Duration as StdDuration;

    async fn apply_snapshot(
        storage: &dyn StorageBackend,
        request: &LocalMigrationSnapshotRequest,
        snapshot: &LocalMigrationSnapshot,
        target: &SequenceDefinition,
    ) -> Result<bool, StorageError> {
        let mut next = request.execution.clone();
        next.epoch = next.epoch.checked_next().unwrap();
        next.updated_at = Utc::now();
        let mut applied = snapshot.record.clone();
        applied.state = LiveMigrationState::Applied;
        applied.applied_epoch = Some(next.epoch);
        applied.applied_at = Some(next.updated_at);
        let checkpoint = Checkpoint {
            id: uuid::Uuid::new_v4(),
            instance_id: next.current_instance_id,
            checkpoint_data: snapshot.source.checkpoint.checkpoint_data.clone(),
            created_at: next.updated_at,
        };
        storage
            .commit_live_migration_transition(LiveMigrationTransition {
                tenant_id: &request.execution.tenant_id,
                expected_record: &snapshot.record,
                next_record: &applied,
                expected_execution: &request.execution,
                next_execution: &next,
                expected_instance_state: snapshot.source.instance.state,
                expected_instance_updated_at: request.expected_instance_updated_at,
                expected_checkpoint_id: snapshot.source.checkpoint.id,
                expected_source_fingerprint: &snapshot.fingerprint,
                expected_target_sequence: target,
                next_instance_state: InstanceState::Paused,
                next_fire_at: None,
                next_sequence_id: target.id,
                next_context: &snapshot.source.instance.context,
                checkpoint: &checkpoint,
                forbid_effects_epoch: None,
            })
            .await
    }

    #[tokio::test]
    async fn postgres_source_definition_stays_locked_after_commit_proof() {
        let Some(b) = orch8_storage::testing::backends::TestBackend::postgres().await else {
            return;
        };
        let request = fixture(b.storage.as_ref()).await;
        let Capture::Ready(snapshot) = b
            .storage
            .prepare_local_migration_snapshot(&request)
            .await
            .unwrap()
        else {
            panic!("snapshot refused")
        };
        let source_id = snapshot.source.sequence.id;
        let mut target = snapshot.source.sequence.clone();
        target.id = snapshot.record.plan.to_sequence_id;
        target.version = snapshot.record.plan.to_version;
        b.storage.create_sequence(&target).await.unwrap();
        let key = i64::from(uuid::Uuid::new_v4().as_fields().0 & 0x7fff_ffff);
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE FUNCTION pause_migration_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({key}); RETURN NEW; END $$;
             CREATE TRIGGER pause_migration_commit BEFORE UPDATE ON live_migrations FOR EACH ROW EXECUTE FUNCTION pause_migration_commit();"
        ))).execute(b.pg_pool()).await.unwrap();
        let mut gate = b.pg_pool().begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(key)
            .execute(&mut *gate)
            .await
            .unwrap();
        let storage = b.storage.clone();
        let migrating = tokio::spawn(async move {
            apply_snapshot(storage.as_ref(), &request, &snapshot, &target).await
        });
        let pid = tokio::time::timeout(StdDuration::from_secs(10), async {
            loop {
                let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_locks WHERE locktype='advisory' AND objid=$1::bigint::oid AND NOT granted LIMIT 1")
                    .bind(key).fetch_optional(b.pg_pool()).await.unwrap();
                if let Some(pid) = pid { break pid; }
                tokio::time::sleep(StdDuration::from_millis(10)).await;
            }
        }).await;
        let Ok(pid) = pid else {
            gate.rollback().await.unwrap();
            migrating.abort();
            panic!("migration never reached its post-proof barrier");
        };
        let pool = b.pg_pool().clone();
        let editing = tokio::spawn(async move {
            sqlx::query("UPDATE sequences SET definition=jsonb_set(definition,'{blocks,0,handler}','\"new-handler\"'::jsonb) WHERE id=$1")
                .bind(source_id.into_uuid()).execute(&pool).await
        });
        let waited = tokio::time::timeout(StdDuration::from_secs(10), async {
            loop {
                let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE 'UPDATE sequences%')")
                    .bind(pid).fetch_one(b.pg_pool()).await.unwrap();
                if blocked { break true; }
                if editing.is_finished() { break false; }
                tokio::time::sleep(StdDuration::from_millis(10)).await;
            }
        }).await.unwrap_or(false);
        gate.rollback().await.unwrap();
        let committed = tokio::time::timeout(StdDuration::from_secs(10), migrating)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let edited = tokio::time::timeout(StdDuration::from_secs(10), editing)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(committed);
        assert_eq!(
            edited.rows_affected(),
            1,
            "definition edits remain usable after the migration commits"
        );
        assert!(
            waited,
            "source definition changed after the migration's final source proof"
        );
    }
}
