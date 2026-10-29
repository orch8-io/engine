//! Compensation freezes forward authority without destroying worker outcomes.
use chrono::{Duration, Timelike, Utc};
use orch8_storage::{
    CompensationStart, StorageBackend,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    continuity::{
        ContinuityExecution, ContinuityId, EffectId, EffectKind, EffectReceipt, EffectState,
        ExecutionEpoch, OwnershipState, RuntimeId,
    },
    continuity_advanced::CompensationRunRecord,
    ids::BlockId,
    instance::{InstanceState, TaskInstance},
    worker::{WorkerClaim, WorkerTask, WorkerTaskState},
};
use serde_json::json;

async fn fixture(
    storage: &dyn StorageBackend,
) -> (
    ContinuityExecution,
    TaskInstance,
    EffectReceipt,
    CompensationRunRecord,
) {
    let sequence = SequenceBuilder::new("compensation-start")
        .step("charge", "noop")
        .build();
    storage.create_sequence(&sequence).await.unwrap();
    let mut instance = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Waiting)
        .build();
    // Exercise PostgreSQL timestamp precision even on clocks aligned to microseconds.
    instance.updated_at = instance.updated_at.with_nanosecond(123_456_789).unwrap();
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
    let effect = EffectReceipt {
        id: EffectId::new(),
        tenant_id: owner.tenant_id.clone(),
        continuity_id: owner.continuity_id,
        epoch: owner.epoch,
        instance_id: instance.id,
        block_id: BlockId::new("charge"),
        kind: EffectKind::Worker,
        state: EffectState::Committed,
        destination_fingerprint: "provider".into(),
        idempotency_key: None,
        request_sha256: "a".repeat(64),
        provider_receipt_id: None,
        attempt: 0,
        created_at: now,
        updated_at: now,
    };
    storage.create_effect_receipt(&effect).await.unwrap();
    let run:CompensationRunRecord=serde_json::from_value(json!({"id":uuid::Uuid::new_v4(),"tenant_id":owner.tenant_id,"continuity_id":owner.continuity_id,
        "source_instance_id":instance.id,"state":"planned","version":0,"steps":[{"plan":{"effect_id":effect.id,"effect_block_id":"charge","handler":"refund",
        "params":{"secret":"compensation-only"},"idempotency_key":"refund:charge","verification":"handler_result"},"state":"pending","attempt":0,"lease_owner":null,
        "lease_expires_at":null,"provider_receipt_id":null,"error":null,"updated_at":now}],"hazards":[],"residual_effects":[],"created_at":now,"updated_at":now,"completed_at":null})).unwrap();
    (owner, instance, effect, run)
}

fn task(instance: &TaskInstance, epoch: ExecutionEpoch) -> WorkerTask {
    WorkerTask {
        id: uuid::Uuid::now_v7(),
        instance_id: instance.id,
        block_id: BlockId::new("other"),
        handler_name: "worker_charge".into(),
        queue_name: None,
        requirements: orch8_types::continuity::CapsuleRequirements::default(),
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
        continuity_epoch: Some(epoch.get()),
        lease_secs: None,
        carries_credentials: false,
        claimed_runtime_kind: None,
    }
}

orch8_storage::backend_test!(async fn compensation_start_is_atomic_and_fences_all_poll_paths(b) {
    let (owner,instance,_effect,run)=fixture(b.storage.as_ref()).await;
    let mut pending=task(&instance,owner.epoch);pending.queue_name=Some("gated".into());b.storage.create_worker_task(&pending).await.unwrap();
    let effects=b.storage.collect_compensation_effects(&owner.tenant_id,owner.continuity_id).await.unwrap();
    assert!(b.storage.begin_compensation_run(CompensationStart {run:&run,execution:&owner,instance:&instance,effects:&effects}).await.unwrap());
    let frozen=b.storage.get_continuity_execution(&owner.tenant_id,owner.continuity_id).await.unwrap().unwrap();
    assert_eq!(frozen.state,OwnershipState::Compensating);assert_eq!(frozen.epoch,owner.epoch);
    assert_eq!(b.storage.get_instance(instance.id).await.unwrap().unwrap().state,InstanceState::Paused);
    assert!(!b.storage.cas_continuity_owner(&owner.tenant_id,owner.continuity_id,owner.epoch,owner.owner_runtime_id,&owner).await.unwrap());
    let stored=b.storage.get_compensation_run(&run.tenant_id,run.id).await.unwrap().unwrap();
    assert_eq!(stored.steps[0].plan.params,json!({"secret":"compensation-only"}));
    b.storage.update_instance_state(instance.id,InstanceState::Waiting,None).await.unwrap();
    assert!(b.storage.claim_worker_tasks("worker_charge","worker",1).await.unwrap().is_empty());
    assert!(b.storage.claim_worker_tasks_for_tenant("worker_charge","worker",&owner.tenant_id,1).await.unwrap().is_empty());
    let now=Utc::now();let capabilities=serde_json::from_value(json!({"runtime_id":RuntimeId::new(),"kind":"server","trust":"registered","observed_at":now,"expires_at":now+Duration::minutes(5)})).unwrap();
    for tenant in [None,Some(&owner.tenant_id)] {
        assert!(b.storage.claim_worker_tasks_matching("worker_charge","worker",tenant,None,&capabilities,1).await.unwrap().is_empty());
    }
    assert!(b.storage.claim_worker_tasks_from_queue("gated","worker_charge","worker",1).await.unwrap().is_empty());
    assert!(b.storage.claim_worker_tasks_from_queue_for_tenant("gated","worker_charge","worker",&owner.tenant_id,1).await.unwrap().is_empty());
    assert_eq!(b.storage.get_worker_task(pending.id).await.unwrap().unwrap().state,WorkerTaskState::Pending);
    let (control_owner,control_instance,_,_)=fixture(b.storage.as_ref()).await;
    let mut control=task(&control_instance,control_owner.epoch);control.queue_name=Some("gated".into());
    b.storage.create_worker_task(&control).await.unwrap();
    let claimed=b.storage.claim_worker_tasks_from_queue("gated","worker_charge","worker",1).await.unwrap();
    assert_eq!(claimed.len(),1);assert_eq!(claimed[0].id,control.id);
    control.id=uuid::Uuid::now_v7();control.block_id=BlockId::new("tenant-control");
    b.storage.create_worker_task(&control).await.unwrap();
    let claimed=b.storage.claim_worker_tasks_from_queue_for_tenant("gated","worker_charge","worker",&owner.tenant_id,1).await.unwrap();
    assert_eq!(claimed.len(),1);assert_eq!(claimed[0].id,control.id);

});

orch8_storage::backend_test!(async fn compensation_start_rejects_late_evidence_without_partial_freeze(b) {
    let (owner,instance,mut effect,run)=fixture(b.storage.as_ref()).await;
    let effects=b.storage.collect_compensation_effects(&owner.tenant_id,owner.continuity_id).await.unwrap();
    effect.id=EffectId::new();b.storage.create_effect_receipt(&effect).await.unwrap();
    assert!(!b.storage.begin_compensation_run(CompensationStart {run:&run,execution:&owner,instance:&instance,effects:&effects}).await.unwrap());
    assert!(b.storage.get_compensation_run(&owner.tenant_id,run.id).await.unwrap().is_none());
    assert_eq!(b.storage.get_continuity_execution(&owner.tenant_id,owner.continuity_id).await.unwrap().unwrap(),owner);
    assert_eq!(b.storage.get_instance(instance.id).await.unwrap().unwrap().state,InstanceState::Waiting);
});

orch8_storage::backend_test!(async fn compensation_fence_preserves_inflight_worker_outcome_authority(b) {
    let (owner,instance,_effect,run)=fixture(b.storage.as_ref()).await;
    let task=task(&instance,owner.epoch);b.storage.create_worker_task(&task).await.unwrap();
    let claimed=b.storage.claim_worker_tasks("worker_charge","already-running",1).await.unwrap().pop().unwrap();
    let effects=b.storage.collect_compensation_effects(&owner.tenant_id,owner.continuity_id).await.unwrap();
    assert!(b.storage.begin_compensation_run(CompensationStart {run:&run,execution:&owner,instance:&instance,effects:&effects}).await.unwrap());
    assert!(orch8_storage::fencing::worker_task_ownership_current(b.storage.as_ref(),&owner.tenant_id,&claimed).await.unwrap());
    let claim=WorkerClaim::new("already-running",claimed.claim_epoch);
    assert!(b.storage.heartbeat_worker_task(claimed.id,&claim).await.unwrap());
    assert!(b.storage.complete_worker_task(claimed.id,&claim,&json!({"provider_receipt":"late"})).await.unwrap());
    assert_eq!(b.storage.get_worker_task(claimed.id).await.unwrap().unwrap().state,WorkerTaskState::Completed);
    assert_eq!(b.storage.get_instance(instance.id).await.unwrap().unwrap().state,InstanceState::Paused);
});

orch8_storage::backend_test!(async fn concurrent_compensation_start_has_one_winner(b) {
    let (owner,instance,_effect,run)=fixture(b.storage.as_ref()).await;
    let effects=b.storage.collect_compensation_effects(&owner.tenant_id,owner.continuity_id).await.unwrap();
    let mut other=run.clone();other.id=orch8_types::continuity_advanced::CompensationRunId::new();
    let start=CompensationStart {run:&run,execution:&owner,instance:&instance,effects:&effects};
    let (first,second)=tokio::join!(b.storage.begin_compensation_run(start),b.storage.begin_compensation_run(CompensationStart {run:&other,..start}));
    assert_ne!(first.unwrap(),second.unwrap());
    let persisted=[b.storage.get_compensation_run(&owner.tenant_id,run.id).await.unwrap(),b.storage.get_compensation_run(&owner.tenant_id,other.id).await.unwrap()];
    assert_eq!(persisted.iter().filter(|r|r.is_some()).count(),1);
    assert_eq!(b.storage.get_continuity_execution(&owner.tenant_id,owner.continuity_id).await.unwrap().unwrap().state,OwnershipState::Compensating);
});

orch8_storage::backend_test!(async fn failed_compensation_freeze_rolls_back_run_admission(b) {
    let (owner,instance,_effect,run)=fixture(b.storage.as_ref()).await;
    let effects=b.storage.collect_compensation_effects(&owner.tenant_id,owner.continuity_id).await.unwrap();
    if b.kind.is_postgres() {
        sqlx::raw_sql("CREATE FUNCTION reject_compensation_freeze() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state='compensating' THEN RAISE EXCEPTION 'injected compensation freeze failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_compensation_freeze BEFORE UPDATE ON continuity_executions FOR EACH ROW EXECUTE FUNCTION reject_compensation_freeze();").execute(b.pg_pool()).await.unwrap();
    } else {
        sqlx::raw_sql("CREATE TRIGGER reject_compensation_freeze BEFORE UPDATE ON continuity_executions WHEN NEW.state='compensating' BEGIN SELECT RAISE(ABORT,'injected compensation freeze failure'); END;").execute(b.sqlite_pool()).await.unwrap();
    }
    assert!(b.storage.begin_compensation_run(CompensationStart {run:&run,execution:&owner,instance:&instance,effects:&effects}).await.is_err());
    assert!(b.storage.get_compensation_run(&owner.tenant_id,run.id).await.unwrap().is_none());
    assert_eq!(b.storage.get_continuity_execution(&owner.tenant_id,owner.continuity_id).await.unwrap().unwrap(),owner);
    assert_eq!(b.storage.get_instance(instance.id).await.unwrap().unwrap().state,InstanceState::Waiting);
});

orch8_storage::backend_test!(async fn compensation_fence_rejects_prepared_forward_admission(b) {
    let (owner,instance,mut effect,mut run)=fixture(b.storage.as_ref()).await;
    effect.id=EffectId::new();effect.state=EffectState::Prepared;b.storage.create_effect_receipt(&effect).await.unwrap();
    let hazard=format!("PENDING_EFFECT_MUST_BE_SETTLED:{}:{}",effect.block_id,effect.id);
    run.hazards.push(hazard.clone());run.residual_effects.push(hazard);
    let effects=b.storage.collect_compensation_effects(&owner.tenant_id,owner.continuity_id).await.unwrap();
    assert!(b.storage.begin_compensation_run(CompensationStart {run:&run,execution:&owner,instance:&instance,effects:&effects}).await.unwrap());
    effect.state=EffectState::Dispatched;effect.updated_at=Utc::now();
    let outcome=b.storage.dispatch_effect_receipt_owned(&owner.tenant_id,&effect,&orch8_storage::EffectDispatchFence {
        instance_id:instance.id,epoch:owner.epoch,owner_runtime_id:owner.owner_runtime_id,claim_token:None},false).await.unwrap();
    assert_eq!(outcome,orch8_types::continuity::EffectDispatchOutcome::Stale);
    assert_eq!(b.storage.get_effect_receipt(&owner.tenant_id,effect.id).await.unwrap().unwrap().state,EffectState::Prepared);
});

orch8_storage::backend_test!(async fn stale_live_migration_cannot_reopen_compensating_source(b) {
    let (owner,instance,_effect,run)=fixture(b.storage.as_ref()).await;
    let now=Utc::now();
    let checkpoint=orch8_types::checkpoint::Checkpoint {id:uuid::Uuid::now_v7(),instance_id:instance.id,checkpoint_data:json!({}),created_at:now};
    let target=SequenceBuilder::new(owner.tenant_id.as_str()).step("charge","noop").build();
    b.storage.create_sequence(&target).await.unwrap();
    let record:orch8_types::continuity_advanced::LiveMigrationRecord=serde_json::from_value(json!({
        "plan":{"id":uuid::Uuid::new_v4(),"tenant_id":owner.tenant_id,"continuity_id":owner.continuity_id,"from_sequence_id":instance.sequence_id,
            "from_version":1,"to_sequence_id":target.id,"to_version":1,"disposition":"automatic","transforms":[],"finding_codes":[],"rollback_capsule_required":false,"created_at":now},
        "expected_epoch":owner.epoch,"source_checkpoint":checkpoint,"source_context":instance.context,"source_state":"waiting","rollback_capsule":null,
        "state":"planned","applied_epoch":null,"rollback_expires_at":now+Duration::hours(1),"applied_at":null,"rolled_back_at":null})).unwrap();
    b.storage.save_live_migration(&record).await.unwrap();
    let effects=b.storage.collect_compensation_effects(&owner.tenant_id,owner.continuity_id).await.unwrap();
    assert!(b.storage.begin_compensation_run(CompensationStart {run:&run,execution:&owner,instance:&instance,effects:&effects}).await.unwrap());
    let mut applied=record.clone();applied.state=orch8_types::continuity_advanced::LiveMigrationState::Applied;
    let next=ContinuityExecution {epoch:owner.epoch.checked_next().unwrap(),updated_at:now+Duration::seconds(1),..owner.clone()};
    applied.applied_epoch=Some(next.epoch);applied.applied_at=Some(next.updated_at);
    assert!(!b.storage.commit_live_migration_transition(orch8_storage::LiveMigrationTransition {
        tenant_id:&owner.tenant_id,expected_record:&record,next_record:&applied,expected_execution:&owner,next_execution:&next,
        expected_instance_state:InstanceState::Paused,expected_instance_updated_at:instance.updated_at,expected_checkpoint_id:checkpoint.id,expected_source_fingerprint:&"a".repeat(64),expected_target_sequence:&target,next_instance_state:InstanceState::Scheduled,next_fire_at:None,next_sequence_id:target.id,
        next_context:&instance.context,checkpoint:&checkpoint,forbid_effects_epoch:None}).await.unwrap(),
        "a stale migration reopened compensation-fenced ownership");
    assert_eq!(b.storage.get_continuity_execution(&owner.tenant_id,owner.continuity_id).await.unwrap().unwrap().state,OwnershipState::Compensating);
    assert_eq!(b.storage.get_live_migration(&owner.tenant_id,record.plan.id).await.unwrap().unwrap().state,orch8_types::continuity_advanced::LiveMigrationState::Planned);
    assert_eq!(b.storage.get_instance(instance.id).await.unwrap().unwrap().sequence_id,instance.sequence_id);
});

orch8_storage::backend_test!(async fn compensation_start_rejects_changed_owner_or_instance(b) {
    for change in 0..3 {
        let (owner,instance,_effect,run)=fixture(b.storage.as_ref()).await;
        let effects=b.storage.collect_compensation_effects(&owner.tenant_id,owner.continuity_id).await.unwrap();
        if change == 0 {
            let other=ContinuityExecution {owner_runtime_id:RuntimeId::new(),..owner.clone()};
            assert!(b.storage.cas_continuity_owner(&owner.tenant_id,owner.continuity_id,owner.epoch,owner.owner_runtime_id,&other).await.unwrap());
        } else if change == 1 {
            b.storage.update_instance_state(instance.id,InstanceState::Paused,None).await.unwrap();
        } else {
            // A context write changes only the revision; state and sequence stay unchanged.
            b.storage.update_instance_context(instance.id,&instance.context).await.unwrap();
            let current=b.storage.get_instance(instance.id).await.unwrap().unwrap();
            assert_eq!(current.state,instance.state);
            assert_eq!(current.sequence_id,instance.sequence_id);
            assert_ne!(current.updated_at,instance.updated_at);
        }
        assert!(!b.storage.begin_compensation_run(CompensationStart {run:&run,execution:&owner,instance:&instance,effects:&effects}).await.unwrap());
        assert!(b.storage.get_compensation_run(&owner.tenant_id,run.id).await.unwrap().is_none());
        assert_eq!(b.storage.get_continuity_execution(&owner.tenant_id,owner.continuity_id).await.unwrap().unwrap().state,OwnershipState::Owned);
    }
});

orch8_storage::backend_test!(async fn compensation_start_cannot_omit_unsettled_residuals(b) {
    let (owner,instance,mut effect,run)=fixture(b.storage.as_ref()).await;
    effect.id=EffectId::new();effect.state=EffectState::Dispatched;b.storage.create_effect_receipt(&effect).await.unwrap();
    let effects=b.storage.collect_compensation_effects(&owner.tenant_id,owner.continuity_id).await.unwrap();
    assert!(b.storage.begin_compensation_run(CompensationStart {run:&run,execution:&owner,instance:&instance,effects:&effects}).await.is_err());
    assert!(b.storage.get_compensation_run(&owner.tenant_id,run.id).await.unwrap().is_none());
    assert_eq!(b.storage.get_continuity_execution(&owner.tenant_id,owner.continuity_id).await.unwrap().unwrap(),owner);
});

orch8_storage::backend_test!(async fn compensation_fence_rejects_late_attempt_preparation(b) {
    let (owner,instance,mut effect,run)=fixture(b.storage.as_ref()).await;
    let effects=b.storage.collect_compensation_effects(&owner.tenant_id,owner.continuity_id).await.unwrap();
    assert!(b.storage.begin_compensation_run(CompensationStart {run:&run,execution:&owner,instance:&instance,effects:&effects}).await.unwrap());
    effect.id=EffectId::new();effect.block_id=BlockId::new("after-freeze");effect.state=EffectState::Planned;
    assert!(b.storage.ensure_effect_attempt(&effect,EffectId::new()).await.is_err());
    assert!(b.storage.get_effect_receipt(&owner.tenant_id,effect.id).await.unwrap().is_none());
});

orch8_storage::backend_test!(async fn active_compensation_lookup_tracks_states_and_preserves_encrypted_params(b) {
    use orch8_types::continuity_advanced::{CompensationRunState,CompensationStepState};
    let (owner,instance,mut effect,mut run)=fixture(b.storage.as_ref()).await;
    run.steps[0].plan.verification=orch8_types::sequence::CompensationVerificationPolicy::Manual;
    assert!(b.storage.get_active_compensation_run(&owner.tenant_id,owner.continuity_id).await.unwrap().is_none());
    let effects=b.storage.collect_compensation_effects(&owner.tenant_id,owner.continuity_id).await.unwrap();
    assert!(b.storage.begin_compensation_run(CompensationStart {run:&run,execution:&owner,instance:&instance,effects:&effects}).await.unwrap());
    for state in [CompensationRunState::Planned,CompensationRunState::Running,CompensationRunState::AwaitingVerification] {
        if state!=CompensationRunState::Planned {
            let expected=run.version;run.version+=1;run.state=state;run.steps[0].attempt=1;
            run.steps[0].state=if state==CompensationRunState::Running {CompensationStepState::Claimed} else {CompensationStepState::VerificationPending};
            run.steps[0].lease_owner=(state==CompensationRunState::Running).then(||"reader-test".into());
            run.steps[0].lease_expires_at=(state==CompensationRunState::Running).then(||Utc::now()+Duration::minutes(1));
            assert!(b.storage.cas_compensation_run(&owner.tenant_id,expected,&run).await.unwrap());
        }
        let active=b.storage.get_active_compensation_run(&owner.tenant_id,owner.continuity_id).await.unwrap().unwrap();
        assert_eq!(active.id,run.id);assert_eq!(active.state,state);assert_eq!(active.version,run.version);
        assert_eq!(active.steps[0].plan.params,json!({"secret":"compensation-only"}));
    }
    assert!(b.storage.get_active_compensation_run(&orch8_types::ids::TenantId::new("foreign-tenant").unwrap(),owner.continuity_id).await.unwrap().is_none());
    assert!(b.storage.get_active_compensation_run(&owner.tenant_id,ContinuityId::new()).await.unwrap().is_none());
    effect.transition(EffectState::Compensated,Utc::now()).unwrap();
    assert!(b.storage.cas_effect_receipt(&owner.tenant_id,effect.id,EffectState::Committed,&effect).await.unwrap());
    let expected=run.version;run.version+=1;run.state=CompensationRunState::Completed;run.steps[0].state=CompensationStepState::Succeeded;run.completed_at=Some(Utc::now());
    assert!(b.storage.cas_compensation_run(&owner.tenant_id,expected,&run).await.unwrap());
    assert!(b.storage.get_active_compensation_run(&owner.tenant_id,owner.continuity_id).await.unwrap().is_none());
    assert_eq!(b.storage.get_compensation_run(&owner.tenant_id,run.id).await.unwrap().unwrap().state,CompensationRunState::Completed);
});

orch8_storage::backend_test!(async fn compensation_lookups_reject_corrupt_scope_and_indexed_state(b) {
    for (field,value) in [
        ("tenant_id","foreign-tenant".to_owned()),
        ("continuity_id",ContinuityId::new().to_string()),
        ("id",uuid::Uuid::new_v4().to_string()),
        ("state","completed".to_owned()),
        ("version",String::new()),
    ] {
        let (owner,instance,_effect,run)=fixture(b.storage.as_ref()).await;
        let effects=b.storage.collect_compensation_effects(&owner.tenant_id,owner.continuity_id).await.unwrap();
        assert!(b.storage.begin_compensation_run(CompensationStart {run:&run,execution:&owner,instance:&instance,effects:&effects}).await.unwrap());
        if b.kind.is_postgres() {
            if field=="version" {
                sqlx::query("UPDATE compensation_runs SET version=version+1 WHERE id=$1").bind(run.id.into_uuid()).execute(b.pg_pool()).await.unwrap();
            } else {
                sqlx::query("UPDATE compensation_runs SET record=jsonb_set(record,ARRAY[$1::text],to_jsonb($2::text)) WHERE id=$3").bind(field).bind(&value).bind(run.id.into_uuid()).execute(b.pg_pool()).await.unwrap();
            }
        } else if field=="version" {
            sqlx::query("UPDATE compensation_runs SET version=version+1 WHERE id=?1").bind(run.id.to_string()).execute(b.sqlite_pool()).await.unwrap();
        } else {
            sqlx::query("UPDATE compensation_runs SET record=json_set(record,?1,?2) WHERE id=?3").bind(format!("$.{field}")).bind(&value).bind(run.id.to_string()).execute(b.sqlite_pool()).await.unwrap();
        }
        assert!(b.storage.get_active_compensation_run(&owner.tenant_id,owner.continuity_id).await.is_err(),"active lookup accepted inconsistent {field}");
        assert!(b.storage.get_compensation_run(&owner.tenant_id,run.id).await.is_err(),"known-ID lookup accepted inconsistent {field}");
    }
});

orch8_storage::backend_test!(async fn compensation_cas_rejects_identity_or_revision_changes(b) {
    use orch8_types::continuity_advanced::{CompensationRunState,CompensationStepState};
    for field in ["tenant","continuity","source","created","unchanged_version","skipped_version","regressed_version","overflowed_version","storage_version_exhausted"] {
        let (owner,instance,_effect,mut run)=fixture(b.storage.as_ref()).await;
        let effects=b.storage.collect_compensation_effects(&owner.tenant_id,owner.continuity_id).await.unwrap();
        assert!(b.storage.begin_compensation_run(CompensationStart {run:&run,execution:&owner,instance:&instance,effects:&effects}).await.unwrap());
        if field=="regressed_version" {
            run.version=1;run.state=CompensationRunState::Running;run.steps[0].state=CompensationStepState::Claimed;
            run.steps[0].attempt=1;run.steps[0].lease_owner=Some("mutation-control".into());run.steps[0].lease_expires_at=Some(Utc::now()+Duration::minutes(1));
            assert!(b.storage.cas_compensation_run(&owner.tenant_id,0,&run).await.unwrap());
        }
        let mut next=run.clone();next.version=run.version+1;next.updated_at=Utc::now();
        match field {
            "tenant"=>next.tenant_id=orch8_types::ids::TenantId::new("foreign-mutation").unwrap(),
            "continuity"=>next.continuity_id=ContinuityId::new(),
            "source"=>next.source_instance_id=orch8_types::ids::InstanceId::new(),
            "created"=>next.created_at+=Duration::seconds(1),
            "unchanged_version"=>next.version=run.version,
            "skipped_version"=>next.version=run.version+2,
            "regressed_version"=>next.version=run.version-1,
            "overflowed_version"=>next.version=0,
            "storage_version_exhausted"=>next.version=(i64::MAX as u64)+1,
            _=>unreachable!(),
        }
        let expected=match field {"overflowed_version"=>u64::MAX,"storage_version_exhausted"=>i64::MAX as u64,_=>run.version};
        let result=b.storage.cas_compensation_run(&owner.tenant_id,expected,&next).await;
        assert!(matches!(&result,Ok(false)|Err(orch8_types::error::StorageError::Constraint(_))),"accepted or mishandled {field}: {result:?}");
        let unchanged=b.storage.get_compensation_run(&owner.tenant_id,run.id).await.unwrap().unwrap();
        assert_eq!(serde_json::to_value(unchanged).unwrap(),serde_json::to_value(run).unwrap(),"rejected {field} mutated the stored run");
    }
});

orch8_storage::backend_test!(async fn compensation_cas_has_one_claim_winner_and_rejects_stale_updates(b) {
    use orch8_types::continuity_advanced::{CompensationRunState,CompensationStepState};
    let (owner,instance,_effect,run)=fixture(b.storage.as_ref()).await;
    let effects=b.storage.collect_compensation_effects(&owner.tenant_id,owner.continuity_id).await.unwrap();
    assert!(b.storage.begin_compensation_run(CompensationStart {run:&run,execution:&owner,instance:&instance,effects:&effects}).await.unwrap());
    let mut first=run.clone();first.version=1;first.state=CompensationRunState::Running;
    first.steps[0].state=CompensationStepState::Claimed;first.steps[0].attempt=1;first.steps[0].lease_owner=Some("first".into());
    first.steps[0].lease_expires_at=Some(Utc::now()+Duration::minutes(1));
    let mut second=first.clone();second.steps[0].lease_owner=Some("second".into());
    let (a,c)=tokio::join!(b.storage.cas_compensation_run(&owner.tenant_id,0,&first),b.storage.cas_compensation_run(&owner.tenant_id,0,&second));
    let a=a.unwrap();let c=c.unwrap();assert_ne!(a,c);
    let current=b.storage.get_compensation_run(&owner.tenant_id,run.id).await.unwrap().unwrap();
    assert_eq!(current.version,1);assert_eq!(current.steps[0].lease_owner.as_deref(),Some(if a {"first"} else {"second"}));
    assert_eq!(current.steps[0].plan.params,json!({"secret":"compensation-only"}));
    assert!(!b.storage.cas_compensation_run(&owner.tenant_id,0,&first).await.unwrap());
    assert_eq!(serde_json::to_value(b.storage.get_compensation_run(&owner.tenant_id,run.id).await.unwrap().unwrap()).unwrap(),serde_json::to_value(current).unwrap());
});
