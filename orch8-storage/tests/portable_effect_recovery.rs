//! Portable recovery publishes identity, outcomes and policy in one transaction.
use chrono::Utc;
use orch8_storage::{
    ContinuityStore as _, StorageBackend,
    portable_effects::CapsuleEffectImport,
    restore::InstanceSnapshot,
    testing::{
        backends::{RawPool, TestBackend},
        fixtures::{InstanceBuilder, SequenceBuilder},
    },
};
use orch8_types::{
    checkpoint::Checkpoint,
    continuity::{
        CapsuleEffectBinding, CapsuleEffectState, CapsuleId, ContinuityExecution, ContinuityId,
        EffectId, EffectKind, EffectReceipt, EffectState, ExecutionEpoch, OwnershipState,
        RuntimeId,
    },
    continuity_advanced::{InvariantId, InvariantRule, WorkflowInvariant},
    ids::{BlockId, InstanceId, TenantId},
    instance::InstanceState,
};

#[derive(Clone)]
struct Fixture {
    snapshot: InstanceSnapshot,
    checkpoint: Checkpoint,
    effects: CapsuleEffectImport,
    capsule: CapsuleId,
    runtime: RuntimeId,
}
async fn fixture(storage: &dyn StorageBackend) -> Fixture {
    fixture_for_tenant(storage, "portable-recovery").await
}
async fn fixture_for_tenant(storage: &dyn StorageBackend, tenant: &str) -> Fixture {
    let sequence = SequenceBuilder::new(tenant).step("work", "charge").build();
    storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Paused)
        .build();
    let source = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: instance.tenant_id.clone(),
        current_instance_id: InstanceId::new(),
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    let receipt = EffectReceipt {
        id: EffectId::new(),
        tenant_id: instance.tenant_id.clone(),
        continuity_id: source.continuity_id,
        epoch: source.epoch,
        instance_id: source.current_instance_id,
        block_id: BlockId::new("work"),
        kind: EffectKind::Worker,
        state: EffectState::Prepared,
        destination_fingerprint: "provider".into(),
        request_sha256: "request".into(),
        idempotency_key: None,
        provider_receipt_id: None,
        attempt: 0,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let guard = WorkflowInvariant {
        id: InvariantId::new(),
        tenant_id: instance.tenant_id.clone(),
        sequence_id: sequence.id,
        sequence_version: Some(sequence.version),
        name: "frozen charge policy".into(),
        rule: InvariantRule::EffectAtMostOnce {
            kind: EffectKind::Worker,
        },
        commit_guard: true,
        enabled: true,
        created_at: Utc::now(),
    };
    let state = CapsuleEffectState {
        active_attempts: vec![CapsuleEffectBinding {
            block_id: receipt.block_id.clone(),
            attempt: 0,
            effect_id: receipt.id,
        }],
        reset_blocks: vec![receipt.block_id.clone()],
        receipts: vec![receipt],
        commit_guards: vec![guard],
    };
    let checkpoint = Checkpoint {
        id: uuid::Uuid::now_v7(),
        instance_id: instance.id,
        checkpoint_data: serde_json::json!({}),
        created_at: Utc::now(),
    };
    let capsule = CapsuleId::new();
    let runtime = RuntimeId::new();
    let manifest = manifest(&source, sequence.id, capsule, runtime);
    Fixture {
        snapshot: InstanceSnapshot {
            instance,
            nodes: Vec::new(),
            outputs: Vec::new(),
            payloads: Vec::new(),
        },
        checkpoint,
        effects: CapsuleEffectImport {
            manifest,
            source,
            manifest_sha256: "a".repeat(64),
            state,
        },
        capsule,
        runtime,
    }
}
async fn import(
    storage: &dyn StorageBackend,
    f: &Fixture,
) -> Result<InstanceId, orch8_types::error::StorageError> {
    storage
        .import_capsule_recovery(
            f.capsule,
            f.runtime,
            &f.snapshot,
            &f.checkpoint,
            Some(&f.effects),
        )
        .await
}
fn other_candidate(f: &Fixture) -> Fixture {
    let mut next = f.clone();
    next.snapshot.instance.id = InstanceId::new();
    next.checkpoint.instance_id = next.snapshot.instance.id;
    next.checkpoint.id = uuid::Uuid::now_v7();
    next
}
async fn count(b: &TestBackend, table: &str) -> i64 {
    let sql = sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}"));
    match &b.raw {
        RawPool::Sqlite(pool) => sqlx::query_scalar(sql).fetch_one(pool).await.unwrap(),
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => sqlx::query_scalar(sql).fetch_one(pool).await.unwrap(),
    }
}
async fn assert_empty(b: &TestBackend) {
    for table in [
        "task_instances",
        "checkpoints",
        "continuity_executions",
        "continuity_locations",
        "capsule_imports",
        "capsule_effect_imports",
        "execution_capsules",
        "effect_receipts",
        "effect_attempt_bindings",
        "effect_block_resets",
        "workflow_invariants",
        "continuity_effect_guards",
    ] {
        assert_eq!(
            count(b, table).await,
            0,
            "partial recovery escaped rollback in {table}"
        );
    }
}
async fn advance_receipt(storage: &dyn StorageBackend, f: &Fixture) -> EffectReceipt {
    let mut receipt = f.effects.state.receipts[0].clone();
    for state in [EffectState::Dispatched, EffectState::Committed] {
        let previous = receipt.state;
        receipt.transition(state, Utc::now()).unwrap();
        if state == EffectState::Committed {
            receipt.provider_receipt_id = Some("provider-confirmed".into());
        }
        assert!(
            storage
                .cas_effect_receipt(&receipt.tenant_id, receipt.id, previous, &receipt)
                .await
                .unwrap()
        );
    }
    receipt
}

orch8_storage::backend_test!(async fn final_import_proof_failure_rolls_back_all_recovery_state(b) {
    let f=fixture(b.storage.as_ref()).await;
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::query("CREATE TRIGGER fail_effect_proof BEFORE INSERT ON capsule_effect_imports BEGIN SELECT RAISE(ABORT,'injected final proof failure'); END").execute(pool).await.unwrap();}
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("CREATE FUNCTION fail_effect_proof() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected final proof failure'; END $$; CREATE TRIGGER fail_effect_proof BEFORE INSERT ON capsule_effect_imports FOR EACH ROW EXECUTE FUNCTION fail_effect_proof();").execute(pool).await.unwrap();}
    }
    assert!(import(b.storage.as_ref(),&f).await.is_err());assert_empty(&b).await;
});

orch8_storage::backend_test!(async fn invalid_portable_evidence_never_publishes_an_instance(b) {
    let f=fixture(b.storage.as_ref()).await;
    for case in 0..12 {
        let mut bad=f.clone();
        match case {
            0=>bad.effects.state.receipts[0].tenant_id=TenantId::unchecked("foreign"),
            1=>bad.effects.state.receipts[0].continuity_id=ContinuityId::new(),
            2=>bad.effects.state.receipts[0].epoch=ExecutionEpoch::initial().checked_next().unwrap(),
            3=>bad.effects.state.active_attempts[0].effect_id=EffectId::new(),
            4=>bad.effects.state.active_attempts.push(bad.effects.state.active_attempts[0].clone()),
            5=>bad.effects.state.reset_blocks.push(BlockId::new("work")),
            6=>bad.effects.state.commit_guards[0].enabled=false,
            7=>bad.effects.state.commit_guards[0].tenant_id=TenantId::unchecked("foreign"),
            8=>bad.effects.state.commit_guards[0].sequence_id=orch8_types::ids::SequenceId::new(),
            9=>bad.effects.source.state=OwnershipState::Transferring,
            10=>bad.effects.state.commit_guards[0].sequence_version=Some(2),
            _=>bad.effects.manifest_sha256="invalid".into(),
        }
        assert!(import(b.storage.as_ref(),&bad).await.is_err(),"accepted invalid evidence case {case}");
        assert_empty(&b).await;
    }
});

orch8_storage::backend_test!(async fn redelivery_preserves_advanced_outcomes_and_rejects_digest_or_schema_conflicts(b) {
    let f=fixture(b.storage.as_ref()).await;let id=import(b.storage.as_ref(),&f).await.unwrap();
    let advanced=advance_receipt(b.storage.as_ref(),&f).await;
    let owner=b.storage.get_continuity_execution(&f.effects.source.tenant_id,f.effects.source.continuity_id).await.unwrap().unwrap();
    let accepted=ContinuityExecution{current_instance_id:id,owner_runtime_id:f.runtime,epoch:owner.epoch.checked_next().unwrap(),state:OwnershipState::Owned,updated_at:Utc::now(),..owner.clone()};
    assert!(b.storage.cas_continuity_owner(&owner.tenant_id,owner.continuity_id,owner.epoch,owner.owner_runtime_id,&accepted).await.unwrap());
    let mut replay=other_candidate(&f);assert_eq!(import(b.storage.as_ref(),&replay).await.unwrap(),id);
    assert_eq!(b.storage.get_effect_receipt(&advanced.tenant_id,advanced.id).await.unwrap().unwrap(),advanced);
    replay.effects.manifest_sha256="b".repeat(64);assert!(import(b.storage.as_ref(),&replay).await.is_err());
    assert!(b.storage.import_capsule_snapshot(f.capsule,f.runtime,&replay.snapshot,&replay.checkpoint).await.is_err(),"legacy import must not bypass a 1.2 recovery proof");
    assert!(b.storage.get_instance(replay.snapshot.instance.id).await.unwrap().is_none());
    assert_eq!(count(&b,"task_instances").await,1);assert_eq!(count(&b,"capsule_effect_imports").await,1);
});

orch8_storage::backend_test!(async fn concurrent_imports_publish_one_complete_destination(b) {
    let first=fixture(b.storage.as_ref()).await;let second=other_candidate(&first);
    let (left,right)=tokio::join!(import(b.storage.as_ref(),&first),import(b.storage.as_ref(),&second));
    assert_eq!(left.unwrap(),right.unwrap());
    for table in ["task_instances","checkpoints","capsule_imports","capsule_effect_imports",
        "execution_capsules","continuity_executions","continuity_locations","effect_receipts","effect_attempt_bindings","effect_block_resets","workflow_invariants","continuity_effect_guards"] {assert_eq!(count(&b,table).await,1,"incomplete or duplicate {table}");}
});

orch8_storage::backend_test!(async fn existing_advanced_origin_evidence_is_preserved_but_substitution_rolls_back(b) {
    let mut f=fixture(b.storage.as_ref()).await;
    b.storage.create_continuity_execution(&f.effects.source).await.unwrap();b.storage.create_effect_receipt(&f.effects.state.receipts[0]).await.unwrap();
    let advanced=advance_receipt(b.storage.as_ref(),&f).await;
    f.effects.state.receipts[0].updated_at=advanced.updated_at+chrono::Duration::days(1);
    import(b.storage.as_ref(),&f).await.unwrap();
    assert_eq!(b.storage.get_effect_receipt(&advanced.tenant_id,advanced.id).await.unwrap().unwrap(),advanced);
    let mut changed=other_candidate(&f);changed.capsule=CapsuleId::new();changed.effects.manifest.capsule_id=changed.capsule;changed.effects.state.receipts[0].request_sha256="substituted".into();
    assert!(import(b.storage.as_ref(),&changed).await.is_err());assert!(b.storage.get_instance(changed.snapshot.instance.id).await.unwrap().is_none());
    assert_eq!(count(&b,"task_instances").await,1);assert_eq!(count(&b,"capsule_effect_imports").await,1);
});

orch8_storage::backend_test!(async fn foreign_receipt_identity_collision_cannot_cross_tenant_boundaries(b) {
    let f=fixture(b.storage.as_ref()).await;
    let mut foreign=f.effects.source.clone();foreign.tenant_id=TenantId::unchecked("foreign");foreign.continuity_id=ContinuityId::new();foreign.current_instance_id=InstanceId::new();
    b.storage.create_continuity_execution(&foreign).await.unwrap();
    let mut receipt=f.effects.state.receipts[0].clone();receipt.tenant_id=foreign.tenant_id.clone();receipt.continuity_id=foreign.continuity_id;receipt.instance_id=foreign.current_instance_id;
    b.storage.create_effect_receipt(&receipt).await.unwrap();
    assert!(import(b.storage.as_ref(),&f).await.is_err());assert!(b.storage.get_instance(f.snapshot.instance.id).await.unwrap().is_none());
    assert_eq!(b.storage.get_effect_receipt(&foreign.tenant_id,receipt.id).await.unwrap().unwrap(),receipt);
    assert!(b.storage.get_continuity_execution(&f.effects.source.tenant_id,f.effects.source.continuity_id).await.unwrap().is_none());
    assert_eq!(count(&b,"capsule_effect_imports").await,0);
});

orch8_storage::backend_test!(async fn conflicting_local_policy_or_target_namespace_is_rejected_atomically(b) {
    let f=fixture(b.storage.as_ref()).await;
    let mut local=f.effects.state.commit_guards[0].clone();local.rule=InvariantRule::EffectAtMostOnce{kind:EffectKind::Http};
    b.storage.create_workflow_invariant(&local).await.unwrap();
    assert!(import(b.storage.as_ref(),&f).await.is_err());assert!(b.storage.get_instance(f.snapshot.instance.id).await.unwrap().is_none());
    assert_eq!(count(&b,"capsule_effect_imports").await,0);assert_eq!(count(&b,"effect_receipts").await,0);
    let mut other=other_candidate(&f);other.effects.state.commit_guards.clear();other.capsule=CapsuleId::new();other.effects.manifest.capsule_id=other.capsule;
    b.storage.create_continuity_execution(&other.effects.source).await.unwrap();
    let conflicting=ContinuityExecution{continuity_id:ContinuityId::new(),current_instance_id:other.snapshot.instance.id,..other.effects.source.clone()};
    b.storage.create_continuity_execution(&conflicting).await.unwrap();
    assert!(import(b.storage.as_ref(),&other).await.is_err());assert!(b.storage.get_instance(other.snapshot.instance.id).await.unwrap().is_none());assert_eq!(count(&b,"capsule_effect_imports").await,0);
});

orch8_storage::backend_test!(async fn outcome_merge_preserves_resolution_and_refuses_incomparable_or_conflicting_receipts(b) {
    for case in 0..4 {
        let mut f=fixture(b.storage.as_ref()).await;
        b.storage.create_continuity_execution(&f.effects.source).await.unwrap();
        let mut existing=f.effects.state.receipts[0].clone();
        existing.state=if case==0 {EffectState::Prepared} else {EffectState::Committed};
        if case>=2 {existing.provider_receipt_id=Some("known-provider".into());}
        b.storage.create_effect_receipt(&existing).await.unwrap();
        let incoming=&mut f.effects.state.receipts[0];
        incoming.state=if case==2 {EffectState::Verified} else {EffectState::Committed};
        incoming.provider_receipt_id=Some(if case==3 {"substituted-provider"} else {"known-provider"}.into());
        let result=import(b.storage.as_ref(),&f).await;
        if case<2 {
            result.unwrap();let stored=b.storage.get_effect_receipt(&existing.tenant_id,existing.id).await.unwrap().unwrap();
            assert_eq!(stored.state,EffectState::Committed);assert_eq!(stored.provider_receipt_id.as_deref(),Some("known-provider"));assert_eq!(stored.instance_id,existing.instance_id);
        } else {
            assert!(result.is_err());assert!(b.storage.get_instance(f.snapshot.instance.id).await.unwrap().is_none());
            assert_eq!(b.storage.get_effect_receipt(&existing.tenant_id,existing.id).await.unwrap().unwrap(),existing);
        }
    }
});

fn manifest(
    source: &ContinuityExecution,
    sequence: orch8_types::ids::SequenceId,
    capsule: CapsuleId,
    runtime: RuntimeId,
) -> orch8_types::continuity::CapsuleManifest {
    orch8_types::continuity::CapsuleManifest {
        schema: orch8_types::continuity::CapsuleSchemaVersion::CURRENT,
        capsule_id: capsule,
        continuity_id: source.continuity_id,
        source_instance_id: source.current_instance_id,
        epoch: source.epoch,
        tenant_id: source.tenant_id.clone(),
        source_runtime_id: source.owner_runtime_id,
        allowed_destination_runtime_id: Some(runtime),
        sequence: orch8_types::continuity::SequenceIdentity {
            id: sequence,
            version: 1,
            content_sha256: "a".repeat(64),
        },
        checkpoint: orch8_types::continuity::CheckpointIdentity {
            block_id: BlockId::new("work"),
            sha256: "a".repeat(64),
        },
        requirements_sha256: "a".repeat(64),
        payload_artifact: orch8_types::continuity::ArtifactReference {
            key: "source/capsule".into(),
            sha256: "a".repeat(64),
            bytes: 1,
        },
        provenance_head: None,
        issued_at: Utc::now(),
        expires_at: Utc::now() + chrono::Duration::minutes(5),
        signing_key_id: "signer".into(),
        encryption_key_id: "payload".into(),
    }
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_downgrade_preserves_portable_proof_guards_and_aliases() {
    let Some(b) = TestBackend::postgres().await else {
        return;
    };
    let f = fixture(b.storage.as_ref()).await;
    import(b.storage.as_ref(), &f).await.unwrap();
    let down = include_str!("../../migrations/down/128_portable_effect_state.sql");
    assert!(sqlx::raw_sql(down).execute(b.pg_pool()).await.is_err());
    sqlx::query("DELETE FROM capsule_effect_imports")
        .execute(b.pg_pool())
        .await
        .unwrap();
    assert!(
        sqlx::raw_sql(down).execute(b.pg_pool()).await.is_err(),
        "frozen policy prevents downgrade"
    );
    sqlx::query("DELETE FROM continuity_effect_guards")
        .execute(b.pg_pool())
        .await
        .unwrap();
    assert!(
        sqlx::raw_sql(down).execute(b.pg_pool()).await.is_err(),
        "origin-divergent aliases prevent downgrade"
    );
    sqlx::query("DELETE FROM effect_attempt_bindings")
        .execute(b.pg_pool())
        .await
        .unwrap();
    sqlx::raw_sql(down).execute(b.pg_pool()).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../migrations/128_portable_effect_state.sql"
    ))
    .execute(b.pg_pool())
    .await
    .unwrap();
    assert_eq!(
        count(&b, "effect_receipts").await,
        1,
        "downgrade must retain original outcomes"
    );
    assert!(
        import(b.storage.as_ref(), &f).await.is_err(),
        "legacy replay cannot bypass a removed recovery proof"
    );
    let fresh = fixture(b.storage.as_ref()).await;
    import(b.storage.as_ref(), &fresh).await.unwrap();
}

#[tokio::test]
async fn sqlite_reopen_preserves_import_proof_aliases_and_frozen_guards() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("portable.sqlite");
    let path = path.to_str().unwrap();
    let store = orch8_storage::sqlite::SqliteStorage::file(path)
        .await
        .unwrap();
    let f = fixture(&store).await;
    let imported = import(&store, &f).await.unwrap();
    store.close().await;
    let store = orch8_storage::sqlite::SqliteStorage::file(path)
        .await
        .unwrap();
    assert_eq!(
        import(&store, &other_candidate(&f)).await.unwrap(),
        imported
    );
    let mut accepted = f.effects.source.clone();
    accepted.current_instance_id = imported;
    accepted.epoch = accepted.epoch.checked_next().unwrap();
    let (bindings, resets) = store.export_effect_attempts(&accepted).await.unwrap();
    assert_eq!(bindings, f.effects.state.active_attempts);
    assert_eq!(resets, f.effects.state.reset_blocks);
    let guards = store
        .load_effect_guards(
            &accepted.tenant_id,
            accepted.continuity_id,
            f.snapshot.instance.sequence_id,
            1,
            None,
        )
        .await
        .unwrap();
    assert_eq!(guards, f.effects.state.commit_guards);
    let mut changed = f.clone();
    changed.effects.manifest_sha256 = "b".repeat(64);
    assert!(import(&store, &changed).await.is_err());
    activate(&store, &f).await.unwrap();
    orch8_storage::InstanceStore::update_instance_state(
        &store,
        imported,
        InstanceState::Waiting,
        None,
    )
    .await
    .unwrap();
    let waiting = orch8_storage::InstanceStore::get_instance(&store, imported)
        .await
        .unwrap()
        .unwrap();
    store.close().await;
    let store = orch8_storage::sqlite::SqliteStorage::file(path)
        .await
        .unwrap();
    activate(&store, &f).await.unwrap();
    assert_eq!(
        serde_json::to_value(waiting).unwrap(),
        serde_json::to_value(
            orch8_storage::InstanceStore::get_instance(&store, imported)
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap()
    );
    store.close().await;
}

async fn activate(
    storage: &dyn StorageBackend,
    f: &Fixture,
) -> Result<(), orch8_types::error::StorageError> {
    storage
        .activate_capsule_import(
            &f.effects.source.tenant_id,
            f.capsule,
            f.runtime,
            f.snapshot.instance.id,
            Utc::now(),
        )
        .await
}

orch8_storage::backend_test!(async fn activation_retries_preserve_progress_claims_and_completed_ownership(b) {
    let f=fixture(b.storage.as_ref()).await; import(b.storage.as_ref(),&f).await.unwrap();
    activate(b.storage.as_ref(),&f).await.unwrap();
    let tenant=&f.effects.source.tenant_id; let id=f.snapshot.instance.id;
    let claimed=b.storage.claim_due_instances(Utc::now()+chrono::Duration::seconds(1),1,0).await.unwrap();
    assert_eq!(claimed.len(),1);
    let before=b.storage.get_instance(id).await.unwrap().unwrap();
    assert!(before.claimed_at.is_some());
    activate(b.storage.as_ref(),&f).await.unwrap();
    assert_eq!(serde_json::to_value(&before).unwrap(),serde_json::to_value(b.storage.get_instance(id).await.unwrap().unwrap()).unwrap());
    for state in [InstanceState::Scheduled,InstanceState::Running,InstanceState::Waiting,InstanceState::Paused,InstanceState::Completed,InstanceState::Failed,InstanceState::Cancelled] {
        b.storage.update_instance_state(id,state,None).await.unwrap();
        let before=b.storage.get_instance(id).await.unwrap().unwrap();
        activate(b.storage.as_ref(),&f).await.unwrap();
        assert_eq!(serde_json::to_value(&before).unwrap(),serde_json::to_value(b.storage.get_instance(id).await.unwrap().unwrap()).unwrap(),"retry changed {state}");
    }
    let owner=b.storage.get_continuity_execution(tenant,f.effects.source.continuity_id).await.unwrap().unwrap();
    let mut completed=owner.clone();completed.state=OwnershipState::Completed;
    assert!(b.storage.cas_continuity_owner(tenant,owner.continuity_id,owner.epoch,owner.owner_runtime_id,&completed).await.unwrap());
    activate(b.storage.as_ref(),&f).await.unwrap();
    assert_eq!(b.storage.get_continuity_execution(tenant,owner.continuity_id).await.unwrap().unwrap(),completed);
    assert_eq!(b.storage.list_continuity_locations(tenant,owner.continuity_id,100).await.unwrap().len(),2);
});

orch8_storage::backend_test!(async fn activation_schedule_failure_rolls_back_ownership_and_location(b) {
    let f=fixture(b.storage.as_ref()).await;import(b.storage.as_ref(),&f).await.unwrap();
    let tenant=&f.effects.source.tenant_id;
    let owner=b.storage.get_continuity_execution(tenant,f.effects.source.continuity_id).await.unwrap().unwrap();
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::query("CREATE TRIGGER fail_capsule_schedule BEFORE UPDATE OF state ON task_instances WHEN NEW.state='scheduled' BEGIN SELECT RAISE(ABORT,'injected schedule failure'); END").execute(pool).await.unwrap();}
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("CREATE FUNCTION fail_capsule_schedule() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state::text='scheduled' THEN RAISE EXCEPTION 'injected schedule failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_capsule_schedule BEFORE UPDATE OF state ON task_instances FOR EACH ROW EXECUTE FUNCTION fail_capsule_schedule();").execute(pool).await.unwrap();}
    }
    assert!(activate(b.storage.as_ref(),&f).await.is_err());
    assert_eq!(b.storage.get_continuity_execution(tenant,owner.continuity_id).await.unwrap().unwrap(),owner);
    assert_eq!(b.storage.get_instance(f.snapshot.instance.id).await.unwrap().unwrap().state,InstanceState::Paused);
    assert_eq!(b.storage.list_continuity_locations(tenant,owner.continuity_id,100).await.unwrap().len(),1);
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::query("DROP TRIGGER fail_capsule_schedule").execute(pool).await.unwrap();}
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("DROP TRIGGER fail_capsule_schedule ON task_instances; DROP FUNCTION fail_capsule_schedule();").execute(pool).await.unwrap();}
    }
    activate(b.storage.as_ref(),&f).await.unwrap();
});

orch8_storage::backend_test!(async fn concurrent_capsule_activation_accepts_once_without_rescheduling(b) {
    let f=fixture(b.storage.as_ref()).await;import(b.storage.as_ref(),&f).await.unwrap();
    let tasks=(0..16).map(|n| {let storage=b.storage.clone();let f=f.clone();tokio::spawn(async move {
        storage.activate_capsule_import(&f.effects.source.tenant_id,f.capsule,f.runtime,f.snapshot.instance.id,Utc::now()+chrono::Duration::seconds(n)).await.unwrap();
    })}).collect::<Vec<_>>();
    for task in tasks {task.await.unwrap();}
    let owner=b.storage.get_continuity_execution(&f.effects.source.tenant_id,f.effects.source.continuity_id).await.unwrap().unwrap();
    let instance=b.storage.get_instance(f.snapshot.instance.id).await.unwrap().unwrap();
    assert_eq!(owner.epoch,f.effects.source.epoch.checked_next().unwrap());
    assert_eq!(owner.owner_runtime_id,f.runtime);assert_eq!(owner.state,OwnershipState::Owned);
    assert_eq!(instance.state,InstanceState::Scheduled);assert_eq!(instance.next_fire_at,Some(instance.updated_at));
    // PostgreSQL stores microseconds in SQL while the JSON owner retains nanos.
    assert_eq!(instance.updated_at.timestamp_micros(),owner.updated_at.timestamp_micros());
    assert_eq!(b.storage.list_continuity_locations(&owner.tenant_id,owner.continuity_id,100).await.unwrap().len(),2);
});

orch8_storage::backend_test!(async fn activation_refuses_wrong_scope_missing_proof_and_pending_running_instance(b) {
    let f=fixture(b.storage.as_ref()).await;import(b.storage.as_ref(),&f).await.unwrap();
    let tenant=&f.effects.source.tenant_id;let id=f.snapshot.instance.id;
    for (t,c,r,i) in [(TenantId::unchecked("foreign"),f.capsule,f.runtime,id),(tenant.clone(),CapsuleId::new(),f.runtime,id),(tenant.clone(),f.capsule,RuntimeId::new(),id),(tenant.clone(),f.capsule,f.runtime,InstanceId::new())] {
        assert!(b.storage.activate_capsule_import(&t,c,r,i,Utc::now()).await.is_err());
    }
    b.storage.update_instance_state(id,InstanceState::Running,None).await.unwrap();
    assert!(activate(b.storage.as_ref(),&f).await.is_err());
    let owner=b.storage.get_continuity_execution(tenant,f.effects.source.continuity_id).await.unwrap().unwrap();
    assert_eq!(owner.state,OwnershipState::Transferring);assert_eq!(owner.epoch,f.effects.source.epoch);
    b.storage.update_instance_state(id,InstanceState::Paused,None).await.unwrap();
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::query("DELETE FROM capsule_effect_imports").execute(pool).await.unwrap();}
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::query("DELETE FROM capsule_effect_imports").execute(pool).await.unwrap();}
    }
    assert!(activate(b.storage.as_ref(),&f).await.is_err());
    assert_eq!(b.storage.get_instance(id).await.unwrap().unwrap().state,InstanceState::Paused);
});

orch8_storage::backend_test!(async fn old_activation_cannot_reverse_a_subsequent_ownership_transfer(b) {
    let f=fixture(b.storage.as_ref()).await;import(b.storage.as_ref(),&f).await.unwrap();activate(b.storage.as_ref(),&f).await.unwrap();
    let tenant=&f.effects.source.tenant_id;
    let current=b.storage.get_continuity_execution(tenant,f.effects.source.continuity_id).await.unwrap().unwrap();
    let mut transferring=current.clone();transferring.state=OwnershipState::Transferring;
    assert!(b.storage.cas_continuity_owner(tenant,current.continuity_id,current.epoch,current.owner_runtime_id,&transferring).await.unwrap());
    assert!(activate(b.storage.as_ref(),&f).await.is_err());
    let next=ContinuityExecution {epoch:current.epoch.checked_next().unwrap(),owner_runtime_id:RuntimeId::new(),state:OwnershipState::Owned,..transferring.clone()};
    assert!(b.storage.cas_continuity_owner(tenant,current.continuity_id,current.epoch,current.owner_runtime_id,&next).await.unwrap());
    assert!(activate(b.storage.as_ref(),&f).await.is_err());
    assert_eq!(b.storage.get_continuity_execution(tenant,current.continuity_id).await.unwrap().unwrap(),next);
});

#[tokio::test]
async fn activation_never_schedules_a_registered_account_without_catalog_admission() {
    let b = TestBackend::sqlite().await;
    let f = fixture_for_tenant(b.storage.as_ref(), "mobile-account-activation-account").await;
    import(b.storage.as_ref(), &f).await.unwrap();
    let pool = b.sqlite_pool();
    sqlx::query("INSERT INTO mobile_accounts(id,account_key,principal_issuer,principal_subject,epoch,active) VALUES('activation-account','account','issuer','subject',1,0)").execute(pool).await.unwrap();
    for active in [0, 1] {
        sqlx::query("UPDATE mobile_accounts SET active=?1")
            .bind(active)
            .execute(pool)
            .await
            .unwrap();
        assert!(activate(b.storage.as_ref(), &f).await.is_err());
        let owner = b
            .storage
            .get_continuity_execution(&f.effects.source.tenant_id, f.effects.source.continuity_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(owner.state, OwnershipState::Transferring);
        assert_eq!(owner.epoch, f.effects.source.epoch);
        assert_eq!(
            b.storage
                .get_instance(f.snapshot.instance.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            InstanceState::Paused
        );
    }
}

orch8_storage::backend_test!(async fn lost_activation_acknowledgement_does_not_reschedule_a_live_claim(b) {
    use orch8_storage::testing::faulty::FaultStorage;
    use orch8_types::error::StorageError;
    let f=fixture(b.storage.as_ref()).await;import(b.storage.as_ref(),&f).await.unwrap();
    let (storage,faults)=FaultStorage::wrap(b.storage.clone());
    faults.on("activate_capsule_import").times(1).fail_after_commit(StorageError::Connection("lost activation acknowledgement".into()));
    assert!(matches!(activate(storage.as_ref(),&f).await,Err(StorageError::Connection(_))));
    let claimed=b.storage.claim_due_instances(Utc::now()+chrono::Duration::seconds(1),1,0).await.unwrap();
    assert_eq!(claimed.len(),1);assert!(claimed[0].claimed_at.is_some());
    let before=b.storage.get_instance(f.snapshot.instance.id).await.unwrap().unwrap();
    activate(storage.as_ref(),&f).await.unwrap();
    assert_eq!(serde_json::to_value(&before).unwrap(),serde_json::to_value(b.storage.get_instance(before.id).await.unwrap().unwrap()).unwrap());
    let owner=b.storage.get_continuity_execution(&f.effects.source.tenant_id,f.effects.source.continuity_id).await.unwrap().unwrap();
    assert_eq!(owner.epoch,f.effects.source.epoch.checked_next().unwrap());
    assert_eq!(owner.owner_runtime_id,f.runtime);
    assert_eq!(b.storage.list_continuity_locations(&owner.tenant_id,owner.continuity_id,100).await.unwrap().len(),2);
});
