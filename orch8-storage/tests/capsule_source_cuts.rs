//! Source cuts preserve one durable recovery point before external publication.
use chrono::{Duration, Utc};
use orch8_storage::{
    StorageBackend,
    capsule_export::{CapsuleCutCapture, CapsuleCutRequest, CapsuleSourceCut},
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    checkpoint::Checkpoint,
    continuity::{
        CapsuleManifest, ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState,
        RuntimeId,
    },
    ids::{BlockId, InstanceId, TenantId},
    instance::InstanceState,
    output::BlockOutput,
};
use serde_json::json;

async fn fixture(storage: &dyn StorageBackend) -> CapsuleCutRequest {
    let sequence = SequenceBuilder::new("source-cut")
        .step("done", "noop")
        .step("charge", "http_request")
        .build();
    storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence)
        .data(json!({"secret":"ctx-cut-only"}))
        .build();
    storage.create_instance(&instance).await.unwrap();
    let claimed = storage
        .claim_due_instances(Utc::now() + Duration::seconds(1), 1, 0)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    storage
        .update_instance_state(instance.id, InstanceState::Waiting, None)
        .await
        .unwrap();
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
    storage
        .save_checkpoint(&Checkpoint {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            checkpoint_data: json!({"secret":"cp-cut-only"}),
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    storage
        .save_block_output(&BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            block_id: BlockId::new("done"),
            output: json!({"secret":"out-cut-only"}),
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    storage
        .restore_externalized_entry(&orch8_types::output::ExternalizedEntry {
            instance_id: instance.id,
            ref_key: format!("inst:{}:ctx:snapshot", instance.id),
            payload: json!({"secret":"ext-cut-only"}),
            created_at: Utc::now(),
            expires_at: Some(Utc::now() + Duration::days(1)),
        })
        .await
        .unwrap();
    CapsuleCutRequest {
        owner,
        destination_runtime_id: Some(RuntimeId::new()),
        options_sha256: "a".repeat(64),
        mobile_access: None,
        now: Utc::now(),
        expires_at: Utc::now() + Duration::minutes(5),
    }
}
async fn capture(storage: &dyn StorageBackend, request: &CapsuleCutRequest) -> CapsuleSourceCut {
    match storage.capture_capsule_cut(request).await.unwrap() {
        CapsuleCutCapture::Ready(cut) => *cut,
        CapsuleCutCapture::Refused(reason) => panic!("capture refused: {reason:?}"),
    }
}
fn manifest(cut: &CapsuleSourceCut, bytes: &[u8]) -> CapsuleManifest {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    let mut sha = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(&mut sha, "{byte:02x}").unwrap();
    }
    let source = cut.snapshot.as_ref().unwrap();
    CapsuleManifest {
        schema: orch8_types::continuity::CapsuleSchemaVersion::CURRENT,
        capsule_id: cut.id,
        continuity_id: cut.request.owner.continuity_id,
        source_instance_id: source.instance.id,
        epoch: cut.request.owner.epoch,
        tenant_id: source.instance.tenant_id.clone(),
        source_runtime_id: cut.request.owner.owner_runtime_id,
        allowed_destination_runtime_id: cut.request.destination_runtime_id,
        sequence: orch8_types::continuity::SequenceIdentity {
            id: source.sequence.id,
            version: source.sequence.version,
            content_sha256: "b".repeat(64),
        },
        checkpoint: orch8_types::continuity::CheckpointIdentity {
            block_id: BlockId::new("checkpoint"),
            sha256: "c".repeat(64),
        },
        requirements_sha256: "d".repeat(64),
        payload_artifact: orch8_types::continuity::ArtifactReference {
            key: format!("{}/{}", source.instance.id, cut.id),
            sha256: sha,
            bytes: bytes.len() as u64,
        },
        provenance_head: None,
        issued_at: cut.request.now,
        expires_at: cut.request.expires_at,
        signing_key_id: "signer".into(),
        encryption_key_id: "transfer".into(),
    }
}

orch8_storage::backend_test!(async fn capture_revokes_claims_and_retries_the_original_snapshot(b) {
    let request=fixture(b.storage.as_ref()).await;
    let cut=capture(b.storage.as_ref(),&request).await;
    let snapshot=cut.snapshot.as_ref().unwrap();
    assert_eq!(snapshot.instance.context.data,json!({"secret":"ctx-cut-only"}));assert_eq!(snapshot.outputs.len(),1);assert_eq!(snapshot.execution_state.payloads[0].payload,json!({"secret":"ext-cut-only"}));
    let current=b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap();
    assert_eq!(current.state,InstanceState::Paused);assert!(current.claimed_at.is_none());assert!(current.next_fire_at.is_none());
    let owner=b.storage.get_continuity_execution(&request.owner.tenant_id,request.owner.continuity_id).await.unwrap().unwrap();assert_eq!(owner.state,OwnershipState::Transferring);
    assert!(b.storage.update_instance_state(current.id,InstanceState::Running,None).await.is_err());
    let mut late=snapshot.outputs[0].clone();late.id=uuid::Uuid::now_v7();late.output=json!({"late":true});b.storage.save_block_output(&late).await.unwrap();
    let retry=capture(b.storage.as_ref(),&request).await;assert_eq!(retry.id,cut.id);
    assert_eq!(serde_json::to_value(retry.snapshot).unwrap(),serde_json::to_value(cut.snapshot).unwrap());
});

orch8_storage::backend_test!(async fn sealed_cut_is_immutable_and_cannot_be_locally_aborted(b) {
    let request=fixture(b.storage.as_ref()).await;let cut=capture(b.storage.as_ref(),&request).await;
    let bytes=b"sealed-payload-a";let header=manifest(&cut,bytes);
    let sealed=b.storage.seal_capsule_cut(&request.owner.tenant_id,cut.id,&header,bytes).await.unwrap();
    assert!(sealed.snapshot.is_none());assert_eq!(sealed.manifest,Some(header.clone()));assert_eq!(sealed.sealed_payload,Some(bytes.to_vec()));
    let retry=b.storage.seal_capsule_cut(&request.owner.tenant_id,cut.id,&manifest(&cut,b"another-candidate"),b"another-candidate").await.unwrap();
    assert_eq!(retry.manifest,sealed.manifest);assert_eq!(retry.sealed_payload,sealed.sealed_payload);
    assert!(!b.storage.abort_preparing_capsule_cut(&request.owner.tenant_id,cut.id).await.unwrap());
    let cached=capture(b.storage.as_ref(),&request).await;assert_eq!(cached.manifest,Some(header));
    let mut changed=request.clone();changed.destination_runtime_id=Some(RuntimeId::new());
    assert!(matches!(b.storage.capture_capsule_cut(&changed).await.unwrap(),CapsuleCutCapture::Refused(_)));
    changed=request.clone();changed.options_sha256="b".repeat(64);
    assert!(matches!(b.storage.capture_capsule_cut(&changed).await.unwrap(),CapsuleCutCapture::Refused(_)));
});

orch8_storage::backend_test!(async fn captured_validation_failure_can_release_only_an_unsealed_cut(b) {
    let request=fixture(b.storage.as_ref()).await;let cut=capture(b.storage.as_ref(),&request).await;
    assert!(b.storage.abort_preparing_capsule_cut(&request.owner.tenant_id,cut.id).await.unwrap());
    let current=b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap();assert_eq!(current.state,InstanceState::Waiting);assert!(current.claimed_at.is_none());
    let owner=b.storage.get_continuity_execution(&request.owner.tenant_id,request.owner.continuity_id).await.unwrap().unwrap();assert_eq!(owner.state,OwnershipState::Owned);
    let next=capture(b.storage.as_ref(),&request).await;assert_ne!(next.id,cut.id);
});

orch8_storage::backend_test!(async fn source_cut_never_accepts_unrecorded_or_stale_ownership(b) {
    let request=fixture(b.storage.as_ref()).await;
    for case in 0..5 {let mut wrong=request.clone();match case {0=>wrong.owner.owner_runtime_id=RuntimeId::new(),1=>wrong.owner.epoch=wrong.owner.epoch.checked_next().unwrap(),2=>wrong.owner.continuity_id=ContinuityId::new(),3=>wrong.owner.current_instance_id=InstanceId::new(),_=>wrong.owner.tenant_id=TenantId::unchecked("foreign")}assert!(matches!(b.storage.capture_capsule_cut(&wrong).await.unwrap(),CapsuleCutCapture::Refused(_)));}
    assert_eq!(b.storage.get_continuity_execution(&request.owner.tenant_id,request.owner.continuity_id).await.unwrap().unwrap().state,OwnershipState::Owned);
});

orch8_storage::backend_test!(async fn capture_recovers_after_a_lost_commit_acknowledgement(b) {
    use orch8_storage::testing::faulty::FaultStorage;
    use orch8_types::error::StorageError;
    let request=fixture(b.storage.as_ref()).await;
    let (storage,faults)=FaultStorage::wrap(b.storage.clone());
    faults.on("capture_capsule_cut").times(1).fail_after_commit(StorageError::Connection("lost capture acknowledgement".into()));
    assert!(matches!(storage.capture_capsule_cut(&request).await,Err(StorageError::Connection(_))));
    let recovered=capture(storage.as_ref(),&request).await;
    assert!(recovered.snapshot.is_some());
    let count=b.raw.count("capsule_source_cuts","capsule_id",&recovered.id.to_string()).await;assert_eq!(count,1);
    let owner=b.storage.get_continuity_execution(&request.owner.tenant_id,request.owner.continuity_id).await.unwrap().unwrap();assert_eq!(owner.state,OwnershipState::Transferring);
});

orch8_storage::backend_test!(async fn concurrent_captures_converge_on_one_cut_after_retryable_conflicts(b) {
    let request=fixture(b.storage.as_ref()).await;
    let tasks=(0..12).map(|_|{let storage=b.storage.clone();let request=request.clone();tokio::spawn(async move {storage.capture_capsule_cut(&request).await})}).collect::<Vec<_>>();
    let mut ids=Vec::new();
    for task in tasks {match task.await.unwrap() {Ok(CapsuleCutCapture::Ready(cut))=>ids.push(cut.id),Ok(CapsuleCutCapture::Refused(reason))=>panic!("same request refused: {reason:?}"),Err(error)=>assert!(error.is_transient(),"unexpected capture error: {error}")}}
    assert!(!ids.is_empty());let first=ids[0];assert!(ids.iter().all(|id|*id==first));
    let retry=capture(b.storage.as_ref(),&request).await;assert_eq!(retry.id,first);
    assert_eq!(b.raw.count("capsule_source_cuts","continuity_id",&request.owner.continuity_id.to_string()).await,1);
});

orch8_storage::backend_test!(async fn cut_insert_failure_rolls_back_owner_and_original_claim(b) {
    use orch8_storage::testing::backends::RawPool;
    let request=fixture(b.storage.as_ref()).await;
    let before=b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap();
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::query("CREATE TRIGGER fail_source_cut BEFORE INSERT ON capsule_source_cuts BEGIN SELECT RAISE(ABORT,'injected cut failure'); END").execute(pool).await.unwrap();}
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("CREATE FUNCTION fail_source_cut() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected cut failure'; END $$; CREATE TRIGGER fail_source_cut BEFORE INSERT ON capsule_source_cuts FOR EACH ROW EXECUTE FUNCTION fail_source_cut();").execute(pool).await.unwrap();}
    }
    assert!(b.storage.capture_capsule_cut(&request).await.is_err());
    let after=b.storage.get_instance(before.id).await.unwrap().unwrap();assert_eq!(serde_json::to_value(after).unwrap(),serde_json::to_value(before).unwrap());
    assert_eq!(b.storage.get_continuity_execution(&request.owner.tenant_id,request.owner.continuity_id).await.unwrap().unwrap(),request.owner);
    assert_eq!(b.raw.count("capsule_source_cuts","continuity_id",&request.owner.continuity_id.to_string()).await,0);
});

orch8_storage::backend_test!(async fn active_cuts_pin_artifacts_and_terminal_sources_until_preseal_abort(b) {
    let request=fixture(b.storage.as_ref()).await;let cut=capture(b.storage.as_ref(),&request).await;
    let id=request.owner.current_instance_id;
    b.storage.update_instance_state(id,InstanceState::Completed,None).await.unwrap();
    let cutoff=Utc::now()+Duration::days(1);
    assert!(!b.storage.list_artifact_gc_candidates(cutoff,100).await.unwrap().contains(&id));
    assert!(!b.storage.list_instance_gc_candidates(cutoff,100).await.unwrap().contains(&id));
    assert_eq!(b.storage.purge_terminal_instances(&[id]).await.unwrap(),0);
    assert!(b.storage.abort_preparing_capsule_cut(&request.owner.tenant_id,cut.id).await.unwrap());
    assert!(b.storage.list_instance_gc_candidates(cutoff,100).await.unwrap().contains(&id));
    assert_eq!(b.storage.purge_terminal_instances(&[id]).await.unwrap(),1);
});

orch8_storage::backend_test!(async fn handoff_commit_requires_matching_sealed_source_cut(b) {
    use orch8_types::continuity::{CapsuleId, ExecutionHandoff, HandoffId, HandoffState};
    let request = fixture(b.storage.as_ref()).await;
    let cut = capture(b.storage.as_ref(), &request).await;
    let quiescing = ExecutionHandoff {
        id: HandoffId::new(),
        continuity_id: request.owner.continuity_id,
        tenant_id: request.owner.tenant_id.clone(),
        source_runtime_id: request.owner.owner_runtime_id,
        destination_runtime_id: request.destination_runtime_id.unwrap(),
        expected_epoch: request.owner.epoch,
        state: HandoffState::Quiescing,
        capsule_id: None,
        placement_decision_id: None,
        preview_sha256: "a".repeat(64),
        version: 1,
        failure_code: None,
        created_at: request.now,
        updated_at: request.now,
    };
    b.storage.create_handoff(&quiescing).await.unwrap();
    let mut exported = quiescing.clone();
    exported.state = HandoffState::Exported;
    exported.capsule_id = Some(cut.id);
    exported.version += 1;
    let mut transferring = request.owner.clone();
    transferring.state = OwnershipState::Transferring;
    let tenant = &request.owner.tenant_id;
    // A captured snapshot alone does not authorize transport publication.
    assert!(!b.storage.commit_handoff_export(tenant, &quiescing, &exported, &request.owner, &transferring).await.unwrap());
    // A retry presenting the current transferring state still needs sealed proof.
    assert!(!b.storage.commit_handoff_export(tenant, &quiescing, &exported, &transferring, &transferring).await.unwrap());
    let bytes = b"sealed handoff";
    b.storage.seal_capsule_cut(tenant, cut.id, &manifest(&cut, bytes), bytes).await.unwrap();
    let mut substituted = exported.clone();
    substituted.capsule_id = Some(CapsuleId::new());
    assert!(!b.storage.commit_handoff_export(tenant, &quiescing, &substituted, &request.owner, &transferring).await.unwrap());
    substituted = exported.clone();
    substituted.destination_runtime_id = RuntimeId::new();
    assert!(!b.storage.commit_handoff_export(tenant, &quiescing, &substituted, &request.owner, &transferring).await.unwrap());
    let mut wrong_instance = request.owner.clone();
    wrong_instance.current_instance_id = InstanceId::new();
    let mut wrong_transferring = wrong_instance.clone();
    wrong_transferring.state = OwnershipState::Transferring;
    assert!(!b.storage.commit_handoff_export(tenant, &quiescing, &exported, &wrong_instance, &wrong_transferring).await.unwrap());
    assert!(b.storage.commit_handoff_export(tenant, &quiescing, &exported, &transferring, &transferring).await.unwrap());
    // Re-delivery cannot overwrite a later handoff transition.
    assert!(!b.storage.commit_handoff_export(tenant, &quiescing, &exported, &request.owner, &transferring).await.unwrap());
    assert_eq!(b.storage.get_handoff(tenant, quiescing.id).await.unwrap().unwrap(), exported);
});

#[allow(clippy::too_many_lines)] // one transaction scenario checks rollback and both ownership modes
async fn export_provenance_conflict(storage: &dyn StorageBackend, captured: bool) {
    use orch8_types::continuity::{ExecutionHandoff, HandoffId, HandoffState, ProvenanceEntry};
    let request = fixture(storage).await;
    let capsule_id = if captured {
        let cut = capture(storage, &request).await;
        let bytes = b"audited transport";
        storage
            .seal_capsule_cut(
                &request.owner.tenant_id,
                cut.id,
                &manifest(&cut, bytes),
                bytes,
            )
            .await
            .unwrap();
        cut.id
    } else {
        orch8_types::continuity::CapsuleId::new()
    };
    let quiescing = ExecutionHandoff {
        id: HandoffId::new(),
        continuity_id: request.owner.continuity_id,
        tenant_id: request.owner.tenant_id.clone(),
        source_runtime_id: request.owner.owner_runtime_id,
        destination_runtime_id: request.destination_runtime_id.unwrap(),
        expected_epoch: request.owner.epoch,
        state: HandoffState::Quiescing,
        capsule_id: None,
        placement_decision_id: None,
        preview_sha256: "a".repeat(64),
        version: 1,
        failure_code: None,
        created_at: request.now,
        updated_at: request.now,
    };

    storage.create_handoff(&quiescing).await.unwrap();
    let mut exported = quiescing.clone();
    exported.state = HandoffState::Exported;
    exported.capsule_id = Some(capsule_id);
    exported.version += 1;
    let tenant = &request.owner.tenant_id;
    let before = storage
        .get_continuity_execution(tenant, request.owner.continuity_id)
        .await
        .unwrap()
        .unwrap();
    let mut transferring = before.clone();
    transferring.state = OwnershipState::Transferring;
    transferring.updated_at += Duration::seconds(1);
    let genesis = ProvenanceEntry {
        id: uuid::Uuid::now_v7(),
        continuity_id: request.owner.continuity_id,
        tenant_id: tenant.clone(),
        epoch: request.owner.epoch,
        kind: "existing".into(),
        redacted_summary: None,
        payload_sha256: "a".repeat(64),
        previous_sha256: None,
        entry_sha256: "b".repeat(64),
        signing_key_id: None,
        signature: None,
        created_at: request.now,
    };
    storage.append_provenance(&genesis).await.unwrap();
    let mut entry = ProvenanceEntry {
        id: uuid::Uuid::now_v7(),
        kind: "capsule_exported".into(),
        entry_sha256: "c".repeat(64),
        ..genesis.clone()
    };
    // Another writer already consumed the genesis predecessor: neither owner nor
    // handoff may advance if the export audit insert cannot join the chain.
    assert!(
        storage
            .commit_handoff_export_with_provenance(
                tenant,
                &quiescing,
                &exported,
                &before,
                &transferring,
                &entry
            )
            .await
            .is_err()
    );
    assert_eq!(
        storage
            .get_handoff(tenant, quiescing.id)
            .await
            .unwrap()
            .unwrap(),
        quiescing
    );
    assert_eq!(
        storage
            .get_continuity_execution(tenant, request.owner.continuity_id)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    entry.previous_sha256 = Some(genesis.entry_sha256.clone());
    let mut foreign = entry.clone();
    foreign.tenant_id = TenantId::new("foreign-export").unwrap();
    assert!(
        storage
            .commit_handoff_export_with_provenance(
                tenant,
                &quiescing,
                &exported,
                &before,
                &transferring,
                &foreign
            )
            .await
            .is_err()
    );
    assert!(
        storage
            .commit_handoff_export_with_provenance(
                tenant,
                &quiescing,
                &exported,
                &before,
                &transferring,
                &entry
            )
            .await
            .unwrap()
    );
    assert!(
        !storage
            .commit_handoff_export_with_provenance(
                tenant,
                &quiescing,
                &exported,
                &before,
                &transferring,
                &entry
            )
            .await
            .unwrap()
    );
    assert_eq!(
        storage
            .list_provenance(tenant, request.owner.continuity_id, 100)
            .await
            .unwrap(),
        vec![genesis, entry]
    );
}
orch8_storage::backend_test!(async fn export_provenance_conflict_rolls_back_the_entire_handoff(b) {
    export_provenance_conflict(b.storage.as_ref(), true).await;
});
orch8_storage::backend_test!(async fn remote_owned_export_provenance_conflict_is_atomic(b) {
    export_provenance_conflict(b.storage.as_ref(), false).await;
});

orch8_storage::backend_test!(async fn acknowledged_cut_compacts_payload_and_never_regresses_ownership(b) {
    use orch8_storage::testing::{faulty::FaultStorage, backends::RawPool};
    use orch8_types::error::StorageError;
    let request = fixture(b.storage.as_ref()).await;
    let cut = capture(b.storage.as_ref(), &request).await;
    let tenant = &request.owner.tenant_id;
    let mut accepted = request.owner.clone();
    accepted.epoch = accepted.epoch.checked_next().unwrap();
    accepted.owner_runtime_id = request.destination_runtime_id.unwrap();
    accepted.current_instance_id = InstanceId::new();
    accepted.state = OwnershipState::Owned;
    // No destination acknowledgement can retire a snapshot that was never sealed.
    assert!(b.storage.acknowledge_capsule_cut(tenant, cut.id, &accepted).await.is_err());
    let bytes = vec![7; 1024 * 1024];
    let header = manifest(&cut, &bytes);
    b.storage.seal_capsule_cut(tenant, cut.id, &header, &bytes).await.unwrap();
    for bad in [
        ContinuityExecution { owner_runtime_id: RuntimeId::new(), ..accepted.clone() },
        ContinuityExecution { current_instance_id: request.owner.current_instance_id, ..accepted.clone() },
        ContinuityExecution { epoch: accepted.epoch.checked_next().unwrap(), ..accepted.clone() },
        ContinuityExecution { state: OwnershipState::Transferring, ..accepted.clone() },
        ContinuityExecution { continuity_id: ContinuityId::new(), ..accepted.clone() },
        ContinuityExecution { tenant_id: TenantId::new("other-account").unwrap(), ..accepted.clone() },
    ] {
        assert!(b.storage.acknowledge_capsule_cut(tenant, cut.id, &bad).await.is_err());
    }
    let (wrapped, faults) = FaultStorage::wrap(b.storage.clone());
    faults.on("acknowledge_capsule_cut").times(1).fail_after_commit(StorageError::Connection("lost destination acknowledgement".into()));
    assert!(wrapped.acknowledge_capsule_cut(tenant, cut.id, &accepted).await.is_err());
    assert_eq!(b.storage.get_continuity_execution(tenant, accepted.continuity_id).await.unwrap().unwrap(), accepted);
    let receipt = wrapped.acknowledge_capsule_cut(tenant, cut.id, &accepted).await.unwrap();
    assert_eq!(receipt.manifest, header);
    assert_eq!(receipt.accepted_owner, accepted);
    let locations = b.storage.list_continuity_locations(tenant, accepted.continuity_id, 100).await.unwrap();
    assert!(locations.iter().any(|location| location.epoch == accepted.epoch
        && location.runtime_id == accepted.owner_runtime_id && location.instance_id == accepted.current_instance_id));

    let compact: bool = match &b.raw {
        RawPool::Sqlite(pool) => sqlx::query_scalar("SELECT state='acknowledged' AND snapshot IS NULL AND sealed_payload IS NULL AND acknowledgement IS NOT NULL AND manifest IS NOT NULL FROM capsule_source_cuts WHERE capsule_id=?1").bind(cut.id.to_string()).fetch_one(pool).await.unwrap(),
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool) => sqlx::query_scalar("SELECT state='acknowledged' AND snapshot IS NULL AND sealed_payload IS NULL AND acknowledgement IS NOT NULL AND manifest IS NOT NULL FROM capsule_source_cuts WHERE capsule_id=$1").bind(cut.id.into_uuid()).fetch_one(pool).await.unwrap(),
    };
    assert!(compact);
    assert_eq!(b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap().state, InstanceState::Paused);
    assert!(b.storage.seal_capsule_cut(tenant, cut.id, &header, &bytes).await.is_err());
    assert!(!b.storage.abort_preparing_capsule_cut(tenant, cut.id).await.unwrap());
    assert!(matches!(b.storage.capture_capsule_cut(&request).await.unwrap(), CapsuleCutCapture::Refused(_)));
    let mut later = accepted.clone();
    later.epoch = later.epoch.checked_next().unwrap();
    later.owner_runtime_id = RuntimeId::new();
    assert!(b.storage.cas_continuity_owner(tenant, accepted.continuity_id, accepted.epoch, accepted.owner_runtime_id, &later).await.unwrap());
    assert_eq!(b.storage.acknowledge_capsule_cut(tenant, cut.id, &accepted).await.unwrap(), receipt);
    assert_eq!(b.storage.get_continuity_execution(tenant, later.continuity_id).await.unwrap().unwrap(), later);
    let changed = ContinuityExecution { current_instance_id: InstanceId::new(), ..accepted };
    assert!(b.storage.acknowledge_capsule_cut(tenant, cut.id, &changed).await.is_err());
});

orch8_storage::backend_test!(async fn cut_acknowledgement_preserves_already_completed_destination(b) {
    let request = fixture(b.storage.as_ref()).await;
    let cut = capture(b.storage.as_ref(), &request).await;
    let bytes = b"delayed acknowledgement";
    b.storage.seal_capsule_cut(&request.owner.tenant_id, cut.id, &manifest(&cut, bytes), bytes).await.unwrap();
    let accepted = ContinuityExecution { epoch: request.owner.epoch.checked_next().unwrap(),
        owner_runtime_id: request.destination_runtime_id.unwrap(), current_instance_id: InstanceId::new(),
        state: OwnershipState::Owned, ..request.owner.clone() };
    let completed = ContinuityExecution { state: OwnershipState::Completed, ..accepted.clone() };
    assert!(b.storage.cas_continuity_owner(&request.owner.tenant_id, request.owner.continuity_id,
        request.owner.epoch, request.owner.owner_runtime_id, &completed).await.unwrap());
    assert!(!b.storage.capsule_cut_publishable(&request.owner.tenant_id, cut.id).await.unwrap());
    b.storage.acknowledge_capsule_cut(&request.owner.tenant_id, cut.id, &accepted).await.unwrap();
    assert_eq!(b.storage.get_continuity_execution(&request.owner.tenant_id, request.owner.continuity_id).await.unwrap().unwrap(), completed);
});

async fn delayed_first_acknowledgement(storage: &dyn StorageBackend, advanced: bool) {
    let request = fixture(storage).await;
    let cut = capture(storage, &request).await;
    let bytes = b"late first acknowledgement";
    storage
        .seal_capsule_cut(
            &request.owner.tenant_id,
            cut.id,
            &manifest(&cut, bytes),
            bytes,
        )
        .await
        .unwrap();
    let accepted = ContinuityExecution {
        epoch: request.owner.epoch.checked_next().unwrap(),
        owner_runtime_id: request.destination_runtime_id.unwrap(),
        current_instance_id: InstanceId::new(),
        state: OwnershipState::Owned,
        ..request.owner.clone()
    };
    let mut latest = ContinuityExecution {
        state: OwnershipState::Transferring,
        ..accepted.clone()
    };
    assert!(
        storage
            .cas_continuity_owner(
                &request.owner.tenant_id,
                request.owner.continuity_id,
                request.owner.epoch,
                request.owner.owner_runtime_id,
                &latest
            )
            .await
            .unwrap()
    );
    if advanced {
        let next = ContinuityExecution {
            epoch: latest.epoch.checked_next().unwrap(),
            owner_runtime_id: RuntimeId::new(),
            current_instance_id: InstanceId::new(),
            state: OwnershipState::Owned,
            ..latest.clone()
        };
        assert!(
            storage
                .cas_continuity_owner(
                    &request.owner.tenant_id,
                    latest.continuity_id,
                    latest.epoch,
                    latest.owner_runtime_id,
                    &next
                )
                .await
                .unwrap()
        );
        latest = next;
    }
    let receipt = storage
        .acknowledge_capsule_cut(&request.owner.tenant_id, cut.id, &accepted)
        .await
        .unwrap();
    assert_eq!(receipt.accepted_owner, accepted);
    assert_eq!(
        storage
            .get_continuity_execution(&request.owner.tenant_id, latest.continuity_id)
            .await
            .unwrap()
            .unwrap(),
        latest
    );
}
orch8_storage::backend_test!(async fn first_ack_during_next_transfer_does_not_restore_owned_state(b) {
    delayed_first_acknowledgement(b.storage.as_ref(), false).await;
});
orch8_storage::backend_test!(async fn first_ack_after_later_epoch_does_not_rewind_ownership(b) {
    delayed_first_acknowledgement(b.storage.as_ref(), true).await;
});

orch8_storage::backend_test!(async fn artifact_retirement_tombstone_survives_cut_purge_and_rotates_claims(b) {
    use orch8_storage::testing::backends::RawPool;
    let request = fixture(b.storage.as_ref()).await;
    let cut = capture(b.storage.as_ref(), &request).await;
    let bytes = b"retired transport";
    let header = manifest(&cut, bytes);
    b.storage.seal_capsule_cut(&request.owner.tenant_id, cut.id, &header, bytes).await.unwrap();
    assert!(b.storage.capsule_cut_publishable(&request.owner.tenant_id, cut.id).await.unwrap());
    let accepted = ContinuityExecution { epoch: request.owner.epoch.checked_next().unwrap(),
        owner_runtime_id: request.destination_runtime_id.unwrap(), current_instance_id: InstanceId::new(),
        state: OwnershipState::Owned, ..request.owner.clone() };
    b.storage.acknowledge_capsule_cut(&request.owner.tenant_id, cut.id, &accepted).await.unwrap();
    assert!(!b.storage.capsule_cut_publishable(&request.owner.tenant_id, cut.id).await.unwrap());
    match &b.raw {
        RawPool::Sqlite(pool) => { sqlx::query("DELETE FROM capsule_source_cuts WHERE capsule_id=?1").bind(cut.id.to_string()).execute(pool).await.unwrap(); }
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool) => { sqlx::query("DELETE FROM capsule_source_cuts WHERE capsule_id=$1").bind(cut.id.into_uuid()).execute(pool).await.unwrap(); }
    }
    let now = Utc::now() + Duration::seconds(1);
    let retry = now + Duration::minutes(5);
    assert!(b.storage.claim_retired_capsule_artifacts(now, retry, 0).await.unwrap().is_empty());
    assert!(b.storage.claim_retired_capsule_artifacts(now, now, 1).await.is_err());
    assert_eq!(b.storage.claim_retired_capsule_artifacts(now, retry, 1).await.unwrap(), vec![header.payload_artifact.key.clone()]);
    assert!(b.storage.claim_retired_capsule_artifacts(now, retry, 1).await.unwrap().is_empty());
    assert_eq!(b.storage.claim_retired_capsule_artifacts(retry, retry + Duration::minutes(5), 1).await.unwrap(), vec![header.payload_artifact.key]);
});

#[cfg(feature = "postgres")]
#[tokio::test]
// Keep the complete downgrade/recovery lifecycle visible in this scenario.
#[allow(clippy::too_many_lines)]
async fn postgres_source_cut_downgrade_preserves_recovery_and_retirement_evidence() {
    use orch8_storage::testing::backends::TestBackend;
    let Some(b) = TestBackend::postgres().await else {
        return;
    };
    let down = include_str!("../../migrations/down/129_capsule_source_cuts.sql");
    let up = include_str!("../../migrations/129_capsule_source_cuts.sql");
    let retirement_down =
        include_str!("../../migrations/down/130_physical_instance_retirements.sql");
    let retirement_up = include_str!("../../migrations/130_physical_instance_retirements.sql");
    let pool = b.pg_pool();
    let encoding_down = include_str!("../../migrations/down/138_native_json_encoding.sql");
    let encoding_up = include_str!("../../migrations/138_native_json_encoding.sql");
    sqlx::raw_sql(include_str!(
        "../../migrations/down/139_worker_resolution_application.sql"
    ))
    .execute(pool)
    .await
    .unwrap();
    sqlx::raw_sql(encoding_down).execute(pool).await.unwrap();
    // An empty installation can roll back and reapply without changing the
    // independent continuity schema. Reapplying is safe after an ambiguous DDL ack.
    // Roll migrations back in dependency order, then restore both layers.
    let captured_down = include_str!("../../migrations/down/131_captured_source_authority.sql");
    let captured_up = include_str!("../../migrations/131_captured_source_authority.sql");
    assert!(sqlx::raw_sql(retirement_down).execute(pool).await.is_err());
    sqlx::raw_sql(captured_down).execute(pool).await.unwrap();
    sqlx::raw_sql(retirement_down).execute(pool).await.unwrap();
    sqlx::raw_sql(down).execute(pool).await.unwrap();
    sqlx::raw_sql(up).execute(pool).await.unwrap();
    sqlx::raw_sql(up).execute(pool).await.unwrap();
    sqlx::raw_sql(retirement_up).execute(pool).await.unwrap();
    sqlx::raw_sql(captured_up).execute(pool).await.unwrap();
    sqlx::raw_sql(encoding_up).execute(pool).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../migrations/139_worker_resolution_application.sql"
    ))
    .execute(pool)
    .await
    .unwrap();
    let request = fixture(b.storage.as_ref()).await;
    let cut = capture(b.storage.as_ref(), &request).await;
    assert!(sqlx::raw_sql(captured_down).execute(pool).await.is_err());
    assert!(sqlx::raw_sql(down).execute(pool).await.is_err());
    let bytes = b"recovery must survive a refused downgrade";
    let header = manifest(&cut, bytes);
    b.storage
        .seal_capsule_cut(&request.owner.tenant_id, cut.id, &header, bytes)
        .await
        .unwrap();
    assert!(sqlx::raw_sql(down).execute(pool).await.is_err());
    // A failed rollback leaves the sealed retry bytes available verbatim.
    assert_eq!(
        capture(b.storage.as_ref(), &request)
            .await
            .sealed_payload
            .as_deref(),
        Some(bytes.as_slice())
    );
    assert!(sqlx::raw_sql(captured_down).execute(pool).await.is_err());
    let accepted = ContinuityExecution {
        epoch: request.owner.epoch.checked_next().unwrap(),
        owner_runtime_id: request.destination_runtime_id.unwrap(),
        current_instance_id: InstanceId::new(),
        state: OwnershipState::Owned,
        ..request.owner.clone()
    };
    let receipt = b
        .storage
        .acknowledge_capsule_cut(&request.owner.tenant_id, cut.id, &accepted)
        .await
        .unwrap();
    assert!(sqlx::raw_sql(down).execute(pool).await.is_err());
    assert_eq!(
        b.storage
            .acknowledge_capsule_cut(&request.owner.tenant_id, cut.id, &accepted)
            .await
            .unwrap(),
        receipt
    );
    // Even after an explicit history purge, the outbox must prevent downgrade:
    // an upload that finishes late still needs its durable deletion tombstone.
    sqlx::query("DELETE FROM capsule_source_cuts WHERE capsule_id=$1")
        .bind(cut.id.into_uuid())
        .execute(pool)
        .await
        .unwrap();
    assert!(sqlx::raw_sql(down).execute(pool).await.is_err());
    let keys = b
        .storage
        .claim_retired_capsule_artifacts(
            Utc::now() + Duration::seconds(1),
            Utc::now() + Duration::minutes(5),
            1,
        )
        .await
        .unwrap();
    assert_eq!(keys, vec![header.payload_artifact.key.clone()]);
    // Artifact reconciliation cannot discard permanent physical retirement guards.
    sqlx::query("DELETE FROM capsule_artifact_retirements WHERE artifact_key=$1")
        .bind(&header.payload_artifact.key)
        .execute(pool)
        .await
        .unwrap();
    assert!(sqlx::raw_sql(down).execute(pool).await.is_err());
    assert!(sqlx::raw_sql(retirement_down).execute(pool).await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM capsule_source_cuts")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(
        b.storage
            .get_continuity_execution(&accepted.tenant_id, accepted.continuity_id)
            .await
            .unwrap()
            .unwrap(),
        accepted
    );
}

orch8_storage::backend_test!(async fn generic_owner_cas_cannot_revive_a_frozen_capsule_source(b) {
    for phase in ["captured", "sealed", "retired"] {
        let request = fixture(b.storage.as_ref()).await;
        let cut = capture(b.storage.as_ref(), &request).await;
        if phase != "captured" {
            let bytes = b"generic CAS must preserve source authority";
            b.storage.seal_capsule_cut(&request.owner.tenant_id, cut.id, &manifest(&cut, bytes), bytes).await.unwrap();
        }
        if phase == "retired" {
            // Model account retirement after a seal: transport bytes are gone,
            // but the unacknowledged source must remain fenced permanently.
            if b.kind.is_postgres() {
                sqlx::query("UPDATE capsule_source_cuts SET state='retired',snapshot=NULL,sealed_payload=NULL WHERE capsule_id=$1")
                    .bind(cut.id.into_uuid()).execute(b.pg_pool()).await.unwrap();
            } else {
                sqlx::query("UPDATE capsule_source_cuts SET state='retired',snapshot=NULL,sealed_payload=NULL WHERE capsule_id=?")
                    .bind(cut.id.to_string()).execute(b.sqlite_pool()).await.unwrap();
            }
        }
        let frozen = b.storage.get_continuity_execution(&request.owner.tenant_id, request.owner.continuity_id).await.unwrap().unwrap();
        assert_eq!(frozen.state, OwnershipState::Transferring);
        for state in [OwnershipState::Owned, OwnershipState::Completed] {
            let stale = ContinuityExecution { state, updated_at: Utc::now(), ..request.owner.clone() };
            assert!(!b.storage.cas_continuity_owner(&frozen.tenant_id, frozen.continuity_id, frozen.epoch, frozen.owner_runtime_id, &stale).await.unwrap(),
                "generic owner CAS revived a {phase} source as {state:?}");
            assert_eq!(b.storage.get_continuity_execution(&frozen.tenant_id, frozen.continuity_id).await.unwrap().unwrap(), frozen);
        }
        if phase == "captured" {
            assert!(b.storage.abort_preparing_capsule_cut(&frozen.tenant_id, cut.id).await.unwrap());
            let restored = b.storage.get_continuity_execution(&frozen.tenant_id, frozen.continuity_id).await.unwrap().unwrap();
            assert_eq!(restored.state, OwnershipState::Owned);
            assert!(b.storage.cas_continuity_owner(&restored.tenant_id, restored.continuity_id, restored.epoch, restored.owner_runtime_id, &restored).await.unwrap());
        }
    }
});

#[tokio::test]
async fn postgres_owner_cas_rechecks_cut_after_waiting_for_capture_commit() {
    use orch8_storage::testing::backends::TestBackend;
    use std::time::Duration as StdDuration;
    let Some(b) = TestBackend::postgres().await else {
        return;
    };
    let request = fixture(b.storage.as_ref()).await;
    let key = i64::from(uuid::Uuid::new_v4().as_fields().0 & 0x7fff_ffff);
    let ddl = sqlx::AssertSqlSafe(format!(
        "CREATE FUNCTION source_cut_insert_barrier() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN PERFORM pg_advisory_xact_lock({key}); RETURN NEW; END $$;
         CREATE TRIGGER source_cut_insert_barrier BEFORE INSERT ON capsule_source_cuts
         FOR EACH ROW EXECUTE FUNCTION source_cut_insert_barrier();"
    ));
    sqlx::raw_sql(ddl).execute(b.pg_pool()).await.unwrap();
    let mut gate = b.pg_pool().begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut *gate)
        .await
        .unwrap();
    let storage = b.storage.clone();
    let capture_request = request.clone();
    let capture_task =
        tokio::spawn(async move { storage.capture_capsule_cut(&capture_request).await });
    let capture_pid = tokio::time::timeout(StdDuration::from_secs(10), async {
        loop {
            let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_locks WHERE locktype='advisory' AND objid=$1::bigint::oid AND NOT granted LIMIT 1")
                .bind(key).fetch_optional(b.pg_pool()).await.unwrap();
            if let Some(pid) = pid { break pid; }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    }).await;
    let Ok(capture_pid) = capture_pid else {
        gate.rollback().await.unwrap();
        capture_task.abort();
        panic!("capture never reached its insert barrier");
    };
    let storage = b.storage.clone();
    let stale = request.owner.clone();
    let cas_task = tokio::spawn(async move {
        storage
            .cas_continuity_owner(
                &stale.tenant_id,
                stale.continuity_id,
                stale.epoch,
                stale.owner_runtime_id,
                &stale,
            )
            .await
    });
    let cas_waited = tokio::time::timeout(StdDuration::from_secs(10), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE '%continuity_executions%')")
                .bind(capture_pid).fetch_one(b.pg_pool()).await.unwrap();
            if waiting { break; }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    }).await.is_ok();
    gate.rollback().await.unwrap();
    let captured = tokio::time::timeout(StdDuration::from_secs(10), capture_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let changed = tokio::time::timeout(StdDuration::from_secs(10), cas_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(cas_waited, "CAS did not wait on capture's ownership row");
    assert!(matches!(captured, CapsuleCutCapture::Ready(_)));
    assert!(
        !changed,
        "CAS used a pre-capture snapshot to revive the source"
    );
    assert_eq!(
        b.storage
            .get_continuity_execution(&request.owner.tenant_id, request.owner.continuity_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        OwnershipState::Transferring
    );
}

orch8_storage::backend_test!(async fn generic_owner_cas_rejects_record_identity_mismatch(b) {
    let request = fixture(b.storage.as_ref()).await;
    for foreign in [
        ContinuityExecution { tenant_id: TenantId::new("foreign").unwrap(), ..request.owner.clone() },
        ContinuityExecution { continuity_id: ContinuityId::new(), ..request.owner.clone() },
    ] {
        assert!(b.storage.cas_continuity_owner(&request.owner.tenant_id, request.owner.continuity_id,
            request.owner.epoch, request.owner.owner_runtime_id, &foreign).await.is_err());
        assert_eq!(b.storage.get_continuity_execution(&request.owner.tenant_id,
            request.owner.continuity_id).await.unwrap().unwrap(), request.owner);
    }
});

orch8_storage::backend_test!(async fn acknowledged_source_cannot_become_owner_again(b) {
    let request=fixture(b.storage.as_ref()).await;
    let cut=capture(b.storage.as_ref(),&request).await;
    let bytes=b"permanently retired physical source";
    b.storage.seal_capsule_cut(&request.owner.tenant_id,cut.id,&manifest(&cut,bytes),bytes).await.unwrap();
    let accepted=ContinuityExecution {
        current_instance_id:InstanceId::new(),owner_runtime_id:request.destination_runtime_id.unwrap(),
        epoch:request.owner.epoch.checked_next().unwrap(),state:OwnershipState::Owned,updated_at:Utc::now(),..request.owner.clone()
    };
    b.storage.acknowledge_capsule_cut(&accepted.tenant_id,cut.id,&accepted).await.unwrap();
    for epoch in [accepted.epoch,accepted.epoch.checked_next().unwrap()] {
        for state in [OwnershipState::Owned,OwnershipState::Completed] {
            let revived=ContinuityExecution {current_instance_id:request.owner.current_instance_id,epoch,state,updated_at:Utc::now(),..accepted.clone()};
            assert!(!b.storage.cas_continuity_owner(&accepted.tenant_id,accepted.continuity_id,accepted.epoch,accepted.owner_runtime_id,&revived).await.unwrap(),"acknowledged source was revived at {epoch:?} as {state:?}");
            assert_eq!(b.storage.get_continuity_execution(&accepted.tenant_id,accepted.continuity_id).await.unwrap().unwrap(),accepted);
        }
    }
    let later=ContinuityExecution {current_instance_id:InstanceId::new(),epoch:accepted.epoch.checked_next().unwrap(),owner_runtime_id:RuntimeId::new(),updated_at:Utc::now(),..accepted.clone()};
    assert!(b.storage.cas_continuity_owner(&accepted.tenant_id,accepted.continuity_id,accepted.epoch,accepted.owner_runtime_id,&later).await.unwrap(),"a fresh destination remains eligible");
    let revived=ContinuityExecution {current_instance_id:request.owner.current_instance_id,epoch:later.epoch.checked_next().unwrap(),..later.clone()};
    assert!(!b.storage.cas_continuity_owner(&later.tenant_id,later.continuity_id,later.epoch,later.owner_runtime_id,&revived).await.unwrap());
    assert_eq!(b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap().state,InstanceState::Paused);
});

// Full source history can be collected after a successful move. Its physical
// identity must remain retired independently of either native row or continuity.
orch8_storage::backend_test!(async fn acknowledged_physical_source_cannot_be_recreated_after_history_gc(b) {
    let request = fixture(b.storage.as_ref()).await;
    let mut source = b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap();
    let cut = capture(b.storage.as_ref(), &request).await;
    let bytes = b"acknowledged source must never become a fresh execution";
    b.storage.seal_capsule_cut(&request.owner.tenant_id, cut.id, &manifest(&cut, bytes), bytes).await.unwrap();
    let destination_id = InstanceId::new();
    let mut destination = source.clone();
    destination.id = destination_id;
    destination.state = InstanceState::Completed;
    destination.claimed_at = None;
    b.storage.create_instance(&destination).await.unwrap();
    let accepted = ContinuityExecution {
        current_instance_id: destination_id,
        owner_runtime_id: request.destination_runtime_id.unwrap(),
        epoch: request.owner.epoch.checked_next().unwrap(),
        state: OwnershipState::Completed,
        updated_at: Utc::now(),
        ..request.owner.clone()
    };
    b.storage.acknowledge_capsule_cut(&accepted.tenant_id, cut.id, &accepted).await.unwrap();
    b.storage.update_instance_state(source.id, InstanceState::Cancelled, None).await.unwrap();
    assert_eq!(b.storage.purge_terminal_instances(&[source.id, destination_id]).await.unwrap(), 2);
    assert!(b.storage.get_instance(source.id).await.unwrap().is_none());
    assert!(b.storage.get_continuity_execution(&accepted.tenant_id, accepted.continuity_id).await.unwrap().is_none());
    source.state = InstanceState::Scheduled;
    source.claimed_at = None;
    source.next_fire_at = Some(Utc::now());
    let result = b.storage.create_instance(&source).await;
    assert!(result.is_err(), "acknowledged physical ID was recreated after source and destination history GC");
    assert!(b.storage.get_instance(source.id).await.unwrap().is_none(), "refusal must not publish native context");
    assert!(b.storage.create_instance_admitted(&source, 10).await.is_err());
    let snapshot = orch8_storage::restore::InstanceSnapshot {
        instance: source.clone(), nodes: vec![], outputs: vec![], payloads: vec![],
    };
    assert!(b.storage.restore_instance_snapshot(&snapshot).await.is_err());
    let mut fresh = source.clone(); fresh.id = InstanceId::new();
    assert!(b.storage.create_instances_batch(&[fresh.clone(), source.clone()]).await.is_err());
    assert!(b.storage.get_instance(fresh.id).await.unwrap().is_none(), "batch refusal must be atomic");
    for tenant in [source.tenant_id.clone(), TenantId::new("foreign-retirement-scope").unwrap()] {
        let revived = ContinuityExecution {
            continuity_id: ContinuityId::new(), tenant_id: tenant,
            current_instance_id: source.id, epoch: ExecutionEpoch::initial(),
            owner_runtime_id: RuntimeId::new(), state: OwnershipState::Owned, updated_at: Utc::now(),
        };
        assert!(b.storage.create_continuity_execution(&revived).await.is_err(), "retirement must cover every continuity and tenant");
        assert!(b.storage.get_continuity_execution(&revived.tenant_id, revived.continuity_id).await.unwrap().is_none());
    }
    // Retiring one identity must leave a genuinely fresh invocation usable.
    source.id = InstanceId::new();
    b.storage.create_instance(&source).await.unwrap();
});

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_native_insert_rechecks_retirement_after_unique_wait() {
    use orch8_storage::testing::backends::TestBackend;
    use std::time::Duration as StdDuration;
    let Some(b) = TestBackend::postgres().await else {
        return;
    };
    let sequence = SequenceBuilder::new("physical-retirement-wait")
        .step("run", "noop")
        .build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).build();
    b.storage.create_instance(&instance).await.unwrap();
    let mut tx = b.pg_pool().begin().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM task_instances WHERE id=$1")
        .bind(instance.id.into_uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO physical_instance_retirements VALUES($1,$2,$3,$4,$5)")
        .bind(instance.id.into_uuid())
        .bind(instance.tenant_id.as_str())
        .bind(uuid::Uuid::new_v4())
        .bind(uuid::Uuid::new_v4())
        .bind(Utc::now())
        .execute(&mut *tx)
        .await
        .unwrap();
    let storage = b.storage.clone();
    let replay = instance.clone();
    let insertion = tokio::spawn(async move { storage.create_instance(&replay).await });
    let waited = tokio::time::timeout(StdDuration::from_secs(10), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE '%INSERT INTO task_instances%')")
                .bind(pid).fetch_one(b.pg_pool()).await.unwrap();
            if waiting { break; }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    }).await.is_ok();
    tx.commit().await.unwrap();
    let result = tokio::time::timeout(StdDuration::from_secs(10), insertion)
        .await
        .unwrap()
        .unwrap();
    assert!(
        waited,
        "insertion must reach its uniqueness wait before retirement commits"
    );
    assert!(
        result.is_err(),
        "post-wait insert missed committed retirement evidence"
    );
    assert!(b.storage.get_instance(instance.id).await.unwrap().is_none());
}

#[tokio::test]
async fn sqlite_retirement_upgrade_retries_failed_backfill_but_skips_completed_history() {
    use orch8_storage::{ContinuityStore, InstanceStore, sqlite::SqliteStorage};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("physical-lifetime.db");
    let path = path.to_str().unwrap();
    let storage = SqliteStorage::file(path).await.unwrap();
    // Model a retained pre-upgrade cut without dropping existing evidence.
    sqlx::raw_sql(
        "DELETE FROM schema_versions WHERE version IN (60,61);
        DROP TRIGGER physical_retirement_from_cut;
        DROP TRIGGER physical_retirement_from_inserted_cut;",
    )
    .execute(storage.pool())
    .await
    .unwrap();
    let request = fixture(&storage).await;
    let cut = capture(&storage, &request).await;
    let bytes = b"old source cut requiring upgrade";
    storage
        .seal_capsule_cut(
            &request.owner.tenant_id,
            cut.id,
            &manifest(&cut, bytes),
            bytes,
        )
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE TRIGGER backfill_failure BEFORE INSERT ON physical_instance_retirements
        BEGIN SELECT RAISE(ABORT,'test interruption before retirement backfill completes'); END;",
    )
    .execute(storage.pool())
    .await
    .unwrap();
    storage.close().await;
    assert!(
        SqliteStorage::file(path).await.is_err(),
        "failed security backfill must refuse open"
    );
    let raw = sqlx::SqlitePool::connect(&format!("sqlite://{path}"))
        .await
        .unwrap();
    let completed: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM schema_versions WHERE version IN (60,61)")
            .fetch_one(&raw)
            .await
            .unwrap();
    let markers: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM physical_instance_retirements")
        .fetch_one(&raw)
        .await
        .unwrap();
    assert_eq!(
        (completed, markers),
        (0, 0),
        "failed backfill cannot publish a completion marker or partial evidence"
    );
    sqlx::query("DROP TRIGGER backfill_failure")
        .execute(&raw)
        .await
        .unwrap();
    raw.close().await;
    let storage = SqliteStorage::file(path).await.unwrap();
    let markers: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM physical_instance_retirements WHERE instance_id=?1",
    )
    .bind(request.owner.current_instance_id.to_string())
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(
        markers, 1,
        "retry must backfill the retained source identity"
    );
    // A duplicate backfill insertion would hit this trigger even with OR IGNORE.
    // Reopen must rely on durable migration completion, not rescan/reinsert history.
    sqlx::raw_sql("CREATE TRIGGER completed_backfill_must_not_repeat BEFORE INSERT ON physical_instance_retirements
        BEGIN SELECT RAISE(ABORT,'completed security backfill was repeated'); END;")
        .execute(storage.pool()).await.unwrap();
    storage.close().await;
    let reopened = SqliteStorage::file(path)
        .await
        .expect("completed retirement backfill must not repeat on reopen");
    assert!(
        reopened
            .get_instance(request.owner.current_instance_id)
            .await
            .unwrap()
            .is_some()
    );
    reopened.close().await;
}

async fn insert_pre_guard_pending(
    backend: &orch8_storage::testing::backends::TestBackend,
    task: &orch8_types::worker::WorkerTask,
) {
    use orch8_storage::testing::backends::RawPool;
    match &backend.raw {
        RawPool::Sqlite(pool) => {
            sqlx::query("DROP TRIGGER physical_source_worker_enqueue")
                .execute(pool)
                .await
                .unwrap();
            backend.storage.create_worker_task(task).await.unwrap();
            sqlx::raw_sql(include_str!("../src/sqlite/physical_retirement.sql"))
                .execute(pool)
                .await
                .unwrap();
        }
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => {
            sqlx::query("ALTER TABLE worker_tasks DISABLE TRIGGER physical_source_worker_enqueue")
                .execute(pool)
                .await
                .unwrap();
            backend.storage.create_worker_task(task).await.unwrap();
            sqlx::query("ALTER TABLE worker_tasks ENABLE TRIGGER physical_source_worker_enqueue")
                .execute(pool)
                .await
                .unwrap();
        }
    }
}

async fn restore_pre_guard_source_schedule(
    backend: &orch8_storage::testing::backends::TestBackend,
    id: InstanceId,
) {
    // Simulate an inconsistent pre-guard row; retain the independent marker
    // and reinstall the guard before testing operational query eligibility.
    match &backend.raw {
        orch8_storage::testing::backends::RawPool::Sqlite(pool) => {
            let mut tx = pool.begin().await.unwrap();
            sqlx::raw_sql("DROP TRIGGER physical_retirement_native_update; DROP TRIGGER captured_source_native_update;")
                .execute(&mut *tx)
                .await
                .unwrap();
            sqlx::query(
                "UPDATE task_instances SET state='scheduled',next_fire_at=NULL WHERE id=?1",
            )
            .bind(id.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
            sqlx::raw_sql(include_str!("../src/sqlite/physical_retirement.sql"))
                .execute(&mut *tx)
                .await
                .unwrap();
            sqlx::raw_sql(include_str!("../src/sqlite/captured_source.sql"))
                .execute(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
        #[cfg(feature = "postgres")]
        orch8_storage::testing::backends::RawPool::Postgres(pool) => {
            let mut tx = pool.begin().await.unwrap();
            sqlx::raw_sql(
                "ALTER TABLE task_instances DISABLE TRIGGER physical_retirement_native_update; ALTER TABLE task_instances DISABLE TRIGGER captured_source_native_update;",
            )
            .execute(&mut *tx)
            .await
            .unwrap();
            sqlx::query(
                "UPDATE task_instances SET state='scheduled',next_fire_at=NULL WHERE id=$1",
            )
            .bind(id.into_uuid())
            .execute(&mut *tx)
            .await
            .unwrap();
            sqlx::raw_sql(
                "ALTER TABLE task_instances ENABLE TRIGGER physical_retirement_native_update; ALTER TABLE task_instances ENABLE TRIGGER captured_source_native_update;",
            )
            .execute(&mut *tx)
            .await
            .unwrap();
            tx.commit().await.unwrap();
        }
    }
}

async fn legacy_retired_source(
    backend: &orch8_storage::testing::backends::TestBackend,
) -> (CapsuleCutRequest, orch8_types::worker::WorkerTask) {
    let storage = backend.storage.as_ref();
    let request = fixture(storage).await;
    let cut = capture(storage, &request).await;
    // Inject pre-guard state, restoring both native guards before exercising
    // retirement, history GC and every operational polling path.
    restore_pre_guard_source_schedule(backend, request.owner.current_instance_id).await;
    let pending: orch8_types::worker::WorkerTask = serde_json::from_value(json!({
        "id": uuid::Uuid::new_v4(), "instance_id": request.owner.current_instance_id,
        "block_id": "legacy", "handler_name": "retired_source", "params": {}, "context": {},
        "attempt": 0, "state": "pending", "created_at": Utc::now()
    }))
    .unwrap();
    insert_pre_guard_pending(backend, &pending).await;
    let mut destination = storage
        .get_instance(request.owner.current_instance_id)
        .await
        .unwrap()
        .unwrap();
    destination.id = InstanceId::new();
    destination.state = InstanceState::Completed;
    destination.claimed_at = None;
    storage.create_instance(&destination).await.unwrap();
    let bytes = b"legacy writer must not resurrect acknowledged work";
    storage
        .seal_capsule_cut(
            &request.owner.tenant_id,
            cut.id,
            &manifest(&cut, bytes),
            bytes,
        )
        .await
        .unwrap();
    let accepted = ContinuityExecution {
        current_instance_id: destination.id,
        epoch: request.owner.epoch.checked_next().unwrap(),
        owner_runtime_id: request.destination_runtime_id.unwrap(),
        state: OwnershipState::Completed,
        updated_at: Utc::now(),
        ..request.owner.clone()
    };
    storage
        .acknowledge_capsule_cut(&accepted.tenant_id, cut.id, &accepted)
        .await
        .unwrap();
    assert_eq!(
        storage
            .purge_terminal_instances(&[destination.id])
            .await
            .unwrap(),
        1
    );
    assert!(
        storage
            .get_continuity_execution(&accepted.tenant_id, accepted.continuity_id)
            .await
            .unwrap()
            .is_none()
    );
    restore_pre_guard_source_schedule(backend, request.owner.current_instance_id).await;
    assert!(
        storage
            .is_physical_instance_retired(request.owner.current_instance_id)
            .await
            .unwrap()
    );
    assert!(
        !storage
            .is_physical_instance_retired(InstanceId::new())
            .await
            .unwrap()
    );
    (request, pending)
}

orch8_storage::backend_test!(async fn retired_source_history_gc_cannot_restore_native_claim_eligibility(b) {
    let (request, _) = legacy_retired_source(&b).await;
    let original = b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap();
    for per_tenant in [0, 1] {
        let mut fresh = original.clone(); fresh.id = InstanceId::new();
        b.storage.create_instance(&fresh).await.unwrap();
        let claimed = b.storage.claim_due_instances(Utc::now() + Duration::seconds(1), 10, per_tenant).await.unwrap();
        assert_eq!(claimed.iter().map(|i| i.id).collect::<Vec<_>>(), vec![fresh.id], "retirement must survive loss of continuity lookup and must not block healthy work");
    }
    assert_eq!(b.storage.get_instance(original.id).await.unwrap().unwrap().claimed_at, original.claimed_at);
});

orch8_storage::backend_test!(async fn retired_source_history_gc_fences_every_worker_poll_path(b) {
    let (request, pending) = legacy_retired_source(&b).await;
    assert!(b.storage.claim_worker_tasks("retired_source", "worker", 10).await.unwrap().is_empty());
    assert!(b.storage.claim_worker_tasks_for_tenant("retired_source", "worker", &request.owner.tenant_id, 10).await.unwrap().is_empty());
    let now = Utc::now();
    let capabilities = serde_json::from_value(json!({"runtime_id": RuntimeId::new(), "kind": "server", "trust": "registered", "observed_at": now, "expires_at": now + Duration::minutes(5)})).unwrap();
    for tenant in [None, Some(&request.owner.tenant_id)] {
        assert!(b.storage.claim_worker_tasks_matching("retired_source", "worker", tenant, None, &capabilities, 10).await.unwrap().is_empty());
    }
    assert_eq!(b.storage.get_worker_task(pending.id).await.unwrap().unwrap().state, orch8_types::worker::WorkerTaskState::Pending, "refusal must retain uncertain legacy task evidence");
    let mut healthy = b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap();
    healthy.id = InstanceId::new(); b.storage.create_instance(&healthy).await.unwrap();
    let mut control = pending.clone(); control.id = uuid::Uuid::new_v4(); control.instance_id = healthy.id;
    b.storage.create_worker_task(&control).await.unwrap();
    let claimed = b.storage.claim_worker_tasks("retired_source", "worker", 10).await.unwrap();
    assert_eq!(claimed.iter().map(|t|t.id).collect::<Vec<_>>(),vec![control.id], "retired work must not disable a healthy worker queue");
});

async fn captured_source_with_late_queue(
    backend: &orch8_storage::testing::backends::TestBackend,
) -> (
    CapsuleCutRequest,
    CapsuleSourceCut,
    orch8_types::worker::WorkerTask,
) {
    let storage = backend.storage.as_ref();
    let request = fixture(storage).await;
    let cut = capture(storage, &request).await;
    // Inject an inconsistent pre-guard row, then reinstall database guards
    // before proving operational claims still skip it.
    restore_pre_guard_source_schedule(backend, request.owner.current_instance_id).await;
    let task = serde_json::from_value(json!({"id":uuid::Uuid::new_v4(), "instance_id":request.owner.current_instance_id,
        "block_id":"late-queue", "handler_name":"captured-source", "params":{},"context":{},"attempt":0,"state":"pending","created_at":Utc::now()})).unwrap();
    insert_pre_guard_pending(backend, &task).await;
    (request, cut, task)
}

orch8_storage::backend_test!(async fn captured_source_is_ineligible_for_native_claim_before_seal(b) {
    let (request, _, _) = captured_source_with_late_queue(&b).await;
    let source = b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap();
    for cap in [0, 1] {
        let mut fresh = source.clone(); fresh.id = InstanceId::new();
        b.storage.create_instance(&fresh).await.unwrap();
        let claimed = b.storage.claim_due_instances(Utc::now() + Duration::seconds(1), 10, cap).await.unwrap();
        assert_eq!(claimed.iter().map(|i|i.id).collect::<Vec<_>>(),vec![fresh.id],"captured sources must not regain a native claim before seal or abort");
    }
});

orch8_storage::backend_test!(async fn captured_source_is_ineligible_for_worker_claim_before_seal(b) {
    let (request, _, pending) = captured_source_with_late_queue(&b).await;
    assert!(b.storage.claim_worker_tasks("captured-source", "worker", 10).await.unwrap().is_empty());
    assert!(b.storage.claim_worker_tasks_for_tenant("captured-source", "worker", &request.owner.tenant_id, 10).await.unwrap().is_empty());
    let now = Utc::now();
    let capabilities = serde_json::from_value(json!({"runtime_id":RuntimeId::new(),"kind":"server","trust":"registered","observed_at":now,"expires_at":now+Duration::minutes(5)})).unwrap();
    for tenant in [None,Some(&request.owner.tenant_id)] {
        assert!(b.storage.claim_worker_tasks_matching("captured-source","worker",tenant,None,&capabilities,10).await.unwrap().is_empty());
    }
    assert_eq!(b.storage.get_worker_task(pending.id).await.unwrap().unwrap().state,orch8_types::worker::WorkerTaskState::Pending);
});

orch8_storage::backend_test!(async fn permanently_retired_source_refuses_forward_state_writes(b) {
    let request = fixture(b.storage.as_ref()).await;
    let cut = capture(b.storage.as_ref(), &request).await;
    let bytes = b"native forward mutation must remain fenced";
    b.storage.seal_capsule_cut(&request.owner.tenant_id,cut.id,&manifest(&cut,bytes),bytes).await.unwrap();
    let frozen = b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap();
    for state in [InstanceState::Scheduled,InstanceState::Running,InstanceState::Waiting] {
        assert!(b.storage.update_instance_state(frozen.id,state,Some(Utc::now())).await.is_err(),"retired source accepted forward state {state:?}");
        assert_eq!(b.storage.get_instance(frozen.id).await.unwrap().unwrap().state,frozen.state);
    }
    assert!(!matches!(b.storage.conditional_update_instance_state(frozen.id,InstanceState::Paused,InstanceState::Running,None).await,Ok(true)));
    // Outcome/evidence cleanup remains possible; this is not forward admission.
    b.storage.update_instance_state(frozen.id,InstanceState::Cancelled,None).await.unwrap();
});

async fn try_legacy_worker_claim(
    backend: &orch8_storage::testing::backends::TestBackend,
    id: uuid::Uuid,
) -> bool {
    use orch8_storage::testing::backends::RawPool;
    match &backend.raw {
        RawPool::Sqlite(pool) => {
            let mut tx = pool.begin().await.unwrap();
            let refused = sqlx::query("UPDATE worker_tasks SET state='claimed',worker_id='older-writer',claim_epoch=claim_epoch+1 WHERE id=?1")
                .bind(id.to_string()).execute(&mut *tx).await.is_err();
            tx.rollback().await.unwrap();
            refused
        }
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => {
            let mut tx = pool.begin().await.unwrap();
            let refused = sqlx::query("UPDATE worker_tasks SET state='claimed',worker_id='older-writer',claim_epoch=claim_epoch+1 WHERE id=$1")
                .bind(id).execute(&mut *tx).await.is_err();
            tx.rollback().await.unwrap();
            refused
        }
    }
}

orch8_storage::backend_test!(async fn database_guard_blocks_legacy_captured_worker_claim_and_abort_releases_it(b) {
    let (request, cut, pending) = captured_source_with_late_queue(&b).await;
    assert!(try_legacy_worker_claim(&b,pending.id).await,"older worker UPDATE bypassed a captured source fence");
    let saved = b.storage.get_worker_task(pending.id).await.unwrap().unwrap();
    assert_eq!(saved.state,orch8_types::worker::WorkerTaskState::Pending); assert_eq!(saved.claim_epoch,0);
    assert!(b.storage.abort_preparing_capsule_cut(&request.owner.tenant_id,cut.id).await.unwrap());
    assert!(!b.storage.is_physical_instance_retired(request.owner.current_instance_id).await.unwrap());
    let released = b.storage.claim_worker_tasks("captured-source","worker",1).await.unwrap();
    assert_eq!(released.iter().map(|t|t.id).collect::<Vec<_>>(),vec![pending.id],"explicit unsealed abort must preserve normal execution");
});

orch8_storage::backend_test!(async fn database_guard_blocks_legacy_retired_worker_claim_after_history_gc(b) {
    let (_, pending) = legacy_retired_source(&b).await;
    assert!(try_legacy_worker_claim(&b,pending.id).await,"older worker UPDATE bypassed permanent retirement after history GC");
    let saved = b.storage.get_worker_task(pending.id).await.unwrap().unwrap();
    assert_eq!(saved.state,orch8_types::worker::WorkerTaskState::Pending); assert_eq!(saved.claim_epoch,0);
});

orch8_storage::backend_test!(async fn mixed_batch_reschedule_preserves_frozen_sources_and_healthy_work(b) {
    for sealed in [false,true] {
        let request = fixture(b.storage.as_ref()).await;
        let cut = capture(b.storage.as_ref(),&request).await;
        if sealed {
            let bytes = b"bulk rescheduling must skip frozen sources";
            b.storage.seal_capsule_cut(&request.owner.tenant_id,cut.id,&manifest(&cut,bytes),bytes).await.unwrap();
        }
        let frozen = b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap();
        let mut fresh = frozen.clone();fresh.id=InstanceId::new();
        b.storage.create_instance(&fresh).await.unwrap();
        b.storage.batch_reschedule_instances(&[frozen.id,fresh.id],Utc::now()).await.unwrap();
        assert_eq!(b.storage.get_instance(frozen.id).await.unwrap().unwrap().state,InstanceState::Paused,"batch reschedule changed frozen source");
        assert_eq!(b.storage.get_instance(fresh.id).await.unwrap().unwrap().state,InstanceState::Scheduled,"frozen source must not disable healthy batch work");
        b.storage.update_instance_state(fresh.id,InstanceState::Cancelled,None).await.unwrap();
    }
});

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_forward_write_rechecks_retirement_after_quarantine_row_wait() {
    use orch8_storage::testing::backends::TestBackend;
    use std::time::Duration as StdDuration;
    let Some(b) = TestBackend::postgres().await else {
        return;
    };
    let sequence = SequenceBuilder::new("retirement-forward-wait")
        .step("run", "noop")
        .build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Waiting)
        .build();
    b.storage.create_instance(&instance).await.unwrap();
    let mut tx = b.pg_pool().begin().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO physical_instance_retirements VALUES($1,$2,$3,$4,$5)")
        .bind(instance.id.into_uuid())
        .bind(instance.tenant_id.as_str())
        .bind(uuid::Uuid::new_v4())
        .bind(uuid::Uuid::new_v4())
        .bind(Utc::now())
        .execute(&mut *tx)
        .await
        .unwrap();
    let storage = b.storage.clone();
    let id = instance.id;
    let forward = tokio::spawn(async move {
        storage
            .update_instance_state(id, InstanceState::Scheduled, Some(Utc::now()))
            .await
    });
    let waited = tokio::time::timeout(StdDuration::from_secs(10),async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE '%UPDATE task_instances%')")
                .bind(pid).fetch_one(b.pg_pool()).await.unwrap();
            if blocked {break}
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    }).await.is_ok();
    tx.commit().await.unwrap();
    let result = tokio::time::timeout(StdDuration::from_secs(10), forward)
        .await
        .unwrap()
        .unwrap();
    assert!(
        waited,
        "forward write did not wait for retirement quarantine"
    );
    assert!(
        result.is_err(),
        "post-wait native writer missed permanent retirement"
    );
    let frozen = b.storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(frozen.state, InstanceState::Paused);
    assert!(frozen.claimed_at.is_none());
    assert!(frozen.next_fire_at.is_none());
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_capture_cannot_miss_worker_committed_while_waiting_for_parent() {
    use orch8_storage::testing::backends::TestBackend;
    use std::time::Duration as StdDuration;
    let Some(b) = TestBackend::postgres().await else {
        return;
    };
    let request = fixture(b.storage.as_ref()).await;
    let id = uuid::Uuid::new_v4();
    let mut producer = b.pg_pool().begin().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *producer)
        .await
        .unwrap();
    sqlx::query("INSERT INTO worker_tasks(id,instance_id,tenant_id,block_id,handler_name) VALUES($1,$2,$3,'pending-before-cut','capture-snapshot-race')")
        .bind(id).bind(request.owner.current_instance_id.into_uuid()).bind(request.owner.tenant_id.as_str())
        .execute(&mut *producer).await.unwrap();
    let storage = b.storage.clone();
    let attempt = request.clone();
    let capture = tokio::spawn(async move { storage.capture_capsule_cut(&attempt).await });
    let waited = tokio::time::timeout(StdDuration::from_secs(10),async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE '%task_instances%FOR UPDATE%')")
                .bind(pid).fetch_one(b.pg_pool()).await.unwrap();
            if blocked {break}
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    }).await.is_ok();
    producer.commit().await.unwrap();
    let outcome = tokio::time::timeout(StdDuration::from_secs(10), capture)
        .await
        .unwrap()
        .unwrap();
    assert!(
        waited,
        "capture must reach the producer's parent-row lock before commit"
    );
    assert!(
        !matches!(outcome, Ok(CapsuleCutCapture::Ready(_))),
        "repeatable-read capture missed a worker committed while its parent lock waited"
    );
    assert!(matches!(
        b.storage.capture_capsule_cut(&request).await.unwrap(),
        CapsuleCutCapture::Refused(
            orch8_storage::capsule_export::CapsuleCutRefusal::WorkerTasksInFlight(1)
        )
    ));
    assert_eq!(
        b.storage
            .get_continuity_execution(&request.owner.tenant_id, request.owner.continuity_id)
            .await
            .unwrap()
            .unwrap(),
        request.owner
    );
    assert_eq!(
        b.storage.get_worker_task(id).await.unwrap().unwrap().state,
        orch8_types::worker::WorkerTaskState::Pending
    );
}

orch8_storage::backend_test!(async fn frozen_source_rejects_new_enqueue_but_unsealed_abort_restores_admission(b) {
    for sealed in [false,true] {
        let request = fixture(b.storage.as_ref()).await;
        let cut = capture(b.storage.as_ref(),&request).await;
        if sealed {
            let bytes=b"enqueue remains frozen after seal";
            b.storage.seal_capsule_cut(&request.owner.tenant_id,cut.id,&manifest(&cut,bytes),bytes).await.unwrap();
        }
        let task: orch8_types::worker::WorkerTask=serde_json::from_value(json!({"id":uuid::Uuid::new_v4(),"instance_id":request.owner.current_instance_id,
            "block_id":"new-dispatch","handler_name":"new-after-cut","params":{},"context":{},"attempt":0,"state":"pending","created_at":Utc::now()})).unwrap();
        assert!(b.storage.create_worker_task(&task).await.is_err(),"frozen source accepted a new pending dispatch");
        assert!(b.storage.get_worker_task(task.id).await.unwrap().is_none(),"refusal must not publish partial dispatch state");
        if sealed {
            assert!(!b.storage.abort_preparing_capsule_cut(&request.owner.tenant_id,cut.id).await.unwrap());
        } else {
            assert!(b.storage.abort_preparing_capsule_cut(&request.owner.tenant_id,cut.id).await.unwrap());
            b.storage.create_worker_task(&task).await.unwrap();
            assert!(b.storage.get_worker_task(task.id).await.unwrap().is_some());
        }
    }
});

#[cfg(feature = "postgres")]
async fn flat_retry_task_fixture(
    b: &orch8_storage::testing::backends::TestBackend,
) -> orch8_types::worker::WorkerTask {
    let request = fixture(b.storage.as_ref()).await;
    // Model a dispatched flat invocation: owner epoch and durable run/step
    // must already exist before a retry can target the native parent.
    b.storage
        .delete_execution_tree(request.owner.current_instance_id)
        .await
        .unwrap();
    let mut native = b
        .storage
        .get_instance(request.owner.current_instance_id)
        .await
        .unwrap()
        .unwrap();
    native.context.runtime.run_id = Some(uuid::Uuid::new_v4().to_string());
    native.context.runtime.current_step = Some(BlockId::new("charge"));
    native.context.runtime.current_step_started_at = Some(Utc::now());
    b.storage
        .update_instance_context(native.id, &native.context)
        .await
        .unwrap();
    let task: orch8_types::worker::WorkerTask=serde_json::from_value(json!({"id":uuid::Uuid::new_v4(),"instance_id":request.owner.current_instance_id,"block_id":"charge",
        "handler_name":"retry-lock-order","params":{},"context":{},"attempt":0,"continuity_epoch":request.owner.epoch.get(),"state":"pending","created_at":Utc::now()})).unwrap();
    b.storage.create_worker_task(&task).await.unwrap();
    task
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_worker_retry_and_duplicate_enqueue_keep_parent_before_worker_lock_order() {
    use orch8_storage::testing::backends::TestBackend;
    use orch8_types::worker::{
        WorkerAttemptEventKind, WorkerTaskResolution, WorkerTaskResolutionAction, WorkerTaskState,
    };
    use std::time::Duration as StdDuration;
    let Some(b) = TestBackend::postgres().await else {
        return;
    };
    let task = flat_retry_task_fixture(&b).await;
    let key = i64::from(uuid::Uuid::new_v4().as_fields().0 & 0x7fff_ffff);
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE FUNCTION pause_duplicate_enqueue() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.id='{}'::uuid THEN PERFORM pg_advisory_xact_lock({key}); END IF; RETURN NEW; END $$;
        CREATE TRIGGER zz_pause_duplicate_enqueue BEFORE INSERT ON worker_tasks FOR EACH ROW EXECUTE FUNCTION pause_duplicate_enqueue();",task.id)))
        .execute(b.pg_pool()).await.unwrap();
    let mut gate = b.pg_pool().begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut *gate)
        .await
        .unwrap();
    let storage = b.storage.clone();
    let duplicate = task.clone();
    let producer = tokio::spawn(async move { storage.create_worker_task(&duplicate).await });
    let producer_pid=tokio::time::timeout(StdDuration::from_secs(10),async {
        loop {
            let pid: Option<i32>=sqlx::query_scalar("SELECT pid FROM pg_locks WHERE locktype='advisory' AND objid=$1::bigint::oid AND NOT granted LIMIT 1")
                .bind(key).fetch_optional(b.pg_pool()).await.unwrap();
            if let Some(pid)=pid {break pid}
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    }).await;
    let Ok(producer_pid) = producer_pid else {
        gate.rollback().await.unwrap();
        producer.abort();
        panic!("duplicate enqueue did not reach its parent-held barrier")
    };
    let mut next = task.clone();
    next.id = uuid::Uuid::new_v4();
    next.attempt = 1;
    let new_id = next.id;
    let resolution = WorkerTaskResolution {
        task_id: task.id,
        instance_id: task.instance_id,
        expected_state: WorkerTaskState::Pending,
        expected_claim_epoch: 0,
        expected_worker_id: None,
        holder_worker_id: None,
        event: WorkerAttemptEventKind::TimedOut,
        effect_resolution: orch8_types::worker::WorkerTaskEffectResolution::Keep,
        reason: "retry pending timeout".into(),
        retryable: true,
        action: WorkerTaskResolutionAction::Retry {
            retry_task: Box::new(next),
            retry_output: None,
            node_id: None,
            fire_at: Utc::now(),
        },
    };
    let storage = b.storage.clone();
    let resolver = tokio::spawn(async move { storage.resolve_worker_task(&resolution).await });
    let waited = tokio::time::timeout(StdDuration::from_secs(10), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",
            )
            .bind(producer_pid)
            .fetch_one(b.pg_pool())
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    gate.rollback().await.unwrap();
    let (enqueue_result, retry_result) = tokio::join!(
        tokio::time::timeout(StdDuration::from_secs(10), producer),
        tokio::time::timeout(StdDuration::from_secs(10), resolver)
    );
    assert!(
        waited,
        "retry resolution did not serialize behind the producer's parent lock"
    );
    enqueue_result.unwrap().unwrap().unwrap();
    assert!(retry_result.unwrap().unwrap().unwrap());
    assert!(b.storage.get_worker_task(task.id).await.unwrap().is_none());
    let saved = b.storage.get_worker_task(new_id).await.unwrap().unwrap();
    assert_eq!(saved.attempt, 1);
    assert_eq!(saved.state, WorkerTaskState::Pending);
}

#[tokio::test]
async fn sqlite_quarantine_upgrade_is_atomic_and_not_repeated_on_reopen() {
    use orch8_storage::{ContinuityStore, InstanceStore, sqlite::SqliteStorage};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("quarantine-upgrade.db");
    let path = path.to_str().unwrap();
    let storage = SqliteStorage::file(path).await.unwrap();
    let request = fixture(&storage).await;
    let cut = capture(&storage, &request).await;
    let bytes = b"quarantine upgrade preserves context";
    storage
        .seal_capsule_cut(
            &request.owner.tenant_id,
            cut.id,
            &manifest(&cut, bytes),
            bytes,
        )
        .await
        .unwrap();
    // Retained schema-60 evidence with an inconsistent pre-quarantine native row.
    sqlx::raw_sql("DROP TRIGGER physical_retirement_native_update; DELETE FROM schema_versions WHERE version=61;
        CREATE TRIGGER fail_quarantine_completion BEFORE INSERT ON schema_versions WHEN NEW.version=61
        BEGIN SELECT RAISE(ABORT,'test interruption after quarantine update'); END;").execute(storage.pool()).await.unwrap();
    sqlx::query("UPDATE task_instances SET state='scheduled',claimed_at='legacy-claim',next_fire_at='2099-01-01T00:00:00Z' WHERE id=?1")
        .bind(request.owner.current_instance_id.to_string()).execute(storage.pool()).await.unwrap();
    storage.close().await;
    assert!(
        SqliteStorage::file(path).await.is_err(),
        "failed quarantine completion must refuse open"
    );
    let raw = sqlx::SqlitePool::connect(&format!("sqlite://{path}"))
        .await
        .unwrap();
    let old: (String, Option<String>, Option<String>) =
        sqlx::query_as("SELECT state,claimed_at,next_fire_at FROM task_instances WHERE id=?1")
            .bind(request.owner.current_instance_id.to_string())
            .fetch_one(&raw)
            .await
            .unwrap();
    assert_eq!(
        old,
        (
            "scheduled".into(),
            Some("legacy-claim".into()),
            Some("2099-01-01T00:00:00Z".into())
        ),
        "failed completion must roll back quarantine updates too"
    );
    let complete: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM schema_versions WHERE version=61)")
            .fetch_one(&raw)
            .await
            .unwrap();
    assert!(!complete);
    sqlx::query("DROP TRIGGER fail_quarantine_completion")
        .execute(&raw)
        .await
        .unwrap();
    raw.close().await;
    let storage = SqliteStorage::file(path).await.unwrap();
    let quarantined = storage
        .get_instance(request.owner.current_instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(quarantined.state, InstanceState::Paused);
    assert!(quarantined.claimed_at.is_none());
    assert!(quarantined.next_fire_at.is_none());
    assert_eq!(quarantined.context.data, json!({"secret":"ctx-cut-only"}));
    sqlx::raw_sql("CREATE TRIGGER completed_quarantine_must_not_repeat BEFORE UPDATE OF state ON task_instances
        BEGIN SELECT RAISE(ABORT,'completed quarantine was repeated'); END;").execute(storage.pool()).await.unwrap();
    storage.close().await;
    let reopened = SqliteStorage::file(path)
        .await
        .expect("completed quarantine must not repeat on reopen");
    assert!(
        reopened
            .is_physical_instance_retired(request.owner.current_instance_id)
            .await
            .unwrap()
    );
    reopened.close().await;
}

orch8_storage::backend_test!(async fn captured_native_writes_require_explicit_abort(b) {
    let request=fixture(b.storage.as_ref()).await;
    let cut=capture(b.storage.as_ref(),&request).await;
    let id=request.owner.current_instance_id;
    for state in [InstanceState::Scheduled,InstanceState::Running,InstanceState::Waiting] {
        assert!(b.storage.update_instance_state(id,state,Some(Utc::now())).await.is_err(),"captured source accepted a direct {state:?} write");
        assert_eq!(b.storage.get_instance(id).await.unwrap().unwrap().state,InstanceState::Paused);
    }
    assert!(!matches!(b.storage.conditional_update_instance_state(id,InstanceState::Paused,InstanceState::Running,None).await,Ok(true)));
    assert!(b.storage.abort_preparing_capsule_cut(&request.owner.tenant_id,cut.id).await.unwrap());
    assert_eq!(b.storage.get_instance(id).await.unwrap().unwrap().state,InstanceState::Waiting);
    b.storage.update_instance_state(id,InstanceState::Running,None).await.unwrap();
});

orch8_storage::backend_test!(async fn captured_owner_cannot_advance_to_a_fresh_destination_before_seal(b) {
    let request=fixture(b.storage.as_ref()).await;
    let cut=capture(b.storage.as_ref(),&request).await;
    let mut destination=b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap();
    destination.id=InstanceId::new();b.storage.create_instance(&destination).await.unwrap();
    let frozen=b.storage.get_continuity_execution(&request.owner.tenant_id,request.owner.continuity_id).await.unwrap().unwrap();
    let next=ContinuityExecution {current_instance_id:destination.id,owner_runtime_id:request.destination_runtime_id.unwrap(),
        epoch:frozen.epoch.checked_next().unwrap(),state:OwnershipState::Owned,updated_at:Utc::now(),..frozen.clone()};
    assert!(!b.storage.cas_continuity_owner(&frozen.tenant_id,frozen.continuity_id,frozen.epoch,frozen.owner_runtime_id,&next).await.unwrap(),"unsealed capture admitted a new owner");
    assert_eq!(b.storage.get_continuity_execution(&frozen.tenant_id,frozen.continuity_id).await.unwrap().unwrap(),frozen);
    let bytes=b"explicit publication precedes destination ownership";
    b.storage.seal_capsule_cut(&frozen.tenant_id,cut.id,&manifest(&cut,bytes),bytes).await.unwrap();
    assert!(b.storage.cas_continuity_owner(&frozen.tenant_id,frozen.continuity_id,frozen.epoch,frozen.owner_runtime_id,&next).await.unwrap());
});

async fn raw_owner_write(
    b: &orch8_storage::testing::backends::TestBackend,
    next: &ContinuityExecution,
) -> Result<(), orch8_types::error::StorageError> {
    let state = serde_json::to_value(next.state)
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    match &b.raw {
        orch8_storage::testing::backends::RawPool::Sqlite(pool) => {
            sqlx::query("UPDATE continuity_executions SET state=?1,record=?2,epoch=?3,owner_runtime_id=?4 WHERE tenant_id=?5 AND continuity_id=?6")
                .bind(state).bind(serde_json::to_string(next).unwrap()).bind(i64::try_from(next.epoch.get()).unwrap()).bind(next.owner_runtime_id.to_string())
                .bind(next.tenant_id.as_str()).bind(next.continuity_id.to_string()).execute(pool).await?;
        }
        #[cfg(feature = "postgres")]
        orch8_storage::testing::backends::RawPool::Postgres(pool) => {
            sqlx::query("UPDATE continuity_executions SET state=$1,record=$2,epoch=$3,owner_runtime_id=$4 WHERE tenant_id=$5 AND continuity_id=$6")
                .bind(state).bind(serde_json::to_value(next).unwrap()).bind(i64::try_from(next.epoch.get()).unwrap()).bind(next.owner_runtime_id.into_uuid())
                .bind(next.tenant_id.as_str()).bind(next.continuity_id.into_uuid()).execute(pool).await?;
        }
    }
    Ok(())
}

orch8_storage::backend_test!(async fn captured_database_owner_guard_blocks_old_writers_and_preserves_compensation(b) {
    let request=fixture(b.storage.as_ref()).await;
    let _cut=capture(b.storage.as_ref(),&request).await;
    let frozen=b.storage.get_continuity_execution(&request.owner.tenant_id,request.owner.continuity_id).await.unwrap().unwrap();
    for variant in 0..5 {
        let mut next=frozen.clone();
        match variant {
            0=>next.state=OwnershipState::Owned,
            1=>next.state=OwnershipState::Completed,
            2=>next.current_instance_id=InstanceId::new(),
            3=>next.owner_runtime_id=RuntimeId::new(),
            _=>next.epoch=next.epoch.checked_next().unwrap(),
        }
        assert!(raw_owner_write(&b,&next).await.is_err(),"captured owner accepted old-writer variant {variant}");
        assert_eq!(b.storage.get_continuity_execution(&frozen.tenant_id,frozen.continuity_id).await.unwrap().unwrap(),frozen);
    }
    // Evidence timestamp refresh and a same-owner compensation fence admit no
    // new forward authority; compensation may still settle existing effects.
    raw_owner_write(&b,&frozen).await.unwrap();
    let fenced=ContinuityExecution {state:OwnershipState::Compensating,..frozen};
    raw_owner_write(&b,&fenced).await.unwrap();
});

orch8_storage::backend_test!(async fn captured_source_cannot_be_assigned_to_another_continuity(b) {
    let request=fixture(b.storage.as_ref()).await;
    let _cut=capture(b.storage.as_ref(),&request).await;
    let another=ContinuityExecution {continuity_id:ContinuityId::new(),..request.owner.clone()};
    assert!(b.storage.create_continuity_execution(&another).await.is_err(),"new continuity adopted a captured physical source");
    assert!(b.storage.get_continuity_execution(&another.tenant_id,another.continuity_id).await.unwrap().is_none());
});

orch8_storage::backend_test!(async fn captured_terminal_outcome_is_preserved_by_abort(b) {
    let request=fixture(b.storage.as_ref()).await;let cut=capture(b.storage.as_ref(),&request).await;
    b.storage.update_instance_state(request.owner.current_instance_id,InstanceState::Completed,None).await.unwrap();
    assert!(b.storage.abort_preparing_capsule_cut(&request.owner.tenant_id,cut.id).await.unwrap());
    assert_eq!(b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap().state,InstanceState::Completed);
});

orch8_storage::backend_test!(async fn active_cut_cannot_be_deleted_without_explicit_retirement(b) {
    use orch8_storage::testing::backends::RawPool;
    for sealed in [false,true] {
        let request=fixture(b.storage.as_ref()).await;let cut=capture(b.storage.as_ref(),&request).await;
        if sealed {let bytes=b"preserve active recovery point";b.storage.seal_capsule_cut(&request.owner.tenant_id,cut.id,&manifest(&cut,bytes),bytes).await.unwrap();}
        let refused=match &b.raw {
            RawPool::Sqlite(pool)=>sqlx::query("DELETE FROM capsule_source_cuts WHERE capsule_id=?1").bind(cut.id.to_string()).execute(pool).await.is_err(),
            #[cfg(feature="postgres")]
            RawPool::Postgres(pool)=>sqlx::query("DELETE FROM capsule_source_cuts WHERE capsule_id=$1").bind(cut.id.into_uuid()).execute(pool).await.is_err(),
        };
        assert!(refused,"active recovery point was deleted");
        assert_eq!(capture(b.storage.as_ref(),&request).await.id,cut.id);
        if !sealed {
            assert!(b.storage.abort_preparing_capsule_cut(&request.owner.tenant_id,cut.id).await.unwrap());
            match &b.raw {
                RawPool::Sqlite(pool)=>{sqlx::query("DELETE FROM capsule_source_cuts WHERE capsule_id=?1").bind(cut.id.to_string()).execute(pool).await.unwrap();}
                #[cfg(feature="postgres")]
                RawPool::Postgres(pool)=>{sqlx::query("DELETE FROM capsule_source_cuts WHERE capsule_id=$1").bind(cut.id.into_uuid()).execute(pool).await.unwrap();}
            }
        }
    }
});

orch8_storage::backend_test!(async fn captured_record_state_cannot_disagree_with_indexed_authority(b) {
    use orch8_storage::testing::backends::RawPool;
    let request=fixture(b.storage.as_ref()).await;let _cut=capture(b.storage.as_ref(),&request).await;
    let mut record=serde_json::to_value(&request.owner).unwrap();record["state"]=json!("owned");
    let refused=match &b.raw {
        RawPool::Sqlite(pool)=>sqlx::query("UPDATE continuity_executions SET record=?1 WHERE tenant_id=?2 AND continuity_id=?3")
            .bind(record.to_string()).bind(request.owner.tenant_id.as_str()).bind(request.owner.continuity_id.to_string()).execute(pool).await.is_err(),
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>sqlx::query("UPDATE continuity_executions SET record=$1 WHERE tenant_id=$2 AND continuity_id=$3")
            .bind(record).bind(request.owner.tenant_id.as_str()).bind(request.owner.continuity_id.into_uuid()).execute(pool).await.is_err(),
    };
    assert!(refused,"JSON owner state bypassed the indexed authority guard");
});

orch8_storage::backend_test!(async fn failed_abort_rolls_back_cut_release_and_owner_restoration(b) {
    use orch8_storage::testing::backends::RawPool;
    let request=fixture(b.storage.as_ref()).await;let cut=capture(b.storage.as_ref(),&request).await;
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::raw_sql("CREATE TRIGGER fail_abort_restore BEFORE UPDATE ON continuity_executions WHEN NEW.state='owned' BEGIN SELECT RAISE(ABORT,'test failed owner restoration'); END;").execute(pool).await.unwrap();},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("CREATE FUNCTION fail_abort_restore() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test failed owner restoration'; END $$;
            CREATE TRIGGER fail_abort_restore BEFORE UPDATE ON continuity_executions FOR EACH ROW WHEN (NEW.state='owned') EXECUTE FUNCTION fail_abort_restore();").execute(pool).await.unwrap();},
    }
    assert!(b.storage.abort_preparing_capsule_cut(&request.owner.tenant_id,cut.id).await.is_err());
    assert_eq!(capture(b.storage.as_ref(),&request).await.id,cut.id,"failed abort released the durable cut");
    assert_eq!(b.storage.get_continuity_execution(&request.owner.tenant_id,request.owner.continuity_id).await.unwrap().unwrap().state,OwnershipState::Transferring);
    assert!(b.storage.update_instance_state(request.owner.current_instance_id,InstanceState::Running,None).await.is_err());
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::query("DROP TRIGGER fail_abort_restore").execute(pool).await.unwrap();}
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("DROP TRIGGER fail_abort_restore ON continuity_executions; DROP FUNCTION fail_abort_restore();").execute(pool).await.unwrap();}
    }
    assert!(b.storage.abort_preparing_capsule_cut(&request.owner.tenant_id,cut.id).await.unwrap());
    assert_eq!(b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap().state,InstanceState::Waiting);
});

#[cfg(feature = "postgres")]
#[derive(Clone, Copy)]
enum LegacyAuthorityMutation {
    Native,
    NativeDelete,
    Owner,
    Downgrade,
}

#[cfg(feature = "postgres")]
async fn legacy_authority_mutation(
    pool: &sqlx::PgPool,
    source: ContinuityExecution,
    destination: InstanceId,
    mutation: LegacyAuthorityMutation,
) -> Result<(), sqlx::Error> {
    match mutation {
        LegacyAuthorityMutation::Downgrade => sqlx::raw_sql(include_str!(
            "../../migrations/down/131_captured_source_authority.sql"
        ))
        .execute(pool)
        .await
        .map(|_| ()),
        LegacyAuthorityMutation::Owner => {
            let next = ContinuityExecution {
                current_instance_id: destination,
                owner_runtime_id: RuntimeId::new(),
                epoch: source.epoch.checked_next().unwrap(),
                ..source
            };
            sqlx::query("UPDATE continuity_executions SET state='owned',epoch=$1,owner_runtime_id=$2,record=$3 WHERE tenant_id=$4 AND continuity_id=$5")
                .bind(i64::try_from(next.epoch.get()).unwrap()).bind(next.owner_runtime_id.into_uuid()).bind(serde_json::to_value(&next).unwrap())
                .bind(next.tenant_id.as_str()).bind(next.continuity_id.into_uuid()).execute(pool).await.map(|_|())
        }
        LegacyAuthorityMutation::NativeDelete => {
            sqlx::query("DELETE FROM task_instances WHERE id=$1")
                .bind(source.current_instance_id.into_uuid())
                .execute(pool)
                .await
                .map(|_| ())
        }
        LegacyAuthorityMutation::Native => {
            sqlx::query("UPDATE task_instances SET state='running',claimed_at=NOW() WHERE id=$1")
                .bind(source.current_instance_id.into_uuid())
                .execute(pool)
                .await
                .map(|_| ())
        }
    }
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_legacy_authority_writes_recheck_capture_after_parent_wait() {
    use orch8_storage::testing::backends::TestBackend;
    use std::time::Duration as StdDuration;
    for mutation in [
        LegacyAuthorityMutation::Native,
        LegacyAuthorityMutation::NativeDelete,
        LegacyAuthorityMutation::Owner,
        LegacyAuthorityMutation::Downgrade,
    ] {
        let Some(b) = TestBackend::postgres().await else {
            return;
        };
        let request = fixture(b.storage.as_ref()).await;
        let mut destination = b
            .storage
            .get_instance(request.owner.current_instance_id)
            .await
            .unwrap()
            .unwrap();
        destination.id = InstanceId::new();
        destination.state = InstanceState::Paused;
        b.storage.create_instance(&destination).await.unwrap();
        let key = i64::from(uuid::Uuid::new_v4().as_fields().0 & 0x7fff_ffff);
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE FUNCTION pause_captured_source() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({key}); RETURN NEW; END $$;
            CREATE TRIGGER pause_captured_source BEFORE INSERT ON capsule_source_cuts FOR EACH ROW EXECUTE FUNCTION pause_captured_source();"))).execute(b.pg_pool()).await.unwrap();
        let mut gate = b.pg_pool().begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(key)
            .execute(&mut *gate)
            .await
            .unwrap();
        let storage = b.storage.clone();
        let capture_request = request.clone();
        let capturing =
            tokio::spawn(async move { storage.capture_capsule_cut(&capture_request).await });
        let capture_pid=tokio::time::timeout(StdDuration::from_secs(10),async {loop {
            let pid:Option<i32>=sqlx::query_scalar("SELECT pid FROM pg_locks WHERE locktype='advisory' AND objid=$1::bigint::oid AND NOT granted LIMIT 1").bind(key).fetch_optional(b.pg_pool()).await.unwrap();
            if let Some(pid)=pid {break pid;} tokio::time::sleep(StdDuration::from_millis(10)).await;
        }}).await;
        let Ok(capture_pid) = capture_pid else {
            gate.rollback().await.unwrap();
            capturing.abort();
            panic!("capture did not reach its parent-held barrier")
        };
        let pool = b.pg_pool().clone();
        let source = request.owner.clone();
        let writing = tokio::spawn(async move {
            legacy_authority_mutation(&pool, source, destination.id, mutation).await
        });
        let waited=tokio::time::timeout(StdDuration::from_secs(10),async {loop {
            let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))").bind(capture_pid).fetch_one(b.pg_pool()).await.unwrap();
            if blocked {break;}tokio::time::sleep(StdDuration::from_millis(10)).await;
        }}).await.is_ok();
        gate.rollback().await.unwrap();
        let cut = tokio::time::timeout(StdDuration::from_secs(10), capturing)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let result = tokio::time::timeout(StdDuration::from_secs(10), writing)
            .await
            .unwrap()
            .unwrap();
        assert!(waited, "old writer did not wait on capture's parent lock");
        assert!(matches!(cut, CapsuleCutCapture::Ready(_)));
        assert!(
            result.is_err(),
            "old writer missed the captured cut after its parent wait"
        );
        assert_eq!(
            b.storage
                .get_instance(request.owner.current_instance_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            InstanceState::Paused
        );
        assert_eq!(
            b.storage
                .get_continuity_execution(&request.owner.tenant_id, request.owner.continuity_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            OwnershipState::Transferring
        );
    }
}

#[tokio::test]
async fn sqlite_captured_quarantine_upgrade_is_atomic_and_runs_once() {
    use orch8_storage::{InstanceStore, sqlite::SqliteStorage};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("captured-quarantine.db");
    let path = path.to_str().unwrap();
    let storage = SqliteStorage::file(path).await.unwrap();
    let request = fixture(&storage).await;
    let cut = capture(&storage, &request).await;
    sqlx::raw_sql("DROP TRIGGER captured_source_native_update; DELETE FROM schema_versions WHERE version=62;
        CREATE TRIGGER fail_captured_upgrade BEFORE INSERT ON schema_versions WHEN NEW.version=62 BEGIN SELECT RAISE(ABORT,'test interrupted captured quarantine'); END;")
        .execute(storage.pool()).await.unwrap();
    sqlx::query("UPDATE task_instances SET state='running',claimed_at='legacy',next_fire_at='2099-01-01T00:00:00Z' WHERE id=?1").bind(request.owner.current_instance_id.to_string()).execute(storage.pool()).await.unwrap();
    storage.close().await;
    assert!(SqliteStorage::file(path).await.is_err());
    let raw = sqlx::SqlitePool::connect(&format!("sqlite://{path}"))
        .await
        .unwrap();
    let old: (String, Option<String>) =
        sqlx::query_as("SELECT state,claimed_at FROM task_instances WHERE id=?1")
            .bind(request.owner.current_instance_id.to_string())
            .fetch_one(&raw)
            .await
            .unwrap();
    assert_eq!(old, ("running".into(), Some("legacy".into())));
    let complete: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM schema_versions WHERE version=62)")
            .fetch_one(&raw)
            .await
            .unwrap();
    assert!(!complete);
    sqlx::query("DROP TRIGGER fail_captured_upgrade")
        .execute(&raw)
        .await
        .unwrap();
    raw.close().await;
    let storage = SqliteStorage::file(path).await.unwrap();
    let native = storage
        .get_instance(request.owner.current_instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(native.state, InstanceState::Paused);
    assert!(native.claimed_at.is_none());
    assert!(native.next_fire_at.is_none());
    assert_eq!(native.context.data, json!({"secret":"ctx-cut-only"}));
    assert_eq!(capture(&storage, &request).await.id, cut.id);
    assert!(
        storage
            .update_instance_state(native.id, InstanceState::Running, None)
            .await
            .is_err()
    );
    sqlx::raw_sql("CREATE TRIGGER fail_repeated_captured_quarantine BEFORE UPDATE ON task_instances WHEN EXISTS(SELECT 1 FROM capsule_source_cuts WHERE state='captured') BEGIN SELECT RAISE(ABORT,'completed quarantine must not repeat'); END;")
        .execute(storage.pool()).await.unwrap();
    storage.close().await;
    let storage = SqliteStorage::file(path).await.unwrap();
    assert_eq!(
        storage
            .get_instance(native.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Paused
    );
    storage.close().await;
}

orch8_storage::backend_test!(async fn captured_paused_source_refuses_new_claim_and_fire_time(b) {
    use orch8_storage::testing::backends::RawPool;
    let request=fixture(b.storage.as_ref()).await;let _cut=capture(b.storage.as_ref(),&request).await;
    for claim in [true,false] {
        let refused=match &b.raw {
            RawPool::Sqlite(pool)=>sqlx::query(if claim {"UPDATE task_instances SET claimed_at='2099-01-01T00:00:00Z' WHERE id=?1"} else {"UPDATE task_instances SET next_fire_at='2099-01-01T00:00:00Z' WHERE id=?1"})
                .bind(request.owner.current_instance_id.to_string()).execute(pool).await.is_err(),
            #[cfg(feature="postgres")]
            RawPool::Postgres(pool)=>sqlx::query(if claim {"UPDATE task_instances SET claimed_at=NOW() WHERE id=$1"} else {"UPDATE task_instances SET next_fire_at=NOW() WHERE id=$1"})
                .bind(request.owner.current_instance_id.into_uuid()).execute(pool).await.is_err(),
        };
        assert!(refused,"paused captured source accepted a new claim or fire time");
    }
    let frozen=b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap();
    assert!(frozen.claimed_at.is_none());assert!(frozen.next_fire_at.is_none());
});

orch8_storage::backend_test!(async fn captured_physical_identity_cannot_be_recreated_after_native_deletion(b) {
    use orch8_storage::testing::backends::RawPool;
    let request=fixture(b.storage.as_ref()).await;let cut=capture(b.storage.as_ref(),&request).await;
    let frozen=b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap();
    // Inject native history loss from before the cleanup guard existed. The
    // current API must still refuse identity reuse; authorization-dependent
    // snapshot replay is a separate recovery contract.
    match &b.raw {
        RawPool::Sqlite(pool)=>{
            let mut tx=pool.begin().await.unwrap();
            sqlx::query("DROP TRIGGER captured_source_native_delete").execute(&mut *tx).await.unwrap();
            sqlx::query("DELETE FROM task_instances WHERE id=?1").bind(frozen.id.to_string()).execute(&mut *tx).await.unwrap();
            sqlx::raw_sql(include_str!("../src/sqlite/captured_source.sql")).execute(&mut *tx).await.unwrap();tx.commit().await.unwrap();
        }
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{
            let mut tx=pool.begin().await.unwrap();
            sqlx::query("ALTER TABLE task_instances DISABLE TRIGGER captured_source_native_delete").execute(&mut *tx).await.unwrap();
            sqlx::query("DELETE FROM task_instances WHERE id=$1").bind(frozen.id.into_uuid()).execute(&mut *tx).await.unwrap();
            sqlx::query("ALTER TABLE task_instances ENABLE TRIGGER captured_source_native_delete").execute(&mut *tx).await.unwrap();tx.commit().await.unwrap();
        }
    }
    assert!(b.storage.create_instance(&frozen).await.is_err(),"captured physical identity was recreated");
    assert!(b.storage.get_instance(frozen.id).await.unwrap().is_none());
    assert_eq!(b.raw.count("capsule_source_cuts","capsule_id",&cut.id.to_string()).await,1,"native deletion discarded durable cut evidence");
});

orch8_storage::backend_test!(async fn captured_source_cannot_be_adopted_by_raw_foreign_tenant_owner(b) {
    use orch8_storage::testing::backends::RawPool;
    let request=fixture(b.storage.as_ref()).await;let _cut=capture(b.storage.as_ref(),&request).await;
    let another=ContinuityExecution {continuity_id:ContinuityId::new(),tenant_id:TenantId::unchecked("foreign-owner"),..request.owner.clone()};
    let error=match &b.raw {
        RawPool::Sqlite(pool)=>sqlx::query("INSERT INTO continuity_executions(continuity_id,tenant_id,epoch,owner_runtime_id,state,record,updated_at) VALUES(?1,?2,0,?3,'owned',?4,?5)")
            .bind(another.continuity_id.to_string()).bind(another.tenant_id.as_str()).bind(another.owner_runtime_id.to_string()).bind(serde_json::to_string(&another).unwrap()).bind(Utc::now().to_rfc3339()).execute(pool).await.unwrap_err().to_string(),
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>sqlx::query("INSERT INTO continuity_executions(continuity_id,tenant_id,epoch,owner_runtime_id,state,record,updated_at) VALUES($1,$2,0,$3,'owned',$4,$5)")
            .bind(another.continuity_id.into_uuid()).bind(another.tenant_id.as_str()).bind(another.owner_runtime_id.into_uuid()).bind(serde_json::to_value(&another).unwrap()).bind(Utc::now()).execute(pool).await.unwrap_err().to_string(),
    };
    assert!(error.contains("captured source"),"foreign-scope refusal did not use the captured authority guard: {error}");
    assert!(b.storage.get_continuity_execution(&another.tenant_id,another.continuity_id).await.unwrap().is_none());
});

orch8_storage::backend_test!(async fn active_cut_native_cleanup_requires_explicit_abort_or_acknowledgement(b) {
    use orch8_storage::testing::backends::RawPool;
    for sealed in [false,true] {
        let request=fixture(b.storage.as_ref()).await;let cut=capture(b.storage.as_ref(),&request).await;
        if sealed {let bytes=b"native evidence is not yet transport proof";b.storage.seal_capsule_cut(&request.owner.tenant_id,cut.id,&manifest(&cut,bytes),bytes).await.unwrap();}
        // A late terminal outcome does not certify transport of local waits,
        // signals or independent callback evidence; generic GC must retain it.
        b.storage.update_instance_state(request.owner.current_instance_id,InstanceState::Completed,None).await.unwrap();
        assert_eq!(b.storage.purge_terminal_instances(&[request.owner.current_instance_id]).await.unwrap(),0);
        let mut healthy=b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap();
        healthy.id=InstanceId::new();b.storage.create_instance(&healthy).await.unwrap();
        assert_eq!(b.storage.purge_terminal_instances(&[request.owner.current_instance_id,healthy.id]).await.unwrap(),1,"retained source must not block healthy history GC");
        assert!(b.storage.get_instance(healthy.id).await.unwrap().is_none());
        let refused=match &b.raw {
            RawPool::Sqlite(pool)=>sqlx::query("DELETE FROM task_instances WHERE id=?1").bind(request.owner.current_instance_id.to_string()).execute(pool).await.is_err(),
            #[cfg(feature="postgres")]
            RawPool::Postgres(pool)=>sqlx::query("DELETE FROM task_instances WHERE id=$1").bind(request.owner.current_instance_id.into_uuid()).execute(pool).await.is_err(),
        };
        assert!(refused,"active source native payload was discarded");
        assert_eq!(b.storage.get_instance(request.owner.current_instance_id).await.unwrap().unwrap().context.data,json!({"secret":"ctx-cut-only"}));
        assert_eq!(capture(b.storage.as_ref(),&request).await.id,cut.id);
        if sealed {
            let accepted=ContinuityExecution {current_instance_id:InstanceId::new(),epoch:request.owner.epoch.checked_next().unwrap(),
                owner_runtime_id:request.destination_runtime_id.unwrap(),state:OwnershipState::Owned,..request.owner.clone()};
            b.storage.acknowledge_capsule_cut(&request.owner.tenant_id,cut.id,&accepted).await.unwrap();
        } else {
            assert!(b.storage.abort_preparing_capsule_cut(&request.owner.tenant_id,cut.id).await.unwrap());
        }
        assert_eq!(b.storage.purge_terminal_instances(&[request.owner.current_instance_id]).await.unwrap(),1,"explicit release must permit ordinary native GC");
    }
});

#[tokio::test]
async fn sqlite_captured_guards_use_point_indexes() {
    use orch8_storage::sqlite::SqliteStorage;
    use sqlx::Row;
    let storage = SqliteStorage::in_memory().await.unwrap();
    let definitions: Vec<String> = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name LIKE 'captured_source_%'",
    )
    .fetch_all(storage.pool())
    .await
    .unwrap();
    let mut queries = 0;
    // Read the installed guards themselves so a future predicate edit cannot
    // leave a copied EXPLAIN fixture green while production regresses.
    for definition in definitions {
        let mut rest = definition.as_str();
        while let Some(start) = rest.find("EXISTS(") {
            let body = &rest[start + 7..];
            let mut depth = 1;
            let end = body
                .char_indices()
                .find_map(|(i, c)| {
                    match c {
                        '(' => depth += 1,
                        ')' => depth -= 1,
                        _ => {}
                    }
                    (depth == 0).then_some(i)
                })
                .unwrap();
            let mut select = body[..end].to_owned();
            rest = &body[end + 1..];
            for (name, value) in [
                ("NEW.record", "'{\"current_instance_id\":\"physical\"}'"),
                ("OLD.record", "'{\"current_instance_id\":\"physical\"}'"),
                ("NEW.tenant_id", "'tenant'"),
                ("OLD.tenant_id", "'tenant'"),
                ("NEW.continuity_id", "'continuity'"),
                ("OLD.continuity_id", "'continuity'"),
                ("NEW.id", "'physical'"),
                ("OLD.id", "'physical'"),
            ] {
                select = select.replace(name, value);
            }
            let rows = sqlx::query(sqlx::AssertSqlSafe(format!("EXPLAIN QUERY PLAN {select}")))
                .fetch_all(storage.pool())
                .await
                .unwrap();
            let plan = rows
                .iter()
                .map(|r| r.get::<String, _>("detail"))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                plan.contains("SEARCH capsule_source_cuts")
                    && plan.contains("idx_capsule_source_cut_"),
                "captured guard is not a point lookup: {select}\n{plan}"
            );
            assert!(
                !plan.contains("SCAN capsule_source_cuts"),
                "captured guard scans retained history: {plan}"
            );
            queries += 1;
        }
    }
    assert!(
        queries >= 7,
        "guard query coverage unexpectedly shrank: {queries}"
    );
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn rejected_acknowledgement_releases_owner_lock_before_returning() {
    let Some(b) = orch8_storage::testing::backends::TestBackend::postgres().await else {
        return;
    };
    let request = fixture(b.storage.as_ref()).await;
    let cut = capture(b.storage.as_ref(), &request).await;
    let second = b.connect_another_postgres().await.unwrap();
    let mut observer = second.pool().acquire().await.unwrap();
    let mut accepted = request.owner.clone();
    accepted.epoch = accepted.epoch.checked_next().unwrap();
    accepted.current_instance_id = InstanceId::new();
    accepted.owner_runtime_id = request.destination_runtime_id.unwrap();
    assert!(
        b.storage
            .acknowledge_capsule_cut(&accepted.tenant_id, cut.id, &accepted)
            .await
            .is_err(),
        "unsealed cut was acknowledged"
    );
    let unlocked = sqlx::query("SELECT continuity_id FROM continuity_executions WHERE tenant_id=$1 AND continuity_id=$2 FOR UPDATE NOWAIT")
        .bind(request.owner.tenant_id.as_str()).bind(request.owner.continuity_id.into_uuid())
        .fetch_one(&mut *observer).await.is_ok();
    drop(observer);
    second.pool().close().await;
    assert!(
        unlocked,
        "rejected acknowledgement returned with its owner row still locked"
    );
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_downgrade_dependencies_are_scoped_to_the_source_cut_schema() {
    let Some(b) = orch8_storage::testing::backends::TestBackend::postgres().await else {
        return;
    };
    let schema = b.pg_schema().unwrap();
    let shadow = format!("orch8_shadow_{}", uuid::Uuid::new_v4().simple());
    let mut tx = b.pg_pool().begin().await.unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE SCHEMA \"{shadow}\"; CREATE TABLE \"{shadow}\".physical_instance_retirements(id uuid); CREATE FUNCTION \"{shadow}\".enforce_captured_native_authority() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$; SET LOCAL search_path TO \"{schema}\",\"{shadow}\",public;")))
        .execute(&mut *tx).await.unwrap();
    let result = async {
        sqlx::raw_sql(include_str!(
            "../../migrations/down/131_captured_source_authority.sql"
        ))
        .execute(&mut *tx)
        .await?;
        sqlx::raw_sql(include_str!(
            "../../migrations/down/130_physical_instance_retirements.sql"
        ))
        .execute(&mut *tx)
        .await?;
        sqlx::raw_sql(include_str!(
            "../../migrations/down/129_capsule_source_cuts.sql"
        ))
        .execute(&mut *tx)
        .await?;
        let untouched: bool = sqlx::query_scalar(
            "SELECT to_regclass($1) IS NOT NULL AND to_regprocedure($2) IS NOT NULL",
        )
        .bind(format!("{shadow}.physical_instance_retirements"))
        .bind(format!("{shadow}.enforce_captured_native_authority()"))
        .fetch_one(&mut *tx)
        .await?;
        Ok::<_, sqlx::Error>(untouched)
    }
    .await;
    tx.rollback().await.unwrap();
    assert!(
        matches!(result, Ok(true)),
        "downgrade consulted or changed an unrelated schema: {result:?}"
    );
}

orch8_storage::backend_test!(async fn pending_accepted_worker_result_blocks_capture_after_hot_gc(b) {
    use orch8_types::worker::{WorkerClaim, WorkerTask};
    use orch8_storage::capsule_export::CapsuleCutRefusal;
    let storage=b.storage.as_ref();let request=fixture(storage).await;
    let task:WorkerTask=serde_json::from_value(json!({"id":uuid::Uuid::now_v7(),"instance_id":request.owner.current_instance_id,"block_id":"charge","handler_name":"remote.capture-test","attempt":0,"params":{},"context":{},"state":"pending","created_at":Utc::now()})).unwrap();
    storage.create_worker_task(&task).await.unwrap();
    let held=storage.claim_worker_tasks(&task.handler_name,"worker",1).await.unwrap().pop().unwrap();
    let claim=WorkerClaim::new("worker",held.claim_epoch);
    assert!(storage.complete_worker_task(task.id,&claim,&json!({"accepted":true})).await.unwrap());
    storage.delete_worker_task(task.id).await.unwrap();
    assert!(matches!(storage.capture_capsule_cut(&request).await.unwrap(),CapsuleCutCapture::Refused(CapsuleCutRefusal::UnsafeBoundary)));
    let error = match &b.raw {
        orch8_storage::testing::backends::RawPool::Sqlite(pool) => sqlx::query("INSERT INTO capsule_source_cuts(source_instance_id,state) VALUES(?1,'captured')")
            .bind(task.instance_id.to_string()).execute(pool).await.unwrap_err(),
        #[cfg(feature="postgres")]
        orch8_storage::testing::backends::RawPool::Postgres(pool) => sqlx::query("INSERT INTO capsule_source_cuts(source_instance_id,state) VALUES($1,'captured')")
            .bind(task.instance_id.into_uuid()).execute(pool).await.unwrap_err(),
    };
    assert!(error.to_string().contains("unapplied worker outcome prevents capsule cut"),"{error}");
    assert!(storage.get_worker_accepted_outcome(Some(&request.owner.tenant_id),task.id,&claim).await.unwrap().is_some());
});

orch8_storage::backend_test!(async fn lossless_native_json_survives_checkpoint_source_capture_replay_and_abort(b){
    let s=b.storage.as_ref();let request=fixture(s).await;
    let payload=json!({"key\0λ":"value\0λ","float":1.0,"negative_zero":-0.0,"large_float":1e18});
    let mut instance=s.get_instance(request.owner.current_instance_id).await.unwrap().unwrap();
    instance.context.data=payload.clone();instance.context.config=payload.clone();
    instance.context.audit.push(orch8_types::context::AuditEntry{timestamp:Utc::now(),event:"audit\0λ".into(),details:payload.clone()});
    s.update_instance_context(instance.id,&instance.context).await.unwrap();
    let checkpoint=Checkpoint{id:uuid::Uuid::now_v7(),instance_id:instance.id,checkpoint_data:payload.clone(),created_at:Utc::now()+Duration::seconds(1)};
    s.save_checkpoint(&checkpoint).await.unwrap();
    assert_eq!(serde_json::to_string(&s.get_latest_checkpoint(instance.id).await.unwrap().unwrap().checkpoint_data).unwrap(),serde_json::to_string(&payload).unwrap());
    assert_eq!(serde_json::to_string(&s.list_checkpoints(instance.id,1).await.unwrap()[0].checkpoint_data).unwrap(),serde_json::to_string(&payload).unwrap());
    s.save_block_output(&BlockOutput{id:uuid::Uuid::now_v7(),instance_id:instance.id,block_id:BlockId::new("lossless"),output:payload.clone(),output_ref:None,output_size:0,attempt:0,created_at:Utc::now()}).await.unwrap();
    let external=orch8_types::output::ExternalizedEntry{instance_id:instance.id,ref_key:format!("inst:{}:ctx:lossless",instance.id),payload:payload.clone(),created_at:Utc::now(),expires_at:None};
    s.restore_externalized_entry(&external).await.unwrap();
    let first=capture(s,&request).await;
    let snapshot=first.snapshot.as_ref().unwrap();
    assert_eq!(serde_json::to_string(&snapshot.instance.context).unwrap(),serde_json::to_string(&instance.context).unwrap());
    assert_eq!(serde_json::to_string(&snapshot.checkpoint.checkpoint_data).unwrap(),serde_json::to_string(&payload).unwrap());
    assert!(snapshot.outputs.iter().any(|output|output.block_id.as_str()=="lossless" && serde_json::to_string(&output.output).unwrap()==serde_json::to_string(&payload).unwrap()));
    assert!(snapshot.execution_state.payloads.iter().any(|entry|entry.ref_key==external.ref_key && serde_json::to_string(&entry.payload).unwrap()==serde_json::to_string(&payload).unwrap()));
    let replay=capture(s,&request).await;
    assert_eq!(serde_json::to_string(&replay.snapshot).unwrap(),serde_json::to_string(&first.snapshot).unwrap());
    assert!(s.abort_preparing_capsule_cut(&request.owner.tenant_id,first.id).await.unwrap());
    assert_eq!(serde_json::to_string(&s.get_instance(instance.id).await.unwrap().unwrap().context).unwrap(),serde_json::to_string(&instance.context).unwrap());
});

orch8_storage::backend_test!(async fn pending_reported_worker_failure_blocks_capture_after_hot_gc(b) {
    use orch8_types::worker::{WorkerClaim, WorkerTask};
    use orch8_storage::capsule_export::CapsuleCutRefusal;
    let storage=b.storage.as_ref();let request=fixture(storage).await;
    let task:WorkerTask=serde_json::from_value(json!({"id":uuid::Uuid::now_v7(),"instance_id":request.owner.current_instance_id,"block_id":"charge","handler_name":"remote.capture-test","attempt":0,"params":{},"context":{},"state":"pending","created_at":Utc::now()})).unwrap();
    storage.create_worker_task(&task).await.unwrap();
    let held=storage.claim_worker_tasks(&task.handler_name,"worker",1).await.unwrap().pop().unwrap();
    let claim=WorkerClaim::new("worker",held.claim_epoch);
    assert!(storage.resolve_worker_task(&orch8_types::worker::WorkerTaskResolution {
        task_id: held.id, instance_id: held.instance_id, expected_state: held.state,
        expected_claim_epoch: held.claim_epoch, expected_worker_id: held.worker_id.clone(),
        holder_worker_id: held.worker_id.clone(), event: orch8_types::worker::WorkerAttemptEventKind::Failed,
        effect_resolution: orch8_types::worker::WorkerTaskEffectResolution::Unknown,
        reason: "failed during pause".into(), retryable: true,
        action: orch8_types::worker::WorkerTaskResolutionAction::FailTaskOnly,
    }).await.unwrap());
    storage.delete_worker_task(task.id).await.unwrap();
    assert!(matches!(storage.capture_capsule_cut(&request).await.unwrap(),CapsuleCutCapture::Refused(CapsuleCutRefusal::UnsafeBoundary)));
    let error = match &b.raw {
        orch8_storage::testing::backends::RawPool::Sqlite(pool) => sqlx::query("INSERT INTO capsule_source_cuts(source_instance_id,state) VALUES(?1,'captured')")
            .bind(task.instance_id.to_string()).execute(pool).await.unwrap_err(),
        #[cfg(feature="postgres")]
        orch8_storage::testing::backends::RawPool::Postgres(pool) => sqlx::query("INSERT INTO capsule_source_cuts(source_instance_id,state) VALUES($1,'captured')")
            .bind(task.instance_id.into_uuid()).execute(pool).await.unwrap_err(),
    };
    assert!(error.to_string().contains("unapplied worker failure prevents capsule cut"),"{error}");
    assert!(storage.get_worker_resolution_outcome(Some(&request.owner.tenant_id),task.id,Some(&claim)).await.unwrap().is_some());
});

async fn named_queue_skips_frozen_source(
    backend: &orch8_storage::testing::backends::TestBackend,
    retired: bool,
    tenant_scoped: bool,
) {
    let (request, pending) = if retired {
        legacy_retired_source(backend).await
    } else {
        let (request, _, pending) = captured_source_with_late_queue(backend).await;
        (request, pending)
    };
    let storage = backend.storage.as_ref();
    let queue = format!("frozen-source-{}", uuid::Uuid::new_v4());
    let mut blocked = pending;
    blocked.id = uuid::Uuid::new_v4();
    blocked.block_id = BlockId::new(format!("named-{}", blocked.id));
    blocked.queue_name = Some(queue.clone());
    insert_pre_guard_pending(backend, &blocked).await;
    assert_eq!(
        storage
            .get_worker_task(blocked.id)
            .await
            .unwrap()
            .unwrap()
            .queue_name,
        Some(queue.clone()),
    );
    let mut healthy = storage
        .get_instance(request.owner.current_instance_id)
        .await
        .unwrap()
        .unwrap();
    healthy.id = InstanceId::new();
    storage.create_instance(&healthy).await.unwrap();
    let mut control = blocked.clone();
    control.id = uuid::Uuid::new_v4();
    control.instance_id = healthy.id;
    control.created_at = blocked.created_at + Duration::seconds(1);
    storage.create_worker_task(&control).await.unwrap();
    let result = if tenant_scoped {
        storage
            .claim_worker_tasks_from_queue_for_tenant(
                &queue,
                &blocked.handler_name,
                "queue-worker",
                &request.owner.tenant_id,
                1,
            )
            .await
    } else {
        storage
            .claim_worker_tasks_from_queue(&queue, &blocked.handler_name, "queue-worker", 1)
            .await
    };
    let claimed = result.expect("a frozen source must not poison a named-queue poll");
    assert_eq!(
        claimed.iter().map(|task| task.id).collect::<Vec<_>>(),
        vec![control.id]
    );
    let retained = storage.get_worker_task(blocked.id).await.unwrap().unwrap();
    assert_eq!(
        retained.state,
        orch8_types::worker::WorkerTaskState::Pending
    );
    assert_eq!(retained.claim_epoch, 0);
}

orch8_storage::backend_test!(kinds=[sqlite,encrypted_sqlite], async fn named_queue_claim_skips_captured_source_before_limit(b) {
    named_queue_skips_frozen_source(&b,false,false).await;
});
orch8_storage::backend_test!(kinds=[sqlite,encrypted_sqlite], async fn named_queue_tenant_claim_skips_captured_source_before_limit(b) {
    named_queue_skips_frozen_source(&b,false,true).await;
});
orch8_storage::backend_test!(kinds=[sqlite,encrypted_sqlite], async fn named_queue_claim_skips_retired_source_after_history_gc(b) {
    named_queue_skips_frozen_source(&b,true,false).await;
});
orch8_storage::backend_test!(kinds=[sqlite,encrypted_sqlite], async fn named_queue_tenant_claim_skips_retired_source_after_history_gc(b) {
    named_queue_skips_frozen_source(&b,true,true).await;
});
