//! Full effect recovery must survive isolated and same-store runtime transfers.
use chrono::{Duration, Utc};
use ed25519_dalek::SigningKey;
use orch8_engine::{
    capsule::{
        CapsuleExportRequest, CapsuleImportRequest, export_paused_capsule,
        verify_and_import_paused_capsule_bytes,
    },
    effect_guard::EffectAdmission,
    handlers::{
        HandlerRegistry,
        step::{StepExecParams, execute_step_dry},
    },
};
use orch8_storage::{
    StorageBackend,
    artifacts::ObjectArtifactStore,
    sqlite::SqliteStorage,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    checkpoint::Checkpoint,
    continuity::{
        CapsuleRequirements, ContinuityExecution, ContinuityId, EffectId, EffectState,
        ExecutionEpoch, OwnershipState, RuntimeId,
    },
    ids::BlockId,
    instance::{InstanceState, TaskInstance},
};
use serde_json::json;
use std::sync::{Arc, Mutex};
mod common;

fn step_params(instance: &TaskInstance, block: &str) -> StepExecParams {
    StepExecParams {
        claim_token: None,
        instance_id: instance.id,
        tenant_id: instance.tenant_id.clone(),
        block_id: BlockId::new(block),
        handler_name: "native-charge".into(),
        params: json!({}),
        context: instance.context.clone(),
        attempt: 0,
        timeout: None,
        externalize_threshold: 0,
        wait_for_input: None,
        cache_key: None,
        output_schema: None,
    }
}

async fn source_admission(
    storage: Arc<dyn StorageBackend>,
    instance: &TaskInstance,
) -> (EffectAdmission, tokio::task::JoinHandle<()>) {
    let (send, receive) = tokio::sync::oneshot::channel();
    let send = Arc::new(Mutex::new(Some(send)));
    let mut registry = HandlerRegistry::new();
    registry.register_with_deferred_effect_admission("native-charge", move |ctx| {
        let send = send.clone();
        async move {
            send.lock()
                .unwrap()
                .take()
                .unwrap()
                .send(ctx.effect_admission.unwrap())
                .ok()
                .unwrap();
            std::future::pending().await
        }
    });
    let params = step_params(instance, "work");
    let task = tokio::spawn(async move {
        let _ = execute_step_dry(&storage, &registry, params).await;
    });
    let admission = tokio::time::timeout(std::time::Duration::from_secs(10), receive)
        .await
        .unwrap()
        .unwrap();
    (admission, task)
}

async fn run_destination(
    storage: Arc<dyn StorageBackend>,
    instance: &TaskInstance,
) -> Vec<EffectId> {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let seen = calls.clone();
    let mut registry = HandlerRegistry::new();
    registry.register_with_deferred_effect_admission("native-charge", move |ctx| {
        let seen = seen.clone();
        async move {
            let admission = ctx.effect_admission.as_ref().unwrap();
            let id = admission
                .effect_id_for(&ctx.tenant_id, ctx.instance_id, &ctx.block_id, ctx.attempt)
                .unwrap();
            admission.admit().await?;
            seen.lock().unwrap().push(id);
            Ok(json!({"receipt_id":id.to_string()}))
        }
    });
    let registry = Arc::new(registry);
    let sem = common::semaphore(4);
    let config = common::default_config();
    let cache = common::cache();
    let cancel = tokio_util::sync::CancellationToken::new();
    for _ in 0..20 {
        orch8_engine::scheduler::tick_once(&storage, &registry, &sem, &config, &cache, &cancel)
            .await
            .unwrap();
        if storage
            .get_instance(instance.id)
            .await
            .unwrap()
            .unwrap()
            .state
            .is_terminal()
        {
            break;
        }
    }
    calls.lock().unwrap().clone()
}

async fn assert_no_unclaimed_entry(
    storage: &Arc<dyn StorageBackend>,
    instance: &TaskInstance,
    original: &ContinuityExecution,
) {
    storage
        .update_instance_state(instance.id, InstanceState::Running, None)
        .await
        .unwrap();
    let scope = orch8_engine::effect_guard::ensure_effect_scope(
        storage.as_ref(),
        &instance.tenant_id,
        instance.id,
    )
    .await
    .unwrap();
    assert_eq!(
        scope.continuity_id, original.continuity_id,
        "imported/superseded instance forked an unrelated effect namespace"
    );
    let mut registry = HandlerRegistry::new();
    registry.register("native-charge", |_| async {
        panic!("quarantined or superseded provider entered")
    });
    let result = execute_step_dry(storage, &registry, step_params(instance, "work")).await;
    assert!(matches!(
        result,
        Err(orch8_engine::error::EngineError::ClaimLost { .. })
    ));
    storage
        .update_instance_state(instance.id, InstanceState::Paused, None)
        .await
        .unwrap();
}

async fn seed_guard(
    storage: &Arc<dyn StorageBackend>,
    instance: &TaskInstance,
    version: i32,
    history: bool,
) -> orch8_types::continuity_advanced::InvariantId {
    use orch8_types::continuity_advanced::{InvariantId, InvariantRule, WorkflowInvariant};
    let id = InvariantId::new();
    storage
        .create_workflow_invariant(&WorkflowInvariant {
            id,
            tenant_id: instance.tenant_id.clone(),
            sequence_id: instance.sequence_id,
            sequence_version: Some(version),
            name: "portable at most once".into(),
            rule: InvariantRule::EffectAtMostOnce {
                kind: orch8_types::continuity::EffectKind::Worker,
            },
            commit_guard: true,
            enabled: true,
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    if history {
        let mut registry = HandlerRegistry::new();
        registry.register("native-charge", |_| async {
            Ok(json!({"receipt_id":"already-applied"}))
        });
        execute_step_dry(storage, &registry, step_params(instance, "already-paid"))
            .await
            .unwrap();
    }
    id
}

#[derive(Clone, Copy)]
enum Scenario {
    Prepared,
    Dispatched,
    Unknown,
    Committed,
    GuardedHistory,
    Loop,
    GuardedLoop,
    LegacyPrepared,
    LegacyUnknown,
    UnboundUnknown,
}

#[allow(clippy::too_many_lines)] // one scenario audits the entire durable transfer boundary
async fn transfer(destination: Option<Arc<dyn StorageBackend>>, scenario: Scenario) {
    let ambiguous = matches!(
        scenario,
        Scenario::Dispatched | Scenario::Unknown | Scenario::Committed | Scenario::LegacyUnknown
    );
    let guarded = matches!(scenario, Scenario::GuardedHistory | Scenario::GuardedLoop);
    let looped = matches!(scenario, Scenario::Loop | Scenario::GuardedLoop);
    let sqlite = Arc::new(
        SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
    );
    let source: Arc<dyn StorageBackend> = sqlite.clone();
    let destination = destination.unwrap_or_else(|| source.clone());
    let builder = SequenceBuilder::new("portable-effect-state");
    let sequence = if looped {
        builder
            .block(orch8_types::sequence::BlockDefinition::ForEach(Box::new(
                orch8_types::sequence::ForEachDef {
                    id: BlockId::new("each"),
                    collection: "items".into(),
                    item_var: "item".into(),
                    body: vec![common::mk_step("work", "native-charge")],
                    max_iterations: 10,
                    retain_iterations: None,
                },
            )))
            .build()
    } else {
        builder.step("work", "native-charge").build()
    };
    source.create_sequence(&sequence).await.unwrap();
    if !Arc::ptr_eq(&source, &destination) {
        destination.create_sequence(&sequence).await.unwrap();
    }
    let instance = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Running)
        .data(json!({"items":[1,1]}))
        .build();
    source.create_instance(&instance).await.unwrap();
    let owner = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: instance.tenant_id.clone(),
        current_instance_id: instance.id,
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    source.create_continuity_execution(&owner).await.unwrap();
    let guard_id = if guarded {
        Some(seed_guard(&source, &instance, sequence.version, !looped).await)
    } else {
        None
    };
    let (admission, task) = source_admission(source.clone(), &instance).await;
    let mut original = admission
        .effect_id_for(&instance.tenant_id, instance.id, &BlockId::new("work"), 0)
        .unwrap();
    if matches!(
        scenario,
        Scenario::LegacyPrepared | Scenario::LegacyUnknown | Scenario::UnboundUnknown
    ) {
        // Recreate a pre-binding database: immutable origin receipt, no alias or
        // retirement marker. Hash the historical public ID recipe independently.
        use sha2::{Digest, Sha256};
        let mut receipt = source
            .get_effect_receipt(&instance.tenant_id, original)
            .await
            .unwrap()
            .unwrap();
        sqlx::query("DELETE FROM effect_attempt_bindings")
            .execute(sqlite.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM effect_receipts")
            .execute(sqlite.pool())
            .await
            .unwrap();
        let mut hash = Sha256::new();
        hash.update(b"orch8-effect-v1\0");
        hash.update(owner.continuity_id.as_uuid().as_bytes());
        hash.update(owner.epoch.get().to_be_bytes());
        hash.update(instance.id.into_uuid().as_bytes());
        hash.update(b"work");
        hash.update(0_u32.to_be_bytes());
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&hash.finalize()[..16]);
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        original = if matches!(scenario, Scenario::UnboundUnknown) {
            EffectId::new()
        } else {
            EffectId::from_uuid(uuid::Uuid::from_bytes(bytes))
        };
        receipt.id = original;
        if !matches!(scenario, Scenario::LegacyPrepared) {
            receipt
                .transition(EffectState::Dispatched, Utc::now())
                .unwrap();
            receipt
                .transition(EffectState::Unknown, Utc::now())
                .unwrap();
        }
        source.create_effect_receipt(&receipt).await.unwrap();
    } else if ambiguous {
        admission.admit().await.unwrap();
    }
    if matches!(scenario, Scenario::Unknown | Scenario::Committed) {
        let mut receipt = source
            .get_effect_receipt(&instance.tenant_id, original)
            .await
            .unwrap()
            .unwrap();
        let previous = receipt.state;
        let next = if matches!(scenario, Scenario::Unknown) {
            EffectState::Unknown
        } else {
            EffectState::Committed
        };
        receipt.transition(next, Utc::now()).unwrap();
        if next == EffectState::Committed {
            receipt.provider_receipt_id = Some("confirmed-before-transfer".into());
        }
        assert!(
            source
                .cas_effect_receipt(&instance.tenant_id, original, previous, &receipt)
                .await
                .unwrap()
        );
    }

    source
        .update_instance_state(instance.id, InstanceState::Paused, None)
        .await
        .unwrap();
    source
        .save_checkpoint(&Checkpoint {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            checkpoint_data: json!({"completed_blocks":[]}),
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    let runtime = RuntimeId::new();
    let encryptor = orch8_types::encryption::FieldEncryptor::from_bytes(&[9; 32]);
    let signed = export_paused_capsule(
        source.as_ref(),
        CapsuleExportRequest {
            continuity: owner.clone(),
            destination_runtime_id: Some(runtime),
            requirements: CapsuleRequirements::default(),
            expires_at: Utc::now() + Duration::minutes(5),
            signing_key_id: "signer".into(),
            encryption_key_id: "payload".into(),
        },
        &SigningKey::from_bytes(&[7; 32]),
        &encryptor,
    )
    .await;
    if matches!(scenario, Scenario::UnboundUnknown) {
        assert!(matches!(
            signed,
            Err(orch8_engine::capsule::CapsuleServiceError::MissingEffectState)
        ));
        task.abort();
        let _ = task.await;
        return;
    }
    let signed = signed.unwrap();
    let sealed = source
        .get_artifact(&signed.manifest.payload_artifact.key)
        .await
        .unwrap()
        .unwrap();
    let transferring = ContinuityExecution {
        state: OwnershipState::Transferring,
        updated_at: Utc::now(),
        ..owner.clone()
    };
    assert!(
        source
            .cas_continuity_owner(
                &instance.tenant_id,
                owner.continuity_id,
                owner.epoch,
                owner.owner_runtime_id,
                &transferring
            )
            .await
            .unwrap()
    );
    let (imported, _) = verify_and_import_paused_capsule_bytes(
        destination.as_ref(),
        &signed,
        &sealed,
        CapsuleImportRequest {
            tenant_id: &instance.tenant_id,
            destination_runtime_id: runtime,
            destination_instance_id: None,
            expected_epoch: owner.epoch,
            trusted_public_keys: std::slice::from_ref(&signed.public_key),
            now: Utc::now(),
        },
        &encryptor,
    )
    .await
    .unwrap();
    assert_no_unclaimed_entry(&destination, &imported, &owner).await;
    let activated = orch8_engine::capsule::activate_imported_capsule(
        destination.as_ref(),
        &owner.tenant_id,
        signed.manifest.capsule_id,
        runtime,
        imported.id,
        Utc::now(),
    )
    .await
    .unwrap();
    assert_eq!(activated.state, InstanceState::Scheduled);
    assert_no_unclaimed_entry(&source, &instance, &owner).await;

    if !ambiguous {
        assert!(
            admission.admit().await.is_err(),
            "source admission survived transfer"
        );
    }
    let calls = run_destination(destination.clone(), &imported).await;
    task.abort();
    let _ = task.await;
    let before = destination
        .get_instance(imported.id)
        .await
        .unwrap()
        .unwrap()
        .state;
    for requested_id in [None, Some(imported.id)] {
        let (again, _) = verify_and_import_paused_capsule_bytes(
            destination.as_ref(),
            &signed,
            &sealed,
            CapsuleImportRequest {
                tenant_id: &instance.tenant_id,
                destination_runtime_id: runtime,
                destination_instance_id: requested_id,
                expected_epoch: owner.epoch,
                trusted_public_keys: std::slice::from_ref(&signed.public_key),
                now: Utc::now(),
            },
            &encryptor,
        )
        .await
        .unwrap();
        assert_eq!(again.id, imported.id);
        assert_eq!(
            again.state, before,
            "redelivery rewound an advanced destination"
        );
    }
    let mut changed = signed.manifest.clone();
    changed.source_runtime_id = RuntimeId::new();
    let changed =
        orch8_publisher::capsule::sign_capsule_manifest(changed, &SigningKey::from_bytes(&[7; 32]))
            .unwrap();
    assert!(
        verify_and_import_paused_capsule_bytes(
            destination.as_ref(),
            &changed,
            &sealed,
            CapsuleImportRequest {
                tenant_id: &instance.tenant_id,
                destination_runtime_id: runtime,
                destination_instance_id: None,
                expected_epoch: owner.epoch,
                trusted_public_keys: std::slice::from_ref(&changed.public_key),
                now: Utc::now()
            },
            &encryptor
        )
        .await
        .is_err(),
        "same capsule ID accepted different immutable source metadata"
    );

    if ambiguous {
        assert!(
            calls.is_empty(),
            "transferred uncertain attempt reentered the provider"
        );
        assert_eq!(
            destination
                .get_effect_receipt(&instance.tenant_id, original)
                .await
                .unwrap()
                .unwrap()
                .state,
            if matches!(scenario, Scenario::Committed) {
                EffectState::Committed
            } else {
                EffectState::Unknown
            }
        );
    } else if guarded {
        if looped {
            assert_eq!(calls, vec![original]);
        } else {
            assert!(calls.is_empty());
        }
        let results = destination
            .list_invariant_results(&instance.tenant_id, owner.continuity_id, 100)
            .await
            .unwrap();
        assert!(
            results
                .iter()
                .any(|result| Some(result.invariant_id) == guard_id
                    && result.status == orch8_types::continuity_advanced::EvidenceStatus::Fail
                    && result.epoch == owner.epoch.checked_next().unwrap()),
            "transferred guard must record current-epoch failure evidence"
        );
        if !Arc::ptr_eq(&source, &destination) {
            assert!(
                destination
                    .list_effect_commit_guards(
                        &instance.tenant_id,
                        sequence.id,
                        sequence.version,
                        orch8_types::continuity::EffectKind::Worker,
                        100
                    )
                    .await
                    .unwrap()
                    .is_empty(),
                "import must not enable a global policy for unrelated runs"
            );
        }
    } else if looped {
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], original);
        assert_ne!(
            calls[0], calls[1],
            "retired imported alias was reused for the next loop intent"
        );
    } else {
        assert_eq!(
            calls,
            vec![original],
            "handoff changed the prepared provider identity"
        );
        let receipt = destination
            .get_effect_receipt(&instance.tenant_id, original)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.state, EffectState::Committed);
        assert_eq!(
            receipt.instance_id, instance.id,
            "origin evidence was rewritten"
        );
        assert_eq!(receipt.epoch, owner.epoch);
    }
}

orch8_storage::backend_test!(async fn prepared_effect_preserves_identity_and_origin_across_transfer(b) {
    transfer(Some(b.storage),Scenario::Prepared).await;
});
orch8_storage::backend_test!(async fn uncertain_effect_blocks_replay_after_transfer(b) {
    transfer(Some(b.storage),Scenario::Dispatched).await;
});
#[tokio::test]
async fn same_store_transfer_preserves_prepared_provider_identity() {
    transfer(None, Scenario::Prepared).await;
}

orch8_storage::backend_test!(async fn committed_history_and_required_guards_survive_transfer(b) {
    transfer(Some(b.storage),Scenario::GuardedHistory).await;
});
orch8_storage::backend_test!(async fn imported_attempt_retires_before_the_next_loop_intent(b) {
    transfer(Some(b.storage),Scenario::Loop).await;
});
orch8_storage::backend_test!(async fn frozen_guard_keeps_history_across_destination_loop_reset(b) {
    transfer(Some(b.storage),Scenario::GuardedLoop).await;
});

#[tokio::test]
async fn same_store_loop_retires_the_imported_alias() {
    transfer(None, Scenario::Loop).await;
}
#[tokio::test]
async fn same_store_frozen_guard_keeps_history() {
    transfer(None, Scenario::GuardedLoop).await;
}

orch8_storage::backend_test!(async fn already_unknown_effect_is_not_replayed_after_transfer(b) {
    transfer(Some(b.storage),Scenario::Unknown).await;
});
orch8_storage::backend_test!(async fn committed_active_effect_is_not_replayed_after_transfer(b) {
    transfer(Some(b.storage),Scenario::Committed).await;
});

orch8_storage::backend_test!(async fn legacy_prepared_receipt_transfers_without_changing_provider_identity(b) {
    transfer(Some(b.storage), Scenario::LegacyPrepared).await;
});
orch8_storage::backend_test!(async fn legacy_unknown_receipt_stays_blocked_after_transfer(b) {
    transfer(Some(b.storage), Scenario::LegacyUnknown).await;
});
#[tokio::test]
async fn unbound_noncanonical_unknown_receipt_refuses_export() {
    transfer(None, Scenario::UnboundUnknown).await;
}
