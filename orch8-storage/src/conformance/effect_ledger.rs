use chrono::Utc;
use orch8_types::continuity::{
    ContinuityExecution, ContinuityId, EffectDispatchOutcome, EffectId, EffectKind, EffectReceipt,
    EffectState, ExecutionEpoch, OwnershipState, ProvenanceEntry, RuntimeId,
};
use orch8_types::ids::{BlockId, TenantId};
use orch8_types::instance::InstanceState;
use uuid::Uuid;

use super::{
    ConformanceError, ConformanceReport, backend, conformance_instance, conformance_sequence,
    require,
};
use crate::effect_resolution::{EffectResolutionCommit, effect_resolution_digest};
use crate::{EffectDispatchFence, StorageBackend};

const DISPATCH: &str = "effect_owned_dispatch";
const AMBIGUOUS: &str = "effect_unresolved_blocks_dispatch";
const RESOLUTION: &str = "effect_atomic_resolution";
const DUPLICATE: &str = "effect_duplicate_resolution";

/// Verify effect dispatch, ambiguous results, operator settlement, and duplicate
/// completion. An unresolved receipt must block both replay and a competing
/// guarded write with the same request fingerprint.
pub async fn run_effect_ledger_conformance(
    storage: &dyn StorageBackend,
) -> Result<ConformanceReport, ConformanceError> {
    let marker = Uuid::now_v7();
    let scope = format!("orch8-effect-conformance-{marker}");
    let tenant = TenantId::new(scope.clone()).map_err(|message| ConformanceError::Violation {
        check: "scope",
        message,
    })?;
    let sequence = conformance_sequence(&tenant, marker);
    let instance = conformance_instance(&tenant, sequence.id, marker);
    backend(DISPATCH, storage.create_sequence(&sequence).await)?;
    backend(DISPATCH, storage.create_instance(&instance).await)?;
    let owner = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: tenant.clone(),
        current_instance_id: instance.id,
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    backend(DISPATCH, storage.create_continuity_execution(&owner).await)?;
    let unresolved = verify_dispatch(storage, &owner).await?;
    verify_resolution(storage, &unresolved).await?;
    backend(
        RESOLUTION,
        storage
            .update_instance_state(instance.id, InstanceState::Cancelled, None)
            .await,
    )?;
    Ok(ConformanceReport {
        scope,
        sequence_id: sequence.id,
        instance_id: instance.id,
        checks: vec![DISPATCH, AMBIGUOUS, RESOLUTION, DUPLICATE],
    })
}

async fn verify_dispatch(
    storage: &dyn StorageBackend,
    owner: &ContinuityExecution,
) -> Result<EffectReceipt, ConformanceError> {
    let tenant = &owner.tenant_id;
    let prepared = fixture(owner);
    let fence = EffectDispatchFence {
        instance_id: owner.current_instance_id,
        epoch: owner.epoch,
        owner_runtime_id: owner.owner_runtime_id,
        claim_token: None,
    };
    backend(DISPATCH, storage.create_effect_receipt(&prepared).await)?;
    let dispatched = EffectReceipt {
        state: EffectState::Dispatched,
        ..prepared.clone()
    };
    let outcome = backend(
        DISPATCH,
        storage
            .dispatch_effect_receipt_owned(tenant, &dispatched, &fence, true)
            .await,
    )?;
    require(
        DISPATCH,
        outcome == EffectDispatchOutcome::Dispatched,
        "prepared effect did not dispatch under the original owner",
    )?;

    let unresolved = EffectReceipt {
        state: EffectState::Unknown,
        ..dispatched.clone()
    };
    let changed = backend(
        AMBIGUOUS,
        storage
            .cas_effect_receipt(tenant, unresolved.id, EffectState::Dispatched, &unresolved)
            .await,
    )?;
    require(
        AMBIGUOUS,
        changed,
        "an ambiguous dispatch did not persist as unknown",
    )?;
    let found = backend(
        AMBIGUOUS,
        storage
            .find_unresolved_effect_receipt(
                tenant,
                owner.continuity_id,
                owner.current_instance_id,
                &unresolved.block_id,
                unresolved.attempt,
            )
            .await,
    )?;
    require(
        AMBIGUOUS,
        found.as_ref() == Some(&unresolved),
        "unresolved receipt could not be found by its original attempt",
    )?;

    let replay = backend(
        AMBIGUOUS,
        storage
            .dispatch_effect_receipt_owned(tenant, &dispatched, &fence, true)
            .await,
    )?;
    require(
        AMBIGUOUS,
        replay == EffectDispatchOutcome::Stale,
        "the unresolved effect itself was eligible for redispatch",
    )?;
    let duplicate = EffectReceipt {
        id: EffectId::new(),
        attempt: 1,
        ..prepared
    };
    backend(AMBIGUOUS, storage.create_effect_receipt(&duplicate).await)?;
    let competing = EffectReceipt {
        state: EffectState::Dispatched,
        ..duplicate
    };
    let outcome = backend(
        AMBIGUOUS,
        storage
            .dispatch_effect_receipt_owned(tenant, &competing, &fence, true)
            .await,
    )?;
    require(
        AMBIGUOUS,
        outcome == EffectDispatchOutcome::Duplicate,
        "a competing effect dispatched despite the unresolved receipt",
    )?;
    Ok(unresolved)
}

async fn verify_resolution(
    storage: &dyn StorageBackend,
    unresolved: &EffectReceipt,
) -> Result<(), ConformanceError> {
    let resolved = EffectReceipt {
        state: EffectState::Committed,
        provider_receipt_id: Some("operator-confirmed".into()),
        updated_at: Utc::now(),
        ..unresolved.clone()
    };
    let evidence = ProvenanceEntry {
        id: Uuid::now_v7(),
        tenant_id: unresolved.tenant_id.clone(),
        continuity_id: unresolved.continuity_id,
        epoch: unresolved.epoch,
        kind: "effect_resolved".into(),
        redacted_summary: Some("conformance: provider confirmed completion".into()),
        payload_sha256: backend(RESOLUTION, effect_resolution_digest(&resolved))?,
        previous_sha256: None,
        entry_sha256: Uuid::now_v7().simple().to_string().repeat(2),
        signing_key_id: None,
        signature: None,
        created_at: Utc::now(),
    };
    let commit = backend(
        RESOLUTION,
        storage
            .resolve_effect_with_provenance(unresolved, &resolved, &evidence)
            .await,
    )?;
    require(
        RESOLUTION,
        commit == EffectResolutionCommit::Applied,
        "settlement and provenance did not commit",
    )?;
    let replay = backend(
        DUPLICATE,
        storage
            .resolve_effect_with_provenance(unresolved, &resolved, &evidence)
            .await,
    )?;
    let stored = backend(
        DUPLICATE,
        storage
            .get_effect_receipt(&unresolved.tenant_id, unresolved.id)
            .await,
    )?;
    let provenance = backend(
        DUPLICATE,
        storage
            .list_provenance(&unresolved.tenant_id, unresolved.continuity_id, 2)
            .await,
    )?;
    require(
        DUPLICATE,
        replay == EffectResolutionCommit::Stale
            && stored.as_ref() == Some(&resolved)
            && provenance.len() == 1
            && provenance[0].id == evidence.id,
        "duplicate completion replaced the original evidence or appended another resolution",
    )
}

fn fixture(owner: &ContinuityExecution) -> EffectReceipt {
    let now = Utc::now();
    EffectReceipt {
        id: EffectId::new(),
        tenant_id: owner.tenant_id.clone(),
        continuity_id: owner.continuity_id,
        epoch: owner.epoch,
        instance_id: owner.current_instance_id,
        block_id: BlockId::new("effect"),
        kind: EffectKind::Worker,
        state: EffectState::Prepared,
        destination_fingerprint: "conformance-provider".into(),
        idempotency_key: Some("conformance-request".into()),
        request_sha256: "e".repeat(64),
        provider_receipt_id: None,
        attempt: 0,
        created_at: now,
        updated_at: now,
    }
}
