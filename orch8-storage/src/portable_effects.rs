//! Immutable receipt provenance, current-location aliases and frozen guard context.
use orch8_types::{
    continuity::{
        CapsuleEffectState, CapsulePayload, ContinuityExecution, EffectReceipt, EffectState,
        OwnershipState,
    },
    continuity_advanced::{InvariantRule, WorkflowInvariant},
    error::StorageError,
    ids::{InstanceId, SequenceId},
};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct CapsuleEffectImport {
    pub manifest: orch8_types::continuity::CapsuleManifest,
    pub source: ContinuityExecution,
    pub manifest_sha256: String,
    pub state: CapsuleEffectState,
}
impl CapsuleEffectImport {
    pub fn validate(&self, sequence: SequenceId) -> Result<(), StorageError> {
        let manifest = &self.manifest;
        if manifest.tenant_id != self.source.tenant_id
            || manifest.continuity_id != self.source.continuity_id
            || manifest.source_instance_id != self.source.current_instance_id
            || manifest.source_runtime_id != self.source.owner_runtime_id
            || manifest.epoch != self.source.epoch
            || manifest.sequence.id != sequence
            || !orch8_types::continuity::CapsuleSchemaVersion::CURRENT.can_read(manifest.schema)
        {
            return Err(invalid());
        }

        if self.manifest_sha256.len() != 64
            || !self.manifest_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(invalid());
        }
        validate_effect_state(
            &self.source,
            &self.state,
            sequence,
            manifest.sequence.version,
        )
    }
    pub(crate) fn accepts_owner(
        &self,
        owner: &ContinuityExecution,
        destination: InstanceId,
    ) -> bool {
        owner.tenant_id == self.source.tenant_id
            && owner.continuity_id == self.source.continuity_id
            && owner.epoch == self.source.epoch
            && owner.owner_runtime_id == self.source.owner_runtime_id
            && matches!(
                owner.state,
                OwnershipState::Owned | OwnershipState::Transferring
            )
            && (owner.current_instance_id == self.source.current_instance_id
                || (owner.state == OwnershipState::Transferring
                    && owner.current_instance_id == destination))
    }
}
/// Shared structural validation for imported and source-frozen effect evidence.
pub(crate) fn validate_effect_state(
    source: &ContinuityExecution,
    state: &CapsuleEffectState,
    sequence: SequenceId,
    sequence_version: i32,
) -> Result<(), StorageError> {
    if source.state != OwnershipState::Owned
        || state.receipts.len() > CapsulePayload::MAX_PENDING_ITEMS
        || state.active_attempts.len() > CapsulePayload::MAX_PENDING_ITEMS
        || state.reset_blocks.len() > CapsulePayload::MAX_PENDING_ITEMS
        || state.commit_guards.len() > CapsulePayload::MAX_PENDING_ITEMS
    {
        return Err(invalid());
    }
    let receipts: HashMap<_, _> = state.receipts.iter().map(|r| (r.id, r)).collect();
    if receipts.len() != state.receipts.len()
        || state.receipts.iter().any(|r| {
            r.tenant_id != source.tenant_id
                || r.continuity_id != source.continuity_id
                || r.epoch.get() > source.epoch.get()
        })
    {
        return Err(invalid());
    }
    let mut keys = HashSet::new();
    for binding in &state.active_attempts {
        let Some(receipt) = receipts.get(&binding.effect_id) else {
            return Err(invalid());
        };
        if receipt.block_id != binding.block_id
            || receipt.attempt != binding.attempt
            || !keys.insert((&binding.block_id, binding.attempt))
        {
            return Err(invalid());
        }
    }
    let resets: HashSet<_> = state.reset_blocks.iter().collect();
    let guards: HashSet<_> = state.commit_guards.iter().map(|g| g.id).collect();
    if resets.len() != state.reset_blocks.len()
        || guards.len() != state.commit_guards.len()
        || state.commit_guards.iter().any(|g| {
            g.tenant_id != source.tenant_id
                || g.sequence_id != sequence
                || g.sequence_version
                    .is_some_and(|version| version != sequence_version)
                || !g.enabled
                || !g.commit_guard
                || !matches!(g.rule, InvariantRule::EffectAtMostOnce { .. })
        })
    {
        return Err(invalid());
    }
    source.epoch.checked_next().map_err(|_| invalid())?;
    Ok(())
}
fn invalid() -> StorageError {
    StorageError::Constraint("invalid portable effect evidence".into())
}

/// Existing evidence may have advanced after the capsule cut; never rewind it.
pub(crate) fn merge_receipt(
    existing: &EffectReceipt,
    incoming: &EffectReceipt,
) -> Result<EffectReceipt, StorageError> {
    let mut a = existing.clone();
    let mut b = incoming.clone();
    a.state = EffectState::Planned;
    b.state = EffectState::Planned;
    a.updated_at = a.created_at;
    b.updated_at = b.created_at;
    a.provider_receipt_id = None;
    b.provider_receipt_id = None;
    if a != b
        || (existing.provider_receipt_id.is_some()
            && incoming.provider_receipt_id.is_some()
            && existing.provider_receipt_id != incoming.provider_receipt_id)
    {
        return Err(invalid());
    }
    let mut merged = if reaches(incoming.state, existing.state) {
        existing.clone()
    } else if reaches(existing.state, incoming.state) {
        incoming.clone()
    } else {
        return Err(StorageError::Conflict(
            "portable receipt outcomes conflict".into(),
        ));
    };
    merged.provider_receipt_id = existing
        .provider_receipt_id
        .clone()
        .or_else(|| incoming.provider_receipt_id.clone());
    if existing.state == incoming.state {
        merged.updated_at = existing.updated_at.max(incoming.updated_at);
    }
    Ok(merged)
}
fn reaches(from: EffectState, to: EffectState) -> bool {
    let states = [
        EffectState::Planned,
        EffectState::Prepared,
        EffectState::Dispatched,
        EffectState::Unknown,
        EffectState::Committed,
        EffectState::Verified,
        EffectState::Compensated,
        EffectState::Abandoned,
    ];
    let mut pending = vec![from];
    let mut seen = HashSet::new();
    while let Some(state) = pending.pop() {
        if state == to {
            return true;
        }
        if seen.insert(state) {
            pending.extend(
                states
                    .iter()
                    .copied()
                    .filter(|next| state.can_transition_to(*next)),
            );
        }
    }
    false
}

pub(crate) fn merge_guards(
    global: Vec<WorkflowInvariant>,
    frozen: Vec<WorkflowInvariant>,
) -> Result<Vec<WorkflowInvariant>, StorageError> {
    let mut guards = HashMap::new();
    for guard in global.into_iter().chain(frozen) {
        if let Some(old) = guards.insert(guard.id, guard.clone())
            && old != guard
        {
            return Err(StorageError::Conflict(
                "frozen effect guard differs from current policy".into(),
            ));
        }
        if guards.len() > CapsulePayload::MAX_PENDING_ITEMS {
            return Err(invalid());
        }
    }
    let mut guards: Vec<_> = guards.into_values().collect();
    guards.sort_unstable_by_key(|g| (g.created_at, g.id.into_uuid()));
    Ok(guards)
}
