//! Versioned portable recovery rows and destination-local identity remapping.
use super::CapsuleServiceError;
use orch8_storage::restore::InstanceSnapshot;
use orch8_types::{
    continuity::{CapsulePayload, CapsuleSchemaVersion},
    execution::BlockType,
    ids::{ExecutionNodeId, InstanceId},
    instance::TaskInstance,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Adopt only exact pre-binding IDs at the current source location. Retired
/// generations and receipt origins from earlier hops are history, not aliases.
pub(super) fn complete_legacy_bindings(
    owner: &orch8_types::continuity::ContinuityExecution,
    state: &mut orch8_types::continuity::CapsuleEffectState,
) -> Result<(), CapsuleServiceError> {
    let mut keys: HashSet<_> = state
        .active_attempts
        .iter()
        .map(|binding| (binding.block_id.clone(), binding.attempt))
        .collect();
    let resets: HashSet<_> = state.reset_blocks.iter().collect();
    for receipt in &state.receipts {
        if receipt.instance_id != owner.current_instance_id
            || receipt.epoch != owner.epoch
            || keys.contains(&(receipt.block_id.clone(), receipt.attempt))
            || resets.contains(&receipt.block_id)
        {
            continue;
        }
        let legacy = crate::effect_guard::legacy_effect_id(
            owner.continuity_id,
            owner.epoch,
            owner.current_instance_id,
            &receipt.block_id,
            receipt.attempt,
        );
        if receipt.id == legacy {
            state
                .active_attempts
                .push(orch8_types::continuity::CapsuleEffectBinding {
                    block_id: receipt.block_id.clone(),
                    attempt: receipt.attempt,
                    effect_id: receipt.id,
                });
            keys.insert((receipt.block_id.clone(), receipt.attempt));
        } else if !receipt.state.is_resolved() {
            return Err(CapsuleServiceError::MissingEffectState);
        }
    }
    Ok(())
}

pub(super) fn validate_version(
    payload: &CapsulePayload,
    schema: CapsuleSchemaVersion,
) -> Result<(), CapsuleServiceError> {
    if (schema == CapsuleSchemaVersion::V1) != payload.execution_state.is_none() {
        return Err(CapsuleServiceError::MissingExecutionState);
    }
    if (schema.minor >= 2) != payload.effect_state.is_some()
        || (payload.effect_state.is_none() && !payload.effect_ids.is_empty())
    {
        return Err(CapsuleServiceError::MissingEffectState);
    }
    Ok(())
}

/// Imports stay paused until the destination has accepted exclusive ownership.
pub(super) fn paused_instance(
    payload: &CapsulePayload,
    tenant_id: &orch8_types::ids::TenantId,
    id: InstanceId,
    now: chrono::DateTime<chrono::Utc>,
) -> TaskInstance {
    TaskInstance {
        claimed_at: None,
        sub_tenant: None,
        id,
        sequence_id: payload.instance.sequence_id,
        tenant_id: tenant_id.clone(),
        namespace: payload.instance.namespace.clone(),
        state: orch8_types::instance::InstanceState::Paused,
        next_fire_at: None,
        priority: payload.instance.priority,
        timezone: payload.instance.timezone.clone(),
        metadata: payload.instance.metadata.clone(),
        context: payload.instance.context.clone(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: payload.instance.parent_instance_id,
        budget: payload.instance.budget.clone(),
        created_at: now,
        updated_at: now,
    }
}

pub(super) fn restore(
    payload: &CapsulePayload,
    source_id: InstanceId,
    mut instance: TaskInstance,
    mut outputs: Vec<orch8_types::output::BlockOutput>,
) -> Result<InstanceSnapshot, CapsuleServiceError> {
    let state = payload.execution_state.clone().unwrap_or_default();
    let mut source = instance.clone();
    source.id = source_id;
    InstanceSnapshot {
        instance: source,
        nodes: state.nodes.clone(),
        outputs: payload.outputs.clone(),
        payloads: state.payloads.clone(),
    }
    .validate()
    .map_err(|_| CapsuleServiceError::PayloadIdentityMismatch)?;
    let mut nodes = state.nodes;
    nodes.sort_unstable_by_key(|node| node.id);
    let mut new_ids = (0..nodes.len())
        .map(|_| ExecutionNodeId::new())
        .collect::<Vec<_>>();
    new_ids.sort_unstable();
    let node_ids: HashMap<_, _> = nodes.iter().map(|node| node.id).zip(new_ids).collect();
    for node in &mut nodes {
        node.id = node_ids[&node.id];
        node.instance_id = instance.id;
        node.parent_id = node.parent_id.map(|id| node_ids[&id]);
    }
    let references: HashMap<_, _> = state
        .payloads
        .iter()
        .filter(|entry| {
            entry
                .expires_at
                .is_none_or(|expiry| expiry > instance.created_at)
        })
        .map(|entry| {
            (
                entry.ref_key.clone(),
                destination_key(&entry.ref_key, source_id, instance.id),
            )
        })
        .collect();
    let mut payloads = state.payloads;
    for entry in &mut payloads {
        entry.instance_id = instance.id;
        entry.ref_key = destination_key(&entry.ref_key, source_id, instance.id);
        rewrite_markers(&mut entry.payload, &references)?;
    }
    let foreach_blocks: HashSet<_> = nodes
        .iter()
        .filter(|node| node.block_type == BlockType::ForEach)
        .map(|node| &node.block_id)
        .collect();
    let active_foreach: HashSet<_> = nodes
        .iter()
        .filter(|node| node.block_type == BlockType::ForEach && !node.state.is_terminal())
        .map(|node| &node.block_id)
        .collect();
    for output in &mut outputs {
        if active_foreach.contains(&output.block_id)
            && !references.contains_key(&format!(
                "inst:{source_id}:foreach:{}:items",
                output.block_id
            ))
        {
            return Err(CapsuleServiceError::MissingExecutionState);
        }

        rewrite_markers(&mut output.output, &references)?;
        if let Some(reference) = output.output_ref.as_mut()
            && let Some(remapped) = references.get(reference)
        {
            *reference = remapped.clone();
        }
        if foreach_blocks.contains(&output.block_id)
            && let Some(reference) = output.output.get_mut("_snapshot_ref")
        {
            let key = reference
                .as_str()
                .and_then(|key| references.get(key))
                .ok_or(CapsuleServiceError::MissingExecutionState)?;
            *reference = Value::String(key.clone());
        }
    }
    rewrite_markers(&mut instance.context.data, &references)?;
    rewrite_markers(&mut instance.context.config, &references)?;
    instance.context.runtime.instance_id = Some(instance.id.to_string());
    let snapshot = InstanceSnapshot {
        instance,
        nodes,
        outputs,
        payloads,
    };
    snapshot.validate()?;
    Ok(snapshot)
}

fn destination_key(key: &str, source: InstanceId, destination: InstanceId) -> String {
    if let Some(suffix) = key.strip_prefix(&format!("inst:{source}:")) {
        format!("inst:{destination}:{suffix}")
    } else if let Some(suffix) = key.strip_prefix(&format!("{source}:")) {
        format!("{destination}:{suffix}")
    } else {
        format!("capsule:{destination}:{key}")
    }
}

fn rewrite_markers(
    value: &mut Value,
    references: &HashMap<String, String>,
) -> Result<(), CapsuleServiceError> {
    if let Some(key) = crate::externalized::extract_ref_key(value) {
        let key = references
            .get(key)
            .ok_or(CapsuleServiceError::MissingExecutionState)?
            .clone();
        value["_ref"] = Value::String(key);
        return Ok(());
    }
    match value {
        Value::Object(object) => {
            for value in object.values_mut() {
                rewrite_markers(value, references)?;
            }
        }
        Value::Array(array) => {
            for value in array {
                rewrite_markers(value, references)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Bind portable outcomes and active aliases to the verified immutable header.
pub(super) fn effect_import(
    payload: &CapsulePayload,
    signed: &orch8_publisher::capsule::SignedCapsuleManifest,
) -> Result<orch8_storage::portable_effects::CapsuleEffectImport, CapsuleServiceError> {
    let effects = orch8_storage::portable_effects::CapsuleEffectImport {
        manifest: signed.manifest.clone(),
        source: orch8_types::continuity::ContinuityExecution {
            continuity_id: signed.manifest.continuity_id,
            tenant_id: signed.manifest.tenant_id.clone(),
            current_instance_id: signed.manifest.source_instance_id,
            owner_runtime_id: signed.manifest.source_runtime_id,
            epoch: signed.manifest.epoch,
            state: orch8_types::continuity::OwnershipState::Owned,
            updated_at: signed.manifest.issued_at,
        },
        manifest_sha256: signed.manifest_sha256.clone(),
        state: payload.effect_state.clone().unwrap_or_default(),
    };
    effects.validate(payload.instance.sequence_id)?;
    let ids: std::collections::HashSet<_> = payload.effect_ids.iter().copied().collect();
    if ids.len() != payload.effect_ids.len()
        || ids != effects.state.receipts.iter().map(|r| r.id).collect()
    {
        return Err(CapsuleServiceError::PayloadIdentityMismatch);
    }
    Ok(effects)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::continuity::CapsuleExecutionState;
    use serde_json::json;

    fn payload() -> CapsulePayload {
        serde_json::from_value(json!({
            "instance": {"sequence_id": orch8_types::ids::SequenceId::new(), "namespace":"default", "priority":"normal", "timezone":"UTC", "metadata":{}, "context":{}, "budget":null, "parent_instance_id":null},
            "checkpoint": {"id":uuid::Uuid::now_v7(), "instance_id":InstanceId::new(), "checkpoint_data":{}, "created_at":chrono::Utc::now()}
        })).unwrap()
    }

    #[test]
    fn schema_and_recovery_envelope_must_agree() {
        let mut payload = payload();
        assert!(validate_version(&payload, CapsuleSchemaVersion::V1).is_ok());
        assert!(matches!(
            validate_version(&payload, CapsuleSchemaVersion::CURRENT),
            Err(CapsuleServiceError::MissingExecutionState)
        ));
        payload.execution_state = Some(CapsuleExecutionState::default());
        assert!(validate_version(&payload, CapsuleSchemaVersion::V1_1).is_ok());
        assert!(matches!(
            validate_version(&payload, CapsuleSchemaVersion::CURRENT),
            Err(CapsuleServiceError::MissingEffectState)
        ));
        payload.effect_state = Some(orch8_types::continuity::CapsuleEffectState::default());
        assert!(validate_version(&payload, CapsuleSchemaVersion::CURRENT).is_ok());
        assert!(matches!(
            validate_version(&payload, CapsuleSchemaVersion::V1_1),
            Err(CapsuleServiceError::MissingEffectState)
        ));
        assert!(matches!(
            validate_version(&payload, CapsuleSchemaVersion::V1),
            Err(CapsuleServiceError::MissingExecutionState)
        ));
    }

    #[test]
    fn legacy_capsules_cannot_claim_effect_identity_without_outcome_evidence() {
        let mut payload = payload();
        payload
            .effect_ids
            .push(orch8_types::continuity::EffectId::new());
        assert!(matches!(
            validate_version(&payload, CapsuleSchemaVersion::V1),
            Err(CapsuleServiceError::MissingEffectState)
        ));
        payload.execution_state = Some(CapsuleExecutionState::default());
        assert!(matches!(
            validate_version(&payload, CapsuleSchemaVersion::V1_1),
            Err(CapsuleServiceError::MissingEffectState)
        ));
    }

    #[test]
    fn reference_rewrite_preserves_user_data_and_refuses_missing_ownership() {
        let references = HashMap::from([("owned".into(), "destination-owned".into())]);
        let literal = json!({"_externalized":true,"_ref":"owned","business":"literal"});
        let mut value = json!({"literal":literal,"nested":[{"_externalized":true,"_ref":"owned"}]});
        rewrite_markers(&mut value, &references).unwrap();
        assert_eq!(value["literal"], literal);
        assert_eq!(value["nested"][0]["_ref"], "destination-owned");
        assert!(matches!(
            rewrite_markers(
                &mut json!({"_externalized":true,"_ref":"foreign"}),
                &references
            ),
            Err(CapsuleServiceError::MissingExecutionState)
        ));
    }
    #[test]
    fn active_foreach_requires_an_unexpired_frozen_snapshot_even_without_marker_ref() {
        let mut payload = payload();
        let source = payload.checkpoint.instance_id;
        let now = chrono::Utc::now();
        let node = orch8_types::execution::ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id: source,
            block_id: orch8_types::ids::BlockId::new("each"),
            parent_id: None,
            block_type: BlockType::ForEach,
            branch_index: None,
            state: orch8_types::execution::NodeState::Running,
            started_at: Some(now),
            completed_at: None,
        };
        let output = orch8_types::output::BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: source,
            block_id: node.block_id.clone(),
            output: json!({"_index":1,"_total":3}),
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: now,
        };
        payload.outputs.push(output);
        payload.execution_state = Some(CapsuleExecutionState {
            nodes: vec![node],
            payloads: vec![],
        });
        let instance = paused_instance(
            &payload,
            &orch8_types::ids::TenantId::new("account").unwrap(),
            InstanceId::new(),
            now,
        );
        let mut outputs = payload.outputs.clone();
        outputs[0].instance_id = instance.id;
        assert!(matches!(
            restore(&payload, source, instance.clone(), outputs.clone()),
            Err(CapsuleServiceError::MissingExecutionState)
        ));
        let entry = orch8_types::output::ExternalizedEntry {
            instance_id: source,
            ref_key: format!("inst:{source}:foreach:each:items"),
            payload: json!([10, 20, 30]),
            created_at: now,
            expires_at: Some(now - chrono::Duration::seconds(1)),
        };
        payload
            .execution_state
            .as_mut()
            .unwrap()
            .payloads
            .push(entry);
        assert!(matches!(
            restore(&payload, source, instance.clone(), outputs.clone()),
            Err(CapsuleServiceError::MissingExecutionState)
        ));
        payload.execution_state.as_mut().unwrap().payloads[0].expires_at =
            Some(now + chrono::Duration::days(1));
        assert!(restore(&payload, source, instance, outputs).is_ok());
    }
}
