//! Versioned portable recovery rows and destination-local identity remapping.
use super::CapsuleServiceError;
use orch8_storage::{StorageBackend, restore::InstanceSnapshot};
use orch8_types::{
    continuity::{CapsuleExecutionState, CapsulePayload, CapsuleSchemaVersion},
    execution::BlockType,
    ids::{ExecutionNodeId, InstanceId},
    instance::TaskInstance,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

pub(super) async fn export(
    storage: &dyn StorageBackend,
    instance: InstanceId,
) -> Result<CapsuleExecutionState, CapsuleServiceError> {
    let nodes = storage.get_execution_tree(instance).await?;
    if nodes.len() > CapsulePayload::MAX_EXECUTION_NODES {
        return Err(CapsuleServiceError::PayloadTooLarge);
    }
    let mut payloads = Vec::new();
    let mut seen = HashSet::new();
    loop {
        let after = payloads
            .last()
            .map(|entry: &orch8_types::output::ExternalizedEntry| entry.ref_key.as_str());
        let page = storage
            .list_externalized_entries(instance, after, 1000)
            .await?;
        if page.is_empty() {
            break;
        }
        if payloads.len() + page.len() > CapsulePayload::MAX_EXTERNALIZED_ENTRIES {
            return Err(CapsuleServiceError::PayloadTooLarge);
        }
        // Cursor comparisons belong to the backend's text collation. Detect
        // repeated keys directly, without imposing Rust ordering on SQL pages.
        if page
            .iter()
            .any(|entry| entry.instance_id != instance || !seen.insert(entry.ref_key.clone()))
        {
            return Err(CapsuleServiceError::PayloadIdentityMismatch);
        }
        payloads.extend(page);
    }
    Ok(CapsuleExecutionState { nodes, payloads })
}

/// Never publish the capped prefix returned by the legacy ledger list API.
pub(super) async fn effect_ids(
    storage: &dyn StorageBackend,
    owner: &orch8_types::continuity::ContinuityExecution,
) -> Result<Vec<orch8_types::continuity::EffectId>, CapsuleServiceError> {
    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    let mut cursor = None;
    loop {
        let page = storage
            .page_effect_receipts(&owner.tenant_id, owner.continuity_id, cursor.as_ref(), 1000)
            .await?;
        if page
            .next
            .as_ref()
            .is_some_and(|next| page.receipts.last().map(|receipt| receipt.id) != Some(next.id))
        {
            return Err(CapsuleServiceError::PayloadIdentityMismatch);
        }
        if ids.len() + page.receipts.len() > CapsulePayload::MAX_PENDING_ITEMS {
            return Err(CapsuleServiceError::PayloadTooLarge);
        }
        for receipt in page.receipts {
            if receipt.tenant_id != owner.tenant_id
                || receipt.continuity_id != owner.continuity_id
                || !seen.insert(receipt.id)
            {
                return Err(CapsuleServiceError::PayloadIdentityMismatch);
            }
            ids.push(receipt.id);
        }
        match page.next {
            None => break,
            Some(next) if cursor.as_ref() != Some(&next) => cursor = Some(next),
            Some(_) => return Err(CapsuleServiceError::PayloadIdentityMismatch),
        }
    }
    Ok(ids)
}

pub(super) fn validate_version(
    payload: &CapsulePayload,
    schema: CapsuleSchemaVersion,
) -> Result<(), CapsuleServiceError> {
    if (schema == CapsuleSchemaVersion::V1) != payload.execution_state.is_none() {
        return Err(CapsuleServiceError::MissingExecutionState);
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

#[cfg(test)]
mod tests {
    use super::*;
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
        assert!(validate_version(&payload, CapsuleSchemaVersion::CURRENT).is_ok());
        assert!(matches!(
            validate_version(&payload, CapsuleSchemaVersion::V1),
            Err(CapsuleServiceError::MissingExecutionState)
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
