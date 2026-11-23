//! Fence child workflows from the preceding aggregate attempt before replay.

use std::collections::HashSet;

use orch8_storage::StorageBackend;
use orch8_types::{
    continuity::EffectState,
    error::StorageError,
    execution::BlockType,
    ids::{ExecutionNodeId, InstanceId},
    instance::TaskInstance,
    signal::{Signal, SignalType},
};
use sha2::{Digest as _, Sha256};

use super::{Boundary, CHILD_ATTEMPT_PREFIX};
use crate::{error::EngineError, evaluator, ownership::LocalOwnership};

pub(super) fn attempt_segment(boundary: ExecutionNodeId, attempt: u32) -> String {
    format!("{CHILD_ATTEMPT_PREFIX}{boundary}:{attempt}")
}

fn attempt_matches(activation: &str, prefix: &str, segment: &str, attempt: u32) -> bool {
    activation
        .split('/')
        .skip(1)
        .find(|part| part.starts_with(prefix))
        .map_or(attempt == 0, |part| part == segment)
}

pub(super) async fn quiescent(
    boundary: &Boundary<'_>,
    attempt: u32,
    check_effects: bool,
) -> Result<bool, EngineError> {
    let descendants: Vec<_> = evaluator::strict_descendants(boundary.tree, boundary.node.id)
        .into_iter()
        .filter(|node| node.block_type == BlockType::SubSequence)
        .collect();
    if descendants.is_empty() {
        return Ok(true);
    }
    let node_ids: HashSet<_> = descendants.iter().map(|node| node.id.to_string()).collect();
    let blocks: HashSet<_> = descendants
        .iter()
        .map(|node| node.block_id.as_str())
        .collect();
    let prefix = format!("{CHILD_ATTEMPT_PREFIX}{}:", boundary.node.id);
    let segment = attempt_segment(boundary.node.id, attempt);
    let storage = boundary.storage.backend();
    let mut children = storage.get_child_instances(boundary.instance.id).await?;
    children.retain(|child| match child.metadata.get("_parent_activation") {
        Some(activation) => activation.as_str().is_some_and(|activation| {
            activation
                .split('/')
                .next()
                .is_some_and(|node| node_ids.contains(node))
                && attempt_matches(activation, &prefix, &segment, attempt)
        }),
        None => {
            attempt == 0
                && child.metadata["_parent_block_id"]
                    .as_str()
                    .is_some_and(|block| blocks.contains(block))
        }
    });

    let mut seen = HashSet::new();
    let mut settled = true;
    while let Some(child) = children.pop() {
        if !seen.insert(child.id)
            || child.tenant_id != boundary.instance.tenant_id
            || child.namespace != boundary.instance.namespace
            || child.sub_tenant != boundary.instance.sub_tenant
        {
            return Err(EngineError::InvalidConfig(
                "invalid child lineage for composite retry".into(),
            ));
        }
        match crate::ownership::local_ownership(storage, &child.tenant_id, child.id).await? {
            LocalOwnership::Owned => {}
            LocalOwnership::Transferring => {
                settled = false;
                continue;
            }
            LocalOwnership::Compensating | LocalOwnership::Superseded => {
                return Err(EngineError::InvalidConfig(
                    "composite retry cannot verify a child with retired or compensating ownership"
                        .into(),
                ));
            }
        }
        if !child.state.is_terminal() {
            cancel_child(boundary, &child).await?;
            settled = false;
            continue;
        }
        if check_effects {
            check_child_effects(storage, &child).await?;
        }
        children.extend(storage.get_child_instances(child.id).await?);
    }
    Ok(settled)
}

fn cancel_signal_id(boundary: InstanceId, child: InstanceId) -> uuid::Uuid {
    let digest =
        Sha256::digest(format!("orch8-composite-child-cancel\0{boundary}\0{child}").as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Builder::from_custom_bytes(bytes).into_uuid()
}

async fn cancel_child(boundary: &Boundary<'_>, child: &TaskInstance) -> Result<(), EngineError> {
    let parent = boundary.instance;
    let signal = Signal {
        id: cancel_signal_id(parent.id, child.id),
        instance_id: child.id,
        signal_type: SignalType::Cancel,
        payload: serde_json::json!({}),
        delivered: false,
        created_at: boundary.now,
        delivered_at: None,
    };
    match boundary
        .storage
        .backend()
        .enqueue_signal_for_caller(
            parent.id,
            &parent.tenant_id,
            &signal,
            Some(boundary.claim_token),
        )
        .await
    {
        Ok(true) | Err(StorageError::Conflict(_) | StorageError::TerminalTarget { .. }) => Ok(()),
        Ok(false) => Err(EngineError::ClaimLost {
            instance_id: parent.id,
        }),
        Err(error) => Err(error.into()),
    }
}

async fn check_child_effects(
    storage: &dyn StorageBackend,
    child: &TaskInstance,
) -> Result<(), EngineError> {
    let Some(owner) = storage
        .get_continuity_execution_by_instance(&child.tenant_id, child.id)
        .await?
    else {
        return Ok(());
    };
    let mut cursor = None;
    loop {
        let page = storage
            .page_effect_receipts(&child.tenant_id, owner.continuity_id, cursor.as_ref(), 256)
            .await?;
        if let Some(effect) = page
            .receipts
            .into_iter()
            .find(|effect| matches!(effect.state, EffectState::Dispatched | EffectState::Unknown))
        {
            return Err(EngineError::EffectBlocked {
                effect_id: effect.id,
                block_id: effect.block_id,
                state: effect.state,
            });
        }
        cursor = page.next;
        if cursor.is_none() {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_attempt_matching_preserves_legacy_and_distinguishes_nested_epochs() {
        let outer = ExecutionNodeId::new();
        let inner = ExecutionNodeId::new();
        let outer_segment = attempt_segment(outer, 1);
        let inner_segment = attempt_segment(inner, 2);
        let activation = format!("node/map:4/{inner_segment}/{outer_segment}");
        let prefix = format!("@orch8-retry:{outer}:");
        assert!(attempt_matches(&activation, &prefix, &outer_segment, 1));
        assert!(!attempt_matches(
            &activation,
            &prefix,
            &attempt_segment(outer, 2),
            2
        ));
        assert!(!attempt_matches(
            &activation,
            &prefix,
            &attempt_segment(outer, 0),
            0
        ));
        assert!(attempt_matches(
            &format!("node/map:0/{inner_segment}"),
            &prefix,
            &attempt_segment(outer, 0),
            0
        ));
    }
}
