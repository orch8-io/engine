use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use tracing::debug;

use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::ids::BlockId;
use orch8_types::instance::TaskInstance;
use orch8_types::output::BlockOutput;
use orch8_types::sequence::ABSplitDef;

use crate::error::EngineError;
use crate::evaluator::{all_completed, any_failed, children_of, complete_node, fail_node};
use crate::handlers::HandlerRegistry;

/// Execute an A/B split node.
///
/// Selection algorithm: deterministic hash of `(instance_id, block_id)` modulo
/// total weight. This ensures the same instance always takes the same path,
/// even across re-executions, without requiring external randomness state.
pub async fn execute_ab_split(
    storage: &dyn StorageBackend,
    _handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    ab_def: &ABSplitDef,
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    let children = children_of(tree, node.id, None);

    // If all children are still pending, we need to pick a variant and activate it.
    let all_pending = children.iter().all(|c| c.state == NodeState::Pending);

    if all_pending {
        let chosen_index = select_variant(instance, &ab_def.id, &ab_def.variants);

        // Skip all non-chosen variants, activate the chosen one.
        for child in &children {
            let branch_idx = child.branch_index.unwrap_or(-1);
            let chosen_i16 = i16::try_from(chosen_index).unwrap_or(i16::MAX);
            if branch_idx == chosen_i16 {
                storage
                    .update_node_state(child.id, NodeState::Running)
                    .await?;
            } else {
                storage
                    .update_node_state(child.id, NodeState::Skipped)
                    .await?;
            }
        }

        // Record which variant was chosen as the block output.
        let variant_name = ab_def
            .variants
            .get(chosen_index)
            .map_or("unknown", |v| v.name.as_str());

        debug!(
            instance_id = %instance.id,
            block_id = %ab_def.id.0,
            variant = variant_name,
            variant_index = chosen_index,
            "A/B split: chose variant"
        );

        let output = BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            block_id: ab_def.id.clone(),
            output: serde_json::json!({
                "variant": variant_name,
                "variant_index": chosen_index,
            }),
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: chrono::Utc::now(),
        };
        storage.save_block_output(&output).await?;

        return Ok(true);
    }

    // Check completion of the chosen branch.
    let active_children: Vec<_> = children
        .iter()
        .filter(|c| !matches!(c.state, NodeState::Skipped))
        .copied()
        .collect();

    if all_completed(&active_children) {
        complete_node(storage, node.id).await?;
        return Ok(true);
    }

    if any_failed(&active_children) {
        fail_node(storage, node.id).await?;
        return Ok(true);
    }

    // Still running — no action needed this tick.
    Ok(true)
}

/// Deterministically select a variant index based on instance ID and block ID.
/// Uses a hash to distribute uniformly across the total weight.
fn select_variant(
    instance: &TaskInstance,
    block_id: &BlockId,
    variants: &[orch8_types::sequence::ABVariant],
) -> usize {
    let total_weight: u64 = variants.iter().map(|v| u64::from(v.weight)).sum();
    if total_weight == 0 || variants.is_empty() {
        return 0;
    }

    let mut hasher = DefaultHasher::new();
    instance.id.0.hash(&mut hasher);
    block_id.0.hash(&mut hasher);
    let hash_val = hasher.finish();
    let target = hash_val % total_weight;

    let mut cumulative: u64 = 0;
    for (i, variant) in variants.iter().enumerate() {
        cumulative += u64::from(variant.weight);
        if target < cumulative {
            return i;
        }
    }
    variants.len() - 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::Priority;
    use orch8_types::sequence::ABVariant;

    fn make_instance() -> TaskInstance {
        let now = chrono::Utc::now();
        TaskInstance {
            id: InstanceId::new(),
            sequence_id: SequenceId::new(),
            tenant_id: TenantId("test".into()),
            namespace: Namespace("default".into()),
            state: orch8_types::instance::InstanceState::Running,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: serde_json::Value::Null,
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn select_variant_deterministic() {
        let instance = make_instance();
        let block_id = BlockId("split_1".into());
        let variants = vec![
            ABVariant {
                name: "control".into(),
                weight: 50,
                blocks: vec![],
            },
            ABVariant {
                name: "variant_a".into(),
                weight: 50,
                blocks: vec![],
            },
        ];

        let v1 = select_variant(&instance, &block_id, &variants);
        let v2 = select_variant(&instance, &block_id, &variants);
        assert_eq!(
            v1, v2,
            "same instance+block must always select same variant"
        );
    }

    #[test]
    fn select_variant_distribution() {
        let block_id = BlockId("split_dist".into());
        let variants = vec![
            ABVariant {
                name: "a".into(),
                weight: 70,
                blocks: vec![],
            },
            ABVariant {
                name: "b".into(),
                weight: 30,
                blocks: vec![],
            },
        ];

        let mut counts = [0u32; 2];
        for _ in 0..1000 {
            let instance = make_instance(); // random instance ID each time
            let idx = select_variant(&instance, &block_id, &variants);
            counts[idx] += 1;
        }

        // With 70/30 weights over 1000 samples, variant A should get ~700.
        // Allow wide margin for hash distribution.
        assert!(
            counts[0] > 500,
            "variant A should get majority: {}",
            counts[0]
        );
        assert!(
            counts[1] > 100,
            "variant B should get some traffic: {}",
            counts[1]
        );
    }

    #[test]
    fn select_variant_empty() {
        let instance = make_instance();
        let block_id = BlockId("empty".into());
        assert_eq!(select_variant(&instance, &block_id, &[]), 0);
    }

    #[test]
    fn select_variant_single() {
        let instance = make_instance();
        let block_id = BlockId("single".into());
        let variants = vec![ABVariant {
            name: "only".into(),
            weight: 100,
            blocks: vec![],
        }];
        assert_eq!(select_variant(&instance, &block_id, &variants), 0);
    }
}
