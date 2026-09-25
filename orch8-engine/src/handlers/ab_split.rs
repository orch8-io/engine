use sha2::{Digest, Sha256};
use tracing::debug;

use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::ids::BlockId;
use orch8_types::instance::TaskInstance;
use orch8_types::output::BlockOutput;
use orch8_types::sequence::ABSplitDef;

use crate::error::EngineError;
use crate::evaluator::{
    SeqProgress, advance_sequence, children_of, settle_composite, skip_subtrees,
};
use crate::handlers::HandlerRegistry;

/// Execute an A/B split node.
///
/// Selection algorithm: deterministic hash of `(instance_id, block_id)` modulo
/// total weight. This ensures the same instance always takes the same path,
/// even across re-executions, without requiring external randomness state.
///
/// The chosen variant's blocks run as an ordered sequence (one at a time,
/// fail-fast); the other variants' subtrees are skipped on the first tick.
pub async fn execute_ab_split(
    storage: &dyn StorageBackend,
    _handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    ab_def: &ABSplitDef,
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    let children = children_of(tree, node.id, None);
    let chosen_index = select_variant(instance, &ab_def.id, &ab_def.variants);
    let chosen_i16 = i16::try_from(chosen_index).map_err(|_| {
        EngineError::InvalidConfig(format!(
            "ab_split chosen_index {chosen_index} exceeds i16 range"
        ))
    })?;

    // First tick (nothing started yet): record the choice and skip every
    // non-chosen variant. Selection is deterministic, so later ticks simply
    // recompute the same index.
    if children.iter().all(|c| c.state == NodeState::Pending) {
        let non_chosen: Vec<_> = children
            .iter()
            .filter(|c| c.branch_index != Some(chosen_i16))
            .map(|c| c.id)
            .collect();
        skip_subtrees(storage, instance.id, tree, &non_chosen).await?;

        // Record which variant was chosen as the block output.
        let variant_name = ab_def
            .variants
            .get(chosen_index)
            .map_or("unknown", |v| v.name.as_str());

        debug!(
            instance_id = %instance.id,
            block_id = %ab_def.id.as_str(),
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
    }

    // Run the chosen variant sequentially. An empty variant (or all-empty
    // variants) is `Done` immediately, so the node completes on this tick
    // instead of re-choosing and re-writing its output forever.
    let chosen_children: Vec<_> = children
        .iter()
        .filter(|c| c.branch_index == Some(chosen_i16))
        .copied()
        .collect();
    let final_state = match advance_sequence(storage, &chosen_children).await? {
        SeqProgress::Advanced | SeqProgress::Blocked => return Ok(true),
        SeqProgress::Failed | SeqProgress::Cancelled => NodeState::Failed,
        SeqProgress::Done => NodeState::Completed,
    };
    settle_composite(storage, instance.id, tree, node.id, final_state).await?;
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

    // Ref#2: use SHA-256 instead of `DefaultHasher`. `DefaultHasher` is
    // explicitly undefined-stable across Rust releases, which means a compiler
    // bump silently reshuffles every running A/B assignment. SHA-256 is a
    // deterministic, stable hash — slower than SipHash but negligible next to
    // the sequence-tick cost. We hash the instance + block bytes separated
    // by a NUL so "ab" + "c" cannot collide with "a" + "bc".
    let mut hasher = Sha256::new();
    hasher.update(instance.id.into_uuid().as_bytes());
    hasher.update(b"\0");
    hasher.update(block_id.as_str().as_bytes());
    let digest = hasher.finalize();
    let hash_val = u64::from_be_bytes(digest[..8].try_into().unwrap_or([0u8; 8]));
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
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::Priority;
    use orch8_types::sequence::ABVariant;

    fn make_instance() -> TaskInstance {
        let now = chrono::Utc::now();
        TaskInstance {
            id: InstanceId::new(),
            sequence_id: SequenceId::new(),
            tenant_id: TenantId::unchecked("test"),
            namespace: Namespace::new("default"),
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
            budget: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn select_variant_deterministic() {
        let instance = make_instance();
        let block_id = BlockId::new("split_1");
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
        let block_id = BlockId::new("split_dist");
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
        let block_id = BlockId::new("empty");
        assert_eq!(select_variant(&instance, &block_id, &[]), 0);
    }

    #[test]
    fn select_variant_single() {
        let instance = make_instance();
        let block_id = BlockId::new("single");
        let variants = vec![ABVariant {
            name: "only".into(),
            weight: 100,
            blocks: vec![],
        }];
        assert_eq!(select_variant(&instance, &block_id, &variants), 0);
    }

    #[test]
    fn select_variant_with_weight_zero_is_never_selected() {
        // #177 — a variant with weight=0 must never be returned. We verify
        // by running many instance IDs against a fixed (1, 0, 1) weighting:
        // only index 0 and 2 should ever be chosen.
        let block_id = BlockId::new("zw");
        let variants = vec![
            ABVariant {
                name: "a".into(),
                weight: 1,
                blocks: vec![],
            },
            ABVariant {
                name: "dead".into(),
                weight: 0,
                blocks: vec![],
            },
            ABVariant {
                name: "c".into(),
                weight: 1,
                blocks: vec![],
            },
        ];

        for _ in 0..500 {
            let instance = make_instance();
            let idx = select_variant(&instance, &block_id, &variants);
            assert_ne!(idx, 1, "weight=0 variant must never be chosen");
        }
    }

    #[test]
    fn select_variant_single_variant_always_selected() {
        // #176 — only one variant defined → index 0 every time, regardless
        // of the instance ID hash. Covers the trivial single-arm rollout
        // case (e.g. launching a feature to 100% of users).
        let block_id = BlockId::new("only-one");
        let variants = vec![ABVariant {
            name: "solo".into(),
            weight: 1,
            blocks: vec![],
        }];
        for _ in 0..100 {
            let instance = make_instance();
            assert_eq!(select_variant(&instance, &block_id, &variants), 0);
        }
    }

    #[test]
    fn select_variant_total_weight_zero_defaults_to_zero() {
        // All weights zero is degenerate but must not divide-by-zero —
        // the helper short-circuits to index 0.
        let block_id = BlockId::new("dz");
        let variants = vec![
            ABVariant {
                name: "a".into(),
                weight: 0,
                blocks: vec![],
            },
            ABVariant {
                name: "b".into(),
                weight: 0,
                blocks: vec![],
            },
        ];
        let instance = make_instance();
        assert_eq!(select_variant(&instance, &block_id, &variants), 0);
    }

    fn ab_node(
        inst: InstanceId,
        block: &str,
        parent: Option<orch8_types::ids::ExecutionNodeId>,
        branch: Option<i16>,
        state: NodeState,
    ) -> ExecutionNode {
        ExecutionNode {
            id: orch8_types::ids::ExecutionNodeId::new(),
            instance_id: inst,
            block_id: BlockId::new(block),
            parent_id: parent,
            block_type: if parent.is_none() {
                orch8_types::execution::BlockType::ABSplit
            } else {
                orch8_types::execution::BlockType::Step
            },
            branch_index: branch,
            state,
            started_at: None,
            completed_at: None,
        }
    }

    fn one_variant_def(blocks: Vec<orch8_types::sequence::BlockDefinition>) -> ABSplitDef {
        ABSplitDef {
            id: BlockId::new("ab"),
            variants: vec![ABVariant {
                name: "only".into(),
                weight: 1,
                blocks,
            }],
        }
    }

    async fn ab_storage(inst: &TaskInstance, nodes: &[ExecutionNode]) -> SqliteStorage {
        use orch8_storage::{ExecutionTreeStore, InstanceStore};
        let s = SqliteStorage::in_memory().await.unwrap();
        s.create_instance(inst).await.unwrap();
        if !nodes.is_empty() {
            s.create_execution_nodes_batch(nodes).await.unwrap();
        }
        s
    }

    /// ENG-C-N8: all-empty variants complete on the first tick instead of
    /// re-choosing (and re-writing the output) forever.
    #[tokio::test]
    async fn empty_variants_complete_immediately() {
        use orch8_storage::{ExecutionTreeStore, OutputStore};
        let inst = make_instance();
        let ab = ab_node(inst.id, "ab", None, None, NodeState::Running);
        let s = ab_storage(&inst, std::slice::from_ref(&ab)).await;
        let tree = s.get_execution_tree(inst.id).await.unwrap();
        execute_ab_split(
            &s,
            &HandlerRegistry::new(),
            &inst,
            &ab,
            &one_variant_def(vec![]),
            &tree,
        )
        .await
        .unwrap();
        let after = s.get_execution_tree(inst.id).await.unwrap();
        assert_eq!(after[0].state, NodeState::Completed);
        assert_eq!(s.get_all_outputs(inst.id).await.unwrap().len(), 1);
    }

    /// ENG-C-N2: the chosen variant runs sequentially with fail-fast.
    #[tokio::test]
    async fn chosen_variant_runs_sequentially_and_fails_fast() {
        use orch8_storage::ExecutionTreeStore;
        let inst = make_instance();
        let ab = ab_node(inst.id, "ab", None, None, NodeState::Running);
        let v1 = ab_node(inst.id, "v1", Some(ab.id), Some(0), NodeState::Pending);
        let v2 = ab_node(inst.id, "v2", Some(ab.id), Some(0), NodeState::Pending);
        let s = ab_storage(&inst, &[ab.clone(), v1.clone(), v2.clone()]).await;
        let def = one_variant_def(vec![]);
        let tree = s.get_execution_tree(inst.id).await.unwrap();
        execute_ab_split(&s, &HandlerRegistry::new(), &inst, &ab, &def, &tree)
            .await
            .unwrap();
        let after = s.get_execution_tree(inst.id).await.unwrap();
        let state = |t: &[ExecutionNode], id| t.iter().find(|n| n.id == id).unwrap().state;
        assert_eq!(state(&after, v1.id), NodeState::Running);
        assert_eq!(
            state(&after, v2.id),
            NodeState::Pending,
            "one block at a time"
        );

        s.update_node_state(v1.id, NodeState::Failed).await.unwrap();
        let tree = s.get_execution_tree(inst.id).await.unwrap();
        execute_ab_split(&s, &HandlerRegistry::new(), &inst, &ab, &def, &tree)
            .await
            .unwrap();
        let after = s.get_execution_tree(inst.id).await.unwrap();
        assert_eq!(state(&after, ab.id), NodeState::Failed);
        assert_eq!(
            state(&after, v2.id),
            NodeState::Skipped,
            "successor must never run"
        );
    }
}
