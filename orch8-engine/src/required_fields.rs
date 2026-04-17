//! Pre-computed map of which `context.data` top-level fields each step needs.
//!
//! Built once per sequence definition, cached per-scheduler. Used by the
//! scheduler's preload step (M3.9) to fetch only the externalized fields
//! that dispatched steps will actually read, and by the marker resolution
//! path to avoid round-tripping every ref-key.
//!
//! Semantics (see `FieldAccess`):
//! - `FieldAccess::Fields { fields }` -> `Some(fields.to_vec())` — selective preload
//! - `FieldAccess::Bool(true)` / `Keyword::All` -> `None` — "fetch everything",
//!   scheduler falls back to full-context hydration for that step
//! - `FieldAccess::Bool(false)` / `Keyword::None` -> `Some(vec![])` — nothing
//!   to preload, handler gets an empty `data`
//! - No `context_access` declared on the step -> `None` (legacy = full access)
//!
//! The RFT is derived from `SequenceDefinition` alone; it does not observe
//! runtime state. A change in the sequence definition should invalidate the
//! cached tree (the scheduler keys its `DashMap` by
//! `(SequenceId, version)` so structural changes naturally invalidate).
use std::collections::HashMap;

use orch8_types::ids::BlockId;
use orch8_types::sequence::{BlockDefinition, SequenceDefinition};

/// Per-sequence static map: block -> required top-level `context.data` keys.
///
/// `None` value = fetch everything (All access or no declaration).
/// `Some(empty)` = fetch nothing.
#[derive(Debug, Clone, Default)]
pub struct RequiredFieldTree {
    per_block: HashMap<BlockId, Option<Vec<String>>>,
}

impl RequiredFieldTree {
    /// Build an RFT by walking every step in the sequence recursively.
    #[must_use]
    pub fn from_sequence(seq: &SequenceDefinition) -> Self {
        let mut per_block = HashMap::new();
        visit_blocks(&seq.blocks, &mut per_block);
        Self { per_block }
    }

    /// Returns the declared field list for `block`.
    /// - `Some(&[..])` — selective preload
    /// - `Some(&[])` — empty: handler declared no-access
    /// - `None` — full fetch required (All access or unknown block)
    #[must_use]
    pub fn fields_for(&self, block: &BlockId) -> Option<&[String]> {
        self.per_block.get(block)?.as_deref()
    }

    /// Number of blocks recorded. Exposed for metrics / tests.
    #[must_use]
    pub fn len(&self) -> usize {
        self.per_block.len()
    }

    /// Whether the tree has no recorded blocks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.per_block.is_empty()
    }
}

/// Recurse into composite blocks so that every nested Step contributes to
/// the RFT. Only Step blocks carry `context_access`; composites are
/// transparent wrappers for sequencing.
fn visit_blocks(blocks: &[BlockDefinition], out: &mut HashMap<BlockId, Option<Vec<String>>>) {
    for block in blocks {
        match block {
            BlockDefinition::Step(step) => {
                // Three-way collapse:
                //   no context_access at all       -> None   (full access, legacy default)
                //   context_access with All / bool -> None   (full access)
                //   context_access with Fields(..) -> Some(..)
                //   context_access with None       -> Some(vec![]) — required_fields()
                //                                     returns None for None too, so we
                //                                     separately check allows_any.
                let entry = match step.context_access.as_ref() {
                    None => None,
                    Some(ca) => match ca.data.required_fields() {
                        Some(fs) => Some(fs.to_vec()),
                        None => {
                            if ca.data.allows_any() {
                                None
                            } else {
                                Some(Vec::new())
                            }
                        }
                    },
                };
                out.insert(step.id.clone(), entry);
            }
            BlockDefinition::Parallel(p) => {
                for branch in &p.branches {
                    visit_blocks(branch, out);
                }
            }
            BlockDefinition::Race(r) => {
                for branch in &r.branches {
                    visit_blocks(branch, out);
                }
            }
            BlockDefinition::Loop(l) => visit_blocks(&l.body, out),
            BlockDefinition::ForEach(fe) => visit_blocks(&fe.body, out),
            BlockDefinition::Router(r) => {
                for route in &r.routes {
                    visit_blocks(&route.blocks, out);
                }
            }
            BlockDefinition::TryCatch(tc) => {
                visit_blocks(&tc.try_block, out);
                visit_blocks(&tc.catch_block, out);
                if let Some(finally) = &tc.finally_block {
                    visit_blocks(finally, out);
                }
            }
            BlockDefinition::SubSequence(_) => {
                // SubSequence dispatches into a separate SequenceDefinition; its
                // steps live in that definition's RFT, not this one.
            }
            BlockDefinition::ABSplit(ab) => {
                for variant in &ab.variants {
                    visit_blocks(&variant.blocks, out);
                }
            }
            BlockDefinition::CancellationScope(cs) => visit_blocks(&cs.blocks, out),
        }
    }
}

/// Extract the top-level key from a dotted path
/// (`"user.profile.avatar"` -> `"user"`).
///
/// Matches the granularity of [`FieldAccess::Fields`] (per top-level
/// `context.data` key); used by callers that receive dotted field references.
#[must_use]
pub fn top_level_key(path: &str) -> &str {
    path.split_once('.').map_or(path, |(head, _)| head)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use orch8_types::ids::{Namespace, SequenceId, TenantId};
    use orch8_types::sequence::{
        AccessKeyword, ContextAccess, FieldAccess, ParallelDef, SequenceDefinition, StepDef,
    };

    fn step_def(id: &str, access: Option<ContextAccess>) -> StepDef {
        StepDef {
            id: BlockId(id.into()),
            handler: "noop".into(),
            params: serde_json::Value::Null,
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: access,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
        }
    }

    fn seq_with_blocks(blocks: Vec<BlockDefinition>) -> SequenceDefinition {
        SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId("t1".into()),
            namespace: Namespace("ns".into()),
            name: "seq".into(),
            version: 1,
            deprecated: false,
            blocks,
            interceptors: None,
            created_at: Utc::now(),
        }
    }

    fn access_with(data: FieldAccess) -> ContextAccess {
        ContextAccess {
            data,
            config: true,
            audit: false,
            runtime: false,
        }
    }

    #[test]
    fn rft_collects_fields_from_step_context_access() {
        let step = step_def(
            "step-1",
            Some(access_with(FieldAccess::Fields {
                fields: vec!["user_id".into(), "order_id".into()],
            })),
        );
        let rft =
            RequiredFieldTree::from_sequence(&seq_with_blocks(vec![BlockDefinition::Step(step)]));
        assert_eq!(
            rft.fields_for(&BlockId("step-1".into())),
            Some(&["user_id".into(), "order_id".into()][..])
        );
    }

    #[test]
    fn rft_returns_none_for_all_access_via_bool() {
        let step = step_def("step-1", Some(access_with(FieldAccess::ALL)));
        let rft =
            RequiredFieldTree::from_sequence(&seq_with_blocks(vec![BlockDefinition::Step(step)]));
        assert_eq!(rft.fields_for(&BlockId("step-1".into())), None);
    }

    #[test]
    fn rft_returns_none_for_all_access_via_keyword() {
        let step = step_def(
            "step-1",
            Some(access_with(FieldAccess::Keyword(AccessKeyword::All))),
        );
        let rft =
            RequiredFieldTree::from_sequence(&seq_with_blocks(vec![BlockDefinition::Step(step)]));
        assert_eq!(rft.fields_for(&BlockId("step-1".into())), None);
    }

    #[test]
    fn rft_empty_vec_for_no_access() {
        let step = step_def("step-1", Some(access_with(FieldAccess::NONE)));
        let rft =
            RequiredFieldTree::from_sequence(&seq_with_blocks(vec![BlockDefinition::Step(step)]));
        assert_eq!(rft.fields_for(&BlockId("step-1".into())), Some(&[][..]));
    }

    #[test]
    fn rft_returns_none_when_context_access_not_declared() {
        let step = step_def("step-1", None);
        let rft =
            RequiredFieldTree::from_sequence(&seq_with_blocks(vec![BlockDefinition::Step(step)]));
        // Unknown block and "no declaration" are both None — caller falls back
        // to full fetch.
        assert_eq!(rft.fields_for(&BlockId("step-1".into())), None);
    }

    #[test]
    fn rft_recurses_into_parallel_branches() {
        let step_a = step_def(
            "a",
            Some(access_with(FieldAccess::Fields {
                fields: vec!["x".into()],
            })),
        );
        let step_b = step_def(
            "b",
            Some(access_with(FieldAccess::Fields {
                fields: vec!["y".into()],
            })),
        );
        let par = ParallelDef {
            id: BlockId("par".into()),
            branches: vec![
                vec![BlockDefinition::Step(step_a)],
                vec![BlockDefinition::Step(step_b)],
            ],
        };
        let rft =
            RequiredFieldTree::from_sequence(&seq_with_blocks(vec![BlockDefinition::Parallel(
                par,
            )]));
        assert_eq!(
            rft.fields_for(&BlockId("a".into())),
            Some(&["x".into()][..])
        );
        assert_eq!(
            rft.fields_for(&BlockId("b".into())),
            Some(&["y".into()][..])
        );
    }

    #[test]
    fn rft_unknown_block_returns_none() {
        let rft = RequiredFieldTree::default();
        assert_eq!(rft.fields_for(&BlockId("missing".into())), None);
    }

    #[test]
    fn top_level_key_extracts_head() {
        assert_eq!(top_level_key("user"), "user");
        assert_eq!(top_level_key("user.profile.avatar"), "user");
        assert_eq!(top_level_key(""), "");
    }

    #[test]
    fn rft_recurses_into_loop_body() {
        use orch8_types::sequence::LoopDef;
        let step = step_def(
            "body-step",
            Some(access_with(FieldAccess::Fields {
                fields: vec!["count".into()],
            })),
        );
        let lp = LoopDef {
            id: BlockId("lp".into()),
            condition: "false".into(),
            body: vec![BlockDefinition::Step(step)],
            max_iterations: 10,
        };
        let rft =
            RequiredFieldTree::from_sequence(&seq_with_blocks(vec![BlockDefinition::Loop(lp)]));
        assert_eq!(
            rft.fields_for(&BlockId("body-step".into())),
            Some(&["count".into()][..])
        );
        assert_eq!(rft.len(), 1, "Loop itself not recorded, only inner steps");
    }

    #[test]
    fn rft_recurses_into_for_each_and_try_catch_and_finally() {
        use orch8_types::sequence::{ForEachDef, TryCatchDef};
        let fe_step = step_def(
            "fe-s",
            Some(access_with(FieldAccess::Fields {
                fields: vec!["items".into()],
            })),
        );
        let try_step = step_def(
            "try-s",
            Some(access_with(FieldAccess::Fields {
                fields: vec!["a".into()],
            })),
        );
        let catch_step = step_def(
            "catch-s",
            Some(access_with(FieldAccess::Fields {
                fields: vec!["b".into()],
            })),
        );
        let finally_step = step_def(
            "finally-s",
            Some(access_with(FieldAccess::Fields {
                fields: vec!["c".into()],
            })),
        );
        let fe = ForEachDef {
            id: BlockId("fe".into()),
            collection: "xs".into(),
            item_var: "item".into(),
            body: vec![BlockDefinition::Step(fe_step)],
            max_iterations: 10,
        };
        let tc = TryCatchDef {
            id: BlockId("tc".into()),
            try_block: vec![BlockDefinition::Step(try_step)],
            catch_block: vec![BlockDefinition::Step(catch_step)],
            finally_block: Some(vec![BlockDefinition::Step(finally_step)]),
        };
        let rft = RequiredFieldTree::from_sequence(&seq_with_blocks(vec![
            BlockDefinition::ForEach(fe),
            BlockDefinition::TryCatch(tc),
        ]));
        assert_eq!(
            rft.fields_for(&BlockId("fe-s".into())),
            Some(&["items".into()][..])
        );
        assert_eq!(
            rft.fields_for(&BlockId("try-s".into())),
            Some(&["a".into()][..])
        );
        assert_eq!(
            rft.fields_for(&BlockId("catch-s".into())),
            Some(&["b".into()][..])
        );
        assert_eq!(
            rft.fields_for(&BlockId("finally-s".into())),
            Some(&["c".into()][..])
        );
    }

    #[test]
    fn rft_recurses_into_cancellation_scope() {
        use orch8_types::sequence::CancellationScopeDef;
        let step = step_def(
            "in-scope",
            Some(access_with(FieldAccess::Fields {
                fields: vec!["s".into()],
            })),
        );
        let cs = CancellationScopeDef {
            id: BlockId("cs".into()),
            blocks: vec![BlockDefinition::Step(step)],
        };
        let rft = RequiredFieldTree::from_sequence(&seq_with_blocks(vec![
            BlockDefinition::CancellationScope(cs),
        ]));
        assert_eq!(
            rft.fields_for(&BlockId("in-scope".into())),
            Some(&["s".into()][..])
        );
    }

    #[test]
    fn rft_sub_sequence_is_opaque_to_parent_tree() {
        use orch8_types::sequence::SubSequenceDef;
        // Sub-sequence steps belong to the CHILD's RFT, not the parent.
        let sub = SubSequenceDef {
            id: BlockId("sub".into()),
            sequence_name: "child".into(),
            version: None,
            input: serde_json::Value::Null,
        };
        let rft =
            RequiredFieldTree::from_sequence(&seq_with_blocks(vec![BlockDefinition::SubSequence(
                sub,
            )]));
        assert_eq!(
            rft.len(),
            0,
            "SubSequence must not contribute to parent RFT"
        );
        assert!(rft.is_empty());
    }
}
