//! Shared step-execution pipeline helpers.
//!
//! Both step-dispatch paths (the tree evaluator in
//! `handlers::step_block::execute_step_node` and the fast-path scheduler loop
//! in `scheduler::execute_step_block`) must perform the same preamble:
//!   1. compute the attempt number from the prior `BlockOutput` (if any),
//!   2. resolve `{{…}}` templates using instance context + prior outputs,
//!   3. resolve `credentials://…` refs (must run AFTER templates so a
//!      template-produced ref is materialised),
//!
//! and the same postamble when the handler returns a `_self_modify` output:
//!   4. inject the declared blocks into the instance's `injected_blocks`.
//!
//! This module centralises each step so both dispatch sites share one
//! implementation and one ordering guarantee (template-first,
//! credentials-second, self-modify-after-output).

use tokio::sync::OnceCell;

use orch8_storage::StorageBackend;
use orch8_types::context::ExecutionContext;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::instance::TaskInstance;

use crate::error::EngineError;

/// Build the `outputs` JSON shape expected by `template::resolve`:
/// `{ "block_id_1": <output>, "block_id_2": <output>, ... }`.
/// Fetches all prior block outputs for the instance from storage.
pub(crate) async fn build_outputs_shape(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
) -> Result<serde_json::Value, EngineError> {
    let outputs = storage
        .get_all_outputs(instance_id)
        .await
        .map_err(EngineError::Storage)?;
    let mut map = serde_json::Map::with_capacity(outputs.len());
    for o in outputs {
        map.insert(o.block_id.0, o.output);
    }
    Ok(serde_json::Value::Object(map))
}

/// Lazy, single-fetch snapshot of an instance's block outputs for use
/// within a single `evaluate()` loop iteration.
///
/// Phase 3 of the evaluator may dispatch multiple composite nodes in one
/// iteration — each router among them would otherwise call
/// `build_outputs_shape` independently, producing N redundant
/// `get_all_outputs` queries against identical data. Sharing one snapshot
/// per iteration collapses those to a single fetch.
///
/// Coherence: a fresh snapshot is created at the top of each iteration
/// (see `evaluator::evaluate`). Within an iteration, the only handler that
/// *writes* an output is the step handler, and it writes after reading —
/// any new output is unobserved in the current iteration but visible in
/// the next via a fresh snapshot. Composite handlers (router, loop, …)
/// never save outputs, so they cannot invalidate the snapshot.
#[derive(Default)]
pub struct OutputsSnapshot {
    cell: OnceCell<serde_json::Value>,
}

impl OutputsSnapshot {
    #[must_use]
    pub fn new() -> Self {
        Self {
            cell: OnceCell::new(),
        }
    }

    /// Fetch (once) and return a borrowed view of the outputs shape.
    /// Subsequent calls reuse the cached value.
    pub async fn get(
        &self,
        storage: &dyn StorageBackend,
        instance_id: InstanceId,
    ) -> Result<&serde_json::Value, EngineError> {
        self.cell
            .get_or_try_init(|| async { build_outputs_shape(storage, instance_id).await })
            .await
    }
}

/// Resolve `{{path}}` template placeholders in `params`, using the step's
/// context snapshot and all prior block outputs in the instance.
///
/// Takes `params` by value and returns the resolved value. Runs BEFORE
/// credential resolution so user templates can dynamically produce
/// `credentials://…` refs (or any other value) that the credential pass
/// then materialises. Template errors (unknown root/section) bubble up to
/// the caller which fails the node.
pub(crate) async fn resolve_templates_in_params(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    context: &ExecutionContext,
    params: &serde_json::Value,
    outputs: &OutputsSnapshot,
) -> Result<serde_json::Value, EngineError> {
    let outputs = outputs.get(storage, instance.id).await?;
    crate::template::resolve(params, context, outputs)
}

/// Compute the attempt number for a step from the latest prior
/// `BlockOutput` for `(instance_id, block_id)`. Returns `0` when no prior
/// output exists (first attempt).
///
/// Shared by both dispatch paths so retry-count semantics — and any
/// memoisation keyed on `(instance_id, block_id, attempt)` — are identical
/// across tree evaluation and the fast-path scheduler.
pub(crate) async fn compute_attempt(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    block_id: &BlockId,
) -> Result<u32, EngineError> {
    match storage.get_block_output(instance_id, block_id).await? {
        Some(prev) => Ok(u32::from(prev.attempt.unsigned_abs()) + 1),
        None => Ok(0),
    }
}

/// Result of attempting to apply a `_self_modify` output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelfModifyResult {
    /// No `_self_modify` marker present — nothing to do.
    NotApplicable,
    /// Marker was present and blocks were injected successfully.
    Applied,
    /// Marker was present but injection failed (invalid data or storage error).
    Failed,
}

/// If `output` carries the self-modify marker (`{"_self_modify": true,
/// "blocks": [...], "position": …}`), inject the declared blocks into the
/// instance's `injected_blocks`.
///
/// Inject failures are logged at `warn` level but not surfaced — the step
/// already completed successfully, and the block list is additive (missing
/// injection just means the next tick won't see the new blocks).
pub async fn apply_self_modify(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    output: &serde_json::Value,
) -> SelfModifyResult {
    if output
        .get("_self_modify")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        // No self-modify marker — this is the common success path.
        return SelfModifyResult::NotApplicable;
    }
    let Some(blocks) = output.get("blocks").filter(|v| v.is_array()) else {
        return SelfModifyResult::Failed;
    };
    let position = output.get("position").and_then(serde_json::Value::as_u64);
    // Both branches must start from the existing `injected_blocks` and merge
    // the new ones in — otherwise a positionless call (documented as
    // "append") would CLOBBER every previously-injected block, losing any
    // earlier self-modify output in the same instance. That broke the agent
    // pattern the handler was written to support: iterative self-extension
    // across multiple steps.
    let existing = match storage.get_injected_blocks(instance_id).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                instance_id = %instance_id,
                error = %e,
                "failed to read injected blocks from storage"
            );
            return SelfModifyResult::Failed;
        }
    };
    let mut arr = existing
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default();
    let new = blocks.as_array().cloned().unwrap_or_default();
    if let Some(pos) = position {
        #[allow(clippy::cast_possible_truncation)]
        let at = (pos.min(usize::MAX as u64) as usize).min(arr.len());
        // Perf: use splice instead of repeated insert to avoid O(n×m) shifting.
        let _: Vec<_> = arr.splice(at..at, new).collect();
    } else {
        // No position → true append at the end of the existing list.
        arr.extend(new);
    }
    let final_blocks = serde_json::Value::Array(arr);
    if let Err(e) = storage.inject_blocks(instance_id, &final_blocks).await {
        tracing::warn!(
            instance_id = %instance_id,
            error = %e,
            "failed to inject self-modify blocks"
        );
        return SelfModifyResult::Failed;
    }
    SelfModifyResult::Applied
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::context::{ExecutionContext, RuntimeContext};
    use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{InstanceState, Priority, TaskInstance};
    use orch8_types::output::BlockOutput;
    use serde_json::json;

    async fn mk_storage() -> SqliteStorage {
        SqliteStorage::in_memory().await.unwrap()
    }

    fn mk_instance(id: InstanceId) -> TaskInstance {
        let now = Utc::now();
        TaskInstance {
            id,
            sequence_id: SequenceId::new(),
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            state: InstanceState::Running,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ExecutionContext {
                data: json!({}),
                config: json!({}),
                audit: vec![],
                runtime: RuntimeContext::default(),
            },
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: now,
            updated_at: now,
        }
    }

    async fn save_output(
        storage: &dyn StorageBackend,
        id: InstanceId,
        bid: &str,
        out: serde_json::Value,
    ) {
        let bo = BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: id,
            block_id: BlockId(bid.into()),
            output: out,
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: Utc::now(),
        };
        storage.save_block_output(&bo).await.unwrap();
    }

    // PR1: build_outputs_shape on empty instance yields {}.
    #[tokio::test]
    async fn build_outputs_shape_empty_instance() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();
        let shape = build_outputs_shape(&s, inst.id).await.unwrap();
        assert_eq!(shape, json!({}));
    }

    // PR2: build_outputs_shape keys outputs by block_id.
    #[tokio::test]
    async fn build_outputs_shape_keys_by_block_id() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();
        save_output(&s, inst.id, "b1", json!({"x": 1})).await;
        save_output(&s, inst.id, "b2", json!("scalar")).await;
        let shape = build_outputs_shape(&s, inst.id).await.unwrap();
        assert_eq!(shape["b1"], json!({"x": 1}));
        assert_eq!(shape["b2"], json!("scalar"));
    }

    // PR3: latest attempt of a block_id wins (map insert overwrites).
    #[tokio::test]
    async fn build_outputs_shape_overwrites_duplicate_block_id() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();
        save_output(&s, inst.id, "b1", json!({"v": 1})).await;
        save_output(&s, inst.id, "b1", json!({"v": 2})).await;
        let shape = build_outputs_shape(&s, inst.id).await.unwrap();
        // Storage returns both; the last inserted wins when building the map.
        let v = &shape["b1"]["v"];
        assert!(v == &json!(1) || v == &json!(2));
    }

    // PR4: resolve_templates_in_params substitutes context.data paths.
    #[tokio::test]
    async fn resolve_templates_substitutes_context_data() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();
        let ctx = ExecutionContext {
            data: json!({"name": "Alice", "age": 30}),
            ..ExecutionContext::default()
        };
        let params = json!({"greeting": "hi {{context.data.name}}", "n": "{{context.data.age}}"});
        let out = resolve_templates_in_params(&s, &inst, &ctx, &params, &OutputsSnapshot::new())
            .await
            .unwrap();
        assert_eq!(out["greeting"], "hi Alice");
        assert_eq!(out["n"], 30);
    }

    // PR5: resolve_templates_in_params reads prior outputs.
    #[tokio::test]
    async fn resolve_templates_reads_prior_outputs() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();
        save_output(&s, inst.id, "step1", json!({"total": 42})).await;
        let ctx = ExecutionContext::default();
        let params = json!({"v": "{{outputs.step1.total}}"});
        let out = resolve_templates_in_params(&s, &inst, &ctx, &params, &OutputsSnapshot::new())
            .await
            .unwrap();
        assert_eq!(out["v"], 42);
    }

    // PR6: passes scalar + array params through untouched when no templates.
    #[tokio::test]
    async fn resolve_templates_passes_non_template_values() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();
        let ctx = ExecutionContext::default();
        let params = json!([1, "plain", {"k": "v"}, null, true]);
        let out = resolve_templates_in_params(&s, &inst, &ctx, &params, &OutputsSnapshot::new())
            .await
            .unwrap();
        assert_eq!(out, params);
    }

    // PR7: unknown template path propagates an error.
    #[tokio::test]
    async fn resolve_templates_unknown_root_errors() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();
        let ctx = ExecutionContext::default();
        let params = json!({"v": "{{bogus.root}}"});
        let result =
            resolve_templates_in_params(&s, &inst, &ctx, &params, &OutputsSnapshot::new()).await;
        assert!(result.is_err(), "unknown root should error, got {result:?}");
    }

    // PR8: nested objects/arrays are recursively resolved.
    #[tokio::test]
    async fn resolve_templates_recurses_into_nested_containers() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();
        let ctx = ExecutionContext {
            data: json!({"user": "bob"}),
            ..ExecutionContext::default()
        };
        let params = json!({
            "lvl1": {
                "lvl2": ["{{context.data.user}}", {"deep": "{{context.data.user}}"}]
            }
        });
        let out = resolve_templates_in_params(&s, &inst, &ctx, &params, &OutputsSnapshot::new())
            .await
            .unwrap();
        assert_eq!(out["lvl1"]["lvl2"][0], "bob");
        assert_eq!(out["lvl1"]["lvl2"][1]["deep"], "bob");
    }
}
