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
///
/// Internal bookkeeping rows are NOT step outputs and must never surface in
/// `{{ outputs.* }}` templates, so they are filtered here as defense in
/// depth (the completed-id queries already exclude them, but a stale row can
/// still be returned by `get_all_outputs` — e.g. the sentinel left behind by
/// a crashed attempt after the re-executed step saves its real output):
///   - `__in_progress__`: crash-mid-step sentinel — its `{"_sentinel":
///     true, ...}` payload is garbage to downstream templates.
///   - `__retry__`: attempt-counter marker — likewise not a real result.
///
/// `__error__` rows are kept deliberately: they are genuine failure evidence
/// a compensation / `try_catch` path may legitimately reference.
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
        if matches!(
            o.output_ref.as_deref(),
            Some(IN_PROGRESS_SENTINEL | "__retry__")
        ) {
            continue;
        }
        map.insert(o.block_id.as_str().to_owned(), o.output);
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
    if !crate::template::contains_template(params) {
        return Ok(params.clone());
    }
    let outputs = outputs.get(storage, instance.id).await?;
    // Only pay the instance-KV round-trip + serialization when a template
    // actually references `{{ state.* }}`. The common case (outputs-/context-
    // only templates) skips it entirely.
    let state_value = if crate::template::references_state(params) {
        let state_map = storage
            .get_all_instance_kv(instance.id)
            .await
            .map_err(EngineError::Storage)?;
        Some(serde_json::to_value(&state_map).unwrap_or(serde_json::Value::Null))
    } else {
        None
    };
    crate::template::resolve_with_state(params, context, outputs, state_value.as_ref())
}

/// `output_ref` marker for the pre-execution sentinel written by the fast-path
/// scheduler before invoking a side-effectful handler. It is NOT a real output:
/// `compute_attempt` and the memoization guard in `execute_step_dry` must treat
/// it specially so a crash mid-step neither replays a bogus output nor inflates
/// the retry count. It is likewise excluded from the completed-block set
/// (`get_completed_block_ids*`) so a crashed step re-executes on recovery, and
/// filtered from `build_outputs_shape` so it can never reach `{{ outputs.* }}`.
pub(crate) const IN_PROGRESS_SENTINEL: &str = "__in_progress__";

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
        // An `__in_progress__` sentinel is the most-recent row only when a
        // prior execution of THIS attempt crashed after writing the sentinel
        // but before persisting the real output. Resume the SAME attempt
        // (no `+ 1`): the sentinel carries the in-flight attempt number, so we
        // neither inflate the retry count nor explode to `u16::MAX + 1` (which
        // previously fast-tracked the step straight to the DLQ). The handler
        // re-runs at the correct attempt — the engine's at-least-once contract.
        Some(prev) if prev.output_ref.as_deref() == Some(IN_PROGRESS_SENTINEL) => {
            Ok(u32::from(prev.attempt))
        }
        Some(prev) => Ok(u32::from(prev.attempt) + 1),
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
/// Delegates the merge entirely to `StorageBackend::inject_blocks_at_position`
/// rather than reading `injected_blocks` here and writing the merged result
/// back with the plain `inject_blocks` -- that two-step read-then-write was
/// not atomic (a concurrent self-modify call could interleave and lose a
/// write) and, since `inject_blocks` itself appends its argument onto
/// whatever is already stored, double-counted every block that was already
/// present by the time this function's own merge ran (a previously-injected
/// block would come back twice). `inject_blocks_at_position` does the read
/// and the position-clamped insert inside a single transaction, so there is
/// exactly one merge, not two. A missing `position` is expressed as
/// `Some(usize::MAX)`, which the storage layer clamps to the current
/// length -- i.e. append at the end.
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
    #[allow(clippy::cast_possible_truncation)]
    let position = output
        .get("position")
        .and_then(serde_json::Value::as_u64)
        .map_or(usize::MAX, |pos| pos.min(usize::MAX as u64) as usize);
    if let Err(e) = storage
        .inject_blocks_at_position(instance_id, blocks, Some(position))
        .await
    {
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
    use orch8_storage::{InstanceStore, sqlite::SqliteStorage};
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
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("ns"),
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
            budget: None,
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
        save_output_with_ref(storage, id, bid, out, None).await;
    }

    async fn save_output_with_ref(
        storage: &dyn StorageBackend,
        id: InstanceId,
        bid: &str,
        out: serde_json::Value,
        output_ref: Option<&str>,
    ) {
        let bo = BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: id,
            block_id: BlockId::new(bid),
            output: out,
            output_ref: output_ref.map(str::to_owned),
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
        // `get_all_outputs` orders by `created_at, id` and ids are UUIDv7
        // (time-ordered), so the second save deterministically wins even if
        // both rows land in the same millisecond.
        assert_eq!(shape["b1"], json!({"v": 2}));
    }

    // PR3b: a crash-mid-step sentinel must never surface as a block's output —
    // its `{"_sentinel": true, ...}` payload is garbage to `{{ outputs.* }}`.
    // Regression guard for the fast-path crash-recovery contract (the sentinel
    // is also excluded from the completed-block set so the step re-executes).
    #[tokio::test]
    async fn build_outputs_shape_filters_in_progress_sentinel() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();
        save_output_with_ref(
            &s,
            inst.id,
            "b1",
            json!({"_sentinel": true, "_handler": "h"}),
            Some(IN_PROGRESS_SENTINEL),
        )
        .await;
        save_output(&s, inst.id, "b2", json!({"ok": true})).await;
        let shape = build_outputs_shape(&s, inst.id).await.unwrap();
        assert_eq!(shape, json!({"b2": {"ok": true}}));
    }

    // PR3c: a `__retry__` attempt marker is internal bookkeeping too — a block
    // whose only row is a retry marker has no output yet.
    #[tokio::test]
    async fn build_outputs_shape_filters_retry_marker() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();
        save_output_with_ref(
            &s,
            inst.id,
            "b1",
            json!({"_retry_marker": true, "error": "boom"}),
            Some("__retry__"),
        )
        .await;
        let shape = build_outputs_shape(&s, inst.id).await.unwrap();
        assert_eq!(shape, json!({}));
    }

    // PR3d: a stale sentinel BEHIND a real output (left over when a crashed
    // attempt is re-executed and succeeds) must not shadow the real result.
    #[tokio::test]
    async fn build_outputs_shape_real_output_wins_over_stale_sentinel() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();
        save_output_with_ref(
            &s,
            inst.id,
            "b1",
            json!({"_sentinel": true, "_handler": "h"}),
            Some(IN_PROGRESS_SENTINEL),
        )
        .await;
        save_output(&s, inst.id, "b1", json!({"result": 42})).await;
        let shape = build_outputs_shape(&s, inst.id).await.unwrap();
        assert_eq!(shape, json!({"b1": {"result": 42}}));
    }

    #[tokio::test]
    async fn outputs_snapshot_is_single_fetch_and_refreshes_only_when_recreated() {
        let storage = mk_storage().await;
        let instance = mk_instance(InstanceId::new());
        storage.create_instance(&instance).await.unwrap();
        save_output(&storage, instance.id, "first", json!(1)).await;
        let snapshot = OutputsSnapshot::new();

        let first_read = snapshot.get(&storage, instance.id).await.unwrap();
        assert_eq!(first_read["first"], 1);
        save_output(&storage, instance.id, "second", json!(2)).await;

        let cached_read = snapshot.get(&storage, instance.id).await.unwrap();
        assert!(std::ptr::eq(first_read, cached_read));
        assert!(cached_read.get("second").is_none());

        let refreshed = OutputsSnapshot::new();
        let refreshed_read = refreshed.get(&storage, instance.id).await.unwrap();
        assert_eq!(refreshed_read["second"], 2);
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

    #[tokio::test]
    async fn resolve_state_template_propagates_storage_failure() {
        let storage = mk_storage().await;
        let instance = mk_instance(InstanceId::new());
        storage.create_instance(&instance).await.unwrap();
        let outputs = OutputsSnapshot::new();
        // Prime the independent output snapshot so the closed pool below is
        // observed specifically by the state-KV lookup.
        outputs.get(&storage, instance.id).await.unwrap();
        storage.pool().close().await;

        let result = resolve_templates_in_params(
            &storage,
            &instance,
            &ExecutionContext::default(),
            &json!({"value": "{{ state.key }}"}),
            &outputs,
        )
        .await;

        assert!(matches!(result, Err(EngineError::Storage(_))));
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

    // ─── apply_self_modify ───────────────────────────────────────────────
    //
    // The self-modify handler only emits a marker output; this helper is the
    // code that actually mutates the instance's `injected_blocks`. It was
    // previously exercised only indirectly through `step_block`/`step_exec`
    // with mock handlers. The tests below pin its contract directly so the
    // injection invariants — especially "positionless append must NOT clobber
    // prior injections" — can't silently regress.

    /// Minimal block-shaped JSON. `apply_self_modify` does not validate the
    /// `BlockDefinition` shape (that's the handler's job) — it stores whatever
    /// array it's given — so a lightweight object is sufficient and keeps the
    /// test focused on the merge/splice logic.
    fn blk(id: &str) -> serde_json::Value {
        json!({ "type": "step", "id": id, "handler": "noop", "params": {} })
    }

    /// PR-SM1: no `_self_modify` marker → `NotApplicable`, and storage is
    /// untouched (the common success path for ordinary steps).
    #[tokio::test]
    async fn apply_self_modify_no_marker_is_not_applicable() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        let res = apply_self_modify(&s, inst.id, &json!({"result": "ok"})).await;
        assert_eq!(res, SelfModifyResult::NotApplicable);
        assert_eq!(s.get_injected_blocks(inst.id).await.unwrap(), None);
    }

    /// PR-SM2: marker present but explicitly `false` → `NotApplicable`.
    #[tokio::test]
    async fn apply_self_modify_marker_false_is_not_applicable() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        let res = apply_self_modify(
            &s,
            inst.id,
            &json!({"_self_modify": false, "blocks": [blk("x")]}),
        )
        .await;
        assert_eq!(res, SelfModifyResult::NotApplicable);
        assert_eq!(s.get_injected_blocks(inst.id).await.unwrap(), None);
    }

    /// PR-SM3: marker present but `blocks` missing → Failed, nothing stored.
    #[tokio::test]
    async fn apply_self_modify_missing_blocks_fails() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        let res = apply_self_modify(&s, inst.id, &json!({"_self_modify": true})).await;
        assert_eq!(res, SelfModifyResult::Failed);
        assert_eq!(s.get_injected_blocks(inst.id).await.unwrap(), None);
    }

    /// PR-SM4: `blocks` present but not an array → Failed.
    #[tokio::test]
    async fn apply_self_modify_blocks_not_array_fails() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        let res = apply_self_modify(
            &s,
            inst.id,
            &json!({"_self_modify": true, "blocks": "nope"}),
        )
        .await;
        assert_eq!(res, SelfModifyResult::Failed);
        assert_eq!(s.get_injected_blocks(inst.id).await.unwrap(), None);
    }

    /// PR-SM5: positionless append into an empty list stores exactly the new
    /// blocks, in order.
    #[tokio::test]
    async fn apply_self_modify_append_to_empty() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        let res = apply_self_modify(
            &s,
            inst.id,
            &json!({"_self_modify": true, "blocks": [blk("a"), blk("b")]}),
        )
        .await;
        assert_eq!(res, SelfModifyResult::Applied);

        let stored = s.get_injected_blocks(inst.id).await.unwrap().unwrap();
        let ids: Vec<&str> = stored
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["a", "b"]);
    }

    /// PR-SM6 (regression): a positionless append MUST preserve blocks injected
    /// by an earlier self-modify call — it appends, it does not clobber. This
    /// is the exact invariant called out in the function's comment; the agent
    /// self-extension pattern depends on it.
    #[tokio::test]
    async fn apply_self_modify_append_preserves_prior_injections() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        // First injection.
        apply_self_modify(
            &s,
            inst.id,
            &json!({"_self_modify": true, "blocks": [blk("first")]}),
        )
        .await;
        // Second injection, also positionless — must append after "first".
        let res = apply_self_modify(
            &s,
            inst.id,
            &json!({"_self_modify": true, "blocks": [blk("second")]}),
        )
        .await;
        assert_eq!(res, SelfModifyResult::Applied);

        let stored = s.get_injected_blocks(inst.id).await.unwrap().unwrap();
        let ids: Vec<&str> = stored
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["id"].as_str().unwrap())
            .collect();
        assert_eq!(
            ids,
            vec!["first", "second"],
            "second append must not clobber the first injection"
        );
    }

    /// PR-SM7: position 0 splices the new blocks at the FRONT of the existing
    /// injected list.
    #[tokio::test]
    async fn apply_self_modify_position_zero_inserts_at_front() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        apply_self_modify(
            &s,
            inst.id,
            &json!({"_self_modify": true, "blocks": [blk("orig")]}),
        )
        .await;
        let res = apply_self_modify(
            &s,
            inst.id,
            &json!({"_self_modify": true, "blocks": [blk("front")], "position": 0}),
        )
        .await;
        assert_eq!(res, SelfModifyResult::Applied);

        let stored = s.get_injected_blocks(inst.id).await.unwrap().unwrap();
        let ids: Vec<&str> = stored
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["front", "orig"]);
    }

    /// PR-SM8: a positioned splice inserts into the MIDDLE of an existing list
    /// without dropping any element.
    #[tokio::test]
    async fn apply_self_modify_position_splices_into_middle() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        apply_self_modify(
            &s,
            inst.id,
            &json!({"_self_modify": true, "blocks": [blk("a"), blk("c")]}),
        )
        .await;
        let res = apply_self_modify(
            &s,
            inst.id,
            &json!({"_self_modify": true, "blocks": [blk("b")], "position": 1}),
        )
        .await;
        assert_eq!(res, SelfModifyResult::Applied);

        let stored = s.get_injected_blocks(inst.id).await.unwrap().unwrap();
        let ids: Vec<&str> = stored
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    /// PR-SM9: a position past the end of the list is clamped to the end
    /// (append semantics) rather than panicking on an out-of-range splice.
    #[tokio::test]
    async fn apply_self_modify_position_beyond_len_clamps_to_end() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        apply_self_modify(
            &s,
            inst.id,
            &json!({"_self_modify": true, "blocks": [blk("a")]}),
        )
        .await;
        let res = apply_self_modify(
            &s,
            inst.id,
            &json!({"_self_modify": true, "blocks": [blk("z")], "position": 999}),
        )
        .await;
        assert_eq!(res, SelfModifyResult::Applied);

        let stored = s.get_injected_blocks(inst.id).await.unwrap().unwrap();
        let ids: Vec<&str> = stored
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["a", "z"]);
    }

    /// PR-SM10: an empty `blocks` array is a successful no-op injection —
    /// existing injected blocks are preserved unchanged.
    #[tokio::test]
    async fn apply_self_modify_empty_blocks_preserves_existing() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        apply_self_modify(
            &s,
            inst.id,
            &json!({"_self_modify": true, "blocks": [blk("keep")]}),
        )
        .await;
        let res =
            apply_self_modify(&s, inst.id, &json!({"_self_modify": true, "blocks": []})).await;
        assert_eq!(res, SelfModifyResult::Applied);

        let stored = s.get_injected_blocks(inst.id).await.unwrap().unwrap();
        let ids: Vec<&str> = stored
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["keep"]);
    }
}
