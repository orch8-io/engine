//! Shared template + credential parameter-resolution helpers.
//!
//! Both step-dispatch paths (the tree evaluator in
//! `handlers::step_block::execute_step_node` and the fast-path scheduler loop
//! in `scheduler::execute_step_block`) must resolve `{{…}}` templates and
//! `credentials://…` refs before handing params to any handler or external
//! worker. This module centralises the pipeline so both sites share one
//! implementation and one ordering guarantee (template-first,
//! credentials-second).

use orch8_storage::StorageBackend;
use orch8_types::context::ExecutionContext;
use orch8_types::ids::InstanceId;
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
    params: serde_json::Value,
) -> Result<serde_json::Value, EngineError> {
    let outputs = build_outputs_shape(storage, instance.id).await?;
    crate::template::resolve(&params, context, &outputs)
}
