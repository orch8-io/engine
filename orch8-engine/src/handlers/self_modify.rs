//! Self-modify handler: allows a running step to inject blocks into its own
//! instance's sequence at runtime. Enables agent self-modification patterns
//! where an AI agent decides its next steps dynamically.
//!
//! Params:
//! - `blocks` (array, required): Array of `BlockDefinition` objects to inject.
//! - `position` (u64, optional): 0-indexed position to insert at. If omitted, appends.
//!
//! Returns: `{ "injected_count": N, "position": P }`

use serde_json::{json, Value};

use orch8_types::error::StepError;

use super::StepContext;

/// Handle self-modification: inject blocks into the running instance.
pub async fn handle_self_modify(ctx: StepContext) -> Result<Value, StepError> {
    let blocks = ctx
        .params
        .get("blocks")
        .filter(|v| v.is_array())
        .cloned()
        .ok_or_else(|| StepError::Permanent {
            message: "missing required param: blocks (must be a JSON array)".into(),
            details: None,
        })?;

    let count = blocks.as_array().map_or(0, Vec::len);
    #[allow(clippy::cast_possible_truncation)]
    let position = ctx
        .params
        .get("position")
        .and_then(Value::as_u64)
        .map(|p| p.min(usize::MAX as u64) as usize);

    // Validate blocks parse as BlockDefinition.
    if let Err(e) =
        serde_json::from_value::<Vec<orch8_types::sequence::BlockDefinition>>(blocks.clone())
    {
        return Err(StepError::Permanent {
            message: format!("invalid blocks: {e}"),
            details: Some(blocks),
        });
    }

    // Store the injection request in the step output. The evaluator picks up
    // injected blocks from storage on the next tick. We use a special output
    // key `_self_modify` so the caller knows this step performed injection.
    //
    // The actual injection is done via the storage layer — we write to the
    // instance's injected_blocks field. This requires access to storage,
    // which step handlers don't have directly. Instead, we return a special
    // output that the step_block executor recognizes and acts on.
    Ok(json!({
        "_self_modify": true,
        "blocks": blocks,
        "position": position,
        "injected_count": count,
    }))
}
