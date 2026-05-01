use std::collections::HashSet;

use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef, BUILTIN_HANDLER_NAMES};

#[derive(Debug, Clone)]
pub struct LintWarning {
    pub block_id: String,
    pub message: String,
}

impl std::fmt::Display for LintWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.block_id, self.message)
    }
}

pub fn lint_sequence(seq: &SequenceDefinition) -> Vec<LintWarning> {
    let mut warnings = Vec::new();

    // Collect all block IDs for cross-reference validation
    let mut all_ids = HashSet::new();
    collect_all_block_ids(&seq.blocks, &mut all_ids);

    for block in &seq.blocks {
        lint_block(block, &all_ids, &mut warnings);
    }

    // Cross-cutting lints that need the full block set
    lint_output_references(seq, &all_ids, &mut warnings);
    lint_for_each_safety(seq, &mut warnings);
    lint_step_after_fail(seq, &mut warnings);

    warnings
}

fn collect_all_block_ids(blocks: &[BlockDefinition], ids: &mut HashSet<String>) {
    for block in blocks {
        match block {
            BlockDefinition::Step(s) => {
                ids.insert(s.id.0.clone());
            }
            BlockDefinition::Parallel(p) => {
                ids.insert(p.id.0.clone());
                for branch in &p.branches {
                    collect_all_block_ids(branch, ids);
                }
            }
            BlockDefinition::Race(r) => {
                ids.insert(r.id.0.clone());
                for branch in &r.branches {
                    collect_all_block_ids(branch, ids);
                }
            }
            BlockDefinition::Loop(l) => {
                ids.insert(l.id.0.clone());
                collect_all_block_ids(&l.body, ids);
            }
            BlockDefinition::ForEach(fe) => {
                ids.insert(fe.id.0.clone());
                collect_all_block_ids(&fe.body, ids);
            }
            BlockDefinition::Router(r) => {
                ids.insert(r.id.0.clone());
                for route in &r.routes {
                    collect_all_block_ids(&route.blocks, ids);
                }
                if let Some(d) = &r.default {
                    collect_all_block_ids(d, ids);
                }
            }
            BlockDefinition::TryCatch(tc) => {
                ids.insert(tc.id.0.clone());
                collect_all_block_ids(&tc.try_block, ids);
                collect_all_block_ids(&tc.catch_block, ids);
                if let Some(f) = &tc.finally_block {
                    collect_all_block_ids(f, ids);
                }
            }
            BlockDefinition::SubSequence(s) => {
                ids.insert(s.id.0.clone());
            }
            BlockDefinition::ABSplit(ab) => {
                ids.insert(ab.id.0.clone());
                for v in &ab.variants {
                    collect_all_block_ids(&v.blocks, ids);
                }
            }
            BlockDefinition::CancellationScope(cs) => {
                ids.insert(cs.id.0.clone());
                collect_all_block_ids(&cs.blocks, ids);
            }
        }
    }
}

fn lint_blocks(
    blocks: &[BlockDefinition],
    all_ids: &HashSet<String>,
    warnings: &mut Vec<LintWarning>,
) {
    for b in blocks {
        lint_block(b, all_ids, warnings);
    }
}

fn lint_block(block: &BlockDefinition, all_ids: &HashSet<String>, warnings: &mut Vec<LintWarning>) {
    match block {
        BlockDefinition::Step(s) => lint_step(s, warnings),
        BlockDefinition::Parallel(p) => lint_parallel(p, all_ids, warnings),
        BlockDefinition::Race(r) => lint_race(r, all_ids, warnings),
        BlockDefinition::Loop(l) => lint_loop(l, all_ids, warnings),
        BlockDefinition::ForEach(fe) => lint_blocks(&fe.body, all_ids, warnings),
        BlockDefinition::Router(r) => lint_router(r, all_ids, warnings),
        BlockDefinition::TryCatch(tc) => lint_try_catch(tc, all_ids, warnings),
        BlockDefinition::SubSequence(_) => {}
        BlockDefinition::ABSplit(ab) => lint_ab_split(ab, all_ids, warnings),
        BlockDefinition::CancellationScope(cs) => lint_blocks(&cs.blocks, all_ids, warnings),
    }
}

fn lint_parallel(
    p: &orch8_types::sequence::ParallelDef,
    all_ids: &HashSet<String>,
    warnings: &mut Vec<LintWarning>,
) {
    for branch in &p.branches {
        if branch.is_empty() {
            warnings.push(LintWarning {
                block_id: p.id.0.clone(),
                message: "parallel branch is empty (will be a no-op)".into(),
            });
        }
        lint_blocks(branch, all_ids, warnings);
    }
}

fn lint_race(
    r: &orch8_types::sequence::RaceDef,
    all_ids: &HashSet<String>,
    warnings: &mut Vec<LintWarning>,
) {
    if r.branches.len() < 2 {
        warnings.push(LintWarning {
            block_id: r.id.0.clone(),
            message: "race with fewer than 2 branches has no racing effect".into(),
        });
    }
    for branch in &r.branches {
        lint_blocks(branch, all_ids, warnings);
    }
}

fn lint_loop(
    l: &orch8_types::sequence::LoopDef,
    all_ids: &HashSet<String>,
    warnings: &mut Vec<LintWarning>,
) {
    lint_expression(&l.id.0, "condition", &l.condition, warnings);
    if let Some(ref break_on) = l.break_on {
        lint_expression(&l.id.0, "break_on", break_on, warnings);
    }
    lint_blocks(&l.body, all_ids, warnings);
}

fn lint_router(
    r: &orch8_types::sequence::RouterDef,
    all_ids: &HashSet<String>,
    warnings: &mut Vec<LintWarning>,
) {
    for route in &r.routes {
        lint_expression(&r.id.0, "route.condition", &route.condition, warnings);
        if route.blocks.is_empty() {
            warnings.push(LintWarning {
                block_id: r.id.0.clone(),
                message: "router route has empty blocks (will be a no-op)".into(),
            });
        }
        lint_blocks(&route.blocks, all_ids, warnings);
    }
    if let Some(default) = &r.default {
        lint_blocks(default, all_ids, warnings);
    }
}

fn lint_try_catch(
    tc: &orch8_types::sequence::TryCatchDef,
    all_ids: &HashSet<String>,
    warnings: &mut Vec<LintWarning>,
) {
    if tc.catch_block.is_empty() {
        warnings.push(LintWarning {
            block_id: tc.id.0.clone(),
            message: "try_catch has empty catch_block (errors will be silently swallowed)".into(),
        });
    }
    lint_blocks(&tc.try_block, all_ids, warnings);
    lint_blocks(&tc.catch_block, all_ids, warnings);
    if let Some(finally) = &tc.finally_block {
        lint_blocks(finally, all_ids, warnings);
    }
}

fn lint_ab_split(
    ab: &orch8_types::sequence::ABSplitDef,
    all_ids: &HashSet<String>,
    warnings: &mut Vec<LintWarning>,
) {
    let all_zero = ab.variants.iter().all(|v| v.weight == 0);
    if !all_zero {
        for v in &ab.variants {
            if v.weight == 0 {
                warnings.push(LintWarning {
                    block_id: ab.id.0.clone(),
                    message: format!(
                        "ab_split variant `{}` has weight 0 (will never be selected)",
                        v.name
                    ),
                });
            }
        }
    }
    for v in &ab.variants {
        lint_blocks(&v.blocks, all_ids, warnings);
    }
}

fn lint_step(s: &StepDef, warnings: &mut Vec<LintWarning>) {
    if BUILTIN_HANDLER_NAMES.contains(&s.handler.as_str()) {
        lint_handler_params(&s.id.0, &s.handler, &s.params, warnings);
    }

    if s.deadline.is_some() && s.timeout.is_some() {
        if let (Some(deadline), Some(timeout)) = (s.deadline, s.timeout) {
            if timeout > deadline {
                warnings.push(LintWarning {
                    block_id: s.id.0.clone(),
                    message: format!(
                        "step timeout ({:.1}s) exceeds deadline ({:.1}s) — step will be \
                         deadline-breached before timeout fires",
                        timeout.as_secs_f64(),
                        deadline.as_secs_f64()
                    ),
                });
            }
        }
    }

    if let Some(ref fallback) = s.fallback_handler {
        if fallback == &s.handler {
            warnings.push(LintWarning {
                block_id: s.id.0.clone(),
                message: "fallback_handler is the same as primary handler".into(),
            });
        }
        if !fallback.is_empty()
            && !BUILTIN_HANDLER_NAMES.contains(&fallback.as_str())
            && BUILTIN_HANDLER_NAMES.contains(&s.handler.as_str())
        {
            let suggestion = orch8_types::suggest::did_you_mean(fallback, BUILTIN_HANDLER_NAMES);
            let hint = match suggestion {
                Some(s) => format!(" (did you mean \"{s}\"?)"),
                None => String::new(),
            };
            warnings.push(LintWarning {
                block_id: s.id.0.clone(),
                message: format!("unknown fallback_handler \"{fallback}\"{hint}"),
            });
        }
    }

    if let Some(ref esc) = s.on_deadline_breach {
        if s.deadline.is_none() {
            warnings.push(LintWarning {
                block_id: s.id.0.clone(),
                message: "on_deadline_breach is set but no deadline is configured".into(),
            });
        }
        if esc.handler.is_empty() {
            warnings.push(LintWarning {
                block_id: s.id.0.clone(),
                message: "on_deadline_breach handler name is empty".into(),
            });
        }
    }
}

fn lint_handler_params(
    block_id: &str,
    handler: &str,
    params: &serde_json::Value,
    warnings: &mut Vec<LintWarning>,
) {
    match handler {
        "http_request" | "tool_call" => {
            check_required_str(block_id, handler, params, "url", warnings);
        }
        "llm_call" if params.get("messages").is_none() && params.get("system").is_none() => {
            warnings.push(LintWarning {
                block_id: block_id.into(),
                message: "llm_call: requires at least `messages` or `system` param".into(),
            });
        }
        "send_signal" => {
            check_required_str(block_id, handler, params, "instance_id", warnings);
            check_required(block_id, handler, params, "signal_type", warnings);
        }
        "emit_event" => {
            check_required_str(block_id, handler, params, "trigger_slug", warnings);
        }
        "query_instance" => {
            check_required_str(block_id, handler, params, "instance_id", warnings);
        }
        "set_state" => {
            check_required_str(block_id, handler, params, "key", warnings);
            check_required(block_id, handler, params, "value", warnings);
        }
        "get_state" | "delete_state" => {
            check_required_str(block_id, handler, params, "key", warnings);
        }
        "assert" => {
            check_required_str(block_id, handler, params, "condition", warnings);
        }
        "merge_state" => lint_merge_state_params(block_id, params, warnings),
        _ => {}
    }
}

fn check_required(
    block_id: &str,
    handler: &str,
    params: &serde_json::Value,
    key: &str,
    warnings: &mut Vec<LintWarning>,
) {
    if is_template_value(params, key) {
        return;
    }
    if params.get(key).is_none() {
        warnings.push(LintWarning {
            block_id: block_id.into(),
            message: format!("{handler}: missing required param `{key}`"),
        });
    }
}

fn check_required_str(
    block_id: &str,
    handler: &str,
    params: &serde_json::Value,
    key: &str,
    warnings: &mut Vec<LintWarning>,
) {
    if is_template_value(params, key) {
        return;
    }
    match params.get(key) {
        None => {
            warnings.push(LintWarning {
                block_id: block_id.into(),
                message: format!("{handler}: missing required param `{key}`"),
            });
        }
        Some(v) => {
            if let Some(s) = v.as_str() {
                if s.trim().is_empty() {
                    warnings.push(LintWarning {
                        block_id: block_id.into(),
                        message: format!("{handler}: param `{key}` is empty"),
                    });
                }
            }
        }
    }
}

fn lint_merge_state_params(
    block_id: &str,
    params: &serde_json::Value,
    warnings: &mut Vec<LintWarning>,
) {
    if let Some(v) = params.get("values") {
        if !v.is_object() {
            warnings.push(LintWarning {
                block_id: block_id.into(),
                message: "merge_state: `values` must be an object".into(),
            });
        }
    } else {
        warnings.push(LintWarning {
            block_id: block_id.into(),
            message: "merge_state: missing required param `values`".into(),
        });
    }
}

fn is_template_value(params: &serde_json::Value, key: &str) -> bool {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .is_some_and(|s| s.contains("{{"))
}

fn lint_expression(block_id: &str, field: &str, expr: &str, warnings: &mut Vec<LintWarning>) {
    let dummy_context = orch8_types::context::ExecutionContext {
        data: serde_json::json!({}),
        config: serde_json::json!({}),
        audit: vec![],
        runtime: orch8_types::context::RuntimeContext::default(),
    };
    let result = crate::expression::try_evaluate(expr, &dummy_context, &serde_json::json!({}));
    if let Err(e) = result {
        warnings.push(LintWarning {
            block_id: block_id.into(),
            message: format!("{field}: expression parse error — {}", e.message),
        });
    }
}

// ─── Cross-cutting lints ────────────────────────────────────────────────────

/// Lint 1: Validate that `outputs.<id>` and `steps.<id>` references in
/// templates and expressions point to block IDs that actually exist.
fn lint_output_references(
    seq: &SequenceDefinition,
    all_ids: &HashSet<String>,
    warnings: &mut Vec<LintWarning>,
) {
    for block in &seq.blocks {
        lint_output_refs_in_block(block, all_ids, warnings);
    }
}

fn lint_output_refs_in_block(
    block: &BlockDefinition,
    all_ids: &HashSet<String>,
    warnings: &mut Vec<LintWarning>,
) {
    match block {
        BlockDefinition::Step(s) => {
            check_refs_in_json(&s.id.0, "params", &s.params, all_ids, warnings);
        }
        BlockDefinition::Parallel(p) => {
            for branch in &p.branches {
                for b in branch {
                    lint_output_refs_in_block(b, all_ids, warnings);
                }
            }
        }
        BlockDefinition::Race(r) => {
            for branch in &r.branches {
                for b in branch {
                    lint_output_refs_in_block(b, all_ids, warnings);
                }
            }
        }
        BlockDefinition::Loop(l) => {
            check_refs_in_str(&l.id.0, "condition", &l.condition, all_ids, warnings);
            if let Some(ref bo) = l.break_on {
                check_refs_in_str(&l.id.0, "break_on", bo, all_ids, warnings);
            }
            for b in &l.body {
                lint_output_refs_in_block(b, all_ids, warnings);
            }
        }
        BlockDefinition::ForEach(fe) => {
            check_refs_in_str(&fe.id.0, "collection", &fe.collection, all_ids, warnings);
            for b in &fe.body {
                lint_output_refs_in_block(b, all_ids, warnings);
            }
        }
        BlockDefinition::Router(r) => {
            for route in &r.routes {
                check_refs_in_str(
                    &r.id.0,
                    "route.condition",
                    &route.condition,
                    all_ids,
                    warnings,
                );
                for b in &route.blocks {
                    lint_output_refs_in_block(b, all_ids, warnings);
                }
            }
            if let Some(d) = &r.default {
                for b in d {
                    lint_output_refs_in_block(b, all_ids, warnings);
                }
            }
        }
        BlockDefinition::TryCatch(tc) => {
            for b in &tc.try_block {
                lint_output_refs_in_block(b, all_ids, warnings);
            }
            for b in &tc.catch_block {
                lint_output_refs_in_block(b, all_ids, warnings);
            }
            if let Some(f) = &tc.finally_block {
                for b in f {
                    lint_output_refs_in_block(b, all_ids, warnings);
                }
            }
        }
        BlockDefinition::SubSequence(s) => {
            check_refs_in_json(&s.id.0, "input", &s.input, all_ids, warnings);
        }
        BlockDefinition::ABSplit(ab) => {
            for v in &ab.variants {
                for b in &v.blocks {
                    lint_output_refs_in_block(b, all_ids, warnings);
                }
            }
        }
        BlockDefinition::CancellationScope(cs) => {
            for b in &cs.blocks {
                lint_output_refs_in_block(b, all_ids, warnings);
            }
        }
    }
}

fn check_refs_in_json(
    block_id: &str,
    field: &str,
    value: &serde_json::Value,
    all_ids: &HashSet<String>,
    warnings: &mut Vec<LintWarning>,
) {
    match value {
        serde_json::Value::String(s) => check_refs_in_str(block_id, field, s, all_ids, warnings),
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let child = format!("{field}.{k}");
                check_refs_in_json(block_id, &child, v, all_ids, warnings);
            }
        }
        serde_json::Value::Array(arr) => {
            for (i, v) in arr.iter().enumerate() {
                let child = format!("{field}[{i}]");
                check_refs_in_json(block_id, &child, v, all_ids, warnings);
            }
        }
        _ => {}
    }
}

fn check_refs_in_str(
    block_id: &str,
    field: &str,
    s: &str,
    all_ids: &HashSet<String>,
    warnings: &mut Vec<LintWarning>,
) {
    // Match `steps.BLOCK_ID` and `outputs.BLOCK_ID` anywhere in the string
    for prefix in ["steps.", "outputs."] {
        let mut search = s;
        while let Some(pos) = search.find(prefix) {
            let after = &search[pos + prefix.len()..];
            let ref_id = extract_identifier(after);
            if !ref_id.is_empty() && !all_ids.contains(ref_id) {
                warnings.push(LintWarning {
                    block_id: block_id.into(),
                    message: format!(
                        "{field}: references `{prefix}{ref_id}` but no block with id `{ref_id}` exists"
                    ),
                });
            }
            search = &search[pos + prefix.len() + ref_id.len().max(1)..];
        }
    }
}

fn extract_identifier(s: &str) -> &str {
    let end = s
        .find(|c: char| !c.is_alphanumeric() && c != '_' && c != '-')
        .unwrap_or(s.len());
    &s[..end]
}

/// Lint 2: for_each safety — warn when max_iterations uses the default 1000
/// without explicit override (easy to miss for large collections).
fn lint_for_each_safety(seq: &SequenceDefinition, warnings: &mut Vec<LintWarning>) {
    for block in &seq.blocks {
        lint_for_each_in_block(block, warnings);
    }
}

fn lint_for_each_in_block(block: &BlockDefinition, warnings: &mut Vec<LintWarning>) {
    match block {
        BlockDefinition::ForEach(fe) => {
            if fe.max_iterations == 1000 {
                warnings.push(LintWarning {
                    block_id: fe.id.0.clone(),
                    message: "for_each uses default max_iterations (1000) — set explicitly to confirm intent".into(),
                });
            }
            for b in &fe.body {
                lint_for_each_in_block(b, warnings);
            }
        }
        BlockDefinition::Parallel(p) => {
            for branch in &p.branches {
                for b in branch {
                    lint_for_each_in_block(b, warnings);
                }
            }
        }
        BlockDefinition::Race(r) => {
            for branch in &r.branches {
                for b in branch {
                    lint_for_each_in_block(b, warnings);
                }
            }
        }
        BlockDefinition::Loop(l) => {
            for b in &l.body {
                lint_for_each_in_block(b, warnings);
            }
        }
        BlockDefinition::Router(r) => {
            for route in &r.routes {
                for b in &route.blocks {
                    lint_for_each_in_block(b, warnings);
                }
            }
            if let Some(d) = &r.default {
                for b in d {
                    lint_for_each_in_block(b, warnings);
                }
            }
        }
        BlockDefinition::TryCatch(tc) => {
            for b in &tc.try_block {
                lint_for_each_in_block(b, warnings);
            }
            for b in &tc.catch_block {
                lint_for_each_in_block(b, warnings);
            }
            if let Some(f) = &tc.finally_block {
                for b in f {
                    lint_for_each_in_block(b, warnings);
                }
            }
        }
        BlockDefinition::ABSplit(ab) => {
            for v in &ab.variants {
                for b in &v.blocks {
                    lint_for_each_in_block(b, warnings);
                }
            }
        }
        BlockDefinition::CancellationScope(cs) => {
            for b in &cs.blocks {
                lint_for_each_in_block(b, warnings);
            }
        }
        _ => {}
    }
}

/// Lint 3: Detect unreachable blocks — steps after a `fail` handler in the
/// same sequential block list can never execute.
fn lint_step_after_fail(seq: &SequenceDefinition, warnings: &mut Vec<LintWarning>) {
    lint_unreachable_in_list(&seq.blocks, warnings);
    for block in &seq.blocks {
        lint_unreachable_recursive(block, warnings);
    }
}

fn lint_unreachable_in_list(blocks: &[BlockDefinition], warnings: &mut Vec<LintWarning>) {
    let mut saw_fail = false;
    let mut fail_id = String::new();
    for block in blocks {
        if saw_fail {
            let unreachable_id = block_id_of(block);
            warnings.push(LintWarning {
                block_id: unreachable_id,
                message: format!("unreachable — previous block `{fail_id}` uses `fail` handler which always terminates"),
            });
        }
        if let BlockDefinition::Step(s) = block {
            if s.handler == "fail" {
                saw_fail = true;
                fail_id = s.id.0.clone();
            }
        }
    }
}

fn lint_unreachable_recursive(block: &BlockDefinition, warnings: &mut Vec<LintWarning>) {
    match block {
        BlockDefinition::Parallel(p) => {
            for branch in &p.branches {
                lint_unreachable_in_list(branch, warnings);
                for b in branch {
                    lint_unreachable_recursive(b, warnings);
                }
            }
        }
        BlockDefinition::Race(r) => {
            for branch in &r.branches {
                lint_unreachable_in_list(branch, warnings);
                for b in branch {
                    lint_unreachable_recursive(b, warnings);
                }
            }
        }
        BlockDefinition::Loop(l) => {
            lint_unreachable_in_list(&l.body, warnings);
            for b in &l.body {
                lint_unreachable_recursive(b, warnings);
            }
        }
        BlockDefinition::ForEach(fe) => {
            lint_unreachable_in_list(&fe.body, warnings);
            for b in &fe.body {
                lint_unreachable_recursive(b, warnings);
            }
        }
        BlockDefinition::Router(r) => {
            for route in &r.routes {
                lint_unreachable_in_list(&route.blocks, warnings);
                for b in &route.blocks {
                    lint_unreachable_recursive(b, warnings);
                }
            }
            if let Some(d) = &r.default {
                lint_unreachable_in_list(d, warnings);
                for b in d {
                    lint_unreachable_recursive(b, warnings);
                }
            }
        }
        BlockDefinition::TryCatch(tc) => {
            lint_unreachable_in_list(&tc.try_block, warnings);
            for b in &tc.try_block {
                lint_unreachable_recursive(b, warnings);
            }
            lint_unreachable_in_list(&tc.catch_block, warnings);
            for b in &tc.catch_block {
                lint_unreachable_recursive(b, warnings);
            }
            if let Some(f) = &tc.finally_block {
                lint_unreachable_in_list(f, warnings);
                for b in f {
                    lint_unreachable_recursive(b, warnings);
                }
            }
        }
        BlockDefinition::ABSplit(ab) => {
            for v in &ab.variants {
                lint_unreachable_in_list(&v.blocks, warnings);
                for b in &v.blocks {
                    lint_unreachable_recursive(b, warnings);
                }
            }
        }
        BlockDefinition::CancellationScope(cs) => {
            lint_unreachable_in_list(&cs.blocks, warnings);
            for b in &cs.blocks {
                lint_unreachable_recursive(b, warnings);
            }
        }
        _ => {}
    }
}

fn block_id_of(block: &BlockDefinition) -> String {
    match block {
        BlockDefinition::Step(s) => s.id.0.clone(),
        BlockDefinition::Parallel(p) => p.id.0.clone(),
        BlockDefinition::Race(r) => r.id.0.clone(),
        BlockDefinition::Loop(l) => l.id.0.clone(),
        BlockDefinition::ForEach(fe) => fe.id.0.clone(),
        BlockDefinition::Router(r) => r.id.0.clone(),
        BlockDefinition::TryCatch(tc) => tc.id.0.clone(),
        BlockDefinition::SubSequence(s) => s.id.0.clone(),
        BlockDefinition::ABSplit(ab) => ab.id.0.clone(),
        BlockDefinition::CancellationScope(cs) => cs.id.0.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::ids::*;
    use orch8_types::sequence::*;
    use serde_json::json;
    use std::time::Duration;

    fn sample_seq(blocks: Vec<BlockDefinition>) -> SequenceDefinition {
        SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId("t".into()),
            namespace: Namespace("default".into()),
            name: "sample".into(),
            version: 1,
            deprecated: false,
            blocks,
            interceptors: None,
            created_at: chrono::Utc::now(),
        }
    }

    fn make_step(id: &str, handler: &str, params: serde_json::Value) -> BlockDefinition {
        BlockDefinition::Step(Box::new(StepDef {
            id: BlockId(id.into()),
            handler: handler.into(),
            params,
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
            cache_key: None,
        }))
    }

    // ─── handler param lint ───

    #[test]
    fn http_request_missing_url() {
        let seq = sample_seq(vec![make_step("s1", "http_request", json!({}))]);
        let w = lint_sequence(&seq);
        assert_eq!(w.len(), 1);
        assert!(w[0].message.contains("missing required param `url`"));
    }

    #[test]
    fn http_request_empty_url() {
        let seq = sample_seq(vec![make_step("s1", "http_request", json!({"url": ""}))]);
        let w = lint_sequence(&seq);
        assert_eq!(w.len(), 1);
        assert!(w[0].message.contains("param `url` is empty"));
    }

    #[test]
    fn http_request_template_url_no_warning() {
        let seq = sample_seq(vec![make_step(
            "s1",
            "http_request",
            json!({"url": "{{ context.data.url }}"}),
        )]);
        let w = lint_sequence(&seq);
        assert!(w.is_empty());
    }

    #[test]
    fn http_request_valid() {
        let seq = sample_seq(vec![make_step(
            "s1",
            "http_request",
            json!({"url": "https://example.com"}),
        )]);
        let w = lint_sequence(&seq);
        assert!(w.is_empty());
    }

    #[test]
    fn llm_call_no_messages_or_system() {
        let seq = sample_seq(vec![make_step(
            "s1",
            "llm_call",
            json!({"model": "gpt-4o"}),
        )]);
        let w = lint_sequence(&seq);
        assert_eq!(w.len(), 1);
        assert!(w[0].message.contains("messages"));
    }

    #[test]
    fn llm_call_with_system_only() {
        let seq = sample_seq(vec![make_step(
            "s1",
            "llm_call",
            json!({"system": "You are a helper"}),
        )]);
        let w = lint_sequence(&seq);
        assert!(w.is_empty());
    }

    #[test]
    fn llm_call_with_messages_only() {
        let seq = sample_seq(vec![make_step(
            "s1",
            "llm_call",
            json!({"messages": [{"role": "user", "content": "hi"}]}),
        )]);
        let w = lint_sequence(&seq);
        assert!(w.is_empty());
    }

    #[test]
    fn set_state_missing_key_and_value() {
        let seq = sample_seq(vec![make_step("s1", "set_state", json!({}))]);
        let w = lint_sequence(&seq);
        assert_eq!(w.len(), 2);
        assert!(w.iter().any(|w| w.message.contains("`key`")));
        assert!(w.iter().any(|w| w.message.contains("`value`")));
    }

    #[test]
    fn merge_state_values_not_object() {
        let seq = sample_seq(vec![make_step(
            "s1",
            "merge_state",
            json!({"values": "not_an_object"}),
        )]);
        let w = lint_sequence(&seq);
        assert_eq!(w.len(), 1);
        assert!(w[0].message.contains("must be an object"));
    }

    #[test]
    fn assert_missing_condition() {
        let seq = sample_seq(vec![make_step("s1", "assert", json!({}))]);
        let w = lint_sequence(&seq);
        assert_eq!(w.len(), 1);
        assert!(w[0].message.contains("`condition`"));
    }

    #[test]
    fn send_signal_missing_params() {
        let seq = sample_seq(vec![make_step("s1", "send_signal", json!({}))]);
        let w = lint_sequence(&seq);
        assert!(w.len() >= 2);
        assert!(w.iter().any(|w| w.message.contains("`instance_id`")));
        assert!(w.iter().any(|w| w.message.contains("`signal_type`")));
    }

    #[test]
    fn emit_event_missing_trigger_slug() {
        let seq = sample_seq(vec![make_step("s1", "emit_event", json!({}))]);
        let w = lint_sequence(&seq);
        assert_eq!(w.len(), 1);
        assert!(w[0].message.contains("`trigger_slug`"));
    }

    #[test]
    fn noop_handler_no_warnings() {
        let seq = sample_seq(vec![make_step("s1", "noop", json!({}))]);
        let w = lint_sequence(&seq);
        assert!(w.is_empty());
    }

    #[test]
    fn custom_handler_no_param_lint() {
        let seq = sample_seq(vec![make_step("s1", "my_custom_handler", json!({}))]);
        let w = lint_sequence(&seq);
        assert!(w.is_empty());
    }

    // ─── expression lint ───

    #[test]
    fn loop_bad_expression_warned() {
        let seq = sample_seq(vec![BlockDefinition::Loop(Box::new(LoopDef {
            id: BlockId("loop1".into()),
            condition: "data.x > > 5".into(),
            body: vec![make_step("s1", "noop", json!({}))],
            max_iterations: 10,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
        }))]);
        let w = lint_sequence(&seq);
        assert!(!w.is_empty());
        assert!(w[0].message.contains("expression parse error"));
    }

    #[test]
    fn loop_valid_expression_no_warning() {
        let seq = sample_seq(vec![BlockDefinition::Loop(Box::new(LoopDef {
            id: BlockId("loop1".into()),
            condition: "context.data.count < 10".into(),
            body: vec![make_step("s1", "noop", json!({}))],
            max_iterations: 10,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
        }))]);
        let w = lint_sequence(&seq);
        assert!(w.is_empty());
    }

    #[test]
    fn router_bad_condition_warned() {
        let seq = sample_seq(vec![BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId("r1".into()),
            routes: vec![Route {
                condition: "a + + b".into(),
                blocks: vec![make_step("s1", "noop", json!({}))],
            }],
            default: None,
        }))]);
        let w = lint_sequence(&seq);
        assert!(!w.is_empty(), "expected expression parse error warning");
        assert!(w[0].message.contains("expression parse error"));
    }

    #[test]
    fn router_valid_condition_no_warning() {
        let seq = sample_seq(vec![BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId("r1".into()),
            routes: vec![Route {
                condition: "context.data.x == true".into(),
                blocks: vec![make_step("s1", "noop", json!({}))],
            }],
            default: None,
        }))]);
        let w = lint_sequence(&seq);
        assert!(w.is_empty());
    }

    // ─── structural lint ───

    #[test]
    fn race_single_branch_warned() {
        let seq = sample_seq(vec![BlockDefinition::Race(Box::new(RaceDef {
            id: BlockId("r1".into()),
            branches: vec![vec![make_step("s1", "noop", json!({}))]],
            semantics: RaceSemantics::default(),
        }))]);
        let w = lint_sequence(&seq);
        assert_eq!(w.len(), 1);
        assert!(w[0].message.contains("fewer than 2 branches"));
    }

    #[test]
    fn try_catch_empty_catch_warned() {
        let seq = sample_seq(vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
            id: BlockId("tc1".into()),
            try_block: vec![make_step("s1", "noop", json!({}))],
            catch_block: vec![],
            finally_block: None,
        }))]);
        let w = lint_sequence(&seq);
        assert_eq!(w.len(), 1);
        assert!(w[0].message.contains("empty catch_block"));
    }

    #[test]
    fn parallel_empty_branch_warned() {
        let seq = sample_seq(vec![BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId("p1".into()),
            branches: vec![vec![make_step("s1", "noop", json!({}))], vec![]],
        }))]);
        let w = lint_sequence(&seq);
        assert_eq!(w.len(), 1);
        assert!(w[0].message.contains("empty"));
    }

    #[test]
    fn ab_split_zero_weight_variant_warned() {
        let seq = sample_seq(vec![BlockDefinition::ABSplit(Box::new(ABSplitDef {
            id: BlockId("ab1".into()),
            variants: vec![
                ABVariant {
                    name: "control".into(),
                    weight: 70,
                    blocks: vec![make_step("s1", "noop", json!({}))],
                },
                ABVariant {
                    name: "dead".into(),
                    weight: 0,
                    blocks: vec![make_step("s2", "noop", json!({}))],
                },
            ],
        }))]);
        let w = lint_sequence(&seq);
        assert_eq!(w.len(), 1);
        assert!(w[0].message.contains("weight 0"));
    }

    // ─── step-level lint ───

    #[test]
    fn fallback_same_as_primary_warned() {
        let mut s = StepDef {
            id: BlockId("s1".into()),
            handler: "http_request".into(),
            params: json!({"url": "https://x.com"}),
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: Some("http_request".into()),
            cache_key: None,
        };
        let mut warnings = Vec::new();
        lint_step(&s, &mut warnings);
        assert!(warnings
            .iter()
            .any(|w| w.message.contains("same as primary")));

        // unknown fallback
        warnings.clear();
        s.fallback_handler = Some("htpt_request".into());
        lint_step(&s, &mut warnings);
        assert!(warnings
            .iter()
            .any(|w| w.message.contains("unknown fallback_handler")));
    }

    #[test]
    fn timeout_exceeds_deadline_warned() {
        let s = StepDef {
            id: BlockId("s1".into()),
            handler: "noop".into(),
            params: json!({}),
            delay: None,
            retry: None,
            timeout: Some(Duration::from_mins(1)),
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: Some(Duration::from_secs(30)),
            on_deadline_breach: None,
            fallback_handler: None,
            cache_key: None,
        };
        let mut warnings = Vec::new();
        lint_step(&s, &mut warnings);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].message.contains("exceeds deadline"));
    }

    #[test]
    fn on_deadline_breach_without_deadline_warned() {
        let s = StepDef {
            id: BlockId("s1".into()),
            handler: "noop".into(),
            params: json!({}),
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: Some(EscalationDef {
                handler: "notify".into(),
                params: json!({}),
            }),
            fallback_handler: None,
            cache_key: None,
        };
        let mut warnings = Vec::new();
        lint_step(&s, &mut warnings);
        assert!(warnings.iter().any(|w| w.message.contains("no deadline")));
    }

    #[test]
    fn tool_call_missing_url() {
        let seq = sample_seq(vec![make_step("s1", "tool_call", json!({}))]);
        let w = lint_sequence(&seq);
        assert_eq!(w.len(), 1);
        assert!(w[0].message.contains("`url`"));
    }

    #[test]
    fn router_empty_route_blocks_warned() {
        let seq = sample_seq(vec![BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId("r1".into()),
            routes: vec![Route {
                condition: "true".into(),
                blocks: vec![],
            }],
            default: None,
        }))]);
        let w = lint_sequence(&seq);
        assert!(w.iter().any(|w| w.message.contains("empty blocks")));
    }

    #[test]
    fn deeply_nested_lint_descends() {
        let inner_step = make_step("inner", "set_state", json!({}));
        let seq = sample_seq(vec![BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId("p1".into()),
            branches: vec![vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
                id: BlockId("tc1".into()),
                try_block: vec![inner_step],
                catch_block: vec![make_step("catch", "noop", json!({}))],
                finally_block: None,
            }))]],
        }))]);
        let w = lint_sequence(&seq);
        assert!(
            w.iter()
                .any(|w| w.block_id == "inner" && w.message.contains("`key`")),
            "should find set_state missing key warning inside nested blocks: {w:?}",
        );
    }

    // ─── output reference lint ───

    #[test]
    fn output_ref_to_existing_block_no_warning() {
        let seq = sample_seq(vec![
            make_step("fetch", "http_request", json!({"url": "https://x.com"})),
            make_step("use", "log", json!({"message": "{{ steps.fetch.body }}"})),
        ]);
        let w = lint_sequence(&seq);
        assert!(
            !w.iter().any(|w| w.message.contains("no block with id")),
            "should not warn about existing block ref: {w:?}"
        );
    }

    #[test]
    fn output_ref_to_missing_block_warned() {
        let seq = sample_seq(vec![make_step(
            "s1",
            "log",
            json!({"message": "{{ steps.nonexistent.value }}"}),
        )]);
        let w = lint_sequence(&seq);
        assert!(
            w.iter()
                .any(|w| w.message.contains("no block with id `nonexistent`")),
            "should warn about missing block ref: {w:?}"
        );
    }

    #[test]
    fn outputs_ref_to_missing_block_warned() {
        let seq = sample_seq(vec![make_step(
            "s1",
            "log",
            json!({"message": "{{ outputs.ghost.data }}"}),
        )]);
        let w = lint_sequence(&seq);
        assert!(
            w.iter()
                .any(|w| w.message.contains("no block with id `ghost`")),
            "should warn about missing outputs ref: {w:?}"
        );
    }

    #[test]
    fn output_ref_in_router_condition_warned() {
        let seq = sample_seq(vec![BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId("r1".into()),
            routes: vec![Route {
                condition: "steps.missing_step.value == true".into(),
                blocks: vec![make_step("s1", "noop", json!({}))],
            }],
            default: None,
        }))]);
        let w = lint_sequence(&seq);
        assert!(
            w.iter()
                .any(|w| w.message.contains("no block with id `missing_step`")),
            "should warn about missing ref in router condition: {w:?}"
        );
    }

    #[test]
    fn output_ref_in_for_each_collection_warned() {
        let seq = sample_seq(vec![BlockDefinition::ForEach(Box::new(ForEachDef {
            id: BlockId("fe1".into()),
            collection: "{{ steps.missing.items }}".into(),
            item_var: "item".into(),
            body: vec![make_step("s1", "noop", json!({}))],
            max_iterations: 50,
        }))]);
        let w = lint_sequence(&seq);
        assert!(
            w.iter()
                .any(|w| w.message.contains("no block with id `missing`")),
            "should warn about missing ref in for_each collection: {w:?}"
        );
    }

    // ─── for_each safety lint ───

    #[test]
    fn for_each_default_max_iterations_warned() {
        let seq = sample_seq(vec![BlockDefinition::ForEach(Box::new(ForEachDef {
            id: BlockId("fe1".into()),
            collection: "{{ context.data.items }}".into(),
            item_var: "item".into(),
            body: vec![make_step("s1", "noop", json!({}))],
            max_iterations: 1000, // default
        }))]);
        let w = lint_sequence(&seq);
        assert!(
            w.iter()
                .any(|w| w.block_id == "fe1" && w.message.contains("default max_iterations")),
            "should warn about default max_iterations: {w:?}"
        );
    }

    #[test]
    fn for_each_explicit_max_iterations_no_warning() {
        let seq = sample_seq(vec![BlockDefinition::ForEach(Box::new(ForEachDef {
            id: BlockId("fe1".into()),
            collection: "{{ context.data.items }}".into(),
            item_var: "item".into(),
            body: vec![make_step("s1", "noop", json!({}))],
            max_iterations: 50,
        }))]);
        let w = lint_sequence(&seq);
        assert!(
            !w.iter()
                .any(|w| w.message.contains("default max_iterations")),
            "should not warn with explicit max_iterations: {w:?}"
        );
    }

    // ─── unreachable block lint ───

    #[test]
    fn step_after_fail_warned() {
        let seq = sample_seq(vec![
            make_step("s1", "fail", json!({"message": "abort"})),
            make_step("s2", "noop", json!({})),
        ]);
        let w = lint_sequence(&seq);
        assert!(
            w.iter()
                .any(|w| w.block_id == "s2" && w.message.contains("unreachable")),
            "should warn that s2 is unreachable after fail: {w:?}"
        );
    }

    #[test]
    fn step_after_fail_in_nested_branch_warned() {
        let seq = sample_seq(vec![BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId("p1".into()),
            branches: vec![vec![
                make_step("die", "fail", json!({})),
                make_step("dead", "noop", json!({})),
            ]],
        }))]);
        let w = lint_sequence(&seq);
        assert!(
            w.iter()
                .any(|w| w.block_id == "dead" && w.message.contains("unreachable")),
            "should detect unreachable in nested parallel branch: {w:?}"
        );
    }

    #[test]
    fn no_unreachable_without_fail() {
        let seq = sample_seq(vec![
            make_step("s1", "noop", json!({})),
            make_step("s2", "log", json!({"message": "ok"})),
        ]);
        let w = lint_sequence(&seq);
        assert!(
            !w.iter().any(|w| w.message.contains("unreachable")),
            "no unreachable warning without fail: {w:?}"
        );
    }

    #[test]
    fn multiple_refs_both_valid_and_invalid() {
        let seq = sample_seq(vec![
            make_step("step_a", "noop", json!({})),
            make_step(
                "step_b",
                "log",
                json!({
                    "message": "{{ steps.step_a.x }} and {{ steps.bogus.y }}"
                }),
            ),
        ]);
        let w = lint_sequence(&seq);
        assert!(
            !w.iter().any(|w| w.message.contains("`step_a`")),
            "should not warn about valid ref step_a: {w:?}"
        );
        assert!(
            w.iter().any(|w| w.message.contains("`bogus`")),
            "should warn about invalid ref bogus: {w:?}"
        );
    }
}
