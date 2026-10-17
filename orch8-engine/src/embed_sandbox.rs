//! Sandbox for sequences authored by an embedded end customer (sub-tenant).
//!
//! An embed token holding `builder:edit` lets an end customer write a sequence
//! (`PUT /embed/sequences/{name}`) that then runs with the *tenant's*
//! authority: outbound HTTP, the tenant's LLM and credentials, signals to
//! other instances. That authority belongs to the vendor, so a sequence owned
//! by a sub-tenant is confined to an allowlist of inert handlers and may not
//! reference credentials, reach other sub-tenants' instances or sequences.
//!
//! The rules are enforced twice:
//!
//! * **write time** ([`check_sequence`], used by the embed `PUT`): a violation
//!   is rejected with a message naming the offending block;
//! * **execution time** ([`check_step`], [`is_sandboxed`], the `sub_sequence`
//!   dispatch and the by-id handlers): independent of how the sequence got
//!   into storage, so sequences stored before the sandbox existed, and values
//!   only known at run time (a template resolving to `credentials://…`), are
//!   covered too.
//!
//! "Sandboxed" means: the instance belongs to a sub-tenant **and** its
//! sequence is owned by a sub-tenant. Vendor-authored (tenant-level) sequences,
//! including gallery templates, are never restricted.

use std::collections::BTreeSet;
use std::fmt;

use serde_json::Value;

use orch8_storage::StorageBackend;
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef};

use crate::error::EngineError;

/// Handlers a sub-tenant-owned sequence may use unless the vendor configures
/// `[embed] sandbox_handlers`. Pure / control-flow / instance-local state and
/// human review only: nothing that reaches the network (apart from the
/// SSRF-guarded `human_review` notify URL, which the sandbox forbids), the
/// tenant's credentials or LLM budget, other instances, events, memory, blobs
/// or workers.
pub const DEFAULT_SANDBOX_HANDLERS: &[&str] = &[
    "noop",
    "log",
    "sleep",
    "fail",
    "assert",
    "human_review",
    "set_state",
    "get_state",
    "delete_state",
    "merge_state",
    "transform",
];

/// Params that make an otherwise inert allowlisted handler reach the network.
const FORBIDDEN_PARAMS: &[(&str, &[&str])] = &[("human_review", &["notify_url", "notify_headers"])];

/// The handler allowlist applied to sub-tenant-owned sequences.
#[derive(Debug, Clone)]
pub struct SandboxPolicy {
    handlers: BTreeSet<String>,
}

impl Default for SandboxPolicy {
    fn default() -> Self {
        Self {
            handlers: DEFAULT_SANDBOX_HANDLERS
                .iter()
                .map(|h| (*h).to_owned())
                .collect(),
        }
    }
}

impl SandboxPolicy {
    /// Build from the `[embed] sandbox_handlers` comma-separated list. Empty
    /// (or blank) selects [`DEFAULT_SANDBOX_HANDLERS`]; anything else
    /// **replaces** it.
    #[must_use]
    pub fn from_config_list(raw: &str) -> Self {
        let handlers: BTreeSet<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|h| !h.is_empty())
            .map(str::to_owned)
            .collect();
        if handlers.is_empty() {
            Self::default()
        } else {
            Self { handlers }
        }
    }

    #[must_use]
    pub fn allows(&self, handler: &str) -> bool {
        self.handlers.contains(handler)
    }

    /// The effective allowlist, sorted.
    pub fn allowed_handlers(&self) -> impl Iterator<Item = &str> {
        self.handlers.iter().map(String::as_str)
    }
}

/// One sandbox rule a sequence or step breaks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// Block (or `interceptors`) the violation was found in.
    pub block_id: String,
    pub message: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "block \"{}\": {}", self.block_id, self.message)
    }
}

fn handler_violation(
    block_id: &str,
    handler: &str,
    role: &str,
    policy: &SandboxPolicy,
) -> Violation {
    let allowed: Vec<&str> = policy.allowed_handlers().collect();
    Violation {
        block_id: block_id.to_owned(),
        message: format!(
            "{role} \"{handler}\" is not allowed in a sequence authored by an embedded \
             sub-tenant (allowed: {})",
            allowed.join(", ")
        ),
    }
}

fn credentials_violation(block_id: &str) -> Violation {
    Violation {
        block_id: block_id.to_owned(),
        message: "credentials:// references are not allowed in a sequence authored by an \
                  embedded sub-tenant"
            .into(),
    }
}

fn contains_credentials(value: &Value) -> bool {
    orch8_types::worker::contains_credential_reference(value)
}

/// Handler names a step can dispatch, with the role each plays.
fn step_handlers(step: &StepDef) -> Vec<(&'static str, &str)> {
    let mut out = vec![("handler", step.handler.as_str())];
    if let Some(h) = &step.fallback_handler {
        out.push(("fallback_handler", h.as_str()));
    }
    if let Some(e) = &step.on_deadline_breach {
        out.push(("on_deadline_breach handler", e.handler.as_str()));
    }
    if let Some(h) = step
        .wait_for_input
        .as_ref()
        .and_then(|w| w.escalation_handler.as_deref())
    {
        out.push(("escalation_handler", h));
    }
    if let Some(c) = &step.compensation {
        out.push(("compensation handler", c.handler.as_str()));
    }
    out
}

fn forbidden_param(handler: &str, params: &Value) -> Option<&'static str> {
    let obj = params.as_object()?;
    FORBIDDEN_PARAMS
        .iter()
        .find(|(h, _)| *h == handler)
        .and_then(|(_, keys)| keys.iter().find(|k| obj.contains_key(**k)))
        .copied()
}

fn check_step_def(step: &StepDef, policy: &SandboxPolicy, out: &mut Vec<Violation>) {
    let id = step.id.as_str();
    for (role, handler) in step_handlers(step) {
        if !policy.allows(handler) {
            out.push(handler_violation(id, handler, role, policy));
        }
    }
    if let Some(key) = forbidden_param(&step.handler, &step.params) {
        out.push(Violation {
            block_id: id.to_owned(),
            message: format!(
                "param \"{key}\" is not allowed on handler \"{}\" in a sequence authored by an \
                 embedded sub-tenant",
                step.handler
            ),
        });
    }
    if step
        .wait_for_input
        .as_ref()
        .is_some_and(|w| w.auto_decide.is_some())
    {
        out.push(Violation {
            block_id: id.to_owned(),
            message: "wait_for_input.auto_decide calls an external model with the operator's \
                      or a stored credential and is not allowed in a sequence authored by an \
                      embedded sub-tenant"
                .into(),
        });
    }
    // Serialising the step covers every field that could carry a reference
    // (params, compensation/escalation params, auto_decide.api_key, …).
    if serde_json::to_value(step)
        .as_ref()
        .is_ok_and(contains_credentials)
    {
        out.push(credentials_violation(id));
    }
}

fn walk(blocks: &[BlockDefinition], policy: &SandboxPolicy, out: &mut Vec<Violation>) {
    for block in blocks {
        match block {
            BlockDefinition::Step(s) => check_step_def(s, policy, out),
            BlockDefinition::Parallel(p) => p.branches.iter().for_each(|b| walk(b, policy, out)),
            BlockDefinition::Race(r) => r.branches.iter().for_each(|b| walk(b, policy, out)),
            BlockDefinition::Loop(l) => walk(&l.body, policy, out),
            BlockDefinition::ForEach(fe) => walk(&fe.body, policy, out),
            BlockDefinition::Router(r) => {
                r.routes
                    .iter()
                    .for_each(|route| walk(&route.blocks, policy, out));
                if let Some(default) = &r.default {
                    walk(default, policy, out);
                }
            }
            BlockDefinition::TryCatch(tc) => {
                walk(&tc.try_block, policy, out);
                walk(&tc.catch_block, policy, out);
                if let Some(finally) = &tc.finally_block {
                    walk(finally, policy, out);
                }
            }
            BlockDefinition::SubSequence(ss) => {
                if contains_credentials(&ss.input) {
                    out.push(credentials_violation(ss.id.as_str()));
                }
            }
            BlockDefinition::ABSplit(ab) => {
                ab.variants
                    .iter()
                    .for_each(|v| walk(&v.blocks, policy, out));
            }
            BlockDefinition::CancellationScope(cs) => walk(&cs.blocks, policy, out),
            BlockDefinition::Saga(saga) => {
                for step in &saga.steps {
                    walk(std::slice::from_ref(&step.action), policy, out);
                    if let Some(comp) = &step.compensation {
                        walk(std::slice::from_ref(comp), policy, out);
                    }
                }
            }
        }
    }
}

/// Every sandbox violation in `seq` (empty = the sequence is admissible).
/// Covers the main blocks, `on_failure` / `on_cancel` hooks and interceptors.
#[must_use]
pub fn check_sequence(seq: &SequenceDefinition, policy: &SandboxPolicy) -> Vec<Violation> {
    let mut out = Vec::new();
    walk(&seq.blocks, policy, &mut out);
    for hook in [&seq.on_failure, &seq.on_cancel].into_iter().flatten() {
        walk(hook, policy, &mut out);
    }
    if let Some(interceptors) = &seq.interceptors {
        for action in [
            &interceptors.before_step,
            &interceptors.after_step,
            &interceptors.on_signal,
            &interceptors.on_complete,
            &interceptors.on_failure,
        ]
        .into_iter()
        .flatten()
        {
            if !policy.allows(&action.handler) {
                out.push(handler_violation(
                    "interceptors",
                    &action.handler,
                    "interceptor handler",
                    policy,
                ));
            }
            if contains_credentials(&action.params) {
                out.push(credentials_violation("interceptors"));
            }
        }
    }
    out
}

/// `(block id, target sequence name)` of every `sub_sequence` block in `seq`.
#[must_use]
pub fn sub_sequence_targets(seq: &SequenceDefinition) -> Vec<(String, String)> {
    fn collect(blocks: &[BlockDefinition], out: &mut Vec<(String, String)>) {
        for block in blocks {
            match block {
                BlockDefinition::SubSequence(ss) => {
                    out.push((ss.id.as_str().to_owned(), ss.sequence_name.clone()));
                }
                BlockDefinition::Parallel(p) => p.branches.iter().for_each(|b| collect(b, out)),
                BlockDefinition::Race(r) => r.branches.iter().for_each(|b| collect(b, out)),
                BlockDefinition::Loop(l) => collect(&l.body, out),
                BlockDefinition::ForEach(fe) => collect(&fe.body, out),
                BlockDefinition::Router(r) => {
                    r.routes
                        .iter()
                        .for_each(|route| collect(&route.blocks, out));
                    if let Some(default) = &r.default {
                        collect(default, out);
                    }
                }
                BlockDefinition::TryCatch(tc) => {
                    collect(&tc.try_block, out);
                    collect(&tc.catch_block, out);
                    if let Some(finally) = &tc.finally_block {
                        collect(finally, out);
                    }
                }
                BlockDefinition::ABSplit(ab) => {
                    ab.variants.iter().for_each(|v| collect(&v.blocks, out));
                }
                BlockDefinition::CancellationScope(cs) => collect(&cs.blocks, out),
                BlockDefinition::Saga(saga) => {
                    for step in &saga.steps {
                        collect(std::slice::from_ref(&step.action), out);
                        if let Some(comp) = &step.compensation {
                            collect(std::slice::from_ref(comp), out);
                        }
                    }
                }
                BlockDefinition::Step(_) => {}
            }
        }
    }
    let mut out = Vec::new();
    collect(&seq.blocks, &mut out);
    for hook in [&seq.on_failure, &seq.on_cancel].into_iter().flatten() {
        collect(hook, &mut out);
    }
    out
}

/// Whether `instance` runs under the sandbox: it belongs to a sub-tenant and
/// its sequence is sub-tenant-owned. A missing sequence fails closed.
///
/// Instances without a sub-tenant return without touching storage.
///
/// # Errors
/// Storage failures reading the sequence.
pub async fn is_sandboxed(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
) -> Result<bool, EngineError> {
    if instance.sub_tenant.is_none() {
        return Ok(false);
    }
    let sequence = storage.get_sequence(instance.sequence_id).await?;
    Ok(sequence.is_none_or(|s| s.sub_tenant.is_some()))
}

/// Execution-time check of one step about to be dispatched, against the
/// *resolved* params (templates already expanded, credentials not yet).
/// `Err` carries the reason the step must fail instead of running.
///
/// # Errors
/// A message naming the step and the rule it breaks.
pub fn check_step(
    step: &StepDef,
    resolved_params: &Value,
    policy: &SandboxPolicy,
) -> Result<(), String> {
    let mut violations = Vec::new();
    for (role, handler) in step_handlers(step) {
        if !policy.allows(handler) {
            violations.push(handler_violation(step.id.as_str(), handler, role, policy));
        }
    }
    if let Some(key) = forbidden_param(&step.handler, resolved_params) {
        violations.push(Violation {
            block_id: step.id.as_str().to_owned(),
            message: format!(
                "param \"{key}\" is not allowed on handler \"{}\"",
                step.handler
            ),
        });
    }
    if contains_credentials(resolved_params) {
        violations.push(credentials_violation(step.id.as_str()));
    }
    match violations.into_iter().next() {
        None => Ok(()),
        Some(v) => Err(format!("embed sandbox: {v}")),
    }
}

/// Execution-time check of a handler dispatched outside the main step path
/// (cleanup hooks, deadline escalations): `Some(reason)` when `instance` is
/// sandboxed and `handler` is not allowed.
///
/// # Errors
/// Storage failures reading the sequence.
pub async fn deny_handler(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    policy: &SandboxPolicy,
    block_id: &str,
    handler: &str,
) -> Result<Option<String>, EngineError> {
    if policy.allows(handler) || !is_sandboxed(storage, instance).await? {
        return Ok(None);
    }
    Ok(Some(format!(
        "embed sandbox: {}",
        handler_violation(block_id, handler, "handler", policy)
    )))
}

/// Whether a `sub_sequence` block of `parent` may spawn `child`. A sandboxed
/// parent may only start sequences owned by its own sub-tenant; anything
/// else (vendor sequences, gallery templates, other sub-tenants') is denied.
#[must_use]
pub fn sub_sequence_allowed(parent: &TaskInstance, child: &SequenceDefinition) -> bool {
    parent.sub_tenant.is_some() && child.sub_tenant == parent.sub_tenant
}

#[cfg(test)]
#[allow(clippy::needless_pass_by_value)]
mod tests {
    use super::*;
    use orch8_types::ids::SequenceId;
    use serde_json::json;

    fn seq_from(blocks: Value) -> SequenceDefinition {
        serde_json::from_value(json!({
            "id": SequenceId::new(),
            "tenant_id": "t",
            "namespace": "default",
            "name": "s",
            "version": 1,
            "blocks": blocks,
            "created_at": chrono::Utc::now(),
        }))
        .unwrap()
    }

    fn step(id: &str, handler: &str, params: Value) -> Value {
        json!({"type": "step", "id": id, "handler": handler, "params": params})
    }

    fn violations(blocks: Value) -> Vec<Violation> {
        check_sequence(&seq_from(blocks), &SandboxPolicy::default())
    }

    #[test]
    fn default_policy_admits_inert_handlers() {
        let v = violations(json!([
            step("a", "set_state", json!({"key": "k", "value": 1})),
            step("b", "transform", json!({"x": "{{ context.data.y }}"})),
            step("c", "human_review", json!({"instructions": "ok?"})),
            step("d", "sleep", json!({"duration_ms": 10})),
        ]));
        assert!(v.is_empty(), "{v:?}");
    }

    #[test]
    fn default_policy_rejects_dangerous_handlers_naming_the_block() {
        for handler in [
            "http_request",
            "llm_call",
            "query_instance",
            "send_signal",
            "email",
            "notify",
            "self_modify",
            "agent",
            "emit_event",
            "tool_call",
            "mcp_call",
            "my_worker_handler",
        ] {
            let v = violations(json!([step("bad_block", handler, json!({}))]));
            assert_eq!(v.len(), 1, "{handler}: {v:?}");
            assert_eq!(v[0].block_id, "bad_block");
            assert!(v[0].message.contains(handler), "{}", v[0].message);
        }
    }

    #[test]
    fn nested_blocks_hooks_and_aux_handlers_are_checked() {
        let seq = seq_from(json!([{
            "type": "parallel", "id": "p",
            "branches": [[
                {"type": "router", "id": "r", "routes": [
                    {"condition": "true", "blocks": [step("deep", "http_request", json!({}))]}
                ]}
            ]]
        }]));
        let v = check_sequence(&seq, &SandboxPolicy::default());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].block_id, "deep");

        let mut seq = seq_from(json!([step("a", "noop", json!({}))]));
        seq.on_failure =
            Some(serde_json::from_value(json!([step("hook", "llm_call", json!({}))])).unwrap());
        let v = check_sequence(&seq, &SandboxPolicy::default());
        assert_eq!(v[0].block_id, "hook");

        let seq = seq_from(json!([{
            "type": "step", "id": "a", "handler": "noop",
            "fallback_handler": "http_request",
            "on_deadline_breach": {"handler": "notify", "params": {}},
            "compensation": {"handler": "email"}
        }]));
        let v = check_sequence(&seq, &SandboxPolicy::default());
        assert_eq!(v.len(), 3, "{v:?}");
    }

    #[test]
    fn credential_references_are_rejected_everywhere() {
        let v = violations(json!([step(
            "a",
            "noop",
            json!({"k": {"n": ["credentials://prod"]}})
        )]));
        assert!(
            v.iter()
                .any(|x| x.block_id == "a" && x.message.contains("credentials://"))
        );

        let v = violations(json!([{
            "type": "sub_sequence", "id": "s", "sequence_name": "x",
            "input": {"token": "credentials://prod/key"}
        }]));
        assert_eq!(v[0].block_id, "s");

        let mut seq = seq_from(json!([step("a", "noop", json!({}))]));
        seq.interceptors = Some(
            serde_json::from_value(
                json!({"before_step": {"handler": "noop", "params": {"k": "credentials://x"}}}),
            )
            .unwrap(),
        );
        let v = check_sequence(&seq, &SandboxPolicy::default());
        assert_eq!(v[0].block_id, "interceptors");
    }

    #[test]
    fn auto_decide_and_review_network_params_are_rejected() {
        let v = violations(json!([step(
            "a",
            "human_review",
            json!({"notify_url": "https://evil"})
        )]));
        assert!(v[0].message.contains("notify_url"));
        let v = violations(json!([{
            "type": "step", "id": "g", "handler": "human_review",
            "wait_for_input": {"prompt": "?", "auto_decide": {}}
        }]));
        assert!(v.iter().any(|x| x.message.contains("auto_decide")), "{v:?}");
    }

    #[test]
    fn config_list_replaces_the_default() {
        let policy = SandboxPolicy::from_config_list(" noop , crm_lookup ,");
        assert!(policy.allows("crm_lookup") && policy.allows("noop"));
        assert!(!policy.allows("set_state"));
        let policy = SandboxPolicy::from_config_list("  ");
        assert!(policy.allows("set_state") && !policy.allows("http_request"));
    }

    #[test]
    fn sub_sequence_targets_found_in_nested_blocks() {
        let seq = seq_from(json!([{
            "type": "loop", "id": "l", "condition": "true", "max_iterations": 2,
            "body": [{"type": "sub_sequence", "id": "ss", "sequence_name": "other"}]
        }]));
        assert_eq!(
            sub_sequence_targets(&seq),
            vec![("ss".to_owned(), "other".to_owned())]
        );
    }

    #[test]
    fn check_step_blocks_runtime_credentials_and_handlers() {
        let policy = SandboxPolicy::default();
        let BlockDefinition::Step(ok) = seq_from(json!([step("a", "noop", json!({}))]))
            .blocks
            .remove(0)
        else {
            unreachable!()
        };
        assert!(check_step(&ok, &json!({"a": 1}), &policy).is_ok());
        let err = check_step(&ok, &json!({"a": "credentials://prod"}), &policy).unwrap_err();
        assert!(
            err.contains("credentials://") && err.contains("\"a\""),
            "{err}"
        );

        let BlockDefinition::Step(bad) = seq_from(json!([step("h", "http_request", json!({}))]))
            .blocks
            .remove(0)
        else {
            unreachable!()
        };
        assert!(
            check_step(&bad, &json!({}), &policy)
                .unwrap_err()
                .contains("http_request")
        );
    }
}
