//! Sequence preflight: pure readiness rules over a snapshot of the
//! runtime inventory.
//!
//! Lint stays pure (definition only); preflight adds "will it actually
//! run here?" by checking the definition against workers, credentials,
//! plugins, queues, and sub-sequences. The storage snapshot is collected
//! by the caller (API service / CLI) and passed in as a
//! [`RuntimeInventory`] — these rules never touch storage themselves.
//!
//! Any inventory section that is `None` (not collected / unavailable)
//! makes its checks report [`PreflightStatus::Unknown`]: uncertainty is
//! never turned into success.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde_json::Value;

use orch8_types::finding::{
    Confidence, Evidence, Finding, FindingSeverity, Remediation, ResourceRef,
};
use orch8_types::preflight::{PreflightCheck, PreflightReport, PreflightStatus};
use orch8_types::queue_dispatch::{DispatchMode, QueueDispatchConfig};
use orch8_types::queue_routing::QueueRoutingRule;
use orch8_types::sequence::{BUILTIN_HANDLER_NAMES, SequenceDefinition, SequenceStatus};
use orch8_types::worker::{WorkerRegistration, WorkerVersionPin, version_satisfies};

use crate::lint::lint_sequence;

/// Prefix marking credential references inside step params.
const CREDENTIAL_PREFIX: &str = "credentials://";

/// A snapshot of runtime facts, collected by the caller. `None` sections
/// were not collected and yield `Unknown` check results.
#[derive(Debug, Clone, Default)]
pub struct RuntimeInventory {
    /// Live worker registrations (caller applies its own liveness window).
    pub worker_registrations: Option<Vec<WorkerRegistration>>,
    /// Active worker version pins.
    pub version_pins: Option<Vec<WorkerVersionPin>>,
    /// Known credentials visible to the sequence's tenant.
    pub credentials: Option<Vec<CredentialInfo>>,
    /// Known plugins visible to the sequence's tenant.
    pub plugins: Option<Vec<PluginInfo>>,
    /// Queue dispatch configs (push/poll) visible to the tenant.
    pub queue_dispatch: Option<Vec<QueueDispatchConfig>>,
    /// Queue routing rules visible to the tenant.
    pub routing_rules: Option<Vec<QueueRoutingRule>>,
    /// Published sequences visible to the tenant (for sub-sequence refs).
    pub sequences: Option<Vec<SubSequenceInfo>>,
}

/// Minimal credential facts needed by preflight (never the secret).
#[derive(Debug, Clone)]
pub struct CredentialInfo {
    pub id: String,
    pub enabled: bool,
    pub expires_at: Option<DateTime<Utc>>,
}

/// Minimal plugin facts needed by preflight.
#[derive(Debug, Clone)]
pub struct PluginInfo {
    pub name: String,
    pub enabled: bool,
}

/// Minimal sequence facts needed for sub-sequence checks.
#[derive(Debug, Clone)]
pub struct SubSequenceInfo {
    pub name: String,
    pub namespace: String,
    pub version: i32,
    pub status: SequenceStatus,
}

/// References extracted from a sequence definition.
#[derive(Debug, Default, PartialEq)]
struct SequenceRefs {
    /// (block id, handler) for every step.
    steps: Vec<(String, String)>,
    /// Explicit queue names steps target.
    explicit_queues: BTreeSet<String>,
    /// `credentials://ID` references (bare id, field suffix stripped).
    credential_ids: BTreeSet<String>,
    /// (block id, `sequence_name`, version) for sub-sequence blocks.
    sub_sequences: Vec<(String, String, Option<i32>)>,
    /// Rate-limit keys referenced by steps.
    rate_limit_keys: BTreeSet<String>,
}

/// Walk the serialized definition and collect everything preflight needs.
/// JSON-walking (rather than matching every DSL variant) keeps this in
/// sync with new composite block types automatically.
fn collect_refs(seq: &SequenceDefinition) -> SequenceRefs {
    fn walk(value: &Value, refs: &mut SequenceRefs) {
        match value {
            Value::Object(map) => {
                let ty = map.get("type").and_then(Value::as_str);
                if let (Some(Value::String(id)), Some(Value::String(handler))) =
                    (map.get("id"), map.get("handler"))
                {
                    refs.steps.push((id.clone(), handler.clone()));
                    if let Some(Value::String(q)) = map.get("queue_name") {
                        refs.explicit_queues.insert(q.clone());
                    }
                    if let Some(Value::String(k)) = map.get("rate_limit_key") {
                        refs.rate_limit_keys.insert(k.clone());
                    }
                }
                if ty == Some("sub_sequence")
                    && let (Some(Value::String(id)), Some(Value::String(name))) =
                        (map.get("id"), map.get("sequence_name"))
                {
                    let version = map
                        .get("version")
                        .and_then(Value::as_i64)
                        .and_then(|v| i32::try_from(v).ok());
                    refs.sub_sequences.push((id.clone(), name.clone(), version));
                }
                for child in map.values() {
                    walk(child, refs);
                }
            }
            Value::Array(items) => {
                for item in items {
                    walk(item, refs);
                }
            }
            Value::String(s) => {
                if let Some(rest) = s.strip_prefix(CREDENTIAL_PREFIX) {
                    // `credentials://ID` or `credentials://ID/field`
                    let id = rest.split('/').next().unwrap_or(rest);
                    if !id.is_empty() {
                        refs.credential_ids.insert(id.to_string());
                    }
                }
            }
            _ => {}
        }
    }
    let mut refs = SequenceRefs::default();
    if let Ok(v) = serde_json::to_value(seq) {
        walk(&v, &mut refs);
    }
    refs
}

/// Run every preflight check. Pure: all runtime facts come from
/// `inventory`, the current time from `now`.
#[must_use]
pub fn run_preflight(
    seq: &SequenceDefinition,
    inventory: &RuntimeInventory,
    now: DateTime<Utc>,
) -> PreflightReport {
    let refs = collect_refs(seq);
    let checks = vec![
        check_definition(seq, now),
        check_lint(seq, now),
        check_input_schema(seq, now),
        check_output_schemas(seq, now),
        check_when_guards(seq, now),
        check_handlers(seq, &refs, inventory, now),
        check_plugins(&refs, inventory, now),
        check_credentials(&refs, inventory, now),
        check_queues(&refs, inventory, now),
        check_sub_sequences(seq, &refs, inventory, now),
    ];
    PreflightReport::new(seq.name.clone(), i64::from(seq.version), checks, now)
}

fn unknown(id: &str, what: &str) -> PreflightCheck {
    PreflightCheck::with_status(
        id,
        PreflightStatus::Unknown,
        format!("{what} inventory was not collected — readiness unproven"),
        vec![],
    )
}

fn check_definition(seq: &SequenceDefinition, now: DateTime<Utc>) -> PreflightCheck {
    match seq.validate() {
        Ok(()) => PreflightCheck::pass("definition_valid", "definition validates"),
        Err(e) => PreflightCheck::with_status(
            "definition_valid",
            PreflightStatus::Fail,
            "definition is invalid",
            vec![Finding::new(
                "INVALID_DEFINITION",
                FindingSeverity::Error,
                e.to_string(),
                Confidence::Certain,
                now,
            )],
        ),
    }
}

fn check_lint(seq: &SequenceDefinition, now: DateTime<Utc>) -> PreflightCheck {
    let warnings = lint_sequence(seq);
    if warnings.is_empty() {
        return PreflightCheck::pass("lint_clean", "no lint warnings");
    }
    let findings = warnings
        .iter()
        .map(|w| {
            Finding::new(
                "LINT_WARNING",
                FindingSeverity::Warning,
                w.to_string(),
                Confidence::Certain,
                now,
            )
            .with_resource(ResourceRef::new("block", w.block_id.clone()))
        })
        .collect::<Vec<_>>();
    PreflightCheck::with_status(
        "lint_clean",
        PreflightStatus::Warning,
        format!("{} lint warning(s)", findings.len()),
        findings,
    )
}

fn check_input_schema(seq: &SequenceDefinition, now: DateTime<Utc>) -> PreflightCheck {
    match &seq.input_schema {
        None => PreflightCheck::pass("input_schema_valid", "no input schema declared"),
        Some(Value::Object(_)) => {
            PreflightCheck::pass("input_schema_valid", "input schema is well-formed JSON")
        }
        Some(other) => PreflightCheck::with_status(
            "input_schema_valid",
            PreflightStatus::Fail,
            "input schema must be a JSON object",
            vec![Finding::new(
                "INVALID_INPUT_SCHEMA",
                FindingSeverity::Error,
                format!(
                    "input_schema must be a JSON Schema object, found {}",
                    json_type_name(other)
                ),
                Confidence::Certain,
                now,
            )],
        ),
    }
}

fn check_output_schemas(seq: &SequenceDefinition, now: DateTime<Utc>) -> PreflightCheck {
    fn walk(value: &Value, findings: &mut Vec<Finding>, now: DateTime<Utc>) {
        match value {
            Value::Object(map) => {
                if let Some(Value::String(id)) = map.get("id")
                    && let Some(schema) = map.get("output_schema")
                    && !schema.is_null()
                {
                    if !schema.is_object() {
                        findings.push(
                            Finding::new(
                                "INVALID_OUTPUT_SCHEMA",
                                FindingSeverity::Error,
                                format!(
                                    "step '{id}' output_schema must be a JSON Schema object, found {}",
                                    json_type_name(schema)
                                ),
                                Confidence::Certain,
                                now,
                            )
                            .with_resource(ResourceRef::new("block", id.clone())),
                        );
                    } else if jsonschema::validator_for(schema).is_err() {
                        findings.push(
                            Finding::new(
                                "INVALID_OUTPUT_SCHEMA",
                                FindingSeverity::Error,
                                format!("step '{id}' output_schema is not a valid JSON Schema"),
                                Confidence::Certain,
                                now,
                            )
                            .with_resource(ResourceRef::new("block", id.clone())),
                        );
                    }
                }
                for child in map.values() {
                    walk(child, findings, now);
                }
            }
            Value::Array(items) => {
                for item in items {
                    walk(item, findings, now);
                }
            }
            _ => {}
        }
    }

    let Ok(val) = serde_json::to_value(seq) else {
        return PreflightCheck::pass(
            "output_schemas_valid",
            "no output schemas (serialization failed)",
        );
    };
    let mut findings = Vec::new();
    walk(&val, &mut findings, now);
    if findings.is_empty() {
        PreflightCheck::pass("output_schemas_valid", "all output schemas are well-formed")
    } else {
        PreflightCheck::with_status(
            "output_schemas_valid",
            PreflightStatus::Fail,
            format!("{} invalid output schema(s)", findings.len()),
            findings,
        )
    }
}

fn check_when_guards(seq: &SequenceDefinition, now: DateTime<Utc>) -> PreflightCheck {
    fn walk(value: &Value, findings: &mut Vec<Finding>, now: DateTime<Utc>) {
        match value {
            Value::Object(map) => {
                if let Some(Value::String(id)) = map.get("id")
                    && let Some(Value::String(when_expr)) = map.get("when")
                    && when_expr.trim().is_empty()
                {
                    findings.push(
                        Finding::new(
                            "EMPTY_WHEN_GUARD",
                            FindingSeverity::Warning,
                            format!("step '{id}' has an empty `when` guard expression"),
                            Confidence::Certain,
                            now,
                        )
                        .with_resource(ResourceRef::new("block", id.clone())),
                    );
                }
                for child in map.values() {
                    walk(child, findings, now);
                }
            }
            Value::Array(items) => {
                for item in items {
                    walk(item, findings, now);
                }
            }
            _ => {}
        }
    }

    let Ok(val) = serde_json::to_value(seq) else {
        return PreflightCheck::pass("when_guards_valid", "no when guards (serialization failed)");
    };
    let mut findings = Vec::new();
    walk(&val, &mut findings, now);
    if findings.is_empty() {
        PreflightCheck::pass(
            "when_guards_valid",
            "all `when` guard expressions are well-formed",
        )
    } else {
        PreflightCheck::with_status(
            "when_guards_valid",
            PreflightStatus::Fail,
            format!("{} invalid `when` guard(s)", findings.len()),
            findings,
        )
    }
}

const fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Handlers not covered by built-ins or enabled plugins need a live,
/// version-compatible worker.
#[allow(clippy::too_many_lines)] // one coverage rule per handler source, kept together
fn check_handlers(
    seq: &SequenceDefinition,
    refs: &SequenceRefs,
    inventory: &RuntimeInventory,
    now: DateTime<Utc>,
) -> PreflightCheck {
    let Some(registrations) = &inventory.worker_registrations else {
        return unknown("handlers_have_workers", "worker registration");
    };

    let plugin_names: Option<BTreeSet<&str>> = inventory.plugins.as_ref().map(|ps| {
        ps.iter()
            .filter(|p| p.enabled)
            .map(|p| p.name.as_str())
            .collect()
    });

    let tenant = seq.tenant_id.as_str();
    let mut findings = Vec::new();
    let mut external: BTreeSet<&str> = BTreeSet::new();

    for (_, handler) in &refs.steps {
        if BUILTIN_HANDLER_NAMES.contains(&handler.as_str()) {
            continue;
        }
        if let Some(plugins) = &plugin_names
            && plugins.contains(handler.as_str())
        {
            continue; // served in-process by an enabled plugin
        }
        external.insert(handler.as_str());
    }

    for handler in external {
        let matching: Vec<&WorkerRegistration> = registrations
            .iter()
            .filter(|r| {
                r.handler_name == handler
                    && r.tenant_id
                        .as_deref()
                        .is_none_or(|t| t.is_empty() || t == tenant)
            })
            .collect();

        if matching.is_empty() {
            let blocks: Vec<&str> = refs
                .steps
                .iter()
                .filter(|(_, h)| h == handler)
                .map(|(b, _)| b.as_str())
                .collect();
            findings.push(
                Finding::new(
                    "NO_COMPATIBLE_WORKER",
                    FindingSeverity::Error,
                    format!(
                        "handler '{handler}' (blocks: {}) has no live worker, is not a \
                         built-in, and matches no enabled plugin",
                        blocks.join(", ")
                    ),
                    Confidence::High,
                    now,
                )
                .with_resource(ResourceRef::new("handler", handler))
                .with_remediation(Remediation::new(
                    "start a worker that registers this handler, or fix the handler name",
                )),
            );
            continue;
        }

        // A pin narrows which of the live workers count.
        let pin = inventory.version_pins.as_ref().and_then(|pins| {
            pins.iter()
                .find(|p| p.handler_name == handler && p.tenant_id == tenant)
        });
        if let Some(pin) = pin {
            let satisfied = matching
                .iter()
                .any(|r| version_satisfies(r.version.as_deref(), &pin.min_version));
            if !satisfied {
                let seen: Vec<String> = matching
                    .iter()
                    .map(|r| {
                        format!(
                            "{}@{}",
                            r.worker_id,
                            r.version.as_deref().unwrap_or("unversioned")
                        )
                    })
                    .collect();
                findings.push(
                    Finding::new(
                        "WORKER_BELOW_VERSION_PIN",
                        FindingSeverity::Error,
                        format!(
                            "handler '{handler}' is pinned to >= {} but no live worker \
                             satisfies it",
                            pin.min_version
                        ),
                        Confidence::High,
                        now,
                    )
                    .with_resource(ResourceRef::new("handler", handler))
                    .with_evidence(
                        Evidence::new("live_workers", format!("live: {}", seen.join(", ")))
                            .observed_at(now),
                    )
                    .with_remediation(Remediation::new(
                        "upgrade the worker or relax the version pin",
                    )),
                );
            }
        }
    }

    if findings.is_empty() {
        PreflightCheck::pass(
            "handlers_have_workers",
            "every handler is a built-in, an enabled plugin, or has a live compatible worker",
        )
    } else {
        PreflightCheck::with_status(
            "handlers_have_workers",
            PreflightStatus::Fail,
            format!("{} handler(s) cannot be served", findings.len()),
            findings,
        )
    }
}

/// Handlers that name a *disabled* plugin fail fast at runtime — surface
/// that here.
fn check_plugins(
    refs: &SequenceRefs,
    inventory: &RuntimeInventory,
    now: DateTime<Utc>,
) -> PreflightCheck {
    let Some(plugins) = &inventory.plugins else {
        return unknown("plugins_enabled", "plugin");
    };
    let mut findings = Vec::new();
    for (block, handler) in &refs.steps {
        if let Some(plugin) = plugins.iter().find(|p| &p.name == handler)
            && !plugin.enabled
        {
            findings.push(
                Finding::new(
                    "PLUGIN_DISABLED",
                    FindingSeverity::Error,
                    format!("block '{block}' uses plugin '{handler}', which is disabled"),
                    Confidence::Certain,
                    now,
                )
                .with_resource(ResourceRef::new("plugin", handler))
                .with_remediation(Remediation::new("enable the plugin or change the handler")),
            );
        }
    }
    if findings.is_empty() {
        PreflightCheck::pass("plugins_enabled", "no disabled plugins referenced")
    } else {
        PreflightCheck::with_status(
            "plugins_enabled",
            PreflightStatus::Fail,
            format!("{} disabled plugin reference(s)", findings.len()),
            findings,
        )
    }
}

fn check_credentials(
    refs: &SequenceRefs,
    inventory: &RuntimeInventory,
    now: DateTime<Utc>,
) -> PreflightCheck {
    if refs.credential_ids.is_empty() {
        return PreflightCheck::pass("credentials_present", "no credential references");
    }
    let Some(credentials) = &inventory.credentials else {
        return unknown("credentials_present", "credential");
    };

    let mut findings = Vec::new();
    for id in &refs.credential_ids {
        match credentials.iter().find(|c| &c.id == id) {
            None => findings.push(
                Finding::new(
                    "CREDENTIAL_MISSING",
                    FindingSeverity::Error,
                    format!("credential '{id}' is referenced but does not exist"),
                    Confidence::Certain,
                    now,
                )
                .with_resource(ResourceRef::new("credential", id.clone()))
                .with_remediation(
                    Remediation::new("create the credential")
                        .with_command(format!("orch8 credential create {id} ...")),
                ),
            ),
            Some(c) if !c.enabled => findings.push(
                Finding::new(
                    "CREDENTIAL_DISABLED",
                    FindingSeverity::Error,
                    format!("credential '{id}' exists but is disabled"),
                    Confidence::Certain,
                    now,
                )
                .with_resource(ResourceRef::new("credential", id.clone())),
            ),
            Some(c) => {
                if let Some(expires) = c.expires_at
                    && expires <= now
                {
                    findings.push(
                        Finding::new(
                            "CREDENTIAL_EXPIRED",
                            FindingSeverity::Error,
                            format!("credential '{id}' expired at {expires}"),
                            Confidence::Certain,
                            now,
                        )
                        .with_resource(ResourceRef::new("credential", id.clone()))
                        .with_remediation(Remediation::new("rotate or refresh the credential")),
                    );
                }
            }
        }
    }

    if findings.is_empty() {
        PreflightCheck::pass(
            "credentials_present",
            format!(
                "all {} referenced credential(s) exist, are enabled, and are unexpired",
                refs.credential_ids.len()
            ),
        )
    } else {
        PreflightCheck::with_status(
            "credentials_present",
            PreflightStatus::Fail,
            format!("{} credential problem(s)", findings.len()),
            findings,
        )
    }
}

/// Steps that target an explicit queue need *someone* consuming that
/// queue: a live worker registered on it, or a push dispatch config.
/// Routing rules may redirect, so problems here are warnings, not
/// failures.
fn check_queues(
    refs: &SequenceRefs,
    inventory: &RuntimeInventory,
    now: DateTime<Utc>,
) -> PreflightCheck {
    if refs.explicit_queues.is_empty() {
        return PreflightCheck::pass("queues_consumable", "no explicit queue targets");
    }
    let (Some(registrations), Some(dispatch)) =
        (&inventory.worker_registrations, &inventory.queue_dispatch)
    else {
        return unknown("queues_consumable", "queue consumer");
    };

    let redirected: BTreeSet<&str> = inventory
        .routing_rules
        .as_ref()
        .map(|rules| {
            rules
                .iter()
                .filter(|r| r.enabled)
                .filter_map(|r| r.match_queue.as_deref())
                .collect()
        })
        .unwrap_or_default();

    let mut findings = Vec::new();
    for queue in &refs.explicit_queues {
        let has_worker = registrations
            .iter()
            .any(|r| r.queue_name.as_deref() == Some(queue.as_str()));
        let has_push = dispatch
            .iter()
            .any(|d| d.queue_name == *queue && d.mode == DispatchMode::Push);
        let is_redirected = redirected.contains(queue.as_str());
        if !has_worker && !has_push && !is_redirected {
            findings.push(
                Finding::new(
                    "QUEUE_HAS_NO_CONSUMER",
                    FindingSeverity::Warning,
                    format!(
                        "queue '{queue}' has no live polling worker, no push dispatch \
                         config, and no routing rule redirecting it"
                    ),
                    Confidence::Medium,
                    now,
                )
                .with_resource(ResourceRef::new("queue", queue.clone())),
            );
        }
    }

    if findings.is_empty() {
        PreflightCheck::pass("queues_consumable", "every explicit queue has a consumer")
    } else {
        PreflightCheck::with_status(
            "queues_consumable",
            PreflightStatus::Warning,
            format!("{} queue(s) may have no consumer", findings.len()),
            findings,
        )
    }
}

fn check_sub_sequences(
    seq: &SequenceDefinition,
    refs: &SequenceRefs,
    inventory: &RuntimeInventory,
    now: DateTime<Utc>,
) -> PreflightCheck {
    if refs.sub_sequences.is_empty() {
        return PreflightCheck::pass("sub_sequences_available", "no sub-sequence references");
    }
    let Some(sequences) = &inventory.sequences else {
        return unknown("sub_sequences_available", "sequence");
    };

    let mut findings = Vec::new();
    for (block, name, version) in &refs.sub_sequences {
        let candidates: Vec<&SubSequenceInfo> = sequences
            .iter()
            .filter(|s| {
                &s.name == name
                    && s.namespace == seq.namespace.as_str()
                    && version.is_none_or(|v| s.version == v)
            })
            .collect();

        if candidates.is_empty() {
            findings.push(
                Finding::new(
                    "SUB_SEQUENCE_MISSING",
                    FindingSeverity::Error,
                    match version {
                        Some(v) => format!(
                            "block '{block}' references sub-sequence '{name}' v{v}, which \
                             does not exist in namespace '{}'",
                            seq.namespace.as_str()
                        ),
                        None => format!(
                            "block '{block}' references sub-sequence '{name}', which does \
                             not exist in namespace '{}'",
                            seq.namespace.as_str()
                        ),
                    },
                    Confidence::Certain,
                    now,
                )
                .with_resource(ResourceRef::new("sequence", name.clone())),
            );
            continue;
        }
        // A resolvable candidate must be usable: unpublished-only is fatal,
        // draft-only is a warning.
        let usable = candidates.iter().any(|s| {
            matches!(
                s.status,
                SequenceStatus::Staging | SequenceStatus::Production
            )
        });
        let all_unpublished = candidates
            .iter()
            .all(|s| matches!(s.status, SequenceStatus::Unpublished));
        if all_unpublished {
            findings.push(
                Finding::new(
                    "SUB_SEQUENCE_UNPUBLISHED",
                    FindingSeverity::Error,
                    format!(
                        "block '{block}' references sub-sequence '{name}', but every \
                             matching version is unpublished"
                    ),
                    Confidence::Certain,
                    now,
                )
                .with_resource(ResourceRef::new("sequence", name.clone())),
            );
        } else if !usable {
            findings.push(
                Finding::new(
                    "SUB_SEQUENCE_DRAFT_ONLY",
                    FindingSeverity::Warning,
                    format!(
                        "block '{block}' references sub-sequence '{name}', which only \
                             exists as a draft"
                    ),
                    Confidence::Certain,
                    now,
                )
                .with_resource(ResourceRef::new("sequence", name.clone())),
            );
        }
    }

    if findings.is_empty() {
        PreflightCheck::pass(
            "sub_sequences_available",
            "all referenced sub-sequences exist and are published",
        )
    } else {
        let worst = if findings
            .iter()
            .any(|f| f.severity >= FindingSeverity::Error)
        {
            PreflightStatus::Fail
        } else {
            PreflightStatus::Warning
        };
        PreflightCheck::with_status(
            "sub_sequences_available",
            worst,
            format!("{} sub-sequence problem(s)", findings.len()),
            findings,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 11, 0, 0, 0).unwrap()
    }

    #[allow(clippy::needless_pass_by_value)]
    fn seq(blocks: Value) -> SequenceDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::now_v7(),
            "tenant_id": "t1",
            "namespace": "default",
            "name": "preflight-seq",
            "version": 1,
            "blocks": blocks,
            "created_at": "2026-01-01T00:00:00Z"
        }))
        .expect("valid sequence")
    }

    fn full_inventory() -> RuntimeInventory {
        RuntimeInventory {
            worker_registrations: Some(vec![]),
            version_pins: Some(vec![]),
            credentials: Some(vec![]),
            plugins: Some(vec![]),
            queue_dispatch: Some(vec![]),
            routing_rules: Some(vec![]),
            sequences: Some(vec![]),
        }
    }

    fn registration(
        handler: &str,
        queue: Option<&str>,
        version: Option<&str>,
    ) -> WorkerRegistration {
        WorkerRegistration {
            worker_id: format!("w-{handler}"),
            handler_name: handler.to_string(),
            queue_name: queue.map(ToString::to_string),
            version: version.map(ToString::to_string),
            tenant_id: None,
            last_seen_at: t0(),
        }
    }

    fn check<'a>(report: &'a PreflightReport, id: &str) -> &'a PreflightCheck {
        report
            .checks
            .iter()
            .find(|c| c.id == id)
            .unwrap_or_else(|| panic!("missing check {id}"))
    }

    // --- reference collection ---

    #[test]
    fn collect_refs_finds_steps_queues_credentials_and_subsequences() {
        let s = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {"key": "credentials://stripe/api_key"}, "queue_name": "billing"},
            {"type": "parallel", "id": "p", "branches": [
                [{"type": "step", "id": "b", "handler": "custom", "params": {"c": "credentials://slack"}, "rate_limit_key": "mailbox:x"}]
            ]},
            {"type": "sub_sequence", "id": "child", "sequence_name": "refund-flow", "version": 3, "input": {}}
        ]));
        let refs = collect_refs(&s);
        assert_eq!(refs.steps.len(), 2);
        assert!(refs.explicit_queues.contains("billing"));
        assert_eq!(
            refs.credential_ids,
            ["slack", "stripe"]
                .iter()
                .map(ToString::to_string)
                .collect()
        );
        assert_eq!(
            refs.sub_sequences,
            vec![("child".into(), "refund-flow".into(), Some(3))]
        );
        assert!(refs.rate_limit_keys.contains("mailbox:x"));
    }

    #[test]
    fn collect_refs_strips_credential_field_suffix() {
        let s = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {"k": "credentials://vault-key/token"}}
        ]));
        let refs = collect_refs(&s);
        assert!(refs.credential_ids.contains("vault-key"));
        assert!(!refs.credential_ids.contains("vault-key/token"));
    }

    // --- unknown propagation ---

    #[test]
    fn missing_inventory_sections_yield_unknown_not_pass() {
        let s = seq(json!([
            {"type": "step", "id": "a", "handler": "external_x", "params": {"k": "credentials://c1"}, "queue_name": "q"}
        ]));
        let report = run_preflight(&s, &RuntimeInventory::default(), t0());
        assert_eq!(
            check(&report, "handlers_have_workers").status,
            PreflightStatus::Unknown
        );
        assert_eq!(
            check(&report, "credentials_present").status,
            PreflightStatus::Unknown
        );
        assert_eq!(
            check(&report, "queues_consumable").status,
            PreflightStatus::Unknown
        );
        assert_eq!(
            check(&report, "plugins_enabled").status,
            PreflightStatus::Unknown
        );
        assert_eq!(report.overall, PreflightStatus::Unknown);
        assert!(!report.is_ready());
    }

    #[test]
    fn builtin_only_sequence_passes_on_empty_inventory() {
        let s = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}},
            {"type": "step", "id": "b", "handler": "transform", "params": {}}
        ]));
        let report = run_preflight(&s, &full_inventory(), t0());
        assert_eq!(report.overall, PreflightStatus::Pass, "{report:?}");
        assert!(report.is_ready());
    }

    // --- handler checks ---

    #[test]
    fn external_handler_without_worker_fails() {
        let s = seq(json!([
            {"type": "step", "id": "charge", "handler": "charge_card", "params": {}}
        ]));
        let report = run_preflight(&s, &full_inventory(), t0());
        let c = check(&report, "handlers_have_workers");
        assert_eq!(c.status, PreflightStatus::Fail);
        assert_eq!(c.findings[0].code, "NO_COMPATIBLE_WORKER");
        assert!(c.findings[0].summary.contains("charge_card"));
        assert!(c.findings[0].summary.contains("charge"));
    }

    #[test]
    fn external_handler_with_live_worker_passes() {
        let s = seq(json!([
            {"type": "step", "id": "charge", "handler": "charge_card", "params": {}}
        ]));
        let mut inv = full_inventory();
        inv.worker_registrations = Some(vec![registration("charge_card", None, Some("1.0.0"))]);
        let report = run_preflight(&s, &inv, t0());
        assert_eq!(
            check(&report, "handlers_have_workers").status,
            PreflightStatus::Pass
        );
    }

    #[test]
    fn worker_below_version_pin_fails_with_evidence() {
        let s = seq(json!([
            {"type": "step", "id": "charge", "handler": "charge_card", "params": {}}
        ]));
        let mut inv = full_inventory();
        inv.worker_registrations = Some(vec![registration("charge_card", None, Some("1.1.0"))]);
        inv.version_pins = Some(vec![WorkerVersionPin {
            tenant_id: "t1".into(),
            handler_name: "charge_card".into(),
            min_version: "1.2.0".into(),
            created_at: t0(),
            updated_at: t0(),
        }]);
        let report = run_preflight(&s, &inv, t0());
        let c = check(&report, "handlers_have_workers");
        assert_eq!(c.status, PreflightStatus::Fail);
        assert_eq!(c.findings[0].code, "WORKER_BELOW_VERSION_PIN");
        assert!(c.findings[0].evidence[0].summary.contains("1.1.0"));
    }

    #[test]
    fn version_pin_for_other_tenant_is_ignored() {
        let s = seq(json!([
            {"type": "step", "id": "charge", "handler": "charge_card", "params": {}}
        ]));
        let mut inv = full_inventory();
        inv.worker_registrations = Some(vec![registration("charge_card", None, Some("1.1.0"))]);
        inv.version_pins = Some(vec![WorkerVersionPin {
            tenant_id: "other-tenant".into(),
            handler_name: "charge_card".into(),
            min_version: "9.9.9".into(),
            created_at: t0(),
            updated_at: t0(),
        }]);
        let report = run_preflight(&s, &inv, t0());
        assert_eq!(
            check(&report, "handlers_have_workers").status,
            PreflightStatus::Pass
        );
    }

    #[test]
    fn worker_scoped_to_other_tenant_does_not_count() {
        let s = seq(json!([
            {"type": "step", "id": "charge", "handler": "charge_card", "params": {}}
        ]));
        let mut reg = registration("charge_card", None, None);
        reg.tenant_id = Some("someone-else".into());
        let mut inv = full_inventory();
        inv.worker_registrations = Some(vec![reg]);
        let report = run_preflight(&s, &inv, t0());
        assert_eq!(
            check(&report, "handlers_have_workers").status,
            PreflightStatus::Fail
        );
    }

    #[test]
    fn enabled_plugin_covers_handler_without_worker() {
        let s = seq(json!([
            {"type": "step", "id": "x", "handler": "sentiment", "params": {}}
        ]));
        let mut inv = full_inventory();
        inv.plugins = Some(vec![PluginInfo {
            name: "sentiment".into(),
            enabled: true,
        }]);
        let report = run_preflight(&s, &inv, t0());
        assert_eq!(
            check(&report, "handlers_have_workers").status,
            PreflightStatus::Pass
        );
        assert_eq!(
            check(&report, "plugins_enabled").status,
            PreflightStatus::Pass
        );
    }

    #[test]
    fn disabled_plugin_fails_both_checks() {
        let s = seq(json!([
            {"type": "step", "id": "x", "handler": "sentiment", "params": {}}
        ]));
        let mut inv = full_inventory();
        inv.plugins = Some(vec![PluginInfo {
            name: "sentiment".into(),
            enabled: false,
        }]);
        let report = run_preflight(&s, &inv, t0());
        // Disabled plugin doesn't serve the handler...
        assert_eq!(
            check(&report, "handlers_have_workers").status,
            PreflightStatus::Fail
        );
        // ...and is explicitly reported as disabled.
        let c = check(&report, "plugins_enabled");
        assert_eq!(c.status, PreflightStatus::Fail);
        assert_eq!(c.findings[0].code, "PLUGIN_DISABLED");
    }

    // --- credentials ---

    #[test]
    fn missing_disabled_and_expired_credentials_fail() {
        let s = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {
                "k1": "credentials://absent",
                "k2": "credentials://disabled",
                "k3": "credentials://expired"
            }}
        ]));
        let mut inv = full_inventory();
        inv.credentials = Some(vec![
            CredentialInfo {
                id: "disabled".into(),
                enabled: false,
                expires_at: None,
            },
            CredentialInfo {
                id: "expired".into(),
                enabled: true,
                expires_at: Some(t0() - chrono::Duration::hours(1)),
            },
        ]);
        let report = run_preflight(&s, &inv, t0());
        let c = check(&report, "credentials_present");
        assert_eq!(c.status, PreflightStatus::Fail);
        let codes: Vec<&str> = c.findings.iter().map(|f| f.code.as_str()).collect();
        assert!(codes.contains(&"CREDENTIAL_MISSING"));
        assert!(codes.contains(&"CREDENTIAL_DISABLED"));
        assert!(codes.contains(&"CREDENTIAL_EXPIRED"));
    }

    #[test]
    fn healthy_credentials_pass() {
        let s = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {"k": "credentials://ok"}}
        ]));
        let mut inv = full_inventory();
        inv.credentials = Some(vec![CredentialInfo {
            id: "ok".into(),
            enabled: true,
            expires_at: Some(t0() + chrono::Duration::days(30)),
        }]);
        let report = run_preflight(&s, &inv, t0());
        assert_eq!(
            check(&report, "credentials_present").status,
            PreflightStatus::Pass
        );
    }

    // --- queues ---

    #[test]
    fn explicit_queue_without_consumer_warns() {
        let s = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}, "queue_name": "orphan"}
        ]));
        let report = run_preflight(&s, &full_inventory(), t0());
        let c = check(&report, "queues_consumable");
        assert_eq!(c.status, PreflightStatus::Warning);
        assert_eq!(c.findings[0].code, "QUEUE_HAS_NO_CONSUMER");
        // Warnings keep the sequence "ready" (it may still be intentional).
        assert!(report.is_ready());
    }

    #[test]
    fn queue_with_polling_worker_passes() {
        let s = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}, "queue_name": "billing"}
        ]));
        let mut inv = full_inventory();
        inv.worker_registrations = Some(vec![registration("whatever", Some("billing"), None)]);
        let report = run_preflight(&s, &inv, t0());
        assert_eq!(
            check(&report, "queues_consumable").status,
            PreflightStatus::Pass
        );
    }

    #[test]
    fn queue_with_push_dispatch_passes() {
        let s = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}, "queue_name": "push-q"}
        ]));
        let mut inv = full_inventory();
        inv.queue_dispatch = Some(vec![
            serde_json::from_value(json!({
                "tenant_id": "t1",
                "queue_name": "push-q",
                "mode": "push",
                "push_url": "https://worker.example.com/tasks",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z"
            }))
            .unwrap(),
        ]);
        let report = run_preflight(&s, &inv, t0());
        assert_eq!(
            check(&report, "queues_consumable").status,
            PreflightStatus::Pass
        );
    }

    // --- sub-sequences ---

    #[test]
    fn missing_sub_sequence_fails() {
        let s = seq(json!([
            {"type": "sub_sequence", "id": "child", "sequence_name": "ghost", "input": {}}
        ]));
        let report = run_preflight(&s, &full_inventory(), t0());
        let c = check(&report, "sub_sequences_available");
        assert_eq!(c.status, PreflightStatus::Fail);
        assert_eq!(c.findings[0].code, "SUB_SEQUENCE_MISSING");
    }

    #[test]
    fn unpublished_sub_sequence_fails_draft_warns() {
        let s = seq(json!([
            {"type": "sub_sequence", "id": "c1", "sequence_name": "unpub", "input": {}},
            {"type": "sub_sequence", "id": "c2", "sequence_name": "draft", "input": {}}
        ]));
        let mut inv = full_inventory();
        inv.sequences = Some(vec![
            SubSequenceInfo {
                name: "unpub".into(),
                namespace: "default".into(),
                version: 1,
                status: SequenceStatus::Unpublished,
            },
            SubSequenceInfo {
                name: "draft".into(),
                namespace: "default".into(),
                version: 1,
                status: SequenceStatus::Draft,
            },
        ]);
        let report = run_preflight(&s, &inv, t0());
        let c = check(&report, "sub_sequences_available");
        assert_eq!(c.status, PreflightStatus::Fail);
        let codes: Vec<&str> = c.findings.iter().map(|f| f.code.as_str()).collect();
        assert!(codes.contains(&"SUB_SEQUENCE_UNPUBLISHED"));
        assert!(codes.contains(&"SUB_SEQUENCE_DRAFT_ONLY"));
    }

    #[test]
    fn production_sub_sequence_passes_and_respects_version_filter() {
        let s = seq(json!([
            {"type": "sub_sequence", "id": "c", "sequence_name": "refund", "version": 2, "input": {}}
        ]));
        let mut inv = full_inventory();
        inv.sequences = Some(vec![SubSequenceInfo {
            name: "refund".into(),
            namespace: "default".into(),
            version: 2,
            status: SequenceStatus::Production,
        }]);
        let report = run_preflight(&s, &inv, t0());
        assert_eq!(
            check(&report, "sub_sequences_available").status,
            PreflightStatus::Pass
        );

        // Asking for a version that doesn't exist fails.
        let s3 = seq(json!([
            {"type": "sub_sequence", "id": "c", "sequence_name": "refund", "version": 3, "input": {}}
        ]));
        let report3 = run_preflight(&s3, &inv, t0());
        assert_eq!(
            check(&report3, "sub_sequences_available").status,
            PreflightStatus::Fail
        );
    }

    #[test]
    fn sub_sequence_in_other_namespace_does_not_count() {
        let s = seq(json!([
            {"type": "sub_sequence", "id": "c", "sequence_name": "refund", "input": {}}
        ]));
        let mut inv = full_inventory();
        inv.sequences = Some(vec![SubSequenceInfo {
            name: "refund".into(),
            namespace: "other-ns".into(),
            version: 1,
            status: SequenceStatus::Production,
        }]);
        let report = run_preflight(&s, &inv, t0());
        assert_eq!(
            check(&report, "sub_sequences_available").status,
            PreflightStatus::Fail
        );
    }

    // --- schema / definition ---

    #[test]
    fn non_object_input_schema_fails() {
        let mut s = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}}
        ]));
        s.input_schema = Some(json!("not a schema"));
        let report = run_preflight(&s, &full_inventory(), t0());
        let c = check(&report, "input_schema_valid");
        assert_eq!(c.status, PreflightStatus::Fail);
        assert!(c.findings[0].summary.contains("string"));
    }

    #[test]
    fn report_overall_reflects_worst_check() {
        // A failing handler check makes the whole report Fail even with
        // everything else passing.
        let s = seq(json!([
            {"type": "step", "id": "x", "handler": "nonexistent_handler", "params": {}}
        ]));
        let report = run_preflight(&s, &full_inventory(), t0());
        assert_eq!(report.overall, PreflightStatus::Fail);
        assert!(!report.is_ready());
    }
}
