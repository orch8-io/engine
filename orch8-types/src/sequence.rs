use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use utoipa::ToSchema;

use crate::ids::{BlockId, Namespace, SequenceId, TenantId};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SequenceDefinition {
    pub id: SequenceId,
    pub tenant_id: TenantId,
    pub namespace: Namespace,
    pub name: String,
    pub version: i32,
    /// If true, this version is deprecated. New instances should use a newer version.
    /// Running instances bound to this version continue unaffected.
    #[serde(default)]
    pub deprecated: bool,
    pub blocks: Vec<BlockDefinition>,
    /// Lifecycle interceptors (before/after step, on-signal, on-complete, on-failure).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interceptors: Option<crate::interceptor::InterceptorDef>,
    pub created_at: DateTime<Utc>,
}

/// A block is either a leaf (step) or a composite (parallel, race, etc.).
///
/// This recursive enum IS the workflow DSL.
/// Each variant wraps its definition in `Box<T>` so the enum itself stays a
/// single word. `StepDef` is large (14 fields with many `Option<...>`), and
/// without boxing every `BlockDefinition` — even `SubSequence`, which is small
/// — paid the full size. `Box<T>` is transparent to both `serde` (the default
/// impl delegates to the inner type so wire format is unchanged) and `utoipa`
/// (which forwards `ToSchema` through `Box<T>`), so this is an internal
/// representation change only.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(no_recursion)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BlockDefinition {
    Step(Box<StepDef>),
    Parallel(Box<ParallelDef>),
    Race(Box<RaceDef>),
    Loop(Box<LoopDef>),
    ForEach(Box<ForEachDef>),
    Router(Box<RouterDef>),
    TryCatch(Box<TryCatchDef>),
    /// Invoke another sequence as a sub-workflow.
    SubSequence(Box<SubSequenceDef>),
    /// A/B split: route traffic to one of several variants by weight.
    ABSplit(Box<ABSplitDef>),
    /// Cancellation scope: child blocks cannot be cancelled by external cancel signals.
    /// Provides subtree-level non-cancellability (Temporal-style structured concurrency).
    CancellationScope(Box<CancellationScopeDef>),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StepDef {
    pub id: BlockId,
    pub handler: String,
    #[serde(default)]
    pub params: serde_json::Value,
    pub delay: Option<DelaySpec>,
    pub retry: Option<RetryPolicy>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_duration_opt"
    )]
    #[schema(value_type = Option<u64>)]
    pub timeout: Option<Duration>,
    /// If set, this step consumes a rate limit token for the given resource key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_key: Option<String>,
    /// If set, only execute during the specified time window (per instance timezone).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_window: Option<SendWindow>,
    /// Restrict which context sections this step can access. If omitted, all sections visible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_access: Option<ContextAccess>,
    /// If false, this step will not be cancelled when the instance receives a cancel signal.
    /// Used for cleanup/finalization steps that must complete.
    #[serde(default = "default_true_seq")]
    pub cancellable: bool,
    /// If set, this step pauses execution and waits for human input via a signal.
    /// The signal name is `human_input:{block_id}`. Contains optional timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_for_input: Option<HumanInputDef>,
    /// Named task queue for routing to dedicated worker pools.
    /// If omitted, uses the default queue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_name: Option<String>,
    /// SLA deadline: maximum wall-clock time from when this step starts running.
    /// If breached, the escalation handler is invoked and the step is failed.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_duration_opt"
    )]
    #[schema(value_type = Option<u64>)]
    pub deadline: Option<Duration>,
    /// Handler to invoke when the SLA deadline is breached.
    /// If omitted but deadline is set, the step simply fails on breach.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_deadline_breach: Option<EscalationDef>,
    /// Fallback handler to invoke when the primary handler's circuit breaker
    /// is `Open`. When set, step dispatch re-targets to this handler instead
    /// of deferring the instance for the cooldown window; when unset, the
    /// legacy behaviour applies (defer to `now + remaining_cooldown_secs`).
    /// Uses the same params + context as the primary handler; its own
    /// failures are tracked under its own breaker key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_handler: Option<String>,
    /// If set, cache step output under this key in the instance KV state.
    /// On subsequent executions, if a cached value exists for the resolved key,
    /// the handler is skipped and the cached value is returned directly.
    /// The key is template-resolved before lookup (e.g. `"rate_{{ data.currency }}"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DelaySpec {
    #[serde(with = "crate::serde_duration")]
    #[schema(value_type = u64)]
    pub duration: Duration,
    #[serde(default)]
    pub business_days_only: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_duration_opt"
    )]
    #[schema(value_type = Option<u64>)]
    pub jitter: Option<Duration>,
    /// Holiday dates (YYYY-MM-DD) to skip when `business_days_only` is true.
    /// Merged with `context.config.holidays` at runtime for tenant-level calendars.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub holidays: Vec<String>,
    /// Fire at a specific local wall-clock time (ISO 8601 `NaiveDateTime`,
    /// e.g. `"2026-03-08T02:30:00"`). When set, `duration` is ignored and
    /// the engine converts this local time to UTC using the step-level
    /// `timezone` (or the instance timezone as fallback). DST transitions
    /// are handled by rolling forward to the next valid local time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fire_at_local: Option<String>,
    /// Timezone for `fire_at_local` (IANA, e.g. `"America/New_York"`).
    /// Falls back to the instance's timezone if omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
}

/// Time window during which a step is allowed to execute.
/// Hours are in 24h format relative to the instance's timezone.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SendWindow {
    /// Start hour (0-23). Defaults to 9.
    #[serde(default = "default_window_start")]
    pub start_hour: u8,
    /// End hour (0-23, exclusive). Defaults to 17.
    #[serde(default = "default_window_end")]
    pub end_hour: u8,
    /// Days of week allowed (0=Mon .. 6=Sun). Empty means all days.
    #[serde(default)]
    pub days: Vec<u8>,
}

const fn default_window_start() -> u8 {
    9
}

const fn default_window_end() -> u8 {
    17
}

/// Controls which context sections a step handler can see.
/// When set, only the listed sections are passed to the handler.
///
/// `data` supports field-level granularity via [`FieldAccess`]; other sections
/// are all-or-nothing because they are small by design (`config`, `runtime`)
/// or append-only streams (`audit`).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ContextAccess {
    /// Controls read access to `context.data`. Accepts legacy `true`/`false`,
    /// the keywords `"all"`/`"none"`, or an explicit `{"fields": [..]}` list.
    #[serde(default)]
    #[schema(value_type = serde_json::Value)]
    pub data: FieldAccess,
    /// Allow reading `context.config`.
    #[serde(default = "default_true_seq")]
    pub config: bool,
    /// Allow reading `context.audit`.
    #[serde(default)]
    pub audit: bool,
    /// Allow reading `context.runtime`.
    #[serde(default)]
    pub runtime: bool,
}

const fn default_true_seq() -> bool {
    true
}

/// Field-level access control for a context section.
///
/// Backward-compatible with the legacy boolean form: pre-M3 sequence
/// definitions wrote `{"data": true}` and are still accepted. New sequences
/// can opt into selective fetch with `{"data": {"fields": ["user_id"]}}`,
/// which lets the scheduler preload only the required fields and skip the
/// rest when hydrating externalized context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FieldAccess {
    /// Legacy boolean: `true` = all fields, `false` = no fields.
    Bool(bool),
    /// Explicit field list: only the listed top-level keys of `context.data`
    /// are visible to the handler.
    Fields { fields: Vec<String> },
    /// String keyword: `"all"` or `"none"`.
    Keyword(AccessKeyword),
}

/// String form of [`FieldAccess`] for human-authored YAML/JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AccessKeyword {
    All,
    None,
}

impl Default for FieldAccess {
    fn default() -> Self {
        // Matches the old `default_true_seq` behavior for ContextAccess.data.
        Self::Bool(true)
    }
}

impl FieldAccess {
    /// Canonical "grant everything" value.
    pub const ALL: Self = Self::Bool(true);
    /// Canonical "grant nothing" value.
    pub const NONE: Self = Self::Bool(false);

    /// Return `true` if access to `key` is permitted.
    #[must_use]
    pub fn allows(&self, key: &str) -> bool {
        match self {
            Self::Bool(b) => *b,
            Self::Keyword(AccessKeyword::All) => true,
            Self::Keyword(AccessKeyword::None) => false,
            Self::Fields { fields } => fields.iter().any(|f| f == key),
        }
    }

    /// Return `true` if _any_ field is permitted. Used by code paths that
    /// want to skip work entirely when the handler cannot read the section.
    #[must_use]
    pub const fn allows_any(&self) -> bool {
        match self {
            Self::Bool(b) => *b,
            Self::Keyword(AccessKeyword::All) => true,
            Self::Keyword(AccessKeyword::None) => false,
            Self::Fields { fields } => !fields.is_empty(),
        }
    }

    /// Return the explicit field list when this is a `Fields` variant; `None`
    /// for `All`/`None` (caller must fall back to full fetch or skip).
    #[must_use]
    pub const fn required_fields(&self) -> Option<&[String]> {
        match self {
            Self::Fields { fields } => Some(fields.as_slice()),
            _ => None,
        }
    }
}

/// One option presented to the human reviewer in advanced mode.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct HumanChoice {
    /// Display text shown in the UI.
    pub label: String,
    /// Stable identifier stored in context and used for router matching.
    pub value: String,
}

/// Configuration for human-in-the-loop steps.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HumanInputDef {
    /// Prompt or instructions for the human reviewer.
    #[serde(default)]
    pub prompt: String,
    /// Timeout in seconds before the step fails or escalates.
    /// If omitted, waits indefinitely.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_duration_opt"
    )]
    #[schema(value_type = Option<u64>)]
    pub timeout: Option<Duration>,
    /// If set and timeout expires, send a signal to this escalation target
    /// instead of failing the step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalation_handler: Option<String>,
    /// Choices the human can pick from. If `None`, the engine applies the
    /// default yes/no preset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub choices: Option<Vec<HumanChoice>>,
    /// Context-variable name under which the picked value is stored
    /// (`context.data[store_as]`). If `None`, the block id is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store_as: Option<String>,
}

impl HumanInputDef {
    /// Structural validation for a single `human_review` definition.
    ///
    /// Rules:
    /// - If `choices` is `Some`, the vector must be non-empty.
    /// - If `choices` is `Some`, every `HumanChoice::value` must be unique
    ///   (router targets rely on a stable 1:1 value → branch mapping).
    /// - If `store_as` is `Some`, the string must be non-empty — an empty
    ///   key would collide with the block-id fallback and is almost
    ///   certainly a client mistake.
    pub fn validate(&self) -> Result<(), String> {
        if let Some(choices) = &self.choices {
            if choices.is_empty() {
                return Err("human_review: `choices` must be non-empty when provided".into());
            }
            let mut seen = std::collections::HashSet::new();
            for c in choices {
                if !seen.insert(c.value.as_str()) {
                    return Err(format!(
                        "human_review: duplicate choice value `{}`",
                        c.value
                    ));
                }
            }
        }
        if let Some(s) = &self.store_as {
            if s.is_empty() {
                return Err("human_review: `store_as` must be non-empty".into());
            }
        }
        Ok(())
    }

    /// Return the choices to present to the human. Defaults to Yes/No when
    /// `choices` is `None`.
    #[must_use]
    pub fn effective_choices(&self) -> Vec<HumanChoice> {
        match &self.choices {
            Some(c) => c.clone(),
            None => vec![
                HumanChoice {
                    label: "Yes".into(),
                    value: "yes".into(),
                },
                HumanChoice {
                    label: "No".into(),
                    value: "no".into(),
                },
            ],
        }
    }
}

/// Action to take when an SLA deadline is breached.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EscalationDef {
    /// Handler name to invoke on breach (e.g. `"notify_slack"`, `"send_alert"`).
    pub handler: String,
    /// Parameters passed to the escalation handler.
    #[serde(default)]
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    #[serde(with = "crate::serde_duration")]
    #[schema(value_type = u64)]
    pub initial_backoff: Duration,
    #[serde(with = "crate::serde_duration")]
    #[schema(value_type = u64)]
    pub max_backoff: Duration,
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
}

const fn default_backoff_multiplier() -> f64 {
    2.0
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ParallelDef {
    pub id: BlockId,
    pub branches: Vec<Vec<BlockDefinition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RaceDef {
    pub id: BlockId,
    pub branches: Vec<Vec<BlockDefinition>>,
    #[serde(default)]
    pub semantics: RaceSemantics,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RaceSemantics {
    #[default]
    FirstToResolve,
    FirstToSucceed,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TryCatchDef {
    pub id: BlockId,
    pub try_block: Vec<BlockDefinition>,
    pub catch_block: Vec<BlockDefinition>,
    #[serde(default)]
    pub finally_block: Option<Vec<BlockDefinition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LoopDef {
    pub id: BlockId,
    pub condition: String,
    pub body: Vec<BlockDefinition>,
    #[serde(default = "default_max_iterations")]
    pub max_iterations: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub break_on: Option<String>,
    #[serde(default)]
    pub continue_on_error: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poll_interval: Option<u64>,
}

const fn default_max_iterations() -> u32 {
    1000
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ForEachDef {
    pub id: BlockId,
    pub collection: String,
    #[serde(default = "default_item_var")]
    pub item_var: String,
    pub body: Vec<BlockDefinition>,
    #[serde(default = "default_max_iterations")]
    pub max_iterations: u32,
}

fn default_item_var() -> String {
    "item".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RouterDef {
    pub id: BlockId,
    pub routes: Vec<Route>,
    #[serde(default)]
    pub default: Option<Vec<BlockDefinition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Route {
    pub condition: String,
    pub blocks: Vec<BlockDefinition>,
}

/// Invoke another sequence as a child workflow.
/// The child instance is created and linked; the parent waits for completion.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubSequenceDef {
    pub id: BlockId,
    /// Name of the sequence to invoke (resolved by tenant + namespace + name).
    pub sequence_name: String,
    /// Optional specific version. If omitted, uses the latest non-deprecated version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<i32>,
    /// Input data to pass as the child instance's initial context data.
    #[serde(default)]
    pub input: serde_json::Value,
}

/// A/B split: deterministically route each instance to one of several
/// weighted variants. The chosen variant is persisted in the block output
/// so re-executions always follow the same path.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ABSplitDef {
    pub id: BlockId,
    /// Weighted variants. Weights are relative (e.g. 70 + 30 = 100%).
    pub variants: Vec<ABVariant>,
}

/// Cancellation scope: wraps child blocks in a non-cancellable boundary.
///
/// When a cancel signal is received, blocks inside a `CancellationScope`
/// continue executing until completion. The cancel takes effect only after
/// all scoped blocks finish.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CancellationScopeDef {
    pub id: BlockId,
    /// Child blocks protected from cancellation.
    pub blocks: Vec<BlockDefinition>,
}

/// One arm of an A/B split.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ABVariant {
    /// Human-readable label (e.g. "control", `"variant_a"`).
    pub name: String,
    /// Relative weight. Higher = more traffic.
    pub weight: u32,
    /// Blocks to execute when this variant is chosen.
    pub blocks: Vec<BlockDefinition>,
}

/// Validation error produced by [`SequenceDefinition::validate`].
#[derive(Debug, Clone, thiserror::Error)]
pub enum SequenceValidationError {
    /// Two or more blocks in the tree share the same `id`.
    #[error("duplicate block id: {0}")]
    DuplicateBlockId(String),
    /// A step's `wait_for_input` (`HumanInputDef`) is structurally invalid.
    /// Carries the offending block id and the underlying error message.
    #[error("invalid human_review on block `{block_id}`: {message}")]
    InvalidHumanInput { block_id: String, message: String },
}

/// Known built-in handler names shipped with the engine. Used for
/// create-time validation warnings when a sequence references an
/// unknown handler (likely a typo).
pub const BUILTIN_HANDLER_NAMES: &[&str] = &[
    "noop",
    "log",
    "sleep",
    "fail",
    "http_request",
    "llm_call",
    "tool_call",
    "human_review",
    "self_modify",
    "emit_event",
    "send_signal",
    "query_instance",
    "set_state",
    "get_state",
    "delete_state",
    "transform",
    "assert",
    "merge_state",
];

impl SequenceDefinition {
    /// Structural validation performed at submit time (before the sequence
    /// reaches storage). Currently checks that every `BlockId` in the
    /// recursive block tree is unique — the engine relies on ids being
    /// distinct keys for block outputs, execution-tree edges, and signal
    /// routing, so duplicates would break every one of those subsystems.
    pub fn validate(&self) -> Result<(), SequenceValidationError> {
        let mut seen = std::collections::HashSet::new();
        for block in &self.blocks {
            walk_block_ids(block, &mut seen)?;
        }
        Ok(())
    }

    /// Collect all handler names referenced by Step blocks in the sequence.
    pub fn handler_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for block in &self.blocks {
            collect_handler_names(block, &mut names);
        }
        names.sort();
        names.dedup();
        names
    }

    /// Check for handler names that are not in the built-in list and
    /// return suggestions using fuzzy matching.
    pub fn unknown_handler_warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        for name in self.handler_names() {
            if !BUILTIN_HANDLER_NAMES.contains(&name.as_str()) {
                let suggestion = crate::suggest::did_you_mean(&name, BUILTIN_HANDLER_NAMES);
                match suggestion {
                    Some(s) => warnings.push(format!(
                        "unknown handler \"{name}\" (did you mean \"{s}\"?)"
                    )),
                    None => warnings.push(format!(
                        "unknown handler \"{name}\" — not a built-in; ensure a custom handler is registered"
                    )),
                }
            }
        }
        warnings
    }
}

fn collect_handler_names(block: &BlockDefinition, names: &mut Vec<String>) {
    match block {
        BlockDefinition::Step(s) => {
            names.push(s.handler.clone());
        }
        BlockDefinition::Parallel(p) => {
            for branch in &p.branches {
                for b in branch {
                    collect_handler_names(b, names);
                }
            }
        }
        BlockDefinition::Race(r) => {
            for branch in &r.branches {
                for b in branch {
                    collect_handler_names(b, names);
                }
            }
        }
        BlockDefinition::Loop(l) => {
            for b in &l.body {
                collect_handler_names(b, names);
            }
        }
        BlockDefinition::ForEach(fe) => {
            for b in &fe.body {
                collect_handler_names(b, names);
            }
        }
        BlockDefinition::Router(r) => {
            for route in &r.routes {
                for b in &route.blocks {
                    collect_handler_names(b, names);
                }
            }
            if let Some(default) = &r.default {
                for b in default {
                    collect_handler_names(b, names);
                }
            }
        }
        BlockDefinition::TryCatch(tc) => {
            for b in &tc.try_block {
                collect_handler_names(b, names);
            }
            for b in &tc.catch_block {
                collect_handler_names(b, names);
            }
            if let Some(finally) = &tc.finally_block {
                for b in finally {
                    collect_handler_names(b, names);
                }
            }
        }
        BlockDefinition::SubSequence(_) => {}
        BlockDefinition::ABSplit(ab) => {
            for variant in &ab.variants {
                for b in &variant.blocks {
                    collect_handler_names(b, names);
                }
            }
        }
        BlockDefinition::CancellationScope(cs) => {
            for b in &cs.blocks {
                collect_handler_names(b, names);
            }
        }
    }
}

/// Recursively descend into a block and every composite child body,
/// inserting each encountered `BlockId` into `seen`. The first duplicate
/// encountered short-circuits with [`SequenceValidationError::DuplicateBlockId`].
fn walk_block_ids(
    block: &BlockDefinition,
    seen: &mut std::collections::HashSet<String>,
) -> Result<(), SequenceValidationError> {
    let (id, children): (&BlockId, Vec<&[BlockDefinition]>) = match block {
        BlockDefinition::Step(s) => {
            if let Some(human) = &s.wait_for_input {
                human
                    .validate()
                    .map_err(|message| SequenceValidationError::InvalidHumanInput {
                        block_id: s.id.0.clone(),
                        message,
                    })?;
            }
            (&s.id, vec![])
        }
        BlockDefinition::Parallel(p) => (&p.id, p.branches.iter().map(Vec::as_slice).collect()),
        BlockDefinition::Race(r) => (&r.id, r.branches.iter().map(Vec::as_slice).collect()),
        BlockDefinition::Loop(l) => (&l.id, vec![l.body.as_slice()]),
        BlockDefinition::ForEach(fe) => (&fe.id, vec![fe.body.as_slice()]),
        BlockDefinition::Router(r) => {
            let mut kids: Vec<&[BlockDefinition]> =
                r.routes.iter().map(|rt| rt.blocks.as_slice()).collect();
            if let Some(default) = r.default.as_ref() {
                kids.push(default.as_slice());
            }
            (&r.id, kids)
        }
        BlockDefinition::TryCatch(tc) => {
            let mut kids: Vec<&[BlockDefinition]> =
                vec![tc.try_block.as_slice(), tc.catch_block.as_slice()];
            if let Some(finally) = tc.finally_block.as_ref() {
                kids.push(finally.as_slice());
            }
            (&tc.id, kids)
        }
        BlockDefinition::SubSequence(s) => (&s.id, vec![]),
        BlockDefinition::ABSplit(ab) => (
            &ab.id,
            ab.variants.iter().map(|v| v.blocks.as_slice()).collect(),
        ),
        BlockDefinition::CancellationScope(cs) => (&cs.id, vec![cs.blocks.as_slice()]),
    };

    if !seen.insert(id.0.clone()) {
        return Err(SequenceValidationError::DuplicateBlockId(id.0.clone()));
    }
    for child in children {
        for b in child {
            walk_block_ids(b, seen)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_access_defaults() {
        let ca: ContextAccess = serde_json::from_str("{}").unwrap();
        assert!(ca.data.allows("anything"));
        assert!(ca.config);
        assert!(!ca.audit);
        assert!(!ca.runtime);
    }

    #[test]
    fn field_access_parses_bool_true_as_all() {
        let fa: FieldAccess = serde_json::from_str("true").unwrap();
        assert_eq!(fa, FieldAccess::Bool(true));
        assert!(fa.allows("anything"));
    }

    #[test]
    fn field_access_parses_bool_false_as_none() {
        let fa: FieldAccess = serde_json::from_str("false").unwrap();
        assert_eq!(fa, FieldAccess::Bool(false));
        assert!(!fa.allows("anything"));
    }

    #[test]
    fn field_access_parses_field_list() {
        let fa: FieldAccess = serde_json::from_str(r#"{"fields": ["a", "b"]}"#).unwrap();
        assert_eq!(
            fa,
            FieldAccess::Fields {
                fields: vec!["a".into(), "b".into()]
            }
        );
        assert!(fa.allows("a"));
        assert!(fa.allows("b"));
        assert!(!fa.allows("c"));
    }

    #[test]
    fn field_access_parses_keywords() {
        let fa: FieldAccess = serde_json::from_str(r#""all""#).unwrap();
        assert_eq!(fa, FieldAccess::Keyword(AccessKeyword::All));
        assert!(fa.allows("anything"));

        let fa: FieldAccess = serde_json::from_str(r#""none""#).unwrap();
        assert_eq!(fa, FieldAccess::Keyword(AccessKeyword::None));
        assert!(!fa.allows("anything"));
    }

    #[test]
    fn field_access_required_fields() {
        assert_eq!(
            FieldAccess::Fields {
                fields: vec!["user".into()]
            }
            .required_fields(),
            Some(&["user".into()][..])
        );
        assert_eq!(FieldAccess::Bool(true).required_fields(), None);
        assert_eq!(FieldAccess::Bool(false).required_fields(), None);
        assert_eq!(
            FieldAccess::Keyword(AccessKeyword::All).required_fields(),
            None
        );
    }

    #[test]
    fn field_access_allows_any() {
        assert!(FieldAccess::Bool(true).allows_any());
        assert!(!FieldAccess::Bool(false).allows_any());
        assert!(FieldAccess::Keyword(AccessKeyword::All).allows_any());
        assert!(!FieldAccess::Keyword(AccessKeyword::None).allows_any());
        assert!(FieldAccess::Fields {
            fields: vec!["a".into()]
        }
        .allows_any());
        assert!(!FieldAccess::Fields { fields: vec![] }.allows_any());
    }

    #[test]
    fn context_access_accepts_legacy_bool_data() {
        // Legacy payloads that predate M3.2 use `"data": true/false`. The
        // untagged serde representation must still accept them.
        let ca: ContextAccess = serde_json::from_str(r#"{"data": true}"#).unwrap();
        assert!(ca.data.allows("anything"));
        let ca: ContextAccess = serde_json::from_str(r#"{"data": false}"#).unwrap();
        assert!(!ca.data.allows("anything"));
    }

    #[test]
    fn context_access_accepts_field_list_data() {
        let ca: ContextAccess =
            serde_json::from_str(r#"{"data": {"fields": ["user_id"]}}"#).unwrap();
        assert!(ca.data.allows("user_id"));
        assert!(!ca.data.allows("other"));
        assert_eq!(ca.data.required_fields(), Some(&["user_id".into()][..]));
    }

    #[test]
    fn retry_policy_round_trip() {
        let json = r#"{
            "max_attempts": 5,
            "initial_backoff": 1000,
            "max_backoff": 30000
        }"#;
        let rp: RetryPolicy = serde_json::from_str(json).unwrap();
        assert_eq!(rp.max_attempts, 5);
        assert_eq!(rp.initial_backoff, Duration::from_secs(1));
        assert_eq!(rp.max_backoff, Duration::from_secs(30));
        assert!((rp.backoff_multiplier - 2.0).abs() < f64::EPSILON);

        let out = serde_json::to_value(&rp).unwrap();
        assert_eq!(out["initial_backoff"], 1000);
        assert_eq!(out["max_backoff"], 30000);
    }

    #[test]
    fn send_window_defaults() {
        let sw: SendWindow = serde_json::from_str("{}").unwrap();
        assert_eq!(sw.start_hour, 9);
        assert_eq!(sw.end_hour, 17);
        assert!(sw.days.is_empty());
    }

    #[test]
    fn delay_spec_round_trip() {
        let ds: DelaySpec = serde_json::from_str(r#"{"duration": 5000}"#).unwrap();
        assert_eq!(ds.duration, Duration::from_secs(5));
        assert!(!ds.business_days_only);
        assert!(ds.jitter.is_none());
        assert!(ds.holidays.is_empty());
    }

    #[test]
    fn loop_def_defaults() {
        let json = r#"{"id": "loop-1", "condition": "data.count < 10", "body": []}"#;
        let ld: LoopDef = serde_json::from_str(json).unwrap();
        assert_eq!(ld.max_iterations, 1000);
    }

    #[test]
    fn for_each_def_defaults() {
        let json = r#"{"id": "fe-1", "collection": "data.items", "body": []}"#;
        let fe: ForEachDef = serde_json::from_str(json).unwrap();
        assert_eq!(fe.item_var, "item");
        assert_eq!(fe.max_iterations, 1000);
    }

    #[test]
    fn race_semantics_default() {
        assert!(matches!(
            RaceSemantics::default(),
            RaceSemantics::FirstToResolve
        ));
    }

    #[test]
    fn block_definition_tagged_step() {
        let json = r#"{
            "type": "step",
            "id": "s1",
            "handler": "http_request",
            "params": {"url": "https://example.com"},
            "delay": null,
            "retry": null
        }"#;
        let block: BlockDefinition = serde_json::from_str(json).unwrap();
        if let BlockDefinition::Step(s) = block {
            assert_eq!(s.handler, "http_request");
            assert!(s.cancellable);
        } else {
            panic!("expected Step variant");
        }
    }

    #[test]
    fn block_definition_tagged_parallel() {
        let json = r#"{"type": "parallel", "id": "p1", "branches": [[]]}"#;
        let block: BlockDefinition = serde_json::from_str(json).unwrap();
        assert!(matches!(block, BlockDefinition::Parallel(_)));
    }

    #[test]
    fn block_definition_rejects_unknown_type() {
        let json = r#"{"type": "unknown_block", "id": "x"}"#;
        assert!(serde_json::from_str::<BlockDefinition>(json).is_err());
    }

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

    fn step(id: &str) -> BlockDefinition {
        BlockDefinition::Step(Box::new(StepDef {
            id: BlockId(id.into()),
            handler: "noop".into(),
            params: serde_json::Value::Null,
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

    #[test]
    fn validate_accepts_unique_ids() {
        let seq = sample_seq(vec![step("a"), step("b"), step("c")]);
        assert!(seq.validate().is_ok());
    }

    #[test]
    fn validate_rejects_duplicate_top_level() {
        let seq = sample_seq(vec![step("dup"), step("dup")]);
        let err = seq.validate().unwrap_err();
        assert!(matches!(err, SequenceValidationError::DuplicateBlockId(ref s) if s == "dup"));
    }

    #[test]
    fn validate_descends_into_parallel_branches() {
        let seq = sample_seq(vec![
            step("outer"),
            BlockDefinition::Parallel(Box::new(ParallelDef {
                id: BlockId("par".into()),
                branches: vec![vec![step("outer")]],
            })),
        ]);
        assert!(matches!(
            seq.validate().unwrap_err(),
            SequenceValidationError::DuplicateBlockId(_)
        ));
    }

    #[test]
    fn validate_descends_into_try_catch() {
        let seq = sample_seq(vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
            id: BlockId("tc".into()),
            try_block: vec![step("x")],
            catch_block: vec![step("x")],
            finally_block: None,
        }))]);
        assert!(matches!(
            seq.validate().unwrap_err(),
            SequenceValidationError::DuplicateBlockId(_)
        ));
    }

    #[test]
    fn validate_descends_into_router() {
        let seq = sample_seq(vec![BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId("r".into()),
            routes: vec![Route {
                condition: "true".into(),
                blocks: vec![step("dup")],
            }],
            default: Some(vec![step("dup")]),
        }))]);
        assert!(matches!(
            seq.validate().unwrap_err(),
            SequenceValidationError::DuplicateBlockId(_)
        ));
    }

    #[test]
    fn validate_descends_into_cancellation_scope_and_ab_split() {
        let seq = sample_seq(vec![
            BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
                id: BlockId("cs".into()),
                blocks: vec![step("shared")],
            })),
            BlockDefinition::ABSplit(Box::new(ABSplitDef {
                id: BlockId("ab".into()),
                variants: vec![ABVariant {
                    name: "v1".into(),
                    weight: 1,
                    blocks: vec![step("shared")],
                }],
            })),
        ]);
        assert!(matches!(
            seq.validate().unwrap_err(),
            SequenceValidationError::DuplicateBlockId(_)
        ));
    }

    #[test]
    fn human_choice_deserializes_label_value() {
        let choice: HumanChoice =
            serde_json::from_str(r#"{"label":"Approve","value":"approve"}"#).unwrap();
        assert_eq!(choice.label, "Approve");
        assert_eq!(choice.value, "approve");
    }

    #[test]
    fn human_input_def_without_choices_yields_none() {
        let j = r#"{"prompt":"Approve?"}"#;
        let d: HumanInputDef = serde_json::from_str(j).unwrap();
        assert!(d.choices.is_none());
        assert!(d.store_as.is_none());
    }

    #[test]
    fn human_input_def_with_choices_and_store_as() {
        let j = r#"{
            "prompt":"pick",
            "store_as":"decision",
            "choices":[
              {"label":"A","value":"a"},
              {"label":"B","value":"b"}
            ]
        }"#;
        let d: HumanInputDef = serde_json::from_str(j).unwrap();
        assert_eq!(d.store_as.as_deref(), Some("decision"));
        assert_eq!(d.choices.as_ref().unwrap().len(), 2);
        assert_eq!(d.choices.as_ref().unwrap()[0].value, "a");
    }

    #[test]
    fn sequence_validate_rejects_invalid_human_input_on_step() {
        let bad_human = HumanInputDef {
            prompt: String::new(),
            timeout: None,
            escalation_handler: None,
            choices: Some(vec![]),
            store_as: None,
        };
        let step_with_bad = BlockDefinition::Step(Box::new(StepDef {
            id: BlockId("review".into()),
            handler: "human_review".into(),
            params: serde_json::Value::Null,
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: Some(bad_human),
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
            cache_key: None,
        }));
        let seq = sample_seq(vec![step_with_bad]);
        let err = seq.validate().unwrap_err();
        assert!(matches!(
            err,
            SequenceValidationError::InvalidHumanInput { ref block_id, .. } if block_id == "review"
        ));
    }

    #[test]
    fn empty_choices_vec_is_rejected() {
        let d = HumanInputDef {
            prompt: String::new(),
            timeout: None,
            escalation_handler: None,
            choices: Some(vec![]),
            store_as: None,
        };
        assert!(d.validate().is_err());
    }

    #[test]
    fn duplicate_choice_values_are_rejected() {
        let d = HumanInputDef {
            prompt: String::new(),
            timeout: None,
            escalation_handler: None,
            choices: Some(vec![
                HumanChoice {
                    label: "A".into(),
                    value: "x".into(),
                },
                HumanChoice {
                    label: "B".into(),
                    value: "x".into(),
                },
            ]),
            store_as: None,
        };
        assert!(d.validate().is_err());
    }

    #[test]
    fn empty_store_as_string_is_rejected() {
        let d = HumanInputDef {
            prompt: String::new(),
            timeout: None,
            escalation_handler: None,
            choices: None,
            store_as: Some(String::new()),
        };
        assert!(d.validate().is_err());
    }

    #[test]
    fn valid_choices_pass() {
        let d = HumanInputDef {
            prompt: String::new(),
            timeout: None,
            escalation_handler: None,
            choices: Some(vec![
                HumanChoice {
                    label: "Yes".into(),
                    value: "yes".into(),
                },
                HumanChoice {
                    label: "No".into(),
                    value: "no".into(),
                },
            ]),
            store_as: Some("decision".into()),
        };
        assert!(d.validate().is_ok());
    }

    #[test]
    fn valid_no_choices_and_no_store_as_passes() {
        let d = HumanInputDef {
            prompt: String::new(),
            timeout: None,
            escalation_handler: None,
            choices: None,
            store_as: None,
        };
        assert!(d.validate().is_ok());
    }

    #[test]
    fn effective_choices_defaults_to_yes_no() {
        let d = HumanInputDef {
            prompt: String::new(),
            timeout: None,
            escalation_handler: None,
            choices: None,
            store_as: None,
        };
        let c = d.effective_choices();
        assert_eq!(c.len(), 2);
        assert_eq!(c[0].value, "yes");
        assert_eq!(c[0].label, "Yes");
        assert_eq!(c[1].value, "no");
        assert_eq!(c[1].label, "No");
    }

    #[test]
    fn effective_choices_uses_author_choices_when_present() {
        let d = HumanInputDef {
            prompt: String::new(),
            timeout: None,
            escalation_handler: None,
            choices: Some(vec![HumanChoice {
                label: "Approve".into(),
                value: "approve".into(),
            }]),
            store_as: None,
        };
        let c = d.effective_choices();
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].value, "approve");
    }

    #[test]
    fn handler_names_collects_from_steps() {
        let seq = sample_seq(vec![step("a"), step("b")]);
        let names = seq.handler_names();
        assert_eq!(names, vec!["noop"]); // step() helper uses "noop"
    }

    #[test]
    fn unknown_handler_warnings_detects_typo() {
        let mut seq = sample_seq(vec![step("a")]);
        // Manually change the handler to a typo
        if let BlockDefinition::Step(ref mut s) = seq.blocks[0] {
            s.handler = "http_requst".into(); // typo for http_request
        }
        let warnings = seq.unknown_handler_warnings();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("http_request"), "got: {}", warnings[0]);
    }

    #[test]
    fn unknown_handler_warnings_empty_for_builtins() {
        let seq = sample_seq(vec![step("a")]);
        assert!(seq.unknown_handler_warnings().is_empty());
    }

    #[test]
    fn builtin_handler_names_includes_expected() {
        assert!(BUILTIN_HANDLER_NAMES.contains(&"noop"));
        assert!(BUILTIN_HANDLER_NAMES.contains(&"http_request"));
        assert!(BUILTIN_HANDLER_NAMES.contains(&"human_review"));
    }
}
