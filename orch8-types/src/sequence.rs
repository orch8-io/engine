use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use utoipa::ToSchema;

use crate::ids::{BlockId, Namespace, SequenceId, TenantId};

/// Current persisted workflow-definition format. Older documents without the
/// field decode as v1; future incompatible formats must be upgraded explicitly.
pub const SEQUENCE_SCHEMA_VERSION: u32 = 1;

const fn default_sequence_schema_version() -> u32 {
    SEQUENCE_SCHEMA_VERSION
}

/// Lifecycle status for sequences: Draft → Staging → Production → Unpublished.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SequenceStatus {
    Draft,
    Staging,
    #[default]
    Production,
    Unpublished,
}

impl std::fmt::Display for SequenceStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Draft => f.write_str("draft"),
            Self::Staging => f.write_str("staging"),
            Self::Production => f.write_str("production"),
            Self::Unpublished => f.write_str("unpublished"),
        }
    }
}

impl std::str::FromStr for SequenceStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "draft" => Ok(Self::Draft),
            "staging" => Ok(Self::Staging),
            "production" => Ok(Self::Production),
            "unpublished" => Ok(Self::Unpublished),
            other => Err(format!("unknown sequence status: {other}")),
        }
    }
}

impl SequenceStatus {
    pub fn valid_transitions(self) -> &'static [SequenceStatus] {
        match self {
            Self::Draft => &[Self::Staging, Self::Unpublished],
            Self::Staging => &[Self::Production, Self::Unpublished],
            Self::Production => &[Self::Unpublished],
            Self::Unpublished => &[],
        }
    }

    pub fn can_transition_to(self, target: Self) -> bool {
        self.valid_transitions().contains(&target)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SequenceDefinition {
    /// Editor-facing JSON Schema link. It is metadata and does not affect execution.
    #[serde(default, rename = "$schema", skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(default = "default_sequence_schema_version")]
    pub schema_version: u32,
    pub id: SequenceId,
    pub tenant_id: TenantId,
    pub namespace: Namespace,
    pub name: String,
    pub version: i32,
    /// If true, this version is deprecated. New instances should use a newer version.
    /// Running instances bound to this version continue unaffected.
    #[serde(default)]
    pub deprecated: bool,
    #[serde(default)]
    pub status: SequenceStatus,
    pub blocks: Vec<BlockDefinition>,
    /// Lifecycle interceptors (before/after step, on-signal, on-complete, on-failure).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interceptors: Option<crate::interceptor::InterceptorDef>,
    /// Optional JSON Schema validated against `context.data` at instance
    /// create. When present, a create whose data fails validation is
    /// rejected (HTTP 422) before the instance is persisted. Doubles as the
    /// contract the dashboard renders an input form from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<serde_json::Value>,
    /// Alert-only SLA policy. When set, an instance exceeding `max_runtime`
    /// (or a step exceeding `max_step_runtime`) emits an `instance.sla_breached`
    /// webhook and increments `orch8_sla_breached_total` — the instance is NOT
    /// failed or paused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sla: Option<SlaPolicy>,
    /// Best-effort cleanup blocks run when the instance reaches terminal
    /// `Failed`. Each top-level step block is dispatched once; errors are
    /// swallowed (the instance is already failing). Use to release resources,
    /// send a death notification, etc. — so a failed run doesn't "just die".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_failure: Option<Vec<BlockDefinition>>,
    /// Best-effort cleanup blocks run when the instance reaches terminal
    /// `Cancelled`. Same semantics as `on_failure`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_cancel: Option<Vec<BlockDefinition>>,
    pub created_at: DateTime<Utc>,
}

/// Alert-only service-level-agreement policy for a sequence. A breach is a
/// signal, not a state change: it fires one webhook + metric per breach kind
/// and leaves the instance running. Both bounds are optional.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct SlaPolicy {
    /// Maximum wall-clock lifetime of an instance, measured from `created_at`.
    /// One `max_runtime` alert per instance.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_duration_opt"
    )]
    #[schema(value_type = Option<u64>)]
    pub max_runtime: Option<Duration>,
    /// Maximum wall-clock time the current step may stay running/waiting,
    /// measured from when it started. One alert per breaching step.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_duration_opt"
    )]
    #[schema(value_type = Option<u64>)]
    pub max_step_runtime: Option<Duration>,
}

/// A block is either a leaf (step) or a composite (parallel, race, etc.).
///
/// This recursive enum IS the workflow DSL.
/// Each variant wraps its definition in `Box<T>` so the enum carries a pointer
/// plus its variant tag. `StepDef` is large, with many `Option<...>` fields, and
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
    #[serde(rename = "ab_split", alias = "a_b_split")]
    ABSplit(Box<ABSplitDef>),
    /// Cancellation scope: child blocks cannot be cancelled by external cancel signals.
    /// Provides subtree-level non-cancellability (Temporal-style structured concurrency).
    CancellationScope(Box<CancellationScopeDef>),
    /// Saga: sequential steps with compensating actions, rolled back in
    /// reverse order if any step's action fails.
    Saga(Box<SagaDef>),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StepDef {
    pub id: BlockId,
    pub handler: String,
    #[serde(default)]
    pub params: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay: Option<DelaySpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    /// Optional JSON Schema that the handler output must conform to.
    /// Validated after handler returns; schema violation is a permanent failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<serde_json::Value>,
    /// Conditional guard expression. When set, the step is only executed if the
    /// expression evaluates to a truthy value; otherwise the step is skipped
    /// (marked `NodeState::Skipped`). Uses the same expression syntax as router
    /// conditions — `data.*`, `outputs.*`, comparisons, boolean ops.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    /// Receipt-backed recovery action for this externally visible step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compensation: Option<StepCompensation>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CompensationVerificationPolicy {
    /// A successful handler response is evidence of the compensation attempt,
    /// but does not claim the external world was fully restored.
    #[default]
    HandlerResult,
    /// Require a provider receipt identifier before the step is accepted.
    ProviderReceipt,
    /// Hold completion for explicit operator verification.
    Manual,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StepCompensation {
    pub handler: String,
    #[serde(default)]
    pub params: serde_json::Value,
    /// Effect blocks that must be compensated after this block. The planner
    /// reverses these forward dependencies deterministically.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<BlockId>,
    #[serde(default)]
    pub verification: CompensationVerificationPolicy,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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

impl<'de> Deserialize<'de> for FieldAccess {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FieldAccessVisitor;

        impl<'de> serde::de::Visitor<'de> for FieldAccessVisitor {
            type Value = FieldAccess;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(
                    "true, false, \"all\", \"none\", or an object {\"fields\": [\"field\"]}",
                )
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(FieldAccess::Bool(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "all" => Ok(FieldAccess::Keyword(AccessKeyword::All)),
                    "none" => Ok(FieldAccess::Keyword(AccessKeyword::None)),
                    other => Err(E::unknown_variant(other, &["all", "none"])),
                }
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut fields = None;
                while let Some(key) = map.next_key::<String>()? {
                    if key == "fields" {
                        if fields.is_some() {
                            return Err(serde::de::Error::duplicate_field("fields"));
                        }
                        fields = Some(map.next_value::<Vec<String>>()?);
                    } else {
                        return Err(serde::de::Error::unknown_field(&key, &["fields"]));
                    }
                }
                fields
                    .map(|fields| FieldAccess::Fields { fields })
                    .ok_or_else(|| serde::de::Error::missing_field("fields"))
            }
        }

        deserializer.deserialize_any(FieldAccessVisitor)
    }
}

/// A workflow-definition decoding failure with a JSON path suitable for CLI
/// and API diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct SequenceDecodeError {
    message: String,
}

impl SequenceDecodeError {
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

const SEQUENCE_FIELD_NAMES: &[&str] = &[
    "id",
    "tenant_id",
    "namespace",
    "name",
    "version",
    "deprecated",
    "status",
    "blocks",
    "interceptors",
    "input_schema",
    "sla",
    "on_failure",
    "on_cancel",
    "created_at",
    "type",
    "handler",
    "params",
    "delay",
    "retry",
    "timeout",
    "rate_limit_key",
    "send_window",
    "context_access",
    "cancellable",
    "wait_for_input",
    "queue_name",
    "deadline",
    "on_deadline_breach",
    "fallback_handler",
    "cache_key",
    "when",
    "retry_if",
    "max_attempts",
    "initial_backoff",
    "max_backoff",
    "backoff_multiplier",
    "output_schema",
    "compensation",
    "branches",
    "semantics",
    "condition",
    "body",
    "max_iterations",
    "break_on",
    "continue_on_error",
    "poll_interval",
    "retain_iterations",
    "collection",
    "item_var",
    "routes",
    "default",
    "try_block",
    "catch_block",
    "finally_block",
    "sequence_name",
    "input",
    "variants",
    "steps",
    "action",
    "name",
    "weight",
    "duration",
    "business_days_only",
    "jitter",
    "holidays",
    "fire_at_local",
    "timezone",
    "start_hour",
    "end_hour",
    "days",
    "data",
    "fields",
    "config",
    "audit",
    "runtime",
    "prompt",
    "escalation_handler",
    "choices",
    "store_as",
    "allow_comment",
    "auto_decide",
    "threshold",
    "instructions",
    "model",
    "api_key",
    "base_url",
    "interpret_replies",
    "label",
    "value",
    "non_retryable_codes",
    "depends_on",
    "verification",
];

fn collect_unknown_block_fields(
    blocks: &serde_json::Value,
    path: &str,
    unknown: &mut Vec<String>,
    extra: &mut Vec<String>,
) {
    let Some(blocks) = blocks.as_array() else {
        return;
    };
    for (index, block) in blocks.iter().enumerate() {
        collect_unknown_block_field(block, &format!("{path}.{index}"), unknown, extra);
    }
}

/// Decode `block` as its concrete variant type under `serde_ignored` so every
/// unknown key in the block *and its nested sub-objects* (`retry`,
/// `send_window`, `context_access`, `wait_for_input`, routes, variants, saga
/// steps, ...) is reported. Nested `BlockDefinition`s are internally tagged,
/// which buffers their content and hides it from `serde_ignored`, so child
/// blocks are visited by the caller instead. Decode errors are ignored here;
/// they surface from the main decode with a precise path.
fn collect_unknown_typed<T>(block: &serde_json::Value, path: &str, unknown: &mut Vec<String>)
where
    T: serde::de::DeserializeOwned,
{
    let _ = serde_ignored::deserialize::<_, _, T>(block, |ignored| {
        let ignored = ignored.to_string();
        // The enum tag is consumed by `BlockDefinition`, not the variant type.
        if ignored != "type" {
            unknown.push(format!("{path}.{ignored}"));
        }
    });
}

/// `context_access: {...}` without `data` silently defaults to full data
/// access. That is the documented legacy default when `context_access` is
/// omitted entirely, but an explicit restriction object that forgets (or
/// misspells) `data` is almost certainly a mistake that fails open.
fn context_access_warning(
    block: &serde_json::Map<String, serde_json::Value>,
    path: &str,
) -> Option<String> {
    let access = block.get("context_access")?.as_object()?;
    if access.contains_key("data") {
        return None;
    }
    Some(format!(
        "{}.context_access has no \"data\" key, so the step can read all of context.data; \
         set \"data\" explicitly (true, false, \"all\", \"none\", or {{\"fields\": [..]}})",
        readable_json_path(path)
    ))
}

fn collect_unknown_block_field(
    block: &serde_json::Value,
    path: &str,
    unknown: &mut Vec<String>,
    extra: &mut Vec<String>,
) {
    let Some(object) = block.as_object() else {
        return;
    };
    let Some(block_type) = object.get("type").and_then(serde_json::Value::as_str) else {
        return;
    };
    match block_type {
        "step" => {
            collect_unknown_typed::<StepDef>(block, path, unknown);
            extra.extend(context_access_warning(object, path));
        }
        "parallel" => collect_unknown_typed::<ParallelDef>(block, path, unknown),
        "race" => collect_unknown_typed::<RaceDef>(block, path, unknown),
        "loop" => collect_unknown_typed::<LoopDef>(block, path, unknown),
        "for_each" => collect_unknown_typed::<ForEachDef>(block, path, unknown),
        "router" => collect_unknown_typed::<RouterDef>(block, path, unknown),
        "try_catch" => collect_unknown_typed::<TryCatchDef>(block, path, unknown),
        "sub_sequence" => collect_unknown_typed::<SubSequenceDef>(block, path, unknown),
        "ab_split" | "a_b_split" => collect_unknown_typed::<ABSplitDef>(block, path, unknown),
        "cancellation_scope" => {
            collect_unknown_typed::<CancellationScopeDef>(block, path, unknown);
        }
        "saga" => collect_unknown_typed::<SagaDef>(block, path, unknown),
        _ => return,
    }

    let mut children = |value: Option<&serde_json::Value>, child_path: String| {
        if let Some(value) = value {
            collect_unknown_block_fields(value, &child_path, unknown, extra);
        }
    };
    match block_type {
        "parallel" | "race" => {
            if let Some(branches) = object.get("branches").and_then(serde_json::Value::as_array) {
                for (index, branch) in branches.iter().enumerate() {
                    children(Some(branch), format!("{path}.branches.{index}"));
                }
            }
        }
        "loop" | "for_each" => children(object.get("body"), format!("{path}.body")),
        "router" => {
            if let Some(routes) = object.get("routes").and_then(serde_json::Value::as_array) {
                for (index, route) in routes.iter().enumerate() {
                    children(route.get("blocks"), format!("{path}.routes.{index}.blocks"));
                }
            }
            children(object.get("default"), format!("{path}.default"));
        }
        "try_catch" => {
            for field in ["try_block", "catch_block", "finally_block"] {
                children(object.get(field), format!("{path}.{field}"));
            }
        }
        "ab_split" | "a_b_split" => {
            if let Some(variants) = object.get("variants").and_then(serde_json::Value::as_array) {
                for (index, variant) in variants.iter().enumerate() {
                    children(
                        variant.get("blocks"),
                        format!("{path}.variants.{index}.blocks"),
                    );
                }
            }
        }
        "cancellation_scope" => children(object.get("blocks"), format!("{path}.blocks")),
        "saga" => {
            if let Some(steps) = object.get("steps").and_then(serde_json::Value::as_array) {
                for (index, step) in steps.iter().enumerate() {
                    for field in ["action", "compensation"] {
                        if let Some(child) = step.get(field) {
                            collect_unknown_block_field(
                                child,
                                &format!("{path}.steps.{index}.{field}"),
                                unknown,
                                extra,
                            );
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

fn typed_block_decode_error<T>(value: &serde_json::Value, path: &str) -> Option<SequenceDecodeError>
where
    T: serde::de::DeserializeOwned,
{
    let error = serde_path_to_error::deserialize::<_, T>(value.clone()).err()?;
    let inner_path = readable_json_path(&error.path().to_string());
    let message = if inner_path.is_empty() {
        format!("{path}: {}", error.inner())
    } else {
        format!("{path}.{inner_path}: {}", error.inner())
    };
    Some(SequenceDecodeError { message })
}

fn nested_block_decode_error(value: &serde_json::Value, path: &str) -> Option<SequenceDecodeError> {
    let object = value.as_object()?;
    let block_type = object.get("type").and_then(serde_json::Value::as_str)?;
    let inspect_list = |value: &serde_json::Value, child_path: &str| {
        value.as_array().and_then(|blocks| {
            blocks.iter().enumerate().find_map(|(index, block)| {
                precise_block_decode_error(block, &format!("{child_path}[{index}]"))
            })
        })
    };

    match block_type {
        "parallel" | "race" => object
            .get("branches")
            .and_then(serde_json::Value::as_array)
            .and_then(|branches| {
                branches.iter().enumerate().find_map(|(index, branch)| {
                    inspect_list(branch, &format!("{path}.branches[{index}]"))
                })
            }),
        "loop" | "for_each" => object
            .get("body")
            .and_then(|body| inspect_list(body, &format!("{path}.body"))),
        "router" => object
            .get("routes")
            .and_then(serde_json::Value::as_array)
            .and_then(|routes| {
                routes.iter().enumerate().find_map(|(index, route)| {
                    route.get("blocks").and_then(|blocks| {
                        inspect_list(blocks, &format!("{path}.routes[{index}].blocks"))
                    })
                })
            })
            .or_else(|| {
                object
                    .get("default")
                    .and_then(|blocks| inspect_list(blocks, &format!("{path}.default")))
            }),
        "try_catch" => ["try_block", "catch_block", "finally_block"]
            .into_iter()
            .find_map(|field| {
                object
                    .get(field)
                    .and_then(|blocks| inspect_list(blocks, &format!("{path}.{field}")))
            }),
        "ab_split" | "a_b_split" => object
            .get("variants")
            .and_then(serde_json::Value::as_array)
            .and_then(|variants| {
                variants.iter().enumerate().find_map(|(index, variant)| {
                    variant.get("blocks").and_then(|blocks| {
                        inspect_list(blocks, &format!("{path}.variants[{index}].blocks"))
                    })
                })
            }),
        "cancellation_scope" => object
            .get("blocks")
            .and_then(|blocks| inspect_list(blocks, &format!("{path}.blocks"))),
        "saga" => object
            .get("steps")
            .and_then(serde_json::Value::as_array)
            .and_then(|steps| {
                steps.iter().enumerate().find_map(|(index, step)| {
                    ["action", "compensation"].into_iter().find_map(|field| {
                        step.get(field).and_then(|block| {
                            precise_block_decode_error(
                                block,
                                &format!("{path}.steps[{index}].{field}"),
                            )
                        })
                    })
                })
            }),
        _ => None,
    }
}

/// Decode variants directly so serde's internally-tagged enum adapter cannot
/// erase the failing field path. Composite children are inspected first.
fn precise_block_decode_error(
    value: &serde_json::Value,
    path: &str,
) -> Option<SequenceDecodeError> {
    let object = value.as_object()?;
    let block_type = object.get("type").and_then(serde_json::Value::as_str)?;
    if let Some(error) = nested_block_decode_error(value, path) {
        return Some(error);
    }

    match block_type {
        "step" => typed_block_decode_error::<StepDef>(value, path),
        "parallel" => typed_block_decode_error::<ParallelDef>(value, path),
        "race" => typed_block_decode_error::<RaceDef>(value, path),
        "loop" => typed_block_decode_error::<LoopDef>(value, path),
        "for_each" => typed_block_decode_error::<ForEachDef>(value, path),
        "router" => typed_block_decode_error::<RouterDef>(value, path),
        "try_catch" => typed_block_decode_error::<TryCatchDef>(value, path),
        "sub_sequence" => typed_block_decode_error::<SubSequenceDef>(value, path),
        "ab_split" | "a_b_split" => typed_block_decode_error::<ABSplitDef>(value, path),
        "cancellation_scope" => typed_block_decode_error::<CancellationScopeDef>(value, path),
        "saga" => typed_block_decode_error::<SagaDef>(value, path),
        _ => typed_block_decode_error::<BlockDefinition>(value, path),
    }
}

fn precise_sequence_decode_error(value: &serde_json::Value) -> Option<SequenceDecodeError> {
    ["blocks", "on_failure", "on_cancel"]
        .into_iter()
        .find_map(|field| {
            value
                .get(field)
                .and_then(serde_json::Value::as_array)
                .and_then(|blocks| {
                    blocks.iter().enumerate().find_map(|(index, block)| {
                        precise_block_decode_error(block, &format!("{field}[{index}]"))
                    })
                })
        })
}

fn readable_json_path(path: &str) -> String {
    let mut rendered = String::new();
    for segment in path.split('.') {
        if segment == "?" || segment.is_empty() {
            continue;
        }
        if segment.bytes().all(|byte| byte.is_ascii_digit()) {
            rendered.push('[');
            rendered.push_str(segment);
            rendered.push(']');
        } else {
            if !rendered.is_empty() {
                rendered.push('.');
            }
            rendered.push_str(segment);
        }
    }
    rendered
}

fn unknown_field_message(path: &str) -> String {
    let path = readable_json_path(path);
    let field = path
        .rsplit(['.', ']'])
        .find(|part| !part.is_empty() && !part.bytes().all(|byte| byte.is_ascii_digit()))
        .unwrap_or(path.as_str());
    let common_typo = match field {
        "retires" | "retries" => Some("retry"),
        "wehn" => Some("when"),
        "timeout_ms" => Some("timeout"),
        _ => None,
    };
    let suggestion = common_typo
        .or_else(|| crate::suggest::did_you_mean(field, SEQUENCE_FIELD_NAMES))
        .filter(|candidate| *candidate != field)
        .map_or_else(String::new, |candidate| {
            format!(" (did you mean \"{candidate}\"?)")
        });
    format!("unknown field \"{field}\" at {path}{suggestion}")
}

/// Deserialize a workflow while retaining unknown-field diagnostics.
/// Syntax/type errors retain their precise JSON path.
pub fn deserialize_sequence_lenient(
    value: &serde_json::Value,
) -> Result<(SequenceDefinition, Vec<String>), SequenceDecodeError> {
    let bytes = serde_json::to_vec(value).map_err(|error| SequenceDecodeError {
        message: format!("could not encode sequence input: {error}"),
    })?;
    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    let mut ignored = Vec::new();
    let mut extra = Vec::new();
    for field in ["blocks", "on_failure", "on_cancel"] {
        if let Some(blocks) = value.get(field) {
            collect_unknown_block_fields(blocks, field, &mut ignored, &mut extra);
        }
    }
    let parsed = serde_ignored::deserialize(&mut deserializer, |path| {
        ignored.push(path.to_string());
    });

    if let Ok(sequence) = parsed {
        ignored.sort();
        ignored.dedup();
        let warnings = ignored
            .iter()
            .map(|path| unknown_field_message(path))
            .chain(extra)
            .collect();
        Ok((sequence, warnings))
    } else {
        if let Some(error) = precise_sequence_decode_error(value) {
            return Err(error);
        }
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        serde_path_to_error::deserialize(&mut deserializer).map_err(|error| {
            let path = readable_json_path(&error.path().to_string());
            let message = if path.is_empty() {
                error.inner().to_string()
            } else {
                format!("{path}: {}", error.inner())
            };
            SequenceDecodeError { message }
        })
    }
}

/// Deserialize a workflow and reject every unknown field. Use this mode for
/// local authoring/CI and explicit API `?strict=true` validation.
pub fn deserialize_sequence_strict(
    value: &serde_json::Value,
) -> Result<SequenceDefinition, SequenceDecodeError> {
    let (sequence, warnings) = deserialize_sequence_lenient(value)?;
    if warnings.is_empty() {
        Ok(sequence)
    } else {
        Err(SequenceDecodeError {
            message: warnings.join("; "),
        })
    }
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
    /// Timeout in milliseconds before the step fails or escalates.
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
    /// When true, the reviewer can attach a free-text comment to their decision.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_comment: bool,
    /// Confidence-gated automation: ask the Jev decision model to answer the
    /// gate first. At or above `threshold` confidence the gate is accepted
    /// without a human (the decision, confidence and probabilities are kept
    /// on the gate's output as evidence); below it — or when the model is
    /// unavailable — the step parks for a human exactly as without this
    /// setting.
    /// Boxed: most gates don't set it, and `HumanInputDef` rides inside
    /// every `StepDef` (and so inside large async state machines).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_decide: Option<Box<AutoDecideDef>>,
}

/// Default confidence required before a gate is decided without a human.
pub const DEFAULT_AUTO_DECIDE_THRESHOLD: f64 = 0.9;

const fn default_auto_decide_threshold() -> f64 {
    DEFAULT_AUTO_DECIDE_THRESHOLD
}

/// `wait_for_input.auto_decide`: let a calibrated decision model answer a
/// human gate when it is confident enough.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct AutoDecideDef {
    /// Minimum model confidence in `[0, 1]` to accept without a human.
    #[serde(default = "default_auto_decide_threshold")]
    pub threshold: f64,
    /// Question put to the model; defaults to the gate's `prompt`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    /// Model id; defaults to `jev-latest`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// API key (literal or `credentials://` reference). When omitted the
    /// operator's `TYPESAFE_API_KEY` is used — only for the default endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    /// Endpoint override; requires an explicit `api_key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Map a free-text human reply (`{"text": "sure, go ahead"}`) onto one of
    /// the gate's choices instead of rejecting it as an invalid value.
    #[serde(default = "crate::serde_defaults::yes")]
    pub interpret_replies: bool,
}

impl AutoDecideDef {
    /// Structural validation.
    ///
    /// # Errors
    /// Returns a message when `threshold` is outside `[0, 1]` or a custom
    /// `base_url` is set without an explicit `api_key`.
    pub fn validate(&self) -> Result<(), String> {
        if !(0.0..=1.0).contains(&self.threshold) {
            return Err(format!(
                "auto_decide.threshold must be within [0, 1], got {}",
                self.threshold
            ));
        }
        if self.base_url.is_some() && self.api_key.is_none() {
            return Err("auto_decide.base_url requires an explicit api_key".into());
        }
        Ok(())
    }
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
        if let Some(s) = &self.store_as
            && s.is_empty()
        {
            return Err("human_review: `store_as` must be non-empty".into());
        }
        if let Some(auto) = &self.auto_decide {
            auto.validate().map_err(|e| format!("human_review: {e}"))?;
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
    #[serde(default = "default_initial_backoff", with = "crate::serde_duration")]
    #[schema(value_type = u64)]
    pub initial_backoff: Duration,
    #[serde(default = "default_max_backoff", with = "crate::serde_duration")]
    #[schema(value_type = u64)]
    pub max_backoff: Duration,
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_if: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub non_retryable_codes: Option<Vec<String>>,
}

fn default_initial_backoff() -> Duration {
    Duration::from_secs(1)
}

fn default_max_backoff() -> Duration {
    Duration::from_secs(60)
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    /// Seconds to wait between loop-condition polls.
    pub poll_interval: Option<u64>,
    /// Keep only the most recent N iterations' body-step outputs; older outputs
    /// are compacted (deleted) at each iteration boundary to bound storage
    /// growth on long-running loops. `None` retains everything (default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain_iterations: Option<u32>,
}

const fn default_max_iterations() -> u32 {
    1000
}

/// A saga: a sequence of steps that each carry an optional compensating
/// action. If any step's action fails, the compensations for every
/// already-completed step run in reverse order (LIFO) — the classic saga
/// pattern for distributed transactions without two-phase commit.
///
/// Compensation failures are recorded but do not stop the rollback: every
/// completed step's compensation gets a chance to run, best-effort. The
/// saga node itself always fails once compensation finishes (whether or
/// not every compensation succeeded) — the original action failure is
/// never silently absorbed.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SagaDef {
    pub id: BlockId,
    pub steps: Vec<SagaStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SagaStep {
    pub id: BlockId,
    /// The forward action for this saga step.
    pub action: Box<BlockDefinition>,
    /// Compensating action run (in reverse order across all completed
    /// steps) if a later step's action fails. `None` means this step has
    /// no compensation — it is skipped during rollback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compensation: Option<Box<BlockDefinition>>,
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
    /// Keep only the most recent N iterations' body-step outputs; older outputs
    /// are compacted at each iteration boundary. `None` retains everything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain_iterations: Option<u32>,
}

fn default_item_var() -> String {
    "item".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RouterDef {
    pub id: BlockId,
    pub routes: Vec<Route>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    #[error("duplicate block id: {0}")]
    DuplicateBlockId(String),
    #[error("invalid human_review on block `{block_id}`: {message}")]
    InvalidHumanInput { block_id: String, message: String },
    #[error("block `{block_id}`: {message}")]
    InvalidBlock { block_id: String, message: String },
}

impl SequenceValidationError {
    /// Error-catalog key (see [`crate::error_catalog`]) for this failure.
    #[must_use]
    pub const fn catalog_key(&self) -> &'static str {
        match self {
            Self::DuplicateBlockId(_) => "DUPLICATE_BLOCK_ID",
            Self::InvalidHumanInput { .. } => "INVALID_HUMAN_INPUT",
            Self::InvalidBlock { .. } => "INVALID_BLOCK",
        }
    }
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
    "mcp_call",
    "agent",
    "embed",
    "memory_store",
    "memory_search",
    "memory_delete",
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
    "blob_put",
    "blob_get",
    "wait_for_event",
    "jev",
    "email",
    "notify",
];

impl SequenceDefinition {
    /// Structural validation performed at submit time (before the sequence
    /// reaches storage).
    pub fn validate(&self) -> Result<(), SequenceValidationError> {
        if self.schema_version == 0 || self.schema_version > SEQUENCE_SCHEMA_VERSION {
            return Err(SequenceValidationError::InvalidBlock {
                block_id: "(root)".into(),
                message: format!(
                    "unsupported schema_version {} (this engine supports {})",
                    self.schema_version, SEQUENCE_SCHEMA_VERSION
                ),
            });
        }
        if self.blocks.is_empty() {
            return Err(SequenceValidationError::InvalidBlock {
                block_id: "(root)".into(),
                message: "sequence has no blocks".into(),
            });
        }
        let mut seen = std::collections::HashSet::new();
        for block in &self.blocks {
            validate_block(block, &mut seen, 0)?;
        }
        // Depth is bounded by `validate_block` above, so this recursion is
        // safe to run afterwards.
        for block in self
            .blocks
            .iter()
            .chain(self.on_failure.iter().flatten())
            .chain(self.on_cancel.iter().flatten())
        {
            check_nested_iterations(block, 1)?;
        }
        // `seen` gained exactly one entry per unique block id visited above
        // (duplicates are rejected by `check_id`), so its size is the total
        // block count across the whole tree.
        let mut total_blocks = seen.len();
        // The cleanup trees (`on_failure` / `on_cancel`) are dispatched
        // independently of the main tree and of each other, so block ids may
        // be reused across trees — each gets its own `seen` set. They still
        // need the same per-block checks (nesting depth, branch/iteration
        // caps, handler sanity) and still count toward the total-block cap,
        // or the DoS bounds are trivially bypassed via these fields.
        for cleanup_tree in [&self.on_failure, &self.on_cancel].into_iter().flatten() {
            let mut cleanup_seen = std::collections::HashSet::new();
            for block in cleanup_tree {
                validate_block(block, &mut cleanup_seen, 0)?;
            }
            total_blocks += cleanup_seen.len();
        }
        if total_blocks > MAX_TOTAL_BLOCKS {
            return Err(SequenceValidationError::InvalidBlock {
                block_id: "(root)".into(),
                message: format!(
                    "sequence has {total_blocks} blocks, exceeding the maximum of {MAX_TOTAL_BLOCKS}",
                ),
            });
        }
        Ok(())
    }

    /// Collect all locally referenced handlers, including recovery and lifecycle
    /// hooks. Sub-sequence definitions must be inspected separately.
    pub fn handler_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for block in &self.blocks {
            collect_handler_names(block, &mut names);
        }
        for blocks in [&self.on_failure, &self.on_cancel].into_iter().flatten() {
            for block in blocks {
                collect_handler_names(block, &mut names);
            }
        }
        if let Some(hooks) = &self.interceptors {
            for action in [
                &hooks.before_step,
                &hooks.after_step,
                &hooks.on_signal,
                &hooks.on_complete,
                &hooks.on_failure,
            ]
            .into_iter()
            .flatten()
            {
                names.push(action.handler.clone());
            }
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
            if let Some(handler) = &s.fallback_handler {
                names.push(handler.clone());
            }
            if let Some(escalation) = &s.on_deadline_breach {
                names.push(escalation.handler.clone());
            }
            if let Some(handler) = s
                .wait_for_input
                .as_ref()
                .and_then(|input| input.escalation_handler.as_ref())
            {
                names.push(handler.clone());
            }
            if let Some(compensation) = &s.compensation {
                names.push(compensation.handler.clone());
            }
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
        BlockDefinition::Saga(saga) => {
            for step in &saga.steps {
                collect_handler_names(&step.action, names);
                if let Some(comp) = &step.compensation {
                    collect_handler_names(comp, names);
                }
            }
        }
    }
}

fn block_err(id: &str, msg: impl Into<String>) -> SequenceValidationError {
    SequenceValidationError::InvalidBlock {
        block_id: id.into(),
        message: msg.into(),
    }
}

fn check_id(
    id: &BlockId,
    seen: &mut std::collections::HashSet<String>,
) -> Result<(), SequenceValidationError> {
    if id.as_str().is_empty() {
        return Err(block_err("(empty)", "block id must not be empty"));
    }
    if !seen.insert(id.as_str().to_owned()) {
        return Err(SequenceValidationError::DuplicateBlockId(
            id.as_str().to_owned(),
        ));
    }
    Ok(())
}

fn validate_step(
    s: &StepDef,
    seen: &mut std::collections::HashSet<String>,
) -> Result<(), SequenceValidationError> {
    check_id(&s.id, seen)?;
    let id = s.id.as_str();

    if s.handler.is_empty() {
        return Err(block_err(id, "handler name must not be empty"));
    }
    if let Some(compensation) = &s.compensation {
        if compensation.handler.trim().is_empty() {
            return Err(block_err(id, "compensation.handler must not be empty"));
        }
        if compensation.depends_on.len() > 256 {
            return Err(block_err(
                id,
                "compensation.depends_on must not exceed 256 entries",
            ));
        }
        let mut dependencies = std::collections::HashSet::new();
        for dependency in &compensation.depends_on {
            if dependency == &s.id {
                return Err(block_err(
                    id,
                    "compensation cannot depend on its own effect",
                ));
            }
            if !dependencies.insert(dependency) {
                return Err(block_err(
                    id,
                    "compensation.depends_on must not contain duplicates",
                ));
            }
        }
    }

    if let Some(retry) = &s.retry {
        if retry.max_attempts == 0 {
            return Err(block_err(id, "retry.max_attempts must be > 0"));
        }
        if retry.max_attempts > MAX_RETRY_ATTEMPTS {
            return Err(block_err(
                id,
                format!("retry.max_attempts must not exceed {MAX_RETRY_ATTEMPTS}"),
            ));
        }
        if !retry.backoff_multiplier.is_finite() || retry.backoff_multiplier <= 0.0 {
            return Err(block_err(
                id,
                "retry.backoff_multiplier must be finite and > 0",
            ));
        }
        if retry.initial_backoff > retry.max_backoff {
            return Err(block_err(
                id,
                "retry.initial_backoff must be <= retry.max_backoff",
            ));
        }
        if let Some(expr) = &retry.retry_if
            && expr.trim().is_empty()
        {
            return Err(block_err(id, "retry.retry_if must not be empty"));
        }
        if let Some(codes) = &retry.non_retryable_codes
            && codes.iter().any(|c| c.trim().is_empty())
        {
            return Err(block_err(
                id,
                "retry.non_retryable_codes must not contain empty strings",
            ));
        }
    }

    if let Some(sw) = &s.send_window {
        if sw.start_hour > 23 {
            return Err(block_err(id, "send_window.start_hour must be 0-23"));
        }
        if sw.end_hour > 23 {
            return Err(block_err(id, "send_window.end_hour must be 0-23"));
        }
        if sw.start_hour == sw.end_hour {
            return Err(block_err(
                id,
                "send_window.start_hour must differ from end_hour",
            ));
        }
        for &d in &sw.days {
            if d > 6 {
                return Err(block_err(
                    id,
                    format!("send_window.days value {d} out of range 0-6"),
                ));
            }
        }
    }

    if let Some(human) = &s.wait_for_input {
        human
            .validate()
            .map_err(|message| SequenceValidationError::InvalidHumanInput {
                block_id: id.to_string(),
                message,
            })?;
    }

    Ok(())
}

/// Maximum block-tree nesting depth accepted by validation. Legitimate
/// workflows nest only a handful of levels; this cap (well below `serde_json`'s
/// own ~128 deserialization recursion limit) turns a maliciously deep tree into
/// a clean validation error instead of a stack-overflow on the worker thread
/// that traverses it.
const MAX_NESTING_DEPTH: usize = 64;

/// Maximum `Parallel`/`Race` branch count. Unbounded branches let a single
/// definition fan out tens of thousands of concurrent tasks at dispatch time.
const MAX_BRANCHES: usize = 256;

/// Maximum `Loop`/`ForEach` `max_iterations`. Without a ceiling,
/// `u32::MAX` passes validation and the engine happily starts a loop it can
/// never realistically finish.
const MAX_ITERATIONS: u32 = 100_000;

/// Maximum `retry.max_attempts`. Each attempt is a durable dispatch plus
/// backoff timer; beyond this a "retry forever" step is a stuck instance
/// rather than a recoverable one (use a loop with an explicit condition).
const MAX_RETRY_ATTEMPTS: u32 = 10_000;

/// Maximum `Loop.poll_interval` in seconds (one year). Larger values are
/// almost certainly unit mistakes (ms for s) and overflow timer arithmetic.
const MAX_POLL_INTERVAL_SECS: u64 = 365 * 24 * 60 * 60;

/// Maximum product of `max_iterations` along any chain of nested
/// `Loop`/`ForEach` blocks. Each level is capped by [`MAX_ITERATIONS`], but
/// nesting multiplies: three nested 100k loops is 10^15 body executions from
/// one definition. Two nested loops at the default `max_iterations` (1000)
/// are 10^6 and pass; deeper nesting must lower the inner bounds.
const MAX_NESTED_ITERATIONS: u64 = 10_000_000;

/// Maximum total block count across the whole tree (root + nested).
/// `SequenceDefinition::validate`'s `seen` set already tracks every unique
/// block id it visits, so its final size is exactly the block count.
const MAX_TOTAL_BLOCKS: usize = 5_000;

fn depth_err() -> SequenceValidationError {
    block_err(
        "(nested)",
        format!("block nesting exceeds the maximum depth of {MAX_NESTING_DEPTH}"),
    )
}

fn iterations_err(id: &BlockId, kind: &str) -> SequenceValidationError {
    block_err(
        id.as_str(),
        format!("{kind} max_iterations must not exceed {MAX_ITERATIONS}"),
    )
}

fn validate_branches(
    id: &BlockId,
    label: &str,
    branches: &[Vec<BlockDefinition>],
    seen: &mut std::collections::HashSet<String>,
    depth: usize,
) -> Result<(), SequenceValidationError> {
    check_id(id, seen)?;
    if branches.is_empty() {
        return Err(block_err(
            id.as_str(),
            format!("{label} must have at least one branch"),
        ));
    }
    if branches.len() > MAX_BRANCHES {
        return Err(block_err(
            id.as_str(),
            format!("{label} must not have more than {MAX_BRANCHES} branches"),
        ));
    }
    for branch in branches {
        for b in branch {
            validate_block(b, seen, depth)?;
        }
    }
    Ok(())
}

fn validate_ab_split(
    ab: &ABSplitDef,
    seen: &mut std::collections::HashSet<String>,
    depth: usize,
) -> Result<(), SequenceValidationError> {
    check_id(&ab.id, seen)?;
    if ab.variants.len() < 2 {
        return Err(block_err(
            ab.id.as_str(),
            "ab_split must have at least 2 variants",
        ));
    }
    let total_weight: u32 = ab
        .variants
        .iter()
        .fold(0u32, |acc, v| acc.saturating_add(v.weight));
    if total_weight == 0 {
        return Err(block_err(
            ab.id.as_str(),
            "ab_split total weight must be > 0",
        ));
    }
    let mut names_seen = std::collections::HashSet::new();
    for v in &ab.variants {
        if v.name.trim().is_empty() {
            return Err(block_err(
                ab.id.as_str(),
                "ab_split variant name must not be empty",
            ));
        }
        if !names_seen.insert(&v.name) {
            return Err(block_err(
                ab.id.as_str(),
                format!("ab_split duplicate variant name `{}`", v.name),
            ));
        }
        for b in &v.blocks {
            validate_block(b, seen, depth)?;
        }
    }
    Ok(())
}

fn validate_children(
    blocks: &[BlockDefinition],
    seen: &mut std::collections::HashSet<String>,
    depth: usize,
) -> Result<(), SequenceValidationError> {
    for b in blocks {
        validate_block(b, seen, depth)?;
    }
    Ok(())
}

/// Reject chains of nested loops whose iteration bounds multiply past
/// [`MAX_NESTED_ITERATIONS`]. `outer` is the product of the enclosing loops'
/// `max_iterations`.
fn check_nested_iterations(
    block: &BlockDefinition,
    outer: u64,
) -> Result<(), SequenceValidationError> {
    let (id, iterations) = match block {
        BlockDefinition::Loop(l) => (Some(&l.id), u64::from(l.max_iterations)),
        BlockDefinition::ForEach(fe) => (Some(&fe.id), u64::from(fe.max_iterations)),
        _ => (None, 1),
    };
    let product = outer.saturating_mul(iterations);
    if let Some(id) = id
        && product > MAX_NESTED_ITERATIONS
    {
        return Err(block_err(
            id.as_str(),
            format!(
                "nested loop iterations multiply to {product}, exceeding the maximum of \
                 {MAX_NESTED_ITERATIONS}; lower max_iterations on the nested loops"
            ),
        ));
    }
    let check = |blocks: &[BlockDefinition]| {
        blocks
            .iter()
            .try_for_each(|b| check_nested_iterations(b, product))
    };
    match block {
        BlockDefinition::Step(_) | BlockDefinition::SubSequence(_) => Ok(()),
        BlockDefinition::Parallel(p) => p.branches.iter().try_for_each(|b| check(b)),
        BlockDefinition::Race(r) => r.branches.iter().try_for_each(|b| check(b)),
        BlockDefinition::Loop(l) => check(&l.body),
        BlockDefinition::ForEach(fe) => check(&fe.body),
        BlockDefinition::Router(r) => {
            r.routes.iter().try_for_each(|route| check(&route.blocks))?;
            r.default.as_deref().map_or(Ok(()), check)
        }
        BlockDefinition::TryCatch(tc) => {
            check(&tc.try_block)?;
            check(&tc.catch_block)?;
            tc.finally_block.as_deref().map_or(Ok(()), check)
        }
        BlockDefinition::ABSplit(ab) => ab.variants.iter().try_for_each(|v| check(&v.blocks)),
        BlockDefinition::CancellationScope(cs) => check(&cs.blocks),
        BlockDefinition::Saga(saga) => saga.steps.iter().try_for_each(|step| {
            check_nested_iterations(&step.action, product)?;
            step.compensation
                .as_deref()
                .map_or(Ok(()), |comp| check_nested_iterations(comp, product))
        }),
    }
}

#[allow(clippy::too_many_lines)]
fn validate_block(
    block: &BlockDefinition,
    seen: &mut std::collections::HashSet<String>,
    depth: usize,
) -> Result<(), SequenceValidationError> {
    // Bound recursion before descending — `depth` is the level of `block`
    // itself; children are validated at `depth + 1`.
    if depth > MAX_NESTING_DEPTH {
        return Err(depth_err());
    }
    let child_depth = depth + 1;
    match block {
        BlockDefinition::Step(s) => validate_step(s, seen),
        BlockDefinition::Parallel(p) => {
            validate_branches(&p.id, "parallel", &p.branches, seen, child_depth)
        }
        BlockDefinition::Race(r) => {
            validate_branches(&r.id, "race", &r.branches, seen, child_depth)
        }

        BlockDefinition::Loop(l) => {
            check_id(&l.id, seen)?;
            if l.condition.trim().is_empty() {
                return Err(block_err(l.id.as_str(), "loop condition must not be empty"));
            }
            if l.body.is_empty() {
                return Err(block_err(l.id.as_str(), "loop body must not be empty"));
            }
            if l.max_iterations == 0 {
                return Err(block_err(l.id.as_str(), "loop max_iterations must be > 0"));
            }
            if l.max_iterations > MAX_ITERATIONS {
                return Err(iterations_err(&l.id, "loop"));
            }
            if l.poll_interval
                .is_some_and(|secs| secs > MAX_POLL_INTERVAL_SECS)
            {
                return Err(block_err(
                    l.id.as_str(),
                    format!("loop poll_interval must not exceed {MAX_POLL_INTERVAL_SECS} seconds"),
                ));
            }
            validate_children(&l.body, seen, child_depth)
        }

        BlockDefinition::ForEach(fe) => {
            check_id(&fe.id, seen)?;
            if fe.collection.trim().is_empty() {
                return Err(block_err(
                    fe.id.as_str(),
                    "for_each collection must not be empty",
                ));
            }
            if fe.body.is_empty() {
                return Err(block_err(fe.id.as_str(), "for_each body must not be empty"));
            }
            if fe.item_var.trim().is_empty() {
                return Err(block_err(
                    fe.id.as_str(),
                    "for_each item_var must not be empty",
                ));
            }
            if fe.max_iterations == 0 {
                return Err(block_err(
                    fe.id.as_str(),
                    "for_each max_iterations must be > 0",
                ));
            }
            if fe.max_iterations > MAX_ITERATIONS {
                return Err(iterations_err(&fe.id, "for_each"));
            }
            validate_children(&fe.body, seen, child_depth)
        }

        BlockDefinition::Router(r) => {
            check_id(&r.id, seen)?;
            if r.routes.is_empty() && r.default.is_none() {
                return Err(block_err(
                    r.id.as_str(),
                    "router must have at least one route or a default",
                ));
            }
            for route in &r.routes {
                if route.condition.trim().is_empty() {
                    return Err(block_err(
                        r.id.as_str(),
                        "router route condition must not be empty",
                    ));
                }
                validate_children(&route.blocks, seen, child_depth)?;
            }
            if let Some(default) = &r.default {
                validate_children(default, seen, child_depth)?;
            }
            Ok(())
        }

        BlockDefinition::TryCatch(tc) => {
            check_id(&tc.id, seen)?;
            if tc.try_block.is_empty() {
                return Err(block_err(
                    tc.id.as_str(),
                    "try_catch try_block must not be empty",
                ));
            }
            validate_children(&tc.try_block, seen, child_depth)?;
            validate_children(&tc.catch_block, seen, child_depth)?;
            if let Some(finally) = &tc.finally_block {
                validate_children(finally, seen, child_depth)?;
            }
            Ok(())
        }

        BlockDefinition::SubSequence(s) => {
            check_id(&s.id, seen)?;
            if s.sequence_name.trim().is_empty() {
                return Err(block_err(
                    s.id.as_str(),
                    "sub_sequence sequence_name must not be empty",
                ));
            }
            Ok(())
        }

        BlockDefinition::ABSplit(ab) => validate_ab_split(ab, seen, child_depth),

        BlockDefinition::CancellationScope(cs) => {
            check_id(&cs.id, seen)?;
            if cs.blocks.is_empty() {
                return Err(block_err(
                    cs.id.as_str(),
                    "cancellation_scope must have at least one block",
                ));
            }
            validate_children(&cs.blocks, seen, child_depth)
        }

        BlockDefinition::Saga(saga) => {
            check_id(&saga.id, seen)?;
            // An empty saga is a valid no-op (flagged by lint, not rejected
            // here) — consistent with the e2e/unit-test contract that it
            // completes immediately rather than failing sequence creation.
            for step in &saga.steps {
                check_id(&step.id, seen)?;
                validate_children(
                    std::slice::from_ref(step.action.as_ref()),
                    seen,
                    child_depth,
                )?;
                if let Some(comp) = &step.compensation {
                    validate_children(std::slice::from_ref(comp.as_ref()), seen, child_depth)?;
                }
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build `depth` nested Loop blocks, innermost wrapping a single step, so
    /// validation must recurse `depth` levels.
    fn nested_loops(depth: usize) -> BlockDefinition {
        let mut inner = BlockDefinition::Step(Box::new(StepDef {
            id: BlockId::new("leaf"),
            handler: "noop".into(),
            params: serde_json::json!({}),
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
            output_schema: None,
            when: None,
            compensation: None,
        }));
        for i in 0..depth {
            inner = BlockDefinition::Loop(Box::new(LoopDef {
                id: BlockId::new(format!("loop{i}")),
                condition: "true".into(),
                body: vec![inner],
                max_iterations: 1,
                break_on: None,
                continue_on_error: false,
                poll_interval: None,
                retain_iterations: None,
            }));
        }
        inner
    }

    fn loop_around(id: &str, max_iterations: u32, body: BlockDefinition) -> BlockDefinition {
        BlockDefinition::Loop(Box::new(LoopDef {
            id: BlockId::new(id),
            condition: "true".into(),
            body: vec![body],
            max_iterations,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
            retain_iterations: None,
        }))
    }

    #[test]
    fn validation_bounds_nested_loop_iteration_product() {
        // Two nested default-sized loops (10^6) are fine.
        let ok = loop_around("outer", 1000, loop_around("inner", 1000, nested_loops(0)));
        seq_with(ok).validate().unwrap();
        // Three nested 1000-iteration loops (10^9) are not, even though each
        // level is under MAX_ITERATIONS.
        let deep = loop_around(
            "a",
            1000,
            loop_around("b", 1000, loop_around("c", 1000, nested_loops(0))),
        );
        let err = seq_with(deep).validate().unwrap_err();
        assert!(err.to_string().contains("nested loop iterations"), "{err}");
        // The product is tracked through non-loop wrappers too.
        let wrapped = loop_around(
            "a",
            100_000,
            BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
                id: BlockId::new("scope"),
                blocks: vec![loop_around("b", 1000, nested_loops(0))],
            })),
        );
        assert!(seq_with(wrapped).validate().is_err());
    }

    #[test]
    fn validation_caps_loop_poll_interval() {
        let mut block = loop_around("l", 1, nested_loops(0));
        if let BlockDefinition::Loop(l) = &mut block {
            l.poll_interval = Some(MAX_POLL_INTERVAL_SECS + 1);
        }
        let err = seq_with(block.clone()).validate().unwrap_err();
        assert!(err.to_string().contains("poll_interval"), "{err}");
        if let BlockDefinition::Loop(l) = &mut block {
            l.poll_interval = Some(60);
        }
        seq_with(block).validate().unwrap();
    }

    #[test]
    fn validation_caps_retry_max_attempts() {
        let mut leaf = nested_loops(0);
        if let BlockDefinition::Step(step) = &mut leaf {
            step.retry = Some(
                serde_json::from_value(serde_json::json!({"max_attempts": u32::MAX})).unwrap(),
            );
        }
        let err = seq_with(leaf.clone()).validate().unwrap_err();
        assert!(
            err.to_string().contains("max_attempts must not exceed"),
            "{err}"
        );
        if let BlockDefinition::Step(step) = &mut leaf {
            step.retry = Some(
                serde_json::from_value(serde_json::json!({"max_attempts": MAX_RETRY_ATTEMPTS}))
                    .unwrap(),
            );
        }
        seq_with(leaf).validate().unwrap();
    }

    fn seq_with(block: BlockDefinition) -> SequenceDefinition {
        SequenceDefinition {
            schema: None,
            schema_version: SEQUENCE_SCHEMA_VERSION,
            id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("default"),
            name: "s".into(),
            version: 1,
            deprecated: false,
            status: SequenceStatus::default(),
            blocks: vec![block],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn validation_accepts_reasonable_nesting() {
        assert!(
            seq_with(nested_loops(10)).validate().is_ok(),
            "10 levels must validate"
        );
    }

    #[test]
    fn validation_rejects_pathological_nesting() {
        // Deep enough to exceed MAX_NESTING_DEPTH but well under serde_json's
        // own recursion limit, so it would otherwise reach the recursive
        // validator and risk a stack overflow.
        let err = seq_with(nested_loops(MAX_NESTING_DEPTH + 20))
            .validate()
            .expect_err("over-deep nesting must be rejected");
        assert!(
            format!("{err:?}").contains("nesting"),
            "error should mention nesting depth, got: {err:?}"
        );
    }

    /// H-5: `max_iterations` had a lower bound (> 0) but no upper bound, so
    /// `u32::MAX` passed validation for both `Loop` and `ForEach`.
    #[test]
    fn validation_rejects_excessive_loop_iterations() {
        let block = BlockDefinition::Loop(Box::new(LoopDef {
            id: BlockId::new("loop0"),
            condition: "true".into(),
            body: vec![BlockDefinition::Step(Box::new(StepDef {
                id: BlockId::new("leaf"),
                handler: "noop".into(),
                params: serde_json::json!({}),
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
                output_schema: None,
                when: None,
                compensation: None,
            }))],
            max_iterations: u32::MAX,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
            retain_iterations: None,
        }));
        let err = seq_with(block)
            .validate()
            .expect_err("u32::MAX iterations must be rejected");
        assert!(
            format!("{err:?}").contains("max_iterations"),
            "error should mention max_iterations, got: {err:?}"
        );
    }

    /// H-5: `Parallel`/`Race` branch count was unbounded.
    #[test]
    fn validation_rejects_excessive_branch_count() {
        let leaf = || {
            vec![BlockDefinition::Step(Box::new(StepDef {
                id: BlockId::new(format!("leaf-{}", uuid::Uuid::new_v4())),
                handler: "noop".into(),
                params: serde_json::json!({}),
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
                output_schema: None,
                when: None,
                compensation: None,
            }))]
        };
        let branches: Vec<Vec<BlockDefinition>> = (0..=MAX_BRANCHES).map(|_| leaf()).collect();
        let block = BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("p"),
            branches,
        }));
        let err = seq_with(block)
            .validate()
            .expect_err("excessive branch count must be rejected");
        assert!(
            format!("{err:?}").contains("branches"),
            "error should mention branches, got: {err:?}"
        );
    }

    /// H-5: total block count across the tree had no cap.
    #[test]
    fn validation_rejects_excessive_total_block_count() {
        // Flat, unique-id sibling steps (not nested) so this exercises the
        // total-block-count cap specifically, without also tripping
        // MAX_NESTING_DEPTH.
        let blocks: Vec<BlockDefinition> = (0..(MAX_TOTAL_BLOCKS + 10))
            .map(|i| {
                BlockDefinition::Step(Box::new(StepDef {
                    id: BlockId::new(format!("s{i}")),
                    handler: "noop".into(),
                    params: serde_json::json!({}),
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
                    output_schema: None,
                    when: None,
                    compensation: None,
                }))
            })
            .collect();
        let mut seq = seq_with(blocks[0].clone());
        seq.blocks = blocks;
        let err = seq
            .validate()
            .expect_err("excessive total block count must be rejected");
        assert!(
            format!("{err:?}").contains("blocks"),
            "error should mention block count, got: {err:?}"
        );
    }

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
    fn field_access_invalid_value_lists_accepted_forms() {
        let error = serde_json::from_str::<FieldAccess>(r#""some""#).unwrap_err();
        assert!(error.to_string().contains("unknown variant `some`"));
        assert!(error.to_string().contains("`all` or `none`"));

        let error = serde_json::from_str::<FieldAccess>(r#"{"fieldz": []}"#).unwrap_err();
        assert!(error.to_string().contains("unknown field `fieldz`"));
        assert!(error.to_string().contains("expected `fields`"));
    }

    #[test]
    fn ab_split_uses_documented_name_and_accepts_legacy_alias() {
        let block = BlockDefinition::ABSplit(Box::new(ABSplitDef {
            id: BlockId::new("experiment"),
            variants: vec![],
        }));
        let value = serde_json::to_value(&block).unwrap();
        assert_eq!(value["type"], "ab_split");

        let legacy = serde_json::json!({
            "type": "a_b_split",
            "id": "experiment",
            "variants": []
        });
        assert!(serde_json::from_value::<BlockDefinition>(legacy).is_ok());
    }

    #[test]
    fn strict_sequence_decode_rejects_unknown_fields_with_path_and_suggestion() {
        let value = serde_json::json!({
            "id": uuid::Uuid::nil(),
            "tenant_id": "tenant",
            "namespace": "default",
            "name": "typo",
            "version": 1,
            "blocks": [{
                "type": "step",
                "id": "work",
                "handler": "http",
                "retires": {"max_attempts": 3}
            }],
            "created_at": "2026-09-01T00:00:00Z"
        });

        let error = deserialize_sequence_strict(&value).unwrap_err();
        assert!(error.to_string().contains("retires"), "{error}");
        assert!(error.to_string().contains("blocks[0]"), "{error}");
        assert!(
            error.to_string().contains("did you mean \"retry\""),
            "{error}"
        );
    }

    #[test]
    fn auto_decide_defaults_validation_and_strict_typos() {
        let def: HumanInputDef = serde_json::from_value(serde_json::json!({
            "prompt": "ok?", "auto_decide": {}
        }))
        .unwrap();
        let auto = def.auto_decide.clone().unwrap();
        assert!((auto.threshold - DEFAULT_AUTO_DECIDE_THRESHOLD).abs() < f64::EPSILON);
        assert!(auto.interpret_replies);
        assert!(def.validate().is_ok());

        let mut bad = def.clone();
        bad.auto_decide = Some(Box::new(AutoDecideDef {
            threshold: 1.5,
            ..(*auto).clone()
        }));
        assert!(bad.validate().is_err());
        bad.auto_decide = Some(Box::new(AutoDecideDef {
            base_url: Some("https://proxy.example".into()),
            ..*auto
        }));
        assert!(bad.validate().is_err(), "custom endpoint needs its own key");

        let value = serde_json::json!({
            "id": uuid::Uuid::nil(),
            "tenant_id": "tenant",
            "namespace": "default",
            "name": "typo",
            "version": 1,
            "blocks": [{
                "type": "step", "id": "gate", "handler": "noop",
                "wait_for_input": {"prompt": "ok?", "auto_decide": {"threshhold": 0.8}}
            }],
            "created_at": "2026-09-01T00:00:00Z"
        });
        let error = deserialize_sequence_strict(&value).unwrap_err();
        assert!(error.to_string().contains("threshhold"), "{error}");
    }

    #[test]
    fn strict_sequence_decode_reports_nested_type_error_path() {
        let value = serde_json::json!({
            "id": uuid::Uuid::nil(),
            "tenant_id": "tenant",
            "namespace": "default",
            "name": "bad-handler",
            "version": 1,
            "blocks": [{"type": "step", "id": "ok", "handler": "http"}, {
                "type": "step",
                "id": "bad",
                "handler": 42
            }],
            "created_at": "2026-09-01T00:00:00Z"
        });

        let error = deserialize_sequence_strict(&value).unwrap_err();
        assert!(error.to_string().contains("blocks[1]"), "{error}");
        assert!(error.to_string().contains("expected a string"), "{error}");
    }

    #[test]
    fn strict_sequence_decode_reports_type_error_inside_composite() {
        let value = serde_json::json!({
            "id": uuid::Uuid::nil(),
            "tenant_id": "tenant",
            "namespace": "default",
            "name": "bad-nested-handler",
            "version": 1,
            "blocks": [{
                "type": "parallel",
                "id": "fanout",
                "branches": [[{
                    "type": "step",
                    "id": "bad",
                    "handler": 42
                }]]
            }],
            "created_at": "2026-09-01T00:00:00Z"
        });

        let error = deserialize_sequence_strict(&value).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("blocks[0].branches[0][0].handler"),
            "{error}"
        );
        assert!(error.to_string().contains("expected a string"), "{error}");
    }

    #[test]
    fn lenient_decode_reports_unknown_fields_in_nested_wrappers() {
        let value = serde_json::json!({
            "id": uuid::Uuid::nil(),
            "tenant_id": "tenant",
            "namespace": "default",
            "name": "nested-typo",
            "version": 1,
            "blocks": [{
                "type": "router",
                "id": "route",
                "routes": [{
                    "condition": "true",
                    "blokcs": [],
                    "blocks": [{
                        "type": "step",
                        "id": "work",
                        "handler": "noop",
                        "wehn": "true"
                    }]
                }]
            }],
            "created_at": "2026-09-01T00:00:00Z"
        });

        let (sequence, warnings) = deserialize_sequence_lenient(&value).unwrap();
        assert_eq!(sequence.name, "nested-typo");
        assert!(
            warnings
                .iter()
                .any(|warning| { warning.contains("blocks[0].routes[0].blokcs") })
        );
        assert!(warnings.iter().any(|warning| {
            warning.contains("blocks[0].routes[0].blocks[0].wehn")
                && warning.contains("did you mean \"when\"")
        }));
        assert!(deserialize_sequence_strict(&value).is_err());
    }

    #[allow(clippy::needless_pass_by_value)]
    fn seq_with_blocks(blocks: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "id": uuid::Uuid::nil(),
            "tenant_id": "tenant",
            "namespace": "default",
            "name": "nested",
            "version": 1,
            "blocks": blocks,
            "created_at": "2026-09-01T00:00:00Z"
        })
    }

    #[test]
    fn strict_decode_rejects_unknown_key_inside_context_access() {
        // `dta` typo used to be accepted and default to full data access.
        let value = seq_with_blocks(serde_json::json!([{
            "type": "step",
            "id": "work",
            "handler": "noop",
            "context_access": {"dta": {"fields": ["user_id"]}}
        }]));
        let error = deserialize_sequence_strict(&value).unwrap_err();
        assert!(
            error.to_string().contains("blocks[0].context_access.dta"),
            "{error}"
        );
        assert!(
            error.to_string().contains("did you mean \"data\""),
            "{error}"
        );
        let (_, warnings) = deserialize_sequence_lenient(&value).unwrap();
        assert!(warnings.iter().any(|w| w.contains("context_access.dta")));
        assert!(warnings.iter().any(|w| w.contains("no \"data\" key")));
    }

    #[test]
    fn strict_decode_rejects_context_access_without_data() {
        let value = seq_with_blocks(serde_json::json!([{
            "type": "step",
            "id": "work",
            "handler": "noop",
            "context_access": {"config": false}
        }]));
        let error = deserialize_sequence_strict(&value).unwrap_err();
        assert!(error.to_string().contains("no \"data\" key"), "{error}");
        let (sequence, warnings) = deserialize_sequence_lenient(&value).unwrap();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        // Lenient mode keeps the legacy default.
        let BlockDefinition::Step(step) = &sequence.blocks[0] else {
            panic!("expected step");
        };
        assert_eq!(step.context_access.as_ref().unwrap().data, FieldAccess::ALL);

        let explicit = seq_with_blocks(serde_json::json!([{
            "type": "step",
            "id": "work",
            "handler": "noop",
            "context_access": {"data": true, "config": false}
        }]));
        assert!(deserialize_sequence_strict(&explicit).is_ok());
    }

    #[test]
    fn strict_decode_rejects_unknown_keys_in_all_step_sub_objects() {
        for (field, value, typo_path) in [
            (
                "retry",
                serde_json::json!({"max_attempts": 2, "backof": 1}),
                "retry.backof",
            ),
            (
                "send_window",
                serde_json::json!({"start": 9}),
                "send_window.start",
            ),
            (
                "delay",
                serde_json::json!({"duration": 10, "jiter": 1}),
                "delay.jiter",
            ),
            (
                "wait_for_input",
                serde_json::json!({"promt": "x"}),
                "wait_for_input.promt",
            ),
            (
                "on_deadline_breach",
                serde_json::json!({"handler": "h", "parms": {}}),
                "on_deadline_breach.parms",
            ),
            (
                "compensation",
                serde_json::json!({"handler": "h", "depend_on": []}),
                "compensation.depend_on",
            ),
        ] {
            let mut step = serde_json::json!({"type": "step", "id": "s", "handler": "noop"});
            step[field] = value;
            // Nest it inside composites to prove recursion still works.
            let value = seq_with_blocks(serde_json::json!([{
                "type": "saga",
                "id": "saga",
                "steps": [{
                    "id": "st",
                    "action": {"type": "loop", "id": "l", "condition": "false", "body": [step]}
                }]
            }]));
            let error = deserialize_sequence_strict(&value).unwrap_err();
            let expected = format!("blocks[0].steps[0].action.body[0].{typo_path}");
            assert!(error.to_string().contains(&expected), "{field}: {error}");
        }
    }

    #[test]
    fn strict_decode_accepts_valid_nested_sub_objects() {
        let value = seq_with_blocks(serde_json::json!([{
            "type": "step",
            "id": "work",
            "handler": "noop",
            "retry": {"max_attempts": 2, "initial_backoff": 100, "retry_if": "true"},
            "send_window": {"start_hour": 8, "end_hour": 18, "days": [0, 1]},
            "context_access": {"data": {"fields": ["a"]}, "config": false},
            "wait_for_input": {"prompt": "ok?", "choices": [{"label": "Yes", "value": "y"}]}
        }, {
            "type": "a_b_split",
            "id": "ab",
            "variants": [{"name": "a", "weight": 1, "blocks": []}]
        }]));
        deserialize_sequence_strict(&value).unwrap();
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
        assert!(
            FieldAccess::Fields {
                fields: vec!["a".into()]
            }
            .allows_any()
        );
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
    fn retry_policy_has_author_friendly_backoff_defaults() {
        let retry: RetryPolicy = serde_json::from_str(r#"{"max_attempts":3}"#).unwrap();
        assert_eq!(retry.max_attempts, 3);
        assert_eq!(retry.initial_backoff, Duration::from_secs(1));
        assert_eq!(retry.max_backoff, Duration::from_secs(60));
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
            schema: None,
            schema_version: SEQUENCE_SCHEMA_VERSION,
            id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("default"),
            name: "sample".into(),
            version: 1,
            deprecated: false,
            status: SequenceStatus::default(),
            blocks,
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            created_at: chrono::Utc::now(),
        }
    }

    fn step(id: &str) -> BlockDefinition {
        BlockDefinition::Step(Box::new(StepDef {
            id: BlockId::new(id),
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
            output_schema: None,
            when: None,
            compensation: None,
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
    fn validate_rejects_duplicate_ids_within_on_failure_tree() {
        // The cleanup trees get the same per-tree checks as the main tree.
        let mut seq = sample_seq(vec![step("main")]);
        seq.on_failure = Some(vec![step("dup"), step("dup")]);
        let err = seq.validate().unwrap_err();
        assert!(matches!(err, SequenceValidationError::DuplicateBlockId(ref s) if s == "dup"));
    }

    #[test]
    fn validate_allows_id_reuse_across_cleanup_trees() {
        // Cleanup trees are dispatched independently of the main tree and of
        // each other, so a block id may appear once per tree.
        let mut seq = sample_seq(vec![step("shared")]);
        seq.on_failure = Some(vec![step("shared")]);
        seq.on_cancel = Some(vec![step("shared")]);
        assert!(seq.validate().is_ok());
    }

    #[test]
    fn validate_rejects_pathological_nesting_in_on_cancel() {
        // The nesting-depth cap must apply to cleanup trees too — otherwise a
        // maliciously deep `on_cancel` bypasses the stack-overflow guard.
        let mut seq = sample_seq(vec![step("main")]);
        seq.on_cancel = Some(vec![nested_loops(MAX_NESTING_DEPTH + 20)]);
        let err = seq
            .validate()
            .expect_err("over-deep on_cancel must be rejected");
        assert!(
            format!("{err:?}").contains("nesting"),
            "error should mention nesting depth, got: {err:?}"
        );
    }

    #[test]
    fn validate_counts_cleanup_trees_toward_total_block_cap() {
        // 1 main block + MAX_TOTAL_BLOCKS cleanup blocks = cap + 1.
        let mut seq = sample_seq(vec![step("main")]);
        seq.on_failure = Some(
            (0..MAX_TOTAL_BLOCKS)
                .map(|i| step(&format!("cleanup{i}")))
                .collect(),
        );
        let err = seq
            .validate()
            .expect_err("cleanup blocks must count toward MAX_TOTAL_BLOCKS");
        assert!(
            format!("{err:?}").contains("exceeding the maximum"),
            "error should mention the total-block cap, got: {err:?}"
        );
    }

    #[test]
    fn validate_descends_into_parallel_branches() {
        let seq = sample_seq(vec![
            step("outer"),
            BlockDefinition::Parallel(Box::new(ParallelDef {
                id: BlockId::new("par"),
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
            id: BlockId::new("tc"),
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
            id: BlockId::new("r"),
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
                id: BlockId::new("cs"),
                blocks: vec![step("shared")],
            })),
            BlockDefinition::ABSplit(Box::new(ABSplitDef {
                id: BlockId::new("ab"),
                variants: vec![
                    ABVariant {
                        name: "v1".into(),
                        weight: 1,
                        blocks: vec![step("shared")],
                    },
                    ABVariant {
                        name: "v2".into(),
                        weight: 1,
                        blocks: vec![],
                    },
                ],
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
            allow_comment: false,
            auto_decide: None,
        };
        let step_with_bad = BlockDefinition::Step(Box::new(StepDef {
            id: BlockId::new("review"),
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
            output_schema: None,
            when: None,
            compensation: None,
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
            allow_comment: false,
            auto_decide: None,
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
            allow_comment: false,
            auto_decide: None,
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
            allow_comment: false,
            auto_decide: None,
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
            allow_comment: false,
            auto_decide: None,
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
            allow_comment: false,
            auto_decide: None,
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
            allow_comment: false,
            auto_decide: None,
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
            allow_comment: false,
            auto_decide: None,
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
    fn handler_names_include_recovery_and_lifecycle_hooks() {
        let mut value = serde_json::to_value(sample_seq(vec![step("main")])).unwrap();
        value["blocks"][0]["fallback_handler"] = serde_json::json!("fallback");
        value["blocks"][0]["on_deadline_breach"] = serde_json::json!({"handler": "deadline"});
        value["blocks"][0]["wait_for_input"] =
            serde_json::json!({"escalation_handler": "human_timeout"});
        value["on_failure"] = serde_json::json!([
            {"type": "step", "id": "cleanup", "handler": "failure_cleanup"}
        ]);
        value["on_cancel"] = serde_json::json!([
            {"type": "step", "id": "cleanup", "handler": "cancel_cleanup"}
        ]);
        for hook in [
            "before_step",
            "after_step",
            "on_signal",
            "on_complete",
            "on_failure",
        ] {
            value["interceptors"][hook] = serde_json::json!({"handler": hook});
        }
        let sequence: SequenceDefinition = serde_json::from_value(value).unwrap();
        assert!(sequence.validate().is_ok());
        assert_eq!(
            sequence.handler_names(),
            [
                "after_step",
                "before_step",
                "cancel_cleanup",
                "deadline",
                "failure_cleanup",
                "fallback",
                "human_timeout",
                "noop",
                "on_complete",
                "on_failure",
                "on_signal",
            ]
        );
    }

    #[test]
    fn retry_multiplier_must_be_positive_and_finite() {
        let mut block = step("retry");
        let BlockDefinition::Step(definition) = &mut block else {
            unreachable!()
        };
        definition.retry = Some(RetryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            retry_if: None,
            non_retryable_codes: None,
        });
        for multiplier in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, -1.0] {
            definition.retry.as_mut().unwrap().backoff_multiplier = multiplier;
            assert!(
                sample_seq(vec![BlockDefinition::Step(definition.clone())])
                    .validate()
                    .is_err()
            );
        }
        for multiplier in [0.5, 1.0, 2.0] {
            definition.retry.as_mut().unwrap().backoff_multiplier = multiplier;
            assert!(
                sample_seq(vec![BlockDefinition::Step(definition.clone())])
                    .validate()
                    .is_ok()
            );
        }
    }

    #[test]
    fn handler_names_and_validation_include_compensations() {
        let mut block = step("charge");
        let BlockDefinition::Step(definition) = &mut block else {
            unreachable!();
        };
        definition.compensation = Some(StepCompensation {
            handler: "refund".into(),
            params: serde_json::Value::Null,
            depends_on: Vec::new(),
            verification: CompensationVerificationPolicy::ProviderReceipt,
        });
        let sequence = sample_seq(vec![block]);
        assert_eq!(sequence.handler_names(), vec!["noop", "refund"]);
        assert!(sequence.validate().is_ok());
    }

    #[test]
    fn validation_rejects_self_dependent_compensation() {
        let mut block = step("charge");
        let BlockDefinition::Step(definition) = &mut block else {
            unreachable!();
        };
        definition.compensation = Some(StepCompensation {
            handler: "refund".into(),
            params: serde_json::Value::Null,
            depends_on: vec![BlockId::new("charge")],
            verification: CompensationVerificationPolicy::HandlerResult,
        });
        assert!(sample_seq(vec![block]).validate().is_err());
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

    // ─── structural validation ───

    #[test]
    fn validate_rejects_empty_blocks() {
        let seq = sample_seq(vec![]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("no blocks"));
    }

    #[test]
    fn validate_rejects_empty_block_id() {
        let seq = sample_seq(vec![step("")]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn validate_rejects_empty_handler() {
        let mut s = step("s1");
        if let BlockDefinition::Step(ref mut sd) = s {
            sd.handler = String::new();
        }
        let seq = sample_seq(vec![s]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("handler name must not be empty"));
    }

    #[test]
    fn validate_rejects_empty_parallel_branches() {
        let seq = sample_seq(vec![BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("p"),
            branches: vec![],
        }))]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("at least one branch"));
    }

    #[test]
    fn validate_rejects_empty_loop_body() {
        let seq = sample_seq(vec![BlockDefinition::Loop(Box::new(LoopDef {
            id: BlockId::new("l"),
            condition: "true".into(),
            body: vec![],
            max_iterations: 10,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
            retain_iterations: None,
        }))]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("body must not be empty"));
    }

    #[test]
    fn validate_rejects_empty_loop_condition() {
        let seq = sample_seq(vec![BlockDefinition::Loop(Box::new(LoopDef {
            id: BlockId::new("l"),
            condition: "  ".into(),
            body: vec![step("s1")],
            max_iterations: 10,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
            retain_iterations: None,
        }))]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("condition must not be empty"));
    }

    #[test]
    fn validate_rejects_zero_max_iterations() {
        let seq = sample_seq(vec![BlockDefinition::Loop(Box::new(LoopDef {
            id: BlockId::new("l"),
            condition: "true".into(),
            body: vec![step("s1")],
            max_iterations: 0,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
            retain_iterations: None,
        }))]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("max_iterations must be > 0"));
    }

    #[test]
    fn validate_rejects_for_each_empty_collection() {
        let seq = sample_seq(vec![BlockDefinition::ForEach(Box::new(ForEachDef {
            id: BlockId::new("fe"),
            collection: "  ".into(),
            item_var: "item".into(),
            body: vec![step("s1")],
            max_iterations: 10,
            retain_iterations: None,
        }))]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("collection must not be empty"));
    }

    #[test]
    fn validate_rejects_for_each_empty_body() {
        let seq = sample_seq(vec![BlockDefinition::ForEach(Box::new(ForEachDef {
            id: BlockId::new("fe"),
            collection: "data.items".into(),
            item_var: "item".into(),
            body: vec![],
            max_iterations: 10,
            retain_iterations: None,
        }))]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("body must not be empty"));
    }

    #[test]
    fn validate_rejects_router_no_routes_and_no_default() {
        let seq = sample_seq(vec![BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId::new("r"),
            routes: vec![],
            default: None,
        }))]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("at least one route"));
    }

    #[test]
    fn validate_accepts_router_with_default_only() {
        let seq = sample_seq(vec![BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId::new("r"),
            routes: vec![],
            default: Some(vec![step("s1")]),
        }))]);
        assert!(seq.validate().is_ok());
    }

    #[test]
    fn validate_rejects_router_empty_condition() {
        let seq = sample_seq(vec![BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId::new("r"),
            routes: vec![Route {
                condition: String::new(),
                blocks: vec![step("s1")],
            }],
            default: None,
        }))]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("condition must not be empty"));
    }

    #[test]
    fn validate_rejects_try_catch_empty_try() {
        let seq = sample_seq(vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
            id: BlockId::new("tc"),
            try_block: vec![],
            catch_block: vec![step("c1")],
            finally_block: None,
        }))]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("try_block must not be empty"));
    }

    #[test]
    fn validate_rejects_ab_split_one_variant() {
        let seq = sample_seq(vec![BlockDefinition::ABSplit(Box::new(ABSplitDef {
            id: BlockId::new("ab"),
            variants: vec![ABVariant {
                name: "only".into(),
                weight: 1,
                blocks: vec![],
            }],
        }))]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("at least 2 variants"));
    }

    #[test]
    fn validate_rejects_ab_split_zero_total_weight() {
        let seq = sample_seq(vec![BlockDefinition::ABSplit(Box::new(ABSplitDef {
            id: BlockId::new("ab"),
            variants: vec![
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
            ],
        }))]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("total weight must be > 0"));
    }

    #[test]
    fn validate_rejects_ab_split_duplicate_variant_names() {
        let seq = sample_seq(vec![BlockDefinition::ABSplit(Box::new(ABSplitDef {
            id: BlockId::new("ab"),
            variants: vec![
                ABVariant {
                    name: "v1".into(),
                    weight: 1,
                    blocks: vec![],
                },
                ABVariant {
                    name: "v1".into(),
                    weight: 1,
                    blocks: vec![],
                },
            ],
        }))]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("duplicate variant name"));
    }

    #[test]
    fn validate_rejects_ab_split_empty_variant_name() {
        let seq = sample_seq(vec![BlockDefinition::ABSplit(Box::new(ABSplitDef {
            id: BlockId::new("ab"),
            variants: vec![
                ABVariant {
                    name: String::new(),
                    weight: 1,
                    blocks: vec![],
                },
                ABVariant {
                    name: "b".into(),
                    weight: 1,
                    blocks: vec![],
                },
            ],
        }))]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("variant name must not be empty"));
    }

    #[test]
    fn validate_rejects_sub_sequence_empty_name() {
        let seq = sample_seq(vec![BlockDefinition::SubSequence(Box::new(
            SubSequenceDef {
                id: BlockId::new("ss"),
                sequence_name: "  ".into(),
                version: None,
                input: serde_json::Value::Null,
            },
        ))]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("sequence_name must not be empty"));
    }

    #[test]
    fn validate_rejects_cancellation_scope_empty_blocks() {
        let seq = sample_seq(vec![BlockDefinition::CancellationScope(Box::new(
            CancellationScopeDef {
                id: BlockId::new("cs"),
                blocks: vec![],
            },
        ))]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("at least one block"));
    }

    #[test]
    fn validate_rejects_retry_zero_max_attempts() {
        let mut s = step("s1");
        if let BlockDefinition::Step(ref mut sd) = s {
            sd.retry = Some(RetryPolicy {
                max_attempts: 0,
                initial_backoff: Duration::from_secs(1),
                max_backoff: Duration::from_secs(10),
                backoff_multiplier: 2.0,
                retry_if: None,
                non_retryable_codes: None,
            });
        }
        let seq = sample_seq(vec![s]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("max_attempts must be > 0"));
    }

    #[test]
    fn validate_rejects_retry_initial_exceeds_max_backoff() {
        let mut s = step("s1");
        if let BlockDefinition::Step(ref mut sd) = s {
            sd.retry = Some(RetryPolicy {
                max_attempts: 3,
                initial_backoff: Duration::from_secs(60),
                max_backoff: Duration::from_secs(10),
                backoff_multiplier: 2.0,
                retry_if: None,
                non_retryable_codes: None,
            });
        }
        let seq = sample_seq(vec![s]);
        let err = seq.validate().unwrap_err();
        assert!(
            err.to_string()
                .contains("initial_backoff must be <= retry.max_backoff")
        );
    }

    #[test]
    fn validate_rejects_send_window_invalid_hours() {
        let mut s = step("s1");
        if let BlockDefinition::Step(ref mut sd) = s {
            sd.send_window = Some(SendWindow {
                start_hour: 25,
                end_hour: 17,
                days: vec![],
            });
        }
        let seq = sample_seq(vec![s]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("start_hour must be 0-23"));
    }

    #[test]
    fn validate_rejects_send_window_same_start_end() {
        let mut s = step("s1");
        if let BlockDefinition::Step(ref mut sd) = s {
            sd.send_window = Some(SendWindow {
                start_hour: 9,
                end_hour: 9,
                days: vec![],
            });
        }
        let seq = sample_seq(vec![s]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("must differ from end_hour"));
    }

    #[test]
    fn validate_rejects_send_window_invalid_day() {
        let mut s = step("s1");
        if let BlockDefinition::Step(ref mut sd) = s {
            sd.send_window = Some(SendWindow {
                start_hour: 9,
                end_hour: 17,
                days: vec![0, 7],
            });
        }
        let seq = sample_seq(vec![s]);
        let err = seq.validate().unwrap_err();
        assert!(err.to_string().contains("out of range 0-6"));
    }

    #[test]
    fn validate_accepts_valid_retry() {
        let mut s = step("s1");
        if let BlockDefinition::Step(ref mut sd) = s {
            sd.retry = Some(RetryPolicy {
                max_attempts: 3,
                initial_backoff: Duration::from_secs(1),
                max_backoff: Duration::from_secs(30),
                backoff_multiplier: 2.0,
                retry_if: None,
                non_retryable_codes: None,
            });
        }
        let seq = sample_seq(vec![s]);
        assert!(seq.validate().is_ok());
    }

    #[test]
    fn validate_accepts_valid_send_window() {
        let mut s = step("s1");
        if let BlockDefinition::Step(ref mut sd) = s {
            sd.send_window = Some(SendWindow {
                start_hour: 9,
                end_hour: 17,
                days: vec![0, 1, 2, 3, 4],
            });
        }
        let seq = sample_seq(vec![s]);
        assert!(seq.validate().is_ok());
    }

    #[test]
    fn sequence_status_default_is_production() {
        assert_eq!(SequenceStatus::default(), SequenceStatus::Production);
    }

    #[test]
    fn sequence_status_display_and_parse() {
        for status in [
            SequenceStatus::Draft,
            SequenceStatus::Staging,
            SequenceStatus::Production,
            SequenceStatus::Unpublished,
        ] {
            let s = status.to_string();
            let parsed: SequenceStatus = s.parse().unwrap();
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn sequence_status_parse_unknown_fails() {
        assert!("bogus".parse::<SequenceStatus>().is_err());
    }

    #[test]
    fn sequence_status_valid_transitions() {
        assert!(SequenceStatus::Draft.can_transition_to(SequenceStatus::Staging));
        assert!(SequenceStatus::Draft.can_transition_to(SequenceStatus::Unpublished));
        assert!(!SequenceStatus::Draft.can_transition_to(SequenceStatus::Production));

        assert!(SequenceStatus::Staging.can_transition_to(SequenceStatus::Production));
        assert!(SequenceStatus::Staging.can_transition_to(SequenceStatus::Unpublished));
        assert!(!SequenceStatus::Staging.can_transition_to(SequenceStatus::Draft));

        assert!(SequenceStatus::Production.can_transition_to(SequenceStatus::Unpublished));
        assert!(!SequenceStatus::Production.can_transition_to(SequenceStatus::Draft));
        assert!(!SequenceStatus::Production.can_transition_to(SequenceStatus::Staging));

        assert!(SequenceStatus::Unpublished.valid_transitions().is_empty());
    }

    #[test]
    fn sequence_status_serde_round_trip() {
        let status = SequenceStatus::Staging;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, r#""staging""#);
        let parsed: SequenceStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, status);
    }

    #[test]
    fn sequence_definition_default_status_deserialization() {
        let json = r#"{
            "id": "00000000-0000-0000-0000-000000000001",
            "tenant_id": "t1",
            "namespace": "default",
            "name": "test",
            "version": 1,
            "deprecated": false,
            "blocks": [],
            "created_at": "2025-01-01T00:00:00Z"
        }"#;
        let seq: SequenceDefinition = serde_json::from_str(json).unwrap();
        assert_eq!(seq.status, SequenceStatus::Production);
    }
}
