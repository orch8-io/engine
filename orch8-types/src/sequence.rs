use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use utoipa::ToSchema;

use crate::finding::{Confidence, Evidence, Finding, FindingSeverity, ResourceRef};
use crate::ids::{BlockId, Namespace, SequenceId, TenantId};
use std::collections::HashSet;

/// Lifecycle status for sequences: Draft → Staging → Production → Unpublished.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema, JsonSchema)]
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

/// A versioned workflow definition — the root of the sequence JSON document.
///
/// `id` and `created_at` are optional on input: a missing `id` gets a fresh
/// UUID v7 and a missing `created_at` is stamped with the server clock, so
/// authors (human or LLM) never have to invent either. Both are always
/// present on output, so stored rows round-trip unchanged.
///
/// The document may also carry a top-level `schema_version` (see
/// [`crate::sequence_parse::SEQUENCE_SCHEMA_VERSION`]); it is checked and
/// consumed by [`crate::sequence_parse::parse_sequence_value`] and is not a
/// field of this struct.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct SequenceDefinition {
    /// Unique id of this sequence *version*. Defaults to a new UUID v7 when
    /// omitted on input.
    #[serde(default = "SequenceId::new")]
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
    /// Creation timestamp. Defaults to "now" when omitted on input.
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,
}

/// Alert-only service-level-agreement policy for a sequence. A breach is a
/// signal, not a state change: it fires one webhook + metric per breach kind
/// and leaves the instance running. Both bounds are optional.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct SlaPolicy {
    /// Maximum wall-clock lifetime of an instance, measured from `created_at`.
    /// One `max_runtime` alert per instance.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_duration_opt"
    )]
    #[schema(value_type = Option<u64>)]
    #[schemars(with = "Option<u64>")]
    pub max_runtime: Option<Duration>,
    /// Maximum wall-clock time the current step may stay running/waiting,
    /// measured from when it started. One alert per breaching step.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_duration_opt"
    )]
    #[schema(value_type = Option<u64>)]
    #[schemars(with = "Option<u64>")]
    pub max_step_runtime: Option<Duration>,
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
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
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
    ///
    /// Wire tag is `ab_split` (what the docs, dashboard and `BlockType`
    /// always used); `a_b_split` — the tag `rename_all = "snake_case"` used
    /// to derive — is still accepted so previously stored rows load.
    #[serde(rename = "ab_split", alias = "a_b_split")]
    ABSplit(Box<ABSplitDef>),
    /// Cancellation scope: child blocks cannot be cancelled by external cancel signals.
    /// Provides subtree-level non-cancellability (Temporal-style structured concurrency).
    CancellationScope(Box<CancellationScopeDef>),
    /// Saga: sequential steps with compensating actions, rolled back in
    /// reverse order if any step's action fails.
    Saga(Box<SagaDef>),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
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
    #[schemars(with = "Option<u64>")]
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
    #[schemars(with = "Option<u64>")]
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema, JsonSchema)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema, JsonSchema)]
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct DelaySpec {
    #[serde(with = "crate::serde_duration")]
    #[schema(value_type = u64)]
    #[schemars(with = "u64")]
    pub duration: Duration,
    #[serde(default)]
    pub business_days_only: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_duration_opt"
    )]
    #[schema(value_type = Option<u64>)]
    #[schemars(with = "Option<u64>")]
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
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
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
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
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
///
/// Deserialization is hand-written (see the `Deserialize` impl below) so a
/// malformed value produces one error that lists every accepted form instead
/// of serde's opaque "data did not match any variant of untagged enum".
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

/// String form of [`FieldAccess`] for human-authored YAML/JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum AccessKeyword {
    All,
    None,
}

/// The accepted spellings of a [`FieldAccess`] value, quoted in error
/// messages and in the JSON Schema description.
pub const FIELD_ACCESS_ACCEPTED_FORMS: &str =
    r#"true, false, "all", "none", or {"fields": ["key", ...]}"#;

impl<'de> Deserialize<'de> for FieldAccess {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        let value = serde_json::Value::deserialize(d)?;
        let invalid = |got: &str| {
            D::Error::custom(format!(
                "invalid field access {got}: expected {FIELD_ACCESS_ACCEPTED_FORMS}"
            ))
        };
        match value {
            serde_json::Value::Bool(b) => Ok(Self::Bool(b)),
            serde_json::Value::String(s) => match s.as_str() {
                "all" => Ok(Self::Keyword(AccessKeyword::All)),
                "none" => Ok(Self::Keyword(AccessKeyword::None)),
                other => Err(invalid(&format!("\"{other}\""))),
            },
            serde_json::Value::Object(map) => {
                let mut fields = None;
                for (key, val) in map {
                    if key != "fields" {
                        return Err(invalid(&format!("with unknown key \"{key}\"")));
                    }
                    let list = val
                        .as_array()
                        .ok_or_else(|| invalid("(\"fields\" must be an array of strings)"))?;
                    let mut out = Vec::with_capacity(list.len());
                    for item in list {
                        match item.as_str() {
                            Some(s) => out.push(s.to_owned()),
                            None => {
                                return Err(invalid(
                                    "(\"fields\" must be an array of strings)",
                                ));
                            }
                        }
                    }
                    fields = Some(out);
                }
                fields
                    .map(|fields| Self::Fields { fields })
                    .ok_or_else(|| invalid("{} (missing \"fields\")"))
            }
            other => Err(invalid(&other.to_string())),
        }
    }
}

impl JsonSchema for FieldAccess {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "FieldAccess".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": format!("Field-level access to a context section: {FIELD_ACCESS_ACCEPTED_FORMS}"),
            "oneOf": [
                { "type": "boolean" },
                { "type": "string", "enum": ["all", "none"] },
                {
                    "type": "object",
                    "properties": { "fields": { "type": "array", "items": { "type": "string" } } },
                    "required": ["fields"],
                    "additionalProperties": false
                }
            ]
        })
    }
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
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema, PartialEq, Eq)]
pub struct HumanChoice {
    /// Display text shown in the UI.
    pub label: String,
    /// Stable identifier stored in context and used for router matching.
    pub value: String,
}

/// Configuration for human-in-the-loop steps.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct HumanInputDef {
    /// Prompt or instructions for the human reviewer.
    #[serde(default)]
    pub prompt: String,
    /// Timeout in **milliseconds** before the step fails or escalates
    /// (same integer-millisecond encoding as every other duration).
    /// If omitted, waits indefinitely.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_duration_opt"
    )]
    #[schema(value_type = Option<u64>)]
    #[schemars(with = "Option<u64>")]
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
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct EscalationDef {
    /// Handler name to invoke on breach (e.g. `"notify_slack"`, `"send_alert"`).
    pub handler: String,
    /// Parameters passed to the escalation handler.
    #[serde(default)]
    pub params: serde_json::Value,
}

/// Automatic retry/backoff policy for a step. Only `max_attempts` is
/// required: `{"max_attempts": 3}` retries with the documented defaults
/// (1 s initial backoff, 60 s cap, ×2 multiplier, jitter on).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    /// Backoff before the second attempt, in milliseconds. Default 1000.
    #[serde(default = "default_initial_backoff", with = "crate::serde_duration")]
    #[schema(value_type = u64)]
    #[schemars(with = "u64")]
    pub initial_backoff: Duration,
    /// Upper bound on the backoff, in milliseconds. Default 60000.
    #[serde(default = "default_max_backoff", with = "crate::serde_duration")]
    #[schema(value_type = u64)]
    #[schemars(with = "u64")]
    pub max_backoff: Duration,
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
    /// Randomise each backoff by up to ±25% so retries from many instances
    /// do not synchronise. Default `true` when omitted (`None`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jitter: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_if: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub non_retryable_codes: Option<Vec<String>>,
}

const fn default_backoff_multiplier() -> f64 {
    2.0
}

/// Default `RetryPolicy::initial_backoff`: 1 second.
pub const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(1000);
/// Default `RetryPolicy::max_backoff`: 60 seconds.
pub const DEFAULT_MAX_BACKOFF: Duration = Duration::from_millis(60_000);

const fn default_initial_backoff() -> Duration {
    DEFAULT_INITIAL_BACKOFF
}

const fn default_max_backoff() -> Duration {
    DEFAULT_MAX_BACKOFF
}

impl RetryPolicy {
    /// Effective jitter setting (`jitter` defaults to `true` when omitted).
    #[must_use]
    pub fn jitter_enabled(&self) -> bool {
        self.jitter.unwrap_or(true)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct ParallelDef {
    pub id: BlockId,
    pub branches: Vec<Vec<BlockDefinition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct RaceDef {
    pub id: BlockId,
    pub branches: Vec<Vec<BlockDefinition>>,
    #[serde(default)]
    pub semantics: RaceSemantics,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RaceSemantics {
    #[default]
    FirstToResolve,
    FirstToSucceed,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct TryCatchDef {
    pub id: BlockId,
    pub try_block: Vec<BlockDefinition>,
    pub catch_block: Vec<BlockDefinition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finally_block: Option<Vec<BlockDefinition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
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
    /// Wait this many **seconds** between iterations: after each iteration
    /// the instance is rescheduled to `now + poll_interval` instead of
    /// re-evaluating the condition immediately. Note the unit — this is the
    /// one sequence field expressed in seconds rather than milliseconds
    /// (kept for wire compatibility with existing definitions).
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct SagaDef {
    pub id: BlockId,
    pub steps: Vec<SagaStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct RouterDef {
    pub id: BlockId,
    pub routes: Vec<Route>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Vec<BlockDefinition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct Route {
    pub condition: String,
    pub blocks: Vec<BlockDefinition>,
}

/// Invoke another sequence as a child workflow.
/// The child instance is created and linked; the parent waits for completion.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
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
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
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
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct CancellationScopeDef {
    pub id: BlockId,
    /// Child blocks protected from cancellation.
    pub blocks: Vec<BlockDefinition>,
}

/// One arm of an A/B split.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
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

/// Stable machine codes carried by the [`Finding`]s that
/// [`SequenceDefinition::validate_all`] produces. Clients may switch on
/// these; the human-readable summaries may change, the codes will not.
pub mod validation_codes {
    /// `blocks` is empty.
    pub const SEQ_NO_BLOCKS: &str = "SEQ_NO_BLOCKS";
    /// Total block count (root + nested + cleanup trees) exceeds the cap.
    pub const SEQ_TOO_MANY_BLOCKS: &str = "SEQ_TOO_MANY_BLOCKS";
    /// The same block id appears twice within one block tree.
    pub const SEQ_DUPLICATE_BLOCK_ID: &str = "SEQ_DUPLICATE_BLOCK_ID";
    /// A block id is the empty string.
    pub const SEQ_EMPTY_BLOCK_ID: &str = "SEQ_EMPTY_BLOCK_ID";
    /// Block nesting exceeds the maximum depth.
    pub const SEQ_NESTING_TOO_DEEP: &str = "SEQ_NESTING_TOO_DEEP";
    /// A step's `handler` is empty.
    pub const SEQ_EMPTY_HANDLER: &str = "SEQ_EMPTY_HANDLER";
    /// A step's `compensation` is malformed.
    pub const SEQ_INVALID_COMPENSATION: &str = "SEQ_INVALID_COMPENSATION";
    /// A step's `retry` policy is malformed.
    pub const SEQ_INVALID_RETRY: &str = "SEQ_INVALID_RETRY";
    /// A step's `send_window` is malformed.
    pub const SEQ_INVALID_SEND_WINDOW: &str = "SEQ_INVALID_SEND_WINDOW";
    /// A step's `wait_for_input` is malformed.
    pub const SEQ_INVALID_HUMAN_INPUT: &str = "SEQ_INVALID_HUMAN_INPUT";
    /// `parallel`/`race` has no branches.
    pub const SEQ_EMPTY_BRANCHES: &str = "SEQ_EMPTY_BRANCHES";
    /// `parallel`/`race` has more branches than the cap.
    pub const SEQ_TOO_MANY_BRANCHES: &str = "SEQ_TOO_MANY_BRANCHES";
    /// A `loop` condition or router route condition is empty.
    pub const SEQ_EMPTY_CONDITION: &str = "SEQ_EMPTY_CONDITION";
    /// A block body that must not be empty is empty (`loop.body`,
    /// `for_each.body`, `try_catch.try_block`, `cancellation_scope.blocks`).
    pub const SEQ_EMPTY_BODY: &str = "SEQ_EMPTY_BODY";
    /// `max_iterations` is zero or above the cap.
    pub const SEQ_INVALID_MAX_ITERATIONS: &str = "SEQ_INVALID_MAX_ITERATIONS";
    /// `for_each.collection` is empty.
    pub const SEQ_EMPTY_COLLECTION: &str = "SEQ_EMPTY_COLLECTION";
    /// `for_each.item_var` is empty.
    pub const SEQ_EMPTY_ITEM_VAR: &str = "SEQ_EMPTY_ITEM_VAR";
    /// A router has neither routes nor a default.
    pub const SEQ_ROUTER_NO_ROUTES: &str = "SEQ_ROUTER_NO_ROUTES";
    /// `sub_sequence.sequence_name` is empty.
    pub const SEQ_EMPTY_SEQUENCE_NAME: &str = "SEQ_EMPTY_SEQUENCE_NAME";
    /// An `ab_split` has too few variants, zero total weight, or bad names.
    pub const SEQ_INVALID_AB_SPLIT: &str = "SEQ_INVALID_AB_SPLIT";
    /// The document's `schema_version` is newer than this build supports
    /// (raised by `sequence_parse`, listed here so all codes live together).
    pub const SEQ_SCHEMA_VERSION_UNSUPPORTED: &str = "SEQ_SCHEMA_VERSION_UNSUPPORTED";
}

use validation_codes as codes;

/// Which `SequenceValidationError` variant an issue maps to when reported
/// through the fail-fast [`SequenceDefinition::validate`] API.
#[derive(Debug, Clone, Copy)]
enum IssueKind {
    DuplicateBlockId,
    HumanInput,
    Block,
}

/// One structural problem, collected by the validator walk.
#[derive(Debug, Clone)]
struct Issue {
    code: &'static str,
    /// Location in the JSON document, e.g. `blocks[2].retry.max_attempts`.
    path: String,
    block_id: String,
    message: String,
    kind: IssueKind,
}

impl Issue {
    fn to_error(&self) -> SequenceValidationError {
        match self.kind {
            IssueKind::DuplicateBlockId => {
                SequenceValidationError::DuplicateBlockId(self.block_id.clone())
            }
            IssueKind::HumanInput => SequenceValidationError::InvalidHumanInput {
                block_id: self.block_id.clone(),
                message: self.message.clone(),
            },
            IssueKind::Block => SequenceValidationError::InvalidBlock {
                block_id: self.block_id.clone(),
                message: self.message.clone(),
            },
        }
    }

    fn into_finding(self, now: DateTime<Utc>) -> Finding {
        let summary = self.to_error().to_string();
        Finding::new(
            self.code,
            FindingSeverity::Error,
            summary,
            Confidence::Certain,
            now,
        )
        .with_resource(ResourceRef::new("block", self.block_id))
        .with_evidence(Evidence::new("path", self.path))
    }
}

struct Issues(Vec<Issue>);

impl Issues {
    fn block(
        &mut self,
        code: &'static str,
        path: impl Into<String>,
        block_id: &str,
        message: impl Into<String>,
    ) {
        self.0.push(Issue {
            code,
            path: path.into(),
            block_id: block_id.to_owned(),
            message: message.into(),
            kind: IssueKind::Block,
        });
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
];

impl SequenceDefinition {
    /// Structural validation performed at submit time (before the sequence
    /// reaches storage). Fail-fast: returns the first problem found, in
    /// document order. Use [`Self::validate_all`] to get every problem.
    pub fn validate(&self) -> Result<(), SequenceValidationError> {
        match self.collect_issues().first() {
            None => Ok(()),
            Some(issue) => Err(issue.to_error()),
        }
    }

    /// Collect **every** structural problem instead of stopping at the
    /// first. Each finding carries a stable `code` (see
    /// [`validation_codes`]), a `path` evidence entry locating the problem in
    /// the JSON document (e.g. `blocks[2].retry.max_attempts`), and the
    /// offending block as `affected_resource`. An empty vector means the
    /// definition is valid. `now` stamps `observed_at` so callers on a
    /// virtual clock stay deterministic.
    pub fn validate_all_at(&self, now: DateTime<Utc>) -> Vec<Finding> {
        self.collect_issues()
            .into_iter()
            .map(|issue| issue.into_finding(now))
            .collect()
    }

    /// [`Self::validate_all_at`] stamped with the wall clock.
    pub fn validate_all(&self) -> Vec<Finding> {
        self.validate_all_at(Utc::now())
    }

    fn collect_issues(&self) -> Vec<Issue> {
        let mut issues = Issues(Vec::new());
        if self.blocks.is_empty() {
            issues.block(codes::SEQ_NO_BLOCKS, "blocks", "(root)", "sequence has no blocks");
        }
        let mut seen = HashSet::new();
        for (i, block) in self.blocks.iter().enumerate() {
            validate_block(block, &format!("blocks[{i}]"), &mut seen, &mut issues, 0);
        }
        // `seen` gained exactly one entry per unique block id visited above
        // (duplicates are reported by `check_id`), so its size is the total
        // block count across the whole tree.
        let mut total_blocks = seen.len();
        // The cleanup trees (`on_failure` / `on_cancel`) are dispatched
        // independently of the main tree and of each other, so block ids may
        // be reused across trees — each gets its own `seen` set. They still
        // need the same per-block checks (nesting depth, branch/iteration
        // caps, handler sanity) and still count toward the total-block cap,
        // or the DoS bounds are trivially bypassed via these fields.
        for (field, cleanup_tree) in [("on_failure", &self.on_failure), ("on_cancel", &self.on_cancel)]
        {
            let Some(cleanup_tree) = cleanup_tree else {
                continue;
            };
            let mut cleanup_seen = HashSet::new();
            for (i, block) in cleanup_tree.iter().enumerate() {
                validate_block(
                    block,
                    &format!("{field}[{i}]"),
                    &mut cleanup_seen,
                    &mut issues,
                    0,
                );
            }
            total_blocks += cleanup_seen.len();
        }
        if total_blocks > MAX_TOTAL_BLOCKS {
            issues.block(
                codes::SEQ_TOO_MANY_BLOCKS,
                "blocks",
                "(root)",
                format!(
                    "sequence has {total_blocks} blocks, exceeding the maximum of {MAX_TOTAL_BLOCKS}",
                ),
            );
        }
        issues.0
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

/// Register a block id. Reports an empty or duplicate id; returns whether
/// the id was fresh. Validation of the block's contents continues either
/// way so one bad id does not hide the rest of the report.
fn check_id(id: &BlockId, path: &str, seen: &mut HashSet<String>, issues: &mut Issues) -> bool {
    if id.as_str().is_empty() {
        issues.block(
            codes::SEQ_EMPTY_BLOCK_ID,
            format!("{path}.id"),
            "(empty)",
            "block id must not be empty",
        );
        return false;
    }
    if !seen.insert(id.as_str().to_owned()) {
        issues.0.push(Issue {
            code: codes::SEQ_DUPLICATE_BLOCK_ID,
            path: format!("{path}.id"),
            block_id: id.as_str().to_owned(),
            message: format!("duplicate block id: {id}"),
            kind: IssueKind::DuplicateBlockId,
        });
        return false;
    }
    true
}

#[allow(clippy::too_many_lines)] // one linear pass over every StepDef field; splitting it hides the field order.
fn validate_step(s: &StepDef, path: &str, seen: &mut HashSet<String>, issues: &mut Issues) {
    check_id(&s.id, path, seen, issues);
    let id = s.id.as_str();

    if s.handler.is_empty() {
        issues.block(
            codes::SEQ_EMPTY_HANDLER,
            format!("{path}.handler"),
            id,
            "handler name must not be empty",
        );
    }
    if let Some(compensation) = &s.compensation {
        if compensation.handler.trim().is_empty() {
            issues.block(
                codes::SEQ_INVALID_COMPENSATION,
                format!("{path}.compensation.handler"),
                id,
                "compensation.handler must not be empty",
            );
        }
        if compensation.depends_on.len() > 256 {
            issues.block(
                codes::SEQ_INVALID_COMPENSATION,
                format!("{path}.compensation.depends_on"),
                id,
                "compensation.depends_on must not exceed 256 entries",
            );
        }
        let mut dependencies = HashSet::new();
        for (i, dependency) in compensation.depends_on.iter().enumerate() {
            if dependency == &s.id {
                issues.block(
                    codes::SEQ_INVALID_COMPENSATION,
                    format!("{path}.compensation.depends_on[{i}]"),
                    id,
                    "compensation cannot depend on its own effect",
                );
            }
            if !dependencies.insert(dependency) {
                issues.block(
                    codes::SEQ_INVALID_COMPENSATION,
                    format!("{path}.compensation.depends_on[{i}]"),
                    id,
                    "compensation.depends_on must not contain duplicates",
                );
            }
        }
    }

    if let Some(retry) = &s.retry {
        if retry.max_attempts == 0 {
            issues.block(
                codes::SEQ_INVALID_RETRY,
                format!("{path}.retry.max_attempts"),
                id,
                "retry.max_attempts must be > 0",
            );
        }
        if retry.backoff_multiplier <= 0.0 {
            issues.block(
                codes::SEQ_INVALID_RETRY,
                format!("{path}.retry.backoff_multiplier"),
                id,
                "retry.backoff_multiplier must be > 0",
            );
        }
        if retry.initial_backoff > retry.max_backoff {
            issues.block(
                codes::SEQ_INVALID_RETRY,
                format!("{path}.retry.initial_backoff"),
                id,
                "retry.initial_backoff must be <= retry.max_backoff",
            );
        }
        if let Some(expr) = &retry.retry_if
            && expr.trim().is_empty()
        {
            issues.block(
                codes::SEQ_INVALID_RETRY,
                format!("{path}.retry.retry_if"),
                id,
                "retry.retry_if must not be empty",
            );
        }
        if let Some(codes_list) = &retry.non_retryable_codes
            && codes_list.iter().any(|c| c.trim().is_empty())
        {
            issues.block(
                codes::SEQ_INVALID_RETRY,
                format!("{path}.retry.non_retryable_codes"),
                id,
                "retry.non_retryable_codes must not contain empty strings",
            );
        }
    }

    if let Some(sw) = &s.send_window {
        if sw.start_hour > 23 {
            issues.block(
                codes::SEQ_INVALID_SEND_WINDOW,
                format!("{path}.send_window.start_hour"),
                id,
                "send_window.start_hour must be 0-23",
            );
        }
        if sw.end_hour > 23 {
            issues.block(
                codes::SEQ_INVALID_SEND_WINDOW,
                format!("{path}.send_window.end_hour"),
                id,
                "send_window.end_hour must be 0-23",
            );
        }
        if sw.start_hour == sw.end_hour {
            issues.block(
                codes::SEQ_INVALID_SEND_WINDOW,
                format!("{path}.send_window.end_hour"),
                id,
                "send_window.start_hour must differ from end_hour",
            );
        }
        for (i, &d) in sw.days.iter().enumerate() {
            if d > 6 {
                issues.block(
                    codes::SEQ_INVALID_SEND_WINDOW,
                    format!("{path}.send_window.days[{i}]"),
                    id,
                    format!("send_window.days value {d} out of range 0-6"),
                );
            }
        }
    }

    if let Some(human) = &s.wait_for_input
        && let Err(message) = human.validate()
    {
        issues.0.push(Issue {
            code: codes::SEQ_INVALID_HUMAN_INPUT,
            path: format!("{path}.wait_for_input"),
            block_id: id.to_owned(),
            message,
            kind: IssueKind::HumanInput,
        });
    }
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

/// Maximum total block count across the whole tree (root + nested).
/// `SequenceDefinition::validate`'s `seen` set already tracks every unique
/// block id it visits, so its final size is exactly the block count.
const MAX_TOTAL_BLOCKS: usize = 5_000;

fn check_max_iterations(
    id: &BlockId,
    kind: &str,
    max_iterations: u32,
    path: &str,
    issues: &mut Issues,
) {
    if max_iterations == 0 {
        issues.block(
            codes::SEQ_INVALID_MAX_ITERATIONS,
            format!("{path}.max_iterations"),
            id.as_str(),
            format!("{kind} max_iterations must be > 0"),
        );
    } else if max_iterations > MAX_ITERATIONS {
        issues.block(
            codes::SEQ_INVALID_MAX_ITERATIONS,
            format!("{path}.max_iterations"),
            id.as_str(),
            format!("{kind} max_iterations must not exceed {MAX_ITERATIONS}"),
        );
    }
}

fn validate_branches(
    id: &BlockId,
    label: &str,
    branches: &[Vec<BlockDefinition>],
    path: &str,
    seen: &mut HashSet<String>,
    issues: &mut Issues,
    depth: usize,
) {
    check_id(id, path, seen, issues);
    if branches.is_empty() {
        issues.block(
            codes::SEQ_EMPTY_BRANCHES,
            format!("{path}.branches"),
            id.as_str(),
            format!("{label} must have at least one branch"),
        );
    }
    if branches.len() > MAX_BRANCHES {
        issues.block(
            codes::SEQ_TOO_MANY_BRANCHES,
            format!("{path}.branches"),
            id.as_str(),
            format!("{label} must not have more than {MAX_BRANCHES} branches"),
        );
    }
    for (i, branch) in branches.iter().enumerate() {
        validate_children(branch, &format!("{path}.branches[{i}]"), seen, issues, depth);
    }
}

fn validate_ab_split(
    ab: &ABSplitDef,
    path: &str,
    seen: &mut HashSet<String>,
    issues: &mut Issues,
    depth: usize,
) {
    check_id(&ab.id, path, seen, issues);
    let id = ab.id.as_str();
    if ab.variants.len() < 2 {
        issues.block(
            codes::SEQ_INVALID_AB_SPLIT,
            format!("{path}.variants"),
            id,
            "ab_split must have at least 2 variants",
        );
    }
    let total_weight: u32 = ab
        .variants
        .iter()
        .fold(0u32, |acc, v| acc.saturating_add(v.weight));
    if total_weight == 0 {
        issues.block(
            codes::SEQ_INVALID_AB_SPLIT,
            format!("{path}.variants"),
            id,
            "ab_split total weight must be > 0",
        );
    }
    let mut names_seen = HashSet::new();
    for (i, v) in ab.variants.iter().enumerate() {
        if v.name.trim().is_empty() {
            issues.block(
                codes::SEQ_INVALID_AB_SPLIT,
                format!("{path}.variants[{i}].name"),
                id,
                "ab_split variant name must not be empty",
            );
        } else if !names_seen.insert(&v.name) {
            issues.block(
                codes::SEQ_INVALID_AB_SPLIT,
                format!("{path}.variants[{i}].name"),
                id,
                format!("ab_split duplicate variant name `{}`", v.name),
            );
        }
        validate_children(
            &v.blocks,
            &format!("{path}.variants[{i}].blocks"),
            seen,
            issues,
            depth,
        );
    }
}

/// Validate every block of a child slice; `path` is the slice's own path
/// (e.g. `blocks[0].body`), each child gets `path[i]`.
fn validate_children(
    blocks: &[BlockDefinition],
    path: &str,
    seen: &mut HashSet<String>,
    issues: &mut Issues,
    depth: usize,
) {
    for (i, b) in blocks.iter().enumerate() {
        validate_block(b, &format!("{path}[{i}]"), seen, issues, depth);
    }
}

#[allow(clippy::too_many_lines)] // one arm per block variant; the match is the readable shape.
fn validate_block(
    block: &BlockDefinition,
    path: &str,
    seen: &mut HashSet<String>,
    issues: &mut Issues,
    depth: usize,
) {
    // Bound recursion before descending — `depth` is the level of `block`
    // itself; children are validated at `depth + 1`. An over-deep subtree is
    // reported once and not descended into.
    if depth > MAX_NESTING_DEPTH {
        issues.block(
            codes::SEQ_NESTING_TOO_DEEP,
            path,
            "(nested)",
            format!("block nesting exceeds the maximum depth of {MAX_NESTING_DEPTH}"),
        );
        return;
    }
    let child_depth = depth + 1;
    match block {
        BlockDefinition::Step(s) => validate_step(s, path, seen, issues),
        BlockDefinition::Parallel(p) => validate_branches(
            &p.id,
            "parallel",
            &p.branches,
            path,
            seen,
            issues,
            child_depth,
        ),
        BlockDefinition::Race(r) => {
            validate_branches(&r.id, "race", &r.branches, path, seen, issues, child_depth);
        }

        BlockDefinition::Loop(l) => {
            check_id(&l.id, path, seen, issues);
            if l.condition.trim().is_empty() {
                issues.block(
                    codes::SEQ_EMPTY_CONDITION,
                    format!("{path}.condition"),
                    l.id.as_str(),
                    "loop condition must not be empty",
                );
            }
            if l.body.is_empty() {
                issues.block(
                    codes::SEQ_EMPTY_BODY,
                    format!("{path}.body"),
                    l.id.as_str(),
                    "loop body must not be empty",
                );
            }
            check_max_iterations(&l.id, "loop", l.max_iterations, path, issues);
            validate_children(&l.body, &format!("{path}.body"), seen, issues, child_depth);
        }

        BlockDefinition::ForEach(fe) => {
            check_id(&fe.id, path, seen, issues);
            if fe.collection.trim().is_empty() {
                issues.block(
                    codes::SEQ_EMPTY_COLLECTION,
                    format!("{path}.collection"),
                    fe.id.as_str(),
                    "for_each collection must not be empty",
                );
            }
            if fe.body.is_empty() {
                issues.block(
                    codes::SEQ_EMPTY_BODY,
                    format!("{path}.body"),
                    fe.id.as_str(),
                    "for_each body must not be empty",
                );
            }
            if fe.item_var.trim().is_empty() {
                issues.block(
                    codes::SEQ_EMPTY_ITEM_VAR,
                    format!("{path}.item_var"),
                    fe.id.as_str(),
                    "for_each item_var must not be empty",
                );
            }
            check_max_iterations(&fe.id, "for_each", fe.max_iterations, path, issues);
            validate_children(&fe.body, &format!("{path}.body"), seen, issues, child_depth);
        }

        BlockDefinition::Router(r) => {
            check_id(&r.id, path, seen, issues);
            if r.routes.is_empty() && r.default.is_none() {
                issues.block(
                    codes::SEQ_ROUTER_NO_ROUTES,
                    format!("{path}.routes"),
                    r.id.as_str(),
                    "router must have at least one route or a default",
                );
            }
            for (i, route) in r.routes.iter().enumerate() {
                if route.condition.trim().is_empty() {
                    issues.block(
                        codes::SEQ_EMPTY_CONDITION,
                        format!("{path}.routes[{i}].condition"),
                        r.id.as_str(),
                        "router route condition must not be empty",
                    );
                }
                validate_children(
                    &route.blocks,
                    &format!("{path}.routes[{i}].blocks"),
                    seen,
                    issues,
                    child_depth,
                );
            }
            if let Some(default) = &r.default {
                validate_children(default, &format!("{path}.default"), seen, issues, child_depth);
            }
        }

        BlockDefinition::TryCatch(tc) => {
            check_id(&tc.id, path, seen, issues);
            if tc.try_block.is_empty() {
                issues.block(
                    codes::SEQ_EMPTY_BODY,
                    format!("{path}.try_block"),
                    tc.id.as_str(),
                    "try_catch try_block must not be empty",
                );
            }
            validate_children(
                &tc.try_block,
                &format!("{path}.try_block"),
                seen,
                issues,
                child_depth,
            );
            validate_children(
                &tc.catch_block,
                &format!("{path}.catch_block"),
                seen,
                issues,
                child_depth,
            );
            if let Some(finally) = &tc.finally_block {
                validate_children(
                    finally,
                    &format!("{path}.finally_block"),
                    seen,
                    issues,
                    child_depth,
                );
            }
        }

        BlockDefinition::SubSequence(s) => {
            check_id(&s.id, path, seen, issues);
            if s.sequence_name.trim().is_empty() {
                issues.block(
                    codes::SEQ_EMPTY_SEQUENCE_NAME,
                    format!("{path}.sequence_name"),
                    s.id.as_str(),
                    "sub_sequence sequence_name must not be empty",
                );
            }
        }

        BlockDefinition::ABSplit(ab) => validate_ab_split(ab, path, seen, issues, child_depth),

        BlockDefinition::CancellationScope(cs) => {
            check_id(&cs.id, path, seen, issues);
            if cs.blocks.is_empty() {
                issues.block(
                    codes::SEQ_EMPTY_BODY,
                    format!("{path}.blocks"),
                    cs.id.as_str(),
                    "cancellation_scope must have at least one block",
                );
            }
            validate_children(&cs.blocks, &format!("{path}.blocks"), seen, issues, child_depth);
        }

        BlockDefinition::Saga(saga) => {
            check_id(&saga.id, path, seen, issues);
            // An empty saga is a valid no-op (flagged by lint, not rejected
            // here) — consistent with the e2e/unit-test contract that it
            // completes immediately rather than failing sequence creation.
            for (i, step) in saga.steps.iter().enumerate() {
                let step_path = format!("{path}.steps[{i}]");
                check_id(&step.id, &step_path, seen, issues);
                validate_block(
                    step.action.as_ref(),
                    &format!("{step_path}.action"),
                    seen,
                    issues,
                    child_depth,
                );
                if let Some(comp) = &step.compensation {
                    validate_block(
                        comp.as_ref(),
                        &format!("{step_path}.compensation"),
                        seen,
                        issues,
                        child_depth,
                    );
                }
            }
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

    fn seq_with(block: BlockDefinition) -> SequenceDefinition {
        SequenceDefinition {
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
