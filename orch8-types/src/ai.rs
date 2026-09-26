//! AI governance types: the prompt registry, the `llm_call` response cache,
//! and tenant spend budgets.
//!
//! Everything here is plain data plus pure helpers (rendering, hashing,
//! period math, canary selection) so the engine, API, CLI and storage
//! backends share one definition and the logic is unit-testable without I/O.

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use utoipa::ToSchema;
use uuid::Uuid;

// ===========================================================================
// Prompt registry
// ===========================================================================

/// Max length of a prompt name or label.
pub const MAX_PROMPT_NAME_LEN: usize = 128;
/// Max serialized size of one prompt version (system + messages + params +
/// schema). Prompts are text; anything bigger belongs in an artifact.
pub const MAX_PROMPT_BYTES: usize = 256 * 1024;
/// Conventional label for the version production traffic should use. It is a
/// convention only: a prompt reference with neither `version` nor `label`
/// resolves to the latest version, not to this label.
pub const CONVENTIONAL_PRODUCTION_LABEL: &str = "production";

/// One templated chat message of a prompt version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PromptMessage {
    /// Chat role (`system`, `user`, `assistant`).
    pub role: String,
    /// Message text; `{{ variable }}` placeholders are rendered at dispatch.
    pub content: String,
}

/// An immutable, tenant-scoped prompt version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PromptTemplate {
    pub tenant_id: String,
    pub name: String,
    /// Monotonic per `(tenant_id, name)`, starting at 1. Never reused.
    pub version: i32,
    /// Optional system prompt (templated).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    /// Templated messages, sent before any `messages` the step itself adds.
    #[serde(default)]
    pub messages: Vec<PromptMessage>,
    /// Variable names referenced by the template (derived at push time).
    #[serde(default)]
    pub variables: Vec<String>,
    /// Default model params (`provider`, `model`, `temperature`,
    /// `max_tokens`, …). Step params override them key by key.
    #[serde(default = "empty_object")]
    #[schema(value_type = Object)]
    pub model_params: Value,
    /// Optional JSON Schema the response must satisfy (see `llm_call`
    /// `response_schema`). A step-level `response_schema` wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Object>)]
    pub response_schema: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// SHA-256 over the canonical template content (not name/version), used
    /// to make `push` idempotent and to prove which content a step ran.
    pub content_hash: String,
    pub created_at: DateTime<Utc>,
}

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

/// Canary split of a label between its stable version and a candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PromptCanary {
    /// Candidate version receiving `percent` of executions.
    pub version: i32,
    /// 0–100. Selection is a deterministic hash of `(instance, block, name)`,
    /// the same scheme `ab_split` uses, so one execution always sees one side.
    pub percent: u8,
}

/// A movable alias (`production`, `canary`, …) onto an immutable version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PromptLabel {
    pub tenant_id: String,
    pub name: String,
    pub label: String,
    /// Stable version the label points at.
    pub version: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canary: Option<PromptCanary>,
    pub updated_at: DateTime<Utc>,
}

/// How a step asked for a prompt (the `prompt` param of `llm_call`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptRef {
    pub name: String,
    #[serde(default)]
    pub version: Option<i32>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub variables: Option<Value>,
}

/// Which version a step resolved, recorded in the step output and pinned per
/// `(instance, block)` so retries and replays reuse the same version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PromptResolution {
    pub name: String,
    pub version: i32,
    /// Label the version was resolved through, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// `stable` or `canary` when resolved through a label; `pinned` when an
    /// explicit version was requested; `latest` otherwise.
    pub variant: String,
    pub content_hash: String,
}

/// Validate a prompt name or label: 1–128 chars of `[A-Za-z0-9._/-]`.
///
/// # Errors
/// Returns a human-readable reason when invalid.
pub fn validate_prompt_identifier(kind: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > MAX_PROMPT_NAME_LEN {
        return Err(format!("{kind} must be 1-{MAX_PROMPT_NAME_LEN} characters"));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/'))
    {
        return Err(format!(
            "{kind} may only contain letters, digits, '.', '_', '-', '/'"
        ));
    }
    Ok(())
}

/// Extract `{{ variable }}` names (trimmed, deduplicated, in first-seen
/// order) from a template string. Only the root segment of a dotted path is
/// reported (`{{ user.name }}` → `user`).
#[must_use]
pub fn template_variables(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("{{") {
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else { break };
        let path = after[..end].trim();
        let root = path.split('.').next().unwrap_or("").trim();
        if !root.is_empty() && !out.iter().any(|v| v == root) {
            out.push(root.to_string());
        }
        rest = &after[end + 2..];
    }
    out
}

impl PromptTemplate {
    /// All variables referenced by `system` and `messages`.
    #[must_use]
    pub fn referenced_variables(&self) -> Vec<String> {
        let mut vars = Vec::new();
        let texts = self
            .system
            .iter()
            .map(String::as_str)
            .chain(self.messages.iter().map(|m| m.content.as_str()));
        for text in texts {
            for v in template_variables(text) {
                if !vars.contains(&v) {
                    vars.push(v);
                }
            }
        }
        vars
    }

    /// Canonical content hash over everything that affects a model call.
    #[must_use]
    pub fn compute_content_hash(&self) -> String {
        let content = serde_json::json!({
            "system": self.system,
            "messages": self.messages,
            "model_params": self.model_params,
            "response_schema": self.response_schema,
        });
        sha256_hex(canonical_json(&content).as_bytes())
    }
}

/// Render `{{ path }}` placeholders against `variables`. Strings are inserted
/// verbatim; other JSON values are serialized. Dotted paths index objects.
///
/// # Errors
/// Returns the list of unresolved placeholder paths (strict rendering: a
/// prompt never silently ships with a hole in it).
pub fn render_prompt_text(text: &str, variables: &Value) -> Result<String, Vec<String>> {
    let mut out = String::with_capacity(text.len());
    let mut missing: Vec<String> = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            out.push_str(&rest[start..]);
            rest = "";
            break;
        };
        let path = after[..end].trim();
        match lookup_path(variables, path) {
            Some(Value::String(s)) => out.push_str(s),
            Some(v) => out.push_str(&v.to_string()),
            None => {
                if !missing.iter().any(|m| m == path) {
                    missing.push(path.to_string());
                }
            }
        }
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    if missing.is_empty() {
        Ok(out)
    } else {
        Err(missing)
    }
}

fn lookup_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    if path.is_empty() {
        return None;
    }
    let mut cur = root;
    for seg in path.split('.') {
        let seg = seg.trim();
        cur = match cur {
            Value::Object(map) => map.get(seg)?,
            Value::Array(items) => items.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    if cur.is_null() { None } else { Some(cur) }
}

/// Deterministic canary bucket in `0..100` for one execution of one step.
/// Mirrors `ab_split`'s `(instance_id, block_id)` hashing, salted with the
/// prompt name so two prompts in one step don't correlate.
#[must_use]
pub fn canary_bucket(instance_id: &str, block_id: &str, prompt_name: &str) -> u8 {
    let mut hasher = Sha256::new();
    hasher.update(instance_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(block_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(prompt_name.as_bytes());
    let digest = hasher.finalize();
    let n = u64::from_be_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ]);
    u8::try_from(n % 100).unwrap_or(0)
}

impl PromptLabel {
    /// Resolve this label for one execution: `(version, "stable"|"canary")`.
    #[must_use]
    pub fn select(&self, instance_id: &str, block_id: &str) -> (i32, &'static str) {
        match self.canary {
            Some(c)
                if c.percent > 0
                    && canary_bucket(instance_id, block_id, &self.name) < c.percent.min(100) =>
            {
                (c.version, "canary")
            }
            _ => (self.version, "stable"),
        }
    }
}

// ===========================================================================
// LLM response cache
// ===========================================================================

/// Default cache entry lifetime.
pub const DEFAULT_LLM_CACHE_TTL_SECS: u64 = 3_600;
/// Upper bound on a cache entry lifetime (30 days).
pub const MAX_LLM_CACHE_TTL_SECS: u64 = 30 * 24 * 3_600;
/// Default per-entry size cap (serialized response bytes).
pub const DEFAULT_LLM_CACHE_MAX_ENTRY_BYTES: usize = 256 * 1024;
/// Hard ceiling for `max_entry_bytes`.
pub const MAX_LLM_CACHE_ENTRY_BYTES: usize = 1024 * 1024;
/// Default cosine-similarity threshold for `semantic` mode.
pub const DEFAULT_SEMANTIC_THRESHOLD: f64 = 0.95;
/// Max candidates scanned per semantic lookup (most recent first).
pub const SEMANTIC_CANDIDATE_LIMIT: u32 = 200;

/// One cached `llm_call` response. `response` and `embedding` hold the same
/// data class as `context.data` and are encrypted at rest by the encrypting
/// storage decorator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct LlmCacheEntry {
    pub tenant_id: String,
    /// Hex SHA-256 of the normalized request (see engine `llm::cache`).
    pub cache_key: String,
    /// Hex SHA-256 of the request minus its message text: semantic lookups
    /// only compare entries that agree on provider/model/tools/schema/prompt.
    pub partition_key: String,
    pub provider: String,
    pub model: String,
    #[schema(value_type = Object)]
    pub response: Value,
    /// Embedding of the request text (semantic mode only), as a JSON array.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Vec<f64>>)]
    pub embedding: Option<Value>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub size_bytes: i64,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// Cosine similarity of two equal-length vectors; `None` if shapes differ or
/// either vector has zero norm.
#[must_use]
pub fn cosine_similarity(a: &[f64], b: &[f64]) -> Option<f64> {
    if a.len() != b.len() || a.is_empty() {
        return None;
    }
    let (mut dot, mut na, mut nb) = (0.0, 0.0, 0.0);
    for (x, y) in a.iter().zip(b) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return None;
    }
    Some(dot / (na.sqrt() * nb.sqrt()))
}

// ===========================================================================
// Tenant budgets
// ===========================================================================

/// Budget accounting period (UTC calendar boundaries).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BudgetPeriod {
    Daily,
    Monthly,
}

impl BudgetPeriod {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Daily => "daily",
            Self::Monthly => "monthly",
        }
    }

    /// Parse the storage/API spelling.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "daily" => Some(Self::Daily),
            "monthly" => Some(Self::Monthly),
            _ => None,
        }
    }

    /// `[start, end)` of the period containing `now` (UTC).
    #[must_use]
    pub fn window(self, now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
        let date = now.date_naive();
        match self {
            Self::Daily => {
                let start = midnight(date);
                (start, start + Duration::days(1))
            }
            Self::Monthly => {
                let first = NaiveDate::from_ymd_opt(date.year(), date.month(), 1).unwrap_or(date);
                let next = if date.month() == 12 {
                    NaiveDate::from_ymd_opt(date.year() + 1, 1, 1)
                } else {
                    NaiveDate::from_ymd_opt(date.year(), date.month() + 1, 1)
                }
                .unwrap_or(first);
                (midnight(first), midnight(next))
            }
        }
    }
}

fn midnight(date: NaiveDate) -> DateTime<Utc> {
    Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).unwrap_or_default())
}

/// Default soft thresholds (percent of `limit_usd`).
#[must_use]
pub fn default_budget_thresholds() -> Vec<u8> {
    vec![50, 80, 100]
}

const fn yes() -> bool {
    true
}

/// A tenant spend budget over estimated LLM cost (the same list-price
/// estimates `GET /usage` reports).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TenantBudget {
    pub id: Uuid,
    pub tenant_id: String,
    /// Optional model filter: a prefix matched against the recorded model
    /// (`gpt-5` matches `gpt-5.4-mini`). `None` = all models.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub period: BudgetPeriod,
    /// Spend limit in USD for one period.
    pub limit_usd: f64,
    /// Soft thresholds in percent (each emits one alert per period).
    #[serde(default = "default_budget_thresholds")]
    pub thresholds: Vec<u8>,
    /// When true, new `llm_call` dispatches fail closed once spend reaches
    /// `limit_usd` (cache hits are still served — they cost nothing).
    #[serde(default = "yes")]
    pub hard_cap: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TenantBudget {
    /// Whether usage attributed to `model` counts against this budget.
    #[must_use]
    pub fn matches_model(&self, model: &str) -> bool {
        match &self.model {
            None => true,
            Some(prefix) => model
                .trim()
                .to_ascii_lowercase()
                .starts_with(&prefix.trim().to_ascii_lowercase()),
        }
    }

    /// Validate user-supplied fields.
    ///
    /// # Errors
    /// Returns a human-readable reason when invalid.
    pub fn validate(&self) -> Result<(), String> {
        if !self.limit_usd.is_finite() || self.limit_usd <= 0.0 {
            return Err("limit_usd must be a positive number".into());
        }
        if self.thresholds.len() > 10 {
            return Err("at most 10 thresholds".into());
        }
        if self.thresholds.iter().any(|t| *t == 0 || *t > 100) {
            return Err("thresholds must be percentages in 1..=100".into());
        }
        if let Some(m) = &self.model
            && (m.trim().is_empty() || m.len() > 256)
        {
            return Err("model must be 1-256 characters when set".into());
        }
        Ok(())
    }
}

/// Budget health derived from spend vs. limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BudgetState {
    Ok,
    /// At or above the lowest configured threshold, below 100%.
    Warning,
    /// At or above 100% of the limit.
    Exceeded,
}

/// Current-period status of one budget.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct BudgetStatus {
    pub budget: TenantBudget,
    pub period_start: DateTime<Utc>,
    pub period_end: DateTime<Utc>,
    pub spend_usd: f64,
    pub percent_used: f64,
    pub state: BudgetState,
    /// True when a hard-capped budget is exhausted: new dispatches fail.
    pub blocking: bool,
    /// Usage from models without a known price is not counted.
    pub unpriced_events: i64,
}

impl BudgetStatus {
    #[must_use]
    pub fn compute(
        budget: TenantBudget,
        period_start: DateTime<Utc>,
        period_end: DateTime<Utc>,
        spend_usd: f64,
        unpriced_events: i64,
    ) -> Self {
        let percent_used = if budget.limit_usd > 0.0 {
            spend_usd / budget.limit_usd * 100.0
        } else {
            0.0
        };
        let lowest = budget.thresholds.iter().copied().min().unwrap_or(100);
        let state = if percent_used >= 100.0 {
            BudgetState::Exceeded
        } else if percent_used >= f64::from(lowest) {
            BudgetState::Warning
        } else {
            BudgetState::Ok
        };
        let blocking = budget.hard_cap && state == BudgetState::Exceeded;
        Self {
            budget,
            period_start,
            period_end,
            spend_usd,
            percent_used,
            state,
            blocking,
            unpriced_events,
        }
    }

    /// Thresholds crossed so far this period.
    #[must_use]
    pub fn crossed_thresholds(&self) -> Vec<u8> {
        self.budget
            .thresholds
            .iter()
            .copied()
            .filter(|t| self.percent_used >= f64::from(*t))
            .collect()
    }
}

/// Event name of a budget threshold alert (tracing event message and the
/// `event` field of [`BudgetAlert`]). Alert destinations subscribe to this.
pub const BUDGET_THRESHOLD_EVENT: &str = "budget.threshold_crossed";

/// Durable record that a budget crossed a soft threshold. Exactly one row per
/// `(budget_id, period_start, threshold_percent)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct BudgetAlert {
    pub id: Uuid,
    /// Always [`BUDGET_THRESHOLD_EVENT`].
    pub event: String,
    pub tenant_id: String,
    pub budget_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub period: BudgetPeriod,
    pub period_start: DateTime<Utc>,
    pub threshold_percent: u8,
    pub spend_usd: f64,
    pub limit_usd: f64,
    /// Whether this threshold makes new dispatches fail (100% + hard cap).
    pub blocking: bool,
    pub created_at: DateTime<Utc>,
}

// ===========================================================================
// Hashing helpers
// ===========================================================================

/// Lowercase hex SHA-256.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(bytes);
    let mut s = String::with_capacity(64);
    for b in digest {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Serialize JSON with object keys sorted recursively, independent of the
/// `serde_json` map ordering feature, so hashes are stable.
#[must_use]
pub fn canonical_json(value: &Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String((*k).clone()).to_string());
                out.push(':');
                write_canonical(&map[*k], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, v) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(v, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_variables_in_order_without_duplicates() {
        assert_eq!(
            template_variables("Hi {{ name }}, {{user.city}} {{name}} {{ }}"),
            vec!["name".to_string(), "user".to_string()]
        );
    }

    #[test]
    fn renders_strings_values_and_paths_strictly() {
        let vars = json!({"name": "Ada", "n": 3, "user": {"city": "Kyiv"}});
        assert_eq!(
            render_prompt_text("Hi {{ name }} x{{n}} from {{user.city}}", &vars).unwrap(),
            "Hi Ada x3 from Kyiv"
        );
        assert_eq!(
            render_prompt_text("{{ a }} {{b.c}} {{a}}", &vars).unwrap_err(),
            vec!["a".to_string(), "b.c".to_string()]
        );
        // Unterminated placeholder is left verbatim.
        assert_eq!(render_prompt_text("x {{ y", &vars).unwrap(), "x {{ y");
    }

    #[test]
    fn content_hash_ignores_identity_and_key_order() {
        let now = Utc::now();
        let mk = |name: &str, version, params: Value| PromptTemplate {
            tenant_id: "t".into(),
            name: name.into(),
            version,
            system: Some("s".into()),
            messages: vec![],
            variables: vec![],
            model_params: params,
            response_schema: None,
            description: None,
            content_hash: String::new(),
            created_at: now,
        };
        let a = mk("a", 1, json!({"model": "m", "temperature": 0}));
        let b = mk("b", 7, json!({"temperature": 0, "model": "m"}));
        assert_eq!(a.compute_content_hash(), b.compute_content_hash());
        let c = mk("a", 1, json!({"model": "other"}));
        assert_ne!(a.compute_content_hash(), c.compute_content_hash());
    }

    #[test]
    fn canary_selection_is_deterministic_and_proportional() {
        let label = PromptLabel {
            tenant_id: "t".into(),
            name: "p".into(),
            label: "production".into(),
            version: 1,
            canary: Some(PromptCanary {
                version: 2,
                percent: 20,
            }),
            updated_at: Utc::now(),
        };
        let mut canary = 0;
        for i in 0..2_000 {
            let id = format!("inst-{i}");
            let first = label.select(&id, "llm");
            assert_eq!(first, label.select(&id, "llm"), "stable per execution");
            if first.1 == "canary" {
                assert_eq!(first.0, 2);
                canary += 1;
            }
        }
        assert!((300..500).contains(&canary), "~20% canary, got {canary}");
        let mut off = label.clone();
        off.canary = Some(PromptCanary {
            version: 2,
            percent: 0,
        });
        assert_eq!(off.select("x", "y"), (1, "stable"));
    }

    #[test]
    fn identifiers_are_validated() {
        assert!(validate_prompt_identifier("name", "support/triage-v2.en").is_ok());
        assert!(validate_prompt_identifier("name", "").is_err());
        assert!(validate_prompt_identifier("name", "bad name").is_err());
        assert!(validate_prompt_identifier("name", &"x".repeat(129)).is_err());
    }

    #[test]
    fn budget_windows_follow_utc_calendar() {
        let t = Utc.with_ymd_and_hms(2026, 12, 31, 18, 30, 0).unwrap();
        let (s, e) = BudgetPeriod::Monthly.window(t);
        assert_eq!(s, Utc.with_ymd_and_hms(2026, 12, 1, 0, 0, 0).unwrap());
        assert_eq!(e, Utc.with_ymd_and_hms(2027, 1, 1, 0, 0, 0).unwrap());
        let (s, e) = BudgetPeriod::Daily.window(t);
        assert_eq!(s, Utc.with_ymd_and_hms(2026, 12, 31, 0, 0, 0).unwrap());
        assert_eq!(e, Utc.with_ymd_and_hms(2027, 1, 1, 0, 0, 0).unwrap());
    }

    fn budget(limit: f64, hard: bool) -> TenantBudget {
        let now = Utc::now();
        TenantBudget {
            id: Uuid::now_v7(),
            tenant_id: "t".into(),
            model: Some("gpt-5".into()),
            period: BudgetPeriod::Monthly,
            limit_usd: limit,
            thresholds: default_budget_thresholds(),
            hard_cap: hard,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn budget_status_states_and_thresholds() {
        let now = Utc::now();
        let s = BudgetStatus::compute(budget(10.0, true), now, now, 4.0, 0);
        assert_eq!(s.state, BudgetState::Ok);
        assert!(s.crossed_thresholds().is_empty());
        let s = BudgetStatus::compute(budget(10.0, true), now, now, 8.5, 0);
        assert_eq!(s.state, BudgetState::Warning);
        assert_eq!(s.crossed_thresholds(), vec![50, 80]);
        assert!(!s.blocking);
        let s = BudgetStatus::compute(budget(10.0, true), now, now, 10.0, 0);
        assert_eq!(s.state, BudgetState::Exceeded);
        assert!(s.blocking);
        let s = BudgetStatus::compute(budget(10.0, false), now, now, 12.0, 0);
        assert!(!s.blocking, "soft-only budget never blocks");
    }

    #[test]
    fn budget_model_prefix_and_validation() {
        let b = budget(1.0, true);
        assert!(b.matches_model("GPT-5.4-mini"));
        assert!(!b.matches_model("claude-opus-5"));
        assert!(b.validate().is_ok());
        let mut bad = b.clone();
        bad.limit_usd = 0.0;
        assert!(bad.validate().is_err());
        bad = b;
        bad.thresholds = vec![0];
        assert!(bad.validate().is_err());
    }

    #[test]
    fn cosine_and_canonical_json() {
        assert!((cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]).unwrap() - 1.0).abs() < 1e-12);
        assert!(cosine_similarity(&[1.0], &[1.0, 2.0]).is_none());
        assert!(cosine_similarity(&[0.0, 0.0], &[1.0, 2.0]).is_none());
        assert_eq!(
            canonical_json(&json!({"b": [1, {"d": 1, "c": 2}], "a": "x"})),
            r#"{"a":"x","b":[1,{"c":2,"d":1}]}"#
        );
    }
}
