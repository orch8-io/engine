//! Static LLM model pricing table used to estimate USD cost from token usage.
//!
//! Prices are **estimates** (USD per 1M tokens, list prices as of mid-2025)
//! and exist so `GET /usage` can attach a ballpark `cost_usd` to each
//! aggregate without calling out to any provider. Deployments can correct or
//! extend the table at process start via the `ORCH8_MODEL_PRICING` env var
//! (a JSON object merged over the defaults, see [`price_for_model`]).

use std::collections::HashMap;
use std::sync::OnceLock;

use serde::Deserialize;

/// USD price per 1M input/output tokens for a single model family.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
pub struct ModelPrice {
    /// USD per 1M input (prompt) tokens.
    pub input_per_1m: f64,
    /// USD per 1M output (completion) tokens.
    pub output_per_1m: f64,
}

/// Default pricing table: model-name prefix → (input, output) USD per 1M
/// tokens. Entries are matched by longest normalized prefix, so versioned
/// names like `gpt-4o-2024-08-06` or `claude-sonnet-4-6` resolve to their
/// family entry.
const DEFAULT_PRICES: &[(&str, ModelPrice)] = &[
    // OpenAI
    ("gpt-4o", price(2.50, 10.00)),
    ("gpt-4o-mini", price(0.15, 0.60)),
    ("gpt-4.1", price(2.00, 8.00)),
    ("gpt-4.1-mini", price(0.40, 1.60)),
    ("gpt-4.1-nano", price(0.10, 0.40)),
    ("gpt-4-turbo", price(10.00, 30.00)),
    ("gpt-3.5-turbo", price(0.50, 1.50)),
    ("o1", price(15.00, 60.00)),
    ("o3", price(2.00, 8.00)),
    ("o3-mini", price(1.10, 4.40)),
    ("o4-mini", price(1.10, 4.40)),
    // Anthropic
    ("claude-opus-4", price(15.00, 75.00)),
    ("claude-sonnet-4", price(3.00, 15.00)),
    ("claude-haiku", price(1.00, 5.00)),
    ("claude-haiku-4-5", price(1.00, 5.00)),
    ("claude-3-5-sonnet", price(3.00, 15.00)),
    ("claude-3-5-haiku", price(0.80, 4.00)),
    ("claude-3-7-sonnet", price(3.00, 15.00)),
    ("claude-3-opus", price(15.00, 75.00)),
    // Google
    ("gemini-2.5-pro", price(1.25, 10.00)),
    ("gemini-2.5-flash", price(0.30, 2.50)),
    ("gemini-2.0-flash", price(0.10, 0.40)),
    ("gemini-1.5-pro", price(1.25, 5.00)),
    ("gemini-1.5-flash", price(0.075, 0.30)),
    // DeepSeek
    ("deepseek-chat", price(0.27, 1.10)),
    ("deepseek-reasoner", price(0.55, 2.19)),
    // Mistral
    ("mistral-large", price(2.00, 6.00)),
    ("mistral-medium", price(0.40, 2.00)),
    ("mistral-small", price(0.10, 0.30)),
    // Meta
    ("llama-3.3-70b", price(0.59, 0.79)),
    ("llama-3.1-70b", price(0.59, 0.79)),
    ("llama-3.1-8b", price(0.05, 0.08)),
    ("llama-3.1-405b", price(3.50, 3.50)),
    // xAI
    ("grok-3", price(3.00, 15.00)),
    ("grok-3-mini", price(0.30, 0.50)),
    ("grok-2", price(2.00, 10.00)),
    // Alibaba
    ("qwen-max", price(1.60, 6.40)),
    ("qwen-plus", price(0.40, 1.20)),
    ("qwen-turbo", price(0.05, 0.20)),
    // Cohere
    ("command-r", price(0.15, 0.60)),
    ("command-r-plus", price(2.50, 10.00)),
    // Amazon
    ("nova-pro", price(0.80, 3.20)),
    ("nova-lite", price(0.06, 0.24)),
];

/// `const`-context constructor so [`DEFAULT_PRICES`] stays readable.
const fn price(input_per_1m: f64, output_per_1m: f64) -> ModelPrice {
    ModelPrice {
        input_per_1m,
        output_per_1m,
    }
}

/// Env var holding pricing overrides: a JSON object of
/// `{"model-prefix": {"input_per_1m": <f64>, "output_per_1m": <f64>}}`
/// merged over the built-in defaults (replacing existing entries, adding new
/// ones). Read once on first use; invalid JSON logs a warning and is ignored.
pub const PRICING_ENV_VAR: &str = "ORCH8_MODEL_PRICING";

/// Build the effective pricing table: defaults merged with optional JSON
/// overrides (keys are normalized to lowercase). Invalid JSON logs a warning
/// and yields the defaults unchanged.
fn build_table(overrides_json: Option<&str>) -> HashMap<String, ModelPrice> {
    let mut table: HashMap<String, ModelPrice> = DEFAULT_PRICES
        .iter()
        .map(|(name, p)| (name.to_lowercase(), *p))
        .collect();

    if let Some(json) = overrides_json {
        match serde_json::from_str::<HashMap<String, ModelPrice>>(json) {
            Ok(overrides) => {
                for (name, p) in overrides {
                    table.insert(name.to_lowercase(), p);
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "invalid {PRICING_ENV_VAR} JSON; using default model pricing"
                );
            }
        }
    }
    table
}

/// The process-wide pricing table, initialised once from defaults +
/// `ORCH8_MODEL_PRICING`.
fn pricing_table() -> &'static HashMap<String, ModelPrice> {
    static TABLE: OnceLock<HashMap<String, ModelPrice>> = OnceLock::new();
    TABLE.get_or_init(|| build_table(std::env::var(PRICING_ENV_VAR).ok().as_deref()))
}

/// Normalize a model identifier for table lookup: trim, lowercase, and drop
/// any provider path prefix (`anthropic/claude-sonnet-4` → `claude-sonnet-4`).
fn normalize(model: &str) -> String {
    let trimmed = model.trim().to_lowercase();
    match trimmed.rsplit_once('/') {
        Some((_, name)) => name.to_string(),
        None => trimmed,
    }
}

/// Longest-prefix lookup against `table` for an already-normalized name.
///
/// A table key matches when it is a prefix of `normalized` and the remainder
/// is empty or starts at a separator (non-alphanumeric), so `gpt-4o-mini`
/// never falls back to the `gpt-4o` entry while `gpt-4o-2024-08-06` does.
/// Among matches the longest key wins.
fn lookup(table: &HashMap<String, ModelPrice>, normalized: &str) -> Option<ModelPrice> {
    table
        .iter()
        .filter(|(key, _)| {
            normalized.starts_with(key.as_str())
                && normalized[key.len()..]
                    .chars()
                    .next()
                    .is_none_or(|c| !c.is_ascii_alphanumeric())
        })
        .max_by_key(|(key, _)| key.len())
        .map(|(_, p)| *p)
}

/// Look up the estimated price for `model` (case-insensitive, longest-prefix
/// match after normalization). Returns `None` for unknown models.
pub fn price_for_model(model: &str) -> Option<ModelPrice> {
    lookup(pricing_table(), &normalize(model))
}

/// Estimate the USD cost of `input_tokens`/`output_tokens` against `model`'s
/// table entry. Returns `None` when the model is unknown. The result is an
/// **estimate** — list prices only, no caching/batch/tier discounts.
pub fn estimate_cost_usd(model: &str, input_tokens: i64, output_tokens: i64) -> Option<f64> {
    let p = price_for_model(model)?;
    #[allow(clippy::cast_precision_loss)]
    let cost = (input_tokens as f64 / 1_000_000.0) * p.input_per_1m
        + (output_tokens as f64 / 1_000_000.0) * p.output_per_1m;
    Some(cost)
}

#[cfg(test)]
// Exact float comparison is intentional here: the assertions compare values
// copied verbatim from the constant pricing table, not computed results.
#[allow(clippy::float_cmp)]
mod tests {
    use super::*;

    #[test]
    fn exact_match_basic_models() {
        let p = price_for_model("gpt-4o").unwrap();
        assert_eq!((p.input_per_1m, p.output_per_1m), (2.50, 10.00));
        let p = price_for_model("claude-opus-4").unwrap();
        assert_eq!((p.input_per_1m, p.output_per_1m), (15.00, 75.00));
    }

    #[test]
    fn longest_prefix_wins_gpt4o_vs_mini() {
        // "gpt-4o-mini" must hit its own entry, not the shorter "gpt-4o".
        let mini = price_for_model("gpt-4o-mini").unwrap();
        assert_eq!((mini.input_per_1m, mini.output_per_1m), (0.15, 0.60));
        // A dated snapshot of the mini model still resolves to mini.
        let mini = price_for_model("gpt-4o-mini-2024-07-18").unwrap();
        assert_eq!(mini.input_per_1m, 0.15);
        // While a dated base model resolves to the base entry.
        let base = price_for_model("gpt-4o-2024-08-06").unwrap();
        assert_eq!(base.input_per_1m, 2.50);
    }

    #[test]
    fn versioned_claude_matches_family_prefix() {
        let p = price_for_model("claude-sonnet-4-6").unwrap();
        assert_eq!((p.input_per_1m, p.output_per_1m), (3.00, 15.00));
        // "claude-haiku-4-5" has its own (longer) entry over "claude-haiku".
        let p = price_for_model("claude-haiku-4-5").unwrap();
        assert_eq!(p.input_per_1m, 1.00);
    }

    #[test]
    fn lookup_is_case_insensitive_and_strips_provider_prefix() {
        assert!(price_for_model("GPT-4o").is_some());
        assert!(price_for_model("  Claude-Sonnet-4 ").is_some());
        let p = price_for_model("anthropic/claude-sonnet-4").unwrap();
        assert_eq!(p.output_per_1m, 15.00);
    }

    #[test]
    fn unknown_model_is_none() {
        assert!(price_for_model("totally-unknown-model").is_none());
        assert!(price_for_model("").is_none());
        // Alphanumeric continuation is NOT a family match.
        assert!(price_for_model("gpt-4o2").is_none());
    }

    #[test]
    fn cost_math() {
        // 1M input + 1M output of gpt-4o = $2.50 + $10.00.
        assert_eq!(
            estimate_cost_usd("gpt-4o", 1_000_000, 1_000_000),
            Some(12.5)
        );
        // 100k in / 50k out of gpt-4o-mini = 0.1*0.15 + 0.05*0.60 = 0.045.
        let cost = estimate_cost_usd("gpt-4o-mini", 100_000, 50_000).unwrap();
        assert!((cost - 0.045).abs() < 1e-12);
        assert_eq!(estimate_cost_usd("gpt-4o", 0, 0), Some(0.0));
        assert_eq!(estimate_cost_usd("no-such-model", 1_000, 1_000), None);
    }

    // The merge logic is tested with an injected JSON string rather than the
    // real env var: the table behind `price_for_model` is a process-wide
    // OnceLock, so mutating the env in a parallel test run would be racy.
    #[test]
    fn override_json_merges_over_defaults() {
        let json = r#"{
            "gpt-4o": {"input_per_1m": 1.0, "output_per_1m": 2.0},
            "My-Custom-Model": {"input_per_1m": 5.0, "output_per_1m": 6.0}
        }"#;
        let table = build_table(Some(json));
        // Existing entry replaced.
        let p = lookup(&table, "gpt-4o").unwrap();
        assert_eq!((p.input_per_1m, p.output_per_1m), (1.0, 2.0));
        // New entry added, key lowercased.
        let p = lookup(&table, &normalize("my-custom-model-v2")).unwrap();
        assert_eq!((p.input_per_1m, p.output_per_1m), (5.0, 6.0));
        // Untouched defaults survive the merge.
        assert_eq!(lookup(&table, "gpt-4o-mini").unwrap().input_per_1m, 0.15);
    }

    #[test]
    fn invalid_override_json_falls_back_to_defaults() {
        let table = build_table(Some("not json at all"));
        assert_eq!(table.len(), DEFAULT_PRICES.len());
        assert_eq!(lookup(&table, "gpt-4o").unwrap().input_per_1m, 2.50);
    }

    #[test]
    fn no_override_yields_defaults() {
        let table = build_table(None);
        assert_eq!(table.len(), DEFAULT_PRICES.len());
    }
}
