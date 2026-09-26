//! Opt-in `llm_call` response cache: `cache: {mode, ttl, ...}`.
//!
//! - `mode: "exact"` — key = SHA-256 over `(tenant, normalized request)`:
//!   provider(s), model, messages/system (after prompt rendering), sampling
//!   params, tools, `response_schema`, base URL, and the resolved prompt
//!   `(name, version, content_hash)`. Credentials, timeouts, `stream` and the
//!   `cache` block itself are excluded (they do not change the answer).
//! - `mode: "semantic"` — exact lookup first; on a miss, embeds the request
//!   text (via the `embed` handler's `/embeddings` client, configured by
//!   `cache.embedding`) and returns the most similar unexpired entry whose
//!   *partition* (everything except the message text) matches, if its cosine
//!   similarity ≥ `similarity_threshold` (default 0.95). Brute-force scan of
//!   at most [`SEMANTIC_CANDIDATE_LIMIT`] newest entries, like agent memory.
//!
//! Never cached: errors (only successful outputs reach the store), dry-runs,
//! outputs larger than `max_entry_bytes`, requests with artifact-backed
//! images (the key would name the artifact, not its bytes), and tool-call
//! turns (`message.tool_calls` present) unless `allow_tool_calls: true`.
//! Entries are tenant-scoped and sealed at rest by the encrypting storage.

use std::time::Duration;

use chrono::Utc;
use serde_json::{Value, json};
use tracing::{info, warn};

use orch8_storage::StorageBackend;
use orch8_types::ai::{
    DEFAULT_LLM_CACHE_MAX_ENTRY_BYTES, DEFAULT_LLM_CACHE_TTL_SECS, DEFAULT_SEMANTIC_THRESHOLD,
    LlmCacheEntry, MAX_LLM_CACHE_ENTRY_BYTES, MAX_LLM_CACHE_TTL_SECS, PromptResolution,
    SEMANTIC_CANDIDATE_LIMIT, canonical_json, cosine_similarity, sha256_hex,
};
use orch8_types::error::StepError;

use super::common::{permanent, usage_tokens};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CacheMode {
    Exact,
    Semantic,
}

impl CacheMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Semantic => "semantic",
        }
    }
}

/// Parsed `cache` param.
#[derive(Debug, Clone)]
pub(crate) struct CacheConfig {
    pub(crate) mode: CacheMode,
    pub(crate) ttl: Duration,
    pub(crate) max_entry_bytes: usize,
    pub(crate) allow_tool_calls: bool,
    pub(crate) similarity_threshold: f64,
    /// `embed`-handler config (`model`, `base_url`, `api_key_env`, …).
    pub(crate) embedding: Value,
}

/// Parse a TTL: seconds as a number, or a string with an `s`/`m`/`h`/`d`
/// suffix (`"90s"`, `"15m"`, `"1h"`, `"7d"`).
fn parse_ttl(v: &Value) -> Option<u64> {
    if let Some(n) = v.as_u64() {
        return Some(n);
    }
    let s = v.as_str()?.trim();
    let (num, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: u64 = num.parse().ok()?;
    let mult = match unit.trim() {
        "" | "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        _ => return None,
    };
    n.checked_mul(mult)
}

impl CacheConfig {
    /// `Ok(None)` when the step has no `cache` param (or `cache: false`).
    pub(crate) fn from_params(params: &Value) -> Result<Option<Self>, StepError> {
        let Some(raw) = params.get("cache") else {
            return Ok(None);
        };
        if raw.is_null() || raw == &Value::Bool(false) {
            return Ok(None);
        }
        let Some(obj) = raw.as_object() else {
            return Err(permanent(
                "llm_call: `cache` must be an object like {\"mode\": \"exact\", \"ttl\": \"1h\"}"
                    .into(),
            ));
        };
        for key in obj.keys() {
            if !matches!(
                key.as_str(),
                "mode"
                    | "ttl"
                    | "max_entry_bytes"
                    | "allow_tool_calls"
                    | "similarity_threshold"
                    | "embedding"
            ) {
                return Err(permanent(format!("llm_call: unknown `cache.{key}`")));
            }
        }
        let mode = match obj.get("mode").and_then(Value::as_str).unwrap_or("exact") {
            "exact" => CacheMode::Exact,
            "semantic" => CacheMode::Semantic,
            other => {
                return Err(permanent(format!(
                    "llm_call: cache.mode '{other}' must be 'exact' or 'semantic'"
                )));
            }
        };
        let ttl_secs = match obj.get("ttl") {
            None => DEFAULT_LLM_CACHE_TTL_SECS,
            Some(v) => parse_ttl(v).filter(|s| *s > 0).ok_or_else(|| {
                permanent(
                    "llm_call: cache.ttl must be seconds or like \"15m\", \"1h\", \"7d\"".into(),
                )
            })?,
        };
        if ttl_secs > MAX_LLM_CACHE_TTL_SECS {
            return Err(permanent(format!(
                "llm_call: cache.ttl exceeds the {MAX_LLM_CACHE_TTL_SECS}s maximum"
            )));
        }
        let max_entry_bytes = obj
            .get("max_entry_bytes")
            .and_then(Value::as_u64)
            .map_or(DEFAULT_LLM_CACHE_MAX_ENTRY_BYTES, |n| {
                usize::try_from(n).unwrap_or(usize::MAX)
            })
            .min(MAX_LLM_CACHE_ENTRY_BYTES);
        let similarity_threshold = obj
            .get("similarity_threshold")
            .and_then(Value::as_f64)
            .unwrap_or(DEFAULT_SEMANTIC_THRESHOLD);
        if !(0.5..=1.0).contains(&similarity_threshold) {
            return Err(permanent(
                "llm_call: cache.similarity_threshold must be within 0.5..=1.0".into(),
            ));
        }
        let embedding = obj.get("embedding").cloned().unwrap_or_else(|| json!({}));
        if mode == CacheMode::Semantic && !embedding.is_object() {
            return Err(permanent(
                "llm_call: cache.embedding must be an object (embed handler config)".into(),
            ));
        }
        Ok(Some(Self {
            mode,
            ttl: Duration::from_secs(ttl_secs),
            max_entry_bytes,
            allow_tool_calls: obj
                .get("allow_tool_calls")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            similarity_threshold,
            embedding,
        }))
    }
}

/// Params that never influence the model's answer.
const NON_SEMANTIC_PARAMS: &[&str] = &[
    "api_key",
    "api_key_env",
    "cache",
    "prompt",
    "stream",
    "stream_idle_timeout_secs",
    "total_timeout_secs",
    "per_provider_timeout_secs",
    "max_image_bytes",
];

/// Derived identity of one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CacheKeys {
    pub(crate) key: String,
    pub(crate) partition: String,
    pub(crate) provider: String,
    pub(crate) model: String,
    /// Request text embedded in semantic mode.
    pub(crate) text: String,
}

fn has_artifact_image(params: &Value) -> bool {
    params
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|m| m.get("content").and_then(Value::as_array))
        .flatten()
        .any(|block| block.get("artifact").is_some())
}

fn strip(value: &mut Value) {
    if let Some(obj) = value.as_object_mut() {
        for k in NON_SEMANTIC_PARAMS {
            obj.remove(*k);
        }
        if let Some(Value::Array(providers)) = obj.get_mut("providers") {
            for p in providers.iter_mut() {
                strip(p);
            }
        }
    }
}

fn request_text(params: &Value) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(s) = params.get("system").and_then(Value::as_str) {
        parts.push(s.to_string());
    }
    for m in params
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        match m.get("content") {
            Some(Value::String(s)) => parts.push(s.clone()),
            Some(Value::Array(blocks)) => parts.extend(
                blocks
                    .iter()
                    .filter_map(|b| b.get("text").and_then(Value::as_str))
                    .map(str::to_string),
            ),
            _ => {}
        }
    }
    parts.join("\n")
}

/// Compute cache identity. `None` = this request is not cacheable.
pub(crate) fn compute_keys(
    tenant_id: &str,
    params: &Value,
    prompt: Option<&PromptResolution>,
) -> Option<CacheKeys> {
    if has_artifact_image(params) {
        return None;
    }
    let (provider, model) = if let Some(list) = params.get("providers").and_then(Value::as_array) {
        let first = list.first()?;
        let merged = super::merge_provider_params(params, first);
        let name = first
            .get("provider")
            .and_then(Value::as_str)
            .unwrap_or("openai")
            .to_string();
        let model = super::resolve_model(&merged, &name).unwrap_or_default();
        ("failover".to_string(), model)
    } else {
        let name = params
            .get("provider")
            .and_then(Value::as_str)
            .unwrap_or("openai")
            .to_string();
        let model = super::resolve_model(params, &name).unwrap_or_default();
        (name, model)
    };
    let mut normalized = params.clone();
    strip(&mut normalized);
    if let Some(obj) = normalized.as_object_mut() {
        // Pin the effective model so a default-model change can't serve a
        // stale answer from the old default.
        obj.insert("model".into(), Value::String(model.clone()));
    }
    let prompt_id =
        prompt.map(|p| json!({"name": p.name, "version": p.version, "hash": p.content_hash}));
    let key = sha256_hex(
        canonical_json(&json!({
            "v": 1,
            "tenant": tenant_id,
            "request": normalized,
            "prompt": prompt_id,
        }))
        .as_bytes(),
    );
    let mut partition_req = normalized;
    if let Some(obj) = partition_req.as_object_mut() {
        obj.remove("messages");
        obj.remove("system");
    }
    let partition = sha256_hex(
        canonical_json(&json!({
            "v": 1,
            "tenant": tenant_id,
            "request": partition_req,
            "prompt": prompt_id,
        }))
        .as_bytes(),
    );
    Some(CacheKeys {
        key,
        partition,
        provider,
        model,
        text: request_text(params),
    })
}

/// A cache hit, ready to return.
pub(crate) struct CacheHit {
    pub(crate) entry: LlmCacheEntry,
    pub(crate) similarity: Option<f64>,
}

/// Result of a lookup: the hit, plus (semantic mode) the request embedding
/// to reuse when storing the fresh response after a miss.
pub(crate) struct Lookup {
    pub(crate) hit: Option<CacheHit>,
    pub(crate) embedding: Option<Vec<f64>>,
}

fn as_vector(v: &Value) -> Option<Vec<f64>> {
    v.as_array()?.iter().map(Value::as_f64).collect()
}

/// Look up a cached response. Cache failures degrade to a miss (the call
/// proceeds to the provider) — the cache is an optimization, never a gate.
pub(crate) async fn lookup(
    storage: &dyn StorageBackend,
    tenant_id: &str,
    cfg: &CacheConfig,
    keys: &CacheKeys,
) -> Lookup {
    let now = Utc::now();
    match storage.get_llm_cache_entry(tenant_id, &keys.key, now).await {
        Ok(Some(entry)) => {
            return Lookup {
                hit: Some(CacheHit {
                    entry,
                    similarity: None,
                }),
                embedding: None,
            };
        }
        Ok(None) => {}
        Err(e) => {
            warn!(error = %e, "llm_call cache: lookup failed; treating as miss");
            return Lookup {
                hit: None,
                embedding: None,
            };
        }
    }
    if cfg.mode != CacheMode::Semantic || keys.text.is_empty() {
        return Lookup {
            hit: None,
            embedding: None,
        };
    }
    let query = match crate::handlers::memory::embed_inputs(
        &cfg.embedding,
        std::slice::from_ref(&keys.text),
    )
    .await
    {
        Ok(batch) => batch.vectors.into_iter().next(),
        Err(e) => {
            warn!(error = %e, "llm_call cache: embedding failed; exact-only lookup");
            None
        }
    };
    let Some(query) = query else {
        return Lookup {
            hit: None,
            embedding: None,
        };
    };
    let candidates = match storage
        .list_llm_cache_partition(tenant_id, &keys.partition, now, SEMANTIC_CANDIDATE_LIMIT)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "llm_call cache: semantic scan failed; treating as miss");
            Vec::new()
        }
    };
    let best = candidates
        .into_iter()
        .filter_map(|entry| {
            let v = entry.embedding.as_ref().and_then(as_vector)?;
            let sim = cosine_similarity(&query, &v)?;
            Some((sim, entry))
        })
        .filter(|(sim, _)| *sim >= cfg.similarity_threshold)
        .max_by(|a, b| a.0.total_cmp(&b.0));
    Lookup {
        hit: best.map(|(sim, entry)| CacheHit {
            entry,
            similarity: Some(sim),
        }),
        embedding: Some(query),
    }
}

/// Whether `out` is a tool-call turn (non-deterministic follow-up expected).
fn is_tool_call_turn(out: &Value) -> bool {
    out.get("message")
        .and_then(|m| m.get("tool_calls"))
        .and_then(Value::as_array)
        .is_some_and(|calls| !calls.is_empty())
}

/// Store a fresh successful response. Returns why it was not stored, if not.
pub(crate) async fn store(
    storage: &dyn StorageBackend,
    tenant_id: &str,
    cfg: &CacheConfig,
    keys: &CacheKeys,
    out: &Value,
    embedding: Option<Vec<f64>>,
) -> Result<(), &'static str> {
    // A "success" without an assistant message (e.g. an error body behind a
    // 200) is not an answer worth replaying.
    if !out.get("message").is_some_and(Value::is_object) {
        return Err("empty_response");
    }
    if !cfg.allow_tool_calls && is_tool_call_turn(out) {
        return Err("tool_call_turn");
    }
    let size = serde_json::to_vec(out).map_or(usize::MAX, |b| b.len());
    if size > cfg.max_entry_bytes {
        return Err("too_large");
    }
    let (input_tokens, output_tokens) = usage_tokens(out);
    let now = Utc::now();
    let ttl = chrono::Duration::from_std(cfg.ttl).unwrap_or_else(|_| chrono::Duration::hours(1));
    let entry = LlmCacheEntry {
        tenant_id: tenant_id.to_string(),
        cache_key: keys.key.clone(),
        partition_key: keys.partition.clone(),
        provider: keys.provider.clone(),
        model: out
            .get("model")
            .and_then(Value::as_str)
            .filter(|m| !m.is_empty())
            .unwrap_or(&keys.model)
            .to_string(),
        response: out.clone(),
        embedding: embedding.map(|v| json!(v)),
        input_tokens,
        output_tokens,
        size_bytes: i64::try_from(size).unwrap_or(i64::MAX),
        created_at: now,
        expires_at: now + ttl,
    };
    storage.put_llm_cache_entry(&entry).await.map_err(|e| {
        warn!(error = %e, "llm_call cache: store failed");
        "store_failed"
    })
}

/// Build the step output for a hit and record its telemetry + savings.
pub(crate) async fn serve_hit(
    storage: &dyn StorageBackend,
    tenant_id: &str,
    instance_id: orch8_types::ids::InstanceId,
    block_id: &str,
    cfg: &CacheConfig,
    hit: CacheHit,
) -> Value {
    let entry = hit.entry;
    info!(
        target: "orch8::llm_cache",
        tenant_id,
        block_id,
        mode = cfg.mode.as_str(),
        model = %entry.model,
        saved_input_tokens = entry.input_tokens,
        saved_output_tokens = entry.output_tokens,
        similarity = hit.similarity,
        "orch8.llm_cache.hit"
    );
    if entry.input_tokens > 0 || entry.output_tokens > 0 {
        let event = orch8_storage::UsageEvent {
            tenant_id: tenant_id.to_string(),
            instance_id: Some(instance_id),
            block_id: Some(block_id.to_string()),
            kind: crate::tenant_budgets::CACHE_HIT_USAGE_KIND.to_string(),
            model: entry.model.clone(),
            input_tokens: entry.input_tokens,
            output_tokens: entry.output_tokens,
            created_at: Utc::now(),
        };
        if let Err(e) = storage.record_usage_event(&event).await {
            warn!(error = %e, "llm_call cache: failed to record cache-hit savings");
        }
    }
    let mut out = entry.response;
    if let Some(obj) = out.as_object_mut() {
        obj.insert(
            "cache".into(),
            json!({
                "hit": true,
                "mode": cfg.mode.as_str(),
                "similarity": hit.similarity,
                "cached_at": entry.created_at,
                "expires_at": entry.expires_at,
                "key": &entry.cache_key[..entry.cache_key.len().min(16)],
            }),
        );
    }
    out
}

/// Annotate a fresh (miss) output with the cache outcome.
pub(crate) fn annotate_miss(out: &mut Value, cfg: &CacheConfig, stored: Result<(), &'static str>) {
    if let Some(obj) = out.as_object_mut() {
        obj.insert(
            "cache".into(),
            json!({
                "hit": false,
                "mode": cfg.mode.as_str(),
                "stored": stored.is_ok(),
                "skip_reason": stored.err(),
            }),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_parsing_defaults_and_limits() {
        assert!(CacheConfig::from_params(&json!({})).unwrap().is_none());
        assert!(
            CacheConfig::from_params(&json!({"cache": false}))
                .unwrap()
                .is_none()
        );
        let c = CacheConfig::from_params(&json!({"cache": {"mode": "exact", "ttl": "15m"}}))
            .unwrap()
            .unwrap();
        assert_eq!((c.mode, c.ttl.as_secs()), (CacheMode::Exact, 900));
        assert_eq!(c.max_entry_bytes, DEFAULT_LLM_CACHE_MAX_ENTRY_BYTES);
        let c =
            CacheConfig::from_params(&json!({"cache": {"ttl": 30, "max_entry_bytes": 99_999_999}}))
                .unwrap()
                .unwrap();
        assert_eq!(c.max_entry_bytes, MAX_LLM_CACHE_ENTRY_BYTES, "capped");
        for bad in [
            json!({"cache": "yes"}),
            json!({"cache": {"mode": "fuzzy"}}),
            json!({"cache": {"ttl": "1y"}}),
            json!({"cache": {"ttl": 0}}),
            json!({"cache": {"ttl": "400d"}}),
            json!({"cache": {"similarity_threshold": 0.1}}),
            json!({"cache": {"bogus": 1}}),
        ] {
            assert!(CacheConfig::from_params(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn keys_ignore_credentials_and_timeouts_but_not_content() {
        let base = json!({
            "provider": "openai", "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "api_key": "sk-a", "total_timeout_secs": 5,
            "tools": [{"type": "function", "function": {"name": "f"}}],
        });
        let k1 = compute_keys("t1", &base, None).unwrap();
        let mut other_key = base.clone();
        other_key["api_key"] = json!("sk-b");
        other_key["stream"] = json!(true);
        other_key["cache"] = json!({"mode": "exact"});
        assert_eq!(compute_keys("t1", &other_key, None).unwrap().key, k1.key);

        assert_ne!(
            compute_keys("t2", &base, None).unwrap().key,
            k1.key,
            "tenant-scoped"
        );
        let mut msg = base.clone();
        msg["messages"][0]["content"] = json!("hello");
        let k_msg = compute_keys("t1", &msg, None).unwrap();
        assert_ne!(k_msg.key, k1.key);
        assert_eq!(
            k_msg.partition, k1.partition,
            "partition ignores message text"
        );
        let mut tools = base.clone();
        tools["tools"] = json!([]);
        assert_ne!(
            compute_keys("t1", &tools, None).unwrap().partition,
            k1.partition
        );

        let prompt = PromptResolution {
            name: "p".into(),
            version: 2,
            label: None,
            variant: "pinned".into(),
            content_hash: "h".into(),
        };
        assert_ne!(
            compute_keys("t1", &base, Some(&prompt)).unwrap().key,
            k1.key
        );
        assert_eq!(k1.text, "hi");

        let image = json!({"messages": [{"role": "user", "content": [
            {"type": "image", "artifact": "a/b.png"}]}]});
        assert!(compute_keys("t1", &image, None).is_none());
    }

    #[test]
    fn tool_call_turns_are_detected() {
        assert!(is_tool_call_turn(
            &json!({"message": {"tool_calls": [{"id": "1"}]}})
        ));
        assert!(!is_tool_call_turn(&json!({"message": {"tool_calls": []}})));
        assert!(!is_tool_call_turn(&json!({"message": {"content": "x"}})));
        assert_eq!(parse_ttl(&json!("7d")), Some(604_800));
        assert_eq!(parse_ttl(&json!("45")), Some(45));
    }
}
