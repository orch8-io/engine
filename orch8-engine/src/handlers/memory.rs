//! Built-in agent-memory handlers — `embed`, `memory_store`, `memory_search`.
//!
//! Durable, semantically-searchable memory for agents, built on the engine's
//! existing instance key-value store (the same persistence as `set_state`) or
//! a tenant-isolated shared knowledge namespace. Memories survive crashes and
//! replay like any other durable engine state.
//!
//! Ranking is a cosine-similarity scan in plain Rust — **no pgvector,
//! sqlite-vec, or any database extension**. That keeps the engine a single
//! binary and, critically, lets agent memory work **offline on a phone**
//! (`SQLite` backend), where a vector-DB extension is not an option. Brute-force
//! scan is appropriate for the hundreds-to-thousands of memories an agent
//! accumulates within an instance/session or bounded shared namespace.
//!
//! ## Handlers
//!
//! - **`embed`** — `{ input }` → `{ embedding | embeddings, model, dimensions }`.
//!   Calls an `OpenAI`-compatible `/embeddings` endpoint.
//! - **`memory_store`** — `{ text, [embedding], [key], [metadata] }` → persists a
//!   memory (embedding computed if not supplied). Returns `{ key, dimensions }`.
//! - **`memory_search`** — `{ query | query_embedding, [top_k] }` → ranked
//!   `{ results: [{ key, text, score, metadata }], count }`.
//!
//! Embedding config (`model`, `api_key`/`api_key_env`, `base_url`, `timeout_ms`)
//! is shared by all three.

use std::fmt::Write as _;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tracing::debug;

use orch8_types::error::StepError;

use super::StepContext;
use crate::memory_governance::{
    MemoryOperation, load_namespace_policy, validate_residency, validate_target_namespace,
};

/// Prefix marking an instance-KV entry as a memory record, so `memory_search`
/// can pick memories out of the shared KV namespace without colliding with
/// `set_state` keys.
const MEMORY_KEY_PREFIX: &str = "__mem__:";
/// Default embeddings model (`OpenAI`). Override with `model`.
const DEFAULT_EMBED_MODEL: &str = "text-embedding-3-small";
/// Default embeddings base URL (`OpenAI`-compatible).
const DEFAULT_EMBED_BASE: &str = "https://api.openai.com/v1";
/// Default number of results returned by `memory_search`.
const DEFAULT_TOP_K: u64 = 5;
const DEFAULT_NAMESPACE: &str = "default";
const MAX_SHARED_RECORDS: u32 = 10_000;
const MAX_TOP_K: u64 = 100;
const MAX_EMBEDDING_DIMENSIONS: usize = 16_384;
const MAX_EMBED_INPUTS: usize = 2_048;
const MAX_EMBED_INPUT_BYTES: usize = 10_485_760;
const INSTANCE_DEFAULT_RETENTION_SECS: u64 = 30 * 24 * 60 * 60;
const INSTANCE_MAX_RETENTION_SECS: u64 = 365 * 24 * 60 * 60;

// ===========================================================================
// embed
// ===========================================================================

pub async fn handle_embed(ctx: StepContext) -> Result<Value, StepError> {
    requested_model(&ctx.params)?;
    let input = ctx
        .params
        .get("input")
        .ok_or_else(|| permanent("embed: `input` (string or array of strings) is required"))?;
    let is_batch = input.is_array();
    let inputs = to_input_list(input)?;
    resolve_embedding_url(&ctx.params)?;
    resolve_embedding_timeout(&ctx.params)?;

    // Dry-run: `input` is validated; skip the embedding-provider call. Mirror
    // the batch/single output shape with empty vectors.
    if ctx.is_dry_run() {
        let model = resolve_model(&ctx.params);
        let shape = if input.is_array() {
            json!({ "embeddings": [], "model": model, "dimensions": 0 })
        } else {
            json!({ "embedding": [], "model": model, "dimensions": 0 })
        };
        return Ok(super::util::dry_run_stub("embed", Value::Null, shape));
    }

    let batch = embed_inputs(&ctx.params, &inputs).await?;
    let dimensions = batch.vectors.first().map_or(0, Vec::len);

    if is_batch {
        Ok(json!({
            "embeddings": batch.vectors,
            "model": batch.model,
            "dimensions": dimensions,
        }))
    } else {
        Ok(json!({
            "embedding": batch.vectors.into_iter().next().unwrap_or_default(),
            "model": batch.model,
            "dimensions": dimensions,
        }))
    }
}

// ===========================================================================
// memory_store
// ===========================================================================

pub async fn handle_memory_store(ctx: StepContext) -> Result<Value, StepError> {
    let scope = memory_scope(&ctx.params)?;
    let namespace = memory_namespace(&ctx.params)?;
    let authorization = authorize_memory(&ctx, scope, &namespace, MemoryOperation::Store).await?;
    let retention_secs = retention_secs(&ctx.params, &authorization)?;
    let declared_model = requested_model(&ctx.params)?;
    let mut embedding_model = if ctx.params.get("embedding").is_some() {
        declared_model.map(str::to_string)
    } else {
        Some(resolve_model(&ctx.params))
    };
    let text = ctx
        .params
        .get("text")
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| permanent("memory_store: `text` must be a string"))
        })
        .transpose()?;
    if let Some(text) = text.as_deref() {
        validate_memory_text(text)?;
    }
    if let Some(value) = ctx.params.get("key") {
        let key = value
            .as_str()
            .ok_or_else(|| permanent("memory_store: `key` must be a string"))?;
        validate_memory_key(key)?;
    }

    // Dry-run: do not embed or persist anything.
    if ctx.is_dry_run() {
        let supplied = ctx
            .params
            .get("embedding")
            .map(parse_supplied_embedding)
            .transpose()?;
        if text.is_none() && supplied.is_none() {
            return Err(permanent("memory_store: `text` or `embedding` is required"));
        }
        if supplied.is_none() {
            validate_embedding_input_text(text.as_deref().unwrap_or_default())?;
            resolve_embedding_url(&ctx.params)?;
            resolve_embedding_timeout(&ctx.params)?;
        }
        let key = memory_key(&ctx.params, text.as_deref(), supplied.as_deref())?;
        let key = preserve_legacy_text_key(&ctx, scope, &namespace, key, text.as_deref()).await?;
        validate_memory_key(&key)?;
        return Ok(super::util::dry_run_stub(
            "memory_store",
            Value::Null,
            json!({ "key": key, "stored": false, "dimensions": 0, "scope": scope, "namespace": namespace, "residency": authorization.residency, "retention_secs": retention_secs }),
        ));
    }

    // Use a supplied embedding if present; otherwise embed `text`.
    let embedding: Vec<f64> = if let Some(value) = ctx.params.get("embedding") {
        parse_supplied_embedding(value)?
    } else {
        let text = text
            .as_deref()
            .ok_or_else(|| permanent("memory_store: `text` or `embedding` is required"))?;
        let batch = embed_inputs(&ctx.params, &[text.to_string()]).await?;
        embedding_model = Some(batch.model);
        batch
            .vectors
            .into_iter()
            .next()
            .ok_or_else(|| retryable("memory_store: embedding provider returned no vector"))?
    };

    let metadata = ctx.params.get("metadata").cloned().unwrap_or(json!({}));
    let key = memory_key(&ctx.params, text.as_deref(), Some(&embedding))?;
    let key = preserve_legacy_text_key(&ctx, scope, &namespace, key, text.as_deref()).await?;
    validate_memory_key(&key)?;

    let record = memory_record(
        text.as_deref(),
        &embedding,
        embedding_model.as_deref(),
        &metadata,
        &authorization,
        retention_secs,
        &ctx,
    )?;
    match scope {
        MemoryScope::Instance => {
            let storage_key = format!("{MEMORY_KEY_PREFIX}{key}");
            ctx.storage
                .set_instance_kv(ctx.instance_id, &storage_key, &record)
                .await
        }
        MemoryScope::Tenant => {
            ctx.storage
                .set_shared_knowledge(ctx.tenant_id.as_str(), &namespace, &key, &record)
                .await
        }
    }
    .map_err(|e| retryable(format!("memory_store storage error: {e}")))?;

    debug!(key = %key, dimensions = embedding.len(), "memory_store: persisted");
    Ok(
        json!({ "key": key, "stored": true, "dimensions": embedding.len(), "scope": scope, "namespace": namespace, "residency": authorization.residency, "retention_secs": retention_secs, "policy_version": authorization.policy_version }),
    )
}

// ===========================================================================
// memory_search
// ===========================================================================

pub async fn handle_memory_search(ctx: StepContext) -> Result<Value, StepError> {
    let scope = memory_scope(&ctx.params)?;
    let namespace = memory_namespace(&ctx.params)?;
    let authorization = authorize_memory(&ctx, scope, &namespace, MemoryOperation::Search).await?;
    let top_k = parse_top_k(&ctx.params)?;
    let declared_model = requested_model(&ctx.params)?;
    let mut query_model = if ctx.params.get("query_embedding").is_some() {
        declared_model.map(str::to_string)
    } else {
        Some(resolve_model(&ctx.params))
    };
    // Dry-run: skip the embedding-provider call (the query vector would
    // otherwise be embedded via an external API). Return an empty result set.
    if ctx.is_dry_run() {
        if let Some(value) = ctx.params.get("query_embedding") {
            let array = value
                .as_array()
                .ok_or_else(|| permanent("memory_search: `query_embedding` must be an array"))?;
            parse_vector(array)?;
        } else {
            let query = ctx
                .params
                .get("query")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    permanent("memory_search: `query` or `query_embedding` is required")
                })?;
            validate_embedding_input_text(query)?;
            resolve_embedding_url(&ctx.params)?;
            resolve_embedding_timeout(&ctx.params)?;
        }
        return Ok(super::util::dry_run_stub(
            "memory_search",
            Value::Null,
            json!({ "results": [], "count": 0, "scope": scope, "namespace": namespace, "residency": authorization.residency, "policy_version": authorization.policy_version }),
        ));
    }

    // The query vector: precomputed `query_embedding`, or embed `query`.
    let query_embedding: Vec<f64> = if let Some(value) = ctx.params.get("query_embedding") {
        let arr = value
            .as_array()
            .ok_or_else(|| permanent("memory_search: `query_embedding` must be an array"))?;
        parse_vector(arr)?
    } else {
        let query = ctx
            .params
            .get("query")
            .and_then(Value::as_str)
            .ok_or_else(|| permanent("memory_search: `query` or `query_embedding` is required"))?;
        let batch = embed_inputs(&ctx.params, &[query.to_string()]).await?;
        query_model = Some(batch.model);
        batch
            .vectors
            .into_iter()
            .next()
            .ok_or_else(|| retryable("memory_search: embedding provider returned no vector"))?
    };

    let (records, corpus_truncated) = match scope {
        MemoryScope::Instance => {
            let kv = ctx
                .storage
                .get_all_instance_kv(ctx.instance_id)
                .await
                .map_err(|e| retryable(format!("memory_search storage error: {e}")))?;
            (extract_memory_records(&kv), false)
        }
        MemoryScope::Tenant => {
            // One lookahead record distinguishes an exact 10,000-record corpus
            // from a larger one. Rank the lookahead too, since the storage API
            // returns a map without retaining its recency order.
            let records = ctx
                .storage
                .list_shared_knowledge(ctx.tenant_id.as_str(), &namespace, MAX_SHARED_RECORDS + 1)
                .await
                .map_err(|e| retryable(format!("memory_search storage error: {e}")))?;
            let truncated = records.len() > MAX_SHARED_RECORDS as usize;
            (records.into_iter().collect(), truncated)
        }
    };
    let (records, expired) = governed_records(records, &authorization, scope, Utc::now());
    purge_expired(&ctx, scope, &namespace, &expired).await?;
    let results = rank_memories(&query_embedding, records, top_k, query_model.as_deref());

    Ok(
        json!({ "results": results, "count": results_len(&results), "scope": scope, "namespace": namespace, "residency": authorization.residency, "policy_version": authorization.policy_version, "expired_deleted": expired.len(), "corpus_truncated": corpus_truncated }),
    )
}

/// Permanently delete one governed memory. Tenant-scoped deletion is denied
/// unless the authoritative namespace policy grants `delete` to this sequence.
pub async fn handle_memory_delete(ctx: StepContext) -> Result<Value, StepError> {
    let scope = memory_scope(&ctx.params)?;
    let namespace = memory_namespace(&ctx.params)?;
    let authorization = authorize_memory(&ctx, scope, &namespace, MemoryOperation::Delete).await?;
    let key = ctx
        .params
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| permanent("memory_delete: `key` is required"))?;
    validate_memory_key(key)?;
    if ctx.is_dry_run() {
        return Ok(super::util::dry_run_stub(
            "memory_delete",
            Value::Null,
            json!({ "key": key, "deleted": false, "scope": scope, "namespace": namespace, "residency": authorization.residency }),
        ));
    }
    match scope {
        MemoryScope::Instance => {
            ctx.storage
                .delete_instance_kv(ctx.instance_id, &format!("{MEMORY_KEY_PREFIX}{key}"))
                .await
        }
        MemoryScope::Tenant => {
            ctx.storage
                .delete_shared_knowledge(ctx.tenant_id.as_str(), &namespace, key)
                .await
        }
    }
    .map_err(|error| retryable(format!("memory_delete storage error: {error}")))?;
    Ok(
        json!({ "key": key, "deleted": true, "scope": scope, "namespace": namespace, "residency": authorization.residency, "policy_version": authorization.policy_version }),
    )
}

// ===========================================================================
// Embedding provider call
// ===========================================================================

/// Hard cap on the `/embeddings` response body: `base_url` is
/// workflow-controlled (SSRF-checked, but any public URL is allowed), so a
/// hostile endpoint must not stream an unbounded body and OOM the worker.
/// Same 10 MB cap `http_request` uses.
const MAX_EMBED_RESPONSE_BYTES: usize = 10_485_760; // 10 MB

#[derive(Debug)]
struct EmbeddingBatch {
    vectors: Vec<Vec<f64>>,
    model: String,
}

/// Resolve config and POST to the `/embeddings` endpoint, returning one vector
/// per input string (in order).
async fn embed_inputs(params: &Value, inputs: &[String]) -> Result<EmbeddingBatch, StepError> {
    requested_model(params)?;
    if inputs.is_empty() {
        return Err(permanent("embed: `input` array is empty"));
    }
    validate_embedding_batch(inputs.iter().map(String::as_str))?;
    let url = resolve_embedding_url(params)?;

    if !super::builtin::is_url_safe(&url).await {
        return Err(permanent("embed: base_url is not allowed"));
    }

    let api_key = resolve_api_key(params)?;
    let model = resolve_model(params);
    let timeout = resolve_embedding_timeout(params)?;

    let body = build_embeddings_request(&model, inputs);
    let client = super::llm::http_client();
    let resp = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {api_key}"))
        .timeout(timeout)
        .json(&body)
        .send()
        .await
        .map_err(|e| {
            if e.is_timeout() || e.is_connect() {
                retryable(format!(
                    "embed network error: {}",
                    crate::outbound::redact_error(&e)
                ))
            } else {
                permanent(format!(
                    "embed request error: {}",
                    crate::outbound::redact_error(&e)
                ))
            }
        })?;

    read_embedding_response(resp, inputs.len(), &model).await
}

async fn read_embedding_response(
    resp: reqwest::Response,
    expected_count: usize,
    requested_model: &str,
) -> Result<EmbeddingBatch, StepError> {
    let status = resp.status().as_u16();
    if let Some(error) = embedding_http_error(status) {
        return Err(error);
    }
    // Stream with the hard cap instead of buffering the whole response.
    let bytes = super::builtin::read_body_capped(resp, MAX_EMBED_RESPONSE_BYTES)
        .await
        .map_err(|e| match e {
            super::builtin::BodyReadError::TooLarge(cap) => {
                permanent(format!("embed response body exceeded {cap} bytes"))
            }
            super::builtin::BodyReadError::Io(msg) => {
                retryable(format!("embed body read error: {msg}"))
            }
        })?;
    parse_embedding_response(status, &bytes, expected_count, requested_model)
}

fn resolve_model(params: &Value) -> String {
    params
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_EMBED_MODEL)
        .to_string()
}

fn resolve_embedding_url(params: &Value) -> Result<String, StepError> {
    let base = match params.get("base_url") {
        Some(value) => value
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| permanent("embed: `base_url` must be a non-empty URL string"))?,
        None => DEFAULT_EMBED_BASE,
    }
    .trim_end_matches('/');
    let parsed = url::Url::parse(base)
        .map_err(|_| permanent("embed: `base_url` must be a valid absolute URL"))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(permanent(
            "embed: `base_url` must be an HTTP(S) URL without credentials, query, or fragment",
        ));
    }
    Ok(format!("{base}/embeddings"))
}

fn resolve_embedding_timeout(params: &Value) -> Result<Duration, StepError> {
    let millis = match params.get("timeout_ms") {
        Some(value) => value
            .as_u64()
            .filter(|millis| (1..=300_000).contains(millis))
            .ok_or_else(|| permanent("embed: `timeout_ms` must be 1..=300000"))?,
        None => 30_000,
    };
    Ok(Duration::from_millis(millis))
}

fn requested_model(params: &Value) -> Result<Option<&str>, StepError> {
    match params.get("model") {
        Some(value) => {
            let model = value
                .as_str()
                .filter(|model| !model.trim().is_empty())
                .ok_or_else(|| permanent("embed: `model` must be a non-empty string"))?;
            Ok(Some(model))
        }
        None => Ok(None),
    }
}

/// Resolve the API key from `api_key` or the env var named by `api_key_env`.
fn resolve_api_key(params: &Value) -> Result<String, StepError> {
    if let Some(value) = params.get("api_key") {
        let key = value
            .as_str()
            .filter(|key| !key.is_empty())
            .ok_or_else(|| permanent("embed: `api_key` must be a non-empty string"))?;
        return Ok(key.to_string());
    }
    if let Some(value) = params.get("api_key_env") {
        let env_name = value
            .as_str()
            .ok_or_else(|| permanent("embed: `api_key_env` must be a string"))?;
        // Same guard as the llm handler: a workflow-controlled env var name must
        // not be allowed to read the engine's own secrets or infrastructure
        // credentials and ship them to a workflow-controlled `base_url`.
        if !crate::handlers::llm::common::is_allowed_api_key_env(env_name) {
            return Err(permanent(format!(
                "embed: api_key_env '{env_name}' is not permitted: reading engine \
                 or infrastructure secrets via api_key_env is blocked"
            )));
        }
        return std::env::var(env_name)
            .ok()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| permanent(format!("embed: env var {env_name} not set or empty")));
    }
    Err(permanent("embed: `api_key` or `api_key_env` is required"))
}

// ===========================================================================
// Pure helpers (unit-tested without network or storage)
// ===========================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum MemoryScope {
    Instance,
    Tenant,
}

fn memory_scope(params: &Value) -> Result<MemoryScope, StepError> {
    let scope = match params.get("scope") {
        Some(value) => value
            .as_str()
            .ok_or_else(|| permanent("memory: `scope` must be a string"))?,
        None => "instance",
    };
    match scope {
        "instance" => Ok(MemoryScope::Instance),
        "tenant" => Ok(MemoryScope::Tenant),
        other => Err(permanent(format!(
            "memory: unsupported scope `{other}`; expected `instance` or `tenant`"
        ))),
    }
}

fn memory_namespace(params: &Value) -> Result<String, StepError> {
    let namespace = match params.get("namespace") {
        Some(value) => value
            .as_str()
            .ok_or_else(|| permanent("memory: `namespace` must be a string"))?,
        None => DEFAULT_NAMESPACE,
    };
    validate_target_namespace(namespace).map_err(permanent)?;
    Ok(namespace.to_string())
}

#[derive(Debug)]
struct MemoryAuthorization {
    sequence_id: orch8_types::ids::SequenceId,
    tenant_id: String,
    instance_id: String,
    residency: String,
    policy_version: u64,
    default_retention_secs: u64,
    max_retention_secs: u64,
}

async fn authorize_memory(
    ctx: &StepContext,
    scope: MemoryScope,
    namespace: &str,
    operation: MemoryOperation,
) -> Result<MemoryAuthorization, StepError> {
    let instance = ctx
        .storage
        .get_instance(ctx.instance_id)
        .await
        .map_err(|error| retryable(format!("memory authorization storage error: {error}")))?
        .ok_or_else(|| permanent("memory authorization: instance does not exist"))?;
    if instance.tenant_id != ctx.tenant_id {
        return Err(permanent(
            "memory authorization: instance tenant does not match caller tenant",
        ));
    }

    let requested_residency = ctx
        .params
        .get("residency")
        .map(|value| {
            let residency = value
                .as_str()
                .ok_or_else(|| permanent("memory: `residency` must be a string"))?;
            validate_residency(residency).map_err(permanent)?;
            Ok::<_, StepError>(residency)
        })
        .transpose()?;
    let (residency, policy_version, default_retention_secs, max_retention_secs) = match scope {
        MemoryScope::Instance => {
            let residency = requested_residency.unwrap_or("local");
            (
                residency.to_string(),
                1,
                INSTANCE_DEFAULT_RETENTION_SECS,
                INSTANCE_MAX_RETENTION_SECS,
            )
        }
        MemoryScope::Tenant => {
            let policy = load_namespace_policy(ctx.storage.as_ref(), &ctx.tenant_id, namespace)
                .await
                .map_err(|error| retryable(format!("memory policy storage error: {error}")))?
                .ok_or_else(|| {
                    permanent(format!(
                        "memory: tenant namespace '{namespace}' has no governance policy"
                    ))
                })?;
            if !policy.authorizes(instance.sequence_id, operation) {
                return Err(permanent(format!(
                    "memory: sequence {} is not authorized for {operation:?} in namespace '{namespace}'",
                    instance.sequence_id
                )));
            }
            if requested_residency.is_some_and(|value| value != policy.residency) {
                return Err(permanent(format!(
                    "memory: requested residency does not match policy residency '{}'",
                    policy.residency
                )));
            }
            (
                policy.residency,
                policy.policy_version,
                policy.default_retention_secs,
                policy.max_retention_secs,
            )
        }
    };
    Ok(MemoryAuthorization {
        sequence_id: instance.sequence_id,
        tenant_id: ctx.tenant_id.to_string(),
        instance_id: ctx.instance_id.to_string(),
        residency,
        policy_version,
        default_retention_secs,
        max_retention_secs,
    })
}

fn retention_secs(params: &Value, authorization: &MemoryAuthorization) -> Result<u64, StepError> {
    let retention = match params.get("retention_secs") {
        Some(value) => value
            .as_u64()
            .ok_or_else(|| permanent("memory: `retention_secs` must be a positive integer"))?,
        None => authorization.default_retention_secs,
    };
    if retention == 0 || retention > authorization.max_retention_secs {
        return Err(permanent(format!(
            "memory: retention_secs must be 1..={} under the active policy",
            authorization.max_retention_secs
        )));
    }
    Ok(retention)
}

fn parse_top_k(params: &Value) -> Result<usize, StepError> {
    let value = match params.get("top_k") {
        Some(value) => value
            .as_u64()
            .ok_or_else(|| permanent("memory_search: `top_k` must be an integer"))?,
        None => DEFAULT_TOP_K,
    };
    if !(1..=MAX_TOP_K).contains(&value) {
        return Err(permanent(format!(
            "memory_search: `top_k` must be 1..={MAX_TOP_K}"
        )));
    }
    usize::try_from(value).map_err(|_| permanent("memory_search: `top_k` is too large"))
}

fn validate_memory_key(key: &str) -> Result<(), StepError> {
    if key.is_empty() || key.len() > 256 || key.chars().any(char::is_control) {
        return Err(permanent(
            "memory: key must be 1-256 characters and contain no control characters",
        ));
    }
    Ok(())
}

/// Normalize `input` (a string or array of strings) into a list of strings.
fn to_input_list(input: &Value) -> Result<Vec<String>, StepError> {
    match input {
        Value::String(s) => {
            validate_embedding_input_text(s)?;
            Ok(vec![s.clone()])
        }
        Value::Array(arr) => {
            if arr.len() > MAX_EMBED_INPUTS {
                return Err(permanent(format!(
                    "embed: `input` array must contain at most {MAX_EMBED_INPUTS} strings"
                )));
            }
            let strings = arr
                .iter()
                .map(|value| value.as_str())
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| permanent("embed: `input` array must contain only strings"))?;
            validate_embedding_batch(strings.iter().copied())?;
            if strings.is_empty() {
                return Err(permanent("embed: `input` array is empty"));
            }
            Ok(strings.into_iter().map(str::to_string).collect())
        }
        _ => Err(permanent(
            "embed: `input` must be a string or array of strings",
        )),
    }
}

fn validate_embedding_input_text(input: &str) -> Result<(), StepError> {
    if input.is_empty() {
        return Err(permanent("embed: input text must not be empty"));
    }
    if input.len() > MAX_EMBED_INPUT_BYTES {
        return Err(permanent(format!(
            "embed: input text must not exceed {MAX_EMBED_INPUT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_memory_text(text: &str) -> Result<(), StepError> {
    if text.len() > MAX_EMBED_INPUT_BYTES {
        return Err(permanent(format!(
            "memory_store: `text` must not exceed {MAX_EMBED_INPUT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_embedding_batch<'a>(
    inputs: impl ExactSizeIterator<Item = &'a str>,
) -> Result<(), StepError> {
    if inputs.len() > MAX_EMBED_INPUTS {
        return Err(permanent(format!(
            "embed: `input` array must contain at most {MAX_EMBED_INPUTS} strings"
        )));
    }
    let mut total_bytes = 0usize;
    for input in inputs {
        validate_embedding_input_text(input)?;
        total_bytes = total_bytes.saturating_add(input.len());
        if total_bytes > MAX_EMBED_INPUT_BYTES {
            return Err(permanent(format!(
                "embed: total input text must not exceed {MAX_EMBED_INPUT_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

/// Build an `OpenAI`-compatible `/embeddings` request body.
fn build_embeddings_request(model: &str, inputs: &[String]) -> Value {
    json!({ "model": model, "input": inputs })
}

/// Parse one embedding per request input, ordered by the response `index`.
fn parse_embedding_response(
    status: u16,
    body: &[u8],
    expected_count: usize,
    requested_model: &str,
) -> Result<EmbeddingBatch, StepError> {
    if let Some(error) = embedding_http_error(status) {
        return Err(error);
    }
    let parsed: Value = serde_json::from_slice(body)
        .map_err(|e| permanent(format!("embed response was not valid JSON: {e}")))?;
    let model = match parsed.get("model") {
        Some(value) => value
            .as_str()
            .filter(|model| !model.trim().is_empty())
            .ok_or_else(|| permanent("embed response `model` must be a non-empty string"))?
            .to_string(),
        None => requested_model.to_string(),
    };
    let data = parsed
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| permanent("embed response missing `data` array"))?;

    if data.len() != expected_count {
        return Err(permanent(format!(
            "embed response returned {} vectors for {expected_count} inputs",
            data.len()
        )));
    }
    let mut ordered = vec![None; expected_count];
    let mut dimensions = None;
    for (pos, item) in data.iter().enumerate() {
        let arr = item
            .get("embedding")
            .and_then(Value::as_array)
            .ok_or_else(|| permanent("embed response item missing `embedding`"))?;
        let idx = match item.get("index") {
            Some(value) => value
                .as_u64()
                .and_then(|index| usize::try_from(index).ok())
                .ok_or_else(|| permanent("embed response index is invalid"))?,
            None => pos,
        };
        if idx >= expected_count || ordered[idx].is_some() {
            return Err(permanent(
                "embed response indices are duplicate or out of range",
            ));
        }
        let vector = parse_vector(arr)?;
        if dimensions.is_some_and(|expected| expected != vector.len()) {
            return Err(permanent(
                "embed response vectors have different dimensions",
            ));
        }
        dimensions = Some(vector.len());
        ordered[idx] = Some(vector);
    }
    let vectors = ordered
        .into_iter()
        .map(|vector| vector.ok_or_else(|| permanent("embed response index is missing")))
        .collect::<Result<_, _>>()?;
    Ok(EmbeddingBatch { vectors, model })
}

fn embedding_http_error(status: u16) -> Option<StepError> {
    if status >= 500 || status == 429 || status == 408 {
        return Some(retryable(format!("embed provider returned HTTP {status}")));
    }
    if !(200..300).contains(&status) {
        return Some(permanent(format!("embed provider returned HTTP {status}")));
    }
    None
}

/// Reject malformed vectors at the boundary instead of changing their direction.
fn parse_vector(arr: &[Value]) -> Result<Vec<f64>, StepError> {
    if arr.is_empty() || arr.len() > MAX_EMBEDDING_DIMENSIONS {
        return Err(permanent(format!(
            "embedding vector must have 1..={MAX_EMBEDDING_DIMENSIONS} dimensions"
        )));
    }
    let vector: Vec<f64> = arr
        .iter()
        .map(|value| {
            value
                .as_f64()
                .filter(|number| number.is_finite())
                .ok_or_else(|| permanent("embedding vector must contain only finite numbers"))
        })
        .collect::<Result<_, _>>()?;
    if vector.iter().all(|value| *value == 0.0) {
        return Err(permanent("embedding vector must not be all zero"));
    }
    Ok(vector)
}

fn parse_supplied_embedding(value: &Value) -> Result<Vec<f64>, StepError> {
    let array = value
        .as_array()
        .ok_or_else(|| permanent("memory_store: `embedding` must be an array"))?;
    parse_vector(array)
}

/// Build a persisted memory record with retention and recorded provenance.
fn memory_record(
    text: Option<&str>,
    embedding: &[f64],
    embedding_model: Option<&str>,
    metadata: &Value,
    authorization: &MemoryAuthorization,
    retention_secs: u64,
    ctx: &StepContext,
) -> Result<Value, StepError> {
    let created_at = Utc::now();
    let retention = i64::try_from(retention_secs)
        .map_err(|_| permanent("memory: retention_secs is too large"))?;
    let duration = chrono::Duration::try_seconds(retention)
        .ok_or_else(|| permanent("memory: retention_secs exceeds supported duration range"))?;
    let expires_at = created_at
        .checked_add_signed(duration)
        .ok_or_else(|| permanent("memory: retention_secs exceeds supported timestamp range"))?;
    let content = json!({
        "text": text,
        "embedding": embedding,
        "embedding_model": embedding_model,
        "metadata": metadata,
    });
    let content_sha256 = memory_content_sha256(&content)?;
    Ok(json!({
        "text": text,
        "embedding": embedding,
        "embedding_model": embedding_model,
        "metadata": metadata,
        "governance": {
            "schema_version": 1,
            "tenant_id": authorization.tenant_id,
            "sequence_id": authorization.sequence_id,
            "instance_id": authorization.instance_id,
            "block_id": ctx.block_id,
            "policy_version": authorization.policy_version,
            "residency": authorization.residency,
            "created_at": created_at,
            "expires_at": expires_at,
            "content_sha256": content_sha256,
        }
    }))
}

fn memory_content_sha256(content: &Value) -> Result<String, StepError> {
    let canonical = orch8_publisher::manifest::canonical_json(content)
        .map_err(|error| permanent(format!("memory provenance serialization failed: {error}")))?;
    let digest = Sha256::digest(canonical.as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut hex, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(hex)
}

fn matches_content_commitment(record: &Value, governance: &Value) -> bool {
    let legacy = match governance.get("schema_version") {
        None => true,
        Some(version) if version.as_u64() == Some(1) => false,
        // Unknown governed schemas must not be interpreted as this version.
        Some(_) => return false,
    };
    let Some(expected) = governance.get("content_sha256") else {
        // Only unversioned legacy records may omit the commitment.
        return legacy;
    };
    let Some(expected) = expected.as_str() else {
        return false;
    };
    let content = json!({
        "text": record.get("text").cloned().unwrap_or(Value::Null),
        "embedding": record.get("embedding").cloned().unwrap_or(Value::Null),
        "embedding_model": record.get("embedding_model").cloned().unwrap_or(Value::Null),
        "metadata": record.get("metadata").cloned().unwrap_or(Value::Null),
    });
    memory_content_sha256(&content).is_ok_and(|actual| actual == expected)
}

/// Stable content-addressed key for new text memories. The `txt-` prefix
/// distinguishes it from the older `DefaultHasher` key format.
fn content_key(text: &str) -> String {
    let digest = Sha256::digest(text.as_bytes());
    let mut key = String::from("txt-");
    for byte in digest {
        write!(&mut key, "{byte:02x}").expect("writing to String cannot fail");
    }
    key
}

fn legacy_content_key(text: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    text.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

async fn preserve_legacy_text_key(
    ctx: &StepContext,
    scope: MemoryScope,
    namespace: &str,
    proposed: String,
    text: Option<&str>,
) -> Result<String, StepError> {
    if ctx.params.get("key").is_some() {
        return Ok(proposed);
    }
    let Some(text) = text else {
        return Ok(proposed);
    };
    let legacy = legacy_content_key(text);
    let existing = match scope {
        MemoryScope::Instance => {
            ctx.storage
                .get_instance_kv(ctx.instance_id, &format!("{MEMORY_KEY_PREFIX}{legacy}"))
                .await
        }
        MemoryScope::Tenant => {
            ctx.storage
                .get_shared_knowledge(ctx.tenant_id.as_str(), namespace, &legacy)
                .await
        }
    }
    .map_err(|error| retryable(format!("memory_store legacy key lookup failed: {error}")))?;
    if existing
        .as_ref()
        .and_then(|record| record.get("text"))
        .and_then(Value::as_str)
        == Some(text)
    {
        let modern = match scope {
            MemoryScope::Instance => {
                ctx.storage
                    .get_instance_kv(ctx.instance_id, &format!("{MEMORY_KEY_PREFIX}{proposed}"))
                    .await
            }
            MemoryScope::Tenant => {
                ctx.storage
                    .get_shared_knowledge(ctx.tenant_id.as_str(), namespace, &proposed)
                    .await
            }
        }
        .map_err(|error| retryable(format!("memory_store key lookup failed: {error}")))?;
        if modern
            .as_ref()
            .and_then(|record| record.get("text"))
            .and_then(Value::as_str)
            == Some(text)
        {
            return Ok(proposed);
        }
        Ok(legacy)
    } else {
        Ok(proposed)
    }
}

fn memory_key(
    params: &Value,
    text: Option<&str>,
    embedding: Option<&[f64]>,
) -> Result<String, StepError> {
    if let Some(value) = params.get("key") {
        return value
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| permanent("memory_store: `key` must be a string"));
    }
    if let Some(text) = text {
        return Ok(content_key(text));
    }
    let vector =
        embedding.ok_or_else(|| permanent("memory_store: `text` or `embedding` is required"))?;
    let mut hasher = Sha256::new();
    for value in vector {
        hasher.update(
            if *value == 0.0 { 0.0_f64 } else { *value }
                .to_bits()
                .to_be_bytes(),
        );
    }
    let digest = hasher.finalize();
    let mut key = String::from("vec-");
    for byte in &digest[..16] {
        write!(&mut key, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(key)
}

/// Cosine similarity in `[-1, 1]`. Returns 0.0 for zero-norm or
/// mismatched-length vectors (a non-comparable pair scores neutral).
fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let a_scale = a.iter().fold(0.0_f64, |max, value| max.max(value.abs()));
    let b_scale = b.iter().fold(0.0_f64, |max, value| max.max(value.abs()));
    if a_scale == 0.0 || b_scale == 0.0 || !a_scale.is_finite() || !b_scale.is_finite() {
        return 0.0;
    }
    let mut dot = 0.0;
    let mut na = 0.0;
    let mut nb = 0.0;
    for (x, y) in a.iter().zip(b.iter()) {
        let x = x / a_scale;
        let y = y / b_scale;
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    (dot / (na.sqrt() * nb.sqrt())).clamp(-1.0, 1.0)
}

/// Pull memory records out of the instance KV map, stripping the prefix.
fn extract_memory_records(kv: &std::collections::HashMap<String, Value>) -> Vec<(String, Value)> {
    kv.iter()
        .filter_map(|(k, v)| {
            k.strip_prefix(MEMORY_KEY_PREFIX)
                .map(|key| (key.to_string(), v.clone()))
        })
        .collect()
}

fn governed_records(
    records: Vec<(String, Value)>,
    authorization: &MemoryAuthorization,
    scope: MemoryScope,
    now: DateTime<Utc>,
) -> (Vec<(String, Value)>, Vec<String>) {
    let mut active = Vec::new();
    let mut expired = Vec::new();
    for (key, record) in records {
        let Some(governance) = record.get("governance") else {
            // Records written before governed memory shipped have no policy
            // envelope. Instance KV already binds them to this exact
            // instance and tenant, so keep them readable for compatibility.
            // Shared tenant records do not have that provenance guarantee and
            // therefore remain fail-closed until explicitly rewritten.
            if scope == MemoryScope::Instance {
                active.push((key, record));
            }
            continue;
        };
        if governance.get("tenant_id").and_then(Value::as_str)
            != Some(authorization.tenant_id.as_str())
            || governance.get("residency").and_then(Value::as_str)
                != Some(authorization.residency.as_str())
            || !matches_content_commitment(&record, governance)
        {
            continue;
        }
        let valid_instance = governance
            .get("instance_id")
            .and_then(Value::as_str)
            .is_some_and(|instance_id| instance_id == authorization.instance_id);
        let expires_at = governance
            .get("expires_at")
            .and_then(Value::as_str)
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc));
        let Some(expires_at) = expires_at else {
            continue;
        };
        // Instance-scoped records must originate from this exact instance.
        // Tenant records may originate from any authorized sequence/instance.
        if scope == MemoryScope::Instance && !valid_instance {
            continue;
        }
        if expires_at <= now {
            expired.push(key);
        } else {
            active.push((key, record));
        }
    }
    (active, expired)
}

async fn purge_expired(
    ctx: &StepContext,
    scope: MemoryScope,
    namespace: &str,
    expired: &[String],
) -> Result<(), StepError> {
    let result = match scope {
        MemoryScope::Instance => {
            let keys = expired
                .iter()
                .map(|key| format!("{MEMORY_KEY_PREFIX}{key}"))
                .collect::<Vec<_>>();
            ctx.storage
                .delete_instance_kv_batch(ctx.instance_id, &keys)
                .await
        }
        MemoryScope::Tenant => {
            ctx.storage
                .delete_shared_knowledge_batch(ctx.tenant_id.as_str(), namespace, expired)
                .await
        }
    };
    result.map_err(|error| retryable(format!("memory retention cleanup failed: {error}")))
}

/// Rank memories against a query embedding, returning the top `k` as result
/// objects sorted by descending score.
fn rank_memories(
    query: &[f64],
    records: Vec<(String, Value)>,
    top_k: usize,
    query_model: Option<&str>,
) -> Vec<Value> {
    let mut scored: Vec<(f64, String, Value)> = records
        .into_iter()
        .filter_map(|(key, record)| {
            if query_model.is_some_and(|query_model| {
                record
                    .get("embedding_model")
                    .and_then(Value::as_str)
                    .is_some_and(|stored_model| stored_model != query_model)
            }) {
                return None;
            }
            let emb = record
                .get("embedding")
                .and_then(Value::as_array)
                .and_then(|array| parse_vector(array).ok())?;
            if emb.len() != query.len() {
                return None;
            }
            let score = cosine_similarity(query, &emb);
            Some((score, key, record))
        })
        .collect();

    // Break equal-score ties by key so HashMap/database iteration order cannot
    // change which records land in a truncated top-k result set.
    scored.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    scored.truncate(top_k);

    scored
        .into_iter()
        .map(|(score, key, record)| {
            json!({
                "key": key,
                "text": record.get("text").cloned().unwrap_or(Value::Null),
                "embedding_model": record.get("embedding_model").cloned().unwrap_or(Value::Null),
                "score": score,
                "metadata": record.get("metadata").cloned().unwrap_or(json!({})),
                "provenance": record.get("governance").cloned().unwrap_or(Value::Null),
            })
        })
        .collect()
}

fn results_len(results: &[Value]) -> usize {
    results.len()
}

fn permanent(message: impl Into<String>) -> StepError {
    StepError::Permanent {
        message: message.into(),
        details: None,
    }
}

fn retryable(message: impl Into<String>) -> StepError {
    StepError::Retryable {
        message: message.into(),
        details: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn to_input_list_string_and_array() {
        assert_eq!(to_input_list(&json!("hi")).unwrap(), vec!["hi".to_string()]);
        assert_eq!(
            to_input_list(&json!(["a", "b"])).unwrap(),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn to_input_list_rejects_bad_shapes() {
        assert!(to_input_list(&json!(42)).is_err());
        assert!(to_input_list(&json!([])).is_err());
        assert!(to_input_list(&json!([1, 2])).is_err());
        assert!(to_input_list(&json!("")).is_err());
        assert!(to_input_list(&json!(["valid", ""])).is_err());
    }

    #[test]
    fn embed_input_limits_apply_before_request_construction() {
        let max_batch = vec!["x"; MAX_EMBED_INPUTS];
        assert_eq!(
            to_input_list(&json!(max_batch)).unwrap().len(),
            MAX_EMBED_INPUTS
        );
        assert!(to_input_list(&json!(vec!["x"; MAX_EMBED_INPUTS + 1])).is_err());

        let half_plus_one = "x".repeat(MAX_EMBED_INPUT_BYTES / 2 + 1);
        assert!(validate_embedding_input_text(&half_plus_one).is_ok());
        assert!(validate_embedding_batch([half_plus_one.as_str(); 2].into_iter()).is_err());
        assert!(validate_embedding_input_text(&"x".repeat(MAX_EMBED_INPUT_BYTES + 1)).is_err());
    }

    #[test]
    fn memory_text_limit_applies_even_with_a_supplied_vector() {
        assert!(validate_memory_text("").is_ok());
        assert!(validate_memory_text(&"x".repeat(MAX_EMBED_INPUT_BYTES)).is_ok());
        assert!(validate_memory_text(&"x".repeat(MAX_EMBED_INPUT_BYTES + 1)).is_err());
    }

    #[test]
    fn build_embeddings_request_shape() {
        let req = build_embeddings_request("text-embedding-3-small", &["hi".to_string()]);
        assert_eq!(req["model"], "text-embedding-3-small");
        assert_eq!(req["input"][0], "hi");
    }

    #[test]
    fn parse_embedding_response_orders_by_index() {
        let body = br#"{"data":[
            {"index":1,"embedding":[0.0,1.0]},
            {"index":0,"embedding":[1.0,0.0]}
        ],"model":"m"}"#;
        let batch = parse_embedding_response(200, body, 2, "requested").unwrap();
        assert_eq!(batch.model, "m");
        assert_eq!(batch.vectors.len(), 2);
        assert_eq!(batch.vectors[0], vec![1.0, 0.0]); // index 0 first
        assert_eq!(batch.vectors[1], vec![0.0, 1.0]);
    }

    #[test]
    fn parse_embedding_response_uses_requested_model_only_when_provider_omits_it() {
        let body = br#"{"data":[{"index":0,"embedding":[1.0]}]}"#;
        let batch = parse_embedding_response(200, body, 1, "requested-alias").unwrap();
        assert_eq!(batch.model, "requested-alias");
        for invalid_model in ["null", "12", "\"\"", "\"   \""] {
            let body = format!(
                "{{\"model\":{invalid_model},\"data\":[{{\"index\":0,\"embedding\":[1.0]}}]}}"
            );
            assert!(parse_embedding_response(200, body.as_bytes(), 1, "requested").is_err());
        }
    }

    #[test]
    fn parse_embedding_response_errors() {
        for status in [408, 429, 500, 503] {
            assert!(matches!(
                parse_embedding_response(status, b"x", 1, "m").unwrap_err(),
                StepError::Retryable { .. }
            ));
        }
        assert!(matches!(
            parse_embedding_response(401, b"x", 1, "m").unwrap_err(),
            StepError::Permanent { .. }
        ));
        for status in [101, 302, 304, 399] {
            let body = br#"{"data":[{"index":0,"embedding":[1.0]}],"model":"m"}"#;
            assert!(matches!(
                parse_embedding_response(status, body, 1, "m").unwrap_err(),
                StepError::Permanent { .. }
            ));
        }
        assert!(matches!(
            parse_embedding_response(200, b"not json", 1, "m").unwrap_err(),
            StepError::Permanent { .. }
        ));
        assert!(matches!(
            parse_embedding_response(200, br#"{"no":"data"}"#, 1, "m").unwrap_err(),
            StepError::Permanent { .. }
        ));
    }

    #[tokio::test]
    async fn embedding_http_errors_ignore_oversized_response_bodies() {
        for (status, retryable_error) in [(429, true), (503, true), (400, false), (302, false)] {
            let response = http::Response::builder()
                .status(status)
                .body(vec![0; MAX_EMBED_RESPONSE_BYTES + 1])
                .unwrap();
            let error = read_embedding_response(response.into(), 1, "model")
                .await
                .unwrap_err();
            match (retryable_error, error) {
                (true, StepError::Retryable { message, .. })
                | (false, StepError::Permanent { message, .. }) => {
                    assert!(message.contains(&format!("HTTP {status}")), "{message}");
                }
                (_, other) => panic!("wrong error classification: {other:?}"),
            }
        }

        let response = http::Response::builder()
            .status(200)
            .body(vec![0; MAX_EMBED_RESPONSE_BYTES + 1])
            .unwrap();
        let error = read_embedding_response(response.into(), 1, "model")
            .await
            .unwrap_err();
        assert!(matches!(error, StepError::Permanent { .. }));
    }

    #[test]
    fn parse_embedding_response_rejects_missing_duplicate_and_mixed_vectors() {
        for body in [
            r#"{"data":[{"index":0,"embedding":[1.0]}]}"#,
            r#"{"data":[{"index":0,"embedding":[1.0]},{"index":0,"embedding":[2.0]}]}"#,
            r#"{"data":[{"index":0,"embedding":[1.0]},{"index":1,"embedding":[1.0,2.0]}]}"#,
            r#"{"data":[{"index":0,"embedding":[1.0]},{"index":1,"embedding":["bad"]}]}"#,
        ] {
            assert!(
                parse_embedding_response(200, body.as_bytes(), 2, "m").is_err(),
                "{body}"
            );
        }
    }

    #[test]
    fn cosine_identical_is_one() {
        let v = vec![1.0, 2.0, 3.0];
        assert!((cosine_similarity(&v, &v) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn cosine_orthogonal_is_zero() {
        assert!((cosine_similarity(&[1.0, 0.0], &[0.0, 1.0])).abs() < 1e-9);
    }

    #[test]
    fn cosine_opposite_is_negative_one() {
        assert!((cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]) + 1.0).abs() < 1e-9);
    }

    #[test]
    fn cosine_handles_degenerate_inputs() {
        assert!(cosine_similarity(&[1.0], &[1.0, 2.0]).abs() < f64::EPSILON); // length mismatch
        assert!(cosine_similarity(&[], &[]).abs() < f64::EPSILON); // empty
        assert!(cosine_similarity(&[0.0, 0.0], &[1.0, 1.0]).abs() < f64::EPSILON);
        // zero norm
    }

    #[test]
    fn cosine_handles_large_finite_components_without_nan() {
        assert!((cosine_similarity(&[1e300, 0.0], &[1e300, 0.0]) - 1.0).abs() < f64::EPSILON);
        assert!((cosine_similarity(&[1e300, 0.0], &[-1e300, 0.0]) + 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn content_key_is_stable_and_hex() {
        let k1 = content_key("hello world");
        let k2 = content_key("hello world");
        assert_eq!(k1, k2);
        assert_ne!(k1, content_key("different"));
        assert_eq!(
            k1,
            "txt-b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
        assert_eq!(legacy_content_key("hello world").len(), 16);
    }

    #[test]
    fn embedding_only_keys_are_distinct_and_stable() {
        let params = json!({});
        let first = memory_key(&params, None, Some(&[1.0, 0.0])).unwrap();
        let again = memory_key(&params, None, Some(&[1.0, -0.0])).unwrap();
        let second = memory_key(&params, None, Some(&[0.0, 1.0])).unwrap();
        assert_eq!(first, again);
        assert_ne!(first, second);
        assert!(first.starts_with("vec-"));
    }

    #[test]
    fn explicit_memory_key_must_be_a_string() {
        for value in [json!(null), json!(42), json!(["key"])] {
            assert!(memory_key(&json!({"key": value}), Some("fact"), None).is_err());
        }
    }

    #[test]
    fn extract_memory_records_filters_prefix() {
        let mut kv = HashMap::new();
        kv.insert(
            format!("{MEMORY_KEY_PREFIX}a"),
            json!({"text": "mem a", "embedding": [1.0]}),
        );
        kv.insert("regular_state".to_string(), json!({"not": "a memory"}));
        let records = extract_memory_records(&kv);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, "a");
    }

    #[test]
    fn rank_memories_orders_by_similarity_and_truncates() {
        let records = vec![
            (
                "sky".to_string(),
                json!({"text": "sky is blue", "embedding": [1.0, 0.0]}),
            ),
            (
                "cat".to_string(),
                json!({"text": "cats", "embedding": [0.0, 1.0]}),
            ),
            (
                "sky2".to_string(),
                json!({"text": "blue sky", "embedding": [0.9, 0.1]}),
            ),
        ];
        // Query close to the "sky" direction.
        let results = rank_memories(&[1.0, 0.0], records, 2, None);
        assert_eq!(results.len(), 2);
        // Top two are the sky-ish memories, best first.
        assert_eq!(results[0]["key"], "sky");
        assert_eq!(results[1]["key"], "sky2");
        let top_score = results[0]["score"].as_f64().unwrap();
        let second_score = results[1]["score"].as_f64().unwrap();
        assert!(top_score >= second_score);
        assert!(top_score > 0.99);
    }

    #[test]
    fn rank_memories_breaks_equal_score_ties_by_key() {
        let records = vec![
            ("zeta".to_string(), json!({"embedding": [1.0, 0.0]})),
            ("alpha".to_string(), json!({"embedding": [2.0, 0.0]})),
        ];
        for order in [records.clone(), records.into_iter().rev().collect()] {
            let results = rank_memories(&[1.0, 0.0], order, 1, None);
            assert_eq!(results.len(), 1);
            assert_eq!(results[0]["key"], "alpha");
        }
    }

    #[test]
    fn rank_memories_skips_missing_embedding() {
        let records = vec![("bad".to_string(), json!({"text": "no embedding"}))];
        let results = rank_memories(&[1.0, 0.0], records, 5, None);
        assert!(results.is_empty());
    }

    #[test]
    fn rank_memories_filters_known_model_mismatches_but_keeps_legacy_records() {
        let records = vec![
            (
                "matching".to_string(),
                json!({"embedding": [1.0], "embedding_model": "model-a"}),
            ),
            (
                "different".to_string(),
                json!({"embedding": [1.0], "embedding_model": "model-b"}),
            ),
            ("legacy".to_string(), json!({"embedding": [1.0]})),
        ];
        let labeled = rank_memories(&[1.0], records.clone(), 5, Some("model-a"));
        assert_eq!(labeled.len(), 2);
        assert!(labeled.iter().any(|result| result["key"] == "matching"));
        assert!(labeled.iter().any(|result| result["key"] == "legacy"));
        assert!(labeled.iter().all(|result| result["key"] != "different"));
        assert_eq!(rank_memories(&[1.0], records, 5, None).len(), 3);
    }

    #[test]
    fn model_parameter_must_be_a_nonempty_string() {
        assert_eq!(requested_model(&json!({})).unwrap(), None);
        assert_eq!(
            requested_model(&json!({"model": "custom"})).unwrap(),
            Some("custom")
        );
        for value in [json!(null), json!(42), json!(""), json!("   ")] {
            assert!(requested_model(&json!({"model": value})).is_err());
        }
    }

    #[test]
    fn rank_memories_empty_corpus() {
        assert!(rank_memories(&[1.0], vec![], 5, None).is_empty());
    }

    #[test]
    fn parse_vector_rejects_invalid_elements_and_empty_vectors() {
        assert!(parse_vector(&[json!(1.5), json!("x")]).is_err());
        assert!(parse_vector(&[]).is_err());
        assert!(parse_vector(&[json!(0.0), json!(0.0)]).is_err());
        assert_eq!(
            parse_vector(&[json!(1.5), json!(2.0)]).unwrap(),
            vec![1.5, 2.0]
        );
    }

    #[test]
    fn memory_scope_and_namespace_are_bounded() {
        assert!(matches!(
            memory_scope(&json!({"scope": "tenant"})),
            Ok(MemoryScope::Tenant)
        ));
        assert!(memory_scope(&json!({"scope": "global"})).is_err());
        assert_eq!(
            memory_namespace(&json!({"namespace": "support/product-a"})).unwrap(),
            "support/product-a"
        );
        assert!(memory_namespace(&json!({"namespace": "bad namespace"})).is_err());
        for value in [json!(null), json!(42), json!(["tenant"])] {
            assert!(memory_scope(&json!({"scope": value})).is_err());
            assert!(memory_namespace(&json!({"namespace": value})).is_err());
        }
    }

    #[test]
    fn top_k_rejects_invalid_explicit_values() {
        assert_eq!(parse_top_k(&json!({})).unwrap(), 5);
        assert_eq!(parse_top_k(&json!({"top_k": 100})).unwrap(), 100);
        for value in [json!(null), json!("5"), json!(-1), json!(0), json!(101)] {
            assert!(parse_top_k(&json!({"top_k": value})).is_err());
        }
    }

    #[test]
    fn governance_filters_expired_and_legacy_shared_records() {
        let now = Utc::now();
        let authorization = MemoryAuthorization {
            sequence_id: orch8_types::ids::SequenceId::new(),
            tenant_id: "tenant-a".into(),
            instance_id: "instance-a".into(),
            residency: "br-south-1".into(),
            policy_version: 1,
            default_retention_secs: 60,
            max_retention_secs: 120,
        };
        let governed = |expires_at: DateTime<Utc>| {
            json!({
                "text": "fact",
                "embedding": [1.0],
                "metadata": {},
                "governance": {
                    "tenant_id": "tenant-a",
                    "instance_id": "instance-a",
                    "residency": "br-south-1",
                    "expires_at": expires_at,
                }
            })
        };
        let records = vec![
            (
                "active".into(),
                governed(now + chrono::Duration::seconds(1)),
            ),
            (
                "expired".into(),
                governed(now - chrono::Duration::seconds(1)),
            ),
            (
                "legacy".into(),
                json!({"text": "old", "embedding": [1.0], "metadata": {}}),
            ),
        ];

        let (active, expired) =
            governed_records(records.clone(), &authorization, MemoryScope::Tenant, now);
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].0, "active");
        assert_eq!(expired, vec!["expired"]);

        let (instance_active, _) =
            governed_records(records, &authorization, MemoryScope::Instance, now);
        assert!(instance_active.iter().any(|(key, _)| key == "legacy"));
    }
}

/// Tests that drive the async network (`embed`) and storage-backed
/// (`memory_store` / `memory_search`) paths against an in-process HTTP mock
/// and in-memory `SQLite`, without the e2e server.
#[cfg(test)]
mod net_tests {
    use super::*;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use orch8_storage::{StorageBackend, sqlite::SqliteStorage};
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{InstanceState, Priority, TaskInstance};

    async fn seed_instance(
        storage: &Arc<dyn StorageBackend>,
        instance_id: InstanceId,
        tenant_id: TenantId,
        sequence_id: SequenceId,
    ) {
        let now = Utc::now();
        storage
            .create_instance(&TaskInstance {
                id: instance_id,
                sequence_id,
                tenant_id,
                namespace: Namespace::new("default"),
                state: InstanceState::Running,
                next_fire_at: None,
                priority: Priority::Normal,
                timezone: "UTC".into(),
                metadata: json!({}),
                context: ExecutionContext::default(),
                concurrency_key: None,
                max_concurrency: None,
                idempotency_key: None,
                session_id: None,
                parent_instance_id: None,
                budget: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
    }

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    async fn read_request_body(sock: &mut tokio::net::TcpStream) -> String {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        loop {
            let n = sock.read(&mut tmp).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&buf[..pos]).to_lowercase();
                let want = headers
                    .split("content-length:")
                    .nth(1)
                    .and_then(|s| s.trim().split([' ', '\r', '\n']).next())
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                let body_start = pos + 4;
                if buf.len() >= body_start + want {
                    return String::from_utf8_lossy(&buf[body_start..body_start + want])
                        .to_string();
                }
            }
        }
        String::new()
    }

    /// Spawn an embeddings mock that returns `[0.1, 0.2, 0.3]` per input.
    async fn spawn_embed_mock(count: usize) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for _ in 0..count {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let body = read_request_body(&mut sock).await;
                let req: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
                let inputs = req
                    .get("input")
                    .and_then(Value::as_array)
                    .map_or(1, Vec::len);
                let data: Vec<Value> = (0..inputs)
                    .map(|i| json!({ "index": i, "embedding": [0.1, 0.2, 0.3] }))
                    .collect();
                let resp = json!({ "data": data, "model": "m" }).to_string();
                let out = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp}",
                    resp.len()
                );
                let _ = sock.write_all(out.as_bytes()).await;
                let _ = sock.flush().await;
            }
        });
        format!("http://127.0.0.1:{}", addr.port())
    }

    async fn mk_ctx(params: Value) -> StepContext {
        // Mark the embeddings endpoint safe via the cache (not env) so the
        // loopback mock is reachable without racing other parallel tests.
        if let Some(base) = params.get("base_url").and_then(Value::as_str) {
            let url = format!("{}/embeddings", base.trim_end_matches('/'));
            super::super::builtin::mark_url_safe_for_test(&url).await;
        }
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let instance_id = InstanceId::new();
        let tenant_id = TenantId::unchecked("t");
        seed_instance(&storage, instance_id, tenant_id.clone(), SequenceId::new()).await;
        StepContext {
            instance_id,
            tenant_id,
            block_id: BlockId::new("b"),
            params,
            context: Arc::new(ExecutionContext::default()),
            attempt: 0,
            storage,
            wait_for_input: None,
        }
    }

    #[tokio::test]
    async fn embed_single_input_returns_vector() {
        let url = spawn_embed_mock(1).await;
        let ctx = mk_ctx(json!({ "input": "hello", "base_url": url, "api_key": "k" })).await;
        let out = handle_embed(ctx).await.unwrap();
        assert_eq!(out["embedding"], json!([0.1, 0.2, 0.3]));
        assert_eq!(out["model"], "m");
        assert_eq!(out["dimensions"], 3);
    }

    #[tokio::test]
    async fn embed_batch_input_returns_vectors() {
        let url = spawn_embed_mock(1).await;
        let ctx = mk_ctx(json!({ "input": ["a", "b"], "base_url": url, "api_key": "k" })).await;
        let out = handle_embed(ctx).await.unwrap();
        assert_eq!(out["embeddings"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn embed_missing_input_is_permanent() {
        let ctx = mk_ctx(json!({ "api_key": "k" })).await;
        assert!(matches!(
            handle_embed(ctx).await.unwrap_err(),
            StepError::Permanent { .. }
        ));
    }

    #[tokio::test]
    async fn dry_run_embed_skips_provider() {
        // No mock spawned: if the handler tried to embed, it would fail to
        // connect. Returning Ok proves the provider call was skipped.
        let mut ctx =
            mk_ctx(json!({ "input": "hello", "base_url": "http://127.0.0.1:1", "api_key": "k" }))
                .await;
        Arc::make_mut(&mut ctx.context).runtime.dry_run = true;
        let out = handle_embed(ctx).await.unwrap();
        assert_eq!(out["dry_run"], true);
        assert_eq!(out["embedding"], json!([]));
        assert_eq!(out["dimensions"], 0);
    }

    #[tokio::test]
    async fn dry_run_embed_rejects_empty_batch_member() {
        let mut ctx = mk_ctx(json!({"input": ["valid", ""]})).await;
        Arc::make_mut(&mut ctx.context).runtime.dry_run = true;
        assert!(matches!(
            handle_embed(ctx).await,
            Err(StepError::Permanent { .. })
        ));
    }

    #[tokio::test]
    async fn embed_rejects_malformed_provider_options_before_network_access() {
        for dry_run in [false, true] {
            for params in [
                json!({"input": "hello", "base_url": 42, "api_key": "k"}),
                json!({"input": "hello", "base_url": "https://example.com/v1?token=x", "api_key": "k"}),
                json!({"input": "hello", "timeout_ms": 0, "api_key": "k"}),
                json!({"input": "hello", "timeout_ms": "30000", "api_key": "k"}),
            ] {
                let mut ctx = mk_ctx(params).await;
                Arc::make_mut(&mut ctx.context).runtime.dry_run = dry_run;
                assert!(matches!(
                    handle_embed(ctx).await,
                    Err(StepError::Permanent { .. })
                ));
            }
        }
    }

    #[tokio::test]
    async fn dry_run_memory_store_does_not_persist() {
        let mut ctx = mk_ctx(json!({ "text": "remember me", "key": "k1" })).await;
        Arc::make_mut(&mut ctx.context).runtime.dry_run = true;
        let storage = ctx.storage.clone();
        let instance_id = ctx.instance_id;
        let out = handle_memory_store(ctx).await.unwrap();
        assert_eq!(out["dry_run"], true);
        assert_eq!(out["stored"], false);
        // Nothing was written to the instance KV.
        let stored = storage
            .get_instance_kv(instance_id, &format!("{MEMORY_KEY_PREFIX}k1"))
            .await
            .unwrap();
        assert!(stored.is_none(), "dry-run must not persist a memory record");
    }

    #[tokio::test]
    async fn dry_run_memory_search_returns_empty() {
        let mut ctx =
            mk_ctx(json!({ "query": "anything", "base_url": "http://127.0.0.1:1" })).await;
        Arc::make_mut(&mut ctx.context).runtime.dry_run = true;
        let out = handle_memory_search(ctx).await.unwrap();
        assert_eq!(out["dry_run"], true);
        assert_eq!(out["results"], json!([]));
        assert_eq!(out["count"], 0);
    }

    #[tokio::test]
    async fn store_then_search_with_precomputed_embeddings() {
        // Share one storage handle / instance across store + search so the
        // instance KV persists between the two handler calls. Precomputed
        // embeddings → no network, so no SSRF cache seeding is needed.
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let instance_id = InstanceId::new();
        seed_instance(
            &storage,
            instance_id,
            TenantId::unchecked("t"),
            SequenceId::new(),
        )
        .await;
        let base = StepContext {
            instance_id,
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("b"),
            params: Value::Null,
            context: Arc::new(ExecutionContext::default()),
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };

        let mut store_sky = base.clone();
        store_sky.params = json!({ "key": "sky", "text": "sky is blue", "embedding": [1.0, 0.0] });
        let stored = handle_memory_store(store_sky).await.unwrap();
        assert_eq!(stored["key"], "sky");
        assert_eq!(stored["stored"], true);
        assert_eq!(stored["dimensions"], 2);

        let mut store_cat = base.clone();
        store_cat.params = json!({ "key": "cat", "text": "cats", "embedding": [0.0, 1.0] });
        handle_memory_store(store_cat).await.unwrap();

        let mut search = base.clone();
        search.params = json!({ "query_embedding": [1.0, 0.0], "top_k": 1 });
        let found = handle_memory_search(search).await.unwrap();
        assert_eq!(found["count"], 1);
        assert_eq!(found["results"][0]["key"], "sky");
        assert!(found["results"][0]["score"].as_f64().unwrap() > 0.99);
    }

    #[tokio::test]
    async fn embedding_only_memories_do_not_overwrite_each_other() {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let instance_id = InstanceId::new();
        seed_instance(
            &storage,
            instance_id,
            TenantId::unchecked("t"),
            SequenceId::new(),
        )
        .await;
        let base = StepContext {
            instance_id,
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("b"),
            params: Value::Null,
            context: Arc::new(ExecutionContext::default()),
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };
        let mut first = base.clone();
        first.params = json!({"embedding": [1.0, 0.0]});
        let first_key = handle_memory_store(first).await.unwrap()["key"]
            .as_str()
            .unwrap()
            .to_string();
        let mut second = base.clone();
        second.params = json!({"embedding": [0.0, 1.0]});
        let second_key = handle_memory_store(second).await.unwrap()["key"]
            .as_str()
            .unwrap()
            .to_string();
        assert_ne!(first_key, second_key);

        let mut search = base.clone();
        search.params = json!({"query_embedding": [1.0, 0.0], "top_k": 2});
        let result = handle_memory_search(search).await.unwrap();
        assert_eq!(result["count"], 2);
        assert_eq!(result["results"][0]["key"], first_key);
    }

    #[tokio::test]
    async fn memory_search_excludes_known_other_embedding_models() {
        let base = mk_ctx(json!({})).await;
        for (key, model) in [
            ("a", Some("model-a")),
            ("b", Some("model-b")),
            ("legacy", None),
        ] {
            let mut store = base.clone();
            store.params = json!({"key": key, "embedding": [1.0, 0.0]});
            if let Some(model) = model {
                store.params["model"] = json!(model);
            }
            handle_memory_store(store).await.unwrap();
        }

        let mut labeled_search = base.clone();
        labeled_search.params = json!({"query_embedding": [1.0, 0.0], "model": "model-a"});
        let labeled = handle_memory_search(labeled_search).await.unwrap();
        assert_eq!(labeled["count"], 2);
        let keys: Vec<&str> = labeled["results"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|result| result["key"].as_str())
            .collect();
        assert!(keys.contains(&"a"));
        assert!(keys.contains(&"legacy"));
        assert!(!keys.contains(&"b"));

        let mut unlabeled_search = base;
        unlabeled_search.params = json!({"query_embedding": [1.0, 0.0]});
        assert_eq!(
            handle_memory_search(unlabeled_search).await.unwrap()["count"],
            3
        );
    }

    #[tokio::test]
    async fn memory_search_top_k_uses_stable_key_order_for_score_ties() {
        let base = mk_ctx(json!({})).await;
        for key in ["zeta", "alpha"] {
            let mut store = base.clone();
            store.params = json!({"key": key, "embedding": [1.0, 0.0]});
            handle_memory_store(store).await.unwrap();
        }
        let mut search = base;
        search.params = json!({"query_embedding": [1.0, 0.0], "top_k": 1});
        let result = handle_memory_search(search).await.unwrap();
        assert_eq!(result["count"], 1);
        assert_eq!(result["results"][0]["key"], "alpha");
    }

    #[tokio::test]
    async fn store_and_search_reject_malformed_precomputed_vectors() {
        let mut store = mk_ctx(json!({"text": "fact", "embedding": [1.0, "bad"]})).await;
        assert!(matches!(
            handle_memory_store(store.clone()).await,
            Err(StepError::Permanent { .. })
        ));
        store.params = json!({"query_embedding": [0.0, 0.0]});
        assert!(matches!(
            handle_memory_search(store).await,
            Err(StepError::Permanent { .. })
        ));
    }

    #[tokio::test]
    async fn supplied_embedding_rejects_oversized_text_before_persistence() {
        let base = mk_ctx(json!({
            "text": "x".repeat(MAX_EMBED_INPUT_BYTES + 1),
            "embedding": [1.0],
        }))
        .await;
        for dry_run in [false, true] {
            let mut ctx = base.clone();
            Arc::make_mut(&mut ctx.context).runtime.dry_run = dry_run;
            assert!(matches!(
                handle_memory_store(ctx).await,
                Err(StepError::Permanent { .. })
            ));
        }
        assert!(
            base.storage
                .get_all_instance_kv(base.instance_id)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn memory_handlers_reject_mistyped_routing_and_retention_before_writes() {
        let base = mk_ctx(json!({"text": "fact", "embedding": [1.0]})).await;
        for params in [
            json!({"text": "fact", "embedding": [1.0], "scope": null}),
            json!({"text": "fact", "embedding": [1.0], "namespace": 42}),
            json!({"text": "fact", "embedding": [1.0], "retention_secs": "forever"}),
            json!({"text": "fact", "embedding": [1.0], "key": 42}),
            json!({"text": "fact", "embedding": [1.0], "residency": null}),
            json!({"text": "fact", "embedding": [1.0], "residency": 42}),
            json!({"text": 42, "embedding": [1.0]}),
        ] {
            let mut store = base.clone();
            store.params = params;
            assert!(matches!(
                handle_memory_store(store).await,
                Err(StepError::Permanent { .. })
            ));
        }
        assert!(
            base.storage
                .get_all_instance_kv(base.instance_id)
                .await
                .unwrap()
                .is_empty()
        );

        let mut search = base.clone();
        search.params = json!({"query_embedding": [1.0], "scope": false});
        assert!(matches!(
            handle_memory_search(search).await,
            Err(StepError::Permanent { .. })
        ));

        for dry_run in [false, true] {
            let mut search = base.clone();
            search.params = json!({"query_embedding": [1.0], "residency": false});
            Arc::make_mut(&mut search.context).runtime.dry_run = dry_run;
            assert!(matches!(
                handle_memory_search(search).await,
                Err(StepError::Permanent { .. })
            ));

            let mut delete = base.clone();
            delete.params = json!({"key": "fact", "residency": []});
            Arc::make_mut(&mut delete.context).runtime.dry_run = dry_run;
            assert!(matches!(
                handle_memory_delete(delete).await,
                Err(StepError::Permanent { .. })
            ));
        }
    }

    #[tokio::test]
    async fn invalid_store_and_search_inputs_fail_before_embedding_provider_call() {
        // A closed localhost port would produce a retryable network error if
        // any invalid request reached the embedding provider.
        let base = mk_ctx(json!({
            "base_url": "http://127.0.0.1:1",
            "api_key": "k",
        }))
        .await;
        for params in [
            json!({"text": "fact", "key": "", "base_url": "http://127.0.0.1:1", "api_key": "k"}),
            json!({"text": "", "base_url": "http://127.0.0.1:1", "api_key": "k"}),
        ] {
            for dry_run in [false, true] {
                let mut store = base.clone();
                store.params = params.clone();
                Arc::make_mut(&mut store.context).runtime.dry_run = dry_run;
                assert!(matches!(
                    handle_memory_store(store).await,
                    Err(StepError::Permanent { .. })
                ));
            }
        }
        for dry_run in [false, true] {
            let mut search = base.clone();
            search.params = json!({"query": "", "base_url": "http://127.0.0.1:1", "api_key": "k"});
            Arc::make_mut(&mut search.context).runtime.dry_run = dry_run;
            assert!(matches!(
                handle_memory_search(search).await,
                Err(StepError::Permanent { .. })
            ));
        }
    }

    #[tokio::test]
    async fn search_rejects_invalid_top_k_in_normal_and_dry_run_modes() {
        let base = mk_ctx(json!({"query_embedding": [1.0]})).await;
        for dry_run in [false, true] {
            let mut search = base.clone();
            search.params = json!({"query_embedding": [1.0], "top_k": "many"});
            Arc::make_mut(&mut search.context).runtime.dry_run = dry_run;
            assert!(matches!(
                handle_memory_search(search).await,
                Err(StepError::Permanent { .. })
            ));
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn tenant_memory_is_shared_across_instances_but_not_tenants() {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let context = Arc::new(ExecutionContext::default());
        let sequence_a = SequenceId::new();
        let sequence_b = SequenceId::new();
        let writer_instance = InstanceId::new();
        let reader_instance = InstanceId::new();
        let isolated_instance = InstanceId::new();
        seed_instance(
            &storage,
            writer_instance,
            TenantId::unchecked("tenant-a"),
            sequence_a,
        )
        .await;
        seed_instance(
            &storage,
            reader_instance,
            TenantId::unchecked("tenant-a"),
            sequence_a,
        )
        .await;
        seed_instance(
            &storage,
            isolated_instance,
            TenantId::unchecked("tenant-b"),
            sequence_b,
        )
        .await;
        let policy = |sequence_id| crate::memory_governance::MemoryNamespacePolicy {
            policy_version: 1,
            allowed_sequence_ids: vec![sequence_id],
            operations: vec![
                MemoryOperation::Store,
                MemoryOperation::Search,
                MemoryOperation::Delete,
            ],
            residency: "br-south-1".into(),
            default_retention_secs: 3_600,
            max_retention_secs: 86_400,
        };
        crate::memory_governance::install_namespace_policy(
            storage.as_ref(),
            &TenantId::unchecked("tenant-a"),
            "research",
            &policy(sequence_a),
        )
        .await
        .unwrap();
        crate::memory_governance::install_namespace_policy(
            storage.as_ref(),
            &TenantId::unchecked("tenant-b"),
            "research",
            &policy(sequence_b),
        )
        .await
        .unwrap();
        let base = |tenant: &str, instance_id: InstanceId, params: Value| StepContext {
            instance_id,
            tenant_id: TenantId::unchecked(tenant),
            block_id: BlockId::new("memory"),
            params,
            context: Arc::clone(&context),
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };

        handle_memory_store(base(
            "tenant-a",
            writer_instance,
            json!({
                "scope": "tenant",
                "namespace": "research",
                "key": "shared-fact",
                "text": "shared across agents",
                "embedding": [1.0, 0.0]
            }),
        ))
        .await
        .unwrap();

        let found = handle_memory_search(base(
            "tenant-a",
            reader_instance,
            json!({
                "scope": "tenant",
                "namespace": "research",
                "query_embedding": [1.0, 0.0]
            }),
        ))
        .await
        .unwrap();
        assert_eq!(found["count"], 1);
        assert_eq!(found["results"][0]["key"], "shared-fact");
        assert_eq!(found["residency"], "br-south-1");
        assert_eq!(found["corpus_truncated"], false);
        assert_eq!(
            found["results"][0]["provenance"]["sequence_id"],
            sequence_a.to_string()
        );

        let isolated = handle_memory_search(base(
            "tenant-b",
            isolated_instance,
            json!({
                "scope": "tenant",
                "namespace": "research",
                "query_embedding": [1.0, 0.0]
            }),
        ))
        .await
        .unwrap();
        assert_eq!(isolated["count"], 0);

        let mut expired_record = storage
            .get_shared_knowledge("tenant-a", "research", "shared-fact")
            .await
            .unwrap()
            .unwrap();
        expired_record["governance"]["expires_at"] = json!("2020-01-01T00:00:00Z");
        storage
            .set_shared_knowledge("tenant-a", "research", "expired-a", &expired_record)
            .await
            .unwrap();
        storage
            .set_shared_knowledge("tenant-a", "research", "expired-b", &expired_record)
            .await
            .unwrap();

        let cleaned = handle_memory_search(base(
            "tenant-a",
            reader_instance,
            json!({
                "scope": "tenant",
                "namespace": "research",
                "query_embedding": [1.0, 0.0]
            }),
        ))
        .await
        .unwrap();
        assert_eq!(cleaned["expired_deleted"], 2);
        assert!(
            storage
                .get_shared_knowledge("tenant-a", "research", "expired-a")
                .await
                .unwrap()
                .is_none()
        );

        let deleted = handle_memory_delete(base(
            "tenant-a",
            writer_instance,
            json!({
                "scope": "tenant",
                "namespace": "research",
                "key": "shared-fact"
            }),
        ))
        .await
        .unwrap();
        assert_eq!(deleted["deleted"], true);

        let after_delete = handle_memory_search(base(
            "tenant-a",
            reader_instance,
            json!({
                "scope": "tenant",
                "namespace": "research",
                "query_embedding": [1.0, 0.0]
            }),
        ))
        .await
        .unwrap();
        assert_eq!(after_delete["count"], 0);
    }

    #[tokio::test]
    async fn tenant_search_reports_when_the_corpus_exceeds_its_scan_limit() {
        let ctx = mk_ctx(json!({
            "scope": "tenant",
            "namespace": "large",
            "query_embedding": [1.0],
            "top_k": 1
        }))
        .await;
        let instance = ctx
            .storage
            .get_instance(ctx.instance_id)
            .await
            .unwrap()
            .unwrap();
        crate::memory_governance::install_namespace_policy(
            ctx.storage.as_ref(),
            &ctx.tenant_id,
            "large",
            &crate::memory_governance::MemoryNamespacePolicy {
                policy_version: 1,
                allowed_sequence_ids: vec![instance.sequence_id],
                operations: vec![MemoryOperation::Search],
                residency: "local".into(),
                default_retention_secs: 3_600,
                max_retention_secs: 3_600,
            },
        )
        .await
        .unwrap();
        let authorization = MemoryAuthorization {
            sequence_id: instance.sequence_id,
            tenant_id: ctx.tenant_id.to_string(),
            instance_id: ctx.instance_id.to_string(),
            residency: "local".into(),
            policy_version: 1,
            default_retention_secs: 3_600,
            max_retention_secs: 3_600,
        };
        let record = memory_record(
            Some("fact"),
            &[1.0],
            None,
            &json!({}),
            &authorization,
            3_600,
            &ctx,
        )
        .unwrap();
        for index in 0..MAX_SHARED_RECORDS {
            ctx.storage
                .set_shared_knowledge(
                    ctx.tenant_id.as_str(),
                    "large",
                    &format!("k-{index:05}"),
                    &record,
                )
                .await
                .unwrap();
        }
        let exact_limit = handle_memory_search(ctx.clone()).await.unwrap();
        assert_eq!(exact_limit["corpus_truncated"], false);
        assert_eq!(exact_limit["count"], 1);

        ctx.storage
            .set_shared_knowledge(ctx.tenant_id.as_str(), "large", "overflow", &record)
            .await
            .unwrap();
        let overflow = handle_memory_search(ctx).await.unwrap();
        assert_eq!(overflow["corpus_truncated"], true);
        assert_eq!(overflow["count"], 1);
    }

    #[tokio::test]
    async fn tenant_memory_denies_unlisted_sequence_and_excess_retention() {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let tenant = TenantId::unchecked("tenant-a");
        let allowed_sequence = SequenceId::new();
        let denied_sequence = SequenceId::new();
        let allowed_instance = InstanceId::new();
        let denied_instance = InstanceId::new();
        seed_instance(&storage, allowed_instance, tenant.clone(), allowed_sequence).await;
        seed_instance(&storage, denied_instance, tenant.clone(), denied_sequence).await;
        crate::memory_governance::install_namespace_policy(
            storage.as_ref(),
            &tenant,
            "bounded",
            &crate::memory_governance::MemoryNamespacePolicy {
                policy_version: 1,
                allowed_sequence_ids: vec![allowed_sequence],
                operations: vec![MemoryOperation::Store],
                residency: "br-south-1".into(),
                default_retention_secs: 60,
                max_retention_secs: 120,
            },
        )
        .await
        .unwrap();
        let context = Arc::new(ExecutionContext::default());
        let make_ctx = |instance_id, retention_secs| StepContext {
            instance_id,
            tenant_id: tenant.clone(),
            block_id: BlockId::new("memory"),
            params: json!({
                "scope": "tenant",
                "namespace": "bounded",
                "text": "fact",
                "embedding": [1.0],
                "retention_secs": retention_secs
            }),
            context: Arc::clone(&context),
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };

        assert!(matches!(
            handle_memory_store(make_ctx(denied_instance, 60)).await,
            Err(StepError::Permanent { .. })
        ));
        assert!(matches!(
            handle_memory_store(make_ctx(allowed_instance, 121)).await,
            Err(StepError::Permanent { .. })
        ));
    }

    #[tokio::test]
    async fn store_computes_embedding_from_text() {
        let url = spawn_embed_mock(1).await;
        let ctx =
            mk_ctx(json!({ "key": "d1", "text": "doc", "base_url": url, "api_key": "k" })).await;
        let out = handle_memory_store(ctx.clone()).await.unwrap();
        assert_eq!(out["key"], "d1");
        assert_eq!(out["dimensions"], 3);
        let record = ctx
            .storage
            .get_instance_kv(ctx.instance_id, "__mem__:d1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record["embedding_model"], "m");
    }

    #[tokio::test]
    async fn store_reuses_matching_legacy_text_key_without_overwriting_a_collision() {
        let ctx = mk_ctx(json!({"text": "fact", "embedding": [1.0]})).await;
        let legacy = legacy_content_key("fact");
        let storage_key = format!("{MEMORY_KEY_PREFIX}{legacy}");
        ctx.storage
            .set_instance_kv(
                ctx.instance_id,
                &storage_key,
                &json!({"text": "other fact", "embedding": [0.0, 1.0]}),
            )
            .await
            .unwrap();

        let new_key = handle_memory_store(ctx.clone()).await.unwrap()["key"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(new_key, content_key("fact"));
        assert_eq!(
            ctx.storage
                .get_instance_kv(ctx.instance_id, &storage_key)
                .await
                .unwrap()
                .unwrap()["text"],
            "other fact"
        );

        ctx.storage
            .set_instance_kv(
                ctx.instance_id,
                &storage_key,
                &json!({"text": "fact", "embedding": [1.0]}),
            )
            .await
            .unwrap();
        assert_eq!(
            handle_memory_store(ctx.clone()).await.unwrap()["key"],
            new_key
        );
        ctx.storage
            .delete_instance_kv(ctx.instance_id, &format!("{MEMORY_KEY_PREFIX}{new_key}"))
            .await
            .unwrap();
        assert_eq!(handle_memory_store(ctx).await.unwrap()["key"], legacy);
    }

    #[tokio::test]
    async fn tenant_key_lookup_preserves_matching_legacy_memory() {
        let ctx = mk_ctx(json!({"text": "shared fact", "embedding": [1.0]})).await;
        let legacy = legacy_content_key("shared fact");
        ctx.storage
            .set_shared_knowledge(
                ctx.tenant_id.as_str(),
                "research",
                &legacy,
                &json!({"text": "shared fact", "embedding": [1.0]}),
            )
            .await
            .unwrap();
        let key = preserve_legacy_text_key(
            &ctx,
            MemoryScope::Tenant,
            "research",
            content_key("shared fact"),
            Some("shared fact"),
        )
        .await
        .unwrap();
        assert_eq!(key, legacy);
    }

    #[tokio::test]
    async fn search_uses_provider_model_when_request_used_an_alias() {
        let url = spawn_embed_mock(1).await;
        let mut ctx = mk_ctx(json!({
            "key": "canonical-model",
            "text": "fact",
            "embedding": [0.1, 0.2, 0.3],
            "model": "m"
        }))
        .await;
        handle_memory_store(ctx.clone()).await.unwrap();

        super::super::builtin::mark_url_safe_for_test(&format!("{url}/embeddings")).await;
        ctx.params = json!({"query": "fact", "base_url": url, "api_key": "k"});
        let found = handle_memory_search(ctx).await.unwrap();
        assert_eq!(found["count"], 1);
        assert_eq!(found["results"][0]["key"], "canonical-model");
        assert_eq!(found["results"][0]["embedding_model"], "m");
    }

    #[tokio::test]
    async fn store_without_text_or_embedding_is_permanent() {
        let ctx = mk_ctx(json!({ "key": "x" })).await;
        assert!(matches!(
            handle_memory_store(ctx).await.unwrap_err(),
            StepError::Permanent { .. }
        ));
    }

    #[tokio::test]
    async fn search_without_query_is_permanent() {
        let ctx = mk_ctx(json!({ "top_k": 3 })).await;
        assert!(matches!(
            handle_memory_search(ctx).await.unwrap_err(),
            StepError::Permanent { .. }
        ));
    }

    #[tokio::test]
    async fn search_empty_corpus_returns_no_results() {
        let ctx = mk_ctx(json!({ "query_embedding": [1.0, 0.0] })).await;
        let out = handle_memory_search(ctx).await.unwrap();
        assert_eq!(out["count"], 0);
    }

    #[test]
    fn resolve_api_key_direct() {
        assert_eq!(
            resolve_api_key(&json!({ "api_key": "abc" })).unwrap(),
            "abc"
        );
    }

    #[test]
    fn resolve_api_key_from_env() {
        // Read an env var that is already present (PATH) rather than mutating
        // the process environment, which would race other parallel tests.
        let expected = std::env::var("PATH").expect("PATH is set");
        assert_eq!(
            resolve_api_key(&json!({ "api_key_env": "PATH" })).unwrap(),
            expected
        );
    }

    #[test]
    fn resolve_api_key_missing_is_permanent() {
        assert!(matches!(
            resolve_api_key(&json!({})).unwrap_err(),
            StepError::Permanent { .. }
        ));
        assert!(matches!(
            resolve_api_key(&json!({ "api_key_env": "ORCH8_DEFINITELY_UNSET_VAR_QQ" }))
                .unwrap_err(),
            StepError::Permanent { .. }
        ));
        assert!(resolve_api_key(&json!({"api_key": ""})).is_err());
        assert!(resolve_api_key(&json!({"api_key": 123, "api_key_env": "PATH"})).is_err());
        assert!(resolve_api_key(&json!({"api_key_env": 123})).is_err());
    }

    #[test]
    fn provider_url_and_timeout_defaults_and_bounds() {
        assert_eq!(
            resolve_embedding_url(&json!({})).unwrap(),
            "https://api.openai.com/v1/embeddings"
        );
        assert_eq!(
            resolve_embedding_url(&json!({"base_url": "https://example.com/v2/"})).unwrap(),
            "https://example.com/v2/embeddings"
        );
        for base in [
            json!(null),
            json!(""),
            json!("ftp://example.com"),
            json!("https://user:pass@example.com/v1"),
            json!("https://example.com/v1#fragment"),
        ] {
            assert!(resolve_embedding_url(&json!({"base_url": base})).is_err());
        }
        assert_eq!(
            resolve_embedding_timeout(&json!({})).unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            resolve_embedding_timeout(&json!({"timeout_ms": 300_000})).unwrap(),
            Duration::from_secs(300)
        );
        assert!(resolve_embedding_timeout(&json!({"timeout_ms": 300_001})).is_err());
    }

    #[test]
    fn resolve_model_default_and_override() {
        assert_eq!(resolve_model(&json!({})), DEFAULT_EMBED_MODEL);
        assert_eq!(resolve_model(&json!({ "model": "custom" })), "custom");
    }
}

#[cfg(test)]
#[path = "memory_coverage_tests.rs"]
mod memory_coverage_tests;
