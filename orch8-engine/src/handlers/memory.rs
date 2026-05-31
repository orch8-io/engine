//! Built-in agent-memory handlers — `embed`, `memory_store`, `memory_search`.
//!
//! Durable, semantically-searchable memory for agents, built on the engine's
//! existing instance key-value store (the same persistence as `set_state`).
//! Memories survive crashes and replay like any other instance state.
//!
//! Ranking is a cosine-similarity scan in plain Rust — **no pgvector,
//! sqlite-vec, or any database extension**. That keeps the engine a single
//! binary and, critically, lets agent memory work **offline on a phone**
//! (`SQLite` backend), where a vector-DB extension is not an option. Brute-force
//! scan is appropriate for the hundreds-to-thousands of memories an agent
//! accumulates within an instance/session.
//!
//! Scope: memories are instance-scoped (an agent's working memory across its
//! own steps). Tenant-scoped long-term recall across instances is a
//! storage-backed follow-on.
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

use std::time::Duration;

use serde_json::{json, Value};
use tracing::debug;

use orch8_types::error::StepError;

use super::StepContext;

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

// ===========================================================================
// embed
// ===========================================================================

pub async fn handle_embed(ctx: StepContext) -> Result<Value, StepError> {
    let input = ctx
        .params
        .get("input")
        .ok_or_else(|| permanent("embed: `input` (string or array of strings) is required"))?;

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

    let is_batch = input.is_array();
    let inputs = to_input_list(input)?;
    let embeddings = embed_inputs(&ctx.params, &inputs).await?;
    let dimensions = embeddings.first().map_or(0, Vec::len);

    if is_batch {
        Ok(json!({
            "embeddings": embeddings,
            "model": resolve_model(&ctx.params),
            "dimensions": dimensions,
        }))
    } else {
        Ok(json!({
            "embedding": embeddings.into_iter().next().unwrap_or_default(),
            "model": resolve_model(&ctx.params),
            "dimensions": dimensions,
        }))
    }
}

// ===========================================================================
// memory_store
// ===========================================================================

pub async fn handle_memory_store(ctx: StepContext) -> Result<Value, StepError> {
    let text = ctx
        .params
        .get("text")
        .and_then(Value::as_str)
        .map(str::to_string);

    // Dry-run: do not embed or persist anything.
    if ctx.is_dry_run() {
        let key = ctx.params.get("key").and_then(Value::as_str).map_or_else(
            || content_key(text.as_deref().unwrap_or("")),
            str::to_string,
        );
        return Ok(super::util::dry_run_stub(
            "memory_store",
            Value::Null,
            json!({ "key": key, "stored": false, "dimensions": 0 }),
        ));
    }

    // Use a supplied embedding if present; otherwise embed `text`.
    let embedding: Vec<f64> =
        if let Some(arr) = ctx.params.get("embedding").and_then(Value::as_array) {
            parse_vector(arr)
        } else {
            let text = text
                .as_deref()
                .ok_or_else(|| permanent("memory_store: `text` or `embedding` is required"))?;
            embed_inputs(&ctx.params, std::slice::from_ref(&text.to_string()))
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| retryable("memory_store: embedding provider returned no vector"))?
        };

    let metadata = ctx.params.get("metadata").cloned().unwrap_or(json!({}));
    let key = ctx.params.get("key").and_then(Value::as_str).map_or_else(
        || content_key(text.as_deref().unwrap_or("")),
        str::to_string,
    );

    let record = memory_record(text.as_deref(), &embedding, &metadata);
    let storage_key = format!("{MEMORY_KEY_PREFIX}{key}");

    ctx.storage
        .set_instance_kv(ctx.instance_id, &storage_key, &record)
        .await
        .map_err(|e| retryable(format!("memory_store storage error: {e}")))?;

    debug!(key = %key, dimensions = embedding.len(), "memory_store: persisted");
    Ok(json!({ "key": key, "stored": true, "dimensions": embedding.len() }))
}

// ===========================================================================
// memory_search
// ===========================================================================

pub async fn handle_memory_search(ctx: StepContext) -> Result<Value, StepError> {
    // Dry-run: skip the embedding-provider call (the query vector would
    // otherwise be embedded via an external API). Return an empty result set.
    if ctx.is_dry_run() {
        return Ok(super::util::dry_run_stub(
            "memory_search",
            Value::Null,
            json!({ "results": [], "count": 0 }),
        ));
    }

    let top_k_u64 = ctx
        .params
        .get("top_k")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_TOP_K)
        .max(1);
    let top_k = usize::try_from(top_k_u64).unwrap_or(usize::MAX);

    // The query vector: precomputed `query_embedding`, or embed `query`.
    let query_embedding: Vec<f64> = if let Some(arr) =
        ctx.params.get("query_embedding").and_then(Value::as_array)
    {
        parse_vector(arr)
    } else {
        let query = ctx
            .params
            .get("query")
            .and_then(Value::as_str)
            .ok_or_else(|| permanent("memory_search: `query` or `query_embedding` is required"))?;
        embed_inputs(&ctx.params, std::slice::from_ref(&query.to_string()))
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| retryable("memory_search: embedding provider returned no vector"))?
    };

    let kv = ctx
        .storage
        .get_all_instance_kv(ctx.instance_id)
        .await
        .map_err(|e| retryable(format!("memory_search storage error: {e}")))?;

    let records = extract_memory_records(&kv);
    let results = rank_memories(&query_embedding, records, top_k);

    Ok(json!({ "results": results, "count": results_len(&results) }))
}

// ===========================================================================
// Embedding provider call
// ===========================================================================

/// Resolve config and POST to the `/embeddings` endpoint, returning one vector
/// per input string (in order).
async fn embed_inputs(params: &Value, inputs: &[String]) -> Result<Vec<Vec<f64>>, StepError> {
    let base = params
        .get("base_url")
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_EMBED_BASE)
        .trim_end_matches('/')
        .to_string();
    let url = format!("{base}/embeddings");

    if !super::builtin::is_url_safe(&url).await {
        return Err(permanent(format!("embed: base_url is not allowed: {base}")));
    }

    let api_key = resolve_api_key(params)?;
    let model = resolve_model(params);
    let timeout = Duration::from_millis(
        params
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(30_000),
    );

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
                retryable(format!("embed network error: {e}"))
            } else {
                permanent(format!("embed request error: {e}"))
            }
        })?;

    let status = resp.status().as_u16();
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| retryable(format!("embed body read error: {e}")))?;
    parse_embedding_response(status, &bytes)
}

fn resolve_model(params: &Value) -> String {
    params
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_EMBED_MODEL)
        .to_string()
}

/// Resolve the API key from `api_key` or the env var named by `api_key_env`.
fn resolve_api_key(params: &Value) -> Result<String, StepError> {
    if let Some(key) = params.get("api_key").and_then(Value::as_str) {
        return Ok(key.to_string());
    }
    if let Some(env_name) = params.get("api_key_env").and_then(Value::as_str) {
        return std::env::var(env_name)
            .map_err(|_| permanent(format!("embed: env var {env_name} not set")));
    }
    Err(permanent("embed: `api_key` or `api_key_env` is required"))
}

// ===========================================================================
// Pure helpers (unit-tested without network or storage)
// ===========================================================================

/// Normalize `input` (a string or array of strings) into a list of strings.
fn to_input_list(input: &Value) -> Result<Vec<String>, StepError> {
    match input {
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                let s = v
                    .as_str()
                    .ok_or_else(|| permanent("embed: `input` array must contain only strings"))?;
                out.push(s.to_string());
            }
            if out.is_empty() {
                return Err(permanent("embed: `input` array is empty"));
            }
            Ok(out)
        }
        _ => Err(permanent(
            "embed: `input` must be a string or array of strings",
        )),
    }
}

/// Build an `OpenAI`-compatible `/embeddings` request body.
fn build_embeddings_request(model: &str, inputs: &[String]) -> Value {
    json!({ "model": model, "input": inputs })
}

/// Parse an `OpenAI`-compatible embeddings response into vectors (ordered by the
/// `index` field when present, else by position).
fn parse_embedding_response(status: u16, body: &[u8]) -> Result<Vec<Vec<f64>>, StepError> {
    if status >= 500 {
        return Err(retryable(format!("embed provider returned HTTP {status}")));
    }
    if status >= 400 {
        return Err(permanent(format!("embed provider returned HTTP {status}")));
    }
    let parsed: Value = serde_json::from_slice(body)
        .map_err(|e| permanent(format!("embed response was not valid JSON: {e}")))?;
    let data = parsed
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| permanent("embed response missing `data` array"))?;

    let mut indexed: Vec<(usize, Vec<f64>)> = Vec::with_capacity(data.len());
    for (pos, item) in data.iter().enumerate() {
        let arr = item
            .get("embedding")
            .and_then(Value::as_array)
            .ok_or_else(|| permanent("embed response item missing `embedding`"))?;
        let idx = item
            .get("index")
            .and_then(Value::as_u64)
            .map_or(pos, |i| usize::try_from(i).unwrap_or(pos));
        indexed.push((idx, parse_vector(arr)));
    }
    indexed.sort_by_key(|(i, _)| *i);
    Ok(indexed.into_iter().map(|(_, v)| v).collect())
}

/// Parse a JSON array of numbers into `Vec<f64>` (non-numbers become 0.0).
fn parse_vector(arr: &[Value]) -> Vec<f64> {
    arr.iter().map(|v| v.as_f64().unwrap_or(0.0)).collect()
}

/// Build a persisted memory record.
fn memory_record(text: Option<&str>, embedding: &[f64], metadata: &Value) -> Value {
    json!({
        "text": text,
        "embedding": embedding,
        "metadata": metadata,
    })
}

/// Content-addressed key for a memory (stable hash of the text), used when the
/// caller does not supply an explicit `key`. Avoids needing a clock/RNG and
/// makes re-storing identical text idempotent.
fn content_key(text: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    text.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Cosine similarity in `[-1, 1]`. Returns 0.0 for zero-norm or
/// mismatched-length vectors (a non-comparable pair scores neutral).
fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0;
    let mut na = 0.0;
    let mut nb = 0.0;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
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

/// Rank memories against a query embedding, returning the top `k` as result
/// objects sorted by descending score.
fn rank_memories(query: &[f64], records: Vec<(String, Value)>, top_k: usize) -> Vec<Value> {
    let mut scored: Vec<(f64, String, Value)> = records
        .into_iter()
        .map(|(key, record)| {
            let emb = record
                .get("embedding")
                .and_then(Value::as_array)
                .map(|a| parse_vector(a))
                .unwrap_or_default();
            let score = cosine_similarity(query, &emb);
            (score, key, record)
        })
        .collect();

    // Sort by score descending. Partial ordering: NaN scores (shouldn't occur)
    // sink to the bottom.
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(top_k);

    scored
        .into_iter()
        .map(|(score, key, record)| {
            json!({
                "key": key,
                "text": record.get("text").cloned().unwrap_or(Value::Null),
                "score": score,
                "metadata": record.get("metadata").cloned().unwrap_or(json!({})),
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
        let vecs = parse_embedding_response(200, body).unwrap();
        assert_eq!(vecs.len(), 2);
        assert_eq!(vecs[0], vec![1.0, 0.0]); // index 0 first
        assert_eq!(vecs[1], vec![0.0, 1.0]);
    }

    #[test]
    fn parse_embedding_response_errors() {
        assert!(matches!(
            parse_embedding_response(500, b"x").unwrap_err(),
            StepError::Retryable { .. }
        ));
        assert!(matches!(
            parse_embedding_response(401, b"x").unwrap_err(),
            StepError::Permanent { .. }
        ));
        assert!(matches!(
            parse_embedding_response(200, b"not json").unwrap_err(),
            StepError::Permanent { .. }
        ));
        assert!(matches!(
            parse_embedding_response(200, br#"{"no":"data"}"#).unwrap_err(),
            StepError::Permanent { .. }
        ));
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
    fn content_key_is_stable_and_hex() {
        let k1 = content_key("hello world");
        let k2 = content_key("hello world");
        assert_eq!(k1, k2);
        assert_ne!(k1, content_key("different"));
        assert_eq!(k1.len(), 16);
        assert!(k1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn memory_record_shape() {
        let r = memory_record(Some("the sky"), &[0.1, 0.2], &json!({"src": "test"}));
        assert_eq!(r["text"], "the sky");
        assert_eq!(r["embedding"][1], 0.2);
        assert_eq!(r["metadata"]["src"], "test");
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
        let results = rank_memories(&[1.0, 0.0], records, 2);
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
    fn rank_memories_handles_missing_embedding() {
        let records = vec![("bad".to_string(), json!({"text": "no embedding"}))];
        let results = rank_memories(&[1.0, 0.0], records, 5);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["score"], 0.0);
    }

    #[test]
    fn rank_memories_empty_corpus() {
        assert!(rank_memories(&[1.0], vec![], 5).is_empty());
    }

    #[test]
    fn parse_vector_coerces_non_numbers_to_zero() {
        assert_eq!(
            parse_vector(&[json!(1.5), json!("x"), json!(2.0)]),
            vec![1.5, 0.0, 2.0]
        );
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

    use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId, TenantId};

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
        StepContext {
            instance_id: InstanceId::new(),
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("b"),
            params,
            context: ExecutionContext::default(),
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
        ctx.context.runtime.dry_run = true;
        let out = handle_embed(ctx).await.unwrap();
        assert_eq!(out["dry_run"], true);
        assert_eq!(out["embedding"], json!([]));
        assert_eq!(out["dimensions"], 0);
    }

    #[tokio::test]
    async fn dry_run_memory_store_does_not_persist() {
        let mut ctx = mk_ctx(json!({ "text": "remember me", "key": "k1" })).await;
        ctx.context.runtime.dry_run = true;
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
        ctx.context.runtime.dry_run = true;
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
        let base = StepContext {
            instance_id,
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("b"),
            params: Value::Null,
            context: ExecutionContext::default(),
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
    async fn store_computes_embedding_from_text() {
        let url = spawn_embed_mock(1).await;
        let ctx =
            mk_ctx(json!({ "key": "d1", "text": "doc", "base_url": url, "api_key": "k" })).await;
        let out = handle_memory_store(ctx).await.unwrap();
        assert_eq!(out["key"], "d1");
        assert_eq!(out["dimensions"], 3);
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
    }

    #[test]
    fn resolve_model_default_and_override() {
        assert_eq!(resolve_model(&json!({})), DEFAULT_EMBED_MODEL);
        assert_eq!(resolve_model(&json!({ "model": "custom" })), "custom");
    }
}
