//! Built-in `blob_put` / `blob_get` handlers — durable binary I/O.
//!
//! These are the kernel primitive for moving binary artifacts *between* steps:
//! a workflow generates bytes (an image, a rendered document, a fetched asset),
//! stores them with `blob_put`, passes the returned ref forward, and reads them
//! back with `blob_get`. Bytes never travel inline in the execution context —
//! only the small [`ArtifactRef`](orch8_types::artifact::ArtifactRef) does.
//!
//! Backed by the storage layer's artifact store (local FS / S3-compatible), so
//! durability, encryption, and externalization come for free.
//!
//! ## `blob_put`
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `text` | string | — | UTF-8 content to store (one of `text`/`data` required) |
//! | `data` | string | — | base64-encoded bytes to store |
//! | `content_type` | string | inferred | MIME type stored with the blob |
//! | `max_size_bytes` | u64 | 25 MiB | Reject payloads larger than this |
//!
//! Output: `{ "artifact": { id, key, content_type, size, uri, … } }`.
//!
//! ## `blob_get`
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `ref` | string \| object | **required** | Artifact key, `{key}`, or `{artifact:{key}}` |
//! | `encoding` | string | `"base64"` | `"base64"` or `"utf8"` |
//! | `max_size_bytes` | u64 | 25 MiB | Reject artifacts larger than this |
//!
//! Output (base64): `{ "data": "<b64>", "encoding": "base64", "size": N }`.
//! Output (utf8): `{ "text": "<string>", "encoding": "utf8", "size": N }`.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use serde_json::{Value, json};
use tracing::debug;

use orch8_types::error::StepError;

use super::StepContext;

/// Default ceiling on a single blob, in bytes (25 MiB). Generous enough for
/// images/documents, small enough to protect engine memory.
const DEFAULT_MAX_BYTES: u64 = 25 * 1024 * 1024;

/// Map an artifact-backend storage error to a step error. A `Unsupported`
/// backend (not configured) is a permanent misconfiguration — retrying would
/// only burn the budget. Transient backend failures stay retryable.
/// Shared with `llm_call`'s multimodal image resolution.
pub(crate) fn artifact_step_err(context: &str, e: &orch8_types::error::StorageError) -> StepError {
    match e {
        orch8_types::error::StorageError::Unsupported(_) => StepError::Permanent {
            message: format!("{context}: {e}"),
            details: None,
        },
        _ => StepError::Retryable {
            message: format!("{context}: {e}"),
            details: None,
        },
    }
}

/// Resolve an artifact key from a `ref` param. Accepts a bare key string, a
/// `{ "key": "…" }` object, or the canonical `{ "artifact": { "key": "…" } }`
/// shape that `blob_put` (and `tool_call`) emit — so callers can wire
/// `{{outputs.put.artifact}}` straight through. Shared with `llm_call`'s
/// multimodal image blocks, which accept the same `artifact` ref shapes.
pub(crate) fn extract_key(refval: &Value) -> Option<String> {
    match refval {
        Value::String(s) => Some(s.clone()),
        Value::Object(_) => {
            let inner = refval.get("artifact").unwrap_or(refval);
            inner.get("key").and_then(Value::as_str).map(str::to_string)
        }
        _ => None,
    }
}

/// Store bytes as a durable artifact, returning its ref.
pub async fn handle_blob_put(ctx: StepContext) -> Result<Value, StepError> {
    let max_size = ctx
        .params
        .get("max_size_bytes")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_MAX_BYTES);

    // Resolve bytes + a sensible default content type from whichever input
    // is present. `text` is checked first so a UTF-8 default is applied.
    let (bytes, default_ct): (Vec<u8>, &str) = if let Some(text) =
        ctx.params.get("text").and_then(Value::as_str)
    {
        (text.as_bytes().to_vec(), "text/plain; charset=utf-8")
    } else if let Some(data) = ctx.params.get("data").and_then(Value::as_str) {
        let decoded = STANDARD.decode(data).map_err(|e| StepError::Permanent {
            message: format!("blob_put: `data` is not valid base64: {e}"),
            details: None,
        })?;
        (decoded, "application/octet-stream")
    } else {
        return Err(StepError::Permanent {
            message: "blob_put: one of `text` (utf-8 string) or `data` (base64 string) is required"
                .into(),
            details: None,
        });
    };

    if bytes.len() as u64 > max_size {
        return Err(StepError::Permanent {
            message: format!(
                "blob_put: payload {} bytes exceeds max_size_bytes {}",
                bytes.len(),
                max_size
            ),
            details: None,
        });
    }

    let content_type = ctx
        .params
        .get("content_type")
        .and_then(Value::as_str)
        .unwrap_or(default_ct)
        .to_string();

    // Dry-run: report what *would* be stored without touching the backend.
    if ctx.is_dry_run() {
        return Ok(json!({
            "dry_run": true,
            "size": bytes.len(),
            "content_type": content_type,
        }));
    }

    let aref = ctx
        .storage
        .put_artifact(ctx.instance_id, &content_type, Bytes::from(bytes))
        .await
        .map_err(|e| artifact_step_err("blob_put store error", &e))?;

    debug!(
        instance_id = %ctx.instance_id,
        block_id = %ctx.block_id,
        key = %aref.key,
        size = aref.size,
        "blob_put stored artifact"
    );
    Ok(aref.into_output())
}

/// Read a stored artifact back as base64 or UTF-8 text.
pub async fn handle_blob_get(ctx: StepContext) -> Result<Value, StepError> {
    let key = ctx
        .params
        .get("ref")
        .and_then(extract_key)
        .ok_or_else(|| StepError::Permanent {
            message: "blob_get: `ref` is required (artifact ref object or key string)".into(),
            details: None,
        })?;

    // IDOR guard: artifact object keys are `<instance_id>/<artifact_id>`. A
    // workflow may only read back artifacts produced by *its own* instance,
    // never a sibling/other-tenant instance's — otherwise a crafted `ref` like
    // `<victim-instance-uuid>/<artifact-id>` would exfiltrate cross-tenant data.
    // Mirrors the same check `tool_call` applies to fetched artifacts.
    if !super::tool_call::artifact_key_owned_by(&key, ctx.instance_id) {
        return Err(StepError::Permanent {
            message: format!("blob_get: ref '{key}' does not belong to this instance"),
            details: None,
        });
    }

    let encoding = ctx
        .params
        .get("encoding")
        .and_then(Value::as_str)
        .unwrap_or("base64");

    let max_size = ctx
        .params
        .get("max_size_bytes")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_MAX_BYTES);

    let bytes = ctx
        .storage
        .get_artifact(&key)
        .await
        .map_err(|e| artifact_step_err("blob_get fetch error", &e))?
        .ok_or_else(|| StepError::Permanent {
            message: format!("blob_get: artifact not found: {key}"),
            details: None,
        })?;

    if bytes.len() as u64 > max_size {
        return Err(StepError::Permanent {
            message: format!(
                "blob_get: artifact {} bytes exceeds max_size_bytes {}",
                bytes.len(),
                max_size
            ),
            details: None,
        });
    }
    let size = bytes.len();

    match encoding {
        "base64" => Ok(json!({
            "data": STANDARD.encode(&bytes),
            "encoding": "base64",
            "size": size,
        })),
        "utf8" | "text" => {
            let text = String::from_utf8(bytes).map_err(|e| StepError::Permanent {
                message: format!(
                    "blob_get: artifact is not valid UTF-8 (use encoding=base64): {e}"
                ),
                details: None,
            })?;
            Ok(json!({ "text": text, "encoding": "utf8", "size": size }))
        }
        other => Err(StepError::Permanent {
            message: format!("blob_get: unknown encoding `{other}` (use `base64` or `utf8`)"),
            details: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::StorageBackend;
    use orch8_storage::artifacts::ObjectArtifactStore;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId, TenantId};
    use std::sync::Arc;

    async fn storage_with_artifacts() -> Arc<dyn StorageBackend> {
        Arc::new(
            SqliteStorage::in_memory()
                .await
                .unwrap()
                .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
        )
    }

    async fn storage_without_artifacts() -> Arc<dyn StorageBackend> {
        Arc::new(SqliteStorage::in_memory().await.unwrap())
    }

    fn ctx(
        storage: &Arc<dyn StorageBackend>,
        instance_id: InstanceId,
        params: Value,
    ) -> StepContext {
        StepContext {
            instance_id,
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("b"),
            params,
            context: ExecutionContext::default(),
            attempt: 0,
            storage: Arc::clone(storage),
            wait_for_input: None,
        }
    }

    fn key_of(put_output: &Value) -> String {
        put_output["artifact"]["key"]
            .as_str()
            .expect("put output has artifact.key")
            .to_string()
    }

    #[tokio::test]
    async fn put_text_then_get_utf8_roundtrip() {
        let storage = storage_with_artifacts().await;
        let id = InstanceId::new();
        let put = handle_blob_put(ctx(&storage, id, json!({ "text": "hello world" })))
            .await
            .unwrap();
        let key = key_of(&put);
        assert_eq!(put["artifact"]["size"], 11);

        let got = handle_blob_get(ctx(&storage, id, json!({ "ref": key, "encoding": "utf8" })))
            .await
            .unwrap();
        assert_eq!(got["text"], "hello world");
        assert_eq!(got["encoding"], "utf8");
        assert_eq!(got["size"], 11);
    }

    #[tokio::test]
    async fn get_rejects_cross_instance_key_idor() {
        // A victim instance stores an artifact.
        let storage = storage_with_artifacts().await;
        let victim = InstanceId::new();
        let put = handle_blob_put(ctx(&storage, victim, json!({ "text": "victim secret" })))
            .await
            .unwrap();
        let victim_key = key_of(&put);

        // An attacker instance tries to read it back by supplying the victim's
        // object key directly. This must be refused before touching storage.
        let attacker = InstanceId::new();
        let err = handle_blob_get(ctx(&storage, attacker, json!({ "ref": victim_key })))
            .await
            .expect_err("cross-instance artifact read must be rejected");
        let StepError::Permanent { message, .. } = err else {
            panic!("expected Permanent");
        };
        assert!(
            message.contains("does not belong to this instance"),
            "got: {message}"
        );
    }

    #[tokio::test]
    async fn put_base64_then_get_base64_roundtrip() {
        let storage = storage_with_artifacts().await;
        let id = InstanceId::new();
        let raw = b"\x00\x01\x02\xff binary";
        let b64 = STANDARD.encode(raw);

        let put = handle_blob_put(ctx(&storage, id, json!({ "data": b64 })))
            .await
            .unwrap();
        let key = key_of(&put);

        let got = handle_blob_get(ctx(&storage, id, json!({ "ref": key })))
            .await
            .unwrap();
        assert_eq!(got["encoding"], "base64");
        let decoded = STANDARD.decode(got["data"].as_str().unwrap()).unwrap();
        assert_eq!(decoded, raw);
    }

    #[tokio::test]
    async fn put_infers_content_type_for_text_and_data() {
        let storage = storage_with_artifacts().await;
        let id = InstanceId::new();

        let text = handle_blob_put(ctx(&storage, id, json!({ "text": "hi" })))
            .await
            .unwrap();
        assert_eq!(
            text["artifact"]["content_type"],
            "text/plain; charset=utf-8"
        );

        let data = handle_blob_put(ctx(&storage, id, json!({ "data": STANDARD.encode(b"x") })))
            .await
            .unwrap();
        assert_eq!(data["artifact"]["content_type"], "application/octet-stream");
    }

    #[tokio::test]
    async fn put_respects_explicit_content_type() {
        let storage = storage_with_artifacts().await;
        let id = InstanceId::new();
        let put = handle_blob_put(ctx(
            &storage,
            id,
            json!({ "data": STANDARD.encode(b"\x89PNG"), "content_type": "image/png" }),
        ))
        .await
        .unwrap();
        assert_eq!(put["artifact"]["content_type"], "image/png");
    }

    #[tokio::test]
    async fn put_requires_an_input() {
        let storage = storage_with_artifacts().await;
        let err = handle_blob_put(ctx(&storage, InstanceId::new(), json!({})))
            .await
            .unwrap_err();
        let StepError::Permanent { message, .. } = err else {
            panic!("expected Permanent");
        };
        assert!(message.contains("text") && message.contains("data"));
    }

    #[tokio::test]
    async fn put_rejects_invalid_base64() {
        let storage = storage_with_artifacts().await;
        let err = handle_blob_put(ctx(
            &storage,
            InstanceId::new(),
            json!({ "data": "!!!not b64" }),
        ))
        .await
        .unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[tokio::test]
    async fn put_enforces_max_size() {
        let storage = storage_with_artifacts().await;
        let err = handle_blob_put(ctx(
            &storage,
            InstanceId::new(),
            json!({ "text": "way too long", "max_size_bytes": 4 }),
        ))
        .await
        .unwrap_err();
        let StepError::Permanent { message, .. } = err else {
            panic!("expected Permanent");
        };
        assert!(message.contains("exceeds max_size_bytes"));
    }

    #[tokio::test]
    async fn put_dry_run_skips_storage() {
        let storage = storage_with_artifacts().await;
        let id = InstanceId::new();
        let mut context = ExecutionContext::default();
        context.runtime.dry_run = true;
        let c = StepContext {
            instance_id: id,
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("b"),
            params: json!({ "text": "hello" }),
            context,
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };
        let out = handle_blob_put(c).await.unwrap();
        assert_eq!(out["dry_run"], true);
        assert_eq!(out["size"], 5);
        // Nothing was actually persisted.
        assert!(storage.list_artifacts(id).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn put_without_artifact_backend_is_permanent() {
        let storage = storage_without_artifacts().await;
        let err = handle_blob_put(ctx(&storage, InstanceId::new(), json!({ "text": "x" })))
            .await
            .unwrap_err();
        // Unsupported backend → permanent (not retryable).
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[tokio::test]
    async fn get_requires_ref() {
        let storage = storage_with_artifacts().await;
        let err = handle_blob_get(ctx(&storage, InstanceId::new(), json!({})))
            .await
            .unwrap_err();
        let StepError::Permanent { message, .. } = err else {
            panic!("expected Permanent");
        };
        assert!(message.contains("ref"));
    }

    #[tokio::test]
    async fn get_missing_artifact_is_permanent() {
        let storage = storage_with_artifacts().await;
        let id = InstanceId::new();
        // Key is owned by this instance (passes the IDOR guard) but doesn't exist.
        let owned_missing = format!("{}/nonexistent", id.into_uuid());
        let err = handle_blob_get(ctx(&storage, id, json!({ "ref": owned_missing })))
            .await
            .unwrap_err();
        let StepError::Permanent { message, .. } = err else {
            panic!("expected Permanent");
        };
        assert!(message.contains("not found"));
    }

    #[tokio::test]
    async fn get_accepts_ref_as_string_object_and_artifact_wrapper() {
        let storage = storage_with_artifacts().await;
        let id = InstanceId::new();
        let put = handle_blob_put(ctx(&storage, id, json!({ "text": "shared" })))
            .await
            .unwrap();
        let key = key_of(&put);
        let artifact = put["artifact"].clone();

        // 1. bare string key
        let a = handle_blob_get(ctx(&storage, id, json!({ "ref": key, "encoding": "utf8" })))
            .await
            .unwrap();
        // 2. { key } object
        let b = handle_blob_get(ctx(
            &storage,
            id,
            json!({ "ref": { "key": key }, "encoding": "utf8" }),
        ))
        .await
        .unwrap();
        // 3. { artifact: { … } } wrapper (straight from blob_put output)
        let c = handle_blob_get(ctx(
            &storage,
            id,
            json!({ "ref": { "artifact": artifact }, "encoding": "utf8" }),
        ))
        .await
        .unwrap();

        assert_eq!(a["text"], "shared");
        assert_eq!(b["text"], "shared");
        assert_eq!(c["text"], "shared");
    }

    #[tokio::test]
    async fn get_unknown_encoding_is_permanent() {
        let storage = storage_with_artifacts().await;
        let id = InstanceId::new();
        let put = handle_blob_put(ctx(&storage, id, json!({ "text": "x" })))
            .await
            .unwrap();
        let key = key_of(&put);
        let err = handle_blob_get(ctx(&storage, id, json!({ "ref": key, "encoding": "xml" })))
            .await
            .unwrap_err();
        let StepError::Permanent { message, .. } = err else {
            panic!("expected Permanent");
        };
        assert!(message.contains("unknown encoding"));
    }

    #[tokio::test]
    async fn get_non_utf8_as_utf8_is_permanent() {
        let storage = storage_with_artifacts().await;
        let id = InstanceId::new();
        // 0xFF is not valid UTF-8.
        let put = handle_blob_put(ctx(
            &storage,
            id,
            json!({ "data": STANDARD.encode([0xFF, 0xFE]) }),
        ))
        .await
        .unwrap();
        let key = key_of(&put);
        let err = handle_blob_get(ctx(&storage, id, json!({ "ref": key, "encoding": "utf8" })))
            .await
            .unwrap_err();
        let StepError::Permanent { message, .. } = err else {
            panic!("expected Permanent");
        };
        assert!(message.contains("UTF-8"));
    }

    #[tokio::test]
    async fn get_enforces_max_size() {
        let storage = storage_with_artifacts().await;
        let id = InstanceId::new();
        let put = handle_blob_put(ctx(&storage, id, json!({ "text": "0123456789" })))
            .await
            .unwrap();
        let key = key_of(&put);
        let err = handle_blob_get(ctx(
            &storage,
            id,
            json!({ "ref": key, "max_size_bytes": 2 }),
        ))
        .await
        .unwrap_err();
        let StepError::Permanent { message, .. } = err else {
            panic!("expected Permanent");
        };
        assert!(message.contains("exceeds max_size_bytes"));
    }
}
