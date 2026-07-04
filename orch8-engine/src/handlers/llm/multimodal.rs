//! Multimodal message content for `llm_call` — images via the artifact store.
//!
//! A message's `content` may be a plain string (unchanged behavior) or an
//! array of content blocks. Supported block shapes:
//!
//! - `{"type": "text", "text": "…"}` — plain text, passed through.
//! - `{"type": "image", "artifact": "<key>", "media_type": "image/png"}` —
//!   image bytes fetched from the artifact store. The `artifact` value accepts
//!   the same shapes as `blob_get`'s `ref`: a bare key string, `{"key": "…"}`,
//!   or `{"artifact": {"key": "…"}}`.
//! - `{"type": "image", "data": "<base64>", "media_type": "image/png"}` —
//!   inline base64 image, passed through.
//!
//! [`resolve_message_images`] runs ONCE in `handle_llm_call` — after the
//! dry-run skip (no storage reads in dry-run) but before the failover loop and
//! schema paths — rewriting every image block into the normalized internal
//! form `{"type": "image", "media_type": "…", "data": "<b64>"}` so each
//! provider attempt reuses the already-fetched bytes. The provider adapters
//! then convert the normalized form to their wire shape at request-build time
//! ([`to_openai_message`] / [`to_anthropic_message`]). Plain-string `content`
//! passes through every stage byte-for-byte unchanged.

use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Value, json};

use orch8_storage::StorageBackend;
use orch8_types::error::StepError;
use orch8_types::ids::InstanceId;

use super::common::permanent;
use crate::handlers::blob::{artifact_step_err, extract_key};
use crate::handlers::tool_call::artifact_key_owned_by;

/// Default per-image size cap in bytes, pre-encoding (20 MiB). The
/// `max_image_bytes` param can lower this, never raise it.
pub(super) const DEFAULT_MAX_IMAGE_BYTES: u64 = 20 * 1024 * 1024;

/// Media type assumed when an image block doesn't specify one. The storage
/// layer's `get_artifact` returns raw bytes without a content type, so the
/// block-level `media_type` (or this default) is authoritative.
const DEFAULT_MEDIA_TYPE: &str = "image/png";

/// Resolve artifact-backed image blocks in `params.messages` into the
/// normalized internal form, in place.
///
/// - String `content` (and messages without array content) is untouched.
/// - `{"type": "image", "artifact": …}` blocks are fetched from the artifact
///   store — with the same key-shape tolerance and per-instance ownership
///   guard as `blob_get` — and rewritten to
///   `{"type": "image", "media_type": …, "data": "<b64>"}`.
/// - `{"type": "image", "data": …}` inline blocks only get a `media_type`
///   default applied.
/// - A missing artifact, a foreign-instance key, or an image exceeding the
///   size cap is a [`StepError::Permanent`].
pub(super) async fn resolve_message_images(
    storage: &Arc<dyn StorageBackend>,
    instance_id: InstanceId,
    params: &mut Value,
) -> Result<(), StepError> {
    let max_bytes = params
        .get("max_image_bytes")
        .and_then(Value::as_u64)
        .map_or(DEFAULT_MAX_IMAGE_BYTES, |v| v.min(DEFAULT_MAX_IMAGE_BYTES));

    let Some(messages) = params.get_mut("messages").and_then(Value::as_array_mut) else {
        return Ok(());
    };
    for msg in messages.iter_mut() {
        let Some(blocks) = msg.get_mut("content").and_then(Value::as_array_mut) else {
            continue; // plain-string (or absent) content: untouched
        };
        for block in blocks.iter_mut() {
            if block.get("type").and_then(Value::as_str) == Some("image") {
                normalize_image_block(storage, instance_id, max_bytes, block).await?;
            }
        }
    }
    Ok(())
}

/// Normalize one `{"type": "image", …}` block in place (see
/// [`resolve_message_images`] for the rules).
async fn normalize_image_block(
    storage: &Arc<dyn StorageBackend>,
    instance_id: InstanceId,
    max_bytes: u64,
    block: &mut Value,
) -> Result<(), StepError> {
    let Some(obj) = block.as_object_mut() else {
        return Ok(()); // malformed block: leave for the provider to reject
    };

    // Inline base64 passthrough: already in normalized form. Default the media
    // type and enforce the (approximate, pre-encoding) size cap.
    if let Some(data) = obj.get("data").and_then(Value::as_str) {
        let approx_raw = (data.len() as u64 / 4) * 3;
        if approx_raw > max_bytes {
            return Err(permanent(format!(
                "llm_call: inline image (~{approx_raw} bytes decoded) exceeds max_image_bytes {max_bytes}"
            )));
        }
        obj.remove("artifact");
        obj.entry("media_type")
            .or_insert_with(|| json!(DEFAULT_MEDIA_TYPE));
        return Ok(());
    }

    let key = obj.get("artifact").and_then(extract_key).ok_or_else(|| {
        permanent(
            "llm_call: image block requires `artifact` (key string, {key}, or \
             {artifact: {key}}) or `data` (base64)"
                .to_string(),
        )
    })?;

    // Same per-instance ownership guard as `blob_get` / `tool_call`: a crafted
    // key must not read another instance's (or tenant's) artifacts.
    if !artifact_key_owned_by(&key, instance_id) {
        return Err(permanent(format!(
            "llm_call: image artifact '{key}' does not belong to this instance"
        )));
    }

    let bytes = storage
        .get_artifact(&key)
        .await
        .map_err(|e| artifact_step_err("llm_call image artifact fetch error", &e))?
        .ok_or_else(|| permanent(format!("llm_call: image artifact not found: {key}")))?;

    if bytes.len() as u64 > max_bytes {
        return Err(permanent(format!(
            "llm_call: image artifact '{key}' is {} bytes, exceeds max_image_bytes {max_bytes}",
            bytes.len()
        )));
    }

    if !obj.contains_key("media_type") {
        obj.insert("media_type".into(), json!(DEFAULT_MEDIA_TYPE));
    }
    obj.remove("artifact");
    obj.insert("data".into(), json!(STANDARD.encode(&bytes)));
    Ok(())
}

/// Convert one chat message to the OpenAI-compatible wire shape.
///
/// Plain-string `content` (and any non-array content) is returned unchanged —
/// zero regression for existing string messages. In array content, normalized
/// image blocks become
/// `{"type": "image_url", "image_url": {"url": "data:<media_type>;base64,<data>"}}`;
/// text and unknown blocks pass through as-is.
pub(super) fn to_openai_message(msg: &Value) -> Value {
    convert_message(msg, |media_type, data| {
        json!({
            "type": "image_url",
            "image_url": { "url": format!("data:{media_type};base64,{data}") },
        })
    })
}

/// Convert one chat message to the Anthropic wire shape.
///
/// Plain-string `content` (and any non-array content) is returned unchanged.
/// In array content, normalized image blocks become
/// `{"type": "image", "source": {"type": "base64", "media_type": …, "data": …}}`;
/// text and unknown blocks pass through as-is.
pub(super) fn to_anthropic_message(msg: &Value) -> Value {
    convert_message(msg, |media_type, data| {
        json!({
            "type": "image",
            "source": { "type": "base64", "media_type": media_type, "data": data },
        })
    })
}

/// Apply `convert_image(media_type, b64_data)` to each normalized image block
/// of an array-content message; everything else (string content, text blocks,
/// unknown or unresolved blocks) is cloned unchanged.
fn convert_message(msg: &Value, convert_image: impl Fn(&str, &str) -> Value) -> Value {
    let Some(blocks) = msg.get("content").and_then(Value::as_array) else {
        return msg.clone();
    };
    let converted: Vec<Value> = blocks
        .iter()
        .map(|b| match normalized_image_parts(b) {
            Some((media_type, data)) => convert_image(media_type, data),
            None => b.clone(),
        })
        .collect();
    let mut out = msg.clone();
    if let Some(obj) = out.as_object_mut() {
        obj.insert("content".into(), Value::Array(converted));
    }
    out
}

/// `(media_type, data)` of a normalized image block; `None` for any other
/// block (including an image block that was never resolved to `data`).
fn normalized_image_parts(block: &Value) -> Option<(&str, &str)> {
    if block.get("type").and_then(Value::as_str) != Some("image") {
        return None;
    }
    let data = block.get("data").and_then(Value::as_str)?;
    let media_type = block
        .get("media_type")
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_MEDIA_TYPE);
    Some((media_type, data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use orch8_storage::artifacts::ObjectArtifactStore;
    use orch8_storage::sqlite::SqliteStorage;

    async fn storage_with_artifacts() -> Arc<dyn StorageBackend> {
        Arc::new(
            SqliteStorage::in_memory()
                .await
                .unwrap()
                .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
        )
    }

    #[tokio::test]
    async fn string_content_untouched() {
        let storage = storage_with_artifacts().await;
        let mut params = json!({
            "messages": [
                {"role": "system", "content": "be brief"},
                {"role": "user", "content": "hello"},
            ],
        });
        let before = params.clone();
        resolve_message_images(&storage, InstanceId::new(), &mut params)
            .await
            .unwrap();
        assert_eq!(params, before, "plain-string messages must not change");
    }

    #[tokio::test]
    async fn inline_base64_block_gets_media_type_default() {
        let storage = storage_with_artifacts().await;
        let b64 = STANDARD.encode(b"fake-jpeg");
        let mut params = json!({
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "what is this?"},
                {"type": "image", "data": b64},
            ]}],
        });
        resolve_message_images(&storage, InstanceId::new(), &mut params)
            .await
            .unwrap();
        let blocks = params["messages"][0]["content"].as_array().unwrap();
        assert_eq!(blocks[0], json!({"type": "text", "text": "what is this?"}));
        assert_eq!(blocks[1]["media_type"], "image/png");
        assert_eq!(blocks[1]["data"], json!(b64));
    }

    #[tokio::test]
    async fn inline_base64_explicit_media_type_preserved() {
        let storage = storage_with_artifacts().await;
        let mut params = json!({
            "messages": [{"role": "user", "content": [
                {"type": "image", "data": "QUJD", "media_type": "image/webp"},
            ]}],
        });
        resolve_message_images(&storage, InstanceId::new(), &mut params)
            .await
            .unwrap();
        assert_eq!(
            params["messages"][0]["content"][0]["media_type"],
            "image/webp"
        );
    }

    #[tokio::test]
    async fn artifact_block_resolved_for_all_ref_shapes() {
        let storage = storage_with_artifacts().await;
        let id = InstanceId::new();
        let raw = b"\x89PNG fake image bytes";
        let aref = storage
            .put_artifact(id, "image/png", Bytes::from_static(raw))
            .await
            .unwrap();
        let expected_b64 = STANDARD.encode(raw);

        // Same key-extraction tolerance as blob_get: bare string, {key},
        // {artifact: {key}}.
        for refval in [
            json!(aref.key),
            json!({"key": aref.key}),
            json!({"artifact": {"key": aref.key}}),
        ] {
            let mut params = json!({
                "messages": [{"role": "user", "content": [
                    {"type": "image", "artifact": refval, "media_type": "image/png"},
                ]}],
            });
            resolve_message_images(&storage, id, &mut params)
                .await
                .unwrap();
            let block = &params["messages"][0]["content"][0];
            assert_eq!(block["type"], "image");
            assert_eq!(block["media_type"], "image/png");
            assert_eq!(block["data"], json!(expected_b64));
            assert!(block.get("artifact").is_none(), "artifact ref replaced");
        }
    }

    #[tokio::test]
    async fn artifact_exceeding_size_cap_is_permanent() {
        let storage = storage_with_artifacts().await;
        let id = InstanceId::new();
        let aref = storage
            .put_artifact(id, "image/png", Bytes::from(vec![0u8; 64]))
            .await
            .unwrap();
        let mut params = json!({
            "max_image_bytes": 16,
            "messages": [{"role": "user", "content": [
                {"type": "image", "artifact": aref.key},
            ]}],
        });
        let err = resolve_message_images(&storage, id, &mut params)
            .await
            .expect_err("oversized image must be rejected");
        let StepError::Permanent { message, .. } = err else {
            panic!("expected Permanent");
        };
        assert!(message.contains("exceeds max_image_bytes"), "{message}");
    }

    #[tokio::test]
    async fn inline_data_exceeding_size_cap_is_permanent() {
        let storage = storage_with_artifacts().await;
        let mut params = json!({
            "max_image_bytes": 16,
            "messages": [{"role": "user", "content": [
                {"type": "image", "data": STANDARD.encode(vec![0u8; 64])},
            ]}],
        });
        let err = resolve_message_images(&storage, InstanceId::new(), &mut params)
            .await
            .expect_err("oversized inline image must be rejected");
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[tokio::test]
    async fn missing_artifact_is_permanent() {
        let storage = storage_with_artifacts().await;
        let id = InstanceId::new();
        // Owned by this instance (passes the ownership guard) but nonexistent.
        let missing = format!("{}/nonexistent", id.into_uuid());
        let mut params = json!({
            "messages": [{"role": "user", "content": [
                {"type": "image", "artifact": missing},
            ]}],
        });
        let err = resolve_message_images(&storage, id, &mut params)
            .await
            .expect_err("missing artifact must be rejected");
        let StepError::Permanent { message, .. } = err else {
            panic!("expected Permanent");
        };
        assert!(message.contains("not found"), "{message}");
    }

    #[tokio::test]
    async fn cross_instance_artifact_key_is_rejected() {
        let storage = storage_with_artifacts().await;
        let victim = InstanceId::new();
        let aref = storage
            .put_artifact(victim, "image/png", Bytes::from_static(b"secret"))
            .await
            .unwrap();
        let mut params = json!({
            "messages": [{"role": "user", "content": [
                {"type": "image", "artifact": aref.key},
            ]}],
        });
        let err = resolve_message_images(&storage, InstanceId::new(), &mut params)
            .await
            .expect_err("foreign-instance artifact must be rejected");
        let StepError::Permanent { message, .. } = err else {
            panic!("expected Permanent");
        };
        assert!(
            message.contains("does not belong to this instance"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn image_block_without_artifact_or_data_is_permanent() {
        let storage = storage_with_artifacts().await;
        let mut params = json!({
            "messages": [{"role": "user", "content": [{"type": "image"}]}],
        });
        let err = resolve_message_images(&storage, InstanceId::new(), &mut params)
            .await
            .expect_err("image block without a source must be rejected");
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[test]
    fn to_openai_message_string_content_unchanged() {
        let msg = json!({"role": "user", "content": "hello"});
        assert_eq!(to_openai_message(&msg), msg);
    }

    #[test]
    fn to_anthropic_message_string_content_unchanged() {
        let msg = json!({"role": "user", "content": "hello"});
        assert_eq!(to_anthropic_message(&msg), msg);
    }

    #[test]
    fn to_openai_message_converts_normalized_image_to_data_url() {
        let msg = json!({"role": "user", "content": [
            {"type": "text", "text": "describe"},
            {"type": "image", "media_type": "image/jpeg", "data": "QUJD"},
        ]});
        let out = to_openai_message(&msg);
        let blocks = out["content"].as_array().unwrap();
        assert_eq!(blocks[0], json!({"type": "text", "text": "describe"}));
        assert_eq!(
            blocks[1],
            json!({
                "type": "image_url",
                "image_url": {"url": "data:image/jpeg;base64,QUJD"},
            })
        );
    }

    #[test]
    fn to_anthropic_message_converts_normalized_image_to_source_block() {
        let msg = json!({"role": "user", "content": [
            {"type": "text", "text": "describe"},
            {"type": "image", "media_type": "image/jpeg", "data": "QUJD"},
        ]});
        let out = to_anthropic_message(&msg);
        let blocks = out["content"].as_array().unwrap();
        assert_eq!(blocks[0], json!({"type": "text", "text": "describe"}));
        assert_eq!(
            blocks[1],
            json!({
                "type": "image",
                "source": {"type": "base64", "media_type": "image/jpeg", "data": "QUJD"},
            })
        );
    }

    #[test]
    fn unknown_blocks_pass_through_both_conversions() {
        let msg = json!({"role": "user", "content": [
            {"type": "tool_result", "id": "x"},
        ]});
        assert_eq!(to_openai_message(&msg), msg);
        assert_eq!(to_anthropic_message(&msg), msg);
    }
}
