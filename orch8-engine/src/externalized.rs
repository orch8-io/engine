//! Hydration of persisted context references and escaped application values.

use orch8_storage::{StorageBackend, externalizing};
use orch8_types::context::ExecutionContext;
use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;
use orch8_types::{instance::TaskInstance, sequence::SequenceDefinition};
use serde_json::Value;

pub use externalizing::{EXTERNALIZED_FLAG, REF_KEY, is_marker as is_ref_marker};

/// Return the reference key for either stored envelope version.
#[must_use]
pub fn extract_ref_key(value: &Value) -> Option<&str> {
    externalizing::ref_key(value, true)
}

/// Turn an old payload into an opaque value for the second hydration pass.
/// Old payload rows contain original application data, including any shapes
/// that now have meaning in the storage format.
pub(crate) fn prepare_fetched_context_payload(mut payload: Value, versioned: bool) -> Value {
    if !versioned {
        externalizing::escape_literals(&mut payload);
    }
    payload
}

/// Resolve owner-scoped references, then decode escaped application literals.
/// Missing rows keep their markers so callers can see incomplete state.
pub async fn resolve_context_markers(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    mut ctx: ExecutionContext,
) -> Result<ExecutionContext, StorageError> {
    let mut references = std::collections::BTreeSet::new();
    externalizing::visit_context_refs(&ctx.data, |key, versioned| {
        references.insert((key.to_owned(), versioned));
    });
    let mut fetched = std::collections::HashMap::with_capacity(references.len());
    for (key, versioned) in references {
        if let Some(value) = storage.get_externalized_state(instance_id, &key).await? {
            fetched.insert(key, prepare_fetched_context_payload(value, versioned));
        }
    }
    externalizing::replace_context_refs(&mut ctx.data, |key, _| fetched.get(key).cloned());
    externalizing::unescape_literals(&mut ctx.data);
    Ok(ctx)
}

pub(crate) async fn resolved_context_for_instance<'a>(
    storage: &dyn StorageBackend,
    instance: &'a TaskInstance,
) -> Result<std::borrow::Cow<'a, ExecutionContext>, StorageError> {
    if !externalizing::has_context_envelope(&instance.context.data) {
        return Ok(std::borrow::Cow::Borrowed(&instance.context));
    }
    resolve_context_markers(storage, instance.id, instance.context.clone())
        .await
        .map(std::borrow::Cow::Owned)
}

/// Compact configured paths against the authoritative context revision.
/// The scheduler's prefetched instance can already contain hydrated values.
pub(crate) async fn compact_claimed_context(
    storage: &dyn StorageBackend,
    instance: TaskInstance,
    sequence: &SequenceDefinition,
    claim_token: chrono::DateTime<chrono::Utc>,
) -> Result<TaskInstance, crate::error::EngineError> {
    if sequence.externalized_context_paths().next().is_none() {
        return Ok(instance);
    }
    let id = instance.id;
    for _ in 0..4 {
        let Some(mut current) = storage.get_instance(id).await? else {
            break;
        };
        if current.claimed_at != Some(claim_token) {
            break;
        }
        let payloads = externalizing::externalize_paths(
            &mut current.context.data,
            &id.to_string(),
            sequence.externalized_context_paths(),
        );
        if payloads.is_empty()
            || storage
                .update_prepared_context_cas(
                    id,
                    &current.context,
                    &payloads,
                    current.updated_at,
                    Some(claim_token),
                )
                .await?
        {
            return Ok(current);
        }
    }
    Err(crate::error::EngineError::ClaimLost { instance_id: id })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn is_ref_marker_true_for_envelope() {
        let v = json!({"_externalized": true, "_ref": "k"});
        assert!(is_ref_marker(&v));
    }

    #[test]
    fn is_ref_marker_false_for_extra_keys() {
        // Payloads that happen to have _ref but also other keys
        // are NOT markers — they're real data.
        let v = json!({"_externalized": true, "_ref": "k", "other": 1});
        assert!(!is_ref_marker(&v));
    }

    #[test]
    fn is_ref_marker_false_for_missing_flag() {
        let v = json!({"_ref": "k", "other": 1});
        assert!(!is_ref_marker(&v));
    }

    #[test]
    fn is_ref_marker_false_for_flag_not_true() {
        let v = json!({"_externalized": false, "_ref": "k"});
        assert!(!is_ref_marker(&v));
    }

    #[test]
    fn is_ref_marker_false_for_non_object() {
        assert!(!is_ref_marker(&json!("string")));
        assert!(!is_ref_marker(&json!(42)));
        assert!(!is_ref_marker(&json!([1, 2])));
        assert!(!is_ref_marker(&json!(null)));
    }

    #[test]
    fn extract_ref_key_returns_key() {
        let v = json!({"_externalized": true, "_ref": "inst:block"});
        assert_eq!(extract_ref_key(&v), Some("inst:block"));
    }

    #[test]
    fn extract_ref_key_none_for_non_marker() {
        assert_eq!(extract_ref_key(&json!({"other": 1})), None);
        assert_eq!(extract_ref_key(&json!("scalar")), None);
    }

    #[test]
    fn is_ref_marker_false_for_non_string_ref() {
        // A user payload might happen to have `_externalized: true` and a
        // numeric / null / array `_ref`. That is not a marker — the engine
        // must leave it alone so the data survives the round-trip.
        assert!(!is_ref_marker(&json!({"_externalized": true, "_ref": 42})));
        assert!(!is_ref_marker(
            &json!({"_externalized": true, "_ref": null})
        ));
        assert!(!is_ref_marker(
            &json!({"_externalized": true, "_ref": ["a"]})
        ));
        assert!(!is_ref_marker(
            &json!({"_externalized": true, "_ref": {"nested": "k"}})
        ));
    }

    #[test]
    fn is_ref_marker_false_for_flag_not_bool() {
        // `_externalized: "true"` (string) must not match. The flag is a bool
        // by contract; tolerating coercion would create false positives on
        // user payloads that happen to stringify a truthy marker field.
        assert!(!is_ref_marker(
            &json!({"_externalized": "true", "_ref": "k"})
        ));
        assert!(!is_ref_marker(&json!({"_externalized": 1, "_ref": "k"})));
    }

    #[test]
    fn is_ref_marker_false_for_empty_object() {
        // Zero keys fails the length check immediately.
        assert!(!is_ref_marker(&json!({})));
    }

    #[test]
    fn extract_ref_key_returns_empty_string_when_ref_is_empty() {
        // An empty `_ref` is still a valid-shape marker — the engine
        // will just fail the batch_get lookup. This test documents that
        // extract_ref_key does not enforce non-empty refs itself; that
        // validation lives in the storage layer.
        let v = json!({"_externalized": true, "_ref": ""});
        assert_eq!(extract_ref_key(&v), Some(""));
    }

    #[test]
    fn extract_ref_key_unicode_key_preserved() {
        // Ref keys are `{instance}:ctx:data:{field}` — ULIDs are ASCII in
        // practice, but non-ASCII field names can leak through. The helper
        // must return the bytes verbatim.
        let v = json!({"_externalized": true, "_ref": "inst:ctx:data:日本語"});
        assert_eq!(extract_ref_key(&v), Some("inst:ctx:data:日本語"));
    }
}
