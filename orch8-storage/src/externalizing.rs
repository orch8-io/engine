//! Externalization helpers for the storage layer.
//!
//! The scheduler fast path keeps contexts inlined in `task_instances.context`.
//! When a context field (or a block output) exceeds the configured threshold
//! we replace the inline value with an externalization marker
//! (`{"_externalized": true, "_ref": "<key>"}`) and stash the original payload
//! in `externalized_state`. Readers recognize the marker and re-hydrate.
//!
//! These constants MUST match `orch8_engine::externalized::*`. A mismatch
//! would silently break marker recognition; the `marker_shape_matches_engine`
//! test in the storage integration suite guards against drift.
//!
//! See `docs/CONTEXT_MANAGEMENT.md` §8.5 for the envelope contract.
use serde_json::{Map, Value};

pub const EXTERNALIZED_FLAG: &str = "_externalized";
pub const REF_KEY: &str = "_ref";

/// Shape of a (`ref_key`, payload) pair produced by [`externalize_fields`].
/// The caller persists each payload at `ref_key` in `externalized_state`
/// before committing the caller's transaction.
pub type ExternalizedRef = (String, Value);

/// Build the two-key externalization envelope for `ref_key`.
#[must_use]
pub fn marker(ref_key: &str) -> Value {
    let mut obj = Map::with_capacity(2);
    obj.insert(EXTERNALIZED_FLAG.to_string(), Value::Bool(true));
    obj.insert(REF_KEY.to_string(), Value::String(ref_key.to_string()));
    Value::Object(obj)
}

/// Return `true` iff `v` is already a marker envelope (exactly two keys,
/// `_externalized: true`, `_ref: <string>`).
#[must_use]
pub fn is_marker(v: &Value) -> bool {
    let Some(obj) = v.as_object() else {
        return false;
    };
    if obj.len() != 2 {
        return false;
    }
    matches!(obj.get(EXTERNALIZED_FLAG), Some(Value::Bool(true)))
        && matches!(obj.get(REF_KEY), Some(Value::String(_)))
}

/// Build the ref-key for a context.data top-level field.
#[must_use]
pub fn context_data_ref_key(instance_id_str: &str, field: &str) -> String {
    format!("{instance_id_str}:ctx:data:{field}")
}

/// Walk top-level keys of a context `data` object and externalize any value
/// whose serialized size meets or exceeds `threshold_bytes`.
///
/// For each externalized key:
/// - The value is replaced in-place by a marker referencing
///   `{instance_id}:ctx:data:{field}`.
/// - The original value is returned in the result vector so the caller can
///   persist it to `externalized_state` inside the same transaction.
///
/// No-ops in every other case: `data` is not an object, `threshold_bytes == 0`,
/// the value is already a marker, or the value is below the threshold.
///
/// The in-place mutation keeps allocation minimal on the hot write path;
/// callers that need an immutable input should clone before calling.
pub fn externalize_fields(
    data: &mut Value,
    instance_id_str: &str,
    threshold_bytes: u32,
) -> Vec<ExternalizedRef> {
    // Threshold of 0 disables externalization (treated as "never" regardless
    // of `ExternalizationMode`). The scheduler never calls with 0 — guard is
    // here so fuzz/property tests can't trip it.
    if threshold_bytes == 0 {
        return Vec::new();
    }
    let Some(obj) = data.as_object_mut() else {
        return Vec::new();
    };

    let mut refs = Vec::new();
    let threshold = threshold_bytes as usize;

    // Collect field names first so we can mutate without borrow conflicts.
    // Cloning the names is cheap versus cloning the payloads.
    let field_names: Vec<String> = obj.keys().cloned().collect();

    for field in field_names {
        // Safe: we just iterated obj.keys().
        let Some(value) = obj.get(&field) else {
            continue;
        };
        if is_marker(value) {
            // Already externalized — don't double-wrap.
            continue;
        }
        // Size check uses the serialized length. serde_json::to_vec allocates
        // but so would any other measurement (byte count on a raw &str is
        // only accurate for already-stringified JSON).
        let Ok(encoded) = serde_json::to_vec(value) else {
            // If a value can't be serialized it definitely can't be stored;
            // leave it in place and let the caller's persist step surface the
            // error with full context.
            continue;
        };
        if encoded.len() < threshold {
            continue;
        }
        let ref_key = context_data_ref_key(instance_id_str, &field);
        // Swap the payload out so we can return ownership without cloning.
        if let Some(original) = obj.insert(field.clone(), marker(&ref_key)) {
            refs.push((ref_key, original));
        }
    }

    refs
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn marker_shape_is_exactly_two_keys() {
        let m = marker("inst1:ctx:data:blob");
        assert!(is_marker(&m));
        assert_eq!(m["_externalized"], json!(true));
        assert_eq!(m["_ref"], json!("inst1:ctx:data:blob"));
    }

    #[test]
    fn externalize_fields_no_op_for_non_object() {
        let mut v = json!("scalar");
        let refs = externalize_fields(&mut v, "inst1", 16);
        assert!(refs.is_empty());
        assert_eq!(v, json!("scalar"));

        let mut v = json!([1, 2, 3]);
        let refs = externalize_fields(&mut v, "inst1", 16);
        assert!(refs.is_empty());
    }

    #[test]
    fn externalize_fields_no_op_for_zero_threshold() {
        let mut v = json!({ "k": "x".repeat(100) });
        let refs = externalize_fields(&mut v, "inst1", 0);
        assert!(refs.is_empty());
        assert_eq!(v["k"], json!("x".repeat(100)));
    }

    #[test]
    fn externalize_fields_replaces_large_values() {
        let big = "x".repeat(2048);
        let mut v = json!({
            "small": "tiny",
            "big": big.clone(),
        });
        let refs = externalize_fields(&mut v, "inst1", 1024);

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].0, "inst1:ctx:data:big");
        assert_eq!(refs[0].1, json!(big));

        // Small value stayed inline.
        assert_eq!(v["small"], json!("tiny"));
        // Big value replaced with marker.
        assert!(is_marker(&v["big"]));
        assert_eq!(v["big"]["_ref"], json!("inst1:ctx:data:big"));
    }

    #[test]
    fn externalize_fields_skips_existing_marker() {
        // Already-externalized values should not be double-wrapped. The marker
        // itself is only ~60 bytes so we need an artificially low threshold
        // to prove we're gating on shape, not size.
        let existing = marker("inst0:ctx:data:earlier");
        let mut v = json!({ "already": existing.clone() });
        let refs = externalize_fields(&mut v, "inst1", 1);
        assert!(refs.is_empty(), "markers must not be re-externalized");
        assert_eq!(v["already"], existing);
    }

    #[test]
    fn externalize_fields_below_threshold_left_inline() {
        let mut v = json!({ "k": "short" });
        let refs = externalize_fields(&mut v, "inst1", 1024);
        assert!(refs.is_empty());
        assert_eq!(v["k"], json!("short"));
    }

    #[test]
    fn externalize_fields_multiple_over_threshold() {
        let big1 = "a".repeat(2048);
        let big2 = "b".repeat(4096);
        let mut v = json!({
            "a": big1.clone(),
            "b": big2.clone(),
            "c": "tiny",
        });
        let mut refs = externalize_fields(&mut v, "inst42", 1024);
        refs.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].0, "inst42:ctx:data:a");
        assert_eq!(refs[0].1, json!(big1));
        assert_eq!(refs[1].0, "inst42:ctx:data:b");
        assert_eq!(refs[1].1, json!(big2));

        assert!(is_marker(&v["a"]));
        assert!(is_marker(&v["b"]));
        assert_eq!(v["c"], json!("tiny"));
    }

    #[test]
    fn marker_recognized_by_is_marker() {
        // Drift guard: if the canonical shape changes here it MUST also change
        // in orch8-engine::externalized and in docs/CONTEXT_MANAGEMENT.md §8.5.
        assert!(is_marker(&json!({"_externalized": true, "_ref": "k"})));
        assert!(!is_marker(&json!({"_externalized": true, "_ref": "k", "x": 1})));
        assert!(!is_marker(&json!({"_externalized": false, "_ref": "k"})));
        assert!(!is_marker(&json!({"_ref": "k"})));
        assert!(!is_marker(&json!("scalar")));
    }
}
