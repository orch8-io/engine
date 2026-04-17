//! Externalization marker helpers.
//!
//! A "marker" is a JSON object of exactly the shape
//! `{"_externalized": true, "_ref": "<key>"}`. Producers write these;
//! the engine inflates them before handlers see the context.
//!
//! The "exactly two keys" rule prevents false positives on user payloads
//! that happen to include a `_ref` field alongside other data.

use serde_json::Value;

pub const EXTERNALIZED_FLAG: &str = "_externalized";
pub const REF_KEY: &str = "_ref";

/// Return `true` iff `v` is exactly the externalization envelope — an object
/// with precisely two keys (`_externalized: true`, `_ref: "<string>"`).
#[must_use]
pub fn is_ref_marker(v: &Value) -> bool {
    let Some(obj) = v.as_object() else {
        return false;
    };
    if obj.len() != 2 {
        return false;
    }
    matches!(obj.get(EXTERNALIZED_FLAG), Some(Value::Bool(true)))
        && matches!(obj.get(REF_KEY), Some(Value::String(_)))
}

/// Return the `_ref` string if `v` is a marker, else `None`.
#[must_use]
pub fn extract_ref_key(v: &Value) -> Option<&str> {
    if !is_ref_marker(v) {
        return None;
    }
    v.as_object()?.get(REF_KEY)?.as_str()
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
}
