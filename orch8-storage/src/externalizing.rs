//! Externalization helpers for the storage layer.
//!
//! The scheduler fast path keeps contexts inlined in `task_instances.context`.
//! When a context field (or a block output) exceeds the configured threshold
//! we replace the inline value with a versioned marker and write the payload
//! to `externalized_state`. Historical markers remain readable.
use serde_json::{Map, Value};
use std::io;

/// `io::Write` sink that just tallies bytes written and discards them —
/// used by `externalize_fields` to compute JSON-serialized length without
/// allocating a `Vec<u8>` for the full encoded payload.
#[derive(Default)]
struct CountingWriter {
    count: usize,
}

impl io::Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.count += buf.len();
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub const EXTERNALIZED_FLAG: &str = "_externalized";
pub const REF_KEY: &str = "_ref";
const REF_V1: &str = "ref:v1";
const LITERAL_V1: &str = "literal:v1";
const LITERAL_VALUE: &str = "_value";

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

/// Return whether the value is a legacy or versioned reference envelope.
#[must_use]
pub fn is_marker(v: &Value) -> bool {
    ref_key(v, true).is_some()
}

/// Legacy references are recognized only where earlier writers could put them.
pub fn ref_key(value: &Value, allow_legacy: bool) -> Option<&str> {
    let object = value.as_object()?;
    if object.len() != 2 {
        return None;
    }
    let tag = object.get(EXTERNALIZED_FLAG)?;
    if tag.as_str() != Some(REF_V1) && !(allow_legacy && tag.as_bool() == Some(true)) {
        return None;
    }
    object.get(REF_KEY)?.as_str()
}

pub fn literal_payload(value: &Value) -> Option<&Value> {
    let object = value.as_object()?;
    if object.len() != 2 || object.get(EXTERNALIZED_FLAG)?.as_str()? != LITERAL_V1 {
        return None;
    }
    object.get(LITERAL_VALUE)
}

/// Whether a context needs reference hydration or literal decoding.
pub fn has_context_envelope(data: &Value) -> bool {
    fn walk(value: &Value, allow_legacy: bool, children_allow_legacy: bool) -> bool {
        if literal_payload(value).is_some() || ref_key(value, allow_legacy).is_some() {
            return true;
        }
        match value {
            Value::Object(object) => object
                .values()
                .any(|value| walk(value, children_allow_legacy, false)),
            Value::Array(array) => array.iter().any(|value| walk(value, false, false)),
            _ => false,
        }
    }
    walk(data, false, true)
}

/// Encode newly supplied application JSON before combining it with persisted
/// context. Preserve reserved-looking user objects verbatim inside the escape.
pub fn escape_literals(value: &mut Value) {
    if is_marker(value) || literal_payload(value).is_some() {
        *value = serde_json::json!({EXTERNALIZED_FLAG: LITERAL_V1, LITERAL_VALUE: value.take()});
        return;
    }
    for_each_child_mut(value, escape_literals);
}

/// Decode a context payload that has already been fetched. Do not interpret
/// marker-shaped objects inside it as references.
pub fn unescape_literals(value: &mut Value) {
    if let Some(literal) = literal_payload(value) {
        *value = literal.clone();
        return;
    }
    for_each_child_mut(value, unescape_literals);
}

fn for_each_child_mut(value: &mut Value, mut visit: impl FnMut(&mut Value)) {
    match value {
        Value::Object(object) => object.values_mut().for_each(&mut visit),
        Value::Array(array) => array.iter_mut().for_each(&mut visit),
        _ => {}
    }
}

/// Visit references in a persisted context. Older references were written
/// only to top-level fields of the data object.
pub fn visit_context_refs(data: &Value, mut visit: impl FnMut(&str, bool)) {
    fn walk(value: &Value, depth: usize, visit: &mut impl FnMut(&str, bool)) {
        if literal_payload(value).is_some() {
            return;
        }
        if let Some(reference) = ref_key(value, depth == 1) {
            visit(reference, ref_key(value, false).is_some());
            return;
        }
        match value {
            Value::Object(object) => {
                for value in object.values() {
                    walk(value, depth + 1, visit);
                }
            }
            Value::Array(array) => {
                for value in array {
                    walk(value, depth + 2, visit);
                }
            }
            _ => {}
        }
    }
    walk(data, 0, &mut visit);
}

/// Replace fetched context references while preserving encoded literals.
pub fn replace_context_refs(
    data: &mut Value,
    mut resolve: impl FnMut(&str, bool) -> Option<Value>,
) {
    fn walk(
        value: &mut Value,
        depth: usize,
        resolve: &mut impl FnMut(&str, bool) -> Option<Value>,
    ) {
        if literal_payload(value).is_some() {
            return;
        }
        if let Some(reference) = ref_key(value, depth == 1) {
            if let Some(payload) = resolve(reference, ref_key(value, false).is_some()) {
                *value = payload;
            }
            return;
        }
        let child_depth = depth + if value.is_array() { 2 } else { 1 };
        for_each_child_mut(value, |child| walk(child, child_depth, resolve));
    }
    walk(data, 0, &mut resolve);
}

fn versioned_marker(ref_key: &str) -> Value {
    serde_json::json!({EXTERNALIZED_FLAG: REF_V1, REF_KEY: ref_key})
}

/// Promote the values selected by validated RFC 6901 pointers relative to
/// context.data. Missing paths and existing references require no payload write.
pub fn externalize_paths<'a>(
    data: &mut Value,
    instance_id: &str,
    paths: impl IntoIterator<Item = &'a str>,
) -> Vec<ExternalizedRef> {
    let mut payloads = Vec::new();
    let mut revision = None;
    for path in paths {
        let Some(value) = context_pointer_mut(data, path) else {
            continue;
        };
        let top_level = !path.strip_prefix('/').unwrap_or(path).contains('/');
        if ref_key(value, top_level).is_some() {
            continue;
        }
        let revision = revision.get_or_insert_with(uuid::Uuid::now_v7);
        let key = format!("{instance_id}:ctx:pointer:v1:{revision}:{path}");
        let payload = std::mem::replace(value, versioned_marker(&key));
        payloads.push((key, payload));
    }
    payloads
}

/// Navigate inline context while treating both envelope types as opaque.
pub fn context_pointer_mut<'a>(mut data: &'a mut Value, path: &str) -> Option<&'a mut Value> {
    for (depth, segment) in path.strip_prefix('/')?.split('/').enumerate() {
        if literal_payload(data).is_some() || ref_key(data, depth == 1).is_some() {
            return None;
        }
        data = data.pointer_mut(&format!("/{segment}"))?;
    }
    Some(data)
}

/// Build the legacy ref-key for a context.data top-level field.
/// New writes use immutable revision keys.
#[must_use]
pub fn context_data_ref_key(instance_id_str: &str, field: &str) -> String {
    format!("{instance_id_str}:ctx:data:{field}")
}

/// Walk top-level keys of a context `data` object and externalize any value
/// whose serialized size meets or exceeds `threshold_bytes`.
///
/// For each externalized key:
/// - The value is replaced in-place by a marker referencing
///   `{instance_id}:ctx:v1:{revision}:{field}`.
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
    if threshold_bytes == 0 || literal_payload(data).is_some() {
        return Vec::new();
    }
    let Some(obj) = data.as_object_mut() else {
        return Vec::new();
    };

    let mut refs = Vec::new();
    let mut revision = None;
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
        // Size check streams the serialized bytes into a CountingWriter and
        // discards them — we only need the length, not the payload. Avoids
        // the `serde_json::to_vec` allocation on the hot write path. If
        // serialization fails the value definitely can't be stored; leave
        // it in place and let the caller's persist step surface the error
        // with full context.
        let mut counter = CountingWriter::default();
        if serde_json::to_writer(&mut counter, value).is_err() {
            continue;
        }
        if counter.count < threshold {
            continue;
        }
        let revision = revision.get_or_insert_with(uuid::Uuid::now_v7);
        let ref_key = format!("{instance_id_str}:ctx:v1:{revision}:{field}");
        // Swap the payload out so we can return ownership without cloning.
        if let Some(original) = obj.insert(field, versioned_marker(&ref_key)) {
            refs.push((ref_key, original));
        }
    }

    refs
}

// Keep the existing storage API path while sharing the logical wire type.
pub use orch8_types::output::ExternalizedEntry;

pub(crate) fn decode_payload(
    payload: Option<Value>,
    bytes: Option<Vec<u8>>,
    codec: Option<&str>,
) -> Result<Value, orch8_types::error::StorageError> {
    use orch8_types::error::StorageError;
    match codec {
        Some("zstd") => {
            crate::compression::decompress(&bytes.ok_or_else(|| {
                StorageError::Query("externalized payload bytes are missing".into())
            })?)
        }
        None => {
            payload.ok_or_else(|| StorageError::Query("externalized payload is missing".into()))
        }
        Some(other) => Err(StorageError::Query(format!(
            "unknown externalized payload codec: {other}"
        ))),
    }
}

/// An instance and the pre-encoded externalized payloads it references.
/// Storage backends must publish them in the same transaction.
pub struct PreparedInstance {
    pub instance: orch8_types::instance::TaskInstance,
    pub payloads: Vec<(String, serde_json::Value)>,
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
            "big": big,
        });
        let refs = externalize_fields(&mut v, "inst1", 1024);

        assert_eq!(refs.len(), 1);
        assert!(refs[0].0.starts_with("inst1:ctx:v1:"));
        assert!(refs[0].0.ends_with(":big"));
        assert_eq!(refs[0].1, json!(big));

        // Small value stayed inline.
        assert_eq!(v["small"], json!("tiny"));
        // Big value replaced with marker.
        assert!(is_marker(&v["big"]));
        assert_eq!(v["big"]["_ref"], json!(refs[0].0));
    }

    #[test]
    fn externalize_fields_skips_existing_marker() {
        // Already-externalized values should not be double-wrapped. The marker
        // itself is only ~60 bytes so we need an artificially low threshold
        // to prove we're gating on shape, not size.
        let existing = marker("inst0:ctx:data:earlier");
        let mut v = json!({ "already": existing });
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
            "a": big1,
            "b": big2,
            "c": "tiny",
        });
        let mut refs = externalize_fields(&mut v, "inst42", 1024);
        refs.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(refs.len(), 2);
        assert!(refs[0].0.starts_with("inst42:ctx:v1:"));
        assert!(refs[0].0.ends_with(":a"));
        assert_eq!(refs[0].1, json!(big1));
        assert!(refs[1].0.starts_with("inst42:ctx:v1:"));
        assert!(refs[1].0.ends_with(":b"));
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
        assert!(!is_marker(
            &json!({"_externalized": true, "_ref": "k", "x": 1})
        ));
        assert!(!is_marker(&json!({"_externalized": false, "_ref": "k"})));
        assert!(!is_marker(&json!({"_ref": "k"})));
        assert!(!is_marker(&json!("scalar")));
    }

    #[test]
    fn reserved_shapes_round_trip_as_opaque_literals() {
        let input = json!({"payload": [
            marker("legacy"),
            versioned_marker("versioned"),
            {"_externalized": "literal:v1", "_value": marker("nested")},
            {"_externalized": true, "_ref": "ordinary", "extra": true}
        ]});
        let mut stored = input.clone();
        escape_literals(&mut stored);
        assert_eq!(
            literal_payload(&stored["payload"][0]),
            Some(&input["payload"][0])
        );
        assert_eq!(
            literal_payload(&stored["payload"][1]),
            Some(&input["payload"][1])
        );
        assert_eq!(
            literal_payload(&stored["payload"][2]),
            Some(&input["payload"][2])
        );
        assert_eq!(stored["payload"][3], input["payload"][3]);
        visit_context_refs(&stored, |_, _| panic!("literal interpreted as reference"));
        unescape_literals(&mut stored);
        assert_eq!(stored, input);
    }

    #[test]
    fn selected_paths_support_arrays_and_escaped_tokens_without_rewriting_siblings() {
        let mut data = json!({"a/b": [{"title":"draft", "body~html":"large"}]});
        let path = "/a~1b/0/body~0html";
        let payloads = externalize_paths(&mut data, "owner", [path, "/missing"]);
        assert_eq!(payloads.len(), 1);
        let (key, payload) = &payloads[0];
        assert_eq!(*payload, "large");
        let mut refs = Vec::new();
        visit_context_refs(&data, |key, versioned| {
            refs.push((key.to_owned(), versioned));
        });
        assert_eq!(refs, vec![(key.clone(), true)]);
        *context_pointer_mut(&mut data, "/a~1b/0/title").unwrap() = json!("published");
        assert!(externalize_paths(&mut data, "owner", [path]).is_empty());
        assert_eq!(
            ref_key(data.pointer(path).unwrap(), false),
            Some(key.as_str())
        );
    }

    #[test]
    fn externalize_fields_at_exact_threshold_boundary() {
        // `encoded.len() < threshold` is the skip condition, so a value whose
        // serialized size exactly equals the threshold MUST be externalized.
        // This pins the boundary so a refactor to `<=` would immediately fail.
        let payload = "x".repeat(10);
        // serialized form is `"xxxxxxxxxx"` (12 bytes including quotes).
        let encoded_len = serde_json::to_vec(&json!(payload)).unwrap().len();
        let mut v = json!({ "k": payload });
        #[allow(clippy::cast_possible_truncation)]
        let refs = externalize_fields(&mut v, "inst1", encoded_len as u32);
        assert_eq!(
            refs.len(),
            1,
            "value at exact threshold must be externalized"
        );
        assert!(is_marker(&v["k"]));

        // One byte above threshold — still not externalized.
        let mut v = json!({ "k": payload });
        #[allow(clippy::cast_possible_truncation)]
        let refs = externalize_fields(&mut v, "inst1", (encoded_len + 1) as u32);
        assert!(
            refs.is_empty(),
            "value one byte under threshold stays inline"
        );
    }

    #[test]
    fn externalize_fields_empty_object_is_no_op() {
        // Empty input must not panic and must return no refs — belt-and-suspenders
        // for the fast write path where zero-field contexts are common.
        let mut v = json!({});
        let refs = externalize_fields(&mut v, "inst1", 1);
        assert!(refs.is_empty());
        assert_eq!(v, json!({}));
    }

    #[test]
    fn replacements_use_distinct_revision_keys_and_keep_existing_markers() {
        let mut first = json!({"field":"one"});
        let first_ref = externalize_fields(&mut first, "owner", 1).remove(0);
        let mut second = json!({"field":"two", "untouched":first["field"]});
        let second_ref = externalize_fields(&mut second, "owner", 1).remove(0);
        assert_ne!(first_ref.0, second_ref.0);
        assert_eq!(first_ref.1, "one");
        assert_eq!(second_ref.1, "two");
        assert_eq!(second["untouched"], first["field"]);
    }

    #[test]
    fn externalize_fields_handles_field_name_with_colon() {
        // The field is the complete suffix after the revision, including any
        // colons present in the original key.
        let big = "x".repeat(2048);
        let mut v = json!({ "weird:field": big });
        let refs = externalize_fields(&mut v, "inst1", 1024);
        assert_eq!(refs.len(), 1);
        assert!(refs[0].0.starts_with("inst1:ctx:v1:"));
        assert!(refs[0].0.ends_with(":weird:field"));
        assert_eq!(v["weird:field"]["_ref"], json!(refs[0].0));
    }

    #[test]
    fn externalize_fields_handles_null_and_number_payloads() {
        // Scalars below threshold stay inline regardless of type. This guards
        // against any accidental special-casing of non-string values.
        let mut v = json!({ "n": 42, "null": null, "bool": true });
        let refs = externalize_fields(&mut v, "inst1", 1024);
        assert!(refs.is_empty());
        assert_eq!(v["n"], json!(42));
        assert_eq!(v["null"], json!(null));
        assert_eq!(v["bool"], json!(true));
    }

    #[test]
    fn context_data_ref_key_format_is_stable() {
        // The key format is a storage-layer contract — GC sweepers, debug
        // tooling, and FK cascade all rely on it. Any format change is a
        // breaking migration.
        assert_eq!(
            context_data_ref_key("01HXYZ", "user_payload"),
            "01HXYZ:ctx:data:user_payload"
        );
        // Empty instance / field are not valid in practice but must not
        // produce malformed keys that break parsers downstream.
        assert_eq!(context_data_ref_key("", "field"), ":ctx:data:field");
        assert_eq!(context_data_ref_key("inst", ""), "inst:ctx:data:");
    }

    #[test]
    fn marker_roundtrips_through_is_marker_with_various_keys() {
        // Ref-key content should not matter for marker recognition — only
        // the two-key shape and value types do.
        for key in [
            "",
            "simple",
            "with:colons",
            "with spaces",
            "日本語",
            "a".repeat(1024).as_str(),
        ] {
            let m = marker(key);
            assert!(is_marker(&m), "marker('{key}') not recognized as marker");
            assert_eq!(m[REF_KEY], json!(key));
        }
    }
}
