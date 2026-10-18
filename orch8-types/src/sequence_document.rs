//! Sequence documents on disk and over the wire: JSON or YAML.
//!
//! A sequence has one schema (`contracts/sequence.schema.json`); YAML is
//! only an alternative *syntax* for the same document. Everything that reads
//! an authoring file (CLI, API `Content-Type: application/yaml`) parses it
//! into a `serde_json::Value` here and then runs the exact same decode and
//! validation path as JSON, so the two formats can never drift apart.
//!
//! Parse errors carry a 1-based line/column so editors and terminals can
//! point at the offending token.

use std::fmt;
use std::path::Path;

use serde_json::Value;

/// Syntax of a sequence document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentFormat {
    Json,
    Yaml,
}

impl DocumentFormat {
    /// Pick the format from a file extension: `.yaml` / `.yml` (any case)
    /// are YAML, everything else is JSON.
    #[must_use]
    pub fn from_path(path: &Path) -> Self {
        match path
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("yaml" | "yml") => Self::Yaml,
            _ => Self::Json,
        }
    }

    /// Pick the format from an HTTP `Content-Type`. Returns `None` for media
    /// types that are neither JSON nor YAML.
    #[must_use]
    pub fn from_content_type(content_type: &str) -> Option<Self> {
        let essence = content_type
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        match essence.as_str() {
            "application/yaml" | "application/x-yaml" | "text/yaml" | "text/x-yaml" => {
                Some(Self::Yaml)
            }
            "application/json" | "" => Some(Self::Json),
            other if other.ends_with("+json") => Some(Self::Json),
            other if other.ends_with("+yaml") => Some(Self::Yaml),
            _ => None,
        }
    }

    /// Lowercase display name (`json` / `yaml`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Yaml => "yaml",
        }
    }

    /// Conventional file extension (without the dot).
    #[must_use]
    pub const fn extension(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Yaml => "yaml",
        }
    }
}

/// True when `path` has a sequence-document extension (`.json`, `.yaml`,
/// `.yml`).
#[must_use]
pub fn is_document_path(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase)
        .is_some_and(|e| matches!(e.as_str(), "json" | "yaml" | "yml"))
}

/// A syntax error in a sequence document, with its 1-based location when
/// the parser reported one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentError {
    pub format: DocumentFormat,
    pub message: String,
    pub line: Option<usize>,
    pub column: Option<usize>,
}

impl fmt::Display for DocumentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.format {
            DocumentFormat::Json => "JSON",
            DocumentFormat::Yaml => "YAML",
        };
        match (self.line, self.column) {
            (Some(line), Some(column)) => write!(
                f,
                "invalid {kind} at line {line}, column {column}: {}",
                self.message
            ),
            (Some(line), None) => write!(f, "invalid {kind} at line {line}: {}", self.message),
            _ => write!(f, "invalid {kind}: {}", self.message),
        }
    }
}

impl std::error::Error for DocumentError {}

/// Strip the location suffix serde's error types append to their messages
/// (" at line 3 column 5"), since [`DocumentError`] reports it separately.
fn strip_location_suffix(message: &str) -> String {
    let trimmed = message.trim();
    if let Some(idx) = trimmed.rfind(" at line ") {
        let tail = &trimmed[idx + " at line ".len()..];
        if tail
            .split(|c: char| !c.is_ascii_digit())
            .next()
            .is_some_and(|n| !n.is_empty())
        {
            return trimmed[..idx].to_string();
        }
    }
    trimmed.to_string()
}

/// Upper bound on the size of a YAML document *after* alias expansion, in
/// abstract bytes (each node has a fixed cost plus its string length).
///
/// YAML anchors/aliases let a ~1 KB body expand into millions of values
/// ("billion laughs"); the YAML library's repetition limit does not bound the
/// expanded size. 16 MiB comfortably exceeds any real sequence document (the
/// HTTP body itself is capped at a few MiB) while keeping a hostile expansion
/// from exhausting memory.
pub const MAX_YAML_EXPANDED_BYTES: usize = 16 * 1024 * 1024;
const YAML_NODE_COST: usize = 16;

/// Deserialize YAML into a `serde_json::Value`, charging every produced node
/// and string byte against a budget so alias expansion aborts early.
struct BoundedValue<'b> {
    budget: &'b std::cell::Cell<usize>,
}

impl BoundedValue<'_> {
    fn charge<E: serde::de::Error>(&self, bytes: usize) -> Result<(), E> {
        let cost = YAML_NODE_COST.saturating_add(bytes);
        match self.budget.get().checked_sub(cost) {
            Some(left) => {
                self.budget.set(left);
                Ok(())
            }
            None => Err(E::custom(format!(
                "document expands beyond {MAX_YAML_EXPANDED_BYTES} bytes \
                 (too many or too deeply nested aliases)"
            ))),
        }
    }
}

impl<'de> serde::de::DeserializeSeed<'de> for BoundedValue<'_> {
    type Value = Value;

    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> serde::de::Visitor<'de> for BoundedValue<'_> {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any YAML value")
    }

    fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Value, E> {
        self.charge(0)?;
        Ok(Value::Bool(v))
    }

    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Value, E> {
        self.charge(0)?;
        Ok(Value::from(v))
    }

    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Value, E> {
        self.charge(0)?;
        Ok(Value::from(v))
    }

    fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Value, E> {
        self.charge(0)?;
        Ok(serde_json::Number::from_f64(v).map_or(Value::Null, Value::Number))
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Value, E> {
        self.charge(v.len())?;
        Ok(Value::String(v.to_owned()))
    }

    fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Value, E> {
        self.charge(v.len())?;
        Ok(Value::String(v))
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<Value, E> {
        self.charge(0)?;
        Ok(Value::Null)
    }

    fn visit_none<E: serde::de::Error>(self) -> Result<Value, E> {
        self.visit_unit()
    }

    fn visit_some<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        serde::de::DeserializeSeed::deserialize(self, deserializer)
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        self.charge(0)?;
        let mut items = Vec::new();
        while let Some(item) = seq.next_element_seed(BoundedValue {
            budget: self.budget,
        })? {
            items.push(item);
        }
        Ok(Value::Array(items))
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        self.charge(0)?;
        let mut object = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            self.charge(key.len())?;
            let value = map.next_value_seed(BoundedValue {
                budget: self.budget,
            })?;
            object.insert(key, value);
        }
        Ok(Value::Object(object))
    }
}

fn parse_bounded_yaml(text: &str) -> Result<Value, serde_norway::Error> {
    let budget = std::cell::Cell::new(MAX_YAML_EXPANDED_BYTES);
    serde::de::DeserializeSeed::deserialize(
        BoundedValue { budget: &budget },
        serde_norway::Deserializer::from_str(text),
    )
}

/// Parse a document into a JSON value. The caller then runs the normal
/// sequence decode/validation on the value.
pub fn parse_document(text: &str, format: DocumentFormat) -> Result<Value, DocumentError> {
    match format {
        DocumentFormat::Json => serde_json::from_str(text).map_err(|e| DocumentError {
            format,
            message: strip_location_suffix(&e.to_string()),
            line: (e.line() > 0).then_some(e.line()),
            column: (e.column() > 0).then_some(e.column()),
        }),
        DocumentFormat::Yaml => {
            if text.trim().is_empty() {
                return Err(DocumentError {
                    format,
                    message: "document is empty".into(),
                    line: None,
                    column: None,
                });
            }
            parse_bounded_yaml(text).map_err(|e| {
                let location = e.location();
                DocumentError {
                    format,
                    message: strip_location_suffix(&e.to_string()),
                    line: location.as_ref().map(serde_norway::Location::line),
                    column: location.as_ref().map(serde_norway::Location::column),
                }
            })
        }
    }
}

/// Render a JSON value in the requested syntax (pretty JSON with a trailing
/// newline, or block-style YAML).
pub fn render_document(value: &Value, format: DocumentFormat) -> Result<String, DocumentError> {
    match format {
        DocumentFormat::Json => serde_json::to_string_pretty(value)
            .map(|mut s| {
                s.push('\n');
                s
            })
            .map_err(|e| DocumentError {
                format,
                message: e.to_string(),
                line: None,
                column: None,
            }),
        DocumentFormat::Yaml => serde_norway::to_string(value).map_err(|e| DocumentError {
            format,
            message: e.to_string(),
            line: None,
            column: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const YAML_SEQ: &str = r#"
$schema: https://orch8.io/contracts/sequence.schema.json
tenant_id: demo
namespace: default
name: hello-yaml
version: 1
blocks:
  - type: step
    id: greet
    handler: log
    params:
      message: "Hello: from YAML"
  - type: router
    id: route
    routes:
      - condition: data.plan == "paid"
        blocks:
          - { type: step, id: paid, handler: noop }
    default:
      - type: step
        id: other
        handler: noop
        delay: { duration: 5000 }
"#;

    #[test]
    fn format_from_path_and_content_type() {
        assert_eq!(
            DocumentFormat::from_path(Path::new("a/seq.YAML")),
            DocumentFormat::Yaml
        );
        assert_eq!(
            DocumentFormat::from_path(Path::new("seq.yml")),
            DocumentFormat::Yaml
        );
        assert_eq!(
            DocumentFormat::from_path(Path::new("seq.json")),
            DocumentFormat::Json
        );
        assert_eq!(
            DocumentFormat::from_path(Path::new("seq")),
            DocumentFormat::Json
        );
        assert_eq!(
            DocumentFormat::from_content_type("application/yaml; charset=utf-8"),
            Some(DocumentFormat::Yaml)
        );
        assert_eq!(
            DocumentFormat::from_content_type("text/x-yaml"),
            Some(DocumentFormat::Yaml)
        );
        assert_eq!(
            DocumentFormat::from_content_type("application/json"),
            Some(DocumentFormat::Json)
        );
        assert_eq!(
            DocumentFormat::from_content_type("application/merge-patch+json"),
            Some(DocumentFormat::Json)
        );
        assert_eq!(DocumentFormat::from_content_type("text/plain"), None);
        assert!(is_document_path(Path::new("x.yml")));
        assert!(is_document_path(Path::new("x.JSON")));
        assert!(!is_document_path(Path::new("x.toml")));
    }

    #[test]
    fn yaml_decodes_to_the_same_sequence_as_json() {
        let from_yaml = parse_document(YAML_SEQ, DocumentFormat::Yaml).unwrap();
        let as_json = render_document(&from_yaml, DocumentFormat::Json).unwrap();
        let from_json = parse_document(&as_json, DocumentFormat::Json).unwrap();
        assert_eq!(from_yaml, from_json);
        assert_eq!(
            from_yaml["blocks"][0]["params"]["message"],
            "Hello: from YAML"
        );
        assert_eq!(
            from_yaml["blocks"][1]["default"][0]["delay"]["duration"],
            5000
        );

        // The value decodes through the one strict sequence decoder.
        let mut value = from_yaml;
        value["id"] = serde_json::json!(uuid::Uuid::now_v7());
        value["created_at"] = serde_json::json!("2026-01-01T00:00:00Z");
        let seq = crate::sequence::deserialize_sequence_strict(&value).unwrap();
        seq.validate().unwrap();
        assert_eq!(seq.name, "hello-yaml");
    }

    #[test]
    fn yaml_round_trips_losslessly() {
        let original = parse_document(YAML_SEQ, DocumentFormat::Yaml).unwrap();
        let yaml = render_document(&original, DocumentFormat::Yaml).unwrap();
        let back = parse_document(&yaml, DocumentFormat::Yaml).unwrap();
        assert_eq!(original, back);
        // Strings that look like other YAML scalars stay strings.
        let tricky = serde_json::json!({"a": "yes", "b": "1.0", "c": "null", "d": "- x", "e": ""});
        let yaml = render_document(&tricky, DocumentFormat::Yaml).unwrap();
        assert_eq!(parse_document(&yaml, DocumentFormat::Yaml).unwrap(), tricky);
    }

    #[test]
    fn yaml_errors_report_line_and_column() {
        let bad = "name: x\nblocks:\n  - type: step\n    id: [unclosed\n";
        let err = parse_document(bad, DocumentFormat::Yaml).unwrap_err();
        assert_eq!(err.format, DocumentFormat::Yaml);
        assert!(err.line.is_some(), "{err:?}");
        assert!(err.column.is_some(), "{err:?}");
        assert!(err.to_string().starts_with("invalid YAML at line"), "{err}");

        let tab_indent = "name: x\nblocks:\n\t- id: a\n";
        let err = parse_document(tab_indent, DocumentFormat::Yaml).unwrap_err();
        assert_eq!(err.line, Some(3), "{err:?}");

        let bad_json = "{\n  \"name\": \"x\",\n  \"blocks\": [,]\n}";
        let err = parse_document(bad_json, DocumentFormat::Json).unwrap_err();
        assert_eq!(err.line, Some(3));
        assert!(err.column.is_some());
        assert!(!err.message.contains(" at line "), "{}", err.message);
    }

    #[test]
    fn empty_yaml_is_an_error_not_null() {
        let err = parse_document("  \n", DocumentFormat::Yaml).unwrap_err();
        assert!(err.message.contains("empty"));
    }

    /// C1-1 / T2-GAP-API-06: ~1 KB of nested aliases expands to ~1.3 M
    /// values. The YAML library's own repetition limit (100 alias jumps per
    /// event) does not stop this, so the expansion itself is budgeted.
    #[test]
    fn yaml_alias_bomb_is_rejected_before_it_expands() {
        let leaf = vec!["x"; 200].join(", ");
        let nine = |name: &str| vec![format!("*{name}"); 9].join(", ");
        let bomb = format!(
            "a: &a [{leaf}]\nb: &b [{}]\nc: &c [{}]\nd: &d [{}]\ne: [{}]\n",
            nine("a"),
            nine("b"),
            nine("c"),
            nine("d")
        );
        assert!(bomb.len() < 2_048, "bomb is {} bytes", bomb.len());

        let err = parse_document(&bomb, DocumentFormat::Yaml).unwrap_err();

        assert!(err.message.contains("expands beyond"), "{err}");
    }

    #[test]
    fn yaml_alias_of_a_large_scalar_repeated_is_rejected() {
        let big = "y".repeat(100_000);
        let refs = vec!["*s"; 300].join(", ");
        let doc = format!("s: &s \"{big}\"\nl: [{refs}]\n");

        let err = parse_document(&doc, DocumentFormat::Yaml).unwrap_err();

        assert!(err.message.contains("expands beyond"), "{err}");
    }

    #[test]
    fn yaml_with_modest_anchor_reuse_still_parses() {
        let doc = "defaults: &d {retries: 3, timeout: 30}\nblocks:\n  - {id: a, params: *d}\n  - {id: b, params: *d}\n";

        let value = parse_document(doc, DocumentFormat::Yaml).unwrap();

        assert_eq!(value["blocks"][1]["params"]["retries"], 3);
    }
}
