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
            serde_norway::from_str::<Value>(text).map_err(|e| {
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
}
