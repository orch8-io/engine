//! Reading and writing sequence documents (JSON or YAML) for every CLI
//! command that touches an authoring file.
//!
//! The format is chosen by extension (`.yaml` / `.yml` → YAML, anything
//! else → JSON); both parse into the same `serde_json::Value` and then go
//! through the unchanged decode/validation path, so YAML is purely a
//! syntax choice over `contracts/sequence.schema.json`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use serde_json::Value;

pub use orch8_types::sequence_document::{DocumentFormat, is_document_path};
use orch8_types::sequence_document::{parse_document, render_document};

/// File names probed (in order) when a directory is given instead of a file.
pub const DEFAULT_SEQUENCE_FILES: &[&str] = &["sequence.json", "sequence.yaml", "sequence.yml"];

/// Parse document text whose origin is `path` (used for the format and for
/// error messages of the form `path:line:column: message`).
pub fn parse_text(path: &Path, text: &str) -> Result<Value> {
    let format = DocumentFormat::from_path(path);
    parse_document(text, format).map_err(|e| match (e.line, e.column) {
        (Some(line), Some(column)) => anyhow!(
            "{}:{line}:{column}: invalid {}: {}{}",
            path.display(),
            format.as_str().to_ascii_uppercase(),
            e.message,
            coded_suffix("DOCUMENT_SYNTAX")
        ),
        _ => anyhow!("{}: {e}{}", path.display(), coded_suffix("DOCUMENT_SYNTAX")),
    })
}

/// ` [ORCH8-V001] — see https://orch8.io/docs/errors#ORCH8-V001` for a
/// catalogued key, empty otherwise. Appended to local validation errors so
/// CLI output carries the same stable codes as API error bodies.
pub fn coded_suffix(key: &str) -> String {
    orch8_types::error_catalog::lookup(key)
        .map(|entry| format!(" [{}] — see {}", entry.code, entry.docs_url()))
        .unwrap_or_default()
}

/// Read and parse a JSON or YAML document from disk.
pub fn read_document(path: &Path) -> Result<Value> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    parse_text(path, &text)
}

/// Render `value` in the syntax implied by `path`'s extension.
pub fn render_for_path(path: &Path, value: &Value) -> Result<String> {
    render(value, DocumentFormat::from_path(path))
}

/// Render `value` as pretty JSON or YAML.
pub fn render(value: &Value, format: DocumentFormat) -> Result<String> {
    render_document(value, format).map_err(|e| anyhow!("{e}"))
}

/// Atomically write `value` to `path` in the syntax implied by its extension.
pub fn write_document(path: &Path, value: &Value) -> Result<()> {
    let rendered = render_for_path(path, value)?;
    crate::atomic_write(path, rendered.as_bytes())
}

/// First existing default sequence file in `dir` (`sequence.json`,
/// `sequence.yaml`, `sequence.yml`).
pub fn default_sequence_in(dir: &Path) -> Option<PathBuf> {
    DEFAULT_SEQUENCE_FILES
        .iter()
        .map(|name| dir.join(name))
        .find(|candidate| candidate.is_file())
}

/// Value parser for `--format json|yaml` style flags.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum FormatArg {
    #[default]
    Json,
    Yaml,
}

impl From<FormatArg> for DocumentFormat {
    fn from(value: FormatArg) -> Self {
        match value {
            FormatArg::Json => Self::Json,
            FormatArg::Yaml => Self::Yaml,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yaml_errors_are_prefixed_with_path_line_and_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flow.yaml");
        std::fs::write(&path, "name: x\nblocks:\n  - id: [oops\n").unwrap();
        let err = read_document(&path).unwrap_err().to_string();
        let prefix = format!("{}:", path.display());
        assert!(err.starts_with(&prefix), "{err}");
        assert!(err.contains("invalid YAML"), "{err}");
        // path:LINE:COL:
        let rest = &err[prefix.len()..];
        let mut parts = rest.splitn(3, ':');
        assert!(parts.next().unwrap().parse::<usize>().is_ok(), "{err}");
        assert!(parts.next().unwrap().parse::<usize>().is_ok(), "{err}");
    }

    #[test]
    fn write_then_read_round_trips_in_both_formats() {
        let dir = tempfile::tempdir().unwrap();
        let value = serde_json::json!({"name": "n", "blocks": [{"type": "step", "id": "a", "handler": "noop"}]});
        for file in ["s.json", "s.yaml", "s.yml"] {
            let path = dir.path().join(file);
            write_document(&path, &value).unwrap();
            assert_eq!(read_document(&path).unwrap(), value, "{file}");
        }
        let yaml = std::fs::read_to_string(dir.path().join("s.yaml")).unwrap();
        assert!(yaml.contains("name: n"), "{yaml}");
    }

    #[test]
    fn default_sequence_prefers_json_then_yaml() {
        let dir = tempfile::tempdir().unwrap();
        assert!(default_sequence_in(dir.path()).is_none());
        std::fs::write(dir.path().join("sequence.yml"), "a: 1").unwrap();
        assert!(
            default_sequence_in(dir.path())
                .unwrap()
                .ends_with("sequence.yml")
        );
        std::fs::write(dir.path().join("sequence.json"), "{}").unwrap();
        assert!(
            default_sequence_in(dir.path())
                .unwrap()
                .ends_with("sequence.json")
        );
    }
}
