//! Built-in sequence templates embedded at compile time.
//!
//! Each template is a complete sequence JSON that `orch8 init --template`
//! writes as `sequence.json` and `orch8 templates show` prints verbatim.
//! The agent-pattern templates are embedded from `docs/agent-patterns/` via
//! `include_str!`, so docs and CLI can never drift apart. Tests at the bottom
//! guarantee every entry is valid JSON and that its `blocks` deserialize into
//! the engine's `BlockDefinition` DSL.

use anyhow::{anyhow, Result};

/// A built-in sequence template shipped with the CLI.
#[derive(Debug)]
pub struct Template {
    /// Kebab-case slug used for lookup (e.g. `react-loop`).
    pub name: &'static str,
    /// One-line summary shown by `orch8 templates list`.
    pub description: &'static str,
    /// Raw sequence definition JSON.
    pub json: &'static str,
}

/// The scaffold written by `orch8 init` when no `--template` is given:
/// a minimal three-step hello-world sequence.
const DEFAULT_JSON: &str = r#"{
  "tenant_id": "demo",
  "namespace": "default",
  "name": "hello-world",
  "version": 1,
  "blocks": [
    {
      "type": "step",
      "id": "greet",
      "handler": "greet_user",
      "params": { "message": "Hello from Orch8!" }
    },
    {
      "type": "step",
      "id": "wait",
      "handler": "noop",
      "delay": { "duration": 5000 }
    },
    {
      "type": "step",
      "id": "complete",
      "handler": "noop",
      "params": { "message": "Workflow complete." }
    }
  ]
}
"#;

/// Registry of all built-in templates, in display order.
pub const TEMPLATES: &[Template] = &[
    Template {
        name: "default",
        description: "Minimal hello-world sequence: greet, delayed wait, complete.",
        json: DEFAULT_JSON,
    },
    Template {
        name: "react-loop",
        description: "Observe-Think-Act agent loop with tool calling, iterating until done.",
        json: include_str!("../../docs/agent-patterns/react-loop.json"),
    },
    Template {
        name: "tool-calling-pipeline",
        description: "LLM plans, executes tools in sequence, then synthesizes the results.",
        json: include_str!("../../docs/agent-patterns/tool-calling-pipeline.json"),
    },
    Template {
        name: "guardrail-validation",
        description: "Input guardrail, LLM generation, output guardrail, human review gate.",
        json: include_str!("../../docs/agent-patterns/guardrail-validation.json"),
    },
    Template {
        name: "multi-agent-delegation",
        description: "Orchestrator decomposes a task and delegates sub-tasks to parallel agents.",
        json: include_str!("../../docs/agent-patterns/multi-agent-delegation.json"),
    },
];

/// Look up a template by name, or fail with an error that lists every
/// available template so the user can self-correct.
pub fn find(name: &str) -> Result<&'static Template> {
    TEMPLATES.iter().find(|t| t.name == name).ok_or_else(|| {
        anyhow!(
            "unknown template `{name}` (available: {})",
            available_names().join(", ")
        )
    })
}

/// Names of all built-in templates, in display order.
pub fn available_names() -> Vec<&'static str> {
    TEMPLATES.iter().map(|t| t.name).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_template_is_valid_json() {
        for t in TEMPLATES {
            let parsed: Result<serde_json::Value, _> = serde_json::from_str(t.json);
            assert!(
                parsed.is_ok(),
                "template `{}` is not valid JSON: {:?}",
                t.name,
                parsed.err()
            );
        }
    }

    #[test]
    fn every_template_has_name_and_blocks() {
        for t in TEMPLATES {
            let v: serde_json::Value = serde_json::from_str(t.json).unwrap();
            let name = v.get("name").and_then(serde_json::Value::as_str);
            assert!(
                name.is_some_and(|n| !n.is_empty()),
                "template `{}` is missing a non-empty top-level `name`",
                t.name
            );
            let blocks = v.get("blocks").and_then(serde_json::Value::as_array);
            assert!(
                blocks.is_some_and(|b| !b.is_empty()),
                "template `{}` is missing a non-empty top-level `blocks` array",
                t.name
            );
        }
    }

    /// The full `SequenceDefinition` carries server-assigned fields (`id`,
    /// `created_at`) that authoring payloads never include, so the typed
    /// guarantee targets the part the engine actually interprets: every
    /// template's `blocks` must deserialize into the workflow DSL.
    #[test]
    fn every_template_blocks_deserialize_into_block_definitions() {
        for t in TEMPLATES {
            let v: serde_json::Value = serde_json::from_str(t.json).unwrap();
            let blocks = v.get("blocks").cloned().expect("blocks checked above");
            let typed: Result<Vec<orch8_types::sequence::BlockDefinition>, _> =
                serde_json::from_value(blocks);
            assert!(
                typed.is_ok(),
                "template `{}` blocks do not deserialize into BlockDefinition: {:?}",
                t.name,
                typed.err()
            );
        }
    }

    #[test]
    fn template_names_are_unique_kebab_case() {
        let mut seen = std::collections::HashSet::new();
        for t in TEMPLATES {
            assert!(seen.insert(t.name), "duplicate template name `{}`", t.name);
            assert!(
                t.name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
                "template name `{}` is not kebab-case",
                t.name
            );
            assert!(
                !t.description.is_empty(),
                "template `{}` has an empty description",
                t.name
            );
        }
    }

    #[test]
    fn find_returns_known_templates() {
        assert_eq!(find("default").unwrap().name, "default");
        assert_eq!(find("react-loop").unwrap().name, "react-loop");
    }

    #[test]
    fn find_unknown_lists_available_templates() {
        let err = find("nope").unwrap_err().to_string();
        assert!(err.contains("unknown template `nope`"), "got: {err}");
        for name in available_names() {
            assert!(err.contains(name), "error should list `{name}`: {err}");
        }
    }
}
