use std::fmt::Write as _;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use serde_json::{Value, json};

#[derive(Args)]
pub struct GenerateCmd {
    /// Natural-language workflow description, or @path to read it from a file.
    pub prompt: String,
    /// OpenAI-compatible chat-completions URL.
    #[arg(
        long,
        env = "ORCH8_LLM_URL",
        default_value = "https://api.openai.com/v1/chat/completions"
    )]
    pub llm_url: String,
    /// Model understood by the configured provider.
    #[arg(long, env = "ORCH8_LLM_MODEL", default_value = "gpt-5-mini")]
    pub model: String,
    /// Provider API key.
    #[arg(long, env = "ORCH8_LLM_API_KEY", hide_env_values = true)]
    pub llm_api_key: String,
    /// Destination sequence file.
    #[arg(long, default_value = "sequence.json")]
    pub out: PathBuf,
    /// Maximum generate/validate/repair attempts.
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u8).range(1..=8))]
    pub attempts: u8,
    /// Restrict generated steps to a vendor catalog (file path or http(s)
    /// URL). Every step handler must be listed; violations are repaired or
    /// rejected. Format: `{"handlers": [...], "pieces": [{"name", "actions"?}]}`
    /// (an Activepieces sidecar `/catalog` response also works).
    #[arg(long, value_name = "CATALOG")]
    pub pieces_from: Option<String>,
}

/// Allowed step handlers for `--pieces-from`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Catalog {
    /// Exact handler names (built-ins or worker handlers).
    pub handlers: std::collections::BTreeSet<String>,
    /// Activepieces piece -> allowed actions (`None` = every action).
    pub pieces: std::collections::BTreeMap<String, Option<std::collections::BTreeSet<String>>>,
}

impl Catalog {
    pub fn parse(value: &Value) -> Result<Self> {
        let mut catalog = Self::default();
        let object = value
            .as_object()
            .context("catalog must be a JSON object with `handlers` and/or `pieces`")?;
        if let Some(handlers) = object.get("handlers") {
            for handler in handlers.as_array().context("`handlers` must be an array")? {
                let name = handler
                    .as_str()
                    .context("`handlers` entries must be strings")?;
                catalog.handlers.insert(name.to_owned());
            }
        }
        if let Some(pieces) = object.get("pieces") {
            for piece in pieces.as_array().context("`pieces` must be an array")? {
                let (name, actions) = match piece {
                    Value::String(name) => (name.as_str(), None),
                    Value::Object(piece) => (
                        piece
                            .get("name")
                            .and_then(Value::as_str)
                            .context("every piece needs a `name`")?,
                        piece.get("actions").and_then(Value::as_array),
                    ),
                    _ => bail!("`pieces` entries must be strings or objects"),
                };
                let name = name.trim_start_matches("@activepieces/piece-");
                let actions = actions.map(|list| {
                    list.iter()
                        .filter_map(|a| {
                            a.as_str()
                                .or_else(|| a.get("name").and_then(Value::as_str))
                                .map(ToOwned::to_owned)
                        })
                        .collect()
                });
                catalog.pieces.insert(name.to_owned(), actions);
            }
        }
        if catalog.handlers.is_empty() && catalog.pieces.is_empty() {
            bail!("catalog lists no handlers or pieces");
        }
        Ok(catalog)
    }

    pub fn allows(&self, handler: &str) -> bool {
        if self.handlers.contains(handler) {
            return true;
        }
        let Some(rest) = handler.strip_prefix("ap://") else {
            return false;
        };
        let (piece, action) = rest.split_once('.').unwrap_or((rest, ""));
        match self.pieces.get(piece) {
            Some(None) => true,
            Some(Some(actions)) => actions.contains(action),
            None => false,
        }
    }

    /// Human/LLM-readable list of what may be used.
    pub fn describe(&self) -> String {
        let mut allowed: Vec<String> = self.handlers.iter().cloned().collect();
        for (piece, actions) in &self.pieces {
            match actions {
                None => allowed.push(format!("ap://{piece}.<any action>")),
                Some(actions) => {
                    allowed.extend(actions.iter().map(|a| format!("ap://{piece}.{a}")));
                }
            }
        }
        allowed.join(", ")
    }
}

async fn load_catalog(source: &str) -> Result<Catalog> {
    let text = if source.starts_with("https://") || source.starts_with("http://") {
        crate::external_client()?
            .get(source)
            .send()
            .await
            .with_context(|| format!("fetch catalog {source}"))?
            .error_for_status()?
            .text()
            .await?
    } else {
        std::fs::read_to_string(source).with_context(|| format!("read catalog {source}"))?
    };
    let value: Value = serde_json::from_str(&text).context("catalog is not JSON")?;
    Catalog::parse(&value)
}

/// Every step handler (and fallback handler) referenced by a sequence
/// document. Step `params` are opaque and not searched.
pub fn referenced_handlers(value: &Value) -> Vec<String> {
    fn walk(value: &Value, out: &mut Vec<String>) {
        match value {
            Value::Object(object) => {
                for key in ["handler", "fallback_handler"] {
                    if let Some(Value::String(name)) = object.get(key) {
                        out.push(name.clone());
                    }
                }
                for (key, child) in object {
                    if !matches!(key.as_str(), "params" | "context" | "input" | "metadata") {
                        walk(child, out);
                    }
                }
            }
            Value::Array(items) => items.iter().for_each(|item| walk(item, out)),
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(value, &mut out);
    out.sort();
    out.dedup();
    out
}

fn catalog_violations(catalog: &Catalog, value: &Value) -> Vec<String> {
    referenced_handlers(value)
        .into_iter()
        .filter(|handler| !catalog.allows(handler))
        .collect()
}

fn strip_fence(content: &str) -> &str {
    let trimmed = content.trim();
    let Some(rest) = trimmed.strip_prefix("```") else {
        return trimmed;
    };
    let rest = rest.strip_prefix("json").unwrap_or(rest).trim_start();
    rest.strip_suffix("```").unwrap_or(rest).trim_end()
}

fn prompt_text(argument: &str) -> Result<String> {
    if let Some(path) = argument.strip_prefix('@') {
        std::fs::read_to_string(path).with_context(|| format!("reading prompt {path}"))
    } else {
        Ok(argument.to_owned())
    }
}

fn add_authoring_defaults(value: &mut Value, schema_url: &str) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    object
        .entry("$schema")
        .or_insert_with(|| Value::String(schema_url.to_owned()));
    object
        .entry("schema_version")
        .or_insert_with(|| Value::from(orch8_types::sequence::SEQUENCE_SCHEMA_VERSION));
    object
        .entry("id")
        .or_insert_with(|| Value::String(uuid::Uuid::new_v4().to_string()));
    object
        .entry("tenant_id")
        .or_insert_with(|| Value::String("default".to_owned()));
    object
        .entry("namespace")
        .or_insert_with(|| Value::String("default".to_owned()));
    object.entry("version").or_insert_with(|| Value::from(1));
    object.entry("created_at").or_insert_with(|| {
        Value::String(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
    });
}

pub async fn run(cmd: GenerateCmd) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(180))
        .build()?;
    let request = prompt_text(&cmd.prompt)?;
    let schema_url = "https://orch8.io/contracts/sequence.schema.json";
    let catalog = match &cmd.pieces_from {
        Some(source) => Some(load_catalog(source).await?),
        None => None,
    };
    let mut system = format!(
        "You author Orch8 SequenceDefinition JSON. Return JSON only. Use schema {schema_url}. \
         Include id (UUID), tenant_id, namespace, name, version, created_at, and blocks. \
         Block types include step, parallel, race, loop, for_each, router, try_catch, \
         sub_sequence, ab_split, cancellation_scope, and saga. Never invent fields."
    );
    if let Some(catalog) = &catalog {
        let _ = write!(
            system,
            " Every step `handler` MUST be one of: {}. Do not use any other handler.",
            catalog.describe()
        );
    }
    let mut messages = vec![
        json!({"role": "system", "content": system}),
        json!({"role": "user", "content": request}),
    ];

    for attempt in 1..=cmd.attempts {
        let response = client
            .post(&cmd.llm_url)
            .bearer_auth(&cmd.llm_api_key)
            .json(&json!({
                "model": cmd.model,
                "messages": messages,
                "response_format": {"type": "json_object"},
            }))
            .send()
            .await
            .with_context(|| format!("calling LLM provider at {}", cmd.llm_url))?
            .error_for_status()?
            .json::<Value>()
            .await?;
        let content = response
            .pointer("/choices/0/message/content")
            .and_then(Value::as_str)
            .context("provider response has no choices[0].message.content")?;
        let mut value: Value = match serde_json::from_str(strip_fence(content)) {
            Ok(value) => value,
            Err(error) if attempt < cmd.attempts => {
                messages.push(json!({"role": "assistant", "content": content}));
                messages.push(json!({"role": "user", "content": format!(
                    "That was not JSON ({error}). Return one corrected JSON object only."
                )}));
                continue;
            }
            Err(error) => return Err(error).context("generated output is not JSON"),
        };
        add_authoring_defaults(&mut value, schema_url);
        let violations = catalog
            .as_ref()
            .map(|catalog| catalog_violations(catalog, &value))
            .unwrap_or_default();
        let decoded = if violations.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "handlers not in the vendor catalog: {}",
                violations.join(", ")
            ))
        };
        let decoded = decoded.and_then(|()| {
            orch8_types::sequence::deserialize_sequence_strict(&value)
                .map_err(|error| error.to_string())
                .and_then(|sequence| {
                    sequence
                        .validate()
                        .map(|()| sequence)
                        .map_err(|error| error.to_string())
                })
        });
        match decoded {
            Ok(_) => {
                // `--out flow.yaml` writes YAML; any other extension JSON.
                crate::seqdoc::write_document(&cmd.out, &value)?;
                println!("generated and validated {}", cmd.out.display());
                return Ok(());
            }
            Err(error) if attempt < cmd.attempts => {
                messages.push(json!({"role": "assistant", "content": content}));
                messages.push(json!({"role": "user", "content": format!(
                    "Strict Orch8 validation failed: {error}. Repair the JSON and return only the full object."
                )}));
            }
            Err(error) => bail!("generated sequence is invalid after {attempt} attempts: {error}"),
        }
    }
    unreachable!("attempt range is non-empty")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_restricts_handlers_and_pieces() {
        let catalog = Catalog::parse(&json!({
            "handlers": ["noop", "http_request"],
            "pieces": [
                {"name": "slack", "actions": ["send_message"]},
                {"name": "@activepieces/piece-gmail"},
                "hubspot"
            ]
        }))
        .unwrap();
        assert!(catalog.allows("noop"));
        assert!(catalog.allows("ap://slack.send_message"));
        assert!(!catalog.allows("ap://slack.delete_channel"));
        assert!(catalog.allows("ap://gmail.send_email"));
        assert!(catalog.allows("ap://hubspot.create_contact"));
        assert!(!catalog.allows("llm_call"));
        assert!(!catalog.allows("ap://stripe.charge"));
        assert!(catalog.describe().contains("ap://slack.send_message"));
        assert!(Catalog::parse(&json!({})).is_err());
    }

    #[test]
    fn violations_walk_nested_blocks_but_not_params() {
        let catalog = Catalog::parse(&json!({"handlers": ["noop"]})).unwrap();
        let sequence = json!({
            "blocks": [
                {"type": "step", "id": "a", "handler": "noop",
                 "params": {"handler": "ignored_inside_params"}},
                {"type": "parallel", "id": "p", "branches": [[
                    {"type": "step", "id": "b", "handler": "ap://stripe.charge",
                     "fallback_handler": "noop"}
                ]]}
            ]
        });
        assert_eq!(
            catalog_violations(&catalog, &sequence),
            vec!["ap://stripe.charge".to_owned()]
        );
    }

    #[test]
    fn strips_json_fences() {
        assert_eq!(strip_fence("```json\n{\"a\":1}\n```"), "{\"a\":1}");
    }

    #[test]
    fn authoring_defaults_fill_server_fields_without_overwriting_values() {
        let mut value = json!({"name": "demo", "tenant_id": "acme", "blocks": []});
        add_authoring_defaults(&mut value, "https://example/schema.json");
        assert_eq!(value["tenant_id"], "acme");
        assert_eq!(value["$schema"], "https://example/schema.json");
        assert_eq!(value["schema_version"], 1);
        assert!(uuid::Uuid::parse_str(value["id"].as_str().unwrap()).is_ok());
        assert!(value["created_at"].as_str().is_some());
    }
}
