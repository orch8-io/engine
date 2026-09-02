use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use serde_json::{Value, json};

use crate::atomic_write;

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
    let system = format!(
        "You author Orch8 SequenceDefinition JSON. Return JSON only. Use schema {schema_url}. \
         Include id (UUID), tenant_id, namespace, name, version, created_at, and blocks. \
         Block types include step, parallel, race, loop, for_each, router, try_catch, \
         sub_sequence, ab_split, cancellation_scope, and saga. Never invent fields."
    );
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
        let decoded = orch8_types::sequence::deserialize_sequence_strict(&value)
            .map_err(|error| error.to_string())
            .and_then(|sequence| {
                sequence
                    .validate()
                    .map(|()| sequence)
                    .map_err(|error| error.to_string())
            });
        match decoded {
            Ok(_) => {
                atomic_write(
                    &cmd.out,
                    format!("{}\n", serde_json::to_string_pretty(&value)?).as_bytes(),
                )?;
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
