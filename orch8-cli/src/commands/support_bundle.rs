use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;
use reqwest::Client;
use serde_json::{Map, Value, json};

use crate::atomic_write;

#[derive(Debug, Args)]
pub struct SupportBundleCmd {
    /// Destination JSON file (atomically replaced).
    #[arg(long = "out-file", default_value = "orch8-support-bundle.json")]
    pub output: PathBuf,
    /// Optional local typed config to include in redacted form.
    #[arg(long, default_value = "orch8.toml")]
    pub config: PathBuf,
    /// Optional instance whose read-only diagnosis should be included.
    #[arg(long)]
    pub instance: Option<uuid::Uuid>,
    /// Maximum number of context-free workload summaries.
    #[arg(long, default_value_t = 100)]
    pub max_instances: u32,
}

fn sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "context",
        "payload",
        "output",
        "params",
        "credential",
        "secret",
        "password",
        "token",
        "api_key",
        "encryption_key",
        "private_key",
        "database_url",
    ]
    .iter()
    .any(|needle| key.contains(needle))
}

fn sanitize(value: Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(key, value)| {
                    let value = if sensitive_key(&key) {
                        Value::String("[REDACTED]".into())
                    } else {
                        sanitize(value)
                    };
                    (key, value)
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(sanitize).collect()),
        other => other,
    }
}

fn workload_summaries(value: &Value, limit: usize) -> Vec<Value> {
    let values = value
        .get("items")
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
        .map(Vec::as_slice)
        .unwrap_or_default();
    values
        .iter()
        .take(limit)
        .filter_map(|instance| {
            let object = instance.as_object()?;
            let mut summary = Map::new();
            for key in [
                "id",
                "sequence_id",
                "state",
                "priority",
                "created_at",
                "updated_at",
                "next_fire_at",
            ] {
                if let Some(value) = object.get(key) {
                    summary.insert(key.into(), value.clone());
                }
            }
            Some(Value::Object(summary))
        })
        .collect()
}

async fn get_json(client: &Client, url: &str) -> Value {
    match client.get(url).send().await {
        Ok(response) => {
            let status = response.status();
            match response.json::<Value>().await {
                Ok(value) => json!({"status": status.as_u16(), "body": sanitize(value)}),
                Err(_) => json!({"status": status.as_u16()}),
            }
        }
        Err(error) => json!({"error": error.to_string()}),
    }
}

pub async fn run(client: &Client, base: &str, command: SupportBundleCmd) -> Result<()> {
    let origin = base.strip_suffix("/api/v1").unwrap_or(base);
    let config = if command.config.exists() {
        let contents = std::fs::read_to_string(&command.config)
            .with_context(|| format!("read {}", command.config.display()))?;
        let typed: orch8_types::config::EngineConfig = toml::from_str(&contents)
            .with_context(|| format!("parse {}", command.config.display()))?;
        sanitize(serde_json::to_value(typed)?)
    } else {
        Value::Null
    };
    let info = get_json(client, &format!("{origin}/info")).await;
    let live = get_json(client, &format!("{origin}/health/live")).await;
    let ready = get_json(client, &format!("{origin}/health/ready")).await;
    let instances = get_json(
        client,
        &format!(
            "{base}/instances?limit={}",
            command.max_instances.clamp(1, 1_000)
        ),
    )
    .await;
    let summaries = instances
        .get("body")
        .map(|value| {
            workload_summaries(
                value,
                usize::try_from(command.max_instances.clamp(1, 1_000)).unwrap_or(1_000),
            )
        })
        .unwrap_or_default();
    let diagnosis = if let Some(instance) = command.instance {
        get_json(client, &format!("{base}/instances/{instance}/diagnosis")).await
    } else {
        Value::Null
    };
    let bundle = json!({
        "bundle_schema": 1,
        "generated_at": chrono::Utc::now(),
        "cli_version": env!("CARGO_PKG_VERSION"),
        "config": config,
        "server_info": info,
        "health": {"live": live, "ready": ready},
        "workloads": summaries,
        "diagnosis": sanitize(diagnosis),
        "exclusions": ["secrets", "credentials", "contexts", "payloads", "params", "outputs"]
    });
    let bytes = serde_json::to_vec_pretty(&bundle)?;
    atomic_write(&command.output, &bytes)?;
    println!(
        "Wrote redacted support bundle to {}",
        command.output.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recursive_sanitizer_removes_execution_data_and_secrets() {
        let value = json!({
            "api_key": "top-secret",
            "nested": {"context": {"customer": "alice"}, "safe": 7},
            "events": [{"payload_json": "private", "state": "running"}]
        });
        let rendered = sanitize(value).to_string();
        assert!(!rendered.contains("top-secret"));
        assert!(!rendered.contains("alice"));
        assert!(!rendered.contains("private"));
        assert!(rendered.contains("running"));
    }

    #[test]
    fn workload_summary_is_allowlist_only() {
        let source = json!({"items": [{
            "id": "i1", "state": "running", "context": {"secret": true}, "metadata": {"pii": 1}
        }]});
        let rendered = serde_json::to_string(&workload_summaries(&source, 10)).unwrap();
        assert!(rendered.contains("running"));
        assert!(!rendered.contains("context"));
        assert!(!rendered.contains("metadata"));
    }
}

#[cfg(test)]
#[path = "support_bundle_coverage_tests.rs"]
mod support_bundle_coverage_tests;
