//! Bounded terminal execution debugger.

use anyhow::{Context as _, Result, bail};
use clap::Subcommand;
use reqwest::Client;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{OutputFormat, format_table};

#[derive(Subcommand)]
pub enum DebugCmd {
    /// Show a bounded timeline with checkpoint and effect summaries.
    Open {
        instance_id: Uuid,
        #[arg(long, default_value = "100", value_parser = clap::value_parser!(u32).range(1..=500))]
        limit: u32,
    },
    /// Fork a dry-run sandbox from a selected block.
    Fork {
        instance_id: Uuid,
        #[arg(long)]
        from_block: String,
        /// Explicit acknowledgement required to allow live side effects.
        #[arg(long)]
        live_effects: bool,
    },
}

pub async fn run(client: &Client, base: &str, cmd: DebugCmd, format: OutputFormat) -> Result<()> {
    match cmd {
        DebugCmd::Open { instance_id, limit } => {
            let (timeline, checkpoints, effects) = tokio::try_join!(
                get_json(client, format!("{base}/instances/{instance_id}/timeline")),
                get_json(
                    client,
                    format!("{base}/instances/{instance_id}/checkpoints")
                ),
                get_json(client, format!("{base}/instances/{instance_id}/effects")),
            )?;
            let report = json!({
                "instance_id": instance_id,
                "timeline": bounded_items(&timeline, limit),
                "checkpoints": bounded_items(&checkpoints, limit),
                "effects": bounded_items(&effects, limit),
            });
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
                OutputFormat::Table => print_report(&report),
            }
        }
        DebugCmd::Fork {
            instance_id,
            from_block,
            live_effects,
        } => {
            if from_block.trim().is_empty() {
                bail!("--from-block must not be empty");
            }
            let response = client
                .post(format!("{base}/instances/{instance_id}/fork"))
                .json(&json!({
                    "from_block_id": from_block,
                    "dry_run": !live_effects,
                    "injected_signals": [],
                }))
                .send()
                .await?;
            if !response.status().is_success() {
                bail!("fork failed: {}", response.text().await?);
            }
            let body: Value = response.json().await?;
            println!("{}", serde_json::to_string_pretty(&body)?);
        }
    }
    Ok(())
}

async fn get_json(client: &Client, url: String) -> Result<Value> {
    let response = client.get(&url).send().await?;
    if !response.status().is_success() {
        bail!("debugger request {url} failed: {}", response.status());
    }
    response.json().await.context("decode debugger response")
}

fn bounded_items(value: &Value, limit: u32) -> Value {
    let limit = limit.min(500) as usize;
    match value {
        Value::Array(items) => Value::Array(items.iter().take(limit).cloned().collect()),
        Value::Object(object) => {
            for key in ["items", "entries", "checkpoints", "effects"] {
                if let Some(Value::Array(items)) = object.get(key) {
                    return Value::Array(items.iter().take(limit).cloned().collect());
                }
            }
            value.clone()
        }
        _ => value.clone(),
    }
}

fn print_report(report: &Value) {
    let mut rows = Vec::new();
    for section in ["timeline", "checkpoints", "effects"] {
        let count = report
            .get(section)
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        rows.push(vec![section.into(), count.to_string()]);
    }
    print!("{}", format_table(&["evidence", "count"], &rows));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_debug_evidence_never_exceeds_limit() {
        let value = Value::Array((0..1_000).map(Value::from).collect());
        assert_eq!(bounded_items(&value, 100).as_array().unwrap().len(), 100);
        assert_eq!(bounded_items(&value, 999).as_array().unwrap().len(), 500);
    }
}
