//! `orch8 inspect template` — resolve a block's parameters against a
//! supplied context or an executed instance, showing value, type, source
//! path, fallback usage, and missing/null/error status. Read-only; the
//! handler is never invoked.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Subcommand;
use reqwest::Client;
use serde_json::Value;
use uuid::Uuid;

use crate::OutputFormat;

#[derive(Subcommand)]
pub enum InspectCmd {
    /// Trace template resolution for one block's params.
    Template {
        /// Block id to inspect.
        #[arg(long)]
        block: String,
        /// Executed instance to resolve against (uses its real context
        /// and recorded outputs).
        #[arg(long, conflicts_with_all = ["sequence_file", "context"])]
        instance: Option<Uuid>,
        /// Historical boundary: resolve as of just before this block
        /// (only with --instance).
        #[arg(long, requires = "instance")]
        at_block: Option<String>,
        /// Local draft sequence definition to inspect.
        #[arg(long)]
        sequence_file: Option<PathBuf>,
        /// JSON context data fixture (inline or @file), for --sequence-file.
        #[arg(long)]
        context: Option<String>,
        /// JSON outputs fixture `{block_id: output}` (inline or @file).
        #[arg(long)]
        outputs: Option<String>,
    },
}

fn parse_json_arg(s: &str) -> Result<Value> {
    let raw = if let Some(path) = s.strip_prefix('@') {
        std::fs::read_to_string(path).with_context(|| format!("failed to read {path}"))?
    } else {
        s.to_string()
    };
    serde_json::from_str(&raw).context("invalid JSON argument")
}

pub async fn run(client: &Client, base: &str, cmd: InspectCmd, format: OutputFormat) -> Result<()> {
    match cmd {
        InspectCmd::Template {
            block,
            instance,
            at_block,
            sequence_file,
            context,
            outputs,
        } => {
            let resp = if let Some(instance_id) = instance {
                let mut url =
                    format!("{base}/instances/{instance_id}/blocks/{block}/resolved-input");
                if let Some(at) = &at_block {
                    url.push_str(&format!("?at_block={at}"));
                }
                client.get(url).send().await?
            } else {
                let seq_path = sequence_file
                    .context("pass --instance <id> or --sequence-file <path>")?;
                let seq_raw = std::fs::read_to_string(&seq_path)
                    .with_context(|| format!("failed to read {}", seq_path.display()))?;
                let sequence: Value = serde_json::from_str(&seq_raw)
                    .with_context(|| format!("invalid JSON in {}", seq_path.display()))?;
                let body = serde_json::json!({
                    "sequence": sequence,
                    "block_id": block,
                    "context_data": context.as_deref().map(parse_json_arg).transpose()?
                        .unwrap_or_else(|| serde_json::json!({})),
                    "outputs": outputs.as_deref().map(parse_json_arg).transpose()?
                        .unwrap_or_else(|| serde_json::json!({})),
                });
                client
                    .post(format!("{base}/sequences/inspect-template"))
                    .json(&body)
                    .send()
                    .await?
            };

            if !resp.status().is_success() {
                anyhow::bail!("inspect request failed: {}", resp.status());
            }
            let trace: Value = resp.json().await?;
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&trace)?),
                OutputFormat::Table => print_trace(&trace),
            }
        }
    }
    Ok(())
}

fn print_trace(trace: &Value) {
    println!(
        "template resolution for block '{}'\n",
        trace["block_id"].as_str().unwrap_or("?")
    );
    let entries = trace["entries"].as_array();
    if entries.is_none_or(Vec::is_empty) {
        println!("  (no template expressions in this block's params)");
    }
    for e in entries.into_iter().flatten() {
        let status = e["status"].as_str().unwrap_or("?");
        println!(
            "  [{}] {} = {{{{ {} }}}}",
            status.to_uppercase(),
            e["param_path"].as_str().unwrap_or("?"),
            e["expression"].as_str().unwrap_or("?"),
        );
        if let Some(v) = e.get("value") {
            println!(
                "      value: {v}  (type: {})",
                e["result_type"].as_str().unwrap_or("?")
            );
        }
        if let Some(src) = e["source"].as_str() {
            let fb = if e["fallback_used"] == Value::Bool(true) {
                " — via fallback"
            } else {
                ""
            };
            println!("      source: {src}{fb}");
        }
        if e["coerced_to_string"] == Value::Bool(true) {
            println!("      note: non-string value interpolated into a string");
        }
        if let Some(err) = e["error"].as_str() {
            println!("      error: {err}");
        }
    }
    if let Some(resolved) = trace.get("resolved_params") {
        println!(
            "\nresolved params:\n{}",
            serde_json::to_string_pretty(resolved).unwrap_or_default()
        );
    }
}
