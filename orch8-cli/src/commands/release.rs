//! `orch8 release` — the safe workflow release control plane.

use anyhow::Result;
use clap::Subcommand;
use reqwest::Client;
use serde_json::Value;
use uuid::Uuid;

use crate::{OutputFormat, print_response};

#[derive(Subcommand)]
pub enum ReleaseCmd {
    /// Create a release candidate from two sequence versions.
    Create {
        #[arg(long)]
        tenant_id: String,
        #[arg(long)]
        baseline: Uuid,
        #[arg(long)]
        candidate: Uuid,
        /// Gate: max error-rate regression (e.g. 0.05) with --min-sample.
        #[arg(long)]
        max_error_regression: Option<f64>,
        #[arg(long, default_value = "20")]
        min_sample: u32,
    },
    /// List releases.
    List {
        #[arg(long)]
        tenant_id: Option<String>,
    },
    /// Show one release.
    Get { id: Uuid },
    /// Semantic diff of a release's baseline vs candidate.
    Diff { id: Uuid },
    /// Replay the candidate against real execution history (offline).
    Validate {
        id: Uuid,
        /// Number of historical runs to replay.
        #[arg(long)]
        sample: Option<u32>,
        /// Skip validation (audited).
        #[arg(long)]
        skip: bool,
    },
    /// Route a percentage of new instances to the candidate.
    Canary {
        id: Uuid,
        #[arg(long)]
        percent: u8,
    },
    /// Evaluate gates now (auto-rolls back on a failing gate).
    Evaluate { id: Uuid },
    /// Promote: all new instances run the candidate.
    Promote {
        id: Uuid,
        /// Promote even when gates are inconclusive/failing.
        #[arg(long)]
        force: bool,
    },
    /// Pause the canary (traffic returns to the baseline; resumable).
    Pause { id: Uuid },
    /// Roll back: all new traffic returns to the baseline (idempotent).
    Rollback { id: Uuid },
    /// Show the immutable decision audit trail.
    Decisions { id: Uuid },
}

#[allow(clippy::too_many_lines)]
pub async fn run(client: &Client, base: &str, cmd: ReleaseCmd, format: OutputFormat) -> Result<()> {
    match cmd {
        ReleaseCmd::Create {
            tenant_id,
            baseline,
            candidate,
            max_error_regression,
            min_sample,
        } => {
            let gates: Vec<Value> = max_error_regression
                .map(|max| {
                    vec![serde_json::json!({
                        "metric": "error_rate",
                        "max_regression": max,
                        "min_sample": min_sample,
                    })]
                })
                .unwrap_or_default();
            let resp = client
                .post(format!("{base}/releases"))
                .json(&serde_json::json!({
                    "tenant_id": tenant_id,
                    "baseline_sequence_id": baseline,
                    "candidate_sequence_id": candidate,
                    "gates": gates,
                }))
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        ReleaseCmd::List { tenant_id } => {
            let mut params: Vec<(&str, String)> = Vec::new();
            if let Some(t) = tenant_id {
                params.push(("tenant_id", t));
            }
            let resp = client
                .get(format!("{base}/releases"))
                .query(&params)
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        ReleaseCmd::Get { id } => {
            let resp = client.get(format!("{base}/releases/{id}")).send().await?;
            print_response(resp, format).await?;
        }
        ReleaseCmd::Diff { id } => {
            let resp = client
                .get(format!("{base}/releases/{id}/diff"))
                .send()
                .await?;
            if !resp.status().is_success() {
                anyhow::bail!("diff request failed: {}", resp.status());
            }
            let diff: Value = resp.json().await?;
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&diff)?),
                OutputFormat::Table => print_diff(&diff),
            }
        }
        ReleaseCmd::Validate { id, sample, skip } => {
            let resp = client
                .post(format!("{base}/releases/{id}/validate"))
                .json(&serde_json::json!({"sample": sample, "skip": skip}))
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        ReleaseCmd::Canary { id, percent } => {
            let resp = client
                .post(format!("{base}/releases/{id}/canary"))
                .json(&serde_json::json!({"percent": percent}))
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        ReleaseCmd::Evaluate { id } => {
            let resp = client
                .post(format!("{base}/releases/{id}/evaluate"))
                .json(&serde_json::json!({}))
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        ReleaseCmd::Promote { id, force } => {
            let resp = client
                .post(format!("{base}/releases/{id}/promote"))
                .json(&serde_json::json!({"force": force}))
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        ReleaseCmd::Pause { id } => {
            let resp = client
                .post(format!("{base}/releases/{id}/pause"))
                .json(&serde_json::json!({}))
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        ReleaseCmd::Rollback { id } => {
            let resp = client
                .post(format!("{base}/releases/{id}/rollback"))
                .json(&serde_json::json!({}))
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        ReleaseCmd::Decisions { id } => {
            let resp = client
                .get(format!("{base}/releases/{id}/decisions"))
                .send()
                .await?;
            print_response(resp, format).await?;
        }
    }
    Ok(())
}

fn print_diff(diff: &Value) {
    let entries = diff["entries"].as_array();
    if entries.is_none_or(Vec::is_empty) {
        println!("no semantic differences.");
    } else {
        for e in entries.into_iter().flatten() {
            println!(
                "  [{}] {}{}: {}",
                e["severity"].as_str().unwrap_or("?"),
                e["category"].as_str().unwrap_or("?"),
                e["block_id"]
                    .as_str()
                    .map(|b| format!(" ({b})"))
                    .unwrap_or_default(),
                e["summary"].as_str().unwrap_or(""),
            );
        }
    }
    for warning in diff["candidate_lint"].as_array().into_iter().flatten() {
        println!("  lint: {}", warning.as_str().unwrap_or(""));
    }
}
