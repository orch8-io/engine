//! Golden-path signed package deployment.

use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};
use clap::Args;
use reqwest::Client;
use serde::Serialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::OutputFormat;

#[derive(Debug, Args)]
pub struct DeployCmd {
    /// Signed `.orch8pkg` to verify before touching the control plane.
    #[arg(long)]
    pub package: PathBuf,
    /// Existing release binding baseline and candidate sequence versions.
    #[arg(long)]
    pub release_id: Uuid,
    #[arg(long, default_value = "5", value_parser = clap::value_parser!(u8).range(1..=50))]
    pub canary_percent: u8,
    /// Bounded evaluation passes before promotion.
    #[arg(long, default_value = "1", value_parser = clap::value_parser!(u8).range(1..=20))]
    pub observations: u8,
    /// Promote after every validation and canary gate passes.
    #[arg(long)]
    pub promote: bool,
}

#[derive(Debug, Serialize)]
struct DeployEvidence {
    release_id: Uuid,
    package_hash: String,
    package_status: &'static str,
    semantic_diff_status: &'static str,
    historical_validation_status: &'static str,
    canary_percent: u8,
    evaluations: u8,
    promotion_status: &'static str,
}

pub async fn run(client: &Client, base: &str, cmd: DeployCmd, format: OutputFormat) -> Result<()> {
    let bytes = std::fs::read(&cmd.package)
        .with_context(|| format!("read signed package {}", cmd.package.display()))?;
    let package: orch8_publisher::package::SignedPackage =
        serde_json::from_slice(&bytes).context("decode signed package")?;
    orch8_publisher::package::verify_package(&package)
        .map_err(|error| anyhow::anyhow!("signed package verification failed: {error}"))?;

    crate::commands::release::check_gate(client, base, cmd.release_id, None, false, 0, 0).await?;
    post_success(
        client,
        format!("{base}/releases/{}/canary", cmd.release_id),
        json!({"percent": cmd.canary_percent}),
    )
    .await?;
    for _ in 0..cmd.observations {
        let evaluation = post_success(
            client,
            format!("{base}/releases/{}/evaluate", cmd.release_id),
            json!({}),
        )
        .await?;
        if evaluation
            .get("state")
            .and_then(Value::as_str)
            .is_some_and(|state| matches!(state, "rolled_back" | "failed"))
        {
            bail!("canary evaluation failed; release was not promoted");
        }
    }
    if cmd.promote {
        post_success(
            client,
            format!("{base}/releases/{}/promote", cmd.release_id),
            json!({"force": false}),
        )
        .await?;
    }
    let evidence = DeployEvidence {
        release_id: cmd.release_id,
        package_hash: package.content_hash,
        package_status: "verified",
        semantic_diff_status: "checked",
        historical_validation_status: "passed",
        canary_percent: cmd.canary_percent,
        evaluations: cmd.observations,
        promotion_status: if cmd.promote { "promoted" } else { "canary" },
    };
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&evidence)?),
        OutputFormat::Table => println!(
            "release {}: verified package, validated history, observed {} canary evaluation(s), status={}",
            evidence.release_id, evidence.evaluations, evidence.promotion_status
        ),
    }
    Ok(())
}

async fn post_success(client: &Client, url: String, body: Value) -> Result<Value> {
    response_json(client.post(url).json(&body).send().await?).await
}

async fn response_json(response: reqwest::Response) -> Result<Value> {
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        bail!("deploy gate failed ({status}): {text}");
    }
    serde_json::from_str(&text).context("decode deploy gate response")
}
