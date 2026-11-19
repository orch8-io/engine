//! Golden-path signed package deployment.

use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};
use clap::Args;
use orch8_types::release::{GateEvaluation, GateVerdict, ReleaseState};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::OutputFormat;

#[derive(Debug, Args)]
pub struct DeployCmd {
    /// Signed `.orch8pkg` to verify before touching the control plane.
    #[arg(long)]
    pub package: PathBuf,
    /// Signed archive member that must match the target environment's candidate.
    #[arg(long)]
    pub package_sequence: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    artifact_candidate_id: Option<uuid::Uuid>,
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

    let artifact_candidate_id = match &cmd.package_sequence {
        Some(path) => {
            Some(verify_candidate_artifact(client, base, cmd.release_id, &package, path).await?)
        }
        None => None,
    };

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
        let evaluation: CanaryEvaluation =
            serde_json::from_value(evaluation).context("invalid canary evaluation response")?;
        if evaluation.release_state != ReleaseState::Canary
            || evaluation.auto_rolled_back
            || evaluation
                .gates
                .iter()
                .any(|gate| gate.verdict == GateVerdict::Fail)
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
        artifact_candidate_id,
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

fn sequence_content(value: Value) -> Result<Value> {
    let sequence = orch8_types::sequence::deserialize_sequence_strict(
        &super::test_cmd::authoring_defaults(value),
    )?;
    sequence.validate()?;
    let mut content = serde_json::to_value(sequence)?;
    let record = content
        .as_object_mut()
        .context("sequence must be an object")?;
    for key in [
        "$schema",
        "id",
        "tenant_id",
        "namespace",
        "version",
        "created_at",
        "deprecated",
        "status",
    ] {
        record.remove(key);
    }
    Ok(content)
}

async fn verify_candidate_artifact(
    client: &Client,
    base: &str,
    release_id: Uuid,
    package: &orch8_publisher::package::SignedPackage,
    path: &str,
) -> Result<Uuid> {
    let source = package
        .archive
        .files
        .get(path)
        .with_context(|| format!("signed package does not contain '{path}'"))?;
    let signed = sequence_content(crate::seqdoc::parse_text(
        std::path::Path::new(path),
        source,
    )?)
    .context("signed archive member is not a valid sequence")?;
    let release = response_json(
        client
            .get(format!("{base}/releases/{release_id}"))
            .send()
            .await?,
    )
    .await?;
    let candidate_id: Uuid = serde_json::from_value(release["candidate_sequence_id"].clone())
        .context("release has no valid candidate sequence ID")?;
    let candidate = response_json(
        client
            .get(format!("{base}/sequences/{candidate_id}"))
            .send()
            .await?,
    )
    .await?;
    anyhow::ensure!(
        candidate["id"] == serde_json::json!(candidate_id),
        "candidate response has a different sequence ID"
    );
    anyhow::ensure!(
        sequence_content(candidate)? == signed,
        "target release candidate does not match the signed sequence '{path}'"
    );
    Ok(candidate_id)
}

/// Required fields from the API's `EvaluationReport`. Unknown additional fields
/// remain compatible, while incomplete observations cannot certify a canary.
#[derive(Deserialize)]
struct CanaryEvaluation {
    release_state: ReleaseState,
    auto_rolled_back: bool,
    gates: Vec<GateEvaluation>,
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

#[cfg(test)]
#[path = "deploy_flow_tests.rs"]
mod deploy_flow_tests;
