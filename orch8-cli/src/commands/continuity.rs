//! Portable execution continuity commands.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Subcommand;
use orch8_publisher::capsule::{
    SignedCapsuleManifest, check_capsule_key_trust, verify_signed_capsule,
};
use reqwest::Client;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{OutputFormat, print_response};

const MAX_CONTINUITY_REQUEST_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Subcommand)]
pub enum ExecutionCmd {
    /// Preview compatibility, policy, and effect risks before handoff.
    HandoffPreview {
        continuity_id: Uuid,
        #[arg(long)]
        tenant_id: String,
        #[arg(long)]
        to: Uuid,
        /// JSON file containing `CapsuleRequirements`.
        #[arg(long)]
        requirements: Option<PathBuf>,
    },
    /// Preview and create a preview-bound handoff request.
    Handoff {
        continuity_id: Uuid,
        #[arg(long)]
        tenant_id: String,
        #[arg(long)]
        to: Uuid,
        #[arg(long)]
        requirements: Option<PathBuf>,
    },
    /// Export a paused/waiting requested handoff to an encrypted capsule.
    Export {
        handoff_id: Uuid,
        #[arg(long)]
        tenant_id: String,
        #[arg(long)]
        requirements: Option<PathBuf>,
        #[arg(long, default_value = "300")]
        expires_in_seconds: u32,
        /// Optional output file for the signed manifest envelope.
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Verify a signed capsule manifest entirely offline.
    Verify {
        file: PathBuf,
        #[arg(long = "trusted-key")]
        trusted_keys: Vec<String>,
    },
    /// Import a verified capsule into paused quarantine.
    Import {
        file: PathBuf,
        #[arg(long)]
        tenant_id: String,
        #[arg(long)]
        to: Uuid,
        #[arg(long)]
        expected_epoch: u64,
    },
    /// Accept an exported handoff using an imported paused instance.
    Accept {
        handoff_id: Uuid,
        #[arg(long)]
        tenant_id: String,
        #[arg(long)]
        destination_instance_id: Uuid,
    },
    /// Resume an accepted handoff.
    Resume {
        handoff_id: Uuid,
        #[arg(long)]
        tenant_id: String,
    },
    /// Revoke an exported handoff.
    Revoke {
        handoff_id: Uuid,
        #[arg(long)]
        tenant_id: String,
    },
    /// List effect receipts for a continuity identity.
    Effects {
        continuity_id: Uuid,
        #[arg(long)]
        tenant_id: String,
    },
    /// List or verify the provenance chain.
    Provenance {
        continuity_id: Uuid,
        #[arg(long)]
        tenant_id: String,
        #[arg(long)]
        verify: bool,
        #[arg(long)]
        expected_head: Option<String>,
    },
    /// List checkpoint boundaries for time-travel inspection.
    Checkpoints {
        continuity_id: Uuid,
        #[arg(long)]
        tenant_id: String,
    },
    /// Run an effect-free what-if scenario from a checkpoint request file.
    WhatIf {
        continuity_id: Uuid,
        request: PathBuf,
    },
    /// Extract a sanitized regression fixture from production evidence.
    ExtractTest {
        continuity_id: Uuid,
        request: PathBuf,
    },
    /// Evaluate configured invariants for an execution.
    EvaluateInvariants {
        continuity_id: Uuid,
        #[arg(long)]
        tenant_id: String,
    },
    /// Create a sequence invariant from a JSON request.
    CreateInvariant { request: PathBuf },
    /// Reserve multidimensional execution budget from a JSON request.
    ReserveBudget {
        continuity_id: Uuid,
        request: PathBuf,
    },
    /// List durable evaluation evidence.
    Evaluations {
        continuity_id: Uuid,
        #[arg(long)]
        tenant_id: String,
    },
    /// Append durable evaluation evidence from a JSON request.
    AppendEvaluation {
        continuity_id: Uuid,
        request: PathBuf,
    },
    /// Compile a live-migration plan from a JSON request.
    MigrationPlan { request: PathBuf },
    /// Inspect a durable live-migration plan and rollback window.
    MigrationGet {
        migration_id: Uuid,
        #[arg(long)]
        tenant_id: String,
    },
    /// Apply a live-migration plan from an approval JSON request.
    MigrationApply {
        migration_id: Uuid,
        request: PathBuf,
    },
    /// Roll back an applied migration before target-version effects commit.
    MigrationRollback {
        migration_id: Uuid,
        request: PathBuf,
    },
    /// Preview receipt-derived compensation ordering and hazards.
    CompensationPreview {
        continuity_id: Uuid,
        request: PathBuf,
    },
    /// Create a durable compensation run from committed effect receipts.
    CompensationCreate {
        continuity_id: Uuid,
        request: PathBuf,
    },
    /// Inspect a durable compensation run.
    CompensationGet {
        run_id: Uuid,
        #[arg(long)]
        tenant_id: String,
    },
    /// Claim the next ordered compensation step.
    CompensationClaim { run_id: Uuid, request: PathBuf },
    /// Complete a claimed compensation step.
    CompensationComplete {
        run_id: Uuid,
        effect_id: Uuid,
        request: PathBuf,
    },
    /// Record a failed or outcome-unknown compensation attempt.
    CompensationFail {
        run_id: Uuid,
        effect_id: Uuid,
        request: PathBuf,
    },
    /// Verify a manual or uncertain compensation outcome.
    CompensationVerify {
        run_id: Uuid,
        effect_id: Uuid,
        request: PathBuf,
    },
    /// Generate bounded fault/state-space scenarios from a JSON request.
    GenerateScenarios { request: PathBuf },
    /// Minimize an incident reproduction from a JSON request.
    ReproduceIncident { request: PathBuf },
    /// Select a provider under quality, locality, latency, and cost policy.
    ChooseProvider { request: PathBuf },
    /// Generate evidence-backed workflow optimization recommendations.
    RecommendOptimizations { request: PathBuf },
    /// Evaluate a continuous-quality promotion gate.
    EvaluationGate { request: PathBuf },
    /// Produce fail-closed residency evidence.
    EvaluateResidency { request: PathBuf },
    /// Minimize confidential disclosure according to an allowlist.
    MinimizeDisclosure { request: PathBuf },
    /// Verify a signed federation envelope.
    VerifyFederation { request: PathBuf },
    /// Claim a destination-bound device delegation.
    ClaimDelegation { request: PathBuf },
    /// Create a durable human-attention task.
    CreateAttention { request: PathBuf },
    /// Assign an attention task using capability-aware scheduling.
    AssignAttention { task_id: Uuid, request: PathBuf },
}

#[derive(Subcommand)]
pub enum RuntimeCmd {
    /// Register or refresh a bounded runtime capability advertisement.
    Register {
        #[arg(long)]
        tenant_id: String,
        /// JSON file containing `RuntimeCapabilities`.
        capabilities: PathBuf,
    },
    /// List active runtime capability advertisements.
    List {
        #[arg(long)]
        tenant_id: String,
    },
}

fn read_json(path: Option<&Path>) -> Result<Value> {
    match path {
        Some(path) => serde_json::from_slice(&read_bounded_file(path)?)
            .with_context(|| format!("parse JSON from {}", path.display())),
        None => Ok(json!({})),
    }
}

fn read_capsule(path: &Path) -> Result<SignedCapsuleManifest> {
    serde_json::from_slice(&read_bounded_file(path)?)
        .with_context(|| format!("parse signed capsule from {}", path.display()))
}

fn read_bounded_file(path: &Path) -> Result<Vec<u8>> {
    let metadata =
        std::fs::metadata(path).with_context(|| format!("inspect {}", path.display()))?;
    if metadata.len() > MAX_CONTINUITY_REQUEST_BYTES {
        bail!(
            "{} exceeds the {} MiB continuity request limit",
            path.display(),
            MAX_CONTINUITY_REQUEST_BYTES / (1024 * 1024)
        );
    }
    std::fs::read(path).with_context(|| format!("read {}", path.display()))
}

async fn response_json(response: reqwest::Response, action: &str) -> Result<Value> {
    let status = response.status();
    let body: Value = response.json().await.context("decode server response")?;
    if !status.is_success() {
        bail!("{action} failed ({status}): {body}");
    }
    Ok(body)
}

fn print_json_value(value: &Value, _format: OutputFormat) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

async fn post_json_file(
    client: &Client,
    url: String,
    request: &Path,
    format: OutputFormat,
) -> Result<()> {
    let response = client
        .post(url)
        .json(&read_json(Some(request))?)
        .send()
        .await?;
    print_response(response, format).await
}

#[allow(clippy::too_many_lines)] // CLI dispatch keeps command-to-endpoint mappings auditable
pub async fn run_execution(
    client: &Client,
    base: &str,
    cmd: ExecutionCmd,
    format: OutputFormat,
) -> Result<()> {
    match cmd {
        ExecutionCmd::HandoffPreview {
            continuity_id,
            tenant_id,
            to,
            requirements,
        } => {
            let response = client
                .post(format!(
                    "{base}/continuity/executions/{continuity_id}/handoff-preview"
                ))
                .json(&json!({
                    "tenant_id": tenant_id,
                    "destination_runtime_id": to,
                    "requirements": read_json(requirements.as_deref())?,
                }))
                .send()
                .await?;
            print_response(response, format).await
        }
        ExecutionCmd::Handoff {
            continuity_id,
            tenant_id,
            to,
            requirements,
        } => {
            let requirements = read_json(requirements.as_deref())?;
            let preview = response_json(
                client
                    .post(format!(
                        "{base}/continuity/executions/{continuity_id}/handoff-preview"
                    ))
                    .json(&json!({
                        "tenant_id": tenant_id,
                        "destination_runtime_id": to,
                        "requirements": requirements,
                    }))
                    .send()
                    .await?,
                "handoff preview",
            )
            .await?;
            if preview.get("compatible").and_then(Value::as_bool) != Some(true) {
                bail!("handoff preview is not compatible: {preview}");
            }
            let response = client
                .post(format!("{base}/continuity/handoffs"))
                .json(&json!({
                    "tenant_id": tenant_id,
                    "continuity_id": continuity_id,
                    "destination_runtime_id": to,
                    "requirements": requirements,
                    "preview_sha256": preview["preview_sha256"],
                }))
                .send()
                .await?;
            print_response(response, format).await
        }
        ExecutionCmd::Export {
            handoff_id,
            tenant_id,
            requirements,
            expires_in_seconds,
            output,
        } => {
            let value = response_json(
                client
                    .post(format!("{base}/continuity/handoffs/{handoff_id}/export"))
                    .json(&json!({
                        "tenant_id": tenant_id,
                        "requirements": read_json(requirements.as_deref())?,
                        "expires_in_seconds": expires_in_seconds,
                    }))
                    .send()
                    .await?,
                "capsule export",
            )
            .await?;
            if let Some(path) = output {
                std::fs::write(&path, serde_json::to_vec_pretty(&value["capsule"])?)
                    .with_context(|| format!("write {}", path.display()))?;
            }
            print_json_value(&value, format)
        }
        ExecutionCmd::Verify { file, trusted_keys } => {
            let capsule = read_capsule(&file)?;
            verify_signed_capsule(&capsule)?;
            let trusted = if trusted_keys.is_empty() {
                false
            } else {
                check_capsule_key_trust(&capsule, &trusted_keys)?;
                true
            };
            print_json_value(
                &json!({
                    "valid": true,
                    "trusted": trusted,
                    "manifest_sha256": capsule.manifest_sha256,
                    "manifest": capsule.manifest,
                }),
                format,
            )
        }
        ExecutionCmd::Import {
            file,
            tenant_id,
            to,
            expected_epoch,
        } => {
            let capsule = read_capsule(&file)?;
            let response = client
                .post(format!("{base}/continuity/capsules/import"))
                .json(&json!({
                    "tenant_id": tenant_id,
                    "destination_runtime_id": to,
                    "expected_epoch": expected_epoch,
                    "capsule": capsule,
                }))
                .send()
                .await?;
            print_response(response, format).await
        }
        ExecutionCmd::Accept {
            handoff_id,
            tenant_id,
            destination_instance_id,
        } => {
            let response = client
                .post(format!("{base}/continuity/handoffs/{handoff_id}/accept"))
                .json(&json!({
                    "tenant_id": tenant_id,
                    "destination_instance_id": destination_instance_id,
                }))
                .send()
                .await?;
            print_response(response, format).await
        }
        ExecutionCmd::Resume {
            handoff_id,
            tenant_id,
        } => {
            let response = client
                .post(format!("{base}/continuity/handoffs/{handoff_id}/resume"))
                .json(&json!({"tenant_id": tenant_id}))
                .send()
                .await?;
            print_response(response, format).await
        }
        ExecutionCmd::Revoke {
            handoff_id,
            tenant_id,
        } => {
            let response = client
                .post(format!("{base}/continuity/handoffs/{handoff_id}/revoke"))
                .json(&json!({"tenant_id": tenant_id}))
                .send()
                .await?;
            print_response(response, format).await
        }
        ExecutionCmd::Effects {
            continuity_id,
            tenant_id,
        } => {
            let response = client
                .get(format!(
                    "{base}/continuity/executions/{continuity_id}/effects"
                ))
                .query(&[("tenant_id", tenant_id)])
                .send()
                .await?;
            print_response(response, format).await
        }
        ExecutionCmd::Provenance {
            continuity_id,
            tenant_id,
            verify,
            expected_head,
        } => {
            let suffix = if verify { "/verify" } else { "" };
            let mut query = vec![("tenant_id", tenant_id)];
            if let Some(head) = expected_head {
                query.push(("expected_head", head));
            }
            let response = client
                .get(format!(
                    "{base}/continuity/executions/{continuity_id}/provenance{suffix}"
                ))
                .query(&query)
                .send()
                .await?;
            print_response(response, format).await
        }
        ExecutionCmd::Checkpoints {
            continuity_id,
            tenant_id,
        } => {
            let response = client
                .get(format!(
                    "{base}/continuity/executions/{continuity_id}/checkpoints"
                ))
                .query(&[("tenant_id", tenant_id)])
                .send()
                .await?;
            print_response(response, format).await
        }
        ExecutionCmd::WhatIf {
            continuity_id,
            request,
        } => {
            post_json_file(
                client,
                format!("{base}/continuity/executions/{continuity_id}/what-if"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::ExtractTest {
            continuity_id,
            request,
        } => {
            post_json_file(
                client,
                format!("{base}/continuity/executions/{continuity_id}/test-fixture"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::EvaluateInvariants {
            continuity_id,
            tenant_id,
        } => {
            let response = client
                .post(format!(
                    "{base}/continuity/executions/{continuity_id}/invariants/evaluate"
                ))
                .json(&json!({"tenant_id": tenant_id}))
                .send()
                .await?;
            print_response(response, format).await
        }
        ExecutionCmd::CreateInvariant { request } => {
            post_json_file(
                client,
                format!("{base}/continuity/invariants"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::ReserveBudget {
            continuity_id,
            request,
        } => {
            post_json_file(
                client,
                format!("{base}/continuity/executions/{continuity_id}/budget-reservations"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::Evaluations {
            continuity_id,
            tenant_id,
        } => {
            let response = client
                .get(format!(
                    "{base}/continuity/executions/{continuity_id}/evaluations"
                ))
                .query(&[("tenant_id", tenant_id)])
                .send()
                .await?;
            print_response(response, format).await
        }
        ExecutionCmd::AppendEvaluation {
            continuity_id,
            request,
        } => {
            post_json_file(
                client,
                format!("{base}/continuity/executions/{continuity_id}/evaluations"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::MigrationPlan { request } => {
            post_json_file(
                client,
                format!("{base}/continuity/migrations/plan"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::MigrationGet {
            migration_id,
            tenant_id,
        } => {
            let response = client
                .get(format!("{base}/continuity/migrations/{migration_id}"))
                .query(&[("tenant_id", tenant_id)])
                .send()
                .await?;
            print_response(response, format).await
        }
        ExecutionCmd::MigrationApply {
            migration_id,
            request,
        } => {
            post_json_file(
                client,
                format!("{base}/continuity/migrations/{migration_id}/apply"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::MigrationRollback {
            migration_id,
            request,
        } => {
            post_json_file(
                client,
                format!("{base}/continuity/migrations/{migration_id}/rollback"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::CompensationPreview {
            continuity_id,
            request,
        } => {
            post_json_file(
                client,
                format!("{base}/continuity/executions/{continuity_id}/compensations/preview"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::CompensationCreate {
            continuity_id,
            request,
        } => {
            post_json_file(
                client,
                format!("{base}/continuity/executions/{continuity_id}/compensations"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::CompensationGet { run_id, tenant_id } => {
            let response = client
                .get(format!("{base}/continuity/compensations/{run_id}"))
                .query(&[("tenant_id", tenant_id)])
                .send()
                .await?;
            print_response(response, format).await
        }
        ExecutionCmd::CompensationClaim { run_id, request } => {
            post_json_file(
                client,
                format!("{base}/continuity/compensations/{run_id}/claim"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::CompensationComplete {
            run_id,
            effect_id,
            request,
        } => {
            post_json_file(
                client,
                format!("{base}/continuity/compensations/{run_id}/steps/{effect_id}/complete"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::CompensationFail {
            run_id,
            effect_id,
            request,
        } => {
            post_json_file(
                client,
                format!("{base}/continuity/compensations/{run_id}/steps/{effect_id}/fail"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::CompensationVerify {
            run_id,
            effect_id,
            request,
        } => {
            post_json_file(
                client,
                format!("{base}/continuity/compensations/{run_id}/steps/{effect_id}/verify"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::GenerateScenarios { request } => {
            post_json_file(
                client,
                format!("{base}/continuity/scenarios/generate"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::ReproduceIncident { request } => {
            post_json_file(
                client,
                format!("{base}/continuity/scenarios/reproduce"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::ChooseProvider { request } => {
            post_json_file(
                client,
                format!("{base}/continuity/providers/choose"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::RecommendOptimizations { request } => {
            post_json_file(
                client,
                format!("{base}/continuity/optimizations/recommend"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::EvaluationGate { request } => {
            post_json_file(
                client,
                format!("{base}/continuity/evaluations/gate"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::EvaluateResidency { request } => {
            post_json_file(
                client,
                format!("{base}/continuity/residency/evaluate"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::MinimizeDisclosure { request } => {
            post_json_file(
                client,
                format!("{base}/continuity/disclosure/minimize"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::VerifyFederation { request } => {
            post_json_file(
                client,
                format!("{base}/continuity/federation/verify"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::ClaimDelegation { request } => {
            post_json_file(
                client,
                format!("{base}/continuity/delegations/claim"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::CreateAttention { request } => {
            post_json_file(
                client,
                format!("{base}/continuity/attention"),
                &request,
                format,
            )
            .await
        }
        ExecutionCmd::AssignAttention { task_id, request } => {
            post_json_file(
                client,
                format!("{base}/continuity/attention/{task_id}/assign"),
                &request,
                format,
            )
            .await
        }
    }
}

pub async fn run_runtime(
    client: &Client,
    base: &str,
    cmd: RuntimeCmd,
    format: OutputFormat,
) -> Result<()> {
    match cmd {
        RuntimeCmd::Register {
            tenant_id,
            capabilities,
        } => {
            let response = client
                .post(format!("{base}/runtimes/register"))
                .json(&json!({
                    "tenant_id": tenant_id,
                    "capabilities": read_json(Some(&capabilities))?,
                }))
                .send()
                .await?;
            print_response(response, format).await
        }
        RuntimeCmd::List { tenant_id } => {
            let response = client
                .get(format!("{base}/runtimes"))
                .query(&[("tenant_id", tenant_id)])
                .send()
                .await?;
            print_response(response, format).await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    #[test]
    fn continuity_request_files_are_size_bounded() {
        let file = tempfile::NamedTempFile::new().unwrap();
        file.as_file()
            .set_len(MAX_CONTINUITY_REQUEST_BYTES + 1)
            .unwrap();

        let error = read_bounded_file(file.path()).unwrap_err();
        assert!(error.to_string().contains("continuity request limit"));
    }

    #[test]
    fn bounded_json_files_are_decoded() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(br#"{"tenant_id":"tenant-a"}"#).unwrap();

        assert_eq!(
            read_json(Some(file.path())).unwrap(),
            json!({"tenant_id": "tenant-a"})
        );
    }
}
