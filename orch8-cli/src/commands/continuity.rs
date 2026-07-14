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

#[derive(Subcommand)]
pub enum ExecutionCmd {
    /// Preview compatibility, policy, and effect risks before handoff.
    HandoffPreview {
        continuity_id: Uuid,
        #[arg(long)]
        tenant_id: String,
        #[arg(long)]
        to: Uuid,
        /// JSON file containing CapsuleRequirements.
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
}

#[derive(Subcommand)]
pub enum RuntimeCmd {
    /// Register or refresh a bounded runtime capability advertisement.
    Register {
        #[arg(long)]
        tenant_id: String,
        /// JSON file containing RuntimeCapabilities.
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
        Some(path) => serde_json::from_slice(&std::fs::read(path)?)
            .with_context(|| format!("parse JSON from {}", path.display())),
        None => Ok(json!({})),
    }
}

fn read_capsule(path: &Path) -> Result<SignedCapsuleManifest> {
    serde_json::from_slice(&std::fs::read(path)?)
        .with_context(|| format!("parse signed capsule from {}", path.display()))
}

async fn response_json(response: reqwest::Response, action: &str) -> Result<Value> {
    let status = response.status();
    let body: Value = response.json().await.context("decode server response")?;
    if !status.is_success() {
        bail!("{action} failed ({status}): {body}");
    }
    Ok(body)
}

fn print_json_value(value: &Value, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(value)?),
        OutputFormat::Table => println!("{}", serde_json::to_string_pretty(value)?),
    }
    Ok(())
}

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
