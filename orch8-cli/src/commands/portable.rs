//! Local-first adoption and conformance tooling for portable agents.

use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};
use chrono::{Duration, Utc};
use clap::{Subcommand, ValueEnum};
use orch8_types::continuity::{
    RuntimeCapabilities, RuntimeConnectivity, RuntimeId, RuntimeKind, RuntimeTrustLevel,
};
use orch8_types::continuity_product::{
    CURRENT_PROTOCOL, ConformanceCheckResult, GatewayAdapter, GatewayManifest, PortableWorkOffer,
    TrustBoundaryProfile, compile_placement_policy, score_conformance,
};
use orch8_types::worker::WorkerTask;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::{OutputFormat, atomic_write};

#[derive(Debug, Subcommand)]
pub enum PortableCmd {
    /// Print the versioned Durable Agent Handoff Protocol contract.
    Protocol,
    /// Compile a human-readable placement policy into core policy JSON.
    CompilePolicy {
        /// Semicolon-separated policy statements.
        source: String,
    },
    /// Wrap an existing HTTP, MCP, process, or mobile worker entrypoint.
    Wrap {
        name: String,
        #[arg(long, value_enum)]
        adapter: AdapterArg,
        #[arg(long)]
        entrypoint: String,
        /// Exact process argument; repeat for multiple arguments.
        #[arg(long = "arg")]
        arguments: Vec<String>,
        #[arg(long)]
        handler: String,
        #[arg(long, default_value = "classification=internal")]
        policy: String,
        /// Environment variable names that may be inherited (comma-separated).
        #[arg(long, value_delimiter = ',')]
        allow_env: Vec<String>,
        /// Secret references passed by name, never secret values (comma-separated).
        #[arg(long, value_delimiter = ',')]
        secret_ref: Vec<String>,
        /// Manifest destination (distinct from global `--output` format).
        #[arg(long, default_value = "orch8-portable.json")]
        manifest_output: PathBuf,
    },
    /// Run a wrapped local process as a capability-aware trusted desktop worker.
    Worker {
        manifest: PathBuf,
        /// Stable runtime UUID. Persist this value across restarts.
        #[arg(long)]
        runtime_id: uuid::Uuid,
        /// Poll once and exit, including when no work is available.
        #[arg(long)]
        once: bool,
        #[arg(long, default_value_t = 2_000)]
        poll_interval_ms: u64,
    },
    /// Print a production-ready trust-boundary profile.
    Profile {
        #[arg(value_enum)]
        profile: ProfileArg,
    },
    /// Score a JSON array of conformance check results.
    Score { results: PathBuf },
}

#[derive(Debug, Deserialize)]
struct WorkerPollResponse {
    tasks: Vec<WorkerTask>,
    poll_after_ms: u64,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum AdapterArg {
    Http,
    Mcp,
    Process,
    Mobile,
}

impl From<AdapterArg> for GatewayAdapter {
    fn from(value: AdapterArg) -> Self {
        match value {
            AdapterArg::Http => Self::GenericHttp,
            AdapterArg::Mcp => Self::Mcp,
            AdapterArg::Process => Self::LocalProcess,
            AdapterArg::Mobile => Self::MobileWorker,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ProfileArg {
    PrivateRag,
    BiometricApproval,
    DataResidency,
    BillOfExecution,
    AuditEvidence,
    SecretSafeCoding,
    RegulatedOnboarding,
    FraudChallenge,
    ExecutiveAirlock,
    PersonalDataVault,
}

impl From<ProfileArg> for TrustBoundaryProfile {
    fn from(value: ProfileArg) -> Self {
        match value {
            ProfileArg::PrivateRag => Self::PrivateRag,
            ProfileArg::BiometricApproval => Self::BiometricApproval,
            ProfileArg::DataResidency => Self::DataResidency,
            ProfileArg::BillOfExecution => Self::BillOfExecution,
            ProfileArg::AuditEvidence => Self::AuditEvidence,
            ProfileArg::SecretSafeCoding => Self::SecretSafeCoding,
            ProfileArg::RegulatedOnboarding => Self::RegulatedOnboarding,
            ProfileArg::FraudChallenge => Self::FraudChallenge,
            ProfileArg::ExecutiveAirlock => Self::ExecutiveAirlock,
            ProfileArg::PersonalDataVault => Self::PersonalDataVault,
        }
    }
}

#[derive(Debug, Serialize)]
struct ProtocolOutput {
    name: &'static str,
    version: orch8_types::continuity_product::ProtocolVersion,
    invariants: [&'static str; 7],
    adapters: [&'static str; 4],
}

pub async fn run(
    client: &Client,
    base: &str,
    command: PortableCmd,
    format: OutputFormat,
) -> Result<()> {
    match command {
        PortableCmd::Protocol => print_value(
            &ProtocolOutput {
                name: "Durable Agent Handoff Protocol",
                version: CURRENT_PROTOCOL,
                invariants: [
                    "atomic_ownership",
                    "monotonic_epoch",
                    "stale_owner_rejected",
                    "duplicate_effect_fenced",
                    "capability_policy_enforced",
                    "tenant_isolation",
                    "receipt_verifiable",
                ],
                adapters: ["http", "mcp", "local_process", "mobile_worker"],
            },
            format,
        ),
        PortableCmd::CompilePolicy { source } => {
            let compiled = compile_placement_policy(&source)?;
            print_value(&compiled, format)
        }
        PortableCmd::Wrap {
            name,
            adapter,
            entrypoint,
            arguments,
            handler,
            policy,
            allow_env,
            secret_ref,
            manifest_output,
        } => {
            let manifest = GatewayManifest {
                protocol: CURRENT_PROTOCOL,
                name,
                adapter: adapter.into(),
                entrypoint,
                arguments,
                handler,
                policy_source: policy,
                environment_allowlist: allow_env,
                secret_references: secret_ref,
                receipt_required: true,
            };
            manifest.validate()?;
            let bytes = serde_json::to_vec_pretty(&manifest)?;
            atomic_write(&manifest_output, &bytes)?;
            println!("wrote {}", manifest_output.display());
            Ok(())
        }
        PortableCmd::Worker {
            manifest,
            runtime_id,
            once,
            poll_interval_ms,
        } => run_worker(client, base, &manifest, runtime_id, once, poll_interval_ms).await,
        PortableCmd::Profile { profile } => {
            let profile: TrustBoundaryProfile = profile.into();
            profile.compile()?;
            print_value(&profile.contract(), format)
        }
        PortableCmd::Score { results } => {
            let bytes =
                std::fs::read(&results).with_context(|| format!("read {}", results.display()))?;
            let results: Vec<ConformanceCheckResult> = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse {}", results.display()))?;
            let score = score_conformance(&results);
            if !score.mandatory_failures.is_empty() {
                print_value(&score, format)?;
                bail!("mandatory continuity checks failed");
            }
            print_value(&score, format)
        }
    }
}

async fn run_worker(
    client: &Client,
    base: &str,
    manifest_path: &std::path::Path,
    runtime_id: uuid::Uuid,
    once: bool,
    poll_interval_ms: u64,
) -> Result<()> {
    let bytes = std::fs::read(manifest_path)
        .with_context(|| format!("read {}", manifest_path.display()))?;
    let manifest: GatewayManifest = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse {}", manifest_path.display()))?;
    let compiled = manifest.validate()?;
    if manifest.adapter == GatewayAdapter::MobileWorker {
        bail!("mobile-worker manifests run through a native SDK, not the desktop worker");
    }
    let runtime_id = RuntimeId::from_uuid(runtime_id);
    loop {
        let now = Utc::now();
        let capabilities = worker_capabilities(&manifest, &compiled, runtime_id, now);
        let response = client
            .post(format!("{base}/workers/tasks/poll"))
            .json(&serde_json::json!({
                "handler_name": &manifest.handler,
                "worker_id": runtime_id.to_string(),
                "limit": 1,
                "version": env!("CARGO_PKG_VERSION"),
                "capabilities": capabilities,
            }))
            .send()
            .await?;
        if !response.status().is_success() {
            bail!(
                "worker poll failed: {} {}",
                response.status(),
                response.text().await?
            );
        }
        let poll: WorkerPollResponse = response.json().await?;
        for task in poll.tasks {
            execute_task(client, base, &manifest, runtime_id, &task).await?;
        }
        if once {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(
            poll_interval_ms.max(poll.poll_after_ms).max(100),
        ))
        .await;
    }
}

fn worker_capabilities(
    manifest: &GatewayManifest,
    compiled: &orch8_types::continuity_product::CompiledPlacementPolicy,
    runtime_id: RuntimeId,
    now: chrono::DateTime<Utc>,
) -> RuntimeCapabilities {
    let rule = &compiled.policy.rules[0];
    RuntimeCapabilities {
        runtime_id,
        kind: RuntimeKind::Desktop,
        // A local manifest is not an attestation. The server's verified
        // registration flow is the only authority allowed to elevate trust.
        trust: RuntimeTrustLevel::Registered,
        handlers: vec![manifest.handler.clone()],
        plugins: compiled.requirements.plugins.clone(),
        credentials: compiled.requirements.credentials.clone(),
        regions: rule.allowed_regions.clone(),
        hardware: compiled.requirements.hardware.clone(),
        offline_capable: rule.require_offline.unwrap_or(false),
        connectivity: Some(RuntimeConnectivity::Ethernet),
        battery_percent: None,
        estimated_cost_microunits: None,
        estimated_latency_ms: None,
        draining: false,
        capsule_signing_public_key: None,
        observed_at: now,
        expires_at: now + Duration::minutes(4),
    }
}

async fn execute_task(
    client: &Client,
    base: &str,
    manifest: &GatewayManifest,
    runtime_id: RuntimeId,
    task: &WorkerTask,
) -> Result<()> {
    let outcome = match manifest.adapter {
        GatewayAdapter::LocalProcess => execute_local_process(manifest, task).await?,
        GatewayAdapter::Mcp => execute_mcp(client, manifest, task).await?,
        GatewayAdapter::GenericHttp => execute_http_gateway(client, manifest, task).await?,
        GatewayAdapter::MobileWorker => {
            bail!("mobile-worker manifests run through a native SDK")
        }
    };
    let (path, body) = match outcome {
        LocalProcessOutcome::Complete(output) => (
            "complete",
            serde_json::json!({"worker_id": runtime_id.to_string(), "claim_epoch": task.claim_epoch, "output": output}),
        ),
        LocalProcessOutcome::Fail { message, retryable } => (
            "fail",
            serde_json::json!({"worker_id": runtime_id.to_string(), "claim_epoch": task.claim_epoch, "message": message, "retryable": retryable}),
        ),
    };
    let response = client
        .post(format!("{base}/workers/tasks/{}/{path}", task.id))
        .json(&body)
        .send()
        .await?;
    if !response.status().is_success() {
        bail!(
            "worker {path} failed: {} {}",
            response.status(),
            response.text().await?
        );
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
enum LocalProcessOutcome {
    Complete(serde_json::Value),
    Fail { message: String, retryable: bool },
}

fn validate_gateway_url(entrypoint: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(entrypoint).context("gateway entrypoint must be a URL")?;
    let loopback = url
        .host_str()
        .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"));
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        bail!("gateway entrypoint must use HTTPS or loopback HTTP");
    }
    Ok(url)
}

fn mcp_call(task: &WorkerTask) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": task.id.to_string(),
        "method": "tools/call",
        "params": {
            "name": task.handler_name,
            "arguments": task.params,
            "_meta": {
                "orch8_instance_id": task.instance_id.to_string(),
                "orch8_claim_epoch": task.claim_epoch,
                "orch8_context": task.context,
            }
        }
    })
}

fn parse_mcp_output(value: &serde_json::Value) -> LocalProcessOutcome {
    if let Some(error) = value.get("error") {
        return LocalProcessOutcome::Fail {
            message: format!("MCP tool error: {error}"),
            retryable: false,
        };
    }
    match value.get("result") {
        Some(result) => LocalProcessOutcome::Complete(result.clone()),
        None => LocalProcessOutcome::Fail {
            message: "MCP response contains neither result nor error".into(),
            retryable: false,
        },
    }
}

async fn execute_mcp(
    client: &Client,
    manifest: &GatewayManifest,
    task: &WorkerTask,
) -> Result<LocalProcessOutcome> {
    let url = validate_gateway_url(&manifest.entrypoint)?;
    let response = client.post(url).json(&mcp_call(task)).send().await?;
    if !response.status().is_success() {
        return Ok(LocalProcessOutcome::Fail {
            message: format!("MCP gateway returned HTTP {}", response.status()),
            retryable: response.status().is_server_error(),
        });
    }
    if response
        .content_length()
        .is_some_and(|length| length > PortableWorkOffer::MAX_INPUT_BYTES as u64)
    {
        return Ok(LocalProcessOutcome::Fail {
            message: "MCP response exceeds 1 MiB".into(),
            retryable: false,
        });
    }
    let bytes = response.bytes().await?;
    if bytes.len() > PortableWorkOffer::MAX_INPUT_BYTES {
        return Ok(LocalProcessOutcome::Fail {
            message: "MCP response exceeds 1 MiB".into(),
            retryable: false,
        });
    }
    let value = serde_json::from_slice(&bytes).context("MCP response must be JSON-RPC JSON")?;
    Ok(parse_mcp_output(&value))
}

async fn execute_http_gateway(
    client: &Client,
    manifest: &GatewayManifest,
    task: &WorkerTask,
) -> Result<LocalProcessOutcome> {
    let url = validate_gateway_url(&manifest.entrypoint)?;
    let response = client
        .post(url)
        .json(&serde_json::json!({
            "protocol": CURRENT_PROTOCOL,
            "task_id": task.id,
            "instance_id": task.instance_id,
            "claim_epoch": task.claim_epoch,
            "handler": task.handler_name,
            "params": task.params,
            "context": task.context,
        }))
        .send()
        .await?;
    if !response.status().is_success() {
        return Ok(LocalProcessOutcome::Fail {
            message: format!("HTTP gateway returned {}", response.status()),
            retryable: response.status().is_server_error(),
        });
    }
    if response
        .content_length()
        .is_some_and(|length| length > PortableWorkOffer::MAX_INPUT_BYTES as u64)
    {
        return Ok(LocalProcessOutcome::Fail {
            message: "HTTP gateway response exceeds 1 MiB".into(),
            retryable: false,
        });
    }
    let bytes = response.bytes().await?;
    if bytes.len() > PortableWorkOffer::MAX_INPUT_BYTES {
        return Ok(LocalProcessOutcome::Fail {
            message: "HTTP gateway response exceeds 1 MiB".into(),
            retryable: false,
        });
    }
    let value = serde_json::from_slice(&bytes).context("HTTP gateway response must be JSON")?;
    Ok(LocalProcessOutcome::Complete(value))
}

async fn execute_local_process(
    manifest: &GatewayManifest,
    task: &WorkerTask,
) -> Result<LocalProcessOutcome> {
    use tokio::io::AsyncWriteExt as _;

    let mut command = tokio::process::Command::new(&manifest.entrypoint);
    command.args(&manifest.arguments);
    command.env_clear();
    for name in &manifest.environment_allowlist {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    command.stdin(std::process::Stdio::piped());
    command.stdout(std::process::Stdio::piped());
    command.stderr(std::process::Stdio::piped());
    command.kill_on_drop(true);
    let mut child = command
        .spawn()
        .with_context(|| format!("start {}", manifest.entrypoint))?;
    let input =
        serde_json::to_vec(&serde_json::json!({"params": task.params, "context": task.context}))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(&input).await?;
    }
    let timeout = std::time::Duration::from_millis(
        task.timeout_ms
            .and_then(|value| u64::try_from(value).ok())
            .unwrap_or(60_000)
            .min(3_600_000),
    );
    let result = tokio::time::timeout(timeout, child.wait_with_output()).await;
    let outcome = match result {
        Ok(Ok(output))
            if output.status.success()
                && output.stdout.len() <= PortableWorkOffer::MAX_INPUT_BYTES =>
        {
            match serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                Ok(parsed) => LocalProcessOutcome::Complete(parsed),
                Err(error) => LocalProcessOutcome::Fail {
                    message: format!("local worker stdout is not JSON: {error}"),
                    retryable: false,
                },
            }
        }
        Ok(Ok(output)) => {
            let message = if output.stdout.len() > PortableWorkOffer::MAX_INPUT_BYTES {
                "local worker output exceeds 1 MiB".to_string()
            } else {
                String::from_utf8_lossy(&output.stderr)
                    .chars()
                    .take(4_096)
                    .collect()
            };
            LocalProcessOutcome::Fail {
                message,
                retryable: false,
            }
        }
        Ok(Err(error)) => LocalProcessOutcome::Fail {
            message: error.to_string(),
            retryable: true,
        },
        Err(_) => LocalProcessOutcome::Fail {
            message: "local worker timed out".into(),
            retryable: true,
        },
    };
    Ok(outcome)
}

fn print_value(value: &impl Serialize, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Table => {
            println!("{}", serde_json::to_string_pretty(value)?);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_cli_profile_maps_to_a_valid_product_contract() {
        for profile in [
            ProfileArg::PrivateRag,
            ProfileArg::BiometricApproval,
            ProfileArg::DataResidency,
            ProfileArg::BillOfExecution,
            ProfileArg::AuditEvidence,
            ProfileArg::SecretSafeCoding,
            ProfileArg::RegulatedOnboarding,
            ProfileArg::FraudChallenge,
            ProfileArg::ExecutiveAirlock,
            ProfileArg::PersonalDataVault,
        ] {
            let profile: TrustBoundaryProfile = profile.into();
            assert!(profile.compile().is_ok());
        }
    }

    #[tokio::test]
    async fn wrap_writes_a_valid_mcp_manifest_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("portable.json");
        run(
            &Client::new(),
            "http://127.0.0.1:1",
            PortableCmd::Wrap {
                name: "existing-agent".into(),
                adapter: AdapterArg::Mcp,
                entrypoint: "mcp://local/tools".into(),
                arguments: vec![],
                handler: "tool.run".into(),
                policy: "classification=restricted;runtime_kinds=desktop;min_trust=signed".into(),
                allow_env: vec!["PATH".into()],
                secret_ref: vec!["vault://token".into()],
                manifest_output: output.clone(),
            },
            OutputFormat::Json,
        )
        .await
        .unwrap();
        let manifest: GatewayManifest =
            serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
        assert_eq!(manifest.adapter, GatewayAdapter::Mcp);
        assert!(manifest.receipt_required);
    }

    #[tokio::test]
    async fn score_command_fails_when_mandatory_check_fails() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("results.json");
        let result = ConformanceCheckResult {
            check: orch8_types::continuity_product::ConformanceCheck::AtomicOwnership,
            passed: false,
            evidence_sha256: "0".repeat(64),
            duration_ms: 1,
            finding: Some("two owners".into()),
        };
        std::fs::write(&path, serde_json::to_vec(&vec![result]).unwrap()).unwrap();
        assert!(
            run(
                &Client::new(),
                "http://127.0.0.1:1",
                PortableCmd::Score { results: path },
                OutputFormat::Json
            )
            .await
            .is_err()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_worker_executes_exact_entrypoint_and_parses_json_output() {
        let manifest = GatewayManifest {
            protocol: CURRENT_PROTOCOL,
            name: "local-worker".into(),
            adapter: GatewayAdapter::LocalProcess,
            entrypoint: "/bin/sh".into(),
            arguments: vec!["-c".into(), "printf '{\"ok\":true}'".into()],
            handler: "tool.run".into(),
            policy_source: "classification=internal;runtime_kinds=desktop;handlers=tool.run".into(),
            environment_allowlist: vec![],
            secret_references: vec![],
            receipt_required: true,
        };
        let now = Utc::now();
        let task = WorkerTask {
            id: uuid::Uuid::now_v7(),
            instance_id: orch8_types::ids::InstanceId::new(),
            block_id: orch8_types::ids::BlockId::new("local"),
            handler_name: "tool.run".into(),
            queue_name: None,
            requirements: orch8_types::continuity::CapsuleRequirements::default(),
            params: serde_json::json!({"input": 1}),
            context: serde_json::json!({}),
            attempt: 1,
            timeout_ms: Some(5_000),
            state: orch8_types::worker::WorkerTaskState::Claimed,
            worker_id: Some("worker".into()),
            claimed_at: Some(now),
            heartbeat_at: Some(now),
            claim_epoch: 1,
            resume_checkpoint: None,
            checkpoint_seq: 0,
            completed_at: None,
            output: None,
            error_message: None,
            error_retryable: None,
            created_at: now,
        };
        assert_eq!(
            execute_local_process(&manifest, &task).await.unwrap(),
            LocalProcessOutcome::Complete(serde_json::json!({"ok": true}))
        );

        let request = mcp_call(&task);
        assert_eq!(request["method"], "tools/call");
        assert_eq!(request["params"]["name"], "tool.run");
        assert_eq!(request["params"]["_meta"]["orch8_claim_epoch"], 1);
        assert_eq!(
            parse_mcp_output(
                &serde_json::json!({"jsonrpc":"2.0", "id": task.id, "result":{"ok":true}})
            ),
            LocalProcessOutcome::Complete(serde_json::json!({"ok": true}))
        );
        assert!(matches!(
            parse_mcp_output(
                &serde_json::json!({"jsonrpc":"2.0", "id": task.id, "error":{"code":-32000}})
            ),
            LocalProcessOutcome::Fail {
                retryable: false,
                ..
            }
        ));
    }

    #[test]
    fn local_manifest_cannot_self_assert_signed_or_attested_trust() {
        let manifest = GatewayManifest {
            protocol: CURRENT_PROTOCOL,
            name: "local-worker".into(),
            adapter: GatewayAdapter::LocalProcess,
            entrypoint: "/bin/echo".into(),
            arguments: vec![],
            handler: "tool.run".into(),
            policy_source:
                "classification=restricted;runtime_kinds=desktop;min_trust=attested;handlers=tool.run"
                    .into(),
            environment_allowlist: vec![],
            secret_references: vec![],
            receipt_required: true,
        };
        let compiled = manifest.validate().unwrap();
        let capabilities = worker_capabilities(&manifest, &compiled, RuntimeId::new(), Utc::now());
        assert_eq!(capabilities.trust, RuntimeTrustLevel::Registered);
        assert_eq!(
            compiled.requirements.minimum_trust,
            Some(RuntimeTrustLevel::Attested)
        );
    }
}
