//! `orch8 release` — the safe workflow release control plane.

use anyhow::{Context, Result, bail};
use clap::Subcommand;
use reqwest::Client;
use serde::Serialize;
use serde_json::{Value, json};
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
    /// Run the non-interactive release proof gate used by CI.
    Gate {
        id: Uuid,
        /// Number of baseline executions to replay when validation has not run.
        #[arg(long)]
        sample: Option<u32>,
        /// Permit semantic diff entries classified as side-effect risk.
        #[arg(long)]
        allow_side_effect_risk: bool,
        /// Maximum historical replay divergences accepted by the gate.
        #[arg(long, default_value = "0")]
        max_divergences: u32,
        /// Maximum inconclusive historical replays accepted by the gate.
        #[arg(long, default_value = "0")]
        max_inconclusive: u32,
    },
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
        ReleaseCmd::Gate {
            id,
            sample,
            allow_side_effect_risk,
            max_divergences,
            max_inconclusive,
        } => {
            run_gate(
                client,
                base,
                id,
                sample,
                allow_side_effect_risk,
                max_divergences,
                max_inconclusive,
                format,
            )
            .await?;
        }
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct GateCheck {
    name: &'static str,
    passed: bool,
    evidence: String,
}

#[derive(Debug, Serialize)]
struct GateReport {
    release_id: Uuid,
    passed: bool,
    checks: Vec<GateCheck>,
}

#[allow(clippy::too_many_arguments)]
async fn run_gate(
    client: &Client,
    base: &str,
    id: Uuid,
    sample: Option<u32>,
    allow_side_effect_risk: bool,
    max_divergences: u32,
    max_inconclusive: u32,
    format: OutputFormat,
) -> Result<()> {
    let release = get_json(client, format!("{base}/releases/{id}"), "release").await?;
    let candidate = release["candidate_sequence_id"]
        .as_str()
        .context("release response is missing candidate_sequence_id")?;
    let (diff, preflight) = tokio::try_join!(
        get_json(
            client,
            format!("{base}/releases/{id}/diff"),
            "semantic diff"
        ),
        get_json(
            client,
            format!("{base}/sequences/{candidate}/preflight"),
            "candidate preflight"
        )
    )?;
    let validation = if release["state"] == "draft" {
        post_json(
            client,
            format!("{base}/releases/{id}/validate"),
            json!({"sample": sample, "skip": false}),
            "historical validation",
        )
        .await?
    } else {
        release["validation_summary"].clone()
    };

    let report = evaluate_gate(
        id,
        &diff,
        &preflight,
        &validation,
        allow_side_effect_risk,
        max_divergences,
        max_inconclusive,
    );
    print_gate_report(&report, format)?;
    if !report.passed {
        bail!("release proof gate failed");
    }
    Ok(())
}

async fn get_json(client: &Client, url: String, label: &str) -> Result<Value> {
    let response = client.get(url).send().await?;
    response_json(response, label).await
}

async fn post_json(client: &Client, url: String, body: Value, label: &str) -> Result<Value> {
    let response = client.post(url).json(&body).send().await?;
    response_json(response, label).await
}

async fn response_json(response: reqwest::Response, label: &str) -> Result<Value> {
    let status = response.status();
    let bytes = response.bytes().await?;
    if !status.is_success() {
        bail!(
            "{label} request failed ({status}): {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    serde_json::from_slice(&bytes).with_context(|| format!("{label} returned invalid JSON"))
}

fn evaluate_gate(
    release_id: Uuid,
    diff: &Value,
    preflight: &Value,
    validation: &Value,
    allow_side_effect_risk: bool,
    max_divergences: u32,
    max_inconclusive: u32,
) -> GateReport {
    let severity = diff["max_severity"].as_str().unwrap_or_else(|| {
        if diff["entries"].as_array().is_none_or(Vec::is_empty) {
            "none"
        } else {
            "unknown"
        }
    });
    let diff_passed = match severity {
        "none" | "informational" | "behavioral" => true,
        "side_effect_risk" => allow_side_effect_risk,
        _ => false,
    };
    let preflight_status = preflight["overall"].as_str().unwrap_or("unknown");
    let preflight_passed = matches!(preflight_status, "pass" | "warning");
    let divergences = validation["divergences"]
        .as_array()
        .map_or(u32::MAX, |values| {
            u32::try_from(values.len()).unwrap_or(u32::MAX)
        });
    let inconclusive = validation["inconclusive"]
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or(u32::MAX);
    let validation_passed = divergences <= max_divergences && inconclusive <= max_inconclusive;
    let checks = vec![
        GateCheck {
            name: "semantic_diff",
            passed: diff_passed,
            evidence: format!("max severity: {severity}"),
        },
        GateCheck {
            name: "candidate_preflight",
            passed: preflight_passed,
            evidence: format!("overall: {preflight_status}"),
        },
        GateCheck {
            name: "historical_validation",
            passed: validation_passed,
            evidence: format!(
                "divergences: {divergences}/{max_divergences}, inconclusive: {inconclusive}/{max_inconclusive}"
            ),
        },
    ];
    GateReport {
        release_id,
        passed: checks.iter().all(|check| check.passed),
        checks,
    }
}

fn print_gate_report(report: &GateReport, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(report)?),
        OutputFormat::Table => {
            for check in &report.checks {
                println!(
                    "{} {:<24} {}",
                    if check.passed { "PASS" } else { "FAIL" },
                    check.name,
                    check.evidence
                );
            }
            println!("gate: {}", if report.passed { "PASS" } else { "FAIL" });
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_gate_accepts_clean_evidence() {
        let report = evaluate_gate(
            Uuid::nil(),
            &json!({"max_severity": "behavioral"}),
            &json!({"overall": "warning"}),
            &json!({"divergences": [], "inconclusive": 0}),
            false,
            0,
            0,
        );
        assert!(report.passed);
    }

    #[test]
    fn strict_gate_rejects_each_unsafe_proof() {
        let report = evaluate_gate(
            Uuid::nil(),
            &json!({"max_severity": "side_effect_risk"}),
            &json!({"overall": "fail"}),
            &json!({"divergences": [{}], "inconclusive": 1}),
            false,
            0,
            0,
        );
        assert!(!report.passed);
        assert!(report.checks.iter().all(|check| !check.passed));
    }

    #[test]
    fn explicit_thresholds_relax_only_requested_checks() {
        let report = evaluate_gate(
            Uuid::nil(),
            &json!({"max_severity": "side_effect_risk"}),
            &json!({"overall": "pass"}),
            &json!({"divergences": [{}], "inconclusive": 2}),
            true,
            1,
            2,
        );
        assert!(report.passed);
    }

    #[test]
    fn malformed_diff_evidence_fails_closed() {
        let report = evaluate_gate(
            Uuid::nil(),
            &json!({"entries": [{"severity": "behavioral"}]}),
            &json!({"overall": "pass"}),
            &json!({"divergences": [], "inconclusive": 0}),
            true,
            0,
            0,
        );
        assert!(!report.passed);
        assert!(!report.checks[0].passed);
    }
}
