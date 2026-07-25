use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;
use reqwest::{Client, StatusCode};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::OutputFormat;

#[derive(Debug, Args)]
pub struct DoctorCmd {
    /// Validate this local configuration in addition to remote checks.
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Include ranked diagnosis for one execution instance.
    #[arg(long)]
    pub instance: Option<Uuid>,
    /// Exit non-zero for warnings as well as errors.
    #[arg(long)]
    pub strict: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckStatus {
    Pass,
    Warning,
    Error,
    Skipped,
}

#[derive(Debug, Serialize)]
struct DoctorCheck {
    name: &'static str,
    status: CheckStatus,
    summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    evidence: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remediation: Option<String>,
}

#[derive(Debug, Serialize)]
struct DoctorReport {
    passed: bool,
    strict: bool,
    checks: Vec<DoctorCheck>,
}

pub async fn run(
    client: &Client,
    base: &str,
    command: DoctorCmd,
    format: OutputFormat,
) -> Result<()> {
    let mut checks = vec![check_config(command.config.as_ref())];
    let root = base.strip_suffix("/api/v1").unwrap_or(base);

    checks.push(
        fetch_status(
            client,
            "connectivity",
            format!("{root}/health/ready"),
            "start the server and verify database/scheduler readiness",
        )
        .await,
    );
    checks.push(check_version(client, root).await);
    checks.push(
        fetch_json(
            client,
            "sequence_store",
            format!("{base}/sequences?limit=1"),
            "verify authentication, tenant selection, and storage migrations",
            |_| None,
        )
        .await,
    );
    checks.push(
        fetch_json(
            client,
            "worker_inventory",
            format!("{base}/workers?include_stale=true"),
            "start or reconnect a worker if this workflow needs external handlers",
            |value| {
                value
                    .as_array()
                    .filter(|workers| workers.is_empty())
                    .map(|_| {
                        "no external workers are registered; built-in-only workflows are unaffected"
                            .into()
                    })
            },
        )
        .await,
    );
    checks.push(
        fetch_json(
            client,
            "continuity_control",
            format!("{base}/runtimes"),
            "verify tenant access and continuity storage migrations",
            |_| None,
        )
        .await,
    );

    if let Some(instance) = command.instance {
        checks.push(
            fetch_json(
                client,
                "instance_diagnosis",
                format!("{base}/instances/{instance}/diagnosis"),
                "inspect the ranked remediation commands and authorize one explicitly",
                diagnosis_warning,
            )
            .await,
        );
    }

    checks.sort_by_key(|check| match check.status {
        CheckStatus::Error => 0,
        CheckStatus::Warning => 1,
        CheckStatus::Pass => 2,
        CheckStatus::Skipped => 3,
    });

    let passed = checks.iter().all(|check| match check.status {
        CheckStatus::Pass | CheckStatus::Skipped => true,
        CheckStatus::Warning => !command.strict,
        CheckStatus::Error => false,
    });
    let report = DoctorReport {
        passed,
        strict: command.strict,
        checks,
    };
    print_report(&report, format)?;
    if !passed {
        anyhow::bail!("doctor found checks that require attention");
    }
    Ok(())
}

fn check_config(path: Option<&PathBuf>) -> DoctorCheck {
    let Some(path) = path else {
        return DoctorCheck {
            name: "config",
            status: CheckStatus::Skipped,
            summary: "no --config path supplied".into(),
            evidence: None,
            remediation: None,
        };
    };
    let result = std::fs::read_to_string(path)
        .with_context(|| format!("read {}", path.display()))
        .and_then(|contents| {
            let config: orch8_types::config::EngineConfig =
                toml::from_str(&contents).with_context(|| format!("parse {}", path.display()))?;
            config
                .validate()
                .map_err(|errors| anyhow::anyhow!(errors.join("; ")))
        });
    match result {
        Ok(()) => DoctorCheck {
            name: "config",
            status: CheckStatus::Pass,
            summary: format!("{} is valid", path.display()),
            evidence: None,
            remediation: None,
        },
        Err(error) => DoctorCheck {
            name: "config",
            status: CheckStatus::Error,
            summary: error.to_string(),
            evidence: None,
            remediation: Some(
                "run `orch8 config validate` and correct every reported field".into(),
            ),
        },
    }
}

async fn fetch_status(
    client: &Client,
    name: &'static str,
    url: String,
    remediation: &'static str,
) -> DoctorCheck {
    match client.get(url).send().await {
        Ok(response) if response.status().is_success() => DoctorCheck {
            name,
            status: CheckStatus::Pass,
            summary: format!("HTTP {}", response.status()),
            evidence: None,
            remediation: None,
        },
        Ok(response) => failed_http(name, response.status(), remediation),
        Err(error) => DoctorCheck {
            name,
            status: CheckStatus::Error,
            summary: error.to_string(),
            evidence: None,
            remediation: Some(remediation.into()),
        },
    }
}

async fn fetch_json(
    client: &Client,
    name: &'static str,
    url: String,
    remediation: &'static str,
    warning: impl FnOnce(&Value) -> Option<String>,
) -> DoctorCheck {
    let response = match client.get(url).send().await {
        Ok(response) => response,
        Err(error) => {
            return DoctorCheck {
                name,
                status: CheckStatus::Error,
                summary: error.to_string(),
                evidence: None,
                remediation: Some(remediation.into()),
            };
        }
    };
    if !response.status().is_success() {
        return failed_http(name, response.status(), remediation);
    }
    match response.json::<Value>().await {
        Ok(value) => {
            let warning = warning(&value);
            DoctorCheck {
                name,
                status: if warning.is_some() {
                    CheckStatus::Warning
                } else {
                    CheckStatus::Pass
                },
                summary: warning.unwrap_or_else(|| "available and readable".into()),
                evidence: Some(value),
                remediation: None,
            }
        }
        Err(error) => DoctorCheck {
            name,
            status: CheckStatus::Error,
            summary: format!("invalid JSON response: {error}"),
            evidence: None,
            remediation: Some("verify API compatibility and reverse-proxy behavior".into()),
        },
    }
}

async fn check_version(client: &Client, root: &str) -> DoctorCheck {
    fetch_json(
        client,
        "version_compatibility",
        format!("{root}/info"),
        "upgrade the CLI or server so their major versions match",
        |value| {
            let server = value["version"].as_str()?;
            let cli_major = env!("CARGO_PKG_VERSION").split('.').next()?;
            let server_major = server.split('.').next()?;
            (server_major != cli_major).then(|| {
                format!(
                    "CLI {} and server {server} have different major versions",
                    env!("CARGO_PKG_VERSION")
                )
            })
        },
    )
    .await
}

fn diagnosis_warning(value: &Value) -> Option<String> {
    let diagnoses = value["diagnoses"].as_array()?;
    let actionable = diagnoses
        .iter()
        .filter(|diagnosis| {
            matches!(
                diagnosis["health"].as_str(),
                Some("degraded" | "inconsistent")
            )
        })
        .count();
    (actionable > 0).then(|| format!("{actionable} degraded or inconsistent diagnosis finding(s)"))
}

fn failed_http(name: &'static str, status: StatusCode, remediation: &str) -> DoctorCheck {
    DoctorCheck {
        name,
        status: CheckStatus::Error,
        summary: format!("HTTP {status}"),
        evidence: None,
        remediation: Some(remediation.into()),
    }
}

fn print_report(report: &DoctorReport, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(report)?),
        OutputFormat::Table => {
            for check in &report.checks {
                println!("{:?}\t{}\t{}", check.status, check.name, check.summary);
                if let Some(remediation) = &check.remediation {
                    println!("  remediation: {remediation}");
                }
            }
            println!("doctor: {}", if report.passed { "PASS" } else { "FAIL" });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnosis_warns_only_for_actionable_health() {
        assert!(
            diagnosis_warning(&serde_json::json!({
                "diagnoses": [{"health": "expected"}]
            }))
            .is_none()
        );
        assert_eq!(
            diagnosis_warning(&serde_json::json!({
                "diagnoses": [{"health": "degraded"}, {"health": "inconsistent"}]
            }))
            .as_deref(),
            Some("2 degraded or inconsistent diagnosis finding(s)")
        );
    }

    #[test]
    fn absent_config_is_explicitly_skipped() {
        assert_eq!(check_config(None).status, CheckStatus::Skipped);
    }
}
