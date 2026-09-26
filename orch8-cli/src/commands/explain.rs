//! `orch8 explain <instance>` — plain-language "what happened and what do I
//! do now" for a stuck or failed instance.
//!
//! Thin client over `GET /instances/{id}/explain`: the server combines the
//! ranked stuck-instance diagnosis with the failure envelope and renders a
//! deterministic explanation. `--llm` asks the server for an additional
//! LLM-written narrative (evidence is redacted server-side before it is
//! sent; the provider comes from the server's `ORCH8_EXPLAIN_LLM_*` config).

use anyhow::{Context, Result};
use clap::Args;
use owo_colors::OwoColorize;
use reqwest::Client;
use serde_json::Value;
use uuid::Uuid;

use crate::OutputFormat;

#[derive(Debug, Args)]
pub struct ExplainCmd {
    /// Instance to explain.
    pub instance_id: Uuid,
    /// Also ask the server for an LLM-written narrative (uses the server's
    /// configured provider; evidence is redacted before it is sent).
    #[arg(long)]
    pub llm: bool,
}

pub async fn run(client: &Client, base: &str, cmd: ExplainCmd, format: OutputFormat) -> Result<()> {
    let mut request = client.get(format!("{base}/instances/{}/explain", cmd.instance_id));
    if cmd.llm {
        request = request.query(&[("llm", "true")]);
    }
    let response = request.send().await?;
    let status = response.status();
    let text = response
        .text()
        .await
        .with_context(|| format!("failed to read explain response (HTTP {status})"))?;
    let body: Value = serde_json::from_str(&text).unwrap_or(Value::String(text));
    if !status.is_success() {
        anyhow::bail!(
            "{status}: {}",
            crate::describe_api_error(&body, status.canonical_reason().unwrap_or("request failed"))
        );
    }
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&body)?),
        OutputFormat::Table => print!("{}", render(&body)),
    }
    Ok(())
}

fn str_of<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

/// Human rendering of an explanation.
pub fn render(e: &Value) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{} {} ({})",
        "instance".dimmed(),
        str_of(e, "instance_id"),
        crate::colorize_state(str_of(e, "state"))
    );
    let _ = writeln!(out, "\n{}", str_of(e, "headline").bold());
    let code = str_of(e, "code");
    let _ = writeln!(
        out,
        "\n{} {}\n  {}",
        "Likely cause".bold(),
        format_args!(
            "({code}{}, confidence: {})",
            crate::error_code_suffix(e),
            str_of(e, "confidence")
        )
        .to_string()
        .dimmed(),
        str_of(e, "likely_cause")
    );
    let evidence = e.get("evidence").and_then(Value::as_array);
    if let Some(items) = evidence.filter(|items| !items.is_empty()) {
        let _ = writeln!(out, "\n{}", "Evidence".bold());
        for item in items {
            let _ = writeln!(out, "  • {}", item.as_str().unwrap_or_default());
        }
    }
    let _ = writeln!(
        out,
        "\n{}\n  {}",
        "Suggested fix".bold(),
        str_of(e, "suggested_fix")
    );
    if let Some(commands) = e.get("commands").and_then(Value::as_array)
        && !commands.is_empty()
    {
        let _ = writeln!(out, "\n{}", "Commands".bold());
        for command in commands {
            let _ = writeln!(out, "  $ {}", command.as_str().unwrap_or_default().cyan());
        }
    }
    if let Some(alternatives) = e.get("alternatives").and_then(Value::as_array)
        && !alternatives.is_empty()
    {
        let _ = writeln!(out, "\n{}", "Other possibilities".bold());
        for alt in alternatives {
            let _ = writeln!(out, "  - {}", alt.as_str().unwrap_or_default());
        }
    }
    if let Some(narrative) = e.get("narrative").filter(|n| !n.is_null()) {
        let _ = writeln!(
            out,
            "\n{} {}\n  {}",
            "LLM summary".bold(),
            format!(
                "({} / {})",
                str_of(narrative, "provider"),
                str_of(narrative, "model")
            )
            .dimmed(),
            str_of(narrative, "text")
        );
    }
    if let Some(err) = e.get("llm_error").and_then(Value::as_str) {
        let _ = writeln!(
            out,
            "\n{} LLM narrative unavailable: {err}",
            "note:".yellow().bold()
        );
    }
    if let Some(url) = e.get("docs_url").and_then(Value::as_str) {
        let _ = writeln!(out, "\n{} {url}", "Docs:".dimmed());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strip_ansi(text: &str) -> String {
        let mut out = String::new();
        let mut chars = text.chars();
        while let Some(c) = chars.next() {
            if c == '\u{1b}' {
                for next in chars.by_ref() {
                    if next.is_ascii_alphabetic() {
                        break;
                    }
                }
            } else {
                out.push(c);
            }
        }
        out
    }

    #[test]
    fn renders_every_section() {
        let e = serde_json::json!({
            "instance_id": "0199",
            "state": "waiting",
            "headline": "The instance cannot progress: no compatible worker is running.",
            "likely_cause": "Handler 'charge' has no live worker.",
            "evidence": ["worker_registrations: 0"],
            "suggested_fix": "Start a worker.",
            "commands": ["orch8 instance diagnose 0199"],
            "code": "NO_COMPATIBLE_WORKER",
            "error_code": "ORCH8-P001",
            "docs_url": "https://orch8.io/docs/errors#ORCH8-P001",
            "confidence": "high",
            "alternatives": ["SCHEDULER_LAG: overdue"],
            "mode": "llm",
            "narrative": {"provider": "anthropic", "model": "m", "text": "Start the worker."},
        });
        let out = strip_ansi(&render(&e));
        for needle in [
            "no compatible worker is running",
            "(NO_COMPATIBLE_WORKER [ORCH8-P001], confidence: high)",
            "• worker_registrations: 0",
            "Start a worker.",
            "$ orch8 instance diagnose 0199",
            "- SCHEDULER_LAG: overdue",
            "LLM summary (anthropic / m)",
            "Docs: https://orch8.io/docs/errors#ORCH8-P001",
        ] {
            assert!(out.contains(needle), "missing {needle:?} in:\n{out}");
        }
    }

    #[tokio::test]
    async fn requests_llm_mode_and_surfaces_errors_with_codes() {
        use crate::commands::test_support::mock_api_with_responses;
        use axum::http::StatusCode;
        let id = Uuid::nil();
        let api = mock_api_with_responses(vec![
            (StatusCode::OK, r#"{"instance_id":"x","state":"waiting","headline":"h","likely_cause":"c","suggested_fix":"f","code":"PAUSED","confidence":"high","mode":"template"}"#.into()),
            (StatusCode::NOT_FOUND, r#"{"error":{"code":"not_found","message":"not found: instance"}}"#.into()),
        ])
        .await;
        let client = Client::new();
        run(
            &client,
            &api.base,
            ExplainCmd {
                instance_id: id,
                llm: true,
            },
            OutputFormat::Json,
        )
        .await
        .unwrap();
        let err = run(
            &client,
            &api.base,
            ExplainCmd {
                instance_id: id,
                llm: false,
            },
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("not found: instance"), "{err}");
        let log = api.log.snapshot();
        assert_eq!(log[0].uri, format!("/instances/{id}/explain?llm=true"));
        assert_eq!(log[1].uri, format!("/instances/{id}/explain"));
    }
}
