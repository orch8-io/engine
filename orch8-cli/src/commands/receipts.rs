//! `orch8 receipts export|verify`: signed effect-receipt bundles
//! (at-most-once dispatch evidence; see docs/HYBRID.md#receipts).

use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};
use chrono::{DateTime, Utc};
use clap::Subcommand;
use orch8_engine::receipt_bundle::{VerifyReport, verify_bundle};
use reqwest::Client;

use crate::OutputFormat;

#[derive(Debug, Subcommand)]
pub enum ReceiptsCmd {
    /// Download a signed JSONL bundle of effect receipts for one instance or
    /// a time window.
    Export {
        /// Instance whose ledger to export.
        #[arg(long, conflicts_with_all = ["from", "to"])]
        instance: Option<uuid::Uuid>,
        /// Window start (RFC 3339, inclusive).
        #[arg(long, requires = "to")]
        from: Option<DateTime<Utc>>,
        /// Window end (RFC 3339, exclusive).
        #[arg(long, requires = "from")]
        to: Option<DateTime<Utc>>,
        /// Output file (default: stdout).
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Verify a bundle offline: content digest, Ed25519 signature, record
    /// count, and ledger summary. Exits non-zero when invalid.
    Verify {
        /// Bundle file, or `-` for stdin.
        bundle: PathBuf,
        /// Pin the engine's public key (base64, from `GET /receipts/signing-key`
        /// or `orch8 receipts signing-key`). Without it only integrity is proven.
        #[arg(long)]
        public_key: Option<String>,
    },
    /// Print the engine's receipt-signing public key (for pinning).
    SigningKey,
}

pub async fn run(
    client: &Client,
    base: &str,
    cmd: ReceiptsCmd,
    format: OutputFormat,
) -> Result<()> {
    match cmd {
        ReceiptsCmd::Export {
            instance,
            from,
            to,
            out,
        } => {
            let request = match (instance, from, to) {
                (Some(id), None, None) => {
                    client.get(format!("{base}/instances/{id}/receipts/export"))
                }
                (None, Some(from), Some(to)) => {
                    if from >= to {
                        bail!("--from must be before --to");
                    }
                    client
                        .get(format!("{base}/receipts/export"))
                        .query(&[("from", from.to_rfc3339()), ("to", to.to_rfc3339())])
                }
                _ => bail!("pass either --instance <id> or --from <t> --to <t>"),
            };
            let response = request.send().await.context("request receipt export")?;
            let status = response.status();
            let body = response.text().await?;
            if !status.is_success() {
                let value: serde_json::Value =
                    serde_json::from_str(&body).unwrap_or(serde_json::Value::String(body));
                bail!(
                    "{status}: {}",
                    crate::describe_api_error(&value, "receipt export failed")
                );
            }
            // Refuse to write something that does not verify.
            let report =
                verify_bundle(&body, None).map_err(|e| anyhow::anyhow!("server bundle: {e}"))?;
            match out {
                Some(path) => {
                    crate::atomic_write(&path, body.as_bytes())?;
                    eprintln!(
                        "wrote {} receipt(s) for {} instance(s) to {} (signed by {})",
                        report.records,
                        report.instances,
                        path.display(),
                        report.signing_key_id
                    );
                }
                None => print!("{body}"),
            }
            Ok(())
        }
        ReceiptsCmd::Verify { bundle, public_key } => {
            let text = if bundle.as_os_str() == "-" {
                let mut buf = String::new();
                std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf)?;
                buf
            } else {
                std::fs::read_to_string(&bundle)
                    .with_context(|| format!("read {}", bundle.display()))?
            };
            match verify_bundle(&text, public_key.as_deref()) {
                Ok(report) => {
                    print_report(&report, format)?;
                    Ok(())
                }
                Err(error) => bail!("bundle INVALID: {error}"),
            }
        }
        ReceiptsCmd::SigningKey => {
            let response = client
                .get(format!("{base}/receipts/signing-key"))
                .send()
                .await
                .context("request signing key")?;
            crate::print_response(response, format).await
        }
    }
}

fn print_report(report: &VerifyReport, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(report)?),
        OutputFormat::Table => {
            println!("Bundle valid: {}", report.claim);
            println!("  tenant:       {}", report.tenant_id);
            println!(
                "  signed by:    {} ({})",
                report.signing_key_id,
                if report.key_pinned {
                    "matches pinned key"
                } else {
                    "embedded key only; pass --public-key to prove authorship"
                }
            );
            println!("  generated at: {}", report.generated_at.to_rfc3339());
            println!(
                "  receipts:     {} across {} instance(s)",
                report.records, report.instances
            );
            for (state, n) in &report.by_state {
                println!("    {state:<11} {n}");
            }
            println!(
                "  unresolved:   {} (dispatched/unknown; need a verifier or operator)",
                report.unresolved
            );
            println!("  duplicate attempts: {}", report.duplicate_attempts);
        }
    }
    Ok(())
}
