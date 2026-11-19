use std::path::PathBuf;

use anyhow::{Result, ensure};
use orch8_types::sequence::{
    BlockDefinition, MOBILE_DEFAULT_BUILTINS, MOBILE_OPT_IN_BUILTINS, deserialize_sequence_strict,
};
use serde::Serialize;

use crate::{OutputFormat, commands::test_cmd::authoring_defaults};

#[derive(Clone, Copy, clap::ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum MobileSdk {
    #[value(name = "published-0-7-1")]
    Published071,
    Development,
}

#[derive(clap::Args)]
pub struct MobilePreflightArgs {
    sequence: PathBuf,
    /// Native libraries published as 0.7.1 and source builds have distinct features.
    #[arg(long, value_enum)]
    sdk: MobileSdk,
    /// Handler explicitly registered by the host app. Repeat per handler.
    #[arg(long = "handler")]
    handlers: Vec<String>,
    /// Explicitly enabled network built-in. Reported as unavailable offline.
    #[arg(long = "enable-builtin", value_parser = clap::builder::PossibleValuesParser::new(MOBILE_OPT_IN_BUILTINS.iter().copied()))]
    network_builtins: Vec<String>,
}

#[derive(Serialize)]
struct MobileCheck<'a> {
    block_id: &'a str,
    source: &'static str,
    ready: bool,
    detail: Option<&'static str>,
}

pub(super) fn run(args: &MobilePreflightArgs, format: OutputFormat) -> Result<()> {
    let input = authoring_defaults(crate::seqdoc::read_document(&args.sequence)?);
    let sequence = deserialize_sequence_strict(&input)?;
    sequence.validate()?;

    let mut checks = Vec::new();
    for tree in [
        sequence.blocks.as_slice(),
        sequence.on_failure.as_deref().unwrap_or_default(),
        sequence.on_cancel.as_deref().unwrap_or_default(),
    ] {
        for (id, block) in orch8_engine::evaluator::flatten_blocks(tree) {
            let (source, detail) = match block {
                BlockDefinition::Step(step)
                    if sequence
                        .placement
                        .as_ref()
                        .is_some_and(orch8_types::placement::Placement::has_hard_constraints)
                        || step.placement.as_ref().is_some_and(
                            orch8_types::placement::Placement::has_hard_constraints,
                        )
                        || step.params.get("$runtime").is_some()
                        || step.queue_name.is_some() =>
                {
                    (
                        "remote",
                        Some("verify the worker lane separately; this step may leave the device"),
                    )
                }
                BlockDefinition::Step(step) if args.handlers.contains(&step.handler) => {
                    ("host", None)
                }
                BlockDefinition::Step(step)
                    if matches!(args.sdk, MobileSdk::Development)
                        && MOBILE_DEFAULT_BUILTINS.contains(&step.handler.as_str()) =>
                {
                    ("builtin", None)
                }
                BlockDefinition::Step(step) if args.network_builtins.contains(&step.handler) => (
                    "network",
                    Some("network built-ins need connectivity; use a host offline fallback"),
                ),
                BlockDefinition::Step(_) => (
                    "missing",
                    Some("register this handler in the selected mobile SDK"),
                ),
                BlockDefinition::SubSequence(_) => (
                    "dependency",
                    Some(
                        "preflight the pinned child definition and verify it is locally installed",
                    ),
                ),
                _ => continue,
            };
            checks.push(MobileCheck {
                block_id: id.as_str(),
                source,
                ready: detail.is_none(),
                detail,
            });
        }
    }
    let dataflow = orch8_engine::dataflow::compile(&sequence);
    let ready = checks.iter().all(|check| check.ready)
        && !dataflow.findings.iter().any(|finding| {
            matches!(
                finding.severity,
                orch8_engine::dataflow::DataflowSeverity::Error
            )
        });
    match format {
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "sequence": sequence.name, "sdk": args.sdk, "ready": ready, "checks": checks,
                "dataflow": dataflow,
            }))?
        ),
        OutputFormat::Table => {
            for check in &checks {
                println!(
                    "{}: {} {}",
                    check.block_id,
                    check.source,
                    check.detail.unwrap_or("")
                );
            }
            for finding in &dataflow.findings {
                println!("{}: {}", finding.code, finding.summary);
            }
        }
    }
    ensure!(ready, "mobile compatibility checks failed");
    Ok(())
}
