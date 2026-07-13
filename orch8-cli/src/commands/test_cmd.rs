//! `orch8 test replay` — re-run a recorded sequence run against a different
//! sequence version under virtual time, then diff which blocks executed.
//!
//! The recorded run's per-block outputs are replayed verbatim (each step's
//! handler is mocked to return what it returned originally), so the *new*
//! version's control flow runs against the *old* run's data. The diff shows
//! where the versions diverge: blocks the new version reaches that the old run
//! didn't (`+ added`), and blocks the old run had that the new version skips
//! (`- removed`). It never executes real handlers — safe to run anywhere.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Subcommand;
use reqwest::Client;
use serde_json::Value;
use uuid::Uuid;

use orch8::{
    Clock, CreateInstanceOptions, Engine, ExecutionContext, ManualClock, SequenceDefinition,
    SharedClock, Storage,
};
use orch8_types::contract::{ContractSuite, SuiteReport};
use orch8_types::redaction::RedactionPolicy;

use crate::OutputFormat;
use crate::commands::dev::{block_handlers, next_advance_target};

#[derive(Subcommand)]
pub enum TestCmd {
    /// Replay a recorded run against another sequence version and diff the
    /// blocks that execute.
    Replay {
        /// The recorded instance id to replay.
        instance_id: Uuid,
        /// The sequence version to replay against.
        #[arg(long)]
        against: i32,
    },
    /// Run a workflow contract suite against a sequence definition, fully
    /// offline: every handler is mocked and time is virtual.
    Run {
        /// Path to the contract suite (`<seq>.contracts.json`).
        contract_file: PathBuf,
        /// Path to the sequence definition. Defaults to the contract path
        /// with `.contracts` removed (`checkout.contracts.json` →
        /// `checkout.json`).
        #[arg(long)]
        sequence: Option<PathBuf>,
        /// Recorded outputs fixture (JSON object `{block_id: output}`) for
        /// mocks with `"type": "recorded"`.
        #[arg(long)]
        recorded: Option<PathBuf>,
        /// Report format: human, json, or junit.
        #[arg(long, default_value = "human")]
        report: ReportFormat,
    },
    /// Record a draft contract fixture from an executed instance, with
    /// secrets redacted. Review before committing — recorded values may
    /// still contain domain data you don't want in git.
    Record {
        /// The instance to record.
        instance_id: Uuid,
        /// Output path (defaults to stdout).
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum ReportFormat {
    Human,
    Json,
    Junit,
}

pub async fn run(client: &Client, base: &str, cmd: TestCmd, _format: OutputFormat) -> Result<()> {
    match cmd {
        TestCmd::Replay {
            instance_id,
            against,
        } => replay(client, base, instance_id, against).await,
        TestCmd::Run {
            contract_file,
            sequence,
            recorded,
            report,
        } => {
            run_contracts(
                &contract_file,
                sequence.as_deref(),
                recorded.as_deref(),
                report,
            )
            .await
        }
        TestCmd::Record { instance_id, out } => {
            record(client, base, instance_id, out.as_deref()).await
        }
    }
}

// ---------------------------------------------------------------------------
// `orch8 test run`
// ---------------------------------------------------------------------------

/// Derive the sequence path from `<name>.contracts.json` → `<name>.json`.
fn default_sequence_path(contract_path: &Path) -> Option<PathBuf> {
    let name = contract_path.file_name()?.to_str()?;
    let stem = name.strip_suffix(".contracts.json")?;
    Some(contract_path.with_file_name(format!("{stem}.json")))
}

async fn run_contracts(
    contract_file: &Path,
    sequence: Option<&Path>,
    recorded: Option<&Path>,
    report_format: ReportFormat,
) -> Result<()> {
    let contract_raw = std::fs::read_to_string(contract_file)
        .with_context(|| format!("reading {}", contract_file.display()))?;
    let suite: ContractSuite =
        serde_json::from_str(&contract_raw).context("contract file is not a valid suite")?;

    let seq_path = match sequence {
        Some(p) => p.to_path_buf(),
        None => default_sequence_path(contract_file).context(
            "cannot derive the sequence path (contract file does not end in `.contracts.json`); \
             pass --sequence",
        )?,
    };
    let seq_raw = std::fs::read_to_string(&seq_path)
        .with_context(|| format!("reading {}", seq_path.display()))?;
    let seq: SequenceDefinition =
        serde_json::from_str(&seq_raw).context("sequence file is not a valid definition")?;

    let mut opts = orch8::contract::RunOptions::default();
    if let Some(recorded_path) = recorded {
        let raw = std::fs::read_to_string(recorded_path)
            .with_context(|| format!("reading {}", recorded_path.display()))?;
        let map: HashMap<String, Value> =
            serde_json::from_str(&raw).context("recorded outputs must be {block_id: output}")?;
        opts.recorded_outputs = map;
    }

    let report = orch8::contract::run_suite(&seq, &suite, &opts).await?;

    match report_format {
        ReportFormat::Human => print_human_report(&report),
        ReportFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        ReportFormat::Junit => println!("{}", junit_xml(&report)),
    }

    if report.passed {
        Ok(())
    } else {
        // Non-zero exit for CI.
        std::process::exit(1);
    }
}

fn print_human_report(report: &SuiteReport) {
    println!(
        "contract suite for {} v{}\n",
        report.sequence_name, report.sequence_version
    );
    for case in &report.cases {
        let mark = if case.passed { "PASS" } else { "FAIL" };
        println!(
            "  [{mark}] {}  ({} ticks, {} ms logical, ended {})",
            case.name, case.ticks, case.logical_duration_ms, case.final_state
        );
        for failure in &case.failures {
            println!("         ✗ {failure}");
        }
    }
    let failed = report.failed_cases().len();
    let total = report.cases.len();
    println!(
        "\n{} of {total} cases passed{}",
        total - failed,
        if failed > 0 {
            format!(", {failed} failed")
        } else {
            String::new()
        }
    );
}

/// Minimal `JUnit` XML so CI systems can ingest contract results.
fn junit_xml(report: &SuiteReport) -> String {
    fn escape(s: &str) -> String {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
    }
    use std::fmt::Write as _;
    let failures = report.failed_cases().len();
    let mut out = String::new();
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    let _ = writeln!(
        out,
        "<testsuite name=\"{}\" tests=\"{}\" failures=\"{failures}\">",
        escape(&format!(
            "{} v{}",
            report.sequence_name, report.sequence_version
        )),
        report.cases.len(),
    );
    for case in &report.cases {
        if case.passed {
            let _ = writeln!(out, "  <testcase name=\"{}\"/>", escape(&case.name));
        } else {
            let _ = writeln!(out, "  <testcase name=\"{}\">", escape(&case.name));
            for failure in &case.failures {
                let _ = writeln!(out, "    <failure message=\"{}\"/>", escape(failure));
            }
            out.push_str("  </testcase>\n");
        }
    }
    out.push_str("</testsuite>");
    out
}

// ---------------------------------------------------------------------------
// `orch8 test record`
// ---------------------------------------------------------------------------

async fn record(client: &Client, base: &str, instance_id: Uuid, out: Option<&Path>) -> Result<()> {
    let inst = get_json(client, format!("{base}/instances/{instance_id}")).await?;
    let seq_id = inst["sequence_id"]
        .as_str()
        .context("instance missing sequence_id")?;
    let seq = get_json(client, format!("{base}/sequences/{seq_id}")).await?;
    let outputs: Vec<Value> = get_json(client, format!("{base}/instances/{instance_id}/outputs"))
        .await?
        .as_array()
        .cloned()
        .unwrap_or_default();

    let draft = build_recorded_case(&inst, &seq, &outputs, &RedactionPolicy::default());
    let rendered = serde_json::to_string_pretty(&draft)?;

    match out {
        Some(path) => {
            anyhow::ensure!(
                !path.exists(),
                "{} already exists — refusing to overwrite; review and merge manually",
                path.display()
            );
            std::fs::write(path, &rendered)
                .with_context(|| format!("writing {}", path.display()))?;
            eprintln!(
                "draft contract written to {} — review it before committing.",
                path.display()
            );
        }
        None => println!("{rendered}"),
    }
    Ok(())
}

/// Assemble a draft one-case contract suite from a recorded run. Pure so it
/// can be tested without a server.
fn build_recorded_case(
    inst: &Value,
    seq: &Value,
    outputs: &[Value],
    redaction: &RedactionPolicy,
) -> Value {
    let mut executed: Vec<String> = Vec::new();
    let mut mocks: Vec<Value> = Vec::new();
    for o in outputs {
        let Some(block_id) = o["block_id"].as_str() else {
            continue;
        };
        if block_id.starts_with('_') {
            continue;
        }
        if !executed.iter().any(|b| b == block_id) {
            executed.push(block_id.to_string());
            mocks.push(serde_json::json!({
                "block": block_id,
                "type": "success",
                "output": redaction.redacted(&o["output"]),
            }));
        }
    }

    let state = inst["state"].as_str().unwrap_or("completed");
    serde_json::json!({
        "schema_version": 1,
        "sequence_name": seq["name"],
        "sequence_version": seq["version"],
        "cases": [{
            "name": format!("recorded from instance {}", inst["id"].as_str().unwrap_or("?")),
            "description": "Draft recorded fixture — review values and tighten assertions.",
            "input": redaction.redacted(&inst["context"]["data"]),
            "mocks": mocks,
            "expect": {
                "terminal_state": state,
                "path": {"traversed": executed, "ordered": true}
            }
        }]
    })
}

/// Diff the blocks the recorded run produced against those the replay reached.
/// Returns `(added, removed, matched_count)`: `added` are blocks reached only in
/// the replay (new version's behavior), `removed` only in the record (skipped by
/// the new version). Both lists are sorted for stable output.
fn block_diff(
    recorded: &HashSet<String>,
    replayed: &HashSet<String>,
) -> (Vec<String>, Vec<String>, usize) {
    let mut added: Vec<String> = replayed.difference(recorded).cloned().collect();
    let mut removed: Vec<String> = recorded.difference(replayed).cloned().collect();
    let matched = recorded.intersection(replayed).count();
    added.sort();
    removed.sort();
    (added, removed, matched)
}

async fn get_json(client: &Client, url: String) -> Result<Value> {
    let resp = client.get(&url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("{url} → {}", resp.status());
    }
    Ok(resp.json().await?)
}

/// Build an embedded engine (in-memory, virtual clock, a mock per handler the
/// target uses that returns the recorded output for the block it runs), publish
/// the target version, drive a dry-run instance with `context_data` to terminal
/// under virtual time, and return the set of blocks it reached.
async fn run_replay(
    target_json: &Value,
    recorded: &Arc<HashMap<String, Value>>,
    context_data: Value,
) -> Result<HashSet<String>> {
    let handler_names: HashSet<String> = block_handlers(target_json).into_values().collect();
    let clock = Arc::new(ManualClock::new(chrono::Utc::now()));
    let shared = SharedClock::from_arc(Arc::clone(&clock) as Arc<dyn Clock>);
    let mut builder = Engine::builder()
        .storage(Storage::sqlite_in_memory())
        .clock(shared);
    for handler in &handler_names {
        let rec = Arc::clone(recorded);
        builder = builder.handler(handler, move |ctx: orch8::StepContext| {
            let rec = Arc::clone(&rec);
            async move {
                Ok(rec
                    .get(ctx.block_id.as_str())
                    .cloned()
                    .unwrap_or(Value::Object(serde_json::Map::new())))
            }
        });
    }
    let engine = builder.build().await?;

    let def: SequenceDefinition = serde_json::from_value(target_json.clone())
        .context("target sequence is not a valid definition")?;
    let seq_id = engine.upsert_sequence(def).await?;
    let context = ExecutionContext {
        data: context_data,
        runtime: orch8_types::context::RuntimeContext {
            dry_run: true,
            dry_run_auto_approve: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let replay_id = engine
        .create_instance(
            seq_id,
            CreateInstanceOptions {
                context,
                ..Default::default()
            },
        )
        .await?;

    for _ in 0..5000 {
        let tick = engine.tick_once().await?;
        let inst = engine.get_instance(replay_id).await?;
        if inst.state.is_terminal() {
            break;
        }
        if tick.steps_executed == 0 && tick.instances_advanced == 0 {
            // Idle — advance virtual time past the next deferral, else stop.
            match next_advance_target(inst.state, inst.next_fire_at, clock.now()) {
                Some(t) => clock.set(t + chrono::Duration::seconds(1)),
                None => break,
            }
        }
    }

    Ok(engine
        .block_outputs(replay_id)
        .await?
        .into_iter()
        .filter_map(|o| {
            let b = o.block_id.as_str().to_string();
            (!b.starts_with('_')).then_some(b)
        })
        .collect())
}

async fn replay(client: &Client, base: &str, instance_id: Uuid, against: i32) -> Result<()> {
    // 1. The recorded instance: its input data + sequence identity.
    let inst = get_json(client, format!("{base}/instances/{instance_id}")).await?;
    let context_data = inst["context"]["data"].clone();
    let sequence_id = inst["sequence_id"]
        .as_str()
        .context("instance missing sequence_id")?
        .to_string();
    let tenant_id = inst["tenant_id"].as_str().unwrap_or_default().to_string();
    let namespace = inst["namespace"].as_str().unwrap_or_default().to_string();

    // The original sequence (for its name) and the target version.
    let orig_seq = get_json(client, format!("{base}/sequences/{sequence_id}")).await?;
    let name = orig_seq["name"].as_str().context("sequence missing name")?;
    let orig_version = orig_seq["version"].as_i64().unwrap_or(0);

    let target = client
        .get(format!("{base}/sequences/by-name"))
        .query(&[
            ("tenant_id", &tenant_id),
            ("namespace", &namespace),
            ("name", &name.to_string()),
            ("version", &against.to_string()),
        ])
        .send()
        .await?;
    if !target.status().is_success() {
        anyhow::bail!(
            "no version {against} of sequence {name} (got {})",
            target.status()
        );
    }
    let target_json: Value = target.json().await?;

    // 2. The recorded per-block outputs → block_id → output (last attempt wins).
    let outputs: Vec<Value> = get_json(client, format!("{base}/instances/{instance_id}/outputs"))
        .await?
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut recorded: HashMap<String, Value> = HashMap::new();
    for o in &outputs {
        if let Some(bid) = o["block_id"].as_str() {
            // Skip internal sentinels (e.g. `_sla:*`, interceptor markers).
            if bid.starts_with('_') {
                continue;
            }
            recorded.insert(bid.to_string(), o["output"].clone());
        }
    }
    let recorded = Arc::new(recorded);
    let recorded_ids: HashSet<String> = recorded.keys().cloned().collect();

    // 3-5. Replay the target version under virtual time with the recorded
    //      outputs, and collect which blocks it reached.
    let replayed = run_replay(&target_json, &recorded, context_data).await?;
    let (added, removed, matched) = block_diff(&recorded_ids, &replayed);

    println!("replay of {instance_id} (recorded v{orig_version}) against v{against} of {name}\n");
    println!("  matched blocks: {matched}");
    if added.is_empty() && removed.is_empty() {
        println!("  no divergence — v{against} executes the same blocks for this input.");
    } else {
        for b in &added {
            println!("  + {b}   (executed in v{against}, not in the recorded run)");
        }
        for b in &removed {
            println!("  - {b}   (in the recorded run, skipped by v{against})");
        }
        println!("\n  {} added, {} removed.", added.len(), removed.len());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn identical_runs_have_no_diff() {
        let (added, removed, matched) = block_diff(&set(&["a", "b", "c"]), &set(&["a", "b", "c"]));
        assert!(added.is_empty());
        assert!(removed.is_empty());
        assert_eq!(matched, 3);
    }

    #[test]
    fn detects_added_and_removed_sorted() {
        // Recorded ran a,b,c; replay ran a,c,d,e.
        let (added, removed, matched) =
            block_diff(&set(&["a", "b", "c"]), &set(&["a", "c", "e", "d"]));
        assert_eq!(added, vec!["d".to_string(), "e".to_string()]);
        assert_eq!(removed, vec!["b".to_string()]);
        assert_eq!(matched, 2); // a, c
    }

    #[test]
    fn empty_record_makes_everything_added() {
        let (added, removed, matched) = block_diff(&set(&[]), &set(&["x", "y"]));
        assert_eq!(added, vec!["x".to_string(), "y".to_string()]);
        assert!(removed.is_empty());
        assert_eq!(matched, 0);
    }

    // --- contract run helpers ---

    #[test]
    fn default_sequence_path_strips_contracts_suffix() {
        let p = default_sequence_path(Path::new("workflows/checkout.contracts.json")).unwrap();
        assert_eq!(p, Path::new("workflows/checkout.json"));
    }

    #[test]
    fn default_sequence_path_requires_contracts_suffix() {
        assert!(default_sequence_path(Path::new("workflows/checkout.json")).is_none());
        assert!(default_sequence_path(Path::new("checkout.contracts.yaml")).is_none());
    }

    fn sample_report(passed: bool) -> SuiteReport {
        use orch8_types::contract::CaseReport;
        SuiteReport {
            sequence_name: "checkout <fast>".into(),
            sequence_version: 2,
            passed,
            cases: vec![
                CaseReport {
                    name: "happy".into(),
                    passed: true,
                    failures: vec![],
                    final_state: "completed".into(),
                    executed_blocks: vec!["a".into()],
                    handler_calls: std::collections::BTreeMap::new(),
                    ticks: 4,
                    logical_duration_ms: 12,
                },
                CaseReport {
                    name: "declined & retried".into(),
                    passed,
                    failures: if passed {
                        vec![]
                    } else {
                        vec!["expected terminal state \"failed\"".into()]
                    },
                    final_state: "completed".into(),
                    executed_blocks: vec![],
                    handler_calls: std::collections::BTreeMap::new(),
                    ticks: 9,
                    logical_duration_ms: 3000,
                },
            ],
        }
    }

    #[test]
    fn junit_xml_escapes_and_counts_failures() {
        let xml = junit_xml(&sample_report(false));
        assert!(xml.contains("tests=\"2\" failures=\"1\""), "{xml}");
        // Suite name is escaped.
        assert!(xml.contains("checkout &lt;fast&gt;"), "{xml}");
        // Failure message quotes are escaped.
        assert!(xml.contains("&quot;failed&quot;"), "{xml}");
        // Failed case has a nested <failure>, passing case is self-closing.
        assert!(xml.contains("<testcase name=\"happy\"/>"), "{xml}");
        assert!(
            xml.contains("<testcase name=\"declined &amp; retried\">"),
            "{xml}"
        );
    }

    #[test]
    fn junit_xml_all_passing_has_zero_failures() {
        let xml = junit_xml(&sample_report(true));
        assert!(xml.contains("failures=\"0\""), "{xml}");
        assert!(!xml.contains("<failure"), "{xml}");
    }

    // --- record helpers ---

    #[test]
    fn build_recorded_case_redacts_and_orders() {
        let inst = serde_json::json!({
            "id": "0198aaaa-0000-7000-8000-000000000001",
            "state": "completed",
            "sequence_id": "s",
            "context": {"data": {"card_token": "sk_live_secret", "amount": 100}}
        });
        let seq = serde_json::json!({"name": "checkout", "version": 3});
        let outputs = vec![
            serde_json::json!({"block_id": "validate", "output": {"ok": true}}),
            serde_json::json!({"block_id": "_sla:x", "output": {}}),
            serde_json::json!({"block_id": "charge", "output": {"api_key": "sk_live_abc", "charged": true}}),
            // Duplicate row for a retried block must not duplicate the mock.
            serde_json::json!({"block_id": "validate", "output": {"ok": true}}),
        ];

        let draft = build_recorded_case(&inst, &seq, &outputs, &RedactionPolicy::default());

        // Parses as a valid suite.
        let suite: ContractSuite = serde_json::from_value(draft.clone()).unwrap();
        suite.validate().unwrap();

        // Input fixture is redacted (token-named key).
        assert_eq!(draft["cases"][0]["input"]["card_token"], "[REDACTED]");
        assert_eq!(draft["cases"][0]["input"]["amount"], 100);

        // Sentinel rows are skipped; path preserves first-execution order.
        assert_eq!(
            draft["cases"][0]["expect"]["path"]["traversed"],
            serde_json::json!(["validate", "charge"])
        );

        // Mock outputs are redacted, one mock per block.
        let mocks = draft["cases"][0]["mocks"].as_array().unwrap();
        assert_eq!(mocks.len(), 2);
        assert_eq!(mocks[1]["output"]["api_key"], "[REDACTED]");
        assert_eq!(mocks[1]["output"]["charged"], true);

        // Terminal state mirrors the recorded instance.
        assert_eq!(draft["cases"][0]["expect"]["terminal_state"], "completed");
    }
}
