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
}

pub async fn run(client: &Client, base: &str, cmd: TestCmd, _format: OutputFormat) -> Result<()> {
    match cmd {
        TestCmd::Replay {
            instance_id,
            against,
        } => replay(client, base, instance_id, against).await,
    }
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
}
