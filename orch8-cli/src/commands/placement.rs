use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::OutputFormat;

#[derive(Debug, Subcommand)]
pub enum PlacementCmd {
    /// Show where a saved sequence version can run.
    Plan(PlanArgs),
    /// Preview a saved sequence against the current fleet and fail CI if it is blocked.
    FleetReadiness(PlanArgs),
    /// List the available placement recipes.
    Recipes,
}

#[derive(Debug, Args)]
pub struct PlanArgs {
    /// Sequence version UUID.
    pub sequence: Uuid,
    /// Instance tags used in policy selectors, separated by commas.
    #[arg(long)]
    pub tags: Option<String>,
    /// Preview a recipe as an additional hard requirement.
    #[arg(long)]
    pub recipe: Option<String>,
    /// Only include workers advertising this exact version.
    #[arg(long)]
    pub worker_version: Option<String>,
    /// Preview staged minimum worker versions.
    #[arg(long)]
    pub staged_pins: bool,
}

#[derive(Deserialize)]
struct PlacementReport {
    sequence_id: Uuid,
    ready: bool,
    inventory_complete: bool,
    steps: Vec<PlacementStep>,
}

#[derive(Deserialize)]
struct PlacementStep {
    tree: String,
    block_id: String,
    handler: String,
    route: String,
    pending_tasks: Option<u64>,
    queue_name: Option<String>,
    oldest_pending_age_secs: Option<u64>,
    available_poll_slots: Option<u64>,
    estimated_params_bytes: Option<u64>,
    minimum_worker_version: Option<String>,
    next_action: Option<String>,
    eligible_workers: Vec<String>,
    excluded_workers: Vec<WorkerExclusion>,
    worker_inventory: Option<WorkerInventory>,
}

#[derive(Deserialize)]
struct WorkerExclusion {
    worker_id: String,
    reasons: Vec<String>,
}

#[derive(Deserialize)]
struct WorkerInventory {
    eligible_count: usize,
}

pub async fn run(
    client: &Client,
    base: &str,
    cmd: PlacementCmd,
    format: OutputFormat,
) -> Result<()> {
    let (report, gate) = match cmd {
        PlacementCmd::Plan(args) => (fetch_plan(client, base, args).await?, false),
        PlacementCmd::FleetReadiness(args) => (fetch_plan(client, base, args).await?, true),
        PlacementCmd::Recipes => {
            let report: Value = client
                .get(format!("{base}/placement/recipes"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            return Ok(());
        }
    };
    let decoded = PlacementReport::deserialize(&report).context("decode placement plan")?;
    if matches!(format, OutputFormat::Json) {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_report(&decoded);
    }
    if gate && !decoded.ready {
        anyhow::bail!("fleet is not ready for this sequence");
    }
    Ok(())
}

async fn fetch_plan(client: &Client, base: &str, args: PlanArgs) -> Result<Value> {
    let mut request = client.get(format!("{base}/placement/plan/{}", args.sequence));
    if let Some(tags) = args.tags {
        request = request.query(&[("tags", tags)]);
    }
    if let Some(recipe) = args.recipe {
        request = request.query(&[("recipe", recipe)]);
    }
    if let Some(version) = args.worker_version {
        request = request.query(&[("worker_version", version)]);
    }
    if args.staged_pins {
        request = request.query(&[("staged_pins", true)]);
    }
    let response = request.send().await.context("request placement plan")?;
    let status = response.status();
    if !status.is_success() {
        anyhow::bail!(
            "placement plan failed ({status}): {}",
            response.text().await?
        );
    }
    response.json().await.context("decode placement plan")
}

pub(crate) fn print_plan(report: &Value) {
    match PlacementReport::deserialize(report) {
        Ok(decoded) => print_report(&decoded),
        Err(error) => eprintln!("Could not decode placement plan: {error}"),
    }
}

fn print_report(report: &PlacementReport) {
    println!("Sequence {}", report.sequence_id);
    if !report.inventory_complete {
        println!("Worker inventory reached the preview limit.");
    }
    for step in &report.steps {
        println!(
            "  {}/{}: {} ({})",
            step.tree, step.block_id, step.handler, step.route,
        );
        if let Some(pending) = step.pending_tasks {
            let queue = step.queue_name.as_deref().unwrap_or("default");
            let oldest = step
                .oldest_pending_age_secs
                .map_or_else(|| "none".to_owned(), |age| format!("{age}s"));
            let capacity = step
                .available_poll_slots
                .map_or_else(|| "unknown".to_owned(), |slots| slots.to_string());
            println!(
                "    Queue {queue}: {pending} pending, oldest {oldest}, {capacity} unused poll slots"
            );
        }
        if let Some(size) = step.estimated_params_bytes {
            println!("    Estimated saved params transfer: {size} bytes");
        }
        if let Some(minimum) = &step.minimum_worker_version {
            println!("    Minimum worker version evaluated: {minimum}");
        }
        if let Some(action) = &step.next_action {
            println!("    {action}");
        } else if !step.eligible_workers.is_empty() {
            let sample = step.eligible_workers.join(", ");
            let total = step
                .worker_inventory
                .as_ref()
                .map_or(step.eligible_workers.len(), |inventory| {
                    inventory.eligible_count
                });
            if total > step.eligible_workers.len() {
                println!(
                    "    Eligible ({total}, showing {}): {sample}",
                    step.eligible_workers.len()
                );
            } else {
                println!("    Eligible: {sample}");
            }
        }
        for worker in &step.excluded_workers {
            println!(
                "    Excluded {}: {}",
                worker.worker_id,
                worker.reasons.join(", ")
            );
        }
    }
}
