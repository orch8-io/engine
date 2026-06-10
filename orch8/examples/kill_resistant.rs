//! Kill-resistance demo: workflows survive process exit because all state
//! lives in `SQLite`.
//!
//! Run this binary **twice**:
//!
//! ```sh
//! cargo run -p orch8 --example kill_resistant   # creates a delayed instance, then exits
//! cargo run -p orch8 --example kill_resistant   # the instance resumes and completes
//! ```
//!
//! The first run registers a sequence and creates an instance scheduled a few
//! seconds in the future, then exits before it fires — simulating a crash
//! mid-run. The second run reopens the same database, finds the live
//! instance, and the scheduler picks it up exactly where it left off.

use std::time::Duration;

use orch8::{CreateInstanceOptions, Engine, InstanceFilter, InstanceState, Storage};

const DELAY: Duration = Duration::from_secs(5);

fn sequence_json() -> serde_json::Value {
    serde_json::json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "default",
        "namespace": "default",
        "name": "kill-resistant",
        "version": 1,
        "blocks": [
            { "type": "step", "id": "survive", "handler": "survive", "params": {} },
            { "type": "step", "id": "finish", "handler": "log", "params": { "message": "all steps done" } }
        ],
        "created_at": chrono::Utc::now().to_rfc3339()
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = std::env::temp_dir().join("orch8-kill-resistant.db");
    println!("database: {}", db_path.display());

    let engine = Engine::builder()
        .storage(Storage::sqlite(&db_path))
        .handler("survive", |_ctx: orch8::StepContext| async move {
            println!("[survive] step executed — the workflow outlived the first process!");
            Ok(serde_json::json!({ "survived": true }))
        })
        .build()
        .await?;

    // Any instance still in a non-terminal state from a previous run?
    let live_filter = InstanceFilter {
        states: Some(vec![
            InstanceState::Scheduled,
            InstanceState::Running,
            InstanceState::Waiting,
            InstanceState::Paused,
        ]),
        ..InstanceFilter::default()
    };
    let live = engine.list_instances(&live_filter).await?;

    if let Some(pending) = live.first() {
        // --- Second run: resume the crashed workflow. ---
        println!("found surviving instance {} — resuming", pending.id);
        engine.start();
        loop {
            let snapshot = engine.get_instance(pending.id).await?;
            if matches!(
                snapshot.state,
                InstanceState::Completed | InstanceState::Failed | InstanceState::Cancelled
            ) {
                println!("instance finished with state {:?}", snapshot.state);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        engine.shutdown().await;
    } else {
        // --- First run: create a delayed instance and "crash". ---
        let seq_id = engine
            .upsert_sequence(serde_json::from_value(sequence_json())?)
            .await?;
        let inst = engine
            .create_instance(
                seq_id,
                CreateInstanceOptions {
                    next_fire_at: Some(chrono::Utc::now() + DELAY),
                    ..Default::default()
                },
            )
            .await?;
        engine.start();
        println!("created instance {inst}, scheduled to fire in {DELAY:?}");
        tokio::time::sleep(Duration::from_secs(1)).await;
        println!("exiting before it runs (simulated crash) — run me again to watch it resume");
        // Intentionally no graceful shutdown: the process just goes away.
        // Everything the workflow needs is already on disk.
    }

    Ok(())
}
