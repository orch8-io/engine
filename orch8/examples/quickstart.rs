//! Quickstart: file-backed `SQLite`, one custom handler, a two-step sequence
//! run to completion.
//!
//! ```sh
//! cargo run -p orch8 --example quickstart
//! ```

use std::time::Duration;

use orch8::{Engine, InstanceState, Storage};

/// A two-step workflow: a custom `charge_card` step, then a `send_receipt`
/// step whose params are templated from the first step's output.
fn sequence_json() -> serde_json::Value {
    serde_json::json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "default",
        "namespace": "default",
        "name": "payment",
        "version": 1,
        "blocks": [
            {
                "type": "step",
                "id": "charge",
                "handler": "charge_card",
                "params": { "amount_cents": 4_200 }
            },
            {
                "type": "step",
                "id": "receipt",
                "handler": "send_receipt",
                "params": { "summary": "charged {{outputs.charge.amount_cents}} cents (ref {{outputs.charge.reference}})" }
            }
        ],
        "created_at": chrono::Utc::now().to_rfc3339()
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = std::env::temp_dir().join("orch8-quickstart.db");

    let engine = Engine::builder()
        .storage(Storage::sqlite(&db_path))
        .handler("charge_card", |ctx: orch8::StepContext| async move {
            let amount = ctx.params.get("amount_cents").cloned().unwrap_or_default();
            println!("[charge_card] charging {amount} cents");
            Ok(serde_json::json!({
                "charged": true,
                "amount_cents": amount,
                "reference": "ch_12345",
            }))
        })
        .handler("send_receipt", |ctx: orch8::StepContext| async move {
            let summary = ctx
                .params
                .get("summary")
                .and_then(|v| v.as_str())
                .unwrap_or("(no summary)")
                .to_string();
            println!("[send_receipt] {summary}");
            Ok(serde_json::json!({ "receipt_sent": true, "summary": summary }))
        })
        .build()
        .await?;

    // Background tick loop on this process's tokio runtime.
    engine.start();

    let seq_id = engine
        .upsert_sequence(serde_json::from_value(sequence_json())?)
        .await?;
    let inst = engine
        .create_instance(seq_id, orch8::CreateInstanceOptions::default())
        .await?;
    println!("created instance {inst}");

    // Poll until the instance reaches a terminal state.
    loop {
        let snapshot = engine.get_instance(inst).await?;
        match snapshot.state {
            InstanceState::Completed => {
                println!(
                    "instance completed; final context.data = {}",
                    snapshot.context.data
                );
                break;
            }
            InstanceState::Failed | InstanceState::Cancelled => {
                println!("instance ended in {:?}", snapshot.state);
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }

    engine.shutdown().await;
    Ok(())
}
