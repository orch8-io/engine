use anyhow::Result;
use clap::Subcommand;
use reqwest::Client;
use serde_json::Value;
use tabled::{Table, Tabled};
use uuid::Uuid;

use crate::{print_response, val_str};

#[derive(Subcommand)]
pub enum CronCmd {
    /// List cron schedules.
    List {
        #[arg(long)]
        tenant_id: Option<String>,
    },
    /// Get a cron schedule by ID.
    Get { id: Uuid },
    /// Delete a cron schedule.
    Delete { id: Uuid },
}

#[derive(Tabled)]
struct CronRow {
    id: String,
    tenant: String,
    expression: String,
    enabled: String,
    next_fire: String,
}

pub async fn run(client: &Client, base: &str, cmd: CronCmd) -> Result<()> {
    match cmd {
        CronCmd::List { tenant_id } => {
            let mut params = vec![];
            if let Some(t) = &tenant_id {
                params.push(("tenant_id", t.as_str()));
            }
            let resp = client
                .get(format!("{base}/cron"))
                .query(&params)
                .send()
                .await?;
            let body: Value = resp.json().await?;

            if let Some(arr) = body.as_array() {
                if arr.is_empty() {
                    println!("No cron schedules found.");
                } else {
                    let rows: Vec<CronRow> = arr
                        .iter()
                        .map(|v| CronRow {
                            id: val_str(v, "id"),
                            tenant: val_str(v, "tenant_id"),
                            expression: val_str(v, "cron_expression"),
                            enabled: val_str(v, "enabled"),
                            next_fire: val_str(v, "next_fire_at"),
                        })
                        .collect();
                    println!("{}", Table::new(rows));
                }
            } else {
                println!("{}", serde_json::to_string_pretty(&body)?);
            }
        }
        CronCmd::Get { id } => {
            let resp = client.get(format!("{base}/cron/{id}")).send().await?;
            print_response(resp).await?;
        }
        CronCmd::Delete { id } => {
            let resp = client.delete(format!("{base}/cron/{id}")).send().await?;
            if resp.status().is_success() {
                println!("Deleted cron schedule {id}");
            } else {
                let status = resp.status();
                let body: Value = resp.json().await.unwrap_or(Value::Null);
                anyhow::bail!("{status}: {body}");
            }
        }
    }
    Ok(())
}
