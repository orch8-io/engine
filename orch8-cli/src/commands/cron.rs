use anyhow::Result;
use clap::Subcommand;
use reqwest::Client;
use serde_json::Value;
use uuid::Uuid;

use crate::{humanize_time, print_response, print_table, val_str, OutputFormat};

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

pub async fn run(client: &Client, base: &str, cmd: CronCmd, format: OutputFormat) -> Result<()> {
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

            match format {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&body)?);
                }
                OutputFormat::Table => {
                    if let Some(arr) = body.as_array() {
                        if arr.is_empty() {
                            println!("No cron schedules found.");
                        } else {
                            let rows: Vec<Vec<String>> = arr
                                .iter()
                                .map(|v| {
                                    vec![
                                        val_str(v, "id"),
                                        val_str(v, "tenant_id"),
                                        val_str(v, "cron_expr"),
                                        val_str(v, "enabled"),
                                        humanize_time(&val_str(v, "next_fire_at")),
                                    ]
                                })
                                .collect();
                            print_table(
                                &["id", "tenant", "expression", "enabled", "next_fire"],
                                &rows,
                            );
                        }
                    } else {
                        println!("{}", serde_json::to_string_pretty(&body)?);
                    }
                }
            }
        }
        CronCmd::Get { id } => {
            let resp = client.get(format!("{base}/cron/{id}")).send().await?;
            print_response(resp, format).await?;
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
