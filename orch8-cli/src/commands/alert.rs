//! `orch8 alert` — built-in alert rule management (`/alerts/rules`).

use anyhow::{Context, Result};
use clap::Subcommand;
use reqwest::Client;
use serde_json::Value;
use uuid::Uuid;

use crate::{OutputFormat, print_response, print_table, val_str};

#[derive(Subcommand)]
pub enum AlertCmd {
    /// List alert rules for the current tenant.
    List {
        #[arg(long)]
        tenant_id: Option<String>,
    },
    /// Show one rule with its evaluator state.
    Get { id: Uuid },
    /// Create a rule from a JSON file:
    /// `{"name","condition":{"kind":…},"destination":{"type":…},"cooldown_secs"}`.
    Create {
        #[arg(long)]
        file: std::path::PathBuf,
    },
    /// Replace a rule from a JSON file (same shape as `create`).
    Update {
        id: Uuid,
        #[arg(long)]
        file: std::path::PathBuf,
    },
    /// Delete a rule.
    Delete { id: Uuid },
}

fn read_json(path: &std::path::Path) -> Result<Value> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parse {}", path.display()))
}

pub async fn run(client: &Client, base: &str, cmd: AlertCmd, format: OutputFormat) -> Result<()> {
    match cmd {
        AlertCmd::List { tenant_id } => {
            let mut params = vec![];
            if let Some(t) = &tenant_id {
                params.push(("tenant_id", t.as_str()));
            }
            let resp = client
                .get(format!("{base}/alerts/rules"))
                .query(&params)
                .send()
                .await?;
            if !resp.status().is_success() || matches!(format, OutputFormat::Json) {
                return print_response(resp, format).await;
            }
            let body: Value = resp.json().await?;
            let rows: Vec<Vec<String>> = body
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .map(|v| {
                            vec![
                                val_str(v, "id"),
                                val_str(v, "name"),
                                v.pointer("/condition/kind")
                                    .and_then(Value::as_str)
                                    .unwrap_or("-")
                                    .to_string(),
                                v.pointer("/destination/type")
                                    .and_then(Value::as_str)
                                    .unwrap_or("-")
                                    .to_string(),
                                val_str(v, "enabled"),
                            ]
                        })
                        .collect()
                })
                .unwrap_or_default();
            if rows.is_empty() {
                println!("No alert rules found.");
            } else {
                print_table(
                    &["id", "name", "condition", "destination", "enabled"],
                    &rows,
                );
            }
        }
        AlertCmd::Get { id } => {
            let resp = client
                .get(format!("{base}/alerts/rules/{id}"))
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        AlertCmd::Create { file } => {
            let body = read_json(&file)?;
            let resp = client
                .post(format!("{base}/alerts/rules"))
                .json(&body)
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        AlertCmd::Update { id, file } => {
            let body = read_json(&file)?;
            let resp = client
                .put(format!("{base}/alerts/rules/{id}"))
                .json(&body)
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        AlertCmd::Delete { id } => {
            crate::confirm_destructive(&format!("Delete alert rule {id}?"))?;
            let resp = client
                .delete(format!("{base}/alerts/rules/{id}"))
                .send()
                .await?;
            if resp.status().is_success() {
                println!("Deleted alert rule {id}");
            } else {
                let status = resp.status();
                let body: Value = resp.json().await.unwrap_or(Value::Null);
                anyhow::bail!("{status}: {body}");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_posts_file_body_and_delete_uses_rule_path() {
        let api = crate::commands::test_support::mock_api_with_responses(vec![
            (reqwest::StatusCode::CREATED, r#"{"id":"x"}"#.into()),
            (reqwest::StatusCode::NO_CONTENT, String::new()),
        ])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rule.json");
        std::fs::write(
            &path,
            r#"{"name":"dlq","condition":{"kind":"dlq_growth","threshold":5},"destination":{"type":"pagerduty","routing_key_ref":"credentials://pd"}}"#,
        )
        .unwrap();
        run(
            &Client::new(),
            &api.base,
            AlertCmd::Create { file: path },
            OutputFormat::Json,
        )
        .await
        .unwrap();
        let id = Uuid::now_v7();
        run(
            &Client::new(),
            &api.base,
            AlertCmd::Delete { id },
            OutputFormat::Json,
        )
        .await
        .unwrap();
        let log = api.log.snapshot();
        assert_eq!(log[0].method, reqwest::Method::POST);
        assert_eq!(log[0].uri, "/alerts/rules");
        assert_eq!(log[0].body["condition"]["kind"], "dlq_growth");
        assert_eq!(log[1].method, reqwest::Method::DELETE);
        assert_eq!(log[1].uri, format!("/alerts/rules/{id}"));
    }
}
