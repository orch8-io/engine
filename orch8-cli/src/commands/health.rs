use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;

pub async fn run(client: &Client, base: &str) -> Result<()> {
    let resp = client
        .get(format!("{base}/health/ready"))
        .send()
        .await
        .context("failed to reach server")?;
    let status = resp.status();
    let body: Value = resp.json().await.unwrap_or(Value::Null);
    if status.is_success() {
        println!("OK {}", serde_json::to_string_pretty(&body)?);
    } else {
        anyhow::bail!("Health check failed: {status} {body}");
    }
    Ok(())
}
