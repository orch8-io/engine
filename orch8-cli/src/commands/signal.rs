use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use uuid::Uuid;

use crate::{print_response, OutputFormat};

pub async fn run(
    client: &Client,
    base: &str,
    instance_id: Uuid,
    signal_type: String,
    payload: Option<String>,
    format: OutputFormat,
) -> Result<()> {
    let payload_val: Value = payload
        .map(|p| serde_json::from_str(&p))
        .transpose()
        .context("invalid JSON payload")?
        .unwrap_or(Value::Null);

    let resp = client
        .post(format!("{base}/instances/{instance_id}/signals"))
        .json(&serde_json::json!({
            "signal_type": signal_type,
            "payload": payload_val,
        }))
        .send()
        .await?;
    print_response(resp, format).await?;
    Ok(())
}
