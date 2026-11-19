use anyhow::{Context, Result};
use orch8_types::signal::SignalType;
use reqwest::Client;
use serde_json::Value;
use uuid::Uuid;

use crate::{OutputFormat, print_response};

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
    let signal_type = if signal_type.starts_with("custom:") {
        serde_json::to_value(signal_type.parse::<SignalType>()?)?
    } else {
        Value::String(signal_type)
    };

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::test_support::mock_api_with_responses;

    #[tokio::test]
    async fn custom_signal_uses_the_wire_variant() {
        let api = mock_api_with_responses(vec![(reqwest::StatusCode::CREATED, "{}".into())]).await;
        run(
            &Client::new(),
            &api.base,
            Uuid::now_v7(),
            "custom:human_input:approve_inspection".into(),
            Some(r#"{"value":"approved"}"#.into()),
            OutputFormat::Json,
        )
        .await
        .unwrap();
        let requests = api.log.snapshot();
        assert_eq!(
            requests[0].body["signal_type"],
            serde_json::json!({"custom": "human_input:approve_inspection"})
        );
        assert_eq!(requests[0].body["payload"]["value"], "approved");
    }
}
