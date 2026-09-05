use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;

use crate::OutputFormat;

pub async fn run(client: &Client, base: &str, format: OutputFormat) -> Result<()> {
    let root = base.strip_suffix("/api/v1").unwrap_or(base);
    let resp = client
        .get(format!("{root}/health/ready"))
        .send()
        .await
        .context("failed to reach server")?;
    let status = resp.status();
    let body: Value = resp
        .json()
        .await
        .with_context(|| format!("invalid health response body (HTTP {status})"))?;
    if status.is_success() {
        match format {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&body)?),
            OutputFormat::Table => println!("OK {}", serde_json::to_string_pretty(&body)?),
        }
    } else {
        anyhow::bail!("Health check failed: {status} {body}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn malformed_success_response_is_not_healthy() {
        let api = crate::commands::test_support::mock_api_with_responses(vec![(
            reqwest::StatusCode::OK,
            "not JSON".into(),
        )])
        .await;
        let error = super::run(
            &reqwest::Client::new(),
            &api.base,
            crate::OutputFormat::Json,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("invalid health response body"));
        assert_eq!(api.log.snapshot()[0].uri, "/health/ready");
    }

    #[test]
    fn canonical_api_base_maps_health_to_operational_root() {
        let base = "http://127.0.0.1:8080/api/v1";
        assert_eq!(
            base.strip_suffix("/api/v1").unwrap_or(base),
            "http://127.0.0.1:8080"
        );
    }
}
