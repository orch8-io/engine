use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;

pub async fn run(client: &Client, base: &str) -> Result<()> {
    let root = base.strip_suffix("/api/v1").unwrap_or(base);
    let resp = client
        .get(format!("{root}/health/ready"))
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

#[cfg(test)]
mod tests {
    #[test]
    fn canonical_api_base_maps_health_to_operational_root() {
        let base = "http://127.0.0.1:8080/api/v1";
        assert_eq!(
            base.strip_suffix("/api/v1").unwrap_or(base),
            "http://127.0.0.1:8080"
        );
    }
}
