//! `orch8 share` — public progress links for an instance.

use anyhow::Result;
use clap::Subcommand;
use reqwest::Client;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{OutputFormat, print_response};

#[derive(Subcommand)]
pub enum ShareCmd {
    /// Create a public progress link. The token is printed once.
    Create {
        instance_id: Uuid,
        /// Link lifetime in seconds (default 7 days, max 90 days).
        #[arg(long)]
        expires_in: Option<i64>,
        /// Top-level `context.data` key to expose (repeatable). Default: none.
        #[arg(long = "field")]
        fields: Vec<String>,
    },
    /// List links for an instance (tokens are never shown again).
    List { instance_id: Uuid },
    /// Revoke a link.
    Revoke { instance_id: Uuid, share_id: Uuid },
}

pub async fn run(client: &Client, base: &str, cmd: ShareCmd, format: OutputFormat) -> Result<()> {
    match cmd {
        ShareCmd::Create {
            instance_id,
            expires_in,
            fields,
        } => {
            let mut body = json!({ "allowed_fields": fields });
            if let Some(ttl) = expires_in {
                body["expires_in_secs"] = json!(ttl);
            }
            let resp = client
                .post(format!("{base}/instances/{instance_id}/share"))
                .json(&body)
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        ShareCmd::List { instance_id } => {
            let resp = client
                .get(format!("{base}/instances/{instance_id}/shares"))
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        ShareCmd::Revoke {
            instance_id,
            share_id,
        } => {
            crate::confirm_destructive(&format!("Revoke progress link {share_id}?"))?;
            let resp = client
                .delete(format!("{base}/instances/{instance_id}/share/{share_id}"))
                .send()
                .await?;
            if resp.status().is_success() {
                println!("Revoked progress link {share_id}");
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
    async fn create_sends_fields_and_ttl() {
        let api = crate::commands::test_support::mock_api_with_responses(vec![(
            reqwest::StatusCode::CREATED,
            r#"{"token":"t"}"#.into(),
        )])
        .await;
        let id = Uuid::now_v7();
        run(
            &Client::new(),
            &api.base,
            ShareCmd::Create {
                instance_id: id,
                expires_in: Some(3600),
                fields: vec!["order_id".into()],
            },
            OutputFormat::Json,
        )
        .await
        .unwrap();
        let log = api.log.snapshot();
        assert_eq!(log[0].uri, format!("/instances/{id}/share"));
        assert_eq!(log[0].body["expires_in_secs"], 3600);
        assert_eq!(log[0].body["allowed_fields"][0], "order_id");
    }
}
