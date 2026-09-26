//! `orch8 job` — enqueue and manage background jobs (`/jobs`).

use anyhow::{Context, Result};
use clap::Subcommand;
use reqwest::Client;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{OutputFormat, colorize_state, humanize_time, print_response, print_table, val_str};

#[derive(Subcommand)]
pub enum JobCmd {
    /// Enqueue a job for a handler (no sequence needed).
    Enqueue {
        /// Handler name (built-in or served by an external worker).
        handler: String,
        /// JSON object passed to the handler as params.
        #[arg(long, default_value = "{}")]
        payload: String,
        /// Worker queue name.
        #[arg(long)]
        queue: Option<String>,
        /// Priority: low, normal, high, critical.
        #[arg(long)]
        priority: Option<String>,
        /// Total attempts including the first (enables retries when > 1).
        #[arg(long)]
        max_attempts: Option<u32>,
        /// Initial retry backoff in milliseconds (default 1000).
        #[arg(long, default_value_t = 1000)]
        backoff_ms: u64,
        /// Maximum retry backoff in milliseconds.
        #[arg(long)]
        max_backoff_ms: Option<u64>,
        /// Delay before the first run, in milliseconds.
        #[arg(long, conflicts_with = "run_at")]
        delay_ms: Option<u64>,
        /// Absolute first-run time (RFC 3339).
        #[arg(long)]
        run_at: Option<String>,
        /// Idempotency key (re-enqueueing returns the existing job).
        #[arg(long)]
        idempotency_key: Option<String>,
        /// JSON object of caller metadata.
        #[arg(long)]
        metadata: Option<String>,
    },
    /// Show one job.
    Get { id: Uuid },
    /// List jobs (newest first).
    List {
        #[arg(long)]
        handler: Option<String>,
        /// scheduled | running | completed | failed | cancelled | `dead_lettered`
        #[arg(long)]
        status: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: u32,
        /// `next_cursor` from a previous page.
        #[arg(long)]
        cursor: Option<String>,
    },
    /// Cancel a job.
    Cancel { id: Uuid },
}

fn parse_object(raw: &str, what: &str) -> Result<Value> {
    let value: Value =
        serde_json::from_str(raw).with_context(|| format!("--{what} must be valid JSON"))?;
    anyhow::ensure!(value.is_object(), "--{what} must be a JSON object");
    Ok(value)
}

/// Build the `POST /jobs` body from CLI flags.
#[allow(clippy::too_many_arguments)]
pub(crate) fn enqueue_body(
    handler: &str,
    payload: &str,
    queue: Option<&str>,
    priority: Option<&str>,
    max_attempts: Option<u32>,
    backoff_ms: u64,
    max_backoff_ms: Option<u64>,
    delay_ms: Option<u64>,
    run_at: Option<&str>,
    idempotency_key: Option<&str>,
    metadata: Option<&str>,
) -> Result<Value> {
    let mut body = json!({
        "handler": handler,
        "payload": parse_object(payload, "payload")?,
    });
    if let Some(q) = queue {
        body["queue"] = json!(q);
    }
    if let Some(p) = priority {
        body["priority"] = json!(p);
    }
    if let Some(n) = max_attempts {
        let mut retry = json!({"max_attempts": n, "initial_backoff_ms": backoff_ms});
        if let Some(max) = max_backoff_ms {
            retry["max_backoff_ms"] = json!(max);
        }
        body["retry"] = retry;
    }
    if let Some(d) = delay_ms {
        body["delay_ms"] = json!(d);
    }
    if let Some(at) = run_at {
        body["run_at"] = json!(at);
    }
    if let Some(k) = idempotency_key {
        body["idempotency_key"] = json!(k);
    }
    if let Some(m) = metadata {
        body["metadata"] = parse_object(m, "metadata")?;
    }
    Ok(body)
}

pub async fn run(client: &Client, base: &str, cmd: JobCmd, format: OutputFormat) -> Result<()> {
    match cmd {
        JobCmd::Enqueue {
            handler,
            payload,
            queue,
            priority,
            max_attempts,
            backoff_ms,
            max_backoff_ms,
            delay_ms,
            run_at,
            idempotency_key,
            metadata,
        } => {
            let body = enqueue_body(
                &handler,
                &payload,
                queue.as_deref(),
                priority.as_deref(),
                max_attempts,
                backoff_ms,
                max_backoff_ms,
                delay_ms,
                run_at.as_deref(),
                idempotency_key.as_deref(),
                metadata.as_deref(),
            )?;
            let resp = client
                .post(format!("{base}/jobs"))
                .json(&body)
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        JobCmd::Get { id } => {
            let resp = client.get(format!("{base}/jobs/{id}")).send().await?;
            print_response(resp, format).await?;
        }
        JobCmd::List {
            handler,
            status,
            limit,
            cursor,
        } => {
            let limit = limit.to_string();
            let mut params = vec![("limit", limit.as_str())];
            if let Some(h) = &handler {
                params.push(("handler", h.as_str()));
            }
            if let Some(s) = &status {
                params.push(("status", s.as_str()));
            }
            if let Some(c) = &cursor {
                params.push(("cursor", c.as_str()));
            }
            let resp = client
                .get(format!("{base}/jobs"))
                .query(&params)
                .send()
                .await?;
            if !resp.status().is_success() {
                return print_response(resp, format).await;
            }
            let page: Value = resp.json().await?;
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&page)?),
                OutputFormat::Table => {
                    let items = page["items"].as_array().cloned().unwrap_or_default();
                    if items.is_empty() {
                        println!("No jobs found.");
                    } else {
                        let rows: Vec<Vec<String>> = items
                            .iter()
                            .map(|j| {
                                vec![
                                    val_str(j, "id"),
                                    val_str(j, "handler"),
                                    colorize_state(&val_str(j, "status")),
                                    val_str(j, "attempts"),
                                    humanize_time(&val_str(j, "created_at")),
                                ]
                            })
                            .collect();
                        print_table(&["id", "handler", "status", "attempts", "created"], &rows);
                    }
                    if let Some(next) = page["next_cursor"].as_str() {
                        println!("\nMore results: --cursor {next}");
                    }
                }
            }
        }
        JobCmd::Cancel { id } => {
            crate::confirm_destructive(&format!("Cancel job {id}?"))?;
            let resp = client.delete(format!("{base}/jobs/{id}")).send().await?;
            print_response(resp, format).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_body_maps_flags_to_contract() {
        let body = enqueue_body(
            "send_email",
            r#"{"to":"a@b.c"}"#,
            Some("mail"),
            Some("high"),
            Some(3),
            500,
            Some(10_000),
            Some(2000),
            None,
            Some("k1"),
            Some(r#"{"src":"cli"}"#),
        )
        .unwrap();
        assert_eq!(
            body,
            json!({
                "handler": "send_email",
                "payload": {"to": "a@b.c"},
                "queue": "mail",
                "priority": "high",
                "retry": {"max_attempts": 3, "initial_backoff_ms": 500, "max_backoff_ms": 10000},
                "delay_ms": 2000,
                "idempotency_key": "k1",
                "metadata": {"src": "cli"}
            })
        );
    }

    #[test]
    fn enqueue_body_rejects_non_object_payload() {
        let err = enqueue_body(
            "h", "[1]", None, None, None, 0, None, None, None, None, None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("JSON object"));
    }

    #[tokio::test]
    async fn list_sends_filters_as_query_params() {
        let api = crate::commands::test_support::mock_api_with_responses(vec![(
            reqwest::StatusCode::OK,
            r#"{"items":[],"has_more":false}"#.into(),
        )])
        .await;
        run(
            &Client::new(),
            &api.base,
            JobCmd::List {
                handler: Some("h".into()),
                status: Some("failed".into()),
                limit: 5,
                cursor: None,
            },
            OutputFormat::Json,
        )
        .await
        .unwrap();
        let req = &api.log.snapshot()[0];
        assert!(req.uri.starts_with("/jobs?"), "{}", req.uri);
        assert!(req.uri.contains("handler=h"));
        assert!(req.uri.contains("status=failed"));
        assert!(req.uri.contains("limit=5"));
    }
}
