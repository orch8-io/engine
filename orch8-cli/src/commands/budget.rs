//! `orch8 budget` — tenant LLM spend budgets (admin).

use anyhow::{Result, bail};
use clap::{Subcommand, ValueEnum};
use reqwest::Client;
use serde_json::json;
use uuid::Uuid;

use crate::{OutputFormat, print_response};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Period {
    Daily,
    Monthly,
}

impl Period {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Daily => "daily",
            Self::Monthly => "monthly",
        }
    }
}

#[derive(Subcommand)]
pub enum BudgetCmd {
    /// Create (or with --id, replace) a tenant budget. Requires the root key.
    Set {
        /// Tenant the budget governs.
        #[arg(long)]
        tenant: String,
        #[arg(long, value_enum)]
        period: Period,
        /// Spend limit in USD per period (estimated list-price cost).
        #[arg(long)]
        limit_usd: f64,
        /// Only count models starting with this prefix (e.g. `gpt-5`).
        #[arg(long)]
        model: Option<String>,
        /// Comma-separated alert thresholds in percent (default 50,80,100).
        #[arg(long, value_delimiter = ',')]
        thresholds: Option<Vec<u8>>,
        /// Alert only: never block dispatches at 100%.
        #[arg(long)]
        soft: bool,
        /// Replace this existing budget instead of creating a new one.
        #[arg(long)]
        id: Option<Uuid>,
    },
    /// List budgets with current-period spend, percent used and state.
    List {
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Delete a budget. Requires the root key.
    Delete {
        id: Uuid,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Show `budget.threshold_crossed` alert records, newest first.
    Alerts {
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long, default_value = "100")]
        limit: u32,
    },
}

fn tenant_query(tenant: Option<String>) -> Vec<(&'static str, String)> {
    tenant.map(|t| vec![("tenant_id", t)]).unwrap_or_default()
}

pub async fn run(client: &Client, base: &str, cmd: BudgetCmd, format: OutputFormat) -> Result<()> {
    let resp = match cmd {
        BudgetCmd::Set {
            tenant,
            period,
            limit_usd,
            model,
            thresholds,
            soft,
            id,
        } => {
            if !limit_usd.is_finite() || limit_usd <= 0.0 {
                bail!("--limit-usd must be a positive number");
            }
            let body = json!({
                "tenant_id": tenant,
                "period": period.as_str(),
                "limit_usd": limit_usd,
                "model": model,
                "thresholds": thresholds,
                "hard_cap": !soft,
            });
            match id {
                Some(id) => client.put(format!("{base}/budgets/{id}")).json(&body),
                None => client.post(format!("{base}/budgets")).json(&body),
            }
            .send()
            .await?
        }
        BudgetCmd::List { tenant } => {
            client
                .get(format!("{base}/budgets"))
                .query(&tenant_query(tenant))
                .send()
                .await?
        }
        BudgetCmd::Delete { id, tenant } => {
            client
                .delete(format!("{base}/budgets/{id}"))
                .query(&tenant_query(tenant))
                .send()
                .await?
        }
        BudgetCmd::Alerts { tenant, limit } => {
            let mut q = tenant_query(tenant);
            q.push(("limit", limit.to_string()));
            client
                .get(format!("{base}/budgets/alerts"))
                .query(&q)
                .send()
                .await?
        }
    };
    print_response(resp, format).await
}

#[cfg(test)]
mod tests {
    use axum::http::Method;

    use super::*;
    use crate::commands::test_support::mock_api;

    #[tokio::test]
    async fn budget_commands_use_the_expected_contract() {
        let server = mock_api().await;
        let client = Client::new();
        run(
            &client,
            &server.base,
            BudgetCmd::Set {
                tenant: "acme".into(),
                period: Period::Monthly,
                limit_usd: 250.0,
                model: Some("gpt-5".into()),
                thresholds: Some(vec![75, 100]),
                soft: true,
                id: None,
            },
            OutputFormat::Json,
        )
        .await
        .unwrap();
        run(
            &client,
            &server.base,
            BudgetCmd::Alerts {
                tenant: Some("acme".into()),
                limit: 5,
            },
            OutputFormat::Json,
        )
        .await
        .unwrap();
        let id = Uuid::now_v7();
        run(
            &client,
            &server.base,
            BudgetCmd::Delete {
                id,
                tenant: Some("acme".into()),
            },
            OutputFormat::Json,
        )
        .await
        .unwrap();

        let reqs = server.log.snapshot();
        assert_eq!(reqs[0].method, Method::POST);
        assert_eq!(reqs[0].uri, "/budgets");
        assert_eq!(reqs[0].body["period"], "monthly");
        assert_eq!(reqs[0].body["hard_cap"], false);
        assert_eq!(reqs[0].body["thresholds"], json!([75, 100]));
        assert_eq!(reqs[1].uri, "/budgets/alerts?tenant_id=acme&limit=5");
        assert_eq!(reqs[2].method, Method::DELETE);
        assert_eq!(reqs[2].uri, format!("/budgets/{id}?tenant_id=acme"));
    }

    #[tokio::test]
    async fn set_rejects_non_positive_limits_before_network_io() {
        let err = run(
            &Client::new(),
            "http://127.0.0.1:1",
            BudgetCmd::Set {
                tenant: "t".into(),
                period: Period::Daily,
                limit_usd: 0.0,
                model: None,
                thresholds: None,
                soft: false,
                id: None,
            },
            OutputFormat::Json,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("positive"));
    }
}
