//! Current output projections; history remains an append-only log.

use std::collections::HashMap;

use orch8_types::output::BlockOutput;

// The same key stream and eligibility predicate serve both dialects. Keep
// current business values separate from current diagnostic/retry values.
macro_rules! key_stream {
    ($filter:literal) => {
        concat!(
            r"
WITH RECURSIVE block_keys(block_id) AS (
    SELECT (SELECT b.block_id FROM block_outputs b
    WHERE b.instance_id = $1 AND ",
            $filter,
            r"
      AND substr(b.block_id, 1, 13) <> '_interceptor:'
      AND substr(b.block_id, 1, 5) <> '_sla:'
    ORDER BY b.block_id, b.created_at DESC, b.id DESC LIMIT 1)
    UNION ALL
    SELECT (
        SELECT b.block_id FROM block_outputs b
        WHERE b.instance_id = $1 AND b.block_id > k.block_id AND ",
            $filter,
            r"
          AND substr(b.block_id, 1, 13) <> '_interceptor:'
          AND substr(b.block_id, 1, 5) <> '_sla:'
        ORDER BY b.block_id, b.created_at DESC, b.id DESC LIMIT 1
    ) FROM block_keys k WHERE k.block_id IS NOT NULL
)"
        )
    };
}

macro_rules! sqlite_query {
    ($filter:literal) => {
        concat!(
            key_stream!($filter),
            r"
SELECT o.* FROM block_keys k CROSS JOIN block_outputs o
WHERE o.instance_id = $1 AND o.id = (
    SELECT b.id FROM block_outputs b
    WHERE b.instance_id = $1 AND b.block_id = k.block_id AND ",
            $filter,
            r"
      AND substr(b.block_id, 1, 13) <> '_interceptor:'
      AND substr(b.block_id, 1, 5) <> '_sla:'
    ORDER BY b.created_at DESC, b.id DESC LIMIT 1
)
AND k.block_id IS NOT NULL ORDER BY o.created_at, o.id
"
        )
    };
}

#[cfg(feature = "postgres")]
macro_rules! postgres_query {
    ($filter:literal) => {
        concat!(
            key_stream!($filter),
            r"
SELECT o.* FROM block_keys k CROSS JOIN LATERAL (
    SELECT b.* FROM block_outputs b
    WHERE b.instance_id = $1 AND b.block_id = k.block_id AND ",
            $filter,
            r"
      AND substr(b.block_id, 1, 13) <> '_interceptor:'
      AND substr(b.block_id, 1, 5) <> '_sla:'
    ORDER BY b.created_at DESC, b.id DESC LIMIT 1
) o WHERE k.block_id IS NOT NULL ORDER BY o.created_at, o.id
"
        )
    };
}

pub(crate) const SQLITE_QUERY: &str =
    sqlite_query!("(b.output_ref IS NULL OR b.output_ref NOT IN ('__retry__', '__in_progress__'))");
pub(crate) const SQLITE_DIAGNOSTIC_QUERY: &str =
    sqlite_query!("(b.output_ref IS NULL OR b.output_ref <> '__in_progress__')");
#[cfg(feature = "postgres")]
pub(crate) const POSTGRES_QUERY: &str = postgres_query!(
    "(b.output_ref IS NULL OR b.output_ref NOT IN ('__retry__', '__in_progress__'))"
);
#[cfg(feature = "postgres")]
pub(crate) const POSTGRES_DIAGNOSTIC_QUERY: &str =
    postgres_query!("(b.output_ref IS NULL OR b.output_ref <> '__in_progress__')");

/// Compatibility fallback for third-party backends without a direct query.
pub(crate) fn from_history(outputs: Vec<BlockOutput>) -> Vec<BlockOutput> {
    project_history(outputs, false)
}

pub(crate) fn diagnostic_from_history(outputs: Vec<BlockOutput>) -> Vec<BlockOutput> {
    project_history(outputs, true)
}

fn project_history(mut outputs: Vec<BlockOutput>, retry_diagnostics: bool) -> Vec<BlockOutput> {
    outputs.retain(|output| {
        output.output_ref.as_deref() != Some("__in_progress__")
            && (retry_diagnostics || output.output_ref.as_deref() != Some("__retry__"))
            && !output.block_id.as_str().starts_with("_interceptor:")
            && !output.block_id.as_str().starts_with("_sla:")
    });
    outputs.sort_unstable_by_key(|output| (output.created_at, output.id));
    let mut latest = HashMap::new();
    for output in outputs {
        latest.insert(output.block_id.clone(), output);
    }
    let mut outputs: Vec<_> = latest.into_values().collect();
    outputs.sort_unstable_by_key(|output| (output.created_at, output.id));
    outputs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{
        backends::RawPool,
        fixtures::{InstanceBuilder, SequenceBuilder},
    };
    use chrono::{Duration, Utc};
    use orch8_types::ids::BlockId;
    #[cfg(feature = "postgres")]
    use serde_json::Value;
    use serde_json::json;
    use uuid::Uuid;

    #[cfg(feature = "postgres")]
    fn visits(plan: &Value) -> (f64, bool) {
        let mut rows = if plan["Relation Name"]
            .as_str()
            .is_some_and(|name| name.starts_with("block_outputs"))
        {
            (plan["Actual Rows"].as_f64().unwrap_or(0.0)
                + plan["Rows Removed by Filter"].as_f64().unwrap_or(0.0))
                * plan["Actual Loops"].as_f64().unwrap_or(0.0)
        } else {
            0.0
        };
        let mut indexed = plan["Index Name"].is_string()
            && plan["Index Cond"]
                .as_str()
                .is_some_and(|condition| condition.contains("block_id"));
        for child in plan["Plans"].as_array().into_iter().flatten() {
            let (child_rows, child_indexed) = visits(child);
            rows += child_rows;
            indexed |= child_indexed;
        }
        (rows, indexed)
    }

    #[test]
    fn current_output_fallback_orders_unsorted_backend_history() {
        let at = Utc::now();
        let instance_id = orch8_types::ids::InstanceId::new();
        let row = |id, reference: Option<&str>| BlockOutput {
            id: Uuid::from_u128(id),
            instance_id,
            block_id: BlockId::new("work"),
            output: json!(id.to_string()),
            output_ref: reference.map(str::to_owned),
            output_size: 10,
            attempt: 0,
            created_at: at,
        };
        let rows = vec![row(2, None), row(3, Some("__retry__")), row(1, None)];
        let projected = from_history(rows.clone());
        let diagnostics = diagnostic_from_history(rows);
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].id, Uuid::from_u128(3));
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].id, Uuid::from_u128(2));
        assert!(from_history(vec![]).is_empty());
    }

    crate::backend_test!(kinds = [sqlite, postgres], async fn current_output_query_seeks_past_loop_history(b) {
        let sequence = SequenceBuilder::new("projection-plan").build();
        b.storage.create_sequence(&sequence).await.unwrap();
        let instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
        b.storage.create_instance(&instance).await.unwrap();
        let at = Utc::now();
        for n in 0..512 {
            let row = BlockOutput {
                id: Uuid::now_v7(), instance_id: instance.id,
                block_id: BlockId::new(if n % 2 == 0 { "a" } else { "b" }),
                output: json!({"iteration": n, "padding": "x".repeat(4096)}),
                output_ref: None, output_size: 4096, attempt: 0,
                created_at: at + Duration::microseconds(i64::from(n)),
            };
            b.storage.save_block_output(&row).await.unwrap();
            let mut trace = row;
            trace.id = Uuid::now_v7();
            trace.block_id = BlockId::new("_interceptor:trace");
            b.storage.save_block_output(&trace).await.unwrap();
            let mut retry = trace;
            retry.id = Uuid::now_v7();
            retry.block_id = BlockId::new(if n % 2 == 0 { "a" } else { "b" });
            retry.output_ref = Some("__retry__".into());
            retry.created_at += Duration::seconds(1);
            b.storage.save_block_output(&retry).await.unwrap();
        }
        let projected = b.storage.get_latest_completed_outputs(instance.id).await.unwrap();
        assert_eq!(projected.len(), 2);
        assert_eq!(projected[0].output["iteration"], 510);
        assert_eq!(projected[1].output["iteration"], 511);
        let diagnostics = b.storage.get_latest_diagnostic_outputs(instance.id).await.unwrap();
        assert_eq!(diagnostics.len(), 2);
        assert!(projected.iter().all(|row| row.output_ref.is_none()));
        assert!(diagnostics.iter().all(|row| row.output_ref.as_deref() == Some("__retry__")));
        match &b.raw {
            RawPool::Sqlite(pool) => {
                for (query, index) in [(SQLITE_QUERY, "idx_block_outputs_current"), (SQLITE_DIAGNOSTIC_QUERY, "idx_block_outputs_diagnostics")] {
                let plan: Vec<(i64,i64,i64,String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!("EXPLAIN QUERY PLAN {query}")))
                    .bind(instance.id.to_string()).fetch_all(pool).await.unwrap();
                let steps: Vec<_> = plan.iter().map(|row| row.3.as_str()).collect();
                assert!(steps.iter().any(|step| step.contains(index) && step.contains("block_id>?")), "must seek next distinct block: {steps:?}");
                assert!(!steps.iter().any(|step| step.starts_with("SCAN b") || step.starts_with("SCAN o")), "must not scan loop history: {steps:?}");
                }
            }
            #[cfg(feature = "postgres")]
            RawPool::Postgres(pool) => {
                sqlx::query("ANALYZE block_outputs").execute(pool).await.unwrap();
                for query in [POSTGRES_QUERY, POSTGRES_DIAGNOSTIC_QUERY] {
                let plan: Value = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("EXPLAIN (ANALYZE, FORMAT JSON) {query}")))
                    .bind(instance.id.into_uuid()).fetch_one(pool).await.unwrap();
                let (rows, indexed) = visits(&plan[0]["Plan"]);
                assert!(indexed, "projection must seek a block index");
                assert!(rows <= 20.0, "two current blocks must not visit 512 history rows; visited {rows}");
                }
            }
        }
    });
}
