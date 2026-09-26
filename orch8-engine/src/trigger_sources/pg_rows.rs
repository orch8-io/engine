//! Postgres row-change trigger (`trigger_type: "postgres_rows"`, engine
//! feature `postgres-rows`): start a workflow whenever rows in a user table
//! are inserted, updated or deleted.
//!
//! # Capture
//!
//! [`install_sql`] (printed or applied by `orch8 triggers pg-install`)
//! creates, in the **user's** database:
//!
//! * an outbox table (default `public.orch8_row_changes`) holding one row
//!   per change — written in the same transaction as the change itself, so
//!   a committed change can never be lost, even while the engine is down;
//! * a `plpgsql` trigger function that appends to the outbox and calls
//!   `pg_notify(channel, …)` as a low-latency wake-up;
//! * an `AFTER … FOR EACH ROW` trigger on the watched table.
//!
//! # Consumption
//!
//! The engine `LISTEN`s on the channel and also polls every
//! `poll_interval_ms`, so a missed notification (engine down, connection
//! blip) only delays delivery. Rows are read in `(txid, id)` order, and only
//! rows whose transaction id is older than the oldest in-flight transaction
//! (`pg_snapshot_xmin(pg_current_snapshot())`) are eligible: that set is
//! final, so the durable cursor `(txid, id)` can never skip a row committed
//! late by a long transaction (the classic `BIGSERIAL` outbox gap). The
//! cursor is persisted in the engine's `trigger_poll_state` and advanced only
//! past rows whose instance was durably created; each instance's idempotency
//! key is the outbox row id, following the same replay-safe rule as
//! `docs/POSTGRES_OUTBOX_INTAKE.md`. A per-trigger lease keeps a single
//! engine node consuming. Requires `PostgreSQL` 13+.
//!
//! ```json
//! {
//!   "database_url": "credentials://app-db/url",
//!   "table": "public.orders",
//!   "events": ["insert", "update"],
//!   "outbox_table": "public.orch8_row_changes",
//!   "channel": "orch8_row_changes",
//!   "start_from": "now",           // or "beginning"
//!   "batch_size": 100,
//!   "poll_interval_ms": 5000
//! }
//! ```

use serde_json::{Value, json};

use super::{opt_str, opt_u64, req_str, require_object};

/// Default outbox table.
pub const DEFAULT_OUTBOX_TABLE: &str = "public.orch8_row_changes";
/// Default NOTIFY channel.
pub const DEFAULT_CHANNEL: &str = "orch8_row_changes";

/// A row event kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RowEvent {
    Insert,
    Update,
    Delete,
}

impl RowEvent {
    /// Lowercase name stored in the outbox `op` column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }

    const fn sql_keyword(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }

    /// Parse `insert` / `update` / `delete` (case-insensitive).
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "insert" => Ok(Self::Insert),
            "update" => Ok(Self::Update),
            "delete" => Ok(Self::Delete),
            other => Err(format!(
                "unknown event '{other}' (expected insert, update or delete)"
            )),
        }
    }

    /// Parse a comma-separated list, deduplicated and sorted.
    pub fn parse_list(s: &str) -> Result<Vec<Self>, String> {
        let mut events = s
            .split(',')
            .filter(|p| !p.trim().is_empty())
            .map(Self::parse)
            .collect::<Result<Vec<_>, _>>()?;
        events.sort();
        events.dedup();
        if events.is_empty() {
            return Err("at least one event is required".into());
        }
        Ok(events)
    }
}

/// A validated, optionally schema-qualified SQL identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedName {
    pub schema: String,
    pub name: String,
}

impl std::fmt::Display for QualifiedName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.schema, self.name)
    }
}

fn valid_ident(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    s.len() <= 63
        && (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

impl QualifiedName {
    /// Parse `table` or `schema.table` (unquoted identifiers only; `public`
    /// is assumed when no schema is given). Case is preserved and quoted in
    /// generated SQL, so pass the exact (usually lowercase) catalog name.
    pub fn parse(s: &str) -> Result<Self, String> {
        let (schema, name) = match s.split_once('.') {
            Some((schema, name)) => (schema, name),
            None => ("public", s),
        };
        if !valid_ident(schema) || !valid_ident(name) {
            return Err(format!(
                "'{s}' is not a valid [schema.]table name (letters, digits, _ and $; max 63 chars each)"
            ));
        }
        Ok(Self {
            schema: schema.to_string(),
            name: name.to_string(),
        })
    }

    /// Double-quoted SQL form.
    #[must_use]
    pub fn quoted(&self) -> String {
        format!("\"{}\".\"{}\"", self.schema, self.name)
    }

    /// Quoted name of a sibling object in the same schema.
    #[must_use]
    pub fn sibling(&self, suffix: &str) -> String {
        format!("\"{}\".\"{}{suffix}\"", self.schema, self.name)
    }
}

/// Where a brand-new trigger starts reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartFrom {
    /// Only changes committed after the trigger starts.
    Now,
    /// Every change still in the outbox.
    Beginning,
}

/// Validated `postgres_rows` trigger config.
#[derive(Clone, PartialEq, Eq)]
pub struct PgRowsConfig {
    pub database_url: String,
    pub table: QualifiedName,
    pub events: Vec<RowEvent>,
    pub outbox_table: QualifiedName,
    pub channel: String,
    pub start_from: StartFrom,
    pub batch_size: u64,
    pub poll_interval_ms: u64,
}

impl std::fmt::Debug for PgRowsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgRowsConfig")
            .field("database_url", &"<redacted>")
            .field("table", &self.table)
            .field("events", &self.events)
            .field("outbox_table", &self.outbox_table)
            .field("channel", &self.channel)
            .finish_non_exhaustive()
    }
}

impl PgRowsConfig {
    pub fn parse(config: &Value) -> Result<Self, String> {
        require_object(config, "postgres_rows")?;
        let database_url = req_str(config, "database_url")?;
        if !(database_url.starts_with("postgres://")
            || database_url.starts_with("postgresql://")
            || database_url.starts_with("credentials://"))
        {
            return Err(
                "'database_url' must be a postgres:// URL or credentials:// reference".into(),
            );
        }
        let table = QualifiedName::parse(&req_str(config, "table")?)?;
        let events = match config.get("events") {
            None | Some(Value::Null) => vec![RowEvent::Insert],
            Some(Value::String(s)) => RowEvent::parse_list(s)?,
            Some(Value::Array(items)) => {
                let joined = items
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(ToString::to_string)
                            .ok_or_else(|| "'events' must contain strings".to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .join(",");
                RowEvent::parse_list(&joined)?
            }
            Some(_) => return Err("'events' must be an array or comma-separated string".into()),
        };
        let outbox_table = QualifiedName::parse(
            &opt_str(config, "outbox_table")?.unwrap_or_else(|| DEFAULT_OUTBOX_TABLE.into()),
        )?;
        let channel = opt_str(config, "channel")?.unwrap_or_else(|| DEFAULT_CHANNEL.into());
        if !valid_ident(&channel) {
            return Err("'channel' must be a plain identifier".into());
        }
        let start_from = match opt_str(config, "start_from")?.as_deref() {
            None | Some("now") => StartFrom::Now,
            Some("beginning") => StartFrom::Beginning,
            Some(other) => {
                return Err(format!(
                    "'start_from' must be 'now' or 'beginning', got '{other}'"
                ));
            }
        };
        Ok(Self {
            database_url,
            table,
            events,
            outbox_table,
            channel,
            start_from,
            batch_size: opt_u64(config, "batch_size", 100, 1, 5000)?,
            poll_interval_ms: opt_u64(config, "poll_interval_ms", 5000, 100, 3_600_000)?,
        })
    }
}

/// Name of the per-watched-table Postgres trigger.
pub const PG_TRIGGER_NAME: &str = "orch8_row_change";

/// SQL that installs capture for `table` into `outbox` (idempotent: safe to
/// re-run, e.g. to change the event list).
#[must_use]
pub fn install_sql(
    table: &QualifiedName,
    events: &[RowEvent],
    outbox: &QualifiedName,
    channel: &str,
) -> String {
    let outbox_q = outbox.quoted();
    let func = outbox.sibling("_capture");
    let index = format!("\"{}_lookup_idx\"", outbox.name);
    let table_q = table.quoted();
    let event_list = events
        .iter()
        .map(|e| e.sql_keyword())
        .collect::<Vec<_>>()
        .join(" OR ");
    format!(
        r#"-- Orch8 row-change capture for {table_q} ({events}). Requires PostgreSQL 13+.
-- Safe to re-run. Generated by `orch8 triggers pg-install`.
BEGIN;

CREATE TABLE IF NOT EXISTS {outbox_q} (
    id           BIGSERIAL PRIMARY KEY,
    txid         xid8 NOT NULL DEFAULT pg_current_xact_id(),
    table_schema TEXT NOT NULL,
    table_name   TEXT NOT NULL,
    op           TEXT NOT NULL,
    row_data     JSONB,
    old_data     JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS {index}
    ON {outbox_q} (table_schema, table_name, txid, id);

CREATE OR REPLACE FUNCTION {func}() RETURNS trigger
LANGUAGE plpgsql AS $orch8$
BEGIN
    INSERT INTO {outbox_q} (table_schema, table_name, op, row_data, old_data)
    VALUES (
        TG_TABLE_SCHEMA,
        TG_TABLE_NAME,
        lower(TG_OP),
        CASE WHEN TG_OP = 'DELETE' THEN NULL ELSE to_jsonb(NEW) END,
        CASE WHEN TG_OP = 'INSERT' THEN NULL ELSE to_jsonb(OLD) END
    );
    PERFORM pg_notify(TG_ARGV[0], TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME);
    RETURN NULL;
END
$orch8$;

DROP TRIGGER IF EXISTS "{trigger}" ON {table_q};
CREATE TRIGGER "{trigger}"
    AFTER {event_list} ON {table_q}
    FOR EACH ROW EXECUTE FUNCTION {func}('{channel}');

COMMIT;
"#,
        events = events
            .iter()
            .map(|e| e.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        trigger = PG_TRIGGER_NAME,
    )
}

/// SQL that removes capture from `table` (the shared outbox table and
/// function are kept for other watched tables).
#[must_use]
pub fn uninstall_sql(table: &QualifiedName) -> String {
    format!(
        "-- Remove Orch8 row-change capture from {t}.\nDROP TRIGGER IF EXISTS \"{PG_TRIGGER_NAME}\" ON {t};\n",
        t = table.quoted()
    )
}

/// One outbox row as read by the listener.
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxRow {
    pub id: i64,
    pub txid: String,
    pub table_schema: String,
    pub table_name: String,
    pub op: String,
    pub row_data: Option<Value>,
    pub old_data: Option<Value>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Map an outbox row to `(data, meta, source_id)`. The workflow sees
/// `data.op`, `data.table`, `data.new` (absent on delete) and `data.old`
/// (absent on insert).
#[must_use]
pub fn map_row(outbox: &QualifiedName, row: &OutboxRow) -> (Value, Value, String) {
    let table = format!("{}.{}", row.table_schema, row.table_name);
    let data = json!({
        "op": row.op,
        "table": table,
        "new": row.row_data,
        "old": row.old_data,
    });
    let meta = json!({
        "outbox_table": format!("{}.{}", outbox.schema, outbox.name),
        "outbox_id": row.id,
        "txid": row.txid,
        "captured_at": row.created_at.to_rfc3339(),
    });
    (
        data,
        meta,
        format!("pg:{}.{}/{}", outbox.schema, outbox.name, row.id),
    )
}

/// Durable read position: rows strictly after `(txid, id)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    /// `xid8` rendered as decimal text.
    pub txid: String,
    pub id: i64,
}

impl Cursor {
    /// Parse a stored cursor. A cursor written for a different outbox table
    /// (the trigger's config was edited) is ignored.
    #[must_use]
    pub fn from_value(v: &Value, outbox: &QualifiedName) -> Option<Self> {
        if v.get("outbox")?.as_str()? != outbox.to_string() {
            return None;
        }
        let txid = v.get("txid")?.as_str()?;
        if txid.is_empty() || !txid.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        Some(Self {
            txid: txid.to_string(),
            id: v.get("id")?.as_i64()?,
        })
    }

    #[must_use]
    pub fn to_value(&self, outbox: &QualifiedName) -> Value {
        json!({"outbox": outbox.to_string(), "txid": self.txid, "id": self.id})
    }
}

#[cfg(feature = "postgres-rows")]
pub use listener::run;

#[cfg(feature = "postgres-rows")]
mod listener {
    use std::sync::Arc;
    use std::time::Duration;

    use sqlx::Row;
    use sqlx::postgres::{PgListener, PgPool, PgPoolOptions};
    use tokio_util::sync::CancellationToken;
    use tracing::{error, info, warn};

    use orch8_storage::StorageBackend;
    use orch8_types::trigger::TriggerDef;

    use super::{Cursor, OutboxRow, PgRowsConfig, StartFrom};
    use crate::error::EngineError;
    use crate::trigger_sources::{
        acquire_lease, deliver, failure_backoff, load_cursor, resolved_config, save_cursor,
        sleep_or_cancel,
    };

    const LEASE_HOLD: Duration = Duration::from_secs(60);

    fn db_err(e: impl std::fmt::Display) -> String {
        e.to_string()
    }

    async fn initial_cursor(pool: &PgPool, start: StartFrom) -> Result<Cursor, String> {
        match start {
            StartFrom::Beginning => Ok(Cursor {
                txid: "0".into(),
                id: 0,
            }),
            StartFrom::Now => {
                let row = sqlx::query("SELECT pg_snapshot_xmin(pg_current_snapshot())::text AS x")
                    .fetch_one(pool)
                    .await
                    .map_err(db_err)?;
                let xmin: String = row.try_get("x").map_err(db_err)?;
                // Rows with txid >= xmin are not final yet and stay eligible:
                // start just before the oldest in-flight transaction.
                let txid = xmin
                    .parse::<u64>()
                    .map_err(db_err)?
                    .saturating_sub(1)
                    .to_string();
                Ok(Cursor { txid, id: i64::MAX })
            }
        }
    }

    async fn fetch_batch(
        pool: &PgPool,
        cfg: &PgRowsConfig,
        cursor: &Cursor,
    ) -> Result<Vec<OutboxRow>, String> {
        let ops: Vec<&str> = cfg.events.iter().map(|e| e.as_str()).collect();
        let sql = format!(
            "SELECT id, txid::text AS txid, table_schema, table_name, op, row_data, old_data, created_at \
             FROM {} \
             WHERE table_schema = $1 AND table_name = $2 AND op = ANY($3) \
               AND txid < pg_snapshot_xmin(pg_current_snapshot()) \
               AND (txid, id) > ($4::text::xid8, $5) \
             ORDER BY txid, id LIMIT $6",
            cfg.outbox_table.quoted()
        );
        let rows = sqlx::query(&sql)
            .bind(&cfg.table.schema)
            .bind(&cfg.table.name)
            .bind(&ops)
            .bind(&cursor.txid)
            .bind(cursor.id)
            .bind(i64::try_from(cfg.batch_size).unwrap_or(100))
            .fetch_all(pool)
            .await
            .map_err(db_err)?;
        rows.iter()
            .map(|r| {
                Ok(OutboxRow {
                    id: r.try_get("id").map_err(db_err)?,
                    txid: r.try_get("txid").map_err(db_err)?,
                    table_schema: r.try_get("table_schema").map_err(db_err)?,
                    table_name: r.try_get("table_name").map_err(db_err)?,
                    op: r.try_get("op").map_err(db_err)?,
                    row_data: r.try_get("row_data").map_err(db_err)?,
                    old_data: r.try_get("old_data").map_err(db_err)?,
                    created_at: r.try_get("created_at").map_err(db_err)?,
                })
            })
            .collect()
    }

    /// Deliver every eligible row after `cursor`, advancing it past each
    /// durably created instance. Stops at the first failure (nothing after a
    /// failed row is skipped).
    async fn drain(
        storage: &dyn StorageBackend,
        trigger: &TriggerDef,
        pool: &PgPool,
        cfg: &PgRowsConfig,
        cursor: &mut Cursor,
    ) -> Result<usize, String> {
        let mut total = 0usize;
        loop {
            let batch = fetch_batch(pool, cfg, cursor).await?;
            let full = batch.len() as u64 >= cfg.batch_size;
            for row in &batch {
                let (data, meta, source_id) = super::map_row(&cfg.outbox_table, row);
                deliver(storage, trigger, data, meta, &source_id)
                    .await
                    .map_err(|e| e.to_string())?;
                *cursor = Cursor {
                    txid: row.txid.clone(),
                    id: row.id,
                };
                total += 1;
            }
            if !full {
                return Ok(total);
            }
            save_cursor(
                storage,
                &trigger.slug,
                cursor.to_value(&cfg.outbox_table),
                None,
            )
            .await
            .map_err(|e| e.to_string())?;
        }
    }

    /// Run the Postgres row-change listener until cancelled.
    // One reconnect/consume state machine; splitting it would scatter the
    // lease/cursor invariants across helpers.
    #[allow(clippy::too_many_lines)]
    pub async fn run(
        storage: Arc<dyn StorageBackend>,
        trigger: TriggerDef,
        cancel: CancellationToken,
    ) -> Result<(), EngineError> {
        let config = resolved_config(storage.as_ref(), &trigger).await?;
        let cfg = PgRowsConfig::parse(&config)
            .map_err(|e| EngineError::InvalidConfig(format!("postgres_rows: {e}")))?;
        let slug = trigger.slug.clone();
        let poll_every = Duration::from_millis(cfg.poll_interval_ms);
        let mut failures = 0u32;

        'connect: loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            if !acquire_lease(storage.as_ref(), &slug, LEASE_HOLD).await {
                if sleep_or_cancel(&cancel, LEASE_HOLD / 4).await {
                    return Ok(());
                }
                continue;
            }
            let pool = match PgPoolOptions::new()
                .max_connections(2)
                .acquire_timeout(Duration::from_secs(10))
                .connect(&cfg.database_url)
                .await
            {
                Ok(p) => p,
                Err(e) => {
                    failures = failures.saturating_add(1);
                    warn!(slug, error = %e, "postgres_rows: connect failed");
                    if sleep_or_cancel(&cancel, failure_backoff(failures)).await {
                        return Ok(());
                    }
                    continue;
                }
            };
            let mut listener = match PgListener::connect_with(&pool).await {
                Ok(mut l) => match l.listen(&cfg.channel).await {
                    Ok(()) => Some(l),
                    Err(e) => {
                        warn!(slug, error = %e, "postgres_rows: LISTEN failed; polling only");
                        None
                    }
                },
                Err(e) => {
                    warn!(slug, error = %e, "postgres_rows: listener connect failed; polling only");
                    None
                }
            };
            let mut cursor = match load_cursor(storage.as_ref(), &slug)
                .await?
                .as_ref()
                .and_then(|v| Cursor::from_value(v, &cfg.outbox_table))
            {
                Some(c) => c,
                None => match initial_cursor(&pool, cfg.start_from).await {
                    Ok(c) => {
                        save_cursor(storage.as_ref(), &slug, c.to_value(&cfg.outbox_table), None)
                            .await?;
                        c
                    }
                    Err(e) => {
                        failures = failures.saturating_add(1);
                        warn!(slug, error = %e, "postgres_rows: cannot read snapshot");
                        if sleep_or_cancel(&cancel, failure_backoff(failures)).await {
                            return Ok(());
                        }
                        continue;
                    }
                },
            };
            info!(slug, table = ?cfg.table, channel = %cfg.channel, "postgres_rows trigger listener active");

            loop {
                if !acquire_lease(storage.as_ref(), &slug, LEASE_HOLD).await {
                    warn!(slug, "postgres_rows: consumer lease lost");
                    continue 'connect;
                }
                let before = cursor.clone();
                match drain(storage.as_ref(), &trigger, &pool, &cfg, &mut cursor).await {
                    Ok(_) => {
                        failures = 0;
                        if cursor != before {
                            save_cursor(
                                storage.as_ref(),
                                &slug,
                                cursor.to_value(&cfg.outbox_table),
                                None,
                            )
                            .await?;
                        }
                    }
                    Err(e) => {
                        error!(slug, error = %e, "postgres_rows: delivery failed");
                        let _ = save_cursor(
                            storage.as_ref(),
                            &slug,
                            cursor.to_value(&cfg.outbox_table),
                            Some(e.clone()),
                        )
                        .await;
                        failures = failures.saturating_add(1);
                        if sleep_or_cancel(&cancel, failure_backoff(failures)).await {
                            return Ok(());
                        }
                        continue 'connect;
                    }
                }
                // Wait for a NOTIFY or the fallback poll interval.
                let woke = if let Some(l) = listener.as_mut() {
                    tokio::select! {
                        () = cancel.cancelled() => return Ok(()),
                        n = l.recv() => n.map(|_| ()).map_err(|e| e.to_string()),
                        () = tokio::time::sleep(poll_every) => Ok(()),
                    }
                } else if sleep_or_cancel(&cancel, poll_every).await {
                    return Ok(());
                } else {
                    Ok(())
                };
                if let Err(e) = woke {
                    warn!(slug, error = %e, "postgres_rows: notification stream error; polling only until reconnect");
                    listener = None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualified_name_parsing() {
        assert_eq!(
            QualifiedName::parse("orders").unwrap(),
            QualifiedName {
                schema: "public".into(),
                name: "orders".into()
            }
        );
        assert_eq!(
            QualifiedName::parse("app.Orders").unwrap().quoted(),
            "\"app\".\"Orders\""
        );
        for bad in [
            "",
            "a.b.c",
            "1table",
            "orders; DROP TABLE x",
            "or\"ders",
            ".orders",
            &"x".repeat(64),
        ] {
            assert!(QualifiedName::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn event_list_parsing() {
        assert_eq!(
            RowEvent::parse_list("update, INSERT,update").unwrap(),
            vec![RowEvent::Insert, RowEvent::Update]
        );
        assert!(RowEvent::parse_list("truncate").is_err());
        assert!(RowEvent::parse_list(" , ").is_err());
    }

    #[test]
    fn config_parsing() {
        let c = PgRowsConfig::parse(&json!({
            "database_url": "postgres://u@h/db", "table": "orders"
        }))
        .unwrap();
        assert_eq!(c.events, vec![RowEvent::Insert]);
        assert_eq!(c.outbox_table.quoted(), "\"public\".\"orch8_row_changes\"");
        assert_eq!(c.channel, "orch8_row_changes");
        assert_eq!(c.start_from, StartFrom::Now);
        assert!(!format!("{c:?}").contains("postgres://u@h"));

        let c = PgRowsConfig::parse(&json!({
            "database_url": "credentials://db", "table": "app.users",
            "events": ["insert", "delete"], "outbox_table": "ops.changes",
            "channel": "users_changed", "start_from": "beginning", "batch_size": 10
        }))
        .unwrap();
        assert_eq!(c.events, vec![RowEvent::Insert, RowEvent::Delete]);
        assert_eq!(c.outbox_table.schema, "ops");
        assert_eq!(c.start_from, StartFrom::Beginning);

        for bad in [
            json!({"table": "t"}),
            json!({"database_url": "mysql://x", "table": "t"}),
            json!({"database_url": "postgres://x"}),
            json!({"database_url": "postgres://x", "table": "t", "events": "truncate"}),
            json!({"database_url": "postgres://x", "table": "t", "channel": "bad-name"}),
            json!({"database_url": "postgres://x", "table": "t", "start_from": "yesterday"}),
            json!({"database_url": "postgres://x", "table": "t", "poll_interval_ms": 1}),
        ] {
            assert!(PgRowsConfig::parse(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn install_sql_is_quoted_and_complete() {
        let sql = install_sql(
            &QualifiedName::parse("public.orders").unwrap(),
            &[RowEvent::Insert, RowEvent::Update],
            &QualifiedName::parse(DEFAULT_OUTBOX_TABLE).unwrap(),
            DEFAULT_CHANNEL,
        );
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"public\".\"orch8_row_changes\""));
        assert!(sql.contains("txid         xid8 NOT NULL DEFAULT pg_current_xact_id()"));
        assert!(
            sql.contains("CREATE OR REPLACE FUNCTION \"public\".\"orch8_row_changes_capture\"()")
        );
        assert!(sql.contains("AFTER INSERT OR UPDATE ON \"public\".\"orders\""));
        assert!(sql.contains(
            "EXECUTE FUNCTION \"public\".\"orch8_row_changes_capture\"('orch8_row_changes')"
        ));
        assert!(
            sql.contains("DROP TRIGGER IF EXISTS \"orch8_row_change\" ON \"public\".\"orders\"")
        );
        assert!(sql.trim_end().ends_with("COMMIT;"));
        let un = uninstall_sql(&QualifiedName::parse("orders").unwrap());
        assert!(
            un.contains("DROP TRIGGER IF EXISTS \"orch8_row_change\" ON \"public\".\"orders\"")
        );
    }

    #[test]
    fn row_mapping_and_cursor() {
        let outbox = QualifiedName::parse(DEFAULT_OUTBOX_TABLE).unwrap();
        let row = OutboxRow {
            id: 17,
            txid: "901".into(),
            table_schema: "public".into(),
            table_name: "orders".into(),
            op: "update".into(),
            row_data: Some(json!({"id": 1, "status": "paid"})),
            old_data: Some(json!({"id": 1, "status": "new"})),
            created_at: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        };
        let (data, meta, id) = map_row(&outbox, &row);
        assert_eq!(data["op"], "update");
        assert_eq!(data["table"], "public.orders");
        assert_eq!(data["new"]["status"], "paid");
        assert_eq!(data["old"]["status"], "new");
        assert_eq!(meta["outbox_id"], 17);
        assert_eq!(meta["txid"], "901");
        assert_eq!(id, "pg:public.orch8_row_changes/17");

        let c = Cursor {
            txid: "901".into(),
            id: 17,
        };
        let stored = c.to_value(&outbox);
        assert_eq!(Cursor::from_value(&stored, &outbox), Some(c));
        let other = QualifiedName::parse("ops.changes").unwrap();
        assert!(Cursor::from_value(&stored, &other).is_none());
        assert!(
            Cursor::from_value(
                &json!({"outbox": "public.orch8_row_changes", "txid": "1; drop", "id": 1}),
                &outbox
            )
            .is_none()
        );
        assert!(Cursor::from_value(&json!({"offsets": {}}), &outbox).is_none());
    }
}
