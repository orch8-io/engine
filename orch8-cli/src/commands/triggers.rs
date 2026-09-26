//! `orch8 triggers` — local helpers for trigger sources.

use anyhow::{Context, Result};
use clap::Subcommand;

use orch8_engine::trigger_sources::pg_rows::{
    DEFAULT_CHANNEL, DEFAULT_OUTBOX_TABLE, QualifiedName, RowEvent, install_sql, uninstall_sql,
};

#[derive(Subcommand)]
pub enum TriggersCmd {
    /// Print (or apply) the SQL that captures row changes of a Postgres
    /// table for a `postgres_rows` trigger. Pipe into psql or pass --apply.
    PgInstall {
        /// Table to watch: `table` or `schema.table`.
        #[arg(long)]
        table: String,
        /// Comma-separated events: insert, update, delete.
        #[arg(long, default_value = "insert")]
        events: String,
        /// Outbox table (`schema.table`) the trigger function appends to.
        #[arg(long, default_value = DEFAULT_OUTBOX_TABLE)]
        outbox_table: String,
        /// NOTIFY channel the engine listens on.
        #[arg(long, default_value = DEFAULT_CHANNEL)]
        channel: String,
        /// Emit SQL that removes capture from the table instead.
        #[arg(long)]
        uninstall: bool,
        /// Execute the SQL against --database-url instead of printing it.
        #[arg(long, requires = "database_url")]
        apply: bool,
        /// Target database (the application's, not necessarily Orch8's).
        #[arg(long, env = "ORCH8_TRIGGER_DATABASE_URL", hide_env_values = true)]
        database_url: Option<String>,
    },
}

/// Build the SQL for a `pg-install` invocation.
pub(crate) fn pg_install_sql(
    table: &str,
    events: &str,
    outbox_table: &str,
    channel: &str,
    uninstall: bool,
) -> Result<String> {
    let table = QualifiedName::parse(table).map_err(anyhow::Error::msg)?;
    if uninstall {
        return Ok(uninstall_sql(&table));
    }
    let events = RowEvent::parse_list(events).map_err(anyhow::Error::msg)?;
    let outbox = QualifiedName::parse(outbox_table).map_err(anyhow::Error::msg)?;
    // Same rule the engine applies to the trigger config.
    let valid_channel = !channel.is_empty()
        && channel.len() <= 63
        && channel.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && channel
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$');
    anyhow::ensure!(valid_channel, "--channel must be a plain identifier");
    Ok(install_sql(&table, &events, &outbox, channel))
}

pub async fn run(cmd: TriggersCmd) -> Result<()> {
    match cmd {
        TriggersCmd::PgInstall {
            table,
            events,
            outbox_table,
            channel,
            uninstall,
            apply,
            database_url,
        } => {
            let sql = pg_install_sql(&table, &events, &outbox_table, &channel, uninstall)?;
            if apply {
                let url = database_url.context("--apply requires --database-url")?;
                let pool = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&url)
                    .await
                    .context("connect to --database-url")?;
                sqlx::raw_sql(&sql)
                    .execute(&pool)
                    .await
                    .context("apply row-change capture SQL")?;
                if uninstall {
                    eprintln!("Removed row-change capture from {table}.");
                } else {
                    eprintln!("Installed row-change capture on {table} ({events}).");
                }
            } else {
                print!("{sql}");
            }
            if !uninstall {
                eprintln!(
                    "\nNext: create the trigger, e.g.\n  POST /api/v1/triggers {{\"slug\": \"...\", \"sequence_name\": \"...\", \
                     \"trigger_type\": \"postgres_rows\", \"config\": {{\"database_url\": \"credentials://<id>\", \
                     \"table\": \"{table}\", \"events\": \"{events}\", \"outbox_table\": \"{outbox_table}\", \"channel\": \"{channel}\"}}}}"
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_install_prints_quoted_sql() {
        let sql = pg_install_sql(
            "app.orders",
            "insert,update",
            DEFAULT_OUTBOX_TABLE,
            DEFAULT_CHANNEL,
            false,
        )
        .unwrap();
        assert!(sql.contains("AFTER INSERT OR UPDATE ON \"app\".\"orders\""));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"public\".\"orch8_row_changes\""));
        let un = pg_install_sql(
            "orders",
            "insert",
            DEFAULT_OUTBOX_TABLE,
            DEFAULT_CHANNEL,
            true,
        )
        .unwrap();
        assert!(un.starts_with("-- Remove"));
    }

    #[test]
    fn pg_install_rejects_bad_input() {
        assert!(
            pg_install_sql(
                "x; drop",
                "insert",
                DEFAULT_OUTBOX_TABLE,
                DEFAULT_CHANNEL,
                false
            )
            .is_err()
        );
        assert!(
            pg_install_sql(
                "orders",
                "truncate",
                DEFAULT_OUTBOX_TABLE,
                DEFAULT_CHANNEL,
                false
            )
            .is_err()
        );
        assert!(
            pg_install_sql(
                "orders",
                "insert",
                DEFAULT_OUTBOX_TABLE,
                "bad-channel",
                false
            )
            .is_err()
        );
        assert!(pg_install_sql("orders", "insert", "a.b.c", DEFAULT_CHANNEL, false).is_err());
    }
}
