//! Observe or apply the same reconciliation used at server startup.
use super::SqliteStorage;
use orch8_types::error::StorageError;
use serde::Serialize;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Connection as _, SqliteConnection, SqlitePool};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaChange {
    pub action: &'static str,
    pub kind: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaReconciliation {
    pub previous_version: Option<i64>,
    pub reconciled_version: Option<i64>,
    pub changes: Vec<SchemaChange>,
}

struct Inventory {
    definitions: BTreeMap<(String, String), String>,
    columns: BTreeSet<(String, String)>,
    version: Option<i64>,
}

impl Inventory {
    async fn load(pool: &SqlitePool) -> Result<Self, StorageError> {
        let definitions: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT type,name,sql FROM sqlite_schema
             WHERE name NOT GLOB 'sqlite_*' AND sql IS NOT NULL",
        )
        .fetch_all(pool)
        .await?;
        let definitions: BTreeMap<_, _> = definitions
            .into_iter()
            .map(|(kind, name, sql)| ((kind, name), sql))
            .collect();
        let columns = sqlx::query_as(
            "SELECT s.name,c.name FROM sqlite_schema s JOIN pragma_table_xinfo(s.name) c
             WHERE s.type='table' AND s.name NOT GLOB 'sqlite_*'",
        )
        .fetch_all(pool)
        .await?;
        let version =
            if definitions.contains_key(&("table".to_owned(), "schema_versions".to_owned())) {
                sqlx::query_scalar("SELECT MAX(version) FROM schema_versions")
                    .fetch_one(pool)
                    .await?
            } else {
                None
            };
        Ok(Self {
            definitions,
            columns: columns.into_iter().collect(),
            version,
        })
    }

    fn changes_to(&self, after: &Self) -> SchemaReconciliation {
        let mut changes = Vec::new();
        for ((kind, name), sql) in &after.definitions {
            let action = match self.definitions.get(&(kind.clone(), name.clone())) {
                None => "add",
                Some(before) if before != sql => "update",
                Some(_) => continue,
            };
            changes.push(SchemaChange {
                action,
                kind: kind.clone(),
                name: name.clone(),
            });
        }
        for (kind, name) in self.definitions.keys() {
            if !after
                .definitions
                .contains_key(&(kind.clone(), name.clone()))
            {
                changes.push(SchemaChange {
                    action: "remove",
                    kind: kind.clone(),
                    name: name.clone(),
                });
            }
        }
        for (table, column) in after.columns.difference(&self.columns) {
            if self
                .definitions
                .contains_key(&("table".to_owned(), table.clone()))
            {
                changes.push(SchemaChange {
                    action: "add",
                    kind: "column".into(),
                    name: format!("{table}.{column}"),
                });
            }
        }
        SchemaReconciliation {
            previous_version: self.version,
            reconciled_version: after.version,
            changes,
        }
    }
}

/// Copy a consistent snapshot, including committed WAL frames, without changing the source.
pub async fn copy_snapshot(source: &Path, destination: &Path) -> Result<(), StorageError> {
    let options = SqliteConnectOptions::new().filename(source).read_only(true);
    let mut source = SqliteConnection::connect_with(&options).await?;
    let destination = destination
        .to_str()
        .ok_or_else(|| StorageError::Unsupported("snapshot path must be valid UTF-8".into()))?;
    sqlx::query("VACUUM main INTO ?1")
        .bind(destination)
        .execute(&mut source)
        .await?;
    source.close().await?;
    Ok(())
}

/// Apply startup reconciliation while holding exclusive recovery ownership.
pub async fn reconcile_file_schema(path: &Path) -> Result<SchemaReconciliation, StorageError> {
    if !path.is_file() {
        return Err(StorageError::Connection(
            "schema reconciliation requires an existing database file".into(),
        ));
    }
    let options = SqliteConnectOptions::new()
        .filename(path)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5))
        .foreign_keys(true);
    let options = super::space_reclaiming(options);
    let storage = SqliteStorage::open_owned_unreconciled(
        options,
        SqlitePoolOptions::new().max_connections(1),
    )
    .await?;
    let result = async {
        let before = Inventory::load(storage.pool()).await?;
        storage.create_tables().await?;
        let after = Inventory::load(storage.pool()).await?;
        Ok(before.changes_to(&after))
    }
    .await;
    storage.close().await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_names_changed_objects_and_columns_without_exposing_sql() {
        let before = Inventory {
            definitions: BTreeMap::from([(
                ("table".into(), "state".into()),
                "CREATE TABLE state (key TEXT)".into(),
            )]),
            columns: BTreeSet::from([("state".into(), "key".into())]),
            version: Some(77),
        };
        let after = Inventory {
            definitions: BTreeMap::from([
                (
                    ("table".into(), "state".into()),
                    "CREATE TABLE state (key TEXT, expires TEXT)".into(),
                ),
                (
                    ("index".into(), "state_key".into()),
                    "CREATE INDEX state_key ON state(key)".into(),
                ),
            ]),
            columns: BTreeSet::from([
                ("state".into(), "key".into()),
                ("state".into(), "expires".into()),
            ]),
            version: Some(78),
        };
        let changes = before.changes_to(&after);
        assert_eq!(changes.previous_version, Some(77));
        assert_eq!(changes.reconciled_version, Some(78));
        assert!(changes.changes.iter().any(|change| change.action == "add"
            && change.kind == "column"
            && change.name == "state.expires"));
        assert!(changes.changes.iter().any(|change| change.action == "add"
            && change.kind == "index"
            && change.name == "state_key"));
        assert!(after.changes_to(&after).changes.is_empty());
    }
}
