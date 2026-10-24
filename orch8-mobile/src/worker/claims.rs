//! Durable and in-process ownership of one remote worker claim.
//!
//! A task UUID alone is not a lease identity: another control plane, another
//! worker, or a later claim epoch must retain independent recovery evidence.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Row, SqlitePool};
use tracing::warn;

use orch8_engine::remote_worker::RemoteTask;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ClaimKey {
    pub task_id: uuid::Uuid,
    pub claim_epoch: u64,
    pub worker_id: String,
    pub api_base: String,
}

impl ClaimKey {
    pub fn for_task(task: &RemoteTask, worker_id: &str, api_base: &str) -> Self {
        Self {
            task_id: task.id,
            claim_epoch: task.claim_epoch,
            worker_id: worker_id.to_owned(),
            api_base: api_base.trim_end_matches('/').to_owned(),
        }
    }

    fn database_epoch(&self) -> Result<i64, sqlx::Error> {
        i64::try_from(self.claim_epoch).map_err(|_| {
            sqlx::Error::Protocol("worker claim epoch exceeds SQLite integer range".into())
        })
    }

    pub fn belongs_to(&self, worker_id: &str, api_base: &str) -> bool {
        self.worker_id == worker_id
            && self.api_base.trim_end_matches('/') == api_base.trim_end_matches('/')
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Outcome {
    Complete { output: Value },
    Fail { message: String, retryable: bool },
}

#[derive(Debug, Clone)]
pub(crate) struct ClaimRow {
    pub key: ClaimKey,
    pub started: bool,
    pub outcome: Option<Outcome>,
    pub claimed_at: chrono::DateTime<chrono::Utc>,
}

type LiveClaims = Arc<Mutex<HashSet<ClaimKey>>>;

/// The shared reservation remains live while its worker is stopped or replaced.
/// Dropping a task future also releases it, so recovery can resume on restart.
pub(crate) struct ClaimReservation {
    key: ClaimKey,
    live: LiveClaims,
}

impl Drop for ClaimReservation {
    fn drop(&mut self) {
        self.live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.key);
    }
}

#[derive(Clone)]
pub(crate) struct ClaimStore {
    pool: SqlitePool,
    live: LiveClaims,
    pub(super) admission: Arc<super::admission::Admission>,
}

const CREATE_JOURNAL: &str = "CREATE TABLE IF NOT EXISTS mobile_worker_claims (
    task_id TEXT NOT NULL,
    claim_epoch INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    handler_name TEXT NOT NULL,
    effect_id TEXT,
    api_base TEXT NOT NULL,
    started INTEGER NOT NULL DEFAULT 0,
    outcome TEXT,
    claimed_at TEXT NOT NULL,
    PRIMARY KEY (api_base, worker_id, task_id, claim_epoch)
)";

type StoredClaim = (String, i64, String, String, i64, Option<String>, String);

impl ClaimStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            live: Arc::default(),
            admission: Arc::default(),
        }
    }

    pub async fn init_tables(&self) -> Result<(), sqlx::Error> {
        // Reserve the writer before inspecting the old schema. Production uses
        // SqliteStorage's cancellation-safe pool release hook.
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query(CREATE_JOURNAL).execute(&mut *tx).await?;
        let columns = sqlx::query("PRAGMA table_info(mobile_worker_claims)")
            .fetch_all(&mut *tx)
            .await?;
        let mut primary_key = Vec::new();
        for column in columns {
            let order: i64 = column.try_get("pk")?;
            if order > 0 {
                primary_key.push((order, column.try_get::<String, _>("name")?));
            }
        }
        primary_key.sort_unstable_by_key(|(order, _)| *order);
        let names: Vec<&str> = primary_key.iter().map(|(_, name)| name.as_str()).collect();
        if names == ["task_id"] {
            // Rename, copy, and drop are one transaction: interrupted upgrades
            // expose either the complete old journal or the complete new one.
            sqlx::query("ALTER TABLE mobile_worker_claims RENAME TO mobile_worker_claims_legacy")
                .execute(&mut *tx)
                .await?;
            sqlx::query(CREATE_JOURNAL).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO mobile_worker_claims
                (task_id, claim_epoch, worker_id, handler_name, effect_id, api_base, started, outcome, claimed_at)
                SELECT task_id, claim_epoch, worker_id, handler_name, effect_id, api_base, started, outcome, claimed_at
                FROM mobile_worker_claims_legacy")
                .execute(&mut *tx).await?;
            sqlx::query("DROP TABLE mobile_worker_claims_legacy")
                .execute(&mut *tx)
                .await?;
        } else if names != ["api_base", "worker_id", "task_id", "claim_epoch"] {
            return Err(sqlx::Error::Protocol(
                "unexpected mobile worker journal primary key".into(),
            ));
        }
        tx.commit().await
    }

    pub fn reserve(&self, key: &ClaimKey) -> Option<ClaimReservation> {
        let inserted = self
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key.clone());
        inserted.then(|| ClaimReservation {
            key: key.clone(),
            live: Arc::clone(&self.live),
        })
    }

    /// Identical redelivery never erases a start marker or recorded outcome.
    pub async fn insert(
        &self,
        task: &RemoteTask,
        worker_id: &str,
        api_base: &str,
    ) -> Result<bool, sqlx::Error> {
        let key = ClaimKey::for_task(task, worker_id, api_base);
        let result = sqlx::query("INSERT INTO mobile_worker_claims
            (task_id, claim_epoch, worker_id, handler_name, effect_id, api_base, started, outcome, claimed_at)
            VALUES (?, ?, ?, ?, ?, ?, 0, NULL, ?)
            ON CONFLICT (api_base, worker_id, task_id, claim_epoch) DO NOTHING")
            .bind(key.task_id.to_string()).bind(key.database_epoch()?).bind(&key.worker_id)
            .bind(&task.handler_name).bind(task.effect_id.as_deref()).bind(&key.api_base)
            .bind(chrono::Utc::now().to_rfc3339()).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn mark_started(&self, key: &ClaimKey) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "UPDATE mobile_worker_claims SET started = 1
            WHERE task_id = ? AND claim_epoch = ? AND worker_id = ? AND api_base = ?",
        )
        .bind(key.task_id.to_string())
        .bind(key.database_epoch()?)
        .bind(&key.worker_id)
        .bind(&key.api_base)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn record_outcome(
        &self,
        key: &ClaimKey,
        outcome: &Outcome,
    ) -> Result<bool, sqlx::Error> {
        let json = serde_json::to_string(outcome)
            .map_err(|e| sqlx::Error::Protocol(format!("cannot encode worker outcome: {e}")))?;
        let result = sqlx::query(
            "UPDATE mobile_worker_claims SET outcome = ?
            WHERE task_id = ? AND claim_epoch = ? AND worker_id = ? AND api_base = ?",
        )
        .bind(json)
        .bind(key.task_id.to_string())
        .bind(key.database_epoch()?)
        .bind(&key.worker_id)
        .bind(&key.api_base)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn delete(&self, key: &ClaimKey) {
        let epoch = match key.database_epoch() {
            Ok(epoch) => epoch,
            Err(error) => {
                warn!(%error, "invalid worker journal key");
                return;
            }
        };
        if let Err(error) = sqlx::query(
            "DELETE FROM mobile_worker_claims
            WHERE task_id = ? AND claim_epoch = ? AND worker_id = ? AND api_base = ?",
        )
        .bind(key.task_id.to_string())
        .bind(epoch)
        .bind(&key.worker_id)
        .bind(&key.api_base)
        .execute(&self.pool)
        .await
        {
            warn!(%error, task_id = %key.task_id, "failed to delete settled worker claim");
        }
    }

    pub async fn get(&self, key: &ClaimKey) -> Result<Option<ClaimRow>, sqlx::Error> {
        let row: Option<StoredClaim> = sqlx::query_as("SELECT task_id, claim_epoch, worker_id, api_base, started, outcome, claimed_at
            FROM mobile_worker_claims WHERE task_id = ? AND claim_epoch = ? AND worker_id = ? AND api_base = ?")
            .bind(key.task_id.to_string()).bind(key.database_epoch()?)
            .bind(&key.worker_id).bind(&key.api_base).fetch_optional(&self.pool).await?;
        Ok(row.and_then(decode_row))
    }

    pub async fn list(&self) -> Result<Vec<ClaimRow>, sqlx::Error> {
        let rows: Vec<StoredClaim> = sqlx::query_as("SELECT task_id, claim_epoch, worker_id, api_base, started, outcome, claimed_at
            FROM mobile_worker_claims ORDER BY claimed_at, api_base, worker_id, task_id, claim_epoch")
            .fetch_all(&self.pool).await?;
        Ok(rows.into_iter().filter_map(decode_row).collect())
    }

    pub async fn count(&self) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("SELECT COUNT(*) FROM mobile_worker_claims")
            .fetch_one(&self.pool)
            .await
    }
}

fn decode_row(
    (id, epoch, worker_id, api_base, started, outcome, claimed_at): StoredClaim,
) -> Option<ClaimRow> {
    Some(ClaimRow {
        key: ClaimKey {
            task_id: uuid::Uuid::parse_str(&id).ok()?,
            claim_epoch: u64::try_from(epoch).ok()?,
            worker_id,
            api_base,
        },
        started: started != 0,
        outcome: outcome.and_then(|o| serde_json::from_str(&o).ok()),
        claimed_at: chrono::DateTime::parse_from_rfc3339(&claimed_at)
            .map_or_else(|_| chrono::Utc::now(), |t| t.with_timezone(&chrono::Utc)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::sqlite::SqliteStorage;
    use serde_json::json;

    const LEGACY_SCHEMA: &str = "CREATE TABLE mobile_worker_claims (
        task_id TEXT PRIMARY KEY, claim_epoch INTEGER NOT NULL,
        worker_id TEXT NOT NULL, handler_name TEXT NOT NULL, effect_id TEXT,
        api_base TEXT NOT NULL, started INTEGER NOT NULL DEFAULT 0,
        outcome TEXT, claimed_at TEXT NOT NULL
    )";

    async fn legacy_claim(pool: &SqlitePool) -> (uuid::Uuid, Outcome) {
        sqlx::query(LEGACY_SCHEMA).execute(pool).await.unwrap();
        let id = uuid::Uuid::new_v4();
        let outcome = Outcome::Complete {
            output: json!({"receipt":"durable-before-upgrade"}),
        };
        sqlx::query("INSERT INTO mobile_worker_claims VALUES (?, 17, 'original-worker', 'scan', 'effect-1', 'https://control.example/api/v1', 1, ?, '2026-10-23T01:02:03Z')")
            .bind(id.to_string()).bind(serde_json::to_string(&outcome).unwrap())
            .execute(pool).await.unwrap();
        (id, outcome)
    }

    #[tokio::test]
    async fn upgrading_the_legacy_journal_preserves_identity_and_durable_outcome() {
        let sqlite = SqliteStorage::in_memory().await.unwrap();
        let (id, outcome) = legacy_claim(sqlite.pool()).await;
        let store = ClaimStore::new(sqlite.pool().clone());
        store.init_tables().await.unwrap();
        store.init_tables().await.unwrap();
        let rows = store.list().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key.task_id, id);
        assert_eq!(rows[0].key.claim_epoch, 17);
        assert_eq!(rows[0].key.worker_id, "original-worker");
        assert_eq!(rows[0].key.api_base, "https://control.example/api/v1");
        assert_eq!(rows[0].claimed_at.to_rfc3339(), "2026-10-23T01:02:03+00:00");
        assert!(rows[0].started);
        assert_eq!(rows[0].outcome.as_ref(), Some(&outcome));
        let (handler, effect): (String, String) = sqlx::query_as(
            "SELECT handler_name, effect_id FROM mobile_worker_claims WHERE task_id = ?",
        )
        .bind(id.to_string())
        .fetch_one(sqlite.pool())
        .await
        .unwrap();
        assert_eq!((handler.as_str(), effect.as_str()), ("scan", "effect-1"));
        let task: RemoteTask = serde_json::from_value(json!({
            "id":id,"instance_id":uuid::Uuid::new_v4(),"block_id":"scan","handler_name":"scan","claim_epoch":17
        })).unwrap();
        assert!(
            store
                .insert(&task, "another-worker", "https://control.example/api/v1")
                .await
                .unwrap()
        );
        assert_eq!(store.count().await.unwrap(), 2);
        assert_eq!(
            store.get(&rows[0].key).await.unwrap().unwrap().outcome,
            Some(outcome)
        );
    }

    #[tokio::test]
    async fn failed_upgrade_leaves_the_original_journal_intact() {
        let sqlite = SqliteStorage::in_memory().await.unwrap();
        let (id, outcome) = legacy_claim(sqlite.pool()).await;
        sqlx::query("CREATE TABLE mobile_worker_claims_legacy (keep_me TEXT)")
            .execute(sqlite.pool())
            .await
            .unwrap();
        let store = ClaimStore::new(sqlite.pool().clone());
        assert!(store.init_tables().await.is_err());
        let rows = store.list().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key.task_id, id);
        assert_eq!(rows[0].outcome.as_ref(), Some(&outcome));
        let key: i64 = sqlx::query_scalar(
            "SELECT pk FROM pragma_table_info('mobile_worker_claims') WHERE name = 'task_id'",
        )
        .fetch_one(sqlite.pool())
        .await
        .unwrap();
        assert_eq!(key, 1, "a failed upgrade must roll back schema changes too");
    }

    #[tokio::test]
    async fn concurrent_initialization_converges_without_losing_recovery_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("claims.db");
        let sqlite = SqliteStorage::file(path.to_str().unwrap()).await.unwrap();
        let (id, outcome) = legacy_claim(sqlite.pool()).await;
        let a = ClaimStore::new(sqlite.pool().clone());
        let b = ClaimStore::new(sqlite.pool().clone());
        let (left, right) = tokio::join!(a.init_tables(), b.init_tables());
        left.unwrap();
        right.unwrap();
        let rows = a.list().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key.task_id, id);
        assert_eq!(rows[0].outcome.as_ref(), Some(&outcome));
    }

    #[tokio::test]
    async fn out_of_range_claim_epochs_fail_before_journaling() {
        let sqlite = SqliteStorage::in_memory().await.unwrap();
        let store = ClaimStore::new(sqlite.pool().clone());
        store.init_tables().await.unwrap();
        let task: RemoteTask = serde_json::from_value(json!({
            "id":uuid::Uuid::new_v4(),"instance_id":uuid::Uuid::new_v4(),"block_id":"scan","handler_name":"scan","claim_epoch":u64::MAX
        })).unwrap();
        assert!(
            store
                .insert(&task, "worker", "https://control.example/api/v1")
                .await
                .is_err()
        );
        assert_eq!(store.count().await.unwrap(), 0);
    }
}
