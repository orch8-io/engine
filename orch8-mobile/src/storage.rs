//! Mobile-specific storage operations layered on top of `SqliteStorage`.
//!
//! These tables are created by the bundled schema in `orch8-storage` and are
//! idempotent (`IF NOT EXISTS`). Only the mobile SDK reads/writes them.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::error::StorageError;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::InstanceState;

// Internal persistence foundation; scoped execution and SDK handles are still
// under development. Do not expose this as a complete scoped engine API.
mod legacy_sync;
pub(crate) mod manifest;
#[allow(dead_code)]
pub(crate) mod scopes;
mod starts;

#[cfg(test)]
struct MetadataBarrier {
    entered: tokio::sync::oneshot::Sender<()>,
    release: Arc<tokio::sync::Semaphore>,
}

/// Mobile-specific storage wrapper.
pub struct MobileStorage {
    inner: Arc<SqliteStorage>,
    #[cfg(test)]
    metadata_barrier: std::sync::Mutex<Option<MetadataBarrier>>,
}

#[allow(dead_code)]
impl MobileStorage {
    pub fn new(inner: Arc<SqliteStorage>) -> Self {
        Self {
            inner,
            #[cfg(test)]
            metadata_barrier: std::sync::Mutex::new(None),
        }
    }

    pub(crate) fn pool(&self) -> &sqlx::SqlitePool {
        self.inner.pool()
    }

    /// Return only the latest version number for each locally cached sequence.
    /// Manifest reconciliation does not inspect blocks or schemas, so loading
    /// complete definitions here would retain potentially megabytes per row.
    pub(crate) async fn list_local_sequence_versions(
        &self,
        tenant_id: &TenantId,
        namespace: &Namespace,
        limit: u32,
    ) -> Result<Vec<(String, i32)>, StorageError> {
        sqlx::query_as(
            "SELECT name, MAX(version)
             FROM sequences
             WHERE tenant_id = ?1 AND namespace = ?2
             GROUP BY name
             ORDER BY name
             LIMIT ?3",
        )
        .bind(tenant_id.as_str())
        .bind(namespace.as_str())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await
        .map_err(Into::into)
    }

    /// Select the oldest rows exceeding `retain`, using one statement so the
    /// count and candidate set come from the same `SQLite` snapshot.
    pub(crate) async fn list_excess_oldest_local_sequences(
        &self,
        tenant_id: &TenantId,
        namespace: &Namespace,
        retain: u32,
    ) -> Result<Vec<(SequenceId, String)>, StorageError> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT id, name
             FROM sequences
             WHERE tenant_id = ?1 AND namespace = ?2
             ORDER BY created_at ASC, id ASC
             LIMIT (
                 SELECT CASE WHEN COUNT(*) > ?3 THEN COUNT(*) - ?3 ELSE 0 END
                 FROM sequences
                 WHERE tenant_id = ?1 AND namespace = ?2
             )",
        )
        .bind(tenant_id.as_str())
        .bind(namespace.as_str())
        .bind(i64::from(retain))
        .fetch_all(self.pool())
        .await?;

        rows.into_iter()
            .map(|(id, name)| parse_sequence_id(&id).map(|id| (id, name)))
            .collect()
    }

    // ── Telemetry ──

    /// Append a telemetry event to the local buffer.
    pub async fn append_telemetry_event(
        &self,
        event_type: &str,
        payload: &str,
    ) -> Result<(), StorageError> {
        let created_at = Utc::now().to_rfc3339();
        self.append_telemetry_event_at(event_type, payload, &created_at)
            .await
    }

    /// Append a telemetry event whose timestamp was already captured by the
    /// caller, avoiding a second clock read and RFC 3339 allocation.
    pub async fn append_telemetry_event_at(
        &self,
        event_type: &str,
        payload: &str,
        created_at: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO telemetry_events (event_type, payload, created_at) VALUES (?1, ?2, ?3)",
        )
        .bind(event_type)
        .bind(payload)
        .bind(created_at)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Read up to `limit` telemetry events, oldest first.
    pub async fn read_telemetry_events(
        &self,
        limit: u32,
    ) -> Result<Vec<TelemetryEvent>, StorageError> {
        let rows = sqlx::query_as::<_, TelemetryEventRow>(
            "SELECT id, event_type, payload, created_at FROM telemetry_events ORDER BY id ASC LIMIT ?1",
        )
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Delete telemetry events by their row IDs.
    pub async fn delete_telemetry_events(&self, ids: &[i64]) -> Result<u64, StorageError> {
        if ids.is_empty() {
            return Ok(0);
        }
        // SQLite doesn't support binding arrays directly; build an IN clause safely using QueryBuilder.
        let mut qb = sqlx::QueryBuilder::new("DELETE FROM telemetry_events WHERE id IN (");
        let mut separated = qb.separated(",");
        for id in ids {
            separated.push_bind(id);
        }
        separated.push_unseparated(")");
        let result = qb.build().execute(self.pool()).await?;
        Ok(result.rows_affected())
    }

    /// Count telemetry events in the buffer.
    pub async fn count_telemetry_events(&self) -> Result<u64, StorageError> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events")
            .fetch_one(self.pool())
            .await?;
        #[allow(clippy::cast_sign_loss)]
        Ok(count as u64)
    }

    /// Delete up to `limit` oldest telemetry rows without loading their
    /// payloads into memory first.
    pub async fn delete_oldest_telemetry_events(&self, limit: u64) -> Result<u64, StorageError> {
        if limit == 0 {
            return Ok(0);
        }
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let result = sqlx::query(
            "DELETE FROM telemetry_events WHERE id IN (
                SELECT id FROM telemetry_events ORDER BY id ASC LIMIT ?1
            )",
        )
        .bind(limit)
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected())
    }

    // ── Sync Metadata ──

    pub async fn get_sync_metadata(&self, key: &str) -> Result<Option<String>, StorageError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM sync_metadata WHERE key = ?1")
                .bind(key)
                .fetch_optional(self.pool())
                .await?;
        Ok(row.map(|r| r.0))
    }

    #[cfg(test)]
    pub(crate) fn pause_next_metadata_commit(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        Arc<tokio::sync::Semaphore>,
    ) {
        let (entered, observed) = tokio::sync::oneshot::channel();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        *self.metadata_barrier.lock().unwrap() = Some(MetadataBarrier {
            entered,
            release: Arc::clone(&release),
        });
        (observed, release)
    }

    /// Commit one representation's validators and replay fence together.
    /// A missing value removes that key, including a retired `ETag`.
    pub(crate) async fn update_sync_metadata_batch(
        &self,
        changes: &[(&str, Option<&str>)],
    ) -> Result<(), StorageError> {
        self.update_sync_metadata_batch_inner(changes, None).await
    }

    pub(crate) async fn commit_manifest_evaluation(
        &self,
        changes: &[(&str, Option<&str>)],
        version: i64,
        digest: &str,
    ) -> Result<(), StorageError> {
        let expected_version = version.to_string();
        for (key, value) in changes {
            if [manifest::ACCEPTED_VERSION, manifest::ACCEPTED_DIGEST].contains(key)
                || (*key == "last_manifest_version" && *value != Some(expected_version.as_str()))
            {
                return Err(StorageError::Conflict(
                    "evaluation cannot change manifest acceptance identity".into(),
                ));
            }
        }
        self.update_sync_metadata_batch_inner(changes, Some((version, digest)))
            .await
    }

    async fn update_sync_metadata_batch_inner(
        &self,
        changes: &[(&str, Option<&str>)],
        acceptance: Option<(i64, &str)>,
    ) -> Result<(), StorageError> {
        let mut tx = self.pool().begin().await?;
        if let Some((version, digest)) = acceptance {
            sqlx::query(
                "UPDATE sync_metadata SET value = value WHERE key = 'accepted_manifest_version'",
            )
            .execute(&mut *tx)
            .await?;
            manifest::legacy_trust_in(&mut tx)
                .await?
                .require_current(version, digest)?;
        }
        let timestamp = Utc::now().to_rfc3339();
        for (key, value) in changes {
            if let Some(value) = value {
                sqlx::query("INSERT INTO sync_metadata (key, value, updated_at) VALUES (?1, ?2, ?3) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at")
                    .bind(key).bind(value).bind(&timestamp).execute(&mut *tx).await?;
            } else {
                sqlx::query("DELETE FROM sync_metadata WHERE key = ?1")
                    .bind(key)
                    .execute(&mut *tx)
                    .await?;
            }
            #[cfg(test)]
            {
                let barrier = self.metadata_barrier.lock().unwrap().take();
                if let Some(MetadataBarrier { entered, release }) = barrier {
                    let _ = entered.send(());
                    release.acquire().await.unwrap().forget();
                }
            }
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn set_sync_metadata(&self, key: &str, value: &str) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO sync_metadata (key, value, updated_at) VALUES (?1, ?2, ?3) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
        )
        .bind(key)
        .bind(value)
        .bind(Utc::now().to_rfc3339())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    // ── Trusted Keys ──

    pub async fn get_trusted_key(&self, key_id: &str) -> Result<Option<String>, StorageError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT public_key FROM trusted_keys WHERE key_id = ?1")
                .bind(key_id)
                .fetch_optional(self.pool())
                .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn list_trusted_keys(&self) -> Result<Vec<(String, String)>, StorageError> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT key_id, public_key FROM trusted_keys")
                .fetch_all(self.pool())
                .await?;
        Ok(rows)
    }

    pub async fn upsert_trusted_key(
        &self,
        key_id: &str,
        public_key: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO trusted_keys (key_id, public_key, trusted_since) VALUES (?1, ?2, ?3) \
             ON CONFLICT(key_id) DO UPDATE SET public_key = excluded.public_key, trusted_since = excluded.trusted_since",
        )
        .bind(key_id)
        .bind(public_key)
        .bind(Utc::now().to_rfc3339())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    pub async fn delete_trusted_key(&self, key_id: &str) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM trusted_keys WHERE key_id = ?1")
            .bind(key_id)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    // ── Mobile Dedup (persistent) ──

    pub async fn get_dedup_instance(&self, key: &str) -> Result<Option<String>, StorageError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT instance_id FROM mobile_dedup WHERE dedup_key = ?1")
                .bind(key)
                .fetch_optional(self.pool())
                .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn set_dedup(&self, key: &str, instance_id: &str) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO mobile_dedup (dedup_key, instance_id, created_at) VALUES (?1, ?2, ?3) \
             ON CONFLICT(dedup_key) DO UPDATE SET instance_id = excluded.instance_id",
        )
        .bind(key)
        .bind(instance_id)
        .bind(Utc::now().to_rfc3339())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    pub async fn remove_dedup(&self, instance_id: &str) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM mobile_dedup WHERE instance_id = ?1")
            .bind(instance_id)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// Remove crash leftovers for missing or terminal instances at startup.
    pub async fn prune_stale_dedup(&self) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "DELETE FROM mobile_dedup
             WHERE NOT EXISTS (
                 SELECT 1 FROM task_instances
                 WHERE task_instances.id = mobile_dedup.instance_id
                   AND task_instances.state IN ('scheduled', 'running', 'waiting', 'paused')
             )",
        )
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected())
    }

    /// List all dedup entries for hydration on engine startup.
    pub async fn list_all_dedup(&self) -> Result<Vec<(String, String)>, StorageError> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT dedup_key, instance_id FROM mobile_dedup")
                .fetch_all(self.pool())
                .await?;
        Ok(rows)
    }

    // ── Lightweight instance projections ──

    /// List active instances without loading their metadata, context, budget,
    /// or other scheduler-only columns. The join also avoids one sequence
    /// lookup per summary returned across the mobile FFI boundary.
    pub(crate) async fn list_active_instance_summaries(
        &self,
        limit: u32,
    ) -> Result<Vec<(ActiveInstanceProjection, String)>, StorageError> {
        let rows = sqlx::query_as::<_, ActiveInstanceProjectionRow>(sqlx::AssertSqlSafe(format!(
            "SELECT i.id, i.state, i.created_at, COALESCE(s.name, '') AS sequence_name
             FROM task_instances i
             LEFT JOIN sequences s ON s.id = i.sequence_id
             WHERE i.state IN ('scheduled', 'running', 'waiting', 'paused') AND ({})
             ORDER BY i.updated_at DESC
             LIMIT ?1",
            legacy_sync::OWNER,
        )))
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    /// Return only IDs of active instances older than `cutoff`. GC does not
    /// inspect workflow context, so materializing complete `TaskInstance`
    /// values would waste memory proportional to every context payload.
    pub(crate) async fn list_expired_active_instance_ids(
        &self,
        cutoff: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<InstanceId>, StorageError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT id FROM task_instances
             WHERE state IN ('scheduled', 'running', 'waiting', 'paused')
               AND created_at < ?1
             ORDER BY created_at ASC
             LIMIT ?2",
        )
        .bind(cutoff.to_rfc3339())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;

        rows.into_iter()
            .map(|(id,)| parse_instance_id(&id))
            .collect()
    }

    /// List terminal instance identifiers and states without loading context.
    /// Notification delivery fetches a complete row only for a newly observed
    /// callback, while listener-free dedup cleanup needs IDs alone.
    ///
    /// MC-03: keyset-paged by `id` and drained completely. A fixed
    /// `LIMIT 100 ORDER BY updated_at DESC` silently dropped every transition
    /// beyond the newest hundred when more than 100 instances finished between
    /// two scans, so older completions were never reported.
    pub(crate) async fn list_sync_terminal_instance_states(
        &self,
    ) -> Result<Vec<(InstanceId, InstanceState)>, StorageError> {
        self.list_terminal_instance_states().await
    }

    /// The legacy notification/dedup domain. Catalog callbacks need their own
    /// owner/session-bound projection and cannot reuse a global listener.
    pub(crate) async fn list_terminal_instance_states(
        &self,
    ) -> Result<Vec<(InstanceId, InstanceState)>, StorageError> {
        let mut all = Vec::new();
        let mut after = String::new();
        loop {
            let page = self
                .list_terminal_instance_states_page(&after, SCAN_PAGE)
                .await?;
            let full = page.len() == SCAN_PAGE as usize;
            if let Some((id, _)) = page.last() {
                after = id.to_string();
            }
            all.extend(page);
            if !full {
                return Ok(all);
            }
        }
    }

    async fn list_terminal_instance_states_page(
        &self,
        after: &str,
        limit: u32,
    ) -> Result<Vec<(InstanceId, InstanceState)>, StorageError> {
        let rows: Vec<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT i.id, i.state FROM task_instances i
             WHERE i.state IN ('completed', 'failed', 'cancelled')
               AND i.id > ?1 AND ({})
             ORDER BY i.id
             LIMIT ?2",
            legacy_sync::OWNER,
        )))
        .bind(after)
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;

        rows.into_iter()
            .map(|(id, state)| {
                let id = parse_instance_id(&id)?;
                let state = state
                    .parse()
                    .map_err(|error| projection_error("state", &state, error))?;
                Ok((id, state))
            })
            .collect()
    }

    /// List only the identifiers needed to emit waiting-step callbacks.
    /// Extracting `current_step` inside `SQLite` avoids parsing and retaining up
    /// to `limit` complete execution contexts on every notification scan.
    ///
    /// MC-03: drained completely via keyset paging (see
    /// [`Self::list_terminal_instance_states`]).
    pub(crate) async fn list_waiting_steps(
        &self,
    ) -> Result<Vec<WaitingStepProjection>, StorageError> {
        let mut all = Vec::new();
        let mut after = String::new();
        loop {
            let page = self.list_waiting_steps_page(&after, SCAN_PAGE).await?;
            let full = page.len() == SCAN_PAGE as usize;
            if let Some(last) = page.last() {
                after = last.id.to_string();
            }
            all.extend(page);
            if !full {
                return Ok(all);
            }
        }
    }

    async fn list_waiting_steps_page(
        &self,
        after: &str,
        limit: u32,
    ) -> Result<Vec<WaitingStepProjection>, StorageError> {
        let rows: Vec<(String, String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT i.id, i.sequence_id,
                    CASE WHEN json_valid(i.context) THEN json_extract(i.context, '$.runtime.current_step') END
             FROM task_instances i
             WHERE i.state = 'waiting' AND ({})
               AND CASE WHEN json_valid(i.context) THEN json_type(i.context, '$.runtime.current_step') = 'text' ELSE 0 END
               AND i.id > ?1
             ORDER BY i.id
             LIMIT ?2", legacy_sync::OWNER,
        )))
        .bind(after)
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;

        rows.into_iter()
            .map(|(id, sequence_id, step_id)| {
                Ok(WaitingStepProjection {
                    id: parse_instance_id(&id)?,
                    sequence_id: parse_sequence_id(&sequence_id)?,
                    step_id: BlockId::new(step_id),
                })
            })
            .collect()
    }

    /// Read the small instance header needed for cloud status reporting.
    /// Context JSON can approach the mobile context ceiling and is deliberately
    /// reduced to its current-step string inside `SQLite`.
    ///
    /// MC-03/OPT-01: every non-terminal instance, drained completely via
    /// keyset paging on `id`.
    pub(crate) async fn list_sync_active_instances(
        &self,
    ) -> Result<Vec<SyncInstanceProjection>, StorageError> {
        let mut all = Vec::new();
        let mut after = String::new();
        loop {
            let rows =
                sqlx::query_as::<_, SyncInstanceProjectionRow>(sqlx::AssertSqlSafe(format!(
                    "SELECT i.id, i.sequence_id, i.state,
                        CASE WHEN json_valid(i.context)
                             THEN json_extract(i.context, '$.runtime.current_step')
                        END AS current_step
                 FROM task_instances i
                 WHERE i.state IN ('scheduled', 'running', 'waiting')
                   AND i.id > ?1 AND ({})
                 ORDER BY i.id
                 LIMIT ?2",
                    legacy_sync::OWNER,
                )))
                .bind(&after)
                .bind(i64::from(SCAN_PAGE))
                .fetch_all(self.pool())
                .await?;
            let full = rows.len() == SCAN_PAGE as usize;
            if let Some(last) = rows.last() {
                after.clone_from(&last.id);
            }
            for row in rows {
                all.push(row.try_into()?);
            }
            if !full {
                return Ok(all);
            }
        }
    }

    /// The same status header as [`Self::list_sync_active_instances`] for an
    /// explicit set of (typically newly-terminal) instances.
    pub(crate) async fn list_sync_instances_by_ids(
        &self,
        instance_ids: &[InstanceId],
    ) -> Result<Vec<SyncInstanceProjection>, StorageError> {
        let mut all = Vec::with_capacity(instance_ids.len());
        for chunk in instance_ids.chunks(SCAN_PAGE as usize) {
            let mut query = sqlx::QueryBuilder::new(format!(
                "SELECT i.id, i.sequence_id, i.state,
                        CASE WHEN json_valid(i.context)
                             THEN json_extract(i.context, '$.runtime.current_step')
                        END AS current_step
                 FROM task_instances i WHERE ({}) AND i.id IN (",
                legacy_sync::OWNER,
            ));
            let mut ids = query.separated(",");
            for id in chunk {
                ids.push_bind(id.to_string());
            }
            ids.push_unseparated(") ORDER BY i.id");
            let rows = query
                .build_query_as::<SyncInstanceProjectionRow>()
                .fetch_all(self.pool())
                .await?;
            for row in rows {
                all.push(row.try_into()?);
            }
        }
        Ok(all)
    }

    /// Fetch execution-tree fields for a status batch in one query instead of
    /// issuing one `get_execution_tree` query per instance.
    pub(crate) async fn list_sync_execution_steps(
        &self,
        instance_ids: &[InstanceId],
    ) -> Result<Vec<SyncExecutionStepProjection>, StorageError> {
        let mut all = Vec::new();
        for chunk in instance_ids.chunks(SCAN_PAGE as usize) {
            let mut query = sqlx::QueryBuilder::new(
                "SELECT instance_id, block_id, block_type, state, started_at, completed_at
                 FROM execution_tree WHERE instance_id IN (",
            );
            let mut ids = query.separated(",");
            for instance_id in chunk {
                ids.push_bind(instance_id.to_string());
            }
            ids.push_unseparated(") ORDER BY instance_id, id");

            let rows = query
                .build_query_as::<SyncExecutionStepProjectionRow>()
                .fetch_all(self.pool())
                .await?;
            for row in rows {
                all.push(row.try_into()?);
            }
        }
        Ok(all)
    }
}

/// Page size for the drain-to-completion scans (MC-03).
const SCAN_PAGE: u32 = 500;

/// Fields needed by `MobileEngine::active_instances`; intentionally excludes
/// the potentially large execution context.
pub(crate) struct ActiveInstanceProjection {
    pub id: InstanceId,
    pub state: InstanceState,
    pub created_at: DateTime<Utc>,
}

pub(crate) struct WaitingStepProjection {
    pub id: InstanceId,
    pub sequence_id: SequenceId,
    pub step_id: BlockId,
}

pub(crate) struct SyncInstanceProjection {
    pub id: InstanceId,
    pub sequence_id: SequenceId,
    pub state: InstanceState,
    pub current_step: Option<BlockId>,
}

pub(crate) struct SyncExecutionStepProjection {
    pub instance_id: InstanceId,
    pub block_id: BlockId,
    pub block_type: String,
    pub state: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
}

#[derive(sqlx::FromRow)]
struct SyncInstanceProjectionRow {
    id: String,
    sequence_id: String,
    state: String,
    current_step: Option<String>,
}

impl TryFrom<SyncInstanceProjectionRow> for SyncInstanceProjection {
    type Error = StorageError;

    fn try_from(row: SyncInstanceProjectionRow) -> Result<Self, Self::Error> {
        let state = row
            .state
            .parse()
            .map_err(|error| projection_error("state", &row.state, error))?;
        Ok(Self {
            id: parse_instance_id(&row.id)?,
            sequence_id: parse_sequence_id(&row.sequence_id)?,
            state,
            current_step: row.current_step.map(BlockId::new),
        })
    }
}

#[derive(sqlx::FromRow)]
struct SyncExecutionStepProjectionRow {
    instance_id: String,
    block_id: String,
    block_type: String,
    state: String,
    started_at: Option<String>,
    completed_at: Option<String>,
}

impl TryFrom<SyncExecutionStepProjectionRow> for SyncExecutionStepProjection {
    type Error = StorageError;

    fn try_from(row: SyncExecutionStepProjectionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            instance_id: parse_instance_id(&row.instance_id)?,
            block_id: BlockId::new(row.block_id),
            block_type: row.block_type,
            state: row.state,
            started_at: row.started_at,
            completed_at: row.completed_at,
        })
    }
}

#[derive(sqlx::FromRow)]
struct ActiveInstanceProjectionRow {
    id: String,
    state: String,
    created_at: String,
    sequence_name: String,
}

impl TryFrom<ActiveInstanceProjectionRow> for (ActiveInstanceProjection, String) {
    type Error = StorageError;

    fn try_from(row: ActiveInstanceProjectionRow) -> Result<Self, Self::Error> {
        let id = parse_instance_id(&row.id)?;
        let state = row
            .state
            .parse()
            .map_err(|error| projection_error("state", &row.state, error))?;
        let created_at = row
            .created_at
            .parse()
            .map_err(|error| projection_error("created_at", &row.created_at, error))?;
        Ok((
            ActiveInstanceProjection {
                id,
                state,
                created_at,
            },
            row.sequence_name,
        ))
    }
}

fn parse_instance_id(value: &str) -> Result<InstanceId, StorageError> {
    uuid::Uuid::parse_str(value)
        .map(InstanceId::from_uuid)
        .map_err(|error| projection_error("id", value, error))
}

fn parse_sequence_id(value: &str) -> Result<SequenceId, StorageError> {
    uuid::Uuid::parse_str(value)
        .map(SequenceId::from_uuid)
        .map_err(|error| projection_error("sequence_id", value, error))
}

fn projection_error(field: &str, value: &str, error: impl std::fmt::Display) -> StorageError {
    StorageError::Query(format!(
        "invalid task_instances.{field} value '{value}': {error}"
    ))
}

/// A telemetry event stored in the local `SQLite` buffer.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TelemetryEvent {
    #[serde(skip)]
    pub id: i64,
    pub event_type: String,
    pub payload: String,
    /// Already stored as RFC 3339; keeping it borrowed-ready avoids parsing
    /// and formatting every timestamp again during a telemetry flush.
    #[serde(rename = "timestamp")]
    pub created_at: String,
}

#[derive(sqlx::FromRow)]
struct TelemetryEventRow {
    id: i64,
    event_type: String,
    payload: String,
    created_at: String,
}

impl From<TelemetryEventRow> for TelemetryEvent {
    fn from(row: TelemetryEventRow) -> Self {
        Self {
            id: row.id,
            event_type: row.event_type,
            payload: row.payload,
            created_at: row.created_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> (MobileStorage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db").to_string_lossy().to_string();
        let sqlite = SqliteStorage::file_mobile(&path).await.unwrap();
        (MobileStorage::new(Arc::new(sqlite)), dir)
    }

    #[tokio::test]
    async fn telemetry_roundtrip() {
        let (storage, _dir) = setup().await;

        storage
            .append_telemetry_event("SyncCompleted", r#"{"version":1}"#)
            .await
            .unwrap();

        let events = storage.read_telemetry_events(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "SyncCompleted");
        assert_eq!(events[0].payload, r#"{"version":1}"#);

        let count = storage.count_telemetry_events().await.unwrap();
        assert_eq!(count, 1);

        storage
            .delete_telemetry_events(&[events[0].id])
            .await
            .unwrap();
        let count = storage.count_telemetry_events().await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn sync_metadata_roundtrip() {
        let (storage, _dir) = setup().await;

        assert!(
            storage
                .get_sync_metadata("last_sync_ts")
                .await
                .unwrap()
                .is_none()
        );

        storage
            .set_sync_metadata("last_sync_ts", "2026-01-01T00:00:00Z")
            .await
            .unwrap();

        let val = storage.get_sync_metadata("last_sync_ts").await.unwrap();
        assert_eq!(val, Some("2026-01-01T00:00:00Z".to_string()));
    }

    #[tokio::test]
    async fn trusted_keys_roundtrip() {
        let (storage, _dir) = setup().await;

        storage.upsert_trusted_key("key1", "pubkey1").await.unwrap();

        let key = storage.get_trusted_key("key1").await.unwrap();
        assert_eq!(key, Some("pubkey1".to_string()));

        let keys = storage.list_trusted_keys().await.unwrap();
        assert_eq!(keys.len(), 1);

        storage.delete_trusted_key("key1").await.unwrap();
        let keys = storage.list_trusted_keys().await.unwrap();
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn dedup_roundtrip() {
        let (storage, _dir) = setup().await;

        assert!(storage.get_dedup_instance("dk1").await.unwrap().is_none());

        storage.set_dedup("dk1", "inst-1").await.unwrap();
        let id = storage.get_dedup_instance("dk1").await.unwrap();
        assert_eq!(id, Some("inst-1".to_string()));

        storage.remove_dedup("inst-1").await.unwrap();
        assert!(storage.get_dedup_instance("dk1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dedup_list_all_returns_all_entries() {
        let (storage, _dir) = setup().await;

        storage.set_dedup("dk1", "inst-1").await.unwrap();
        storage.set_dedup("dk2", "inst-2").await.unwrap();

        let all = storage.list_all_dedup().await.unwrap();
        assert_eq!(all.len(), 2);
        let map: std::collections::HashMap<_, _> = all.into_iter().collect();
        assert_eq!(map.get("dk1"), Some(&"inst-1".to_string()));
        assert_eq!(map.get("dk2"), Some(&"inst-2".to_string()));
    }

    #[tokio::test]
    async fn dedup_prune_removes_orphan_rows() {
        let (storage, _dir) = setup().await;
        storage
            .set_dedup("stale", "missing-instance")
            .await
            .unwrap();

        assert_eq!(storage.prune_stale_dedup().await.unwrap(), 1);
        assert!(storage.list_all_dedup().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn terminal_projection_does_not_deserialize_context() {
        let (storage, _dir) = setup().await;
        let id = InstanceId::new();
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO task_instances
             (id, sequence_id, tenant_id, namespace, state, context, created_at, updated_at)
             VALUES (?1, ?2, 'mobile', 'default', 'completed', 'not-json', ?3, ?3)",
        )
        .bind(id.to_string())
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        let projected = storage.list_terminal_instance_states().await.unwrap();
        assert_eq!(projected, vec![(id, InstanceState::Completed)]);
    }

    #[tokio::test]
    async fn waiting_projection_extracts_step_without_deserializing_context() {
        let (storage, _dir) = setup().await;
        let id = InstanceId::new();
        let sequence_id = SequenceId::new();
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO task_instances
             (id, sequence_id, tenant_id, namespace, state, context, created_at, updated_at)
             VALUES (?1, ?2, 'mobile', 'default', 'waiting',
                     '{\"runtime\":{\"current_step\":\"review\"},\"audit\":\"not-an-array\"}',
                     ?3, ?3)",
        )
        .bind(id.to_string())
        .bind(sequence_id.to_string())
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        let projected = storage.list_waiting_steps().await.unwrap();
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].id, id);
        assert_eq!(projected[0].sequence_id, sequence_id);
        assert_eq!(projected[0].step_id.as_str(), "review");
    }

    #[tokio::test]
    async fn sync_projection_excludes_large_context_and_batches_tree_rows() {
        let (storage, _dir) = setup().await;
        let id = InstanceId::new();
        let sequence_id = SequenceId::new();
        let now = Utc::now().to_rfc3339();
        let context = serde_json::json!({
            "data": "x".repeat(256 * 1024),
            "runtime": {"current_step": "review"},
            // This valid JSON intentionally cannot deserialize as ExecutionContext.
            "audit": "not-an-array"
        })
        .to_string();
        sqlx::query(
            "INSERT INTO task_instances
             (id, sequence_id, tenant_id, namespace, state, context, created_at, updated_at)
             VALUES (?1, ?2, 'mobile', 'default', 'waiting', ?3, ?4, ?4)",
        )
        .bind(id.to_string())
        .bind(sequence_id.to_string())
        .bind(&context)
        .bind(&now)
        .execute(storage.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO execution_tree
             (id, instance_id, block_id, block_type, state)
             VALUES (?1, ?2, 'review', 'step', 'waiting')",
        )
        .bind(uuid::Uuid::now_v7().to_string())
        .bind(id.to_string())
        .execute(storage.pool())
        .await
        .unwrap();

        let projected = storage.list_sync_active_instances().await.unwrap();
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].id, id);
        assert_eq!(projected[0].sequence_id, sequence_id);
        assert_eq!(
            projected[0].current_step.as_ref().map(BlockId::as_str),
            Some("review")
        );

        let projected_bytes = id.to_string().len()
            + sequence_id.to_string().len()
            + projected[0].state.to_string().len()
            + projected[0]
                .current_step
                .as_ref()
                .map_or(0, |step| step.as_str().len());
        assert!(context.len() > 256 * 1024);
        assert!(projected_bytes < 128);

        let tree = storage.list_sync_execution_steps(&[id]).await.unwrap();
        assert_eq!(tree.len(), 1);
        assert_eq!(tree[0].instance_id, id);
        assert_eq!(tree[0].block_id.as_str(), "review");
        assert_eq!(tree[0].state, "waiting");
    }

    #[tokio::test]
    async fn telemetry_delete_empty_is_noop() {
        let (storage, _dir) = setup().await;
        let deleted = storage.delete_telemetry_events(&[]).await.unwrap();
        assert_eq!(deleted, 0);
    }

    #[tokio::test]
    async fn telemetry_oldest_first_ordering() {
        let (storage, _dir) = setup().await;

        storage.append_telemetry_event("A", "1").await.unwrap();
        storage.append_telemetry_event("B", "2").await.unwrap();
        storage.append_telemetry_event("C", "3").await.unwrap();

        let events = storage.read_telemetry_events(2).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "A");
        assert_eq!(events[1].event_type, "B");
    }

    #[tokio::test]
    async fn telemetry_delete_oldest_is_bounded() {
        let (storage, _dir) = setup().await;
        storage.append_telemetry_event("A", "1").await.unwrap();
        storage.append_telemetry_event("B", "2").await.unwrap();
        storage.append_telemetry_event("C", "3").await.unwrap();

        assert_eq!(storage.delete_oldest_telemetry_events(2).await.unwrap(), 2);
        let events = storage.read_telemetry_events(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "C");
        assert_eq!(storage.delete_oldest_telemetry_events(0).await.unwrap(), 0);
    }

    async fn setup_with_backend() -> (MobileStorage, Arc<dyn orch8_storage::StorageBackend>) {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        (MobileStorage::new(sqlite.clone()), sqlite)
    }

    fn sequence(
        name: &str,
        version: i32,
        created_at: DateTime<Utc>,
    ) -> orch8_types::sequence::SequenceDefinition {
        orch8_types::sequence::SequenceDefinition {
            embed: None,
            sub_tenant: None,
            schema: None,
            schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
            id: SequenceId::new(),
            tenant_id: TenantId::new("mobile").unwrap(),
            namespace: Namespace::new("default"),
            name: name.to_string(),
            version,
            deprecated: false,
            status: orch8_types::sequence::SequenceStatus::default(),
            blocks: vec![],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            placement: None,
            created_at,
        }
    }

    async fn insert_instance(storage: &MobileStorage, id: &str, state: &str) {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO task_instances
             (id, sequence_id, tenant_id, namespace, state, context, created_at, updated_at)
             VALUES (?1, ?2, 'mobile', 'default', ?3, '{}', ?4, ?4)",
        )
        .bind(id)
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(state)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
    }

    fn mobile_tenant() -> TenantId {
        TenantId::new("mobile").unwrap()
    }

    #[tokio::test]
    async fn version_manifest_reports_the_highest_version_per_name_sorted_and_limited() {
        let (storage, backend) = setup_with_backend().await;
        for (name, version) in [("gamma", 1), ("beta", 1), ("alpha", 1), ("alpha", 3)] {
            backend
                .create_sequence(&sequence(name, version, Utc::now()))
                .await
                .unwrap();
        }
        let ns = Namespace::new("default");

        let all = storage
            .list_local_sequence_versions(&mobile_tenant(), &ns, 1000)
            .await
            .unwrap();
        let limited = storage
            .list_local_sequence_versions(&mobile_tenant(), &ns, 2)
            .await
            .unwrap();

        assert_eq!(
            all,
            vec![
                ("alpha".to_string(), 3),
                ("beta".to_string(), 1),
                ("gamma".to_string(), 1)
            ],
            "one row per name carrying MAX(version)"
        );
        assert_eq!(
            limited,
            vec![("alpha".to_string(), 3), ("beta".to_string(), 1)],
            "the limit counts names, not versions"
        );
    }

    #[tokio::test]
    async fn version_manifest_is_scoped_to_the_tenant() {
        let (storage, backend) = setup_with_backend().await;
        backend
            .create_sequence(&sequence("alpha", 1, Utc::now()))
            .await
            .unwrap();

        let other = storage
            .list_local_sequence_versions(
                &TenantId::new("other").unwrap(),
                &Namespace::new("default"),
                1000,
            )
            .await
            .unwrap();

        assert!(other.is_empty());
    }

    #[tokio::test]
    async fn excess_sequence_candidates_are_empty_at_exactly_the_retain_limit() {
        let (storage, backend) = setup_with_backend().await;
        for index in 0..3 {
            let created_at = Utc::now() - chrono::Duration::hours(index);
            backend
                .create_sequence(&sequence(&format!("seq-{index}"), 1, created_at))
                .await
                .unwrap();
        }

        let candidates = storage
            .list_excess_oldest_local_sequences(&mobile_tenant(), &Namespace::new("default"), 3)
            .await
            .unwrap();

        assert!(candidates.is_empty());
    }

    #[tokio::test]
    async fn dedup_prune_keeps_rows_of_active_instances() {
        let (storage, _backend) = setup_with_backend().await;
        let id = InstanceId::new().to_string();
        insert_instance(&storage, &id, "scheduled").await;
        storage.set_dedup("dk-active", &id).await.unwrap();

        assert_eq!(storage.prune_stale_dedup().await.unwrap(), 0);
        assert_eq!(
            storage.get_dedup_instance("dk-active").await.unwrap(),
            Some(id)
        );
    }

    #[tokio::test]
    async fn dedup_prune_removes_rows_of_failed_and_cancelled_instances() {
        let (storage, _backend) = setup_with_backend().await;
        let failed = InstanceId::new().to_string();
        let cancelled = InstanceId::new().to_string();
        insert_instance(&storage, &failed, "failed").await;
        insert_instance(&storage, &cancelled, "cancelled").await;
        storage.set_dedup("dk-failed", &failed).await.unwrap();
        storage.set_dedup("dk-cancelled", &cancelled).await.unwrap();

        assert_eq!(storage.prune_stale_dedup().await.unwrap(), 2);
        assert!(storage.list_all_dedup().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn execution_steps_for_an_empty_id_list_return_empty_without_an_in_clause() {
        let (storage, _backend) = setup_with_backend().await;

        let steps = storage.list_sync_execution_steps(&[]).await.unwrap();

        assert!(steps.is_empty());
    }

    #[tokio::test]
    async fn terminal_projection_rejects_a_malformed_instance_id_naming_the_column() {
        let (storage, _backend) = setup_with_backend().await;
        insert_instance(&storage, "not-a-uuid", "completed").await;

        let error = storage
            .list_terminal_instance_states()
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("task_instances.id"), "{error}");
        assert!(error.contains("'not-a-uuid'"), "{error}");
    }
}
