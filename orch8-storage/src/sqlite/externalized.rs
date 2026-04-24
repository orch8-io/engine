use std::collections::HashMap;

use chrono::Utc;
use sqlx::Row;

use orch8_types::error::StorageError;
use orch8_types::ids::*;

use super::helpers::ts;
use super::SqliteStorage;
use crate::compression::{compress, decompress, COMPRESSION_THRESHOLD_BYTES};

pub(super) async fn save(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    ref_key: &str,
    payload: &serde_json::Value,
) -> Result<(), StorageError> {
    let raw = serde_json::to_vec(payload).map_err(StorageError::Serialization)?;
    let raw_size = i64::try_from(raw.len()).unwrap_or(i64::MAX);

    if raw.len() >= COMPRESSION_THRESHOLD_BYTES {
        let compressed = compress(payload)?;
        sqlx::query(
            "INSERT OR REPLACE INTO externalized_state \
             (ref_key, instance_id, payload, payload_bytes, compression, size_bytes, created_at) \
             VALUES (?1, ?2, NULL, ?3, 'zstd', ?4, ?5)",
        )
        .bind(ref_key)
        .bind(instance_id.0.to_string())
        .bind(compressed)
        .bind(raw_size)
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await?;
    } else {
        sqlx::query(
            "INSERT OR REPLACE INTO externalized_state \
             (ref_key, instance_id, payload, payload_bytes, compression, size_bytes, created_at) \
             VALUES (?1, ?2, ?3, NULL, NULL, ?4, ?5)",
        )
        .bind(ref_key)
        .bind(instance_id.0.to_string())
        .bind(serde_json::to_string(payload).map_err(StorageError::Serialization)?)
        .bind(raw_size)
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await?;
    }
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    ref_key: &str,
) -> Result<Option<serde_json::Value>, StorageError> {
    let row = sqlx::query(
        "SELECT payload, payload_bytes, compression \
         FROM externalized_state WHERE ref_key = ?1",
    )
    .bind(ref_key)
    .fetch_optional(&storage.pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let compression: Option<String> = row.try_get("compression").unwrap_or(None);
    match compression.as_deref() {
        Some("zstd") => {
            let bytes: Vec<u8> = row.try_get("payload_bytes")?;
            Ok(Some(decompress(&bytes)?))
        }
        None => {
            let payload: Option<String> = row.try_get("payload").unwrap_or(None);
            match payload {
                Some(s) => Ok(Some(
                    serde_json::from_str(&s).map_err(StorageError::Serialization)?,
                )),
                None => Err(StorageError::Query(
                    "externalized_state row has compression=NULL and payload=NULL".into(),
                )),
            }
        }
        Some(other) => Err(StorageError::Query(format!(
            "unknown externalized_state compression codec: {other}"
        ))),
    }
}

/// Atomic batched save. Wraps all inserts in a single transaction so partial
/// failures never leave `task_instances.context` pointing at ref-keys that
/// don't exist. Per-row compression branching mirrors [`save`], but rows are
/// grouped by storage shape and inserted in two bulk statements.
pub(super) async fn batch_save(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    entries: &[(String, serde_json::Value)],
) -> Result<(), StorageError> {
    if entries.is_empty() {
        return Ok(());
    }

    let mut compressed: Vec<(String, Vec<u8>, i64)> = Vec::with_capacity(entries.len());
    let mut inline: Vec<(String, String, i64)> = Vec::with_capacity(entries.len());

    for (ref_key, payload) in entries {
        let raw = serde_json::to_vec(payload).map_err(StorageError::Serialization)?;
        let raw_size = i64::try_from(raw.len()).unwrap_or(i64::MAX);

        if raw.len() >= COMPRESSION_THRESHOLD_BYTES {
            let c = compress(payload)?;
            compressed.push((ref_key.clone(), c, raw_size));
        } else {
            let s = serde_json::to_string(payload).map_err(StorageError::Serialization)?;
            inline.push((ref_key.clone(), s, raw_size));
        }
    }

    let now = ts(Utc::now());
    let inst_id = instance_id.0.to_string();
    let mut tx = storage.pool.begin().await?;

    if !compressed.is_empty() {
        let mut qb = sqlx::QueryBuilder::new(
            "INSERT OR REPLACE INTO externalized_state \
             (ref_key, instance_id, payload, payload_bytes, compression, size_bytes, created_at) ",
        );
        qb.push_values(
            &compressed,
            |mut b, (ref_key, payload_bytes, size_bytes)| {
                b.push_bind(ref_key)
                    .push_bind(&inst_id)
                    .push_bind(None::<String>)
                    .push_bind(payload_bytes)
                    .push_bind("zstd")
                    .push_bind(size_bytes)
                    .push_bind(&now);
            },
        );
        qb.build().execute(&mut *tx).await?;
    }

    if !inline.is_empty() {
        let mut qb = sqlx::QueryBuilder::new(
            "INSERT OR REPLACE INTO externalized_state \
             (ref_key, instance_id, payload, payload_bytes, compression, size_bytes, created_at) ",
        );
        qb.push_values(&inline, |mut b, (ref_key, payload, size_bytes)| {
            b.push_bind(ref_key)
                .push_bind(&inst_id)
                .push_bind(payload)
                .push_bind(None::<Vec<u8>>)
                .push_bind(None::<String>)
                .push_bind(size_bytes)
                .push_bind(&now);
        });
        qb.build().execute(&mut *tx).await?;
    }

    tx.commit().await?;
    Ok(())
}

/// Batched variant of [`get`]. SQLite has no native array binding, so we build
/// an `IN (?,?,...)` clause dynamically. Missing keys are absent from the
/// returned map — callers treat that as "nothing to hydrate".
///
/// SQLite caps a single statement at `SQLITE_MAX_VARIABLE_NUMBER` bound params
/// (default 32766 in modern builds). The scheduler preload path only batches
/// the fields of *one* dispatched step, so in practice `ref_keys` is tiny —
/// no chunking needed here.
pub(super) async fn batch_get(
    storage: &SqliteStorage,
    ref_keys: &[String],
) -> Result<HashMap<String, serde_json::Value>, StorageError> {
    if ref_keys.is_empty() {
        return Ok(HashMap::new());
    }

    let mut qb = sqlx::QueryBuilder::new(
        "SELECT ref_key, payload, payload_bytes, compression \
         FROM externalized_state WHERE ref_key IN (",
    );
    let mut separated = qb.separated(",");
    for key in ref_keys {
        separated.push_bind(key);
    }
    separated.push_unseparated(")");

    let rows = qb.build().fetch_all(&storage.pool).await?;

    let mut out = HashMap::with_capacity(rows.len());
    for row in rows {
        let ref_key: String = row.try_get("ref_key")?;
        let compression: Option<String> = row.try_get("compression").unwrap_or(None);
        let value = match compression.as_deref() {
            Some("zstd") => {
                let bytes: Vec<u8> = row.try_get("payload_bytes")?;
                decompress(&bytes)?
            }
            None => {
                let payload: Option<String> = row.try_get("payload").unwrap_or(None);
                match payload {
                    Some(s) => serde_json::from_str(&s).map_err(StorageError::Serialization)?,
                    None => {
                        return Err(StorageError::Query(
                            "externalized_state row has compression=NULL and payload=NULL".into(),
                        ));
                    }
                }
            }
            Some(other) => {
                return Err(StorageError::Query(format!(
                    "unknown externalized_state compression codec: {other}"
                )));
            }
        };
        out.insert(ref_key, value);
    }
    Ok(out)
}

pub(super) async fn delete(storage: &SqliteStorage, ref_key: &str) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM externalized_state WHERE ref_key=?1")
        .bind(ref_key)
        .execute(&storage.pool)
        .await?;
    Ok(())
}

/// Delete up to `limit` rows whose `expires_at` has elapsed. `expires_at` is
/// stored as ISO-8601 text in SQLite. Both sides of the comparison are
/// normalized through `datetime(...)` so RFC 3339 strings (with a 'T'
/// separator and timezone offset, e.g. `2026-04-16T10:00:00+00:00`) are
/// compared as real timestamps rather than by lexicographic byte order,
/// which would otherwise disagree with `datetime('now')`'s space-separated
/// form and silently skip expired rows.
pub(super) async fn delete_expired(
    storage: &SqliteStorage,
    limit: u32,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        r"DELETE FROM externalized_state
          WHERE ref_key IN (
              SELECT ref_key FROM externalized_state
              WHERE expires_at IS NOT NULL AND datetime(expires_at) <= datetime('now')
              LIMIT ?1
          )",
    )
    .bind(i64::from(limit))
    .execute(&storage.pool)
    .await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use orch8_types::ids::InstanceId;
    use serde_json::json;

    use super::*;

    async fn mk_store() -> SqliteStorage {
        SqliteStorage::in_memory().await.unwrap()
    }

    /// Insert the minimum-viable parent row so the FK on
    /// externalized_state.instance_id is satisfied. We only need the NOT NULL
    /// columns without defaults; everything else uses column defaults.
    async fn seed_instance(store: &SqliteStorage, id: InstanceId) {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO task_instances (id, sequence_id, tenant_id, namespace, created_at, updated_at) \
             VALUES (?1, 'seq', 'tenant', 'default', ?2, ?2)",
        )
        .bind(id.0.to_string())
        .bind(&now)
        .execute(&store.pool)
        .await
        .unwrap();
    }

    async fn set_expires_at(store: &SqliteStorage, ref_key: &str, expires: Option<&str>) {
        sqlx::query("UPDATE externalized_state SET expires_at = ?1 WHERE ref_key = ?2")
            .bind(expires)
            .bind(ref_key)
            .execute(&store.pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delete_expired_removes_past_but_keeps_future_and_null() {
        let store = mk_store().await;
        let inst = InstanceId::new();
        seed_instance(&store, inst).await;

        let past = (Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        let future = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();

        for k in ["a:past", "b:future", "c:null"] {
            save(&store, inst, k, &json!({"v": k})).await.unwrap();
        }
        set_expires_at(&store, "a:past", Some(&past)).await;
        set_expires_at(&store, "b:future", Some(&future)).await;
        // c:null keeps expires_at = NULL (never expires)

        let deleted = delete_expired(&store, 100).await.unwrap();
        assert_eq!(deleted, 1, "only the past-expired row should be deleted");

        assert!(get(&store, "a:past").await.unwrap().is_none());
        assert!(get(&store, "b:future").await.unwrap().is_some());
        assert!(get(&store, "c:null").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn delete_expired_respects_limit() {
        let store = mk_store().await;
        let inst = InstanceId::new();
        seed_instance(&store, inst).await;
        let past = (Utc::now() - chrono::Duration::hours(1)).to_rfc3339();

        for i in 0..5 {
            let k = format!("k:{i}");
            save(&store, inst, &k, &json!({"i": i})).await.unwrap();
            set_expires_at(&store, &k, Some(&past)).await;
        }

        // Sweep in two bounded passes — confirms LIMIT is honored.
        let first = delete_expired(&store, 2).await.unwrap();
        assert_eq!(first, 2);
        let second = delete_expired(&store, 100).await.unwrap();
        assert_eq!(second, 3);
        let third = delete_expired(&store, 100).await.unwrap();
        assert_eq!(third, 0, "idempotent once backlog is drained");
    }

    #[tokio::test]
    async fn delete_expired_on_empty_store_is_zero() {
        let store = mk_store().await;
        let deleted = delete_expired(&store, 100).await.unwrap();
        assert_eq!(deleted, 0);
    }

    /// Zero limit must be a safe no-op — not a "delete everything" escape
    /// hatch. SQLite's `LIMIT 0` returns no rows, which is the correct
    /// semantic; this test pins that in case the binding ever gets refactored
    /// to pass limit through differently.
    #[tokio::test]
    async fn delete_expired_with_zero_limit_deletes_nothing() {
        let store = mk_store().await;
        let inst = InstanceId::new();
        seed_instance(&store, inst).await;
        let past = (Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        save(&store, inst, "k:0", &json!({"v": 0})).await.unwrap();
        set_expires_at(&store, "k:0", Some(&past)).await;

        let deleted = delete_expired(&store, 0).await.unwrap();
        assert_eq!(deleted, 0, "limit=0 must not delete anything");
        // Row is still present.
        assert!(get(&store, "k:0").await.unwrap().is_some());
    }

    /// Multiple instances with expired rows are all eligible for sweeping —
    /// the GC query is global, not instance-scoped. Guards against a
    /// refactor that accidentally adds an instance filter.
    #[tokio::test]
    async fn delete_expired_sweeps_across_instances() {
        let store = mk_store().await;
        let inst_a = InstanceId::new();
        let inst_b = InstanceId::new();
        seed_instance(&store, inst_a).await;
        seed_instance(&store, inst_b).await;
        let past = (Utc::now() - chrono::Duration::hours(1)).to_rfc3339();

        save(&store, inst_a, "a:1", &json!({"v": 1})).await.unwrap();
        save(&store, inst_b, "b:1", &json!({"v": 1})).await.unwrap();
        set_expires_at(&store, "a:1", Some(&past)).await;
        set_expires_at(&store, "b:1", Some(&past)).await;

        let deleted = delete_expired(&store, 100).await.unwrap();
        assert_eq!(deleted, 2, "sweep must span instances");
        assert!(get(&store, "a:1").await.unwrap().is_none());
        assert!(get(&store, "b:1").await.unwrap().is_none());
    }

    /// Core M4 guarantee: deleting a `task_instances` row cascades to its
    /// `externalized_state` children. Prior to the FK + cascade fix, deleted
    /// instances left orphan payload rows that the GC sweeper never touched
    /// (GC only deletes by `expires_at`, not by instance lineage).
    #[tokio::test]
    async fn fk_cascade_deletes_externalized_when_instance_deleted() {
        let store = mk_store().await;
        let inst = InstanceId::new();
        seed_instance(&store, inst).await;
        for k in ["a", "b", "c"] {
            save(&store, inst, &format!("inst:{k}"), &json!({"v": k}))
                .await
                .unwrap();
        }
        // Sanity: all three rows present before the delete.
        let before: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM externalized_state WHERE instance_id = ?1")
                .bind(inst.0.to_string())
                .fetch_one(&store.pool)
                .await
                .unwrap();
        assert_eq!(before, 3);

        sqlx::query("DELETE FROM task_instances WHERE id = ?1")
            .bind(inst.0.to_string())
            .execute(&store.pool)
            .await
            .unwrap();

        let after: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM externalized_state WHERE instance_id = ?1")
                .bind(inst.0.to_string())
                .fetch_one(&store.pool)
                .await
                .unwrap();
        assert_eq!(
            after, 0,
            "ON DELETE CASCADE must remove orphaned externalized_state rows"
        );
    }

    /// Without a parent `task_instances` row the FK must reject the insert.
    /// Regression guard: a test environment that forgets to set
    /// `foreign_keys(true)` on the SQLite connection would silently accept
    /// orphan payload rows — this test fails loudly if that regression slips in.
    #[tokio::test]
    async fn save_rejects_when_parent_instance_missing() {
        let store = mk_store().await;
        let orphan = InstanceId::new(); // intentionally NOT seeded
        let err = save(&store, orphan, "orphan:ref", &json!({"v": 1}))
            .await
            .expect_err("save must fail when parent instance row is absent");
        // Error message is backend-specific but must mention the FK.
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("foreign key") || msg.contains("constraint"),
            "expected FK violation, got: {err}"
        );
    }

    /// Atomicity contract: if any row in a batch violates the FK, the whole
    /// transaction rolls back — no partial writes leaving the caller's
    /// `context` pointing at half-persisted ref-keys.
    #[tokio::test]
    async fn batch_save_rolls_back_when_any_row_violates_fk() {
        let store = mk_store().await;
        let good = InstanceId::new();
        seed_instance(&store, good).await;
        let orphan = InstanceId::new(); // no parent seeded

        // First entry would succeed on its own (valid FK), second would fail.
        // `batch_save` binds every row to the same `instance_id`, so we stage
        // the failure by binding to the orphan id — this tests that
        // a mid-batch FK failure rolls back the row that would have
        // succeeded.
        let entries = vec![
            ("batch:ok".to_string(), json!({"v": 1})),
            ("batch:also_ok".to_string(), json!({"v": 2})),
        ];
        let result = batch_save(&store, orphan, &entries).await;
        assert!(result.is_err(), "batch must fail when FK is violated");

        // Neither row persisted — transaction rolled back.
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM externalized_state WHERE instance_id = ?1")
                .bind(orphan.0.to_string())
                .fetch_one(&store.pool)
                .await
                .unwrap();
        assert_eq!(count, 0, "failed batch must leave zero rows behind");
    }

    /// Deleted instance must not leave the GC sweeper orphan work: after
    /// cascade, the remaining sweeper count for the stale lineage is zero
    /// regardless of `expires_at`. Sanity-checks that cascade and GC are
    /// disjoint concerns (cascade by lineage; GC by TTL).
    #[tokio::test]
    async fn cascade_beats_gc_for_deleted_instance_payloads() {
        let store = mk_store().await;
        let inst = InstanceId::new();
        seed_instance(&store, inst).await;
        save(&store, inst, "will:cascade", &json!({"v": 1}))
            .await
            .unwrap();
        // Give the row a *future* expires_at — GC would NOT delete it.
        let future = (Utc::now() + chrono::Duration::days(30)).to_rfc3339();
        set_expires_at(&store, "will:cascade", Some(&future)).await;

        sqlx::query("DELETE FROM task_instances WHERE id = ?1")
            .bind(inst.0.to_string())
            .execute(&store.pool)
            .await
            .unwrap();

        // GC would report 0 (not expired) — but the row is already gone.
        let gc_swept = delete_expired(&store, 100).await.unwrap();
        assert_eq!(gc_swept, 0, "GC must not find the cascaded row");
        assert!(
            get(&store, "will:cascade").await.unwrap().is_none(),
            "cascade deleted the row before GC could observe it"
        );
    }
}
