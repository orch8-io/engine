// Trait impl methods are exempt from `type_complexity` (clippy skips lints
// on a signature the impl doesn't control), but these free functions --
// extracted from what used to be trait impl bodies -- are not, so the allow
// that used to be implicit must be explicit here.
#![allow(clippy::type_complexity)]

use orch8_types::error::StorageError;
use sqlx::AssertSqlSafe;

use super::SqliteStorage;

pub(super) async fn register_mobile_device(
    storage: &SqliteStorage,
    device: &crate::MobileDevice,
) -> Result<(), StorageError> {
    let result = sqlx::query(
        "INSERT INTO mobile_devices (device_id, tenant_id, push_token, platform, app_version, active, registered_at)
         VALUES (?, ?, ?, ?, ?, 1, datetime('now'))
         ON CONFLICT(device_id) DO UPDATE SET
           push_token = excluded.push_token,
           platform = excluded.platform,
           app_version = excluded.app_version,
           active = 1
         WHERE mobile_devices.tenant_id = excluded.tenant_id",
    )
    .bind(&device.device_id)
    .bind(&device.tenant_id)
    .bind(&device.push_token)
    .bind(&device.platform)
    .bind(&device.app_version)
    .execute(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
    // The upsert only updates a row owned by the same tenant; 0 rows means
    // the device id belongs to another tenant (never overwrite its token).
    if result.rows_affected() == 0 {
        return Err(StorageError::Conflict(format!(
            "device {} is registered to another tenant",
            device.device_id
        )));
    }
    Ok(())
}

pub(super) async fn get_mobile_device(
    storage: &SqliteStorage,
    device_id: &str,
) -> Result<Option<crate::MobileDevice>, StorageError> {
    let row: Option<(String, String, Option<String>, String, Option<String>, bool, Option<String>, String)> =
        sqlx::query_as(
            "SELECT device_id, tenant_id, push_token, platform, app_version, active, last_sync_at, registered_at
             FROM mobile_devices WHERE device_id = ?",
        )
        .bind(device_id)
        .fetch_optional(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(row.map(
        |(
            device_id,
            tenant_id,
            push_token,
            platform,
            app_version,
            active,
            last_sync_at,
            registered_at,
        )| {
            crate::MobileDevice {
                device_id,
                tenant_id,
                push_token,
                platform,
                app_version,
                active,
                last_sync_at,
                registered_at,
            }
        },
    ))
}

pub(super) async fn update_device_last_sync(
    storage: &SqliteStorage,
    device_id: &str,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE mobile_devices SET last_sync_at = datetime('now') WHERE device_id = ?")
        .bind(device_id)
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn list_mobile_devices(
    storage: &SqliteStorage,
    tenant_id: Option<&str>,
    limit: u32,
) -> Result<Vec<crate::MobileDevice>, StorageError> {
    let mut sql = String::from(
        "SELECT device_id, tenant_id, push_token, platform, app_version, active, last_sync_at, registered_at
         FROM mobile_devices",
    );
    if tenant_id.is_some() {
        sql.push_str(" WHERE tenant_id = ?");
    }
    sql.push_str(" ORDER BY registered_at DESC LIMIT ?");

    let mut query = sqlx::query_as::<
        _,
        (
            String,
            String,
            Option<String>,
            String,
            Option<String>,
            bool,
            Option<String>,
            String,
        ),
    >(AssertSqlSafe(sql.as_str()));
    if let Some(tid) = tenant_id {
        query = query.bind(tid);
    }
    query = query.bind(limit);

    let rows = query
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

    Ok(rows
        .into_iter()
        .map(
            |(
                device_id,
                tenant_id,
                push_token,
                platform,
                app_version,
                active,
                last_sync_at,
                registered_at,
            )| {
                crate::MobileDevice {
                    device_id,
                    tenant_id,
                    push_token,
                    platform,
                    app_version,
                    active,
                    last_sync_at,
                    registered_at,
                }
            },
        )
        .collect())
}

pub(super) async fn mark_stale_devices_inactive(
    storage: &SqliteStorage,
    stale_threshold_secs: i64,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        "UPDATE mobile_devices SET active = 0
         WHERE active = 1 AND last_sync_at < datetime('now', '-' || ? || ' seconds')",
    )
    .bind(stale_threshold_secs)
    .execute(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected())
}

const UPSERT_STATUS_SQL: &str = "INSERT INTO mobile_instance_status (device_id, instance_id, sequence_name, state, current_step, handler, context_summary, steps, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(device_id, instance_id) DO UPDATE SET
           sequence_name = excluded.sequence_name,
           state = excluded.state,
           current_step = excluded.current_step,
           handler = excluded.handler,
           context_summary = excluded.context_summary,
           steps = excluded.steps,
           updated_at = excluded.updated_at
         WHERE COALESCE(julianday(mobile_instance_status.updated_at), 0) <= COALESCE(julianday(excluded.updated_at), 0)";

/// The `updated_at` string to store: the device's own text when its time is
/// not in the future (kept verbatim), otherwise the server clock.
fn status_updated_at(
    status: &crate::MobileInstanceStatus,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(chrono::DateTime<chrono::Utc>, String), StorageError> {
    let at = crate::normalize_status_timestamp(&status.updated_at, now)?;
    let text = if at == now {
        now.to_rfc3339()
    } else {
        status.updated_at.clone()
    };
    Ok((at, text))
}

pub(super) async fn upsert_mobile_instance_status(
    storage: &SqliteStorage,
    status: &crate::MobileInstanceStatus,
) -> Result<(), StorageError> {
    let (_, updated_at) = status_updated_at(status, chrono::Utc::now())?;
    sqlx::query(UPSERT_STATUS_SQL)
        .bind(&status.device_id)
        .bind(&status.instance_id)
        .bind(&status.sequence_name)
        .bind(&status.state)
        .bind(&status.current_step)
        .bind(&status.handler)
        .bind(&status.context_summary)
        .bind(&status.steps)
        .bind(updated_at)
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn upsert_mobile_instance_status_batch(
    storage: &SqliteStorage,
    statuses: &[crate::MobileInstanceStatus],
) -> Result<(), StorageError> {
    if statuses.is_empty() {
        return Ok(());
    }
    let now = chrono::Utc::now();
    // Newest wins per (device, instance); order of first appearance is kept.
    let mut order: Vec<(&str, &str)> = Vec::new();
    let mut newest: std::collections::HashMap<
        (&str, &str),
        (
            chrono::DateTime<chrono::Utc>,
            String,
            &crate::MobileInstanceStatus,
        ),
    > = std::collections::HashMap::with_capacity(statuses.len());
    for status in statuses {
        let (at, text) = status_updated_at(status, now)?;
        let key = (status.device_id.as_str(), status.instance_id.as_str());
        match newest.get(&key) {
            Some((existing, _, _)) if *existing > at => {}
            Some(_) => {
                newest.insert(key, (at, text, status));
            }
            None => {
                order.push(key);
                newest.insert(key, (at, text, status));
            }
        }
    }
    let mut tx = storage
        .pool
        .begin()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    for key in order {
        let (_, text, status) = &newest[&key];
        sqlx::query(UPSERT_STATUS_SQL)
            .bind(&status.device_id)
            .bind(&status.instance_id)
            .bind(&status.sequence_name)
            .bind(&status.state)
            .bind(&status.current_step)
            .bind(&status.handler)
            .bind(&status.context_summary)
            .bind(&status.steps)
            .bind(text)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
    }
    tx.commit()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn insert_mobile_approvals_batch(
    storage: &SqliteStorage,
    approvals: &[crate::MobileApprovalRequest],
) -> Result<u64, StorageError> {
    if approvals.is_empty() {
        return Ok(0);
    }
    let mut tx = storage
        .pool
        .begin()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    let mut inserted = 0;
    for approval in approvals {
        let result = sqlx::query(
            "INSERT INTO mobile_approval_requests (id, device_id, tenant_id, instance_id, block_id, sequence_name, prompt, choices, store_as, timeout_secs, metadata, state, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', datetime('now'))
             ON CONFLICT(device_id, instance_id, block_id) DO NOTHING",
        )
        .bind(&approval.id)
        .bind(&approval.device_id)
        .bind(&approval.tenant_id)
        .bind(&approval.instance_id)
        .bind(&approval.block_id)
        .bind(&approval.sequence_name)
        .bind(&approval.prompt)
        .bind(&approval.choices)
        .bind(&approval.store_as)
        .bind(approval.timeout_secs)
        .bind(&approval.metadata)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        inserted += result.rows_affected();
    }
    tx.commit()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(inserted)
}

pub(super) async fn create_mobile_command_if_absent(
    storage: &SqliteStorage,
    command: &crate::MobileCommand,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "INSERT INTO mobile_commands (id, device_id, command_type, payload, created_at)
         VALUES (?, ?, ?, ?, datetime('now'))
         ON CONFLICT(id) DO NOTHING",
    )
    .bind(&command.id)
    .bind(&command.device_id)
    .bind(&command.command_type)
    .bind(&command.payload)
    .execute(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected() == 1)
}

pub(super) async fn list_mobile_instance_status(
    storage: &SqliteStorage,
    tenant_id: Option<&str>,
    device_id: Option<&str>,
    limit: u32,
) -> Result<Vec<crate::MobileInstanceStatus>, StorageError> {
    let mut sql = String::from(
        "SELECT s.device_id, s.instance_id, s.sequence_name, s.state, s.current_step, s.handler, s.context_summary, s.steps, s.updated_at
         FROM mobile_instance_status s",
    );
    let mut conditions = Vec::new();
    if tenant_id.is_some() {
        sql.push_str(" JOIN mobile_devices d ON d.device_id = s.device_id");
        conditions.push("d.tenant_id = ?");
    }
    if device_id.is_some() {
        conditions.push("s.device_id = ?");
    }
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY s.updated_at DESC LIMIT ?");

    let mut query = sqlx::query_as::<
        _,
        (
            String,
            String,
            Option<String>,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
        ),
    >(AssertSqlSafe(sql.as_str()));
    if let Some(tid) = tenant_id {
        query = query.bind(tid);
    }
    if let Some(did) = device_id {
        query = query.bind(did);
    }
    query = query.bind(limit);

    let rows = query
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

    Ok(rows
        .into_iter()
        .map(
            |(
                device_id,
                instance_id,
                sequence_name,
                state,
                current_step,
                handler,
                context_summary,
                steps,
                updated_at,
            )| {
                crate::MobileInstanceStatus {
                    device_id,
                    instance_id,
                    sequence_name,
                    state,
                    current_step,
                    handler,
                    context_summary,
                    steps,
                    updated_at,
                }
            },
        )
        .collect())
}

pub(super) async fn insert_mobile_approval(
    storage: &SqliteStorage,
    approval: &crate::MobileApprovalRequest,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "INSERT INTO mobile_approval_requests (id, device_id, tenant_id, instance_id, block_id, sequence_name, prompt, choices, store_as, timeout_secs, metadata, state, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', datetime('now'))
         ON CONFLICT(device_id, instance_id, block_id) DO NOTHING",
    )
    .bind(&approval.id)
    .bind(&approval.device_id)
    .bind(&approval.tenant_id)
    .bind(&approval.instance_id)
    .bind(&approval.block_id)
    .bind(&approval.sequence_name)
    .bind(&approval.prompt)
    .bind(&approval.choices)
    .bind(&approval.store_as)
    .bind(approval.timeout_secs)
    .bind(&approval.metadata)
    .execute(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected() > 0)
}

pub(super) async fn get_mobile_approval(
    storage: &SqliteStorage,
    id: &str,
) -> Result<Option<crate::MobileApprovalRequest>, StorageError> {
    let row: Option<(String, String, String, String, String, Option<String>, Option<String>, Option<String>, Option<String>, Option<i64>, Option<String>, String, Option<String>, String, Option<String>)> =
        sqlx::query_as(
            "SELECT id, device_id, tenant_id, instance_id, block_id, sequence_name, prompt, choices, store_as, timeout_secs, metadata, state, resolution, created_at, resolved_at
             FROM mobile_approval_requests WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(row.map(
        |(
            id,
            device_id,
            tenant_id,
            instance_id,
            block_id,
            sequence_name,
            prompt,
            choices,
            store_as,
            timeout_secs,
            metadata,
            state,
            resolution,
            created_at,
            resolved_at,
        )| {
            crate::MobileApprovalRequest {
                id,
                device_id,
                tenant_id,
                instance_id,
                block_id,
                sequence_name,
                prompt,
                choices,
                store_as,
                timeout_secs,
                metadata,
                state,
                resolution,
                created_at,
                resolved_at,
            }
        },
    ))
}

pub(super) async fn resolve_mobile_approval(
    storage: &SqliteStorage,
    id: &str,
    resolution: &str,
) -> Result<Option<crate::MobileApprovalRequest>, StorageError> {
    let result = sqlx::query(
        "UPDATE mobile_approval_requests SET state = 'resolved', resolution = ?, resolved_at = datetime('now')
         WHERE id = ? AND state = 'pending'",
    )
    .bind(resolution)
    .bind(id)
    .execute(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;

    if result.rows_affected() == 0 {
        return Ok(None);
    }
    get_mobile_approval(storage, id).await
}

pub(super) async fn list_mobile_approvals_page(
    storage: &SqliteStorage,
    tenant_id: Option<&str>,
    state: Option<&str>,
    limit: u32,
    offset: u64,
) -> Result<Vec<crate::MobileApprovalRequest>, StorageError> {
    let mut sql = String::from(
        "SELECT id, device_id, tenant_id, instance_id, block_id, sequence_name, prompt, choices, store_as, timeout_secs, metadata, state, resolution, created_at, resolved_at
         FROM mobile_approval_requests WHERE 1=1",
    );
    if tenant_id.is_some() {
        sql.push_str(" AND tenant_id = ?");
    }
    if state.is_some() {
        sql.push_str(" AND state = ?");
    }
    sql.push_str(" ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?");

    let mut query = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<i64>,
            Option<String>,
            String,
            Option<String>,
            String,
            Option<String>,
        ),
    >(AssertSqlSafe(sql.as_str()));
    if let Some(tid) = tenant_id {
        query = query.bind(tid);
    }
    if let Some(s) = state {
        query = query.bind(s);
    }
    let offset = i64::try_from(offset).map_err(|_| {
        StorageError::Query("Mobile approval offset exceeds supported range".into())
    })?;
    query = query.bind(limit).bind(offset);

    let rows = query
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

    Ok(rows
        .into_iter()
        .map(
            |(
                id,
                device_id,
                tenant_id,
                instance_id,
                block_id,
                sequence_name,
                prompt,
                choices,
                store_as,
                timeout_secs,
                metadata,
                state,
                resolution,
                created_at,
                resolved_at,
            )| {
                crate::MobileApprovalRequest {
                    id,
                    device_id,
                    tenant_id,
                    instance_id,
                    block_id,
                    sequence_name,
                    prompt,
                    choices,
                    store_as,
                    timeout_secs,
                    metadata,
                    state,
                    resolution,
                    created_at,
                    resolved_at,
                }
            },
        )
        .collect())
}

pub(super) async fn expire_mobile_approvals(storage: &SqliteStorage) -> Result<u64, StorageError> {
    let result = sqlx::query(
        "UPDATE mobile_approval_requests SET state = 'expired'
         WHERE state = 'pending' AND timeout_secs IS NOT NULL
           AND datetime(created_at, '+' || timeout_secs || ' seconds') < datetime('now')",
    )
    .execute(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected())
}

pub(super) async fn create_mobile_command(
    storage: &SqliteStorage,
    command: &crate::MobileCommand,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO mobile_commands (id, device_id, command_type, payload, created_at)
         VALUES (?, ?, ?, ?, datetime('now'))",
    )
    .bind(&command.id)
    .bind(&command.device_id)
    .bind(&command.command_type)
    .bind(&command.payload)
    .execute(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

/// Topic of the per-device wake created for server-to-device commands.
const MOBILE_COMMAND_WAKE_TOPIC: &str = "mobile_command";

pub(super) async fn create_mobile_command_with_wake(
    storage: &SqliteStorage,
    command: &crate::MobileCommand,
    tenant_id: &str,
    created_at: chrono::DateTime<chrono::Utc>,
) -> Result<uuid::Uuid, StorageError> {
    let wake_id = uuid::Uuid::new_v4();
    let mut transaction = storage.pool.begin().await?;
    sqlx::query(
        "INSERT INTO mobile_commands (id, device_id, command_type, payload, created_at) VALUES (?, ?, ?, ?, datetime('now'))",
    )
    .bind(&command.id)
    .bind(&command.device_id)
    .bind(&command.command_type)
    .bind(&command.payload)
    .execute(&mut *transaction)
    .await?;
    // One pending wake per device: collapse onto the newest (the sync it
    // triggers drains every pending command), the same scope
    // `enqueue_collapsible_wake` uses.
    let collapse_key = orch8_push::CollapsibleWake {
        tenant_id: tenant_id.to_owned(),
        device_id: command.device_id.clone(),
        execution_id: String::new(),
        topic: MOBILE_COMMAND_WAKE_TOPIC.to_owned(),
        command_id: String::new(),
        created_at,
    }
    .collapse_key();
    sqlx::query(
        "INSERT INTO push_wake_outbox (id,tenant_id,device_id,command_id,topic,collapse_key,created_at) VALUES (?,?,?,?,?,?,?)",
    )
    .bind(wake_id.to_string())
    .bind(tenant_id)
    .bind(&command.device_id)
    .bind(&command.id)
    .bind(MOBILE_COMMAND_WAKE_TOPIC)
    .bind(&collapse_key)
    .bind(super::helpers::ts(created_at))
    .execute(&mut *transaction)
    .await?;
    sqlx::query("UPDATE push_wake_outbox SET status='terminal',terminal_reason='superseded',superseded_by=? WHERE tenant_id=? AND device_id=? AND collapse_key=? AND status='pending' AND id<>?")
        .bind(&command.id)
        .bind(tenant_id)
        .bind(&command.device_id)
        .bind(&collapse_key)
        .bind(wake_id.to_string())
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(wake_id)
}

pub(super) async fn fetch_pending_commands(
    storage: &SqliteStorage,
    device_id: &str,
    limit: u32,
) -> Result<Vec<crate::MobileCommand>, StorageError> {
    let rows: Vec<(String, String, String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT id, device_id, command_type, payload, created_at, acked_at
         FROM mobile_commands
         WHERE device_id = ? AND acked_at IS NULL
         ORDER BY created_at ASC LIMIT ?",
    )
    .bind(device_id)
    .bind(limit)
    .fetch_all(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;

    Ok(rows
        .into_iter()
        .map(
            |(id, device_id, command_type, payload, created_at, acked_at)| crate::MobileCommand {
                id,
                device_id,
                command_type,
                payload,
                created_at,
                acked_at,
            },
        )
        .collect())
}

pub(super) async fn ack_mobile_commands(
    storage: &SqliteStorage,
    device_id: &str,
    command_ids: &[String],
) -> Result<u64, StorageError> {
    if command_ids.is_empty() {
        return Ok(0);
    }
    let mut qb: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
        "UPDATE mobile_commands SET acked_at = datetime('now') WHERE device_id = ",
    );
    qb.push_bind(device_id);
    qb.push(" AND id IN (");
    let mut separated = qb.separated(", ");
    for id in command_ids {
        separated.push_bind(id);
    }
    separated.push_unseparated(")");
    let result = qb
        .build()
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected())
}

pub(super) async fn cleanup_acked_commands(
    storage: &SqliteStorage,
    older_than_secs: i64,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        "DELETE FROM mobile_commands WHERE acked_at IS NOT NULL AND acked_at < datetime('now', '-' || ? || ' seconds')",
    )
    .bind(older_than_secs)
    .execute(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected())
}

pub(super) async fn cleanup_expired_commands(
    storage: &SqliteStorage,
    ttl_secs: i64,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        "DELETE FROM mobile_commands WHERE acked_at IS NULL AND created_at < datetime('now', '-' || ? || ' seconds')",
    )
    .bind(ttl_secs)
    .execute(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected())
}
