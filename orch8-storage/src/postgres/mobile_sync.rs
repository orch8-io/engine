#![allow(clippy::cast_possible_wrap)]

use async_trait::async_trait;
use std::fmt::Write;

use super::PostgresStorage;
use crate::StorageError;

#[async_trait]
impl crate::MobileSyncStore for PostgresStorage {
    // --- Devices ---

    async fn register_mobile_device(
        &self,
        device: &crate::MobileDevice,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO mobile_devices (device_id, tenant_id, push_token, platform, app_version, active, registered_at)
             VALUES ($1, $2, $3, $4, $5, TRUE, now())
             ON CONFLICT(device_id) DO UPDATE SET
               push_token = EXCLUDED.push_token,
               platform = EXCLUDED.platform,
               app_version = EXCLUDED.app_version,
               active = TRUE",
        )
        .bind(&device.device_id)
        .bind(&device.tenant_id)
        .bind(&device.push_token)
        .bind(&device.platform)
        .bind(&device.app_version)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn get_mobile_device(
        &self,
        device_id: &str,
    ) -> Result<Option<crate::MobileDevice>, StorageError> {
        let row: Option<(
            String,
            String,
            Option<String>,
            String,
            Option<String>,
            bool,
            Option<String>,
            String,
        )> = sqlx::query_as(
            "SELECT device_id, tenant_id, push_token, platform, app_version, active,
                        to_char(last_sync_at, 'YYYY-MM-DD HH24:MI:SS') as last_sync_at,
                        to_char(registered_at, 'YYYY-MM-DD HH24:MI:SS') as registered_at
                 FROM mobile_devices WHERE device_id = $1",
        )
        .bind(device_id)
        .fetch_optional(&self.pool)
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

    async fn update_device_last_sync(&self, device_id: &str) -> Result<(), StorageError> {
        sqlx::query("UPDATE mobile_devices SET last_sync_at = now() WHERE device_id = $1")
            .bind(device_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn list_mobile_devices(
        &self,
        tenant_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<crate::MobileDevice>, StorageError> {
        let rows: Vec<(
            String,
            String,
            Option<String>,
            String,
            Option<String>,
            bool,
            Option<String>,
            String,
        )> = if let Some(tid) = tenant_id {
            sqlx::query_as(
                "SELECT device_id, tenant_id, push_token, platform, app_version, active,
                        to_char(last_sync_at, 'YYYY-MM-DD HH24:MI:SS'),
                        to_char(registered_at, 'YYYY-MM-DD HH24:MI:SS')
                 FROM mobile_devices WHERE tenant_id = $1
                 ORDER BY registered_at DESC LIMIT $2",
            )
            .bind(tid)
            .bind(limit as i32)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?
        } else {
            sqlx::query_as(
                "SELECT device_id, tenant_id, push_token, platform, app_version, active,
                        to_char(last_sync_at, 'YYYY-MM-DD HH24:MI:SS'),
                        to_char(registered_at, 'YYYY-MM-DD HH24:MI:SS')
                 FROM mobile_devices
                 ORDER BY registered_at DESC LIMIT $1",
            )
            .bind(limit as i32)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?
        };

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

    async fn mark_stale_devices_inactive(
        &self,
        stale_threshold_secs: i64,
    ) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "UPDATE mobile_devices SET active = FALSE
             WHERE active = TRUE AND last_sync_at < now() - make_interval(secs => $1::double precision)",
        )
        .bind(stale_threshold_secs)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }

    // --- Instance Status ---

    async fn upsert_mobile_instance_status(
        &self,
        status: &crate::MobileInstanceStatus,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO mobile_instance_status (device_id, instance_id, sequence_name, state, current_step, handler, context_summary, steps, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT(device_id, instance_id) DO UPDATE SET
               sequence_name = EXCLUDED.sequence_name,
               state = EXCLUDED.state,
               current_step = EXCLUDED.current_step,
               handler = EXCLUDED.handler,
               context_summary = EXCLUDED.context_summary,
               steps = EXCLUDED.steps,
               updated_at = EXCLUDED.updated_at",
        )
        .bind(&status.device_id)
        .bind(&status.instance_id)
        .bind(&status.sequence_name)
        .bind(&status.state)
        .bind(&status.current_step)
        .bind(&status.handler)
        .bind(&status.context_summary)
        .bind(&status.steps)
        .bind(&status.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn list_mobile_instance_status(
        &self,
        tenant_id: Option<&str>,
        device_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<crate::MobileInstanceStatus>, StorageError> {
        let mut sql = String::from(
            "SELECT s.device_id, s.instance_id, s.sequence_name, s.state, s.current_step, s.handler, s.context_summary, s.steps, to_char(s.updated_at, 'YYYY-MM-DD HH24:MI:SS') as updated_at
             FROM mobile_instance_status s",
        );
        let mut conditions = Vec::new();
        let mut param_idx: u32 = 1;

        if tenant_id.is_some() {
            sql.push_str(" JOIN mobile_devices d ON d.device_id = s.device_id");
            conditions.push(format!("d.tenant_id = ${param_idx}"));
            param_idx += 1;
        }
        if device_id.is_some() {
            conditions.push(format!("s.device_id = ${param_idx}"));
            param_idx += 1;
        }
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }
        let _ = write!(sql, " ORDER BY s.updated_at DESC LIMIT ${param_idx}");

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
        >(&sql);
        if let Some(tid) = tenant_id {
            query = query.bind(tid);
        }
        if let Some(did) = device_id {
            query = query.bind(did);
        }
        query = query.bind(limit as i32);

        let rows = query
            .fetch_all(&self.pool)
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

    // --- Approval Requests ---

    async fn insert_mobile_approval(
        &self,
        approval: &crate::MobileApprovalRequest,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "INSERT INTO mobile_approval_requests (id, device_id, tenant_id, instance_id, block_id, sequence_name, prompt, choices, store_as, timeout_secs, metadata, state, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'pending', now())
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
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected() > 0)
    }

    async fn get_mobile_approval(
        &self,
        id: &str,
    ) -> Result<Option<crate::MobileApprovalRequest>, StorageError> {
        let row: Option<(String, String, String, String, String, Option<String>, Option<String>, Option<String>, Option<String>, Option<i64>, Option<String>, String, Option<String>, String, Option<String>)> =
            sqlx::query_as(
                "SELECT id, device_id, tenant_id, instance_id, block_id, sequence_name, prompt, choices, store_as, timeout_secs, metadata, state, resolution,
                        to_char(created_at, 'YYYY-MM-DD HH24:MI:SS'),
                        to_char(resolved_at, 'YYYY-MM-DD HH24:MI:SS')
                 FROM mobile_approval_requests WHERE id = $1",
            )
            .bind(id)
            .fetch_optional(&self.pool)
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

    async fn resolve_mobile_approval(
        &self,
        id: &str,
        resolution: &str,
    ) -> Result<Option<crate::MobileApprovalRequest>, StorageError> {
        let result = sqlx::query(
            "UPDATE mobile_approval_requests SET state = 'resolved', resolution = $1, resolved_at = now()
             WHERE id = $2 AND state = 'pending'",
        )
        .bind(resolution)
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Ok(None);
        }
        self.get_mobile_approval(id).await
    }

    async fn list_mobile_approvals(
        &self,
        tenant_id: Option<&str>,
        state: Option<&str>,
        limit: u32,
    ) -> Result<Vec<crate::MobileApprovalRequest>, StorageError> {
        let mut sql = String::from(
            "SELECT id, device_id, tenant_id, instance_id, block_id, sequence_name, prompt, choices, store_as, timeout_secs, metadata, state, resolution,
                    to_char(created_at, 'YYYY-MM-DD HH24:MI:SS'),
                    to_char(resolved_at, 'YYYY-MM-DD HH24:MI:SS')
             FROM mobile_approval_requests WHERE TRUE",
        );
        let mut param_idx: u32 = 1;

        if tenant_id.is_some() {
            let _ = write!(sql, " AND tenant_id = ${param_idx}");
            param_idx += 1;
        }
        if state.is_some() {
            let _ = write!(sql, " AND state = ${param_idx}");
            param_idx += 1;
        }
        let _ = write!(sql, " ORDER BY created_at DESC LIMIT ${param_idx}");

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
        >(&sql);
        if let Some(tid) = tenant_id {
            query = query.bind(tid);
        }
        if let Some(s) = state {
            query = query.bind(s);
        }
        query = query.bind(limit as i32);

        let rows = query
            .fetch_all(&self.pool)
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

    async fn expire_mobile_approvals(&self) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "UPDATE mobile_approval_requests SET state = 'expired'
             WHERE state = 'pending' AND timeout_secs IS NOT NULL
               AND created_at + make_interval(secs => timeout_secs::double precision) < now()",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }

    // --- Commands ---

    async fn create_mobile_command(
        &self,
        command: &crate::MobileCommand,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO mobile_commands (id, device_id, command_type, payload, created_at)
             VALUES ($1, $2, $3, $4, now())",
        )
        .bind(&command.id)
        .bind(&command.device_id)
        .bind(&command.command_type)
        .bind(&command.payload)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn fetch_pending_commands(
        &self,
        device_id: &str,
        limit: u32,
    ) -> Result<Vec<crate::MobileCommand>, StorageError> {
        let rows: Vec<(String, String, String, String, String, Option<String>)> = sqlx::query_as(
            "SELECT id, device_id, command_type, payload,
                    to_char(created_at, 'YYYY-MM-DD HH24:MI:SS'),
                    to_char(acked_at, 'YYYY-MM-DD HH24:MI:SS')
             FROM mobile_commands
             WHERE device_id = $1 AND acked_at IS NULL
             ORDER BY created_at ASC LIMIT $2",
        )
        .bind(device_id)
        .bind(limit as i32)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(
                |(id, device_id, command_type, payload, created_at, acked_at)| {
                    crate::MobileCommand {
                        id,
                        device_id,
                        command_type,
                        payload,
                        created_at,
                        acked_at,
                    }
                },
            )
            .collect())
    }

    async fn ack_mobile_commands(
        &self,
        device_id: &str,
        command_ids: &[String],
    ) -> Result<u64, StorageError> {
        if command_ids.is_empty() {
            return Ok(0);
        }
        let mut qb = sqlx::QueryBuilder::new(
            "UPDATE mobile_commands SET acked_at = now() WHERE device_id = ",
        );
        qb.push_bind(device_id);
        qb.push(" AND id IN (");
        let mut separated = qb.separated(",");
        for id in command_ids {
            separated.push_bind(id);
        }
        separated.push_unseparated(")");

        let result = qb
            .build()
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }

    async fn cleanup_acked_commands(&self, older_than_secs: i64) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "DELETE FROM mobile_commands WHERE acked_at IS NOT NULL AND acked_at < now() - make_interval(secs => $1::double precision)",
        )
        .bind(older_than_secs)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }

    async fn cleanup_expired_commands(&self, ttl_secs: i64) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "DELETE FROM mobile_commands WHERE acked_at IS NULL AND created_at < now() - make_interval(secs => $1::double precision)",
        )
        .bind(ttl_secs)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }
}
