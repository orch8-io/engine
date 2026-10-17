#![allow(clippy::cast_possible_wrap)]

use async_trait::async_trait;
use sqlx::AssertSqlSafe;
use std::fmt::Write;

use super::PostgresStorage;
use crate::StorageError;

/// Topic of the per-device wake created for server-to-device commands.
const MOBILE_COMMAND_WAKE_TOPIC: &str = "mobile_command";

/// Collapse scope of command wakes: one per `(tenant, device)`.
fn mobile_command_collapse_key(tenant_id: &str, device_id: &str) -> String {
    orch8_push::CollapsibleWake {
        tenant_id: tenant_id.to_owned(),
        device_id: device_id.to_owned(),
        execution_id: String::new(),
        topic: MOBILE_COMMAND_WAKE_TOPIC.to_owned(),
        command_id: String::new(),
        created_at: chrono::Utc::now(),
    }
    .collapse_key()
}

#[async_trait]
impl crate::MobileSyncStore for PostgresStorage {
    // --- Devices ---

    async fn register_mobile_device(
        &self,
        device: &crate::MobileDevice,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "INSERT INTO mobile_devices (device_id, tenant_id, push_token, platform, app_version, active, registered_at)
             VALUES ($1, $2, $3, $4, $5, TRUE, now())
             ON CONFLICT(device_id) DO UPDATE SET
               push_token = EXCLUDED.push_token,
               platform = EXCLUDED.platform,
               app_version = EXCLUDED.app_version,
               active = TRUE
             WHERE mobile_devices.tenant_id = EXCLUDED.tenant_id",
        )
        .bind(&device.device_id)
        .bind(&device.tenant_id)
        .bind(&device.push_token)
        .bind(&device.platform)
        .bind(&device.app_version)
        .execute(&self.pool)
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
                        to_char(last_sync_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') as last_sync_at,
                        to_char(registered_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') as registered_at
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
                        to_char(last_sync_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),
                        to_char(registered_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')
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
                        to_char(last_sync_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),
                        to_char(registered_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')
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
        // Parsed here (not by a SQL `::timestamptz` cast) so a malformed
        // timestamp is a typed constraint error, not an opaque query failure
        // the device would retry forever.
        let updated_at = crate::normalize_status_timestamp(&status.updated_at, chrono::Utc::now())?;
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
               updated_at = EXCLUDED.updated_at
             WHERE mobile_instance_status.updated_at <= EXCLUDED.updated_at",
        )
        .bind(&status.device_id)
        .bind(&status.instance_id)
        .bind(&status.sequence_name)
        .bind(&status.state)
        .bind(&status.current_step)
        .bind(&status.handler)
        .bind(&status.context_summary)
        .bind(&status.steps)
        .bind(updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn upsert_mobile_instance_status_batch(
        &self,
        statuses: &[crate::MobileInstanceStatus],
    ) -> Result<(), StorageError> {
        if statuses.is_empty() {
            return Ok(());
        }
        let now = chrono::Utc::now();
        // Collapse duplicate keys to the newest update first: one `ON CONFLICT
        // DO UPDATE` statement may not touch the same row twice.
        let mut newest: std::collections::HashMap<
            (&str, &str),
            (chrono::DateTime<chrono::Utc>, &crate::MobileInstanceStatus),
        > = std::collections::HashMap::with_capacity(statuses.len());
        for status in statuses {
            let at = crate::normalize_status_timestamp(&status.updated_at, now)?;
            let key = (status.device_id.as_str(), status.instance_id.as_str());
            match newest.get(&key) {
                Some((existing, _)) if *existing > at => {}
                _ => {
                    newest.insert(key, (at, status));
                }
            }
        }
        let mut device_ids = Vec::with_capacity(newest.len());
        let mut instance_ids = Vec::with_capacity(newest.len());
        let mut sequence_names = Vec::with_capacity(newest.len());
        let mut states = Vec::with_capacity(newest.len());
        let mut current_steps = Vec::with_capacity(newest.len());
        let mut handlers = Vec::with_capacity(newest.len());
        let mut summaries = Vec::with_capacity(newest.len());
        let mut steps = Vec::with_capacity(newest.len());
        let mut updated = Vec::with_capacity(newest.len());
        for (at, status) in newest.into_values() {
            device_ids.push(status.device_id.clone());
            instance_ids.push(status.instance_id.clone());
            sequence_names.push(status.sequence_name.clone());
            states.push(status.state.clone());
            current_steps.push(status.current_step.clone());
            handlers.push(status.handler.clone());
            summaries.push(status.context_summary.clone());
            steps.push(status.steps.clone());
            updated.push(at);
        }
        sqlx::query(
            "INSERT INTO mobile_instance_status (device_id, instance_id, sequence_name, state, current_step, handler, context_summary, steps, updated_at)
             SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[], $9::timestamptz[])
             ON CONFLICT(device_id, instance_id) DO UPDATE SET
               sequence_name = EXCLUDED.sequence_name,
               state = EXCLUDED.state,
               current_step = EXCLUDED.current_step,
               handler = EXCLUDED.handler,
               context_summary = EXCLUDED.context_summary,
               steps = EXCLUDED.steps,
               updated_at = EXCLUDED.updated_at
             WHERE mobile_instance_status.updated_at <= EXCLUDED.updated_at",
        )
        .bind(&device_ids)
        .bind(&instance_ids)
        .bind(&sequence_names)
        .bind(&states)
        .bind(&current_steps)
        .bind(&handlers)
        .bind(&summaries)
        .bind(&steps)
        .bind(&updated)
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
            "SELECT s.device_id, s.instance_id, s.sequence_name, s.state, s.current_step, s.handler, s.context_summary, s.steps, to_char(s.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') as updated_at
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
        >(AssertSqlSafe(sql.as_str()));
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

    async fn insert_mobile_approvals_batch(
        &self,
        approvals: &[crate::MobileApprovalRequest],
    ) -> Result<u64, StorageError> {
        if approvals.is_empty() {
            return Ok(0);
        }
        // One statement; duplicate (device, instance, block) keys inside the
        // batch collapse to the first, matching sequential DO NOTHING.
        let mut seen = std::collections::HashSet::with_capacity(approvals.len());
        let unique: Vec<&crate::MobileApprovalRequest> = approvals
            .iter()
            .filter(|a| seen.insert((a.device_id.as_str(), a.instance_id.as_str(), a.block_id.as_str())))
            .collect();
        let ids: Vec<&str> = unique.iter().map(|a| a.id.as_str()).collect();
        let device_ids: Vec<&str> = unique.iter().map(|a| a.device_id.as_str()).collect();
        let tenant_ids: Vec<&str> = unique.iter().map(|a| a.tenant_id.as_str()).collect();
        let instance_ids: Vec<&str> = unique.iter().map(|a| a.instance_id.as_str()).collect();
        let block_ids: Vec<&str> = unique.iter().map(|a| a.block_id.as_str()).collect();
        let sequence_names: Vec<Option<&str>> =
            unique.iter().map(|a| a.sequence_name.as_deref()).collect();
        let prompts: Vec<Option<&str>> = unique.iter().map(|a| a.prompt.as_deref()).collect();
        let choices: Vec<Option<&str>> = unique.iter().map(|a| a.choices.as_deref()).collect();
        let store_as: Vec<Option<&str>> = unique.iter().map(|a| a.store_as.as_deref()).collect();
        let timeouts: Vec<Option<i32>> = unique
            .iter()
            .map(|a| a.timeout_secs.and_then(|t| i32::try_from(t).ok()))
            .collect();
        let metadata: Vec<Option<&str>> = unique.iter().map(|a| a.metadata.as_deref()).collect();
        let result = sqlx::query(
            "INSERT INTO mobile_approval_requests (id, device_id, tenant_id, instance_id, block_id, sequence_name, prompt, choices, store_as, timeout_secs, metadata, state, created_at)
             SELECT u.*, 'pending', now() FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[], $9::text[], $10::int[], $11::text[]) AS u
             ON CONFLICT(device_id, instance_id, block_id) DO NOTHING",
        )
        .bind(&ids)
        .bind(&device_ids)
        .bind(&tenant_ids)
        .bind(&instance_ids)
        .bind(&block_ids)
        .bind(&sequence_names)
        .bind(&prompts)
        .bind(&choices)
        .bind(&store_as)
        .bind(&timeouts)
        .bind(&metadata)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }

    async fn get_mobile_approval(
        &self,
        id: &str,
    ) -> Result<Option<crate::MobileApprovalRequest>, StorageError> {
        let row: Option<(String, String, String, String, String, Option<String>, Option<String>, Option<String>, Option<String>, Option<i64>, Option<String>, String, Option<String>, String, Option<String>)> =
            sqlx::query_as(
                "SELECT id, device_id, tenant_id, instance_id, block_id, sequence_name, prompt, choices, store_as, timeout_secs::BIGINT AS timeout_secs, metadata, state, resolution,
                        to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),
                        to_char(resolved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')
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

    async fn list_mobile_approvals_page(
        &self,
        tenant_id: Option<&str>,
        state: Option<&str>,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<crate::MobileApprovalRequest>, StorageError> {
        let mut sql = String::from(
            "SELECT id, device_id, tenant_id, instance_id, block_id, sequence_name, prompt, choices, store_as, timeout_secs::BIGINT AS timeout_secs, metadata, state, resolution,
                    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),
                    to_char(resolved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')
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
        let offset_idx = param_idx + 1;
        let _ = write!(
            sql,
            " ORDER BY created_at DESC, id DESC LIMIT ${param_idx} OFFSET ${offset_idx}"
        );

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
        query = query.bind(i64::from(limit)).bind(offset);

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

    async fn create_mobile_command_if_absent(
        &self,
        command: &crate::MobileCommand,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "INSERT INTO mobile_commands (id, device_id, command_type, payload, created_at)
             VALUES ($1, $2, $3, $4, now())
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(&command.id)
        .bind(&command.device_id)
        .bind(&command.command_type)
        .bind(&command.payload)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected() == 1)
    }

    async fn create_mobile_command_with_wake(
        &self,
        command: &crate::MobileCommand,
        tenant_id: &str,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<uuid::Uuid, StorageError> {
        let wake_id = uuid::Uuid::new_v4();
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO mobile_commands (id, device_id, command_type, payload, created_at) VALUES ($1, $2, $3, $4, now())",
        )
        .bind(&command.id)
        .bind(&command.device_id)
        .bind(&command.command_type)
        .bind(&command.payload)
        .execute(&mut *transaction)
        .await?;
        // One pending wake per device: a burst of commands collapses onto the
        // newest wake (the sync it triggers drains every pending command), the
        // same scope `enqueue_collapsible_wake` uses. The advisory lock
        // serializes competing writers for the device across the fleet.
        let collapse_key = mobile_command_collapse_key(tenant_id, &command.device_id);
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(&collapse_key)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "INSERT INTO push_wake_outbox (id,tenant_id,device_id,command_id,topic,collapse_key,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7)",
        )
        .bind(wake_id)
        .bind(tenant_id)
        .bind(&command.device_id)
        .bind(&command.id)
        .bind(MOBILE_COMMAND_WAKE_TOPIC)
        .bind(&collapse_key)
        .bind(created_at)
        .execute(&mut *transaction)
        .await?;
        sqlx::query("UPDATE push_wake_outbox SET status='terminal',terminal_reason='superseded',superseded_by=$1 WHERE tenant_id=$2 AND device_id=$3 AND collapse_key=$4 AND status='pending' AND id<>$5")
            .bind(&command.id)
            .bind(tenant_id)
            .bind(&command.device_id)
            .bind(&collapse_key)
            .bind(wake_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(wake_id)
    }

    async fn fetch_pending_commands(
        &self,
        device_id: &str,
        limit: u32,
    ) -> Result<Vec<crate::MobileCommand>, StorageError> {
        let rows: Vec<(String, String, String, String, String, Option<String>)> = sqlx::query_as(
            "SELECT id, device_id, command_type, payload,
                    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),
                    to_char(acked_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')
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
        let mut separated = qb.separated(", ");
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
        // Bounded statements (L4-15).
        let mut total = 0;
        loop {
            let deleted = sqlx::query(
                "DELETE FROM mobile_commands WHERE ctid IN (SELECT ctid FROM mobile_commands \
                 WHERE acked_at IS NOT NULL AND acked_at < now() - make_interval(secs => $1::double precision) LIMIT $2)",
            )
            .bind(older_than_secs)
            .bind(super::BULK_STATEMENT_BATCH)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?
            .rows_affected();
            total += deleted;
            if deleted < super::BULK_STATEMENT_BATCH.unsigned_abs() {
                return Ok(total);
            }
        }
    }

    async fn cleanup_expired_commands(&self, ttl_secs: i64) -> Result<u64, StorageError> {
        // Bounded statements (L4-15).
        let mut total = 0;
        loop {
            let deleted = sqlx::query(
                "DELETE FROM mobile_commands WHERE ctid IN (SELECT ctid FROM mobile_commands \
                 WHERE acked_at IS NULL AND created_at < now() - make_interval(secs => $1::double precision) LIMIT $2)",
            )
            .bind(ttl_secs)
            .bind(super::BULK_STATEMENT_BATCH)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?
            .rows_affected();
            total += deleted;
            if deleted < super::BULK_STATEMENT_BATCH.unsigned_abs() {
                return Ok(total);
            }
        }
    }
}
