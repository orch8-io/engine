//! Account/session authorization at the actual instance read/write boundary.
use super::{
    SqliteStorage,
    helpers::{begin_immediate, row_to_instance, ts},
};
use crate::MobileInstanceAccess;
use orch8_types::{
    error::StorageError,
    ids::InstanceId,
    instance::{InstanceState, TaskInstance},
};

/// Shared with the SDK's context-free projections. The trusted SQL alias is `i`.
pub const LEGACY_MOBILE_OWNER_SQL: &str = "i.tenant_id = 'mobile'
 AND NOT EXISTS (SELECT 1 FROM mobile_catalog_run_requests r WHERE r.id=i.id)
 AND NOT EXISTS (SELECT 1 FROM mobile_catalog_run_materializations m WHERE m.request_id=i.id)";

/// Native identity shared by scoped instance access and metadata projections.
/// Trusted aliases: `i` is task_instances, `r` is the catalog run request.
/// Callers separately prove the active session and materialization receipt.
pub const CATALOG_NATIVE_IDENTITY_SQL: &str = "i.id=r.id
 AND i.tenant_id='mobile-account-' || r.account_id
 AND i.namespace='catalog-' || r.catalog_id
 AND i.sequence_id=r.selected_sequence_id AND i.created_at=r.created_at
 AND i.sub_tenant IS NULL AND i.parent_instance_id IS NULL";

fn selection(
    access: &MobileInstanceAccess,
    id: InstanceId,
    columns: &'static str,
    current_execution: bool,
) -> sqlx::QueryBuilder<sqlx::Sqlite> {
    let mut query = sqlx::QueryBuilder::new(format!(
        "SELECT {columns} FROM task_instances i WHERE i.id="
    ));
    query.push_bind(id.to_string());
    match access {
        MobileInstanceAccess::Legacy => {
            query.push(" AND (").push(LEGACY_MOBILE_OWNER_SQL).push(")");
        }
        MobileInstanceAccess::Catalog {
            account_id,
            catalog_id,
            session_epoch,
        } => {
            query
                .push(
                    " AND EXISTS(SELECT 1 FROM mobile_catalog_run_requests r
                JOIN mobile_accounts a ON a.id=r.account_id
                JOIN mobile_catalogs c ON c.account_id=r.account_id AND c.id=r.catalog_id
                JOIN mobile_catalog_run_materializations m ON m.request_id=r.id
                WHERE r.id=i.id AND r.account_id=",
                )
                .push_bind(account_id)
                .push(" AND r.catalog_id=")
                .push_bind(catalog_id)
                .push(" AND a.active=1 AND a.epoch=")
                .push_bind(*session_epoch)
                .push(" AND ")
                .push(if current_execution {
                    "r.owner_epoch=a.epoch AND r.state='prepared' AND r.archive_receipt_id IS NULL"
                } else {
                    "r.owner_epoch<=a.epoch"
                })
                .push(" AND ")
                .push(CATALOG_NATIVE_IDENTITY_SQL)
                .push(")");
        }
    }
    query
}

pub(super) async fn get(
    storage: &SqliteStorage,
    access: &MobileInstanceAccess,
    id: InstanceId,
) -> Result<Option<TaskInstance>, StorageError> {
    let row = selection(access, id, "i.*", false)
        .build()
        .fetch_optional(&storage.pool)
        .await?;
    row.as_ref().map(row_to_instance).transpose()
}

pub(super) async fn cancel(
    storage: &SqliteStorage,
    access: &MobileInstanceAccess,
    id: InstanceId,
) -> Result<Option<InstanceState>, StorageError> {
    // Account retirement and all row changes serialize against this reservation.
    let mut transaction = begin_immediate(&storage.pool).await?;
    let state: Option<String> = selection(access, id, "i.state", false)
        .build_query_scalar()
        .fetch_optional(&mut *transaction)
        .await?;
    let Some(state) = state else { return Ok(None) };
    let state = state
        .parse::<InstanceState>()
        .map_err(StorageError::Query)?;
    if state.is_terminal() {
        transaction.commit().await?;
        return Ok(Some(state));
    }
    let current: Option<String> = selection(access, id, "i.state", true)
        .build_query_scalar()
        .fetch_optional(&mut *transaction)
        .await?;
    if current.is_none() {
        return Ok(None);
    }
    sqlx::query("UPDATE task_instances SET state='cancelled',claimed_at=NULL,next_fire_at=NULL,updated_at=?2 WHERE id=?1")
        .bind(id.to_string()).bind(ts(chrono::Utc::now())).execute(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(Some(InstanceState::Cancelled))
}

pub(super) async fn complete(
    storage: &SqliteStorage,
    access: &MobileInstanceAccess,
    expected_updated_at: chrono::DateTime<chrono::Utc>,
    context: &orch8_types::context::ExecutionContext,
    signal: &orch8_types::signal::Signal,
) -> Result<bool, StorageError> {
    let step = crate::mobile_access::completion_step(signal)?;
    let context_json = serde_json::to_string(context)?;
    let mut transaction = begin_immediate(&storage.pool).await?;
    let mut query = selection(access, signal.instance_id, "i.id", true);
    query
        .push(" AND i.state='waiting' AND i.updated_at=")
        .push_bind(ts(expected_updated_at))
        .push(" AND ((NOT EXISTS(SELECT 1 FROM execution_tree n WHERE n.instance_id=i.id) AND CASE WHEN json_valid(i.context) THEN json_extract(i.context,'$.runtime.current_step') END=")
        .push_bind(step)
        .push(") OR EXISTS(SELECT 1 FROM execution_tree n WHERE n.instance_id=i.id AND n.block_type='step' AND n.state='waiting' AND n.block_id=")
        .push_bind(step)
        .push("))");
    let target: Option<String> = query
        .build_query_scalar()
        .fetch_optional(&mut *transaction)
        .await?;
    if target.is_none() {
        return Ok(false);
    }
    let now = ts(chrono::Utc::now());
    sqlx::query("UPDATE task_instances SET context=?2,state='scheduled',claimed_at=NULL,next_fire_at=?3,updated_at=?3 WHERE id=?1")
        .bind(signal.instance_id.to_string()).bind(context_json).bind(now)
        .execute(&mut *transaction).await?;
    super::signals::bind_signal_insert(sqlx::query(super::signals::SIGNAL_INSERT_SQL), signal)?
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(true)
}
