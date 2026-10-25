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
                    "r.owner_epoch=a.epoch"
                } else {
                    "r.owner_epoch<=a.epoch"
                })
                .push(
                    " AND i.tenant_id='mobile-account-' || r.account_id
                    AND i.namespace='catalog-' || r.catalog_id
                    AND i.sequence_id=r.selected_sequence_id AND i.created_at=r.created_at)",
                );
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
