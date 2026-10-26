//! Bounded owner/session-bound metadata history, including requests without rows.
use super::super::workflows::read_owner;
use super::{CatalogRunState, CatalogScope, invalid, validate};
use crate::storage::MobileStorage;
use orch8_storage::sqlite::CATALOG_NATIVE_IDENTITY_SQL;
use orch8_types::{
    error::StorageError,
    ids::{InstanceId, SequenceId},
    instance::InstanceState,
};
use sqlx::{Row, sqlite::SqliteRow};

pub(crate) const MAX_HISTORY_PAGE: u32 = 128;

/// A cursor controls pagination, never authorization. It is not deserialized
/// from workflow JSON and cannot be reused for another catalog or session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CatalogRunCursor {
    account_id: String,
    catalog_id: String,
    session_epoch: i64,
    after: InstanceId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CatalogNativeState {
    NotMaterialized,
    Present(InstanceState),
    Recorded(InstanceState),
    /// Native data may have been purged, or its identity/evidence is inconsistent.
    /// This makes no execution-outcome claim and never permits rematerialization.
    Unavailable,
}

/// Metadata only: no input/context/callback references or execution authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CatalogRunSummary {
    pub(crate) id: InstanceId,
    pub(crate) account_id: String,
    pub(crate) catalog_id: String,
    pub(crate) owner_epoch: i64,
    pub(crate) sequence_id: SequenceId,
    pub(crate) workflow: String,
    pub(crate) version: i32,
    pub(crate) created_at: chrono::DateTime<chrono::Utc>,
    pub(crate) request_state: CatalogRunState,
    pub(crate) native_state: CatalogNativeState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CatalogRunPage {
    pub(crate) items: Vec<CatalogRunSummary>,
    pub(crate) next: Option<CatalogRunCursor>,
}

fn summary(row: &SqliteRow) -> Result<CatalogRunSummary, StorageError> {
    let workflow: String = row.try_get("workflow_name")?;
    validate(&workflow)?;
    let native: Option<String> = row.try_get("native_state")?;
    let receipt: Option<String> = row.try_get("receipt_id")?;
    let present: bool = row.try_get("native_row_exists")?;
    let mut native_state = match (receipt.is_some(), native) {
        (true, Some(state)) => CatalogNativeState::Present(
            state
                .parse()
                .map_err(|_| invalid("invalid scoped native state"))?,
        ),
        (false, None) if !present => CatalogNativeState::NotMaterialized,
        _ => CatalogNativeState::Unavailable,
    };
    let archived: Option<String> = row.try_get("archive_receipt_id")?;
    if archived.is_some() {
        let compact = super::decode(row)?;
        let archive = super::archive::decode_receipt(row, &compact)?;
        native_state = archive.outcome.map_or(
            CatalogNativeState::Unavailable,
            CatalogNativeState::Recorded,
        );
    }
    Ok(CatalogRunSummary {
        id: InstanceId::from_uuid(
            row.try_get::<String, _>("id")?
                .parse()
                .map_err(|_| invalid("invalid scoped run ID"))?,
        ),
        account_id: row.try_get("account_id")?,
        catalog_id: row.try_get("catalog_id")?,
        owner_epoch: row.try_get("owner_epoch")?,
        sequence_id: SequenceId::from_uuid(
            row.try_get::<String, _>("selected_sequence_id")?
                .parse()
                .map_err(|_| invalid("invalid scoped sequence ID"))?,
        ),
        workflow,
        version: row.try_get("workflow_version")?,
        created_at: chrono::DateTime::parse_from_rfc3339(&row.try_get::<String, _>("created_at")?)
            .map_err(|_| invalid("invalid scoped run timestamp"))?
            .with_timezone(&chrono::Utc),
        request_state: match (
            archived.is_some(),
            row.try_get::<String, _>("state")?.as_str(),
        ) {
            (false, "prepared") => CatalogRunState::Prepared,
            (false, "cancelled") => CatalogRunState::Cancelled,
            (true, "cancelled") => CatalogRunState::Archived,
            _ => return Err(invalid("invalid scoped run state")),
        },
        native_state,
    })
}

impl MobileStorage {
    /// Live keyset pagination in stable ID order, not a durable event stream or
    /// a snapshot spanning pages. The existing owner/id index bounds the scan.
    pub(crate) async fn catalog_run_history(
        &self,
        scope: &CatalogScope,
        cursor: Option<&CatalogRunCursor>,
        limit: u32,
    ) -> Result<CatalogRunPage, StorageError> {
        if limit == 0 || limit > MAX_HISTORY_PAGE {
            return Err(invalid("invalid catalog history page size"));
        }
        if cursor.is_some_and(|cursor| {
            cursor.account_id != scope.account_id()
                || cursor.catalog_id != scope.catalog_id()
                || cursor.session_epoch != scope.epoch()
        }) {
            return Err(invalid("catalog history cursor belongs to another scope"));
        }
        let mut transaction = self.pool().begin().await?;
        read_owner(&mut transaction, scope).await?;
        let after = cursor
            .map(|cursor| cursor.after.to_string())
            .unwrap_or_default();
        // CASE never returns another native owner's state. Neither request input
        // nor native context is selected, parsed or decrypted by this projection.
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT r.id,r.account_id,r.catalog_id,r.owner_epoch,r.selected_sequence_id,
             r.workflow_name,r.workflow_version,r.created_at,r.state,m.request_id AS receipt_id,
             r.archive_receipt_id,r.admitted_manifest_version,r.admitted_manifest_digest,
             r.request_digest,r.requested_version,'null' AS input,4 AS input_bytes,
             a.receipt_json,a.receipt_bytes,a.receipt_digest,m.request_fingerprint,m.definition_fingerprint,
             i.id IS NOT NULL AS native_row_exists,
             CASE WHEN m.request_id IS NOT NULL AND ({CATALOG_NATIVE_IDENTITY_SQL}) THEN i.state END AS native_state
             FROM mobile_catalog_run_requests r
             LEFT JOIN mobile_catalog_run_materializations m ON m.request_id=r.id
             LEFT JOIN task_instances i ON i.id=r.id
             LEFT JOIN mobile_catalog_run_archives a ON a.request_id=r.archive_receipt_id
             WHERE r.account_id=?1 AND r.catalog_id=?2 AND r.owner_epoch<=?3 AND r.id>?4
             ORDER BY r.id LIMIT ?5"
        )))
        .bind(scope.account_id()).bind(scope.catalog_id()).bind(scope.epoch()).bind(after)
        .bind(i64::from(limit)+1).fetch_all(&mut *transaction).await?;
        let limit = usize::try_from(limit).map_err(|_| invalid("invalid history page size"))?;
        let more = rows.len() > limit;
        let items = rows
            .iter()
            .take(limit)
            .map(summary)
            .collect::<Result<Vec<_>, _>>()?;
        let next = if more {
            items.last().map(|last| CatalogRunCursor {
                account_id: scope.account_id().to_owned(),
                catalog_id: scope.catalog_id().to_owned(),
                session_epoch: scope.epoch(),
                after: last.id,
            })
        } else {
            None
        };
        transaction.commit().await?;
        Ok(CatalogRunPage { items, next })
    }
}

#[cfg(test)]
mod tests;
