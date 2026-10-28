//! Claim transition and immutable claimant evidence share one transaction.
use crate::worker_claim::{WorkerClaimPrincipal, WorkerIssuedClaim, invalid};
use orch8_types::{
    error::StorageError,
    ids::{InstanceId, TenantId},
    worker::{WorkerClaim, WorkerTask},
};
use sqlx::Row;
use uuid::Uuid;

pub(super) async fn record_on(
    conn: &mut sqlx::SqliteConnection,
    tasks: &[WorkerTask],
    principal: Option<&WorkerClaimPrincipal>,
) -> Result<(), StorageError> {
    // Bound statement parameters and avoid a read roundtrip per claimed task.
    for batch in tasks.chunks(128) {
        let tasks: std::collections::HashMap<_, _> =
            batch.iter().map(|task| (task.id, task)).collect();
        let mut read = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT t.id,n.tenant_id,n.id AS native_id,v.tenant_id AS invocation_tenant,v.instance_id AS invocation_instance,v.record AS invocation,a.tenant_id AS admission_tenant,a.instance_id AS admission_instance,a.invocation_key,a.record AS admission FROM worker_tasks t JOIN task_instances n ON n.id=t.instance_id LEFT JOIN worker_task_invocations v ON v.task_id=t.id LEFT JOIN worker_task_admissions a ON a.task_id=t.id WHERE t.id IN (",
        );
        let mut ids = read.separated(",");
        for task in batch {
            ids.push_bind(task.id.to_string());
        }
        ids.push_unseparated(")");
        let rows = read.build().fetch_all(&mut *conn).await?;
        if rows.len() != batch.len() {
            return Err(invalid());
        }
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let task = *tasks
                .get(&Uuid::parse_str(&row.get::<String, _>("id")).map_err(|_| invalid())?)
                .ok_or_else(invalid)?;
            let tenant = TenantId::unchecked(row.get::<String, _>("tenant_id"));
            if InstanceId::from_uuid(
                Uuid::parse_str(&row.get::<String, _>("native_id")).map_err(|_| invalid())?,
            ) != task.instance_id
                || row
                    .get::<Option<String>, _>("invocation_tenant")
                    .is_some_and(|id| id != tenant.as_str())
                || row
                    .get::<Option<String>, _>("admission_tenant")
                    .is_some_and(|id| id != tenant.as_str())
                || row
                    .get::<Option<String>, _>("invocation_instance")
                    .is_some_and(|id| id != task.instance_id.to_string())
                || row
                    .get::<Option<String>, _>("admission_instance")
                    .is_some_and(|id| id != task.instance_id.to_string())
            {
                return Err(invalid());
            }
            let legacy: Option<crate::worker_invocation::WorkerInvocation> = row
                .get::<Option<String>, _>("invocation")
                .map(|value| serde_json::from_str(&value))
                .transpose()?;
            if let Some(source) = &legacy {
                source.validate_scope(&tenant, task.id, task.instance_id)?;
            }
            let admission: Option<crate::worker_admission::WorkerDispatchAdmission> = row
                .get::<Option<String>, _>("admission")
                .map(|value| serde_json::from_str(&value))
                .transpose()?;
            if admission.as_ref().is_some_and(|a| {
                Some(a.invocation_key.as_str())
                    != row.get::<Option<String>, _>("invocation_key").as_deref()
            }) {
                return Err(invalid());
            }
            let source = admission.as_ref().map(|a| a.invocation.clone()).or(legacy);
            let claim = WorkerIssuedClaim::new(task, tenant, principal, source, admission)?;
            let record = serde_json::to_string(&claim)?;
            let epoch = i64::try_from(claim.claim.claim_epoch).map_err(|_| invalid())?;
            records.push((claim, record, epoch));
        }
        let mut insert = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "INSERT INTO worker_issued_claims(task_id,claim_epoch,tenant_id,instance_id,worker_id,record,issued_at) ",
        );
        insert.push_values(&records, |mut row, (claim, record, epoch)| {
            row.push_bind(claim.task_id.to_string())
                .push_bind(*epoch)
                .push_bind(claim.tenant_id.as_str())
                .push_bind(claim.instance_id.to_string())
                .push_bind(claim.claim.worker_id.as_str())
                .push_bind(record.as_str())
                .push_bind(super::helpers::ts(claim.issued_at));
        });
        insert.build().execute(&mut *conn).await?;
    }
    Ok(())
}
pub(super) async fn get(
    store: &super::SqliteStorage,
    tenant: Option<&TenantId>,
    task_id: Uuid,
    claim: &WorkerClaim,
) -> Result<Option<WorkerIssuedClaim>, StorageError> {
    let Ok(epoch) = i64::try_from(claim.claim_epoch) else {
        return Ok(None);
    };
    let row=sqlx::query("SELECT tenant_id,instance_id,record FROM worker_issued_claims WHERE task_id=?1 AND claim_epoch=?2 AND (?3 IS NULL OR tenant_id=?3) AND worker_id=?4")
        .bind(task_id.to_string()).bind(epoch).bind(tenant.map(TenantId::as_str)).bind(&claim.worker_id).fetch_optional(&store.pool).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let value: WorkerIssuedClaim = serde_json::from_str(&row.get::<String, _>("record"))?;
    value.validate()?;
    if value.task_id != task_id
        || value.claim != *claim
        || value.tenant_id.as_str() != row.get::<String, _>("tenant_id")
        || row.get::<String, _>("instance_id") != value.instance_id.to_string()
    {
        return Err(invalid());
    }
    Ok(Some(value))
}
