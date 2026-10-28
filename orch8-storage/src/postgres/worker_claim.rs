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
    conn: &mut sqlx::PgConnection,
    tasks: &[WorkerTask],
    principal: Option<&WorkerClaimPrincipal>,
) -> Result<(), StorageError> {
    // Bound statement parameters and avoid a read roundtrip per claimed task.
    for batch in tasks.chunks(128) {
        let tasks: std::collections::HashMap<_, _> =
            batch.iter().map(|task| (task.id, task)).collect();
        let ids: Vec<_> = batch.iter().map(|task| task.id).collect();
        let rows=sqlx::query("SELECT t.id,n.tenant_id,n.id AS native_id,v.tenant_id AS invocation_tenant,v.instance_id AS invocation_instance,v.record AS invocation,a.tenant_id AS admission_tenant,a.instance_id AS admission_instance,a.invocation_key,a.record AS admission,p.tenant_id AS publication_tenant,p.instance_id AS publication_instance,p.delegation_id AS publication_id,p.grant_id AS publication_grant,p.record AS publication FROM worker_tasks t JOIN task_instances n ON n.id=t.instance_id LEFT JOIN worker_task_invocations v ON v.task_id=t.id LEFT JOIN worker_task_admissions a ON a.task_id=t.id LEFT JOIN delegation_publications p ON p.task_id=t.id WHERE t.id=ANY($1)").bind(&ids).fetch_all(&mut *conn).await?;
        if rows.len() != batch.len() {
            return Err(invalid());
        }
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let task = *tasks.get(&row.get::<Uuid, _>("id")).ok_or_else(invalid)?;
            let tenant = TenantId::unchecked(row.get::<String, _>("tenant_id"));
            if InstanceId::from_uuid(row.get("native_id")) != task.instance_id
                || row
                    .get::<Option<String>, _>("invocation_tenant")
                    .is_some_and(|id| id != tenant.as_str())
                || row
                    .get::<Option<String>, _>("admission_tenant")
                    .is_some_and(|id| id != tenant.as_str())
                || row
                    .get::<Option<Uuid>, _>("invocation_instance")
                    .is_some_and(|id| id != task.instance_id.into_uuid())
                || row
                    .get::<Option<Uuid>, _>("admission_instance")
                    .is_some_and(|id| id != task.instance_id.into_uuid())
            {
                return Err(invalid());
            }
            let legacy: Option<crate::worker_invocation::WorkerInvocation> = row
                .get::<Option<serde_json::Value>, _>("invocation")
                .map(serde_json::from_value)
                .transpose()?;
            if let Some(source) = &legacy {
                source.validate_scope(&tenant, task.id, task.instance_id)?;
            }
            let admission: Option<crate::worker_admission::WorkerDispatchAdmission> = row
                .get::<Option<serde_json::Value>, _>("admission")
                .map(serde_json::from_value)
                .transpose()?;
            if admission.as_ref().is_some_and(|a| {
                Some(a.invocation_key.as_str())
                    != row.get::<Option<String>, _>("invocation_key").as_deref()
            }) {
                return Err(invalid());
            }
            let publication: Option<crate::delegation_publication::DelegationPublication> = row
                .get::<Option<serde_json::Value>, _>("publication")
                .map(serde_json::from_value)
                .transpose()?;
            let required=legacy.as_ref().is_some_and(|source|matches!(&source.target,crate::worker_invocation::WorkerInvocationTarget::Unbound{reason} if reason==crate::delegation_publication::INVOCATION_REASON));
            if required && publication.is_none() {
                return Err(invalid());
            }
            if let Some(publication) = publication
                && (row
                    .get::<Option<String>, _>("publication_tenant")
                    .as_deref()
                    != Some(tenant.as_str())
                    || row
                        .get::<Option<Uuid>, _>("publication_instance")
                        .map(|id| id.to_string())
                        != Some(publication.instance_id.to_string())
                    || row
                        .get::<Option<Uuid>, _>("publication_id")
                        .map(|id| id.to_string())
                        != Some(publication.delegation.id.to_string())
                    || row
                        .get::<Option<Uuid>, _>("publication_grant")
                        .map(|id| id.to_string())
                        != Some(publication.delegation.grant_id.to_string())
                    || publication.delegation.tenant_id != tenant
                    || !publication.matches_hot(task)?)
            {
                return Err(invalid());
            }
            let source = admission.as_ref().map(|a| a.invocation.clone()).or(legacy);
            let claim = WorkerIssuedClaim::new(task, tenant, principal, source, admission)?;
            let record = serde_json::to_value(&claim)?;
            let epoch = i64::try_from(claim.claim.claim_epoch).map_err(|_| invalid())?;
            records.push((claim, record, epoch));
        }
        let mut insert = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO worker_issued_claims(task_id,claim_epoch,tenant_id,instance_id,worker_id,record,issued_at) ",
        );
        insert.push_values(&records, |mut row, (claim, record, epoch)| {
            row.push_bind(claim.task_id)
                .push_bind(*epoch)
                .push_bind(claim.tenant_id.as_str())
                .push_bind(claim.instance_id.into_uuid())
                .push_bind(claim.claim.worker_id.as_str())
                .push_bind(record)
                .push_bind(claim.issued_at);
        });
        insert.build().execute(&mut *conn).await?;
    }
    Ok(())
}
pub(super) async fn get(
    store: &super::PostgresStorage,
    tenant: Option<&TenantId>,
    task_id: Uuid,
    claim: &WorkerClaim,
) -> Result<Option<WorkerIssuedClaim>, StorageError> {
    let Ok(epoch) = i64::try_from(claim.claim_epoch) else {
        return Ok(None);
    };
    let row=sqlx::query("SELECT tenant_id,instance_id,worker_id,record FROM worker_issued_claims WHERE task_id=$1 AND claim_epoch=$2 AND ($3::text IS NULL OR tenant_id=$3)")
        .bind(task_id).bind(epoch).bind(tenant.map(TenantId::as_str)).fetch_optional(&store.pool).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let value: WorkerIssuedClaim = serde_json::from_value(row.get("record"))?;
    value.validate()?;
    if value.task_id != task_id
        || value.claim.claim_epoch != claim.claim_epoch
        || value.claim.worker_id != row.get::<String, _>("worker_id")
        || value.tenant_id.as_str() != row.get::<String, _>("tenant_id")
        || row.get::<Uuid, _>("instance_id") != value.instance_id.into_uuid()
    {
        return Err(invalid());
    }
    Ok((value.claim == *claim).then_some(value))
}

pub(super) async fn reconnect(
    store: &super::PostgresStorage,
    request: &crate::worker_claim::WorkerReconnectRequest,
) -> Result<Vec<WorkerTask>, StorageError> {
    request.validate()?;
    if request.limit == 0 {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "SELECT wt.*,ti.tenant_id AS actual_tenant,c.record AS issued_record FROM worker_tasks wt JOIN task_instances ti ON ti.id=wt.instance_id LEFT JOIN worker_issued_claims c ON c.task_id=wt.id AND c.claim_epoch=wt.claim_epoch WHERE wt.state='claimed' AND wt.worker_id=",
    );
    query.push_bind(&request.worker_id);
    if let Some(tenant) = &request.tenant_id {
        query.push(" AND ti.tenant_id=").push_bind(tenant.as_str());
    }
    query.push(" AND wt.handler_name IN (");
    let mut handlers = query.separated(",");
    for handler in &request.handlers {
        handlers.push_bind(handler);
    }
    handlers.push_unseparated(")");
    query.push(" AND (c.task_id IS NULL OR (c.tenant_id=ti.tenant_id AND c.instance_id=wt.instance_id AND c.worker_id=wt.worker_id AND (");
    let (principal, optional_device) = request.lookup_principal()?;
    query.push("c.record->'principal'->>'kind'='legacy' OR ");
    if optional_device {
        query.push("((c.record->'principal'->'device_id' IS NULL OR c.record->'principal'->'device_id'='null'::jsonb) AND ((c.record->'principal')-'device_id')=")
            .push_bind(principal).push("::jsonb)");
    } else {
        query
            .push("c.record->'principal'=")
            .push_bind(principal)
            .push("::jsonb");
    }
    query
        .push("))) ORDER BY wt.created_at,wt.id LIMIT ")
        .push_bind(i64::from(request.limit));
    let rows = query.build().fetch_all(&store.pool).await?;
    let mut tasks = Vec::with_capacity(rows.len());
    for row in rows {
        let task =
            <super::rows::WorkerTaskRow as sqlx::FromRow<'_, sqlx::postgres::PgRow>>::from_row(
                &row,
            )?
            .into_task()?;
        let tenant = TenantId::unchecked(row.get::<String, _>("actual_tenant"));
        let issued: Option<WorkerIssuedClaim> = row
            .get::<Option<serde_json::Value>, _>("issued_record")
            .map(serde_json::from_value)
            .transpose()?;
        if let Some(issued) = issued {
            issued.validate_task_scope(&tenant, &task)?;
            if !matches!(issued.principal, WorkerClaimPrincipal::Legacy {})
                && issued.principal != request.principal
            {
                return Err(invalid());
            }
        }
        tasks.push(task);
    }
    Ok(tasks)
}
