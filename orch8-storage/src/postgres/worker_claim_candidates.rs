//! Revalidate the current locked candidates, not the unlocked discovery snapshot.
use super::{
    PostgresStorage,
    rows::{WorkerTaskRow, worker_task_columns},
};
use crate::worker_claim::ClaimRuntime;
use orch8_types::{error::StorageError, ids::TenantId, worker::WorkerTask};
use sqlx::{Postgres, QueryBuilder};
use uuid::Uuid;

pub(super) struct Selection<'a> {
    pub handler: &'a str,
    pub worker: &'a str,
    pub tenant: Option<&'a TenantId>,
    pub queue: Option<&'a str>,
    pub default_queue_only: bool,
    pub runtime: ClaimRuntime<'a>,
    pub limit: u32,
}

pub(super) async fn claim(
    store: &PostgresStorage,
    ids: &[Uuid],
    selection: Selection<'_>,
) -> Result<Vec<WorkerTask>, StorageError> {
    if selection.limit == 0 {
        return Ok(Vec::new());
    }
    let mut tx = store.pool.begin().await?;
    let result = async {
        let mut selected = Vec::new();
        // Keep each decoding page bounded. Skip stale/incompatible rows and
        // backfill from the over-collected candidates before applying LIMIT.
        let page_size = (selection.limit as usize).clamp(1, 32);
        for page in ids.chunks(page_size) {
            let mut query = QueryBuilder::<Postgres>::new(concat!(
                "SELECT ",
                worker_task_columns!(),
                " FROM worker_tasks WHERE id=ANY("
            ));
            query
                .push_bind(page)
                .push(") AND handler_name=")
                .push_bind(selection.handler);
            selectors(&mut query, &selection);
            guards(&mut query, selection.tenant);
            query.push(" ORDER BY created_at,id FOR UPDATE SKIP LOCKED");
            let rows = query
                .build_query_as::<WorkerTaskRow>()
                .fetch_all(&mut *tx)
                .await?;
            for row in rows {
                let task = row.into_task()?;
                if task.claimable_by(selection.runtime.capabilities, chrono::Utc::now()) {
                    selected.push(task.id);
                    if selected.len() >= selection.limit as usize {
                        break;
                    }
                }
            }
            if selected.len() >= selection.limit as usize {
                break;
            }
        }
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        // Selected task rows stay locked. Recheck parent/cut state and expiry
        // at the actual mutation, rather than renewing an expired observation.
        let mut update =
            QueryBuilder::<Postgres>::new("UPDATE worker_tasks SET state='claimed',worker_id=");
        update
            .push_bind(selection.worker)
            .push(",claimed_at=NOW(),heartbeat_at=NOW(),claim_epoch=claim_epoch+1,lease_secs=")
            .push_bind(
                selection
                    .runtime
                    .capabilities
                    .kind
                    .default_lease_secs()
                    .map(|s| s as i32),
            )
            .push(",claimed_runtime_kind=")
            .push_bind(selection.runtime.capabilities.kind.as_str())
            .push(" WHERE id=ANY(")
            .push_bind(&selected)
            .push(") AND handler_name=")
            .push_bind(selection.handler);
        selectors(&mut update, &selection);
        guards(&mut update, selection.tenant);
        update
            .push(" AND ")
            .push_bind(selection.runtime.capabilities.expires_at)
            .push(" > clock_timestamp() RETURNING ")
            .push(worker_task_columns!());
        let rows = update
            .build_query_as::<WorkerTaskRow>()
            .fetch_all(&mut *tx)
            .await?;
        let tasks = rows
            .into_iter()
            .map(WorkerTaskRow::into_task)
            .collect::<Result<Vec<_>, _>>()?;
        let events = tasks
            .iter()
            .map(|task| {
                super::workers::transition_event(
                    task.id,
                    task.claim_epoch,
                    task.worker_id.clone(),
                    orch8_types::worker::WorkerAttemptEventKind::Claimed,
                    None,
                )
            })
            .collect::<Vec<_>>();
        super::worker_claim::record_on(&mut tx, &tasks, selection.runtime.principal).await?;
        super::workers::insert_attempt_events(&mut tx, &events).await?;
        Ok(tasks)
    }
    .await;
    super::finish_transaction(tx, result).await
}
fn selectors(query: &mut QueryBuilder<Postgres>, selection: &Selection<'_>) {
    if let Some(queue) = selection.queue {
        query.push(" AND queue_name=").push_bind(queue);
    } else if selection.default_queue_only {
        query.push(" AND queue_name IS NULL");
    }
    if let Some(tenant) = selection.tenant {
        query.push(" AND tenant_id=").push_bind(tenant.as_str());
    }
}
fn guards(query: &mut QueryBuilder<Postgres>, tenant: Option<&TenantId>) {
    query.push(" AND state='pending' AND claim_epoch>=0 AND claim_epoch<9223372036854775807 AND NOT awaiting_dispatch AND EXISTS(SELECT 1 FROM task_instances owner WHERE owner.id=worker_tasks.instance_id AND owner.state NOT IN ('completed','failed','cancelled')");
    if let Some(tenant) = tenant {
        query
            .push(" AND owner.tenant_id=")
            .push_bind(tenant.as_str());
    }
    query.push(") AND NOT EXISTS(SELECT 1 FROM delegation_proxy_native_history h WHERE h.instance_id=worker_tasks.instance_id) AND NOT EXISTS(SELECT 1 FROM physical_instance_retirements r WHERE r.instance_id=worker_tasks.instance_id) AND NOT EXISTS(SELECT 1 FROM capsule_source_cuts c WHERE c.source_instance_id=worker_tasks.instance_id AND c.state IN ('captured','sealed')) AND NOT EXISTS(SELECT 1 FROM continuity_executions ce WHERE ce.state='compensating' AND ce.tenant_id=(SELECT t.tenant_id FROM task_instances t WHERE t.id=worker_tasks.instance_id) AND ce.record->>'current_instance_id'=worker_tasks.instance_id::text)");
}
