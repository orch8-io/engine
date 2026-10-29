//! Read-side recovery must not restart permanently stopped forward work.
use super::*;
use orch8_storage::{
    testing::backends::{RawPool, TestBackend},
    worker_claim::WorkerReconnectRequest,
};
use orch8_types::continuity::{ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState};

async fn held(b: &TestBackend) -> WorkerTask {
    let pending = fixture(b.storage.as_ref(), false).await;
    let request = WorkerClaimRequest::new(
        Principal::Root {},
        "noop".into(),
        "reconnect-worker".into(),
        Some(TenantId::unchecked("issued-claims")),
        Some("queue".into()),
        None,
        1,
    )
    .unwrap();
    let held = b
        .storage
        .claim_worker_tasks_with_authority(&request)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(held.id, pending.id);
    held
}
fn reconnect() -> WorkerReconnectRequest {
    WorkerReconnectRequest::new(
        Principal::Root {},
        Some(TenantId::unchecked("issued-claims")),
        "reconnect-worker".into(),
        vec!["noop".into()],
        1,
    )
    .unwrap()
}
async fn hidden(b: &TestBackend, blocked: &WorkerTask) {
    let healthy = held(b).await;
    let before = b
        .storage
        .get_worker_task(blocked.id)
        .await
        .unwrap()
        .unwrap();
    let request = reconnect();
    let old = b
        .storage
        .held_worker_tasks_for_reporter(&request)
        .await
        .unwrap();
    assert_eq!(
        old.iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![healthy.id],
        "blocked work consumed the reconnect window"
    );
    let after = b
        .storage
        .get_worker_task(blocked.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(after).unwrap(),
        serde_json::to_value(before).unwrap(),
        "screening must preserve the old outcome/recovery evidence"
    );
    assert!(
        b.storage
            .get_worker_issued_claim(None, blocked.id, &proof(blocked))
            .await
            .unwrap()
            .is_some()
    );
    b.storage.delete_worker_task(healthy.id).await.unwrap();
}

orch8_storage::backend_test!(async fn reconnect_skips_terminal_parents_before_limit(b) {
    for terminal in [InstanceState::Completed,InstanceState::Failed,InstanceState::Cancelled] {
        let blocked = held(&b).await;
        b.storage.update_instance_state(blocked.instance_id,terminal,None).await.unwrap();
        hidden(&b,&blocked).await;
        b.storage.delete_worker_task(blocked.id).await.unwrap();
    }
});
orch8_storage::backend_test!(async fn reconnect_skips_quarantined_native_history_before_limit(b) {
    let blocked = held(&b).await;
    match &b.raw {
        RawPool::Sqlite(pool) => {sqlx::query("INSERT INTO delegation_proxy_quarantines(instance_id,tenant_id) VALUES(?1,?2)").bind(blocked.instance_id.to_string()).bind("issued-claims").execute(pool).await.unwrap();}
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool) => {sqlx::query("INSERT INTO delegation_proxy_quarantines(instance_id,tenant_id) VALUES($1,$2)").bind(blocked.instance_id.into_uuid()).bind("issued-claims").execute(pool).await.unwrap();}
    }
    hidden(&b,&blocked).await;
});
orch8_storage::backend_test!(async fn reconnect_skips_retired_identity_without_cut_history_before_limit(b) {
    let blocked = held(&b).await;
    let continuity = uuid::Uuid::new_v4(); let capsule = uuid::Uuid::new_v4(); let now=Utc::now();
    match &b.raw {
        RawPool::Sqlite(pool) => {sqlx::query("INSERT INTO physical_instance_retirements VALUES(?1,?2,?3,?4,?5)").bind(blocked.instance_id.to_string()).bind("issued-claims").bind(continuity.to_string()).bind(capsule.to_string()).bind(now.to_rfc3339()).execute(pool).await.unwrap();}
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool) => {sqlx::query("INSERT INTO physical_instance_retirements VALUES($1,$2,$3,$4,$5)").bind(blocked.instance_id.into_uuid()).bind("issued-claims").bind(continuity).bind(capsule).bind(now).execute(pool).await.unwrap();}
    }
    assert!(b.storage.is_physical_instance_retired(blocked.instance_id).await.unwrap());
    hidden(&b,&blocked).await;
});
orch8_storage::backend_test!(async fn reconnect_skips_compensating_forward_work_before_limit(b) {
    let blocked = held(&b).await;
    let owner = ContinuityExecution {continuity_id:ContinuityId::new(), tenant_id:TenantId::unchecked("issued-claims"), current_instance_id:blocked.instance_id, owner_runtime_id:RuntimeId::new(), epoch:ExecutionEpoch::initial(), state:OwnershipState::Compensating, updated_at:Utc::now()};
    b.storage.create_continuity_execution(&owner).await.unwrap();
    hidden(&b,&blocked).await;
});
async fn age(b: &TestBackend, task: &WorkerTask, case: u8) {
    let old = Utc::now() - chrono::Duration::minutes(5);
    let missing = case == 1;
    let lease = if case == 2 { None } else { Some(1i32) };
    let timeout = if case == 2 { Some(1i64) } else { None };
    match &b.raw {
        RawPool::Sqlite(pool) => {
            sqlx::query("UPDATE worker_tasks SET claimed_at=?1,heartbeat_at=?1,lease_secs=?2,timeout_ms=?3,created_at=?4 WHERE id=?5").bind((!missing).then(||old.to_rfc3339())).bind(lease).bind(timeout).bind(old.to_rfc3339()).bind(task.id.to_string()).execute(pool).await.unwrap();
        }
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => {
            sqlx::query("UPDATE worker_tasks SET claimed_at=$1,heartbeat_at=$1,lease_secs=$2,timeout_ms=$3,created_at=$4 WHERE id=$5").bind((!missing).then_some(old)).bind(lease).bind(timeout).bind(old).bind(task.id).execute(pool).await.unwrap();
        }
    }
}
orch8_storage::backend_test!(async fn reconnect_skips_expired_or_unanchored_explicit_leases_and_task_deadlines(b) {
    for case in 0..3 {
        let blocked = held(&b).await;
        age(&b,&blocked,case).await;
        hidden(&b,&blocked).await;
        b.storage.delete_worker_task(blocked.id).await.unwrap();
    }
});

orch8_storage::backend_test!(async fn reconnect_large_timeout_keeps_live_claim_anchor_without_interval_overflow(b) {
    for lease in [3600i32,i32::MAX] {
        let task=held(&b).await;
        match &b.raw {
            RawPool::Sqlite(pool) => {sqlx::query("UPDATE worker_tasks SET heartbeat_at=NULL,lease_secs=?1,timeout_ms=?2 WHERE id=?3").bind(lease).bind(i64::MAX).bind(task.id.to_string()).execute(pool).await.unwrap();}
            #[cfg(feature="postgres")]
            RawPool::Postgres(pool) => {sqlx::query("UPDATE worker_tasks SET heartbeat_at=NULL,lease_secs=$1,timeout_ms=$2 WHERE id=$3").bind(lease).bind(i64::MAX).bind(task.id).execute(pool).await.unwrap();}
        }
        let rows=b.storage.held_worker_tasks_for_reporter(&reconnect()).await.unwrap();
        assert_eq!(rows.iter().map(|t|t.id).collect::<Vec<_>>(),vec![task.id],"a live claim_at fallback or large future deadline was lost");
        assert_eq!(rows[0].claim_epoch,task.claim_epoch);
        b.storage.delete_worker_task(task.id).await.unwrap();
    }
});
