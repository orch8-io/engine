//! Historical claimed rows must not bypass a captured or retired physical source.
use super::*;
use orch8_storage::{
    testing::backends::{RawPool, TestBackend},
    worker_claim::{WorkerClaimPrincipal, WorkerClaimRequest, WorkerReconnectRequest},
};

async fn legacy_held(b: &TestBackend, task: &orch8_types::worker::WorkerTask) {
    match &b.raw {
        RawPool::Sqlite(pool) => {
            let mut tx = pool.begin().await.unwrap();
            sqlx::query("DROP TRIGGER physical_source_worker_claim")
                .execute(&mut *tx)
                .await
                .unwrap();
            sqlx::query("UPDATE worker_tasks SET state='claimed',worker_id='reconnect-worker',claim_epoch=1 WHERE id=?1").bind(task.id.to_string()).execute(&mut *tx).await.unwrap();
            sqlx::raw_sql(include_str!("../../src/sqlite/physical_retirement.sql"))
                .execute(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => {
            let mut tx = pool.begin().await.unwrap();
            sqlx::query("ALTER TABLE worker_tasks DISABLE TRIGGER physical_source_worker_claim")
                .execute(&mut *tx)
                .await
                .unwrap();
            sqlx::query("UPDATE worker_tasks SET state='claimed',worker_id='reconnect-worker',claim_epoch=1 WHERE id=$1").bind(task.id).execute(&mut *tx).await.unwrap();
            sqlx::query("ALTER TABLE worker_tasks ENABLE TRIGGER physical_source_worker_claim")
                .execute(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
    }
}
async fn healthy_held(
    b: &TestBackend,
    request: &CapsuleCutRequest,
    blocked: &orch8_types::worker::WorkerTask,
) -> orch8_types::worker::WorkerTask {
    let mut parent = b
        .storage
        .get_instance(blocked.instance_id)
        .await
        .unwrap()
        .unwrap();
    parent.id = InstanceId::new();
    b.storage.create_instance(&parent).await.unwrap();
    let mut healthy = blocked.clone();
    healthy.id = uuid::Uuid::new_v4();
    healthy.instance_id = parent.id;
    healthy.created_at = blocked.created_at + Duration::seconds(1);
    b.storage.create_worker_task(&healthy).await.unwrap();
    let claim = WorkerClaimRequest::new(
        WorkerClaimPrincipal::Root {},
        healthy.handler_name.clone(),
        "reconnect-worker".into(),
        Some(request.owner.tenant_id.clone()),
        None,
        None,
        1,
    )
    .unwrap();
    assert_eq!(
        b.storage
            .claim_worker_tasks_with_authority(&claim)
            .await
            .unwrap()[0]
            .id,
        healthy.id
    );
    healthy
}
async fn frozen(b: TestBackend, retired: bool) {
    let (request, cut, blocked) = if retired {
        let (request, task) = legacy_retired_source(&b).await;
        (request, None, task)
    } else {
        let (request, cut, task) = captured_source_with_late_queue(&b).await;
        (request, Some(cut), task)
    };
    legacy_held(&b, &blocked).await;
    let before = b
        .storage
        .get_worker_task(blocked.id)
        .await
        .unwrap()
        .unwrap();
    let healthy = healthy_held(&b, &request, &blocked).await;
    let lookup = WorkerReconnectRequest::new(
        WorkerClaimPrincipal::Root {},
        Some(request.owner.tenant_id.clone()),
        "reconnect-worker".into(),
        vec![healthy.handler_name.clone()],
        1,
    )
    .unwrap();
    let found = b
        .storage
        .held_worker_tasks_for_reporter(&lookup)
        .await
        .unwrap();
    assert_eq!(
        found.iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![healthy.id],
        "frozen source consumed the recovery window"
    );
    assert_eq!(
        serde_json::to_value(
            b.storage
                .get_worker_task(blocked.id)
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(before).unwrap()
    );
    if let Some(cut) = cut {
        assert!(
            b.storage
                .abort_preparing_capsule_cut(&request.owner.tenant_id, cut.id)
                .await
                .unwrap()
        );
        let restored = b
            .storage
            .held_worker_tasks_for_reporter(&lookup)
            .await
            .unwrap();
        assert_eq!(
            restored[0].id, blocked.id,
            "an explicit unsealed abort restores eligible held work"
        );
        assert_eq!(restored[0].claim_epoch, 1);
    }
}
orch8_storage::backend_test!(async fn reconnect_skips_captured_source_before_limit_and_unsealed_abort_restores_it(b){Box::pin(frozen(b,false)).await;});
orch8_storage::backend_test!(async fn reconnect_skips_permanently_retired_source_before_limit(b){Box::pin(frozen(b,true)).await;});
