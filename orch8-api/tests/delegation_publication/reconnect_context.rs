//! Reconnect recovers the original generation, with no inline-input fallback.
use super::issued_context::{capabilities, poll_raw};
use super::*;
use orch8_storage::worker_claim::{WorkerClaimPrincipal, WorkerReconnectRequest};
use orch8_types::delegation_definition::claim::DelegationClaimContext;

async fn recovery(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for mode in [Mode::Remote, Mode::Hosted, Mode::Inline] {
        let mut f = fixture(b.storage.as_ref(), &server, mode).await;
        f.request["input"] = json!({"private":"RECONNECT_INPUT_CANARY", "wide":u64::MAX,"body":"x".repeat(128*1024)});
        accepted(&server, &f.request).await;
        let caps = capabilities(&b, &f).await;
        let wire: Value = poll_raw(&server, &f, &caps).await.json().await.unwrap();
        let context: DelegationClaimContext =
            serde_json::from_value(wire["tasks"][0]["delegation"].clone()).unwrap();
        let request = WorkerReconnectRequest::new(
            WorkerClaimPrincipal::Root {},
            Some(f.owner.tenant_id.clone()),
            caps.runtime_id.to_string(),
            vec!["orch8.delegation".into()],
            1,
        )
        .unwrap();
        assert!(
            b.storage
                .held_worker_tasks_for_reporter(&request)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            b.storage
                .held_worker_tasks_with_transport(&request, None)
                .await
                .unwrap()
                .tasks()
                .is_empty()
        );
        let mut small = caps.delegation_protocol.unwrap();
        small.max_input_payload_bytes = 1;
        assert!(
            b.storage
                .held_worker_tasks_with_transport(&request, Some(&small))
                .await
                .unwrap()
                .tasks()
                .is_empty()
        );
        let foreign = WorkerReconnectRequest::new(
            WorkerClaimPrincipal::ApiKey {
                tenant_id: f.owner.tenant_id.clone(),
                key_id: "other-key".into(),
            },
            Some(f.owner.tenant_id.clone()),
            caps.runtime_id.to_string(),
            vec!["orch8.delegation".into()],
            1,
        )
        .unwrap();
        assert!(
            b.storage
                .held_worker_tasks_with_transport(&foreign, caps.delegation_protocol.as_ref())
                .await
                .unwrap()
                .tasks()
                .is_empty()
        );
        let (tasks, contexts) = b
            .storage
            .held_worker_tasks_with_transport(&request, caps.delegation_protocol.as_ref())
            .await
            .unwrap()
            .into_parts();
        assert_eq!(contexts, vec![context.clone()]);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].id, context.scope.task_id);
        assert_eq!(tasks[0].claim_epoch, context.scope.claim_epoch);
        assert_eq!(tasks[0].params, json!({}));
        assert_eq!(tasks[0].context, json!({}));
        assert!(!format!("{tasks:?}").contains("RECONNECT_INPUT_CANARY"));
        let stored = b
            .storage
            .get_worker_task(context.scope.task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.claim_epoch, context.scope.claim_epoch);
        assert_eq!(stored.params["input"], f.request["input"]);
    }
}
orch8_storage::backend_test!(async fn reconnect_preserves_exact_issued_context_without_inline_input(b){recovery(b).await;});

async fn unavailable(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
    accepted(&server, &f.request).await;
    let caps = capabilities(&b, &f).await;
    let wire: Value = poll_raw(&server, &f, &caps).await.json().await.unwrap();
    let mut context: DelegationClaimContext =
        serde_json::from_value(wire["tasks"][0]["delegation"].clone()).unwrap();
    let request = WorkerReconnectRequest::new(
        WorkerClaimPrincipal::Root {},
        Some(f.owner.tenant_id.clone()),
        caps.runtime_id.to_string(),
        vec!["orch8.delegation".into()],
        1,
    )
    .unwrap();
    context.scope.worker_id = "PRIVATE_CORRUPT_SCOPE".into();
    // Corrupt only this owned test database, bypassing immutable-row guards.
    let record = serde_json::to_string(&context).unwrap();
    match &b.raw {
        RawPool::Sqlite(pool) => {
            sqlx::query("DROP TRIGGER worker_issued_delegation_context_update")
                .execute(pool)
                .await
                .unwrap();
            sqlx::query("DROP TRIGGER worker_issued_delegation_context_delete")
                .execute(pool)
                .await
                .unwrap();
            sqlx::query("UPDATE worker_issued_delegation_contexts SET record=?1")
                .bind(record)
                .execute(pool)
                .await
                .unwrap();
        }
        RawPool::Postgres(pool) => {
            sqlx::query("DROP TRIGGER worker_issued_delegation_context_immutable ON worker_issued_delegation_contexts").execute(pool).await.unwrap();
            sqlx::query("UPDATE worker_issued_delegation_contexts SET record=$1")
                .bind(record)
                .execute(pool)
                .await
                .unwrap();
        }
    }
    let error = b
        .storage
        .held_worker_tasks_with_transport(&request, caps.delegation_protocol.as_ref())
        .await
        .unwrap_err();
    assert!(!format!("{error:?}").contains("PRIVATE_CORRUPT_SCOPE"));
    match &b.raw {
        RawPool::Sqlite(pool) => {
            sqlx::query("DELETE FROM worker_issued_delegation_contexts")
                .execute(pool)
                .await
                .unwrap();
        }
        RawPool::Postgres(pool) => {
            sqlx::query("DELETE FROM worker_issued_delegation_contexts")
                .execute(pool)
                .await
                .unwrap();
        }
    }
    assert!(
        b.storage
            .held_worker_tasks_with_transport(&request, caps.delegation_protocol.as_ref())
            .await
            .is_err()
    );
    assert_eq!(
        b.raw
            .count(
                "worker_issued_delegation_contexts",
                "task_id",
                &context.scope.task_id.to_string()
            )
            .await,
        0
    );
    assert_eq!(
        b.storage
            .get_worker_task(context.scope.task_id)
            .await
            .unwrap()
            .unwrap()
            .claim_epoch,
        context.scope.claim_epoch
    );
}
orch8_storage::backend_test!(async fn reconnect_never_reconstructs_corrupt_or_missing_issued_context(b){unavailable(b).await;});

async fn backfill(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
    accepted(&server, &f.request).await;
    let caps = capabilities(&b, &f).await;
    let wire: Value = poll_raw(&server, &f, &caps).await.json().await.unwrap();
    let delegated: Uuid = serde_json::from_value(wire["tasks"][0]["id"].clone()).unwrap();
    let mut ordinary = b.storage.get_worker_task(delegated).await.unwrap().unwrap();
    ordinary.id = Uuid::now_v7();
    ordinary.block_id = orch8_types::ids::BlockId::new("ordinary");
    ordinary.handler_name = "noop".into();
    ordinary.params = json!({"ordinary":true});
    ordinary.context = json!({});
    ordinary.requirements = orch8_types::continuity::CapsuleRequirements::default();
    ordinary.effect_id = None;
    ordinary.continuity_epoch = None;
    ordinary.created_at = Utc::now() + chrono::Duration::seconds(1);
    b.storage.create_worker_task(&ordinary).await.unwrap();
    let mut orphan = ordinary.clone();
    orphan.id = Uuid::now_v7();
    orphan.block_id = orch8_types::ids::BlockId::new("orphan");
    orphan.handler_name = "orch8.delegation".into();
    orphan.created_at = Utc::now() - chrono::Duration::seconds(1);
    b.storage.create_worker_task(&orphan).await.unwrap();
    // PostgreSQL dispatch inserts deliberately do not create held worker leases.
    // Seed the legacy reconnect fixture's owner explicitly on that backend.
    if let RawPool::Postgres(pool) = &b.raw {
        sqlx::query("UPDATE worker_tasks SET worker_id=$1 WHERE id=ANY($2)")
            .bind(caps.runtime_id.to_string())
            .bind(vec![ordinary.id, orphan.id])
            .execute(pool)
            .await
            .unwrap();
    }
    assert_eq!(
        b.storage
            .get_worker_task(ordinary.id)
            .await
            .unwrap()
            .unwrap()
            .worker_id,
        Some(caps.runtime_id.to_string())
    );

    let request = WorkerReconnectRequest::new(
        WorkerClaimPrincipal::Root {},
        Some(f.owner.tenant_id.clone()),
        caps.runtime_id.to_string(),
        vec!["orch8.delegation".into(), "noop".into()],
        1,
    )
    .unwrap();
    let mut small = caps.delegation_protocol.unwrap();
    small.max_input_payload_bytes = 1;
    for protocol in [None, Some(&small)] {
        let batch = b
            .storage
            .held_worker_tasks_with_transport(&request, protocol)
            .await
            .unwrap();
        assert_eq!(
            batch.tasks().iter().map(|t| t.id).collect::<Vec<_>>(),
            vec![ordinary.id],
            "an incompatible older task consumed the delivery limit"
        );
    }
    let legacy = b
        .storage
        .held_worker_tasks_for_reporter(&request)
        .await
        .unwrap();
    assert_eq!(
        legacy.iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![ordinary.id]
    );
    let batch = b
        .storage
        .held_worker_tasks_with_transport(&request, caps.delegation_protocol.as_ref())
        .await
        .unwrap();
    assert_eq!(
        batch.tasks().iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![delegated],
        "an uncertified reserved task consumed the delivery limit"
    );
}
orch8_storage::backend_test!(async fn reconnect_filters_protocol_before_limit_and_backfills_ordinary_tasks(b){backfill(b).await;});
