//! Claim provenance must commit with the lease and survive independent history GC.
use chrono::Utc;
use orch8_storage::{
    StorageBackend,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
    worker_admission::{WorkerDispatchIntent, WorkerDispatchPublication},
    worker_claim::{WorkerClaimPrincipal as Principal, WorkerClaimRequest},
    worker_invocation::WorkerDispatchSelection,
};
use orch8_types::{
    continuity::{RuntimeCapabilities, RuntimeId},
    ids::{BlockId, TenantId},
    instance::InstanceState,
    worker::{WorkerClaim, WorkerTask, WorkerTaskState},
};
use serde_json::json;
use uuid::Uuid;

async fn fixture(s: &dyn StorageBackend, admitted: bool) -> WorkerTask {
    fixture_in_queue(s, admitted, Some("queue")).await
}

async fn fixture_in_queue(
    s: &dyn StorageBackend,
    admitted: bool,
    queue: Option<&str>,
) -> WorkerTask {
    fixture_for_handler(s, admitted, queue, "noop").await
}

async fn fixture_for_handler(
    s: &dyn StorageBackend,
    admitted: bool,
    queue: Option<&str>,
    handler: &str,
) -> WorkerTask {
    let seq = SequenceBuilder::new("issued-claims")
        .step("work", handler)
        .build();
    s.create_sequence(&seq).await.unwrap();
    let mut parent = InstanceBuilder::for_sequence(&seq).due_now().build();
    parent.context.runtime.run_id = Some(Uuid::now_v7().to_string());
    parent.context.runtime.current_step = Some(BlockId::new("work"));
    parent.context.runtime.current_step_started_at = Some(Utc::now());
    parent.context.data = json!({"private":"ISSUED_CLAIM_CONTEXT_CANARY"});
    s.create_instance(&parent).await.unwrap();
    let task:WorkerTask=serde_json::from_value(json!({"id":Uuid::now_v7(),"instance_id":parent.id,"block_id":"work","handler_name":handler,"queue_name":queue,"params":{"private":"ISSUED_CLAIM_INPUT_CANARY"},"context":parent.context,"attempt":0,"state":"pending","created_at":Utc::now()})).unwrap();
    if admitted {
        let parent = s
            .claim_due_instances(Utc::now() + chrono::Duration::seconds(1), 1, 0)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let fence = s
            .prepare_worker_dispatch(
                &parent,
                &task.block_id,
                &WorkerDispatchSelection::Flat {
                    step_started_at: parent.context.runtime.current_step_started_at.unwrap(),
                },
            )
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            s.admit_worker_dispatch(&WorkerDispatchIntent::new(task.clone(), fence, None).unwrap())
                .await
                .unwrap(),
            WorkerDispatchPublication::Published { .. }
        ));
    } else {
        s.create_worker_task(&task).await.unwrap();
    }
    s.update_instance_state(parent.id, InstanceState::Waiting, None)
        .await
        .unwrap();
    s.get_worker_task(task.id).await.unwrap().unwrap()
}
fn capabilities() -> RuntimeCapabilities {
    serde_json::from_value(json!({"runtime_id":RuntimeId::new(),"kind":"server","trust":"registered","handlers":["noop"],"observed_at":Utc::now(),"expires_at":Utc::now()+chrono::Duration::hours(1)})).unwrap()
}
fn request(mode: u8) -> WorkerClaimRequest {
    let principal = if matches!(mode, 0 | 2) {
        Principal::Root {}
    } else {
        Principal::ApiKey {
            tenant_id: TenantId::unchecked("issued-claims"),
            key_id: "ak-original".into(),
        }
    };
    WorkerClaimRequest::new(
        principal,
        "noop".into(),
        "worker".into(),
        None,
        (mode >= 2).then(|| "queue".into()),
        (mode == 4).then(capabilities),
        1,
    )
    .unwrap()
}
fn proof(task: &WorkerTask) -> WorkerClaim {
    WorkerClaim::new(task.worker_id.clone().unwrap(), task.claim_epoch)
}

orch8_storage::backend_test!(async fn runtime_default_selector_leases_only_the_default_queue(b) {
    use orch8_types::continuity::RuntimeKind;
    let s=b.storage.as_ref();
    for kind in [RuntimeKind::Browser,RuntimeKind::Mobile] {
        let named=fixture_in_queue(s,false,Some("queue")).await;
        let default=fixture_in_queue(s,false,None).await;
        let mut caps=capabilities();caps.kind=kind;
        let principal=Principal::Runtime {
            tenant_id:TenantId::unchecked("issued-claims"),runtime_id:caps.runtime_id,
            runtime_kind:kind,device_id:(kind==RuntimeKind::Mobile).then(||"phone-a".into()),
        };
        let request=WorkerClaimRequest::new(principal.clone(),"noop".into(),caps.runtime_id.to_string(),None,None,Some(caps.clone()),10).unwrap();
        let held=s.claim_worker_tasks_with_authority(&request).await.unwrap();
        assert_eq!(held.iter().map(|t|t.id).collect::<Vec<_>>(),vec![default.id]);
        assert_eq!(s.get_worker_task(named.id).await.unwrap().unwrap().state,WorkerTaskState::Pending);
        let request=WorkerClaimRequest::new(principal,"noop".into(),caps.runtime_id.to_string(),None,Some("queue".into()),Some(caps),10).unwrap();
        let held=s.claim_worker_tasks_with_authority(&request).await.unwrap();
        assert_eq!(held.iter().map(|t|t.id).collect::<Vec<_>>(),vec![named.id],"explicit named queue must remain usable");
        for id in [named.id,default.id] {s.delete_worker_task(id).await.unwrap();}
    }
});

orch8_storage::backend_test!(async fn issued_principal_and_original_admission_survive_task_and_native_gc(b) {
    let s=b.storage.as_ref();
    for mode in 0..5 {
        let task=fixture(s,true).await;
        let held=s.claim_worker_tasks_with_authority(&request(mode)).await.unwrap().pop().unwrap();
        assert_eq!(held.id,task.id);
        let claim=proof(&held);let tenant=TenantId::unchecked("issued-claims");
        let issued=s.get_worker_issued_claim(Some(&tenant),held.id,&claim).await.unwrap().unwrap();
        assert_eq!(issued.admission.as_ref(),s.get_worker_dispatch_admission(&tenant,held.id).await.unwrap().as_ref());
        assert_eq!(issued.invocation.as_ref(),issued.admission.as_ref().map(|a|&a.invocation));
        assert!(issued.principal.has_authenticated_owner());
        assert!(!issued.belongs_to(&Principal::ApiKey{tenant_id:tenant.clone(),key_id:"ak-replacement".into()}));
        let body=serde_json::to_string(&issued).unwrap();
        assert!(!body.contains("ISSUED_CLAIM_CONTEXT_CANARY"));assert!(!body.contains("ISSUED_CLAIM_INPUT_CANARY"));assert!(body.len()<8192);
        for wrong in [WorkerClaim::new("other",claim.claim_epoch),WorkerClaim::new("worker",claim.claim_epoch+1),WorkerClaim::new("worker",u64::MAX)]{
            assert!(s.get_worker_issued_claim(Some(&tenant),held.id,&wrong).await.unwrap().is_none());
        }
        assert!(s.get_worker_issued_claim(Some(&TenantId::unchecked("foreign")),held.id,&claim).await.unwrap().is_none());
        s.delete_worker_task(held.id).await.unwrap();
        s.update_instance_state(held.instance_id,InstanceState::Cancelled,None).await.unwrap();
        assert_eq!(s.purge_terminal_instances(&[held.instance_id]).await.unwrap(),1);
        assert_eq!(s.get_worker_issued_claim(None,held.id,&claim).await.unwrap(),Some(issued.clone()));
        assert_eq!(s.get_worker_issued_claim(Some(&tenant),held.id,&claim).await.unwrap(),Some(issued));
    }
});

orch8_storage::backend_test!(async fn legacy_claim_paths_are_retained_without_inventing_authenticated_ownership(b) {
    let s=b.storage.as_ref();let tenant=TenantId::unchecked("issued-claims");
    for mode in 0..5 {
        let task=fixture(s,false).await;
        let held=match mode {
            0=>s.claim_worker_tasks("noop","worker",1).await,
            1=>s.claim_worker_tasks_for_tenant("noop","worker",&tenant,1).await,
            2=>s.claim_worker_tasks_from_queue("queue","noop","worker",1).await,
            3=>s.claim_worker_tasks_from_queue_for_tenant("queue","noop","worker",&tenant,1).await,
            _=>s.claim_worker_tasks_matching("noop","worker",Some(&tenant),Some("queue"),&capabilities(),1).await,
        }.unwrap().pop().unwrap();
        assert_eq!(held.id,task.id);
        let issued=s.get_worker_issued_claim(Some(&tenant),held.id,&proof(&held)).await.unwrap().unwrap();
        assert_eq!(issued.principal,Principal::Legacy {});assert!(!issued.principal.has_authenticated_owner());
        assert!(!issued.belongs_to(&Principal::Legacy {}));assert!(issued.admission.is_none());
        s.delete_worker_task(task.id).await.unwrap();
        assert!(s.create_worker_task(&task).await.is_err(),"issued task identity was reused after hot cleanup");
    }
});

orch8_storage::backend_test!(async fn journal_insert_failure_restores_lease_generation_and_attempt_events(b) {
    use orch8_storage::testing::backends::RawPool;
    let s=b.storage.as_ref();let task=fixture(s,true).await;
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::raw_sql("CREATE TRIGGER reject_issued_claim BEFORE INSERT ON worker_issued_claims BEGIN SELECT RAISE(ABORT,'injected claim journal failure'); END;").execute(pool).await.unwrap();},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("CREATE FUNCTION reject_issued_claim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected claim journal failure'; END $$; CREATE TRIGGER reject_issued_claim BEFORE INSERT ON worker_issued_claims FOR EACH ROW EXECUTE FUNCTION reject_issued_claim();").execute(pool).await.unwrap();},
    }
    for mode in 0..5 {
        assert!(s.claim_worker_tasks_with_authority(&request(mode)).await.is_err());
        let after=s.get_worker_task(task.id).await.unwrap().unwrap();
        assert_eq!(after.state,WorkerTaskState::Pending);assert_eq!(after.claim_epoch,0);assert!(after.worker_id.is_none());
        assert!(s.list_worker_task_attempt_events(task.id,10).await.unwrap().is_empty());
        assert!(s.get_worker_issued_claim(Some(&TenantId::unchecked("issued-claims")),task.id,&WorkerClaim::new("worker",1)).await.unwrap().is_none());
    }
});

orch8_storage::backend_test!(async fn reclaim_keeps_each_original_principal_without_transfer_to_a_new_key(b) {
    let s=b.storage.as_ref();let task=fixture(s,false).await;let tenant=TenantId::unchecked("issued-claims");
    let first=s.claim_worker_tasks_with_authority(&request(1)).await.unwrap().pop().unwrap();
    let original=s.get_worker_issued_claim(Some(&tenant),task.id,&proof(&first)).await.unwrap().unwrap();
    assert_eq!(s.reap_stale_worker_tasks(std::time::Duration::ZERO).await.unwrap(),1);
    let replacement=Principal::ApiKey {tenant_id:tenant.clone(),key_id:"ak-replacement".into()};
    let next=WorkerClaimRequest::new(replacement.clone(),"noop".into(),"worker".into(),Some(tenant.clone()),None,None,1).unwrap();
    let second=s.claim_worker_tasks_with_authority(&next).await.unwrap().pop().unwrap();
    assert!(second.claim_epoch>first.claim_epoch);
    assert_eq!(s.get_worker_issued_claim(Some(&tenant),task.id,&proof(&first)).await.unwrap(),Some(original.clone()));
    assert!(!original.belongs_to(&replacement));
    assert!(s.get_worker_issued_claim(Some(&tenant),task.id,&proof(&second)).await.unwrap().unwrap().belongs_to(&replacement));
});

#[test]
fn runtime_claim_scope_requires_the_bound_device_kind_runtime_and_tenant() {
    use orch8_types::continuity::RuntimeKind;
    let id = RuntimeId::new();
    let tenant = TenantId::unchecked("tenant");
    let actor = Principal::Runtime {
        tenant_id: tenant.clone(),
        runtime_id: id,
        runtime_kind: RuntimeKind::Mobile,
        device_id: Some("phone-a".into()),
    };
    let mut caps = capabilities();
    caps.kind = RuntimeKind::Mobile;
    caps.runtime_id = id;
    assert!(
        WorkerClaimRequest::new(
            actor.clone(),
            "noop".into(),
            id.to_string(),
            Some(tenant.clone()),
            None,
            Some(caps.clone()),
            1
        )
        .is_ok()
    );
    assert!(
        WorkerClaimRequest::new(
            actor.clone(),
            "noop".into(),
            "different".into(),
            None,
            None,
            Some(caps.clone()),
            1
        )
        .is_err()
    );
    assert!(
        WorkerClaimRequest::new(
            actor.clone(),
            "noop".into(),
            id.to_string(),
            Some(TenantId::unchecked("foreign")),
            None,
            Some(caps.clone()),
            1
        )
        .is_err()
    );
    caps.kind = RuntimeKind::Browser;
    assert!(
        WorkerClaimRequest::new(
            actor,
            "noop".into(),
            id.to_string(),
            None,
            None,
            Some(caps),
            1
        )
        .is_err()
    );
}

orch8_storage::backend_test!(async fn batched_journal_failure_rolls_back_prior_batches_and_every_claim(b) {
    use orch8_storage::testing::backends::RawPool;
    let s=b.storage.as_ref();
    let mut ids=Vec::new();for _ in 0..260 {ids.push(fixture(s,false).await.id);}
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::raw_sql("CREATE TRIGGER reject_later_claim_batch BEFORE INSERT ON worker_issued_claims WHEN (SELECT COUNT(*) FROM worker_issued_claims)>=128 BEGIN SELECT RAISE(ABORT,'injected later claim batch failure'); END;").execute(pool).await.unwrap();},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("CREATE FUNCTION reject_later_claim_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF (SELECT COUNT(*) FROM worker_issued_claims)>=128 THEN RAISE EXCEPTION 'injected later claim batch failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_later_claim_batch BEFORE INSERT ON worker_issued_claims FOR EACH ROW EXECUTE FUNCTION reject_later_claim_batch();").execute(pool).await.unwrap();},
    }
    let request=WorkerClaimRequest::new(Principal::Root {},"noop".into(),"batch-worker".into(),None,None,None,260).unwrap();
    assert!(s.claim_worker_tasks_with_authority(&request).await.is_err());
    let count:i64=match &b.raw {
        RawPool::Sqlite(pool)=>sqlx::query_scalar("SELECT COUNT(*) FROM worker_issued_claims").fetch_one(pool).await.unwrap(),
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>sqlx::query_scalar("SELECT COUNT(*) FROM worker_issued_claims").fetch_one(pool).await.unwrap(),
    };assert_eq!(count,0);
    for id in &ids {
        let task=s.get_worker_task(*id).await.unwrap().unwrap();assert_eq!(task.state,WorkerTaskState::Pending);assert_eq!(task.claim_epoch,0);
        assert!(s.list_worker_task_attempt_events(*id,1).await.unwrap().is_empty());
    }
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::raw_sql("DROP TRIGGER reject_later_claim_batch").execute(pool).await.unwrap();},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("DROP TRIGGER reject_later_claim_batch ON worker_issued_claims; DROP FUNCTION reject_later_claim_batch();").execute(pool).await.unwrap();},
    }
    let held=s.claim_worker_tasks_with_authority(&request).await.unwrap();assert_eq!(held.len(),260);
    for task in held {
        let issued=s.get_worker_issued_claim(Some(&TenantId::unchecked("issued-claims")),task.id,&proof(&task)).await.unwrap().unwrap();
        assert_eq!(issued.principal,Principal::Root {});
    }
});

#[cfg(feature = "postgres")]
#[tokio::test]
async fn issued_claim_migration_roundtrip_is_empty_only() {
    let Some(b) = orch8_storage::testing::backends::TestBackend::postgres().await else {
        return;
    };
    let down = include_str!("../../migrations/down/140_worker_issued_claims.sql");
    let up = include_str!("../../migrations/140_worker_issued_claims.sql");
    let mut tx = b.pg_pool().begin().await.unwrap();
    sqlx::raw_sql(down).execute(&mut *tx).await.unwrap();
    sqlx::raw_sql(up).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let task = fixture(b.storage.as_ref(), false).await;
    let held = b
        .storage
        .claim_worker_tasks_with_authority(&request(1))
        .await
        .unwrap()
        .pop()
        .unwrap();
    let tenant = TenantId::unchecked("issued-claims");
    let before = b
        .storage
        .get_worker_issued_claim(Some(&tenant), task.id, &proof(&held))
        .await
        .unwrap()
        .unwrap();
    assert!(sqlx::raw_sql(down).execute(b.pg_pool()).await.is_err());
    assert!(
        sqlx::query("UPDATE worker_issued_claims SET record=record WHERE task_id=$1")
            .bind(task.id)
            .execute(b.pg_pool())
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM worker_issued_claims WHERE task_id=$1")
            .bind(task.id)
            .execute(b.pg_pool())
            .await
            .is_err()
    );
    assert_eq!(
        b.storage
            .get_worker_issued_claim(Some(&tenant), task.id, &proof(&held))
            .await
            .unwrap(),
        Some(before)
    );
}

orch8_storage::backend_test!(async fn issued_claim_records_cannot_be_updated_or_deleted(b) {
    use orch8_storage::testing::backends::RawPool;
    let s=b.storage.as_ref();let task=fixture(s,false).await;
    let held=s.claim_worker_tasks_with_authority(&request(1)).await.unwrap().pop().unwrap();
    let tenant=TenantId::unchecked("issued-claims");
    let original=s.get_worker_issued_claim(Some(&tenant),task.id,&proof(&held)).await.unwrap().unwrap();
    let mut future=original.clone();future.version=2;
    assert!(!future.belongs_to(&original.principal),"unknown claim versions must not certify ownership");
    match &b.raw {
        RawPool::Sqlite(pool)=>{
            assert!(sqlx::query("UPDATE worker_issued_claims SET record=record WHERE task_id=?1").bind(task.id.to_string()).execute(pool).await.is_err());
            assert!(sqlx::query("DELETE FROM worker_issued_claims WHERE task_id=?1").bind(task.id.to_string()).execute(pool).await.is_err());
        },
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{
            assert!(sqlx::query("UPDATE worker_issued_claims SET record=record WHERE task_id=$1").bind(task.id).execute(pool).await.is_err());
            assert!(sqlx::query("DELETE FROM worker_issued_claims WHERE task_id=$1").bind(task.id).execute(pool).await.is_err());
        },
    }
    assert_eq!(s.get_worker_issued_claim(Some(&tenant),task.id,&proof(&held)).await.unwrap(),Some(original));
});

#[cfg(feature = "postgres")]
#[tokio::test]
async fn issued_task_identity_is_not_reused_after_a_concurrent_hot_delete() {
    let Some(b) = orch8_storage::testing::backends::TestBackend::postgres().await else {
        return;
    };
    let mut task = fixture(b.storage.as_ref(), false).await;
    b.storage.delete_worker_task(task.id).await.unwrap();
    task.id = Uuid::now_v7();
    // A legacy writer can create a queue row without an invocation record.
    sqlx::query("INSERT INTO worker_tasks(id,instance_id,block_id,handler_name,params,context,state,attempt,created_at,tenant_id) VALUES($1,$2,'work','noop','{}'::jsonb,'{}'::jsonb,'pending',0,NOW(),'issued-claims')")
        .bind(task.id).bind(task.instance_id.into_uuid()).execute(b.pg_pool()).await.unwrap();
    let held = b
        .storage
        .claim_worker_tasks_with_authority(&request(1))
        .await
        .unwrap()
        .pop()
        .unwrap();
    let tenant = TenantId::unchecked("issued-claims");
    let original = b
        .storage
        .get_worker_issued_claim(Some(&tenant), task.id, &proof(&held))
        .await
        .unwrap()
        .unwrap();
    assert!(original.invocation.is_none());
    assert!(original.admission.is_none());
    // There is deliberately no invocation, accepted outcome or resolution
    // report: only issued-claim provenance can protect this identity.
    let mut inserting = b.pg_pool().acquire().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *inserting)
        .await
        .unwrap();
    let mut deleting = b.pg_pool().begin().await.unwrap();
    sqlx::query("DELETE FROM worker_tasks WHERE id=$1")
        .bind(task.id)
        .execute(&mut *deleting)
        .await
        .unwrap();
    let task_id = task.id;
    let instance_id = task.instance_id;
    let insertion = tokio::spawn(async move {
        sqlx::query("INSERT INTO worker_tasks(id,instance_id,block_id,handler_name,params,context,state,attempt,created_at,tenant_id) VALUES($1,$2,'work','noop','{}'::jsonb,'{}'::jsonb,'pending',0,NOW(),'issued-claims')")
            .bind(task_id).bind(instance_id.into_uuid()).execute(&mut *inserting).await
    });
    let waiting=tokio::time::timeout(std::time::Duration::from_secs(3),async {
        loop {
            let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')").bind(pid).fetch_one(b.pg_pool()).await.unwrap();
            if blocked {break;}
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await;
    deleting.commit().await.unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(3), insertion)
        .await
        .unwrap()
        .unwrap();
    assert!(
        waiting.is_ok(),
        "insertion did not wait for the concurrent deletion"
    );
    let error = result.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("issued worker task identity must not be reused"),
        "{error}"
    );
    assert!(b.storage.get_worker_task(task_id).await.unwrap().is_none());
    assert_eq!(
        b.storage
            .get_worker_issued_claim(Some(&tenant), task_id, &proof(&held))
            .await
            .unwrap(),
        Some(original)
    );
}

orch8_storage::backend_test!(async fn runtime_workload_and_unsecured_claims_keep_their_exact_identity_class(b) {
    use orch8_types::continuity::RuntimeKind;
    let s=b.storage.as_ref();let tenant=TenantId::unchecked("issued-claims");
    for class in 0..4 {
        let task=fixture(s,false).await;let id=RuntimeId::new();
        let principal=match class {
            0=>Principal::Runtime{tenant_id:tenant.clone(),runtime_id:id,runtime_kind:RuntimeKind::Mobile,device_id:Some("phone-a".into())},
            1=>Principal::Runtime{tenant_id:tenant.clone(),runtime_id:id,runtime_kind:RuntimeKind::Browser,device_id:None},
            2=>Principal::Workload{tenant_id:tenant.clone(),identity:"payments-service".into()},
            _=>Principal::Unsecured {},
        };
        let caps=if class<2 {let mut caps=capabilities();caps.runtime_id=id;caps.kind=if class==0{RuntimeKind::Mobile}else{RuntimeKind::Browser};Some(caps)}else{None};
        let worker=if class<2{id.to_string()}else{"worker".into()};
        let request=WorkerClaimRequest::new(principal.clone(),"noop".into(),worker,None,Some("queue".into()),caps,1).unwrap();
        let held=s.claim_worker_tasks_with_authority(&request).await.unwrap().pop().unwrap();assert_eq!(held.id,task.id);
        let issued=s.get_worker_issued_claim(Some(&tenant),task.id,&proof(&held)).await.unwrap().unwrap();
        assert_eq!(issued.principal,principal);assert_eq!(issued.belongs_to(&principal),class!=3);
        if class==0 {
            let other_device=Principal::Runtime{tenant_id:tenant.clone(),runtime_id:id,runtime_kind:RuntimeKind::Mobile,device_id:Some("phone-b".into())};
            assert!(!issued.belongs_to(&other_device),"a different device inherited the runtime's issued claim");
        }
        if class==2 {assert!(!issued.belongs_to(&Principal::Workload{tenant_id:tenant.clone(),identity:"different-service".into()}));}
        s.delete_worker_task(task.id).await.unwrap();
    }
});

#[test]
fn owner_identity_decoding_refuses_unknown_scope_fields() {
    for value in [
        json!({"kind":"root","tenant_id":"foreign"}),
        json!({"kind":"legacy","key_id":"ak-invented"}),
        json!({"kind":"unsecured","tenant_id":"foreign"}),
        json!({"kind":"api_key","tenant_id":"tenant","key_id":"ak-one","realm":"future"}),
        json!({"kind":"runtime","tenant_id":"tenant","runtime_id":RuntimeId::new(),"runtime_kind":"browser","account_scope":"future"}),
        json!({"kind":"workload","tenant_id":"tenant","identity":"service","audience":"future"}),
    ] {
        assert!(
            serde_json::from_value::<Principal>(value.clone()).is_err(),
            "unrecognized identity scope was discarded: {value}"
        );
    }
}

orch8_storage::backend_test!(async fn recorded_reporter_distinguishes_ownership_from_unattributed_history(b) {
    use orch8_storage::worker_claim::{RecordedReporter,recorded_reporter};
    let s=b.storage.as_ref();let tenant=TenantId::unchecked("issued-claims");
    let task=fixture(s,false).await;
    let held=s.claim_worker_tasks_with_authority(&request(1)).await.unwrap().pop().unwrap();
    assert_eq!(held.id,task.id);
    let original=Principal::ApiKey{tenant_id:tenant.clone(),key_id:"ak-original".into()};
    assert_eq!(recorded_reporter(s,Some(&tenant),task.id,&proof(&held),&original).await.unwrap(),RecordedReporter::Matches);
    for foreign in [Principal::Root {},Principal::Unsecured {},Principal::ApiKey{tenant_id:tenant.clone(),key_id:"ak-other".into()}] {
        assert_eq!(recorded_reporter(s,Some(&tenant),task.id,&proof(&held),&foreign).await.unwrap(),RecordedReporter::Different);
    }
    s.delete_worker_task(task.id).await.unwrap();
    assert_eq!(recorded_reporter(s,Some(&tenant),task.id,&proof(&held),&original).await.unwrap(),RecordedReporter::Matches);
    let legacy=fixture(s,false).await;
    let held=s.claim_worker_tasks_for_tenant("noop","old-worker",&tenant,1).await.unwrap().pop().unwrap();
    assert_eq!(held.id,legacy.id);
    assert_eq!(recorded_reporter(s,Some(&tenant),held.id,&proof(&held),&original).await.unwrap(),RecordedReporter::Unattributed);
    assert!(matches!(s.get_worker_issued_claim(Some(&tenant),held.id,&proof(&held)).await.unwrap().unwrap().principal,Principal::Legacy {}));
    s.delete_worker_task(legacy.id).await.unwrap();
    let dev=fixture(s,false).await;
    let request=WorkerClaimRequest::new(Principal::Unsecured {},"noop".into(),"dev-worker".into(),Some(tenant.clone()),None,None,1).unwrap();
    let held=s.claim_worker_tasks_with_authority(&request).await.unwrap().pop().unwrap();
    assert_eq!(held.id,dev.id);
    assert_eq!(recorded_reporter(s,Some(&tenant),held.id,&proof(&held),&Principal::Unsecured {}).await.unwrap(),RecordedReporter::Matches);
    assert_eq!(recorded_reporter(s,Some(&tenant),held.id,&proof(&held),&original).await.unwrap(),RecordedReporter::Different);

});

orch8_storage::backend_test!(async fn reconnect_filters_principal_and_handler_before_limit(b) {
    use orch8_storage::worker_claim::WorkerReconnectRequest;
    let s=b.storage.as_ref();let tenant=TenantId::unchecked("issued-claims");
    let key_a=Principal::ApiKey{tenant_id:tenant.clone(),key_id:"key-a".into()};
    let key_b=Principal::ApiKey{tenant_id:tenant.clone(),key_id:"key-b".into()};
    let other=fixture_for_handler(s,false,None,"other").await;
    let request=WorkerClaimRequest::new(key_b.clone(),"other".into(),"shared-worker".into(),None,None,None,1).unwrap();
    assert_eq!(s.claim_worker_tasks_with_authority(&request).await.unwrap()[0].id,other.id);
    for _ in 0..3 {
        let task=fixture(s,false).await;
        let request=WorkerClaimRequest::new(key_a.clone(),"noop".into(),"shared-worker".into(),None,None,None,1).unwrap();
        assert_eq!(s.claim_worker_tasks_with_authority(&request).await.unwrap()[0].id,task.id);
    }
    let own=fixture(s,false).await;
    let request=WorkerClaimRequest::new(key_b.clone(),"noop".into(),"shared-worker".into(),None,None,None,1).unwrap();
    assert_eq!(s.claim_worker_tasks_with_authority(&request).await.unwrap()[0].id,own.id);
    // Logical export/import can reorder JSON keys. Identity is structural.
    if let orch8_storage::testing::backends::RawPool::Sqlite(pool)=&b.raw {
        let record:String=sqlx::query_scalar("SELECT record FROM worker_issued_claims WHERE task_id=?1").bind(own.id.to_string()).fetch_one(pool).await.unwrap();
        let original=serde_json::to_string(&key_b).unwrap();
        assert!(record.contains(&original));
        let reordered=record.replace(&original,r#"{"key_id":"key-b","tenant_id":"issued-claims","kind":"api_key"}"#);
        assert_ne!(record,reordered);
        sqlx::raw_sql("DROP TRIGGER worker_issued_claim_update").execute(pool).await.unwrap();
        sqlx::query("UPDATE worker_issued_claims SET record=?1 WHERE task_id=?2").bind(reordered).bind(own.id.to_string()).execute(pool).await.unwrap();
        sqlx::raw_sql(include_str!("../src/sqlite/worker_claims.sql")).execute(pool).await.unwrap();
    }
    let reconnect=WorkerReconnectRequest::new(key_b.clone(),None,"shared-worker".into(),vec!["noop".into()],1).unwrap();
    let held=s.held_worker_tasks_for_reporter(&reconnect).await.unwrap();
    assert_eq!(held.iter().map(|t|t.id).collect::<Vec<_>>(),vec![own.id],"another issuer/handler consumed the reconnect window");
    assert_eq!(held[0].params,json!({"private":"ISSUED_CLAIM_INPUT_CANARY"}),"encrypted wrappers must decrypt only selected tasks");
    let root=WorkerReconnectRequest::new(Principal::Root {},Some(tenant.clone()),"shared-worker".into(),vec!["noop".into()],10).unwrap();
    assert!(s.held_worker_tasks_for_reporter(&root).await.unwrap().is_empty());
    let wrong=WorkerReconnectRequest::new(key_b.clone(),None,"wrong-worker".into(),vec!["noop".into()],10).unwrap();
    assert!(s.held_worker_tasks_for_reporter(&wrong).await.unwrap().is_empty());
    assert!(WorkerReconnectRequest::new(key_b.clone(),Some(TenantId::unchecked("foreign")),"shared-worker".into(),vec!["noop".into()],1).is_err());
    assert_eq!(s.reap_stale_worker_tasks(std::time::Duration::ZERO).await.unwrap(),5);
    let request=WorkerClaimRequest::new(key_a.clone(),"noop".into(),"shared-worker".into(),None,None,None,10).unwrap();
    assert_eq!(s.claim_worker_tasks_with_authority(&request).await.unwrap().len(),4);
    assert!(s.held_worker_tasks_for_reporter(&reconnect).await.unwrap().is_empty(),"a historical generation authorized a newer lease");
});

orch8_storage::backend_test!(async fn issued_worker_index_corruption_is_not_unattributed_history(b) {
    use orch8_storage::testing::backends::RawPool;
    let s=b.storage.as_ref();let task=fixture(s,false).await;
    let held=s.claim_worker_tasks_with_authority(&request(1)).await.unwrap().pop().unwrap();
    match &b.raw {
        RawPool::Sqlite(pool)=>{
            sqlx::raw_sql("DROP TRIGGER worker_issued_claim_update").execute(pool).await.unwrap();
            sqlx::query("UPDATE worker_issued_claims SET worker_id='corrupted-index' WHERE task_id=?1").bind(task.id.to_string()).execute(pool).await.unwrap();
        },
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{
            sqlx::raw_sql("DROP TRIGGER worker_issued_claim_update ON worker_issued_claims").execute(pool).await.unwrap();
            sqlx::query("UPDATE worker_issued_claims SET worker_id='corrupted-index' WHERE task_id=$1").bind(task.id).execute(pool).await.unwrap();
        },
    }
    assert!(s.get_worker_issued_claim(Some(&TenantId::unchecked("issued-claims")),task.id,&proof(&held)).await.is_err(),"corrupt provenance became an unjournaled compatibility path");
});

orch8_storage::backend_test!(async fn reconnect_uses_optional_device_identity_semantics(b) {
    use orch8_storage::{worker_claim::{WorkerReconnectRequest,recorded_reporter,RecordedReporter},testing::backends::RawPool};
    use orch8_types::continuity::RuntimeKind;
    let s=b.storage.as_ref();let task=fixture(s,false).await;
    let mut caps=capabilities();caps.kind=RuntimeKind::Browser;
    let principal=Principal::Runtime {tenant_id:TenantId::unchecked("issued-claims"),runtime_id:caps.runtime_id,runtime_kind:RuntimeKind::Browser,device_id:None};
    let request=WorkerClaimRequest::new(principal.clone(),"noop".into(),caps.runtime_id.to_string(),None,Some("queue".into()),Some(caps.clone()),1).unwrap();
    let held=s.claim_worker_tasks_with_authority(&request).await.unwrap().pop().unwrap();
    match &b.raw {
        RawPool::Sqlite(pool)=>{
            sqlx::raw_sql("DROP TRIGGER worker_issued_claim_update").execute(pool).await.unwrap();
            sqlx::query("UPDATE worker_issued_claims SET record=json_remove(record,'$.principal.device_id') WHERE task_id=?1").bind(task.id.to_string()).execute(pool).await.unwrap();
            sqlx::raw_sql(include_str!("../src/sqlite/worker_claims.sql")).execute(pool).await.unwrap();
        },
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{
            sqlx::raw_sql("DROP TRIGGER worker_issued_claim_update ON worker_issued_claims").execute(pool).await.unwrap();
            sqlx::query("UPDATE worker_issued_claims SET record=jsonb_set(record,'{principal}',(record->'principal')-'device_id') WHERE task_id=$1").bind(task.id).execute(pool).await.unwrap();
        },
    }
    assert_eq!(recorded_reporter(s,Some(&TenantId::unchecked("issued-claims")),task.id,&proof(&held),&principal).await.unwrap(),RecordedReporter::Matches);
    let reconnect=WorkerReconnectRequest::new(principal,None,caps.runtime_id.to_string(),vec!["noop".into()],1).unwrap();
    let tasks=s.held_worker_tasks_for_reporter(&reconnect).await.unwrap();
    assert_eq!(tasks.iter().map(|t|t.id).collect::<Vec<_>>(),vec![task.id],"a semantically unchanged optional device identity was lost after import");
});

orch8_storage::backend_test!(async fn reconnect_keeps_devices_distinct_for_the_same_runtime(b) {
    use orch8_storage::worker_claim::WorkerReconnectRequest;
    use orch8_types::continuity::RuntimeKind;
    let s=b.storage.as_ref();let mut caps=capabilities();caps.kind=RuntimeKind::Mobile;
    let mut claims=Vec::new();
    for device in ["phone-a","phone-b"] {
        let task=fixture(s,false).await;
        let principal=Principal::Runtime{tenant_id:TenantId::unchecked("issued-claims"),runtime_id:caps.runtime_id,runtime_kind:RuntimeKind::Mobile,device_id:Some(device.into())};
        let request=WorkerClaimRequest::new(principal.clone(),"noop".into(),caps.runtime_id.to_string(),None,Some("queue".into()),Some(caps.clone()),1).unwrap();
        assert_eq!(s.claim_worker_tasks_with_authority(&request).await.unwrap()[0].id,task.id);
        claims.push((principal,task.id));
    }
    for (principal,id) in claims {
        let reconnect=WorkerReconnectRequest::new(principal,None,caps.runtime_id.to_string(),vec!["noop".into()],1).unwrap();
        let tasks=s.held_worker_tasks_for_reporter(&reconnect).await.unwrap();
        assert_eq!(tasks.iter().map(|task|task.id).collect::<Vec<_>>(),vec![id],"another device inherited this runtime's reconnect payload");
    }
});

async fn set_claim_epoch(b: &orch8_storage::testing::backends::TestBackend, id: Uuid, epoch: i64) {
    use orch8_storage::testing::backends::RawPool;
    match &b.raw {
        RawPool::Sqlite(pool) => {
            sqlx::query("UPDATE worker_tasks SET claim_epoch=?2,created_at='2000-01-01T00:00:00Z' WHERE id=?1").bind(id.to_string()).bind(epoch).execute(pool).await.unwrap();
        }
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => {
            sqlx::query("UPDATE worker_tasks SET claim_epoch=$2,created_at='2000-01-01T00:00:00Z' WHERE id=$1").bind(id).bind(epoch).execute(pool).await.unwrap();
        }
    }
}

orch8_storage::backend_test!(async fn claim_limits_skip_exhausted_rows_before_limit_without_poisoning_healthy_work(b) {
    let s=b.storage.as_ref();
    for mode in 0..5 {
        let exhausted=fixture(s,false).await;set_claim_epoch(&b,exhausted.id,i64::MAX).await;
        let healthy=fixture(s,false).await;
        let held=s.claim_worker_tasks_with_authority(&request(mode)).await.unwrap();
        assert_eq!(held.iter().map(|t|t.id).collect::<Vec<_>>(),vec![healthy.id],"exhausted epoch poisoned selector {mode}");
        let unchanged=s.get_worker_task(exhausted.id).await.unwrap().unwrap();
        assert_eq!(unchanged.claim_epoch,i64::MAX as u64);assert_eq!(unchanged.state,WorkerTaskState::Pending);assert!(unchanged.worker_id.is_none());
        for id in [exhausted.id,healthy.id] {s.delete_worker_task(id).await.unwrap();}
    }
});

orch8_storage::backend_test!(async fn claim_limits_out_of_range_tokens_never_alias_the_last_valid_generation(b) {
    let s=b.storage.as_ref();let task=fixture(s,false).await;set_claim_epoch(&b,task.id,i64::MAX-1).await;
    let held=s.claim_worker_tasks_with_authority(&request(0)).await.unwrap().pop().unwrap();assert_eq!(held.claim_epoch,i64::MAX as u64);
    let bad=WorkerClaim::new("worker",u64::MAX);let legitimate=proof(&held);
    assert!(!matches!(s.heartbeat_worker_task(held.id,&bad).await,Ok(true)),"oversized epoch renewed another generation");
    assert!(!matches!(s.checkpoint_worker_task(held.id,&bad,0,&json!({"forged":true})).await,Ok(Some(_))),"oversized epoch checkpointed another generation");
    assert!(!matches!(s.fail_worker_task(held.id,&bad,"forged failure",false).await,Ok(true)),"oversized epoch failed another generation");
    assert!(!matches!(s.complete_worker_task(held.id,&bad,&json!({"forged":true})).await,Ok(true)),"oversized epoch completed another generation");
    let unchanged=s.get_worker_task(held.id).await.unwrap().unwrap();assert_eq!(unchanged.state,WorkerTaskState::Claimed);assert_eq!(unchanged.checkpoint_seq,0);assert!(unchanged.resume_checkpoint.is_none());
    assert!(s.heartbeat_worker_task(held.id,&legitimate).await.unwrap());
    assert_eq!(s.checkpoint_worker_task(held.id,&legitimate,0,&json!({"legitimate":true})).await.unwrap(),Some(1));
    assert!(s.fail_worker_task(held.id,&legitimate,"legitimate failure",false).await.unwrap());
});

orch8_storage::backend_test!(async fn claim_limits_task_counter_writes_reject_values_that_cannot_round_trip(b) {
    let s=b.storage.as_ref();
    for field in ["claim_epoch","checkpoint_seq","continuity_epoch"] {
        let mut task=fixture(s,false).await;s.delete_worker_task(task.id).await.unwrap();task.id=Uuid::now_v7();
        match field {"claim_epoch"=>task.claim_epoch=u64::MAX,"checkpoint_seq"=>task.checkpoint_seq=u64::MAX,_=>task.continuity_epoch=Some(u64::MAX)}
        assert!(s.create_worker_task(&task).await.is_err(),"{field} was silently clamped");
        assert!(s.get_worker_task(task.id).await.unwrap().is_none());
        task.claim_epoch=0;task.checkpoint_seq=0;task.continuity_epoch=None;s.create_worker_task(&task).await.unwrap();
        assert_eq!(s.get_worker_task(task.id).await.unwrap().unwrap().claim_epoch,0);
    }
});

orch8_storage::backend_test!(async fn claim_limits_attempt_evidence_never_clamps_an_out_of_range_generation(b) {
    use orch8_types::worker::{WorkerTaskAttemptEvent,WorkerAttemptEventKind};
    let s=b.storage.as_ref();let task=fixture(s,false).await;
    let mut event=WorkerTaskAttemptEvent{id:Uuid::now_v7(),task_id:task.id,claim_epoch:u64::MAX,worker_id:Some("worker".into()),event:WorkerAttemptEventKind::StaleMutationRejected,reason:None,created_at:Utc::now()};
    assert!(s.record_worker_task_attempt_event(&event).await.is_err());
    assert!(s.list_worker_task_attempt_events(task.id,10).await.unwrap().is_empty());
    event.claim_epoch=1;s.record_worker_task_attempt_event(&event).await.unwrap();
    let events=s.list_worker_task_attempt_events(task.id,10).await.unwrap();assert_eq!(events.len(),1);assert_eq!(events[0].id,event.id);assert_eq!(events[0].claim_epoch,1);
});

async fn exhausted_backlog(b: &orch8_storage::testing::backends::TestBackend, parent: &WorkerTask) {
    use orch8_storage::testing::backends::RawPool;
    let rows: Vec<_> = (0..4097)
        .map(|n| (Uuid::now_v7(), format!("exhausted-{n}")))
        .collect();
    for chunk in rows.chunks(64) {
        match &b.raw {
            RawPool::Sqlite(pool) => {
                let mut q = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                    "INSERT INTO worker_tasks(id,instance_id,block_id,handler_name,params,context,queue_name,requirements,created_at,claim_epoch,state,tenant_id) ",
                );
                q.push_values(chunk,|mut row,(id,block)|{row.push_bind(id.to_string()).push_bind(parent.instance_id.to_string()).push_bind(block).push_unseparated(", 'noop','{}','{}','queue','{}','2000-01-01T00:00:00Z',9223372036854775807,'pending','issued-claims'");});
                q.build().execute(pool).await.unwrap();
            }
            #[cfg(feature = "postgres")]
            RawPool::Postgres(pool) => {
                let mut q = sqlx::QueryBuilder::<sqlx::Postgres>::new(
                    "INSERT INTO worker_tasks(id,instance_id,block_id,handler_name,params,context,queue_name,requirements,created_at,claim_epoch,state,tenant_id) ",
                );
                q.push_values(chunk,|mut row,(id,block)|{row.push_bind(*id).push_bind(parent.instance_id.into_uuid()).push_bind(block).push_unseparated(", 'noop','{}','{}','queue','{}','2000-01-01T00:00:00Z',9223372036854775807,'pending','issued-claims'");});
                q.build().execute(pool).await.unwrap();
            }
        }
    }
}
orch8_storage::backend_test!(async fn claim_limits_exhausted_backlog_does_not_consume_the_capability_scan_window(b) {
    let s=b.storage.as_ref();let poison=fixture(s,false).await;set_claim_epoch(&b,poison.id,i64::MAX).await;
    exhausted_backlog(&b,&poison).await;
    let healthy=fixture(s,false).await;
    let held=s.claim_worker_tasks_with_authority(&request(4)).await.unwrap();
    assert_eq!(held.iter().map(|t|t.id).collect::<Vec<_>>(),vec![healthy.id]);
});

#[cfg(feature = "postgres")]
async fn one_connection_claimant(
    b: &orch8_storage::testing::backends::TestBackend,
) -> (
    std::sync::Arc<orch8_storage::postgres::PostgresStorage>,
    std::sync::Arc<dyn StorageBackend>,
) {
    use orch8_storage::testing::backends::{TEST_ENCRYPTION_KEY_HEX, postgres_url};
    use std::sync::Arc;
    let one = Arc::new(
        orch8_storage::postgres::PostgresStorage::with_min_connections(
            &postgres_url().unwrap(),
            1,
            1,
            b.pg_schema(),
        )
        .await
        .unwrap(),
    );
    let claimant: Arc<dyn StorageBackend> = if b.kind.is_encrypted() {
        Arc::new(orch8_storage::encrypting::EncryptingStorage::new(
            one.clone(),
            orch8_types::encryption::FieldEncryptor::from_hex_key(TEST_ENCRYPTION_KEY_HEX).unwrap(),
        ))
    } else {
        one.clone()
    };
    (one, claimant)
}
#[cfg(feature = "postgres")]
async fn claim_with_scan_barrier(
    one: &orch8_storage::postgres::PostgresStorage,
    claimant: &dyn StorageBackend,
    req: &WorkerClaimRequest,
    between: impl std::future::Future<Output = ()>,
) -> Vec<WorkerTask> {
    // Fair pool admission puts the test lease between unlocked scanning and
    // the claim's second connection acquisition for its transaction.
    let gate = one.pool().acquire().await.unwrap();
    let claimed = claimant.claim_worker_tasks_with_authority(req);
    tokio::pin!(claimed);
    assert!(futures::poll!(claimed.as_mut()).is_pending());
    let middle = one.pool().acquire();
    tokio::pin!(middle);
    assert!(futures::poll!(middle.as_mut()).is_pending());
    drop(gate);
    let lease=tokio::time::timeout(std::time::Duration::from_secs(10),async {
        tokio::select! {lease=&mut middle=>lease.unwrap(),result=&mut claimed=>panic!("claim escaped the scan/commit barrier: {result:?}")}
    }).await.expect("candidate scan did not release its connection");
    between.await;
    drop(lease);
    tokio::time::timeout(std::time::Duration::from_secs(10), &mut claimed)
        .await
        .unwrap()
        .unwrap()
}
#[cfg(feature = "postgres")]
fn matching_request(caps: RuntimeCapabilities) -> WorkerClaimRequest {
    WorkerClaimRequest::new(
        Principal::Root {},
        "noop".into(),
        "worker".into(),
        None,
        Some("queue".into()),
        Some(caps),
        1,
    )
    .unwrap()
}
orch8_storage::backend_test!(kinds=[postgres,encrypted_postgres], async fn matching_claim_rechecks_parent_terminal_state_after_candidate_scan(b) {
    let (one,claimant)=one_connection_claimant(&b).await;
    for terminal in [InstanceState::Completed,InstanceState::Failed,InstanceState::Cancelled] {
        let task=fixture(b.storage.as_ref(),false).await;let req=matching_request(capabilities());
        let result=claim_with_scan_barrier(&one,claimant.as_ref(),&req,async {
            b.storage.update_instance_state(task.instance_id,terminal,None).await.unwrap();
            assert_eq!(b.storage.get_worker_task(task.id).await.unwrap().unwrap().state,WorkerTaskState::Pending);
        }).await;
        assert!(result.is_empty(),"parent became {terminal:?} after scanning but task was claimed");
        let retained=b.storage.get_worker_task(task.id).await.unwrap().unwrap();assert_eq!(retained.state,WorkerTaskState::Pending);assert_eq!(retained.claim_epoch,0);assert!(retained.worker_id.is_none());
    }
});
orch8_storage::backend_test!(kinds=[postgres,encrypted_postgres], async fn matching_claim_rechecks_current_requirements_and_backfills_healthy_candidates(b) {
    let (one,claimant)=one_connection_claimant(&b).await;
    let changed=fixture(b.storage.as_ref(),false).await;let healthy=fixture(b.storage.as_ref(),false).await;
    let req=matching_request(capabilities());
    let result=claim_with_scan_barrier(&one,claimant.as_ref(),&req,async {
        sqlx::query("UPDATE worker_tasks SET requirements=$2 WHERE id=$1").bind(changed.id).bind(json!({"hardware":["not-advertised"]})).execute(b.pg_pool()).await.unwrap();
    }).await;
    assert_eq!(result.iter().map(|t|t.id).collect::<Vec<_>>(),vec![healthy.id]);
    assert_eq!(b.storage.get_worker_task(changed.id).await.unwrap().unwrap().state,WorkerTaskState::Pending);
});
orch8_storage::backend_test!(kinds=[postgres,encrypted_postgres], async fn matching_claim_rechecks_browser_secret_exclusion_before_ownership(b) {
    let (one,claimant)=one_connection_claimant(&b).await;
    let changed=fixture(b.storage.as_ref(),false).await;let healthy=fixture(b.storage.as_ref(),false).await;
    let mut caps=capabilities();caps.kind=orch8_types::continuity::RuntimeKind::Browser;let req=matching_request(caps);
    let result=claim_with_scan_barrier(&one,claimant.as_ref(),&req,async {
        sqlx::query("UPDATE worker_tasks SET carries_credentials=true WHERE id=$1").bind(changed.id).execute(b.pg_pool()).await.unwrap();
    }).await;
    assert_eq!(result.iter().map(|t|t.id).collect::<Vec<_>>(),vec![healthy.id]);
    let retained=b.storage.get_worker_task(changed.id).await.unwrap().unwrap();assert_eq!(retained.state,WorkerTaskState::Pending);assert!(retained.carries_credentials);
});
orch8_storage::backend_test!(kinds=[postgres,encrypted_postgres], async fn matching_claim_rechecks_capability_expiry_after_scanning(b) {
    let (one,claimant)=one_connection_claimant(&b).await;
    let task=fixture(b.storage.as_ref(),false).await;
    let mut caps=capabilities();let deadline=Utc::now()+chrono::Duration::seconds(8);caps.expires_at=deadline;let req=matching_request(caps);
    let result=claim_with_scan_barrier(&one,claimant.as_ref(),&req,async {
        assert!(Utc::now()<deadline,"expiry fixture elapsed before the scan barrier");
        while let Ok(left)=(deadline-Utc::now()).to_std() {tokio::time::sleep(left+std::time::Duration::from_millis(1)).await;}
    }).await;
    assert!(result.is_empty(),"an expired capability observation acquired new work");
    let retained=b.storage.get_worker_task(task.id).await.unwrap().unwrap();assert_eq!(retained.state,WorkerTaskState::Pending);assert_eq!(retained.claim_epoch,0);
});

orch8_storage::backend_test!(kinds=[sqlite,encrypted_sqlite], async fn claim_timestamps_return_the_exact_committed_lease_anchor(b) {
    let s=b.storage.as_ref();
    for mode in 0..5 {
        let task=fixture(s,false).await;
        let held=s.claim_worker_tasks_with_authority(&request(mode)).await.unwrap().pop().unwrap();
        let stored=s.get_worker_task(task.id).await.unwrap().unwrap();
        assert_eq!(held.claimed_at,stored.claimed_at,"selector {mode} returned a different claim anchor");
        assert_eq!(held.heartbeat_at,stored.heartbeat_at,"selector {mode} returned a different lease anchor");
        s.delete_worker_task(task.id).await.unwrap();
    }
});
orch8_storage::backend_test!(kinds=[sqlite,encrypted_sqlite], async fn claim_timestamps_are_sampled_after_waiting_for_the_database(b) {
    let s=b.storage.as_ref();assert_eq!(b.sqlite_pool().options().get_max_connections(),1);
    for mode in 0..5 {
        let task=fixture(s,false).await;let req=request(mode);
        let gate=b.sqlite_pool().acquire().await.unwrap();
        let claim=s.claim_worker_tasks_with_authority(&req);tokio::pin!(claim);
        assert!(futures::poll!(claim.as_mut()).is_pending());
        let released_at=Utc::now();drop(gate);
        let held=tokio::time::timeout(std::time::Duration::from_secs(10),&mut claim).await.unwrap().unwrap().pop().unwrap();
        let stored=s.get_worker_task(task.id).await.unwrap().unwrap();
        assert!(stored.heartbeat_at.unwrap()>=released_at,"selector {mode} backdated its lease across the database wait");
        assert_eq!(held.heartbeat_at,stored.heartbeat_at);s.delete_worker_task(task.id).await.unwrap();
    }
});

async fn named_queue_skips_quarantined_relay(
    backend: &orch8_storage::testing::backends::TestBackend,
    mode: u8,
) {
    let storage = backend.storage.as_ref();
    let blocked = fixture(storage, false).await;
    match &backend.raw {
        orch8_storage::testing::backends::RawPool::Sqlite(pool) => {
            sqlx::query(
                "INSERT INTO delegation_proxy_quarantines(instance_id,tenant_id) VALUES(?1,?2)",
            )
            .bind(blocked.instance_id.to_string())
            .bind("issued-claims")
            .execute(pool)
            .await
            .unwrap();
        }
        #[cfg(feature = "postgres")]
        orch8_storage::testing::backends::RawPool::Postgres(pool) => {
            sqlx::query(
                "INSERT INTO delegation_proxy_quarantines(instance_id,tenant_id) VALUES($1,$2)",
            )
            .bind(blocked.instance_id.into_uuid())
            .bind("issued-claims")
            .execute(pool)
            .await
            .unwrap();
        }
    }
    let control = fixture(storage, false).await;
    let claimed = storage
        .claim_worker_tasks_with_authority(&request(mode))
        .await
        .unwrap();
    assert_eq!(
        claimed.iter().map(|task| task.id).collect::<Vec<_>>(),
        vec![control.id],
        "a quarantined native relay must not acquire another worker lease"
    );
    let retained = storage.get_worker_task(blocked.id).await.unwrap().unwrap();
    assert_eq!(retained.state, WorkerTaskState::Pending);
    assert_eq!(retained.claim_epoch, 0);
}
orch8_storage::backend_test!(async fn named_queue_claim_skips_quarantined_native_relay(b) {
    named_queue_skips_quarantined_relay(&b,2).await;
});
orch8_storage::backend_test!(async fn named_queue_tenant_claim_skips_quarantined_native_relay(b) {
    named_queue_skips_quarantined_relay(&b,3).await;
});

#[path = "worker_issued_claims/reconnect_guards.rs"]
mod reconnect_guards;
