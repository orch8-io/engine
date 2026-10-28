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
    let seq = SequenceBuilder::new("issued-claims")
        .step("work", "noop")
        .build();
    s.create_sequence(&seq).await.unwrap();
    let mut parent = InstanceBuilder::for_sequence(&seq).due_now().build();
    parent.context.runtime.run_id = Some(Uuid::now_v7().to_string());
    parent.context.runtime.current_step = Some(BlockId::new("work"));
    parent.context.runtime.current_step_started_at = Some(Utc::now());
    parent.context.data = json!({"private":"ISSUED_CLAIM_CONTEXT_CANARY"});
    s.create_instance(&parent).await.unwrap();
    let task:WorkerTask=serde_json::from_value(json!({"id":Uuid::now_v7(),"instance_id":parent.id,"block_id":"work","handler_name":"noop","queue_name":queue,"params":{"private":"ISSUED_CLAIM_INPUT_CANARY"},"context":parent.context,"attempt":0,"state":"pending","created_at":Utc::now()})).unwrap();
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
