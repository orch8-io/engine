//! SLA output and terminal state must commit under the same running claim.
use chrono::Utc;
use orch8_storage::{
    StorageBackend, backend_test,
    testing::{
        backends::RawPool,
        fixtures::{InstanceBuilder, SequenceBuilder},
    },
};
use orch8_types::{
    ids::BlockId,
    instance::{InstanceState, TaskInstance},
    output::BlockOutput,
};
use serde_json::json;

async fn running(store: &dyn StorageBackend) -> TaskInstance {
    let seq = SequenceBuilder::new("sla-claim")
        .step("work", "noop")
        .build();
    store.create_sequence(&seq).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&seq).due_now().build();
    store.create_instance(&instance).await.unwrap();
    store
        .claim_due_instances(Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap()
}
fn marker(id: orch8_types::ids::InstanceId) -> BlockOutput {
    BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id: id,
        block_id: BlockId::new("work"),
        output: json!({"_error":"sla_deadline_breached","private":"diagnostic"}),
        output_ref: None,
        output_size: 60,
        attempt: 0,
        created_at: Utc::now(),
    }
}

backend_test!(async fn claimed_sla_rejects_released_and_replaced_claims(b) {
    let first = running(b.storage.as_ref()).await;
    let old = first.claimed_at.unwrap(); let id = first.id;
    let next = Utc::now()+chrono::Duration::seconds(2);
    assert!(b.storage.conditional_update_claimed_instance_state(id,old,InstanceState::Running,InstanceState::Scheduled,Some(next)).await.unwrap());
    assert!(!b.storage.save_output_and_claimed_transition(&marker(id),InstanceState::Failed,None,old).await.unwrap());
    let current = b.storage.claim_due_instances(next,1,0).await.unwrap().pop().unwrap();
    assert_eq!(current.id,id); let token = current.claimed_at.unwrap(); assert_ne!(old,token);
    let mut context = current.context.clone(); context.data=json!({"owner":"replacement"});
    b.storage.update_instance_context(id,&context).await.unwrap();
    let before = b.storage.get_instance(id).await.unwrap().unwrap();
    assert!(!b.storage.save_output_and_claimed_transition(&marker(id),InstanceState::Failed,None,old).await.unwrap());
    let after=b.storage.get_instance(id).await.unwrap().unwrap();
    assert_eq!(after.state,before.state); assert_eq!(after.updated_at,before.updated_at);
    assert_eq!(after.context.data,context.data); assert_eq!(after.claimed_at,Some(token));
    assert!(b.storage.get_all_outputs(id).await.unwrap().is_empty());
    let output=marker(id);
    assert!(b.storage.save_output_and_claimed_transition(&output,InstanceState::Failed,None,token).await.unwrap());
    assert_eq!(b.storage.get_instance(id).await.unwrap().unwrap().state,InstanceState::Failed);
    let rows=b.storage.get_all_outputs(id).await.unwrap(); assert_eq!(rows.len(),1);
    assert_eq!(rows[0].id,output.id); assert_eq!(rows[0].output,output.output);
    assert!(!b.storage.save_output_and_claimed_transition(&output,InstanceState::Failed,None,token).await.unwrap());
});

backend_test!(async fn claimed_sla_has_one_atomic_winner(b) {
    let instance=running(b.storage.as_ref()).await; let token=instance.claimed_at.unwrap();
    let a=marker(instance.id); let c=marker(instance.id);
    let (a_won,c_won)=tokio::join!(
        b.storage.save_output_and_claimed_transition(&a,InstanceState::Failed,None,token),
        b.storage.save_output_and_claimed_transition(&c,InstanceState::Failed,None,token)
    );
    let a_won=a_won.unwrap(); let c_won=c_won.unwrap(); assert_ne!(a_won,c_won);
    let rows=b.storage.get_all_outputs(instance.id).await.unwrap(); assert_eq!(rows.len(),1);
    assert_eq!(rows[0].id,if a_won {a.id} else {c.id});
    assert_eq!(b.storage.get_instance(instance.id).await.unwrap().unwrap().state,InstanceState::Failed);
});

backend_test!(async fn claimed_sla_output_failure_rolls_back_the_terminal_transition(b) {
    let instance=running(b.storage.as_ref()).await; let token=instance.claimed_at.unwrap();
    let before=b.storage.get_instance(instance.id).await.unwrap().unwrap();
    match &b.raw {
        RawPool::Sqlite(pool)=> { sqlx::query("CREATE TRIGGER reject_claimed_breach BEFORE INSERT ON block_outputs BEGIN SELECT RAISE(ABORT,'simulated SLA output failure'); END").execute(pool).await.unwrap(); }
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=> {
            sqlx::query("CREATE FUNCTION reject_claimed_breach() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING MESSAGE='simulated SLA output failure', ERRCODE='55P03'; END $$").execute(pool).await.unwrap();
            sqlx::query("CREATE TRIGGER reject_claimed_breach BEFORE INSERT ON block_outputs FOR EACH ROW EXECUTE FUNCTION reject_claimed_breach()").execute(pool).await.unwrap();
        }
    }
    let output=marker(instance.id);
    let error=b.storage.save_output_and_claimed_transition(&output,InstanceState::Failed,None,token).await.unwrap_err();
    assert!(error.to_string().contains("simulated SLA output failure"));
    assert_eq!(error.is_transient(),b.kind.is_postgres());
    let after=b.storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(after.state,before.state); assert_eq!(after.updated_at,before.updated_at);
    assert_eq!(after.claimed_at,before.claimed_at);
    assert!(b.storage.get_all_outputs(instance.id).await.unwrap().is_empty());
    match &b.raw {
        RawPool::Sqlite(pool)=> { sqlx::query("DROP TRIGGER reject_claimed_breach").execute(pool).await.unwrap(); }
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=> { sqlx::query("DROP TRIGGER reject_claimed_breach ON block_outputs").execute(pool).await.unwrap(); }
    }
    assert!(b.storage.save_output_and_claimed_transition(&output,InstanceState::Failed,None,token).await.unwrap());
});
