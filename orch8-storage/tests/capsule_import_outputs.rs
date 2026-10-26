//! Capsule quarantine publication includes all recovery evidence atomically.
use chrono::Utc;
use orch8_storage::testing::{
    backends::RawPool,
    fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    checkpoint::Checkpoint,
    continuity::{CapsuleId, RuntimeId},
    ids::BlockId,
    instance::InstanceState,
    output::BlockOutput,
};
use serde_json::json;
use uuid::Uuid;

orch8_storage::backend_test!(async fn capsule_output_import_is_atomic_and_idempotent(b) {
    let sequence = SequenceBuilder::new("capsule-output-import").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let mut instance = InstanceBuilder::for_sequence(&sequence).build();
    instance.state = InstanceState::Paused;
    let checkpoint = Checkpoint { id: Uuid::now_v7(), instance_id: instance.id, checkpoint_data: json!({"resume":true}), created_at: Utc::now() };
    let make = |block: &str| BlockOutput { id: Uuid::now_v7(), instance_id: instance.id, block_id: BlockId::new(block), output: json!({"receipt":"completed"}), output_ref: None, output_size: 23, attempt: 1, created_at: checkpoint.created_at };
    let outputs = [make("done"),make("reject-import")];
    let capsule = CapsuleId::new();
    let runtime = RuntimeId::new();
    // Fail the second output insert, after the first output and checkpoint were
    // written. The import binding must roll back too, allowing a clean retry.
    match &b.raw {
        RawPool::Sqlite(pool) => {
            sqlx::query("CREATE TRIGGER reject_capsule_output BEFORE INSERT ON block_outputs WHEN NEW.block_id = 'reject-import' BEGIN SELECT RAISE(ABORT, 'injected capsule failure'); END").execute(pool).await.unwrap();
        }
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => {
            sqlx::query("CREATE FUNCTION reject_capsule_output() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.block_id = 'reject-import' THEN RAISE EXCEPTION 'injected capsule failure'; END IF; RETURN NEW; END $$").execute(pool).await.unwrap();
            sqlx::query("CREATE TRIGGER reject_capsule_output BEFORE INSERT ON block_outputs FOR EACH ROW EXECUTE FUNCTION reject_capsule_output()").execute(pool).await.unwrap();
        }
    }
    assert!(b.storage.import_capsule_instance_with_outputs(capsule, runtime, &instance, &checkpoint, &outputs).await.is_err());
    assert!(b.storage.get_instance(instance.id).await.unwrap().is_none());
    assert!(b.storage.get_latest_checkpoint(instance.id).await.unwrap().is_none());
    assert!(b.storage.get_all_outputs(instance.id).await.unwrap().is_empty());
    assert!(!b.storage.is_capsule_import_instance(&instance.tenant_id, capsule, runtime, instance.id).await.unwrap());
    match &b.raw {
        RawPool::Sqlite(pool) => { sqlx::query("DROP TRIGGER reject_capsule_output").execute(pool).await.unwrap(); }
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => { sqlx::query("DROP TRIGGER reject_capsule_output ON block_outputs").execute(pool).await.unwrap(); }
    }
    assert_eq!(b.storage.import_capsule_instance_with_outputs(capsule, runtime, &instance, &checkpoint, &outputs).await.unwrap(), instance.id);
    let restored = b.storage.get_all_outputs(instance.id).await.unwrap();
    assert_eq!(restored.len(),2);
    let stored_output: serde_json::Value = match &b.raw {
        RawPool::Sqlite(pool) => {
            let text: String = sqlx::query_scalar("SELECT output FROM block_outputs WHERE id = ?").bind(outputs[0].id.to_string()).fetch_one(pool).await.unwrap();
            serde_json::from_str(&text).unwrap()
        }
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => sqlx::query_scalar("SELECT output FROM block_outputs WHERE instance_id = $1 AND id = $2").bind(instance.id.into_uuid()).bind(outputs[0].id).fetch_one(pool).await.unwrap(),
    };
    assert_eq!(orch8_types::encryption::FieldEncryptor::is_encrypted(&stored_output), b.kind.is_encrypted(), "import must preserve encryption at rest");

    assert!(restored.iter().all(|row| row.output == outputs[0].output));
    assert_eq!(b.storage.get_latest_checkpoint(instance.id).await.unwrap().unwrap().checkpoint_data, checkpoint.checkpoint_data);
    // A retry after destination work must not overwrite or append evidence.
    let extra = make("destination-work");
    b.storage.save_block_output(&extra).await.unwrap();
    let mut retry = instance.clone();
    retry.id = orch8_types::ids::InstanceId::new();
    let mut retry_checkpoint = checkpoint.clone();
    retry_checkpoint.instance_id = retry.id;
    assert_eq!(b.storage.import_capsule_instance_with_outputs(capsule, runtime, &retry, &retry_checkpoint, &[]).await.unwrap(), instance.id);
    assert!(b.storage.get_instance(retry.id).await.unwrap().is_none());
    assert_eq!(b.storage.get_all_outputs(instance.id).await.unwrap().len(),3);
});

orch8_storage::backend_test!(async fn capsule_output_import_rejects_foreign_and_duplicate_evidence(b) {
    let sequence = SequenceBuilder::new("capsule-invalid-output").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).build();
    let checkpoint = Checkpoint { id: Uuid::now_v7(), instance_id: instance.id, checkpoint_data: json!({}), created_at: Utc::now() };
    let output = BlockOutput { id: Uuid::now_v7(), instance_id: instance.id, block_id: BlockId::new("done"), output: json!(1), output_ref: None, output_size: 1, attempt: 0, created_at: Utc::now() };
    let mut foreign = output.clone();
    foreign.instance_id = orch8_types::ids::InstanceId::new();
    for outputs in [vec![foreign],vec![output.clone(),output]] {
        assert!(b.storage.import_capsule_instance_with_outputs(CapsuleId::new(), RuntimeId::new(), &instance, &checkpoint, &outputs).await.is_err());
        assert!(b.storage.get_instance(instance.id).await.unwrap().is_none());
    }
});
