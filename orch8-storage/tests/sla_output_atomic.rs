//! A terminal deadline state and its diagnostic output must commit together.
use chrono::Utc;
use orch8_storage::backend_test;
use orch8_storage::testing::backends::RawPool;
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
use orch8_types::ids::BlockId;
use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;

fn marker(id: orch8_types::ids::InstanceId) -> BlockOutput {
    BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id: id,
        block_id: BlockId::new("s"),
        output: serde_json::json!({"_error": "sla_deadline_breached"}),
        output_ref: None,
        output_size: 0,
        attempt: 0,
        created_at: Utc::now(),
    }
}

backend_test!(async fn conditional_output_has_exactly_one_winner(b) {
    let seq = SequenceBuilder::new("tenant").step("s", "noop").build();
    b.storage.create_sequence(&seq).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&seq).state(InstanceState::Waiting).build();
    b.storage.create_instance(&instance).await.unwrap();
    let first = marker(instance.id);
    let second = marker(instance.id);
    let (a, c) = tokio::join!(
        b.storage.save_output_and_conditional_transition(&first, InstanceState::Waiting, InstanceState::Failed, None),
        b.storage.save_output_and_conditional_transition(&second, InstanceState::Waiting, InstanceState::Failed, None)
    );
    let a = a.unwrap(); let c = c.unwrap();
    assert_ne!(a, c, "exactly one competing deadline sweep must win");
    assert_eq!(b.storage.get_instance(instance.id).await.unwrap().unwrap().state, InstanceState::Failed);
    let outputs = b.storage.get_all_outputs(instance.id).await.unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].id, if a { first.id } else { second.id });
    assert_eq!(outputs[0].output, first.output, "encrypted wrapper must round-trip the marker");
    // Even reusing the winning output ID must return false without attempting
    // another INSERT (and without a unique constraint error).
    assert!(!b.storage.save_output_and_conditional_transition(if a { &first } else { &second }, InstanceState::Waiting, InstanceState::Failed, None).await.unwrap());
});

backend_test!(async fn conditional_output_rejects_changed_active_state(b) {
    let seq = SequenceBuilder::new("tenant").step("s", "noop").build();
    b.storage.create_sequence(&seq).await.unwrap();
    for state in [InstanceState::Running, InstanceState::Cancelled, InstanceState::Completed, InstanceState::Paused] {
        let instance = InstanceBuilder::for_sequence(&seq).state(state).build();
        b.storage.create_instance(&instance).await.unwrap();
        assert!(!b.storage.save_output_and_conditional_transition(&marker(instance.id), InstanceState::Waiting, InstanceState::Failed, None).await.unwrap());
        assert_eq!(b.storage.get_instance(instance.id).await.unwrap().unwrap().state, state);
        assert!(b.storage.get_all_outputs(instance.id).await.unwrap().is_empty());
    }
});

backend_test!(async fn conditional_output_failure_rolls_back_state(b) {
    let seq = SequenceBuilder::new("tenant").step("s", "noop").build();
    b.storage.create_sequence(&seq).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&seq).state(InstanceState::Waiting).build();
    b.storage.create_instance(&instance).await.unwrap();
    // Reject the INSERT after the guarded UPDATE has executed. Exercise real
    // transaction rollback rather than a decorator failing before either SQL.
    match &b.raw {
        RawPool::Sqlite(pool) => { sqlx::query("CREATE TRIGGER reject_breach BEFORE INSERT ON block_outputs BEGIN SELECT RAISE(ABORT, 'injected output failure'); END").execute(pool).await.unwrap(); }
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => { sqlx::query("ALTER TABLE block_outputs ADD CONSTRAINT reject_breach CHECK (block_id <> 's')").execute(pool).await.unwrap(); }
    }
    let output = marker(instance.id);
    assert!(b.storage.save_output_and_conditional_transition(&output, InstanceState::Waiting, InstanceState::Failed, None).await.is_err());
    assert_eq!(b.storage.get_instance(instance.id).await.unwrap().unwrap().state, InstanceState::Waiting);
    assert!(b.storage.get_all_outputs(instance.id).await.unwrap().is_empty());
    match &b.raw {
        RawPool::Sqlite(pool) => { sqlx::query("DROP TRIGGER reject_breach").execute(pool).await.unwrap(); }
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => { sqlx::query("ALTER TABLE block_outputs DROP CONSTRAINT reject_breach").execute(pool).await.unwrap(); }
    }
    assert!(b.storage.save_output_and_conditional_transition(&output, InstanceState::Waiting, InstanceState::Failed, None).await.unwrap());
});
