//! A current view must preserve business semantics without materializing history.
use chrono::{Duration, Utc};
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
use orch8_types::{ids::BlockId, output::BlockOutput};
use serde_json::json;
use uuid::Uuid;

orch8_storage::backend_test!(kinds = [sqlite, postgres], async fn current_outputs_do_not_truncate_many_distinct_blocks(b) {
    let sequence = SequenceBuilder::new("many-current-blocks").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
    b.storage.create_instance(&instance).await.unwrap();
    let at = Utc::now();
    for n in 0..2049 {
        b.storage.save_block_output(&BlockOutput {
            id: Uuid::from_u128(n + 1), instance_id: instance.id,
            block_id: BlockId::new(format!("step-{n:04}")),
            output: json!(n.to_string()), output_ref: None, output_size: 10,
            attempt: 0, created_at: at,
        }).await.unwrap();
    }
    let projected = b.storage.get_latest_completed_outputs(instance.id).await.unwrap();
    assert_eq!(projected.len(), 2049);
    assert_eq!(projected.first().unwrap().block_id.as_str(), "step-0000");
    assert_eq!(projected.last().unwrap().block_id.as_str(), "step-2048");
});

orch8_storage::backend_test!(async fn current_outputs_match_history_projection(b) {
    let sequence = SequenceBuilder::new("current-outputs").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
    b.storage.create_instance(&instance).await.unwrap();
    assert!(b.storage.get_latest_completed_outputs(instance.id).await.unwrap().is_empty());
    let at = Utc::now();
    let make = |id: u128, block: &str, reference: Option<&str>, offset: i64| BlockOutput {
        id: Uuid::from_u128(id), instance_id: instance.id, block_id: BlockId::new(block),
        output: json!({"id": id.to_string(), "_sentinel": true}),
        output_ref: reference.map(str::to_owned), output_size: 50, attempt: 0,
        created_at: at + Duration::seconds(offset),
    };
    // Same-time insertion order disagrees with the UUID tiebreaker.
    let rows = [
        make(2,"work",None,0), make(4,"work",None,0), make(3,"work",None,0),
        make(100,"work",Some("__retry__"),1), make(101,"work",Some("__in_progress__"),2),
        make(5,"_interceptor:before",None,0), make(6,"_sla:late",None,0),
        make(7,"retry_only",Some("__retry__"),0), make(8,"started_only",Some("__in_progress__"),0),
        make(9,"failed",Some("__error__"),0), make(10,"_sentinel",None,0),
        make(11,"ainterceptor:business",None,0), make(12,"_interceptorX:business",None,0),
        make(13,"_slaX:business",None,0), make(14,"日本語",None,0),
        make(15,"",None,0), make(16,"external",Some("blob:result"),0),
    ];
    for row in &rows { b.storage.save_block_output(row).await.unwrap(); }
    for id in 200..456 { b.storage.save_block_output(&make(id,"loop",None,0)).await.unwrap(); }
    let other = InstanceBuilder::for_sequence(&sequence).due_now().build();
    b.storage.create_instance(&other).await.unwrap();
    let mut foreign = make(500,"work",None,3);
    foreign.instance_id = other.id;
    b.storage.save_block_output(&foreign).await.unwrap();
    if matches!(b.kind, orch8_storage::testing::backends::BackendKind::Postgres | orch8_storage::testing::backends::BackendKind::EncryptedPostgres) {
        // PostgreSQL's partitioned key also includes the instance identity.
        let mut collision = foreign.clone();
        collision.id = Uuid::from_u128(4);
        b.storage.save_block_output(&collision).await.unwrap();
    }

    let projected = b.storage.get_latest_completed_outputs(instance.id).await.unwrap();
    let expected: Vec<_> = [4,9,10,11,12,13,14,15,16,455].into_iter().map(Uuid::from_u128).collect();
    assert_eq!(projected.iter().map(|row| row.id).collect::<Vec<_>>(),expected);
    assert!(projected.iter().all(|row| row.output["_sentinel"] == true), "business fields are not sentinel metadata");
    assert_eq!(b.storage.get_all_outputs(instance.id).await.unwrap().len(), rows.len()+256, "history remains intact");
    assert_eq!(b.storage.get_latest_completed_outputs(other.id).await.unwrap()[0].id, foreign.id);
});
