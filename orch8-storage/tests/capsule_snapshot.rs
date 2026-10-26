//! Capsule binding and all recovery rows publish together, or not at all.
use chrono::{Duration, Utc};
use orch8_storage::{
    restore::InstanceSnapshot,
    testing::{
        backends::RawPool,
        fixtures::{InstanceBuilder, SequenceBuilder},
    },
};
use orch8_types::{
    checkpoint::Checkpoint,
    continuity::{CapsuleId, RuntimeId},
    execution::{BlockType, ExecutionNode, NodeState},
    ids::{BlockId, ExecutionNodeId},
    instance::InstanceState,
    output::{BlockOutput, ExternalizedEntry},
};
use serde_json::json;
use uuid::Uuid;

orch8_storage::backend_test!(async fn capsule_snapshot_rolls_back_tree_payloads_and_binding_on_conflict(b) {
    let sequence=SequenceBuilder::new("capsule-snapshot").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let foreign=InstanceBuilder::for_sequence(&sequence).build();
    b.storage.create_instance(&foreign).await.unwrap();
    b.storage.save_externalized_state(foreign.id,"occupied",&json!({"foreign":"preserved"})).await.unwrap();
    let mut instance=InstanceBuilder::for_sequence(&sequence).build();
    instance.state=InstanceState::Paused;
    let at=chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    let checkpoint=Checkpoint{id:Uuid::now_v7(),instance_id:instance.id,checkpoint_data:json!({"checkpoint":1}),created_at:at};
    let root=ExecutionNode{id:ExecutionNodeId::new(),instance_id:instance.id,block_id:BlockId::new("each"),parent_id:None,block_type:BlockType::ForEach,branch_index:None,state:NodeState::Running,started_at:Some(at),completed_at:None};
    let child=ExecutionNode{id:ExecutionNodeId::new(),instance_id:instance.id,block_id:BlockId::new("work"),parent_id:Some(root.id),block_type:BlockType::Step,branch_index:Some(0),state:NodeState::Completed,started_at:Some(at),completed_at:Some(at)};
    let entry=ExternalizedEntry{instance_id:instance.id,ref_key:"new-payload".into(),payload:json!({"secret":"restored"}),created_at:at,expires_at:Some(at+Duration::days(1))};
    let mut conflict=entry.clone();conflict.ref_key="occupied".into();
    let output=BlockOutput{id:Uuid::now_v7(),instance_id:instance.id,block_id:BlockId::new("work"),output:json!("completed"),output_ref:None,output_size:11,attempt:0,created_at:at};
    // Child-before-parent input must still restore in topological order.
    let mut snapshot=InstanceSnapshot{instance:instance.clone(),nodes:vec![child,root],outputs:vec![output],payloads:vec![entry.clone(),conflict]};
    let capsule=CapsuleId::new();let runtime=RuntimeId::new();
    assert!(b.storage.import_capsule_snapshot(capsule,runtime,&snapshot,&checkpoint).await.is_err());
    assert!(b.storage.get_instance(instance.id).await.unwrap().is_none());
    assert!(b.storage.get_execution_tree(instance.id).await.unwrap().is_empty());
    assert!(b.storage.get_externalized_state(instance.id,"new-payload").await.unwrap().is_none());
    assert!(b.storage.get_latest_checkpoint(instance.id).await.unwrap().is_none());
    assert!(!b.storage.is_capsule_import_instance(&instance.tenant_id,capsule,runtime,instance.id).await.unwrap());
    assert_eq!(b.storage.get_externalized_state(foreign.id,"occupied").await.unwrap(),Some(json!({"foreign":"preserved"})));
    snapshot.payloads.pop();
    assert_eq!(b.storage.import_capsule_snapshot(capsule,runtime,&snapshot,&checkpoint).await.unwrap(),instance.id);
    let nodes=b.storage.get_execution_tree(instance.id).await.unwrap();
    assert_eq!(nodes.len(),2);
    assert!(nodes.iter().any(|node|node.parent_id==Some(snapshot.nodes[1].id)&&node.state==NodeState::Completed));
    assert_eq!(b.storage.get_externalized_state(instance.id,"new-payload").await.unwrap(),Some(entry.payload.clone()));
    assert_eq!(b.storage.list_externalized_entries(instance.id,None,100).await.unwrap(),vec![entry.clone()]);
    assert_eq!(b.storage.get_all_outputs(instance.id).await.unwrap()[0].output,json!("completed"));
    let stored:serde_json::Value=match &b.raw {
        RawPool::Sqlite(pool)=>{let text:String=sqlx::query_scalar("SELECT payload FROM externalized_state WHERE ref_key = ?").bind("new-payload").fetch_one(pool).await.unwrap();serde_json::from_str(&text).unwrap()},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>sqlx::query_scalar("SELECT payload FROM externalized_state WHERE ref_key = $1").bind("new-payload").fetch_one(pool).await.unwrap(),
    };
    assert_eq!(orch8_types::encryption::FieldEncryptor::is_encrypted(&stored),b.kind.is_encrypted());
    // Existing destination state wins over valid replayed source state.
    b.storage.save_externalized_state(instance.id,"new-payload",&json!({"destination":"newer"})).await.unwrap();
    assert_eq!(b.storage.import_capsule_snapshot(capsule,runtime,&snapshot,&checkpoint).await.unwrap(),instance.id);
    assert_eq!(b.storage.get_externalized_state(instance.id,"new-payload").await.unwrap(),Some(json!({"destination":"newer"})));
});

orch8_storage::backend_test!(async fn capsule_snapshot_rejects_invalid_graph_before_publication(b) {
    let sequence=SequenceBuilder::new("invalid-capsule-tree").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let instance=InstanceBuilder::for_sequence(&sequence).build();
    let checkpoint=Checkpoint{id:Uuid::now_v7(),instance_id:instance.id,checkpoint_data:json!({}),created_at:Utc::now()};
    let node=ExecutionNode{id:ExecutionNodeId::new(),instance_id:instance.id,block_id:BlockId::new("work"),parent_id:None,block_type:BlockType::Step,branch_index:None,state:NodeState::Completed,started_at:None,completed_at:None};
    let mut missing=node.clone();missing.parent_id=Some(ExecutionNodeId::new());
    let mut cyclic=node.clone();cyclic.parent_id=Some(cyclic.id);
    let mut foreign=node.clone();foreign.instance_id=orch8_types::ids::InstanceId::new();
    for nodes in [vec![missing],vec![cyclic],vec![foreign],vec![node.clone(),node]] {
        let snapshot=InstanceSnapshot{instance:instance.clone(),nodes,outputs:vec![],payloads:vec![]};
        assert!(b.storage.import_capsule_snapshot(CapsuleId::new(),RuntimeId::new(),&snapshot,&checkpoint).await.is_err());
        assert!(b.storage.get_instance(instance.id).await.unwrap().is_none());
        assert!(b.storage.get_execution_tree(instance.id).await.unwrap().is_empty());
    }
});
