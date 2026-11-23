//! Instance imports are atomic, owner-scoped, idempotent, and race-safe.
use chrono::{Duration, Timelike, Utc};
use orch8_storage::testing::backends::{RawPool, TestBackend};
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder, step};
use orch8_storage::{
    backend_test,
    externalizing::{ExternalizedEntry, marker},
    restore::{InstanceHistory, InstanceSnapshot},
};
use orch8_types::{
    execution::{BlockType, ExecutionNode, NodeState},
    ids::{BlockId, ExecutionNodeId},
    instance::InstanceState,
    output::BlockOutput,
    sequence::{BlockDefinition, ParallelDef, SequenceDefinition},
};
use serde_json::json;

fn sequence() -> SequenceDefinition {
    SequenceBuilder::new("restore")
        .block(BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("root"),
            branches: vec![vec![BlockDefinition::Step(Box::new(step("work", "noop")))]],
        })))
        .build()
}

fn snapshot(sequence: &SequenceDefinition) -> InstanceSnapshot {
    let mut instance = InstanceBuilder::for_sequence(sequence)
        .data(json!({"source": "original"}))
        .build();
    instance.state = InstanceState::Completed;
    let at = (Utc::now() - Duration::days(1))
        .with_nanosecond(123_456_000)
        .unwrap();
    let parent_id = ExecutionNodeId::from_uuid(uuid::Uuid::new_v4());
    let root = ExecutionNode {
        id: parent_id,
        instance_id: instance.id,
        block_id: BlockId::new("root"),
        parent_id: None,
        block_type: BlockType::Parallel,
        branch_index: None,
        state: NodeState::Completed,
        started_at: Some(at),
        completed_at: Some(at + Duration::seconds(1)),
    };
    let child = ExecutionNode {
        id: ExecutionNodeId::from_uuid(uuid::Uuid::new_v4()),
        instance_id: instance.id,
        block_id: BlockId::new("work"),
        parent_id: Some(parent_id),
        block_type: BlockType::Step,
        branch_index: Some(0),
        state: NodeState::Completed,
        started_at: Some(at),
        completed_at: Some(at),
    };
    let output_id = uuid::Uuid::new_v4();
    let reference = format!("{}:work:{output_id}", instance.id);
    let payload = json!({"private": "original-payload".repeat(200)});
    let output = BlockOutput {
        id: output_id,
        instance_id: instance.id,
        block_id: BlockId::new("work"),
        output: marker(&reference),
        output_ref: Some(reference.clone()),
        output_size: u32::try_from(serde_json::to_vec(&payload).unwrap().len()).unwrap(),
        attempt: 0,
        created_at: at,
    };
    let entry = ExternalizedEntry {
        instance_id: instance.id,
        ref_key: reference,
        payload,
        created_at: at,
        expires_at: Some(at + Duration::days(30)),
    };
    // UUID order and archive order need not put parents before their children.
    InstanceSnapshot {
        instance,
        nodes: vec![child, root],
        outputs: vec![output],
        payloads: vec![entry],
    }
}

async fn configure_output_fault(
    backend: &TestBackend,
    output: Option<uuid::Uuid>,
) -> Result<(), orch8_types::error::StorageError> {
    match (&backend.raw, output) {
        (RawPool::Sqlite(pool), Some(_)) => {
            sqlx::query("CREATE TRIGGER fail_output_import BEFORE INSERT ON block_outputs WHEN NEW.attempt=1 BEGIN SELECT RAISE(ABORT, 'orch8 conformance output insertion fault'); END")
                .execute(pool).await?;
        }
        (RawPool::Sqlite(pool), None) => {
            sqlx::query("DROP TRIGGER IF EXISTS fail_output_import")
                .execute(pool)
                .await?;
        }
        #[cfg(feature = "postgres")]
        (RawPool::Postgres(pool), Some(_)) => {
            sqlx::query("CREATE FUNCTION reject_output_import() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.attempt=1 THEN RAISE EXCEPTION 'orch8 conformance output insertion fault'; END IF; RETURN NEW; END $$")
                .execute(pool).await?;
            sqlx::query("CREATE TRIGGER fail_output_import BEFORE INSERT ON block_outputs FOR EACH ROW EXECUTE FUNCTION reject_output_import()")
                .execute(pool).await?;
        }
        #[cfg(feature = "postgres")]
        (RawPool::Postgres(pool), None) => {
            sqlx::query("DROP TRIGGER IF EXISTS fail_output_import ON block_outputs")
                .execute(pool)
                .await?;
            sqlx::query("DROP FUNCTION IF EXISTS reject_output_import()")
                .execute(pool)
                .await?;
        }
    }
    Ok(())
}

backend_test!(async fn concurrent_restore_publishes_one_complete_instance(b) {
    let sequence = sequence();
    b.storage.create_sequence(&sequence).await.unwrap();
    let mut snapshot = snapshot(&sequence);
    let (left, right) = tokio::join!(b.storage.restore_instance_snapshot(&snapshot), b.storage.restore_instance_snapshot(&snapshot));
    assert_eq!(u8::from(left.unwrap()) + u8::from(right.unwrap()), 1);
    let id = snapshot.instance.id;
    let stored = b.storage.get_instance(id).await.unwrap().unwrap();
    assert_eq!(stored.context.data, snapshot.instance.context.data);
    let nodes = b.storage.get_execution_tree(id).await.unwrap();
    assert_eq!(nodes.len(), snapshot.nodes.len());
    for expected in &snapshot.nodes {
        let actual = nodes.iter().find(|node| node.id == expected.id).unwrap();
        assert_eq!(actual.parent_id, expected.parent_id);
        assert_eq!(actual.state, expected.state);
        assert_eq!(actual.started_at, expected.started_at);
        assert_eq!(actual.completed_at, expected.completed_at);
    }
    let outputs = b.storage.get_all_outputs(id).await.unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].id, snapshot.outputs[0].id);
    assert_eq!(outputs[0].output, snapshot.outputs[0].output);
    assert_eq!(outputs[0].created_at, snapshot.outputs[0].created_at);
    assert_eq!(b.storage.list_externalized_entries(id, None, 10).await.unwrap(), snapshot.payloads);
    snapshot.instance.context.data = json!({"overwrite": "forbidden"});
    snapshot.payloads[0].payload = json!("do not replace existing data");
    assert!(!b.storage.restore_instance_snapshot(&snapshot).await.unwrap());
    assert_eq!(b.storage.get_instance(id).await.unwrap().unwrap().context.data, stored.context.data);
    assert_eq!(b.storage.get_externalized_state(id, &snapshot.payloads[0].ref_key).await.unwrap(), Some(json!({"private": "original-payload".repeat(200)})));
});

backend_test!(async fn payload_collision_rolls_back_the_entire_import(b) {
    let sequence = sequence();
    b.storage.create_sequence(&sequence).await.unwrap();
    let snapshot = snapshot(&sequence);
    let other = InstanceBuilder::for_sequence(&sequence).build();
    b.storage.create_instance(&other).await.unwrap();
    let mut original = snapshot.payloads[0].clone();
    original.instance_id = other.id;
    original.payload = json!({"owner": "existing"});
    b.storage.restore_externalized_entry(&original).await.unwrap();
    assert!(b.storage.restore_instance_snapshot(&snapshot).await.is_err());
    assert!(b.storage.get_instance(snapshot.instance.id).await.unwrap().is_none());
    assert!(b.storage.get_execution_tree(snapshot.instance.id).await.unwrap().is_empty());
    assert!(b.storage.get_all_outputs(snapshot.instance.id).await.unwrap().is_empty());
    assert!(b.storage.list_externalized_entries(snapshot.instance.id, None, 10).await.unwrap().is_empty());
    assert_eq!(b.storage.get_externalized_state(other.id, &original.ref_key).await.unwrap(), Some(original.payload));
});

backend_test!(async fn public_atomic_restore_conformance(b) {
    let report = orch8_storage::conformance::run_atomic_restore_conformance(
        b.storage.as_ref(), |output| configure_output_fault(&b, output),
    ).await.unwrap();
    assert_eq!(report.checks.len(), 4);
});

backend_test!(async fn read_only_terminal_import_fences_rescheduling_in_storage(b) {
    let sequence = sequence();
    b.storage.create_sequence(&sequence).await.unwrap();
    let mut archive = snapshot(&sequence);
    let migration = uuid::Uuid::now_v7();
    let history = InstanceHistory {
        read_only_migration: Some(migration),
        ..InstanceHistory::default()
    };
    archive.instance.state = InstanceState::Waiting;
    assert!(b.storage.restore_instance_snapshot_with_history(&archive, &history).await.is_err());
    assert!(b.storage.get_instance(archive.instance.id).await.unwrap().is_none());
    archive.instance.state = InstanceState::Failed;
    assert!(b.storage.restore_instance_snapshot_with_history(&archive, &history).await.unwrap());
    assert_eq!(b.storage.read_only_history_migration(archive.instance.id).await.unwrap(), Some(migration));
    assert!(b.storage.update_instance_state(archive.instance.id, InstanceState::Scheduled, Some(Utc::now())).await.is_err());
    assert_eq!(b.storage.get_instance(archive.instance.id).await.unwrap().unwrap().state, InstanceState::Failed);
    assert_eq!(b.storage.get_all_outputs(archive.instance.id).await.unwrap().len(), 1);
});

backend_test!(async fn migration_checkpoint_conflict_rolls_back_the_kv_write_and_owner_release(b) {
    use orch8_types::checkpoint::Checkpoint;
    use orch8_types::continuity::{ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState, RuntimeId};
    use orch8_types::migration::{InstanceKvEntry, MigratedInstanceState};

    let sequence = sequence();
    b.storage.create_sequence(&sequence).await.unwrap();
    let imported = InstanceBuilder::for_sequence(&sequence).state(InstanceState::Paused).build();
    let other = InstanceBuilder::for_sequence(&sequence).build();
    b.storage.create_instance(&imported).await.unwrap();
    b.storage.create_instance(&other).await.unwrap();
    let now = Utc::now().with_nanosecond(123_456_000).unwrap();
    let occupied = Checkpoint { id: uuid::Uuid::now_v7(), instance_id: other.id, checkpoint_data: json!({}), created_at: now };
    b.storage.save_checkpoint(&occupied).await.unwrap();
    let owner = ContinuityExecution {
        continuity_id: ContinuityId::new(), tenant_id: sequence.tenant_id,
        current_instance_id: imported.id, owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::from_u64(1), state: OwnershipState::Transferring, updated_at: now,
    };
    b.storage.create_continuity_execution(&owner).await.unwrap();
    let mut state = MigratedInstanceState {
        instance_kv: vec![InstanceKvEntry {
            instance_id: imported.id, key: "value".into(), value: json!(7), updated_at: now,
            expires_at: Some(now + Duration::hours(1)),
        }],
        checkpoints: vec![Checkpoint { instance_id: imported.id, ..occupied }],
    };
    assert!(b.storage.release_migrated_instance(&owner, &state).await.is_err());
    assert!(b.storage.list_instance_kv_page(imported.id, None, 10).await.unwrap().is_empty());
    let staged = b.storage.get_continuity_execution(&owner.tenant_id, owner.continuity_id).await.unwrap().unwrap();
    assert_eq!(staged.state, OwnershipState::Transferring);
    state.checkpoints[0].id = uuid::Uuid::now_v7();
    assert!(b.storage.release_migrated_instance(&owner, &state).await.unwrap());
    assert!(!b.storage.release_migrated_instance(&owner, &state).await.unwrap());
    assert_eq!(b.storage.list_instance_kv_page(imported.id, None, 10).await.unwrap(), state.instance_kv);
    assert_eq!(b.storage.get_latest_checkpoint(imported.id).await.unwrap().unwrap().id, state.checkpoints[0].id);
    assert!(b.storage.prune_checkpoints_at_epoch(&owner.tenant_id, imported.id, 0, 0).await.unwrap().is_none());
    assert_eq!(b.storage.prune_checkpoints_at_epoch(&owner.tenant_id, imported.id, 1, 0).await.unwrap(), Some(1));
});

backend_test!(async fn invalid_snapshot_rows_never_create_an_instance(b) {
    let sequence = sequence();
    b.storage.create_sequence(&sequence).await.unwrap();
    let source = snapshot(&sequence);
    let other = InstanceBuilder::for_sequence(&sequence).build();
    b.storage.create_instance(&other).await.unwrap();
    let mut invalid = Vec::new();
    let mut node_owner = source.clone(); node_owner.nodes[0].instance_id = other.id; invalid.push(node_owner);
    let mut output_owner = source.clone(); output_owner.outputs[0].instance_id = other.id; invalid.push(output_owner);
    let mut payload_owner = source.clone(); payload_owner.payloads[0].instance_id = other.id; invalid.push(payload_owner);
    let mut duplicate_node = source.clone(); duplicate_node.nodes.push(source.nodes[0].clone()); invalid.push(duplicate_node);
    let mut duplicate_output = source.clone(); duplicate_output.outputs.push(source.outputs[0].clone()); invalid.push(duplicate_output);
    let mut duplicate_ref = source.clone(); duplicate_ref.payloads.push(source.payloads[0].clone()); invalid.push(duplicate_ref);
    let mut missing_parent = source.clone(); missing_parent.nodes[0].parent_id = Some(ExecutionNodeId::new()); invalid.push(missing_parent);
    let mut cycle = source.clone(); cycle.nodes[1].parent_id = Some(cycle.nodes[0].id); invalid.push(cycle);
    for snapshot in invalid {
        assert!(b.storage.restore_instance_snapshot(&snapshot).await.is_err());
        assert!(b.storage.get_instance(source.instance.id).await.unwrap().is_none());
    }
});

backend_test!(async fn checkpoint_identity_collision_rolls_back_history_and_instance(b) {
    use orch8_types::checkpoint::Checkpoint;

    let sequence = sequence();
    b.storage.create_sequence(&sequence).await.unwrap();
    let imported = snapshot(&sequence);
    let owner = InstanceBuilder::for_sequence(&sequence).build();
    b.storage.create_instance(&owner).await.unwrap();
    let occupied = Checkpoint {
        id: uuid::Uuid::new_v4(),
        instance_id: owner.id,
        checkpoint_data: json!({"state": "existing"}),
        created_at: Utc::now(),
    };
    b.storage.save_checkpoint(&occupied).await.unwrap();
    let id = imported.instance.id;
    let checkpoints = [
        Checkpoint { id: uuid::Uuid::new_v4(), instance_id: id, ..occupied.clone() },
        Checkpoint { instance_id: id, ..occupied.clone() },
    ];
    assert!(b.storage.restore_instance_snapshot_with_history(&imported, &InstanceHistory { checkpoints: checkpoints.to_vec(), ..InstanceHistory::default() }).await.is_err());
    assert!(b.storage.get_instance(id).await.unwrap().is_none());
    assert!(b.storage.get_latest_checkpoint(id).await.unwrap().is_none());
    assert!(b.storage.get_execution_tree(id).await.unwrap().is_empty());
    assert_eq!(
        b.storage.get_latest_checkpoint(owner.id).await.unwrap().unwrap().checkpoint_data,
        occupied.checkpoint_data
    );

    let bad_owner = &checkpoints[1..];
    assert!((InstanceHistory { checkpoints: bad_owner.to_vec(), ..InstanceHistory::default() }).validate(&owner).is_err());
});

backend_test!(async fn signal_identity_collision_rolls_back_instance_and_inbox(b) {
    use orch8_types::signal::{Signal, SignalType};

    let sequence = sequence();
    b.storage.create_sequence(&sequence).await.unwrap();
    let imported = snapshot(&sequence);
    let owner = InstanceBuilder::for_sequence(&sequence).build();
    b.storage.create_instance(&owner).await.unwrap();
    let existing = Signal {
        id: uuid::Uuid::new_v4(),
        instance_id: owner.id,
        signal_type: SignalType::Cancel,
        payload: json!(null),
        delivered: true,
        created_at: Utc::now(),
        delivered_at: Some(Utc::now()),
    };
    b.storage.enqueue_signal(&existing).await.unwrap();
    let id = imported.instance.id;
    let first = Signal { id: uuid::Uuid::new_v4(), instance_id: id, ..existing.clone() };
    let collision = Signal { instance_id: id, ..existing.clone() };
    assert!(b.storage.restore_instance_snapshot_with_history(&imported, &InstanceHistory { signals: vec![first, collision], ..InstanceHistory::default() }).await.is_err());
    assert!(b.storage.get_instance(id).await.unwrap().is_none());
    assert!(b.storage.list_signal_history_page(id, None, 1).await.unwrap().is_empty());
    let history = b.storage.list_signal_history_page(owner.id, None, 1).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].id, existing.id);
    assert!(history[0].delivered);
});

backend_test!(async fn instance_kv_import_keeps_expiry_and_checks_ownership(b) {
    use orch8_storage::restore::InstanceKvEntry;

    let sequence = sequence();
    b.storage.create_sequence(&sequence).await.unwrap();
    let imported = snapshot(&sequence);
    let id = imported.instance.id;
    let now = Utc::now().with_nanosecond(0).unwrap();
    let expired = InstanceKvEntry {
        instance_id: id,
        key: String::new(),
        value: json!({"never": "visible"}),
        updated_at: now - Duration::days(1),
        expires_at: Some(now - Duration::hours(1)),
    };
    let present = InstanceKvEntry {
        key: "live".into(),
        value: json!(false),
        expires_at: Some(now + Duration::days(1)),
        ..expired.clone()
    };
    let mut foreign = present.clone();
    foreign.instance_id = orch8_types::ids::InstanceId::new();
    for invalid in [vec![expired.clone(), expired.clone()], vec![foreign]] {
        let history = InstanceHistory { state: invalid, ..InstanceHistory::default() };
        assert!(b.storage.restore_instance_snapshot_with_history(&imported, &history).await.is_err());
        assert!(b.storage.get_instance(id).await.unwrap().is_none());
    }

    let history = InstanceHistory { state: vec![expired.clone(), present.clone()], ..InstanceHistory::default() };
    assert!(b.storage.restore_instance_snapshot_with_history(&imported, &history).await.unwrap());
    assert_eq!(b.storage.get_instance_kv(id, "").await.unwrap(), None);
    assert_eq!(b.storage.get_instance_kv(id, "live").await.unwrap(), Some(json!(false)));
    assert_eq!(b.storage.get_all_instance_kv(id).await.unwrap().len(), 1);

    let first = b.storage.list_instance_kv_page(id, None, 1).await.unwrap();
    assert_eq!(first[0].key, expired.key);
    assert_eq!(first[0].expires_at, expired.expires_at);
    let second = b.storage.list_instance_kv_page(id, Some(&first[0].key), 1).await.unwrap();
    assert_eq!(second[0].key, present.key);
    assert_eq!(second[0].updated_at, present.updated_at);
    assert_eq!(second[0].expires_at, present.expires_at);
});

backend_test!(async fn effect_receipt_collision_rolls_back_owner_and_instance(b) {
    use orch8_storage::restore::EffectArchive;
    use orch8_types::continuity::{
        CapsuleEffectState, ContinuityExecution, ContinuityId, EffectId,
        EffectKind, EffectReceipt, EffectState, ExecutionEpoch, OwnershipState, RuntimeId,
    };

    let sequence = sequence();
    b.storage.create_sequence(&sequence).await.unwrap();
    let existing = InstanceBuilder::for_sequence(&sequence).build();
    b.storage.create_instance(&existing).await.unwrap();
    let imported = snapshot(&sequence);
    let now = Utc::now();
    let owner = ContinuityExecution {
        tenant_id: existing.tenant_id.clone(),
        continuity_id: ContinuityId::new(),
        owner_runtime_id: RuntimeId::new(),
        current_instance_id: existing.id,
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: now,
    };
    b.storage.create_continuity_execution(&owner).await.unwrap();
    let receipt = EffectReceipt {
        id: EffectId::new(),
        instance_id: existing.id,
        continuity_id: owner.continuity_id,
        tenant_id: owner.tenant_id.clone(),
        epoch: owner.epoch,
        block_id: BlockId::new("work"),
        attempt: 0,
        kind: EffectKind::Worker,
        state: EffectState::Unknown,
        idempotency_key: None,
        destination_fingerprint: "test".into(),
        request_sha256: "request".into(),
        provider_receipt_id: None,
        created_at: now,
        updated_at: now,
    };
    b.storage.create_effect_receipt(&receipt).await.unwrap();
    let id = imported.instance.id;
    let recovery_owner = ContinuityExecution { continuity_id: ContinuityId::new(), current_instance_id: id, ..owner.clone() };
    let recovered = EffectReceipt { instance_id: id, continuity_id: recovery_owner.continuity_id, ..receipt.clone() };
    let ledger = EffectArchive {
        owner: recovery_owner.clone(),
        sequence_version: sequence.version,
        state: CapsuleEffectState { receipts: vec![recovered], ..CapsuleEffectState::default() },
    };
    let history = InstanceHistory { effects: Some(ledger), ..InstanceHistory::default() };

    assert!(b.storage.restore_instance_snapshot_with_history(&imported, &history).await.is_err());
    assert!(b.storage.get_instance(id).await.unwrap().is_none());
    assert!(b.storage.get_continuity_execution(&owner.tenant_id, recovery_owner.continuity_id).await.unwrap().is_none());
    assert_eq!(b.storage.get_effect_receipt(&owner.tenant_id, receipt.id).await.unwrap(), Some(receipt));
});

backend_test!(async fn worker_restore_preserves_live_activation_and_renews_authority(b) {
    use orch8_storage::{
        restore::WorkerTaskArchive,
        worker_invocation::{WorkerInvocation, WorkerInvocationTarget},
    };
    use orch8_types::worker::{WorkerClaim, WorkerTask};

    let sequence = sequence();
    b.storage.create_sequence(&sequence).await.unwrap();
    let mut imported = snapshot(&sequence);
    imported.instance.state = InstanceState::Waiting;
    let run_id = uuid::Uuid::now_v7().to_string();
    imported.instance.context.runtime.run_id = Some(run_id.clone());
    let node = &mut imported.nodes[0];
    node.state = NodeState::Waiting;
    node.completed_at = None;
    let target = WorkerInvocationTarget::Tree {
        node_id: node.id,
        activation_id: uuid::Uuid::new_v4(),
    };
    let block_id = node.block_id.clone();
    let task_id = uuid::Uuid::now_v7();
    let checkpoint = json!({"stage": 3});
    let task: WorkerTask = serde_json::from_value(json!({
        "id": task_id, "instance_id": imported.instance.id, "block_id": block_id,
        "handler_name": "noop", "attempt": 2, "params": {}, "context": imported.instance.context,
        "state": "claimed", "worker_id": "worker", "claim_epoch": 3, "checkpoint_seq": 4,
        "resume_checkpoint": checkpoint, "created_at": Utc::now(),
    })).unwrap();
    let binding = WorkerInvocation {
        task_id,
        tenant_id: imported.instance.tenant_id.clone(),
        instance_id: imported.instance.id,
        sequence_id: sequence.id,
        block_id,
        handler_name: task.handler_name.clone(),
        attempt: task.attempt,
        continuity: None,
        continuity_epoch: None,
        effect_id: None,
        run_id: Some(run_id),
        dry_run: false,
        target,
    };
    let mut history = InstanceHistory {
        worker_tasks: vec![WorkerTaskArchive {
            task,
            awaiting_dispatch: false,
            invocation: Some(binding.clone()),
            admission: None,
        }],
        ..InstanceHistory::default()
    };
    history.worker_tasks[0].invocation.as_mut().unwrap().task_id = uuid::Uuid::new_v4();
    assert!(b.storage.restore_instance_snapshot_with_history(&imported, &history).await.is_err());
    assert!(b.storage.get_instance(imported.instance.id).await.unwrap().is_none());
    history.worker_tasks[0].invocation = Some(binding.clone());

    assert!(b.storage.restore_instance_snapshot_with_history(&imported, &history).await.unwrap());
    assert!(!b.storage.restore_instance_snapshot_with_history(&imported, &history).await.unwrap());
    let tasks = b.storage.list_worker_task_archive_page(&imported.instance, None, 1).await.unwrap();
    let new_task = &tasks[0];
    assert_ne!(new_task.task.id, task_id);
    assert_eq!(new_task.invocation.as_ref().unwrap().target, binding.target);
    assert_eq!(new_task.task.resume_checkpoint.as_ref(), Some(&checkpoint));
    assert_eq!(new_task.task.checkpoint_seq, 4);
    let fresh = b.storage.claim_worker_tasks("noop", "worker", 1).await.unwrap().pop().unwrap();
    assert_eq!(fresh.id, new_task.task.id);
    assert_eq!(fresh.claim_epoch, 4);
    let old = WorkerClaim::new("worker", 3);
    assert!(!b.storage.complete_worker_task(task_id, &old, &json!({})).await.unwrap());
    assert!(!b.storage.complete_worker_task(fresh.id, &old, &json!({})).await.unwrap());
    assert!(b.storage.complete_worker_task(fresh.id, &WorkerClaim::new("worker", 4), &json!({})).await.unwrap());
});
