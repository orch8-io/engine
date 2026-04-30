//! Criterion benchmarks for `StorageBackend` hot paths.

use chrono::{Duration, Utc};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use serde_json::json;
use uuid::Uuid;

use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::StorageBackend;
use orch8_types::context::ExecutionContext;
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::{BlockId, ExecutionNodeId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef};
use orch8_types::signal::{Signal, SignalType};
use orch8_types::worker::{WorkerTask, WorkerTaskState};

fn make_sequence() -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("bench".into()),
        namespace: Namespace("default".into()),
        name: "bench_seq".into(),
        version: 1,
        deprecated: false,
        blocks: vec![BlockDefinition::Step(Box::new(StepDef {
            id: BlockId("s1".into()),
            handler: "noop".into(),
            params: json!({}),
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
            cache_key: None,
        }))],
        interceptors: None,
        created_at: Utc::now(),
    }
}

fn make_instance(seq_id: SequenceId) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq_id,
        tenant_id: TenantId("bench".into()),
        namespace: Namespace("default".into()),
        state: InstanceState::Scheduled,
        next_fire_at: Some(now - Duration::seconds(10)),
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({}),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        created_at: now,
        updated_at: now,
    }
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn bench_instance_creation(c: &mut Criterion) {
    let runtime = rt();

    c.bench_function("create_instance_single", |b| {
        b.iter_batched(
            || {
                let s = runtime.block_on(SqliteStorage::in_memory()).unwrap();
                let seq = make_sequence();
                runtime.block_on(s.create_sequence(&seq)).unwrap();
                (s, seq.id)
            },
            |(s, seq_id)| {
                runtime.block_on(async {
                    let inst = make_instance(seq_id);
                    s.create_instance(&inst).await.unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("create_instances_batch_100", |b| {
        b.iter_batched(
            || {
                let s = runtime.block_on(SqliteStorage::in_memory()).unwrap();
                let seq = make_sequence();
                runtime.block_on(s.create_sequence(&seq)).unwrap();
                (s, seq.id)
            },
            |(s, seq_id)| {
                runtime.block_on(async {
                    let batch: Vec<TaskInstance> =
                        (0..100).map(|_| make_instance(seq_id)).collect();
                    s.create_instances_batch(&batch).await.unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_claim_due(c: &mut Criterion) {
    let runtime = rt();

    c.bench_function("claim_due_50_from_500", |b| {
        b.iter_batched(
            || {
                let s = runtime.block_on(SqliteStorage::in_memory()).unwrap();
                let seq = make_sequence();
                runtime.block_on(s.create_sequence(&seq)).unwrap();
                let batch: Vec<TaskInstance> = (0..500).map(|_| make_instance(seq.id)).collect();
                runtime.block_on(s.create_instances_batch(&batch)).unwrap();
                s
            },
            |s| {
                runtime.block_on(async {
                    let now = Utc::now();
                    let claimed = s.claim_due_instances(now, 50, 0).await.unwrap();
                    assert_eq!(claimed.len(), 50);
                });
            },
            BatchSize::LargeInput,
        );
    });
}

fn bench_block_outputs(c: &mut Criterion) {
    let runtime = rt();

    c.bench_function("save_and_get_output", |b| {
        b.iter_batched(
            || runtime.block_on(SqliteStorage::in_memory()).unwrap(),
            |s| {
                runtime.block_on(async {
                    let inst_id = InstanceId::new();
                    let out = BlockOutput {
                        id: Uuid::now_v7(),
                        instance_id: inst_id,
                        block_id: BlockId("step_1".into()),
                        output: json!({"result": "ok", "data": [1,2,3]}),
                        output_ref: None,
                        output_size: 30,
                        attempt: 1,
                        created_at: Utc::now(),
                    };
                    s.save_block_output(&out).await.unwrap();
                    s.get_block_output(inst_id, &BlockId("step_1".into()))
                        .await
                        .unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_signals(c: &mut Criterion) {
    let runtime = rt();

    c.bench_function("enqueue_and_fetch_100_signals", |b| {
        b.iter_batched(
            || runtime.block_on(SqliteStorage::in_memory()).unwrap(),
            |s| {
                runtime.block_on(async {
                    let inst_id = InstanceId::new();
                    for _ in 0..100 {
                        let sig = Signal {
                            id: Uuid::now_v7(),
                            instance_id: inst_id,
                            signal_type: SignalType::Custom("tick".into()),
                            payload: json!(null),
                            delivered: false,
                            created_at: Utc::now(),
                            delivered_at: None,
                        };
                        s.enqueue_signal(&sig).await.unwrap();
                    }
                    let pending = s.get_pending_signals(inst_id).await.unwrap();
                    assert_eq!(pending.len(), 100);
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_execution_tree(c: &mut Criterion) {
    let runtime = rt();

    c.bench_function("create_and_fetch_tree_50_nodes", |b| {
        b.iter_batched(
            || runtime.block_on(SqliteStorage::in_memory()).unwrap(),
            |s| {
                runtime.block_on(async {
                    let inst_id = InstanceId::new();
                    let root_id = ExecutionNodeId::new();
                    let mut nodes = vec![ExecutionNode {
                        id: root_id,
                        instance_id: inst_id,
                        block_id: BlockId("root".into()),
                        parent_id: None,
                        block_type: BlockType::Parallel,
                        branch_index: None,
                        state: NodeState::Running,
                        started_at: Some(Utc::now()),
                        completed_at: None,
                    }];
                    for i in 0..49 {
                        nodes.push(ExecutionNode {
                            id: ExecutionNodeId::new(),
                            instance_id: inst_id,
                            block_id: BlockId(format!("s{i}")),
                            parent_id: Some(root_id),
                            block_type: BlockType::Step,
                            branch_index: Some(i),
                            state: NodeState::Pending,
                            started_at: None,
                            completed_at: None,
                        });
                    }
                    s.create_execution_nodes_batch(&nodes).await.unwrap();
                    let tree = s.get_execution_tree(inst_id).await.unwrap();
                    assert_eq!(tree.len(), 50);
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_worker_tasks(c: &mut Criterion) {
    let runtime = rt();

    c.bench_function("create_and_claim_50_worker_tasks", |b| {
        b.iter_batched(
            || runtime.block_on(SqliteStorage::in_memory()).unwrap(),
            |s| {
                runtime.block_on(async {
                    let inst_id = InstanceId::new();
                    for i in 0..50 {
                        let task = WorkerTask {
                            id: Uuid::now_v7(),
                            instance_id: inst_id,
                            block_id: BlockId(format!("step_{i}")),
                            handler_name: "bench_handler".into(),
                            queue_name: None,
                            params: json!({"i": i}),
                            context: json!({}),
                            attempt: 1,
                            timeout_ms: None,
                            state: WorkerTaskState::Pending,
                            worker_id: None,
                            claimed_at: None,
                            heartbeat_at: None,
                            completed_at: None,
                            output: None,
                            error_message: None,
                            error_retryable: None,
                            created_at: Utc::now(),
                        };
                        s.create_worker_task(&task).await.unwrap();
                    }
                    let claimed = s
                        .claim_worker_tasks("bench_handler", "w1", 50)
                        .await
                        .unwrap();
                    assert_eq!(claimed.len(), 50);
                });
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    bench_instance_creation,
    bench_claim_due,
    bench_block_outputs,
    bench_signals,
    bench_execution_tree,
    bench_worker_tasks,
);
criterion_main!(benches);
