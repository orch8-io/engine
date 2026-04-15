//! Performance benchmark for the Orch8 engine.
//!
//! Measures:
//! 1. Batch INSERT throughput (100K instances)
//! 2. `claim_due_instances` throughput under load
//! 3. End-to-end throughput (schedule → noop completion)
//!
//! Run: cargo run --bin orch8-bench
//! Requires: Postgres running (docker compose up -d)

#![allow(clippy::cast_precision_loss)]

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use tokio_util::sync::CancellationToken;

use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::Engine;
use orch8_storage::postgres::PostgresStorage;
use orch8_storage::StorageBackend;
use orch8_types::config::SchedulerConfig;
use orch8_types::context::ExecutionContext;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef};

const DB_URL: &str = "postgres://orch8:orch8@localhost:5434/orch8";

fn make_sequence(tenant: &str, num_steps: usize) -> SequenceDefinition {
    let blocks = (0..num_steps)
        .map(|i| {
            BlockDefinition::Step(StepDef {
                id: BlockId(format!("s{}", i + 1)),
                handler: "noop".into(),
                params: serde_json::Value::Null,
                delay: None,
                retry: None,
                timeout: None,
                rate_limit_key: None,
                send_window: None,
                context_access: None,
                cancellable: true,
                wait_for_input: None,
            })
        })
        .collect();
    SequenceDefinition {
        id: SequenceId(uuid::Uuid::new_v4()),
        tenant_id: TenantId(tenant.into()),
        namespace: Namespace("default".into()),
        name: format!("bench-seq-{}", uuid::Uuid::new_v4()),
        version: 1,
        blocks,
        created_at: Utc::now(),
    }
}

fn make_instances(count: usize, seq_id: SequenceId, tenant: &str) -> Vec<TaskInstance> {
    let now = Utc::now();
    (0..count)
        .map(|_| TaskInstance {
            id: InstanceId::new(),
            sequence_id: seq_id,
            tenant_id: TenantId(tenant.into()),
            namespace: Namespace("default".into()),
            state: InstanceState::Scheduled,
            next_fire_at: Some(now),
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: serde_json::json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            created_at: now,
            updated_at: now,
        })
        .collect()
}

async fn setup_storage() -> Arc<PostgresStorage> {
    let storage = PostgresStorage::new(DB_URL, 64)
        .await
        .expect("Failed to connect to Postgres — is docker compose running?");
    storage
        .run_migrations()
        .await
        .expect("Failed to run migrations");
    Arc::new(storage)
}

/// Clean up bench instances to avoid polluting the DB.
async fn cleanup(storage: &dyn StorageBackend, tenant: &str) {
    use orch8_types::filter::InstanceFilter;
    let filter = InstanceFilter {
        tenant_id: Some(TenantId(tenant.into())),
        ..Default::default()
    };
    // Bulk cancel then we just leave them — DB is a test DB
    let _ = storage
        .bulk_update_state(&filter, InstanceState::Cancelled)
        .await;
}

#[tokio::main]
async fn main() {
    let storage = setup_storage().await;

    println!("=== Orch8 Performance Benchmark ===\n");

    bench_batch_insert(storage.clone()).await;
    bench_claim_throughput(storage.clone()).await;
    bench_e2e_throughput(storage.clone(), 1).await;
    bench_e2e_throughput(storage.clone(), 3).await;

    println!("\n=== Benchmark Complete ===");
}

/// Benchmark 1: Batch INSERT 100K instances.
/// Target: < 3 seconds.
async fn bench_batch_insert(storage: Arc<PostgresStorage>) {
    let tenant = format!("bench-insert-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let seq = make_sequence(&tenant, 1);
    storage.create_sequence(&seq).await.unwrap();

    let count = 100_000;
    let instances = make_instances(count, seq.id, &tenant);

    println!("--- Batch INSERT: {count} instances ---");

    let start = Instant::now();
    let inserted = storage.create_instances_batch(&instances).await.unwrap();
    let elapsed = start.elapsed();

    let rate = count as f64 / elapsed.as_secs_f64();
    let pass = elapsed < Duration::from_secs(3);

    println!("  Inserted: {inserted}");
    println!("  Time:     {elapsed:.2?}");
    println!("  Rate:     {rate:.0} instances/sec");
    println!(
        "  Target:   < 3s → {}",
        if pass { "PASS" } else { "FAIL" }
    );
    println!();

    cleanup(storage.as_ref(), &tenant).await;
}

/// Benchmark 2: `claim_due_instances` throughput.
/// How fast can we claim batches of 256 from a pool of 10K scheduled instances?
async fn bench_claim_throughput(storage: Arc<PostgresStorage>) {
    let tenant = format!("bench-claim-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let seq = make_sequence(&tenant, 1);
    storage.create_sequence(&seq).await.unwrap();

    let count = 10_000;
    let instances = make_instances(count, seq.id, &tenant);
    storage.create_instances_batch(&instances).await.unwrap();

    println!("--- Claim throughput: {count} instances (batch_size=256) ---");

    let batch_size = 256u32;
    let start = Instant::now();
    let mut claimed_total = 0usize;
    let mut claim_ticks = 0u32;
    loop {
        let claimed = storage
            .claim_due_instances(Utc::now(), batch_size, 0)
            .await
            .unwrap();
        if claimed.is_empty() {
            break;
        }
        claimed_total += claimed.len();
        claim_ticks += 1;
    }
    let elapsed = start.elapsed();

    let rate = claimed_total as f64 / elapsed.as_secs_f64();
    println!("  Claimed:  {claimed_total} in {claim_ticks} ticks");
    println!("  Time:     {elapsed:.2?}");
    println!("  Rate:     {rate:.0} instances/sec");
    println!();

    cleanup(storage.as_ref(), &tenant).await;
}

/// Benchmark: End-to-end throughput.
/// Schedule N instances with noop handler(s), run engine, measure time to complete all.
async fn bench_e2e_throughput(storage: Arc<PostgresStorage>, num_steps: usize) {
    let tenant = format!("bench-e2e-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let seq = make_sequence(&tenant, num_steps);
    storage.create_sequence(&seq).await.unwrap();

    let count = 5_000;
    let instances = make_instances(count, seq.id, &tenant);
    let instance_ids: Vec<InstanceId> = instances.iter().map(|i| i.id).collect();

    storage.create_instances_batch(&instances).await.unwrap();

    println!(
        "--- End-to-end: {count} instances x {num_steps} steps (tick=50ms, batch=512, concurrent=256) ---"
    );

    let cancel = CancellationToken::new();

    let config = SchedulerConfig {
        tick_interval_ms: 50,
        batch_size: 512,
        max_concurrent_steps: 256,
        ..SchedulerConfig::default()
    };

    let mut handlers = HandlerRegistry::new();
    orch8_engine::handlers::builtin::register_builtins(&mut handlers);

    let engine = Engine::new(
        storage.clone() as Arc<dyn StorageBackend>,
        config,
        handlers,
        cancel.clone(),
    );

    let start = Instant::now();

    // Spawn engine
    let engine_handle = tokio::spawn(async move {
        engine.run().await.ok();
    });

    // Poll until all instances reach terminal state
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if Instant::now() > deadline {
            println!("  TIMEOUT after 60s");
            break;
        }

        let mut completed = 0usize;
        let mut failed = 0usize;
        let mut other = 0usize;

        // Sample a subset to avoid overwhelming the DB
        for id in instance_ids.iter().take(100) {
            if let Ok(Some(inst)) = storage.get_instance(*id).await {
                match inst.state {
                    InstanceState::Completed => completed += 1,
                    InstanceState::Failed => failed += 1,
                    _ => other += 1,
                }
            }
        }

        // If all sampled are terminal, check the full set
        if other == 0 && completed + failed == 100 {
            let mut all_done = true;
            let mut total_completed = 0usize;
            let mut total_failed = 0usize;

            for id in &instance_ids {
                if let Ok(Some(inst)) = storage.get_instance(*id).await {
                    if inst.state.is_terminal() {
                        if inst.state == InstanceState::Completed {
                            total_completed += 1;
                        } else {
                            total_failed += 1;
                        }
                    } else {
                        all_done = false;
                        break;
                    }
                }
            }

            if all_done {
                let elapsed = start.elapsed();
                let rate = count as f64 / elapsed.as_secs_f64();
                println!("  Completed: {total_completed}");
                println!("  Failed:    {total_failed}");
                println!("  Time:      {elapsed:.2?}");
                println!("  Rate:      {rate:.0} instances/sec");
                break;
            }
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), engine_handle).await;

    println!();
    cleanup(storage.as_ref(), &tenant).await;
}
