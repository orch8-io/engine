//! Claim replacement must wait while a result write is inside its transaction.
#![cfg(feature = "postgres")]
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use orch8_storage::backend_test;
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
use orch8_types::error::StorageError;
use orch8_types::ids::BlockId;
use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;

// Session advisory locks must never be returned to the pool, even on panic.
struct HeldConnection(Option<sqlx::pool::PoolConnection<sqlx::Postgres>>);
impl Drop for HeldConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.0.take() {
            drop(conn.detach());
        }
    }
}

async fn blocked_by(
    pool: &sqlx::PgPool,
    blocker: i32,
    task: &tokio::task::JoinHandle<Result<bool, StorageError>>,
) -> Option<i32> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if task.is_finished() {
                return None;
            }
            let pid: Option<i32> = sqlx::query_scalar(
                "SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)) LIMIT 1",
            )
            .bind(blocker)
            .fetch_optional(pool)
            .await
            .unwrap();
            if pid.is_some() {
                return pid;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .ok()
    .flatten()
}

backend_test!(kinds = [postgres, encrypted_postgres], async fn replacement_serializes_with_result_write(b) {
    let seq = SequenceBuilder::new("claim-lock").step("work", "noop").build();
    b.storage.create_sequence(&seq).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&seq).due_now().build();
    b.storage.create_instance(&instance).await.unwrap();
    let claimed = b.storage.claim_due_instances(Utc::now(), 1, 0).await.unwrap().pop().unwrap();
    let token = claimed.updated_at;
    let key = i64::from_be_bytes(uuid::Uuid::now_v7().as_bytes()[8..].try_into().unwrap());
    sqlx::query("CREATE TABLE claim_write_gate (lock_key BIGINT NOT NULL)").execute(b.pg_pool()).await.unwrap();
    sqlx::query("INSERT INTO claim_write_gate VALUES ($1)").bind(key).execute(b.pg_pool()).await.unwrap();
    sqlx::query("CREATE FUNCTION hold_claimed_output() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock((SELECT lock_key FROM claim_write_gate)); RETURN NEW; END $$").execute(b.pg_pool()).await.unwrap();
    sqlx::query("CREATE TRIGGER hold_claimed_output BEFORE INSERT ON block_outputs FOR EACH ROW EXECUTE FUNCTION hold_claimed_output()").execute(b.pg_pool()).await.unwrap();
    let mut gate = HeldConnection(Some(b.pg_pool().acquire().await.unwrap()));
    let conn = gate.0.as_mut().unwrap();
    let gate_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut **conn).await.unwrap();
    sqlx::query("SELECT pg_advisory_lock($1)").bind(key).execute(&mut **conn).await.unwrap();
    let store = Arc::clone(&b.storage);
    let output = BlockOutput {
        id: uuid::Uuid::now_v7(), instance_id: instance.id, block_id: BlockId::new("work"),
        output: serde_json::json!({"owner":"first"}), output_ref: None,
        output_size: 20, attempt: 0, created_at: Utc::now(),
    };
    let writer = tokio::spawn(async move { store.save_block_output_claimed(&output, token).await });
    let writer_pid = blocked_by(b.pg_pool(), gate_pid, &writer).await.expect("writer must reach the controlled insert gate");
    let store = Arc::clone(&b.storage);
    let replacement = tokio::spawn(async move {
        store.conditional_update_claimed_instance_state(instance.id, token,
            InstanceState::Running, InstanceState::Scheduled, Some(Utc::now())).await
    });
    // Inspect the actual lock dependency, rather than relying on a sleep to
    // infer that replacement cannot pass the ownership check/write gap.
    let waits_for_writer = blocked_by(b.pg_pool(), writer_pid, &replacement).await.is_some();
    drop(gate);
    assert!(tokio::time::timeout(Duration::from_secs(5), writer).await.unwrap().unwrap().unwrap());
    assert!(tokio::time::timeout(Duration::from_secs(5), replacement).await.unwrap().unwrap().unwrap());
    assert!(waits_for_writer, "claim replacement passed an unfinished result write: ownership check and insert were not atomic");
});

async fn hold_child_insert(pool: &sqlx::PgPool) -> (HeldConnection, i32) {
    let key = i64::from_be_bytes(uuid::Uuid::now_v7().as_bytes()[8..].try_into().unwrap());
    sqlx::query("CREATE TABLE child_write_gate (lock_key BIGINT NOT NULL)")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO child_write_gate VALUES ($1)")
        .bind(key)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("CREATE FUNCTION hold_claimed_child() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock((SELECT lock_key FROM child_write_gate)); RETURN NEW; END $$")
        .execute(pool).await.unwrap();
    sqlx::query("CREATE TRIGGER hold_claimed_child BEFORE INSERT ON task_instances FOR EACH ROW WHEN (NEW.parent_instance_id IS NOT NULL) EXECUTE FUNCTION hold_claimed_child()")
        .execute(pool).await.unwrap();
    let mut gate = HeldConnection(Some(pool.acquire().await.unwrap()));
    let conn = gate.0.as_mut().unwrap();
    let pid = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut **conn)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(key)
        .execute(&mut **conn)
        .await
        .unwrap();
    (gate, pid)
}

backend_test!(kinds = [postgres, encrypted_postgres], async fn replacement_serializes_with_child_creation(b) {
    let sequence = SequenceBuilder::new("child-claim-lock").step("work", "noop").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let parent = InstanceBuilder::for_sequence(&sequence).due_now().build();
    b.storage.create_instance(&parent).await.unwrap();
    let first = b.storage.claim_due_instances(Utc::now(), 1, 0).await.unwrap().pop().unwrap();
    let token = first.claimed_at.unwrap();
    let mut child = InstanceBuilder::for_sequence(&sequence).due_now().build();
    child.parent_instance_id = Some(parent.id);
    let child_id = child.id;
    let (gate, gate_pid) = hold_child_insert(b.pg_pool()).await;
    let store = Arc::clone(&b.storage);
    let writer = tokio::spawn(async move { store.create_child_instance_claimed(parent.id, &child, token).await });
    let writer_pid = blocked_by(b.pg_pool(), gate_pid, &writer).await
        .expect("child insert must reach the controlled transaction gate");
    let store = Arc::clone(&b.storage);
    let replacement = tokio::spawn(async move {
        store.conditional_update_claimed_instance_state(parent.id, token,
            InstanceState::Running, InstanceState::Scheduled, Some(Utc::now())).await
    });
    let waits_for_writer = blocked_by(b.pg_pool(), writer_pid, &replacement).await.is_some();
    drop(gate);
    assert!(tokio::time::timeout(Duration::from_secs(5), writer).await.unwrap().unwrap().unwrap());
    assert!(tokio::time::timeout(Duration::from_secs(5), replacement).await.unwrap().unwrap().unwrap());
    assert!(waits_for_writer, "replacement passed an unfinished child insert; parent guard and child insertion were not atomic");
    assert_eq!(b.storage.get_child_instances(parent.id).await.unwrap()[0].id, child_id);
});
