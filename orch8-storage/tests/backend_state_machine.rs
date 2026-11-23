//! Seeded durable-state comparison across the production database dialects.
#![cfg(feature = "postgres")]

use chrono::{DateTime, TimeDelta, Utc};
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::testing::{
    backends::{TestPostgres, postgres_url},
    fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_storage::{SequenceStore as _, StorageBackend};
use orch8_types::{
    checkpoint::Checkpoint,
    error::StorageError,
    ids::InstanceId,
    instance::InstanceState,
    sequence::SequenceDefinition,
    signal::{Signal, SignalType},
};
use serde::Serialize;
use serde_json::{Value, json};
use uuid::Uuid;

const SLOTS: u8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum Action {
    Create { slot: u8 },
    Signal { slot: u8, number: u64 },
    Claim,
    Checkpoint { slot: u8, number: u64 },
    Cancel { slot: u8 },
}

fn trace(mut random: u64) -> Vec<Action> {
    let mut actions = vec![
        Action::Create { slot: 0 },
        Action::Signal { slot: 0, number: 1 },
        Action::Claim,
        Action::Checkpoint { slot: 0, number: 3 },
        Action::Cancel { slot: 0 },
    ];
    actions.extend((5..64).map(|number| {
        // Fixed xorshift64 makes a failing seed reproducible across toolchains.
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        let slot = u8::try_from(random % u64::from(SLOTS)).unwrap();
        match (random >> 8) % 5 {
            0 => Action::Create { slot },
            1 => Action::Signal { slot, number },
            2 => Action::Claim,
            3 => Action::Checkpoint { slot, number },
            _ => Action::Cancel { slot },
        }
    }));
    actions
}

fn instance(slot: u8) -> InstanceId {
    InstanceId::from_uuid(Uuid::from_u128(u128::from(slot) + 1))
}

async fn apply(
    backend: &dyn StorageBackend,
    sequence: &SequenceDefinition,
    action: Action,
    now: DateTime<Utc>,
) -> Value {
    match action {
        Action::Create { slot } => {
            let id = instance(slot);
            if backend.get_instance(id).await.unwrap().is_some() {
                return json!("existing");
            }
            let mut task = InstanceBuilder::for_sequence(sequence).due_at(now).build();
            task.id = id;
            task.created_at = now;
            task.updated_at = now;
            backend.create_instance(&task).await.unwrap();
            json!("created")
        }
        Action::Signal { slot, number } => {
            let result = backend
                .enqueue_signal_if_active(&Signal {
                    id: Uuid::from_u128(u128::from(number) + 1000),
                    instance_id: instance(slot),
                    signal_type: SignalType::Custom("fixture".into()),
                    payload: json!({"sequence":number}),
                    delivered: false,
                    created_at: now,
                    delivered_at: None,
                })
                .await;
            json!(match result {
                Ok(()) => "queued",
                Err(StorageError::NotFound { .. }) => "not_found",
                Err(StorageError::TerminalTarget { .. }) => "terminal",
                Err(error) => panic!("unexpected signal error: {error}"),
            })
        }
        Action::Claim => {
            let claimed = backend.claim_due_instances(now, 2, 0).await.unwrap();
            json!(claimed.iter().map(|task| task.id).collect::<Vec<_>>())
        }
        Action::Checkpoint { slot, number } => {
            let id = instance(slot);
            let exists = backend.get_instance(id).await.unwrap().is_some();
            if exists {
                backend
                    .save_checkpoint(&Checkpoint {
                        id: Uuid::from_u128(u128::from(number) + 2000),
                        instance_id: id,
                        checkpoint_data: json!({"checkpoint_seq":number}),
                        created_at: now,
                    })
                    .await
                    .unwrap();
            }
            json!(exists)
        }
        Action::Cancel { slot } => {
            let id = instance(slot);
            let mut cancelled = false;
            if let Some(task) = backend.get_instance(id).await.unwrap()
                && !task.state.is_terminal()
            {
                cancelled = backend
                    .conditional_update_instance_state(
                        id,
                        task.state,
                        InstanceState::Cancelled,
                        None,
                    )
                    .await
                    .unwrap();
            }
            json!(cancelled)
        }
    }
}

async fn durable_state(backend: &dyn StorageBackend) -> Value {
    let mut rows = Vec::new();
    for slot in 0..SLOTS {
        let id = instance(slot);
        let task = backend.get_instance(id).await.unwrap();
        let signals = backend
            .list_signal_history_page(id, None, 100)
            .await
            .unwrap();
        let history = backend.list_checkpoints(id, 100).await.unwrap();
        let latest = backend.get_latest_checkpoint(id).await.unwrap();
        rows.push(json!({
            "instance": task.map(|task| json!({
                "state":task.state,
                "has_claim":task.claimed_at.is_some(),
                "due":task.next_fire_at,
            })),
            "signals": signals,
            "checkpoints": history,
            "latest_checkpoint": latest,
        }));
    }
    json!(rows)
}

#[derive(Serialize)]
struct Difference {
    operation: usize,
    sqlite: Value,
    postgres: Value,
}

async fn first_difference(actions: &[Action]) -> Option<Difference> {
    let sqlite = SqliteStorage::in_memory().await.unwrap();
    let postgres = TestPostgres::fresh()
        .await
        .expect("PostgreSQL selected for differential test");
    let sequence = SequenceBuilder::new("backend-state-machine")
        .step("fixture", "noop")
        .build();
    sqlite.create_sequence(&sequence).await.unwrap();
    postgres.create_sequence(&sequence).await.unwrap();
    let epoch = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();

    for (operation, action) in actions.iter().copied().enumerate() {
        let now = epoch + TimeDelta::seconds(i64::try_from(operation).unwrap());
        let (a, b) = tokio::join!(
            apply(&sqlite, &sequence, action, now),
            apply(&postgres.storage, &sequence, action, now)
        );
        let (local, remote) =
            tokio::join!(durable_state(&sqlite), durable_state(&postgres.storage));
        if a != b || local != remote {
            return Some(Difference {
                operation,
                sqlite: json!({"result":a, "state":local}),
                postgres: json!({"result":b, "state":remote}),
            });
        }
    }
    None
}

async fn minimize(
    actions: &[Action],
    mut difference: Difference,
    mut replay: impl AsyncFnMut(&[Action]) -> Option<Difference>,
) -> (Vec<Action>, Difference) {
    // Greedy shrinking gives an upper bound. A failed single deletion can
    // still be part of a successful multi-operation deletion.
    let mut upper = actions[..=difference.operation].to_vec();
    let mut index = 0;
    while index < upper.len() {
        let mut candidate = upper.clone();
        candidate.remove(index);
        if let Some(found) = replay(&candidate).await {
            candidate.truncate(found.operation + 1);
            upper = candidate;
            difference = found;
            index = 0;
        } else {
            index += 1;
        }
    }

    // Search the full seed by increasing length. Later events can reproduce
    // a divergence with fewer operations than the first failing prefix.
    for size in 1..upper.len() {
        let mut positions: Vec<_> = (0..size).collect();
        loop {
            let mut candidate: Vec<_> = positions.iter().map(|&pos| actions[pos]).collect();
            if let Some(found) = replay(&candidate).await {
                candidate.truncate(found.operation + 1);
                return (candidate, found);
            }
            let Some(pivot) = (0..size).rfind(|&i| positions[i] < actions.len() - size + i) else {
                break;
            };
            positions[pivot] += 1;
            for following in pivot + 1..size {
                positions[following] = positions[following - 1] + 1;
            }
        }
    }
    (upper, difference)
}

fn write_trace(path: &std::path::Path, seed: u64, actions: &[Action], difference: &Difference) {
    let file = std::fs::File::create(path).unwrap();
    serde_json::to_writer_pretty(
        file,
        &json!({
            "seed": seed,
            "operations": actions,
            "difference": difference,
        }),
    )
    .unwrap();
}

#[tokio::test]
async fn finds_the_shorter_divergence_missed_by_single_deletions() {
    let signal = Action::Signal { slot: 0, number: 1 };
    let late_signal = Action::Signal { slot: 1, number: 2 };
    let actions = [
        Action::Claim,
        Action::Create { slot: 0 },
        Action::Create { slot: 1 },
        Action::Create { slot: 2 },
        signal,
        late_signal,
    ];
    let replay = async |prefix: &[Action]| -> Option<Difference> {
        let operation = prefix.iter().enumerate().find_map(|(index, &action)| {
            let before = &prefix[..index];
            (action == late_signal
                || (action == signal
                    && (before.contains(&Action::Claim)
                        || (0..3).all(|slot| before.contains(&Action::Create { slot })))))
            .then_some(index)
        })?;
        Some(Difference {
            operation,
            sqlite: json!("a"),
            postgres: json!("b"),
        })
    };
    let prefix = &actions[..5];
    let first = replay(prefix).await.unwrap();
    let (shortest, difference) = minimize(prefix, first, replay).await;
    assert_eq!(shortest, [Action::Claim, signal]);
    assert_eq!(difference.operation, 1);

    let first = replay(&actions).await.unwrap();
    assert_eq!(first.operation, 4);
    let (shortest, difference) = minimize(&actions, first, replay).await;
    assert_eq!(shortest, [late_signal]);
    assert_eq!(difference.operation, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeded_state_machines_agree_across_backends() {
    if postgres_url().is_none() {
        return;
    }
    for seed in [1, 19, 0x1a2b_3c4d, 0x1122_3344_5566_7788] {
        let actions = trace(seed);
        if let Some(difference) = first_difference(&actions).await {
            let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../target/backend-differential");
            std::fs::create_dir_all(&directory).unwrap();
            let path = directory.join(format!("seed-{seed}.json"));
            write_trace(&path, seed, &actions[..=difference.operation], &difference);
            let (actions, difference) = minimize(&actions, difference, first_difference).await;
            write_trace(&path, seed, &actions, &difference);
            panic!(
                "storage backends differ for seed {seed}; minimized trace at {}",
                path.display()
            );
        }
    }
}

#[test]
fn seeds_produce_replayable_traces_with_all_operation_types() {
    let seeded = serde_json::to_value(trace(19)).unwrap();
    assert_eq!(seeded, serde_json::to_value(trace(19)).unwrap());
    assert_ne!(seeded, serde_json::to_value(trace(20)).unwrap());
    let actions = seeded.as_array().unwrap();
    for operation in ["create", "signal", "claim", "checkpoint", "cancel"] {
        assert!(
            actions
                .iter()
                .any(|action| action["operation"] == operation)
        );
    }
}
