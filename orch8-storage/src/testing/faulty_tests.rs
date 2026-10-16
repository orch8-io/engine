use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use orch8_types::error::StorageError;
use orch8_types::instance::InstanceState;

use super::{FaultHandle, FaultStorage, is_known_method};
use crate::StorageBackend;
use crate::sqlite::SqliteStorage;
use crate::testing::fixtures::{InstanceBuilder, SequenceBuilder};

async fn wrapped() -> (
    Arc<dyn StorageBackend>,
    FaultHandle,
    Arc<dyn StorageBackend>,
) {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    (storage, faults, inner)
}

/// Outcome of each of `n` `get_sequence` calls: `true` = Ok.
async fn get_sequence_outcomes(storage: &dyn StorageBackend, n: usize) -> Vec<bool> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(
            storage
                .get_sequence(orch8_types::ids::SequenceId::new())
                .await
                .is_ok(),
        );
    }
    out
}

#[tokio::test]
async fn unarmed_wrapper_forwards_to_inner_backend_and_counts_calls() {
    let (storage, faults, inner) = wrapped().await;
    let seq = SequenceBuilder::new("t").step("a", "noop").build();

    storage.create_sequence(&seq).await.unwrap();

    let stored = inner
        .get_sequence(seq.id)
        .await
        .unwrap()
        .expect("row landed");
    assert_eq!(stored.id, seq.id);
    assert_eq!(faults.calls("create_sequence"), 1);
    assert_eq!(
        faults.calls("get_sequence"),
        0,
        "inner reads bypass counters"
    );
}

#[tokio::test]
async fn times_one_fails_the_first_call_with_a_transient_error_then_recovers() {
    let (storage, faults, _) = wrapped().await;
    faults.on("get_sequence").times(1).fail_transient();

    let first = storage
        .get_sequence(orch8_types::ids::SequenceId::new())
        .await;
    let second = storage
        .get_sequence(orch8_types::ids::SequenceId::new())
        .await;

    let err = first.expect_err("first call is failed");
    assert!(matches!(err, StorageError::Connection(_)), "{err:?}");
    assert!(err.is_transient());
    assert!(second.is_ok());
    assert_eq!(faults.injected("get_sequence"), 1);
    assert_eq!(faults.calls("get_sequence"), 2);
}

#[tokio::test]
async fn fail_permanent_returns_a_non_transient_query_error() {
    let (storage, faults, _) = wrapped().await;
    faults.on("get_sequence").fail_permanent();

    let err = storage
        .get_sequence(orch8_types::ids::SequenceId::new())
        .await
        .expect_err("armed");

    assert!(matches!(err, StorageError::Query(_)), "{err:?}");
    assert!(!err.is_transient());
}

#[tokio::test]
async fn nth_fails_only_that_call() {
    let (storage, faults, _) = wrapped().await;
    faults.on("get_sequence").nth(2).fail_transient();

    let outcomes = get_sequence_outcomes(storage.as_ref(), 4).await;

    assert_eq!(outcomes, [true, false, true, true]);
}

#[tokio::test]
async fn every_nth_fails_each_multiple() {
    let (storage, faults, _) = wrapped().await;
    faults.on("get_sequence").every_nth(2).fail_transient();

    let outcomes = get_sequence_outcomes(storage.as_ref(), 5).await;

    assert_eq!(outcomes, [true, false, true, false, true]);
}

#[tokio::test]
async fn after_lets_the_first_calls_through_then_fails_every_call() {
    let (storage, faults, _) = wrapped().await;
    faults.on("get_sequence").after(2).fail_transient();

    let outcomes = get_sequence_outcomes(storage.as_ref(), 4).await;

    assert_eq!(outcomes, [true, true, false, false]);
}

#[tokio::test]
async fn when_predicate_selects_the_failed_calls() {
    let (storage, faults, _) = wrapped().await;
    faults
        .on("get_sequence")
        .when(|n| n == 1 || n == 3)
        .fail_transient();

    let outcomes = get_sequence_outcomes(storage.as_ref(), 4).await;

    assert_eq!(outcomes, [false, true, false, true]);
}

#[tokio::test]
async fn trigger_counts_calls_from_when_the_rule_was_armed() {
    let (storage, faults, _) = wrapped().await;
    get_sequence_outcomes(storage.as_ref(), 3).await;
    faults.on("get_sequence").nth(1).fail_transient();

    let outcomes = get_sequence_outcomes(storage.as_ref(), 2).await;

    assert_eq!(outcomes, [false, true]);
}

#[tokio::test]
async fn plain_fail_does_not_reach_the_inner_backend() {
    let (storage, faults, inner) = wrapped().await;
    faults.on("create_sequence").times(1).fail_transient();
    let seq = SequenceBuilder::new("t").step("a", "noop").build();

    let result = storage.create_sequence(&seq).await;

    assert!(result.is_err());
    assert!(inner.get_sequence(seq.id).await.unwrap().is_none());
}

#[tokio::test]
async fn fail_after_commit_lands_the_write_but_reports_an_error() {
    let (storage, faults, inner) = wrapped().await;
    let seq = SequenceBuilder::new("t").step("a", "noop").build();
    storage.create_sequence(&seq).await.unwrap();
    let inst = InstanceBuilder::for_sequence(&seq).build();
    faults
        .on("create_instance")
        .times(1)
        .fail_after_commit(StorageError::Connection("ambiguous".into()));

    let result = storage.create_instance(&inst).await;

    assert!(matches!(result, Err(StorageError::Connection(m)) if m == "ambiguous"));
    let landed = inner
        .get_instance(inst.id)
        .await
        .unwrap()
        .expect("committed");
    assert_eq!(landed.state, InstanceState::Scheduled);
}

#[tokio::test]
async fn fail_with_preserves_the_error_variant_on_every_fired_call() {
    let (storage, faults, _) = wrapped().await;
    faults
        .on("get_sequence")
        .times(2)
        .fail_with(StorageError::PoolExhausted);

    let a = storage
        .get_sequence(orch8_types::ids::SequenceId::new())
        .await;
    let b = storage
        .get_sequence(orch8_types::ids::SequenceId::new())
        .await;

    assert!(matches!(a, Err(StorageError::PoolExhausted)));
    assert!(matches!(b, Err(StorageError::PoolExhausted)));
}

#[tokio::test]
async fn delay_stalls_the_call_on_tokio_time() {
    let (storage, faults, _) = wrapped().await;
    faults
        .on("get_sequence")
        .times(1)
        .delay(Duration::from_secs(3600));
    // Pause only after the SQLite pool is up: its acquire timeouts must not
    // be auto-advanced. The 60 s timeout is the earliest timer, so the
    // delayed call never reaches SQLite.
    tokio::time::pause();

    let result = tokio::time::timeout(
        Duration::from_secs(60),
        storage.get_sequence(orch8_types::ids::SequenceId::new()),
    )
    .await;

    assert!(result.is_err(), "the delayed call must still be pending");
    assert_eq!(faults.calls("get_sequence"), 1);
    assert_eq!(faults.injected("get_sequence"), 0, "a delay is not a fault");
}

#[tokio::test]
async fn clear_disarms_every_rule() {
    let (storage, faults, _) = wrapped().await;
    faults.on("get_sequence").fail_permanent();

    faults.clear();

    assert_eq!(
        get_sequence_outcomes(storage.as_ref(), 2).await,
        [true, true]
    );
}

#[tokio::test]
async fn call_log_records_the_order_of_intercepted_calls() {
    let (storage, faults, _) = wrapped().await;
    let seq = SequenceBuilder::new("t").step("a", "noop").build();

    storage.create_sequence(&seq).await.unwrap();
    storage.get_sequence(seq.id).await.unwrap();

    assert_eq!(faults.call_log(), ["create_sequence", "get_sequence"]);
}

#[test]
#[should_panic(expected = "is not a StorageBackend method")]
fn arming_an_unknown_method_name_panics() {
    let faults = FaultHandle::default();
    faults.on("get_sequense").fail_transient();
}

/// Method names of every `pub trait *Store` block in `src`, parsed the same
/// way as the generator that wrote `faulty_methods.rs`.
fn trait_method_names(src: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let mut in_trait = false;
    let mut depth = 0i32;
    for line in src.lines() {
        let trimmed = line.trim_start();
        if !in_trait {
            if trimmed.starts_with("pub trait ") && trimmed.contains("Store") {
                in_trait = true;
                depth = 0;
            } else {
                continue;
            }
        }
        if trimmed.starts_with("//") {
            continue;
        }
        // A method item starts at depth 1 (directly inside the trait body).
        if depth == 1 {
            let rest = trimmed.strip_prefix("async ").unwrap_or(trimmed);
            if let Some(after) = rest.strip_prefix("fn ") {
                let name: String = after
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                names.insert(name);
            }
        }
        depth += i32::try_from(line.matches('{').count()).unwrap();
        depth -= i32::try_from(line.matches('}').count()).unwrap();
        if depth == 0 && line.contains('}') {
            in_trait = false;
        }
    }
    names
}

#[test]
fn every_storage_trait_method_is_intercepted() {
    let mut expected = trait_method_names(include_str!("../lib.rs"));
    expected.extend(trait_method_names(include_str!(
        "../../../orch8-push/src/outbox.rs"
    )));
    let listed: BTreeSet<String> = super::ALL_METHODS
        .iter()
        .flat_map(|m| m.iter().map(|s| (*s).to_owned()))
        .collect();

    let missing: Vec<_> = expected.difference(&listed).collect();
    let extra: Vec<_> = listed.difference(&expected).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "faulty_methods.rs drifted from the StorageBackend traits; \
         missing: {missing:?}, extra: {extra:?}"
    );
    assert!(expected.len() > 400, "parser found only {}", expected.len());
    assert!(is_known_method("claim_due_instances"));
}
