//! End-to-end tests for Conditional Retry Policies (Feature #22).
//!
//! `RetryPolicy.retry_if` is an expression evaluated against the failure
//! (`_error.message` / `_error.details`) and instance `data.*` before each
//! retry attempt; a falsy result fails the step immediately even if
//! `max_attempts` has not been reached. `RetryPolicy.non_retryable_codes`
//! is a denylist checked against `details.error_code`/`details.code` that
//! takes priority over `retry_if`.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::*;
use orch8_types::instance::InstanceState;
use serde_json::json;

// ================================================================
// retry_if: truthy → retries proceed normally until exhausted
// ================================================================

#[tokio::test]
async fn retry_if_truthy_allows_retries_until_exhausted() {
    let (storage, seq, inst) = setup(vec![mk_step_with_conditional_retry(
        "s1",
        "always_transient",
        3,
        Some("contains(_error.message, \"timeout\")"),
        None,
    )])
    .await;

    let mut reg = registry();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_clone = Arc::clone(&calls);
    reg.register("always_transient", move |_ctx| {
        let c = Arc::clone(&calls_clone);
        Box::pin(async move {
            c.fetch_add(1, Ordering::SeqCst);
            Err(orch8_types::error::StepError::Retryable {
                message: "connection timeout".into(),
                details: None,
            })
        })
    });

    drive(&storage, &reg, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
    // Attempt indices 0..=max_attempts all run before the policy is
    // exhausted (max_attempts=3 permits 4 total invocations), since
    // retry_if kept passing on every failure.
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

// ================================================================
// retry_if: falsy → fails immediately after the first attempt
// ================================================================

#[tokio::test]
async fn retry_if_falsy_fails_after_first_attempt() {
    let (storage, seq, inst) = setup(vec![mk_step_with_conditional_retry(
        "s1",
        "always_denied",
        5,
        Some("contains(_error.message, \"timeout\")"),
        None,
    )])
    .await;

    let mut reg = registry();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_clone = Arc::clone(&calls);
    reg.register("always_denied", move |_ctx| {
        let c = Arc::clone(&calls_clone);
        Box::pin(async move {
            c.fetch_add(1, Ordering::SeqCst);
            Err(orch8_types::error::StepError::Retryable {
                message: "permission denied".into(),
                details: None,
            })
        })
    });

    drive(&storage, &reg, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
    // retry_if never matched "timeout" — only the first attempt should run.
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

// ================================================================
// retry_if references instance context data
// ================================================================

#[tokio::test]
async fn retry_if_references_instance_data_allows_retry() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_conditional_retry(
            "s1",
            "flaky",
            3,
            Some("data.allow_retry == true"),
            None,
        )],
        json!({"allow_retry": true}),
    )
    .await;

    let mut reg = registry();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_clone = Arc::clone(&calls);
    reg.register("flaky", move |_ctx| {
        let c = Arc::clone(&calls_clone);
        Box::pin(async move {
            let n = c.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Err(orch8_types::error::StepError::Retryable {
                    message: "transient".into(),
                    details: None,
                })
            } else {
                Ok(json!({"ok": true}))
            }
        })
    });

    drive(&storage, &reg, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn retry_if_references_instance_data_denies_retry() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_conditional_retry(
            "s1",
            "flaky",
            3,
            Some("data.allow_retry == true"),
            None,
        )],
        json!({"allow_retry": false}),
    )
    .await;

    let mut reg = registry();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_clone = Arc::clone(&calls);
    reg.register("flaky", move |_ctx| {
        let c = Arc::clone(&calls_clone);
        Box::pin(async move {
            c.fetch_add(1, Ordering::SeqCst);
            Err(orch8_types::error::StepError::Retryable {
                message: "transient".into(),
                details: None,
            })
        })
    });

    drive(&storage, &reg, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

// ================================================================
// non_retryable_codes: matching code denies retry immediately
// ================================================================

#[tokio::test]
async fn non_retryable_codes_matching_code_denies_retry() {
    let (storage, seq, inst) = setup(vec![mk_step_with_conditional_retry(
        "s1",
        "auth_fail",
        5,
        None,
        Some(vec!["AUTH_FAILED", "INVALID_INPUT"]),
    )])
    .await;

    let mut reg = registry();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_clone = Arc::clone(&calls);
    reg.register("auth_fail", move |_ctx| {
        let c = Arc::clone(&calls_clone);
        Box::pin(async move {
            c.fetch_add(1, Ordering::SeqCst);
            Err(orch8_types::error::StepError::Retryable {
                message: "unauthorized".into(),
                details: Some(json!({"error_code": "AUTH_FAILED"})),
            })
        })
    });

    drive(&storage, &reg, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

// ================================================================
// non_retryable_codes: non-matching code retries normally
// ================================================================

#[tokio::test]
async fn non_retryable_codes_non_matching_code_retries_normally() {
    let (storage, seq, inst) = setup(vec![mk_step_with_conditional_retry(
        "s1",
        "rate_limited",
        3,
        None,
        Some(vec!["AUTH_FAILED"]),
    )])
    .await;

    let mut reg = registry();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_clone = Arc::clone(&calls);
    reg.register("rate_limited", move |_ctx| {
        let c = Arc::clone(&calls_clone);
        Box::pin(async move {
            c.fetch_add(1, Ordering::SeqCst);
            Err(orch8_types::error::StepError::Retryable {
                message: "too many requests".into(),
                details: Some(json!({"error_code": "RATE_LIMITED"})),
            })
        })
    });

    drive(&storage, &reg, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

// ================================================================
// non_retryable_codes takes priority over retry_if
// ================================================================

#[tokio::test]
async fn non_retryable_codes_wins_over_retry_if() {
    let (storage, seq, inst) = setup(vec![mk_step_with_conditional_retry(
        "s1",
        "fatal",
        5,
        Some("true"),
        Some(vec!["FATAL"]),
    )])
    .await;

    let mut reg = registry();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_clone = Arc::clone(&calls);
    reg.register("fatal", move |_ctx| {
        let c = Arc::clone(&calls_clone);
        Box::pin(async move {
            c.fetch_add(1, Ordering::SeqCst);
            Err(orch8_types::error::StepError::Retryable {
                message: "unrecoverable".into(),
                details: Some(json!({"error_code": "FATAL"})),
            })
        })
    });

    drive(&storage, &reg, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

// ================================================================
// No conditional retry config → existing unconditional behavior
// ================================================================

#[tokio::test]
async fn no_conditional_retry_config_retries_unconditionally() {
    let (storage, seq, inst) = setup(vec![mk_step_with_retry("s1", "flaky2", 3)]).await;

    let mut reg = registry();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_clone = Arc::clone(&calls);
    reg.register("flaky2", move |_ctx| {
        let c = Arc::clone(&calls_clone);
        Box::pin(async move {
            let n = c.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Err(orch8_types::error::StepError::Retryable {
                    message: "transient".into(),
                    details: None,
                })
            } else {
                Ok(json!({"ok": true}))
            }
        })
    });

    drive(&storage, &reg, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

// ================================================================
// Serde round-trip: retry_if / non_retryable_codes survive serialization
// ================================================================

#[test]
fn retry_policy_serde_roundtrip() {
    let block = mk_step_with_conditional_retry(
        "s1",
        "h",
        3,
        Some("contains(_error.message, \"x\")"),
        Some(vec!["CODE_A", "CODE_B"]),
    );
    let json_str = serde_json::to_string(&block).unwrap();
    let deserialized: orch8_types::sequence::BlockDefinition =
        serde_json::from_str(&json_str).unwrap();
    if let orch8_types::sequence::BlockDefinition::Step(sd) = &deserialized {
        let retry = sd.retry.as_ref().expect("retry policy must round-trip");
        assert_eq!(
            retry.retry_if.as_deref(),
            Some("contains(_error.message, \"x\")")
        );
        assert_eq!(
            retry.non_retryable_codes,
            Some(vec!["CODE_A".to_string(), "CODE_B".to_string()])
        );
    } else {
        panic!("expected Step variant");
    }
}

#[test]
fn retry_policy_without_conditional_fields_omits_them() {
    let block = mk_step_with_retry("s1", "h", 3);
    let value = serde_json::to_value(&block).unwrap();
    let retry = &value["retry"];
    assert!(
        retry.get("retry_if").is_none(),
        "retry_if should be omitted when None: {retry}"
    );
    assert!(
        retry.get("non_retryable_codes").is_none(),
        "non_retryable_codes should be omitted when None: {retry}"
    );
}

// ================================================================
// Validation: empty retry_if is rejected
// ================================================================

#[test]
fn empty_retry_if_rejected_at_validation() {
    let seq = mk_sequence(vec![mk_step_with_conditional_retry(
        "s1",
        "h",
        3,
        Some(""),
        None,
    )]);
    let err = seq.validate().unwrap_err();
    assert!(err.to_string().contains("retry_if"));
}

#[test]
fn empty_non_retryable_code_rejected_at_validation() {
    let seq = mk_sequence(vec![mk_step_with_conditional_retry(
        "s1",
        "h",
        3,
        None,
        Some(vec!["VALID_CODE", ""]),
    )]);
    let err = seq.validate().unwrap_err();
    assert!(err.to_string().contains("non_retryable_codes"));
}

// ================================================================
// Release diff detects retry_if / non_retryable_codes changes
// ================================================================

#[test]
fn release_diff_detects_retry_if_change() {
    use orch8_engine::release_diff::semantic_diff;

    let v1 = mk_sequence(vec![mk_step_with_conditional_retry(
        "s1",
        "h",
        3,
        Some("contains(_error.message, \"a\")"),
        None,
    )]);
    let v2 = mk_sequence(vec![mk_step_with_conditional_retry(
        "s1",
        "h",
        3,
        Some("contains(_error.message, \"b\")"),
        None,
    )]);

    let diff = semantic_diff(&v1, &v2);
    assert!(
        diff.entries.iter().any(|e| e.category == "retry_changed"),
        "diff should detect retry_if change: {diff:?}"
    );
}
