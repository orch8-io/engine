//! Contract runner: executes a [`ContractSuite`] against a sequence
//! definition on an embedded engine under virtual time.
//!
//! Guarantees:
//! - **Real handlers are never invoked.** Every handler the sequence uses
//!   is replaced with a mock derived from the case (or the suite's
//!   unmocked-handler policy).
//! - **No wall-clock waiting.** Delays, retries, and send windows advance
//!   a [`ManualClock`]; a multi-day workflow tests in milliseconds.
//! - **Deterministic.** Virtual time starts at a fixed epoch unless the
//!   caller overrides it.
//!
//! Shared by `orch8 test` in the CLI and by release validation.

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;

use orch8_types::contract::{
    CaseReport, ContractCase, ContractSuite, MockOutcome, MockPolicy, SuiteReport,
    UnmockedHandlerPolicy, check_call_counts, check_path, evaluate_assertion,
};
use orch8_types::error::StepError;
use orch8_types::sequence::SequenceDefinition;
use orch8_types::signal::SignalType;

use crate::engine::CreateInstanceOptions;
use crate::error::Error;
use crate::{Clock, Engine, ExecutionContext, InstanceState, ManualClock, SharedClock, Storage};

/// Default tick budget per case when the case doesn't set `max_ticks`.
const DEFAULT_MAX_TICKS: u32 = 5_000;

/// Options shared by every case in one runner invocation.
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Recorded block outputs for [`MockPolicy::Recorded`], keyed by block
    /// id (e.g. loaded from an `orch8 test record` fixture).
    pub recorded_outputs: HashMap<String, Value>,
    /// Outputs completed before a selected time-travel boundary. These are
    /// persisted into the sandbox instance so the scheduler skips the prefix
    /// instead of re-executing it. They remain available to downstream
    /// `outputs.*` references but are excluded from the executed-path report.
    pub initial_outputs: HashMap<String, Value>,
    /// Virtual-time epoch the clock starts at. Fixed by default so runs
    /// are reproducible.
    pub start_time: DateTime<Utc>,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            recorded_outputs: HashMap::new(),
            initial_outputs: HashMap::new(),
            // Fixed epoch: contract runs must not depend on the wall clock.
            start_time: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        }
    }
}

/// Run every case in the suite against `seq`. The suite is validated
/// first; an invalid suite fails fast instead of half-running.
///
/// # Errors
/// Returns an error for an invalid suite/sequence or an engine failure.
/// Assertion failures do NOT error — they are reported per case.
pub async fn run_suite(
    seq: &SequenceDefinition,
    suite: &ContractSuite,
    opts: &RunOptions,
) -> Result<SuiteReport, Error> {
    suite
        .validate()
        .map_err(|e| Error::Config(format!("invalid contract suite: {e}")))?;

    let mut cases = Vec::with_capacity(suite.cases.len());
    for case in &suite.cases {
        cases.push(run_case(seq, suite.unmocked_handlers, case, opts).await?);
    }
    let passed = cases.iter().all(|c| c.passed);
    Ok(SuiteReport {
        sequence_name: seq.name.clone(),
        sequence_version: i64::from(seq.version),
        passed,
        cases,
    })
}

/// Shared mutable state observed by mock handlers during one case.
#[derive(Default)]
struct MockState {
    /// Handler name -> number of calls.
    calls: BTreeMap<String, u32>,
    /// Block id -> attempts made so far (for `Attempts` policies).
    attempts: HashMap<String, u32>,
    /// Handlers that were called without a mock (when policy = Fail).
    unmocked_called: Vec<String>,
    /// Blocks whose `Recorded` mock had no recording.
    missing_recordings: Vec<String>,
}

/// Run a single case. Infrastructure errors (engine build, storage)
/// surface as `Err`; behavioral divergences land in the report.
///
/// # Errors
/// Returns an error only for infrastructure failures.
#[allow(clippy::too_many_lines)] // one linear pipeline: mocks → engine → drive → evaluate
pub async fn run_case(
    seq: &SequenceDefinition,
    unmocked_policy: UnmockedHandlerPolicy,
    case: &ContractCase,
    opts: &RunOptions,
) -> Result<CaseReport, Error> {
    let mut initial_outputs = opts.initial_outputs.clone();
    initial_outputs.extend(
        case.initial_outputs
            .iter()
            .map(|(block, output)| (block.clone(), output.clone())),
    );
    // Engine-private outputs (for example `_tree_initialized`) describe the
    // original instance's scheduler state. Replaying them into a fresh
    // sandbox without the matching execution-node rows can suppress tree
    // initialization and strand the case in `scheduled` forever.
    initial_outputs.retain(|block, _| !block.starts_with('_'));
    // --- mock lookup tables ---
    let mut by_handler: HashMap<String, MockPolicy> = HashMap::new();
    let mut by_block: HashMap<String, MockPolicy> = HashMap::new();
    for mock in &case.mocks {
        if let Some(h) = &mock.handler {
            by_handler.insert(h.clone(), mock.policy.clone());
        }
        if let Some(b) = &mock.block {
            by_block.insert(b.clone(), mock.policy.clone());
        }
    }
    let by_handler = Arc::new(by_handler);
    let by_block = Arc::new(by_block);
    let recorded = Arc::new(opts.recorded_outputs.clone());
    let state = Arc::new(Mutex::new(MockState::default()));

    // --- embedded engine with every sequence handler mocked ---
    let clock = Arc::new(ManualClock::new(opts.start_time));
    let shared = SharedClock::from_arc(Arc::clone(&clock) as Arc<dyn Clock>);
    let mut builder = Engine::builder()
        .storage(Storage::sqlite_in_memory())
        .tenant(seq.tenant_id.as_str())
        .clock(shared);

    let handler_names = sequence_handlers(seq);
    for handler_name in &handler_names {
        let handler_name = handler_name.clone();
        let by_handler = Arc::clone(&by_handler);
        let by_block = Arc::clone(&by_block);
        let recorded = Arc::clone(&recorded);
        let state = Arc::clone(&state);
        builder = builder.handler(&handler_name.clone(), move |ctx: crate::StepContext| {
            let handler_name = handler_name.clone();
            let by_handler = Arc::clone(&by_handler);
            let by_block = Arc::clone(&by_block);
            let recorded = Arc::clone(&recorded);
            let state = Arc::clone(&state);
            async move {
                let block_id = ctx.block_id.as_str().to_string();
                let policy = by_block
                    .get(&block_id)
                    .or_else(|| by_handler.get(&handler_name));

                let mut guard = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *guard.calls.entry(handler_name.clone()).or_insert(0) += 1;
                let attempt = guard.attempts.entry(block_id.clone()).or_insert(0);
                let attempt_index = *attempt as usize;
                *attempt += 1;

                match policy {
                    Some(MockPolicy::Success { output }) => Ok(output.clone()),
                    Some(MockPolicy::Failure { message, retryable }) => {
                        mock_failure(message, *retryable)
                    }
                    Some(MockPolicy::Attempts { attempts }) => {
                        let outcome = attempts
                            .get(attempt_index)
                            .or_else(|| attempts.last())
                            .expect("validated: attempts non-empty");
                        match outcome {
                            MockOutcome::Success { output } => Ok(output.clone()),
                            MockOutcome::Failure { message, retryable } => {
                                mock_failure(message, *retryable)
                            }
                        }
                    }
                    Some(MockPolicy::Recorded) => {
                        if let Some(output) = recorded.get(&block_id) {
                            Ok(output.clone())
                        } else {
                            guard.missing_recordings.push(block_id.clone());
                            Err(StepError::Permanent {
                                message: format!(
                                    "contract: no recorded output for block '{block_id}'"
                                ),
                                details: None,
                            })
                        }
                    }
                    None => match unmocked_policy {
                        UnmockedHandlerPolicy::EmptySuccess => {
                            Ok(Value::Object(serde_json::Map::new()))
                        }
                        UnmockedHandlerPolicy::Fail => {
                            guard.unmocked_called.push(handler_name.clone());
                            Err(StepError::Permanent {
                                message: format!(
                                    "contract: handler '{handler_name}' called without a mock"
                                ),
                                details: None,
                            })
                        }
                    },
                }
            }
        });
    }

    let engine = builder.build().await?;
    let seq_id = engine.upsert_sequence(seq.clone()).await?;

    let context = ExecutionContext {
        data: case.input.clone(),
        config: case.config.clone().unwrap_or(Value::Null),
        ..Default::default()
    };
    let instance_id = engine
        .create_instance(
            seq_id,
            CreateInstanceOptions {
                context,
                // Pin the first fire to the virtual epoch. `create_instance`
                // defaults this to the wall clock, which sits months past
                // the fixed epoch and would inject a phantom virtual jump
                // into `logical_duration_ms` (breaking any duration budget
                // smaller than the wall offset).
                next_fire_at: Some(opts.start_time),
                ..Default::default()
            },
        )
        .await?;

    for (block_id, output) in &initial_outputs {
        engine
            .seed_block_output(&orch8_types::output::BlockOutput {
                id: uuid::Uuid::now_v7(),
                instance_id,
                block_id: orch8_types::ids::BlockId::new(block_id),
                output: output.clone(),
                output_ref: None,
                output_size: 0,
                attempt: 0,
                created_at: opts.start_time,
            })
            .await?;
    }

    // --- drive to terminal under virtual time ---
    let max_ticks = case.max_ticks.unwrap_or(DEFAULT_MAX_TICKS);
    let mut ticks = 0u32;
    let mut duration_exceeded = false;
    let mut pending_signals = case.signals.iter();
    let mut invalid_signal: Option<String> = None;

    while ticks < max_ticks {
        let tick = engine.tick_once().await?;
        ticks += 1;
        let inst = engine.get_instance(instance_id).await?;
        if inst.state.is_terminal() {
            break;
        }

        // Deliver the next signal fixture once the instance parks.
        if inst.state == InstanceState::Waiting
            && let Some(fixture) = pending_signals.next()
        {
            match SignalType::from_str(&fixture.signal_type) {
                Ok(signal_type) => {
                    engine
                        .send_signal(instance_id, signal_type, fixture.payload.clone())
                        .await?;
                    continue;
                }
                Err(e) => {
                    invalid_signal = Some(e.to_string());
                    break;
                }
            }
        }

        if tick.steps_executed == 0 && tick.instances_advanced == 0 {
            // Idle: jump virtual time past the next deferral, or give up.
            let now = clock.now();
            let target = match inst.next_fire_at {
                Some(t) if t > now && !inst.state.is_terminal() => Some(t),
                // Due but not yet claimed: that is pending work, not a dead
                // end. Signal wakes stamp `next_fire_at` with the wall
                // clock, which sits *behind* an already-jumped virtual
                // clock — one more tick claims it. Bounded by `max_ticks`.
                Some(_) if !inst.state.is_terminal() => continue,
                _ => None,
            };
            match target {
                Some(t) => {
                    let jump_to = t + chrono::Duration::seconds(1);
                    if let Some(max_ms) = case.max_logical_duration_ms {
                        let elapsed = jump_to - opts.start_time;
                        if elapsed
                            > chrono::Duration::milliseconds(
                                i64::try_from(max_ms).unwrap_or(i64::MAX),
                            )
                        {
                            duration_exceeded = true;
                            break;
                        }
                    }
                    clock.set(jump_to);
                }
                None => break, // parked with nothing to wake it
            }
        }
    }

    // --- collect results ---
    let inst = engine.get_instance(instance_id).await?;
    let final_state = inst.state.to_string();
    let logical_duration_ms =
        u64::try_from((clock.now() - opts.start_time).num_milliseconds().max(0)).unwrap_or(0);

    let raw_outputs = engine.block_outputs(instance_id).await?;
    let mut executed_blocks: Vec<String> = Vec::new();
    let mut outputs: BTreeMap<String, Value> = BTreeMap::new();
    for o in &raw_outputs {
        let block = o.block_id.as_str().to_string();
        if block.starts_with('_') {
            continue; // engine-internal sentinel rows
        }
        if !initial_outputs.contains_key(&block) && !executed_blocks.contains(&block) {
            executed_blocks.push(block.clone());
        }
        outputs.insert(block, o.output.clone()); // last attempt wins
    }

    let mock_state = {
        let guard = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        MockState {
            calls: guard.calls.clone(),
            attempts: guard.attempts.clone(),
            unmocked_called: guard.unmocked_called.clone(),
            missing_recordings: guard.missing_recordings.clone(),
        }
    };

    // --- evaluate expectations ---
    let mut failures: Vec<String> = Vec::new();

    if let Some(err) = invalid_signal {
        failures.push(format!("invalid signal fixture: {err}"));
    }
    if duration_exceeded {
        failures.push(format!(
            "logical duration exceeded the case limit of {} ms",
            case.max_logical_duration_ms.unwrap_or(0)
        ));
    }
    if !inst.state.is_terminal() {
        failures.push(format!(
            "instance did not reach a terminal state (stuck in '{final_state}' after {ticks} ticks)"
        ));
    }
    if let Some(expected) = case.expect.terminal_state
        && inst.state.is_terminal()
        && final_state != expected.as_state_str()
    {
        failures.push(format!(
            "expected terminal state '{}', got '{final_state}'",
            expected.as_state_str()
        ));
    }
    for handler in &mock_state.unmocked_called {
        failures.push(format!(
            "handler '{handler}' was called but has no mock (suite policy: fail)"
        ));
    }
    for block in &mock_state.missing_recordings {
        failures.push(format!("no recorded output available for block '{block}'"));
    }
    if let Some(path) = &case.expect.path
        && let Err(e) = check_path(path, &executed_blocks)
    {
        failures.push(e);
    }
    for skipped in &case.expect.skipped_blocks {
        if executed_blocks.contains(skipped) {
            failures.push(format!(
                "block '{skipped}' executed but was expected to be skipped"
            ));
        }
    }
    for assertion in &case.expect.assertions {
        if let Err(e) = evaluate_assertion(assertion, &outputs, &inst.context.data) {
            failures.push(e);
        }
    }
    if let Err(e) = check_call_counts(&case.expect.call_counts, &mock_state.calls) {
        failures.push(e);
    }

    failures.dedup();

    Ok(CaseReport {
        name: case.name.clone(),
        passed: failures.is_empty(),
        failures,
        final_state,
        executed_blocks,
        handler_calls: mock_state.calls,
        ticks,
        logical_duration_ms,
    })
}

fn mock_failure(message: &str, retryable: bool) -> Result<Value, StepError> {
    if retryable {
        Err(StepError::Retryable {
            message: message.to_string(),
            details: None,
        })
    } else {
        Err(StepError::Permanent {
            message: message.to_string(),
            details: None,
        })
    }
}

/// Every handler name used by any step in the sequence, discovered by
/// walking the serialized definition (covers steps nested in composites
/// without matching every DSL variant).
fn sequence_handlers(seq: &SequenceDefinition) -> Vec<String> {
    fn walk(value: &Value, out: &mut Vec<String>) {
        match value {
            Value::Object(map) => {
                if let (Some(Value::String(_)), Some(Value::String(handler))) =
                    (map.get("id"), map.get("handler"))
                    && !out.contains(handler)
                {
                    out.push(handler.clone());
                }
                for child in map.values() {
                    walk(child, out);
                }
            }
            Value::Array(items) => {
                for item in items {
                    walk(item, out);
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    if let Ok(v) = serde_json::to_value(seq) {
        walk(&v, &mut out);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[allow(clippy::needless_pass_by_value)]
    fn seq(blocks: Value) -> SequenceDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::now_v7(),
            "tenant_id": "default",
            "namespace": "default",
            "name": "contract-test-seq",
            "version": 1,
            "blocks": blocks,
            "created_at": "2026-01-01T00:00:00Z"
        }))
        .expect("valid sequence")
    }

    #[allow(clippy::needless_pass_by_value)]
    fn suite(cases: Value) -> ContractSuite {
        serde_json::from_value(json!({
            "sequence_name": "contract-test-seq",
            "sequence_version": 1,
            "cases": cases
        }))
        .expect("valid suite")
    }

    async fn run(seq_def: &SequenceDefinition, s: &ContractSuite) -> SuiteReport {
        run_suite(seq_def, s, &RunOptions::default())
            .await
            .expect("runner infrastructure")
    }

    #[tokio::test]
    async fn happy_path_case_passes() {
        let seq_def = seq(json!([
            {"type": "step", "id": "validate", "handler": "validator", "params": {}},
            {"type": "step", "id": "charge", "handler": "charger", "params": {}}
        ]));
        let s = suite(json!([{
            "name": "happy",
            "input": {"amount": 100},
            "mocks": [
                {"handler": "validator", "type": "success", "output": {"valid": true}},
                {"handler": "charger", "type": "success", "output": {"charged": true}}
            ],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["validate", "charge"], "ordered": true},
                "assertions": [
                    {"output": {"block": "charge", "path": "charged"}, "op": "equals", "value": true}
                ],
                "call_counts": [
                    {"handler": "charger", "min": 1, "max": 1},
                    {"handler": "validator", "min": 1, "max": 1}
                ]
            }
        }]));
        let report = run(&seq_def, &s).await;
        assert!(report.passed, "failures: {:?}", report.cases[0].failures);
        assert_eq!(report.cases[0].executed_blocks, vec!["validate", "charge"]);
        assert_eq!(report.cases[0].final_state, "completed");
    }

    #[tokio::test]
    async fn failure_mock_fails_the_instance_and_expectation_matches() {
        let seq_def = seq(json!([
            {"type": "step", "id": "charge", "handler": "charger", "params": {}}
        ]));
        let s = suite(json!([{
            "name": "card declined",
            "mocks": [
                {"handler": "charger", "type": "failure", "message": "card declined"}
            ],
            "expect": {"terminal_state": "failed"}
        }]));
        let report = run(&seq_def, &s).await;
        assert!(report.passed, "failures: {:?}", report.cases[0].failures);
        assert_eq!(report.cases[0].final_state, "failed");
    }

    #[tokio::test]
    async fn wrong_terminal_state_is_reported_as_divergence() {
        let seq_def = seq(json!([
            {"type": "step", "id": "charge", "handler": "charger", "params": {}}
        ]));
        let s = suite(json!([{
            "name": "expects failure but succeeds",
            "mocks": [{"handler": "charger", "type": "success", "output": {}}],
            "expect": {"terminal_state": "failed"}
        }]));
        let report = run(&seq_def, &s).await;
        assert!(!report.passed);
        let failures = &report.cases[0].failures;
        assert!(
            failures
                .iter()
                .any(|f| f.contains("expected terminal state 'failed'")),
            "{failures:?}"
        );
    }

    #[tokio::test]
    async fn unmocked_handler_fails_case_by_default() {
        let seq_def = seq(json!([
            {"type": "step", "id": "s1", "handler": "unmocked_thing", "params": {}}
        ]));
        let s = suite(json!([{
            "name": "no mocks",
            "expect": {}
        }]));
        let report = run(&seq_def, &s).await;
        assert!(!report.passed);
        assert!(
            report.cases[0]
                .failures
                .iter()
                .any(|f| f.contains("'unmocked_thing'") && f.contains("no mock")),
            "{:?}",
            report.cases[0].failures
        );
    }

    #[tokio::test]
    async fn unmocked_handler_empty_success_policy_passes() {
        let seq_def = seq(json!([
            {"type": "step", "id": "s1", "handler": "unmocked_thing", "params": {}}
        ]));
        let mut s = suite(json!([{
            "name": "no mocks tolerant",
            "expect": {"terminal_state": "completed"}
        }]));
        s.unmocked_handlers = UnmockedHandlerPolicy::EmptySuccess;
        let report = run(&seq_def, &s).await;
        assert!(report.passed, "{:?}", report.cases[0].failures);
    }

    #[tokio::test]
    async fn builtin_handlers_are_replaced_by_mocks() {
        // http_request is a real built-in with side effects; the contract
        // must override it so no network is touched.
        let seq_def = seq(json!([
            {"type": "step", "id": "call", "handler": "http_request",
             "params": {"url": "https://example.invalid/x", "method": "GET"}}
        ]));
        let s = suite(json!([{
            "name": "http mocked",
            "mocks": [{"handler": "http_request", "type": "success", "output": {"status": 200}}],
            "expect": {
                "terminal_state": "completed",
                "assertions": [
                    {"output": {"block": "call", "path": "status"}, "op": "equals", "value": 200}
                ]
            }
        }]));
        let report = run(&seq_def, &s).await;
        assert!(report.passed, "{:?}", report.cases[0].failures);
    }

    #[tokio::test]
    async fn block_mock_overrides_handler_mock() {
        let seq_def = seq(json!([
            {"type": "step", "id": "a", "handler": "shared", "params": {}},
            {"type": "step", "id": "b", "handler": "shared", "params": {}}
        ]));
        let s = suite(json!([{
            "name": "block precedence",
            "mocks": [
                {"handler": "shared", "type": "success", "output": {"from": "handler"}},
                {"block": "b", "type": "success", "output": {"from": "block"}}
            ],
            "expect": {
                "assertions": [
                    {"output": {"block": "a", "path": "from"}, "op": "equals", "value": "handler"},
                    {"output": {"block": "b", "path": "from"}, "op": "equals", "value": "block"}
                ]
            }
        }]));
        let report = run(&seq_def, &s).await;
        assert!(report.passed, "{:?}", report.cases[0].failures);
    }

    #[tokio::test]
    async fn attempts_policy_drives_retry_then_success() {
        let seq_def = seq(json!([
            {"type": "step", "id": "flaky", "handler": "flaky_handler", "params": {},
             "retry": {"max_attempts": 3, "initial_backoff": 1000, "max_backoff": 60000}}
        ]));
        let s = suite(json!([{
            "name": "retry until success",
            "mocks": [
                {"handler": "flaky_handler", "type": "attempts", "attempts": [
                    {"kind": "failure", "message": "boom", "retryable": true},
                    {"kind": "failure", "message": "boom again", "retryable": true},
                    {"kind": "success", "output": {"ok": true}}
                ]}
            ],
            "expect": {
                "terminal_state": "completed",
                "assertions": [
                    {"output": {"block": "flaky", "path": "ok"}, "op": "equals", "value": true}
                ],
                "call_counts": [{"handler": "flaky_handler", "min": 3, "max": 3}]
            }
        }]));
        let report = run(&seq_def, &s).await;
        assert!(report.passed, "{:?}", report.cases[0].failures);
        // Retries deferred via backoff must advance virtual time, not sleep.
        assert!(report.cases[0].logical_duration_ms > 0);
    }

    #[tokio::test]
    async fn virtual_time_covers_multi_day_delays() {
        let seq_def = seq(json!([
            {"type": "step", "id": "first", "handler": "h", "params": {}},
            {"type": "step", "id": "later", "handler": "h", "params": {},
             "delay": {"duration": 259_200_000}} // 3 days
        ]));
        let s = suite(json!([{
            "name": "three day delay",
            "mocks": [{"handler": "h", "type": "success", "output": {}}],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["first", "later"], "ordered": true}
            }
        }]));
        let started = std::time::Instant::now();
        let report = run(&seq_def, &s).await;
        assert!(report.passed, "{:?}", report.cases[0].failures);
        // ~3 days of logical time...
        assert!(report.cases[0].logical_duration_ms >= 259_200_000);
        // ...in well under a minute of wall time.
        assert!(started.elapsed() < std::time::Duration::from_secs(60));
    }

    #[tokio::test]
    async fn max_logical_duration_fails_overlong_case() {
        let seq_def = seq(json!([
            {"type": "step", "id": "later", "handler": "h", "params": {},
             "delay": {"duration": 86_400_000}} // 1 day
        ]));
        let s = suite(json!([{
            "name": "too slow",
            "max_logical_duration_ms": 3_600_000, // 1 hour budget
            "mocks": [{"handler": "h", "type": "success", "output": {}}],
            "expect": {}
        }]));
        let report = run(&seq_def, &s).await;
        assert!(!report.passed);
        assert!(
            report.cases[0]
                .failures
                .iter()
                .any(|f| f.contains("logical duration exceeded")),
            "{:?}",
            report.cases[0].failures
        );
    }

    #[tokio::test]
    async fn recorded_policy_uses_supplied_outputs() {
        let seq_def = seq(json!([
            {"type": "step", "id": "fetch", "handler": "fetcher", "params": {}}
        ]));
        let s = suite(json!([{
            "name": "replay recorded",
            "mocks": [{"handler": "fetcher", "type": "recorded"}],
            "expect": {
                "terminal_state": "completed",
                "assertions": [
                    {"output": {"block": "fetch", "path": "cached"}, "op": "equals", "value": true}
                ]
            }
        }]));
        let mut opts = RunOptions::default();
        opts.recorded_outputs
            .insert("fetch".to_string(), json!({"cached": true}));
        let report = run_suite(&seq_def, &s, &opts).await.unwrap();
        assert!(report.passed, "{:?}", report.cases[0].failures);
    }

    #[tokio::test]
    async fn recorded_policy_without_recording_fails_explicitly() {
        let seq_def = seq(json!([
            {"type": "step", "id": "fetch", "handler": "fetcher", "params": {}}
        ]));
        let s = suite(json!([{
            "name": "missing recording",
            "mocks": [{"handler": "fetcher", "type": "recorded"}],
            "expect": {}
        }]));
        let report = run(&seq_def, &s).await;
        assert!(!report.passed);
        assert!(
            report.cases[0]
                .failures
                .iter()
                .any(|f| f.contains("no recorded output") && f.contains("'fetch'")),
            "{:?}",
            report.cases[0].failures
        );
    }

    #[tokio::test]
    async fn router_case_asserts_skipped_branch() {
        let seq_def = seq(json!([
            {"type": "router", "id": "route", "routes": [
                {"condition": "kind == \"a\"", "blocks": [
                    {"type": "step", "id": "route_a", "handler": "h", "params": {}}
                ]},
                {"condition": "kind == \"b\"", "blocks": [
                    {"type": "step", "id": "route_b", "handler": "h", "params": {}}
                ]}
            ]}
        ]));
        let s = suite(json!([{
            "name": "takes route a",
            "input": {"kind": "a"},
            "mocks": [{"handler": "h", "type": "success", "output": {}}],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["route_a"], "ordered": false},
                "skipped_blocks": ["route_b"]
            }
        }]));
        let report = run(&seq_def, &s).await;
        assert!(report.passed, "{:?}", report.cases[0].failures);
    }

    #[tokio::test]
    async fn suite_report_aggregates_multiple_cases() {
        let seq_def = seq(json!([
            {"type": "step", "id": "s", "handler": "h", "params": {}}
        ]));
        let s = suite(json!([
            {
                "name": "passes",
                "mocks": [{"handler": "h", "type": "success", "output": {}}],
                "expect": {"terminal_state": "completed"}
            },
            {
                "name": "fails",
                "mocks": [{"handler": "h", "type": "success", "output": {}}],
                "expect": {"terminal_state": "failed"}
            }
        ]));
        let report = run(&seq_def, &s).await;
        assert!(!report.passed);
        assert_eq!(report.cases.len(), 2);
        assert!(report.cases[0].passed);
        assert!(!report.cases[1].passed);
        assert_eq!(report.failed_cases().len(), 1);
    }

    #[tokio::test]
    async fn invalid_suite_is_rejected_before_running() {
        let seq_def = seq(json!([
            {"type": "step", "id": "s", "handler": "h", "params": {}}
        ]));
        let mut s = suite(json!([{"name": "x", "expect": {}}]));
        s.schema_version = 99;
        let err = run_suite(&seq_def, &s, &RunOptions::default())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("schema_version"), "{err}");
    }

    #[test]
    fn sequence_handlers_discovers_nested_steps() {
        let seq_def = seq(json!([
            {"type": "step", "id": "top", "handler": "h1", "params": {}},
            {"type": "parallel", "id": "par", "branches": [
                [{"type": "step", "id": "in_par", "handler": "h2", "params": {}}],
                [{"type": "step", "id": "in_par2", "handler": "h1", "params": {}}]
            ]}
        ]));
        let mut handlers = sequence_handlers(&seq_def);
        handlers.sort();
        assert_eq!(handlers, vec!["h1", "h2"]);
    }
}
