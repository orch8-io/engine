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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;

use orch8_types::contract::{
    CaseReport, ChildExecutionReport, ContractCase, ContractDefinition, ContractSuite, MockOutcome,
    MockPolicy, SuiteReport, UnmockedHandlerPolicy, check_call_counts, check_path,
    evaluate_assertion,
};
use orch8_types::error::StepError;
use orch8_types::execution::NodeState;
use orch8_types::ids::{BlockId, InstanceId, SequenceId};
use orch8_types::instance::TaskInstance;
use orch8_types::output::BlockOutput;
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef, visit_steps};
use orch8_types::signal::SignalType;

use crate::engine::CreateInstanceOptions;
use crate::error::Error;
use crate::{Clock, Engine, ExecutionContext, InstanceState, ManualClock, SharedClock, Storage};

mod pinned;
mod selectors;

use selectors::CaseSelectors;

/// Validate a contract's pinned definition graph and return the reachable
/// child revisions in dependency-first order.
///
/// # Errors
/// Returns an error for invalid, missing, duplicate, or cyclic definitions.
pub fn resolve_pinned_children<'a>(
    root: &'a SequenceDefinition,
    supplied: impl IntoIterator<Item = &'a SequenceDefinition>,
) -> Result<Vec<&'a SequenceDefinition>, Error> {
    pinned::resolve(root, supplied)
}

/// Default tick budget per case when the case doesn't set `max_ticks`.
const DEFAULT_MAX_TICKS: u32 = 5_000;

/// Options shared by every case in one runner invocation.
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Local definitions for explicitly versioned child references. Definitions
    /// must belong to the root's tenant and namespace.
    pub child_sequences: Vec<SequenceDefinition>,
    /// Recorded root block outputs for [`MockPolicy::Recorded`], keyed by
    /// block id (e.g. loaded from an `orch8 test record` fixture).
    pub recorded_outputs: HashMap<String, Value>,
    /// Recordings for exact revisions, including children. These take
    /// precedence over a bare recording when the revision is the root.
    pub definition_recorded_outputs: HashMap<ContractDefinition, HashMap<String, Value>>,
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
            child_sequences: Vec::new(),
            recorded_outputs: HashMap::new(),
            definition_recorded_outputs: HashMap::new(),
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

    let children = resolve_pinned_children(seq, &opts.child_sequences)?;
    let selectors = suite
        .cases
        .iter()
        .map(|case| CaseSelectors::resolve(seq, &children, case))
        .collect::<Result<Vec<_>, _>>()?;
    let mut cases = Vec::with_capacity(suite.cases.len());
    for (case, selectors) in suite.cases.iter().zip(selectors) {
        cases.push(
            run_resolved_case(
                seq,
                &children,
                suite.unmocked_handlers,
                case,
                opts,
                selectors,
            )
            .await?,
        );
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
    /// Instance and block id -> attempts made so far (for `Attempts` policies).
    attempts: HashMap<(InstanceId, String), u32>,
    /// Handlers that were called without a mock (when policy = Fail).
    unmocked_called: Vec<String>,
    /// Blocks whose `Recorded` mock had no recording.
    missing_recordings: Vec<String>,
}

/// Run a single case. Infrastructure errors (engine build, storage)
/// surface as `Err`; behavioral divergences land in the report.
///
/// # Errors
/// Returns an error for an invalid case/sequence or infrastructure failure.
pub async fn run_case(
    seq: &SequenceDefinition,
    unmocked_policy: UnmockedHandlerPolicy,
    case: &ContractCase,
    opts: &RunOptions,
) -> Result<CaseReport, Error> {
    case.validate()
        .map_err(|e| Error::Config(format!("invalid contract case '{}': {e}", case.name)))?;
    let children = resolve_pinned_children(seq, &opts.child_sequences)?;
    let selectors = CaseSelectors::resolve(seq, &children, case)?;
    run_resolved_case(seq, &children, unmocked_policy, case, opts, selectors).await
}

#[allow(clippy::too_many_lines)] // one linear pipeline: mocks → engine → drive → evaluate
async fn run_resolved_case(
    seq: &SequenceDefinition,
    children: &[&SequenceDefinition],
    unmocked_policy: UnmockedHandlerPolicy,
    case: &ContractCase,
    opts: &RunOptions,
    selectors: CaseSelectors,
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
    let by_handler = Arc::new(selectors.by_handler);
    let by_block = Arc::new(selectors.by_block);
    let mut recordings = HashMap::from([(seq.id, opts.recorded_outputs.clone())]);
    let identities: HashMap<_, _> = std::iter::once(seq)
        .chain(children.iter().copied())
        .map(|definition| {
            let identity = ContractDefinition {
                name: definition.name.clone(),
                version: definition.version,
            };
            if let Some(outputs) = opts.definition_recorded_outputs.get(&identity) {
                recordings
                    .entry(definition.id)
                    .or_default()
                    .extend(outputs.clone());
            }
            (definition.id, identity)
        })
        .collect();
    let recorded = Arc::new(recordings);
    let state = Arc::new(Mutex::new(MockState::default()));
    let root_definition = seq.id;
    let has_children = !children.is_empty();
    let identities = Arc::new(identities);

    // --- embedded engine with every sequence handler mocked ---
    let clock = Arc::new(ManualClock::new(opts.start_time));
    let shared = SharedClock::from_arc(Arc::clone(&clock) as Arc<dyn Clock>);
    let mut builder = Engine::builder()
        .storage(Storage::contract_sandbox(shared.clone()))
        .tenant(seq.tenant_id.as_str())
        .clock(shared);

    let handler_names: std::collections::BTreeSet<_> = std::iter::once(seq)
        .chain(children.iter().copied())
        .flat_map(SequenceDefinition::handler_names)
        .collect();
    for handler_name in &handler_names {
        let handler_name = handler_name.clone();
        let by_handler = Arc::clone(&by_handler);
        let by_block = Arc::clone(&by_block);
        let recorded = Arc::clone(&recorded);
        let state = Arc::clone(&state);
        let identities = Arc::clone(&identities);
        builder = builder.handler(&handler_name.clone(), move |ctx: crate::StepContext| {
            let handler_name = handler_name.clone();
            let by_handler = Arc::clone(&by_handler);
            let by_block = Arc::clone(&by_block);
            let recorded = Arc::clone(&recorded);
            let state = Arc::clone(&state);
            let identities = Arc::clone(&identities);
            async move {
                let block_id = ctx.block_id.as_str().to_string();
                let definition_id = if has_children {
                    mock_definition_id(&ctx).await?
                } else {
                    root_definition
                };
                let selector = (definition_id, block_id.clone());
                let policy = by_block
                    .get(&selector)
                    .or_else(|| by_handler.get(&handler_name));

                let mut guard = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *guard.calls.entry(handler_name.clone()).or_insert(0) += 1;
                let attempt = guard
                    .attempts
                    .entry((ctx.instance_id, block_id.clone()))
                    .or_insert(0);
                let attempt_index = *attempt as usize;
                *attempt += 1;

                match policy {
                    Some(MockPolicy::Success { output }) => Ok(output.clone()),
                    Some(MockPolicy::Failure { message, retryable }) => {
                        mock_failure(message, *retryable)
                    }
                    Some(MockPolicy::Attempts { attempts }) => {
                        let Some(outcome) = attempts.get(attempt_index).or_else(|| attempts.last())
                        else {
                            return Err(StepError::Permanent {
                                message: format!(
                                    "contract: attempts mock for block '{block_id}' has an empty attempts list"
                                ),
                                details: None,
                            });
                        };
                        match outcome {
                            MockOutcome::Success { output } => Ok(output.clone()),
                            MockOutcome::Failure { message, retryable } => {
                                mock_failure(message, *retryable)
                            }
                        }
                    }
                    Some(MockPolicy::Recorded) => {
                        if let Some(output) = recorded.get(&definition_id).and_then(|outputs| outputs.get(&block_id)) {
                            Ok(output.clone())
                        } else {
                            let block_label = identities
                                .get(&definition_id)
                                .filter(|_| definition_id != root_definition)
                                .map_or_else(|| block_id.clone(), |definition| {
                                    format!("{}@{}:{block_id}", definition.name, definition.version)
                                });
                            guard.missing_recordings.push(block_label.clone());
                            Err(StepError::Permanent {
                                message: format!(
                                    "contract: no recorded output for block '{block_label}'"
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
    let mut definitions = HashMap::new();
    for &child in children {
        let id = engine.upsert_sequence(child.clone()).await?;
        definitions.insert(id, child);
    }
    let seq_id = engine.upsert_sequence(seq.clone()).await?;
    definitions.insert(seq_id, seq);

    let mut context = ExecutionContext {
        data: case.input.clone(),
        config: case.config.clone().unwrap_or(Value::Null),
        ..Default::default()
    };
    // Contract handlers are deterministic test doubles, not externally
    // visible effects. Mark the sandbox as a dry run so the universal effect
    // ledger does not turn an intentional retryable mock failure into an
    // ambiguous effect that blocks the next attempt.
    context.runtime.dry_run = true;
    let instance_id = engine
        .create_instance(
            seq_id,
            CreateInstanceOptions {
                namespace: seq.namespace.clone(),
                context,
                // Pin the first fire to the case's explicit virtual epoch.
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
                block_id: BlockId::new(block_id),
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
            let (due, target) = next_contract_wake(&engine, inst, &definitions, now).await?;
            if due {
                continue;
            }
            match target {
                Some(jump_to) => {
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
    let (executed_blocks, outputs) = collect_outputs(raw_outputs, &initial_outputs);

    let mock_state = {
        let mut guard = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::take(&mut *guard)
    };
    let mut attempted_by_instance: HashMap<_, Vec<_>> = HashMap::new();
    for (instance, block) in mock_state.attempts.into_keys() {
        attempted_by_instance
            .entry(instance)
            .or_default()
            .push(block);
    }
    for attempted in attempted_by_instance.values_mut() {
        attempted.sort_unstable();
    }
    let attempted_blocks = attempted_by_instance
        .remove(&instance_id)
        .unwrap_or_default();
    let (child_executions, child_outputs) = if has_children {
        collect_child_executions(
            &engine,
            instance_id,
            &identities,
            &mut attempted_by_instance,
            &selectors.assertion_definitions,
        )
        .await?
    } else {
        (Vec::new(), HashMap::new())
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
    let absent = BTreeMap::new();
    for (assertion, definition_id) in case
        .expect
        .assertions
        .iter()
        .zip(selectors.assertion_definitions)
    {
        let actual = if definition_id == seq_id {
            std::slice::from_ref(&outputs)
        } else {
            child_outputs
                .get(&definition_id)
                .map_or_else(|| std::slice::from_ref(&absent), Vec::as_slice)
        };
        for actual in actual {
            if let Err(e) = evaluate_assertion(assertion, actual, &inst.context.data) {
                failures.push(e);
            }
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
        attempted_blocks,
        child_executions,
        handler_calls: mock_state.calls,
        ticks,
        logical_duration_ms,
    })
}

async fn mock_definition_id(ctx: &crate::StepContext) -> Result<SequenceId, StepError> {
    ctx.storage
        .get_instance(ctx.instance_id)
        .await
        .map_err(|error| StepError::Permanent {
            message: format!("contract: could not load mock instance: {error}"),
            details: None,
        })?
        .map(|instance| instance.sequence_id)
        .ok_or_else(|| StepError::Permanent {
            message: format!("contract: mock instance {} is missing", ctx.instance_id),
            details: None,
        })
}

type BlockOutputs = BTreeMap<String, Value>;
type ChildOutputs = HashMap<SequenceId, Vec<BlockOutputs>>;

fn collect_outputs(
    raw: Vec<BlockOutput>,
    seeded: &HashMap<String, Value>,
) -> (Vec<String>, BlockOutputs) {
    let mut executed = Vec::new();
    let mut seen = HashSet::new();
    let mut outputs = BTreeMap::new();
    for output in raw {
        let block = output.block_id.as_str();
        if block.starts_with('_') {
            continue; // engine-internal sentinel rows
        }
        if !seeded.contains_key(block) && seen.insert(block.to_owned()) {
            executed.push(block.to_owned());
        }
        outputs.insert(block.to_owned(), output.output); // last attempt wins
    }
    (executed, outputs)
}

async fn collect_child_executions(
    engine: &Engine,
    root: InstanceId,
    identities: &HashMap<SequenceId, ContractDefinition>,
    attempted: &mut HashMap<InstanceId, Vec<String>>,
    assertion_definitions: &[SequenceId],
) -> Result<(Vec<ChildExecutionReport>, ChildOutputs), Error> {
    let storage = engine.storage_backend();
    let mut queue = std::collections::VecDeque::from([root]);
    let mut reports = Vec::new();
    let mut outputs_by_definition: HashMap<_, Vec<_>> = HashMap::new();
    while let Some(parent) = queue.pop_front() {
        for child in storage.get_child_instances(parent).await? {
            queue.push_back(child.id);
            let Some(definition) = identities.get(&child.sequence_id) else {
                continue;
            };
            let (executed_blocks, outputs) =
                collect_outputs(storage.get_all_outputs(child.id).await?, &HashMap::new());
            if assertion_definitions.contains(&child.sequence_id) {
                outputs_by_definition
                    .entry(child.sequence_id)
                    .or_default()
                    .push(outputs);
            }
            reports.push(ChildExecutionReport {
                definition: definition.clone(),
                executed_blocks,
                attempted_blocks: attempted.remove(&child.id).unwrap_or_default(),
            });
        }
    }
    Ok((reports, outputs_by_definition))
}

async fn next_contract_wake(
    engine: &Engine,
    root: TaskInstance,
    definitions: &HashMap<SequenceId, &SequenceDefinition>,
    now: DateTime<Utc>,
) -> Result<(bool, Option<DateTime<Utc>>), Error> {
    let mut pending = vec![root];
    let mut target = None;
    while let Some(instance) = pending.pop() {
        if instance.state.is_terminal() {
            continue;
        }
        if let Some(fire) = instance.next_fire_at {
            // A due instance can be woken by the next tick without moving time.
            if fire <= now {
                return Ok((true, None));
            }
            target = target
                .into_iter()
                .chain(fire.checked_add_signed(chrono::Duration::seconds(1)))
                .min();
        }
        if instance.state == InstanceState::Waiting
            && let Some(definition) = definitions.get(&instance.sequence_id)
        {
            target = target
                .into_iter()
                .chain(next_waiting_gate_timeout(engine, definition, &instance, now).await?)
                .min();
        }
        if definitions.len() > 1 {
            pending.extend(
                engine
                    .storage_backend()
                    .get_child_instances(instance.id)
                    .await?,
            );
        }
    }
    Ok((false, target))
}

async fn next_waiting_gate_timeout(
    engine: &Engine,
    sequence: &SequenceDefinition,
    instance: &TaskInstance,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, Error> {
    let expires = |step: &StepDef, started: DateTime<Utc>| {
        let timeout = step.wait_for_input.as_ref()?.timeout?;
        let timeout = chrono::Duration::from_std(timeout).ok()?;
        let expiry = started.checked_add_signed(timeout)?;
        let wake = expiry.checked_add_signed(chrono::Duration::milliseconds(1))?;
        (wake > now).then_some(wake)
    };

    let nodes = engine
        .storage_backend()
        .get_execution_tree(instance.id)
        .await?;
    if nodes.is_empty() {
        let runtime = &instance.context.runtime;
        return Ok(sequence.blocks.iter().find_map(|block| {
            let BlockDefinition::Step(step) = block else {
                return None;
            };
            if runtime.current_step.as_ref()? != &step.id {
                return None;
            }
            expires(
                step,
                runtime.current_step_started_at.or(runtime.started_at)?,
            )
        }));
    }

    let mut steps = HashMap::new();
    for block in &sequence.blocks {
        visit_steps(block, &mut |step| {
            steps.insert(&step.id, step);
        });
    }
    Ok(nodes
        .iter()
        .filter(|node| node.state == NodeState::Waiting)
        .filter_map(|node| expires(steps.get(&node.block_id)?, node.started_at?))
        .min())
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
    async fn virtual_time_drives_waiting_approval_timeouts_in_flat_and_nested_steps() {
        let gate = json!({"type":"step","id":"approval","handler":"h",
            "wait_for_input":{"prompt":"Approve?","timeout":1000}});
        for blocks in [
            json!([gate]),
            json!([{"type":"parallel","id":"checks","branches":[[gate]]}]),
        ] {
            let definition = seq(blocks);
            let contract = suite(json!([{
                "name": "approval expires",
                "max_logical_duration_ms": 1001,
                "expect": {"terminal_state": "failed", "call_counts": [{"handler":"h","max":0}]}
            }]));
            let report = run(&definition, &contract).await;
            assert!(report.passed, "{:?}", report.cases[0].failures);
            assert_eq!(report.cases[0].logical_duration_ms, 1001);
        }
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
        assert_eq!(seq_def.handler_names(), vec!["h1", "h2"]);
    }

    /// Typed discovery must not register spurious mocks for step *params*
    /// that merely contain `id`/`handler` string keys.
    #[test]
    fn handler_discovery_ignores_params_shaped_like_steps() {
        let seq_def = seq(json!([
            {"type": "step", "id": "s", "handler": "real", "params": {
                "nested": {"id": "fake", "handler": "not_a_handler"}
            }}
        ]));
        assert_eq!(seq_def.handler_names(), vec!["real"]);
    }

    /// Floating child references are rejected even if a local revision exists.
    #[tokio::test]
    async fn sub_sequence_block_fails_fast() {
        let seq_def = seq(json!([
            {"type": "sub_sequence", "id": "child", "sequence_name": "other-seq"}
        ]));
        let s = suite(json!([{
            "name": "has-sub",
            "input": {},
            "expect": {"terminal_state": "completed"}
        }]));
        let err = run_suite(&seq_def, &s, &RunOptions::default())
            .await
            .expect_err("the child must be pinned");
        assert!(err.to_string().contains("sub_sequence"), "{err}");
        assert!(err.to_string().contains("other-seq"), "{err}");
    }

    /// Nested composites are scanned too: a `sub_sequence` hidden inside a
    /// parallel branch must also fail fast.
    #[tokio::test]
    async fn nested_sub_sequence_block_fails_fast() {
        let seq_def = seq(json!([
            {"type": "parallel", "id": "par", "branches": [
                [{"type": "sub_sequence", "id": "nested-child", "sequence_name": "other-seq"}]
            ]}
        ]));
        let s = suite(json!([{
            "name": "nested-sub",
            "input": {},
            "expect": {"terminal_state": "completed"}
        }]));
        let err = run_suite(&seq_def, &s, &RunOptions::default())
            .await
            .expect_err("the nested child must be pinned");
        assert!(err.to_string().contains("other-seq"), "{err}");
    }

    #[tokio::test]
    async fn exact_child_graph_runs_delayed_mock_under_virtual_time() {
        let root = seq(json!([
            {"type":"sub_sequence","id":"child","sequence_name":"nested","version":2}
        ]));
        let mut child = seq(json!([
            {"type":"sub_sequence","id":"grandchild","sequence_name":"leaf","version":3}
        ]));
        child.name = "nested".into();
        child.version = 2;
        let mut leaf = seq(json!([
            {"type":"step","id":"work","handler":"remote_work","params":{},
             "delay":{"duration":60000}}
        ]));
        leaf.name = "leaf".into();
        leaf.version = 3;
        let mut other = seq(json!([
            {"type":"step","id":"work","handler":"wrong_revision","params":{}}
        ]));
        other.name = "leaf".into();
        other.version = 4;
        let suite = suite(json!([{
            "name":"pinned",
            "input":{},
            "mocks":[{"handler":"remote_work","type":"success","output":{"ok":true}}],
            "expect":{"terminal_state":"completed"}
        }]));
        let opts = RunOptions {
            child_sequences: vec![other, child, leaf],
            ..RunOptions::default()
        };
        let result = run_suite(&root, &suite, &opts).await.unwrap();
        let report = &result.cases[0];
        assert!(report.passed, "{report:?}");
        assert_eq!(report.handler_calls.get("remote_work"), Some(&1));
        assert!(!report.handler_calls.contains_key("wrong_revision"));
        assert!(report.logical_duration_ms >= 60_000);
        assert!(report.attempted_blocks.is_empty());
    }

    #[tokio::test]
    async fn definition_qualified_selectors_keep_same_named_blocks_separate() {
        let root = seq(json!([
            {"type":"step","id":"validate","handler":"validator","params":{}},
            {"type":"sub_sequence","id":"child","sequence_name":"nested","version":2,
             "child_result":"reference"},
            {"type":"sub_sequence","id":"other_child","sequence_name":"nested","version":3,
             "child_result":"reference"}
        ]));
        let mut child = seq(json!([
            {"type":"step","id":"validate","handler":"validator","params":{}}
        ]));
        child.name = "nested".into();
        child.version = 2;
        let mut other = child.clone();
        other.version = 3;
        other.id = SequenceId::new();
        let root_scope = json!({"name":root.name,"version":root.version});
        let child_scope = json!({"name":child.name,"version":child.version});
        let other_scope = json!({"name":other.name,"version":other.version});
        let contract = suite(json!([{
            "name":"separate validation",
            "mocks":[
                {"block":"validate","definition":root_scope,"type":"recorded"},
                {"block":"validate","definition":child_scope,"type":"recorded"},
                {"block":"validate","definition":other_scope,"type":"success","output":"other"}
            ],
            "expect":{"terminal_state":"completed","assertions":[
                {"output":{"block":"validate","definition":root_scope,"path":""},
                 "op":"equals","value":"root"},
                {"output":{"block":"validate","definition":child_scope,"path":""},
                 "op":"equals","value":"child"},
                {"output":{"block":"validate","definition":other_scope,"path":""},
                 "op":"equals","value":"other"}
            ]}
        }]));
        let opts = RunOptions {
            child_sequences: vec![other, child],
            recorded_outputs: HashMap::from([("validate".into(), json!("root"))]),
            definition_recorded_outputs: HashMap::from([(
                serde_json::from_value(child_scope).unwrap(),
                HashMap::from([("validate".into(), json!("child"))]),
            )]),
            ..RunOptions::default()
        };
        let report = run_suite(&root, &contract, &opts).await.unwrap();
        let case = &report.cases[0];
        assert!(case.passed, "{case:?}");
        assert_eq!(case.attempted_blocks, ["validate"]);
        assert_eq!(case.child_executions.len(), 2);
        assert_eq!(case.child_executions[0].definition.version, 2);
        assert_eq!(case.child_executions[0].executed_blocks, ["validate"]);
        assert_eq!(case.child_executions[0].attempted_blocks, ["validate"]);
        assert_eq!(case.child_executions[1].definition.version, 3);

        let mut missing = opts.clone();
        missing.definition_recorded_outputs.clear();
        let report = run_suite(&root, &contract, &missing).await.unwrap();
        assert!(
            report.cases[0]
                .failures
                .iter()
                .any(|failure| failure.contains("no recorded output")
                    && failure.contains("nested@2:validate")),
            "{:?}",
            report.cases[0].failures
        );
    }

    #[tokio::test]
    async fn validation_rejects_ambiguous_unqualified_mocks_and_outputs() {
        let root = seq(json!([
            {"type":"step","id":"validate","handler":"validator","params":{}},
            {"type":"sub_sequence","id":"child","sequence_name":"nested","version":1}
        ]));
        let mut child = seq(json!([
            {"type":"step","id":"validate","handler":"validator","params":{}}
        ]));
        child.name = "nested".into();
        let opts = RunOptions {
            child_sequences: vec![child],
            ..RunOptions::default()
        };
        for contract in [
            suite(json!([{
                "name":"ambiguous mock",
                "mocks":[{"block":"validate","type":"success","output":null}],
                "expect":{}
            }])),
            suite(json!([{
                "name":"ambiguous output",
                "expect":{"assertions":[{"output":{"block":"validate","path":""},"op":"exists"}]}
            }])),
        ] {
            let error = run_suite(&root, &contract, &opts)
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("'validate' is ambiguous"), "{error}");
            assert!(error.contains("specify its definition"), "{error}");
        }
    }

    #[tokio::test]
    async fn child_failure_fails_its_parent_contract() {
        let root = seq(json!([
            {"type":"sub_sequence","id":"child","sequence_name":"nested","version":1}
        ]));
        let mut child = seq(json!([
            {"type":"step","id":"work","handler":"remote_work","params":{}}
        ]));
        child.name = "nested".into();
        let suite = suite(json!([{
            "name":"child failure",
            "input":{},
            "mocks":[{"handler":"remote_work","type":"failure","message":"unavailable"}],
            "expect":{"terminal_state":"failed"}
        }]));
        let opts = RunOptions {
            child_sequences: vec![child],
            ..RunOptions::default()
        };
        let result = run_suite(&root, &suite, &opts).await.unwrap();
        assert!(result.passed, "{:?}", result.cases);
        assert_eq!(result.cases[0].handler_calls.get("remote_work"), Some(&1));
    }

    #[tokio::test]
    async fn parent_can_cancel_while_waiting_for_a_pinned_child() {
        let root = seq(json!([
            {"type":"sub_sequence","id":"child","sequence_name":"nested","version":1}
        ]));
        let mut child = seq(json!([
            {"type":"step","id":"work","handler":"remote_work","params":{},
             "delay":{"duration":60000}}
        ]));
        child.name = "nested".into();
        let suite = suite(json!([{
            "name":"cancel parent",
            "input":{},
            "signals":[{"signal_type":"cancel","payload":{}}],
            "expect":{"terminal_state":"cancelled"}
        }]));
        let opts = RunOptions {
            child_sequences: vec![child],
            ..RunOptions::default()
        };
        let result = run_suite(&root, &suite, &opts).await.unwrap();
        assert!(result.passed, "{:?}", result.cases);
        assert!(result.cases[0].handler_calls.is_empty());
    }

    #[test]
    fn missing_and_cyclic_child_revisions_fail_before_execution() {
        let root = seq(json!([
            {"type":"sub_sequence","id":"child","sequence_name":"nested","version":2}
        ]));
        let mut child = seq(json!([
            {"type":"sub_sequence","id":"back","sequence_name":"contract-test-seq","version":1}
        ]));
        child.name = "nested".into();
        child.version = 2;
        let missing = pinned::resolve(&root, &[]).unwrap_err();
        assert!(missing.to_string().contains("nested' v2"), "{missing}");
        let cycle = pinned::resolve(&root, &[child]).unwrap_err();
        assert!(cycle.to_string().contains("cycle"), "{cycle}");
    }
}
