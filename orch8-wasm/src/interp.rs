//! Pure dry-run interpreter with a virtual clock.
//!
//! This is NOT the engine scheduler: there is no storage, no worker queue, no
//! crash recovery and no real I/O. It walks the block tree the way the engine
//! would, evaluates conditions with the engine's own expression evaluator,
//! runs the side-effect-free builtins for real, and records every handler
//! that would reach the outside world (HTTP, LLM, tools, workers, signals,
//! sub-sequences) as a "would call" entry with a mocked output. Time is
//! virtual: delays, `sleep`, retry backoff and loop poll intervals advance a
//! millisecond counter instead of waiting.

use std::collections::BTreeMap;

use orch8_types::context::ExecutionContext;
use orch8_types::sequence::{
    ABSplitDef, BlockDefinition, ForEachDef, LoopDef, RaceSemantics, RetryPolicy,
    SequenceDefinition, StepDef,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::expression;

/// Builtins the playground executes for real because they only touch the
/// instance context/state.
pub const LOCAL_HANDLERS: &[&str] = &[
    "noop",
    "log",
    "sleep",
    "fail",
    "transform",
    "assert",
    "set_state",
    "get_state",
    "delete_state",
    "merge_state",
];

/// Default instance id used for `ab_split` selection when none is given.
const DEFAULT_INSTANCE_ID: &str = "00000000-0000-7000-8000-000000000001";

/// Canned outcome for one block id, supplied by the caller.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mock {
    /// Output returned instead of running / stubbing the handler.
    #[serde(default)]
    pub output: Option<Value>,
    /// If set, the step fails with this message.
    #[serde(default)]
    pub error: Option<String>,
    /// Whether `error` is retryable (honours the step's retry policy).
    #[serde(default)]
    pub retryable: bool,
    /// Virtual time the call takes.
    #[serde(default)]
    pub duration_ms: u64,
    /// Fail only the first N attempts (retryable), then return `output`.
    #[serde(default)]
    pub fail_attempts: Option<u32>,
}

/// Options for [`run`].
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunOptions {
    /// Initial `context.data`.
    #[serde(default = "empty_object")]
    pub input: Value,
    /// Initial `context.config`.
    #[serde(default = "empty_object")]
    pub config: Value,
    /// Per-block canned outcomes keyed by block id.
    #[serde(default)]
    pub mocks: BTreeMap<String, Mock>,
    /// Upper bound on step attempts, so loops that never terminate in a
    /// dry run (their condition depends on real handler output) stop.
    #[serde(default = "default_max_ticks")]
    pub max_ticks: u32,
    /// Instance id used for deterministic `ab_split` selection — the same
    /// hash as the engine, so a real instance with this id takes the same
    /// variant.
    #[serde(default)]
    pub instance_id: Option<String>,
}

fn empty_object() -> Value {
    json!({})
}

const fn default_max_ticks() -> u32 {
    1_000
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            input: empty_object(),
            config: empty_object(),
            mocks: BTreeMap::new(),
            max_ticks: default_max_ticks(),
            instance_id: None,
        }
    }
}

enum Halt {
    Failed { block_id: String, message: String },
    TickLimit,
}

type Flow = Result<(), Halt>;

struct Interp<'a> {
    now: u64,
    ticks: u32,
    options: &'a RunOptions,
    instance_uuid: [u8; 16],
    context: ExecutionContext,
    outputs: Map<String, Value>,
    state: Map<String, Value>,
    timeline: Vec<Value>,
    would_call: Vec<Value>,
    notes: Vec<String>,
}

/// Run `sequence` to completion (or failure / tick limit) in virtual time.
pub fn run(sequence: &SequenceDefinition, options: &RunOptions) -> Value {
    let instance_id = options
        .instance_id
        .as_deref()
        .unwrap_or(DEFAULT_INSTANCE_ID);
    let instance_uuid = parse_uuid_bytes(instance_id).unwrap_or([0; 16]);
    let mut interp = Interp {
        now: 0,
        ticks: 0,
        options,
        instance_uuid,
        context: ExecutionContext {
            data: options.input.clone(),
            config: options.config.clone(),
            ..Default::default()
        },
        outputs: Map::new(),
        state: Map::new(),
        timeline: Vec::new(),
        would_call: Vec::new(),
        notes: Vec::new(),
    };
    if sequence.interceptors.is_some() {
        interp
            .notes
            .push("interceptors are not executed in the playground".into());
    }
    let result = interp.run_blocks(&sequence.blocks, "");
    let (status, error) = match result {
        Ok(()) => ("completed", Value::Null),
        Err(Halt::TickLimit) => (
            "tick_limit",
            json!({ "message": format!("stopped after {} step attempts (max_ticks)", options.max_ticks) }),
        ),
        Err(Halt::Failed { block_id, message }) => {
            if let Some(cleanup) = &sequence.on_failure {
                // Best-effort, like the engine: cleanup errors are swallowed.
                let _ = interp.run_blocks(cleanup, "on_failure");
            }
            (
                "failed",
                json!({ "block_id": block_id, "message": message }),
            )
        }
    };
    json!({
        "status": status,
        "error": error,
        "virtual_duration_ms": interp.now,
        "ticks": interp.ticks,
        "timeline": interp.timeline,
        "outputs": interp.outputs,
        "context": { "data": interp.context.data, "config": interp.context.config },
        "state": interp.state,
        "would_call": interp.would_call,
        "notes": interp.notes,
    })
}

fn parse_uuid_bytes(s: &str) -> Option<[u8; 16]> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(hex.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

fn join_path(parent: &str, id: &str) -> String {
    if parent.is_empty() {
        id.to_string()
    } else {
        format!("{parent}/{id}")
    }
}

fn block_id(block: &BlockDefinition) -> &str {
    match block {
        BlockDefinition::Step(d) => d.id.as_str(),
        BlockDefinition::Parallel(d) => d.id.as_str(),
        BlockDefinition::Race(d) => d.id.as_str(),
        BlockDefinition::Loop(d) => d.id.as_str(),
        BlockDefinition::ForEach(d) => d.id.as_str(),
        BlockDefinition::Router(d) => d.id.as_str(),
        BlockDefinition::TryCatch(d) => d.id.as_str(),
        BlockDefinition::SubSequence(d) => d.id.as_str(),
        BlockDefinition::ABSplit(d) => d.id.as_str(),
        BlockDefinition::CancellationScope(d) => d.id.as_str(),
        BlockDefinition::Saga(d) => d.id.as_str(),
    }
}

/// Deterministic weighted pick — mirrors `handlers/ab_split.rs` in the engine
/// (SHA-256 over instance-id bytes, NUL, block id; first 8 bytes big-endian
/// modulo total weight).
fn pick_variant(def: &ABSplitDef, instance_uuid: &[u8; 16]) -> Option<usize> {
    let total: u64 = def.variants.iter().map(|v| u64::from(v.weight)).sum();
    if total == 0 {
        return None;
    }
    let mut hasher = Sha256::new();
    hasher.update(instance_uuid);
    hasher.update(b"\0");
    hasher.update(def.id.as_str().as_bytes());
    let digest = hasher.finalize();
    let mut first = [0u8; 8];
    first.copy_from_slice(&digest[..8]);
    let target = u64::from_be_bytes(first) % total;
    let mut cumulative = 0u64;
    for (index, variant) in def.variants.iter().enumerate() {
        cumulative += u64::from(variant.weight);
        if target < cumulative {
            return Some(index);
        }
    }
    Some(def.variants.len() - 1)
}

fn backoff_ms(retry: &RetryPolicy, failed_attempt: u32) -> u64 {
    let initial = u64::try_from(retry.initial_backoff.as_millis()).unwrap_or(u64::MAX);
    let max = u64::try_from(retry.max_backoff.as_millis()).unwrap_or(u64::MAX);
    let exponent = i32::try_from(failed_attempt.saturating_sub(1)).unwrap_or(i32::MAX);
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let scaled = (initial as f64 * retry.backoff_multiplier.powi(exponent)).min(max as f64) as u64;
    scaled.min(max)
}

fn step_entry(id: &str, path: &str, handler: &str, at: u64, status: &str) -> Value {
    json!({
        "step_id": id,
        "path": path,
        "kind": "step",
        "handler": handler,
        "started_at_virtual_ms": at,
        "finished_at_virtual_ms": at,
        "status": status,
        "output": Value::Null,
    })
}

impl Interp<'_> {
    fn outputs_value(&self) -> Value {
        Value::Object(self.outputs.clone())
    }

    fn expr_value(&self, expr: &str) -> Value {
        expression::evaluate(expr, &self.context, &self.outputs_value())
    }

    fn truthy(&self, expr: &str) -> bool {
        expression::is_truthy(&self.expr_value(expr))
    }

    fn push(&mut self, entry: Value) -> usize {
        self.timeline.push(entry);
        self.timeline.len() - 1
    }

    fn decision(&mut self, id: &str, path: &str, kind: &str, detail: Value) {
        let mut entry = json!({
            "step_id": id,
            "path": path,
            "kind": kind,
            "handler": Value::Null,
            "started_at_virtual_ms": self.now,
            "finished_at_virtual_ms": self.now,
            "status": "decision",
        });
        entry["output"] = detail;
        self.push(entry);
    }

    fn run_blocks(&mut self, blocks: &[BlockDefinition], parent: &str) -> Flow {
        for block in blocks {
            self.run_block(block, parent)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn run_block(&mut self, block: &BlockDefinition, parent: &str) -> Flow {
        let id = block_id(block).to_string();
        let path = join_path(parent, &id);
        match block {
            BlockDefinition::Step(step) => self.run_step(step, &path),
            BlockDefinition::Parallel(def) => self.run_parallel(&def.branches, &path),
            BlockDefinition::Race(def) => self.run_race(&def.branches, &def.semantics, &id, &path),
            BlockDefinition::Loop(def) => self.run_loop(def, &id, &path),
            BlockDefinition::ForEach(def) => self.run_for_each(def, &id, &path),
            BlockDefinition::Router(def) => {
                let chosen = def
                    .routes
                    .iter()
                    .position(|route| self.truthy(&route.condition));
                if let Some(index) = chosen {
                    self.decision(&id, &path, "router", json!({ "route": index }));
                    self.run_blocks(&def.routes[index].blocks, &path)
                } else {
                    self.decision(&id, &path, "router", json!({ "route": "default" }));
                    match &def.default {
                        Some(blocks) => self.run_blocks(blocks, &path),
                        None => Ok(()),
                    }
                }
            }
            BlockDefinition::TryCatch(def) => {
                let outcome = match self.run_blocks(&def.try_block, &format!("{path}/try")) {
                    Err(Halt::TickLimit) => return Err(Halt::TickLimit),
                    Err(Halt::Failed { block_id, message }) => {
                        self.decision(
                            &id,
                            &path,
                            "try_catch",
                            json!({ "caught": { "block_id": block_id, "message": message } }),
                        );
                        self.run_blocks(&def.catch_block, &format!("{path}/catch"))
                    }
                    Ok(()) => Ok(()),
                };
                if let Some(finally) = &def.finally_block {
                    self.run_blocks(finally, &format!("{path}/finally"))?;
                }
                outcome
            }
            BlockDefinition::SubSequence(def) => {
                self.tick()?;
                let output = json!({ "dry_run": true, "sequence_name": def.sequence_name });
                self.would_call.push(json!({
                    "step_id": id,
                    "handler": "sub_sequence",
                    "at_virtual_ms": self.now,
                    "params": { "sequence_name": def.sequence_name, "version": def.version, "input": def.input },
                }));
                self.outputs.insert(id.clone(), output.clone());
                let mut entry = step_entry(&id, &path, "sub_sequence", self.now, "mocked");
                entry["kind"] = json!("sub_sequence");
                entry["attempt"] = json!(1);
                entry["output"] = output;
                self.push(entry);
                Ok(())
            }
            BlockDefinition::ABSplit(def) => {
                let Some(index) = pick_variant(def, &self.instance_uuid) else {
                    return Err(Halt::Failed {
                        block_id: id,
                        message: "ab_split: total weight is zero".into(),
                    });
                };
                let name = def.variants[index].name.clone();
                self.outputs
                    .insert(id.clone(), json!({ "variant": name, "index": index }));
                self.decision(&id, &path, "ab_split", json!({ "variant": name }));
                self.run_blocks(&def.variants[index].blocks, &path)
            }
            BlockDefinition::CancellationScope(def) => self.run_blocks(&def.blocks, &path),
            BlockDefinition::Saga(def) => {
                let mut completed = Vec::new();
                for saga_step in &def.steps {
                    match self.run_block(&saga_step.action, &path) {
                        Ok(()) => completed.push(saga_step),
                        Err(Halt::TickLimit) => return Err(Halt::TickLimit),
                        Err(Halt::Failed { block_id, message }) => {
                            self.decision(
                                &id,
                                &path,
                                "saga",
                                json!({ "compensating": completed.len(), "failed": block_id }),
                            );
                            for done in completed.iter().rev() {
                                if let Some(compensation) = &done.compensation
                                    && let Err(Halt::TickLimit) =
                                        self.run_block(compensation, &format!("{path}/compensate"))
                                {
                                    return Err(Halt::TickLimit);
                                }
                            }
                            return Err(Halt::Failed { block_id, message });
                        }
                    }
                }
                Ok(())
            }
        }
    }

    fn run_parallel(&mut self, branches: &[Vec<BlockDefinition>], path: &str) -> Flow {
        let start = self.now;
        let mut end = start;
        let mut first_failure = None;
        for (index, branch) in branches.iter().enumerate() {
            self.now = start;
            let result = self.run_blocks(branch, &format!("{path}[{index}]"));
            end = end.max(self.now);
            match result {
                Err(Halt::TickLimit) => return Err(Halt::TickLimit),
                Err(failure) if first_failure.is_none() => first_failure = Some(failure),
                _ => {}
            }
        }
        self.now = end;
        first_failure.map_or(Ok(()), Err)
    }

    fn run_race(
        &mut self,
        branches: &[Vec<BlockDefinition>],
        semantics: &RaceSemantics,
        id: &str,
        path: &str,
    ) -> Flow {
        struct BranchRun {
            index: usize,
            finished: u64,
            ok: bool,
            entries: std::ops::Range<usize>,
        }
        let start = self.now;
        let mut runs = Vec::new();
        for (index, branch) in branches.iter().enumerate() {
            self.now = start;
            let first_entry = self.timeline.len();
            let result = self.run_blocks(branch, &format!("{path}[{index}]"));
            if matches!(result, Err(Halt::TickLimit)) {
                return Err(Halt::TickLimit);
            }
            runs.push(BranchRun {
                index,
                finished: self.now,
                ok: result.is_ok(),
                entries: first_entry..self.timeline.len(),
            });
        }
        let winner = runs
            .iter()
            .filter(|r| matches!(semantics, RaceSemantics::FirstToResolve) || r.ok)
            .min_by_key(|r| (r.finished, r.index))
            .map(|r| (r.index, r.finished, r.ok));
        for run in &runs {
            if Some(run.index) != winner.map(|w| w.0) {
                for entry in &mut self.timeline[run.entries.clone()] {
                    entry["status"] = json!("cancelled");
                }
            }
        }
        let Some((index, finished, ok)) = winner else {
            self.now = runs.iter().map(|r| r.finished).max().unwrap_or(start);
            return Err(Halt::Failed {
                block_id: id.to_string(),
                message: "race: no branch succeeded".into(),
            });
        };
        self.now = finished;
        self.decision(id, path, "race", json!({ "winner_branch": index }));
        if ok {
            Ok(())
        } else {
            Err(Halt::Failed {
                block_id: id.to_string(),
                message: format!("race: winning branch {index} failed"),
            })
        }
    }

    fn run_loop(&mut self, def: &LoopDef, id: &str, path: &str) -> Flow {
        let poll_ms = def.poll_interval.unwrap_or(0).saturating_mul(1000);
        let mut iterations = 0u32;
        while iterations < def.max_iterations && self.truthy(&def.condition) {
            if iterations > 0 {
                self.now = self.now.saturating_add(poll_ms);
            }
            self.decision(id, path, "loop", json!({ "iteration": iterations }));
            match self.run_blocks(&def.body, &format!("{path}#{iterations}")) {
                Err(Halt::TickLimit) => return Err(Halt::TickLimit),
                Err(failure) if !def.continue_on_error => return Err(failure),
                _ => {}
            }
            iterations += 1;
            if let Some(break_on) = &def.break_on
                && self.truthy(break_on)
            {
                break;
            }
        }
        self.outputs
            .insert(id.to_string(), json!({ "iterations": iterations }));
        Ok(())
    }

    fn run_for_each(&mut self, def: &ForEachDef, id: &str, path: &str) -> Flow {
        let collection = if def.collection.contains("{{") {
            self.expr_value(&def.collection)
        } else {
            let mut current = &self.context.data;
            for part in def.collection.split('.') {
                current = current.get(part).unwrap_or(&Value::Null);
            }
            current.clone()
        };
        if !collection.is_array() {
            self.notes.push(format!(
                "for_each `{id}`: collection `{}` is not an array in the dry-run context; no iterations",
                def.collection
            ));
        }
        let items = collection.as_array().cloned().unwrap_or_default();
        let limit = usize::try_from(def.max_iterations).unwrap_or(usize::MAX);
        for (index, item) in items.iter().take(limit).enumerate() {
            if let Some(data) = self.context.data.as_object_mut() {
                data.insert(def.item_var.clone(), item.clone());
            }
            self.decision(
                id,
                path,
                "for_each",
                json!({ "index": index, "item": item }),
            );
            self.run_blocks(&def.body, &format!("{path}#{index}"))?;
        }
        if let Some(data) = self.context.data.as_object_mut() {
            data.remove(&def.item_var);
        }
        self.outputs.insert(
            id.to_string(),
            json!({ "iterations": items.len().min(limit) }),
        );
        Ok(())
    }

    fn tick(&mut self) -> Flow {
        if self.ticks >= self.options.max_ticks {
            return Err(Halt::TickLimit);
        }
        self.ticks += 1;
        Ok(())
    }

    fn resolve_params(&mut self, value: &Value) -> Value {
        match value {
            Value::String(s) => self.resolve_string(s),
            Value::Array(items) => {
                Value::Array(items.iter().map(|v| self.resolve_params(v)).collect())
            }
            Value::Object(map) => Value::Object(
                map.iter()
                    .map(|(k, v)| (k.clone(), self.resolve_params(v)))
                    .collect(),
            ),
            other => other.clone(),
        }
    }

    /// Resolve `{{ expr }}` templates. A string that is exactly one template
    /// keeps the value's JSON type; embedded templates are stringified.
    /// Pipe filters (`| upper`) are engine-only and left unresolved.
    fn resolve_string(&mut self, s: &str) -> Value {
        if !s.contains("{{") {
            return Value::String(s.to_string());
        }
        let has_filter = |inner: &str| inner.replace("||", "").contains('|');
        let trimmed = s.trim();
        if trimmed.starts_with("{{")
            && trimmed.ends_with("}}")
            && trimmed.matches("{{").count() == 1
        {
            let inner = &trimmed[2..trimmed.len() - 2];
            if has_filter(inner) {
                self.notes.push(format!(
                    "template filter not supported in the playground: {s}"
                ));
                return Value::String(s.to_string());
            }
            return self.expr_value(inner);
        }
        let mut out = String::new();
        let mut rest = s;
        while let Some(start) = rest.find("{{") {
            out.push_str(&rest[..start]);
            let after = &rest[start + 2..];
            let Some(end) = after.find("}}") else {
                out.push_str(&rest[start..]);
                rest = "";
                break;
            };
            let inner = &after[..end];
            if has_filter(inner) {
                self.notes.push(format!(
                    "template filter not supported in the playground: {inner}"
                ));
                out.push_str(&rest[start..start + 2 + end + 2]);
            } else {
                match self.expr_value(inner) {
                    Value::String(text) => out.push_str(&text),
                    Value::Null => {}
                    other => out.push_str(&other.to_string()),
                }
            }
            rest = &after[end + 2..];
        }
        out.push_str(rest);
        Value::String(out)
    }

    fn run_step(&mut self, step: &StepDef, path: &str) -> Flow {
        let id = step.id.as_str().to_string();
        if let Some(guard) = &step.when {
            let passed =
                expression::evaluate_condition_strict(guard, &self.context, &self.outputs_value())
                    .map_err(|error| Halt::Failed {
                        block_id: id.clone(),
                        message: format!("invalid `when` guard: {error}"),
                    })?;
            if !passed {
                self.push(step_entry(&id, path, &step.handler, self.now, "skipped"));
                return Ok(());
            }
        }
        if let Some(delay) = &step.delay {
            let ms = u64::try_from(delay.duration.as_millis()).unwrap_or(u64::MAX);
            if delay.fire_at_local.is_some() || delay.business_days_only {
                self.notes.push(format!(
                    "step `{id}`: fire_at_local / business_days_only depend on the real calendar; the playground applies only `duration`"
                ));
            }
            self.now = self.now.saturating_add(ms);
        }
        if step.wait_for_input.is_some() {
            self.notes.push(format!(
                "step `{id}`: human input is not awaited in the playground; mock it via `mocks.{id}`"
            ));
        }
        let params = self.resolve_params(&step.params);
        let max_attempts = step.retry.as_ref().map_or(1, |r| r.max_attempts.max(1));
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            self.tick()?;
            let started = self.now;
            let (status, result) = self.dispatch(step, &params, attempt);
            let mut entry = step_entry(&id, path, &step.handler, started, status);
            entry["finished_at_virtual_ms"] = json!(self.now);
            entry["attempt"] = json!(attempt);
            match result {
                Ok(output) => {
                    entry["output"] = output.clone();
                    self.push(entry);
                    self.outputs.insert(id, output);
                    return Ok(());
                }
                Err((message, retryable)) => {
                    entry["status"] = json!("failed");
                    entry["error"] = json!(message);
                    self.push(entry);
                    if retryable
                        && attempt < max_attempts
                        && let Some(retry) = &step.retry
                    {
                        self.now = self.now.saturating_add(backoff_ms(retry, attempt));
                        continue;
                    }
                    return Err(Halt::Failed {
                        block_id: id,
                        message,
                    });
                }
            }
        }
    }

    /// Execute one attempt. Returns the timeline status and the result;
    /// errors carry `(message, retryable)`.
    fn dispatch(
        &mut self,
        step: &StepDef,
        params: &Value,
        attempt: u32,
    ) -> (&'static str, Result<Value, (String, bool)>) {
        let id = step.id.as_str();
        if let Some(mock) = self.options.mocks.get(id).cloned() {
            self.now = self.now.saturating_add(mock.duration_ms);
            let failing = match mock.fail_attempts {
                Some(n) => attempt <= n,
                None => mock.error.is_some(),
            };
            if failing {
                let message = mock.error.unwrap_or_else(|| "mocked failure".into());
                let retryable = mock.retryable || mock.fail_attempts.is_some();
                return ("mocked", Err((message, retryable)));
            }
            return ("mocked", Ok(mock.output.unwrap_or_else(|| json!({}))));
        }
        if LOCAL_HANDLERS.contains(&step.handler.as_str()) {
            return ("completed", self.run_local(&step.handler, params));
        }
        self.would_call.push(json!({
            "step_id": id,
            "handler": step.handler,
            "at_virtual_ms": self.now,
            "params": params,
        }));
        (
            "mocked",
            Ok(json!({ "dry_run": true, "handler": step.handler })),
        )
    }

    fn run_local(&mut self, handler: &str, params: &Value) -> Result<Value, (String, bool)> {
        let str_param = |key: &str| params.get(key).and_then(Value::as_str).map(str::to_string);
        let required = |name: &str, key: &str, kind: &str| {
            (format!("{name}: `{key}` ({kind}) is required"), false)
        };
        match handler {
            "log" => Ok(json!({
                "message": params.get("message").cloned().unwrap_or_else(|| json!("no message"))
            })),
            "sleep" => {
                let ms = params
                    .get("duration_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or(100);
                self.now = self.now.saturating_add(ms);
                Ok(json!({ "slept_ms": ms }))
            }
            "fail" => Err((
                str_param("message").unwrap_or_else(|| "forced failure".into()),
                params
                    .get("retryable")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            )),
            "transform" => Ok(params.clone()),
            "assert" => {
                let condition = str_param("condition")
                    .ok_or_else(|| required("assert", "condition", "string"))?;
                // The engine evaluates `assert` against the context only.
                if expression::evaluate_condition(&condition, &self.context, &json!({})) {
                    Ok(json!({ "condition": condition, "passed": true }))
                } else {
                    let message = str_param("message").unwrap_or_else(|| "assertion failed".into());
                    Err((format!("assert: {message} (condition: {condition})"), false))
                }
            }
            "set_state" => {
                let key = str_param("key").ok_or_else(|| required("set_state", "key", "string"))?;
                let value = params.get("value").cloned().unwrap_or(Value::Null);
                self.state.insert(key.clone(), value.clone());
                Ok(json!({ "key": key, "value": value }))
            }
            "get_state" => {
                let key = str_param("key").ok_or_else(|| required("get_state", "key", "string"))?;
                let value = self.state.get(&key).cloned().unwrap_or(Value::Null);
                Ok(json!({ "key": key, "value": value }))
            }
            "delete_state" => {
                let key =
                    str_param("key").ok_or_else(|| required("delete_state", "key", "string"))?;
                self.state.remove(&key);
                Ok(json!({ "key": key, "deleted": true }))
            }
            "merge_state" => {
                let values = params
                    .get("values")
                    .and_then(Value::as_object)
                    .ok_or_else(|| required("merge_state", "values", "object"))?;
                let keys: Vec<String> = values.keys().cloned().collect();
                for (k, v) in values {
                    self.state.insert(k.clone(), v.clone());
                }
                Ok(json!({ "merged_keys": keys }))
            }
            // "noop" and anything else in LOCAL_HANDLERS without state.
            _ => Ok(json!({})),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::decode;

    fn run_json(sequence: &str, options: &RunOptions) -> Value {
        let decoded = decode(sequence).expect("valid sequence");
        run(&decoded.sequence, options)
    }

    fn with_input(input: Value) -> RunOptions {
        RunOptions {
            input,
            ..Default::default()
        }
    }

    fn statuses(result: &Value) -> Vec<String> {
        result["timeline"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["status"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn delay_and_sleep_advance_virtual_time() {
        let r = run_json(
            r#"{"blocks":[
              {"type":"step","id":"a","handler":"log","params":{"message":"hi {{ data.name }}"}},
              {"type":"step","id":"b","handler":"sleep","params":{"duration_ms":5000},"delay":{"duration":60000}}
            ]}"#,
            &with_input(json!({"name": "Ada"})),
        );
        assert_eq!(r["status"], "completed");
        assert_eq!(r["virtual_duration_ms"], 65_000);
        assert_eq!(r["outputs"]["a"]["message"], "hi Ada");
        assert_eq!(r["timeline"][1]["started_at_virtual_ms"], 60_000);
        assert_eq!(r["timeline"][1]["finished_at_virtual_ms"], 65_000);
    }

    #[test]
    fn whole_string_template_keeps_json_type() {
        let r = run_json(
            r#"{"blocks":[{"type":"step","id":"t","handler":"transform","params":{"n":"{{ data.n + 1 }}"}}]}"#,
            &with_input(json!({"n": 41})),
        );
        assert_eq!(r["outputs"]["t"]["n"], 42.0);
    }

    #[test]
    fn external_handlers_are_recorded_not_called() {
        let r = run_json(
            r#"{"blocks":[{"type":"step","id":"ask","handler":"llm_call","params":{"prompt":"{{ data.q }}"}}]}"#,
            &with_input(json!({"q": "why?"})),
        );
        assert_eq!(r["status"], "completed");
        assert_eq!(r["would_call"][0]["handler"], "llm_call");
        assert_eq!(r["would_call"][0]["params"]["prompt"], "why?");
        assert_eq!(r["timeline"][0]["status"], "mocked");
    }

    #[test]
    fn router_uses_engine_expressions_and_outputs() {
        let seq = r#"{"blocks":[
          {"type":"step","id":"score","handler":"transform","params":{"value":"{{ data.n }}"}},
          {"type":"router","id":"r","routes":[
            {"condition":"outputs.score.value > 10","blocks":[{"type":"step","id":"big","handler":"noop"}]}
          ],"default":[{"type":"step","id":"small","handler":"noop"}]}
        ]}"#;
        let big = run_json(seq, &with_input(json!({"n": 42})));
        assert!(big["outputs"].get("big").is_some());
        assert!(big["outputs"].get("small").is_none());
        let small = run_json(seq, &with_input(json!({"n": 1})));
        assert!(small["outputs"].get("small").is_some());
    }

    #[test]
    fn retry_backoff_uses_virtual_time_then_succeeds() {
        let mut mocks = BTreeMap::new();
        mocks.insert(
            "call".to_string(),
            Mock {
                output: Some(json!({"ok": true})),
                error: Some("503".into()),
                fail_attempts: Some(2),
                ..Default::default()
            },
        );
        let r = run_json(
            r#"{"blocks":[{"type":"step","id":"call","handler":"http_request","retry":{"max_attempts":3,"initial_backoff":1000,"backoff_multiplier":2.0}}]}"#,
            &RunOptions {
                mocks,
                ..Default::default()
            },
        );
        assert_eq!(r["status"], "completed");
        assert_eq!(r["virtual_duration_ms"], 3000); // 1000 + 2000
        assert_eq!(statuses(&r), vec!["failed", "failed", "mocked"]);
    }

    #[test]
    fn permanent_failure_runs_catch_and_finally() {
        let r = run_json(
            r#"{"blocks":[{"type":"try_catch","id":"tc",
              "try_block":[{"type":"step","id":"boom","handler":"fail","params":{"message":"nope"}}],
              "catch_block":[{"type":"step","id":"recover","handler":"noop"}],
              "finally_block":[{"type":"step","id":"cleanup","handler":"noop"}]}]}"#,
            &RunOptions::default(),
        );
        assert_eq!(r["status"], "completed");
        assert!(r["outputs"].get("recover").is_some());
        assert!(r["outputs"].get("cleanup").is_some());
    }

    #[test]
    fn uncaught_failure_fails_run_and_runs_on_failure() {
        let r = run_json(
            r#"{"blocks":[{"type":"step","id":"boom","handler":"fail"}],
               "on_failure":[{"type":"step","id":"notify","handler":"log"}]}"#,
            &RunOptions::default(),
        );
        assert_eq!(r["status"], "failed");
        assert_eq!(r["error"]["block_id"], "boom");
        assert!(r["outputs"].get("notify").is_some());
    }

    #[test]
    fn parallel_takes_the_longest_branch() {
        let r = run_json(
            r#"{"blocks":[{"type":"parallel","id":"p","branches":[
              [{"type":"step","id":"a","handler":"sleep","params":{"duration_ms":1000}}],
              [{"type":"step","id":"b","handler":"sleep","params":{"duration_ms":3000}}]
            ]}]}"#,
            &RunOptions::default(),
        );
        assert_eq!(r["virtual_duration_ms"], 3000);
        assert_eq!(r["timeline"][1]["started_at_virtual_ms"], 0);
    }

    #[test]
    fn race_picks_fastest_and_cancels_the_rest() {
        let r = run_json(
            r#"{"blocks":[{"type":"race","id":"r","branches":[
              [{"type":"step","id":"slow","handler":"sleep","params":{"duration_ms":3000}}],
              [{"type":"step","id":"fast","handler":"sleep","params":{"duration_ms":1000}}]
            ]}]}"#,
            &RunOptions::default(),
        );
        assert_eq!(r["virtual_duration_ms"], 1000);
        assert_eq!(statuses(&r), vec!["cancelled", "completed", "decision"]);
    }

    #[test]
    fn for_each_binds_item_var_and_cleans_up() {
        let r = run_json(
            r#"{"blocks":[{"type":"for_each","id":"fe","collection":"users","body":[
              {"type":"step","id":"greet","handler":"log","params":{"message":"hi {{ data.item }}"}}
            ]}]}"#,
            &with_input(json!({"users": ["a", "b"]})),
        );
        assert_eq!(r["status"], "completed");
        assert_eq!(r["outputs"]["fe"]["iterations"], 2);
        assert_eq!(r["outputs"]["greet"]["message"], "hi b");
        assert!(r["context"]["data"].get("item").is_none());
    }

    #[test]
    fn endless_loop_stops_at_tick_limit() {
        let r = run_json(
            r#"{"blocks":[{"type":"loop","id":"l","condition":"true","max_iterations":100000,
              "body":[{"type":"step","id":"s","handler":"noop"}]}]}"#,
            &RunOptions {
                max_ticks: 25,
                ..Default::default()
            },
        );
        assert_eq!(r["status"], "tick_limit");
        assert_eq!(r["ticks"], 25);
    }

    #[test]
    fn loop_poll_interval_advances_time() {
        let r = run_json(
            r#"{"blocks":[{"type":"loop","id":"l","condition":"true","max_iterations":3,"poll_interval":10,
              "body":[{"type":"step","id":"s","handler":"noop"}]}]}"#,
            &RunOptions::default(),
        );
        assert_eq!(r["status"], "completed");
        assert_eq!(r["virtual_duration_ms"], 20_000);
        assert_eq!(r["outputs"]["l"]["iterations"], 3);
    }

    #[test]
    fn when_guard_skips_step() {
        let r = run_json(
            r#"{"blocks":[{"type":"step","id":"s","handler":"noop","when":"data.go == true"}]}"#,
            &RunOptions::default(),
        );
        assert_eq!(statuses(&r), vec!["skipped"]);
    }

    #[test]
    fn saga_compensates_in_reverse() {
        let r = run_json(
            r#"{"blocks":[{"type":"saga","id":"sg","steps":[
              {"id":"s1","action":{"type":"step","id":"reserve","handler":"noop"},
               "compensation":{"type":"step","id":"release","handler":"noop"}},
              {"id":"s2","action":{"type":"step","id":"charge","handler":"fail"}}
            ]}]}"#,
            &RunOptions::default(),
        );
        assert_eq!(r["status"], "failed");
        assert!(r["outputs"].get("release").is_some());
    }

    #[test]
    fn ab_split_is_deterministic_per_instance() {
        let seq = r#"{"blocks":[{"type":"ab_split","id":"ab","variants":[
          {"name":"a","weight":50,"blocks":[{"type":"step","id":"va","handler":"noop"}]},
          {"name":"b","weight":50,"blocks":[{"type":"step","id":"vb","handler":"noop"}]}]}]}"#;
        let first = run_json(seq, &RunOptions::default());
        let second = run_json(seq, &RunOptions::default());
        assert_eq!(first["outputs"]["ab"], second["outputs"]["ab"]);
        let mut seen = std::collections::BTreeSet::new();
        for i in 0..32u8 {
            let r = run_json(
                seq,
                &RunOptions {
                    instance_id: Some(format!("00000000-0000-7000-8000-0000000000{i:02x}")),
                    ..Default::default()
                },
            );
            seen.insert(r["outputs"]["ab"]["variant"].as_str().unwrap().to_string());
        }
        assert_eq!(seen.len(), 2, "both variants reachable across instances");
    }

    #[test]
    fn state_builtins_round_trip() {
        let r = run_json(
            r#"{"blocks":[
              {"type":"step","id":"set","handler":"set_state","params":{"key":"k","value":7}},
              {"type":"step","id":"get","handler":"get_state","params":{"key":"k"}}
            ]}"#,
            &RunOptions::default(),
        );
        assert_eq!(r["outputs"]["get"]["value"], 7);
        assert_eq!(r["state"]["k"], 7);
    }

    #[test]
    fn backoff_is_capped() {
        let retry: RetryPolicy = serde_json::from_value(json!({
            "max_attempts": 10, "initial_backoff": 1000, "max_backoff": 5000, "backoff_multiplier": 3.0
        }))
        .unwrap();
        assert_eq!(backoff_ms(&retry, 1), 1000);
        assert_eq!(backoff_ms(&retry, 2), 3000);
        assert_eq!(backoff_ms(&retry, 3), 5000);
    }
}
