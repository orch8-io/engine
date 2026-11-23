//! AWS Step Functions (Amazon States Language) → Orch8 sequence.
//!
//! ASL is a goto graph; Orch8 sequences are structured blocks. The converter
//! rebuilds structure from the graph:
//!
//! * `Task` → a step (Lambda / activity → external-worker handler, HTTP
//!   task → `http_request`, nested `states:startExecution.sync` →
//!   `sub_sequence`, any other service integration → a worker stub).
//!   `Retry` → `retry`, `TimeoutSeconds` → `timeout`, `Catch` →
//!   `try_catch`. Worker-backed tasks retain their renewable heartbeat interval.
//! * `Choice` → `router`; the branches are converted up to their join
//!   state (the nearest state every branch must reach), which then follows
//!   the router. A missing `Default` becomes a `fail` step
//!   (`States.NoChoiceMatched`), exactly like Step Functions.
//! * `Parallel` → `parallel`, `Map` → `for_each`, `Wait` → delayed `noop`,
//!   `Pass` → `transform`, `Fail` → `fail`, `Succeed` → end of chain.
//! * A Choice-driven cycle (polling loop) → `loop`.
//!
//! `JSONPath` references (`$.x`) are resolved against the data flow: the
//! execution input is `data`, a task's result is `outputs.<block>`, and
//! `ResultPath` / `InputPath` / `OutputPath` are tracked per state. Anything
//! that cannot be expressed is reported (`todos`, `warnings`, `unmapped`)
//! instead of being dropped.

#![allow(clippy::too_many_lines, clippy::too_many_arguments)]

use std::collections::{HashSet, VecDeque};

use anyhow::{Context, Result, bail};
use orch8_types::asl_intrinsic;
use serde_json::{Map, Value, json};

use super::{
    Builder, Conversion, ConvertOptions, UnmappedConstruct, finish, json_path_suffix, non_empty,
    slug, snake,
};

/// Where a Step Functions path points in Orch8 terms. `None` = the value is
/// not addressable (e.g. a `Parallel` result array).
#[derive(Debug, Clone, PartialEq)]
struct Root {
    path: Option<String>,
    /// The root is a `lambda:invoke` result: SFN wraps it in `Payload`, an
    /// Orch8 worker returns the payload itself.
    lambda_payload: bool,
}

impl Root {
    fn at(path: impl Into<String>) -> Self {
        Self {
            path: Some(path.into()),
            lambda_payload: false,
        }
    }
    fn opaque() -> Self {
        Self {
            path: None,
            lambda_payload: false,
        }
    }
}

/// The state input, as a list of overlays keyed by `JSONPath` suffix
/// (`""` = `$`, `".r"` = `$.r`). The longest matching suffix wins.
#[derive(Debug, Clone, PartialEq)]
struct Env {
    overlays: Vec<(String, Root)>,
    item: Option<MapItem>,
    callback_token: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct MapItem {
    value: String,
    index: String,
}

impl Env {
    fn data() -> Self {
        Self {
            overlays: vec![(String::new(), Root::at("data"))],
            item: None,
            callback_token: false,
        }
    }

    fn single(root: Root, item: Option<MapItem>) -> Self {
        Self {
            overlays: vec![(String::new(), root)],
            item,
            callback_token: false,
        }
    }

    /// Resolve a `$...` / `$$...` path to an Orch8 template path.
    fn resolve(&self, path: &str) -> Option<String> {
        let path = path.trim();
        if let Some(rest) = path.strip_prefix("$$.") {
            if let Some(tail) = rest.strip_prefix("Map.Item.Value") {
                let item = &self.item.as_ref()?.value;
                return Some(format!("{item}{}", json_path_suffix(tail)?));
            }
            if rest == "Map.Item.Index" {
                return self.item.as_ref().map(|item| item.index.clone());
            }
            if let Some(tail) = rest.strip_prefix("Execution.Input") {
                return Some(format!("data{}", json_path_suffix(tail)?));
            }
            if rest == "Execution.Id" || rest == "Execution.Name" {
                return Some("instance_id".into());
            }
            return None;
        }
        let suffix = json_path_suffix(path.strip_prefix('$')?)?;
        let (prefix, root) = self
            .overlays
            .iter()
            .filter(|(p, _)| suffix == *p || p.is_empty() || suffix.starts_with(&format!("{p}.")))
            .max_by_key(|(p, _)| p.len())?;
        let mut rest = suffix[prefix.len()..].to_string();
        if root.lambda_payload {
            if rest == ".Payload" {
                rest.clear();
            } else if let Some(r) = rest.strip_prefix(".Payload.") {
                rest = format!(".{r}");
            }
        }
        root.path.as_ref().map(|p| format!("{p}{rest}"))
    }

    /// The env seen through an `InputPath` / `OutputPath` / `ItemsPath`.
    fn narrowed(&self, path: Option<&Value>) -> Self {
        match path {
            None => self.clone(),
            Some(Value::Null) => Self::single(Root::opaque(), self.item.clone()),
            Some(Value::String(p)) if p.trim() == "$" => self.clone(),
            Some(Value::String(p)) => {
                let base = Root {
                    path: self.resolve(p),
                    lambda_payload: false,
                };
                let mut out = Self::single(base, self.item.clone());
                if let Some(sfx) = p.trim().strip_prefix('$').and_then(json_path_suffix) {
                    for (prefix, root) in &self.overlays {
                        if let Some(r) = prefix.strip_prefix(&sfx)
                            && r.starts_with('.')
                        {
                            out.overlays.push((r.to_string(), root.clone()));
                        }
                    }
                }
                out
            }
            Some(_) => Self::single(Root::opaque(), self.item.clone()),
        }
    }

    /// Apply a state's `ResultPath` for a result living at `result`.
    fn with_result(&self, result_path: Option<&Value>, result: Root) -> Self {
        match result_path {
            None => Self::single(result, self.item.clone()),
            Some(Value::Null) => self.clone(),
            Some(Value::String(p)) if p.trim() == "$" => Self::single(result, self.item.clone()),
            Some(Value::String(p)) => {
                let Some(sfx) = p.trim().strip_prefix('$').and_then(json_path_suffix) else {
                    return Self::single(Root::opaque(), self.item.clone());
                };
                let mut out = self.clone();
                out.overlays.retain(|(prefix, _)| {
                    !(*prefix == sfx || prefix.starts_with(&format!("{sfx}.")))
                });
                out.overlays.push((sfx, result));
                out
            }
            Some(_) => Self::single(Root::opaque(), self.item.clone()),
        }
    }
}

fn result_path_expression(
    input: &Env,
    path: &str,
    result: &Root,
) -> Option<asl_intrinsic::Intrinsic> {
    use asl_intrinsic::Intrinsic;

    let mut overlays: Vec<_> = input.overlays.iter().collect();
    overlays.sort_unstable_by_key(|(suffix, _)| suffix.len());
    let ((suffix, root), tail) = overlays.split_first()?;
    if !suffix.is_empty() || root.lambda_payload {
        return None;
    }
    let mut expression = Intrinsic::Path(root.path.clone()?);
    let path = json_path_suffix(path.trim().strip_prefix('$')?)?;
    for (suffix, value) in tail
        .iter()
        .map(|(suffix, root)| (suffix, root))
        .chain([(&path, result)])
    {
        if value.lambda_payload {
            return None;
        }
        expression = Intrinsic::PutResult {
            input: Box::new(expression),
            result: Box::new(Intrinsic::Path(value.path.clone()?)),
            path: suffix
                .strip_prefix('.')?
                .split('.')
                .map(str::to_owned)
                .collect(),
        };
    }
    Some(expression)
}

/// One (sub-)state machine.
struct Machine<'a> {
    states: &'a Map<String, Value>,
}

struct Sfn<'a> {
    file: String,
    text: Option<&'a str>,
    jsonata: bool,
}

/// Convert an ASL definition (or a `describe-state-machine` response whose
/// `definition` is a JSON string). `source` is `(file name, raw text)` and
/// is used to attach line numbers to unmapped constructs.
pub fn convert_stepfunctions(
    export: &Value,
    source: Option<(&str, &str)>,
    options: &ConvertOptions,
) -> Result<Conversion> {
    let (definition, workflow_name) = unwrap_definition(export)?;
    let states = definition
        .get("States")
        .and_then(Value::as_object)
        .context("not a Step Functions definition: missing `States` object")?;
    let start = definition
        .get("StartAt")
        .and_then(Value::as_str)
        .context("not a Step Functions definition: missing `StartAt`")?;
    let (file, text) = source.map_or(("state-machine.asl.json".to_string(), None), |(f, t)| {
        (f.to_string(), Some(t))
    });
    let workflow = workflow_name
        .or_else(|| {
            definition
                .get("Comment")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| {
            std::path::Path::new(&file)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("state machine")
                .trim_end_matches(".asl")
                .to_string()
        });
    let sequence_name = options
        .name
        .clone()
        .unwrap_or_else(|| snake(&workflow).replace('_', "-"));
    let mut b = Builder::new("stepfunctions", &workflow, &sequence_name);
    let sfn = Sfn {
        file,
        text,
        jsonata: definition.get("QueryLanguage").and_then(Value::as_str) == Some("JSONata"),
    };
    if sfn.jsonata {
        sfn.unmapped(
            &mut b,
            "QueryLanguage",
            "QueryLanguage: JSONata",
            "JSONata expressions are not translated; `{% ... %}` values are kept as text \
             and reported individually",
        );
    }
    if let Some(timeout) = definition.get("TimeoutSeconds") {
        sfn.unmapped(
            &mut b,
            "TimeoutSeconds",
            "TimeoutSeconds",
            &format!(
                "state-machine timeout {timeout}s has no sequence-level equivalent; set step \
                 `deadline`s or cancel the instance from a watchdog"
            ),
        );
    }
    let machine = Machine { states };
    let (blocks, _) = sfn.chain(&mut b, &machine, start, None, Env::data(), &mut Vec::new())?;
    finish(b, options, blocks)
}

fn unwrap_definition(export: &Value) -> Result<(Value, Option<String>)> {
    if export.get("States").is_some() {
        return Ok((export.clone(), None));
    }
    let name = export
        .get("name")
        .and_then(Value::as_str)
        .map(str::to_string);
    match export.get("definition") {
        Some(Value::String(raw)) => Ok((
            serde_json::from_str(raw).context("`definition` is not valid ASL JSON")?,
            name,
        )),
        Some(def @ Value::Object(_)) => Ok((def.clone(), name)),
        _ => bail!("not a Step Functions definition: missing `States` / `StartAt`"),
    }
}

fn state_type(state: &Value) -> &str {
    state.get("Type").and_then(Value::as_str).unwrap_or("")
}

fn next_of(state: &Value) -> Option<&str> {
    state.get("Next").and_then(Value::as_str)
}

fn millis(seconds: &Value) -> Option<u64> {
    let s = seconds.as_f64()?;
    if s < 0.0 {
        return None;
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Some((s * 1000.0).round() as u64)
}

impl Machine<'_> {
    fn state(&self, name: &str) -> Result<&Value> {
        self.states
            .get(name)
            .with_context(|| format!("state `{name}` is referenced but not defined"))
    }

    /// Every outgoing edge (including `Catch` edges) of a state.
    fn successors(&self, name: &str) -> Vec<String> {
        let Some(state) = self.states.get(name) else {
            return Vec::new();
        };
        let mut out: Vec<String> = Vec::new();
        if let Some(n) = next_of(state) {
            out.push(n.into());
        }
        if let Some(d) = state.get("Default").and_then(Value::as_str) {
            out.push(d.into());
        }
        for key in ["Choices", "Catch"] {
            for item in state
                .get(key)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(n) = next_of(item) {
                    out.push(n.into());
                }
            }
        }
        out.dedup();
        out
    }

    /// Breadth-first order of the states reachable from `start` (inclusive).
    fn reachable(&self, start: &str) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut order = Vec::new();
        let mut queue = VecDeque::from([start.to_string()]);
        while let Some(s) = queue.pop_front() {
            if !seen.insert(s.clone()) {
                continue;
            }
            order.push(s.clone());
            queue.extend(self.successors(&s));
        }
        order
    }

    /// Does every path from `from` reach `target` (ignoring cycles)?
    fn must_reach(&self, from: &str, target: &str, visiting: &mut HashSet<String>) -> bool {
        if from == target {
            return true;
        }
        if !visiting.insert(from.to_string()) {
            // A cycle back into the path: the loop eventually exits along
            // another edge, which is checked separately.
            return true;
        }
        let succ = self.successors(from);
        let result = !succ.is_empty() && succ.iter().all(|s| self.must_reach(s, target, visiting));
        visiting.remove(from);
        result
    }

    /// The nearest state every target must reach — where diverging branches
    /// join again. `stop` (the enclosing join) always qualifies.
    fn join(&self, targets: &[Option<&str>], stop: Option<&str>) -> Option<String> {
        let live: Vec<&str> = targets.iter().flatten().copied().collect();
        if live.len() < targets.len() {
            // One path ends the machine: nothing after can be shared.
            return stop.map(str::to_string);
        }
        let first = live.first()?;
        for candidate in self.reachable(first) {
            if live
                .iter()
                .all(|t| self.must_reach(t, &candidate, &mut HashSet::new()))
            {
                return Some(candidate);
            }
            if Some(candidate.as_str()) == stop {
                return Some(candidate);
            }
        }
        stop.map(str::to_string)
    }

    fn can_reach(&self, from: &str, target: &str) -> bool {
        self.successors(from)
            .iter()
            .any(|s| self.reachable(s).iter().any(|r| r == target))
    }
}

/// Step Functions comparison operators → Orch8 expression operators.
fn comparison_op(key: &str) -> Option<(&'static str, bool)> {
    let base = key.strip_suffix("Path").unwrap_or(key);
    let op = match base {
        "StringEquals" | "NumericEquals" | "BooleanEquals" | "TimestampEquals" => "==",
        "StringLessThan" | "NumericLessThan" | "TimestampLessThan" => "<",
        "StringGreaterThan" | "NumericGreaterThan" | "TimestampGreaterThan" => ">",
        "StringLessThanEquals" | "NumericLessThanEquals" | "TimestampLessThanEquals" => "<=",
        "StringGreaterThanEquals" | "NumericGreaterThanEquals" | "TimestampGreaterThanEquals" => {
            ">="
        }
        _ => return None,
    };
    Some((op, key.ends_with("Path")))
}

impl Sfn<'_> {
    fn line_of(&self, needle: &str) -> usize {
        let Some(text) = self.text else { return 0 };
        let quoted = format!("\"{needle}\"");
        let is_key = |l: &str| {
            l.find(&quoted)
                .is_some_and(|i| l[i + quoted.len()..].trim_start().starts_with(':'))
        };
        text.lines()
            .position(is_key)
            .or_else(|| text.lines().position(|l| l.contains(&quoted)))
            .map_or(0, |i| i + 1)
    }

    fn unmapped(&self, b: &mut Builder, state: &str, construct: &str, reason: &str) {
        b.report.unmapped.push(UnmappedConstruct {
            file: self.file.clone(),
            line: self.line_of(state),
            construct: construct.into(),
            reason: reason.into(),
        });
    }

    /// Translate a `Parameters` / `Payload` / `Result` object: `"key.$"`
    /// entries become `{{path}}` templates.
    fn params(&self, b: &mut Builder, state: &str, value: &Value, env: &Env) -> Value {
        self.payload(b, state, value, env, None)
    }

    fn payload(
        &self,
        b: &mut Builder,
        state: &str,
        value: &Value,
        env: &Env,
        selector: Option<&str>,
    ) -> Value {
        match value {
            Value::Object(map) => {
                let mut out = Map::new();
                for (k, v) in map {
                    if let Some(key) = k.strip_suffix(".$") {
                        let selected = selector.and_then(|selector| {
                            v.as_str()
                                .and_then(|path| env.resolve(path))
                                .map(|path| (selector, asl_intrinsic::Intrinsic::Path(path)))
                        });
                        let parameter = match selected {
                            Some((selector, path)) => {
                                self.selector_template(b, state, selector, &path, v)
                            }
                            None => self.path_value(b, state, v, env),
                        };
                        out.insert(key.to_string(), parameter);
                    } else {
                        out.insert(k.clone(), self.payload(b, state, v, env, selector));
                    }
                }
                Value::Object(out)
            }
            Value::Array(items) => Value::Array(
                items
                    .iter()
                    .map(|v| self.payload(b, state, v, env, selector))
                    .collect(),
            ),
            Value::String(s) if selector.is_some() && s.contains("{{") => self.selector_template(
                b,
                state,
                selector.unwrap_or_default(),
                &asl_intrinsic::Intrinsic::Literal(value.clone()),
                value,
            ),
            Value::String(s) if s.trim_start().starts_with("{%") => {
                self.unmapped(
                    b,
                    state,
                    s,
                    "JSONata expression kept as literal text — rewrite as an Orch8 template",
                );
                value.clone()
            }
            other => other.clone(),
        }
    }

    fn selector_template(
        &self,
        b: &mut Builder,
        state: &str,
        selector: &str,
        expression: &asl_intrinsic::Intrinsic,
        source: &Value,
    ) -> Value {
        match expression.to_template() {
            Ok(template) => Value::String(template),
            Err(error) => {
                self.unmapped(b, state, selector, &error.to_string());
                source.clone()
            }
        }
    }

    /// A `"key.$"` value: a path or an intrinsic function.
    fn path_value(&self, b: &mut Builder, state: &str, v: &Value, env: &Env) -> Value {
        let Some(path) = v.as_str() else {
            return v.clone();
        };
        if path.trim() == asl_intrinsic::CALLBACK_TOKEN_PATH && env.callback_token {
            return json!({(orch8_types::worker::ASL_CALLBACK_TOKEN): true});
        }
        if let Some(resolved) = env.resolve(path) {
            return json!(format!("{{{{{resolved}}}}}"));
        }
        if path.trim_start().starts_with("States.") {
            let mut needs_callback = false;
            let template = asl_intrinsic::parse(path, |reference| {
                if reference == asl_intrinsic::CALLBACK_TOKEN_PATH && env.callback_token {
                    needs_callback = true;
                    Some(reference.to_owned())
                } else {
                    env.resolve(reference)
                }
            })
            .and_then(|expression| {
                if needs_callback {
                    expression.to_callback_template()
                } else {
                    expression.to_template()
                }
            });
            return match template {
                Ok(template) => json!(template),
                Err(error) => {
                    self.unmapped(
                        b,
                        state,
                        path,
                        &format!("{error}; intrinsic kept as literal text"),
                    );
                    v.clone()
                }
            };
        }
        let reason = if path.contains("$$.Task.Token") {
            "task tokens require a worker-backed .waitForTaskToken Task"
        } else {
            "path does not resolve to addressable data (e.g. a Parallel/Map result array or \
             a context-object field); kept as literal text"
        };
        self.unmapped(b, state, path, reason);
        v.clone()
    }

    fn template(&self, b: &mut Builder, state: &str, path: &str, env: &Env) -> Value {
        self.path_value(b, state, &json!(path), env)
    }

    /// A Choice rule → an Orch8 expression.
    #[allow(clippy::self_only_used_in_recursion)]
    fn condition(&self, b: &mut Builder, state: &str, rule: &Value, env: &Env) -> Option<String> {
        if let Some(all) = rule.get("And").and_then(Value::as_array) {
            let parts: Option<Vec<String>> = all
                .iter()
                .map(|r| self.condition(b, state, r, env).map(|c| format!("({c})")))
                .collect();
            return parts.map(|p| p.join(" && "));
        }
        if let Some(any) = rule.get("Or").and_then(Value::as_array) {
            let parts: Option<Vec<String>> = any
                .iter()
                .map(|r| self.condition(b, state, r, env).map(|c| format!("({c})")))
                .collect();
            return parts.map(|p| p.join(" || "));
        }
        if let Some(inner) = rule.get("Not") {
            return self
                .condition(b, state, inner, env)
                .map(|c| format!("!({c})"));
        }
        let var = env.resolve(rule.get("Variable")?.as_str()?)?;
        for (key, value) in rule.as_object()? {
            if key == "IsPresent" || key == "IsNull" {
                let yes = value.as_bool()?;
                let is_null = (key == "IsNull") == yes;
                return Some(format!("{var} {} null", if is_null { "==" } else { "!=" }));
            }
            if let Some((op, is_path)) = comparison_op(key) {
                if key.starts_with("Timestamp") {
                    b.report.warnings.push(format!(
                        "Choice `{state}`: {key} is compared as an ISO-8601 string; make sure \
                         both sides use the same format and offset"
                    ));
                }
                let rhs = if is_path {
                    env.resolve(value.as_str()?)?
                } else {
                    serde_json::to_string(value).ok()?
                };
                return Some(format!("{var} {op} {rhs}"));
            }
        }
        None
    }

    /// Convert the chain starting at `start` until `stop` (exclusive) or the
    /// end of the machine. Returns the blocks and the env after the chain.
    fn chain(
        &self,
        b: &mut Builder,
        m: &Machine<'_>,
        start: &str,
        stop: Option<&str>,
        mut env: Env,
        path: &mut Vec<String>,
    ) -> Result<(Vec<Value>, Env)> {
        let mut blocks = Vec::new();
        let mut cur = Some(start.to_string());
        let depth = path.len();
        while let Some(name) = cur.take() {
            if Some(name.as_str()) == stop {
                break;
            }
            if path.contains(&name) {
                let stub = b.todo_stub(
                    &format!("{name} loop back"),
                    "Next (cycle)",
                    &json!({ "next": name }),
                    "this transition jumps back to an earlier state in a shape the importer \
                     cannot restructure; rebuild it as an Orch8 `loop` block",
                );
                self.unmapped(
                    b,
                    &name,
                    &format!("Next -> {name}"),
                    "unstructured cycle replaced by a TODO stub",
                );
                blocks.push(stub);
                break;
            }
            // A loop whose header is not the Choice itself is handled when
            // the chain reaches the Choice (do-while shape).
            let state = m.state(&name)?;
            path.push(name.clone());
            let next = self.state(b, m, &name, state, stop, &mut env, &mut blocks, path)?;
            cur = next;
        }
        path.truncate(depth);
        Ok((blocks, env))
    }

    /// Convert one state, appending blocks; returns the next state to visit.
    fn state(
        &self,
        b: &mut Builder,
        m: &Machine<'_>,
        name: &str,
        state: &Value,
        stop: Option<&str>,
        env: &mut Env,
        blocks: &mut Vec<Value>,
        path: &mut Vec<String>,
    ) -> Result<Option<String>> {
        let kind = state_type(state);
        let input = env.narrowed(state.get("InputPath"));
        for key in ["Assign", "Output", "Arguments", "Items"] {
            if state.get(key).is_some() {
                self.unmapped(
                    b,
                    name,
                    &format!("{name}.{key}"),
                    "JSONata/variables field is not translated",
                );
            }
        }
        match kind {
            "Task" => {
                let (block, result) = self.task(b, name, state, &input)?;
                let mut success = vec![block];
                let after = self.after_result(b, name, state, env, result, &mut success);
                self.with_catch(b, m, name, state, success, after, stop, env, blocks, path)
            }
            "Pass" => {
                let has_payload =
                    state.get("Result").is_some() || state.get("Parameters").is_some();
                if has_payload {
                    let params = match state.get("Result") {
                        Some(r) => r.clone(),
                        None => self.params(b, name, &state["Parameters"], &input),
                    };
                    let id = b.unique_id(name);
                    b.mapped(name, "Pass", &id, "transform");
                    let params = if params.is_object() {
                        params
                    } else {
                        json!({ "value": params })
                    };
                    blocks.push(
                        json!({"type": "step", "id": id, "handler": "transform", "params": params}),
                    );
                    let result = Root::at(format!("outputs.{id}"));
                    *env = self.after_result(b, name, state, env, result, blocks);
                } else {
                    b.mapped(name, "Pass", "-", "pass-through (no block)");
                    *env = input.narrowed(state.get("OutputPath"));
                }
                Ok(if state.get("End") == Some(&json!(true)) {
                    None
                } else {
                    next_of(state).map(str::to_string)
                })
            }
            "Wait" => {
                let id = b.unique_id(name);
                let delay = if let Some(ms) = state.get("Seconds").and_then(millis) {
                    Some(json!({ "duration": ms }))
                } else if let Some(ts) = state.get("Timestamp").and_then(Value::as_str) {
                    utc_local(ts).map(
                        |local| json!({ "duration": 0, "fire_at_local": local, "timezone": "UTC" }),
                    )
                } else {
                    None
                };
                if let Some(delay) = delay {
                    b.mapped(name, "Wait", &id, "noop + delay");
                    blocks.push(json!({"type": "step", "id": id, "handler": "noop", "params": {}, "delay": delay}));
                } else if let Some((parameter, path)) =
                    match (state.get("SecondsPath"), state.get("TimestampPath")) {
                        (Some(path), None) if path.is_string() => Some(("seconds", path)),
                        (None, Some(path)) if path.is_string() => Some(("timestamp", path)),
                        _ => None,
                    }
                {
                    let dynamic = self.path_value(b, name, path, &input);
                    b.mapped(name, "Wait", &id, "durable runtime deadline");
                    blocks.push(json!({"type":"step","id":id,"handler":"asl_wait","params":{(parameter):dynamic}}));
                } else {
                    b.used_ids.remove(&id);
                    let stub = b.todo_stub(
                        name,
                        "Wait",
                        state,
                        "wait requires a supported duration or timestamp",
                    );
                    self.unmapped(
                        b,
                        name,
                        &format!("{name}.SecondsPath/TimestampPath"),
                        "invalid wait arguments replaced by a TODO stub",
                    );
                    blocks.push(stub);
                }
                *env = input.narrowed(state.get("OutputPath"));
                Ok(if state.get("End") == Some(&json!(true)) {
                    None
                } else {
                    next_of(state).map(str::to_string)
                })
            }
            "Succeed" => {
                b.mapped(name, "Succeed", "-", "end of chain");
                Ok(None)
            }
            "Fail" => {
                let id = b.unique_id(name);
                let error = state
                    .get("Error")
                    .and_then(Value::as_str)
                    .unwrap_or("States.Fail");
                let cause = state.get("Cause").and_then(Value::as_str).unwrap_or("");
                if state.get("ErrorPath").is_some() || state.get("CausePath").is_some() {
                    self.unmapped(
                        b,
                        name,
                        &format!("{name}.ErrorPath/CausePath"),
                        "dynamic error text is not carried; the static Error/Cause are used",
                    );
                }
                b.mapped(name, "Fail", &id, "fail");
                let message = if cause.is_empty() {
                    error.to_string()
                } else {
                    format!("{error}: {cause}")
                };
                blocks.push(json!({"type": "step", "id": id, "handler": "fail", "params": {"message": message, "error": error, "retryable": false}}));
                Ok(None)
            }
            "Choice" => self.choice(b, m, name, state, stop, env, blocks, path),
            "Parallel" => {
                let id = b.unique_id(name);
                let mut branches = Vec::new();
                let branch_env = self.parameters_env(b, name, state, &input);
                for (i, branch) in state
                    .get("Branches")
                    .and_then(Value::as_array)
                    .context("Parallel state without `Branches`")?
                    .iter()
                    .enumerate()
                {
                    let sub_states = branch
                        .get("States")
                        .and_then(Value::as_object)
                        .with_context(|| format!("Parallel `{name}` branch {i} has no `States`"))?;
                    let sub_start =
                        branch
                            .get("StartAt")
                            .and_then(Value::as_str)
                            .with_context(|| {
                                format!("Parallel `{name}` branch {i} has no `StartAt`")
                            })?;
                    let sub = Machine { states: sub_states };
                    let (bl, _) = self.chain(
                        b,
                        &sub,
                        sub_start,
                        None,
                        branch_env.clone(),
                        &mut Vec::new(),
                    )?;
                    branches.push(Value::Array(non_empty(bl, b, &format!("{id}_branch_{i}"))));
                }
                b.mapped(name, "Parallel", &id, "parallel");
                self.composite_retry(b, name, state, &id);
                let block = json!({"type": "parallel", "id": id, "branches": branches});
                let mut success = vec![block];
                let after = self.after_result(b, name, state, env, Root::opaque(), &mut success);
                self.with_catch(b, m, name, state, success, after, stop, env, blocks, path)
            }
            "Map" => {
                let id = b.unique_id(name);
                let items_path = state
                    .get("ItemsPath")
                    .and_then(Value::as_str)
                    .unwrap_or("$");
                let processor = state
                    .get("ItemProcessor")
                    .or_else(|| state.get("Iterator"))
                    .context("Map state without `ItemProcessor` / `Iterator`")?;
                let processor_mode = processor
                    .pointer("/ProcessorConfig/Mode")
                    .and_then(Value::as_str);
                if let Some(mode) = processor_mode.filter(|mode| *mode != "INLINE") {
                    self.unmapped(b, name, &format!("{name}.ProcessorConfig.Mode={mode}"), "distributed Map runs as an inline `for_each`; child-execution fan-out, ItemReader and ResultWriter are not translated");
                }
                for key in [
                    "ItemReader",
                    "ResultWriter",
                    "ItemBatcher",
                    "ToleratedFailurePercentage",
                    "ToleratedFailureCount",
                ] {
                    if state.get(key).is_some() {
                        self.unmapped(
                            b,
                            name,
                            &format!("{name}.{key}"),
                            "not supported by `for_each`",
                        );
                    }
                }
                let concurrency = state
                    .get("MaxConcurrency")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                if concurrency != 1 {
                    b.report.warnings.push(format!(
                        "Map `{name}`: Orch8 `for_each` processes items one at a time (MaxConcurrency {})",
                        if concurrency == 0 { "unbounded".to_string() } else { concurrency.to_string() }
                    ));
                }
                let item_var = format!("{id}_item");
                let (inner, selector) = self.item_selector(b, name, state, &input, &id, &item_var);
                let collection = match input.resolve(items_path) {
                    Some(p) => format!("{{{{{p}}}}}"),
                    None => {
                        self.unmapped(
                            b,
                            name,
                            items_path,
                            "ItemsPath does not resolve; `collection` left as the raw path",
                        );
                        items_path.to_string()
                    }
                };
                let sub_states = processor
                    .get("States")
                    .and_then(Value::as_object)
                    .with_context(|| format!("Map `{name}` processor has no `States`"))?;
                let sub_start = processor
                    .get("StartAt")
                    .and_then(Value::as_str)
                    .with_context(|| format!("Map `{name}` processor has no `StartAt`"))?;
                let sub = Machine { states: sub_states };
                let (mut body, _) = self.chain(b, &sub, sub_start, None, inner, &mut Vec::new())?;
                if let Some(selector) = selector {
                    body.insert(0, selector);
                }
                let body = non_empty(body, b, &format!("{id}_body"));
                b.mapped(name, "Map", &id, "for_each");
                if processor_mode.is_none_or(|mode| mode == "INLINE") {
                    self.composite_retry(b, name, state, &id);
                } else {
                    self.retry_notes(b, name, state);
                }
                let block = json!({"type": "for_each", "id": id, "collection": collection, "item_var": item_var, "max_iterations": 10000, "body": body});
                let mut success = vec![block];
                let after = self.after_result(b, name, state, env, Root::opaque(), &mut success);
                self.with_catch(b, m, name, state, success, after, stop, env, blocks, path)
            }
            other => bail!("state `{name}` has unsupported Type `{other}`"),
        }
    }

    fn item_selector(
        &self,
        b: &mut Builder,
        name: &str,
        state: &Value,
        input: &Env,
        id: &str,
        item_var: &str,
    ) -> (Env, Option<Value>) {
        let item = MapItem {
            value: format!("data.{item_var}"),
            index: format!("outputs.{id}._index"),
        };
        if let Some(selector) = state
            .get("ItemSelector")
            .or_else(|| state.get("Parameters"))
        {
            if selector.is_object() {
                let scoped = Env {
                    overlays: input.overlays.clone(),
                    item: Some(item.clone()),
                    callback_token: false,
                };
                let params = self.payload(b, name, selector, &scoped, Some("ItemSelector"));
                let selector_id = b.unique_id(&format!("{name}_item_selector"));
                b.mapped(
                    name,
                    "ItemSelector",
                    &selector_id,
                    "transform each item input",
                );
                return (
                    Env::single(Root::at(format!("outputs.{selector_id}")), Some(item)),
                    Some(
                        json!({"type":"step","id":selector_id,"handler":"transform","params":params}),
                    ),
                );
            }
            self.unmapped(
                b,
                name,
                &format!("{name}.ItemSelector"),
                "the selector must be an object",
            );
        }
        (Env::single(Root::at(item.value.clone()), Some(item)), None)
    }

    /// The env a Parallel branch sees (`Parameters` reshape the input).
    fn parameters_env(&self, b: &mut Builder, name: &str, state: &Value, input: &Env) -> Env {
        let Some(params) = state.get("Parameters").and_then(Value::as_object) else {
            return input.clone();
        };
        let mut env = Env::single(Root::opaque(), input.item.clone());
        for (k, v) in params {
            if let (Some(key), Some(p)) = (k.strip_suffix(".$"), v.as_str()) {
                env.overlays.push((
                    format!(".{key}"),
                    Root {
                        path: input.resolve(p),
                        lambda_payload: false,
                    },
                ));
            }
        }
        self.unmapped(b, name, &format!("{name}.Parameters"), "branch input object is not materialized; paths into it were rewritten to their sources");
        env
    }

    /// Env after a result-producing state (`ResultSelector`, `ResultPath`,
    /// `OutputPath`).
    fn after_result(
        &self,
        b: &mut Builder,
        name: &str,
        state: &Value,
        raw_input: &Env,
        mut result: Root,
        blocks: &mut Vec<Value>,
    ) -> Env {
        let mut projected = false;
        if let Some(selector) = state.get("ResultSelector") {
            if result.path.is_some() && (selector.is_object() || selector.is_array()) {
                let id = b.unique_id(&format!("{name}_result_selector"));
                let mut selector_input = Env::single(result.clone(), raw_input.item.clone());
                if result.lambda_payload {
                    // The local Lambda handler supplies the payload. Service
                    // metadata and the complete AWS envelope are unavailable.
                    selector_input = Env::single(Root::opaque(), raw_input.item.clone());
                    selector_input.overlays.push((
                        ".Payload".to_owned(),
                        Root {
                            lambda_payload: false,
                            ..result
                        },
                    ));
                }
                let params =
                    self.payload(b, name, selector, &selector_input, Some("ResultSelector"));
                blocks.push(
                    json!({"type": "step", "id": id, "handler": "transform", "params": params}),
                );
                b.mapped(name, "ResultSelector", &id, "transform");
                result = Root::at(format!("outputs.{id}"));
                projected = true;
            } else {
                self.unmapped(
                    b,
                    name,
                    &format!("{name}.ResultSelector"),
                    "result projection needs an addressable result and an object or array selector",
                );
            }
        }
        let result_path = state.get("ResultPath");
        let mut materialized = None;
        if projected
            && let Some(path) = result_path.and_then(Value::as_str)
            && path.trim() != "$"
        {
            materialized = result_path_expression(raw_input, path, &result)
                .and_then(|expression| expression.to_template().ok())
                .map(|params| {
                    let id = b.unique_id(&format!("{name}_result_path"));
                    blocks.push(
                        json!({"type":"step", "id":id, "handler":"transform", "params":params}),
                    );
                    b.mapped(name, "ResultPath", &id, "transform");
                    Env::single(Root::at(format!("outputs.{id}")), raw_input.item.clone())
                });
            if materialized.is_none() {
                self.unmapped(
                    b,
                    name,
                    &format!("{name}.ResultPath"),
                    "result merge needs addressable input and a single supported reference path",
                );
            }
        }
        let merged = materialized.unwrap_or_else(|| raw_input.with_result(result_path, result));
        merged.narrowed(state.get("OutputPath"))
    }

    fn retry_notes(&self, b: &mut Builder, name: &str, state: &Value) {
        if state.get("Retry").is_some() {
            self.unmapped(
                b,
                name,
                &format!("{name}.Retry"),
                "Retry is unsupported for this state form",
            );
        }
    }

    fn composite_retry(&self, b: &mut Builder, name: &str, state: &Value, id: &str) {
        if let Some(retry) = self.retry(b, name, state) {
            b.composite_retries.insert(id.to_owned(), retry);
        }
    }

    /// Task state → one block plus the Root of its result.
    fn task(
        &self,
        b: &mut Builder,
        name: &str,
        state: &Value,
        input: &Env,
    ) -> Result<(Value, Root)> {
        let resource = state
            .get("Resource")
            .and_then(Value::as_str)
            .with_context(|| format!("Task `{name}` has no `Resource`"))?;
        let (base, suffix) = match resource.rsplit_once('.') {
            Some((base, s @ ("sync" | "waitForTaskToken"))) => (base, Some(s)),
            Some((base, "sync:2")) => (base, Some("sync")),
            _ => (resource, None),
        };
        let base = base.strip_suffix(".sync").unwrap_or(base);
        let mut task_input = input.clone();
        task_input.callback_token =
            suffix == Some("waitForTaskToken") && base != "arn:aws:states:::http:invoke";
        let input = &task_input;
        let params_src = state.get("Parameters");
        let id = b.unique_id(name);
        let mut lambda_payload = false;
        let mut block = if base == "arn:aws:states:::lambda:invoke" {
            lambda_payload = true;
            let function = params_src
                .and_then(|p| p.get("FunctionName"))
                .and_then(Value::as_str)
                .unwrap_or("lambda");
            let handler = lambda_handler(function);
            let payload = match params_src {
                Some(p) if p.get("Payload").is_some() => self.params(b, name, &p["Payload"], input),
                Some(p) if p.get("Payload.$").is_some() => {
                    json!({ "input": self.path_value(b, name, &p["Payload.$"], input) })
                }
                _ => json!({ "input": self.template(b, name, "$", input) }),
            };
            self.worker(b, name, &id, "Task (lambda:invoke)", &handler, payload, &format!("implement Lambda `{function}` as the Orch8 worker handler `{handler}` (it receives the Payload as params)"))
        } else if let Some(function) = base.strip_prefix("arn:aws:lambda:") {
            let fname = function.split(":function:").nth(1).unwrap_or(function);
            let handler = lambda_handler(fname);
            let payload = match params_src {
                Some(p) => self.params(b, name, p, input),
                None => json!({ "input": self.template(b, name, "$", input) }),
            };
            self.worker(
                b,
                name,
                &id,
                "Task (Lambda)",
                &handler,
                payload,
                &format!("implement Lambda `{fname}` as the Orch8 worker handler `{handler}`"),
            )
        } else if let Some(activity) = activity_name(base) {
            let handler = snake(activity);
            let payload = match params_src {
                Some(p) => self.params(b, name, p, input),
                None => json!({ "input": self.template(b, name, "$", input) }),
            };
            b.mapped(
                name,
                "Task (activity)",
                &id,
                &format!("worker handler {handler}"),
            );
            if !b.report.worker_handlers.contains(&handler) {
                b.report.worker_handlers.push(handler.clone());
            }
            json!({"type": "step", "id": id, "handler": handler, "params": payload})
        } else if base == "arn:aws:states:::http:invoke" {
            let p = params_src.cloned().unwrap_or_else(|| json!({}));
            let mut out = Map::new();
            out.insert(
                "method".into(),
                p.get("Method").cloned().unwrap_or(json!("GET")),
            );
            let url = if let Some(u) = p.get("ApiEndpoint.$") {
                self.path_value(b, name, u, input)
            } else {
                p.get("ApiEndpoint").cloned().unwrap_or(Value::Null)
            };
            out.insert("url".into(), url);
            for (src, dst) in [
                ("Headers", "headers"),
                ("RequestBody", "body"),
                ("QueryParameters", "query"),
            ] {
                if let Some(v) = p.get(src) {
                    out.insert(dst.into(), self.params(b, name, v, input));
                } else if let Some(v) = p.get(format!("{src}.$").as_str()) {
                    out.insert(dst.into(), self.path_value(b, name, v, input));
                }
            }
            if p.get("Authentication").is_some() {
                self.unmapped(b, name, &format!("{name}.Authentication"), "EventBridge connection auth is not translated; add a credential reference to the http_request step");
            }
            b.mapped(name, "Task (http:invoke)", &id, "http_request");
            json!({"type": "step", "id": id, "handler": "http_request", "params": Value::Object(out)})
        } else if base == "arn:aws:states:::states:startExecution" && suffix == Some("sync") {
            let p = params_src.cloned().unwrap_or_else(|| json!({}));
            let child = p
                .get("StateMachineArn")
                .and_then(Value::as_str)
                .map_or("child", |arn| arn.rsplit(':').next().unwrap_or(arn));
            let child = snake(child).replace('_', "-");
            let input_value = match (p.get("Input"), p.get("Input.$")) {
                (Some(v), _) => self.params(b, name, v, input),
                (None, Some(v)) => self.path_value(b, name, v, input),
                _ => json!({}),
            };
            b.mapped(
                name,
                "Task (startExecution.sync)",
                &id,
                &format!("sub_sequence {child}"),
            );
            json!({"type": "sub_sequence", "id": id, "sequence_name": child, "input": input_value})
        } else {
            let service = base
                .strip_prefix("arn:aws:states:::")
                .unwrap_or(base)
                .replace([':', '.'], "_");
            let handler = format!("aws_{}", slug(&service));
            let payload = match params_src {
                Some(p) => self.params(b, name, p, input),
                None => json!({ "input": self.template(b, name, "$", input) }),
            };
            self.worker(b, name, &id, &format!("Task ({resource})"), &handler, payload, &format!("service integration `{resource}` — implement it in worker handler `{handler}` (AWS SDK call) or replace it with an http_request step"))
        };
        match suffix {
            Some("waitForTaskToken") if task_input.callback_token => {
                let params = block["params"].take();
                block["params"] = json!({(orch8_types::worker::ASL_CALLBACK_PARAM): {"params":params}});
                b.mapped(name, ".waitForTaskToken", &id, "expiring worker callback token");
            }
            Some("waitForTaskToken") => self.unmapped(b, name, resource, "callback tokens require an external worker task"),
            Some("sync") if block["type"] != "sub_sequence" => self.unmapped(b, name, resource, "`.sync` job: the worker must wait for the job to finish before completing the task"),
            _ => {}
        }
        if block["type"] == "step" {
            if let Some(ms) = state.get("TimeoutSeconds").and_then(millis) {
                block["timeout"] = json!(ms);
            }
            if state.get("Credentials").is_some() {
                self.unmapped(
                    b,
                    name,
                    &format!("{name}.Credentials"),
                    "task credentials must be configured on the worker",
                );
            }
            if let Some(retry) = self.retry(b, name, state) {
                block["retry"] = retry;
            }
        } else {
            self.retry_notes(b, name, state);
        }
        self.worker_heartbeat(b, name, state, input, &mut block, base)?;
        self.worker_timeout_path(b, name, state, input, &mut block, base)?;
        Ok((
            block,
            Root {
                path: Some(format!("outputs.{id}")),
                lambda_payload,
            },
        ))
    }

    fn worker_timeout_path(
        &self,
        b: &mut Builder,
        name: &str,
        state: &Value,
        input: &Env,
        block: &mut Value,
        resource: &str,
    ) -> Result<()> {
        let Some(path) = state.get("TimeoutSecondsPath") else {
            return Ok(());
        };
        anyhow::ensure!(
            state.get("TimeoutSeconds").is_none(),
            "Task `{name}` specifies both TimeoutSeconds and TimeoutSecondsPath"
        );
        let field = format!("{name}.TimeoutSecondsPath");
        if block["type"] != "step" || resource == "arn:aws:states:::http:invoke" {
            self.unmapped(
                b,
                name,
                &field,
                "dynamic timeouts require an external worker task",
            );
            return Ok(());
        }
        let Some(resolved) = path.as_str().and_then(|path| input.resolve(path)) else {
            self.unmapped(
                b,
                name,
                &field,
                "timeout reference path does not resolve to addressable state input",
            );
            return Ok(());
        };
        let params = block["params"].take();
        block["params"] = json!({(orch8_types::worker::ASL_TIMEOUT_PARAM): {
            "seconds": format!("{{{{{resolved}}}}}"), "params": params
        }});
        b.mapped(
            name,
            "TimeoutSecondsPath",
            block["id"].as_str().unwrap_or(name),
            "absolute worker deadline resolved from state input",
        );
        Ok(())
    }

    fn worker_heartbeat(
        &self,
        b: &mut Builder,
        name: &str,
        state: &Value,
        input: &Env,
        block: &mut Value,
        resource: &str,
    ) -> Result<()> {
        let (field, seconds) = match (
            state.get("HeartbeatSeconds"),
            state.get("HeartbeatSecondsPath"),
        ) {
            (None, None) => return Ok(()),
            (Some(_), Some(_)) => {
                bail!("Task `{name}` specifies both HeartbeatSeconds and HeartbeatSecondsPath")
            }
            (Some(seconds), None) => {
                anyhow::ensure!(
                    seconds
                        .as_u64()
                        .and_then(|n| u32::try_from(n).ok())
                        .is_some_and(|n| n > 0),
                    "Task `{name}` HeartbeatSeconds must be a positive integer within the supported range"
                );
                ("HeartbeatSeconds", seconds.clone())
            }
            (None, Some(path)) => {
                let Some(resolved) = path.as_str().and_then(|path| input.resolve(path)) else {
                    self.unmapped(
                        b,
                        name,
                        &format!("{name}.HeartbeatSecondsPath"),
                        "heartbeat reference path does not resolve to addressable state input",
                    );
                    return Ok(());
                };
                ("HeartbeatSecondsPath", json!(format!("{{{{{resolved}}}}}")))
            }
        };
        if block["type"] != "step" || resource == "arn:aws:states:::http:invoke" {
            self.unmapped(
                b,
                name,
                &format!("{name}.{field}"),
                "renewable heartbeats require an external worker task",
            );
            return Ok(());
        }
        if let (Some(seconds), Some(timeout)) = (seconds.as_u64(), block["timeout"].as_u64()) {
            anyhow::ensure!(
                seconds * 1000 < timeout,
                "Task `{name}` HeartbeatSeconds must be less than TimeoutSeconds"
            );
        }
        let params = block["params"].take();
        block["params"] = json!({
            (orch8_types::worker::ASL_HEARTBEAT_PARAM): {"seconds": seconds, "params": params}
        });
        b.mapped(
            name,
            field,
            block["id"].as_str().unwrap_or(name),
            "renewable worker deadline",
        );
        Ok(())
    }

    fn worker(
        &self,
        b: &mut Builder,
        name: &str,
        id: &str,
        node_type: &str,
        handler: &str,
        params: Value,
        reason: &str,
    ) -> Value {
        b.report.todos.push(super::TodoNode {
            node: name.into(),
            node_type: node_type.into(),
            block_id: id.into(),
            handler: handler.into(),
            reason: reason.into(),
        });
        if !b.report.worker_handlers.iter().any(|h| h == handler) {
            b.report.worker_handlers.push(handler.into());
        }
        json!({"type": "step", "id": id, "handler": handler, "params": params})
    }

    /// Retain error matches and attempt budgets in source order.
    fn retry(&self, b: &mut Builder, name: &str, state: &Value) -> Option<Value> {
        let retriers = state.get("Retry")?.as_array()?;
        let rules: Vec<_> = retriers
            .iter()
            .enumerate()
            .filter_map(|(index, rule)| self.retry_rule(b, name, index, rule))
            .collect();
        let mut policy = rules.first()?.clone();
        if rules
            .iter()
            .all(|rule| rule["max_attempts"].as_u64() == Some(0))
        {
            return None;
        }
        let outer = policy.as_object_mut()?;
        outer.remove("error_equals");
        if outer["max_attempts"].as_u64() == Some(0) {
            outer.insert("max_attempts".into(), json!(1));
        }
        outer.insert("rules".into(), json!(rules));
        Some(policy)
    }

    fn retry_rule(&self, b: &mut Builder, name: &str, index: usize, rule: &Value) -> Option<Value> {
        let Some(errors) = rule
            .get("ErrorEquals")
            .and_then(Value::as_array)
            .filter(|errors| !errors.is_empty() && errors.iter().all(Value::is_string))
        else {
            self.unmapped(
                b,
                name,
                &format!("{name}.Retry[{index}].ErrorEquals"),
                "expected a list of error names",
            );
            return None;
        };
        let jitter = match rule.get("JitterStrategy").and_then(Value::as_str) {
            Some("FULL") => "full",
            None | Some("NONE") => "none",
            Some(other) => {
                self.unmapped(
                    b,
                    name,
                    &format!("{name}.Retry[{index}].JitterStrategy"),
                    &format!("unsupported retry jitter {other}"),
                );
                "none"
            }
        };
        let retries = rule.get("MaxAttempts").and_then(Value::as_u64).unwrap_or(3);
        let interval = rule
            .get("IntervalSeconds")
            .and_then(Value::as_f64)
            .unwrap_or(1.0);
        let rate = rule
            .get("BackoffRate")
            .and_then(Value::as_f64)
            .unwrap_or(2.0);
        let max_delay = rule
            .get("MaxDelaySeconds")
            .and_then(Value::as_f64)
            .unwrap_or_else(|| {
                // Without a cap Step Functions keeps growing the interval; the
                // last retry waits interval * rate^(retries - 1).
                #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                let exp = retries.saturating_sub(1).min(64) as i32;
                (interval * rate.powi(exp)).min(86_400.0)
            });
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        Some(json!({
            "error_equals": errors,
            "max_attempts": retries.min(u64::from(u32::MAX)),
            "initial_backoff": (interval.min(max_delay) * 1000.0).round() as u64,
            "max_backoff": (max_delay * 1000.0).round() as u64,
            "backoff_multiplier": rate,
            "jitter": jitter,
        }))
    }

    /// Wrap a result-producing block in `try_catch` when the state has
    /// `Catch`, then append it and pick the next state.
    fn with_catch(
        &self,
        b: &mut Builder,
        m: &Machine<'_>,
        name: &str,
        state: &Value,
        success: Vec<Value>,
        after: Env,
        stop: Option<&str>,
        env: &mut Env,
        blocks: &mut Vec<Value>,
        path: &mut Vec<String>,
    ) -> Result<Option<String>> {
        let normal_next = if state.get("End") == Some(&json!(true)) {
            None
        } else {
            next_of(state).map(str::to_string)
        };
        let catchers = state
            .get("Catch")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if catchers.is_empty() {
            blocks.extend(success);
            *env = after;
            return Ok(normal_next);
        }
        let catch_targets: Vec<_> = catchers
            .iter()
            .map(|catcher| next_of(catcher).context("Catch without `Next`"))
            .collect::<Result<_>>()?;
        let tc_id = b.unique_id(&format!("{name}_try"));
        let targets: Vec<_> = std::iter::once(normal_next.as_deref())
            .chain(catch_targets.iter().copied().map(Some))
            .collect();
        let join = m.join(&targets, stop);
        let router = b.unique_id(&format!("{name}_catch_routes"));
        let mut routes = Vec::new();
        for (index, (catcher, target)) in catchers.iter().zip(catch_targets).enumerate() {
            let errors = catcher["ErrorEquals"]
                .as_array()
                .filter(|errors| !errors.is_empty())
                .context("Catch requires ErrorEquals")?;
            let conditions: Vec<_> = errors.iter().map(|error| {
                let error = error.as_str().context("Catch ErrorEquals entries must be strings")?;
                Ok(match error {
                    "States.ALL" => r#"(data._error.code != "States.Runtime" && data._error.code != "States.DataLimitExceeded")"#.to_owned(),
                    "States.TaskFailed" => r#"(data._error.code != "States.Timeout" && data._error.code != "States.Runtime" && data._error.code != "States.DataLimitExceeded")"#.to_owned(),
                    other => format!("data._error.code == {}", json!(other)),
                })
            }).collect::<Result<_>>()?;
            let mut branch = Vec::new();
            let catch_env = if catcher.get("ResultPath").is_some_and(Value::is_null) {
                env.clone()
            } else {
                let captured = b.unique_id(&format!("{name}_error_{index}"));
                branch.push(json!({"type":"step","id":captured,"handler":"transform",
                    "params":{"Error":"{{ data._error.code }}","Cause":"{{ data._error.message }}"}}));
                self.after_result(
                    b,
                    name,
                    catcher,
                    env,
                    Root::at(format!("outputs.{captured}")),
                    &mut branch,
                )
            };
            let (chain, _) = self.chain(b, m, target, join.as_deref(), catch_env, path)?;
            branch.extend(chain);
            routes.push(json!({
                "condition": conditions.join(" || "),
                "blocks": non_empty(branch, b, &format!("{router}_{index}")),
            }));
        }
        let unmatched = b.unique_id(&format!("{name}_uncaught"));
        let mut catch_blocks = vec![json!({
            "type": "router", "id": router, "routes": routes,
            "default": [{"type": "step", "id": unmatched, "handler": "fail",
                "params": {"error": "{{ data._error.code }}", "message": "{{ data._error.message }}", "retryable": false}}]
        })];
        let rejoins_normal = join.is_some() && join == normal_next;
        b.mapped(
            name,
            "Catch",
            &tc_id,
            "try_catch with an ordered error router",
        );
        if rejoins_normal || normal_next.is_none() {
            let catch_blocks = non_empty(catch_blocks, b, &format!("{tc_id}_catch"));
            blocks.push(json!({"type": "try_catch", "id": tc_id, "try_block": success, "catch_block": catch_blocks}));
            *env = after;
            return Ok(if rejoins_normal { normal_next } else { join });
        }
        // The error path and the success path diverge: mark the error path
        // so a router after the try_catch runs the success chain only when
        // no error was caught.
        let marker = b.unique_id(&format!("{name}_caught"));
        catch_blocks.insert(
            0,
            json!({"type": "step", "id": marker, "handler": "noop", "params": {}}),
        );
        blocks.push(json!({"type": "try_catch", "id": tc_id, "try_block": success, "catch_block": catch_blocks}));
        let (ok_blocks, ok_env) = self.chain(
            b,
            m,
            normal_next.as_deref().unwrap_or_default(),
            join.as_deref(),
            after,
            path,
        )?;
        let route_id = b.unique_id(&format!("{name}_succeeded"));
        blocks.push(json!({
            "type": "router",
            "id": route_id,
            "routes": [{
                "condition": format!("outputs.{marker} == null"),
                "blocks": non_empty(ok_blocks, b, &format!("{route_id}_ok")),
            }],
        }));
        *env = ok_env;
        Ok(join)
    }

    fn choice(
        &self,
        b: &mut Builder,
        m: &Machine<'_>,
        name: &str,
        state: &Value,
        stop: Option<&str>,
        env: &mut Env,
        blocks: &mut Vec<Value>,
        path: &mut Vec<String>,
    ) -> Result<Option<String>> {
        let rules = state
            .get("Choices")
            .and_then(Value::as_array)
            .with_context(|| format!("Choice `{name}` has no `Choices`"))?;
        let default = state.get("Default").and_then(Value::as_str);
        let input = env.narrowed(state.get("InputPath"));

        // A rule that leads back here is a polling loop.
        if m.can_reach(name, name) {
            return self.choice_loop(
                b, m, name, state, rules, default, stop, &input, env, blocks, path,
            );
        }

        let mut targets: Vec<Option<&str>> = rules.iter().map(|r| next_of(r)).collect();
        if let Some(d) = default {
            targets.push(Some(d));
        }
        let join = m.join(&targets, stop);
        let id = b.unique_id(name);
        let mut routes = Vec::new();
        let mut branch_envs = Vec::new();
        for (i, rule) in rules.iter().enumerate() {
            let next =
                next_of(rule).with_context(|| format!("Choice `{name}` rule {i} has no `Next`"))?;
            let condition = self.condition(b, name, rule, &input).unwrap_or_else(|| {
                let flag = format!("todo_{id}_rule_{i}");
                self.unmapped(b, name, &format!("{name}.Choices[{i}] {rule}"), &format!("condition could not be translated; the route is gated on `data.{flag} == true` until rewritten"));
                b.report.warnings.push(format!("Choice `{name}` rule {i}: untranslatable condition — see unmapped"));
                format!("data.{flag} == true")
            });
            let (bl, e) = self.chain(b, m, next, join.as_deref(), input.clone(), path)?;
            branch_envs.push(e);
            routes.push(json!({"condition": condition, "blocks": non_empty(bl, b, &format!("{id}_route_{i}"))}));
        }
        let default_blocks = if let Some(d) = default {
            let (bl, e) = self.chain(b, m, d, join.as_deref(), input.clone(), path)?;
            branch_envs.push(e);
            non_empty(bl, b, &format!("{id}_default"))
        } else {
            let fail_id = b.unique_id(&format!("{name}_no_match"));
            vec![
                json!({"type": "step", "id": fail_id, "handler": "fail", "params": {"message": format!("States.NoChoiceMatched: no rule of Choice `{name}` matched"), "error": "States.NoChoiceMatched", "retryable": false}}),
            ]
        };
        b.mapped(name, "Choice", &id, "router");
        blocks
            .push(json!({"type": "router", "id": id, "routes": routes, "default": default_blocks}));
        *env = if branch_envs.windows(2).all(|w| w[0] == w[1]) {
            branch_envs.pop().unwrap_or(input)
        } else {
            if join.is_some() {
                b.report.warnings.push(format!(
                    "Choice `{name}`: branches produce different data before rejoining; paths after \
                     the router are resolved against the Choice input"
                ));
            }
            input
        };
        Ok(join)
    }

    /// `A → ... → Choice ─(continue)→ ... → A` / `─(exit)→ E`: a do-while.
    /// Emitted as the forward part once, then a `loop` whose body is the
    /// continue path followed by the forward part again.
    fn choice_loop(
        &self,
        b: &mut Builder,
        m: &Machine<'_>,
        name: &str,
        state: &Value,
        rules: &[Value],
        default: Option<&str>,
        stop: Option<&str>,
        input: &Env,
        env: &mut Env,
        blocks: &mut Vec<Value>,
        path: &mut Vec<String>,
    ) -> Result<Option<String>> {
        // The loop header is the earliest state on the current path that the
        // cycle returns to (or the Choice itself).
        let header = path
            .iter()
            .find(|p| m.can_reach(name, p) && m.can_reach(p, name))
            .cloned()
            .unwrap_or_else(|| name.to_string());
        let in_loop = |target: &str| target == header || m.reachable(target).contains(&header);
        let mut continue_rules = Vec::new();
        let mut exit_rules = Vec::new();
        for rule in rules {
            match next_of(rule) {
                Some(n) if in_loop(n) => continue_rules.push(rule.clone()),
                Some(_) => exit_rules.push(rule.clone()),
                None => bail!("Choice `{name}` rule has no `Next`"),
            }
        }
        let default_continues = default.is_some_and(in_loop);
        let continue_targets: HashSet<&str> = continue_rules
            .iter()
            .filter_map(|r| next_of(r))
            .chain(default.filter(|_| default_continues))
            .collect();
        if continue_targets.len() != 1 {
            let stub = b.todo_stub(
                name,
                "Choice (loop)",
                state,
                "loop with several re-entry paths — rebuild it as an Orch8 `loop` block",
            );
            self.unmapped(
                b,
                name,
                &format!("{name} (cycle)"),
                "complex loop replaced by a TODO stub",
            );
            blocks.push(stub);
            return Ok(None);
        }
        let back_start = continue_targets
            .into_iter()
            .next()
            .unwrap_or_default()
            .to_string();

        // Loop conditions read `context.data` only (not step outputs).
        let data_env = Env::data();
        let translate = |sfn: &Sfn<'_>, b: &mut Builder, rules: &[Value]| -> Option<String> {
            let parts: Option<Vec<String>> = rules
                .iter()
                .map(|r| {
                    sfn.condition(b, name, r, &data_env)
                        .map(|c| format!("({c})"))
                })
                .collect();
            parts.map(|p| {
                if p.is_empty() {
                    "false".to_string()
                } else {
                    p.join(" || ")
                }
            })
        };
        let condition = if default_continues {
            translate(self, b, &exit_rules).map(|c| format!("!({c})"))
        } else {
            translate(self, b, &continue_rules)
        };
        let loop_id = b.unique_id(&format!("{header}_loop"));
        let condition = condition.unwrap_or_else(|| {
            self.unmapped(b, name, &format!("{name}.Choices"), &format!("loop condition could not be translated; gated on `data.todo_{loop_id}_continue == true`"));
            format!("data.todo_{loop_id}_continue == true")
        });
        self.unmapped(
            b,
            name,
            &format!("{name} (loop)"),
            &format!(
                "polling loop → `loop` `{loop_id}` with condition `{condition}`: loop conditions read \
                 context.data (not step outputs) — make the worker that produces the checked field \
                 return it at the top level of its output (worker outputs merge into context.data); \
                 the forward part is emitted once before the loop and again at the end of each \
                 iteration"
            ),
        );

        // Body: continue path back to the header, then the forward part
        // (header .. Choice) again.
        let depth = path.len();
        let header_pos = path.iter().position(|p| *p == header).unwrap_or(depth);
        let loop_path: Vec<String> = path[..header_pos].to_vec();
        let mut lp = loop_path.clone();
        let (mut body, _) = if back_start == header {
            (Vec::new(), data_env.clone())
        } else {
            self.chain(b, m, &back_start, Some(&header), input.clone(), &mut lp)?
        };
        if header != name {
            let mut lp = loop_path.clone();
            let (forward, _) = self.chain(b, m, &header, Some(name), input.clone(), &mut lp)?;
            body.extend(forward);
        }
        let body = non_empty(body, b, &format!("{loop_id}_body"));
        b.mapped(name, "Choice (loop)", &loop_id, "loop");
        blocks.push(json!({"type": "loop", "id": loop_id, "condition": condition, "max_iterations": 1000, "body": body}));

        // After the loop: the exit rules pick where to continue.
        *env = input.clone();
        let exit_default = default.filter(|_| !default_continues);
        let mut targets: Vec<Option<&str>> = exit_rules.iter().map(|r| next_of(r)).collect();
        if let Some(d) = exit_default {
            targets.push(Some(d));
        }
        if targets.len() == 1 {
            return Ok(targets[0].map(str::to_string));
        }
        let join = m.join(&targets, stop);
        let id = b.unique_id(&format!("{name}_exit"));
        let mut routes = Vec::new();
        for (i, rule) in exit_rules.iter().enumerate() {
            let cond = self
                .condition(b, name, rule, input)
                .unwrap_or_else(|| format!("data.todo_{id}_rule_{i} == true"));
            let (bl, _) = self.chain(
                b,
                m,
                next_of(rule).unwrap_or_default(),
                join.as_deref(),
                input.clone(),
                path,
            )?;
            routes.push(
                json!({"condition": cond, "blocks": non_empty(bl, b, &format!("{id}_route_{i}"))}),
            );
        }
        let mut router = json!({"type": "router", "id": id, "routes": routes});
        if let Some(d) = exit_default {
            let (bl, _) = self.chain(b, m, d, join.as_deref(), input.clone(), path)?;
            router["default"] = Value::Array(non_empty(bl, b, &format!("{id}_default")));
        }
        blocks.push(router);
        Ok(join)
    }
}

/// `arn:aws:lambda:us-east-1:123:function:Charge:prod` / `Charge` → `charge`.
fn lambda_handler(function: &str) -> String {
    let name = function
        .split(":function:")
        .nth(1)
        .unwrap_or(function)
        .split(':')
        .next()
        .unwrap_or(function);
    snake(name)
}

fn activity_name(resource: &str) -> Option<&str> {
    let (_, name) = resource.split_once(":activity:")?;
    Some(name)
}

/// `2026-01-01T09:00:00Z` → `2026-01-01T09:00:00` (UTC wall clock).
fn utc_local(ts: &str) -> Option<String> {
    let parsed = chrono::DateTime::parse_from_rfc3339(ts).ok()?;
    Some(
        parsed
            .with_timezone(&chrono::Utc)
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string(),
    )
}

#[cfg(test)]
#[path = "import_sfn_tests.rs"]
mod tests;
