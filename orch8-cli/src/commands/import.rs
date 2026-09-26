//! `orch8 import n8n|zapier <file>` — convert an exported workflow into an
//! Orch8 sequence document (JSON or YAML) plus a conversion report.
//!
//! The converters are deliberately conservative: node types with a clear
//! Orch8 equivalent are mapped (HTTP → `http_request`, IF/Switch/Filter/Paths
//! → `router`, fan-out → `parallel`, Wait/Delay → a delayed `noop`, Set /
//! Edit Fields / Formatter → `transform`), code / Slack / email steps become
//! external-worker stubs that carry the original configuration, triggers
//! (webhook, schedule) are reported as trigger / cron definitions to create
//! next to the sequence, and anything else becomes a visible `log` TODO stub.
//! Every decision is listed in the report — nothing is dropped silently.

// The converters are long, table-like `match`es over foreign node types;
// splitting them further would scatter one mapping across several helpers.
#![allow(
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::needless_pass_by_value,
    clippy::single_match_else,
    clippy::match_same_arms,
    clippy::unused_self,
    clippy::map_unwrap_or
)]

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::seqdoc::{DocumentFormat, FormatArg};

#[derive(Debug, Subcommand)]
pub enum ImportCmd {
    /// Convert an exported n8n workflow (`.json` from "Download") to a sequence.
    N8n(ImportArgs),
    /// Convert an exported Zapier zap (zapfile JSON) to a sequence.
    Zapier(ImportArgs),
}

#[derive(Debug, Args)]
pub struct ImportArgs {
    /// Exported workflow file.
    pub file: PathBuf,
    /// Output sequence file (`.json`, `.yaml`, `.yml`); prints to stdout when omitted.
    #[arg(long)]
    pub out: Option<PathBuf>,
    /// Syntax for stdout output (a file's extension wins).
    #[arg(long, value_enum, default_value = "json")]
    pub format: FormatArg,
    /// Sequence name (defaults to a slug of the workflow name).
    #[arg(long)]
    pub name: Option<String>,
    /// Sequence namespace.
    #[arg(long, default_value = "default")]
    pub namespace: String,
    /// Write the machine-readable conversion report (JSON) here.
    #[arg(long)]
    pub report: Option<PathBuf>,
    /// Zapier only: zap id or title to convert when the file holds several.
    #[arg(long)]
    pub zap: Option<String>,
}

/// One converted node.
#[derive(Debug, Clone, Serialize)]
pub struct MappedNode {
    pub node: String,
    pub node_type: String,
    pub block_id: String,
    pub mapped_to: String,
}

/// A node that needs manual work.
#[derive(Debug, Clone, Serialize)]
pub struct TodoNode {
    pub node: String,
    pub node_type: String,
    pub block_id: String,
    /// Handler of the generated stub (`log` for TODO stubs, the worker
    /// handler name for worker stubs).
    pub handler: String,
    pub reason: String,
}

/// Trigger / schedule to create next to the sequence.
#[derive(Debug, Clone, Serialize)]
pub struct TriggerSuggestion {
    pub node: String,
    /// `webhook`, `cron`, or `manual`.
    pub kind: String,
    /// Request body for `POST /triggers` or `POST /cron` (fill in ids).
    pub body: Value,
    pub next_step: String,
}

/// Everything the converter decided.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ConversionReport {
    pub source: String,
    pub workflow: String,
    pub sequence_name: String,
    pub mapped: Vec<MappedNode>,
    pub todos: Vec<TodoNode>,
    pub triggers: Vec<TriggerSuggestion>,
    /// External handlers a worker must implement (code / Slack / email stubs).
    pub worker_handlers: Vec<String>,
    pub warnings: Vec<String>,
}

/// Result of a conversion.
#[derive(Debug, Clone)]
pub struct Conversion {
    /// Authoring-form sequence document (no `id` / `created_at`; `orch8 dev`
    /// and `orch8 sequence apply` stamp those).
    pub sequence: Value,
    pub report: ConversionReport,
}

pub fn run(cmd: ImportCmd, tenant_id: Option<&str>) -> Result<()> {
    let (source, args) = match cmd {
        ImportCmd::N8n(args) => ("n8n", args),
        ImportCmd::Zapier(args) => ("zapier", args),
    };
    let raw = std::fs::read_to_string(&args.file)
        .with_context(|| format!("failed to read {}", args.file.display()))?;
    let export: Value = serde_json::from_str(&raw)
        .with_context(|| format!("{} is not valid JSON", args.file.display()))?;
    let options = ConvertOptions {
        tenant_id: tenant_id.unwrap_or("default").to_string(),
        namespace: args.namespace.clone(),
        name: args.name.clone(),
        zap: args.zap.clone(),
    };
    let conversion = match source {
        "n8n" => convert_n8n(&export, &options)?,
        _ => convert_zapier(&export, &options)?,
    };

    match &args.out {
        Some(path) => {
            crate::seqdoc::write_document(path, &conversion.sequence)?;
            eprintln!("wrote {}", path.display());
        }
        None => print!(
            "{}",
            crate::seqdoc::render(&conversion.sequence, DocumentFormat::from(args.format))?
        ),
    }
    if let Some(path) = &args.report {
        crate::atomic_write(
            path,
            format!("{}\n", serde_json::to_string_pretty(&conversion.report)?).as_bytes(),
        )?;
        eprintln!("wrote conversion report {}", path.display());
    }
    eprint!("{}", summarize(&conversion.report));
    Ok(())
}

/// Human summary printed to stderr.
pub fn summarize(report: &ConversionReport) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "imported {} workflow '{}' as sequence '{}': {} node(s) mapped, {} TODO(s), {} trigger(s)",
        report.source,
        report.workflow,
        report.sequence_name,
        report.mapped.len(),
        report.todos.len(),
        report.triggers.len()
    );
    for todo in &report.todos {
        let _ = writeln!(
            out,
            "  TODO {} ({}) → block '{}' [{}]: {}",
            todo.node, todo.node_type, todo.block_id, todo.handler, todo.reason
        );
    }
    for trigger in &report.triggers {
        let _ = writeln!(
            out,
            "  trigger {} ({}): {}",
            trigger.node, trigger.kind, trigger.next_step
        );
    }
    if !report.worker_handlers.is_empty() {
        let _ = writeln!(
            out,
            "  workers needed for: {} (try `orch8 dev --worker \"<cmd>\"`)",
            report.worker_handlers.join(", ")
        );
    }
    for warning in &report.warnings {
        let _ = writeln!(out, "  warning: {warning}");
    }
    out
}

/// Conversion inputs shared by both sources.
#[derive(Debug, Clone)]
pub struct ConvertOptions {
    pub tenant_id: String,
    pub namespace: String,
    pub name: Option<String>,
    pub zap: Option<String>,
}

impl Default for ConvertOptions {
    fn default() -> Self {
        Self {
            tenant_id: "default".into(),
            namespace: "default".into(),
            name: None,
            zap: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Lowercase `[a-z0-9_]` slug, never empty.
fn slug(text: &str) -> String {
    let mut out = String::new();
    for c in text.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('_') && !out.is_empty() {
            out.push('_');
        }
    }
    let out = out.trim_end_matches('_').to_string();
    if out.is_empty() { "step".into() } else { out }
}

struct Builder {
    used_ids: HashSet<String>,
    report: ConversionReport,
}

impl Builder {
    fn new(source: &str, workflow: &str, sequence_name: &str) -> Self {
        Self {
            used_ids: HashSet::new(),
            report: ConversionReport {
                source: source.into(),
                workflow: workflow.into(),
                sequence_name: sequence_name.into(),
                ..ConversionReport::default()
            },
        }
    }

    fn unique_id(&mut self, name: &str) -> String {
        let base = slug(name);
        let mut id = base.clone();
        let mut n = 2;
        while !self.used_ids.insert(id.clone()) {
            id = format!("{base}_{n}");
            n += 1;
        }
        id
    }

    fn mapped(&mut self, node: &str, node_type: &str, block_id: &str, mapped_to: &str) {
        self.report.mapped.push(MappedNode {
            node: node.into(),
            node_type: node_type.into(),
            block_id: block_id.into(),
            mapped_to: mapped_to.into(),
        });
    }

    /// A `log` TODO stub keeping the original configuration.
    fn todo_stub(&mut self, node: &str, node_type: &str, original: &Value, reason: &str) -> Value {
        let id = self.unique_id(node);
        self.report.todos.push(TodoNode {
            node: node.into(),
            node_type: node_type.into(),
            block_id: id.clone(),
            handler: "log".into(),
            reason: reason.into(),
        });
        json!({
            "type": "step",
            "id": id,
            "handler": "log",
            "params": {
                "message": format!("TODO(import): '{node}' ({node_type}) was not converted — {reason}"),
                "todo": reason,
                "original_type": node_type,
                "original_parameters": original,
            }
        })
    }

    /// An external-worker stub: the step dispatches to `handler`, carrying
    /// the original configuration in `params`.
    fn worker_stub(
        &mut self,
        node: &str,
        node_type: &str,
        handler: &str,
        params: Value,
        reason: &str,
    ) -> Value {
        let id = self.unique_id(node);
        self.report.todos.push(TodoNode {
            node: node.into(),
            node_type: node_type.into(),
            block_id: id.clone(),
            handler: handler.into(),
            reason: reason.into(),
        });
        if !self.report.worker_handlers.iter().any(|h| h == handler) {
            self.report.worker_handlers.push(handler.into());
        }
        json!({ "type": "step", "id": id, "handler": handler, "params": params })
    }
}

/// Wrap blocks so a branch/route is never empty (validation rejects empty
/// branches).
fn non_empty(mut blocks: Vec<Value>, b: &mut Builder, label: &str) -> Vec<Value> {
    if blocks.is_empty() {
        let id = b.unique_id(&format!("{label}_empty"));
        blocks.push(json!({"type": "step", "id": id, "handler": "noop", "params": {}}));
    }
    blocks
}

fn finish(b: Builder, options: &ConvertOptions, blocks: Vec<Value>) -> Result<Conversion> {
    if blocks.is_empty() {
        bail!("the workflow has no convertible steps after its trigger");
    }
    let sequence = json!({
        "$schema": "https://orch8.io/contracts/sequence.schema.json",
        "schema_version": orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
        "tenant_id": options.tenant_id,
        "namespace": options.namespace,
        "name": b.report.sequence_name,
        "version": 1,
        "blocks": blocks,
    });
    Ok(Conversion {
        sequence,
        report: b.report,
    })
}

/// Parse `"5"`, `5`, `5.0` into f64.
fn number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// A condition literal: numbers stay numbers, booleans stay booleans,
/// everything else becomes a quoted string.
fn literal(value: &Value) -> String {
    match value {
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".into(),
        Value::String(s) => {
            if let Ok(n) = s.trim().parse::<f64>()
                && s.trim()
                    .chars()
                    .all(|c| c.is_ascii_digit() || c == '.' || c == '-')
            {
                return format!("{n}");
            }
            serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into())
        }
        other => serde_json::to_string(&other.to_string()).unwrap_or_default(),
    }
}

/// Compose `left OP right` for a normalized operator name.
fn comparison(left: &str, op: &str, right: &str) -> Option<String> {
    Some(match op {
        "eq" => format!("{left} == {right}"),
        "ne" => format!("{left} != {right}"),
        "gt" => format!("{left} > {right}"),
        "gte" => format!("{left} >= {right}"),
        "lt" => format!("{left} < {right}"),
        "lte" => format!("{left} <= {right}"),
        "contains" => format!("contains({left}, {right})"),
        "not_contains" => format!("!contains({left}, {right})"),
        "starts_with" => format!("starts_with({left}, {right})"),
        "ends_with" => format!("ends_with({left}, {right})"),
        "exists" => format!("{left} != null"),
        "not_exists" => format!("{left} == null"),
        "empty" => format!("({left} == null || {left} == \"\")"),
        "not_empty" => format!("({left} != null && {left} != \"\")"),
        "true" => format!("{left} == true"),
        "false" => format!("{left} == false"),
        _ => return None,
    })
}

fn join_conditions(parts: &[String], combinator: &str) -> String {
    match parts.len() {
        0 => "true".into(),
        1 => parts[0].clone(),
        _ => parts
            .iter()
            .map(|p| format!("({p})"))
            .collect::<Vec<_>>()
            .join(combinator),
    }
}

// ---------------------------------------------------------------------------
// n8n
// ---------------------------------------------------------------------------

struct N8nNode {
    name: String,
    node_type: String,
    params: Value,
}

struct N8n<'a> {
    nodes: HashMap<String, N8nNode>,
    /// node → output index → successor node names.
    edges: HashMap<String, Vec<Vec<String>>>,
    in_degree: HashMap<String, usize>,
    /// Node name → block id (or `data` for trigger nodes).
    refs: HashMap<String, String>,
    triggers: HashSet<String>,
    visited: HashSet<String>,
    options: &'a ConvertOptions,
}

fn n8n_short_type(node_type: &str) -> &str {
    node_type.rsplit('.').next().unwrap_or(node_type)
}

fn n8n_is_trigger(node_type: &str) -> bool {
    let short = n8n_short_type(node_type).to_ascii_lowercase();
    short.ends_with("trigger") || matches!(short.as_str(), "webhook" | "cron" | "interval")
}

/// Convert an n8n workflow export.
pub fn convert_n8n(export: &Value, options: &ConvertOptions) -> Result<Conversion> {
    let workflow = export
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("n8n workflow")
        .to_string();
    let raw_nodes = export
        .get("nodes")
        .and_then(Value::as_array)
        .context("not an n8n export: missing `nodes` array")?;
    let mut nodes = HashMap::new();
    for node in raw_nodes {
        let name = node
            .get("name")
            .and_then(Value::as_str)
            .context("n8n node without a name")?
            .to_string();
        let node_type = node
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        if node
            .get("disabled")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            continue;
        }
        nodes.insert(
            name.clone(),
            N8nNode {
                name,
                node_type,
                params: node.get("parameters").cloned().unwrap_or(json!({})),
            },
        );
    }
    let mut edges: HashMap<String, Vec<Vec<String>>> = HashMap::new();
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    if let Some(connections) = export.get("connections").and_then(Value::as_object) {
        for (from, outputs) in connections {
            let Some(main) = outputs.get("main").and_then(Value::as_array) else {
                continue;
            };
            let mut per_output = Vec::new();
            for output in main {
                let targets: Vec<String> = output
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|t| t.get("node").and_then(Value::as_str))
                    .filter(|t| nodes.contains_key(*t))
                    .map(str::to_string)
                    .collect();
                for target in &targets {
                    *in_degree.entry(target.clone()).or_default() += 1;
                }
                per_output.push(targets);
            }
            edges.insert(from.clone(), per_output);
        }
    }

    let name = options
        .name
        .clone()
        .unwrap_or_else(|| slug(&workflow).replace('_', "-"));
    let mut b = Builder::new("n8n", &workflow, &name);
    let triggers: HashSet<String> = nodes
        .values()
        .filter(|n| n8n_is_trigger(&n.node_type))
        .map(|n| n.name.clone())
        .collect();
    let mut conv = N8n {
        nodes,
        edges,
        in_degree,
        refs: HashMap::new(),
        triggers: triggers.clone(),
        visited: HashSet::new(),
        options,
    };

    // Entry points: successors of triggers, else nodes nobody points at.
    let mut sorted_triggers: Vec<&String> = triggers.iter().collect();
    sorted_triggers.sort();
    let mut entries: Vec<String> = Vec::new();
    for trigger in &sorted_triggers {
        conv.describe_trigger(&mut b, trigger);
        conv.refs.insert((*trigger).clone(), "data".into());
        for next in conv.successors(trigger, 0) {
            if !entries.contains(&next) {
                entries.push(next);
            }
        }
    }
    if entries.is_empty() {
        let mut roots: Vec<String> = conv
            .nodes
            .keys()
            .filter(|n| !conv.in_degree.contains_key(*n) && !conv.triggers.contains(*n))
            .cloned()
            .collect();
        roots.sort();
        entries = roots;
        b.report
            .warnings
            .push("no trigger node found; conversion starts at the workflow's root node(s)".into());
    }
    if sorted_triggers.len() > 1 {
        b.report.warnings.push(format!(
            "{} trigger nodes found; their downstream paths are combined into one sequence",
            sorted_triggers.len()
        ));
    }

    let blocks = if entries.len() > 1 {
        let (block, next) = conv.fan_out(&mut b, "start", &entries, None, None);
        let mut blocks = vec![block];
        if let Some(next) = next {
            blocks.extend(conv.chain(&mut b, &next, None, None));
        }
        blocks
    } else if let Some(entry) = entries.first() {
        conv.chain(&mut b, entry, None, None)
    } else {
        Vec::new()
    };

    let mut unreached: Vec<&String> = conv
        .nodes
        .keys()
        .filter(|n| !conv.visited.contains(*n) && !conv.triggers.contains(*n))
        .collect();
    unreached.sort();
    for node in unreached {
        b.report.warnings.push(format!(
            "node '{node}' is not reachable from a trigger and was skipped"
        ));
    }
    finish(b, options, blocks)
}

impl N8n<'_> {
    fn successors(&self, node: &str, output: usize) -> Vec<String> {
        self.edges
            .get(node)
            .and_then(|outs| outs.get(output))
            .cloned()
            .unwrap_or_default()
    }

    fn is_join(&self, node: &str) -> bool {
        self.in_degree.get(node).copied().unwrap_or(0) > 1
    }

    /// Convert a linear chain starting at `start`, stopping at `stop` (a
    /// join node owned by an enclosing branch). `prev` is the node feeding
    /// `start` (its output is what `$json` means there). Returns the blocks.
    fn chain(
        &mut self,
        b: &mut Builder,
        start: &str,
        stop: Option<&str>,
        prev: Option<String>,
    ) -> Vec<Value> {
        let mut blocks = Vec::new();
        let mut current = Some(start.to_string());
        let mut prev = prev;
        while let Some(node) = current.take() {
            if Some(node.as_str()) == stop {
                break;
            }
            if !self.visited.insert(node.clone()) {
                b.report.warnings.push(format!(
                    "node '{node}' is reached twice (loop or re-entry); the second path stops here"
                ));
                break;
            }
            let (new_blocks, next) = self.convert_node(b, &node, prev.as_deref());
            blocks.extend(new_blocks);
            prev = Some(node.clone());
            current = next.filter(|n| Some(n.as_str()) != stop);
        }
        blocks
    }

    /// Convert one node. Returns its blocks and the node to continue with.
    fn convert_node(
        &mut self,
        b: &mut Builder,
        name: &str,
        prev: Option<&str>,
    ) -> (Vec<Value>, Option<String>) {
        let Some(node) = self.nodes.get(name) else {
            return (Vec::new(), None);
        };
        let node_type = node.node_type.clone();
        let params = node.params.clone();
        let short = n8n_short_type(&node_type).to_string();
        let prev_ref = prev
            .and_then(|p| self.refs.get(p).cloned())
            .or_else(|| prev.is_none().then(|| "data".to_string()));

        match short.as_str() {
            "if" | "switch" => {
                // Routers pass their input through: downstream `$json` still
                // means the router's input.
                if let Some(input) = prev_ref.clone() {
                    self.refs.insert(name.to_string(), input);
                }
                return if short == "if" {
                    self.convert_if(b, name, &node_type, &params, prev_ref.as_deref())
                } else {
                    self.convert_switch(b, name, &node_type, &params, prev_ref.as_deref())
                };
            }
            _ => {}
        }

        let successors_all: Vec<Vec<String>> = self.edges.get(name).cloned().unwrap_or_default();
        let first_out = successors_all.first().cloned().unwrap_or_default();

        let block = match short.as_str() {
            "httpRequest" => {
                let id = b.unique_id(name);
                b.mapped(name, &node_type, &id, "http_request");
                Some(self.http_block(b, &id, &params, prev_ref.as_deref()))
            }
            "set" => {
                let id = b.unique_id(name);
                b.mapped(name, &node_type, &id, "transform");
                let fields = self.set_fields(b, &params, prev_ref.as_deref());
                Some(json!({"type": "step", "id": id, "handler": "transform", "params": fields}))
            }
            "wait" => {
                let id = b.unique_id(name);
                match wait_millis(&params) {
                    Some(ms) => {
                        b.mapped(name, &node_type, &id, "noop + delay");
                        Some(
                            json!({"type": "step", "id": id, "handler": "noop", "params": {}, "delay": {"duration": ms}}),
                        )
                    }
                    None => {
                        b.used_ids.remove(&id);
                        Some(b.todo_stub(
                            name,
                            &node_type,
                            &params,
                            "wait-for-webhook / specific-time resume has no direct mapping; use a \
                             `wait_for_input` gate or a signal",
                        ))
                    }
                }
            }
            "merge" => {
                // Pure join: the router/parallel above already converged.
                b.report.mapped.push(MappedNode {
                    node: name.into(),
                    node_type: node_type.clone(),
                    block_id: String::new(),
                    mapped_to: "join (implicit — blocks after a router/parallel run once all branches finish)".into(),
                });
                None
            }
            "noOp" | "stickyNote" => {
                b.report.mapped.push(MappedNode {
                    node: name.into(),
                    node_type: node_type.clone(),
                    block_id: String::new(),
                    mapped_to: "dropped (no-op)".into(),
                });
                None
            }
            "code" | "function" | "functionItem" => {
                let (language, code) =
                    if let Some(code) = params.get("pythonCode").and_then(Value::as_str) {
                        ("python", code.to_string())
                    } else {
                        let code = params
                            .get("jsCode")
                            .or_else(|| params.get("functionCode"))
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        ("javascript", code)
                    };
                let handler = format!("n8n_code_{}", slug(name));
                let block = b.worker_stub(
                    name,
                    &node_type,
                    &handler,
                    json!({
                        "language": language,
                        "code": code,
                        "input": prev_ref.as_deref().map(|r| format!("{{{{{r}}}}}")),
                        "mode": params.get("mode").cloned().unwrap_or(Value::Null),
                    }),
                    &format!(
                        "port this {language} code to a worker handler named '{handler}' (original in params.code)"
                    ),
                );
                Some(block)
            }
            "slack" => {
                let channel = params
                    .pointer("/channelId/value")
                    .or_else(|| params.get("channel"))
                    .cloned()
                    .unwrap_or(Value::Null);
                let text = params
                    .get("text")
                    .map(|t| self.translate_value(b, t, prev_ref.as_deref()))
                    .unwrap_or(Value::Null);
                Some(b.worker_stub(
                    name,
                    &node_type,
                    "slack_post_message",
                    json!({"channel": channel, "text": text, "original_parameters": params}),
                    "no built-in Slack handler; implement 'slack_post_message' in a worker (or use an Activepieces piece)",
                ))
            }
            "emailSend" | "gmail" | "microsoftOutlook" | "sendGrid" | "mailgun" => {
                let pick = |keys: &[&str]| {
                    keys.iter()
                        .find_map(|k| params.get(*k))
                        .cloned()
                        .unwrap_or(Value::Null)
                };
                let to = self.translate_value(
                    b,
                    &pick(&["toEmail", "sendTo", "toRecipients", "toList"]),
                    prev_ref.as_deref(),
                );
                let subject = self.translate_value(b, &pick(&["subject"]), prev_ref.as_deref());
                let body = self.translate_value(
                    b,
                    &pick(&["text", "message", "html", "bodyContent"]),
                    prev_ref.as_deref(),
                );
                Some(b.worker_stub(
                    name,
                    &node_type,
                    "send_email",
                    json!({"to": to, "subject": subject, "body": body, "from": params.get("fromEmail").cloned().unwrap_or(Value::Null)}),
                    "no built-in email handler; implement 'send_email' in a worker (or use an Activepieces piece)",
                ))
            }
            // A trigger in the middle of a flow was already described.
            _ if n8n_is_trigger(&node_type) => None,
            _ => Some(b.todo_stub(
                name,
                &node_type,
                &params,
                "no Orch8 mapping for this node type yet",
            )),
        };

        let mut blocks = Vec::new();
        if let Some(block) = block {
            if let Some(id) = block.get("id").and_then(Value::as_str) {
                self.refs.insert(name.to_string(), id.to_string());
            }
            blocks.push(block);
        } else if let Some(prev_ref) = prev_ref {
            // Transparent node: downstream `$json` still means the previous output.
            self.refs.insert(name.to_string(), prev_ref);
        }

        if successors_all.len() > 1 {
            b.report.warnings.push(format!(
                "node '{name}' has {} outputs; only the first is followed",
                successors_all.len()
            ));
        }
        match first_out.len() {
            0 => (blocks, None),
            1 => {
                let next = first_out[0].clone();
                (blocks, Some(next))
            }
            _ => {
                let (block, next) = self.fan_out(b, name, &first_out, None, Some(name));
                blocks.push(block);
                (blocks, next)
            }
        }
    }

    /// Several successors on one output → `parallel`, branches ending at a
    /// shared join node.
    fn fan_out(
        &mut self,
        b: &mut Builder,
        name: &str,
        targets: &[String],
        stop: Option<&str>,
        prev: Option<&str>,
    ) -> (Value, Option<String>) {
        let join = self.common_join(targets);
        let join_ref = join.as_deref().or(stop);
        let id = b.unique_id(&format!("{name}_fanout"));
        let branches: Vec<Value> = targets
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let blocks = self.chain(b, t, join_ref, prev.map(str::to_string));
                Value::Array(non_empty(blocks, b, &format!("{id}_{i}")))
            })
            .collect();
        b.mapped(name, "fan-out", &id, "parallel");
        (
            json!({"type": "parallel", "id": id, "branches": branches}),
            join,
        )
    }

    /// The first join node (in-degree > 1) reachable from every target, in
    /// BFS order from the first target.
    fn common_join(&self, targets: &[String]) -> Option<String> {
        let reach = |start: &str| -> Vec<String> {
            let mut order = Vec::new();
            let mut seen = HashSet::new();
            let mut queue = std::collections::VecDeque::from([start.to_string()]);
            while let Some(n) = queue.pop_front() {
                if !seen.insert(n.clone()) {
                    continue;
                }
                order.push(n.clone());
                for out in self.edges.get(&n).into_iter().flatten() {
                    queue.extend(out.iter().cloned());
                }
            }
            order
        };
        let first = reach(targets.first()?);
        let others: Vec<HashSet<String>> = targets[1..]
            .iter()
            .map(|t| reach(t).into_iter().collect())
            .collect();
        first
            .into_iter()
            .find(|n| self.is_join(n) && others.iter().all(|set| set.contains(n)))
            .or_else(|| {
                // Branches that do not all converge: still stop at the first
                // join any branch reaches, so it runs once after the block.
                targets
                    .iter()
                    .flat_map(|t| reach(t))
                    .find(|n| self.is_join(n) && !targets.contains(n))
            })
    }

    fn router_from_outputs(
        &mut self,
        b: &mut Builder,
        name: &str,
        node_type: &str,
        conditions: Vec<String>,
        default_output: Option<usize>,
    ) -> (Vec<Value>, Option<String>) {
        let outputs: Vec<Vec<String>> = self.edges.get(name).cloned().unwrap_or_default();
        let mut all_targets: Vec<String> = outputs.iter().flatten().cloned().collect();
        all_targets.dedup();
        let join = self.common_join(&all_targets);
        let id = b.unique_id(name);
        let branch = |conv: &mut Self, b: &mut Builder, idx: usize| -> Vec<Value> {
            let targets = outputs.get(idx).cloned().unwrap_or_default();
            match targets.len() {
                0 => Vec::new(),
                1 => conv.chain(b, &targets[0], join.as_deref(), Some(name.to_string())),
                _ => {
                    let (block, _) = conv.fan_out(
                        b,
                        &format!("{name}_{idx}"),
                        &targets,
                        join.as_deref(),
                        Some(name),
                    );
                    vec![block]
                }
            }
        };
        let mut routes = Vec::new();
        for (idx, condition) in conditions.iter().enumerate() {
            let blocks = branch(self, b, idx);
            let blocks = non_empty(blocks, b, &format!("{id}_route_{idx}"));
            routes.push(json!({"condition": condition, "blocks": blocks}));
        }
        let mut router = json!({"type": "router", "id": id, "routes": routes});
        if let Some(default_idx) = default_output {
            let blocks = branch(self, b, default_idx);
            if !blocks.is_empty() {
                router["default"] = Value::Array(blocks);
            }
        }
        b.mapped(name, node_type, &id, "router");
        let non_converging: Vec<usize> = outputs
            .iter()
            .enumerate()
            .filter(|(_, targets)| !targets.is_empty())
            .filter(|(_, targets)| {
                join.as_ref().is_some_and(|j| {
                    targets.iter().all(|t| {
                        let mut stack = vec![t.clone()];
                        let mut seen = HashSet::new();
                        while let Some(n) = stack.pop() {
                            if &n == j {
                                return false;
                            }
                            if seen.insert(n.clone()) {
                                for out in self.edges.get(&n).into_iter().flatten() {
                                    stack.extend(out.iter().cloned());
                                }
                            }
                        }
                        true
                    })
                })
            })
            .map(|(i, _)| i)
            .collect();
        for i in non_converging {
            b.report.warnings.push(format!(
                "'{name}' output {i} ends without reaching '{}'; in Orch8 the blocks after the \
                 router run for every route",
                join.as_deref().unwrap_or("?")
            ));
        }
        (vec![router], join)
    }

    fn convert_if(
        &mut self,
        b: &mut Builder,
        name: &str,
        node_type: &str,
        params: &Value,
        prev_ref: Option<&str>,
    ) -> (Vec<Value>, Option<String>) {
        let condition = self.n8n_conditions(b, name, params, prev_ref);
        self.router_from_outputs(b, name, node_type, vec![condition], Some(1))
    }

    fn convert_switch(
        &mut self,
        b: &mut Builder,
        name: &str,
        node_type: &str,
        params: &Value,
        prev_ref: Option<&str>,
    ) -> (Vec<Value>, Option<String>) {
        let rules: Vec<Value> = params
            .pointer("/rules/values")
            .or_else(|| params.pointer("/rules/rules"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut conditions = Vec::new();
        for rule in &rules {
            if rule.get("conditions").is_some() {
                conditions.push(self.n8n_conditions(b, name, rule, prev_ref));
            } else if let (Some(value2), Some(value1)) = (
                rule.get("value2"),
                params
                    .get("value1")
                    .or_else(|| params.get("dataPropertyName")),
            ) {
                // Switch v1/v2: rules compare one `value1` to each `value2`.
                let left = self.translate_path(b, value1, prev_ref);
                let op = normalize_n8n_op(
                    rule.get("operation")
                        .and_then(Value::as_str)
                        .unwrap_or("equal"),
                );
                conditions.push(comparison(&left, op, &literal(value2)).unwrap_or_else(|| {
                    b.report.warnings.push(format!(
                        "'{name}': unsupported switch operation; route set to false"
                    ));
                    "false".into()
                }));
            }
        }
        if conditions.is_empty() {
            b.report.warnings.push(format!(
                "'{name}': switch mode not understood (expression mode?); routes need manual conditions"
            ));
            let outputs = self.edges.get(name).map_or(0, Vec::len);
            conditions = vec!["false".into(); outputs.max(1)];
        }
        let fallback = params
            .pointer("/options/fallbackOutput")
            .and_then(|v| match v {
                Value::String(s) if s == "extra" => Some(conditions.len()),
                Value::Number(n) => n.as_u64().and_then(|n| usize::try_from(n).ok()),
                _ => None,
            })
            .or_else(|| {
                params
                    .get("fallbackOutput")
                    .and_then(Value::as_u64)
                    .and_then(|n| usize::try_from(n).ok())
            });
        self.router_from_outputs(b, name, node_type, conditions, fallback)
    }

    /// n8n v2 (`conditions.conditions[]` + `combinator`) or v1
    /// (`conditions.{string,number,boolean}[]` + `combineOperation`).
    fn n8n_conditions(
        &self,
        b: &mut Builder,
        name: &str,
        params: &Value,
        prev_ref: Option<&str>,
    ) -> String {
        let Some(conditions) = params.get("conditions") else {
            b.report
                .warnings
                .push(format!("'{name}': no conditions; route set to true"));
            return "true".into();
        };
        let mut parts = Vec::new();
        if let Some(list) = conditions.get("conditions").and_then(Value::as_array) {
            for c in list {
                let left =
                    self.translate_path(b, c.get("leftValue").unwrap_or(&Value::Null), prev_ref);
                let right = c.get("rightValue").map_or_else(
                    || "null".into(),
                    |r| {
                        if r.as_str().is_some_and(|s| s.starts_with('=')) {
                            self.translate_path(b, r, prev_ref)
                        } else {
                            literal(r)
                        }
                    },
                );
                let op = normalize_n8n_op(
                    c.pointer("/operator/operation")
                        .and_then(Value::as_str)
                        .unwrap_or("equals"),
                );
                match comparison(&left, op, &right) {
                    Some(expr) => parts.push(expr),
                    None => b.report.warnings.push(format!(
                        "'{name}': operator {:?} is not supported; condition dropped",
                        c.pointer("/operator/operation")
                    )),
                }
            }
            let combinator = if conditions.get("combinator").and_then(Value::as_str) == Some("or") {
                " || "
            } else {
                " && "
            };
            return join_conditions(&parts, combinator);
        }
        for kind in ["string", "number", "boolean", "dateTime"] {
            for c in conditions
                .get(kind)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let left =
                    self.translate_path(b, c.get("value1").unwrap_or(&Value::Null), prev_ref);
                let right = literal(c.get("value2").unwrap_or(&Value::Null));
                let op = normalize_n8n_op(
                    c.get("operation")
                        .and_then(Value::as_str)
                        .unwrap_or("equal"),
                );
                if let Some(expr) = comparison(&left, op, &right) {
                    parts.push(expr);
                }
            }
        }
        let combinator = if params.get("combineOperation").and_then(Value::as_str) == Some("any") {
            " || "
        } else {
            " && "
        };
        join_conditions(&parts, combinator)
    }

    fn http_block(
        &self,
        b: &mut Builder,
        id: &str,
        params: &Value,
        prev_ref: Option<&str>,
    ) -> Value {
        let method = params
            .get("method")
            .or_else(|| params.get("requestMethod"))
            .and_then(Value::as_str)
            .unwrap_or("GET")
            .to_ascii_uppercase();
        let url = self.translate_value(b, params.get("url").unwrap_or(&Value::Null), prev_ref);
        let mut out = Map::new();
        out.insert("url".into(), url);
        out.insert("method".into(), json!(method));
        // Headers: `headerParameters.parameters[{name,value}]`.
        let headers: Map<String, Value> = params
            .pointer("/headerParameters/parameters")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|h| {
                Some((
                    h.get("name")?.as_str()?.to_string(),
                    self.translate_value(b, h.get("value")?, prev_ref),
                ))
            })
            .collect();
        if !headers.is_empty() {
            out.insert("headers".into(), Value::Object(headers));
        }
        // Body: raw JSON string, or `bodyParameters.parameters[{name,value}]`.
        if let Some(json_body) = params.get("jsonBody").and_then(Value::as_str) {
            let translated =
                translate_template_text(self, b, json_body.trim_start_matches('='), prev_ref);
            out.insert("body".into(), json!(translated));
        } else if let Some(fields) = params
            .pointer("/bodyParameters/parameters")
            .and_then(Value::as_array)
        {
            let body: Map<String, Value> = fields
                .iter()
                .filter_map(|f| {
                    Some((
                        f.get("name")?.as_str()?.to_string(),
                        self.translate_value(b, f.get("value")?, prev_ref),
                    ))
                })
                .collect();
            out.insert("body".into(), json!(Value::Object(body).to_string()));
            b.report.warnings.push(format!(
                "block '{id}': body parameters were serialized to a JSON string; template \
                 expressions inside it are resolved at runtime"
            ));
        }
        if let Some(timeout) = params.pointer("/options/timeout").and_then(Value::as_u64) {
            out.insert("timeout_ms".into(), json!(timeout));
        }
        if params
            .get("authentication")
            .and_then(Value::as_str)
            .is_some_and(|a| a != "none")
        {
            b.report.warnings.push(format!(
                "block '{id}': n8n credentials are not exported; add an Authorization header \
                 with a `credentials://<id>` reference"
            ));
        }
        json!({"type": "step", "id": id, "handler": "http_request", "params": Value::Object(out)})
    }

    fn set_fields(&self, b: &mut Builder, params: &Value, prev_ref: Option<&str>) -> Value {
        let mut fields = Map::new();
        // Set v3.3+: `assignments.assignments[{name,value}]`.
        for a in params
            .pointer("/assignments/assignments")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let (Some(name), Some(value)) =
                (a.get("name").and_then(Value::as_str), a.get("value"))
            {
                fields.insert(name.to_string(), self.translate_value(b, value, prev_ref));
            }
        }
        // Set v1/v2: `values.{string,number,boolean}[{name,value}]`.
        for kind in ["string", "number", "boolean"] {
            for a in params
                .pointer(&format!("/values/{kind}"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let (Some(name), Some(value)) =
                    (a.get("name").and_then(Value::as_str), a.get("value"))
                {
                    fields.insert(name.to_string(), self.translate_value(b, value, prev_ref));
                }
            }
        }
        // Raw JSON mode.
        if let Some(raw) = params.get("jsonOutput").and_then(Value::as_str)
            && let Ok(Value::Object(map)) =
                serde_json::from_str::<Value>(raw.trim_start_matches('='))
        {
            fields.extend(map);
        }
        Value::Object(fields)
    }

    /// Translate a parameter value: `=`-prefixed strings are n8n expressions.
    fn translate_value(&self, b: &mut Builder, value: &Value, prev_ref: Option<&str>) -> Value {
        match value {
            Value::String(s) if s.starts_with('=') => {
                json!(translate_template_text(self, b, &s[1..], prev_ref))
            }
            Value::Array(items) => Value::Array(
                items
                    .iter()
                    .map(|v| self.translate_value(b, v, prev_ref))
                    .collect(),
            ),
            Value::Object(map) => Value::Object(
                map.iter()
                    .map(|(k, v)| (k.clone(), self.translate_value(b, v, prev_ref)))
                    .collect(),
            ),
            other => other.clone(),
        }
    }

    /// Translate a condition operand to a bare expression path.
    fn translate_path(&self, b: &mut Builder, value: &Value, prev_ref: Option<&str>) -> String {
        match value {
            Value::String(s) if s.starts_with('=') => {
                let inner = s[1..].trim();
                let inner = inner
                    .strip_prefix("{{")
                    .and_then(|r| r.strip_suffix("}}"))
                    .unwrap_or(inner)
                    .trim();
                self.translate_expr(inner, prev_ref).unwrap_or_else(|| {
                    b.report.warnings.push(format!(
                        "expression `{inner}` could not be translated; replace it manually"
                    ));
                    format!("\"TODO: {}\"", inner.replace('"', "'"))
                })
            }
            other => literal(other),
        }
    }

    /// `$json.a.b`, `$('Node').item.json.a`, `$node["Node"].json.a` →
    /// `outputs.<block>.a` (or `data.a` for trigger payloads).
    fn translate_expr(&self, expr: &str, prev_ref: Option<&str>) -> Option<String> {
        let expr = expr.trim();
        let (target, rest) = if let Some(rest) = expr.strip_prefix("$json") {
            (prev_ref?.to_string(), rest)
        } else if let Some(after) = expr
            .strip_prefix("$('")
            .or_else(|| expr.strip_prefix("$(\""))
        {
            let end = after.find(['\'', '"'])?;
            let node = &after[..end];
            let rest = after[end + 1..].strip_prefix(')')?;
            let rest = rest
                .strip_prefix(".item.json")
                .or_else(|| rest.strip_prefix(".first().json"))
                .or_else(|| rest.strip_prefix(".last().json"))
                .or_else(|| rest.strip_prefix(".all()[0].json"))?;
            (self.refs.get(node)?.clone(), rest)
        } else {
            let after = expr
                .strip_prefix("$node[\"")
                .or_else(|| expr.strip_prefix("$node['"))?;
            let end = after.find(['\'', '"'])?;
            let node = &after[..end];
            let rest = after[end + 1..].strip_prefix(']')?.strip_prefix(".json")?;
            (self.refs.get(node)?.clone(), rest)
        };
        let path = json_path_suffix(rest)?;
        if target == "data" {
            // Webhook payloads: n8n nests the request body under `body`.
            let path = path.strip_prefix(".body").unwrap_or(&path);
            Some(format!("data{path}"))
        } else {
            Some(format!("outputs.{target}{path}"))
        }
    }
}

/// `.a.b` / `["a"].b` / empty → normalized `.a.b` suffix; `None` for
/// anything dynamic (method calls, arithmetic).
fn json_path_suffix(rest: &str) -> Option<String> {
    let mut out = String::new();
    let mut s = rest.trim();
    while !s.is_empty() {
        if let Some(r) = s.strip_prefix('.') {
            let end = r
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(r.len());
            if end == 0 {
                return None;
            }
            out.push('.');
            out.push_str(&r[..end]);
            s = &r[end..];
        } else if let Some(r) = s.strip_prefix("[\"").or_else(|| s.strip_prefix("['")) {
            let end = r.find(['"', '\''])?;
            let key = &r[..end];
            if !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return None;
            }
            out.push('.');
            out.push_str(key);
            s = r[end + 1..].strip_prefix(']')?;
        } else {
            let r = s.strip_prefix('[')?;
            let end = r.find(']')?;
            let idx = &r[..end];
            if !idx.chars().all(|c| c.is_ascii_digit()) {
                return None;
            }
            out.push('.');
            out.push_str(idx);
            s = &r[end + 1..];
        }
    }
    Some(out)
}

/// Replace every `{{ expr }}` segment of an n8n template with an Orch8
/// `{{path}}` template.
fn translate_template_text(
    conv: &N8n<'_>,
    b: &mut Builder,
    text: &str,
    prev_ref: Option<&str>,
) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            out.push_str(&rest[start..]);
            return out;
        };
        let expr = after[..end].trim();
        if let Some(path) = conv.translate_expr(expr, prev_ref) {
            out.push_str("{{");
            out.push_str(&path);
            out.push_str("}}");
        } else {
            b.report.warnings.push(format!(
                "expression `{expr}` could not be translated and was left as text"
            ));
            out.push_str("{{ ");
            out.push_str(expr);
            out.push_str(" }}");
        }
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    out
}

fn normalize_n8n_op(op: &str) -> &'static str {
    match op {
        "equals" | "equal" | "is" => "eq",
        "notEquals" | "notEqual" | "isNot" => "ne",
        "gt" | "larger" | "after" => "gt",
        "gte" | "largerEqual" | "afterOrEquals" => "gte",
        "lt" | "smaller" | "before" => "lt",
        "lte" | "smallerEqual" | "beforeOrEquals" => "lte",
        "contains" => "contains",
        "notContains" => "not_contains",
        "startsWith" => "starts_with",
        "endsWith" => "ends_with",
        "exists" => "exists",
        "notExists" => "not_exists",
        "empty" | "isEmpty" => "empty",
        "notEmpty" | "isNotEmpty" => "not_empty",
        "true" => "true",
        "false" => "false",
        _ => "unsupported",
    }
}

/// Wait node → milliseconds (`amount` + `unit`, v1 `value`/`unit`).
fn wait_millis(params: &Value) -> Option<u64> {
    let resume = params
        .get("resume")
        .and_then(Value::as_str)
        .unwrap_or("timeInterval");
    if resume != "timeInterval" {
        return None;
    }
    let amount = number(params.get("amount").or_else(|| params.get("value"))?)?;
    let unit = params
        .get("unit")
        .and_then(Value::as_str)
        .unwrap_or("seconds");
    unit_millis(amount, unit)
}

fn unit_millis(amount: f64, unit: &str) -> Option<u64> {
    let per = match unit.trim_end_matches('s') {
        "second" => 1_000.0,
        "minute" => 60_000.0,
        "hour" => 3_600_000.0,
        "day" => 86_400_000.0,
        "week" => 604_800_000.0,
        _ => return None,
    };
    let ms = (amount * per).round();
    (ms.is_finite() && ms >= 0.0).then(|| {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let ms = ms as u64;
        ms
    })
}

impl N8n<'_> {
    fn describe_trigger(&self, b: &mut Builder, name: &str) {
        let Some(node) = self.nodes.get(name) else {
            return;
        };
        let short = n8n_short_type(&node.node_type).to_string();
        let sequence_name = b.report.sequence_name.clone();
        let tenant = self.options.tenant_id.clone();
        let namespace = self.options.namespace.clone();
        let suggestion = match short.as_str() {
            "webhook" => {
                let path = node
                    .params
                    .get("path")
                    .and_then(Value::as_str)
                    .unwrap_or(name);
                let slug_value = slug(path).replace('_', "-");
                Some(TriggerSuggestion {
                    node: name.into(),
                    kind: "webhook".into(),
                    body: json!({
                        "slug": slug_value,
                        "sequence_name": sequence_name,
                        "tenant_id": tenant,
                        "namespace": namespace,
                        "trigger_type": "webhook",
                        "config": {},
                    }),
                    next_step: format!(
                        "POST the body to /triggers; senders then POST JSON to /webhooks/{slug_value} \
                         (public; set `secret` for HMAC) or /triggers/{slug_value}/fire (API key). The \
                         request body becomes context.data. n8n method was {}.",
                        node.params
                            .get("httpMethod")
                            .and_then(Value::as_str)
                            .unwrap_or("GET")
                    ),
                })
            }
            "scheduleTrigger" | "cron" => match n8n_cron(&node.params) {
                Some(expr) => Some(TriggerSuggestion {
                    node: name.into(),
                    kind: "cron".into(),
                    body: json!({
                        "tenant_id": tenant,
                        "namespace": namespace,
                        "sequence_id": "<id printed by `orch8 sequence create`>",
                        "cron_expr": expr,
                        "timezone": "UTC",
                    }),
                    next_step:
                        "POST the body to /cron after creating the sequence (set sequence_id)"
                            .into(),
                }),
                None => {
                    b.report.warnings.push(format!(
                        "schedule trigger '{name}' uses a rule that could not be converted to cron; \
                         create the cron schedule manually"
                    ));
                    None
                }
            },
            "manualTrigger" => Some(TriggerSuggestion {
                node: name.into(),
                kind: "manual".into(),
                body: json!({}),
                next_step: "start runs with `orch8 instance create` or `orch8 dev`".into(),
            }),
            _ => {
                b.report.todos.push(TodoNode {
                    node: name.into(),
                    node_type: node.node_type.clone(),
                    block_id: String::new(),
                    handler: String::new(),
                    reason: "trigger type has no Orch8 equivalent; start instances via a webhook trigger, NATS, or the API".into(),
                });
                None
            }
        };
        if let Some(s) = suggestion {
            b.report.triggers.push(s);
        }
    }
}

/// n8n Schedule Trigger rule → 5-field cron.
fn n8n_cron(params: &Value) -> Option<String> {
    if let Some(interval) = params.pointer("/rule/interval/0") {
        let field = interval
            .get("field")
            .and_then(Value::as_str)
            .unwrap_or("days");
        let n =
            |key: &str, default: u64| interval.get(key).and_then(Value::as_u64).unwrap_or(default);
        return match field {
            "cronExpression" => interval
                .get("expression")
                .and_then(Value::as_str)
                .map(str::to_string),
            "seconds" => None,
            "minutes" => Some(format!("*/{} * * * *", n("minutesInterval", 5))),
            "hours" => Some(format!(
                "{} */{} * * *",
                n("triggerAtMinute", 0),
                n("hoursInterval", 1)
            )),
            "days" => Some(format!(
                "{} {} */{} * *",
                n("triggerAtMinute", 0),
                n("triggerAtHour", 0),
                n("daysInterval", 1)
            )),
            "weeks" => {
                let days: Vec<String> = interval
                    .get("triggerAtDay")
                    .and_then(Value::as_array)
                    .map(|d| {
                        d.iter()
                            .filter_map(Value::as_u64)
                            .map(|d| d.to_string())
                            .collect()
                    })
                    .unwrap_or_else(|| vec!["0".into()]);
                Some(format!(
                    "{} {} * * {}",
                    n("triggerAtMinute", 0),
                    n("triggerAtHour", 0),
                    days.join(",")
                ))
            }
            "months" => Some(format!(
                "{} {} {} */{} *",
                n("triggerAtMinute", 0),
                n("triggerAtHour", 0),
                n("triggerAtDayOfMonth", 1),
                n("monthsInterval", 1)
            )),
            _ => None,
        };
    }
    // Legacy Cron node: `triggerTimes.item[{mode, hour, minute}]`.
    let item = params.pointer("/triggerTimes/item/0")?;
    let hour = item.get("hour").and_then(Value::as_u64).unwrap_or(0);
    let minute = item.get("minute").and_then(Value::as_u64).unwrap_or(0);
    match item.get("mode").and_then(Value::as_str)? {
        "everyMinute" => Some("* * * * *".into()),
        "everyHour" => Some(format!("{minute} * * * *")),
        "everyDay" => Some(format!("{minute} {hour} * * *")),
        "everyWeek" => Some(format!(
            "{minute} {hour} * * {}",
            item.get("weekday").and_then(Value::as_str).unwrap_or("1")
        )),
        "custom" => item.get("cronExpression").and_then(Value::as_str).map(|c| {
            // n8n custom cron may have a seconds field; the engine accepts 5, 6, or 7.
            c.to_string()
        }),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Zapier
// ---------------------------------------------------------------------------

struct Zapier {
    /// Zapier step id → block id (`data` for the trigger).
    refs: HashMap<String, String>,
}

fn zap_app(step: &Value) -> String {
    step.get("app")
        .or_else(|| step.get("selected_api"))
        .and_then(Value::as_str)
        .unwrap_or("UnknownAPI")
        .to_string()
}

fn zap_title(step: &Value) -> String {
    step.get("title")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| {
            format!(
                "{} {}",
                zap_app(step)
                    .trim_end_matches("API")
                    .trim_end_matches("CLI"),
                step.get("action").and_then(Value::as_str).unwrap_or("step")
            )
        })
}

fn zap_step_id(step: &Value) -> Option<String> {
    step.get("id").map(|id| match id {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    })
}

/// Convert a Zapier export (`{"zaps": [...]}`, a single zap, or a bare
/// `steps` array).
pub fn convert_zapier(export: &Value, options: &ConvertOptions) -> Result<Conversion> {
    let zaps: Vec<Value> = if let Some(zaps) = export.get("zaps").and_then(Value::as_array) {
        zaps.clone()
    } else if export.get("steps").is_some() {
        vec![export.clone()]
    } else if export.is_array() {
        vec![json!({"title": "zap", "steps": export})]
    } else {
        bail!("not a Zapier export: expected `zaps` or `steps`");
    };
    let zap = match &options.zap {
        Some(selector) => zaps
            .iter()
            .find(|z| {
                z.get("title").and_then(Value::as_str) == Some(selector.as_str())
                    || zap_step_id(z).as_deref() == Some(selector.as_str())
            })
            .with_context(|| format!("no zap with id or title '{selector}' in the export"))?,
        None => zaps.first().context("the export contains no zaps")?,
    };
    let workflow = zap
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("zap")
        .to_string();
    let steps = zap
        .get("steps")
        .and_then(Value::as_array)
        .context("zap has no `steps` array")?;
    let name = options
        .name
        .clone()
        .unwrap_or_else(|| slug(&workflow).replace('_', "-"));
    let mut b = Builder::new("zapier", &workflow, &name);
    if zaps.len() > 1 && options.zap.is_none() {
        b.report.warnings.push(format!(
            "the export holds {} zaps; converted the first ('{workflow}'). Use --zap <id|title> for others",
            zaps.len()
        ));
    }
    let mut conv = Zapier {
        refs: HashMap::new(),
    };
    let (trigger, rest) = match steps.first() {
        Some(first)
            if first.get("type_of").and_then(Value::as_str) == Some("read")
                || zap_app(first).contains("Schedule")
                || zap_app(first).contains("WebHook") =>
        {
            (Some(first), &steps[1..])
        }
        _ => (None, &steps[..]),
    };
    if let Some(trigger) = trigger {
        if let Some(id) = zap_step_id(trigger) {
            conv.refs.insert(id, "data".into());
        }
        conv.describe_trigger(&mut b, trigger, options);
    } else {
        b.report
            .warnings
            .push("the zap has no trigger step; start instances via the API".into());
    }
    let blocks = conv.steps(&mut b, rest);
    finish(b, options, blocks)
}

impl Zapier {
    fn steps(&mut self, b: &mut Builder, steps: &[Value]) -> Vec<Value> {
        let mut blocks = Vec::new();
        for (idx, step) in steps.iter().enumerate() {
            let app = zap_app(step);
            let title = zap_title(step);
            // Filter: the rest of the zap only runs when the filter passes.
            if app.starts_with("Filter")
                || step.get("type_of").and_then(Value::as_str) == Some("filter")
            {
                let condition = self.criteria(b, &title, step.pointer("/params/filter_criteria"));
                let id = b.unique_id(&title);
                b.mapped(&title, &app, &id, "router (continue only if)");
                let inner = self.steps(b, &steps[idx + 1..]);
                let inner = non_empty(inner, b, &id);
                blocks.push(json!({
                    "type": "router",
                    "id": id,
                    "routes": [{"condition": condition, "blocks": inner}],
                }));
                return blocks;
            }
            if app.starts_with("Branching") || step.get("paths").is_some() {
                let id = b.unique_id(&title);
                let mut routes = Vec::new();
                for (p, path) in step
                    .get("paths")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .enumerate()
                {
                    let path_title = path.get("title").and_then(Value::as_str).unwrap_or("path");
                    let condition = self.criteria(b, path_title, path.get("conditions"));
                    let inner_steps: Vec<Value> = path
                        .get("steps")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    let inner = self.steps(b, &inner_steps);
                    let inner = non_empty(inner, b, &format!("{id}_path_{p}"));
                    routes.push(json!({"condition": condition, "blocks": inner}));
                }
                if routes.is_empty() {
                    blocks.push(b.todo_stub(
                        &title,
                        &app,
                        step.get("params").unwrap_or(&Value::Null),
                        "paths step without paths",
                    ));
                    continue;
                }
                b.mapped(&title, &app, &id, "router (paths)");
                blocks.push(json!({"type": "router", "id": id, "routes": routes}));
                continue;
            }
            let block = self.step(b, step, &app, &title);
            if let (Some(zap_id), Some(block_id)) =
                (zap_step_id(step), block.get("id").and_then(Value::as_str))
            {
                self.refs.insert(zap_id, block_id.to_string());
            }
            blocks.push(block);
        }
        blocks
    }

    fn step(&self, b: &mut Builder, step: &Value, app: &str, title: &str) -> Value {
        let params = step.get("params").cloned().unwrap_or(json!({}));
        let action = step.get("action").and_then(Value::as_str).unwrap_or("");
        let translated = self.translate(b, &params);
        if app.starts_with("WebHook") || app.starts_with("Webhook") {
            let id = b.unique_id(title);
            b.mapped(title, app, &id, "http_request");
            let method = match action {
                "get" => "GET",
                "put" => "PUT",
                "custom_request" => params
                    .get("method")
                    .and_then(Value::as_str)
                    .unwrap_or("POST"),
                _ => "POST",
            }
            .to_ascii_uppercase();
            let mut out = json!({
                "url": translated.get("url").cloned().unwrap_or(Value::Null),
                "method": method,
            });
            if let Some(headers) = translated.get("headers").filter(|h| h.is_object()) {
                out["headers"] = headers.clone();
            }
            if let Some(data) = translated.get("data").filter(|d| !d.is_null()) {
                out["body"] = json!(match data {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                });
                if params.get("payload_type").and_then(Value::as_str) == Some("form") {
                    b.report.warnings.push(format!(
                        "block '{id}': form payloads were converted to a JSON body; adjust headers if the endpoint needs form encoding"
                    ));
                }
            }
            if let Some(query) = translated.get("query").and_then(Value::as_object) {
                let qs: Vec<String> = query
                    .iter()
                    .map(|(k, v)| {
                        format!(
                            "{k}={}",
                            v.as_str().map_or_else(|| v.to_string(), str::to_string)
                        )
                    })
                    .collect();
                if let Some(url) = out["url"].as_str()
                    && !qs.is_empty()
                {
                    let sep = if url.contains('?') { '&' } else { '?' };
                    out["url"] = json!(format!("{url}{sep}{}", qs.join("&")));
                }
            }
            return json!({"type": "step", "id": id, "handler": "http_request", "params": out});
        }
        if app.starts_with("Delay") {
            let id = b.unique_id(title);
            let amount = params.get("time_value").and_then(number);
            let unit = params
                .get("time_unit")
                .and_then(Value::as_str)
                .unwrap_or("minutes");
            if action == "delay_for"
                && let Some(ms) = amount.and_then(|a| unit_millis(a, unit))
            {
                b.mapped(title, app, &id, "noop + delay");
                return json!({"type": "step", "id": id, "handler": "noop", "params": {}, "delay": {"duration": ms}});
            }
            b.used_ids.remove(&id);
            return b.todo_stub(
                title,
                app,
                &params,
                "delay-until / queue delays have no direct mapping; use a delay or send_window",
            );
        }
        if app.starts_with("Formatter") {
            let id = b.unique_id(title);
            b.mapped(
                title,
                app,
                &id,
                "transform (values passed through; formatting TODO)",
            );
            b.report.warnings.push(format!(
                "block '{id}': Formatter transform '{}' is copied as data; implement the formatting in a template or worker",
                params.get("transform").and_then(Value::as_str).unwrap_or(action)
            ));
            let mut out = translated.clone();
            if let Some(obj) = out.as_object_mut()
                && let Some(input) = obj.get("input").cloned()
            {
                obj.insert("output".into(), input);
            }
            return json!({"type": "step", "id": id, "handler": "transform", "params": out});
        }
        if app.starts_with("Code") {
            let language = if action.contains("python") {
                "python"
            } else {
                "javascript"
            };
            let handler = format!("zapier_code_{}", slug(title));
            return b.worker_stub(
                title,
                app,
                &handler,
                json!({
                    "language": language,
                    "code": params.get("code").cloned().unwrap_or(Value::Null),
                    "input_data": translated.get("input_data").cloned().unwrap_or(json!({})),
                }),
                &format!("port this {language} code to a worker handler named '{handler}' (original in params.code)"),
            );
        }
        if app.starts_with("Slack") {
            return b.worker_stub(
                title,
                app,
                "slack_post_message",
                json!({
                    "channel": translated.get("channel").cloned().unwrap_or(Value::Null),
                    "text": translated.get("text").cloned().unwrap_or(Value::Null),
                }),
                "no built-in Slack handler; implement 'slack_post_message' in a worker (or use an Activepieces piece)",
            );
        }
        if app.starts_with("Email")
            || app.starts_with("Gmail")
            || app.contains("Outlook")
            || app.starts_with("SMTP")
        {
            return b.worker_stub(
                title,
                app,
                "send_email",
                json!({
                    "to": translated.get("to").cloned().unwrap_or(Value::Null),
                    "subject": translated.get("subject").cloned().unwrap_or(Value::Null),
                    "body": translated.get("body").cloned().unwrap_or(Value::Null),
                }),
                "no built-in email handler; implement 'send_email' in a worker (or use an Activepieces piece)",
            );
        }
        b.todo_stub(
            title,
            app,
            &translated,
            "no Orch8 mapping for this Zapier app yet",
        )
    }

    /// `{{123__a__b}}` → `{{outputs.<block>.a.b}}` (or `{{data.a.b}}`).
    fn translate(&self, b: &mut Builder, value: &Value) -> Value {
        match value {
            Value::String(s) => json!(self.translate_text(b, s)),
            Value::Array(items) => {
                Value::Array(items.iter().map(|v| self.translate(b, v)).collect())
            }
            Value::Object(map) => Value::Object(
                map.iter()
                    .map(|(k, v)| (k.clone(), self.translate(b, v)))
                    .collect(),
            ),
            other => other.clone(),
        }
    }

    fn reference(&self, token: &str) -> Option<String> {
        let (step, field) = token.trim().split_once("__")?;
        let target = self.refs.get(step)?;
        let path = field.replace("__", ".");
        if !path
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
        {
            return None;
        }
        Some(if target == "data" {
            format!("data.{path}")
        } else {
            format!("outputs.{target}.{path}")
        })
    }

    fn translate_text(&self, b: &mut Builder, text: &str) -> String {
        let mut out = String::new();
        let mut rest = text;
        while let Some(start) = rest.find("{{") {
            out.push_str(&rest[..start]);
            let after = &rest[start + 2..];
            let Some(end) = after.find("}}") else {
                out.push_str(&rest[start..]);
                return out;
            };
            let token = &after[..end];
            match self.reference(token) {
                Some(path) => {
                    out.push_str("{{");
                    out.push_str(&path);
                    out.push_str("}}");
                }
                None => {
                    b.report.warnings.push(format!(
                        "Zapier field `{{{{{token}}}}}` could not be mapped and was left as text"
                    ));
                    out.push_str(&rest[start..start + 2 + end + 2]);
                }
            }
            rest = &after[end + 2..];
        }
        out.push_str(rest);
        out
    }

    /// Zapier filter criteria: outer array = OR groups, inner = AND.
    fn criteria(&self, b: &mut Builder, title: &str, criteria: Option<&Value>) -> String {
        let groups: Vec<Value> = match criteria.and_then(Value::as_array) {
            Some(groups) if groups.iter().all(Value::is_array) => groups.clone(),
            Some(flat) => vec![Value::Array(flat.clone())],
            None => {
                b.report.warnings.push(format!(
                    "'{title}': no filter criteria; condition set to true"
                ));
                return "true".into();
            }
        };
        let mut ors = Vec::new();
        for group in &groups {
            let mut ands = Vec::new();
            for c in group.as_array().into_iter().flatten() {
                let key = c.get("key").and_then(Value::as_str).unwrap_or_default();
                let left = key
                    .trim()
                    .strip_prefix("{{")
                    .and_then(|k| k.strip_suffix("}}"))
                    .and_then(|k| self.reference(k))
                    .unwrap_or_else(|| {
                        b.report
                            .warnings
                            .push(format!("'{title}': field `{key}` could not be mapped"));
                        format!("\"TODO: {}\"", key.replace('"', "'"))
                    });
                let value = c.get("value").cloned().unwrap_or(Value::Null);
                let m = c.get("match").and_then(Value::as_str).unwrap_or("exact");
                let op = match m {
                    "exact" | "iexact" => "eq",
                    "does_not_exact" | "does_not_iexact" => "ne",
                    "contains" | "icontains" => "contains",
                    "does_not_contain" | "does_not_icontain" => "not_contains",
                    "startswith" | "istartswith" => "starts_with",
                    "endswith" | "iendswith" => "ends_with",
                    "float_greater_than" | "int_greater_than" | "date_after" => "gt",
                    "float_less_than" | "int_less_than" | "date_before" => "lt",
                    "float_exact" | "int_exact" => "eq",
                    "is_true" | "boolean_true" => "true",
                    "is_false" | "boolean_false" => "false",
                    "exists" => "exists",
                    "does_not_exist" => "not_exists",
                    _ => "unsupported",
                };
                let right = if matches!(
                    m,
                    "float_greater_than"
                        | "float_less_than"
                        | "int_greater_than"
                        | "int_less_than"
                        | "float_exact"
                        | "int_exact"
                ) {
                    number(&value).map_or_else(|| literal(&value), |n| format!("{n}"))
                } else {
                    serde_json::to_string(
                        &value
                            .as_str()
                            .map_or_else(|| value.to_string(), str::to_string),
                    )
                    .unwrap_or_default()
                };
                match comparison(&left, op, &right) {
                    Some(expr) => ands.push(expr),
                    None => b.report.warnings.push(format!(
                        "'{title}': match type '{m}' is not supported; condition dropped"
                    )),
                }
            }
            ors.push(join_conditions(&ands, " && "));
        }
        join_conditions(&ors, " || ")
    }

    fn describe_trigger(&self, b: &mut Builder, step: &Value, options: &ConvertOptions) {
        let app = zap_app(step);
        let title = zap_title(step);
        let params = step.get("params").cloned().unwrap_or(json!({}));
        let sequence_name = b.report.sequence_name.clone();
        if app.starts_with("WebHook") || app.starts_with("Webhook") {
            let slug_value = sequence_name.clone();
            b.report.triggers.push(TriggerSuggestion {
                node: title,
                kind: "webhook".into(),
                body: json!({
                    "slug": slug_value,
                    "sequence_name": sequence_name,
                    "tenant_id": options.tenant_id,
                    "namespace": options.namespace,
                    "trigger_type": "webhook",
                    "config": {},
                }),
                next_step: format!(
                    "POST the body to /triggers and point the sender at /webhooks/{slug_value} \
                     (public; set `secret` for HMAC) or /triggers/{slug_value}/fire (API key); \
                     the JSON body becomes context.data"
                ),
            });
            return;
        }
        if app.starts_with("Schedule") {
            let action = step.get("action").and_then(Value::as_str).unwrap_or("");
            let hour = params.get("time_of_day").and_then(number).unwrap_or(9.0);
            let dow = params.get("day_of_week").and_then(number).unwrap_or(1.0);
            let dom = params.get("day_of_month").and_then(number).unwrap_or(1.0);
            let weekdays_only =
                params.get("trigger_on_weekends").and_then(Value::as_str) == Some("no");
            let expr = match action {
                "every_hour" => Some(if weekdays_only {
                    "0 * * * 1-5".to_string()
                } else {
                    "0 * * * *".to_string()
                }),
                "every_day" => Some(format!(
                    "0 {hour} * * {}",
                    if weekdays_only { "1-5" } else { "*" }
                )),
                "every_week" => Some(format!("0 {hour} * * {dow}")),
                "every_month" => Some(format!("0 {hour} {dom} * *")),
                _ => None,
            };
            match expr {
                Some(expr) => b.report.triggers.push(TriggerSuggestion {
                    node: title,
                    kind: "cron".into(),
                    body: json!({
                        "tenant_id": options.tenant_id,
                        "namespace": options.namespace,
                        "sequence_id": "<id printed by `orch8 sequence create`>",
                        "cron_expr": expr,
                        "timezone": params.get("timezone").and_then(Value::as_str).unwrap_or("UTC"),
                    }),
                    next_step: "POST the body to /cron after creating the sequence (set sequence_id)".into(),
                }),
                None => b.report.warnings.push(format!(
                    "schedule trigger '{title}' ({action}) could not be converted; create the cron schedule manually"
                )),
            }
            return;
        }
        b.report.todos.push(TodoNode {
            node: title,
            node_type: app,
            block_id: String::new(),
            handler: String::new(),
            reason: "app trigger (polling another service) has no direct Orch8 equivalent; use an Activepieces poll trigger or a webhook".into(),
        });
    }
}

#[cfg(test)]
#[path = "import_tests.rs"]
mod tests;
