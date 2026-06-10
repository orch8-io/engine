//! `orch8 dev` — local dev loop with an ephemeral in-process engine.
//!
//! One command for instant evaluation: start an in-memory engine (via the
//! `orch8` facade crate, dogfooding the public embedding API), load a
//! sequence JSON file, create an instance immediately, drive fast ticks, and
//! print step-by-step progress as blocks complete. The sequence file is
//! hot-reloaded on save (mtime poll, no extra dependencies): a valid change
//! is published as a new immutable version and a fresh instance starts.
//!
//! With `--skip-timers` the engine runs on a virtual [`ManualClock`]: when a
//! tick executes nothing but the instance is deferred to a future
//! `next_fire_at` (delays, send windows, retry backoff), the clock jumps to
//! that instant — a workflow with a 3-day delay tests in seconds.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use owo_colors::OwoColorize;
use serde_json::Value;

use orch8::{
    BlockOutput, Clock, CreateInstanceOptions, Engine, ExecutionContext, InstanceId, InstanceState,
    ManualClock, SequenceDefinition, SharedClock, Storage,
};

/// Tenant every dev-session sequence and instance runs under.
const DEV_TENANT: &str = "default";
/// How often the dev loop re-stats the sequence file for hot reload.
const WATCH_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// Max characters of a block-output preview printed per completed step.
const PREVIEW_MAX: usize = 96;
/// After this long without progress on a live instance, print a stall hint.
const STALL_HINT_AFTER: Duration = Duration::from_secs(5);

/// `orch8 dev [path]` — run a sequence locally with hot reload.
#[derive(Debug, clap::Args)]
pub struct DevCmd {
    /// Directory containing `sequence.json`, or a sequence file directly.
    #[arg(default_value = ".")]
    pub path: String,

    /// Explicit sequence file (overrides the `[path]` lookup).
    #[arg(long)]
    pub sequence: Option<String>,

    /// Initial instance context as JSON (becomes `context.data`).
    #[arg(long)]
    pub context: Option<String>,

    /// Run on a virtual clock that fast-forwards over delays, send windows
    /// and retry backoffs instead of waiting in real time.
    #[arg(long)]
    pub skip_timers: bool,

    /// Register a stub handler returning fixed JSON (repeatable),
    /// e.g. `--mock send_email='{"sent":true}'`.
    #[arg(long, value_name = "HANDLER=JSON")]
    pub mock: Vec<String>,

    /// Create the instance in dry-run mode: side-effecting built-in handlers
    /// return stub outputs and human gates auto-approve.
    #[arg(long)]
    pub dry_run: bool,

    /// Run one instance to a terminal state, then exit — code 0 if it
    /// completed, 1 if it failed (for CI smoke tests). Disables hot reload.
    #[arg(long)]
    pub once: bool,

    /// Dev-loop tick interval in milliseconds.
    #[arg(long, default_value_t = 25)]
    pub tick_ms: u64,
}

// ---------------------------------------------------------------------------
// Pure, testable pieces of the dev loop.
// ---------------------------------------------------------------------------

/// Resolve which sequence file to run: an explicit `--sequence` flag wins,
/// then `path` itself if it is a file, then `path/sequence.json`.
pub fn resolve_sequence_path(path: &Path, sequence: Option<&Path>) -> Result<PathBuf> {
    if let Some(file) = sequence {
        if file.is_file() {
            return Ok(file.to_path_buf());
        }
        bail!("sequence file not found: {}", file.display());
    }
    if path.is_file() {
        return Ok(path.to_path_buf());
    }
    let candidate = path.join("sequence.json");
    if candidate.is_file() {
        return Ok(candidate);
    }
    bail!(
        "no sequence.json found in {} — pass a file, use --sequence <file>, \
         or scaffold one with `orch8 init`",
        path.display()
    )
}

/// Parse a `--mock HANDLER=JSON` spec into a handler name and its fixed
/// JSON output.
pub fn parse_mock(spec: &str) -> Result<(String, Value)> {
    let (name, json) = spec.split_once('=').ok_or_else(|| {
        anyhow!("--mock expects HANDLER=JSON, e.g. --mock send_email='{{\"sent\":true}}'")
    })?;
    let name = name.trim();
    if name.is_empty() {
        bail!("--mock handler name is empty (expected HANDLER=JSON)");
    }
    let value: Value = serde_json::from_str(json)
        .with_context(|| format!("--mock {name}: output is not valid JSON"))?;
    Ok((name.to_string(), value))
}

/// A sequence definition loaded from disk, plus the `block id -> handler`
/// map used to annotate progress lines.
#[derive(Debug)]
pub struct LoadedSequence {
    /// The parsed, validated definition ready for `upsert_sequence`.
    pub definition: SequenceDefinition,
    /// Step block id -> handler name, collected recursively.
    pub handlers_by_block: HashMap<String, String>,
}

/// Read and parse the sequence file, forcing the dev-session identity fields
/// (`id`, `tenant_id`, `version`, `created_at`) so the same file can be
/// republished as a new immutable version on every hot reload.
pub fn load_sequence(path: &Path, version: i32) -> Result<LoadedSequence> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    parse_sequence(&raw, version)
}

/// Parse raw sequence JSON (see [`load_sequence`]). Split out so the
/// invalid-JSON / invalid-definition error paths are unit-testable.
pub fn parse_sequence(raw: &str, version: i32) -> Result<LoadedSequence> {
    let mut value: Value = serde_json::from_str(raw).context("invalid JSON")?;
    let obj = value
        .as_object_mut()
        .ok_or_else(|| anyhow!("sequence file must be a JSON object"))?;
    // Server-assigned / session-managed fields: always overwrite so authoring
    // payloads (no id / created_at) and full definitions both load, and so
    // each hot reload publishes a fresh immutable version.
    obj.insert("id".into(), serde_json::json!(uuid::Uuid::now_v7()));
    obj.insert("tenant_id".into(), serde_json::json!(DEV_TENANT));
    obj.entry("namespace")
        .or_insert(serde_json::json!("default"));
    obj.insert("version".into(), serde_json::json!(version));
    obj.insert("created_at".into(), serde_json::json!(Utc::now()));

    let handlers_by_block = block_handlers(&value);
    let definition: SequenceDefinition =
        serde_json::from_value(value).context("invalid sequence definition")?;
    definition
        .validate()
        .map_err(|e| anyhow!("invalid sequence: {e}"))?;
    Ok(LoadedSequence {
        definition,
        handlers_by_block,
    })
}

/// Collect `block id -> handler` for every step in the raw sequence JSON,
/// recursing through composites (parallel branches, loop bodies, routers, …)
/// by walking the JSON tree instead of matching every DSL variant.
pub fn block_handlers(value: &Value) -> HashMap<String, String> {
    fn walk(value: &Value, out: &mut HashMap<String, String>) {
        match value {
            Value::Object(map) => {
                if let (Some(Value::String(id)), Some(Value::String(handler))) =
                    (map.get("id"), map.get("handler"))
                {
                    out.insert(id.clone(), handler.clone());
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
    let mut out = HashMap::new();
    walk(value, &mut out);
    out
}

/// Handlers used by the sequence that are neither built-ins nor `--mock`
/// stubs. Steps using them are dispatched to the external worker queue and
/// stall the dev loop, so they get a load-time warning.
pub fn unknown_handlers(loaded: &LoadedSequence, mocks: &HashSet<String>) -> Vec<String> {
    let mut unknown: Vec<String> = loaded
        .handlers_by_block
        .values()
        .filter(|h| {
            !orch8_types::sequence::BUILTIN_HANDLER_NAMES.contains(&h.as_str())
                && !mocks.contains(h.as_str())
        })
        .cloned()
        .collect();
    unknown.sort();
    unknown.dedup();
    unknown
}

/// Detects sequence-file changes with a cheap (mtime, size) signature poll —
/// no file-watcher dependency needed at 500 ms granularity.
pub struct FileWatch {
    path: PathBuf,
    signature: Option<(SystemTime, u64)>,
}

impl FileWatch {
    /// Snapshot the file's current signature as the baseline.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let signature = Self::stat(&path);
        Self { path, signature }
    }

    fn stat(path: &Path) -> Option<(SystemTime, u64)> {
        let meta = std::fs::metadata(path).ok()?;
        let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        Some((mtime, meta.len()))
    }

    /// Re-stat the file; returns `true` when its (mtime, size) signature
    /// changed since the last observation. A missing file is treated as
    /// unchanged so transient editor save dances (unlink + rename) don't
    /// trigger spurious reloads.
    pub fn poll(&mut self) -> bool {
        match Self::stat(&self.path) {
            Some(sig) if Some(sig) != self.signature => {
                self.signature = Some(sig);
                true
            }
            _ => false,
        }
    }
}

/// Where the virtual clock should jump next: the instance's future
/// `next_fire_at`, but only while the instance is still live. Returns `None`
/// when there is nothing to skip (terminal instance, no deferral, or the
/// fire time has already passed).
pub fn next_advance_target(
    state: InstanceState,
    next_fire_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    if state.is_terminal() {
        return None;
    }
    match next_fire_at {
        Some(t) if t > now => Some(t),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Dev session: the engine-driving core, factored so tests can drive it
// without the interactive loop (file watching, ctrl-c, sleeps).
// ---------------------------------------------------------------------------

/// Outcome of one [`DevSession::step`] pass, telling the caller how to pace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepOutcome {
    /// Work happened this tick; tick again immediately.
    Progress,
    /// Nothing ran, but the virtual clock jumped to the next deferral —
    /// tick again immediately.
    Advanced,
    /// Nothing to do right now; sleep one tick interval.
    Idle,
    /// No live instance (waiting for a file change); sleep.
    NoInstance,
    /// The current instance reached a terminal state.
    Terminal(InstanceState),
}

/// Per-instance bookkeeping for progress printing.
struct RunState {
    id: InstanceId,
    handlers_by_block: HashMap<String, String>,
    /// `block_id#attempt` keys already printed.
    seen: HashSet<String>,
}

/// Engine-driving core of `orch8 dev`: owns the embedded engine, the
/// optional virtual clock, and the currently-running instance.
pub struct DevSession {
    engine: Engine,
    manual_clock: Option<Arc<ManualClock>>,
    mock_names: HashSet<String>,
    run: Option<RunState>,
    /// Total steps executed across all instances this session.
    pub steps_executed: u64,
    /// Instances started this session (initial + one per hot reload).
    pub instances_run: u64,
    last_progress: Instant,
    stall_hinted: bool,
    unknown: Vec<String>,
}

impl DevSession {
    /// Wrap an already-built engine (see [`build_engine`]).
    pub fn new(
        engine: Engine,
        manual_clock: Option<Arc<ManualClock>>,
        mock_names: HashSet<String>,
    ) -> Self {
        Self {
            engine,
            manual_clock,
            mock_names,
            run: None,
            steps_executed: 0,
            instances_run: 0,
            last_progress: Instant::now(),
            stall_hinted: false,
            unknown: Vec::new(),
        }
    }

    /// Publish `loaded` as a new sequence version and start a fresh instance
    /// of it. Any previous instance keeps whatever state it had.
    pub async fn start_instance(
        &mut self,
        loaded: &LoadedSequence,
        opts: CreateInstanceOptions,
    ) -> Result<InstanceId> {
        self.unknown = unknown_handlers(loaded, &self.mock_names);
        for handler in &self.unknown {
            eprintln!(
                "{} handler {} is not registered — steps using it wait for an \
                 external worker and will stall; add --mock {handler}='{{...}}' \
                 or run with --dry-run",
                "warning:".yellow().bold(),
                handler.bold(),
            );
        }
        let seq_id = self
            .engine
            .upsert_sequence(loaded.definition.clone())
            .await?;
        let instance_id = self.engine.create_instance(seq_id, opts).await?;
        self.instances_run += 1;
        self.run = Some(RunState {
            id: instance_id,
            handlers_by_block: loaded.handlers_by_block.clone(),
            seen: HashSet::new(),
        });
        self.last_progress = Instant::now();
        self.stall_hinted = false;
        println!(
            "{} {} v{} → instance {}",
            stamp(),
            loaded.definition.name.bold(),
            loaded.definition.version,
            instance_id.to_string().dimmed(),
        );
        Ok(instance_id)
    }

    /// One pass of the dev loop: tick the engine once, print any newly
    /// completed blocks, then decide whether to advance the virtual clock,
    /// idle, or report the instance terminal. Never sleeps — pacing is the
    /// caller's job, which keeps this fully testable.
    pub async fn step(&mut self) -> Result<StepOutcome> {
        let Some(run) = self.run.as_mut() else {
            return Ok(StepOutcome::NoInstance);
        };
        let tick = self.engine.tick_once().await?;
        self.steps_executed += u64::from(tick.steps_executed);

        let outputs = self.engine.block_outputs(run.id).await?;
        let mut printed = false;
        for output in &outputs {
            let key = format!("{}#{}", output.block_id, output.attempt);
            if run.seen.insert(key) {
                let handler = run
                    .handlers_by_block
                    .get(output.block_id.as_str())
                    .map_or("-", String::as_str);
                print_block_line(output, handler);
                printed = true;
            }
        }
        if printed {
            self.last_progress = Instant::now();
            self.stall_hinted = false;
        }

        let instance = self.engine.get_instance(run.id).await?;
        if instance.state.is_terminal() {
            print_terminal_banner(instance.state, &outputs);
            self.run = None;
            return Ok(StepOutcome::Terminal(instance.state));
        }

        if tick.steps_executed == 0 && tick.instances_advanced == 0 {
            if let Some(clock) = &self.manual_clock {
                if let Some(target) =
                    next_advance_target(instance.state, instance.next_fire_at, clock.now())
                {
                    // One second of slack past the deferral, mirroring the
                    // engine's virtual-time tests.
                    let jump = target + chrono::Duration::seconds(1);
                    clock.set(jump);
                    println!(
                        "{} {} clock advanced to {}",
                        stamp(),
                        "⏩".cyan(),
                        jump.format("%Y-%m-%d %H:%M:%S UTC").to_string().cyan(),
                    );
                    return Ok(StepOutcome::Advanced);
                }
            }
            if !self.stall_hinted && self.last_progress.elapsed() >= STALL_HINT_AFTER {
                self.stall_hinted = true;
                self.print_stall_hint();
            }
            return Ok(StepOutcome::Idle);
        }
        Ok(StepOutcome::Progress)
    }

    fn print_stall_hint(&self) {
        if self.unknown.is_empty() {
            eprintln!(
                "{} no progress — the instance is waiting (signal/input/timer); \
                 try --skip-timers for delays, or `orch8 signal` against a real server",
                "hint:".yellow().bold(),
            );
        } else {
            eprintln!(
                "{} no progress — unregistered handler(s) {} are waiting for an \
                 external worker; add --mock <name>='{{...}}' or run with --dry-run",
                "hint:".yellow().bold(),
                self.unknown.join(", ").bold(),
            );
        }
    }
}

/// Build the ephemeral in-process engine: in-memory `SQLite`, the full
/// built-in handler set, any `--mock` stubs layered on top, and an optional
/// injected clock for `--skip-timers`.
pub async fn build_engine(mocks: &[(String, Value)], clock: Option<SharedClock>) -> Result<Engine> {
    let mut builder = Engine::builder().storage(Storage::sqlite_in_memory());
    if let Some(clock) = clock {
        builder = builder.clock(clock);
    }
    for (name, value) in mocks {
        let value = value.clone();
        builder = builder.handler(name, move |_ctx: orch8::StepContext| {
            let output = value.clone();
            async move { Ok(output) }
        });
    }
    Ok(builder.build().await?)
}

// ---------------------------------------------------------------------------
// Output formatting.
// ---------------------------------------------------------------------------

/// `HH:MM:SS.mmm` wall-clock stamp prefixing every progress line.
fn stamp() -> String {
    Utc::now()
        .format("%H:%M:%S%.3f")
        .to_string()
        .dimmed()
        .to_string()
}

/// One-line, truncated JSON preview of a block output.
fn preview(value: &Value) -> String {
    let compact = value.to_string();
    if compact.chars().count() > PREVIEW_MAX {
        let truncated: String = compact.chars().take(PREVIEW_MAX).collect();
        format!("{truncated}…")
    } else {
        compact
    }
}

fn print_block_line(output: &BlockOutput, handler: &str) {
    let failed = output.output.get("error").is_some();
    let mark = if failed {
        "✗".red().to_string()
    } else {
        "✓".green().to_string()
    };
    println!(
        "{} {} {:<24} {:<16} {}",
        stamp(),
        mark,
        output.block_id.as_str().bold(),
        handler.dimmed(),
        preview(&output.output).dimmed(),
    );
}

/// Last `"error"` field recorded in the instance's outputs, if any.
fn extract_error(outputs: &[BlockOutput]) -> Option<String> {
    outputs.iter().rev().find_map(|o| {
        o.output
            .get("error")
            .map(|e| e.as_str().map_or_else(|| e.to_string(), String::from))
    })
}

fn print_terminal_banner(state: InstanceState, outputs: &[BlockOutput]) {
    match state {
        InstanceState::Completed => {
            println!("{} {}", stamp(), "── instance completed ──".green().bold());
        }
        InstanceState::Failed => {
            let error = extract_error(outputs).unwrap_or_else(|| "see outputs above".to_string());
            println!(
                "{} {} {}",
                stamp(),
                "── instance failed ──".red().bold(),
                error.red(),
            );
        }
        other => {
            println!("{} ── instance ended: {other:?} ──", stamp());
        }
    }
}

// ---------------------------------------------------------------------------
// Interactive loop (thin: file watching, pacing, ctrl-c).
// ---------------------------------------------------------------------------

/// Instance-creation options that survive hot reloads.
struct InstanceOpts {
    context_data: Option<Value>,
    dry_run: bool,
}

impl InstanceOpts {
    fn to_options(&self) -> CreateInstanceOptions {
        let mut context = ExecutionContext::default();
        if let Some(data) = &self.context_data {
            context.data = data.clone();
        }
        context.runtime.dry_run = self.dry_run;
        context.runtime.dry_run_auto_approve = self.dry_run;
        CreateInstanceOptions {
            context,
            ..Default::default()
        }
    }
}

/// Entry point for `orch8 dev`.
pub async fn run(cmd: DevCmd) -> Result<()> {
    let started = Instant::now();
    let seq_path =
        resolve_sequence_path(Path::new(&cmd.path), cmd.sequence.as_deref().map(Path::new))?;

    let mocks: Vec<(String, Value)> = cmd
        .mock
        .iter()
        .map(|spec| parse_mock(spec))
        .collect::<Result<_>>()?;
    let mock_names: HashSet<String> = mocks.iter().map(|(n, _)| n.clone()).collect();

    let opts = InstanceOpts {
        context_data: cmd
            .context
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .context("--context must be valid JSON")?,
        dry_run: cmd.dry_run,
    };

    // --skip-timers: keep a ManualClock handle for advancing, hand the engine
    // a shared view of it. Starts at real "now" (forward-only discipline).
    let manual_clock = cmd
        .skip_timers
        .then(|| Arc::new(ManualClock::new(Utc::now())));
    let shared_clock = manual_clock
        .as_ref()
        .map(|c| SharedClock::from_arc(Arc::clone(c) as Arc<dyn Clock>));

    let engine = build_engine(&mocks, shared_clock).await?;
    let mut session = DevSession::new(engine.clone(), manual_clock, mock_names);

    println!(
        "{} dev session: {} (timers: {}, dry-run: {}{})",
        "orch8".bold(),
        seq_path.display().to_string().bold(),
        if cmd.skip_timers { "virtual" } else { "real" },
        if cmd.dry_run { "on" } else { "off" },
        if cmd.once {
            ", once"
        } else {
            ", watching for changes — ctrl-c to stop"
        },
    );

    let mut version = 1;
    let loaded = load_sequence(&seq_path, version)?;
    session.start_instance(&loaded, opts.to_options()).await?;

    let mut watch = FileWatch::new(&seq_path);
    let tick = Duration::from_millis(cmd.tick_ms.max(1));

    let result = tokio::select! {
        r = dev_loop(&mut session, &mut watch, &seq_path, &mut version, cmd.once, tick, &opts) => r,
        _ = tokio::signal::ctrl_c() => Ok(None),
    };

    engine.shutdown().await;
    println!(
        "{} {} instance(s), {} step(s) executed, {:.1}s elapsed",
        "summary:".bold(),
        session.instances_run,
        session.steps_executed,
        started.elapsed().as_secs_f64(),
    );

    match result? {
        Some(state) if state != InstanceState::Completed => {
            bail!("instance ended in state {state:?}")
        }
        _ => Ok(()),
    }
}

/// The pacing loop: poll the sequence file every 500 ms, drive
/// [`DevSession::step`], and sleep when the session reports idle. Returns the
/// terminal state in `--once` mode, or runs until cancelled (ctrl-c).
async fn dev_loop(
    session: &mut DevSession,
    watch: &mut FileWatch,
    seq_path: &Path,
    version: &mut i32,
    once: bool,
    tick: Duration,
    opts: &InstanceOpts,
) -> Result<Option<InstanceState>> {
    let mut last_watch_poll = Instant::now();
    loop {
        if !once && last_watch_poll.elapsed() >= WATCH_POLL_INTERVAL {
            last_watch_poll = Instant::now();
            if watch.poll() {
                match load_sequence(seq_path, *version + 1) {
                    Ok(loaded) => {
                        *version += 1;
                        println!(
                            "{} {} reloaded {} as v{}",
                            stamp(),
                            "↻".cyan(),
                            seq_path.display(),
                            version,
                        );
                        session.start_instance(&loaded, opts.to_options()).await?;
                    }
                    Err(e) => {
                        eprintln!(
                            "{} reload failed ({e:#}) — keeping v{} running",
                            "error:".red().bold(),
                            version,
                        );
                    }
                }
            }
        }

        match session.step().await? {
            StepOutcome::Progress | StepOutcome::Advanced => {}
            StepOutcome::Terminal(state) => {
                if once {
                    return Ok(Some(state));
                }
                println!(
                    "{} waiting for changes to {} …",
                    stamp(),
                    seq_path.display(),
                );
            }
            StepOutcome::Idle => tokio::time::sleep(tick).await,
            StepOutcome::NoInstance => tokio::time::sleep(WATCH_POLL_INTERVAL).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- parse_mock ---------------------------------------------------------

    #[test]
    fn parse_mock_accepts_handler_equals_json() {
        let (name, value) = parse_mock(r#"send_email={"sent":true}"#).unwrap();
        assert_eq!(name, "send_email");
        assert_eq!(value, serde_json::json!({"sent": true}));
    }

    #[test]
    fn parse_mock_accepts_scalar_json_and_equals_in_payload() {
        let (name, value) = parse_mock(r#"score="a=b""#).unwrap();
        assert_eq!(name, "score");
        assert_eq!(value, serde_json::json!("a=b"));
    }

    #[test]
    fn parse_mock_rejects_missing_equals() {
        let err = parse_mock("send_email").unwrap_err().to_string();
        assert!(err.contains("HANDLER=JSON"), "got: {err}");
    }

    #[test]
    fn parse_mock_rejects_invalid_json() {
        let err = format!("{:#}", parse_mock("send_email={nope").unwrap_err());
        assert!(err.contains("not valid JSON"), "got: {err}");
    }

    #[test]
    fn parse_mock_rejects_empty_name() {
        let err = parse_mock("={}").unwrap_err().to_string();
        assert!(err.contains("empty"), "got: {err}");
    }

    // -- resolve_sequence_path ----------------------------------------------

    #[test]
    fn resolve_finds_sequence_json_in_directory() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sequence.json");
        std::fs::write(&file, "{}").unwrap();
        let resolved = resolve_sequence_path(dir.path(), None).unwrap();
        assert_eq!(resolved, file);
    }

    #[test]
    fn resolve_accepts_direct_file_path() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("flow.json");
        std::fs::write(&file, "{}").unwrap();
        let resolved = resolve_sequence_path(&file, None).unwrap();
        assert_eq!(resolved, file);
    }

    #[test]
    fn resolve_prefers_sequence_flag() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("sequence.json"), "{}").unwrap();
        let flagged = dir.path().join("other.json");
        std::fs::write(&flagged, "{}").unwrap();
        let resolved = resolve_sequence_path(dir.path(), Some(&flagged)).unwrap();
        assert_eq!(resolved, flagged);
    }

    #[test]
    fn resolve_errors_when_nothing_found() {
        let dir = tempfile::tempdir().unwrap();
        let err = resolve_sequence_path(dir.path(), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no sequence.json"), "got: {err}");
    }

    #[test]
    fn resolve_errors_on_missing_sequence_flag_target() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope.json");
        let err = resolve_sequence_path(dir.path(), Some(&missing))
            .unwrap_err()
            .to_string();
        assert!(err.contains("not found"), "got: {err}");
    }

    // -- sequence loading ----------------------------------------------------

    const SIMPLE_SEQ: &str = r#"{
        "name": "dev-test",
        "blocks": [
            { "type": "step", "id": "one", "handler": "noop", "params": {} },
            { "type": "step", "id": "two", "handler": "custom_thing", "params": {} }
        ]
    }"#;

    #[test]
    fn parse_sequence_fills_session_fields() {
        let loaded = parse_sequence(SIMPLE_SEQ, 7).unwrap();
        assert_eq!(loaded.definition.name, "dev-test");
        assert_eq!(loaded.definition.version, 7);
        assert_eq!(loaded.definition.tenant_id.as_str(), DEV_TENANT);
        assert_eq!(loaded.handlers_by_block.len(), 2);
        assert_eq!(loaded.handlers_by_block["one"], "noop");
        assert_eq!(loaded.handlers_by_block["two"], "custom_thing");
    }

    #[test]
    fn parse_sequence_loads_builtin_default_template() {
        let template = crate::templates::find("default").unwrap();
        let loaded = parse_sequence(template.json, 1).unwrap();
        assert_eq!(loaded.definition.name, "hello-world");
        assert!(loaded.handlers_by_block.contains_key("greet"));
    }

    #[test]
    fn parse_sequence_rejects_invalid_json() {
        let err = format!("{:#}", parse_sequence("{ not json", 1).unwrap_err());
        assert!(err.contains("invalid JSON"), "got: {err}");
    }

    #[test]
    fn parse_sequence_rejects_non_object() {
        let err = parse_sequence("[1,2,3]", 1).unwrap_err().to_string();
        assert!(err.contains("JSON object"), "got: {err}");
    }

    #[test]
    fn parse_sequence_rejects_invalid_definition() {
        // Valid JSON object, but no `name`/`blocks` — must fail typed parsing.
        let err = format!("{:#}", parse_sequence(r#"{"foo": 1}"#, 1).unwrap_err());
        assert!(err.contains("invalid sequence"), "got: {err}");
    }

    #[test]
    fn load_sequence_reads_from_disk_and_invalid_json_errors() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sequence.json");
        std::fs::write(&file, SIMPLE_SEQ).unwrap();
        let loaded = load_sequence(&file, 1).unwrap();
        assert_eq!(loaded.definition.name, "dev-test");

        std::fs::write(&file, "{ broken").unwrap();
        let err = format!("{:#}", load_sequence(&file, 2).unwrap_err());
        assert!(err.contains("invalid JSON"), "got: {err}");
    }

    #[test]
    fn block_handlers_recurses_into_composites() {
        let value: Value = serde_json::from_str(
            r#"{
                "blocks": [
                    { "type": "parallel", "id": "p", "branches": [
                        [ { "type": "step", "id": "a", "handler": "noop" } ],
                        [ { "type": "step", "id": "b", "handler": "http_request" } ]
                    ]},
                    { "type": "loop", "id": "l", "body": [
                        { "type": "step", "id": "c", "handler": "transform" }
                    ]}
                ]
            }"#,
        )
        .unwrap();
        let map = block_handlers(&value);
        assert_eq!(map.get("a").map(String::as_str), Some("noop"));
        assert_eq!(map.get("b").map(String::as_str), Some("http_request"));
        assert_eq!(map.get("c").map(String::as_str), Some("transform"));
        // Composite blocks have no handler and must not appear.
        assert!(!map.contains_key("p"));
        assert!(!map.contains_key("l"));
    }

    #[test]
    fn unknown_handlers_excludes_builtins_and_mocks() {
        let loaded = parse_sequence(SIMPLE_SEQ, 1).unwrap();
        // `noop` is built-in; `custom_thing` is unknown without a mock.
        let unknown = unknown_handlers(&loaded, &HashSet::new());
        assert_eq!(unknown, vec!["custom_thing".to_string()]);
        // With a mock registered it is no longer unknown.
        let mocks: HashSet<String> = ["custom_thing".to_string()].into_iter().collect();
        assert!(unknown_handlers(&loaded, &mocks).is_empty());
    }

    // -- FileWatch (mtime-poll reload detection) -----------------------------

    #[test]
    fn file_watch_detects_content_change() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sequence.json");
        std::fs::write(&file, "first version").unwrap();

        let mut watch = FileWatch::new(&file);
        assert!(!watch.poll(), "baseline must not report a change");

        // Different length guarantees a signature change even on filesystems
        // with coarse mtime granularity.
        std::fs::write(&file, "second version, longer").unwrap();
        assert!(watch.poll(), "content change must be detected");
        assert!(!watch.poll(), "no further change after acknowledging");
    }

    #[test]
    fn file_watch_ignores_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sequence.json");
        std::fs::write(&file, "x").unwrap();
        let mut watch = FileWatch::new(&file);
        std::fs::remove_file(&file).unwrap();
        assert!(!watch.poll(), "deletion is not a reload");
        // Recreation with different content is a change.
        std::fs::write(&file, "recreated!").unwrap();
        assert!(watch.poll(), "recreation must be detected");
    }

    // -- next_advance_target --------------------------------------------------

    #[test]
    fn advance_target_is_future_fire_at_for_live_instance() {
        let now = Utc::now();
        let fire = now + chrono::Duration::days(3);
        assert_eq!(
            next_advance_target(InstanceState::Scheduled, Some(fire), now),
            Some(fire)
        );
        assert_eq!(
            next_advance_target(InstanceState::Waiting, Some(fire), now),
            Some(fire)
        );
    }

    #[test]
    fn advance_target_none_for_terminal_or_due_instances() {
        let now = Utc::now();
        let future = now + chrono::Duration::hours(1);
        let past = now - chrono::Duration::seconds(1);
        assert_eq!(
            next_advance_target(InstanceState::Completed, Some(future), now),
            None,
            "terminal instances never advance the clock"
        );
        assert_eq!(
            next_advance_target(InstanceState::Failed, Some(future), now),
            None
        );
        assert_eq!(
            next_advance_target(InstanceState::Scheduled, Some(past), now),
            None,
            "already-due instances need a tick, not a time jump"
        );
        assert_eq!(
            next_advance_target(InstanceState::Scheduled, None, now),
            None
        );
    }

    // -- e2e: dev session with virtual time -----------------------------------

    /// A sequence with a 3-day delay completes near-instantly under
    /// `--skip-timers`: the session advances the `ManualClock` to the deferral
    /// instead of waiting.
    #[tokio::test]
    async fn dev_session_skips_three_day_delay_with_virtual_clock() {
        const DELAYED_SEQ: &str = r#"{
            "name": "dev-delay-e2e",
            "blocks": [
                { "type": "step", "id": "wait_3d", "handler": "noop",
                  "delay": { "duration": 259200000 } },
                { "type": "step", "id": "after_delay", "handler": "mocked",
                  "params": {} }
            ]
        }"#;

        let started = Instant::now();
        let mocks = vec![(
            "mocked".to_string(),
            serde_json::json!({"from": "the mock"}),
        )];
        let mock_names: HashSet<String> = mocks.iter().map(|(n, _)| n.clone()).collect();

        let manual = Arc::new(ManualClock::new(Utc::now()));
        let shared = SharedClock::from_arc(Arc::clone(&manual) as Arc<dyn Clock>);
        let engine = build_engine(&mocks, Some(shared)).await.unwrap();
        let mut session = DevSession::new(engine.clone(), Some(Arc::clone(&manual)), mock_names);

        let loaded = parse_sequence(DELAYED_SEQ, 1).unwrap();
        let instance_id = session
            .start_instance(&loaded, CreateInstanceOptions::default())
            .await
            .unwrap();

        // Drive the core loop directly (no sleeps, no file watching): the
        // 3-day delay must resolve within a small bounded number of passes.
        let mut terminal = None;
        let mut advanced = false;
        for _ in 0..200 {
            match session.step().await.unwrap() {
                StepOutcome::Terminal(state) => {
                    terminal = Some(state);
                    break;
                }
                StepOutcome::Advanced => advanced = true,
                StepOutcome::Progress | StepOutcome::Idle => {}
                StepOutcome::NoInstance => panic!("instance vanished mid-run"),
            }
        }

        assert_eq!(
            terminal,
            Some(InstanceState::Completed),
            "delayed sequence must complete under virtual time"
        );
        assert!(advanced, "the session must have fast-forwarded the clock");
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "virtual time must not wait for the real 3-day delay"
        );
        assert!(
            session.steps_executed >= 2,
            "both steps must have executed, got {}",
            session.steps_executed
        );

        // Verify via the facade: both blocks have outputs, and the mock's
        // fixed JSON landed as the second step's output.
        let outputs = engine.block_outputs(instance_id).await.unwrap();
        let blocks: Vec<&str> = outputs.iter().map(|o| o.block_id.as_str()).collect();
        assert!(blocks.contains(&"wait_3d"), "outputs: {blocks:?}");
        assert!(blocks.contains(&"after_delay"), "outputs: {blocks:?}");
        let mocked = outputs
            .iter()
            .find(|o| o.block_id.as_str() == "after_delay")
            .unwrap();
        assert_eq!(mocked.output, serde_json::json!({"from": "the mock"}));

        engine.shutdown().await;
    }

    /// Without `--skip-timers` (system clock), the same delayed sequence
    /// stays deferred — virtual time is strictly opt-in.
    #[tokio::test]
    async fn dev_session_without_virtual_clock_keeps_delay_pending() {
        const DELAYED_SEQ: &str = r#"{
            "name": "dev-delay-real-time",
            "blocks": [
                { "type": "step", "id": "wait_1h", "handler": "noop",
                  "delay": { "duration": 3600000 } }
            ]
        }"#;

        let engine = build_engine(&[], None).await.unwrap();
        let mut session = DevSession::new(engine.clone(), None, HashSet::new());
        let loaded = parse_sequence(DELAYED_SEQ, 1).unwrap();
        let instance_id = session
            .start_instance(&loaded, CreateInstanceOptions::default())
            .await
            .unwrap();

        let mut terminal = None;
        for _ in 0..20 {
            if let StepOutcome::Terminal(state) = session.step().await.unwrap() {
                terminal = Some(state);
                break;
            }
        }
        assert_eq!(terminal, None, "1h delay must still be pending");
        let instance = engine.get_instance(instance_id).await.unwrap();
        assert!(!instance.state.is_terminal());

        engine.shutdown().await;
    }
}
