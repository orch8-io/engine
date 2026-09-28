//! WASM plugin handler.
//!
//! Steps with handler names prefixed `wasm://` dispatch to a WebAssembly module.
//! The WASM module must export:
//!
//! - `alloc(size: i32) -> i32` — allocate memory for input
//! - `dealloc(ptr: i32, size: i32)` — free memory after use
//! - `handle(ptr: i32, len: i32) -> i64` — execute the step (returns packed ptr|len)
//!
//! The protocol is JSON-in/JSON-out:
//! - Input: JSON bytes written to WASM memory via `alloc`
//! - Output: JSON bytes read from WASM memory at the returned pointer
//!
//! WASM modules are cached by file path to avoid repeated compilation.

use serde_json::Value;
#[cfg(feature = "wasm")]
use serde_json::json;

use orch8_types::error::StepError;

use super::StepContext;

/// Check if a handler name is a WASM plugin handler.
pub fn is_wasm_handler(handler_name: &str) -> bool {
    handler_name.starts_with("wasm://")
}

/// Extract the plugin name from a `wasm://plugin-name` handler.
pub fn parse_plugin_name(handler: &str) -> Option<&str> {
    handler.strip_prefix("wasm://")
}

/// Execute a step by running a WASM module.
#[cfg(feature = "wasm")]
pub async fn handle_wasm_plugin(ctx: StepContext, wasm_path: &str) -> Result<Value, StepError> {
    use tracing::debug;

    debug!(
        instance_id = %ctx.instance_id,
        block_id = %ctx.block_id,
        wasm_path,
        "dispatching step to WASM plugin"
    );

    let input = json!({
        "instance_id": ctx.instance_id.to_string(),
        "block_id": ctx.block_id.to_string(),
        "params": ctx.params,
        "context": {
            "data": ctx.context.data,
            "config": ctx.context.config,
        },
        "attempt": ctx.attempt,
    });
    let input_bytes = serde_json::to_vec(&input).map_err(|e| StepError::Permanent {
        message: format!("wasm plugin: failed to serialize input: {e}"),
        details: None,
    })?;

    // Run WASM execution on a blocking thread since wasmtime is sync.
    let wasm_path_owned = wasm_path.to_string();
    let result =
        tokio::task::spawn_blocking(move || execute_wasm_sync(&wasm_path_owned, &input_bytes))
            .await
            .map_err(|e| StepError::Permanent {
                message: format!("wasm plugin: task join error: {e}"),
                details: None,
            })??;

    Ok(result)
}

/// Sandbox limits applied to every WASM plugin invocation.
///
/// Read once from the environment ([`limits::current`]); every knob has a
/// safe default so an unconfigured server is still bounded. Invalid or zero
/// values fall back to the default with a warning — a typo must never turn a
/// limit off.
#[cfg(feature = "wasm")]
pub mod limits {
    use std::sync::OnceLock;
    use std::time::Duration;

    /// Fuel per invocation (`ORCH8_WASM_FUEL`).
    pub const FUEL_ENV: &str = "ORCH8_WASM_FUEL";
    /// Linear-memory ceiling in bytes (`ORCH8_WASM_MAX_MEMORY_BYTES`).
    pub const MAX_MEMORY_ENV: &str = "ORCH8_WASM_MAX_MEMORY_BYTES";
    /// Wall-clock limit per invocation in milliseconds (`ORCH8_WASM_TIMEOUT_MS`).
    pub const TIMEOUT_ENV: &str = "ORCH8_WASM_TIMEOUT_MS";
    /// Largest module file the loader accepts (`ORCH8_WASM_MAX_MODULE_BYTES`).
    pub const MAX_MODULE_ENV: &str = "ORCH8_WASM_MAX_MODULE_BYTES";
    /// Largest output a module may return (`ORCH8_WASM_MAX_OUTPUT_BYTES`).
    pub const MAX_OUTPUT_ENV: &str = "ORCH8_WASM_MAX_OUTPUT_BYTES";

    /// Resolved limits. See `docs/WASM_USER_STEPS.md` for the rationale
    /// behind each default.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct WasmSandboxLimits {
        /// CPU budget: 1 fuel unit ~ 1 Wasm instruction. Deterministic.
        pub fuel: u64,
        /// Linear-memory ceiling per instance (initial size and every `memory.grow`).
        pub max_memory_bytes: usize,
        /// Function-table ceiling per instance.
        pub max_table_elements: usize,
        /// Wall-clock ceiling per invocation (epoch interruption). Catches work
        /// fuel undercounts, e.g. a single `memory.fill` over 64 MiB.
        pub timeout: Duration,
        /// Module file size cap, enforced before the file is read or compiled.
        pub max_module_bytes: u64,
        /// Output size cap, enforced before the output is parsed.
        pub max_output_bytes: usize,
    }

    /// 10M instructions: ~50–200 ms of dense arithmetic on a modern CPU.
    pub const DEFAULT_FUEL: u64 = 10_000_000;
    /// 64 MiB.
    pub const DEFAULT_MAX_MEMORY_BYTES: usize = 64 * 1024 * 1024;
    /// Function-table entries.
    pub const DEFAULT_MAX_TABLE_ELEMENTS: usize = 10_000;
    /// 2 s wall clock.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);
    /// 32 MiB module files.
    pub const DEFAULT_MAX_MODULE_BYTES: u64 = 32 * 1024 * 1024;
    /// 4 MiB of output JSON.
    pub const DEFAULT_MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;

    impl Default for WasmSandboxLimits {
        fn default() -> Self {
            Self {
                fuel: DEFAULT_FUEL,
                max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
                max_table_elements: DEFAULT_MAX_TABLE_ELEMENTS,
                timeout: DEFAULT_TIMEOUT,
                max_module_bytes: DEFAULT_MAX_MODULE_BYTES,
                max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            }
        }
    }

    impl WasmSandboxLimits {
        /// Build limits from a variable lookup (the process env in production).
        pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
            fn positive<T: std::str::FromStr + PartialEq + Default>(
                get: &impl Fn(&str) -> Option<String>,
                key: &str,
                default: T,
            ) -> T {
                let Some(raw) = get(key).filter(|v| !v.trim().is_empty()) else {
                    return default;
                };
                match raw.trim().parse::<T>() {
                    Ok(v) if v != T::default() => v,
                    _ => {
                        tracing::warn!(
                            key,
                            value = %raw,
                            "invalid WASM sandbox limit (must be a positive integer); using the default"
                        );
                        default
                    }
                }
            }
            let d = Self::default();
            #[allow(clippy::cast_possible_truncation)]
            let timeout_ms = positive(&get, TIMEOUT_ENV, d.timeout.as_millis() as u64);
            Self {
                fuel: positive(&get, FUEL_ENV, d.fuel),
                max_memory_bytes: positive(&get, MAX_MEMORY_ENV, d.max_memory_bytes),
                max_table_elements: d.max_table_elements,
                timeout: Duration::from_millis(timeout_ms),
                max_module_bytes: positive(&get, MAX_MODULE_ENV, d.max_module_bytes),
                max_output_bytes: positive(&get, MAX_OUTPUT_ENV, d.max_output_bytes),
            }
        }
    }

    static CURRENT: OnceLock<WasmSandboxLimits> = OnceLock::new();

    /// Process-wide limits, read from the environment on first use.
    pub fn current() -> &'static WasmSandboxLimits {
        CURRENT.get_or_init(|| WasmSandboxLimits::from_lookup(|k| std::env::var(k).ok()))
    }
}

/// Cached WASM engine (expensive to create) and compiled modules.
#[cfg(feature = "wasm")]
pub(crate) mod cache {
    use std::collections::HashMap;
    use std::io::Read;
    use std::path::{Path, PathBuf};
    use std::sync::{OnceLock, RwLock};
    use std::time::{Duration, SystemTime};

    use wasmtime::{Config, Engine, Module};

    use orch8_types::error::StepError;

    /// Epoch tick used for wall-clock interruption. The deadline of a store is
    /// expressed in ticks, so timeouts are accurate to roughly one tick.
    pub const EPOCH_TICK: Duration = Duration::from_millis(10);

    static ENGINE: OnceLock<Result<Engine, StepError>> = OnceLock::new();

    fn init_engine() -> Result<Engine, StepError> {
        let mut config = Config::new();
        // Metered execution — a store out of fuel traps (deterministic CPU cap).
        config.consume_fuel(true);
        // Epoch-based interruption — the wall-clock cap. Driven by the ticker
        // thread below; each store sets its own deadline in ticks.
        config.epoch_interruption(true);
        let engine = Engine::new(&config).map_err(|e| StepError::Permanent {
            message: format!("wasmtime engine init failed: {e}"),
            details: None,
        })?;
        let ticker = engine.clone();
        if let Err(e) = std::thread::Builder::new()
            .name("orch8-wasm-epoch".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(EPOCH_TICK);
                    ticker.increment_epoch();
                }
            })
        {
            // Fuel still bounds CPU; only the wall-clock cap is lost.
            tracing::error!(error = %e, "wasm plugin: cannot start epoch ticker; wall-clock timeouts disabled");
        }
        Ok(engine)
    }

    /// Most modules kept compiled at once; the cache is cleared when full.
    const MAX_CACHED_MODULES: usize = 256;

    /// Optional directory that every WASM plugin `source` must resolve inside
    /// (after canonicalization, so symlinks and `..` cannot escape it).
    pub const WASM_PLUGIN_DIR_ENV: &str = "ORCH8_WASM_PLUGIN_DIR";

    /// Module binary magic (`\0asm`). Checked explicitly so arbitrary text
    /// files are never handed to a parser that may echo their contents.
    const WASM_MAGIC: &[u8; 4] = b"\0asm";

    /// File identity used to invalidate a cached module when the file changes.
    type Stamp = (u64, Option<SystemTime>);

    // RwLock so multiple executors can read-compare the cache without contending.
    // Only misses take the write lock to insert.
    static MODULES: OnceLock<RwLock<HashMap<PathBuf, (Stamp, Module)>>> = OnceLock::new();

    pub fn engine() -> Result<&'static Engine, StepError> {
        ENGINE.get_or_init(init_engine).as_ref().map_err(|e| {
            // Clone the error so callers get an owned value; engine-init failures
            // are rare and fatal, so the extra allocation is acceptable.
            match e {
                StepError::Permanent { message, details } => StepError::Permanent {
                    message: message.clone(),
                    details: details.clone(),
                },
                StepError::Retryable { message, details } => StepError::Retryable {
                    message: message.clone(),
                    details: details.clone(),
                },
            }
        })
    }

    /// Generic load failure. Deliberately carries no path, OS error, or parser
    /// output: the step error is tenant-visible and must not become a file
    /// oracle. Details go to the server log only.
    fn load_failed() -> StepError {
        StepError::Permanent {
            message: "wasm plugin: failed to load module".into(),
            details: None,
        }
    }

    /// Resolve `path` to a canonical regular file, enforcing `plugin_dir`
    /// containment when configured and the `max_bytes` size cap.
    pub(crate) fn resolve_module_path(
        path: &str,
        plugin_dir: Option<&Path>,
        max_bytes: u64,
    ) -> Result<(PathBuf, std::fs::Metadata), StepError> {
        let canonical = std::fs::canonicalize(path).map_err(|e| {
            tracing::warn!(path, error = %e, "wasm plugin: cannot resolve module path");
            load_failed()
        })?;
        if let Some(dir) = plugin_dir {
            let dir = std::fs::canonicalize(dir).map_err(|e| {
                tracing::error!(dir = %dir.display(), error = %e, "wasm plugin: {WASM_PLUGIN_DIR_ENV} is not accessible");
                load_failed()
            })?;
            if !canonical.starts_with(&dir) {
                tracing::warn!(path, dir = %dir.display(), "wasm plugin: module path is outside {WASM_PLUGIN_DIR_ENV}");
                return Err(load_failed());
            }
        }
        let meta = std::fs::metadata(&canonical).map_err(|e| {
            tracing::warn!(path, error = %e, "wasm plugin: cannot stat module");
            load_failed()
        })?;
        // Rejects directories, FIFOs and devices (`/dev/zero`, `/proc/*` have
        // no meaningful length and would bypass the size cap).
        if !meta.is_file() || meta.len() > max_bytes {
            tracing::warn!(
                path,
                len = meta.len(),
                max_bytes,
                "wasm plugin: module is not a regular file within the size cap"
            );
            return Err(load_failed());
        }
        Ok((canonical, meta))
    }

    /// Read at most `max_bytes` and require the binary magic.
    pub(crate) fn read_module_bytes(path: &Path, max_bytes: u64) -> Result<Vec<u8>, StepError> {
        let file = std::fs::File::open(path).map_err(|e| {
            tracing::warn!(path = %path.display(), error = %e, "wasm plugin: cannot open module");
            load_failed()
        })?;
        let mut bytes = Vec::new();
        file.take(max_bytes.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|e| {
                tracing::warn!(path = %path.display(), error = %e, "wasm plugin: cannot read module");
                load_failed()
            })?;
        if bytes.len() as u64 > max_bytes || !bytes.starts_with(WASM_MAGIC) {
            tracing::warn!(path = %path.display(), "wasm plugin: not a binary wasm module (missing \\0asm magic or too large)");
            return Err(load_failed());
        }
        Ok(bytes)
    }

    pub fn get_or_compile(path: &str, max_module_bytes: u64) -> Result<Module, StepError> {
        let plugin_dir = std::env::var_os(WASM_PLUGIN_DIR_ENV).filter(|v| !v.is_empty());
        get_or_compile_in(path, plugin_dir.as_deref().map(Path::new), max_module_bytes)
    }

    pub(crate) fn get_or_compile_in(
        path: &str,
        plugin_dir: Option<&Path>,
        max_module_bytes: u64,
    ) -> Result<Module, StepError> {
        let (canonical, meta) = resolve_module_path(path, plugin_dir, max_module_bytes)?;
        let stamp: Stamp = (meta.len(), meta.modified().ok());

        let modules = MODULES.get_or_init(|| RwLock::new(HashMap::new()));
        // Fast path: read lock, clone on hit (only if the file is unchanged).
        match modules.read() {
            Ok(cache) => {
                if let Some((cached_stamp, m)) = cache.get(&canonical)
                    && *cached_stamp == stamp
                {
                    return Ok(m.clone());
                }
            }
            Err(e) => {
                // Poisoned read guard means a previous writer panicked holding the lock.
                // Don't silently reuse possibly-corrupt cache state — surface the failure
                // so the caller can retry on a fresh invocation.
                tracing::error!(
                    path = %path,
                    "wasm module cache RwLock poisoned on read; failing closed"
                );
                // Drop the poisoned guard explicitly; the whole process's cache will keep
                // returning errors until a manual restart, which is the correct failure mode.
                drop(e);
                return Err(StepError::Retryable {
                    message: "wasm plugin cache temporarily unavailable (lock poisoned)".into(),
                    details: None,
                });
            }
        }

        let engine = engine()?;
        let bytes = read_module_bytes(&canonical, max_module_bytes)?;
        // `from_binary` never falls back to the WAT text parser.
        let module = Module::from_binary(engine, &bytes).map_err(|e| {
            tracing::warn!(path, error = %e, "wasm plugin: module failed to compile");
            load_failed()
        })?;

        match modules.write() {
            Ok(mut cache) => {
                if cache.len() >= MAX_CACHED_MODULES && !cache.contains_key(&canonical) {
                    cache.clear();
                }
                cache.insert(canonical, (stamp, module.clone()));
            }
            Err(e) => {
                tracing::error!(
                    path = %path,
                    "wasm module cache RwLock poisoned on write; running uncached"
                );
                drop(e);
                // Fall through and return the freshly-compiled module even though we
                // couldn't cache it — execution can still proceed.
            }
        }
        Ok(module)
    }
}

/// The sandbox grants no host imports: no WASI, no filesystem, no sockets, no
/// clock, no randomness. A module that declares any import is refused before
/// instantiation, i.e. before any guest code (including a start function)
/// runs.
#[cfg(feature = "wasm")]
fn reject_imports(module: &wasmtime::Module) -> Result<(), String> {
    match module.imports().next() {
        None => Ok(()),
        Some(import) => Err(format!(
            "module imports `{}::{}` but the sandbox grants no host imports (no WASI, filesystem, network or clock)",
            import.module(),
            import.name()
        )),
    }
}

/// Validate an end-user module before accepting it (e.g. in a hosted upload
/// endpoint), without running any of its code.
///
/// Checks, in order: size cap, `\0asm` magic, compiles under the sandbox
/// engine config, declares no imports, exports `memory`, `alloc(i32)->i32`
/// and `handle(i32,i32)->i64` with the right types, and its declared initial
/// memory fits the memory cap. Returns a tenant-safe reason on rejection.
///
/// Passing validation does not make a module trusted: every invocation is
/// still metered by fuel, memory, table and wall-clock limits.
#[cfg(feature = "wasm")]
pub fn validate_module_bytes(
    bytes: &[u8],
    limits: &limits::WasmSandboxLimits,
) -> Result<(), String> {
    use wasmtime::{ExternType, Module, ValType};

    if bytes.len() as u64 > limits.max_module_bytes {
        return Err(format!(
            "module is {} bytes; the limit is {}",
            bytes.len(),
            limits.max_module_bytes
        ));
    }
    if !bytes.starts_with(b"\0asm") {
        return Err("not a binary WebAssembly module (missing \\0asm magic)".into());
    }
    let engine = cache::engine().map_err(|_| "wasm engine unavailable".to_string())?;
    let module = Module::from_binary(engine, bytes).map_err(|e| format!("invalid module: {e}"))?;
    reject_imports(&module)?;

    let func_sig = |name: &str| -> Result<(Vec<ValType>, Vec<ValType>), String> {
        match module.get_export(name) {
            Some(ExternType::Func(f)) => Ok((f.params().collect(), f.results().collect())),
            Some(_) => Err(format!("export `{name}` must be a function")),
            None => Err(format!("missing required export `{name}`")),
        }
    };
    let (p, r) = func_sig("alloc")?;
    if !(p.len() == 1 && p[0].is_i32() && r.len() == 1 && r[0].is_i32()) {
        return Err("export `alloc` must have type (i32) -> i32".into());
    }
    let (p, r) = func_sig("handle")?;
    if !(p.len() == 2 && p.iter().all(ValType::is_i32) && r.len() == 1 && r[0].is_i64()) {
        return Err("export `handle` must have type (i32, i32) -> i64".into());
    }
    match module.get_export("memory") {
        Some(ExternType::Memory(m)) => {
            let initial = m.minimum().saturating_mul(65_536);
            if initial > limits.max_memory_bytes as u64 {
                return Err(format!(
                    "module declares {initial} bytes of initial memory; the limit is {}",
                    limits.max_memory_bytes
                ));
            }
        }
        Some(_) => return Err("export `memory` must be a memory".into()),
        None => return Err("missing required export `memory`".into()),
    }
    Ok(())
}

/// Store state: the resource limiter plus which limit (if any) was hit, so a
/// denied grow can be reported as a limit error rather than a generic trap.
#[cfg(feature = "wasm")]
struct WasmLimits {
    max_memory: usize,
    max_tables: usize,
    memory_limit_hit: bool,
    table_limit_hit: bool,
}

#[cfg(feature = "wasm")]
impl wasmtime::ResourceLimiter for WasmLimits {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        // Denying makes `memory.grow` return -1 (spec behaviour) and makes an
        // over-sized initial memory fail instantiation.
        let ok = desired <= self.max_memory;
        self.memory_limit_hit |= !ok;
        Ok(ok)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let ok = desired <= self.max_tables;
        self.table_limit_hit |= !ok;
        Ok(ok)
    }
}

#[cfg(feature = "wasm")]
fn permanent(message: String) -> StepError {
    StepError::Permanent {
        message,
        details: None,
    }
}

/// Classify a failed guest call. Every limit hit and every guest trap is
/// permanent: retrying the same input on the same module reproduces it.
/// Only non-trap host errors are retryable.
#[cfg(feature = "wasm")]
fn classify_call_error(what: &str, e: &wasmtime::Error, limits: &WasmLimits) -> StepError {
    let msg = e.to_string();
    if limits.memory_limit_hit {
        return permanent(format!(
            "wasm plugin: memory limit exceeded (max {} bytes) during {what} — {msg}",
            limits.max_memory
        ));
    }
    if limits.table_limit_hit {
        return permanent(format!(
            "wasm plugin: table limit exceeded during {what} — {msg}"
        ));
    }
    // Classify via `downcast_ref::<Trap>()` rather than substring search:
    // wasmtime's `Display` for a trapped call only prints the backtrace, not
    // the trap code.
    match e.downcast_ref::<wasmtime::Trap>().copied() {
        Some(wasmtime::Trap::OutOfFuel) => {
            permanent(format!("wasm plugin: fuel exhausted (cpu limit) — {msg}"))
        }
        Some(wasmtime::Trap::Interrupt) => permanent(format!(
            "wasm plugin: wall-clock timeout exceeded during {what} — {msg}"
        )),
        Some(wasmtime::Trap::MemoryOutOfBounds | wasmtime::Trap::HeapMisaligned) => {
            permanent(format!("wasm plugin: memory fault — {msg}"))
        }
        Some(trap) => permanent(format!("wasm plugin: guest trapped ({trap}) — {msg}")),
        None => {
            if msg.contains("all fuel consumed") || msg.contains("fuel") {
                permanent(format!("wasm plugin: fuel exhausted (cpu limit) — {msg}"))
            } else {
                StepError::Retryable {
                    message: format!("wasm plugin: {what} call failed: {e}"),
                    details: None,
                }
            }
        }
    }
}

/// Synchronous WASM execution with the process-wide [`limits::current`].
#[cfg(feature = "wasm")]
fn execute_wasm_sync(wasm_path: &str, input_bytes: &[u8]) -> Result<Value, StepError> {
    execute_wasm_with_limits(wasm_path, input_bytes, limits::current())
}

/// Synchronous WASM execution using wasmtime.
///
/// Each call creates a fresh `Store` (no state survives between calls) with:
/// * fuel = `limits.fuel` — deterministic CPU ceiling (infinite loops trap)
/// * a `ResourceLimiter` capping memory at `limits.max_memory_bytes` and
///   tables at `limits.max_table_elements`
/// * an epoch deadline of `limits.timeout` — wall-clock ceiling, driven by
///   the engine's ticker thread
/// * an empty `Linker` — no host imports; modules with imports are refused
///   before instantiation
/// * output capped at `limits.max_output_bytes`
#[cfg(feature = "wasm")]
#[allow(clippy::too_many_lines)]
fn execute_wasm_with_limits(
    wasm_path: &str,
    input_bytes: &[u8],
    limits: &limits::WasmSandboxLimits,
) -> Result<Value, StepError> {
    use tracing::warn;
    use wasmtime::{Linker, Store};

    let engine = cache::engine()?;
    let module = cache::get_or_compile(wasm_path, limits.max_module_bytes)?;
    reject_imports(&module).map_err(|m| permanent(format!("wasm plugin: {m}")))?;

    let Ok(input_len) = i32::try_from(input_bytes.len()) else {
        return Err(permanent(format!(
            "wasm plugin: input of {} bytes exceeds the 2 GiB ABI limit",
            input_bytes.len()
        )));
    };

    let state = WasmLimits {
        max_memory: limits.max_memory_bytes,
        max_tables: limits.max_table_elements,
        memory_limit_hit: false,
        table_limit_hit: false,
    };
    let mut store: Store<WasmLimits> = Store::new(engine, state);
    store.limiter(|s| s as &mut dyn wasmtime::ResourceLimiter);
    // Each call starts with a full fuel budget; running out traps the store.
    store
        .set_fuel(limits.fuel)
        .map_err(|e| permanent(format!("wasm plugin: set_fuel failed: {e}")))?;
    // Wall-clock cap: trap once the ticker has advanced past the deadline.
    // +1 tick so the effective timeout is never shorter than configured.
    let tick_ms = cache::EPOCH_TICK.as_millis().max(1);
    #[allow(clippy::cast_possible_truncation)]
    let ticks = (limits.timeout.as_millis().div_ceil(tick_ms) as u64).saturating_add(1);
    store.set_epoch_deadline(ticks);
    store.epoch_deadline_trap();

    let linker: Linker<WasmLimits> = Linker::new(engine);

    let instance = match linker.instantiate(&mut store, &module) {
        Ok(i) => i,
        Err(e) if store.data().memory_limit_hit || store.data().table_limit_hit => {
            return Err(classify_call_error("instantiation", &e, store.data()));
        }
        Err(e) if e.downcast_ref::<wasmtime::Trap>().is_some() => {
            return Err(classify_call_error("instantiation", &e, store.data()));
        }
        Err(e) => {
            return Err(permanent(format!("wasm plugin: instantiation failed: {e}")));
        }
    };

    // Get exported functions.
    let alloc = instance
        .get_typed_func::<i32, i32>(&mut store, "alloc")
        .map_err(|e| permanent(format!("wasm plugin: missing 'alloc' export: {e}")))?;

    let handle = instance
        .get_typed_func::<(i32, i32), i64>(&mut store, "handle")
        .map_err(|e| permanent(format!("wasm plugin: missing 'handle' export: {e}")))?;

    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| permanent("wasm plugin: missing 'memory' export".into()))?;

    // Allocate memory and write input.
    let input_ptr = alloc
        .call(&mut store, input_len)
        .map_err(|e| classify_call_error("alloc", &e, store.data()))?;

    // Ref#13: validate the allocator's return value before casting to usize.
    // A malicious or buggy guest can return a negative pointer (→ huge usize
    // after the cast) or a pointer whose end exceeds linear memory — either
    // would panic inside `copy_from_slice` and tear down the executor thread
    // instead of returning a classified step error.
    let Ok(offset) = usize::try_from(input_ptr) else {
        return Err(permanent(format!(
            "wasm plugin: alloc returned negative pointer {input_ptr}; guest is misbehaving"
        )));
    };
    let end = offset.checked_add(input_bytes.len()).ok_or_else(|| {
        permanent(format!(
            "wasm plugin: alloc offset {offset} + input len {} overflows usize",
            input_bytes.len()
        ))
    })?;
    let mem_len = memory.data(&store).len();
    if end > mem_len {
        return Err(permanent(format!(
            "wasm plugin: alloc range {offset}..{end} exceeds linear memory size {mem_len}"
        )));
    }
    memory.data_mut(&mut store)[offset..end].copy_from_slice(input_bytes);

    // Call handle. Fuel, timeout and resource-limit hits surface as errors here.
    let result_packed = handle
        .call(&mut store, (input_ptr, input_len))
        .map_err(|e| classify_call_error("handle", &e, store.data()))?;

    // Unpack result: high 32 bits = ptr, low 32 bits = len.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let result_ptr = (result_packed >> 32) as u32 as usize;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let result_len = (result_packed & 0xFFFF_FFFF) as usize;

    if result_len > limits.max_output_bytes {
        return Err(permanent(format!(
            "wasm plugin: output of {result_len} bytes exceeds the limit of {} bytes",
            limits.max_output_bytes
        )));
    }

    let mem_data = memory.data(&store);
    // Bounds-check via `checked_add` + explicit range to prevent any usize overflow
    // from the untrusted packed pointer/length value.
    let Some(end) = result_ptr.checked_add(result_len) else {
        return Err(permanent(format!(
            "wasm plugin: result range overflows usize (ptr={result_ptr}, len={result_len})"
        )));
    };
    if end > mem_data.len() {
        return Err(permanent(format!(
            "wasm plugin: result out of bounds (ptr={result_ptr}, len={result_len}, mem={})",
            mem_data.len()
        )));
    }

    let output_bytes = &mem_data[result_ptr..end];
    let output: Value = match serde_json::from_slice(output_bytes) {
        Ok(v) => v,
        Err(parse_err) => {
            // The module broke the JSON contract. Log with enough context to debug
            // but don't crash — wrap the raw bytes so the caller sees the failure.
            warn!(
                wasm_path,
                result_len,
                parse_error = %parse_err,
                "wasm plugin: output is not valid JSON, wrapping raw bytes"
            );
            json!({
                "_wasm_plugin_error": "invalid_json_output",
                "raw": String::from_utf8_lossy(output_bytes),
            })
        }
    };

    // Dealloc if available (optional — some modules handle their own cleanup).
    if let Ok(dealloc) = instance.get_typed_func::<(i32, i32), ()>(&mut store, "dealloc") {
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        if let Err(e) = dealloc.call(&mut store, (result_ptr as i32, result_len as i32)) {
            warn!("wasm plugin: dealloc failed (non-fatal): {e}");
        }
    }

    Ok(output)
}

/// Fallback when WASM feature is disabled.
#[cfg(not(feature = "wasm"))]
#[allow(clippy::unused_async)]
pub async fn handle_wasm_plugin(_ctx: StepContext, _wasm_path: &str) -> Result<Value, StepError> {
    Err(StepError::Permanent {
        message: "WASM plugin support is not enabled (compile with --features wasm)".into(),
        details: None,
    })
}

#[cfg(test)]
#[allow(clippy::match_wildcard_for_single_variants)]
mod tests {
    use super::*;

    #[test]
    fn is_wasm_handler_detects_prefix() {
        assert!(is_wasm_handler("wasm://my-plugin"));
        assert!(is_wasm_handler("wasm://transform"));
        assert!(!is_wasm_handler("grpc://localhost:50051/Svc.Method"));
        assert!(!is_wasm_handler("http_request"));
        assert!(!is_wasm_handler("noop"));
    }

    #[test]
    fn parse_plugin_name_extracts_name() {
        assert_eq!(parse_plugin_name("wasm://my-plugin"), Some("my-plugin"));
        assert_eq!(parse_plugin_name("wasm://transform"), Some("transform"));
        assert_eq!(parse_plugin_name("grpc://host"), None);
    }

    #[test]
    fn parse_plugin_name_edge_cases() {
        assert_eq!(parse_plugin_name("wasm://"), Some(""));
        assert_eq!(
            parse_plugin_name("wasm://path/to/module"),
            Some("path/to/module")
        );
        assert_eq!(parse_plugin_name(""), None);
        assert_eq!(parse_plugin_name("http_request"), None);
    }

    #[test]
    fn is_wasm_handler_edge_cases() {
        assert!(!is_wasm_handler(""));
        assert!(is_wasm_handler("wasm://"));
        assert!(!is_wasm_handler("WASM://plugin"));
    }

    // ------------------------------------------------------------------
    // Execution-path tests.
    //
    // Each test compiles a tiny WAT snippet into a .wasm file on disk and
    // drives `execute_wasm_sync` against it. Exercises the real wasmtime
    // instantiation + memory+ABI protocol, not just the parse helpers.
    //
    // Gated on the `wasm` feature since `execute_wasm_sync` only exists
    // when that feature is enabled (same gate the handler itself uses).
    // ------------------------------------------------------------------

    #[cfg(feature = "wasm")]
    mod exec {
        use super::super::*;
        use std::io::Write;
        use tempfile::NamedTempFile;

        /// Compile WAT → wasm bytes → tmp .wasm file. Returns the tempfile
        /// so the caller can keep it alive for the duration of the test
        /// (dropping it deletes the file on disk).
        fn wat_to_tmp_wasm(wat_text: &str) -> NamedTempFile {
            let bytes = wat::parse_str(wat_text).expect("valid WAT");
            let mut f = NamedTempFile::with_suffix(".wasm").expect("tempfile");
            f.write_all(&bytes).expect("write wasm bytes");
            f.flush().expect("flush");
            f
        }

        /// Minimal echo module: writes a fixed JSON output, returns packed ptr|len.
        ///
        /// Layout in memory:
        ///   - offset 0..N reserved for input (written by host)
        ///   - offset 1024..1024+len for output bytes (written by the module)
        const ECHO_WAT: &str = r#"
            (module
              (memory (export "memory") 1)
              (data (i32.const 1024) "{\"echo\":true}")

              ;; Host calls alloc(size) and gets back a pointer.
              ;; We just always return 0 — input is written to offset 0.
              (func (export "alloc") (param $size i32) (result i32)
                i32.const 0)

              ;; Host calls handle(ptr, len) and gets back packed ptr|len for output.
              ;; High 32 bits = output ptr (1024), low 32 bits = len (13).
              (func (export "handle") (param $ptr i32) (param $len i32) (result i64)
                i64.const 4398046511117)  ;; (1024 << 32) | 13 = 4398046511104 | 13
            )
        "#;

        #[test]
        fn echo_module_round_trips_json_output() {
            let tmp = wat_to_tmp_wasm(ECHO_WAT);
            let out = execute_wasm_sync(tmp.path().to_str().unwrap(), br#"{"hello":"world"}"#)
                .expect("echo should succeed");
            assert_eq!(out, serde_json::json!({ "echo": true }));
        }

        #[test]
        fn missing_alloc_export_is_permanent_error() {
            let wat = r#"
                (module
                  (memory (export "memory") 1)
                  (func (export "handle") (param i32 i32) (result i64)
                    i64.const 0)
                )
            "#;
            let tmp = wat_to_tmp_wasm(wat);
            let err =
                execute_wasm_sync(tmp.path().to_str().unwrap(), b"{}").expect_err("should fail");
            match err {
                StepError::Permanent { message, .. } => {
                    assert!(message.contains("alloc"), "unexpected msg: {message}");
                }
                other => panic!("expected Permanent, got {other:?}"),
            }
        }

        #[test]
        fn missing_handle_export_is_permanent_error() {
            let wat = r#"
                (module
                  (memory (export "memory") 1)
                  (func (export "alloc") (param i32) (result i32) i32.const 0)
                )
            "#;
            let tmp = wat_to_tmp_wasm(wat);
            let err =
                execute_wasm_sync(tmp.path().to_str().unwrap(), b"{}").expect_err("should fail");
            match err {
                StepError::Permanent { message, .. } => {
                    assert!(message.contains("handle"), "unexpected msg: {message}");
                }
                other => panic!("expected Permanent, got {other:?}"),
            }
        }

        #[test]
        fn missing_memory_export_is_permanent_error() {
            // No memory export — instantiation will succeed but get_memory fails.
            let wat = r#"
                (module
                  (memory 1)
                  (func (export "alloc") (param i32) (result i32) i32.const 0)
                  (func (export "handle") (param i32 i32) (result i64) i64.const 0)
                )
            "#;
            let tmp = wat_to_tmp_wasm(wat);
            let err =
                execute_wasm_sync(tmp.path().to_str().unwrap(), b"{}").expect_err("should fail");
            match err {
                StepError::Permanent { message, .. } => {
                    assert!(message.contains("memory"), "unexpected msg: {message}");
                }
                other => panic!("expected Permanent, got {other:?}"),
            }
        }

        #[test]
        fn missing_file_is_permanent_error() {
            let err = execute_wasm_sync("/does/not/exist/definitely-missing.wasm", b"{}")
                .expect_err("should fail");
            match err {
                StepError::Permanent { message, .. } => {
                    assert!(message.contains("failed to load"), "unexpected: {message}");
                }
                other => panic!("expected Permanent, got {other:?}"),
            }
        }

        #[test]
        fn invalid_json_output_is_wrapped_as_raw() {
            // Module emits non-JSON bytes. The handler must not error — it
            // wraps the raw bytes with an error marker and returns Ok.
            let wat = r#"
                (module
                  (memory (export "memory") 1)
                  (data (i32.const 1024) "not json at all!")
                  (func (export "alloc") (param i32) (result i32) i32.const 0)
                  (func (export "handle") (param i32 i32) (result i64)
                    i64.const 4398046511120)  ;; (1024 << 32) | 16
                )
            "#;
            let tmp = wat_to_tmp_wasm(wat);
            let out = execute_wasm_sync(tmp.path().to_str().unwrap(), b"{}")
                .expect("wrapper must not propagate parse error");
            assert_eq!(
                out.get("_wasm_plugin_error").and_then(|v| v.as_str()),
                Some("invalid_json_output"),
            );
            assert!(out.get("raw").is_some());
        }

        #[test]
        fn result_pointer_out_of_bounds_is_permanent_error() {
            // Memory is 1 page (64 KiB). Point the result 10 MiB deep.
            let wat = r#"
                (module
                  (memory (export "memory") 1)
                  (func (export "alloc") (param i32) (result i32) i32.const 0)
                  (func (export "handle") (param i32 i32) (result i64)
                    ;; (10_000_000 << 32) | 10 = 42949672960000010
                    i64.const 42949672960000010)
                )
            "#;
            let tmp = wat_to_tmp_wasm(wat);
            let err =
                execute_wasm_sync(tmp.path().to_str().unwrap(), b"{}").expect_err("should fail");
            match err {
                StepError::Permanent { message, .. } => {
                    assert!(
                        message.contains("out of bounds") || message.contains("overflow"),
                        "unexpected msg: {message}"
                    );
                }
                other => panic!("expected Permanent, got {other:?}"),
            }
        }

        #[test]
        fn fuel_exhaustion_is_permanent_error() {
            // Infinite loop — must trap on fuel before running forever.
            let wat = r#"
                (module
                  (memory (export "memory") 1)
                  (func (export "alloc") (param i32) (result i32) i32.const 0)
                  (func (export "handle") (param i32 i32) (result i64)
                    (loop $l (br $l))
                    i64.const 0)
                )
            "#;
            let tmp = wat_to_tmp_wasm(wat);
            let err = execute_wasm_sync(tmp.path().to_str().unwrap(), b"{}")
                .expect_err("infinite loop must trap");
            match err {
                StepError::Permanent { message, .. } => {
                    // wasmtime's trap message varies by version; any of these
                    // substrings identifies the fuel path.
                    let m = message.to_lowercase();
                    assert!(
                        m.contains("fuel") || m.contains("cpu limit"),
                        "unexpected: {message}"
                    );
                }
                other => panic!("expected Permanent, got {other:?}"),
            }
        }

        #[test]
        fn module_cache_returns_same_module_on_second_call() {
            // Compiling the same path twice should hit the RwLock read-path.
            // We verify by running two successful echoes back-to-back.
            let tmp = wat_to_tmp_wasm(ECHO_WAT);
            let path = tmp.path().to_str().unwrap();
            for _ in 0..3 {
                let out = execute_wasm_sync(path, b"{}").expect("ok");
                assert_eq!(out, serde_json::json!({ "echo": true }));
            }
        }

        /// ENG-P-N1: a text file (WAT or anything else) must never reach a
        /// parser, and the step error must not echo the file's contents.
        #[test]
        fn non_binary_source_is_rejected_without_echoing_contents() {
            let mut f = NamedTempFile::with_suffix(".wasm").unwrap();
            f.write_all(b"SECRET_TOKEN=hunter2\n(module)").unwrap();
            f.flush().unwrap();
            let err =
                execute_wasm_sync(f.path().to_str().unwrap(), b"{}").expect_err("should fail");
            match err {
                StepError::Permanent { message, .. } => {
                    assert_eq!(message, "wasm plugin: failed to load module");
                }
                other => panic!("expected Permanent, got {other:?}"),
            }
            // Valid WAT text is also refused — only binary modules load.
            let mut f = NamedTempFile::with_suffix(".wat").unwrap();
            f.write_all(ECHO_WAT.as_bytes()).unwrap();
            f.flush().unwrap();
            assert!(execute_wasm_sync(f.path().to_str().unwrap(), b"{}").is_err());
        }

        #[cfg(unix)]
        #[test]
        fn device_file_source_is_rejected() {
            let err = execute_wasm_sync("/dev/zero", b"{}").expect_err("should fail");
            assert!(matches!(err, StepError::Permanent { .. }));
        }

        #[test]
        fn plugin_dir_pins_module_paths() {
            let dir = tempfile::tempdir().unwrap();
            let inside = dir.path().join("echo.wasm");
            std::fs::write(&inside, wat::parse_str(ECHO_WAT).unwrap()).unwrap();
            let outside = wat_to_tmp_wasm(ECHO_WAT);

            assert!(
                cache::get_or_compile_in(inside.to_str().unwrap(), Some(dir.path()), u64::MAX)
                    .is_ok()
            );
            assert!(
                cache::get_or_compile_in(
                    outside.path().to_str().unwrap(),
                    Some(dir.path()),
                    u64::MAX
                )
                .is_err()
            );
            // `..` escapes are resolved before the containment check.
            let escape = dir
                .path()
                .join("..")
                .join(outside.path().file_name().unwrap());
            if escape.exists() {
                assert!(
                    cache::get_or_compile_in(escape.to_str().unwrap(), Some(dir.path()), u64::MAX)
                        .is_err()
                );
            }
        }

        #[test]
        fn cached_module_is_invalidated_when_file_changes() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("m.wasm");
            std::fs::write(&path, wat::parse_str(ECHO_WAT).unwrap()).unwrap();
            let p = path.to_str().unwrap();
            assert!(execute_wasm_sync(p, b"{}").is_ok());
            // Replace with garbage of a different length → must not serve the
            // stale compiled module.
            std::fs::write(&path, b"garbage").unwrap();
            assert!(execute_wasm_sync(p, b"{}").is_err());
        }

        #[test]
        fn instantiation_failure_on_malformed_module_is_permanent() {
            // Write bytes that are NOT valid wasm.
            let mut f = NamedTempFile::with_suffix(".wasm").unwrap();
            f.write_all(b"definitely not a wasm module").unwrap();
            f.flush().unwrap();
            let err =
                execute_wasm_sync(f.path().to_str().unwrap(), b"{}").expect_err("should fail");
            match err {
                StepError::Permanent { .. } => {}
                other => panic!("expected Permanent, got {other:?}"),
            }
        }

        // ------------------------------------------------------------------
        // Sandbox limits (docs/WASM_USER_STEPS.md). Each test proves one
        // limit actually trips for a hostile module.
        // ------------------------------------------------------------------

        use super::super::limits::WasmSandboxLimits;

        fn permanent_message(err: StepError) -> String {
            match err {
                StepError::Permanent { message, .. } => message,
                other => panic!("expected Permanent, got {other:?}"),
            }
        }

        #[test]
        fn infinite_loop_trips_wall_clock_timeout_when_fuel_is_ample() {
            let wat = r#"
                (module
                  (memory (export "memory") 1)
                  (func (export "alloc") (param i32) (result i32) i32.const 0)
                  (func (export "handle") (param i32 i32) (result i64)
                    (loop $l (br $l))
                    i64.const 0)
                )
            "#;
            let tmp = wat_to_tmp_wasm(wat);
            let limits = WasmSandboxLimits {
                fuel: u64::MAX,
                timeout: std::time::Duration::from_millis(100),
                ..WasmSandboxLimits::default()
            };
            let started = std::time::Instant::now();
            let err = execute_wasm_with_limits(tmp.path().to_str().unwrap(), b"{}", &limits)
                .expect_err("infinite loop must be interrupted");
            let elapsed = started.elapsed();
            let message = permanent_message(err);
            assert!(
                message.contains("wall-clock timeout"),
                "unexpected: {message}"
            );
            assert!(
                elapsed < std::time::Duration::from_secs(5),
                "timeout took {elapsed:?}"
            );
        }

        #[test]
        fn infinite_loop_trips_fuel_limit_with_configured_budget() {
            let wat = r#"
                (module
                  (memory (export "memory") 1)
                  (func (export "alloc") (param i32) (result i32) i32.const 0)
                  (func (export "handle") (param i32 i32) (result i64)
                    (loop $l (br $l))
                    i64.const 0)
                )
            "#;
            let tmp = wat_to_tmp_wasm(wat);
            let limits = WasmSandboxLimits {
                fuel: 10_000,
                timeout: std::time::Duration::from_secs(60),
                ..WasmSandboxLimits::default()
            };
            let message = permanent_message(
                execute_wasm_with_limits(tmp.path().to_str().unwrap(), b"{}", &limits)
                    .expect_err("must run out of fuel"),
            );
            assert!(message.contains("fuel"), "unexpected: {message}");
        }

        #[test]
        fn memory_grow_beyond_cap_is_denied_and_reported() {
            // Grows by 32 pages (2 MiB) against a 1 MiB cap; traps if denied,
            // the way a Rust/C guest allocator aborts on OOM.
            let wat = r#"
                (module
                  (memory (export "memory") 1)
                  (func (export "alloc") (param i32) (result i32) i32.const 0)
                  (func (export "handle") (param i32 i32) (result i64)
                    (if (i32.eq (memory.grow (i32.const 32)) (i32.const -1))
                      (then unreachable))
                    i64.const 0)
                )
            "#;
            let tmp = wat_to_tmp_wasm(wat);
            let limits = WasmSandboxLimits {
                max_memory_bytes: 1024 * 1024,
                ..WasmSandboxLimits::default()
            };
            let message = permanent_message(
                execute_wasm_with_limits(tmp.path().to_str().unwrap(), b"{}", &limits)
                    .expect_err("grow past the cap must fail"),
            );
            assert!(
                message.contains("memory limit exceeded"),
                "unexpected: {message}"
            );

            // The same module is fine when the cap allows the growth.
            let roomy = WasmSandboxLimits {
                max_memory_bytes: 4 * 1024 * 1024,
                ..WasmSandboxLimits::default()
            };
            // Output (ptr 0, len 0) is empty → wrapped as invalid JSON, not an error.
            assert!(execute_wasm_with_limits(tmp.path().to_str().unwrap(), b"{}", &roomy).is_ok());
        }

        #[test]
        fn initial_memory_beyond_cap_fails_instantiation() {
            let wat = r#"
                (module
                  (memory (export "memory") 64)
                  (func (export "alloc") (param i32) (result i32) i32.const 0)
                  (func (export "handle") (param i32 i32) (result i64) i64.const 0)
                )
            "#;
            let tmp = wat_to_tmp_wasm(wat);
            let limits = WasmSandboxLimits {
                max_memory_bytes: 1024 * 1024,
                ..WasmSandboxLimits::default()
            };
            let message = permanent_message(
                execute_wasm_with_limits(tmp.path().to_str().unwrap(), b"{}", &limits)
                    .expect_err("4 MiB initial memory against a 1 MiB cap"),
            );
            assert!(
                message.contains("memory limit exceeded"),
                "unexpected: {message}"
            );
        }

        #[test]
        fn wasi_filesystem_and_socket_imports_are_rejected_before_instantiation() {
            for (module, name) in [
                ("wasi_snapshot_preview1", "path_open"),
                ("wasi_snapshot_preview1", "fd_write"),
                ("wasi_snapshot_preview1", "sock_accept"),
                ("wasi:sockets/tcp", "connect"),
                ("env", "anything"),
            ] {
                // The start function would run at instantiation; it must never run.
                let wat = format!(
                    r#"
                    (module
                      (import "{module}" "{name}" (func $f (param i32) (result i32)))
                      (memory (export "memory") 1)
                      (func $start (drop (call $f (i32.const 0))))
                      (start $start)
                      (func (export "alloc") (param i32) (result i32) i32.const 0)
                      (func (export "handle") (param i32 i32) (result i64) i64.const 0)
                    )
                "#
                );
                let tmp = wat_to_tmp_wasm(&wat);
                let message = permanent_message(
                    execute_wasm_sync(tmp.path().to_str().unwrap(), b"{}")
                        .expect_err("host imports must be refused"),
                );
                assert!(
                    message.contains("grants no host imports") && message.contains(name),
                    "unexpected: {message}"
                );
                let bytes = wat::parse_str(&wat).unwrap();
                let why = validate_module_bytes(&bytes, &WasmSandboxLimits::default())
                    .expect_err("validator must refuse imports");
                assert!(why.contains("grants no host imports"), "unexpected: {why}");
            }
        }

        #[test]
        fn output_beyond_cap_is_rejected_before_parsing() {
            // ECHO_WAT returns 13 bytes.
            let tmp = wat_to_tmp_wasm(ECHO_WAT);
            let limits = WasmSandboxLimits {
                max_output_bytes: 8,
                ..WasmSandboxLimits::default()
            };
            let message = permanent_message(
                execute_wasm_with_limits(tmp.path().to_str().unwrap(), b"{}", &limits)
                    .expect_err("13-byte output over an 8-byte cap"),
            );
            assert!(
                message.contains("exceeds the limit"),
                "unexpected: {message}"
            );
        }

        #[test]
        fn module_file_beyond_size_cap_is_not_loaded() {
            let tmp = wat_to_tmp_wasm(ECHO_WAT);
            let limits = WasmSandboxLimits {
                max_module_bytes: 16,
                ..WasmSandboxLimits::default()
            };
            let message = permanent_message(
                execute_wasm_with_limits(tmp.path().to_str().unwrap(), b"{}", &limits)
                    .expect_err("module larger than 16 bytes"),
            );
            assert_eq!(message, "wasm plugin: failed to load module");
            let bytes = wat::parse_str(ECHO_WAT).unwrap();
            assert!(validate_module_bytes(&bytes, &limits).is_err());
        }

        #[test]
        fn validator_accepts_abi_conformant_module_and_rejects_shape_errors() {
            let ok = wat::parse_str(ECHO_WAT).unwrap();
            let d = WasmSandboxLimits::default();
            validate_module_bytes(&ok, &d).expect("echo module is valid");

            let wrong_handle = wat::parse_str(
                r#"(module (memory (export "memory") 1)
                     (func (export "alloc") (param i32) (result i32) i32.const 0)
                     (func (export "handle") (param i32 i32) (result i32) i32.const 0))"#,
            )
            .unwrap();
            assert!(
                validate_module_bytes(&wrong_handle, &d)
                    .unwrap_err()
                    .contains("handle")
            );

            let no_memory = wat::parse_str(
                r#"(module (memory 1)
                     (func (export "alloc") (param i32) (result i32) i32.const 0)
                     (func (export "handle") (param i32 i32) (result i64) i64.const 0))"#,
            )
            .unwrap();
            assert!(
                validate_module_bytes(&no_memory, &d)
                    .unwrap_err()
                    .contains("memory")
            );

            let small = WasmSandboxLimits {
                max_memory_bytes: 65_536,
                ..d
            };
            let big_memory = wat::parse_str(
                r#"(module (memory (export "memory") 2)
                     (func (export "alloc") (param i32) (result i32) i32.const 0)
                     (func (export "handle") (param i32 i32) (result i64) i64.const 0))"#,
            )
            .unwrap();
            assert!(
                validate_module_bytes(&big_memory, &small)
                    .unwrap_err()
                    .contains("initial memory")
            );
            assert!(validate_module_bytes(b"(module)", &d).is_err());
        }

        #[test]
        fn limits_from_env_use_defaults_and_reject_zero_or_garbage() {
            let d = WasmSandboxLimits::default();
            assert_eq!(WasmSandboxLimits::from_lookup(|_| None), d);

            let parsed = WasmSandboxLimits::from_lookup(|k| match k {
                "ORCH8_WASM_FUEL" => Some("5000".into()),
                "ORCH8_WASM_MAX_MEMORY_BYTES" => Some("1048576".into()),
                "ORCH8_WASM_TIMEOUT_MS" => Some("250".into()),
                "ORCH8_WASM_MAX_MODULE_BYTES" => Some("0".into()),
                "ORCH8_WASM_MAX_OUTPUT_BYTES" => Some("lots".into()),
                _ => None,
            });
            assert_eq!(parsed.fuel, 5000);
            assert_eq!(parsed.max_memory_bytes, 1_048_576);
            assert_eq!(parsed.timeout, std::time::Duration::from_millis(250));
            // Zero and garbage never disable a limit.
            assert_eq!(parsed.max_module_bytes, d.max_module_bytes);
            assert_eq!(parsed.max_output_bytes, d.max_output_bytes);
        }
    }
}
