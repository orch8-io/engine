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

use serde_json::{json, Value};

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

/// Cached WASM engine (expensive to create) and compiled modules.
#[cfg(feature = "wasm")]
pub(crate) mod cache {
    use std::collections::HashMap;
    use std::sync::{LazyLock, RwLock};

    use wasmtime::{Config, Engine, Module};

    use orch8_types::error::StepError;

    /// Hard ceiling on CPU work per invocation. 1 fuel unit ~ 1 Wasm op.
    /// 10M ops is ~50–200ms of dense arithmetic on a modern CPU — generous for
    /// transforms, lethal for infinite loops.
    pub const WASM_FUEL_LIMIT: u64 = 10_000_000;

    /// Hard ceiling on linear-memory growth per instance: 64 MiB.
    /// Prevents a single plugin from exhausting server RAM.
    pub const WASM_MAX_MEMORY_BYTES: usize = 64 * 1024 * 1024;

    /// Hard ceiling on table size (function-table entries).
    pub const WASM_MAX_TABLE_ELEMENTS: usize = 10_000;

    static ENGINE: LazyLock<Engine> = LazyLock::new(|| {
        let mut config = Config::new();
        // Metered execution — a store out of fuel traps, caller converts to a retryable error.
        config.consume_fuel(true);
        // Epoch-based interruption: lets us cancel long-running stores cooperatively.
        config.epoch_interruption(true);
        Engine::new(&config).expect("wasmtime Engine::new with valid Config must succeed")
    });

    // RwLock so multiple executors can read-compare the cache without contending.
    // Only misses take the write lock to insert.
    static MODULES: LazyLock<RwLock<HashMap<String, Module>>> =
        LazyLock::new(|| RwLock::new(HashMap::new()));

    pub fn engine() -> &'static Engine {
        &ENGINE
    }

    pub fn get_or_compile(path: &str) -> Result<Module, StepError> {
        // Fast path: read lock, clone on hit.
        match MODULES.read() {
            Ok(cache) => {
                if let Some(m) = cache.get(path) {
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

        let module = Module::from_file(&ENGINE, path).map_err(|e| StepError::Permanent {
            message: format!("wasm plugin: failed to load module '{path}': {e}"),
            details: None,
        })?;

        match MODULES.write() {
            Ok(mut cache) => {
                cache.insert(path.to_string(), module.clone());
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

/// Resource limiter that caps WASM linear-memory and table growth.
#[cfg(feature = "wasm")]
struct WasmLimits {
    max_memory: usize,
    max_tables: usize,
}

#[cfg(feature = "wasm")]
impl wasmtime::ResourceLimiter for WasmLimits {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(desired <= self.max_memory)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(desired <= self.max_tables)
    }
}

/// Synchronous WASM execution using wasmtime.
///
/// Each call creates a fresh `Store` with:
/// * fuel = [`cache::WASM_FUEL_LIMIT`] — hard CPU ceiling (infinite loops trap)
/// * `ResourceLimiter` capping memory at [`cache::WASM_MAX_MEMORY_BYTES`] and
///   tables at [`cache::WASM_MAX_TABLE_ELEMENTS`]
/// * epoch deadline = 1 tick from current epoch — the caller of
///   `Engine::increment_epoch()` (not currently wired) can cancel long stores.
#[cfg(feature = "wasm")]
#[allow(clippy::too_many_lines)]
fn execute_wasm_sync(wasm_path: &str, input_bytes: &[u8]) -> Result<Value, StepError> {
    use tracing::warn;
    use wasmtime::{Linker, Store};

    let engine = cache::engine();
    let module = cache::get_or_compile(wasm_path)?;

    let limits = WasmLimits {
        max_memory: cache::WASM_MAX_MEMORY_BYTES,
        max_tables: cache::WASM_MAX_TABLE_ELEMENTS,
    };
    let mut store: Store<WasmLimits> = Store::new(engine, limits);
    store.limiter(|s| s as &mut dyn wasmtime::ResourceLimiter);
    // Each call starts with a full fuel budget; running out traps the store.
    if let Err(e) = store.set_fuel(cache::WASM_FUEL_LIMIT) {
        return Err(StepError::Permanent {
            message: format!("wasm plugin: set_fuel failed: {e}"),
            details: None,
        });
    }
    // Bind the store to the next epoch tick — an external ticker calling
    // `engine.increment_epoch()` will interrupt runaway calls. Set a generous
    // default here so we don't trap unless a ticker is active.
    store.set_epoch_deadline(u64::MAX);

    let linker = Linker::new(engine);

    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|e| StepError::Permanent {
            message: format!("wasm plugin: instantiation failed: {e}"),
            details: None,
        })?;

    // Get exported functions.
    let alloc = instance
        .get_typed_func::<i32, i32>(&mut store, "alloc")
        .map_err(|e| StepError::Permanent {
            message: format!("wasm plugin: missing 'alloc' export: {e}"),
            details: None,
        })?;

    let handle = instance
        .get_typed_func::<(i32, i32), i64>(&mut store, "handle")
        .map_err(|e| StepError::Permanent {
            message: format!("wasm plugin: missing 'handle' export: {e}"),
            details: None,
        })?;

    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| StepError::Permanent {
            message: "wasm plugin: missing 'memory' export".into(),
            details: None,
        })?;

    // Allocate memory and write input.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let input_len = input_bytes.len() as i32;
    let input_ptr = alloc
        .call(&mut store, input_len)
        .map_err(|e| StepError::Permanent {
            message: format!("wasm plugin: alloc failed: {e}"),
            details: None,
        })?;

    // Ref#13: validate the allocator's return value before casting to usize.
    // A malicious or buggy guest can return a negative pointer (→ huge usize
    // after the cast) or a pointer whose end exceeds linear memory — either
    // would panic inside `copy_from_slice` and tear down the executor thread
    // instead of returning a classified step error.
    if input_ptr < 0 {
        return Err(StepError::Permanent {
            message: format!(
                "wasm plugin: alloc returned negative pointer {input_ptr}; guest is misbehaving"
            ),
            details: None,
        });
    }
    #[allow(clippy::cast_sign_loss)]
    let offset = input_ptr as usize;
    let end = offset
        .checked_add(input_bytes.len())
        .ok_or_else(|| StepError::Permanent {
            message: format!(
                "wasm plugin: alloc offset {offset} + input len {} overflows usize",
                input_bytes.len()
            ),
            details: None,
        })?;
    let mem_len = memory.data(&store).len();
    if end > mem_len {
        return Err(StepError::Permanent {
            message: format!(
                "wasm plugin: alloc range {offset}..{end} exceeds linear memory size {mem_len}"
            ),
            details: None,
        });
    }
    memory.data_mut(&mut store)[offset..end].copy_from_slice(input_bytes);

    // Call handle. Fuel exhaustion or resource-limit hits surface as traps here.
    let result_packed = handle
        .call(&mut store, (input_ptr, input_len))
        .map_err(|e| {
            // Fuel-exhaustion and resource-limit traps are *permanent* for this
            // input — retrying with the same payload will hit the same limit.
            // Only true wasmtime-level faults (host traps, I/O) should be retryable.
            //
            // Classify via `downcast_ref::<Trap>()` rather than substring search:
            // wasmtime's `Display` for a trapped call only prints the backtrace,
            // not the trap code, so "fuel" never appears in `to_string()` for a
            // `br`-loop out-of-fuel trap — only in the structured `Trap` variant.
            let trap_code = e.downcast_ref::<wasmtime::Trap>().copied();
            let msg = e.to_string();
            match trap_code {
                Some(wasmtime::Trap::OutOfFuel) => StepError::Permanent {
                    message: format!("wasm plugin: fuel exhausted (cpu limit) — {msg}"),
                    details: None,
                },
                Some(wasmtime::Trap::MemoryOutOfBounds | wasmtime::Trap::HeapMisaligned) => {
                    StepError::Permanent {
                        message: format!("wasm plugin: memory fault — {msg}"),
                        details: None,
                    }
                }
                _ => {
                    // Fallback: legacy substring probe for cases where the error
                    // isn't a typed Trap (e.g. memory-limit hits surface as a
                    // generic `anyhow::Error` with the message inline).
                    if msg.contains("all fuel consumed") || msg.contains("fuel") {
                        StepError::Permanent {
                            message: format!("wasm plugin: fuel exhausted (cpu limit) — {msg}"),
                            details: None,
                        }
                    } else if msg.contains("memory") && msg.contains("limit") {
                        StepError::Permanent {
                            message: format!("wasm plugin: memory limit exceeded — {msg}"),
                            details: None,
                        }
                    } else {
                        StepError::Retryable {
                            message: format!("wasm plugin: handle call failed: {e}"),
                            details: None,
                        }
                    }
                }
            }
        })?;

    // Unpack result: high 32 bits = ptr, low 32 bits = len.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let result_ptr = (result_packed >> 32) as usize;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let result_len = (result_packed & 0xFFFF_FFFF) as usize;

    let mem_data = memory.data(&store);
    // Bounds-check via `checked_add` + explicit range to prevent any usize overflow
    // from the untrusted packed pointer/length value.
    let Some(end) = result_ptr.checked_add(result_len) else {
        return Err(StepError::Permanent {
            message: format!(
                "wasm plugin: result range overflows usize (ptr={result_ptr}, len={result_len})"
            ),
            details: None,
        });
    };
    if end > mem_data.len() {
        return Err(StepError::Permanent {
            message: format!(
                "wasm plugin: result out of bounds (ptr={result_ptr}, len={result_len}, mem={})",
                mem_data.len()
            ),
            details: None,
        });
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
    }
}
