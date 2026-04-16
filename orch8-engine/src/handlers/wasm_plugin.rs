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
pub async fn handle_wasm_plugin(
    ctx: StepContext,
    wasm_path: &str,
) -> Result<Value, StepError> {
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
    let result = tokio::task::spawn_blocking(move || {
        execute_wasm_sync(&wasm_path_owned, &input_bytes)
    })
    .await
    .map_err(|e| StepError::Permanent {
        message: format!("wasm plugin: task join error: {e}"),
        details: None,
    })??;

    Ok(result)
}

/// Cached WASM engine (expensive to create) and compiled modules.
#[cfg(feature = "wasm")]
mod cache {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex};

    use wasmtime::{Engine, Module};

    use orch8_types::error::StepError;

    static ENGINE: LazyLock<Engine> = LazyLock::new(Engine::default);
    static MODULES: LazyLock<Mutex<HashMap<String, Module>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    pub fn engine() -> &'static Engine {
        &ENGINE
    }

    pub fn get_or_compile(path: &str) -> Result<Module, StepError> {
        {
            let cache = MODULES.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(m) = cache.get(path) {
                return Ok(m.clone());
            }
        }
        let module = Module::from_file(&ENGINE, path).map_err(|e| StepError::Permanent {
            message: format!("wasm plugin: failed to load module '{path}': {e}"),
            details: None,
        })?;
        let mut cache = MODULES.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.insert(path.to_string(), module.clone());
        Ok(module)
    }
}

/// Synchronous WASM execution using wasmtime.
#[cfg(feature = "wasm")]
fn execute_wasm_sync(
    wasm_path: &str,
    input_bytes: &[u8],
) -> Result<Value, StepError> {
    use tracing::warn;
    use wasmtime::{Linker, Store};

    let engine = cache::engine();
    let module = cache::get_or_compile(wasm_path)?;

    let mut store = Store::new(engine, ());
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
    let input_ptr = alloc.call(&mut store, input_len).map_err(|e| StepError::Permanent {
        message: format!("wasm plugin: alloc failed: {e}"),
        details: None,
    })?;

    #[allow(clippy::cast_sign_loss)]
    let offset = input_ptr as usize;
    memory.data_mut(&mut store)[offset..offset + input_bytes.len()]
        .copy_from_slice(input_bytes);

    // Call handle.
    let result_packed = handle
        .call(&mut store, (input_ptr, input_len))
        .map_err(|e| StepError::Retryable {
            message: format!("wasm plugin: handle call failed: {e}"),
            details: None,
        })?;

    // Unpack result: high 32 bits = ptr, low 32 bits = len.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let result_ptr = (result_packed >> 32) as usize;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let result_len = (result_packed & 0xFFFF_FFFF) as usize;

    let mem_data = memory.data(&store);
    if result_ptr + result_len > mem_data.len() {
        return Err(StepError::Permanent {
            message: format!(
                "wasm plugin: result out of bounds (ptr={result_ptr}, len={result_len}, mem={})",
                mem_data.len()
            ),
            details: None,
        });
    }

    let output_bytes = &mem_data[result_ptr..result_ptr + result_len];
    let output: Value = serde_json::from_slice(output_bytes).unwrap_or_else(|_| {
        warn!("wasm plugin: output is not valid JSON, wrapping as string");
        json!({ "raw": String::from_utf8_lossy(output_bytes) })
    });

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
pub async fn handle_wasm_plugin(
    _ctx: StepContext,
    _wasm_path: &str,
) -> Result<Value, StepError> {
    Err(StepError::Permanent {
        message: "WASM plugin support is not enabled (compile with --features wasm)".into(),
        details: None,
    })
}

#[cfg(test)]
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
        assert_eq!(parse_plugin_name("wasm://path/to/module"), Some("path/to/module"));
        assert_eq!(parse_plugin_name(""), None);
        assert_eq!(parse_plugin_name("http_request"), None);
    }

    #[test]
    fn is_wasm_handler_edge_cases() {
        assert!(!is_wasm_handler(""));
        assert!(is_wasm_handler("wasm://"));
        assert!(!is_wasm_handler("WASM://plugin"));
    }
}
