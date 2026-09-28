//! Built-in step handlers available to the embedded mobile engine.
//!
//! The server registers ~30 builtins, many of which make no sense (or are
//! unsafe) inside an app: SMTP/Resend email, LLM and MCP calls that need
//! server-held API keys, blob storage, cross-instance signalling. The mobile
//! engine therefore ships two allow-lists:
//!
//! - [`DEFAULT_BUILTINS`] — pure data / control-flow handlers and
//!   instance-local state. No I/O beyond the local database, so they are
//!   registered on every engine. A host handler registered under the same
//!   name replaces the builtin.
//! - [`OPT_IN_BUILTINS`] — handlers that reach the network. They must be
//!   enabled explicitly with `MobileEngine::enable_builtin`. `http_request`
//!   keeps the engine's SSRF guard (private/loopback targets are refused).
//!
//! Everything else (`email`, `llm_call`, `tool_call`, `mcp_call`, `agent`,
//! `embed`/`memory_*`, `human_review`, `self_modify`, `emit_event`,
//! `send_signal`, `query_instance`, `blob_*`, `wait_for_event`, `jev`,
//! `notify`) is unavailable on-device; place those steps on a server runtime.

use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::handlers::builtin::register_builtins;

/// Registered on every mobile engine.
pub const DEFAULT_BUILTINS: &[&str] = &[
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

/// Available on request via `MobileEngine::enable_builtin`.
pub const OPT_IN_BUILTINS: &[&str] = &["http_request"];

/// A registry pre-populated with the given builtin names (unknown names are
/// ignored; callers validate against the allow-lists first).
pub(crate) fn registry_with(names: &[&str]) -> HandlerRegistry {
    let mut registry = HandlerRegistry::new();
    register_builtins(&mut registry);
    registry.retain_handlers(|name| names.contains(&name));
    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_registry_contains_exactly_the_pure_builtins() {
        let registry = registry_with(DEFAULT_BUILTINS);
        let mut names = registry.handler_names();
        names.sort_unstable();
        let mut expected = DEFAULT_BUILTINS.to_vec();
        expected.sort_unstable();
        assert_eq!(
            names, expected,
            "every default builtin must exist in the engine"
        );
        for forbidden in [
            "email",
            "llm_call",
            "http_request",
            "blob_put",
            "send_signal",
        ] {
            assert!(
                !registry.contains(forbidden),
                "{forbidden} must not be on-device by default"
            );
        }
    }

    #[test]
    fn opt_in_builtins_exist_in_the_engine() {
        let registry = registry_with(OPT_IN_BUILTINS);
        for name in OPT_IN_BUILTINS {
            assert!(registry.contains(name), "{name}");
        }
    }
}
