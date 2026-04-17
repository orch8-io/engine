pub mod ab_split;
pub mod activepieces;
pub mod builtin;
pub mod cancellation_scope;
pub mod for_each;
pub mod grpc_plugin;
pub mod human_review;
pub mod llm;
pub mod loop_block;
pub mod parallel;
pub mod query_instance;
pub mod race;
pub mod router;
pub mod self_modify;
pub mod step;
pub mod step_block;
pub mod tool_call;
pub mod try_catch;
pub mod wasm_plugin;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use orch8_types::context::ExecutionContext;
use orch8_types::error::StepError;
use orch8_types::ids::{BlockId, InstanceId};

/// Step handlers are plain async functions. No activity ceremony.
pub type StepHandler = Box<
    dyn Fn(
            StepContext,
        ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, StepError>> + Send>>
        + Send
        + Sync,
>;

/// Context passed to step handlers during execution.
pub struct StepContext {
    pub instance_id: InstanceId,
    pub block_id: BlockId,
    pub params: serde_json::Value,
    pub context: ExecutionContext,
    pub attempt: u32,
}

/// Registry of named step handlers.
///
/// Supports mock overrides for testing: when a mock is registered for a handler
/// name, it takes precedence over the real handler.
pub struct HandlerRegistry {
    handlers: HashMap<String, StepHandler>,
    /// Mock overrides. When present, `get()` returns the mock instead of the real handler.
    mocks: HashMap<String, StepHandler>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            mocks: HashMap::new(),
        }
    }

    /// Register a step handler by name.
    pub fn register<F, Fut>(&mut self, name: &str, handler: F)
    where
        F: Fn(StepContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<serde_json::Value, StepError>> + Send + 'static,
    {
        self.handlers.insert(
            name.to_string(),
            Box::new(move |ctx| Box::pin(handler(ctx))),
        );
    }

    /// Look up a handler by name. Mocks take precedence over real handlers.
    pub fn get(&self, name: &str) -> Option<&StepHandler> {
        self.mocks.get(name).or_else(|| self.handlers.get(name))
    }

    pub fn contains(&self, name: &str) -> bool {
        self.mocks.contains_key(name) || self.handlers.contains_key(name)
    }

    /// Install a mock handler that always returns a fixed JSON value.
    /// The mock takes precedence over any real handler with the same name.
    pub fn set_mock(&mut self, name: &str, response: serde_json::Value) {
        self.mocks.insert(
            name.to_string(),
            Box::new(move |_ctx| {
                let val = response.clone();
                Box::pin(async move { Ok(val) })
            }),
        );
    }

    /// Install a mock handler that always fails with a given error.
    /// If `retryable` is true, returns `StepError::Retryable`; otherwise `StepError::Permanent`.
    pub fn set_mock_error(&mut self, name: &str, message: String, retryable: bool) {
        self.mocks.insert(
            name.to_string(),
            Box::new(move |_ctx| {
                let msg = message.clone();
                Box::pin(async move {
                    if retryable {
                        Err(StepError::Retryable {
                            message: msg,
                            details: None,
                        })
                    } else {
                        Err(StepError::Permanent {
                            message: msg,
                            details: None,
                        })
                    }
                })
            }),
        );
    }

    /// Install a mock handler with a custom function (for stateful mocks, delays, etc.).
    pub fn set_mock_fn<F, Fut>(&mut self, name: &str, handler: F)
    where
        F: Fn(StepContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<serde_json::Value, StepError>> + Send + 'static,
    {
        self.mocks.insert(
            name.to_string(),
            Box::new(move |ctx| Box::pin(handler(ctx))),
        );
    }

    /// Remove a single mock, restoring the real handler.
    pub fn clear_mock(&mut self, name: &str) {
        self.mocks.remove(name);
    }

    /// Remove all mocks, restoring all real handlers.
    pub fn clear_all_mocks(&mut self) {
        self.mocks.clear();
    }

    /// Returns true if any mocks are installed.
    pub fn has_mocks(&self) -> bool {
        !self.mocks.is_empty()
    }
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_and_invoke_handler() {
        let mut registry = HandlerRegistry::new();
        registry.register("test_step", |_ctx| async {
            Ok(serde_json::json!({"result": "ok"}))
        });

        let handler = registry.get("test_step").unwrap();
        let ctx = StepContext {
            instance_id: InstanceId::new(),
            block_id: BlockId("step_1".into()),
            params: serde_json::Value::Null,
            context: ExecutionContext::default(),
            attempt: 0,
        };
        let result = handler(ctx).await.unwrap();
        assert_eq!(result, serde_json::json!({"result": "ok"}));
    }

    #[test]
    fn missing_handler_returns_none() {
        let registry = HandlerRegistry::new();
        assert!(registry.get("nonexistent").is_none());
    }

    #[tokio::test]
    async fn mock_overrides_real_handler() {
        let mut registry = HandlerRegistry::new();
        registry.register("my_step", |_ctx| async {
            Ok(serde_json::json!({"source": "real"}))
        });

        // Without mock, real handler runs.
        let ctx = StepContext {
            instance_id: InstanceId::new(),
            block_id: BlockId("s".into()),
            params: serde_json::Value::Null,
            context: ExecutionContext::default(),
            attempt: 0,
        };
        let result = registry.get("my_step").unwrap()(ctx).await.unwrap();
        assert_eq!(result["source"], "real");

        // Install mock.
        registry.set_mock("my_step", serde_json::json!({"source": "mock"}));
        let ctx = StepContext {
            instance_id: InstanceId::new(),
            block_id: BlockId("s".into()),
            params: serde_json::Value::Null,
            context: ExecutionContext::default(),
            attempt: 0,
        };
        let result = registry.get("my_step").unwrap()(ctx).await.unwrap();
        assert_eq!(result["source"], "mock");

        // Clear mock, real handler restored.
        registry.clear_mock("my_step");
        let ctx = StepContext {
            instance_id: InstanceId::new(),
            block_id: BlockId("s".into()),
            params: serde_json::Value::Null,
            context: ExecutionContext::default(),
            attempt: 0,
        };
        let result = registry.get("my_step").unwrap()(ctx).await.unwrap();
        assert_eq!(result["source"], "real");
    }

    #[tokio::test]
    async fn mock_error_handler() {
        let mut registry = HandlerRegistry::new();
        registry.set_mock_error("failing_step", "test error".into(), true);

        let ctx = StepContext {
            instance_id: InstanceId::new(),
            block_id: BlockId("s".into()),
            params: serde_json::Value::Null,
            context: ExecutionContext::default(),
            attempt: 0,
        };
        let result = registry.get("failing_step").unwrap()(ctx).await;
        assert!(result.is_err());
    }

    #[test]
    fn has_mocks_tracking() {
        let mut registry = HandlerRegistry::new();
        assert!(!registry.has_mocks());

        registry.set_mock("step_a", serde_json::json!({}));
        assert!(registry.has_mocks());

        registry.clear_all_mocks();
        assert!(!registry.has_mocks());
    }
}
