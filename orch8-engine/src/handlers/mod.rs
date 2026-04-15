pub mod for_each;
pub mod loop_block;
pub mod parallel;
pub mod race;
pub mod router;
pub mod step;
pub mod step_block;
pub mod try_catch;

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
pub struct HandlerRegistry {
    handlers: HashMap<String, StepHandler>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
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

    pub fn get(&self, name: &str) -> Option<&StepHandler> {
        self.handlers.get(name)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.handlers.contains_key(name)
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
}
