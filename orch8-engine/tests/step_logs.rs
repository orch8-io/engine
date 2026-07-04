//! In-process step-log capture: events emitted inside an `orch8.step` span are
//! buffered and persisted to `step_logs` when the span closes.

use std::sync::Arc;
use std::time::Duration;

use orch8_engine::step_logs::{StepLogLayer, init_step_log_sink};
use orch8_storage::{StorageBackend, sqlite::SqliteStorage};
use orch8_types::ids::InstanceId;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::test]
async fn captures_handler_logs_scoped_to_step_span() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    init_step_log_sink(Arc::clone(&storage));

    let instance_id = InstanceId::new();

    // Install the layer for the duration of this test (thread-scoped).
    let _guard = tracing_subscriber::registry()
        .with(StepLogLayer)
        .set_default();

    {
        // Mirror the engine's `orch8.step` span shape.
        let span = tracing::info_span!(
            "orch8.step",
            instance_id = %instance_id,
            block_id = "step_a",
            handler = "noop",
            tenant_id = "t",
            attempt = 0u32,
        );
        let _e = span.enter();
        tracing::info!("handler started");
        tracing::warn!("something noteworthy");
        // An event outside this step (no span) must not be captured here.
    }
    // Event emitted with no orch8.step span — must not be attributed.
    tracing::info!("not in a step");

    // on_close spawns the persist task; give it a moment.
    for _ in 0..50 {
        let logs = storage.list_step_logs(instance_id).await.unwrap();
        if logs.len() >= 2 {
            assert!(logs.iter().all(|l| l.block_id == "step_a"));
            assert!(
                logs.iter()
                    .any(|l| l.message.contains("handler started") && l.level == "info")
            );
            assert!(
                logs.iter()
                    .any(|l| l.message.contains("something noteworthy") && l.level == "warn")
            );
            assert_eq!(logs.len(), 2, "the out-of-span event must not be captured");
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("step logs were not persisted");
}
