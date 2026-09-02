use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde::Serialize;
use serde_json::Value;

use crate::{
    Clock, CreateInstanceOptions, Engine, Error, InstanceState, ManualClock, SequenceDefinition,
    SharedClock, Storage,
};

/// Result returned by [`run_sequence_once`].
#[derive(Debug, Serialize)]
pub struct EmbeddedRunResult {
    /// Final durable state observed by the runner.
    pub state: InstanceState,
    /// Complete execution context after the last scheduler tick.
    pub context: crate::ExecutionContext,
    /// Persisted block outputs, in storage order.
    pub outputs: Vec<crate::BlockOutput>,
    /// Number of scheduler passes used.
    pub ticks: u32,
}

/// Execute a sequence in an isolated in-memory engine using built-in handlers.
///
/// The runner uses dry-run mode so handlers that could perform external effects
/// return their normal dry-run output. Virtual time advances across delays and
/// retry backoffs. A workflow that waits for human input or an external event
/// returns in `Waiting` state instead of blocking the host process.
pub async fn run_sequence_once(
    sequence: SequenceDefinition,
    input: Value,
    max_ticks: u32,
) -> Result<EmbeddedRunResult, Error> {
    sequence
        .validate()
        .map_err(|error| Error::InvalidSequence(error.to_string()))?;

    let now = Utc::now();
    let clock = Arc::new(ManualClock::new(now));
    let engine = Engine::builder()
        .storage(Storage::sqlite_in_memory())
        .tenant(sequence.tenant_id.as_str())
        .clock(SharedClock::from_arc(
            Arc::clone(&clock) as Arc<dyn crate::Clock>
        ))
        .build()
        .await?;
    let sequence_id = engine.upsert_sequence(sequence.clone()).await?;
    let mut context = crate::ExecutionContext {
        data: input,
        ..Default::default()
    };
    context.runtime.dry_run = true;
    context.runtime.dry_run_auto_approve = true;
    let instance_id = engine
        .create_instance(
            sequence_id,
            CreateInstanceOptions {
                namespace: sequence.namespace,
                context,
                next_fire_at: Some(now),
                ..Default::default()
            },
        )
        .await?;

    let max_ticks = max_ticks.clamp(1, 100_000);
    for ticks in 1..=max_ticks {
        let tick = engine.tick_once().await?;
        let instance = engine.get_instance(instance_id).await?;
        if instance.state.is_terminal() || instance.state == InstanceState::Waiting {
            return Ok(EmbeddedRunResult {
                state: instance.state,
                context: instance.context,
                outputs: engine.block_outputs(instance_id).await?,
                ticks,
            });
        }

        if tick.steps_executed == 0 && tick.instances_advanced == 0 {
            if let Some(next_fire_at) = instance.next_fire_at
                && next_fire_at > clock.now()
            {
                clock.set(next_fire_at);
            } else {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }

    Err(Error::Config(format!(
        "embedded execution did not settle within {max_ticks} scheduler ticks"
    )))
}
