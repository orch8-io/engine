//! In-process step-log capture.
//!
//! Handler invocation is wrapped in an `orch8.step` span carrying `instance_id`
//! and `block_id`. [`StepLogLayer`] is a `tracing` layer that buffers every log
//! event emitted inside such a span and, when the span closes (the handler
//! future resolves), persists the buffer to `step_logs` via a process-global
//! storage sink. Combined with worker-reported logs, this gives a per-step Logs
//! view without any handler-side instrumentation.

use std::sync::{Arc, OnceLock};

use chrono::Utc;
use tracing::field::{Field, Visit};
use tracing::span::Attributes;
use tracing::{Event, Id, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

use orch8_storage::StorageBackend;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::step_log::StepLogEntry;

const STEP_SPAN_NAME: &str = "orch8.step";

static SINK: OnceLock<Arc<dyn StorageBackend>> = OnceLock::new();

/// Wire the step-log sink. In-process handler logs are persisted here when their
/// `orch8.step` span closes. Idempotent — only the first call takes effect.
pub fn init_step_log_sink(storage: Arc<dyn StorageBackend>) {
    let _ = SINK.set(storage);
}

/// Per-step buffer stored in the span's extensions.
struct StepLogBuf {
    instance_id: String,
    block_id: String,
    entries: Vec<StepLogEntry>,
}

/// Extracts `instance_id` / `block_id` from the `orch8.step` span fields.
#[derive(Default)]
struct SpanIdentVisitor {
    instance_id: Option<String>,
    block_id: Option<String>,
}

impl Visit for SpanIdentVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "instance_id" => {
                self.instance_id = Some(format!("{value:?}").trim_matches('"').to_string());
            }
            "block_id" => self.block_id = Some(format!("{value:?}").trim_matches('"').to_string()),
            _ => {}
        }
    }
}

/// Extracts the `message` of a log event.
#[derive(Default)]
struct MessageVisitor {
    message: Option<String>,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{value:?}"));
        }
    }
}

/// Tracing layer that captures log events scoped to an `orch8.step` span.
pub struct StepLogLayer;

impl<S> Layer<S> for StepLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        if span.name() != STEP_SPAN_NAME {
            return;
        }
        let mut v = SpanIdentVisitor::default();
        attrs.record(&mut v);
        // Only buffer steps we can attribute to an instance + block.
        if let (Some(instance_id), Some(block_id)) = (v.instance_id, v.block_id) {
            span.extensions_mut().insert(StepLogBuf {
                instance_id,
                block_id,
                entries: Vec::new(),
            });
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // Walk from the event's span outward to the nearest orch8.step span.
        let Some(scope) = ctx.event_scope(event) else {
            return;
        };
        for span in scope {
            let mut ext = span.extensions_mut();
            if let Some(buf) = ext.get_mut::<StepLogBuf>() {
                let mut mv = MessageVisitor::default();
                event.record(&mut mv);
                let message = mv.message.unwrap_or_default();
                if !message.is_empty() {
                    buf.entries.push(StepLogEntry {
                        ts: Utc::now(),
                        level: event.metadata().level().to_string().to_lowercase(),
                        message,
                    });
                }
                return; // nearest step only
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let buf = span.extensions_mut().remove::<StepLogBuf>();
        let Some(buf) = buf else { return };
        if buf.entries.is_empty() {
            return;
        }
        let Some(storage) = SINK.get() else { return };
        // Persist off the closing path. Requires a tokio runtime — handler spans
        // always close inside one; skip otherwise.
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let storage = Arc::clone(storage);
        tokio::spawn(async move {
            let Ok(uuid) = uuid::Uuid::parse_str(&buf.instance_id) else {
                return;
            };
            let instance_id = InstanceId::from_uuid(uuid);
            let block_id = BlockId::new(buf.block_id);
            if let Err(e) = storage
                .append_step_logs(instance_id, &block_id, &buf.entries)
                .await
            {
                tracing::debug!(error = %e, "failed to persist step logs");
            }
        });
    }
}
