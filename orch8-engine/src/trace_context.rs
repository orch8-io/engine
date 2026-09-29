//! W3C trace-context propagation between the control plane and external
//! workers (see `docs/PLACEMENT.md#tracing`).
//!
//! Dispatch stamps a `traceparent` on the per-step context handed to the
//! worker (`task.context.runtime.traceparent`, carried identically over HTTP
//! polls and the gRPC stream's task JSON). With the `otel` feature and an
//! active OpenTelemetry layer it is the dispatching span's own context, so
//! the worker's spans become children of the engine's step span. Otherwise a
//! deterministic context is issued: trace id = instance id, span id derived
//! from the task id — still correlating every worker span of one instance.
//!
//! On completion a worker may echo its own `traceparent` (HTTP header or
//! body field, gRPC metadata); [`completion_span`] parents the server-side
//! completion span on it so the trace continues back into the engine.

use uuid::Uuid;

/// Parsed W3C `traceparent` (version `00`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceParent {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub sampled: bool,
}

fn hex_to<const N: usize>(text: &str) -> Option<[u8; N]> {
    if text.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(text.get(index * 2..index * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

impl TraceParent {
    /// Parse a `traceparent` header value; `None` when malformed or when
    /// the trace/span id is all zeros (invalid per the spec).
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        let mut parts = value.trim().split('-');
        let (version, trace, span, flags) =
            (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
        if version != "00" || parts.next().is_some() {
            return None;
        }
        let trace_id = hex_to::<16>(trace)?;
        let span_id = hex_to::<8>(span)?;
        let flags = hex_to::<1>(flags)?[0];
        if trace_id == [0; 16] || span_id == [0; 8] {
            return None;
        }
        Some(Self {
            trace_id,
            span_id,
            sampled: flags & 1 == 1,
        })
    }

    /// Deterministic fallback: trace id = instance id, span id = the last 8
    /// bytes of the task id.
    #[must_use]
    pub fn for_task(instance_id: Uuid, task_id: Uuid) -> Self {
        let mut span_id = [0u8; 8];
        span_id.copy_from_slice(&task_id.as_bytes()[8..]);
        if span_id == [0; 8] {
            span_id[7] = 1;
        }
        let mut trace_id = *instance_id.as_bytes();
        if trace_id == [0; 16] {
            trace_id[15] = 1;
        }
        Self {
            trace_id,
            span_id,
            sampled: true,
        }
    }

    #[must_use]
    pub fn trace_id_hex(&self) -> String {
        to_hex(&self.trace_id)
    }
}

impl std::fmt::Display for TraceParent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "00-{}-{}-{:02x}",
            to_hex(&self.trace_id),
            to_hex(&self.span_id),
            u8::from(self.sampled)
        )
    }
}

/// `traceparent` for a task dispatched from the current span.
#[must_use]
pub fn dispatch_traceparent(instance_id: Uuid, task_id: Uuid) -> String {
    current_span_traceparent()
        .unwrap_or_else(|| TraceParent::for_task(instance_id, task_id))
        .to_string()
}

#[cfg(feature = "otel")]
fn current_span_traceparent() -> Option<TraceParent> {
    use opentelemetry::trace::TraceContextExt as _;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    let context = tracing::Span::current().context();
    let span = context.span();
    let span_context = span.span_context();
    if !span_context.is_valid() {
        return None;
    }
    Some(TraceParent {
        trace_id: span_context.trace_id().to_bytes(),
        span_id: span_context.span_id().to_bytes(),
        sampled: span_context.is_sampled(),
    })
}

#[cfg(not(feature = "otel"))]
const fn current_span_traceparent() -> Option<TraceParent> {
    None
}

/// Span for a worker completion, parented on the worker's `traceparent`
/// when one was supplied (and OpenTelemetry is active), so the trace runs
/// control plane → worker → back. The parsed trace id is always recorded
/// as the `trace_id` field for log correlation.
#[must_use]
pub fn completion_span(
    task_id: Uuid,
    instance_id: Uuid,
    traceparent: Option<&str>,
) -> tracing::Span {
    let parsed = traceparent.and_then(TraceParent::parse);
    let span = tracing::info_span!(
        "orch8.worker_task.complete",
        task_id = %task_id,
        instance_id = %instance_id,
        trace_id = tracing::field::Empty,
    );
    if let Some(parent) = parsed {
        span.record("trace_id", parent.trace_id_hex().as_str());
        set_remote_parent(&span, parent);
    }
    span
}

#[cfg(feature = "otel")]
fn set_remote_parent(span: &tracing::Span, parent: TraceParent) {
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId, TraceState,
    };
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    let span_context = SpanContext::new(
        TraceId::from_bytes(parent.trace_id),
        SpanId::from_bytes(parent.span_id),
        if parent.sampled {
            TraceFlags::SAMPLED
        } else {
            TraceFlags::default()
        },
        true,
        TraceState::default(),
    );
    let context = opentelemetry::Context::new().with_remote_span_context(span_context);
    let _ = span.set_parent(context);
}

#[cfg(not(feature = "otel"))]
const fn set_remote_parent(_span: &tracing::Span, _parent: TraceParent) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_w3c_example() {
        let value = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let parsed = TraceParent::parse(value).unwrap();
        assert!(parsed.sampled);
        assert_eq!(parsed.to_string(), value);
    }

    #[test]
    fn rejects_malformed_and_zero_ids() {
        for bad in [
            "",
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            "00-zzf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ] {
            assert!(TraceParent::parse(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn fallback_uses_instance_as_trace_id() {
        let instance = Uuid::now_v7();
        let task = Uuid::now_v7();
        let value = dispatch_traceparent(instance, task);
        let parsed = TraceParent::parse(&value).unwrap();
        assert_eq!(parsed.trace_id, *instance.as_bytes());
        assert_eq!(parsed.span_id, task.as_bytes()[8..]);
    }
}
