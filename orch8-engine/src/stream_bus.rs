//! In-process pub/sub bus for live instance events.
//!
//! Carries incremental events that are NOT part of an instance's durable
//! record — today that is `llm_delta`: token deltas emitted by a streaming
//! `llm_call` step. Producers (step handlers) publish per-instance; consumers
//! (the API's `GET /instances/{id}/stream` SSE endpoint, running in the same
//! process) subscribe per-instance and forward events to connected clients.
//!
//! Design:
//! - One [`tokio::sync::broadcast`] channel per instance, **lazily created on
//!   first subscribe**. Publishing to an instance nobody watches is a cheap
//!   map lookup and a no-op — no channel is allocated.
//! - Capacity-bounded ([`CHANNEL_CAPACITY`]): a slow subscriber lags (drops
//!   oldest events) instead of buffering unboundedly. Deltas are best-effort
//!   by design — the full accumulated text always lands in the step's durable
//!   output.
//! - Channels are dropped when their last subscriber disconnects: a publish
//!   that finds no receivers removes the entry, and every subscribe sweeps
//!   entries whose receiver count reached zero.
//!
//! The bus lives in `orch8-engine` (not the API crate) because handlers are
//! the producers; engine-only deployments (e.g. mobile) compile it unchanged
//! and simply never subscribe, so it stays inert.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, PoisonError};

use serde::Serialize;
use tokio::sync::broadcast;

use orch8_types::ids::InstanceId;

/// Per-instance broadcast capacity. A lagging subscriber loses the oldest
/// deltas (best-effort live view); the durable step output is unaffected.
pub const CHANNEL_CAPACITY: usize = 256;

/// A live (non-durable) event observed while an instance runs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamEvent {
    /// An incremental text delta from a streaming `llm_call` step.
    LlmDelta {
        /// Block id of the `llm_call` step producing the delta.
        block_id: String,
        /// The incremental text fragment (already-accumulated text is not
        /// repeated; concatenating deltas reproduces the full response text).
        delta: String,
    },
}

/// Registry of per-instance broadcast channels. See the module docs for the
/// lifecycle (lazy creation, capacity bound, drop-on-last-unsubscribe).
#[derive(Default)]
pub struct StreamBus {
    channels: Mutex<HashMap<InstanceId, broadcast::Sender<StreamEvent>>>,
}

impl StreamBus {
    /// Lock the registry, recovering from a poisoned lock (the registry holds
    /// only channel handles, so a panicked holder cannot leave it logically
    /// inconsistent).
    fn lock(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<InstanceId, broadcast::Sender<StreamEvent>>> {
        self.channels.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Subscribe to live events for `instance_id`, creating the channel if it
    /// does not exist yet. Also sweeps channels whose subscribers are all gone
    /// (cheap GC keyed to the rare subscribe path).
    pub fn subscribe(&self, instance_id: InstanceId) -> broadcast::Receiver<StreamEvent> {
        let mut map = self.lock();
        map.retain(|_, tx| tx.receiver_count() > 0);
        map.entry(instance_id)
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0)
            .subscribe()
    }

    /// Publish an event to subscribers of `instance_id`. A no-op when nobody
    /// is subscribed; if the last subscriber has gone away, the channel is
    /// dropped here.
    pub fn publish(&self, instance_id: InstanceId, event: StreamEvent) {
        let mut map = self.lock();
        if let Some(tx) = map.get(&instance_id) {
            if tx.send(event).is_err() {
                // No live receivers — drop the channel so the map can't grow
                // with stale entries between subscribes.
                map.remove(&instance_id);
            }
        }
    }

    /// `true` when at least one subscriber is listening for `instance_id`.
    #[must_use]
    pub fn has_subscribers(&self, instance_id: InstanceId) -> bool {
        self.lock()
            .get(&instance_id)
            .is_some_and(|tx| tx.receiver_count() > 0)
    }
}

/// Process-global stream bus. The API layer (same process in `orch8-server`)
/// subscribes here; the `llm_call` handler publishes here.
pub fn stream_bus() -> &'static StreamBus {
    static BUS: OnceLock<StreamBus> = OnceLock::new();
    BUS.get_or_init(StreamBus::default)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delta(block: &str, text: &str) -> StreamEvent {
        StreamEvent::LlmDelta {
            block_id: block.to_string(),
            delta: text.to_string(),
        }
    }

    #[tokio::test]
    async fn publish_reaches_subscriber() {
        let bus = StreamBus::default();
        let id = InstanceId::new();
        let mut rx = bus.subscribe(id);
        bus.publish(id, delta("b1", "hel"));
        bus.publish(id, delta("b1", "lo"));
        assert_eq!(rx.recv().await.unwrap(), delta("b1", "hel"));
        assert_eq!(rx.recv().await.unwrap(), delta("b1", "lo"));
    }

    #[tokio::test]
    async fn publish_without_subscriber_is_noop() {
        let bus = StreamBus::default();
        let id = InstanceId::new();
        bus.publish(id, delta("b", "x"));
        assert!(!bus.has_subscribers(id), "no channel created by publish");
    }

    #[tokio::test]
    async fn channel_dropped_after_last_subscriber_goes_away() {
        let bus = StreamBus::default();
        let id = InstanceId::new();
        let rx = bus.subscribe(id);
        assert!(bus.has_subscribers(id));
        drop(rx);
        assert!(!bus.has_subscribers(id));
        // The next publish observes zero receivers and removes the entry.
        bus.publish(id, delta("b", "x"));
        assert!(bus.lock().get(&id).is_none(), "stale channel removed");
    }

    #[tokio::test]
    async fn events_are_scoped_per_instance() {
        let bus = StreamBus::default();
        let (a, b) = (InstanceId::new(), InstanceId::new());
        let mut rx_a = bus.subscribe(a);
        let mut rx_b = bus.subscribe(b);
        bus.publish(a, delta("blk", "only-a"));
        assert_eq!(rx_a.recv().await.unwrap(), delta("blk", "only-a"));
        assert!(matches!(
            rx_b.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn llm_delta_serializes_with_type_tag() {
        let json = serde_json::to_value(delta("step-1", "hi")).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"type": "llm_delta", "block_id": "step-1", "delta": "hi"})
        );
    }
}
