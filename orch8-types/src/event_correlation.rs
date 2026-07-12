//! Durable event correlation: inbox envelopes, wait registrations, and
//! join policies.
//!
//! Long-lived processes spend most of their time waiting for external
//! facts: payment confirmations, document arrivals, callbacks. Events
//! are ingested into a tenant-scoped inbox with producer-id
//! deduplication; `wait_for_event` blocks register waits keyed by
//! `(tenant, event names, correlation key)` and resume when the join
//! policy is satisfied — whether the events arrived before or after the
//! workflow reached the block.
//!
//! Consumption semantics: **one consumer** — an event is consumed by at
//! most one waiting block (fan-out is explicitly out of scope for v1).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// Inbox lifecycle of an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventStatus {
    /// Waiting to be matched.
    Pending,
    /// Consumed by exactly one waiting block.
    Consumed,
    /// Retention expired before any consumer matched.
    Expired,
}

impl EventStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Consumed => "consumed",
            Self::Expired => "expired",
        }
    }
}

impl std::str::FromStr for EventStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "consumed" => Ok(Self::Consumed),
            "expired" => Ok(Self::Expired),
            other => Err(format!("unknown event status: {other}")),
        }
    }
}

/// One ingested event. Canonical identity is
/// `(tenant_id, event_name, producer_event_id)` — re-sending the same
/// producer id is a safe no-op.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct EventEnvelope {
    pub id: Uuid,
    pub tenant_id: String,
    pub event_name: String,
    /// The producer's idempotency identity for this event.
    pub producer_event_id: String,
    /// Tenant-scoped business correlation key (order id, customer id...).
    pub correlation_key: String,
    #[serde(default)]
    pub payload: serde_json::Value,
    pub status: EventStatus,
    /// The instance whose wait consumed this event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumed_by: Option<Uuid>,
    pub received_at: DateTime<Utc>,
}

/// Join policies for multi-event waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum JoinMode {
    /// Resume on the first matching event.
    Any,
    /// Resume once every named event has arrived (in any order).
    All,
    /// Resume once `count` matching events arrived.
    Count { count: u32 },
}

impl Default for JoinMode {
    fn default() -> Self {
        Self::All
    }
}

/// Wait registration lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WaitStatus {
    Waiting,
    Satisfied,
    Cancelled,
}

impl WaitStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Satisfied => "satisfied",
            Self::Cancelled => "cancelled",
        }
    }
}

impl std::str::FromStr for WaitStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "waiting" => Ok(Self::Waiting),
            "satisfied" => Ok(Self::Satisfied),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(format!("unknown wait status: {other}")),
        }
    }
}

/// A registered `wait_for_event` block instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct EventWait {
    pub id: Uuid,
    pub tenant_id: String,
    pub instance_id: Uuid,
    pub block_id: String,
    /// Event names this wait listens for.
    pub event_names: Vec<String>,
    pub correlation_key: String,
    pub join_mode: JoinMode,
    pub status: WaitStatus,
    /// Names of events matched so far (deduplicated).
    #[serde(default)]
    pub matched_names: Vec<String>,
    /// Envelope ids consumed by this wait so far.
    #[serde(default)]
    pub matched_event_ids: Vec<Uuid>,
    pub created_at: DateTime<Utc>,
}

impl EventWait {
    /// Whether this wait listens for `event_name`.
    #[must_use]
    pub fn listens_for(&self, event_name: &str) -> bool {
        self.event_names.iter().any(|n| n == event_name)
    }

    /// Is the join policy satisfied by the matched set?
    #[must_use]
    pub fn is_satisfied(&self) -> bool {
        match self.join_mode {
            JoinMode::Any => !self.matched_event_ids.is_empty(),
            JoinMode::All => self
                .event_names
                .iter()
                .all(|n| self.matched_names.iter().any(|m| m == n)),
            JoinMode::Count { count } => self.matched_event_ids.len() >= count as usize,
        }
    }

    /// Record a match. Returns `false` when the event adds nothing (e.g.
    /// a second `payment_received` for an `all` join that already has
    /// one — the event stays pending for other consumers).
    pub fn record_match(&mut self, event: &EventEnvelope) -> bool {
        if !self.listens_for(&event.event_name) {
            return false;
        }
        if self.matched_event_ids.contains(&event.id) {
            return false;
        }
        match self.join_mode {
            // For `all`, a name only counts once.
            JoinMode::All => {
                if self.matched_names.iter().any(|m| m == &event.event_name) {
                    return false;
                }
            }
            JoinMode::Any | JoinMode::Count { .. } => {}
        }
        self.matched_event_ids.push(event.id);
        if !self.matched_names.iter().any(|m| m == &event.event_name) {
            self.matched_names.push(event.event_name.clone());
        }
        true
    }
}

/// Outcome of ingesting one event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct IngestOutcome {
    pub event_id: Uuid,
    /// True when the producer id was already known — nothing changed.
    pub duplicate: bool,
    /// The wait (if any) this event was matched into.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_wait: Option<Uuid>,
    /// True when the match satisfied the wait's join and resumed the
    /// waiting instance.
    #[serde(default)]
    pub satisfied: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(name: &str) -> EventEnvelope {
        EventEnvelope {
            id: Uuid::now_v7(),
            tenant_id: "t1".into(),
            event_name: name.into(),
            producer_event_id: Uuid::now_v7().to_string(),
            correlation_key: "order-1".into(),
            payload: serde_json::json!({}),
            status: EventStatus::Pending,
            consumed_by: None,
            received_at: Utc::now(),
        }
    }

    fn wait(names: &[&str], mode: JoinMode) -> EventWait {
        EventWait {
            id: Uuid::now_v7(),
            tenant_id: "t1".into(),
            instance_id: Uuid::now_v7(),
            block_id: "wait".into(),
            event_names: names.iter().map(ToString::to_string).collect(),
            correlation_key: "order-1".into(),
            join_mode: mode,
            status: WaitStatus::Waiting,
            matched_names: vec![],
            matched_event_ids: vec![],
            created_at: Utc::now(),
        }
    }

    #[test]
    fn any_join_satisfied_by_first_match() {
        let mut w = wait(&["a", "b"], JoinMode::Any);
        assert!(!w.is_satisfied());
        assert!(w.record_match(&event("b")));
        assert!(w.is_satisfied());
    }

    #[test]
    fn all_join_requires_every_name_in_any_order() {
        let mut w = wait(&["payment_received", "inventory_reserved"], JoinMode::All);
        assert!(w.record_match(&event("inventory_reserved")));
        assert!(!w.is_satisfied());
        assert!(w.record_match(&event("payment_received")));
        assert!(w.is_satisfied());
    }

    #[test]
    fn all_join_ignores_repeat_names() {
        let mut w = wait(&["a", "b"], JoinMode::All);
        assert!(w.record_match(&event("a")));
        // Second 'a' adds nothing — it must stay available for others.
        assert!(!w.record_match(&event("a")));
        assert!(!w.is_satisfied());
        assert_eq!(w.matched_event_ids.len(), 1);
    }

    #[test]
    fn count_join_counts_matching_events() {
        let mut w = wait(&["reading"], JoinMode::Count { count: 3 });
        for i in 0..3 {
            assert!(!w.is_satisfied(), "not yet at {i}");
            assert!(w.record_match(&event("reading")));
        }
        assert!(w.is_satisfied());
    }

    #[test]
    fn unlistened_events_do_not_match() {
        let mut w = wait(&["a"], JoinMode::Any);
        assert!(!w.record_match(&event("unrelated")));
        assert!(!w.is_satisfied());
    }

    #[test]
    fn same_event_id_never_double_counts() {
        let mut w = wait(&["reading"], JoinMode::Count { count: 2 });
        let e = event("reading");
        assert!(w.record_match(&e));
        assert!(!w.record_match(&e));
        assert!(!w.is_satisfied());
    }

    #[test]
    fn join_mode_serde_shapes() {
        assert_eq!(serde_json::to_string(&JoinMode::Any).unwrap(), r#"{"mode":"any"}"#);
        assert_eq!(
            serde_json::to_string(&JoinMode::Count { count: 2 }).unwrap(),
            r#"{"mode":"count","count":2}"#
        );
        let m: JoinMode = serde_json::from_str(r#"{"mode":"all"}"#).unwrap();
        assert_eq!(m, JoinMode::All);
    }

    #[test]
    fn envelope_round_trips() {
        let e = event("payment_received");
        let back: EventEnvelope = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        assert_eq!(back, e);
    }
}
