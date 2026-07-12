//! # orch8 — durable execution as a Rust library
//!
//! Embed the Orch8 workflow engine directly in your application: workflows
//! survive process crashes and restarts because every state transition is
//! persisted to `SQLite` or `PostgreSQL` before it takes effect. No separate
//! server, no sidecar — just a library on your own tokio runtime.
//!
//! ## Quickstart
//!
//! ```no_run
//! use std::time::Duration;
//!
//! const SEQ_JSON: &str = r#"{
//!     "id": "0195fdc0-0000-7000-8000-000000000001",
//!     "tenant_id": "default",
//!     "namespace": "default",
//!     "name": "payment",
//!     "version": 1,
//!     "blocks": [
//!         { "type": "step", "id": "charge", "handler": "charge_card", "params": {} }
//!     ],
//!     "created_at": "2026-01-01T00:00:00Z"
//! }"#;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let engine = orch8::Engine::builder()
//!     .storage(orch8::Storage::sqlite("app.db")) // or ::sqlite_in_memory(), ::postgres(url)
//!     .handler("charge_card", |_ctx: orch8::StepContext| async move {
//!         Ok(serde_json::json!({ "charged": true }))
//!     })
//!     .build()
//!     .await?;
//!
//! engine.start(); // background tick loop on the host's tokio runtime
//!
//! let seq_id = engine.upsert_sequence(serde_json::from_str(SEQ_JSON)?).await?;
//! let inst = engine.create_instance(seq_id, Default::default()).await?;
//!
//! // ... later: poll progress, send signals, list instances ...
//! let instance = engine.get_instance(inst).await?;
//! println!("state: {:?}", instance.state);
//!
//! engine.shutdown().await; // graceful: stop ticking, drain in-flight steps
//! # Ok(())
//! # }
//! ```
//!
//! ## Host runtime requirement
//!
//! The engine runs on **your** tokio runtime. [`EngineBuilder::build`] is an
//! `async fn` and [`Engine::start`] spawns a task with [`tokio::spawn`], so
//! both must be called from within a tokio runtime context (e.g. inside
//! `#[tokio::main]`). The engine never creates its own runtime — if you need
//! that (e.g. for FFI hosts), see the `orch8-mobile` crate instead.
//!
//! ## Storage choices
//!
//! | Constructor | Backing | Use case |
//! |---|---|---|
//! | [`Storage::sqlite`] | file-backed `SQLite` (WAL) | single-process apps, durable across restarts |
//! | [`Storage::sqlite_in_memory`] | in-memory `SQLite` | tests, ephemeral workloads |
//! | [`Storage::postgres`] | `PostgreSQL` | multi-node deployments, larger scale |
//!
//! `SQLite` schema setup is bundled and applied automatically on build;
//! `PostgreSQL` runs the same migrations the `orch8-server` binary applies at
//! startup.
//!
//! ## Driving the engine
//!
//! Two modes:
//!
//! - **Background loop**: [`Engine::start`] spawns the scheduler tick loop;
//!   [`Engine::shutdown`] cancels it and drains in-flight steps.
//! - **Manual ticking**: call [`Engine::tick_once`] yourself — useful for
//!   test harnesses and hosts that control their own cadence. Don't mix the
//!   two concurrently.
//!
//! ## Stability
//!
//! This crate is a **pre-1.0 facade**: it re-exports a curated, intentionally
//! small subset of the internal crates so internals can evolve behind it.
//! Expect breaking changes between minor versions until 1.0.

mod builder;
pub mod contract;
mod engine;
mod error;
mod storage;

pub use builder::EngineBuilder;
pub use engine::{CreateInstanceOptions, Engine};
pub use error::Error;
pub use storage::Storage;

// ---------------------------------------------------------------------------
// Curated re-exports. These are the only internal types that are part of the
// public embedding surface; everything else in orch8-engine / orch8-storage /
// orch8-types is free to churn without affecting embedders.
// ---------------------------------------------------------------------------

/// Context passed to step handlers during execution (params, instance
/// context, attempt counter, storage handle).
pub use orch8_engine::handlers::StepContext;
/// Result of a single manual tick (see [`Engine::tick_once`]).
pub use orch8_engine::scheduler::TickOnceResult as TickResult;
/// Virtual time: the scheduler reads "now" through a [`Clock`]. Inject a
/// [`ManualClock`] via [`EngineBuilder::clock`] to fast-forward over delays,
/// send windows and retry backoffs (see `orch8-types/src/clock.rs`).
pub use orch8_types::clock::{Clock, ManualClock, SharedClock, SystemClock};
/// Per-instance execution context (`data`, `config`, audit trail).
pub use orch8_types::context::ExecutionContext;
/// Error type returned by step handlers — `Retryable` errors are retried per
/// the step's retry policy, `Permanent` errors fail the step immediately.
pub use orch8_types::error::StepError;
/// Filter for [`Engine::list_instances`].
pub use orch8_types::filter::InstanceFilter;
pub use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
pub use orch8_types::instance::{Budget, InstanceState, Priority, TaskInstance};
/// One persisted step result (see [`Engine::block_outputs`]).
pub use orch8_types::output::BlockOutput;
/// Workflow definition types: a sequence is a list of blocks (steps,
/// parallel groups, loops, routers, ...). Usually deserialized from JSON.
pub use orch8_types::sequence::{BlockDefinition, SequenceDefinition};
/// Signal types for [`Engine::send_signal`] (`Pause`, `Resume`, `Cancel`,
/// `UpdateContext`, or `Custom`).
pub use orch8_types::signal::SignalType;
