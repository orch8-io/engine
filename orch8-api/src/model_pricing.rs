//! Static LLM model pricing table (estimates, USD per 1M tokens).
//!
//! The table lives in [`orch8_engine::model_pricing`] so the engine can
//! enforce tenant spend budgets with the same numbers `GET /usage` reports;
//! this module re-exports it for existing API-crate callers.

pub use orch8_engine::model_pricing::*;
