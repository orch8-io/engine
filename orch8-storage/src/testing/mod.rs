//! Test infrastructure shared by every crate's tests.
//!
//! Compiled for this crate's unit tests and, for other crates (and this
//! crate's `tests/`), behind the `test-util` cargo feature, which only
//! dev-dependencies enable. Nothing here ships in a release build.
//!
//! - [`faulty`]: [`faulty::FaultStorage`], a fault-injecting
//!   `StorageBackend` decorator (T1 HX-1 / T2 H5).
//! - [`fixtures`]: sequence / instance / step builders (T2 H6).

pub mod faulty;
pub mod fixtures;
