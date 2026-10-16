//! Test infrastructure shared by every crate's tests.
//!
//! Compiled for this crate's unit tests and, for other crates (and this
//! crate's `tests/`), behind the `test-util` cargo feature, which only
//! dev-dependencies enable. Nothing here ships in a release build.
//!
//! - [`faulty`]: [`faulty::FaultStorage`], a fault-injecting
//!   `StorageBackend` decorator (T1 HX-1 / T2 H5).
//! - [`fixtures`]: sequence / instance / step builders (T2 H6).
//! - [`backends`]: [`backends::TestBackend`], [`crate::backend_test!`] and
//!   [`backends::postgres_url`] (one body on every backend; per-test
//!   Postgres schemas; missing `DATABASE_URL` fails under CI) (T2 H1).

pub mod backends;
pub mod faulty;
pub mod fixtures;
