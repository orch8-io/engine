//! Shared backend harness for the storage test suites.
//!
//! The behavioural suites (`storage_integration.rs`, `storage_coverage.rs`)
//! are written once against [`TestStore`] and expanded by
//! [`for_each_backend!`] into one `#[tokio::test]` per backend:
//!
//! * `sqlite::<name>` — always runs, against a fresh in-memory database;
//! * `postgres::<name>` — runs when `DATABASE_URL` is set (CI always sets
//!   it), against a freshly created, freshly migrated schema that is unique
//!   to the test so suites can run concurrently without interfering. Without
//!   `DATABASE_URL` the test prints a skip notice and returns, mirroring
//!   `postgres_integration.rs`.
//!
//! # Foreign-key parents
//!
//! Postgres declares `task_instances.sequence_id REFERENCES sequences(id)`;
//! the bundled SQLite schema does not (SQLite cannot add a constraint to an
//! existing table, and the FK was never part of the shipped schema). The
//! suites historically leaned on that gap by creating instances against
//! `SequenceId::new()` without a parent row. To keep the shared tests valid
//! on both backends, [`TestStore`] exposes *inherent* `create_instance*`
//! methods that seed a stub `sequences` row for any unknown `sequence_id`
//! before delegating to the trait method. Inherent methods win method
//! resolution over the `Deref`-reached trait methods, so test bodies need no
//! edits. The divergence itself is documented in
//! `docs/STORAGE_BACKEND_CONFORMANCE.md`.
//!
//! # Schema lifecycle (Postgres)
//!
//! Schemas are named `orch8t_<unix-secs>_<pid>_<n>`. The generated Postgres
//! wrapper drops the schema after a passing test; a panicking test leaves it
//! behind, and the next process to call [`postgres`] sweeps schemas whose
//! timestamp is older than [`STALE_SCHEMA_SECS`], so crashed runs cannot
//! accumulate forever while a concurrently running binary is left alone.

// Shared across several test binaries; not every binary uses every helper.
#![allow(dead_code)]

use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::Utc;
use sqlx::Connection;

use orch8_storage::postgres::PostgresStorage;
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::StorageBackend;
use orch8_types::error::StorageError;
use orch8_types::ids::SequenceId;
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::{SequenceDefinition, SequenceStatus};

/// Schemas older than this (by the timestamp embedded in their name) are
/// considered leftovers of a crashed run and are dropped on the next setup.
const STALE_SCHEMA_SECS: u64 = 30 * 60;

const SCHEMA_PREFIX: &str = "orch8t_";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    Sqlite,
    Postgres,
}

impl BackendKind {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
        }
    }
}

/// A backend under test. Dereferences to `dyn StorageBackend`, so every
/// trait method is callable directly on it.
pub struct TestStore {
    inner: Arc<dyn StorageBackend>,
    kind: BackendKind,
    /// `(DATABASE_URL, schema)` for Postgres so `teardown` and `fresh` can
    /// reach the same database.
    postgres: Option<(String, String)>,
}

impl Deref for TestStore {
    type Target = dyn StorageBackend;

    fn deref(&self) -> &Self::Target {
        &*self.inner
    }
}

impl TestStore {
    #[must_use]
    pub fn kind(&self) -> BackendKind {
        self.kind
    }

    #[must_use]
    pub fn is_sqlite(&self) -> bool {
        self.kind == BackendKind::Sqlite
    }

    /// Shared handle to the underlying backend (e.g. for spawning tasks).
    #[must_use]
    pub fn as_arc(&self) -> Arc<dyn StorageBackend> {
        Arc::clone(&self.inner)
    }

    /// A second, fully independent store of the same backend kind (a new
    /// in-memory database / a new Postgres schema).
    pub async fn fresh(&self) -> TestStore {
        match self.kind {
            BackendKind::Sqlite => sqlite().await,
            BackendKind::Postgres => postgres()
                .await
                .expect("DATABASE_URL was set when the first store was created"),
        }
    }

    /// Future that drops this store's Postgres schema (no-op for SQLite).
    /// Returned as a detached future so the generated wrapper can run it
    /// *after* the test body has consumed `self`.
    #[must_use]
    pub fn teardown(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let target = self.postgres.clone();
        async move {
            if let Some((url, schema)) = target {
                drop_schema(&url, &schema).await;
            }
        }
    }

    /// Seed a stub `sequences` row for `sequence_id` if none exists, so an
    /// instance referencing it satisfies Postgres' FK (see module docs).
    async fn ensure_sequence(&self, inst: &TaskInstance) -> Result<(), StorageError> {
        if self.inner.get_sequence(inst.sequence_id).await?.is_some() {
            return Ok(());
        }
        match self.inner.create_sequence(&stub_sequence(inst)).await {
            Ok(()) | Err(StorageError::Conflict(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Like [`InstanceStore::create_instance`], after seeding the FK parent.
    pub async fn create_instance(&self, inst: &TaskInstance) -> Result<(), StorageError> {
        self.ensure_sequence(inst).await?;
        self.inner.create_instance(inst).await
    }

    /// Like [`InstanceStore::create_instances_batch`], after seeding FK parents.
    pub async fn create_instances_batch(
        &self,
        instances: &[TaskInstance],
    ) -> Result<u64, StorageError> {
        for inst in instances {
            self.ensure_sequence(inst).await?;
        }
        self.inner.create_instances_batch(instances).await
    }

    /// Like [`InstanceStore::create_instance_externalized`], after seeding
    /// the FK parent.
    pub async fn create_instance_externalized(
        &self,
        inst: &TaskInstance,
        threshold_bytes: u32,
    ) -> Result<(), StorageError> {
        self.ensure_sequence(inst).await?;
        self.inner
            .create_instance_externalized(inst, threshold_bytes)
            .await
    }

    /// Like [`InstanceStore::create_instances_batch_externalized`], after
    /// seeding FK parents.
    pub async fn create_instances_batch_externalized(
        &self,
        instances: &[TaskInstance],
        threshold_bytes: u32,
    ) -> Result<u64, StorageError> {
        for inst in instances {
            self.ensure_sequence(inst).await?;
        }
        self.inner
            .create_instances_batch_externalized(instances, threshold_bytes)
            .await
    }
}

fn stub_sequence(inst: &TaskInstance) -> SequenceDefinition {
    SequenceDefinition {
        id: inst.sequence_id,
        tenant_id: inst.tenant_id.clone(),
        namespace: inst.namespace.clone(),
        name: format!("fk-stub-{}", inst.sequence_id),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    }
}

/// A stub sequence definition for `id` under `tenant` (for tests that need
/// an explicit FK parent but do not care about its contents).
#[must_use]
pub fn stub_sequence_for(tenant: &str, id: SequenceId) -> SequenceDefinition {
    SequenceDefinition {
        id,
        tenant_id: orch8_types::ids::TenantId::unchecked(tenant),
        namespace: orch8_types::ids::Namespace::new("default"),
        name: format!("fk-stub-{id}"),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    }
}

/// Raw in-memory `SqliteStorage` for the few tests that need `pool()` to
/// run SQLite-dialect SQL directly. Such tests are SQLite-only by nature.
pub async fn sqlite_storage() -> SqliteStorage {
    SqliteStorage::in_memory()
        .await
        .expect("in-memory sqlite must open")
}

/// Fresh in-memory SQLite store.
pub async fn sqlite() -> TestStore {
    let storage = SqliteStorage::in_memory()
        .await
        .expect("in-memory sqlite must open");
    TestStore {
        inner: Arc::new(storage),
        kind: BackendKind::Sqlite,
        postgres: None,
    }
}

static SCHEMA_COUNTER: AtomicU32 = AtomicU32::new(0);
static SWEPT: AtomicBool = AtomicBool::new(false);

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Fresh Postgres store in a uniquely named, freshly migrated schema, or
/// `None` when `DATABASE_URL` is not set.
///
/// # Panics
/// Panics if `DATABASE_URL` is set but the database is unreachable or the
/// migrations fail — a configured-but-broken database is a real failure, not
/// a reason to skip.
pub async fn postgres() -> Option<TestStore> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let n = SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed);
    let schema = format!(
        "{SCHEMA_PREFIX}{}_{}_{n}",
        now_secs(),
        std::process::id()
    );

    let mut conn = sqlx::PgConnection::connect(&url)
        .await
        .expect("connect to DATABASE_URL");
    if !SWEPT.swap(true, Ordering::Relaxed) {
        sweep_stale_schemas(&mut conn).await;
    }
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&mut conn)
        .await
        .expect("create per-test schema");
    let _ = conn.close().await;

    let storage = PostgresStorage::with_min_connections(&url, 4, 0, Some(&schema))
        .await
        .expect("connect PostgresStorage to per-test schema");
    storage
        .run_migrations()
        .await
        .expect("run migrations in per-test schema");

    Some(TestStore {
        inner: Arc::new(storage),
        kind: BackendKind::Postgres,
        postgres: Some((url, schema)),
    })
}

/// Drop schemas left behind by earlier processes (see module docs).
async fn sweep_stale_schemas(conn: &mut sqlx::PgConnection) {
    let Ok(names) = sqlx::query_scalar::<_, String>(
        "SELECT nspname FROM pg_namespace WHERE nspname LIKE $1",
    )
    .bind(format!("{SCHEMA_PREFIX}%"))
    .fetch_all(&mut *conn)
    .await
    else {
        return;
    };
    let cutoff = now_secs().saturating_sub(STALE_SCHEMA_SECS);
    for name in names {
        let created = name
            .strip_prefix(SCHEMA_PREFIX)
            .and_then(|rest| rest.split('_').next())
            .and_then(|secs| secs.parse::<u64>().ok());
        if created.is_some_and(|secs| secs < cutoff) {
            let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{name}\" CASCADE"))
                .execute(&mut *conn)
                .await;
        }
    }
}

async fn drop_schema(url: &str, schema: &str) {
    let Ok(mut conn) = sqlx::PgConnection::connect(url).await else {
        return;
    };
    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(&mut conn)
        .await;
    let _ = conn.close().await;
}

/// Expand each listed `async fn <name>(s: TestStore)` into a `#[tokio::test]`
/// per backend: `sqlite::<name>` (always) and `postgres::<name>` (skips when
/// `DATABASE_URL` is unset). Requires `#[macro_use] mod common;` in the test
/// file.

macro_rules! for_each_backend {
    ($($name:ident),* $(,)?) => {
        mod sqlite {
            $(
                #[tokio::test]
                async fn $name() {
                    let s = crate::common::sqlite().await;
                    super::$name(s).await;
                }
            )*
        }

        mod postgres {
            $(
                #[tokio::test]
                async fn $name() {
                    let Some(s) = crate::common::postgres().await else {
                        eprintln!(
                            "skipping postgres::{}: DATABASE_URL not set",
                            stringify!($name)
                        );
                        return;
                    };
                    let teardown = s.teardown();
                    super::$name(s).await;
                    teardown.await;
                }
            )*
        }
    };
}
