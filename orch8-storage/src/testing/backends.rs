//! Backend matrix for storage tests (T2 harness H1, T1 HX-8).
//!
//! One test body, every backend:
//!
//! ```ignore
//! orch8_storage::backend_test!(async fn create_then_get_round_trips(b) {
//!     let seq = SequenceBuilder::new("t").step("a", "noop").build();
//!     b.storage.create_sequence(&seq).await.unwrap();
//!     assert!(b.storage.get_sequence(seq.id).await.unwrap().is_some());
//! });
//! // -> create_then_get_round_trips::{sqlite, postgres, encrypted_sqlite, encrypted_postgres}
//!
//! orch8_storage::backend_test!(kinds = [sqlite, postgres], async fn only_two(b) { ... });
//! ```
//!
//! - `SQLite` legs use a fresh in-memory database per test.
//! - Postgres legs get a **fresh schema per test** (`orch8_t_<uuid>`),
//!   migrated, and dropped when the [`TestBackend`] is dropped, so global
//!   sweeps (`claim_due_*`, reapers, `list_*` without a tenant) never see
//!   another test's rows.
//! - Encrypted legs wrap the same backends in
//!   [`crate::encrypting::EncryptingStorage`] with a fixed test key.
//! - [`postgres_url`] is the one place that decides whether a missing
//!   `DATABASE_URL` skips (local runs, with a `SKIPPED` line) or **fails**
//!   (`CI` is set, or `ORCH8_REQUIRE_PG=1`), so a renamed env var in CI can
//!   never turn the Postgres legs into silent no-ops.

use std::sync::Arc;

use orch8_types::encryption::FieldEncryptor;

use crate::StorageBackend;
use crate::encrypting::EncryptingStorage;
use crate::sqlite::SqliteStorage;

/// Hex key used by the encrypted legs.
pub const TEST_ENCRYPTION_KEY_HEX: &str =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// Which backend a [`TestBackend`] runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    Sqlite,
    Postgres,
    EncryptedSqlite,
    EncryptedPostgres,
}

impl BackendKind {
    /// Every kind, in the order `backend_test!` emits them.
    pub const ALL: [Self; 4] = [
        Self::Sqlite,
        Self::Postgres,
        Self::EncryptedSqlite,
        Self::EncryptedPostgres,
    ];

    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
            Self::EncryptedSqlite => "encrypted_sqlite",
            Self::EncryptedPostgres => "encrypted_postgres",
        }
    }

    #[must_use]
    pub const fn is_postgres(self) -> bool {
        matches!(self, Self::Postgres | Self::EncryptedPostgres)
    }

    #[must_use]
    pub const fn is_encrypted(self) -> bool {
        matches!(self, Self::EncryptedSqlite | Self::EncryptedPostgres)
    }
}

/// What [`postgres_url`] concluded from the environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgUrlDecision {
    /// Run the Postgres leg against this URL.
    Use(String),
    /// Skip it (local run without `DATABASE_URL`).
    Skip,
    /// Fail: `DATABASE_URL` is missing but Postgres is required.
    Required,
}

fn truthy(value: Option<&str>) -> bool {
    value.is_some_and(|v| {
        let v = v.trim();
        !v.is_empty() && v != "0" && !v.eq_ignore_ascii_case("false")
    })
}

/// Pure decision behind [`postgres_url`], given the values of
/// `DATABASE_URL`, `CI` and `ORCH8_REQUIRE_PG`. Postgres is required when
/// `CI` or `ORCH8_REQUIRE_PG` is set to anything but empty / `0` / `false`.
#[must_use]
pub fn decide_postgres_url(
    database_url: Option<&str>,
    ci: Option<&str>,
    require_pg: Option<&str>,
) -> PgUrlDecision {
    match database_url {
        Some(url) if !url.trim().is_empty() => PgUrlDecision::Use(url.to_owned()),
        _ if truthy(ci) || truthy(require_pg) => PgUrlDecision::Required,
        _ => PgUrlDecision::Skip,
    }
}

/// The Postgres URL for tests (`DATABASE_URL`).
///
/// - Set and non-empty: `Some(url)`.
/// - Missing under CI / `ORCH8_REQUIRE_PG=1`: **panics** (the leg must run).
/// - Missing locally: prints a `SKIPPED` line to stderr and returns `None`.
///
/// Use it instead of `let Ok(url) = env::var("DATABASE_URL") else { return }`.
#[must_use]
pub fn postgres_url() -> Option<String> {
    let var = |name: &str| std::env::var(name).ok();
    match decide_postgres_url(
        var("DATABASE_URL").as_deref(),
        var("CI").as_deref(),
        var("ORCH8_REQUIRE_PG").as_deref(),
    ) {
        PgUrlDecision::Use(url) => Some(url),
        PgUrlDecision::Required => panic!(
            "DATABASE_URL is not set but Postgres tests are required here \
             (CI or ORCH8_REQUIRE_PG=1); refusing to skip the Postgres leg silently"
        ),
        PgUrlDecision::Skip => {
            eprintln!("SKIPPED: DATABASE_URL not set; Postgres leg not run");
            None
        }
    }
}

/// Raw connection pool behind a [`TestBackend`], for back-dating rows or
/// counting them directly. The Postgres pool's connections already use the
/// test's schema as `search_path`.
#[derive(Clone)]
pub enum RawPool {
    Sqlite(sqlx::SqlitePool),
    #[cfg(feature = "postgres")]
    Postgres(sqlx::PgPool),
}

impl RawPool {
    /// `UPDATE {table} SET {col} = when WHERE {key} = id` (the key compared
    /// as text), for back-dating rows whose timestamps the database stamps
    /// itself (`NOW()`). `table` / `col` / `key` are test-supplied
    /// identifiers, never user input.
    pub async fn backdate(
        &self,
        table: &str,
        col: &str,
        key: &str,
        id: &str,
        when: chrono::DateTime<chrono::Utc>,
    ) {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(pool) => {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {table} SET {col} = $1 WHERE {key}::text = $2"
                )))
                .bind(when)
                .bind(id)
                .execute(pool)
                .await
                .expect("backdate");
            }
            Self::Sqlite(pool) => {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {table} SET {col} = ?1 WHERE {key} = ?2"
                )))
                .bind(when.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
                .bind(id)
                .execute(pool)
                .await
                .expect("backdate");
            }
        }
    }

    /// `SELECT COUNT(*) FROM {table} WHERE {key} = id` (key compared as text).
    pub async fn count(&self, table: &str, key: &str, id: &str) -> i64 {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(pool) => sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT COUNT(*) FROM {table} WHERE {key}::text = $1"
            )))
            .bind(id)
            .fetch_one(pool)
            .await
            .expect("count"),
            Self::Sqlite(pool) => sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT COUNT(*) FROM {table} WHERE {key} = ?1"
            )))
            .bind(id)
            .fetch_one(pool)
            .await
            .expect("count"),
        }
    }
}

/// A backend instance owned by one test. Dropping it drops the Postgres
/// schema.
pub struct TestBackend {
    pub kind: BackendKind,
    pub storage: Arc<dyn StorageBackend>,
    pub raw: RawPool,
    #[cfg(feature = "postgres")]
    schema: Option<PgSchema>,
}

impl std::fmt::Debug for TestBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestBackend")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

fn test_encryptor() -> FieldEncryptor {
    FieldEncryptor::from_hex_key(TEST_ENCRYPTION_KEY_HEX).expect("test key is valid hex")
}

impl TestBackend {
    /// Fresh in-memory `SQLite` backend.
    pub async fn sqlite() -> Self {
        let sqlite = SqliteStorage::in_memory()
            .await
            .expect("in-memory SQLite storage");
        Self {
            kind: BackendKind::Sqlite,
            raw: RawPool::Sqlite(sqlite.pool().clone()),
            storage: Arc::new(sqlite),
            #[cfg(feature = "postgres")]
            schema: None,
        }
    }

    /// Fresh Postgres schema, migrated. `None` (skip) only when
    /// [`postgres_url`] allows skipping.
    #[cfg(feature = "postgres")]
    pub async fn postgres() -> Option<Self> {
        let TestPostgres { storage, schema } = TestPostgres::fresh().await?;
        Some(Self {
            kind: BackendKind::Postgres,
            raw: RawPool::Postgres(storage.pool().clone()),
            storage: Arc::new(storage),
            schema: Some(schema),
        })
    }

    /// Open `kind`; `None` when a Postgres leg is skipped.
    pub async fn open(kind: BackendKind) -> Option<Self> {
        let base = match kind {
            BackendKind::Sqlite | BackendKind::EncryptedSqlite => Self::sqlite().await,
            #[cfg(feature = "postgres")]
            BackendKind::Postgres | BackendKind::EncryptedPostgres => Self::postgres().await?,
            #[cfg(not(feature = "postgres"))]
            BackendKind::Postgres | BackendKind::EncryptedPostgres => {
                panic!("orch8-storage was built without the `postgres` feature")
            }
        };
        Some(if kind.is_encrypted() {
            base.encrypted(kind)
        } else {
            base
        })
    }

    fn encrypted(self, kind: BackendKind) -> Self {
        Self {
            kind,
            storage: Arc::new(EncryptingStorage::new(
                Arc::clone(&self.storage),
                test_encryptor(),
            )),
            ..self
        }
    }

    /// Every kind in `kinds` that is not skipped.
    pub async fn all(kinds: &[BackendKind]) -> Vec<Self> {
        let mut out = Vec::with_capacity(kinds.len());
        for kind in kinds {
            if let Some(b) = Self::open(*kind).await {
                out.push(b);
            }
        }
        out
    }

    /// `SQLite` plus Postgres (when not skipped): the common two-leg matrix.
    pub async fn sqlite_and_postgres() -> Vec<Self> {
        Self::all(&[BackendKind::Sqlite, BackendKind::Postgres]).await
    }

    /// `"sqlite"`, `"postgres"`, ... for assertion messages.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.kind.name()
    }

    /// The Postgres schema of this backend, if it is a Postgres leg.
    #[must_use]
    pub fn pg_schema(&self) -> Option<&str> {
        #[cfg(feature = "postgres")]
        {
            self.schema.as_ref().map(|s| s.schema.as_str())
        }
        #[cfg(not(feature = "postgres"))]
        {
            None
        }
    }

    /// A second, independent connection pool to the same Postgres schema (a
    /// "second node"). `None` on `SQLite`.
    #[cfg(feature = "postgres")]
    pub async fn connect_another_postgres(&self) -> Option<crate::postgres::PostgresStorage> {
        let schema = self.schema.as_ref()?;
        Some(
            crate::postgres::PostgresStorage::new(&schema.url, 5, Some(&schema.schema))
                .await
                .expect("second Postgres pool"),
        )
    }

    /// The raw `SQLite` pool. Panics on Postgres.
    #[must_use]
    pub fn sqlite_pool(&self) -> &sqlx::SqlitePool {
        match &self.raw {
            RawPool::Sqlite(p) => p,
            #[cfg(feature = "postgres")]
            RawPool::Postgres(_) => panic!("{} is not a SQLite backend", self.name()),
        }
    }

    /// The raw Postgres pool (search path = this test's schema). Panics on
    /// `SQLite`.
    #[cfg(feature = "postgres")]
    #[must_use]
    pub fn pg_pool(&self) -> &sqlx::PgPool {
        match &self.raw {
            RawPool::Postgres(p) => p,
            RawPool::Sqlite(_) => panic!("{} is not a Postgres backend", self.name()),
        }
    }
}

/// A concrete [`crate::postgres::PostgresStorage`] on a fresh, migrated
/// schema of its own, for tests that need the concrete type (its `pool()`,
/// `&PostgresStorage` helpers). Derefs to the storage; dropping it drops the
/// schema.
#[cfg(feature = "postgres")]
pub struct TestPostgres {
    pub storage: crate::postgres::PostgresStorage,
    schema: PgSchema,
}

#[cfg(feature = "postgres")]
impl TestPostgres {
    /// Fresh schema, or `None` when [`postgres_url`] allows skipping.
    pub async fn fresh() -> Option<Self> {
        let url = postgres_url()?;
        let (storage, schema) = PgSchema::create(&url).await;
        Some(Self { storage, schema })
    }

    /// This test's schema name.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema.schema
    }

    /// Split into the storage and the guard that drops the schema; keep the
    /// guard alive for as long as the storage is used.
    pub fn into_parts(self) -> (crate::postgres::PostgresStorage, PgSchemaGuard) {
        (self.storage, PgSchemaGuard(self.schema))
    }

    /// A second, independent pool on the same schema (a "second node").
    pub async fn connect_another(&self) -> crate::postgres::PostgresStorage {
        crate::postgres::PostgresStorage::new(&self.schema.url, 5, Some(&self.schema.schema))
            .await
            .expect("second Postgres pool")
    }
}

#[cfg(feature = "postgres")]
impl std::ops::Deref for TestPostgres {
    type Target = crate::postgres::PostgresStorage;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

/// Drops a [`TestPostgres`] schema when dropped (see
/// [`TestPostgres::into_parts`]).
#[cfg(feature = "postgres")]
#[must_use = "dropping the guard drops the schema"]
pub struct PgSchemaGuard(#[allow(dead_code)] PgSchema);

/// A throwaway Postgres schema, dropped on `Drop`.
#[cfg(feature = "postgres")]
struct PgSchema {
    url: String,
    schema: String,
}

#[cfg(feature = "postgres")]
impl PgSchema {
    async fn create(url: &str) -> (crate::postgres::PostgresStorage, Self) {
        use sqlx::AssertSqlSafe;
        let schema = format!("orch8_t_{}", uuid::Uuid::now_v7().simple());
        {
            let admin = sqlx::PgPool::connect(url)
                .await
                .expect("connect to DATABASE_URL");
            sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
                .execute(&admin)
                .await
                .expect("create test schema");
            admin.close().await;
        }
        let pg = crate::postgres::PostgresStorage::with_min_connections(url, 5, 0, Some(&schema))
            .await
            .expect("Postgres storage on the test schema");
        pg.run_migrations()
            .await
            .expect("migrations on the test schema");
        (
            pg,
            Self {
                url: url.to_owned(),
                schema,
            },
        )
    }
}

#[cfg(feature = "postgres")]
impl Drop for PgSchema {
    fn drop(&mut self) {
        // `Drop` cannot await, and the test's runtime may be current-thread
        // (no `block_in_place`), so drop the schema from a helper thread
        // with its own runtime and connection. Best-effort: a leaked schema
        // only costs disk in a throwaway database.
        let url = self.url.clone();
        let schema = std::mem::take(&mut self.schema);
        let _ = std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            rt.block_on(async move {
                if let Ok(admin) = sqlx::PgPool::connect(&url).await {
                    let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
                        "DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"
                    )))
                    .execute(&admin)
                    .await;
                    admin.close().await;
                }
            });
        })
        .join();
    }
}

/// Generate one `#[tokio::test]` per backend kind from a single body.
///
/// ```ignore
/// orch8_storage::backend_test!(async fn name(b) { /* b: TestBackend */ });
/// orch8_storage::backend_test!(kinds = [sqlite, postgres], async fn name(b) { ... });
/// ```
///
/// Expands to `mod name { use super::*; #[tokio::test] async fn sqlite() {..} ... }`
/// so the legs are filterable as `name::postgres`. A skipped Postgres leg
/// returns early (and panics under CI, see [`postgres_url`]). Kinds:
/// `sqlite`, `postgres`, `encrypted_sqlite`, `encrypted_postgres`.
#[macro_export]
macro_rules! backend_test {
    ($(#[$meta:meta])* async fn $name:ident($b:ident) $body:block) => {
        $crate::backend_test!(
            kinds = [sqlite, postgres, encrypted_sqlite, encrypted_postgres],
            $(#[$meta])* async fn $name($b) $body
        );
    };
    (kinds = [$($kind:ident),+ $(,)?], $(#[$meta:meta])* async fn $name:ident($b:ident) $body:block) => {
        #[allow(non_snake_case)]
        mod $name {
            #[allow(unused_imports)]
            use super::*;
            $crate::backend_test!(@legs [$(#[$meta])*] [$($kind),+] $b $body);
        }
    };
    (@legs [$($attr:tt)*] [] $b:ident $body:block) => {};
    (@legs [$($attr:tt)*] [$kind:ident $(, $rest:ident)*] $b:ident $body:block) => {
        #[tokio::test]
        $($attr)*
        async fn $kind() {
            let Some($b) = $crate::testing::backends::TestBackend::open(
                $crate::backend_test!(@kind $kind),
            )
            .await
            else {
                return;
            };
            $body
        }
        $crate::backend_test!(@legs [$($attr)*] [$($rest),*] $b $body);
    };
    (@kind sqlite) => { $crate::testing::backends::BackendKind::Sqlite };
    (@kind postgres) => { $crate::testing::backends::BackendKind::Postgres };
    (@kind encrypted_sqlite) => { $crate::testing::backends::BackendKind::EncryptedSqlite };
    (@kind encrypted_postgres) => { $crate::testing::backends::BackendKind::EncryptedPostgres };
}

#[cfg(test)]
mod tests {
    use super::{PgUrlDecision, decide_postgres_url};

    #[test]
    fn set_database_url_is_used() {
        assert_eq!(
            decide_postgres_url(Some("postgres://x/db"), Some("true"), None),
            PgUrlDecision::Use("postgres://x/db".into())
        );
    }

    #[test]
    fn missing_url_under_ci_is_required_not_skipped() {
        assert_eq!(
            decide_postgres_url(None, Some("true"), None),
            PgUrlDecision::Required
        );
    }

    #[test]
    fn empty_url_under_ci_is_required() {
        assert_eq!(
            decide_postgres_url(Some("  "), Some("1"), None),
            PgUrlDecision::Required
        );
    }

    #[test]
    fn missing_url_with_require_pg_is_required() {
        assert_eq!(
            decide_postgres_url(None, None, Some("1")),
            PgUrlDecision::Required
        );
    }

    #[test]
    fn missing_url_locally_skips() {
        assert_eq!(decide_postgres_url(None, None, None), PgUrlDecision::Skip);
    }

    #[test]
    fn ci_false_or_zero_does_not_require_postgres() {
        assert_eq!(
            decide_postgres_url(None, Some("false"), Some("0")),
            PgUrlDecision::Skip
        );
    }
}
