//! Backend fixtures shared by the backend-generic storage contract suites.
//!
//! Every contract check runs against an in-memory `SQLite` store (always) and
//! against Postgres when `DATABASE_URL` is set. Several storage operations are
//! global by design (`claim_due_wakes`, `recover_stale_instances`), so each
//! Postgres check gets its own freshly-migrated scratch database instead of
//! sharing one with concurrently running tests.

use orch8_storage::postgres::PostgresStorage;
use orch8_storage::sqlite::SqliteStorage;
use uuid::Uuid;

pub async fn sqlite() -> SqliteStorage {
    SqliteStorage::in_memory().await.expect("in-memory sqlite")
}

/// A migrated Postgres database owned by exactly one test.
pub struct ScratchPostgres {
    pub storage: PostgresStorage,
    admin_url: String,
    name: String,
}

impl ScratchPostgres {
    /// Creates and migrates a scratch database, or returns `None` (the caller
    /// skips) when `DATABASE_URL` is not set.
    pub async fn create() -> Option<Self> {
        let Ok(admin_url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping postgres leg: DATABASE_URL not set");
            return None;
        };
        let name = format!("orch8_contract_{}", Uuid::new_v4().simple());
        let admin = sqlx::PgPool::connect(&admin_url)
            .await
            .expect("connect to DATABASE_URL");
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}")))
            .execute(&admin)
            .await
            .expect("create scratch database");
        admin.close().await;
        let storage = PostgresStorage::new(&database_url(&admin_url, &name), 5, None)
            .await
            .expect("connect to scratch database");
        storage.run_migrations().await.expect("run migrations");
        Some(Self {
            storage,
            admin_url,
            name,
        })
    }

    /// Drops the scratch database. A test that panics before reaching this
    /// leaves its database behind; CI databases are ephemeral.
    pub async fn drop_database(self) {
        drop(self.storage);
        let admin = sqlx::PgPool::connect(&self.admin_url)
            .await
            .expect("connect to DATABASE_URL");
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            self.name
        )))
        .execute(&admin)
        .await
        .expect("drop scratch database");
        admin.close().await;
    }
}

/// Replaces the database name in a `postgres://…/<db>[?query]` URL.
fn database_url(url: &str, database: &str) -> String {
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(b, q)| (b, Some(q)));
    let (server, _) = base.rsplit_once('/').expect("DATABASE_URL has a path");
    match query {
        Some(query) => format!("{server}/{database}?{query}"),
        None => format!("{server}/{database}"),
    }
}

/// Instantiates each `async fn check(&S)` as a `sqlite::<check>` and a
/// `postgres::<check>` test.
macro_rules! backend_contract {
    ($($(#[$meta:meta])* $check:ident),+ $(,)?) => {
        mod sqlite {
            $(
                $(#[$meta])*
                #[tokio::test]
                async fn $check() {
                    super::$check(&crate::support::sqlite().await).await;
                }
            )+
        }

        mod postgres {
            $(
                $(#[$meta])*
                #[tokio::test]
                async fn $check() {
                    let Some(db) = crate::support::ScratchPostgres::create().await else {
                        return;
                    };
                    super::$check(&db.storage).await;
                    db.drop_database().await;
                }
            )+
        }
    };
}
