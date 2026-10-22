//! A logical backup must keep one view or fail if its session is lost.
#![cfg(feature = "postgres")]

use orch8_storage::SequenceStore as _;
use orch8_storage::postgres::PostgresStorage;
use orch8_storage::testing::{
    backends::{TestPostgres, postgres_url},
    fixtures::SequenceBuilder,
};
use std::time::Duration;

#[tokio::test]
async fn released_connections_keep_the_original_snapshot() {
    let Some(pg) = TestPostgres::fresh().await else {
        return;
    };
    let sequence = SequenceBuilder::new("consistent-view")
        .step("a", "noop")
        .build();
    pg.create_sequence(&sequence).await.unwrap();
    let source = PostgresStorage::read_snapshot(&postgres_url().unwrap(), Some(pg.schema()))
        .await
        .unwrap();
    assert_eq!(
        source
            .get_sequence(sequence.id)
            .await
            .unwrap()
            .unwrap()
            .version,
        1
    );
    sqlx::query("UPDATE sequences SET version=2 WHERE id=$1")
        .bind(sequence.id.into_uuid())
        .execute(pg.pool())
        .await
        .unwrap();
    for _ in 0..3 {
        assert_eq!(
            source
                .get_sequence(sequence.id)
                .await
                .unwrap()
                .unwrap()
                .version,
            1
        );
    }
    assert_eq!(
        pg.get_sequence(sequence.id).await.unwrap().unwrap().version,
        2
    );
    source.pool().close().await;
}

#[tokio::test]
async fn snapshot_rejects_writes_without_poisoning_later_reads() {
    let Some(pg) = TestPostgres::fresh().await else {
        return;
    };
    let original = SequenceBuilder::new("original").step("a", "noop").build();
    let forbidden = SequenceBuilder::new("forbidden").step("b", "noop").build();
    pg.create_sequence(&original).await.unwrap();
    let source = PostgresStorage::read_snapshot(&postgres_url().unwrap(), Some(pg.schema()))
        .await
        .unwrap();
    let error = source.create_sequence(&forbidden).await.unwrap_err();
    assert!(error.to_string().contains("read-only"));
    assert!(pg.get_sequence(forbidden.id).await.unwrap().is_none());
    assert_eq!(
        source.get_sequence(original.id).await.unwrap().unwrap().id,
        original.id
    );
    // The normal server pool can still write while a backup is open.
    pg.create_sequence(&forbidden).await.unwrap();
    assert!(source.get_sequence(forbidden.id).await.unwrap().is_none());
    source.pool().close().await;
}

#[tokio::test]
async fn lost_snapshot_connection_never_reconnects_with_a_new_view() {
    let Some(pg) = TestPostgres::fresh().await else {
        return;
    };
    let sequence = SequenceBuilder::new("connection-loss")
        .step("a", "noop")
        .build();
    pg.create_sequence(&sequence).await.unwrap();
    let source = PostgresStorage::read_snapshot(&postgres_url().unwrap(), Some(pg.schema()))
        .await
        .unwrap();
    assert!(source.get_sequence(sequence.id).await.unwrap().is_some());
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(source.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE sequences SET version=2 WHERE id=$1")
        .bind(sequence.id.into_uuid())
        .execute(pg.pool())
        .await
        .unwrap();
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(pg.pool())
        .await
        .unwrap();
    assert!(terminated);
    let result = tokio::time::timeout(Duration::from_secs(12), source.get_sequence(sequence.id))
        .await
        .expect("connection-loss failure must remain bounded");
    assert!(
        result.is_err(),
        "a lost snapshot must fail instead of returning the writer's new version"
    );
    source.pool().close().await;
}

#[tokio::test]
async fn ended_snapshot_transaction_never_becomes_an_autocommit_reader() {
    let Some(pg) = TestPostgres::fresh().await else {
        return;
    };
    let sequence = SequenceBuilder::new("ended-view").step("a", "noop").build();
    pg.create_sequence(&sequence).await.unwrap();
    let source = PostgresStorage::read_snapshot(&postgres_url().unwrap(), Some(pg.schema()))
        .await
        .unwrap();
    assert!(source.get_sequence(sequence.id).await.unwrap().is_some());
    // Simulate accidental session manipulation through the public raw pool.
    sqlx::query("COMMIT").execute(source.pool()).await.unwrap();
    sqlx::query("UPDATE sequences SET version=2 WHERE id=$1")
        .bind(sequence.id.into_uuid())
        .execute(pg.pool())
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(12), source.get_sequence(sequence.id))
        .await
        .expect("an ended snapshot must fail within the acquisition deadline");
    assert!(
        result.is_err(),
        "ending the transaction must not expose a new autocommit view"
    );
    source.pool().close().await;
}
