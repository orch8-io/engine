use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;
use orch8_types::signal::Signal;

use super::rows::SignalRow;
use super::PostgresStorage;

/// Canonical INSERT for `signal_inbox`. Shared by [`enqueue`] and
/// [`enqueue_if_active`] so adding a column touches one place.
const SIGNAL_INSERT_SQL: &str = r"
    INSERT INTO signal_inbox
        (id, instance_id, signal_type, payload, delivered, created_at, delivered_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
";

/// Bind a `Signal` to [`SIGNAL_INSERT_SQL`] in canonical column order.
/// Serializes `signal_type` up front so the returned `Query` borrows only
/// from the provided `String` slot.
fn bind_signal_insert<'q>(
    q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    s: &'q Signal,
    signal_type_str: &'q str,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    q.bind(s.id)
        .bind(s.instance_id.0)
        .bind(signal_type_str)
        .bind(&s.payload)
        .bind(s.delivered)
        .bind(s.created_at)
        .bind(s.delivered_at)
}

fn parse_instance_state(s: &str) -> Result<InstanceState, StorageError> {
    match s {
        "scheduled" => Ok(InstanceState::Scheduled),
        "running" => Ok(InstanceState::Running),
        "waiting" => Ok(InstanceState::Waiting),
        "paused" => Ok(InstanceState::Paused),
        "completed" => Ok(InstanceState::Completed),
        "failed" => Ok(InstanceState::Failed),
        "cancelled" => Ok(InstanceState::Cancelled),
        other => Err(StorageError::Query(format!(
            "unknown instance state: {other}"
        ))),
    }
}

pub(super) async fn enqueue(store: &PostgresStorage, signal: &Signal) -> Result<(), StorageError> {
    let signal_type_str = signal.signal_type.to_string();
    bind_signal_insert(sqlx::query(SIGNAL_INSERT_SQL), signal, &signal_type_str)
        .execute(&store.pool)
        .await?;
    Ok(())
}

/// Atomic enqueue gated on target non-terminal state.
///
/// `SELECT ... FOR UPDATE` inside the transaction locks the target row so
/// no concurrent worker can transition it to terminal between our check and
/// our INSERT. The txn stays tight: begin → select → (maybe) insert → commit.
pub(super) async fn enqueue_if_active(
    store: &PostgresStorage,
    signal: &Signal,
) -> Result<(), StorageError> {
    let mut tx = store.pool.begin().await?;

    let row: Option<(String,)> =
        sqlx::query_as("SELECT state FROM task_instances WHERE id = $1 FOR UPDATE")
            .bind(signal.instance_id.0)
            .fetch_optional(&mut *tx)
            .await?;

    let Some((state_str,)) = row else {
        tx.rollback().await?;
        return Err(StorageError::NotFound {
            entity: "task_instance",
            id: signal.instance_id.0.to_string(),
        });
    };

    let state = parse_instance_state(&state_str)?;
    if state.is_terminal() {
        tx.rollback().await?;
        return Err(StorageError::Conflict(format!(
            "target instance {} is in terminal state '{}'",
            signal.instance_id.0, state_str
        )));
    }

    let signal_type_str = signal.signal_type.to_string();
    bind_signal_insert(sqlx::query(SIGNAL_INSERT_SQL), signal, &signal_type_str)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(())
}

pub(super) async fn get_pending(
    store: &PostgresStorage,
    instance_id: InstanceId,
) -> Result<Vec<Signal>, StorageError> {
    let rows = sqlx::query_as::<_, SignalRow>(
        r"SELECT id, instance_id, signal_type, payload, delivered, created_at, delivered_at
           FROM signal_inbox WHERE instance_id = $1 AND delivered = FALSE
           ORDER BY created_at",
    )
    .bind(instance_id.0)
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.into_iter().map(SignalRow::into_signal).collect())
}

pub(super) async fn get_pending_batch(
    store: &PostgresStorage,
    instance_ids: &[InstanceId],
) -> Result<std::collections::HashMap<InstanceId, Vec<Signal>>, StorageError> {
    if instance_ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let uuids: Vec<Uuid> = instance_ids.iter().map(|id| id.0).collect();
    let rows = sqlx::query_as::<_, SignalRow>(
        r"SELECT id, instance_id, signal_type, payload, delivered, created_at, delivered_at
           FROM signal_inbox WHERE instance_id = ANY($1) AND delivered = FALSE
           ORDER BY created_at",
    )
    .bind(&uuids)
    .fetch_all(&store.pool)
    .await?;

    let mut map: std::collections::HashMap<InstanceId, Vec<Signal>> =
        std::collections::HashMap::new();
    for row in rows {
        let signal = row.into_signal();
        map.entry(signal.instance_id).or_default().push(signal);
    }
    Ok(map)
}

pub(super) async fn mark_delivered(
    store: &PostgresStorage,
    signal_id: Uuid,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE signal_inbox SET delivered = TRUE, delivered_at = NOW() WHERE id = $1")
        .bind(signal_id)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn mark_delivered_batch(
    store: &PostgresStorage,
    signal_ids: &[Uuid],
) -> Result<(), StorageError> {
    if signal_ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "UPDATE signal_inbox SET delivered = TRUE, delivered_at = NOW() WHERE id = ANY($1)",
    )
    .bind(signal_ids)
    .execute(&store.pool)
    .await?;
    Ok(())
}
