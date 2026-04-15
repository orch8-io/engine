use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;
use orch8_types::signal::Signal;

use super::rows::SignalRow;
use super::PostgresStorage;

pub(super) async fn enqueue(store: &PostgresStorage, signal: &Signal) -> Result<(), StorageError> {
    let signal_type_str = signal.signal_type.to_string();
    sqlx::query(
        r"
        INSERT INTO signal_inbox (id, instance_id, signal_type, payload, delivered, created_at, delivered_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ",
    )
    .bind(signal.id)
    .bind(signal.instance_id.0)
    .bind(&signal_type_str)
    .bind(&signal.payload)
    .bind(signal.delivered)
    .bind(signal.created_at)
    .bind(signal.delivered_at)
    .execute(&store.pool)
    .await?;
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
