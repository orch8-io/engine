//! Publish an imported instance only when every dependent row is durable.
use super::PostgresStorage;
use crate::restore::InstanceSnapshot;
use orch8_types::{error::StorageError, execution::ExecutionNode};

pub(super) async fn restore(
    store: &PostgresStorage,
    snapshot: &InstanceSnapshot,
) -> Result<bool, StorageError> {
    let nodes = snapshot.checked_nodes()?;
    let mut tx = store.pool.begin().await?;
    match write_on(&mut tx, snapshot, &nodes).await {
        Ok(true) => {
            tx.commit().await?;
            Ok(true)
        }
        Ok(false) => {
            tx.rollback().await?;
            Ok(false)
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

async fn write_on(
    conn: &mut sqlx::PgConnection,
    snapshot: &InstanceSnapshot,
    nodes: &[&ExecutionNode],
) -> Result<bool, StorageError> {
    // SQL combines two trusted constants; every snapshot value is bound.
    let insert = format!(
        "{} ON CONFLICT (id) DO NOTHING",
        super::instances::INSTANCE_INSERT_SQL
    );
    let context = serde_json::to_value(&snapshot.instance.context)?;
    let inserted = super::instances::bind_instance_insert(
        sqlx::query(sqlx::AssertSqlSafe(insert.as_str())),
        &snapshot.instance,
        &context,
    )
    .execute(&mut *conn)
    .await?
    .rows_affected();
    if inserted == 0 {
        return Ok(false);
    }
    for node in nodes {
        super::execution_tree::create_node_on(&mut *conn, node).await?;
    }
    for entry in &snapshot.payloads {
        super::externalized_snapshot::restore_on(&mut *conn, entry).await?;
    }
    for output in &snapshot.outputs {
        super::outputs::save_on(&mut *conn, output).await?;
    }
    Ok(true)
}
