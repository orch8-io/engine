//! Outcomes of adopting a child while holding its parent's execution claim.
use orch8_types::instance::TaskInstance;

/// A held parent claim does not imply a previously selected child is eligible.
#[derive(Debug, Clone)]
pub enum ChildAdoption {
    /// The child disappeared, changed activation, was cancelled or superseded,
    /// or does not belong to this parent and owner scope. Nothing was changed.
    Ineligible,
    /// Fresh committed child, with a failed child's retry completed atomically.
    Adopted(Box<TaskInstance>),
}

// Narrow candidate reads; only the winning child row is fetched.
pub(crate) const SQLITE: &str = r"SELECT * FROM task_instances WHERE id = (
    SELECT id FROM (
        SELECT id, parent_instance_id, metadata, state, created_at, CASE WHEN instr(json_extract(metadata, '$._parent_activation'), '/') = 0 THEN '' ELSE substr(json_extract(metadata, '$._parent_activation'), instr(json_extract(metadata, '$._parent_activation'), '/') + 1) END AS activation_suffix
        FROM task_instances
    ) candidates
    WHERE parent_instance_id = ?1
      AND json_type(metadata, '$._parent_block_id') = 'text'
      AND json_extract(metadata, '$._parent_block_id') = ?2
      AND json_type(metadata, '$._parent_activation') = 'text'
      AND activation_suffix = ?3
      AND json_type(metadata, '$._superseded') IS NULL AND state <> 'cancelled'
    ORDER BY created_at DESC, id DESC LIMIT 1)";

// Hashes bound index keys only. Original block/suffix equality remains required.
#[cfg(feature = "postgres")]
pub(crate) const POSTGRES: &str = r"SELECT * FROM task_instances WHERE id = (
    SELECT id FROM (
        SELECT id, parent_instance_id, metadata, state, created_at, CASE WHEN strpos(metadata->>'_parent_activation', '/') = 0 THEN '' ELSE substr(metadata->>'_parent_activation', strpos(metadata->>'_parent_activation', '/') + 1) END AS activation_suffix
        FROM task_instances
    ) candidates
    WHERE parent_instance_id = $1
      AND jsonb_typeof(metadata->'_parent_block_id') = 'string'
      AND md5(metadata->>'_parent_block_id') = md5($2) AND metadata->>'_parent_block_id' = $2
      AND jsonb_typeof(metadata->'_parent_activation') = 'string'
      AND md5(activation_suffix) = md5($3) AND activation_suffix = $3
      AND NOT (metadata ? '_superseded') AND state <> 'cancelled'
    ORDER BY created_at DESC, id DESC LIMIT 1)";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{
        backends::{BackendKind, RawPool, TestBackend},
        fixtures::{InstanceBuilder, SequenceBuilder},
    };
    use orch8_types::{ids::InstanceId, instance::InstanceState};
    use serde_json::json;
    use sqlx::Row;

    const SUFFIX: &str = "outer:1/inner:2";

    #[tokio::test]
    async fn indexed_adoption_preserves_eligibility_order_scope_and_decryption() {
        for backend in TestBackend::all(&BackendKind::ALL).await {
            let (parent, selected) = seed(&backend).await;
            let got = backend
                .storage
                .get_child_for_adoption(parent, "spawn", SUFFIX)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                got.id,
                selected.id,
                "{} newest eligible child",
                backend.name()
            );
            assert_eq!(got.context.data, json!({"private":"retained"}));
            assert!(
                backend
                    .storage
                    .get_child_for_adoption(InstanceId::new(), "spawn", SUFFIX)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                backend
                    .storage
                    .get_child_for_adoption(parent, "other", SUFFIX)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                backend
                    .storage
                    .get_child_for_adoption(parent, "spawn", "outer:1/inner:20000")
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                backend
                    .storage
                    .get_child_for_adoption(parent, "7", "")
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                backend
                    .storage
                    .get_child_for_adoption(parent, "spawn", "")
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_query_plan(&backend, parent).await;
            println!(
                "{}: indexed adoption contract and query plan verified with 2048 historical children",
                backend.name()
            );
        }
    }

    async fn seed(backend: &TestBackend) -> (InstanceId, TaskInstance) {
        let store = &backend.storage;
        let sequence = SequenceBuilder::new("adoption-query")
            .step("work", "noop")
            .build();
        store.create_sequence(&sequence).await.unwrap();
        let parent = InstanceBuilder::for_sequence(&sequence).build();
        let other = InstanceBuilder::for_sequence(&sequence).build();
        store
            .create_instances_batch(&[parent.clone(), other.clone()])
            .await
            .unwrap();
        let now = chrono::Utc::now();
        let child = |owner, metadata, state, age| {
            let mut child = InstanceBuilder::for_sequence(&sequence)
                .metadata(metadata)
                .data(json!({"private":"retained"}))
                .state(state)
                .build();
            child.parent_instance_id = Some(owner);
            child.created_at = now - chrono::Duration::seconds(age);
            child
        };
        let meta = json!({"_parent_block_id":"spawn","_parent_activation":format!("old/{SUFFIX}")});
        let mut selected = child(parent.id, meta.clone(), InstanceState::Completed, 10);
        let mut tied = child(parent.id, meta.clone(), InstanceState::Running, 10);
        if selected.id.into_uuid() < tied.id.into_uuid() {
            std::mem::swap(&mut selected.id, &mut tied.id);
        }
        let mut rows = vec![
            selected.clone(),
            tied,
            child(other.id, meta.clone(), InstanceState::Completed, 0),
            child(parent.id, meta.clone(), InstanceState::Cancelled, 0),
            child(
                parent.id,
                json!({"_parent_block_id":"spawn"}),
                InstanceState::Completed,
                0,
            ),
            child(
                parent.id,
                json!({"_parent_block_id":"spawn","_parent_activation":null}),
                InstanceState::Completed,
                0,
            ),
            child(
                parent.id,
                json!({"_parent_block_id":7,"_parent_activation":123}),
                InstanceState::Completed,
                0,
            ),
        ];
        rows.push(child(
            parent.id,
            json!({"_parent_block_id":7,"_parent_activation":"old-node"}),
            InstanceState::Completed,
            0,
        ));
        rows.push(child(
            parent.id,
            json!({"_parent_block_id":"spawn","_parent_activation":123}),
            InstanceState::Completed,
            0,
        ));
        for value in [json!(true), json!(false), json!(null), json!(0), json!("")] {
            let mut superseded = meta.clone();
            superseded["_superseded"] = value;
            rows.push(child(parent.id, superseded, InstanceState::Completed, 0));
        }
        for iteration in 0..2048 {
            let activation = if iteration % 2 == 0 {
                format!("old/{SUFFIX}")
            } else {
                format!("old/outer:99/inner:{iteration}")
            };
            rows.push(child(
                parent.id,
                json!({"_parent_block_id":"spawn","_parent_activation":activation}),
                InstanceState::Completed,
                200,
            ));
        }
        store.create_instances_batch(&rows).await.unwrap();
        (parent.id, selected)
    }

    async fn assert_query_plan(backend: &TestBackend, parent: InstanceId) {
        let plan = match &backend.raw {
            RawPool::Sqlite(pool) => {
                // The interpolated SQL is a fixed production constant; values remain binds.
                let query = format!("EXPLAIN QUERY PLAN {SQLITE}");
                let rows = sqlx::query(sqlx::AssertSqlSafe(query))
                    .bind(parent.to_string())
                    .bind("spawn")
                    .bind(SUFFIX)
                    .fetch_all(pool)
                    .await
                    .unwrap();
                rows.iter()
                    .map(|row| row.get::<String, _>("detail"))
                    .collect::<Vec<_>>()
                    .join("\n")
            }
            #[cfg(feature = "postgres")]
            RawPool::Postgres(pool) => {
                sqlx::query("ANALYZE task_instances")
                    .execute(pool)
                    .await
                    .unwrap();
                // The interpolated SQL is a fixed production constant; values remain binds.
                let query = format!("EXPLAIN {POSTGRES}");
                let rows: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(query))
                    .bind(parent.into_uuid())
                    .bind("spawn")
                    .bind(SUFFIX)
                    .fetch_all(pool)
                    .await
                    .unwrap();
                rows.join("\n")
            }
        };
        assert!(
            plan.contains("idx_child_adoption"),
            "{} query plan: {plan}",
            backend.name()
        );
        assert!(
            !plan.contains("Seq Scan on task_instances"),
            "history scan: {plan}"
        );
        assert!(
            !plan.contains("Sort") && !plan.contains("USE TEMP B-TREE"),
            "history sort: {plan}"
        );
        assert!(
            !plan.contains("Bitmap Heap"),
            "unordered history read: {plan}"
        );
    }

    #[tokio::test]
    async fn adoption_accepts_live_and_failed_children_but_never_cancelled() {
        for backend in TestBackend::all(&BackendKind::ALL).await {
            let store = &backend.storage;
            let sequence = SequenceBuilder::new("adoption-state")
                .step("work", "noop")
                .build();
            store.create_sequence(&sequence).await.unwrap();
            for state in [
                InstanceState::Scheduled,
                InstanceState::Running,
                InstanceState::Waiting,
                InstanceState::Paused,
                InstanceState::Completed,
                InstanceState::Failed,
                InstanceState::Cancelled,
            ] {
                let parent = InstanceBuilder::for_sequence(&sequence).build();
                store.create_instance(&parent).await.unwrap();
                let mut child = InstanceBuilder::for_sequence(&sequence)
                    .state(state)
                    .metadata(json!({"_parent_block_id":"spawn", "_parent_activation":"old-node"}))
                    .build();
                child.parent_instance_id = Some(parent.id);
                store.create_instance(&child).await.unwrap();
                let got = store
                    .get_child_for_adoption(parent.id, "spawn", "")
                    .await
                    .unwrap();
                assert_eq!(
                    got.map(|child| child.id),
                    if state == InstanceState::Cancelled {
                        None
                    } else {
                        Some(child.id)
                    },
                    "{}: {state:?}",
                    backend.name()
                );
            }
        }
    }

    #[tokio::test]
    async fn adoption_long_block_and_unicode_activation_suffix_are_storable_and_exact() {
        for backend in TestBackend::all(&BackendKind::ALL).await {
            let store = &backend.storage;
            let sequence = SequenceBuilder::new("adoption-long")
                .step("work", "noop")
                .build();
            store.create_sequence(&sequence).await.unwrap();
            let parent = InstanceBuilder::for_sequence(&sequence).build();
            store.create_instance(&parent).await.unwrap();
            let long = (0..512)
                .map(|_| uuid::Uuid::new_v4().to_string())
                .collect::<Vec<_>>()
                .join("/");
            let mut child = InstanceBuilder::for_sequence(&sequence)
                .metadata(
                    json!({"_parent_block_id":long,"_parent_activation":format!("节点/{long}")}),
                )
                .build();
            child.parent_instance_id = Some(parent.id);
            store.create_instance(&child).await.unwrap();
            assert_eq!(
                store
                    .get_child_for_adoption(parent.id, &long, &long)
                    .await
                    .unwrap()
                    .unwrap()
                    .id,
                child.id
            );
            assert!(
                store
                    .get_child_for_adoption(parent.id, &long, &format!("{long}x"))
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }
}
