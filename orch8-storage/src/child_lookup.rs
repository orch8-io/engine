//! Indexed current-activation lookup queries shared with native plan tests.

pub(crate) const SQLITE: &str = r"SELECT * FROM task_instances WHERE id = (SELECT id FROM (
        SELECT * FROM (SELECT id, 0 AS match_rank FROM task_instances
            WHERE parent_instance_id = ?1 AND json_type(metadata, '$._parent_activation') = 'text'
              AND json_extract(metadata, '$._parent_activation') = ?3
            ORDER BY created_at, id LIMIT 1)
        UNION ALL
        SELECT * FROM (SELECT id, 1 AS match_rank FROM task_instances
            WHERE parent_instance_id = ?1 AND json_type(metadata, '$._parent_activation') IS NULL
              AND json_type(metadata, '$._parent_block_id') = 'text'
              AND json_extract(metadata, '$._parent_block_id') = ?2
            ORDER BY created_at, id LIMIT 1)
        ) ORDER BY match_rank LIMIT 1)";

// MD5 is an index bucket, never authority: full strings are also compared.
// Bounded keys avoid PostgreSQL B-tree limits for long activation paths.
#[cfg(feature = "postgres")]
pub(crate) const POSTGRES: &str = r"SELECT * FROM task_instances WHERE id = (SELECT id FROM (
        (SELECT id, 0 AS match_rank FROM task_instances
            WHERE parent_instance_id = $1 AND jsonb_typeof(metadata->'_parent_activation') = 'string'
              AND md5(metadata->>'_parent_activation') = md5($3)
              AND metadata->>'_parent_activation' = $3
            ORDER BY created_at, id LIMIT 1)
        UNION ALL
        (SELECT id, 1 AS match_rank FROM task_instances
            WHERE parent_instance_id = $1 AND NOT (metadata ? '_parent_activation')
              AND jsonb_typeof(metadata->'_parent_block_id') = 'string'
              AND md5(metadata->>'_parent_block_id') = md5($2)
              AND metadata->>'_parent_block_id' = $2
            ORDER BY created_at, id LIMIT 1)
        ) candidates ORDER BY match_rank LIMIT 1)";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{
        backends::{BackendKind, RawPool, TestBackend},
        fixtures::{InstanceBuilder, SequenceBuilder},
    };
    use orch8_types::{
        ids::InstanceId,
        instance::{InstanceState, TaskInstance},
    };
    use serde_json::json;
    use sqlx::Row;

    #[tokio::test]
    async fn indexed_lookup_preserves_activation_precedence_types_parent_and_decryption() {
        for backend in TestBackend::all(&BackendKind::ALL).await {
            let (parent, legacy, exact) = seed(&backend).await;
            let store = &backend.storage;
            let got = store
                .get_child_for_activation(parent.id, "sub", "current")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                got.id,
                exact.id,
                "{}: exact live child precedes terminal legacy",
                backend.name()
            );
            assert_eq!(got.state, InstanceState::Running);
            assert_eq!(got.context.data, json!({"private":"decrypted"}));
            for activation in ["missing", "123"] {
                assert_eq!(
                    store
                        .get_child_for_activation(parent.id, "sub", activation)
                        .await
                        .unwrap()
                        .unwrap()
                        .id,
                    legacy.id
                );
            }
            assert!(
                store
                    .get_child_for_activation(parent.id, "123", "missing")
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                store
                    .get_child_for_activation(InstanceId::new(), "sub", "current")
                    .await
                    .unwrap()
                    .is_none()
            );

            assert_query_plan(&backend, parent.id).await;
            println!(
                "{}: 2048 historical children, exact/legacy/type/parent/decryption contract and production query indexes verified",
                backend.name()
            );
        }
    }
    #[tokio::test]
    async fn long_activation_and_legacy_keys_remain_storable_and_exact() {
        for backend in TestBackend::all(&BackendKind::ALL).await {
            let store = &backend.storage;
            let sequence = SequenceBuilder::new("long-key")
                .step("work", "noop")
                .build();
            store.create_sequence(&sequence).await.unwrap();
            let parent = InstanceBuilder::for_sequence(&sequence).build();
            store.create_instance(&parent).await.unwrap();
            let long = (0..512)
                .map(|_| uuid::Uuid::new_v4().to_string())
                .collect::<Vec<_>>()
                .join("/");
            let mut exact = InstanceBuilder::for_sequence(&sequence)
                .metadata(json!({"_parent_activation":long}))
                .build();
            exact.parent_instance_id = Some(parent.id);
            let mut legacy = InstanceBuilder::for_sequence(&sequence)
                .metadata(json!({"_parent_block_id":long}))
                .build();
            legacy.parent_instance_id = Some(parent.id);
            store
                .create_instances_batch(&[exact.clone(), legacy.clone()])
                .await
                .unwrap();
            assert_eq!(
                store
                    .get_child_for_activation(parent.id, "sub", &long)
                    .await
                    .unwrap()
                    .unwrap()
                    .id,
                exact.id
            );
            assert_eq!(
                store
                    .get_child_for_activation(parent.id, &long, "missing")
                    .await
                    .unwrap()
                    .unwrap()
                    .id,
                legacy.id
            );
        }
    }

    async fn seed(backend: &TestBackend) -> (TaskInstance, TaskInstance, TaskInstance) {
        let store = &backend.storage;
        let sequence = SequenceBuilder::new("lookup").step("work", "noop").build();
        store.create_sequence(&sequence).await.unwrap();
        let parent = InstanceBuilder::for_sequence(&sequence).build();
        let other = InstanceBuilder::for_sequence(&sequence).build();
        store
            .create_instances_batch(&[parent.clone(), other.clone()])
            .await
            .unwrap();
        let child = |owner: InstanceId, metadata: serde_json::Value, age: i64| -> TaskInstance {
            let mut instance = InstanceBuilder::for_sequence(&sequence)
                .state(InstanceState::Completed)
                .data(json!({"private":"decrypted"}))
                .metadata(metadata)
                .build();
            instance.parent_instance_id = Some(owner);
            instance.created_at = chrono::Utc::now() - chrono::Duration::seconds(age);
            instance
        };
        let legacy = child(parent.id, json!({"_parent_block_id":"sub"}), 20);
        let mut exact = child(
            parent.id,
            json!({"_parent_activation":"current","_parent_block_id":"sub"}),
            10,
        );
        exact.state = InstanceState::Running;
        let mut tied = exact.clone();
        tied.id = InstanceId::new();
        if tied.id.into_uuid() < exact.id.into_uuid() {
            std::mem::swap(&mut tied.id, &mut exact.id);
        }
        tied.state = InstanceState::Completed;
        let rows = vec![
            child(other.id, json!({"_parent_activation":"current"}), 100),
            child(
                parent.id,
                json!({"_parent_activation":null,"_parent_block_id":"sub"}),
                90,
            ),
            child(
                parent.id,
                json!({"_parent_activation":123,"_parent_block_id":"sub"}),
                80,
            ),
            child(parent.id, json!({"_parent_block_id":123}), 70),
            tied,
            legacy.clone(),
            exact.clone(),
            child(parent.id, json!({"_parent_block_id":"sub"}), 5),
            child(parent.id, json!({"_parent_activation":"current"}), 1),
        ];
        store.create_instances_batch(&rows).await.unwrap();
        let mut history = Vec::new();
        for iteration in 0..2048 {
            history.push(child(
                parent.id,
                json!({"_parent_activation":format!("old/{iteration}"),"_parent_block_id":"sub"}),
                200,
            ));
        }
        store.create_instances_batch(&history).await.unwrap();
        (parent, legacy, exact)
    }

    async fn assert_query_plan(backend: &TestBackend, parent: InstanceId) {
        let plan = match &backend.raw {
            RawPool::Sqlite(pool) => {
                // Only fixed production SQL is interpolated; all values remain binds.
                let query = format!("EXPLAIN QUERY PLAN {SQLITE}");
                let rows = sqlx::query(sqlx::AssertSqlSafe(query))
                    .bind(parent.to_string())
                    .bind("sub")
                    .bind("current")
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
                // Only fixed production SQL is interpolated; all values remain binds.
                let query = format!("EXPLAIN {POSTGRES}");
                let rows: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(query))
                    .bind(parent.into_uuid())
                    .bind("sub")
                    .bind("current")
                    .fetch_all(pool)
                    .await
                    .unwrap();
                rows.join("\n")
            }
        };
        assert!(
            plan.contains("idx_child_activation"),
            "{} exact query plan: {plan}",
            backend.name()
        );
        assert!(
            plan.contains("idx_child_legacy_block"),
            "{} legacy query plan: {plan}",
            backend.name()
        );
        assert!(
            !plan.contains("Seq Scan on task_instances"),
            "unbounded history scan: {plan}"
        );
    }
}
