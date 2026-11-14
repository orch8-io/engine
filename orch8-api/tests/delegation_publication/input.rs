//! Publication-owned input bytes and logical resource charges survive encryption.
use super::*;
use orch8_types::delegation_definition::input::{EncodedDelegationInput, InputLimits};

async fn retained_input(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for mode in [Mode::Hosted, Mode::Remote, Mode::Inline] {
        let mut f = fixture(b.storage.as_ref(), &server, mode).await;
        f.request["input"] = json!({"private":"DELEGATION_INPUT_CANARY","escaped":"\0λ😺","wide":u64::MAX,"tiny":f64::from_bits(1),"negative_zero":-0.0});
        let encoded =
            EncodedDelegationInput::from_value(&f.request["input"], InputLimits::default())
                .unwrap();
        let response = claim(&server, &f.request).await;
        assert!(
            response.status().is_success(),
            "{}",
            response.text().await.unwrap()
        );
        let receipt = b
            .storage
            .get_delegation_publication(&f.owner.tenant_id, f.delegation_id())
            .await
            .unwrap()
            .unwrap();
        let record: (String,String,String,i64,i64,i64,i64) = match &b.raw {
            RawPool::Sqlite(pool) => sqlx::query_as("SELECT stored_input,request_sha256,input_sha256,definition_expanded_bytes,input_wire_version,input_bytes,input_expanded_bytes FROM delegation_publication_inputs WHERE tenant_id=?1 AND task_id=?2")
                .bind(f.owner.tenant_id.as_str()).bind(receipt.task_id.to_string()).fetch_one(pool).await.unwrap(),
            RawPool::Postgres(pool) => sqlx::query_as("SELECT stored_input,request_sha256,input_sha256,definition_expanded_bytes::bigint,input_wire_version::bigint,input_bytes::bigint,input_expanded_bytes::bigint FROM delegation_publication_inputs WHERE tenant_id=$1 AND task_id=$2")
                .bind(f.owner.tenant_id.as_str()).bind(receipt.task_id).fetch_one(pool).await.unwrap(),
        };
        assert_eq!(record.1, receipt.request_sha256);
        assert_eq!(record.2, encoded.reference().payload_sha256);
        assert_eq!(record.4, 1);
        assert_eq!(record.5, i64::from(encoded.reference().payload_bytes));
        assert_eq!(record.6, i64::from(encoded.reference().expanded_bytes));
        let frozen = b
            .storage
            .get_published_delegation_definition(&f.owner.tenant_id, receipt.task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.3, i64::from(frozen.wire().expanded_bytes()));
        let mut body: Value = serde_json::from_str(&record.0).unwrap();
        assert_eq!(
            orch8_types::encryption::FieldEncryptor::is_encrypted(&body),
            b.kind.is_encrypted()
        );
        if b.kind.is_encrypted() {
            body = orch8_types::encryption::FieldEncryptor::from_hex_key(
                orch8_storage::testing::backends::TEST_ENCRYPTION_KEY_HEX,
            )
            .unwrap()
            .decrypt_value(&body)
            .unwrap();
        }
        assert_eq!(body.as_str(), Some(encoded.payload_base64().as_str()));
        // Retry keeps the original immutable row rather than inserting a replacement.
        assert!(claim(&server, &f.request).await.status().is_success());
        assert_eq!(
            b.raw
                .count(
                    "delegation_publication_inputs",
                    "task_id",
                    &receipt.task_id.to_string()
                )
                .await,
            1
        );
        for action in [
            "UPDATE delegation_publication_inputs SET stored_input=stored_input WHERE tenant_id=$1",
            "DELETE FROM delegation_publication_inputs WHERE tenant_id=$1",
        ] {
            let rejected = match &b.raw {
                RawPool::Sqlite(pool) => sqlx::query(action)
                    .bind(f.owner.tenant_id.as_str())
                    .execute(pool)
                    .await
                    .is_err(),
                RawPool::Postgres(pool) => sqlx::query(action)
                    .bind(f.owner.tenant_id.as_str())
                    .execute(pool)
                    .await
                    .is_err(),
            };
            assert!(rejected, "retained input was mutable");
        }
    }
}
orch8_storage::backend_test!(async fn publication_retains_exact_input_and_measured_resources(b){retained_input(b).await;});

async fn oversized_input(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for value in [json!("\0".repeat(700_000)), json!(vec![0; 270_000])] {
        let mut f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
        f.request["input"] = value;
        assert!(claim(&server, &f.request).await.status().is_client_error());
        assert_empty(b.storage.as_ref(), &f).await;
        assert_eq!(
            b.raw
                .count(
                    "delegation_publication_inputs",
                    "tenant_id",
                    f.owner.tenant_id.as_str()
                )
                .await,
            0
        );
    }
}
orch8_storage::backend_test!(async fn oversized_input_does_not_spend_the_publication_grant(b){oversized_input(b).await;});
