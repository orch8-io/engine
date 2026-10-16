//! Opening a resumable artifact transfer: the object key must be a bounded
//! `<instance uuid>/<object>` key, the transfer kind is exactly `continuity`
//! or `artifact`, a caller from another tenant sees `NotFound` (no existence
//! leak), a mismatching expected digest is `DataLoss`, the resume offset may
//! be at most the artifact length, and the chunk size is clamped into
//! `[MIN_TRANSFER_CHUNK_BYTES, MAX_TRANSFER_CHUNK_BYTES]`. The chunk/ack
//! stream loop itself is exercised end to end in `tests/e2e.rs`.

use super::*;

const SEQ: &str = "00000000-0000-0000-0000-000000000201";
const INST: &str = "00000000-0000-0000-0000-000000000202";
const FOREIGN_INST: &str = "00000000-0000-0000-0000-000000000203";

async fn artifact_service() -> (Orch8GrpcService, Arc<dyn StorageBackend>) {
    let storage: Arc<dyn StorageBackend> = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(
                orch8_storage::artifacts::ObjectArtifactStore::memory(),
            )),
    );
    (Orch8GrpcService::new(Arc::clone(&storage)), storage)
}

async fn seed_instance(storage: &Arc<dyn StorageBackend>, inst_id: &str, tenant: &str) {
    let sequence: SequenceDefinition = serde_json::from_value(serde_json::json!({
        "id": SEQ,
        "tenant_id": tenant,
        "namespace": "default",
        "name": format!("seq_{SEQ}"),
        "version": 1,
        "deprecated": false,
        "blocks": [{"type": "step", "id": "step_1", "handler": "noop", "params": {}}],
        "created_at": "2024-01-01T00:00:00Z"
    }))
    .unwrap();
    // The same sequence id may back several instances in one storage.
    if storage
        .get_sequence(SequenceId::from_uuid(Uuid::parse_str(SEQ).unwrap()))
        .await
        .unwrap()
        .is_none()
    {
        storage.create_sequence(&sequence).await.unwrap();
    }
    let instance: TaskInstance = serde_json::from_value(serde_json::json!({
        "id": inst_id,
        "sequence_id": SEQ,
        "tenant_id": tenant,
        "namespace": "default",
        "state": "scheduled",
        "priority": "Normal",
        "timezone": "UTC",
        "metadata": {},
        "context": {"data": {}, "config": {}, "audit": [], "runtime": {}},
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z"
    }))
    .unwrap();
    storage.create_instance(&instance).await.unwrap();
}

fn open_for(object_key: &str) -> proto::ArtifactTransferOpen {
    proto::ArtifactTransferOpen {
        object_key: object_key.into(),
        resume_offset: 0,
        chunk_bytes: 16 * 1024,
        expected_sha256: Vec::new(),
        transfer_kind: "continuity".into(),
    }
}

async fn prepared_transfer(
    bytes: &[u8],
) -> (
    Orch8GrpcService,
    Arc<dyn StorageBackend>,
    proto::ArtifactTransferOpen,
) {
    let (service, storage) = artifact_service().await;
    seed_instance(&storage, INST, "test").await;
    let artifact = storage
        .put_artifact(
            InstanceId::from_uuid(Uuid::parse_str(INST).unwrap()),
            "application/octet-stream",
            bytes.to_vec().into(),
        )
        .await
        .unwrap();
    (service, storage, open_for(&artifact.key))
}

async fn open_status(service: &Orch8GrpcService, open: proto::ArtifactTransferOpen) -> Status {
    service
        .prepare_artifact_transfer(open, None)
        .await
        .err()
        .expect("open must be rejected")
}

#[tokio::test]
async fn valid_open_prepares_the_full_artifact_with_its_digest() {
    let bytes: Vec<u8> = (0_u32..10_000)
        .map(|v| u8::try_from(v % 251).unwrap())
        .collect();
    let (service, _storage, open) = prepared_transfer(&bytes).await;
    let prepared = service.prepare_artifact_transfer(open, None).await.unwrap();
    assert_eq!(prepared.total_bytes, 10_000);
    assert_eq!(prepared.bytes, bytes);
    assert_eq!(prepared.object_digest, Sha256::digest(&bytes).to_vec());
    assert_eq!(prepared.resume_offset, 0);
    assert_eq!(prepared.chunk_bytes, 16 * 1024);
    assert_eq!(prepared.transfer_kind, "continuity");
}

#[tokio::test]
async fn artifact_transfer_kind_is_accepted() {
    let (service, _storage, open) = prepared_transfer(b"payload").await;
    let open = proto::ArtifactTransferOpen {
        transfer_kind: "artifact".into(),
        ..open
    };
    let prepared = service.prepare_artifact_transfer(open, None).await.unwrap();
    assert_eq!(prepared.transfer_kind, "artifact");
}

#[tokio::test]
async fn malformed_opens_are_invalid_argument() {
    let (service, _storage, valid) = prepared_transfer(b"payload").await;
    let long_key = format!("{INST}/{}", "k".repeat(1025 - INST.len() - 1));
    assert_eq!(long_key.len(), 1_025);
    let cases = [
        ("unknown transfer kind", "blob", valid.object_key.clone()),
        (
            "transfer kind is case sensitive",
            "Continuity",
            valid.object_key.clone(),
        ),
        ("empty object key", "continuity", String::new()),
        ("1025-byte object key", "continuity", long_key),
        (
            "non-uuid key prefix",
            "continuity",
            "not-a-uuid/object".to_owned(),
        ),
    ];
    for (case, kind, key) in cases {
        let open = proto::ArtifactTransferOpen {
            transfer_kind: kind.into(),
            ..open_for(&key)
        };
        assert_eq!(
            open_status(&service, open).await.code(),
            tonic::Code::InvalidArgument,
            "case: {case}"
        );
    }
}

#[tokio::test]
async fn object_key_of_1024_bytes_passes_the_length_check() {
    let (service, storage) = artifact_service().await;
    seed_instance(&storage, INST, "test").await;
    let key = format!("{INST}/{}", "k".repeat(1024 - INST.len() - 1));
    assert_eq!(key.len(), 1_024);
    // Length is accepted; the (missing) artifact yields NotFound instead.
    assert_eq!(
        open_status(&service, open_for(&key)).await.code(),
        tonic::Code::NotFound
    );
}

#[tokio::test]
async fn unknown_instance_is_not_found() {
    let (service, _storage) = artifact_service().await;
    let key = format!("{INST}/object");
    assert_eq!(
        open_status(&service, open_for(&key)).await.code(),
        tonic::Code::NotFound
    );
}

#[tokio::test]
async fn missing_artifact_of_a_known_instance_is_not_found() {
    let (service, storage) = artifact_service().await;
    seed_instance(&storage, INST, "test").await;
    let key = format!("{INST}/00000000-0000-0000-0000-000000000099");
    assert_eq!(
        open_status(&service, open_for(&key)).await.code(),
        tonic::Code::NotFound
    );
}

#[tokio::test]
async fn cross_tenant_artifact_read_is_not_found() {
    let (service, storage, _own) = prepared_transfer(b"payload").await;
    seed_instance(&storage, FOREIGN_INST, "foreign").await;
    let foreign_artifact = storage
        .put_artifact(
            InstanceId::from_uuid(Uuid::parse_str(FOREIGN_INST).unwrap()),
            "application/octet-stream",
            b"foreign-bytes".to_vec().into(),
        )
        .await
        .unwrap();
    let caller = TenantId::unchecked("test");
    let status = service
        .prepare_artifact_transfer(open_for(&foreign_artifact.key), Some(&caller))
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn same_tenant_caller_reads_its_artifact() {
    let (service, _storage, open) = prepared_transfer(b"payload").await;
    let caller = TenantId::unchecked("test");
    let prepared = service
        .prepare_artifact_transfer(open, Some(&caller))
        .await
        .unwrap();
    assert_eq!(prepared.bytes, b"payload");
}

#[tokio::test]
async fn mismatching_expected_digest_is_data_loss() {
    let (service, _storage, open) = prepared_transfer(b"authentic payload").await;
    let open = proto::ArtifactTransferOpen {
        expected_sha256: Sha256::digest(b"attacker-controlled payload").to_vec(),
        ..open
    };
    assert_eq!(
        open_status(&service, open).await.code(),
        tonic::Code::DataLoss
    );
}

#[tokio::test]
async fn matching_expected_digest_is_accepted() {
    let bytes = b"authentic payload".to_vec();
    let (service, _storage, open) = prepared_transfer(&bytes).await;
    let open = proto::ArtifactTransferOpen {
        expected_sha256: Sha256::digest(&bytes).to_vec(),
        ..open
    };
    assert!(service.prepare_artifact_transfer(open, None).await.is_ok());
}

#[tokio::test]
async fn resume_offset_past_the_end_is_out_of_range() {
    let (service, _storage, open) = prepared_transfer(&[0_u8; 100]).await;
    let open = proto::ArtifactTransferOpen {
        resume_offset: 101,
        ..open
    };
    assert_eq!(
        open_status(&service, open).await.code(),
        tonic::Code::OutOfRange
    );
}

#[tokio::test]
async fn resume_offset_exactly_at_the_end_is_accepted() {
    let (service, _storage, open) = prepared_transfer(&[0_u8; 100]).await;
    let open = proto::ArtifactTransferOpen {
        resume_offset: 100,
        ..open
    };
    let prepared = service.prepare_artifact_transfer(open, None).await.unwrap();
    assert_eq!(prepared.resume_offset, 100);
    assert_eq!(prepared.total_bytes, 100);
}

#[tokio::test]
async fn empty_artifact_opens_at_offset_zero() {
    let (service, _storage, open) = prepared_transfer(&[]).await;
    let prepared = service.prepare_artifact_transfer(open, None).await.unwrap();
    assert_eq!(prepared.total_bytes, 0);
    assert_eq!(prepared.resume_offset, 0);
    assert!(prepared.bytes.is_empty());
}

#[tokio::test]
async fn requested_chunk_size_is_clamped_into_the_negotiable_range() {
    let (service, _storage, open) = prepared_transfer(b"payload").await;
    for (requested, negotiated) in [
        (0, MIN_TRANSFER_CHUNK_BYTES),
        (MIN_TRANSFER_CHUNK_BYTES - 1, MIN_TRANSFER_CHUNK_BYTES),
        (MAX_TRANSFER_CHUNK_BYTES + 1, MAX_TRANSFER_CHUNK_BYTES),
        (u32::MAX, MAX_TRANSFER_CHUNK_BYTES),
    ] {
        let open = proto::ArtifactTransferOpen {
            chunk_bytes: requested,
            ..open.clone()
        };
        let prepared = service.prepare_artifact_transfer(open, None).await.unwrap();
        assert_eq!(prepared.chunk_bytes, negotiated, "requested {requested}");
    }
}
