//! Coverage tests for resumable continuity artifact transfers: object-key
//! and transfer-kind validation, digest (tamper) checks, resume-offset
//! bounds, and chunk-size clamping on the negotiated transfer.
//!
//! Count contract: 31 independently named unit tests.

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

#[tokio::test]
async fn coverage_artifact_001_valid_open_prepares_full_transfer() {
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
async fn coverage_artifact_002_artifact_kind_is_accepted() {
    let (service, _storage, open) = prepared_transfer(b"payload").await;
    let open = proto::ArtifactTransferOpen {
        transfer_kind: "artifact".into(),
        ..open
    };
    let prepared = service.prepare_artifact_transfer(open, None).await.unwrap();
    assert_eq!(prepared.transfer_kind, "artifact");
}

#[tokio::test]
async fn coverage_artifact_003_unknown_transfer_kind_is_invalid_argument() {
    let (service, _storage, open) = prepared_transfer(b"payload").await;
    let open = proto::ArtifactTransferOpen {
        transfer_kind: "blob".into(),
        ..open
    };
    let status = service
        .prepare_artifact_transfer(open, None)
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn coverage_artifact_004_empty_transfer_kind_is_invalid_argument() {
    let (service, _storage, open) = prepared_transfer(b"payload").await;
    let open = proto::ArtifactTransferOpen {
        transfer_kind: String::new(),
        ..open
    };
    let status = service
        .prepare_artifact_transfer(open, None)
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn coverage_artifact_005_transfer_kind_is_case_sensitive() {
    let (service, _storage, open) = prepared_transfer(b"payload").await;
    let open = proto::ArtifactTransferOpen {
        transfer_kind: "Continuity".into(),
        ..open
    };
    let status = service
        .prepare_artifact_transfer(open, None)
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn coverage_artifact_006_empty_object_key_is_invalid_argument() {
    let (service, _storage) = artifact_service().await;
    let status = service
        .prepare_artifact_transfer(open_for(""), None)
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn coverage_artifact_007_object_key_over_1024_bytes_is_invalid_argument() {
    let (service, _storage) = artifact_service().await;
    let key = format!("{INST}/{}", "k".repeat(1025 - INST.len() - 1));
    assert_eq!(key.len(), 1_025);
    let status = service
        .prepare_artifact_transfer(open_for(&key), None)
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn coverage_artifact_008_object_key_at_1024_bytes_passes_length_check() {
    let (service, storage) = artifact_service().await;
    seed_instance(&storage, INST, "test").await;
    let key = format!("{INST}/{}", "k".repeat(1024 - INST.len() - 1));
    assert_eq!(key.len(), 1_024);
    // Length is accepted; the (missing) artifact yields NotFound instead.
    let status = service
        .prepare_artifact_transfer(open_for(&key), None)
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn coverage_artifact_009_object_key_with_non_uuid_prefix_is_invalid_argument() {
    let (service, _storage) = artifact_service().await;
    let status = service
        .prepare_artifact_transfer(open_for("not-a-uuid/object"), None)
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn coverage_artifact_010_object_key_without_separator_is_invalid_argument() {
    let (service, _storage) = artifact_service().await;
    let status = service
        .prepare_artifact_transfer(open_for("singleton"), None)
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn coverage_artifact_011_unknown_instance_is_not_found() {
    let (service, _storage) = artifact_service().await;
    let key = format!("{INST}/object");
    let status = service
        .prepare_artifact_transfer(open_for(&key), None)
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn coverage_artifact_012_cross_tenant_caller_is_not_found() {
    let (service, storage, open) = prepared_transfer(b"payload").await;
    seed_instance(&storage, FOREIGN_INST, "foreign").await;
    let foreign_artifact = storage
        .put_artifact(
            InstanceId::from_uuid(Uuid::parse_str(FOREIGN_INST).unwrap()),
            "application/octet-stream",
            b"foreign-bytes".to_vec().into(),
        )
        .await
        .unwrap();
    drop(open);
    let caller = TenantId::unchecked("test");
    let status = service
        .prepare_artifact_transfer(open_for(&foreign_artifact.key), Some(&caller))
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn coverage_artifact_013_matching_tenant_caller_is_accepted() {
    let (service, _storage, open) = prepared_transfer(b"payload").await;
    let caller = TenantId::unchecked("test");
    let prepared = service
        .prepare_artifact_transfer(open, Some(&caller))
        .await
        .unwrap();
    assert_eq!(prepared.total_bytes, 7);
    assert_eq!(prepared.bytes, b"payload");
}

#[tokio::test]
async fn coverage_artifact_014_anonymous_caller_is_permissive() {
    let (service, _storage, open) = prepared_transfer(b"payload").await;
    let prepared = service.prepare_artifact_transfer(open, None).await.unwrap();
    assert_eq!(prepared.total_bytes, 7);
}

#[tokio::test]
async fn coverage_artifact_015_missing_artifact_is_not_found() {
    let (service, storage) = artifact_service().await;
    seed_instance(&storage, INST, "test").await;
    let key = format!("{INST}/00000000-0000-0000-0000-000000000099");
    let status = service
        .prepare_artifact_transfer(open_for(&key), None)
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn coverage_artifact_016_tampered_expected_digest_is_data_loss() {
    let bytes = b"authentic payload".to_vec();
    let (service, _storage, open) = prepared_transfer(&bytes).await;
    let forged = Sha256::digest(b"attacker-controlled payload").to_vec();
    let open = proto::ArtifactTransferOpen {
        expected_sha256: forged,
        ..open
    };
    let status = service
        .prepare_artifact_transfer(open, None)
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::DataLoss);
}

#[tokio::test]
async fn coverage_artifact_017_matching_expected_digest_is_accepted() {
    let bytes = b"authentic payload".to_vec();
    let (service, _storage, open) = prepared_transfer(&bytes).await;
    let open = proto::ArtifactTransferOpen {
        expected_sha256: Sha256::digest(&bytes).to_vec(),
        ..open
    };
    assert!(service.prepare_artifact_transfer(open, None).await.is_ok());
}

#[tokio::test]
async fn coverage_artifact_018_empty_expected_digest_skips_the_check() {
    let (service, _storage, open) = prepared_transfer(b"payload").await;
    assert!(open.expected_sha256.is_empty());
    assert!(service.prepare_artifact_transfer(open, None).await.is_ok());
}

#[tokio::test]
async fn coverage_artifact_019_resume_offset_beyond_total_is_out_of_range() {
    let (service, _storage, open) = prepared_transfer(&[0_u8; 100]).await;
    let open = proto::ArtifactTransferOpen {
        resume_offset: 101,
        ..open
    };
    let status = service
        .prepare_artifact_transfer(open, None)
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::OutOfRange);
}

#[tokio::test]
async fn coverage_artifact_020_resume_offset_exactly_at_total_is_accepted() {
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
async fn coverage_artifact_021_resume_offset_one_before_end_is_accepted() {
    let (service, _storage, open) = prepared_transfer(&[0_u8; 100]).await;
    let open = proto::ArtifactTransferOpen {
        resume_offset: 99,
        ..open
    };
    assert!(service.prepare_artifact_transfer(open, None).await.is_ok());
}

#[tokio::test]
async fn coverage_artifact_022_resume_offset_on_empty_artifact_is_out_of_range() {
    let (service, _storage, open) = prepared_transfer(&[]).await;
    let open = proto::ArtifactTransferOpen {
        resume_offset: 1,
        ..open
    };
    let status = service
        .prepare_artifact_transfer(open, None)
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::OutOfRange);
}

#[tokio::test]
async fn coverage_artifact_023_zero_chunk_bytes_clamps_up_to_minimum() {
    let (service, _storage, open) = prepared_transfer(b"payload").await;
    let open = proto::ArtifactTransferOpen {
        chunk_bytes: 0,
        ..open
    };
    let prepared = service.prepare_artifact_transfer(open, None).await.unwrap();
    assert_eq!(prepared.chunk_bytes, MIN_TRANSFER_CHUNK_BYTES);
}

#[tokio::test]
async fn coverage_artifact_024_tiny_chunk_bytes_clamps_up_to_minimum() {
    let (service, _storage, open) = prepared_transfer(b"payload").await;
    let open = proto::ArtifactTransferOpen {
        chunk_bytes: 1,
        ..open
    };
    let prepared = service.prepare_artifact_transfer(open, None).await.unwrap();
    assert_eq!(prepared.chunk_bytes, 4 * 1024);
}

#[tokio::test]
async fn coverage_artifact_025_minimum_chunk_bytes_is_preserved() {
    let (service, _storage, open) = prepared_transfer(b"payload").await;
    let open = proto::ArtifactTransferOpen {
        chunk_bytes: MIN_TRANSFER_CHUNK_BYTES,
        ..open
    };
    let prepared = service.prepare_artifact_transfer(open, None).await.unwrap();
    assert_eq!(prepared.chunk_bytes, 4 * 1024);
}

#[tokio::test]
async fn coverage_artifact_026_maximum_chunk_bytes_is_preserved() {
    let (service, _storage, open) = prepared_transfer(b"payload").await;
    let open = proto::ArtifactTransferOpen {
        chunk_bytes: MAX_TRANSFER_CHUNK_BYTES,
        ..open
    };
    let prepared = service.prepare_artifact_transfer(open, None).await.unwrap();
    assert_eq!(prepared.chunk_bytes, 1024 * 1024);
}

#[tokio::test]
async fn coverage_artifact_027_excessive_chunk_bytes_clamps_down_to_maximum() {
    let (service, _storage, open) = prepared_transfer(b"payload").await;
    let open = proto::ArtifactTransferOpen {
        chunk_bytes: u32::MAX,
        ..open
    };
    let prepared = service.prepare_artifact_transfer(open, None).await.unwrap();
    assert_eq!(prepared.chunk_bytes, MAX_TRANSFER_CHUNK_BYTES);
}

#[tokio::test]
async fn coverage_artifact_028_digest_matches_sha256_of_stored_bytes() {
    let bytes: Vec<u8> = (0_u8..=255).cycle().take(5_000).collect();
    let (service, _storage, open) = prepared_transfer(&bytes).await;
    let prepared = service.prepare_artifact_transfer(open, None).await.unwrap();
    assert_eq!(prepared.object_digest, Sha256::digest(&bytes).to_vec());
    assert_ne!(
        prepared.object_digest,
        Sha256::digest(&bytes[..4_999]).to_vec()
    );
}

#[tokio::test]
async fn coverage_artifact_029_chunk_bounds_are_internally_ordered() {
    const { assert!(MIN_TRANSFER_CHUNK_BYTES <= MAX_TRANSFER_CHUNK_BYTES) };
    assert_eq!(MIN_TRANSFER_CHUNK_BYTES, 4 * 1024);
    assert_eq!(MAX_TRANSFER_CHUNK_BYTES, 1024 * 1024);
}

#[tokio::test]
async fn coverage_artifact_030_large_resume_offset_is_out_of_range() {
    let (service, _storage, open) = prepared_transfer(&[0_u8; 8]).await;
    let open = proto::ArtifactTransferOpen {
        resume_offset: u64::MAX,
        ..open
    };
    let status = service
        .prepare_artifact_transfer(open, None)
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::OutOfRange);
}

#[tokio::test]
async fn coverage_artifact_031_empty_artifact_accepts_zero_resume_offset() {
    let (service, _storage, open) = prepared_transfer(&[]).await;
    let prepared = service.prepare_artifact_transfer(open, None).await.unwrap();
    assert_eq!(prepared.total_bytes, 0);
    assert_eq!(prepared.resume_offset, 0);
    assert!(prepared.bytes.is_empty());
}
