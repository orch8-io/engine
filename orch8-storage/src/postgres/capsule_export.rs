//! Durable source capture and immutable transport sealing transactions.
use super::{
    PostgresStorage,
    continuity::{decode, encode, state_name},
};
use crate::capsule_export::{
    CapsuleCutCapture, CapsuleCutRefusal, CapsuleCutRequest, CapsuleSourceCut,
    CapsuleSourceSnapshot, invalid, same_location, transferring, validate_seal,
};
use orch8_types::{
    continuity::{CapsuleId, CapsuleManifest, ContinuityExecution, OwnershipState},
    error::StorageError,
    ids::TenantId,
};
use sqlx::Row;

type DbRow = sqlx::postgres::PgRow;
fn cut(row: &DbRow) -> Result<CapsuleSourceCut, StorageError> {
    Ok(CapsuleSourceCut {
        id: CapsuleId::from_uuid(row.get("capsule_id")),
        request: decode(row.get("request"))?,
        snapshot: row
            .try_get::<Option<serde_json::Value>, _>("snapshot")?
            .map(decode)
            .transpose()?,
        manifest: row
            .try_get::<Option<serde_json::Value>, _>("manifest")?
            .map(decode)
            .transpose()?,
        sealed_payload: row.try_get("sealed_payload")?,
    })
}
pub(super) async fn owner(
    conn: &mut sqlx::PgConnection,
    expected: &ContinuityExecution,
) -> Result<Option<ContinuityExecution>, StorageError> {
    let row=sqlx::query("SELECT record,state FROM continuity_executions WHERE tenant_id=$1 AND continuity_id=$2 AND epoch=$3 AND owner_runtime_id=$4 FOR UPDATE").bind(expected.tenant_id.as_str()).bind(expected.continuity_id.into_uuid()).bind(i64::try_from(expected.epoch.get()).map_err(|_|invalid())?).bind(expected.owner_runtime_id.into_uuid()).fetch_optional(&mut *conn).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let current: ContinuityExecution = decode(row.get("record"))?;
    if !same_location(&current, expected)
        || state_name(current.state)? != row.get::<String, _>("state")
    {
        return Ok(None);
    }
    Ok(Some(current))
}
// Read only ownership metadata before taking the owner lock. Loading the full
// frozen snapshot here would keep a second, potentially 128 MiB copy alive.
async fn load_request(
    conn: &mut sqlx::PgConnection,
    tenant: &TenantId,
    id: CapsuleId,
) -> Result<CapsuleCutRequest, StorageError> {
    let row = sqlx::query("SELECT request FROM capsule_source_cuts WHERE tenant_id=$1 AND capsule_id=$2 AND octet_length(request::text)<=1048576")
        .bind(tenant.as_str()).bind(id.into_uuid())
        .fetch_optional(&mut *conn).await?.ok_or_else(invalid)?;
    decode(row.get("request"))
}
async fn load(
    conn: &mut sqlx::PgConnection,
    tenant: &TenantId,
    id: CapsuleId,
) -> Result<(CapsuleSourceCut, String), StorageError> {
    let row=sqlx::query("SELECT * FROM capsule_source_cuts WHERE tenant_id=$1 AND capsule_id=$2 AND (snapshot IS NULL OR octet_length(snapshot::text)<=134217728) AND (sealed_payload IS NULL OR octet_length(sealed_payload)<=67108928) AND octet_length(request::text)<=1048576").bind(tenant.as_str()).bind(id.into_uuid()).fetch_optional(&mut *conn).await?.ok_or_else(invalid)?;
    Ok((cut(&row)?, row.get("state")))
}
async fn save_owner(
    conn: &mut sqlx::PgConnection,
    value: &ContinuityExecution,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE continuity_executions SET state=$1,record=$2,updated_at=$3,epoch=$4,owner_runtime_id=$5 WHERE tenant_id=$6 AND continuity_id=$7")
        .bind(state_name(value.state)?).bind(encode(value)?).bind(value.updated_at)
        .bind(i64::try_from(value.epoch.get()).map_err(|_|invalid())?).bind(value.owner_runtime_id.into_uuid())
        .bind(value.tenant_id.as_str()).bind(value.continuity_id.into_uuid()).execute(&mut *conn).await?;
    Ok(())
}

pub(super) async fn capture(
    store: &PostgresStorage,
    request: &CapsuleCutRequest,
) -> Result<CapsuleCutCapture, StorageError> {
    request.validate()?;
    let mut tx = store.pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;
    let outcome = capture_on(&mut tx, request).await?;
    if matches!(outcome, CapsuleCutCapture::Ready(_)) {
        tx.commit().await?;
    } else {
        tx.rollback().await?;
    }
    Ok(outcome)
}
async fn capture_on(
    conn: &mut sqlx::PgConnection,
    request: &CapsuleCutRequest,
) -> Result<CapsuleCutCapture, StorageError> {
    if request.mobile_access.is_some() {
        return Ok(CapsuleCutCapture::Refused(CapsuleCutRefusal::Ownership));
    }
    let Some(current) = owner(conn, &request.owner).await? else {
        return Ok(CapsuleCutCapture::Refused(CapsuleCutRefusal::Ownership));
    };
    let row=sqlx::query("SELECT * FROM capsule_source_cuts WHERE tenant_id=$1 AND continuity_id=$2 AND epoch=$3 AND state IN ('captured','sealed') AND (snapshot IS NULL OR octet_length(snapshot::text)<=134217728) AND (sealed_payload IS NULL OR octet_length(sealed_payload)<=67108928) AND octet_length(request::text)<=1048576").bind(current.tenant_id.as_str()).bind(current.continuity_id.into_uuid()).bind(i64::try_from(current.epoch.get()).map_err(|_|invalid())?).fetch_optional(&mut *conn).await?;
    if let Some(row) = row {
        let existing = cut(&row)?;
        if current.state != OwnershipState::Transferring || !existing.request.matches(request) {
            return Ok(CapsuleCutCapture::Refused(CapsuleCutRefusal::Ownership));
        }
        return Ok(CapsuleCutCapture::Ready(Box::new(existing)));
    }
    if current.state != OwnershipState::Owned || request.owner.state != OwnershipState::Owned {
        return Ok(CapsuleCutCapture::Refused(CapsuleCutRefusal::Ownership));
    }
    let exists =
        sqlx::query("SELECT 1 FROM task_instances WHERE tenant_id=$1 AND id=$2 FOR UPDATE")
            .bind(current.tenant_id.as_str())
            .bind(current.current_instance_id.into_uuid())
            .fetch_optional(&mut *conn)
            .await?;
    if exists.is_none() {
        return Ok(CapsuleCutCapture::Refused(CapsuleCutRefusal::Ownership));
    }
    let snapshot = match super::capsule_source_snapshot::read(conn, &current).await? {
        super::capsule_source_snapshot::Read::Ready(snapshot) => snapshot,
        super::capsule_source_snapshot::Read::Refused(reason) => {
            return Ok(CapsuleCutCapture::Refused(reason));
        }
    };
    let id = CapsuleId::new();
    let frozen = transferring(&current, request.now);
    save_owner(conn, &frozen).await?;
    sqlx::query("UPDATE task_instances SET state='paused',claimed_at=NULL,next_fire_at=NULL,updated_at=$1 WHERE id=$2 AND tenant_id=$3").bind(request.now).bind(current.current_instance_id.into_uuid()).bind(current.tenant_id.as_str()).execute(&mut *conn).await?;
    let json = encode(snapshot.as_ref())?;
    if serde_json::to_vec(&json)?.len() > CapsuleSourceSnapshot::MAX_CAPTURE_BYTES {
        return Err(crate::capsule_export::too_large());
    }
    sqlx::query("INSERT INTO capsule_source_cuts(capsule_id,tenant_id,continuity_id,epoch,source_instance_id,source_runtime_id,state,request,snapshot,created_at) VALUES($1,$2,$3,$4,$5,$6,'captured',$7,$8,$9)").bind(id.into_uuid()).bind(current.tenant_id.as_str()).bind(current.continuity_id.into_uuid()).bind(i64::try_from(current.epoch.get()).map_err(|_|invalid())?).bind(current.current_instance_id.into_uuid()).bind(current.owner_runtime_id.into_uuid()).bind(encode(request)?).bind(json).bind(request.now).execute(&mut *conn).await?;
    Ok(CapsuleCutCapture::Ready(Box::new(CapsuleSourceCut {
        id,
        request: request.clone(),
        snapshot: Some(*snapshot),
        manifest: None,
        sealed_payload: None,
    })))
}

pub(super) async fn seal(
    store: &PostgresStorage,
    tenant: &TenantId,
    id: CapsuleId,
    manifest: &CapsuleManifest,
    sealed: &[u8],
) -> Result<CapsuleSourceCut, StorageError> {
    let mut tx = store.pool.begin().await?;
    let request = load_request(&mut tx, tenant, id).await?;
    let current = owner(&mut tx, &request.owner).await?.ok_or_else(invalid)?;
    if current.state != OwnershipState::Transferring {
        return Err(invalid());
    }
    let (mut stored, phase) = load(&mut tx, tenant, id).await?;
    validate_seal(&stored, manifest, sealed)?;
    if phase == "sealed" {
        let existing = stored.manifest.as_ref().ok_or_else(invalid)?;
        let mut candidate = manifest.clone();
        candidate.payload_artifact = existing.payload_artifact.clone();
        if &candidate != existing {
            return Err(invalid());
        }
        tx.commit().await?;
        return Ok(stored);
    }
    if phase != "captured" {
        return Err(invalid());
    }
    sqlx::query("INSERT INTO execution_capsules(id,continuity_id,tenant_id,expires_at,manifest) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING").bind(manifest.capsule_id.into_uuid()).bind(manifest.continuity_id.into_uuid()).bind(manifest.tenant_id.as_str()).bind(manifest.expires_at).bind(encode(manifest)?).execute(&mut *tx).await?;
    let row = sqlx::query("SELECT manifest FROM execution_capsules WHERE id=$1")
        .bind(id.into_uuid())
        .fetch_one(&mut *tx)
        .await?;
    let saved: CapsuleManifest = decode(row.get("manifest"))?;
    if &saved != manifest {
        return Err(invalid());
    }
    sqlx::query("UPDATE capsule_source_cuts SET state='sealed',snapshot=NULL,manifest=$1,sealed_payload=$2 WHERE tenant_id=$3 AND capsule_id=$4 AND state='captured'").bind(encode(manifest)?).bind(sealed).bind(tenant.as_str()).bind(id.into_uuid()).execute(&mut *tx).await?;
    tx.commit().await?;
    stored.snapshot = None;
    stored.manifest = Some(saved);
    stored.sealed_payload = Some(sealed.to_vec());
    Ok(stored)
}

pub(super) async fn abort(
    store: &PostgresStorage,
    tenant: &TenantId,
    id: CapsuleId,
) -> Result<bool, StorageError> {
    let mut tx = store.pool.begin().await?;
    let request = load_request(&mut tx, tenant, id).await?;
    let Some(current) = owner(&mut tx, &request.owner).await? else {
        return Ok(false);
    };
    let (stored, phase) = load(&mut tx, tenant, id).await?;
    if phase != "captured" || current.state != OwnershipState::Transferring {
        return Ok(false);
    }
    let snapshot = stored.snapshot.as_ref().ok_or_else(invalid)?;
    let restored = ContinuityExecution {
        state: OwnershipState::Owned,
        updated_at: chrono::Utc::now(),
        ..current
    };
    // Release the cut first inside this transaction, so the database guards
    // authorize restoration only as part of an explicit atomic abort.
    sqlx::query("UPDATE capsule_source_cuts SET state='aborted',snapshot=NULL WHERE tenant_id=$1 AND capsule_id=$2 AND state='captured'").bind(tenant.as_str()).bind(id.into_uuid()).execute(&mut *tx).await?;
    save_owner(&mut tx, &restored).await?;
    sqlx::query("UPDATE task_instances SET state=$1,next_fire_at=$2,updated_at=$3 WHERE id=$4 AND tenant_id=$5 AND state='paused' AND claimed_at IS NULL AND updated_at=$6").bind(snapshot.instance.state.to_string()).bind(snapshot.instance.next_fire_at).bind(restored.updated_at).bind(restored.current_instance_id.into_uuid()).bind(tenant.as_str()).bind(stored.request.now).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(true)
}

/// Compact a sealed cut without loading its (potentially 64 MiB) ciphertext.
pub(super) async fn acknowledge(
    store: &PostgresStorage,
    tenant: &TenantId,
    id: CapsuleId,
    accepted: &ContinuityExecution,
) -> Result<crate::capsule_export::CapsuleCutReceipt, StorageError> {
    use crate::capsule_export::{CapsuleCutReceipt, validate_acknowledgement};
    let mut tx = store.pool.begin().await?;
    let request = load_request(&mut tx, tenant, id).await?;
    let row = sqlx::query("SELECT record,state,epoch,owner_runtime_id FROM continuity_executions WHERE tenant_id=$1 AND continuity_id=$2 FOR UPDATE").bind(tenant.as_str()).bind(request.owner.continuity_id.into_uuid()).fetch_optional(&mut *tx).await?.ok_or_else(invalid)?;
    let current: ContinuityExecution = decode(row.get("record"))?;
    if i64::try_from(current.epoch.get()).map_err(|_| invalid())? != row.get::<i64, _>("epoch")
        || current.owner_runtime_id.into_uuid() != row.get::<uuid::Uuid, _>("owner_runtime_id")
    {
        return Err(invalid());
    }
    if current.tenant_id != *tenant
        || current.continuity_id != request.owner.continuity_id
        || state_name(current.state)? != row.get::<String, _>("state")
    {
        return Err(invalid());
    }
    let row = sqlx::query("SELECT state,manifest,acknowledgement FROM capsule_source_cuts WHERE tenant_id=$1 AND capsule_id=$2 AND octet_length(manifest::text)<=1048576 AND (acknowledgement IS NULL OR octet_length(acknowledgement::text)<=1048576)").bind(tenant.as_str()).bind(id.into_uuid()).fetch_optional(&mut *tx).await?.ok_or_else(invalid)?;
    let manifest: CapsuleManifest = decode(row.get("manifest"))?;
    validate_acknowledgement(&request, id, &manifest, accepted)?;
    match row.get::<String, _>("state").as_str() {
        "acknowledged" => {
            let recorded: ContinuityExecution = decode(row.get("acknowledgement"))?;
            if !same_location(&recorded, accepted) {
                return Err(invalid());
            }
            tx.commit().await?;
            return Ok(CapsuleCutReceipt {
                manifest,
                accepted_owner: recorded,
            });
        }
        "sealed" | "retired" => {}
        _ => return Err(invalid()),
    }
    if same_location(&current, &request.owner) && current.state == OwnershipState::Transferring {
        save_owner(&mut tx, accepted).await?;
        let location = orch8_types::continuity::ContinuityLocation {
            continuity_id: accepted.continuity_id,
            tenant_id: accepted.tenant_id.clone(),
            epoch: accepted.epoch,
            runtime_id: accepted.owner_runtime_id,
            instance_id: accepted.current_instance_id,
            handoff_id: None,
            entered_at: accepted.updated_at,
        };
        sqlx::query("INSERT INTO continuity_locations(tenant_id,continuity_id,epoch,runtime_id,instance_id,handoff_id,record,entered_at) VALUES($1,$2,$3,$4,$5,NULL,$6,$7)")
            .bind(tenant.as_str()).bind(accepted.continuity_id.into_uuid())
            .bind(i64::try_from(accepted.epoch.get()).map_err(|_|invalid())?).bind(accepted.owner_runtime_id.into_uuid())
            .bind(accepted.current_instance_id.into_uuid()).bind(encode(&location)?).bind(accepted.updated_at)
            .execute(&mut *tx).await?;
    } else if current.epoch < accepted.epoch
        || (current.epoch == accepted.epoch && !same_location(&current, accepted))
    {
        return Err(invalid());
    }
    sqlx::query("UPDATE capsule_source_cuts SET state='acknowledged',snapshot=NULL,sealed_payload=NULL,acknowledgement=$1 WHERE tenant_id=$2 AND capsule_id=$3 AND state IN ('sealed','retired')").bind(encode(accepted)?).bind(tenant.as_str()).bind(id.into_uuid()).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO capsule_artifact_retirements(artifact_key,tenant_id,capsule_id,next_sweep_at) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING")
        .bind(&manifest.payload_artifact.key).bind(tenant.as_str()).bind(id.into_uuid()).bind(chrono::Utc::now()).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(CapsuleCutReceipt {
        manifest,
        accepted_owner: accepted.clone(),
    })
}

pub(super) async fn publishable(
    store: &PostgresStorage,
    tenant: &TenantId,
    id: CapsuleId,
) -> Result<bool, StorageError> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM capsule_source_cuts c JOIN continuity_executions e ON e.tenant_id=c.tenant_id AND e.continuity_id=c.continuity_id WHERE c.tenant_id=$1 AND c.capsule_id=$2 AND c.state='sealed' AND e.state='transferring' AND e.epoch=c.epoch AND e.owner_runtime_id=c.source_runtime_id AND e.record->>'current_instance_id'=c.source_instance_id::text)").bind(tenant.as_str()).bind(id.into_uuid()).fetch_one(&store.pool).await?)
}

pub(super) async fn claim_retired_artifacts(
    store: &PostgresStorage,
    now: chrono::DateTime<chrono::Utc>,
    retry_at: chrono::DateTime<chrono::Utc>,
    limit: u32,
) -> Result<Vec<String>, StorageError> {
    if retry_at <= now {
        return Err(invalid());
    }
    if limit == 0 {
        return Ok(Vec::new());
    }
    let keys = sqlx::query_scalar("WITH due AS (SELECT artifact_key FROM capsule_artifact_retirements WHERE next_sweep_at<=$1 ORDER BY next_sweep_at,artifact_key FOR UPDATE SKIP LOCKED LIMIT $3) UPDATE capsule_artifact_retirements r SET next_sweep_at=$2 FROM due WHERE r.artifact_key=due.artifact_key RETURNING r.artifact_key")
        .bind(now).bind(retry_at).bind(i64::from(limit.min(1000))).fetch_all(&store.pool).await?;
    Ok(keys)
}
