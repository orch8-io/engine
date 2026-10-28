//! One transaction publishes a delegation and all of its recovery evidence.
use super::PostgresStorage;
use crate::delegation_publication::{
    DelegationPublication, DelegationPublishIntent, DelegationPublishResult as Publication,
    conflict, sequence_sha256,
};
use crate::worker_claim::WorkerClaimPrincipal;
use chrono::{DateTime, Utc};
use orch8_types::{
    continuity::{
        ContinuationGrant, ContinuityExecution, EffectDispatchOutcome, EffectState,
        RuntimeCapabilities,
    },
    error::StorageError,
    ids::TenantId,
};
use sqlx::Row;
use uuid::Uuid;

pub(super) async fn get(
    store: &PostgresStorage,
    tenant: &TenantId,
    id: Uuid,
) -> Result<Option<DelegationPublication>, StorageError> {
    let mut conn = store.pool.acquire().await?;
    get_on(&mut conn, tenant, id).await
}
pub(super) async fn get_on(
    conn: &mut sqlx::PgConnection,
    tenant: &TenantId,
    id: Uuid,
) -> Result<Option<DelegationPublication>, StorageError> {
    let row=sqlx::query("SELECT record::text, grant_id::text AS grant_scope, task_id::text AS task_scope, instance_id::text AS instance_scope FROM delegation_publications WHERE tenant_id=$1 AND delegation_id=$2::uuid").bind(tenant.as_str()).bind(id.to_string()).fetch_optional(&mut *conn).await?;
    let Some(row) = row else { return Ok(None) };
    let receipt: DelegationPublication = serde_json::from_str(&row.get::<String, _>("record"))?;
    receipt.validate()?;
    if receipt.delegation.tenant_id != *tenant
        || receipt.delegation.id.into_uuid() != id
        || receipt.delegation.grant_id.to_string() != row.get::<String, _>("grant_scope")
        || receipt.task_id.to_string() != row.get::<String, _>("task_scope")
        || receipt.instance_id.to_string() != row.get::<String, _>("instance_scope")
    {
        return Err(conflict());
    }
    Ok(Some(receipt))
}
pub(super) async fn publish(
    store: &PostgresStorage,
    intent: &DelegationPublishIntent,
) -> Result<Publication, StorageError> {
    intent.validate_stored()?;
    let mut tx = store.pool.begin().await?;
    match on(&mut tx, intent).await {
        Ok(result @ Publication::Published(_)) => {
            tx.commit().await?;
            Ok(result)
        }
        Ok(other) => {
            tx.rollback().await?;
            Ok(other)
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}
async fn clock(conn: &mut sqlx::PgConnection) -> Result<DateTime<Utc>, StorageError> {
    Ok(sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(conn)
        .await?)
}
async fn authority(
    conn: &mut sqlx::PgConnection,
    intent: &DelegationPublishIntent,
) -> Result<Option<DateTime<Utc>>, StorageError> {
    let mut expiry = intent.request.authority_expires_at;
    if let WorkerClaimPrincipal::ApiKey { tenant_id, key_id } = &intent.request.actor {
        let row=sqlx::query("SELECT capabilities_json,revoked,expires_at FROM api_keys WHERE tenant_id=$1 AND id=$2 FOR SHARE").bind(tenant_id.as_str()).bind(key_id).fetch_optional(&mut *conn).await?.ok_or_else(conflict)?;
        let capabilities: Vec<orch8_types::api_key::ApiCapability> =
            serde_json::from_str(&row.get::<String, _>("capabilities_json"))?;
        if row.get::<bool, _>("revoked")
            || !capabilities.contains(&orch8_types::api_key::ApiCapability::Operator)
        {
            return Err(conflict());
        }
        let key_expiry: Option<DateTime<Utc>> = row.get("expires_at");
        if let Some(key) = key_expiry {
            expiry = Some(expiry.map_or(key, |bound| bound.min(key)));
        }
    }
    Ok(expiry)
}
async fn on(
    conn: &mut sqlx::PgConnection,
    intent: &DelegationPublishIntent,
) -> Result<Publication, StorageError> {
    let d = &intent.request.delegation;
    let tenant = &d.tenant_id;
    let expiry = authority(conn, intent).await?;
    let mut ids = vec![intent.parent_owner.current_instance_id, intent.parent.id];
    ids.sort();
    ids.dedup();
    for id in ids {
        super::portable_effects::lock_instance_on(conn, tenant, id).await?;
    }
    // Lock logical ownership before grant/native rows, as handoff and GC do.
    let owner:Option<String>=sqlx::query_scalar("SELECT record::text FROM continuity_executions WHERE tenant_id=$1 AND continuity_id=$2::uuid AND epoch=$3 AND owner_runtime_id=$4::uuid AND state='owned' FOR UPDATE").bind(tenant.as_str()).bind(d.parent_continuity_id.to_string()).bind(i64::try_from(d.parent_epoch.get()).map_err(|_|conflict())?).bind(d.source_runtime_id.to_string()).fetch_optional(&mut *conn).await?;
    let grant=sqlx::query("SELECT record::text AS record,expires_at,nonce_sha256,state,continuity_id::text AS continuity_scope FROM continuation_grants WHERE tenant_id=$1 AND id=$2::uuid FOR UPDATE").bind(tenant.as_str()).bind(d.grant_id.to_string()).fetch_optional(&mut *conn).await?;
    let now = clock(conn).await?;
    if expiry.is_some_and(|expiry| expiry <= now) {
        return Err(conflict());
    }
    // A concurrent identical publisher may have committed while we waited.
    if let Some(receipt) = get_on(conn, tenant, d.id.into_uuid()).await? {
        return if receipt.matches(&intent.request)? {
            Ok(Publication::Published(Box::new(receipt)))
        } else {
            Err(conflict())
        };
    }
    let owner: ContinuityExecution = serde_json::from_str(&owner.ok_or_else(conflict)?)?;
    if !intent.owner_matches(&owner) {
        return Err(conflict());
    }
    let grant_row = grant.ok_or_else(conflict)?;
    let grant: ContinuationGrant = serde_json::from_str(&grant_row.get::<String, _>("record"))?;
    let grant_expiry = row_time(&grant_row, "expires_at")?;
    if grant_row.get::<String, _>("nonce_sha256") != intent.request.nonce_sha256
        || grant_row.get::<String, _>("state") != "active"
        || grant_row.get::<String, _>("continuity_scope") != d.parent_continuity_id.to_string()
        || grant_expiry.timestamp_micros() != grant.expires_at.timestamp_micros()
        || grant_expiry <= now
    {
        return Err(conflict());
    }

    intent.request.live(&grant, now)?;
    let frozen:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=$1::uuid) OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=$1::uuid AND state IN ('captured','sealed'))").bind(owner.current_instance_id.to_string()).fetch_one(&mut *conn).await?;
    if frozen {
        return Err(conflict());
    }
    let runtimes = runtime_snapshots(conn, intent).await?;
    let source = runtimes
        .iter()
        .find(|caps| caps.runtime_id == d.source_runtime_id)
        .ok_or_else(conflict)?;
    let destination = runtimes
        .iter()
        .find(|caps| caps.runtime_id == d.destination_runtime_id)
        .ok_or_else(conflict)?;
    sequence_on(conn, intent).await?;
    intent.runtimes(source, destination, now)?;
    parent_on(conn, intent).await?;
    effect_on(conn, intent).await?;
    let inserted: Uuid = super::workers::insert_task_query(&intent.task, false)?
        .fetch_optional(&mut *conn)
        .await?
        .ok_or_else(conflict)?
        .get("id");
    if inserted != intent.task.id {
        return Err(conflict());
    }
    super::worker_invocations::capture(
        conn,
        inserted,
        Some(crate::worker_invocation::WorkerInvocationTarget::Unbound {
            reason: crate::delegation_publication::INVOCATION_REASON.into(),
        }),
    )
    .await?;
    // Recheck deadlines after every possible lock wait, including effect guards.
    let now = clock(conn).await?;
    intent.request.live(&grant, now)?;
    intent.runtimes(source, destination, now)?;
    if expiry.is_some_and(|expiry| expiry <= now) {
        return Err(conflict());
    }
    if grant_expiry <= now {
        return Err(conflict());
    }
    let mut consumed = grant;
    consumed.state = orch8_types::continuity::ContinuationGrantState::Consumed;
    consumed.consumed_at = Some(now);
    let changed=sqlx::query("UPDATE continuation_grants SET state='consumed',consumed_at=$3::timestamptz,record=$4::jsonb WHERE tenant_id=$1 AND id=$2::uuid AND state='active'").bind(tenant.as_str()).bind(d.grant_id.to_string()).bind(now.to_rfc3339()).bind(serde_json::to_string(&consumed)?).execute(&mut *conn).await?.rows_affected();
    if changed != 1 {
        return Err(conflict());
    }
    if !provenance_on(conn, intent).await? {
        return Ok(Publication::ProvenanceChanged);
    }
    let mut publication = intent.receipt.clone();
    publication.published_at = now;
    sqlx::query("INSERT INTO delegation_publications(delegation_id,tenant_id,grant_id,task_id,instance_id,record,created_at) VALUES($1::uuid,$2,$3::uuid,$4::uuid,$5::uuid,$6::jsonb,$7::timestamptz)").bind(d.id.to_string()).bind(tenant.as_str()).bind(d.grant_id.to_string()).bind(inserted.to_string())
        .bind(intent.parent.id.to_string()).bind(serde_json::to_string(&publication)?).bind(now.to_rfc3339()).execute(&mut *conn).await.map_err(|error|if error.as_database_error().is_some_and(sqlx::error::DatabaseError::is_unique_violation){conflict()}else{error.into()})?;
    // Receipt state is deliberately not returned as mutable task ownership.
    Ok(Publication::Published(Box::new(publication)))
}
async fn sequence_on(
    conn: &mut sqlx::PgConnection,
    intent: &DelegationPublishIntent,
) -> Result<(), StorageError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(format!("delegation-sequence|{}", intent.sequence.id))
        .execute(&mut *conn)
        .await?;
    let exists: Option<String> =
        sqlx::query_scalar("SELECT id::text FROM sequences WHERE id=$1::uuid FOR SHARE")
            .bind(intent.sequence.id.to_string())
            .fetch_optional(&mut *conn)
            .await?;
    if exists.is_none() {
        if intent.request.step.is_none() {
            return Err(conflict());
        }
        super::sequences::create_on(conn, &intent.sequence).await?;
    }
    let actual = super::sequences::get_on(conn, intent.sequence.id)
        .await?
        .ok_or_else(conflict)?;
    if sequence_sha256(&actual)? != intent.receipt.sequence_sha256 {
        return Err(conflict());
    }
    Ok(())
}
async fn parent_on(
    conn: &mut sqlx::PgConnection,
    intent: &DelegationPublishIntent,
) -> Result<(), StorageError> {
    if intent.proxy {
        // Do not silently replace a newly hosted native parent or a prior proxy
        // lacking its durable publication receipt.
        if let Some(parent) =
            super::instances::get_on(conn, intent.parent_owner.current_instance_id).await?
            && (parent.tenant_id != intent.parent_owner.tenant_id
                || parent.metadata.get("orch8_delegation_proxy").is_none())
        {
            return Err(conflict());
        }
        let frozen:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=$1::uuid) OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=$1::uuid AND state IN ('captured','sealed'))").bind(intent.parent.id.to_string()).fetch_one(&mut *conn).await?;
        if frozen
            || super::instances::get_on(conn, intent.parent.id)
                .await?
                .is_some()
        {
            return Err(conflict());
        }
        let context = super::json_encoding::encode_context(&intent.parent.context)?;
        super::instances::bind_instance_insert(
            sqlx::query(super::instances::INSTANCE_INSERT_SQL),
            &intent.parent,
            &context,
        )
        .execute(&mut *conn)
        .await?;
        if !super::worker_admission::ensure_initial_owner_on(conn, &intent.task_owner).await? {
            return Err(conflict());
        }
    } else {
        sqlx::query("SELECT id FROM task_instances WHERE id=$1::uuid FOR UPDATE")
            .bind(intent.parent.id.to_string())
            .execute(&mut *conn)
            .await?;
        let actual = super::instances::get_on(conn, intent.parent.id)
            .await?
            .ok_or_else(conflict)?;
        intent.native_live(&actual)?;
        super::portable_effects::check_namespace_on(conn, &intent.task_owner).await?;
    }
    Ok(())
}
async fn effect_on(
    conn: &mut sqlx::PgConnection,
    intent: &DelegationPublishIntent,
) -> Result<orch8_types::continuity::EffectReceipt, StorageError> {
    let candidate = &intent.effect.receipt;
    let epoch = i64::try_from(candidate.epoch.get()).map_err(|_| conflict())?;
    let mut receipt =
        super::effect_attempt::on(conn, candidate, intent.effect.legacy_id, epoch).await?;
    if receipt.id != candidate.id || receipt.state != EffectState::Planned {
        return Err(conflict());
    }
    receipt.state = EffectState::Prepared;
    let changed=sqlx::query("UPDATE effect_receipts SET state='prepared',record=$3::jsonb WHERE tenant_id=$1 AND id=$2::uuid AND state='planned'").bind(receipt.tenant_id.as_str()).bind(receipt.id.to_string()).bind(serde_json::to_string(&receipt)?).execute(&mut *conn).await?.rows_affected();
    if changed != 1 {
        return Err(conflict());
    }
    let native_sequence = super::sequences::get_on(conn, intent.parent.sequence_id)
        .await?
        .ok_or_else(conflict)?;
    let guards = super::portable_effects::guards_on(
        conn,
        &receipt.tenant_id,
        receipt.continuity_id,
        native_sequence.id,
        native_sequence.version,
        Some(receipt.kind),
    )
    .await?;
    let guarded=guards.iter().any(|guard|guard.commit_guard && matches!(guard.rule,orch8_types::continuity_advanced::InvariantRule::EffectAtMostOnce{kind} if kind==receipt.kind));
    receipt.state = EffectState::Dispatched;
    let fence = crate::EffectDispatchFence {
        instance_id: intent.parent.id,
        epoch: intent.task_owner.epoch,
        owner_runtime_id: intent.task_owner.owner_runtime_id,
        claim_token: None,
    };
    if super::effect_dispatch::on(conn, &receipt.tenant_id, &receipt, &fence, guarded).await?
        != EffectDispatchOutcome::Dispatched
    {
        return Err(conflict());
    }
    Ok(receipt)
}
async fn provenance_on(
    conn: &mut sqlx::PgConnection,
    intent: &DelegationPublishIntent,
) -> Result<bool, StorageError> {
    let entry = &intent.entry;
    let heads=sqlx::query("SELECT head.entry_sha256 FROM provenance_entries head WHERE head.tenant_id=$1 AND head.continuity_id=$2::uuid AND NOT EXISTS(SELECT 1 FROM provenance_entries child WHERE child.tenant_id=head.tenant_id AND child.continuity_id=head.continuity_id AND child.previous_sha256=head.entry_sha256) LIMIT 2").bind(entry.tenant_id.as_str()).bind(entry.continuity_id.to_string()).fetch_all(&mut *conn).await?;
    if heads.len() > 1 {
        return Err(StorageError::Constraint(
            "delegation provenance has multiple heads".into(),
        ));
    }
    let previous = heads
        .first()
        .map(|row| row.get::<String, _>("entry_sha256"));
    if previous != entry.previous_sha256 {
        return Ok(false);
    }
    let epoch = i64::try_from(entry.epoch.get()).map_err(|_| conflict())?;
    Ok(sqlx::query("INSERT INTO provenance_entries(id,continuity_id,tenant_id,epoch,entry_sha256,previous_sha256,record,created_at) VALUES($1::uuid,$2::uuid,$3,$4,$5,$6,$7::jsonb,$8::timestamptz) ON CONFLICT DO NOTHING").bind(entry.id.to_string()).bind(entry.continuity_id.to_string()).bind(entry.tenant_id.as_str()).bind(epoch)
        .bind(&entry.entry_sha256).bind(&entry.previous_sha256).bind(serde_json::to_string(entry)?).bind(entry.created_at.to_rfc3339()).execute(&mut *conn).await?.rows_affected()==1)
}

fn row_time(row: &sqlx::postgres::PgRow, column: &str) -> Result<DateTime<Utc>, StorageError> {
    Ok(row.try_get(column)?)
}

async fn runtime_snapshots(
    conn: &mut sqlx::PgConnection,
    intent: &DelegationPublishIntent,
) -> Result<Vec<RuntimeCapabilities>, StorageError> {
    let d = &intent.request.delegation;
    let tenant = &d.tenant_id;
    let rows=sqlx::query("SELECT record::text AS record,runtime_id::text AS runtime_scope,observed_at,expires_at FROM runtime_capabilities WHERE tenant_id=$1 AND runtime_id IN ($2::uuid,$3::uuid) ORDER BY runtime_id FOR SHARE").bind(tenant.as_str()).bind(d.source_runtime_id.to_string()).bind(d.destination_runtime_id.to_string()).fetch_all(&mut *conn).await?;
    let runtimes = rows
        .iter()
        .map(|row| {
            let caps: RuntimeCapabilities = serde_json::from_str(&row.get::<String, _>("record"))?;
            if caps.runtime_id.to_string() != row.get::<String, _>("runtime_scope")
                || caps.expires_at.timestamp_micros()
                    != row_time(row, "expires_at")?.timestamp_micros()
                || caps.observed_at.timestamp_micros()
                    != row_time(row, "observed_at")?.timestamp_micros()
            {
                return Err(conflict());
            }
            Ok(caps)
        })
        .collect::<Result<Vec<_>, StorageError>>()?;

    Ok(runtimes)
}
