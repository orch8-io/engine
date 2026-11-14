//! Durable remote native root, distinct from signed-catalog admission.
//! Initialization creates paused data only; remote forward authority is separate.
use super::{BoundClaimRun, MobileStorage, StorageError, inputs, invalid, retained_receipt};
use crate::storage::scopes::{peers::CatalogWorkerPeer, runs::materialize::InstanceLimits};
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::{
    context::ExecutionContext,
    delegation_definition::{
        claim::DelegationClaimContext,
        input::DelegationInputReference,
        wire::{DefinitionBundleReference, DefinitionReadScope},
    },
    ids::{InstanceId, Namespace, SequenceId, TenantId},
    instance::{InstanceState, Priority, TaskInstance},
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Root {
    version: u32,
    account: String,
    catalog: String,
    server: String,
    request_sha256: String,
    definition: DefinitionBundleReference,
    input: DelegationInputReference,
    initial_receipt: String,
    original_claim: DefinitionReadScope,
    original_owner_epoch: i64,
    sequence: SequenceId,
    tenant: TenantId,
    namespace: Namespace,
    created_at: chrono::DateTime<chrono::Utc>,
}
fn live(context: &DelegationClaimContext, stop: &CancellationToken) -> Result<(), StorageError> {
    if stop.is_cancelled() || chrono::Utc::now() >= context.expires_at {
        return Err(invalid(
            "remote initialization cancelled or publication expired",
        ));
    }
    Ok(())
}
fn same(instance: &TaskInstance, id: InstanceId, root: &Root) -> Result<(), StorageError> {
    if instance.id != id
        || instance.sequence_id != root.sequence
        || instance.tenant_id != root.tenant
        || instance.namespace != root.namespace
        || instance.created_at != root.created_at
        || instance.parent_instance_id.is_some()
        || instance.sub_tenant.is_some()
    {
        return Err(invalid("remote native instance identity changed"));
    }
    Ok(())
}
fn prepared_id(
    run: &str,
    context: &DelegationClaimContext,
    expected: &BoundClaimRun,
) -> Result<InstanceId, StorageError> {
    let id = InstanceId::from_uuid(
        uuid::Uuid::parse_str(run).map_err(|_| invalid("invalid remote local run id"))?,
    );
    if id != expected.local_run_id
        || id == InstanceId::from_uuid(uuid::Uuid::nil())
        || id.to_string() != run
        || context.scope.worker_id != expected.worker_id
        || context.scope.claim_epoch != expected.claim_epoch
    {
        return Err(invalid("prepared remote run identity changed"));
    }
    Ok(id)
}
#[allow(dead_code)] // Only paused initialization; scoped remote forward admission is still required.
impl MobileStorage {
    pub(crate) async fn initialize_remote_instance_for_run(
        &self,
        peer: &CatalogWorkerPeer,
        context: &DelegationClaimContext,
        expected: &BoundClaimRun,
        limits: InstanceLimits,
        stop: &CancellationToken,
    ) -> Result<TaskInstance, StorageError> {
        live(context, stop)?;
        peer.validate_claim_identity(context)?;
        let scope = peer.scope();
        let server = peer.api_base();
        let mut tx = self.pool().begin().await?;
        crate::storage::scopes::peers::check_in(&mut tx, peer).await?;
        let run = inputs::bound(&mut tx, scope, server, context).await?;
        let id = prepared_id(&run, context, expected)?;
        let input = inputs::read(&mut tx, &run, context)
            .await?
            .ok_or_else(|| invalid("remote initialization requires retained input"))?;
        let initial: String = sqlx::query_scalar(
            "SELECT initial_receipt_id FROM mobile_remote_run_bindings WHERE local_run_id=?1",
        )
        .bind(&run)
        .fetch_one(&mut *tx)
        .await?;
        let retained = retained_receipt(
            &mut tx,
            scope,
            server,
            &context.scope,
            &context.definition,
            &initial,
        )
        .await?;
        if retained.graph != expected.definitions {
            return Err(invalid("prepared remote graph identity changed"));
        }
        let (tenant, _) = super::super::super::workflows::owner(scope)?;
        let mut root = Root {
            version: 1,
            account: scope.account_id().into(),
            catalog: scope.catalog_id().into(),
            server: server.into(),
            request_sha256: context.request_sha256.clone(),
            definition: context.definition.clone(),
            input: context.input.clone(),
            initial_receipt: initial,
            original_claim: retained.claim,
            original_owner_epoch: retained.owner_epoch,
            sequence: retained.graph.root_id,
            tenant,
            namespace: retained.graph.namespace,
            created_at: chrono::Utc::now(),
        };
        let record:Option<Option<String>>=sqlx::query_scalar("SELECT CASE WHEN length(CAST(record AS BLOB))<=16384 THEN record END FROM mobile_remote_run_materializations WHERE instance_id=?1").bind(&run).fetch_optional(&mut *tx).await?;
        let physical:Option<(String,String)>=sqlx::query_as("SELECT instance_id,remote_tenant FROM mobile_remote_physical_roots WHERE api_base=?1 AND task_id=?2").bind(server).bind(context.scope.task_id.to_string()).fetch_optional(&mut *tx).await?;
        let owned = physical.as_ref().is_some_and(|(owner, tenant)| {
            owner == &run && tenant == context.scope.tenant_id.as_str()
        });
        if physical.is_some() && !owned {
            return Err(invalid("remote task already has another physical owner"));
        }
        let existing = SqliteStorage::get_instance_in_transaction(&mut tx, id).await?;
        let instance = match (record, existing) {
            (Some(Some(record)), Some(instance)) => {
                if !owned {
                    return Err(invalid("remote physical ownership receipt is missing"));
                }
                let stored: Root = serde_json::from_str(&record)
                    .map_err(|_| invalid("invalid remote materialization receipt"))?;
                root.created_at = stored.created_at;
                if stored != root {
                    return Err(invalid("remote materialization receipt changed"));
                }
                same(&instance, id, &root)?;
                instance
            }
            (None, None) => {
                if physical.is_some() {
                    return Err(invalid("remote physical root is incomplete"));
                }
                if !crate::storage::scopes::instance_capacity::available(&mut tx, scope, limits)
                    .await?
                {
                    return Err(invalid("remote active-instance capacity exhausted"));
                }
                insert(&mut tx, id, &root, input, context, stop).await?
            }
            _ => {
                return Err(invalid(
                    "remote materialization receipt or instance is missing",
                ));
            }
        };
        live(context, stop)?;
        tx.commit().await?;
        Ok(instance)
    }
}
async fn insert(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: InstanceId,
    root: &Root,
    input: orch8_types::delegation_definition::input::VerifiedDelegationInput,
    context: &DelegationClaimContext,
    stop: &CancellationToken,
) -> Result<TaskInstance, StorageError> {
    let instance = TaskInstance {
        id,
        sequence_id: root.sequence,
        tenant_id: root.tenant.clone(),
        namespace: root.namespace.clone(),
        state: InstanceState::Paused,
        next_fire_at: None,
        claimed_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: serde_json::json!({}),
        context: ExecutionContext {
            data: input.into_value(),
            ..ExecutionContext::default()
        },
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        sub_tenant: None,
        budget: None,
        created_at: root.created_at,
        updated_at: root.created_at,
    };
    live(context, stop)?;
    SqliteStorage::create_instance_in_transaction(tx, &instance).await?;
    let record = serde_json::to_string(&root)?;
    if record.len() > 16384 {
        return Err(invalid("remote materialization receipt is oversized"));
    }
    sqlx::query("INSERT INTO mobile_remote_run_materializations(instance_id,record) VALUES(?1,?2)")
        .bind(id.to_string())
        .bind(&record)
        .execute(&mut **tx)
        .await?;
    let stored: Option<String> = sqlx::query_scalar(
        "SELECT record FROM mobile_remote_run_materializations WHERE instance_id=?1",
    )
    .bind(id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    if stored.as_deref() != Some(record.as_str()) {
        return Err(invalid("remote materialization receipt was not retained"));
    }
    sqlx::query("INSERT INTO mobile_remote_physical_roots(api_base,task_id,remote_tenant,instance_id) VALUES(?1,?2,?3,?4)").bind(&root.server).bind(root.original_claim.task_id.to_string()).bind(root.original_claim.tenant_id.as_str()).bind(id.to_string()).execute(&mut **tx).await?;
    let owner:Option<(String,String)>=sqlx::query_as("SELECT instance_id,remote_tenant FROM mobile_remote_physical_roots WHERE api_base=?1 AND task_id=?2").bind(&root.server).bind(root.original_claim.task_id.to_string()).fetch_optional(&mut **tx).await?;
    if owner != Some((id.to_string(), root.original_claim.tenant_id.to_string())) {
        return Err(invalid("remote physical ownership was not retained"));
    }
    let persisted = SqliteStorage::get_instance_in_transaction(tx, id)
        .await?
        .ok_or_else(|| invalid("remote instance was not retained"))?;
    same(&persisted, id, root)?;
    if serde_json::to_value(&persisted)? != serde_json::to_value(&instance)? {
        return Err(invalid("remote instance insert changed its initial body"));
    }
    Ok(instance)
}
#[cfg(test)]
mod tests;
