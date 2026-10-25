//! Trusted runtime admission for a frozen request, never caller-supplied rows.
use super::{
    CatalogRun, CatalogRunState, CatalogScope, InstanceId, MobileStorage, StorageError,
    checked_definition, decode, fence_catalog, invalid, read_owner, require_reconciled,
};
use crate::MobileError;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::{
    context::ExecutionContext,
    instance::{InstanceState, Priority, TaskInstance},
    sequence::{BlockDefinition, SequenceDefinition, SequenceStatus, StepDef},
};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
pub(crate) struct InstanceLimits {
    pub(crate) global: u32,
    pub(crate) account: u32,
    pub(crate) catalog: u32,
}
impl InstanceLimits {
    pub(crate) fn for_engine(global: u32) -> Self {
        let account = global.div_ceil(2).max(1);
        Self {
            global,
            account,
            catalog: account,
        }
    }
}
pub(crate) struct Snapshot {
    run: CatalogRun,
    definition: SequenceDefinition,
    identity: String,
    body: String,
    policy: ExecutionPolicy,
}
pub(crate) enum Preparation {
    Existing(Box<TaskInstance>),
    Pending(Box<Snapshot>),
}
pub(crate) struct Checked {
    id: InstanceId,
    identity: String,
    body: String,
    policy: ExecutionPolicy,
}

#[derive(Clone, serde::Serialize)]
pub(crate) struct ExecutionPolicy {
    min_sdk: String,
    required: Vec<String>,
}
impl ExecutionPolicy {
    pub(crate) fn permits(&self, sdk: &str, handlers: &HashSet<String>) -> bool {
        crate::sync::sdk_version_meets_min(sdk, &self.min_sdk)
            && self.required.iter().all(|name| handlers.contains(name))
    }
    fn identity(&self) -> Result<String, StorageError> {
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(self)?)))
    }
}

pub(super) fn fingerprint_run(run: &CatalogRun) -> Result<String, StorageError> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
        &run.request_digest,
        run.id,
        &run.account_id,
        &run.catalog_id,
        run.owner_epoch,
        run.sequence_id,
        &run.workflow,
        run.version,
        run.created_at,
        run.admitted_manifest_version,
        &run.admitted_manifest_digest,
        &run.input,
    ))?)))
}
pub(super) fn fingerprint_definition(seq: &SequenceDefinition) -> Result<String, StorageError> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(seq)?)))
}
fn same_instance(
    run: &CatalogRun,
    definition_owner: &CatalogScope,
    instance: &TaskInstance,
) -> Result<(), StorageError> {
    let (tenant, namespace) = super::super::workflows::owner(definition_owner)?;
    if instance.id != run.id
        || instance.sequence_id != run.sequence_id
        || instance.tenant_id != tenant
        || instance.namespace != namespace
        || instance.created_at != run.created_at
        || instance.sub_tenant.is_some()
        || instance.parent_instance_id.is_some()
    {
        return Err(invalid(
            "catalog instance identity is inconsistent with its request",
        ));
    }
    Ok(())
}
async fn read_run(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    scope: &CatalogScope,
    id: InstanceId,
) -> Result<CatalogRun, StorageError> {
    let row = sqlx::query(
        "SELECT * FROM mobile_catalog_run_requests WHERE id=?1 AND account_id=?2 AND catalog_id=?3",
    )
    .bind(id.to_string())
    .bind(&scope.account.id)
    .bind(&scope.id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| invalid("catalog run request is unavailable"))?;
    let run = decode(&row)?;
    if run.owner_epoch != scope.account.epoch || run.state != CatalogRunState::Prepared {
        return Err(invalid(
            "catalog run request is cancelled or belongs to a retired epoch",
        ));
    }
    Ok(run)
}
async fn existing(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    scope: &CatalogScope,
    run: &CatalogRun,
) -> Result<Option<TaskInstance>, StorageError> {
    let receipt: Option<String> = sqlx::query_scalar(
        "SELECT request_fingerprint FROM mobile_catalog_run_materializations WHERE request_id=?1",
    )
    .bind(run.id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    let instance = SqliteStorage::get_instance_in_transaction(tx, run.id).await?;
    match (receipt, instance) {
        (None, None) => Ok(None),
        (Some(fingerprint), Some(instance)) if fingerprint == fingerprint_run(run)? => {
            same_instance(run, scope, &instance)?;
            Ok(Some(instance))
        }
        (Some(_), None) => Err(StorageError::NotFound {
            entity: "catalog instance history",
            id: run.id.to_string(),
        }),
        _ => Err(invalid(
            "catalog materialization receipt is missing or inconsistent",
        )),
    }
}
async fn publication(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    scope: &CatalogScope,
    run: &CatalogRun,
) -> Result<(SequenceDefinition, ExecutionPolicy), StorageError> {
    let (manifest_version, manifest_digest) = require_reconciled(tx, scope).await?;
    let available:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM mobile_catalog_workflows WHERE account_id=?1 AND catalog_id=?2 AND name=?3 AND version=?4 AND sequence_id=?5 AND withdrawn_at_version IS NULL)")
        .bind(&scope.account.id).bind(&scope.id).bind(&run.workflow).bind(run.version).bind(run.sequence_id.to_string()).fetch_one(&mut **tx).await?;
    if !available {
        return Err(invalid("catalog workflow is no longer published"));
    }
    let seq = checked_definition(tx, scope, run.sequence_id, &run.workflow, run.version).await?;
    if seq.deprecated || seq.status != SequenceStatus::Production {
        return Err(invalid("catalog workflow is not executable"));
    }
    let (min_sdk,required):(String,String)=sqlx::query_as("SELECT min_sdk_version,required_handlers FROM mobile_catalog_execution_policy WHERE account_id=?1 AND catalog_id=?2 AND name=?3 AND version=?4 AND manifest_version<=?5 AND (manifest_version<?5 OR manifest_digest=?6)")
        .bind(&scope.account.id).bind(&scope.id).bind(&run.workflow).bind(run.version).bind(manifest_version).bind(manifest_digest)
        .fetch_optional(&mut **tx).await?.ok_or_else(||invalid("catalog workflow has no current signed execution policy"))?;
    Ok((
        seq,
        ExecutionPolicy {
            min_sdk,
            required: serde_json::from_str(&required)?,
        },
    ))
}
impl Snapshot {
    /// CPU validation owns no database transaction or foreign callback reference.
    pub(crate) fn check(
        self,
        handlers: &HashSet<String>,
        sdk: &str,
    ) -> Result<Checked, StorageError> {
        if !self.policy.permits(sdk, handlers) {
            return Err(invalid(
                "catalog workflow is incompatible with this runtime",
            ));
        }
        self.definition
            .validate()
            .map_err(|_| invalid("invalid catalog workflow structure"))?;
        if self.definition.interceptors.is_some()
            || self.definition.on_failure.is_some()
            || self.definition.on_cancel.is_some()
            || self.definition.placement.is_some()
        {
            return Err(invalid(
                "catalog workflow needs unimplemented scoped lifecycle or placement support",
            ));
        }
        for block in &self.definition.blocks {
            check_block(block, handlers)?;
        }
        orch8_engine::input_schema::validate(
            self.definition.input_schema.as_ref(),
            &self.run.input,
        )
        .map_err(|_| invalid("catalog input does not match a usable workflow schema"))?;
        Ok(Checked {
            id: self.run.id,
            identity: self.identity,
            body: self.body,
            policy: self.policy,
        })
    }
}
fn check_step(step: &StepDef, handlers: &HashSet<String>) -> Result<(), StorageError> {
    let allowed = |name: &str| {
        handlers.contains(name)
            && !orch8_engine::handlers::activepieces::is_ap_handler(name)
            && !orch8_engine::handlers::grpc_plugin::is_grpc_handler(name)
            && !orch8_engine::handlers::wasm_plugin::is_wasm_handler(name)
    };
    if !allowed(&step.handler)
        || step
            .fallback_handler
            .as_ref()
            .is_some_and(|name| !allowed(name))
    {
        return Err(invalid("catalog workflow handler is unavailable"));
    }
    if step
        .params
        .get(orch8_types::worker::RUNTIME_REQUIREMENTS_PARAM)
        .is_some()
        || step.placement.is_some()
        || step.queue_name.is_some()
        || step.rate_budget.is_some()
        || step.rate_limit_key.is_some()
        || step.on_deadline_breach.is_some()
        || step.compensation.is_some()
        || step
            .wait_for_input
            .as_ref()
            .is_some_and(|input| input.auto_decide.is_some() || input.escalation_handler.is_some())
    {
        return Err(invalid(
            "catalog step needs unimplemented scoped placement, recovery or escalation support",
        ));
    }
    Ok(())
}
fn check_blocks(
    blocks: &[BlockDefinition],
    handlers: &HashSet<String>,
) -> Result<(), StorageError> {
    for block in blocks {
        check_block(block, handlers)?;
    }
    Ok(())
}
fn check_block(block: &BlockDefinition, handlers: &HashSet<String>) -> Result<(), StorageError> {
    match block {
        BlockDefinition::Step(step) => check_step(step, handlers),
        BlockDefinition::Parallel(p) => {
            for b in &p.branches {
                check_blocks(b, handlers)?;
            }
            Ok(())
        }
        BlockDefinition::Race(p) => {
            for b in &p.branches {
                check_blocks(b, handlers)?;
            }
            Ok(())
        }
        BlockDefinition::Loop(p) => check_blocks(&p.body, handlers),
        BlockDefinition::ForEach(p) => check_blocks(&p.body, handlers),
        BlockDefinition::Router(p) => {
            for r in &p.routes {
                check_blocks(&r.blocks, handlers)?;
            }
            if let Some(b) = &p.default {
                check_blocks(b, handlers)?;
            }
            Ok(())
        }
        BlockDefinition::TryCatch(p) => {
            check_blocks(&p.try_block, handlers)?;
            check_blocks(&p.catch_block, handlers)?;
            if let Some(b) = &p.finally_block {
                check_blocks(b, handlers)?;
            }
            Ok(())
        }
        BlockDefinition::ABSplit(p) => {
            for v in &p.variants {
                check_blocks(&v.blocks, handlers)?;
            }
            Ok(())
        }
        BlockDefinition::CancellationScope(p) => check_blocks(&p.blocks, handlers),
        BlockDefinition::SubSequence(_) | BlockDefinition::Saga(_) => Err(invalid(
            "catalog workflow needs unimplemented scoped child or recovery support",
        )),
    }
}
impl MobileStorage {
    pub(crate) async fn prepare_materialization(
        &self,
        scope: &CatalogScope,
        id: InstanceId,
    ) -> Result<Preparation, StorageError> {
        let mut tx = self.pool().begin().await?;
        read_owner(&mut tx, scope).await?;
        let run = read_run(&mut tx, scope, id).await?;
        if let Some(instance) = existing(&mut tx, scope, &run).await? {
            tx.commit().await?;
            return Ok(Preparation::Existing(Box::new(instance)));
        }
        let (definition, policy) = publication(&mut tx, scope, &run).await?;
        let identity = fingerprint_run(&run)?;
        let body = fingerprint_definition(&definition)?;
        tx.commit().await?;
        Ok(Preparation::Pending(Box::new(Snapshot {
            run,
            definition,
            identity,
            body,
            policy,
        })))
    }
    pub(crate) async fn commit_materialization(
        &self,
        scope: &CatalogScope,
        checked: Checked,
        limits: InstanceLimits,
        stop: &CancellationToken,
    ) -> Result<TaskInstance, MobileError> {
        let mut tx = self.pool().begin().await.map_err(StorageError::from)?;
        let result = insert_checked(&mut tx, scope, checked, limits, stop).await;
        match result {
            Ok(instance) => {
                tx.commit().await.map_err(StorageError::from)?;
                Ok(instance)
            }
            Err(error) => {
                tx.rollback().await.map_err(StorageError::from)?;
                Err(error)
            }
        }
    }
}

async fn insert_checked(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    scope: &CatalogScope,
    checked: Checked,
    limits: InstanceLimits,
    stop: &CancellationToken,
) -> Result<TaskInstance, MobileError> {
    fence_catalog(tx, scope).await?;
    if stop.is_cancelled() {
        return Err(MobileError::Shutdown);
    }
    let run = read_run(tx, scope, checked.id).await?;
    if fingerprint_run(&run)? != checked.identity {
        return Err(invalid("catalog request changed during materialization").into());
    }
    if let Some(instance) = existing(tx, scope, &run).await? {
        return Ok(instance);
    }
    let (definition, policy) = publication(tx, scope, &run).await?;
    if policy.identity()? != checked.policy.identity()? {
        return Err(invalid("catalog runtime policy changed during materialization").into());
    }
    if fingerprint_definition(&definition)? != checked.body {
        return Err(invalid("catalog definition changed during materialization").into());
    }
    let (global,account,catalog):(i64,i64,i64)=sqlx::query_as("SELECT COUNT(*),COALESCE(SUM(tenant_id=?1),0),COALESCE(SUM(tenant_id=?1 AND namespace=?2),0) FROM task_instances WHERE state IN ('scheduled','running','waiting','paused')")
        .bind(definition.tenant_id.as_str()).bind(definition.namespace.as_str()).fetch_one(&mut **tx).await.map_err(StorageError::from)?;
    if [
        (global, limits.global),
        (account, limits.account),
        (catalog, limits.catalog),
    ]
    .iter()
    .any(|(used, max)| u64::try_from(*used).unwrap_or(u64::MAX) >= u64::from(*max))
    {
        return Err(MobileError::ResourceLimit {
            message: "catalog active-instance allowance exhausted".into(),
        });
    }
    let instance = TaskInstance {
        claimed_at: None,
        sub_tenant: None,
        id: run.id,
        sequence_id: run.sequence_id,
        tenant_id: definition.tenant_id,
        namespace: definition.namespace,
        state: InstanceState::Scheduled,
        next_fire_at: Some(run.created_at),
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: serde_json::json!({}),
        context: ExecutionContext {
            data: run.input,
            ..ExecutionContext::default()
        },
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: run.created_at,
        updated_at: run.created_at,
    };
    if stop.is_cancelled() {
        return Err(MobileError::Shutdown);
    }
    SqliteStorage::create_instance_in_transaction(tx, &instance).await?;
    sqlx::query("INSERT INTO mobile_catalog_run_materializations(request_id,request_fingerprint,definition_fingerprint,min_sdk_version,required_handlers,materialized_at) VALUES (?1,?2,?3,?4,?5,?6)")
        .bind(run.id.to_string()).bind(checked.identity).bind(checked.body)
        .bind(&policy.min_sdk).bind(serde_json::to_string(&policy.required).map_err(StorageError::from)?)
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&mut **tx).await.map_err(StorageError::from)?;
    Ok(instance)
}

impl MobileStorage {
    pub(crate) async fn materialized_runtime_policy(
        &self,
        id: InstanceId,
    ) -> Result<ExecutionPolicy, StorageError> {
        let (min_sdk,required):(String,String)=sqlx::query_as("SELECT min_sdk_version,required_handlers FROM mobile_catalog_run_materializations WHERE request_id=?1")
            .bind(id.to_string()).fetch_optional(self.pool()).await?.ok_or_else(||invalid("catalog instance has no materialization receipt"))?;
        Ok(ExecutionPolicy {
            min_sdk,
            required: serde_json::from_str(&required)?,
        })
    }
}
