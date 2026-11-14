//! Compact observed claim identity, independent of immutable cached graph scope.
use super::{DefinitionBundleReference, DefinitionReadScope, StorageError, invalid};
use serde::{Deserialize, Serialize};

const MAX_BYTES: usize = 16 * 1024;

#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Observation {
    version: u32,
    owner_epoch: i64,
    claim: DefinitionReadScope,
    reference: DefinitionBundleReference,
    request_sha256: String,
    definition_receipt_id: String,
}

impl Observation {
    pub(super) fn new(
        owner_epoch: i64,
        claim: DefinitionReadScope,
        reference: DefinitionBundleReference,
        request_sha256: String,
        definition_receipt_id: String,
    ) -> Result<Self, StorageError> {
        if owner_epoch <= 0 || uuid::Uuid::parse_str(&definition_receipt_id).is_err() {
            return Err(invalid("invalid logical run observation identity"));
        }
        claim.validate()?;
        Ok(Self {
            version: 1,
            owner_epoch,
            claim,
            reference,
            request_sha256,
            definition_receipt_id,
        })
    }
}

pub(super) async fn validate_prior(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run_id: &str,
    expected: &Observation,
    receipt_claim: &DefinitionReadScope,
    receipt_owner: i64,
) -> Result<(), StorageError> {
    // Bound stored bytes before fetching or parsing a possibly corrupt record.
    let byte_limit = i64::try_from(MAX_BYTES)
        .map_err(|_| invalid("observation byte limit exceeds SQL range"))?;
    let record: Option<Option<String>> = sqlx::query_scalar(
        "SELECT CASE WHEN length(CAST(record AS BLOB))<=?2 THEN record ELSE NULL END FROM mobile_remote_run_observations WHERE local_run_id=?1",
    ).bind(run_id).bind(byte_limit).fetch_optional(&mut **tx).await?;
    match record {
        None if expected.claim == *receipt_claim && expected.owner_epoch == receipt_owner => {
            // Pre-observation staged bindings are supported only while the
            // immutable receipt independently proves their complete old scope.
            Ok(())
        }
        Some(Some(record)) => {
            let observed: Observation = serde_json::from_str(&record)
                .map_err(|_| invalid("invalid logical run claim observation"))?;
            if observed == *expected {
                Ok(())
            } else {
                Err(invalid("logical run claim observation changed"))
            }
        }
        _ => Err(invalid("logical run claim observation is unavailable")),
    }
}

pub(super) async fn write(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run_id: &str,
    observed: &Observation,
) -> Result<(), StorageError> {
    let record = serde_json::to_string(observed)?;
    if record.len() > MAX_BYTES {
        return Err(invalid("logical run claim observation is oversized"));
    }
    sqlx::query("INSERT INTO mobile_remote_run_observations(local_run_id,record) VALUES(?1,?2) ON CONFLICT(local_run_id) DO UPDATE SET record=excluded.record")
        .bind(run_id).bind(record).execute(&mut **tx).await?;
    Ok(())
}
