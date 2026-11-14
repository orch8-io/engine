//! Native fingerprint codec 1: fixed persisted columns, not typed DSL JSON.
//! Compatible in-memory defaults cannot alter this identity. New execution
//! columns or incompatible encodings require a new codec and explicit migration.
use super::{SequenceId, StorageError, invalid};
use sha2::{Digest as _, Sha256};
use sqlx::Row as _;

const PREFIX: &[u8] = b"orch8.native-definition.v1\0";
const MAX_BYTES: i64 = 8 * 1024 * 1024;
const COLUMNS: &[&str] = &[
    "id",
    "tenant_id",
    "namespace",
    "name",
    "version",
    "deprecated",
    "status",
    "blocks",
    "interceptors",
    "input_schema",
    "sla",
    "on_failure",
    "on_cancel",
    "placement",
    "created_at",
    "sub_tenant",
    "embed",
];
pub(super) struct Fingerprint {
    pub(super) sha256: String,
    pub(super) bytes: i64,
}
pub(super) async fn fingerprint(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: SequenceId,
) -> Result<Fingerprint, StorageError> {
    // Check raw storage lengths before fetching or parsing potentially corrupt
    // large JSON values. Null and empty text remain distinct in the codec.
    let size:Option<i64>=sqlx::query_scalar("SELECT COALESCE(length(CAST(id AS BLOB)),0)+COALESCE(length(CAST(tenant_id AS BLOB)),0)+COALESCE(length(CAST(namespace AS BLOB)),0)+COALESCE(length(CAST(name AS BLOB)),0)+COALESCE(length(CAST(version AS BLOB)),0)+COALESCE(length(CAST(deprecated AS BLOB)),0)+COALESCE(length(CAST(status AS BLOB)),0)+COALESCE(length(CAST(blocks AS BLOB)),0)+COALESCE(length(CAST(interceptors AS BLOB)),0)+COALESCE(length(CAST(input_schema AS BLOB)),0)+COALESCE(length(CAST(sla AS BLOB)),0)+COALESCE(length(CAST(on_failure AS BLOB)),0)+COALESCE(length(CAST(on_cancel AS BLOB)),0)+COALESCE(length(CAST(placement AS BLOB)),0)+COALESCE(length(CAST(created_at AS BLOB)),0)+COALESCE(length(CAST(sub_tenant AS BLOB)),0)+COALESCE(length(CAST(embed AS BLOB)),0) FROM sequences WHERE id=?1").bind(id.to_string()).fetch_optional(&mut **tx).await?;
    if !size.is_some_and(|size| (0..=MAX_BYTES).contains(&size)) {
        return Err(invalid("native definition is unavailable or oversized"));
    }
    let row=sqlx::query("SELECT id,tenant_id,namespace,name,CAST(version AS TEXT) AS version,CAST(deprecated AS TEXT) AS deprecated,status,blocks,interceptors,input_schema,sla,on_failure,on_cancel,placement,created_at,sub_tenant,embed FROM sequences WHERE id=?1").bind(id.to_string()).fetch_one(&mut **tx).await?;
    let mut digest = Sha256::new();
    digest.update(PREFIX);
    let mut bytes =
        i64::try_from(PREFIX.len()).map_err(|_| invalid("native definition size overflow"))?;
    for name in COLUMNS {
        let value: Option<&str> = row.try_get(*name)?;
        match value {
            None => {
                digest.update([0]);
                bytes += 1;
            }
            Some(value) => {
                let data = value.as_bytes();
                bytes += 9 + i64::try_from(data.len())
                    .map_err(|_| invalid("native definition size overflow"))?;
                digest.update([1]);
                digest.update(
                    u64::try_from(data.len())
                        .map_err(|_| invalid("native definition size overflow"))?
                        .to_be_bytes(),
                );
                digest.update(data);
            }
        }
        if bytes > MAX_BYTES {
            return Err(invalid("native definition is oversized"));
        }
    }
    Ok(Fingerprint {
        sha256: hex::encode(digest.finalize()),
        bytes,
    })
}
