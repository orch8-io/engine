//! Revalidate persisted outbox rows before using a legacy device credential.
use super::SyncReporter;
use crate::storage::MobileStorage;

#[derive(serde::Deserialize)]
struct PayloadOwner {
    instance_id: String,
}

impl SyncReporter {
    /// Older versions could queue catalog rows in the global outbox. Neither
    /// that table's key nor payload is authority: both must match a positively
    /// proven legacy instance. A row verified when queued may survive native
    /// task GC, but can never override retained catalog ownership. Invalid or
    /// unowned rows are discarded, never sent
    /// to a new account or silently rehomed into a catalog's future outbox.
    pub(super) async fn legacy_outbox_rows(
        &self,
        rows: Vec<(i64, String, String, bool)>,
        mobile: &MobileStorage,
        approvals: bool,
    ) -> Result<Vec<(i64, String)>, orch8_types::error::StorageError> {
        let owners: Vec<_> = rows
            .iter()
            .map(|(_, key, payload, _)| {
                let owner = serde_json::from_str::<PayloadOwner>(payload).ok()?;
                let id = uuid::Uuid::parse_str(&owner.instance_id).ok()?.to_string();
                let key_id = if approvals {
                    key.split_once(':')?.0
                } else {
                    key
                };
                (owner.instance_id == id && key_id == id).then_some(id)
            })
            .collect();
        let mut fresh_ids = Vec::new();
        let mut retained_ids = Vec::new();
        for ((_, _, _, verified), owner) in rows.iter().zip(&owners) {
            if let Some(id) = owner {
                if *verified {
                    retained_ids.push(id.clone());
                } else {
                    fresh_ids.push(id.clone());
                }
            }
        }
        let (allowed, retained) = tokio::try_join!(
            mobile.legacy_sync_instance_ids(&fresh_ids),
            mobile.retained_legacy_sync_instance_ids(&retained_ids),
        )?;
        let mut rejected = Vec::new();
        let mut valid = Vec::new();
        for ((row, _, payload, verified), owner) in rows.into_iter().zip(owners) {
            if owner.is_some_and(|id| {
                if verified {
                    retained.contains(&id)
                } else {
                    allowed.contains(&id)
                }
            }) {
                valid.push((row, payload));
            } else {
                rejected.push(row);
            }
        }
        super::delete_outbox_rows(&self.pool, &rejected).await?;
        Ok(valid)
    }
}
