//! Stable expected device configuration, not a reusable authentication grant.
use super::{CatalogWorkerPeer, MobileStorage, StorageError, fence_catalog, invalid, read};
fn validate(device: &str) -> Result<(), StorageError> {
    if device.trim().is_empty() || device.len() > 256 || device.chars().any(char::is_control) {
        return Err(invalid("invalid scoped worker device"));
    }
    Ok(())
}
async fn check(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    peer: &CatalogWorkerPeer,
    device: &str,
) -> Result<(), StorageError> {
    validate(device)?;
    fence_catalog(tx, &peer.scope).await?;
    if read(tx, &peer.scope, &peer.key).await?.as_ref() != Some(&peer.record) {
        return Err(invalid("worker peer is missing or changed"));
    }
    let prior:Option<String>=sqlx::query_scalar("SELECT device_id FROM mobile_scoped_worker_devices WHERE account_id=?1 AND catalog_id=?2 AND peer_key=?3")
        .bind(peer.scope.account_id()).bind(peer.scope.catalog_id()).bind(&peer.key).fetch_optional(&mut **tx).await?;
    if prior.as_deref().is_some_and(|d| d != device) {
        return Err(invalid("worker peer belongs to a different device"));
    }
    Ok(())
}
impl MobileStorage {
    pub(crate) async fn check_worker_peer_device(
        &self,
        peer: &CatalogWorkerPeer,
        device: &str,
    ) -> Result<(), StorageError> {
        let mut tx = self.pool().begin().await?;
        check(&mut tx, peer, device).await?;
        tx.commit().await?;
        Ok(())
    }
    /// Called only after a fresh authenticated identity probe matches the host's
    /// expected device. This records configuration; every read still probes anew.
    pub(crate) async fn bind_worker_peer_device(
        &self,
        peer: &CatalogWorkerPeer,
        device: &str,
    ) -> Result<(), StorageError> {
        let mut tx = self.pool().begin().await?;
        check(&mut tx, peer, device).await?;
        sqlx::query("INSERT INTO mobile_scoped_worker_devices(account_id,catalog_id,peer_key,device_id) VALUES(?1,?2,?3,?4) ON CONFLICT DO NOTHING")
            .bind(peer.scope.account_id()).bind(peer.scope.catalog_id()).bind(&peer.key).bind(device).execute(&mut *tx).await?;
        let stored:Option<String>=sqlx::query_scalar("SELECT device_id FROM mobile_scoped_worker_devices WHERE account_id=?1 AND catalog_id=?2 AND peer_key=?3")
            .bind(peer.scope.account_id()).bind(peer.scope.catalog_id()).bind(&peer.key).fetch_optional(&mut *tx).await?;
        if stored.as_deref() != Some(device) {
            return Err(invalid("worker device binding was not retained"));
        }
        tx.commit().await?;
        Ok(())
    }
}
