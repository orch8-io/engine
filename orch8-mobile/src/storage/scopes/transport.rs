use super::{CatalogScope, fence_catalog, invalid};
use crate::storage::MobileStorage;
use orch8_types::{
    catalog::{CatalogAccess, CatalogAudience},
    error::StorageError,
};

pub(crate) struct CatalogBinding {
    pub(crate) audience: CatalogAudience,
    pub(crate) authority: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CatalogTransport {
    pub(crate) manifest_url: String,
    pub(crate) identity_url: String,
}

impl CatalogTransport {
    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        endpoint(&self.manifest_url, false)?;
        endpoint(&self.identity_url, false)
    }
}

fn endpoint(url: &str, insecure: bool) -> Result<(), StorageError> {
    super::validate(url)?;
    let parsed = reqwest::Url::parse(url).map_err(|_| invalid("invalid catalog endpoint"))?;
    if parsed.as_str() != url
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return Err(invalid(
            "catalog endpoints must be canonical and contain no credentials, query or fragment",
        ));
    }
    if !insecure {
        let host = parsed.host_str().unwrap_or("").trim_end_matches('.');
        if host == "localhost" || host.ends_with(".localhost") {
            return Err(invalid("catalog endpoint requires a public host"));
        }
        crate::validate_https_url(url)
            .map_err(|_| invalid("catalog endpoint requires public HTTPS"))?;
    }
    Ok(())
}

impl MobileStorage {
    pub(crate) async fn catalog_binding(
        &self,
        scope: &CatalogScope,
    ) -> Result<CatalogBinding, StorageError> {
        let row: Option<(String, String, String, String, String)> = sqlx::query_as(
            "SELECT a.principal_issuer, a.principal_subject, c.source, c.catalog_key, c.signing_authority FROM mobile_accounts a JOIN mobile_catalogs c ON c.account_id = a.id WHERE a.id = ?1 AND a.epoch = ?2 AND a.active = 1 AND c.id = ?3"
        ).bind(&scope.account.id).bind(scope.account.epoch).bind(&scope.id).fetch_optional(self.pool()).await?;
        let (issuer, subject, source, catalog, authority) =
            row.ok_or_else(|| invalid("mobile catalog scope is retired or foreign"))?;
        Ok(CatalogBinding {
            audience: CatalogAudience {
                source,
                catalog,
                access: CatalogAccess::Principal { issuer, subject },
            },
            authority,
        })
    }

    pub(crate) async fn bind_catalog_transport(
        &self,
        scope: &CatalogScope,
        transport: &CatalogTransport,
    ) -> Result<(), StorageError> {
        self.bind_transport(scope, transport, false).await
    }

    #[cfg(test)]
    pub(crate) async fn bind_catalog_transport_for_test(
        &self,
        scope: &CatalogScope,
        transport: &CatalogTransport,
    ) -> Result<(), StorageError> {
        self.bind_transport(scope, transport, true).await
    }

    async fn bind_transport(
        &self,
        scope: &CatalogScope,
        transport: &CatalogTransport,
        insecure: bool,
    ) -> Result<(), StorageError> {
        endpoint(&transport.manifest_url, insecure)?;
        endpoint(&transport.identity_url, insecure)?;
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        sqlx::query("INSERT INTO mobile_catalog_transports(account_id, catalog_id, manifest_url, identity_url) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(account_id, catalog_id) DO NOTHING")
            .bind(&scope.account.id).bind(&scope.id).bind(&transport.manifest_url).bind(&transport.identity_url).execute(&mut *tx).await?;
        let found: (String, String) = sqlx::query_as("SELECT manifest_url, identity_url FROM mobile_catalog_transports WHERE account_id = ?1 AND catalog_id = ?2")
            .bind(&scope.account.id).bind(&scope.id).fetch_one(&mut *tx).await?;
        if found
            != (
                transport.manifest_url.clone(),
                transport.identity_url.clone(),
            )
        {
            return Err(invalid("mobile catalog endpoint binding mismatch"));
        }
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn catalog_transport(
        &self,
        scope: &CatalogScope,
    ) -> Result<CatalogTransport, StorageError> {
        let row: Option<(String, String)> = sqlx::query_as("SELECT t.manifest_url, t.identity_url FROM mobile_accounts a JOIN mobile_catalogs c ON c.account_id = a.id JOIN mobile_catalog_transports t ON t.account_id = a.id AND t.catalog_id = c.id WHERE a.id = ?1 AND a.epoch = ?2 AND a.active = 1 AND c.id = ?3")
            .bind(&scope.account.id).bind(scope.account.epoch).bind(&scope.id).fetch_optional(self.pool()).await?;
        let (manifest_url, identity_url) =
            row.ok_or_else(|| invalid("catalog transport is unconfigured, retired or foreign"))?;
        Ok(CatalogTransport {
            manifest_url,
            identity_url,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::sqlite::SqliteStorage;
    use std::sync::Arc;

    #[tokio::test]
    async fn bindings_reject_unsafe_or_changing_endpoints_without_partial_state() {
        let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
        let account = storage
            .open_account_scope("a", "issuer", "subject")
            .await
            .unwrap();
        let scope = storage
            .open_catalog_scope(&account, "one", "source", "root")
            .await
            .unwrap();
        for url in [
            "http://catalog.example/manifest",
            "https://127.0.0.1/manifest",
            "https://localhost/manifest",
            "https://u:secret@catalog.example/manifest",
            "https://catalog.example/manifest?token=secret",
            "https://catalog.example/manifest#secret",
            "https://CATALOG.example/manifest",
        ] {
            assert!(
                storage
                    .bind_catalog_transport(
                        &scope,
                        &CatalogTransport {
                            manifest_url: url.into(),
                            identity_url: "https://identity.example/check".into()
                        }
                    )
                    .await
                    .is_err(),
                "{url}"
            );
            assert!(storage.catalog_transport(&scope).await.is_err());
        }
        let transport = CatalogTransport {
            manifest_url: "https://catalog.example/manifest".into(),
            identity_url: "https://identity.example/check".into(),
        };
        storage
            .bind_catalog_transport(&scope, &transport)
            .await
            .unwrap();
        storage
            .bind_catalog_transport(&scope, &transport)
            .await
            .unwrap();
        let changed = CatalogTransport {
            identity_url: "https://another.example/check".into(),
            ..transport.clone()
        };
        assert!(
            storage
                .bind_catalog_transport(&scope, &changed)
                .await
                .is_err()
        );
        assert_eq!(storage.catalog_transport(&scope).await.unwrap(), transport);
        storage.retire_account_scope(&account).await.unwrap();
        assert!(storage.catalog_transport(&scope).await.is_err());
        assert!(
            storage
                .bind_catalog_transport(&scope, &transport)
                .await
                .is_err()
        );
    }
}
