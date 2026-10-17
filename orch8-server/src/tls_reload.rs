//! Change detection for the gRPC server's mTLS material (L5-14).
//!
//! tonic fixes the TLS identity when the server starts, so rotated
//! certificates were never picked up. [`TlsReloader`] re-reads the three PEM
//! files and yields a fresh [`ServerTlsConfig`] only when their content
//! changed *and* the new material is accepted by tonic; a half-written or
//! malformed rotation is rejected and the serving material stays in place.

use std::path::PathBuf;

use tonic::transport::{Certificate, Identity, ServerTlsConfig};

#[derive(Clone, PartialEq, Eq)]
struct Material {
    cert: Vec<u8>,
    key: Vec<u8>,
    client_ca: Vec<u8>,
}

impl Material {
    fn tls_config(&self) -> ServerTlsConfig {
        ServerTlsConfig::new()
            .identity(Identity::from_pem(&self.cert, &self.key))
            .client_ca_root(Certificate::from_pem(&self.client_ca))
    }

    /// tonic only validates PEM when the config is applied to a server.
    fn is_accepted(&self) -> bool {
        tonic::transport::Server::builder()
            .tls_config(self.tls_config())
            .is_ok()
    }
}

pub struct TlsReloader {
    paths: [PathBuf; 3],
    current: Material,
}

impl TlsReloader {
    /// Read the initial material. Fails when any file is unreadable or the
    /// material is not accepted (startup must not silently run without TLS).
    pub async fn load(
        cert: &str,
        key: &str,
        client_ca: &str,
    ) -> anyhow::Result<(Self, ServerTlsConfig)> {
        // `ring` and `aws-lc-rs` are both in the dependency graph, so rustls
        // cannot choose a process default on its own and would panic when the
        // TLS config is built. Idempotent: `Err` just means one is installed.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let paths = [
            PathBuf::from(cert),
            PathBuf::from(key),
            PathBuf::from(client_ca),
        ];
        let current = read(&paths).await?;
        anyhow::ensure!(
            current.is_accepted(),
            "Invalid gRPC mTLS certificate material"
        );
        let config = current.tls_config();
        Ok((Self { paths, current }, config))
    }

    /// `Some(config)` when the files changed to new, valid material.
    pub async fn poll(&mut self) -> Option<ServerTlsConfig> {
        let next = match read(&self.paths).await {
            Ok(next) => next,
            Err(error) => {
                tracing::warn!(%error, "gRPC TLS reload: cannot read material; keeping current");
                return None;
            }
        };
        if next == self.current {
            return None;
        }
        if !next.is_accepted() {
            // Leave `current` untouched so a later complete rewrite is still
            // seen as a change.
            tracing::warn!("gRPC TLS reload: new material rejected; keeping current");
            return None;
        }
        let config = next.tls_config();
        self.current = next;
        Some(config)
    }
}

async fn read(paths: &[PathBuf; 3]) -> anyhow::Result<Material> {
    let (cert, key, client_ca) = tokio::try_join!(
        tokio::fs::read(&paths[0]),
        tokio::fs::read(&paths[1]),
        tokio::fs::read(&paths[2]),
    )
    .map_err(|e| anyhow::anyhow!("Failed to read gRPC mTLS certificate material: {e}"))?;
    Ok(Material {
        cert,
        key,
        client_ca,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixtures() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../orch8-e2e/tests/fixtures/tls")
    }

    fn staged() -> (tempfile::TempDir, [String; 3]) {
        let dir = tempfile::tempdir().unwrap();
        let mut out = Vec::new();
        for (src, dst) in [
            ("server.pem", "cert.pem"),
            ("server.key", "key.pem"),
            ("ca.pem", "ca.pem"),
        ] {
            let to = dir.path().join(dst);
            std::fs::copy(fixtures().join(src), &to).unwrap();
            out.push(to.to_string_lossy().into_owned());
        }
        let [a, b, c]: [String; 3] = out.try_into().unwrap();
        (dir, [a, b, c])
    }

    #[tokio::test]
    async fn unchanged_material_is_not_reported() {
        let (_d, [c, k, ca]) = staged();
        let (mut r, _) = TlsReloader::load(&c, &k, &ca).await.unwrap();
        assert!(r.poll().await.is_none());
    }

    #[tokio::test]
    async fn rotated_material_is_picked_up_once() {
        let (_d, [c, k, ca]) = staged();
        let (mut r, _) = TlsReloader::load(&c, &k, &ca).await.unwrap();
        // A rotated bundle: the same CA listed twice is valid and differs.
        let pem = std::fs::read_to_string(&ca).unwrap();
        std::fs::write(&ca, format!("{pem}\n{pem}")).unwrap();
        assert!(r.poll().await.is_some(), "rotation must be detected");
        assert!(r.poll().await.is_none(), "reported only once");
    }

    #[tokio::test]
    async fn malformed_rotation_is_rejected_and_a_later_good_one_still_applies() {
        let (_d, [c, k, ca]) = staged();
        let (mut r, _) = TlsReloader::load(&c, &k, &ca).await.unwrap();
        let good_key = std::fs::read(&k).unwrap();
        std::fs::write(&k, b"-----BEGIN PRIVATE KEY-----\ngarbage\n").unwrap();
        assert!(
            r.poll().await.is_none(),
            "bad key must not replace the good one"
        );
        std::fs::remove_file(&c).unwrap();
        assert!(r.poll().await.is_none(), "missing file keeps current");
        std::fs::copy(fixtures().join("server.pem"), &c).unwrap();
        std::fs::write(&k, &good_key).unwrap();
        let pem = std::fs::read_to_string(&ca).unwrap();
        std::fs::write(&ca, format!("{pem}\n{pem}")).unwrap();
        assert!(r.poll().await.is_some());
    }
}
