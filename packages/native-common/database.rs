//! Exclusive recovery authority for the native single-owner `SQLite` runner.
use std::{
    fs::{File, OpenOptions},
    sync::{Arc, Mutex},
};

pub(crate) struct Owner {
    file: Mutex<Option<File>>,
}
impl Owner {
    pub(crate) fn acquire(path: &str) -> Result<Option<Arc<Self>>, String> {
        if path == ":memory:" {
            return Ok(None);
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|_| "cannot open native database ownership file".to_owned())?;
        // Keep locks separate from SQLite: some platform/VFS combinations
        // make whole-file locks conflict with SQLite's own locking protocol.
        // Resolve symlinks before choosing the stable companion. Never unlink
        // that companion: unlink/recreate could give two owners different locks.
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if file
                .metadata()
                .map_err(|_| "cannot inspect native database ownership file")?
                .nlink()
                != 1
            {
                return Err("native database hard links are unsupported".into());
            }
        }
        let canonical = std::fs::canonicalize(path)
            .map_err(|_| "cannot resolve native database ownership file".to_owned())?;
        drop(file);
        let mut companion = canonical.into_os_string();
        companion.push(".orch8-owner");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(companion)
            .map_err(|_| "cannot open native database ownership lock".to_owned())?;
        file.try_lock()
            .map_err(|_| "native database is already owned or cannot be locked".to_owned())?;
        Ok(Some(Arc::new(Self {
            file: Mutex::new(Some(file)),
        })))
    }
    /// Only after the shared `SQLite` pool has closed: old passes can no longer
    /// mutate it, even if a physical host callback has yet to return.
    pub(crate) fn release(&self) {
        let retired = self
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(retired);
    }
}

/// Retain recovery authority through build I/O even if its observer is dropped.
/// A detached build cannot publish an engine to a later native owner.
pub(crate) async fn initialize(
    database: Option<Arc<Owner>>,
    build: impl std::future::Future<Output = Result<orch8::Engine, orch8::Error>> + Send + 'static,
) -> Result<(orch8::Engine, Option<Arc<Owner>>), String> {
    tokio::spawn(async move {
        let engine = build.await.map_err(|error| error.to_string())?;
        Ok((engine, database))
    })
    .await
    .map_err(|_| "native database initialization task failed".to_owned())?
}
