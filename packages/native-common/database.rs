//! Exclusive recovery authority for the native single-owner `SQLite` runner.
use std::{
    fs::{File, OpenOptions},
    sync::{Arc, Mutex},
};

pub(crate) struct Owner {
    file: Mutex<Option<File>>,
    path: std::path::PathBuf,
}
impl Owner {
    pub(crate) fn acquire(path: &str) -> Result<Option<Arc<Self>>, String> {
        if path == ":memory:" {
            return Ok(None);
        }
        // Never open/close the database inode to discover its path: a close
        // can cancel the live SQLite pool's process-wide POSIX locks.
        let canonical =
            orch8::resolve_sqlite_database_path(path).map_err(|error| error.to_string())?;
        let mut companion = canonical.clone().into_os_string();
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
        if orch8::resolve_sqlite_database_path(path).map_err(|error| error.to_string())?
            != canonical
        {
            return Err("native database path changed while acquiring ownership".into());
        }
        Ok(Some(Arc::new(Self {
            path: canonical,
            file: Mutex::new(Some(file)),
        })))
    }
    pub(crate) fn path(&self) -> &std::path::Path {
        &self.path
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
