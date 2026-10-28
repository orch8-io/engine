//! Resolve recovery ownership without opening or closing the database inode.
use orch8_types::error::StorageError;
use std::path::{Path, PathBuf};

/// Resolve an existing database or a new filename below an existing directory.
/// Only metadata operations are permitted here: closing even a temporary file
/// descriptor can cancel another SQLite connection's POSIX advisory locks.
pub fn resolve_sqlite_database_path(path: &str) -> Result<PathBuf, StorageError> {
    let path = Path::new(path);
    match std::fs::symlink_metadata(path) {
        Ok(_) => {
            let canonical = std::fs::canonicalize(path).map_err(|_| {
                StorageError::Connection("cannot resolve database ownership file".into())
            })?;
            let metadata = std::fs::metadata(&canonical).map_err(|_| {
                StorageError::Connection("cannot inspect database ownership file".into())
            })?;
            if !metadata.is_file() {
                return Err(StorageError::Unsupported(
                    "database must be a regular file".into(),
                ));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.nlink() != 1 {
                    return Err(StorageError::Unsupported(
                        "database hard links are unsupported".into(),
                    ));
                }
            }
            Ok(canonical)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let name = path
                .file_name()
                .ok_or_else(|| StorageError::Unsupported("database must name a file".into()))?;
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let parent = std::fs::canonicalize(parent).map_err(|_| {
                StorageError::Connection("cannot resolve database ownership directory".into())
            })?;
            Ok(parent.join(name))
        }
        Err(_) => Err(StorageError::Connection(
            "cannot inspect database ownership path".into(),
        )),
    }
}
