//! Stream an authenticated backup envelope through the reference age tool.
use std::fs::File;
use std::io::{Read, Seek as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context as _, Result, bail, ensure};
use ed25519_dalek::VerifyingKey;

use super::{Archive, MAX_ARCHIVE_BYTES, read_archive_from};

const AGE_PREFIX: &[u8] = b"age-encryption.org/";
const MAX_ENCRYPTED_BYTES: u64 = MAX_ARCHIVE_BYTES as u64 + 1024 * 1024;
const MAX_RECIPIENTS: usize = 16;

pub(super) fn parse_recipient(recipient: &str) -> Result<String, String> {
    if recipient.len() != 62
        || !recipient.starts_with("age1")
        || !recipient[4..]
            .bytes()
            .all(|byte| b"qpzry9x8gf2tvdw0s3jn54khce6mua7l".contains(&byte))
    {
        return Err("enter a native age X25519 recipient beginning with age1".into());
    }
    Ok(recipient.into())
}

pub(super) fn validate_recipients(recipients: &[String]) -> Result<()> {
    ensure!(
        recipients.len() <= MAX_RECIPIENTS,
        "a backup may have at most {MAX_RECIPIENTS} recipients"
    );
    let mut seen = std::collections::HashSet::new();
    for recipient in recipients {
        parse_recipient(recipient).map_err(anyhow::Error::msg)?;
        ensure!(seen.insert(recipient), "duplicate age backup recipient");
    }
    Ok(())
}

pub(super) fn encrypt(archive: &Path, destination: &Path, recipients: &[String]) -> Result<u64> {
    let mut size = 0;
    crate::atomic_write_private_with(destination, |file| {
        let input = File::open(archive)?;
        let mut command = Command::new("age");
        command.arg("--encrypt");
        for recipient in recipients {
            command.arg("--recipient").arg(recipient);
        }
        let status = command
            .stdin(input)
            .stdout(file.try_clone()?)
            .stderr(Stdio::null())
            .status()
            .context("run age to encrypt the backup (install age to use recipient encryption)")?;
        ensure!(
            status.success(),
            "age backup encryption failed; check the recipient public keys"
        );
        size = file.metadata()?.len();
        ensure!(
            size <= MAX_ENCRYPTED_BYTES,
            "encrypted backup exceeds the archive size limit"
        );
        Ok(())
    })?;
    Ok(size)
}

pub(super) fn read(
    mut file: File,
    identities: &[PathBuf],
    trusted_key: Option<&VerifyingKey>,
) -> Result<Archive> {
    let mut prefix = [0_u8; AGE_PREFIX.len()];
    let encrypted = file
        .by_ref()
        .take(AGE_PREFIX.len() as u64)
        .read_exact(&mut prefix)
        .is_ok()
        && prefix == AGE_PREFIX;
    file.rewind()?;
    if !encrypted {
        ensure!(
            identities.is_empty(),
            "--identity requires an age-encrypted backup"
        );
        return read_archive_from(file, trusted_key);
    }
    ensure!(
        !identities.is_empty(),
        "this backup is encrypted; pass an age --identity file"
    );
    ensure!(
        file.metadata()?.len() <= MAX_ENCRYPTED_BYTES,
        "encrypted backup exceeds the archive size limit"
    );
    let mut command = Command::new("age");
    command.arg("--decrypt");
    for identity in identities {
        ensure!(
            identity != Path::new("-"),
            "an age identity must be a file path"
        );
        command.arg("--identity").arg(identity);
    }
    let mut child = command
        .stdin(file)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("run age to decrypt the backup (install age to restore it)")?;
    let stdout = child
        .stdout
        .take()
        .expect("age stdout was configured as a pipe");
    let archive = read_archive_from(stdout, trusted_key);
    if archive.is_err() {
        let _ = child.kill();
    }
    let status = child.wait().context("wait for age backup authentication")?;
    match archive {
        Ok(archive) if status.success() => Ok(archive),
        Ok(_) => bail!("age backup authentication failed"),
        Err(error) => Err(error).context("could not decrypt or verify the age backup"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_bounded_nonduplicate_x25519_recipient_lists() {
        let recipient = format!("age1{}", "q".repeat(58));
        assert!(parse_recipient(&recipient).is_ok());
        assert!(parse_recipient("age1plugin1example").is_err());
        assert!(parse_recipient(&format!("age1{}", "b".repeat(58))).is_err());
        assert!(validate_recipients(&[recipient.clone(), recipient.clone()]).is_err());
        assert!(validate_recipients(&vec![recipient; MAX_RECIPIENTS + 1]).is_err());
    }
}
