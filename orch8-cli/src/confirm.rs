//! Confirmation gate for destructive commands (delete, prune, revoke, bulk
//! state changes).
//!
//! Interactive terminals get a `[y/N]` prompt; scripts must pass `--yes`,
//! otherwise the command refuses to run so a CI job can never destroy data
//! by accident.

use std::io::{BufRead as _, IsTerminal as _, Write as _};

use anyhow::{Result, bail};

/// Gate `action` (a short imperative, e.g. `delete cron schedule 42`).
pub fn confirm_destructive(action: &str, yes: bool) -> Result<()> {
    confirm_with(action, yes, std::io::stdin().is_terminal(), || {
        let mut line = String::new();
        std::io::stdin().lock().read_line(&mut line)?;
        Ok(line)
    })
}

/// Testable core of [`confirm_destructive`]: `stdin_is_tty` and the answer
/// reader are injected.
pub fn confirm_with(
    action: &str,
    yes: bool,
    stdin_is_tty: bool,
    read_answer: impl FnOnce() -> std::io::Result<String>,
) -> Result<()> {
    if yes {
        return Ok(());
    }
    if !stdin_is_tty {
        bail!("refusing to run destructive command without --yes ({action})");
    }
    eprint!("About to {action}. Continue? [y/N] ");
    let _ = std::io::stderr().flush();
    let answer = read_answer()?;
    if matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
        Ok(())
    } else {
        bail!("aborted: {action} was not confirmed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yes_flag_skips_every_check() {
        confirm_with("delete x", true, false, || panic!("must not read")).unwrap();
    }

    #[test]
    fn non_tty_without_yes_refuses() {
        let err = confirm_with("delete cron 1", false, false, || panic!("must not read"))
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "refusing to run destructive command without --yes (delete cron 1)"
        );
    }

    #[test]
    fn tty_accepts_y_and_yes_only() {
        confirm_with("prune", false, true, || Ok("y\n".into())).unwrap();
        confirm_with("prune", false, true, || Ok("YES\n".into())).unwrap();
        let err = confirm_with("prune", false, true, || Ok("\n".into()))
            .unwrap_err()
            .to_string();
        assert!(err.contains("aborted"), "{err}");
        let err = confirm_with("prune", false, true, || Ok("no\n".into()))
            .unwrap_err()
            .to_string();
        assert!(err.contains("aborted"), "{err}");
    }
}
