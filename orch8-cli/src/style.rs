//! Terminal styling that honours `NO_COLOR`, `--no-color`, and non-TTY stdout.
//!
//! Colour is decided once at startup ([`init`]) and read through a process
//! global so every module — including `orch8 dev`, which runs before the
//! HTTP client is built — renders identically. Until `init` runs (unit
//! tests, library use) styling is off, which keeps assertions on output
//! deterministic.

use std::fmt::Display;
use std::io::IsTerminal as _;
use std::sync::atomic::{AtomicBool, Ordering};

static ENABLED: AtomicBool = AtomicBool::new(false);

/// Decide whether ANSI colour should be emitted.
///
/// Off when `--no-color` was passed, when `NO_COLOR` is set to a non-empty
/// value (<https://no-color.org>), or when stdout is not a terminal.
#[must_use]
pub fn decide(no_color_flag: bool, no_color_env: bool, stdout_is_tty: bool) -> bool {
    !no_color_flag && !no_color_env && stdout_is_tty
}

/// Read the environment and stdout once and fix the colour mode.
pub fn init(no_color_flag: bool) {
    let env = std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty());
    set_enabled(decide(no_color_flag, env, std::io::stdout().is_terminal()));
}

/// Force the colour mode (tests, embedding).
pub fn set_enabled(enabled: bool) {
    ENABLED.store(enabled, Ordering::Relaxed);
}

/// Whether styled output is currently enabled.
#[must_use]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Chainable ANSI styling for anything printable. Every method returns a
/// `String`, so `"x".yellow().bold()` composes and degrades to plain text
/// when colour is disabled.
pub trait Paint: Display {
    /// Wrap in an SGR sequence when colour is enabled.
    fn paint(&self, sgr: &str) -> String {
        if enabled() {
            format!("\x1b[{sgr}m{self}\x1b[0m")
        } else {
            self.to_string()
        }
    }
    /// Bold weight.
    fn bold(&self) -> String {
        self.paint("1")
    }
    /// Dim / faint.
    fn dimmed(&self) -> String {
        self.paint("2")
    }
    /// Red foreground.
    fn red(&self) -> String {
        self.paint("31")
    }
    /// Green foreground.
    fn green(&self) -> String {
        self.paint("32")
    }
    /// Yellow foreground.
    fn yellow(&self) -> String {
        self.paint("33")
    }
    /// Blue foreground.
    fn blue(&self) -> String {
        self.paint("34")
    }
    /// Cyan foreground.
    fn cyan(&self) -> String {
        self.paint("36")
    }
}

impl<T: Display + ?Sized> Paint for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decide_requires_tty_and_no_opt_out() {
        assert!(decide(false, false, true));
        assert!(!decide(true, false, true), "--no-color wins");
        assert!(!decide(false, true, true), "NO_COLOR wins");
        assert!(!decide(false, false, false), "pipes get plain text");
    }

    #[test]
    fn paint_is_plain_when_disabled_and_ansi_when_enabled() {
        // Tests share the global; restore whatever was set before.
        let previous = enabled();
        set_enabled(false);
        assert_eq!("warn".yellow().bold(), "warn");
        set_enabled(true);
        assert_eq!("warn".yellow(), "\x1b[33mwarn\x1b[0m");
        assert_eq!(
            String::from("x").bold(),
            "\x1b[1mx\x1b[0m",
            "String receivers work too"
        );
        set_enabled(previous);
    }
}
