use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Subcommand;

#[derive(Subcommand)]
pub enum ConfigCmd {
    /// Validate an orch8.toml config file without starting the server.
    Validate {
        /// Path to the config file.
        #[arg(default_value = "orch8.toml")]
        file: PathBuf,
    },
}

pub fn run(cmd: ConfigCmd) -> Result<()> {
    match cmd {
        ConfigCmd::Validate { file } => {
            let content = std::fs::read_to_string(&file)
                .with_context(|| format!("failed to read {}", file.display()))?;
            let cfg: orch8_types::config::EngineConfig = toml::from_str(&content)
                .with_context(|| format!("failed to parse {}", file.display()))?;
            match cfg.validate() {
                Ok(()) => {
                    println!("Config is valid.");
                    Ok(())
                }
                Err(errors) => {
                    eprintln!("Config validation failed:");
                    for e in &errors {
                        eprintln!("  - {e}");
                    }
                    anyhow::bail!("{} validation error(s)", errors.len());
                }
            }
        }
    }
}
