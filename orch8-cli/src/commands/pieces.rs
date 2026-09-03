use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Subcommand;
use serde::Deserialize;

use crate::print_table;

#[derive(Subcommand)]
pub enum PiecesCmd {
    /// Search the Activepieces sidecar connector catalog.
    Search {
        query: Option<String>,
        #[arg(
            long,
            env = "ORCH8_PIECES_URL",
            default_value = "http://127.0.0.1:3001"
        )]
        sidecar_url: String,
    },
    /// Install a connector package into an Activepieces sidecar directory.
    Install {
        name: String,
        #[arg(long, default_value = "activepieces")]
        dir: PathBuf,
    },
}

#[derive(Deserialize)]
struct Catalog {
    pieces: Vec<Piece>,
}

#[derive(Deserialize)]
struct Piece {
    name: String,
    package: String,
    description: String,
    installed: bool,
}

pub async fn run(cmd: PiecesCmd) -> Result<()> {
    match cmd {
        PiecesCmd::Search { query, sidecar_url } => {
            let client = reqwest::Client::new();
            let mut request = client.get(format!("{}/catalog", sidecar_url.trim_end_matches('/')));
            if let Some(query) = query {
                request = request.query(&[("q", query)]);
            }
            let catalog: Catalog = request
                .send()
                .await
                .context("connecting to the Activepieces sidecar")?
                .error_for_status()?
                .json()
                .await?;
            let rows = catalog
                .pieces
                .into_iter()
                .map(|piece| {
                    vec![
                        piece.name,
                        piece.package,
                        piece.installed.to_string(),
                        piece.description,
                    ]
                })
                .collect::<Vec<_>>();
            print_table(&["name", "package", "installed", "description"], &rows);
        }
        PiecesCmd::Install { name, dir } => {
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            {
                bail!("piece name must contain only lowercase letters, digits, and hyphens");
            }
            let package = format!("@activepieces/piece-{name}");
            let status = tokio::process::Command::new("npm")
                .args(["install", "--save", &package])
                .current_dir(&dir)
                .status()
                .await
                .with_context(|| format!("running npm in {}", dir.display()))?;
            if !status.success() {
                bail!("npm failed to install {package}");
            }
            println!("installed {package} in {}", dir.display());
        }
    }
    Ok(())
}
