use std::{env, fs, path::PathBuf};

use orch8_api::openapi::ApiDoc;
use utoipa::OpenApi;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = env::args_os().nth(1).map_or_else(
        || PathBuf::from("../sdk-contract/openapi.json"),
        PathBuf::from,
    );
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output, ApiDoc::openapi().to_pretty_json()?)?;
    println!("wrote {}", output.display());
    Ok(())
}
