//! `rust_embed`'s `#[derive(Embed)]` on `DashboardAssets` (see
//! `src/commands/dev_server.rs`) requires `../dashboard/dist/` to exist at
//! compile time, or the macro silently fails to implement the `Embed` trait
//! (a `cargo build` error, not a runtime one). The dashboard build is
//! optional — a dev server without it just serves no assets — so ensure the
//! directory exists rather than requiring every fresh checkout to build the
//! dashboard first.

use std::fs;
use std::path::Path;

fn main() {
    let dist = Path::new(env!("CARGO_MANIFEST_DIR")).join("../dashboard/dist");
    if !dist.exists() {
        fs::create_dir_all(&dist).expect("failed to create dashboard/dist placeholder");
    }
    println!("cargo:rerun-if-changed={}", dist.display());
}
