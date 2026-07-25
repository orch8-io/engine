use sha2::{Digest as _, Sha256};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "fingerprint".into());
    let document = orch8_api::client_contract::validate_contract()?;
    match mode.as_str() {
        "rust" => print!("{}", orch8_api::client_contract::generate_rust_client()),
        "javascript" => print!(
            "{}",
            orch8_api::client_contract::generate_javascript_client()
        ),
        "fingerprint" => {
            let bytes = serde_json::to_vec(&document)?;
            for byte in Sha256::digest(bytes) {
                print!("{byte:02x}");
            }
            println!();
        }
        _ => return Err("usage: client_contract [rust|javascript|fingerprint]".into()),
    }
    Ok(())
}
