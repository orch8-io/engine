//! Validate one or more mobile sequence JSON files without starting an engine.

use std::path::PathBuf;

use orch8_types::sequence::SequenceDefinition;

fn main() {
    let paths: Vec<PathBuf> = std::env::args_os().skip(1).map(PathBuf::from).collect();
    if paths.is_empty() {
        eprintln!("usage: validate_mobile_sequences <sequence.json>...");
        std::process::exit(2);
    }

    let mut failed = false;
    for path in paths {
        let result = std::fs::read_to_string(&path)
            .map_err(|error| format!("read failed: {error}"))
            .and_then(|json| {
                serde_json::from_str::<SequenceDefinition>(&json)
                    .map_err(|error| format!("JSON/schema invalid: {error}"))
            })
            .and_then(|sequence| {
                sequence
                    .validate()
                    .map_err(|error| format!("sequence invalid: {error}"))?;
                Ok(sequence)
            });

        match result {
            Ok(sequence) => println!(
                "ok\t{}\tv{}\t{}",
                sequence.name,
                sequence.version,
                path.display()
            ),
            Err(error) => {
                failed = true;
                eprintln!("error\t{}\t{error}", path.display());
            }
        }
    }

    if failed {
        std::process::exit(1);
    }
}
