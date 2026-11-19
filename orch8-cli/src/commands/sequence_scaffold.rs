use std::path::PathBuf;

use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::commands::test_cmd::decode_authoring_sequence;

#[derive(Clone, Copy, clap::ValueEnum)]
enum HandlerLanguage {
    Typescript,
    Python,
    Swift,
    Kotlin,
}

#[derive(clap::Args)]
pub struct ScaffoldHandler {
    /// Local sequence definition.
    sequence: PathBuf,
    /// Step with a declared output schema.
    #[arg(long)]
    block: String,
    /// JSON or YAML file containing a representative handler output.
    #[arg(long)]
    example_output: PathBuf,
    /// JSON or YAML file containing sequence input for the generated case.
    #[arg(long)]
    input: Option<PathBuf>,
    /// New directory for the generated files.
    #[arg(long)]
    out_dir: PathBuf,
    /// Language for the handler stub.
    #[arg(long, value_enum, default_value = "typescript")]
    language: HandlerLanguage,
}

pub(super) fn run(args: &ScaffoldHandler) -> Result<()> {
    let sequence = decode_authoring_sequence(crate::seqdoc::read_document(&args.sequence)?)?;
    sequence.validate()?;
    let generated = orch8_engine::dataflow::generate_types(&sequence)?;
    let output_schema = generated.schema["outputs"]
        .get(&args.block)
        .with_context(|| format!("step '{}' does not declare an output schema", args.block))?;

    let example = crate::seqdoc::read_document(&args.example_output)?;
    let input = args
        .input
        .as_deref()
        .map(crate::seqdoc::read_document)
        .transpose()?
        .unwrap_or(Value::Null);
    let (types_file, types, handler_file, stub) = match args.language {
        HandlerLanguage::Typescript => {
            let block_type_key = serde_json::to_string(&args.block)?;
            let stub = format!(
                "import type {{ StepOutputs }} from \"./types.js\";\n\nexport type HandlerOutput = StepOutputs[{block_type_key}];\n\nexport async function execute(_params: unknown): Promise<HandlerOutput> {{\n  throw new Error(\"Implement this worker handler\");\n}}\n"
            );
            ("types.ts", generated.typescript, "handler.ts", stub)
        }
        language => {
            let mut type_source = sequence.clone();
            type_source.input_schema = Some(output_schema.clone());
            let types = orch8_engine::dataflow::generate_types(&type_source)?;
            match language {
                HandlerLanguage::Python => (
                    "handler_types.py", types.python, "handler.py",
                    "from handler_types import SequenceInput as HandlerOutput\n\nasync def execute(_params: object) -> HandlerOutput:\n    raise NotImplementedError(\"Implement this worker handler\")\n".into()
                ),
                HandlerLanguage::Swift => (
                    "HandlerTypes.swift", types.swift, "Handler.swift",
                    "public typealias HandlerOutput = SequenceInput\n\npublic struct HandlerUnimplemented: Error {}\n\npublic func execute(_ params: String) async throws -> HandlerOutput {\n    throw HandlerUnimplemented()\n}\n".into()
                ),
                HandlerLanguage::Kotlin => (
                    "HandlerTypes.kt", types.kotlin, "Handler.kt",
                    "import kotlinx.serialization.json.JsonElement\n\ntypealias HandlerOutput = SequenceInput\n\nsuspend fun execute(params: JsonElement): HandlerOutput =\n    error(\"Implement this worker handler\")\n".into()
                ),
                HandlerLanguage::Typescript => unreachable!(),
            }
        }
    };
    let contract = json!({
        "sequence_name": sequence.name,
        "cases": [{
            "name": format!("{} returns the expected output", args.block),
            "input": input,
            "mocks": [{"block": args.block, "type": "success", "output": example}],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": [args.block]},
                "assertions": [{"output": {"block": args.block, "path": ""},
                    "op": "equals", "value": example}],
            },
        }],
    });
    let contract = serde_json::to_string_pretty(&contract)?;
    std::fs::create_dir(&args.out_dir)
        .with_context(|| format!("creating new output directory {}", args.out_dir.display()))?;
    for (name, content) in [
        (types_file, types.as_str()),
        (handler_file, stub.as_str()),
        ("handler.contracts.json", contract.as_str()),
    ] {
        crate::atomic_write(&args.out_dir.join(name), content.as_bytes())?;
    }
    println!(
        "generated typed handler scaffold in {}",
        args.out_dir.display()
    );
    Ok(())
}
