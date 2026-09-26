//! `orch8 prompt` — versioned prompt registry (push, list, get, label).

use anyhow::{Context, Result, bail};
use clap::Subcommand;
use reqwest::Client;
use serde_json::{Value, json};

use crate::{OutputFormat, print_response};

#[derive(Subcommand)]
pub enum PromptCmd {
    /// Push a prompt version from a JSON file (`name`, `system`, `messages`,
    /// `model_params`, `response_schema`, `description`). Identical content
    /// returns the existing latest version.
    Push {
        /// Path to the prompt JSON file (`-` reads stdin).
        file: String,
        /// Override the file's `name`.
        #[arg(long)]
        name: Option<String>,
        /// Point this label (e.g. `production`) at the pushed version.
        #[arg(long)]
        label: Option<String>,
    },
    /// List prompts with their latest version and labels.
    List,
    /// Show a prompt: all versions + labels, or one resolved version.
    Get {
        name: String,
        /// Exact version.
        #[arg(long, conflicts_with = "label")]
        version: Option<i32>,
        /// Resolve through a label (its stable version).
        #[arg(long)]
        label: Option<String>,
    },
    /// Point a label at a version, optionally with a canary split.
    Label {
        name: String,
        label: String,
        /// Stable version the label points at.
        #[arg(long)]
        version: i32,
        /// Candidate version for a canary split.
        #[arg(long, requires = "canary_percent")]
        canary_version: Option<i32>,
        /// Percent (0-100) of executions routed to the canary version.
        #[arg(long, requires = "canary_version")]
        canary_percent: Option<u8>,
    },
    /// Remove a label (versions are immutable and stay).
    Unlabel { name: String, label: String },
}

fn read_prompt_file(path: &str) -> Result<Value> {
    let text = if path == "-" {
        std::io::read_to_string(std::io::stdin()).context("reading prompt from stdin")?
    } else {
        std::fs::read_to_string(path).with_context(|| format!("reading {path}"))?
    };
    let value: Value =
        serde_json::from_str(&text).with_context(|| format!("{path} is not valid JSON"))?;
    if !value.is_object() {
        bail!("{path}: a prompt file must be a JSON object");
    }
    Ok(value)
}

pub async fn run(client: &Client, base: &str, cmd: PromptCmd, format: OutputFormat) -> Result<()> {
    let resp = match cmd {
        PromptCmd::Push { file, name, label } => {
            let mut body = read_prompt_file(&file)?;
            let obj = body.as_object_mut().expect("checked object");
            if let Some(name) = name {
                obj.insert("name".into(), json!(name));
            }
            if let Some(label) = label {
                obj.insert("label".into(), json!(label));
            }
            if !obj.get("name").is_some_and(Value::is_string) {
                bail!("the prompt needs a `name` (in the file or via --name)");
            }
            client
                .post(format!("{base}/prompts"))
                .json(&body)
                .send()
                .await?
        }
        PromptCmd::List => client.get(format!("{base}/prompts")).send().await?,
        PromptCmd::Get {
            name,
            version,
            label,
        } => {
            if version.is_none() && label.is_none() {
                client.get(format!("{base}/prompts/{name}")).send().await?
            } else {
                let mut query: Vec<(&str, String)> = Vec::new();
                if let Some(v) = version {
                    query.push(("version", v.to_string()));
                }
                if let Some(l) = label {
                    query.push(("label", l));
                }
                client
                    .get(format!("{base}/prompts/{name}/resolve"))
                    .query(&query)
                    .send()
                    .await?
            }
        }
        PromptCmd::Label {
            name,
            label,
            version,
            canary_version,
            canary_percent,
        } => {
            if canary_percent.is_some_and(|p| p > 100) {
                bail!("--canary-percent must be 0-100");
            }
            client
                .put(format!("{base}/prompts/{name}/labels/{label}"))
                .json(&json!({
                    "version": version,
                    "canary_version": canary_version,
                    "canary_percent": canary_percent,
                }))
                .send()
                .await?
        }
        PromptCmd::Unlabel { name, label } => {
            client
                .delete(format!("{base}/prompts/{name}/labels/{label}"))
                .send()
                .await?
        }
    };
    print_response(resp, format).await
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use axum::http::Method;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::commands::test_support::mock_api;

    #[tokio::test]
    async fn prompt_commands_use_the_expected_contract() {
        let server = mock_api().await;
        let client = Client::new();
        let mut file = NamedTempFile::new().unwrap();
        write!(
            file,
            r#"{{"name": "file-name", "system": "Classify {{{{ product }}}}",
                "messages": [{{"role": "user", "content": "{{{{ ticket }}}}"}}]}}"#
        )
        .unwrap();

        run(
            &client,
            &server.base,
            PromptCmd::Push {
                file: file.path().to_string_lossy().into_owned(),
                name: Some("triage".into()),
                label: Some("production".into()),
            },
            OutputFormat::Json,
        )
        .await
        .unwrap();
        run(&client, &server.base, PromptCmd::List, OutputFormat::Json)
            .await
            .unwrap();
        run(
            &client,
            &server.base,
            PromptCmd::Get {
                name: "triage".into(),
                version: None,
                label: Some("production".into()),
            },
            OutputFormat::Json,
        )
        .await
        .unwrap();
        run(
            &client,
            &server.base,
            PromptCmd::Label {
                name: "triage".into(),
                label: "production".into(),
                version: 1,
                canary_version: Some(2),
                canary_percent: Some(10),
            },
            OutputFormat::Json,
        )
        .await
        .unwrap();

        let reqs = server.log.snapshot();
        assert_eq!(reqs[0].method, Method::POST);
        assert_eq!(reqs[0].uri, "/prompts");
        assert_eq!(reqs[0].body["name"], "triage", "--name overrides the file");
        assert_eq!(reqs[0].body["label"], "production");
        assert_eq!(reqs[0].body["system"], "Classify {{ product }}");
        assert_eq!(reqs[1].uri, "/prompts");
        assert_eq!(reqs[2].uri, "/prompts/triage/resolve?label=production");
        assert_eq!(reqs[3].method, Method::PUT);
        assert_eq!(reqs[3].uri, "/prompts/triage/labels/production");
        assert_eq!(reqs[3].body["canary_percent"], 10);
    }

    #[tokio::test]
    async fn push_requires_a_name_and_an_object() {
        let mut file = NamedTempFile::new().unwrap();
        write!(file, r#"{{"system": "x"}}"#).unwrap();
        let err = run(
            &Client::new(),
            "http://127.0.0.1:1",
            PromptCmd::Push {
                file: file.path().to_string_lossy().into_owned(),
                name: None,
                label: None,
            },
            OutputFormat::Json,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("needs a `name`"), "{err}");
    }
}
