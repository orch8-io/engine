use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Subcommand;
use serde::Deserialize;
use serde_json::Value;

use crate::{atomic_write, print_table, templates};

const CATALOG_ENV: &str = "ORCH8_TEMPLATE_CATALOG_URL";
const MAX_CATALOG_BYTES: usize = 8 * 1024 * 1024;

/// Subcommands for browsing the built-in template gallery.
#[derive(Subcommand)]
pub enum TemplatesCmd {
    /// List built-in templates and, when configured, the cloud catalog.
    List {
        /// Catalog endpoint (or set `ORCH8_TEMPLATE_CATALOG_URL`).
        #[arg(long)]
        catalog_url: Option<String>,
    },
    /// Print a built-in or cloud template as JSON (or YAML with `--format yaml`).
    Show {
        /// Template name.
        name: String,
        /// Output syntax.
        #[arg(long, value_enum, default_value = "json")]
        format: crate::seqdoc::FormatArg,
        /// Catalog endpoint (or set `ORCH8_TEMPLATE_CATALOG_URL`).
        #[arg(long)]
        catalog_url: Option<String>,
    },
    /// Download a cloud or built-in template to a file.
    Pull {
        /// Template name.
        name: String,
        /// Destination path; a `.yaml` / `.yml` extension writes YAML.
        #[arg(long, default_value = "sequence.json")]
        out: PathBuf,
        /// Catalog endpoint (or set `ORCH8_TEMPLATE_CATALOG_URL`).
        #[arg(long)]
        catalog_url: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
struct CatalogTemplate {
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    sequence: Option<Value>,
    #[serde(default)]
    download_url: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum CatalogResponse {
    Array(Vec<CatalogTemplate>),
    Envelope { templates: Vec<CatalogTemplate> },
}

impl CatalogResponse {
    fn into_templates(self) -> Vec<CatalogTemplate> {
        match self {
            Self::Array(items) | Self::Envelope { templates: items } => items,
        }
    }
}

fn configured_url(explicit: Option<String>) -> Option<String> {
    explicit.or_else(|| std::env::var(CATALOG_ENV).ok())
}

async fn catalog(client: &reqwest::Client, url: &str) -> Result<Vec<CatalogTemplate>> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("fetching template catalog {url}"))?
        .error_for_status()
        .with_context(|| format!("template catalog {url} returned an error"))?;
    let body = bounded_body(response).await?;
    Ok(serde_json::from_slice::<CatalogResponse>(&body)?.into_templates())
}

async fn bounded_body(mut response: reqwest::Response) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_CATALOG_BYTES as u64)
    {
        bail!("template response exceeds the 8 MiB limit");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len().saturating_add(chunk.len()) > MAX_CATALOG_BYTES {
            bail!("template response exceeds the 8 MiB limit");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn template_json(
    client: &reqwest::Client,
    name: &str,
    catalog_url: Option<String>,
) -> Result<String> {
    if let Ok(template) = templates::find(name) {
        return Ok(template.json.to_string());
    }
    let url = configured_url(catalog_url).with_context(|| {
        format!("template '{name}' is not built in; pass --catalog-url or set {CATALOG_ENV}")
    })?;
    let item = catalog(client, &url)
        .await?
        .into_iter()
        .find(|item| item.name == name)
        .with_context(|| format!("template '{name}' not found in {url}"))?;
    if let Some(sequence) = item.sequence {
        return Ok(format!("{}\n", serde_json::to_string_pretty(&sequence)?));
    }
    if let Some(download_url) = item.download_url {
        let response = client.get(&download_url).send().await?.error_for_status()?;
        return String::from_utf8(bounded_body(response).await?)
            .context("downloaded template is not UTF-8 JSON or YAML");
    }
    bail!("catalog entry '{name}' has neither sequence nor download_url")
}

/// Re-render template text (JSON, or YAML from a catalog `download_url`) in
/// the requested syntax. JSON output of JSON text is passed through verbatim.
fn convert_template(text: &str, format: crate::seqdoc::DocumentFormat) -> Result<String> {
    use crate::seqdoc::DocumentFormat;
    let value: Value = match serde_json::from_str(text) {
        Ok(value) => {
            if format == DocumentFormat::Json {
                return Ok(text.to_string());
            }
            value
        }
        Err(_) => crate::seqdoc::parse_text(std::path::Path::new("template.yaml"), text)
            .context("template is neither JSON nor YAML")?,
    };
    crate::seqdoc::render(&value, format)
}

pub async fn run(cmd: TemplatesCmd) -> Result<()> {
    // Catalogs are a separate trust boundary: never send the engine client's
    // default x-api-key or tenant headers to them.
    let client = crate::external_client()?;
    match cmd {
        TemplatesCmd::List { catalog_url } => {
            let mut rows: Vec<Vec<String>> = templates::TEMPLATES
                .iter()
                .map(|t| {
                    vec![
                        t.name.to_string(),
                        "built-in".into(),
                        t.description.to_string(),
                    ]
                })
                .collect();
            if let Some(url) = configured_url(catalog_url) {
                rows.extend(
                    catalog(&client, &url)
                        .await?
                        .into_iter()
                        .map(|t| vec![t.name, "cloud".into(), t.description]),
                );
            }
            print_table(&["name", "source", "description"], &rows);
        }
        TemplatesCmd::Show {
            name,
            format,
            catalog_url,
        } => {
            let text = template_json(&client, &name, catalog_url).await?;
            print!("{}", convert_template(&text, format.into())?);
        }
        TemplatesCmd::Pull {
            name,
            out,
            catalog_url,
        } => {
            let text = template_json(&client, &name, catalog_url).await?;
            let rendered = convert_template(&text, crate::seqdoc::DocumentFormat::from_path(&out))?;
            atomic_write(&out, rendered.as_bytes())?;
            println!("downloaded {name} → {}", out.display());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::seqdoc::DocumentFormat;

    #[test]
    fn templates_convert_between_json_and_yaml() {
        let json = templates::find("default").unwrap().json;
        assert_eq!(convert_template(json, DocumentFormat::Json).unwrap(), json);
        let yaml = convert_template(json, DocumentFormat::Yaml).unwrap();
        let original: Value = serde_json::from_str(json).unwrap();
        let back = crate::seqdoc::parse_text(std::path::Path::new("t.yaml"), &yaml).unwrap();
        assert_eq!(back, original);
        // YAML catalog payloads convert back to JSON.
        let as_json = convert_template(&yaml, DocumentFormat::Json).unwrap();
        assert_eq!(serde_json::from_str::<Value>(&as_json).unwrap(), original);
    }
}
