use anyhow::Result;
use clap::Subcommand;
use tabled::{Table, Tabled};

use crate::templates;

/// Subcommands for browsing the built-in template gallery.
#[derive(Subcommand)]
pub enum TemplatesCmd {
    /// List built-in sequence templates.
    List,
    /// Print the raw JSON of a template to stdout.
    Show {
        /// Template name (see `orch8 templates list`).
        name: String,
    },
}

#[derive(Tabled)]
struct TemplateRow {
    name: &'static str,
    description: &'static str,
}

pub fn run(cmd: TemplatesCmd) -> Result<()> {
    match cmd {
        TemplatesCmd::List => {
            let rows: Vec<TemplateRow> = templates::TEMPLATES
                .iter()
                .map(|t| TemplateRow {
                    name: t.name,
                    description: t.description,
                })
                .collect();
            println!("{}", Table::new(rows));
        }
        TemplatesCmd::Show { name } => {
            let template = templates::find(&name)?;
            // Templates carry their own trailing newline.
            print!("{}", template.json);
        }
    }
    Ok(())
}
