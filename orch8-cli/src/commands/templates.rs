use anyhow::Result;
use clap::Subcommand;

use crate::{print_table, templates};

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

pub fn run(cmd: TemplatesCmd) -> Result<()> {
    match cmd {
        TemplatesCmd::List => {
            let rows: Vec<Vec<String>> = templates::TEMPLATES
                .iter()
                .map(|t| vec![t.name.to_string(), t.description.to_string()])
                .collect();
            print_table(&["name", "description"], &rows);
        }
        TemplatesCmd::Show { name } => {
            let template = templates::find(&name)?;
            // Templates carry their own trailing newline.
            print!("{}", template.json);
        }
    }
    Ok(())
}
