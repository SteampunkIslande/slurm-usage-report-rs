use clap::{CommandFactory, Parser};
use std::{fs::OpenOptions, path::PathBuf};

use clap_complete::{Shell, generate};

use crate::cli::Cli;

#[derive(Debug, Parser)]
#[command(about = "Générer l'autocomplétion")]
pub struct AutoComplete {
    /// Nom du fichier d'autocomplétion
    #[arg(short, long)]
    pub output: PathBuf,

    /// Type de shell
    #[arg(short, long)]
    pub shell: Shell,

    /// Force overwrite
    #[arg(short, long)]
    pub force: bool,
}

impl AutoComplete {
    pub fn run(&self, _cli: &Cli) {
        let mut command = self::Cli::command();
        match OpenOptions::new()
            .create(true)
            .create_new(!self.force)
            .write(true)
            .open(&self.output)
        {
            Ok(mut complete_file) => {
                generate(
                    self.shell,
                    &mut command,
                    "slurm-usage-report-rs",
                    &mut complete_file,
                );
            }
            Err(e) => {
                eprintln!("Impossible de générer le script d'autocomplétion: {e}")
            }
        }
    }
}
