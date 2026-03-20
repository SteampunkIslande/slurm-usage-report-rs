use clap::Parser;
use std::path::PathBuf;

use clap_complete::Shell;

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
