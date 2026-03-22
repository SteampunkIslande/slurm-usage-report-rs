use clap::Parser;
use slurm_usage_report_rs::utils;
use std::path::PathBuf;

use crate::cli::Cli;

#[derive(Debug, Parser)]
#[command(about = "Convertir un fichier CSV en parquet")]
pub struct CsvToParquet {
    /// Nom du fichier d'entrée
    #[arg(short, long)]
    pub input: PathBuf,

    /// Nom du fichier de sortie
    #[arg(short, long)]
    pub output: PathBuf,

    /// Force overwrite
    #[arg(short, long)]
    pub force: bool,
}

impl CsvToParquet {
    pub fn run(&self, _cli: &Cli) {
        if self.output.exists() && !self.force {
            eprintln!(
                "{} already exists! You can pass the --force flag if you want to overwrite it",
                self.output.display()
            );
        } else {
            utils::csv_to_parquet(self.input.as_path(), self.output.as_path())
                .expect("Error while converting CSV to parquet");
        }
    }
}
