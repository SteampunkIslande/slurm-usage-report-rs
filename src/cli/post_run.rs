use clap::Parser;
use slurm_usage_report_rs::{sacct_get, snakemake_parse_log, utils};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    str::FromStr,
};

use crate::cli::Cli;

const LONG_ABOUT: &'static str = concat!(
    "Script post-run pour générer un rapport d'usage Slurm à partir des logs de Snakemake.\n\n",
    "Possibilité de spécifier plusieurs fichiers de log (ex: .snakemake/log/xxx.log) pour consolider les métriques\n",
    "à partir de plusieurs runs de Snakemake mais avec le même pipeline."
);

#[derive(Debug, Parser)]
#[command(
    about = LONG_ABOUT.split("\n").next().unwrap_or("Cannot get the about string"),
    long_about = LONG_ABOUT
)]
pub struct PostRunCmd {
    /// Chemin vers le(s) fichier(s) de log snakemake.
    #[arg(short, long, num_args = 1..)]
    input: Vec<PathBuf>,

    /// Chemin vers le rapport html d'utilisation du cluster pour le(s) exécutions de snakemake.
    #[arg(long)]
    output_html: PathBuf,

    /// Chemin vers le fichier parquet de rapport d'utilisation
    #[arg(long)]
    output_parquet: Option<PathBuf>,

    /// Chemin vers la base de données SACCT maison du cluster (dossier avec les fichiers parquet).
    ///
    /// Permet de contourner sacct en cherchant directement les données dans les fichiers parquet
    #[arg(short, long)]
    db: Option<PathBuf>,
}

impl PostRunCmd {
    pub fn run(&self, cli: &Cli) {
        let slurm_job_names = snakemake_parse_log::get_slurm_ids(
            &self.input.iter().map(|p| p.as_path()).collect::<Vec<_>>(),
        )
        .expect("Error reading log files:");

        snakemake_parse_log::parse_snakemake_log_files(
            &self.input.iter().map(|p| p.as_path()).collect::<Vec<_>>(),
            &Path::new("input_sizes.csv"),
        )
        .expect("Error trying to get input sizes");

        // Raw parquet files that will be used to analyze slurm usage for snakemake runs given as input
        let sacct_parquets: Vec<PathBuf> = match &self.db {
            Some(db) => {
                if !db.exists() {
                    panic!("{} doesn't exist", db.display());
                } else {
                    let mut dates: HashSet<String> = HashSet::new();
                    for path in &self.input {
                        dates.extend(snakemake_parse_log::get_snakemake_run_span(path.as_path()));
                    }
                    let input_parquets: Vec<PathBuf> = dates
                        .into_iter()
                        .map(|p| db.join(p).with_added_extension("parquet"))
                        .filter(|p| p.exists())
                        .collect();
                    input_parquets
                }
            }
            None => {
                sacct_get::get_sacct_for_runs(
                    &slurm_job_names
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>(),
                    Path::new("sacct.csv"),
                )
                .expect("Error trying to get sacct info");
                let removed_lines = utils::sacct_sanitizer(&Path::new("sacct.csv"), None, None)
                    .expect("Error trying to sanitize sacct CSV");
                if cli.verbose {
                    eprintln!("Removed {} lines from SACCT output", removed_lines);
                }
                vec![
                    PathBuf::from_str("sacct.csv")
                        .expect("Cannot get path sacct.csv")
                        .with_extension("parquet"),
                ]
            }
        };
    }
}
