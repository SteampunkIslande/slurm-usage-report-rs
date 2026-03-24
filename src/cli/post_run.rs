use clap::Parser;
use slurm_usage_report_rs::{UsageReportError, sacct_get, snakemake_parse_log, utils};
use std::{
    collections::HashSet,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use crate::cli::Cli;

const LONG_ABOUT: &str = concat!(
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

    /// Conserver tous les fichiers de sortie intermédiaires.
    ///
    /// Concerne:
    /// - `sacct.parquet` (si `--db` n'est pas précisé): le résultat de la commande sacct pour les runs spécifiés (au format parquet)
    /// - `input_sizes.csv`: la liste des `slurm_jobid`, `job_id`, `rule_name`, `input_size_bytes`, `inputs` pour chaque job snakemake.
    #[arg(long, short)]
    keep_all_outputs: bool,

    /// Chemin vers la base de données SACCT maison du cluster (dossier avec les fichiers parquet).
    ///
    /// Permet de contourner sacct en cherchant directement les données dans les fichiers parquet
    #[arg(short, long)]
    db: Option<PathBuf>,
}

impl PostRunCmd {
    pub fn run(&self, cli: &Cli) -> Result<(), UsageReportError> {
        let slurm_job_names = snakemake_parse_log::get_slurm_ids(
            &self.input.iter().map(|p| p.as_path()).collect::<Vec<_>>(),
        )?;

        snakemake_parse_log::parse_snakemake_log_files(
            &self.input.iter().map(|p| p.as_path()).collect::<Vec<_>>(),
            Path::new("input_sizes.csv"),
        )?;

        let temp_sacct_csv = if self.db.is_none() {
            Some(Path::new("sacct.csv"))
        } else {
            None
        };

        let temp_sacct_parquet = PathBuf::from("sacct.parquet");
        // Raw parquet files that will be used to analyze slurm usage for snakemake runs given as input
        match &self.db {
            Some(db) => {
                if !db.is_dir() {
                    return Err(UsageReportError::IOError(std::io::Error::new(
                        ErrorKind::NotADirectory,
                        format!("{} is not a directory!", db.display()),
                    )));
                } else if !db.exists() {
                    return Err(UsageReportError::IOError(std::io::Error::new(
                        ErrorKind::NotFound,
                        format!("{} does not exist!", db.display()),
                    )));
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
                    utils::merge_parquets(
                        &input_parquets
                            .iter()
                            .map(|p| p.as_path())
                            .collect::<Vec<_>>(),
                        &temp_sacct_parquet,
                    )?;
                }
            }
            None => {
                sacct_get::get_sacct_for_runs(
                    &slurm_job_names
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>(),
                    temp_sacct_csv.ok_or(UsageReportError::NoneValueError {
                        message: "Logical error".into(),
                        file: file!().into(),
                        line: line!(),
                        column: column!(),
                    })?,
                )?;
                let removed_lines = utils::sacct_sanitizer(Path::new("sacct.csv"), None, None)?;
                if cli.verbose {
                    eprintln!("Removed {} lines from SACCT output", removed_lines);
                }
            }
        }
        use slurm_usage_report_rs::post_run::generate_snakemake_efficiency_report;

        generate_snakemake_efficiency_report(
            &self.output_html,
            &temp_sacct_parquet,
            &slurm_job_names
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>(),
            self.output_parquet.as_deref(),
            Some(Path::new("input_sizes.csv")),
        )?;
        Ok(())
    }
}
