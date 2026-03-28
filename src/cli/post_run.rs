use clap::{ArgAction::SetFalse, Parser};
use slurm_usage_report_rs::{UsageReportError, sacct_get, snakemake_parse_log, utils};
use std::{collections::HashSet, io::ErrorKind, path::PathBuf};

use crate::cli::Cli;

const LONG_ABOUT: &str = concat!(
    "Programme post-run pour générer un rapport d'usage Slurm à partir des logs de Snakemake.\n\n",
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

    /// Chemin vers le dossier
    #[arg(long, short)]
    output_dir: Option<PathBuf>,

    /// Ne pas ajouter les métriques relatives aux tailles des fichiers d'entrée
    #[arg(long, action = SetFalse)]
    no_input_size_relative_metrics: bool,

    #[arg(long, action = SetFalse)]
    no_tabs: bool,

    /// Chemin vers la base de données SACCT maison du cluster (dossier avec les fichiers parquet).
    ///
    /// Permet de contourner sacct en cherchant directement les données dans les fichiers parquet
    #[arg(short, long)]
    db: Option<PathBuf>,

    /// Force l'écriture du dossier de sortie si ce dernier existe déjà.
    ///
    /// Attention, cela supprime le dossier déjà existant avant même de démarrer.
    #[arg(short, long)]
    force: bool,
}

fn checks(command: &PostRunCmd, _cli: &Cli) -> Result<(), UsageReportError> {
    if let Some(db) = &command.db {
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
        }
    }
    if let Some(outdir) = &command.output_dir
        && outdir.exists()
        && !command.force
    {
        return Err(UsageReportError::IOError(std::io::Error::new(
            ErrorKind::AlreadyExists,
            format!(
                "Le dossier de sortie existe déjà. Pour forcer l'écriture dans ce dossier, utilisez l'option -f/--force"
            ),
        )));
    }
    Ok(())
}

impl PostRunCmd {
    pub fn run(&self, cli: &Cli) -> Result<(), UsageReportError> {
        // Run checks before even thinking about running this CLI entry point.
        checks(self, cli)?;

        let output_dir: PathBuf = self
            .output_dir
            .clone()
            .unwrap_or(std::env::current_dir()?.join("usage-report"));

        if output_dir.exists() && self.force {
            std::fs::remove_dir_all(&output_dir)?;
        }
        std::fs::create_dir_all(&output_dir)?;

        let output_html: PathBuf = output_dir.join("usage-report.html");
        let output_parquet: PathBuf = output_dir.join("usage-report.parquet");

        // List of slurm names to gather metrics for
        let slurm_job_names = snakemake_parse_log::get_slurm_ids(&self.input)?;

        // Could be confusing, but if no_input_size_relative_metrics is true, this means the user wants them (it's a negative flag)
        let input_sizes_path = if self.no_input_size_relative_metrics {
            let input_sizes_path = output_dir.join("input_sizes.csv");
            // Get input_sizes. If we fail, we remove them
            snakemake_parse_log::parse_snakemake_log_files(&self.input, &input_sizes_path)?;
            Some(input_sizes_path)
        } else {
            None
        };

        // Path to the temporary intermediary sacct.csv. Will only be kept if we cannot convert it to parquet
        let temp_sacct_csv = if self.db.is_none() {
            Some(output_dir.join("sacct.csv"))
        } else {
            None
        };

        let temp_sacct_parquet = output_dir.join("sacct.parquet");
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
                        .filter(|p| p.exists()) // This could make them empty...
                        .collect();
                    if input_parquets.is_empty() {
                        return Err(UsageReportError::EmptyList {
                            listname: "input_parquets".into(),
                            message: "La liste des fichiers parquet pour l'analyse est vide. Impossible de continuer".into(),
                        });
                    }
                    utils::merge_parquets(&input_parquets, &temp_sacct_parquet)?;
                }
            }
            None => {
                sacct_get::get_sacct_for_runs(
                    &slurm_job_names,
                    temp_sacct_csv
                        .as_ref()
                        .ok_or(UsageReportError::NoneValueError {
                            message: "Logical error".into(),
                            file: file!().into(),
                            line: line!(),
                            column: column!(),
                        })?,
                )?;
                let removed_lines = utils::sacct_sanitizer(
                    temp_sacct_csv
                        .as_ref()
                        .ok_or(UsageReportError::NoneValueError {
                            message: "Logical error".into(),
                            file: file!().into(),
                            line: line!(),
                            column: column!(),
                        })?,
                    None,
                    None,
                )?;
                if cli.verbose {
                    eprintln!("Removed {} lines from SACCT output", removed_lines);
                }
                utils::csv_to_parquet(
                    temp_sacct_csv
                        .as_ref()
                        .ok_or(UsageReportError::NoneValueError {
                            message: "Logical error".into(),
                            file: file!().into(),
                            line: line!(),
                            column: column!(),
                        })?
                        .as_path(),
                    temp_sacct_parquet.as_path(),
                )?;
            }
        }
        use slurm_usage_report_rs::post_run::generate_snakemake_efficiency_report;

        generate_snakemake_efficiency_report(
            output_html.as_path(),
            temp_sacct_parquet.as_path(),
            slurm_job_names,
            output_parquet.as_path(),
            input_sizes_path.as_deref(),
            self.no_tabs,
        )?;
        Ok(())
    }
}
