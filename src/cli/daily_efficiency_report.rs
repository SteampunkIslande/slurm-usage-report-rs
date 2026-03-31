use chrono::NaiveDate;
use clap::Parser;
use slurm_usage_report_rs::UsageReportError;
use std::{io::ErrorKind, path::PathBuf};

use crate::cli::Cli;

const LONG_ABOUT: &str = concat!(
    "Collecte les métriques d'utilisation quotidiennes du cluster.\n\n",
    "Produit un rapport quotidien avec les principales métriques d'intérêt pour évaluer la charge de travail sur le cluster.",
);

fn parse_date(arg: &str) -> Result<chrono::NaiveDate, chrono::ParseError> {
    chrono::NaiveDate::parse_from_str(arg, "%Y-%m-%d")
}

#[derive(Debug, Parser)]
#[command(
    about = LONG_ABOUT.split("\n").next().unwrap_or("Cannot get the about string"),
    long_about = LONG_ABOUT
)]
pub struct DailyEfficiency {
    /// Date du rapport au format YYYY-MM-DD
    #[arg(long, value_parser = parse_date)]
    date: NaiveDate,

    /// Chemin vers la base de données SACCT (collection de fichiers parquet)
    #[arg(long)]
    database: PathBuf,

    #[arg(long)]
    html_output: PathBuf,

    #[arg(long)]
    json_output: PathBuf,
}

impl DailyEfficiency {
    pub fn run(&self, _cli: &Cli) -> Result<(), UsageReportError> {
        use slurm_usage_report_rs::daily_efficiency::generate_daily_report;

        if !self.database.exists() {
            return Err(UsageReportError::IOError(std::io::Error::new(
                ErrorKind::NotFound,
                format!("{} does not exist!", self.database.display()),
            )));
        }
        if !self.database.is_dir() {
            return Err(UsageReportError::IOError(std::io::Error::new(
                ErrorKind::NotADirectory,
                format!("{} is not a directory!", self.database.display()),
            )));
        }

        generate_daily_report(
            self.date.format("%Y-%m-%d").to_string().as_str(),
            self.database.as_path(),
            self.html_output.as_path(),
            self.json_output.as_path(),
            None,
        )
    }
}
