use chrono::NaiveDate;
use clap::Parser;
use slurm_usage_report_rs::UsageReportError;
use std::{io::ErrorKind, path::PathBuf};

use crate::cli::Cli;

const LONG_ABOUT: &str = concat!(
    "Aggrège les données quotidiennes d'utilisation du cluster.\n\n",
    "Produit un rapport montrant l'évolution de l'utilisation du cluster, par jour (aggrège quelques métriques sur une fenêtre de temps).",
);

fn parse_date(arg: &str) -> Result<chrono::NaiveDate, chrono::ParseError> {
    chrono::NaiveDate::parse_from_str(arg, "%Y-%m-%d")
}

#[derive(Debug, Parser)]
#[command(
    about = LONG_ABOUT.split("\n").next().unwrap_or("Cannot get the about string"),
    long_about = LONG_ABOUT
)]
pub struct UsageAggregate {
    /// Date du début de l'aggrégat au format YYYY-MM-DD
    #[arg(short, long, value_parser = parse_date)]
    from: NaiveDate,

    /// Date de fin de l'aggrégat au format YYYY-MM-DD
    #[arg(short, long, value_parser = parse_date)]
    to: NaiveDate,

    /// Chemin vers la base de données SACCT (collection de fichiers parquet)
    #[arg(short, long)]
    database: PathBuf,

    #[arg(short, long)]
    output: PathBuf,
}

impl UsageAggregate {
    pub fn run(&self, _cli: &Cli) -> Result<(), UsageReportError> {
        use slurm_usage_report_rs::daily_efficiency::generate_aggregate_report;

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
        generate_aggregate_report(
            &self.from.to_string(),
            &self.to.to_string(),
            self.database.as_path(),
            self.output.as_path(),
        )
    }
}
