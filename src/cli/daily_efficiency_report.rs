use clap::{ArgAction::SetFalse, Parser};
use slurm_usage_report_rs::{UsageReportError, sacct_get, snakemake_parse_log, utils};
use std::{collections::HashSet, io::ErrorKind, path::PathBuf};

use crate::cli::Cli;

const LONG_ABOUT: &str = concat!(
    "Collecte les métriques d'utilisation quotidiennes du cluster.\n\n",
    "Produit un rapport quotidien avec les principales métriques d'intérêt pour évaluer la charge de travail sur le cluster.",
);

#[derive(Debug, Parser)]
#[command(
    about = LONG_ABOUT.split("\n").next().unwrap_or("Cannot get the about string"),
    long_about = LONG_ABOUT
)]
pub struct DailyEfficiency {}

impl DailyEfficiency {
    pub fn run(&self, _cli: &Cli) -> Result<(), UsageReportError> {
        Ok(())
    }
}
