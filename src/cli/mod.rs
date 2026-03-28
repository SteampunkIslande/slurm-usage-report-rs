mod auto_complete;
use auto_complete::*;

mod post_run;
use post_run::*;

mod csv_to_parquet;
use csv_to_parquet::*;

mod daily_efficiency_report;
use daily_efficiency_report::*;

use clap::{Parser, Subcommand};

#[derive(Debug, Subcommand)]
pub enum Commands {
    PostRunCmd(PostRunCmd),
    Autocomplete(AutoComplete),
    CsvToParquet(CsvToParquet),
    DailyEfficiency(DailyEfficiency),
}

#[derive(Debug, Parser)]
#[command(about, long_about)]
pub struct Cli {
    /// Set verbose (ON/OFF)
    #[arg(long, short)]
    pub verbose: bool,

    #[command(subcommand)]
    pub command: Commands,
}
