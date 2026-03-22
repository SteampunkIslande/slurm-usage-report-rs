pub mod auto_complete;
use auto_complete::*;

pub mod post_run;
pub use post_run::*;

pub mod csv_to_parquet;
pub use csv_to_parquet::*;

use clap::{Parser, Subcommand};

#[derive(Debug, Subcommand)]
pub enum Commands {
    PostRunCmd(PostRunCmd),
    Autocomplete(AutoComplete),
    CsvToParquet(CsvToParquet),
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
