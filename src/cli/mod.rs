pub mod auto_complete;
use auto_complete::AutoComplete;

pub mod post_run;
pub use post_run::PostRunCmd;

use clap::{Parser, Subcommand};

#[derive(Debug, Subcommand)]
pub enum Commands {
    PostRunCmd(PostRunCmd),
    Autocomplete(AutoComplete),
}

#[derive(Debug, Parser)]
#[command(about, long_about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}
