pub mod post_run;
pub use post_run::PostRunCmd;

use clap::{Parser, Subcommand};

#[derive(Debug, Subcommand)]
pub enum Commands {
    PostRunCmd(PostRunCmd),
}

#[derive(Debug, Parser)]
#[command(version, about, long_about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}
