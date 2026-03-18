pub mod cli;
pub mod parse;
use clap::Parser;
use cli::*;

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::PostRunCmd(post_run_command) => {
            post_run_command.run();
        }
    }
}
