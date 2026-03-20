pub mod cli;
use clap::{CommandFactory, Parser};
use clap_complete::{
    generate,
    shells::{Bash, Zsh},
};
use cli::*;
use std::env;
use std::io;

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::PostRunCmd(post_run_command) => {
            post_run_command.run();
        }
        Commands::Generate => {
            let mut command = cli::Cli::command();
            if env::var("SHELL")
                .unwrap_or("/bin/bash".into())
                .to_lowercase()
                .contains("zsh")
            {
                generate(
                    Zsh,
                    &mut command,
                    "slurm-usage-report-rs",
                    &mut io::stdout(),
                );
            } else {
                generate(
                    Bash,
                    &mut command,
                    "slurm-usage-report-rs",
                    &mut io::stdout(),
                );
            }
        }
    }
}
