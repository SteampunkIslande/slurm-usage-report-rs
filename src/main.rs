pub mod cli;
use clap::{CommandFactory, Parser};
use clap_complete::generate;
use cli::*;
use std::fs::OpenOptions;

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::PostRunCmd(post_run_command) => {
            post_run_command.run();
        }
        Commands::Generate(auto_complete) => {
            let mut command = cli::Cli::command();
            match OpenOptions::new()
                .create(true)
                .create_new(!auto_complete.force)
                .write(true)
                .open(&auto_complete.output)
            {
                Ok(mut complete_file) => {
                    generate(
                        auto_complete.shell,
                        &mut command,
                        "slurm-usage-report-rs",
                        &mut complete_file,
                    );
                }
                Err(e) => {
                    eprintln!("Impossible de générer le script d'autocomplétion: {e}")
                }
            }
        }
    }
}
