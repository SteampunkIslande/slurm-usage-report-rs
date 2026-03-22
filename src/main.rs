pub mod cli;
use clap::Parser;
use cli::*;

fn main() {
    let cli = Cli::parse();
    match &cli.command {
        Commands::PostRunCmd(post_run_command) => {
            post_run_command.run(&cli);
        }
        Commands::Autocomplete(auto_complete) => {
            auto_complete.run(&cli);
        }
        Commands::CsvToParquet(csv_to_parquet) => {
            csv_to_parquet.run(&cli);
        }
    }
}
