pub mod cli;
use clap::Parser;
use cli::*;
use slurm_usage_report_rs::UsageReportError;

fn main() -> Result<(), UsageReportError> {
    let cli = Cli::parse();
    match &cli.command {
        Commands::PostRunCmd(post_run_command) => {
            post_run_command.run(&cli)?;
        }
        Commands::Autocomplete(auto_complete) => {
            auto_complete.run(&cli);
        }
        Commands::CsvToParquet(csv_to_parquet) => {
            csv_to_parquet.run(&cli)?;
        }
        Commands::DailyEfficiency(daily_efficiency) => {
            daily_efficiency.run(&cli)?;
        }
    }
    Ok(())
}
