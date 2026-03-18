use std::process::Command;

pub mod post_run;

pub use post_run::*;

pub fn run_command(mut command: Command, stdout_file: std::fs::File) -> std::io::Result<()> {
    command.stdout(stdout_file).spawn()?;

    Ok(())
}
