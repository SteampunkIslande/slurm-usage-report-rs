use std::path::Path;
use std::{fs::OpenOptions, process::Command};

use crate::UsageReportError;

pub fn get_sacct_for_runs(run_ids: &[&str], output_path: &Path) -> Result<(), UsageReportError> {
    let out_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output_path)?;
    let cmd_str = format!(
        "{} {} {} {} {} {} {} {} {}",
        "sacct",
        "-S",
        "1970-01-01", // pour être sûr d'avoir tous les jobs, même ceux qui ont été lancés il y a longtemps
        "-a",
        "--name",
        run_ids.join(",").as_str(),
        "-o",
        "ALL",
        "-P",
    );
    let cmd_output = Command::new("sacct")
        .args([
            "sacct",
            "-S",
            "1970-01-01", // pour être sûr d'avoir tous les jobs, même ceux qui ont été lancés il y a longtemps
            "-a",
            "--name",
            run_ids.join(",").as_str(),
            "-o",
            "ALL",
            "-P",
        ])
        .stdout(out_file)
        .output()?;
    if !cmd_output.status.success() {
        return Err(UsageReportError::ExternalProcessError { cmd: cmd_str });
    }
    Ok(())
}
