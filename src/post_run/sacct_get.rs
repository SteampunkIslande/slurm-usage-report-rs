use std::path::Path;
use std::{fs::OpenOptions, process::Command};

use crate::UsageReportError;

pub fn get_sacct_for_runs<I, S, P>(run_ids: I, output_path: P) -> Result<(), UsageReportError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
    P: AsRef<Path>,
{
    let out_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output_path)?;

    let joined = run_ids
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect::<Vec<_>>()
        .join(",");

    let cmd_str = format!("sacct -S 1970-01-01 -a --name {} -o ALL -P", joined);

    let cmd_output = Command::new("sacct")
        .args([
            "-S",
            "1970-01-01",
            "-a",
            "--name",
            &joined,
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
