//! Utilities module for SLURM usage reporting.
//!
//! This module provides utility functions for processing SLURM job data,
//! including column definitions, color maps, and data transformation functions.

pub mod lazyframe_edit;
use in_place::InPlace;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

/// All available columns from sacct output
pub const ALL_COLUMNS: &[&str] = &[
    "Account",
    "AdminComment",
    "AllocCPUS",
    "AllocNodes",
    "AllocTRES",
    "AssocID",
    "AveCPU",
    "AveCPUFreq",
    "AveDiskRead",
    "AveDiskWrite",
    "AvePages",
    "AveRSS",
    "AveVMSize",
    "BlockID",
    "Cluster",
    "Comment",
    "Constraints",
    "ConsumedEnergy",
    "ConsumedEnergyRaw",
    "Container",
    "CPUTime",
    "CPUTimeRAW",
    "DBIndex",
    "DerivedExitCode",
    "Elapsed",
    "ElapsedRaw",
    "Eligible",
    "End",
    "ExitCode",
    "Flags",
    "GID",
    "Group",
    "JobID",
    "JobIDRaw",
    "JobName",
    "Layout",
    "MaxDiskRead",
    "MaxDiskReadNode",
    "MaxDiskReadTask",
    "MaxDiskWrite",
    "MaxDiskWriteNode",
    "MaxDiskWriteTask",
    "MaxPages",
    "MaxPagesNode",
    "MaxPagesTask",
    "MaxRSS",
    "MaxRSSNode",
    "MaxRSSTask",
    "MaxVMSize",
    "MaxVMSizeNode",
    "MaxVMSizeTask",
    "McsLabel",
    "MinCPU",
    "MinCPUNode",
    "MinCPUTask",
    "NCPUS",
    "NNodes",
    "NodeList",
    "NTasks",
    "Partition",
    "Priority",
    "QOS",
    "QOSRAW",
    "Reason",
    "ReqCPUFreq",
    "ReqCPUFreqGov",
    "ReqCPUFreqMax",
    "ReqCPUFreqMin",
    "ReqCPUS",
    "ReqMem",
    "ReqNodes",
    "ReqTRES",
    "Reservation",
    "ReservationId",
    "Reserved",
    "ResvCPU",
    "ResvCPURAW",
    "Start",
    "State",
    "Submit",
    "SubmitLine",
    "Suspended",
    "SystemComment",
    "SystemCPU",
    "Timelimit",
    "TimelimitRaw",
    "TotalCPU",
    "TRESUsageInAve",
    "TRESUsageInMax",
    "TRESUsageInMaxNode",
    "TRESUsageInMaxTask",
    "TRESUsageInMin",
    "TRESUsageInMinNode",
    "TRESUsageInMinTask",
    "TRESUsageInTot",
    "TRESUsageOutAve",
    "TRESUsageOutMax",
    "TRESUsageOutMaxNode",
    "TRESUsageOutMaxTask",
    "TRESUsageOutMin",
    "TRESUsageOutMinNode",
    "TRESUsageOutMinTask",
    "TRESUsageOutTot",
    "UID",
    "User",
    "UserCPU",
    "WCKey",
    "WCKeyID",
    "WorkDir",
];

/// Useful columns for analysis
pub const USEFUL_COLUMNS: &[&str] = &[
    "Account",
    "AllocCPUS",
    "AllocNodes",
    "AveCPU",
    "AveCPUFreq",
    "AveDiskRead",
    "AveDiskWrite",
    "AvePages",
    "AveRSS",
    "AveVMSize",
    "Comment",
    "CPUTime",
    "CPUTimeRAW",
    "DerivedExitCode",
    "Elapsed",
    "ElapsedRaw",
    "End",
    "ExitCode",
    "Flags",
    "Group",
    "JobID",
    "JobName",
    "MaxDiskRead",
    "MaxDiskReadNode",
    "MaxDiskReadTask",
    "MaxDiskWrite",
    "MaxDiskWriteNode",
    "MaxDiskWriteTask",
    "MaxPages",
    "MaxPagesNode",
    "MaxPagesTask",
    "MaxRSS",
    "MaxRSSNode",
    "MaxRSSTask",
    "MaxVMSize",
    "MaxVMSizeNode",
    "MaxVMSizeTask",
    "MinCPU",
    "MinCPUNode",
    "MinCPUTask",
    "NCPUS",
    "NNodes",
    "NodeList",
    "NTasks",
    "Partition",
    "QOS",
    "ReqCPUS",
    "ReqMem",
    "ReqNodes",
    "Start",
    "State",
    "Submit",
    "SubmitLine",
    "Suspended",
    "SystemCPU",
    "Timelimit",
    "TimelimitRaw",
    "TotalCPU",
    "User",
    "UserCPU",
    "WorkDir",
];

/// Interesting columns for common reports
pub const INTERESTING_COLUMNS: &[&str] = &[
    "Account",
    "AllocCPUS",
    "Comment",
    "CPUTime",
    "CPUTimeRAW",
    "DerivedExitCode",
    "Elapsed",
    "ElapsedRaw",
    "End",
    "ExitCode",
    "Group",
    "JobID",
    "JobName",
    "MaxDiskRead",
    "MaxDiskWrite",
    "MaxRSS",
    "MaxVMSize",
    "QOS",
    "ReqCPUS",
    "ReqMem",
    "ReqNodes",
    "Start",
    "State",
    "Submit",
    "SubmitLine",
    "Suspended",
    "SystemCPU",
    "Timelimit",
    "TimelimitRaw",
    "TotalCPU",
    "User",
    "UserCPU",
    "WorkDir",
];

/// Default color map for duration visualization
/// Each entry is ((min_hours, max_hours), color_hex)
pub type ColorMapEntry = ((i32, i32), &'static str);

/// Get the default color map
pub fn get_default_cmap() -> Vec<ColorMapEntry> {
    vec![
        ((0, 20), "#ff0000"),
        ((21, 60), "#d4a500"),
        ((61, 75), "#ffa500"),
        ((76, 100), "#008000"),
        ((101, 125), "#ffa500"),
        ((126, 150), "#ff0000"),
    ]
}

/// Get a color map by name
pub fn get_color_map(name: &str) -> Vec<ColorMapEntry> {
    match name {
        "default" => get_default_cmap(),
        _ => get_default_cmap(),
    }
}

pub fn sacct_sanitizer(
    file_name: &Path,
    col_count: Option<u32>,
    separator: Option<&str>,
) -> std::io::Result<usize> {
    let mut removed_lines = 0usize;
    let inp = InPlace::new(file_name)
        .open()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let reader = BufReader::new(inp.reader());
    let mut writer = inp.writer();
    for line in reader.lines() {
        let line = line?;
        if line.split(separator.unwrap_or("|")).count() != (col_count.unwrap_or(109) - 1) as usize {
            removed_lines += 1;
        } else {
            writeln!(writer, "{line}")?;
        }
    }
    inp.save()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    Ok(removed_lines)
}
