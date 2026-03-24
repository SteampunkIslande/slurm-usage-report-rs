//! Utilities module for SLURM usage reporting.
//!
//! This module provides utility functions for processing SLURM job data,
//! including column definitions, color maps, and data transformation functions.

use in_place::InPlace;
use polars::prelude::*;
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use crate::UsageReportError;

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

pub fn get_color(v: minijinja::Value, col_name: &str) -> Option<String> {
    if let Ok(value) = f32::try_from(v) {
        let c_map = [
            ((0, 20), "#ff0000"),
            ((21, 60), "#d4a500"),
            ((61, 75), "#ffa500"),
            ((76, 100), "#008000"),
            ((101, 125), "#ffa500"),
            ((126, 150), "#ff0000"),
        ];
        let colored_cols = [
            "Efficacité mémoire moyenne",
            "Efficacité mémoire médiane",
            "Efficacité mémoire minimum",
            "Efficacité mémoire maximum",
            "Efficacité CPU moyenne",
            "Efficacité CPU médiane",
            "Efficacité CPU minimum",
            "Efficacité CPU maximum",
        ];
        // No special color for non numbers
        if !colored_cols.contains(&col_name) {
            return None;
        } else {
            if value < (c_map[0].0.0 as f32) {
                return Some(c_map[0].1.to_string());
            }
            if let Some(last) = c_map.last() {
                if value > (last.0.1 as f32) {
                    return Some(last.1.to_string());
                } else {
                    for ((lower_bound, upper_bound), color) in c_map.iter() {
                        if value >= (*lower_bound as f32) && value <= (*upper_bound as f32) {
                            return Some(color.to_string());
                        }
                    }
                }
            }
            return None;
        }
    }
    None
}

pub fn sacct_sanitizer(
    file_name: &Path,
    col_count: Option<u32>,
    separator: Option<&str>,
) -> Result<usize, UsageReportError> {
    let mut removed_lines = 0usize;
    let inp = InPlace::new(file_name).open()?;

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
    inp.save()?;
    Ok(removed_lines)
}

/// Convert a CSV file to Parquet format
///
/// # Arguments
/// * `input_csv` - Path to the input CSV file (pipe-separated)
/// * `output_parquet` - Path to the output Parquet file
///
/// # Returns
/// * `Ok(())` on success
/// * `Err(Box<dyn std::error::Error>)` on failure
pub fn csv_to_parquet<P: AsRef<Path>>(
    input_csv: P,
    output_parquet: P,
) -> Result<(), UsageReportError> {
    // Define the schema for the SLURM sacct output
    let schema = Schema::from_iter(vec![
        Field::new(PlSmallStr::from_str("Account"), DataType::String),
        Field::new(PlSmallStr::from_str("AdminComment"), DataType::String),
        Field::new(PlSmallStr::from_str("AllocCPUS"), DataType::Int64),
        Field::new(PlSmallStr::from_str("AllocNodes"), DataType::Int64),
        Field::new(PlSmallStr::from_str("AllocTRES"), DataType::String),
        Field::new(PlSmallStr::from_str("AssocID"), DataType::Int64),
        Field::new(PlSmallStr::from_str("AveCPU"), DataType::String),
        Field::new(PlSmallStr::from_str("AveCPUFreq"), DataType::String),
        Field::new(PlSmallStr::from_str("AveDiskRead"), DataType::String),
        Field::new(PlSmallStr::from_str("AveDiskWrite"), DataType::String),
        Field::new(PlSmallStr::from_str("AvePages"), DataType::Int64),
        Field::new(PlSmallStr::from_str("AveRSS"), DataType::String),
        Field::new(PlSmallStr::from_str("AveVMSize"), DataType::String),
        Field::new(PlSmallStr::from_str("BlockID"), DataType::String),
        Field::new(PlSmallStr::from_str("Cluster"), DataType::String),
        Field::new(PlSmallStr::from_str("Comment"), DataType::String),
        Field::new(PlSmallStr::from_str("Constraints"), DataType::String),
        Field::new(PlSmallStr::from_str("ConsumedEnergy"), DataType::Int64),
        Field::new(PlSmallStr::from_str("ConsumedEnergyRaw"), DataType::Int64),
        Field::new(PlSmallStr::from_str("Container"), DataType::String),
        Field::new(PlSmallStr::from_str("CPUTime"), DataType::String),
        Field::new(PlSmallStr::from_str("CPUTimeRAW"), DataType::Int64),
        Field::new(PlSmallStr::from_str("DBIndex"), DataType::Int64),
        Field::new(PlSmallStr::from_str("DerivedExitCode"), DataType::String),
        Field::new(PlSmallStr::from_str("Elapsed"), DataType::String),
        Field::new(PlSmallStr::from_str("ElapsedRaw"), DataType::Int64),
        Field::new(PlSmallStr::from_str("Eligible"), DataType::String),
        Field::new(PlSmallStr::from_str("End"), DataType::String),
        Field::new(PlSmallStr::from_str("ExitCode"), DataType::String),
        Field::new(PlSmallStr::from_str("Flags"), DataType::String),
        Field::new(PlSmallStr::from_str("GID"), DataType::Int64),
        Field::new(PlSmallStr::from_str("Group"), DataType::String),
        Field::new(PlSmallStr::from_str("JobID"), DataType::String),
        Field::new(PlSmallStr::from_str("JobIDRaw"), DataType::String),
        Field::new(PlSmallStr::from_str("JobName"), DataType::String),
        Field::new(PlSmallStr::from_str("Layout"), DataType::String),
        Field::new(PlSmallStr::from_str("MaxDiskRead"), DataType::String),
        Field::new(PlSmallStr::from_str("MaxDiskReadNode"), DataType::String),
        Field::new(PlSmallStr::from_str("MaxDiskReadTask"), DataType::Int64),
        Field::new(PlSmallStr::from_str("MaxDiskWrite"), DataType::String),
        Field::new(PlSmallStr::from_str("MaxDiskWriteNode"), DataType::String),
        Field::new(PlSmallStr::from_str("MaxDiskWriteTask"), DataType::Int64),
        Field::new(PlSmallStr::from_str("MaxPages"), DataType::Int64),
        Field::new(PlSmallStr::from_str("MaxPagesNode"), DataType::String),
        Field::new(PlSmallStr::from_str("MaxPagesTask"), DataType::Int64),
        Field::new(PlSmallStr::from_str("MaxRSS"), DataType::String),
        Field::new(PlSmallStr::from_str("MaxRSSNode"), DataType::String),
        Field::new(PlSmallStr::from_str("MaxRSSTask"), DataType::Int64),
        Field::new(PlSmallStr::from_str("MaxVMSize"), DataType::String),
        Field::new(PlSmallStr::from_str("MaxVMSizeNode"), DataType::String),
        Field::new(PlSmallStr::from_str("MaxVMSizeTask"), DataType::Int64),
        Field::new(PlSmallStr::from_str("McsLabel"), DataType::String),
        Field::new(PlSmallStr::from_str("MinCPU"), DataType::String),
        Field::new(PlSmallStr::from_str("MinCPUNode"), DataType::String),
        Field::new(PlSmallStr::from_str("MinCPUTask"), DataType::Int64),
        Field::new(PlSmallStr::from_str("NCPUS"), DataType::Int64),
        Field::new(PlSmallStr::from_str("NNodes"), DataType::Int64),
        Field::new(PlSmallStr::from_str("NodeList"), DataType::String),
        Field::new(PlSmallStr::from_str("NTasks"), DataType::Int64),
        Field::new(PlSmallStr::from_str("Partition"), DataType::String),
        Field::new(PlSmallStr::from_str("Priority"), DataType::Int64),
        Field::new(PlSmallStr::from_str("QOS"), DataType::String),
        Field::new(PlSmallStr::from_str("QOSRAW"), DataType::Int64),
        Field::new(PlSmallStr::from_str("Reason"), DataType::String),
        Field::new(PlSmallStr::from_str("ReqCPUFreq"), DataType::String),
        Field::new(PlSmallStr::from_str("ReqCPUFreqGov"), DataType::String),
        Field::new(PlSmallStr::from_str("ReqCPUFreqMax"), DataType::String),
        Field::new(PlSmallStr::from_str("ReqCPUFreqMin"), DataType::String),
        Field::new(PlSmallStr::from_str("ReqCPUS"), DataType::Int64),
        Field::new(PlSmallStr::from_str("ReqMem"), DataType::String),
        Field::new(PlSmallStr::from_str("ReqNodes"), DataType::Int64),
        Field::new(PlSmallStr::from_str("ReqTRES"), DataType::String),
        Field::new(PlSmallStr::from_str("Reservation"), DataType::String),
        Field::new(PlSmallStr::from_str("ReservationId"), DataType::String),
        Field::new(PlSmallStr::from_str("Reserved"), DataType::String),
        Field::new(PlSmallStr::from_str("ResvCPU"), DataType::String),
        Field::new(PlSmallStr::from_str("ResvCPURAW"), DataType::Int64),
        Field::new(PlSmallStr::from_str("Start"), DataType::String),
        Field::new(PlSmallStr::from_str("State"), DataType::String),
        Field::new(PlSmallStr::from_str("Submit"), DataType::String),
        Field::new(PlSmallStr::from_str("SubmitLine"), DataType::String),
        Field::new(PlSmallStr::from_str("Suspended"), DataType::String),
        Field::new(PlSmallStr::from_str("SystemComment"), DataType::String),
        Field::new(PlSmallStr::from_str("SystemCPU"), DataType::String),
        Field::new(PlSmallStr::from_str("Timelimit"), DataType::String),
        Field::new(PlSmallStr::from_str("TimelimitRaw"), DataType::String),
        Field::new(PlSmallStr::from_str("TotalCPU"), DataType::String),
        Field::new(PlSmallStr::from_str("TRESUsageInAve"), DataType::String),
        Field::new(PlSmallStr::from_str("TRESUsageInMax"), DataType::String),
        Field::new(PlSmallStr::from_str("TRESUsageInMaxNode"), DataType::String),
        Field::new(PlSmallStr::from_str("TRESUsageInMaxTask"), DataType::String),
        Field::new(PlSmallStr::from_str("TRESUsageInMin"), DataType::String),
        Field::new(PlSmallStr::from_str("TRESUsageInMinNode"), DataType::String),
        Field::new(PlSmallStr::from_str("TRESUsageInMinTask"), DataType::String),
        Field::new(PlSmallStr::from_str("TRESUsageInTot"), DataType::String),
        Field::new(PlSmallStr::from_str("TRESUsageOutAve"), DataType::String),
        Field::new(PlSmallStr::from_str("TRESUsageOutMax"), DataType::String),
        Field::new(
            PlSmallStr::from_str("TRESUsageOutMaxNode"),
            DataType::String,
        ),
        Field::new(
            PlSmallStr::from_str("TRESUsageOutMaxTask"),
            DataType::String,
        ),
        Field::new(PlSmallStr::from_str("TRESUsageOutMin"), DataType::String),
        Field::new(
            PlSmallStr::from_str("TRESUsageOutMinNode"),
            DataType::String,
        ),
        Field::new(
            PlSmallStr::from_str("TRESUsageOutMinTask"),
            DataType::String,
        ),
        Field::new(PlSmallStr::from_str("TRESUsageOutTot"), DataType::String),
        Field::new(PlSmallStr::from_str("UID"), DataType::Int64),
        Field::new(PlSmallStr::from_str("User"), DataType::String),
        Field::new(PlSmallStr::from_str("UserCPU"), DataType::String),
        Field::new(PlSmallStr::from_str("WCKey"), DataType::String),
        Field::new(PlSmallStr::from_str("WCKeyID"), DataType::Int64),
        Field::new(PlSmallStr::from_str("WorkDir"), DataType::String),
    ]);

    let lf = LazyCsvReader::new(PlRefPath::try_from_path(input_csv.as_ref())?)
        .with_schema(Some(Arc::new(schema)))
        .with_separator(b'|')
        .with_quote_char(None)
        .finish()?;

    sink_parquet(lf, output_parquet.as_ref())?;

    Ok(())
}

pub fn sink_parquet(lf: LazyFrame, path: &Path) -> Result<(), UsageReportError> {
    let _ = lf
        .sink(
            SinkDestination::File {
                target: SinkTarget::Path(PlRefPath::try_from_path(path)?),
            },
            FileWriteFormat::Parquet(Arc::new(ParquetWriteOptions::default())),
            UnifiedSinkArgs::default(),
        )?
        .collect()?;
    Ok(())
}

pub fn merge_parquets(inputs: &[&Path], output: &Path) -> Result<(), UsageReportError> {
    use duckdb::Connection;

    let conn: Connection = duckdb::Connection::open_in_memory()?;
    let query = format!(
        r#"
        COPY (
            SELECT * FROM read_parquet({input_parquets},union_by_name=true)
        ) TO '{output}'
        "#,
        input_parquets = format!(
            "[{}]",
            inputs
                .iter()
                .map(|p| format!("'{}'", p.display()))
                .collect::<Vec<String>>()
                .join(",")
        ),
        output = output.display()
    );

    conn.execute(&query, [])?;
    Ok(())
}

fn anyvalue_to_json(v: AnyValue) -> Value {
    match v {
        AnyValue::Null => Value::Null,

        AnyValue::Boolean(v) => json!(v),

        AnyValue::Int8(v) => json!(v),
        AnyValue::Int16(v) => json!(v),
        AnyValue::Int32(v) => json!(v),
        AnyValue::Int64(v) => json!(v),

        AnyValue::UInt8(v) => json!(v),
        AnyValue::UInt16(v) => json!(v),
        AnyValue::UInt32(v) => json!(v),
        AnyValue::UInt64(v) => json!(v),

        AnyValue::Float32(v) => json!(v),
        AnyValue::Float64(v) => json!(v),

        AnyValue::String(v) => json!(v),
        AnyValue::StringOwned(v) => json!(v),

        // Dates / Datetime → string ISO (simple et safe)
        AnyValue::Date(v) => json!(v.to_string()),
        AnyValue::Datetime(v, _, _) => json!(v.to_string()),

        // Fallback (Struct, List, etc.)
        _ => json!(v.to_string()),
    }
}

pub fn df_to_columnar_json(df: &DataFrame) -> Result<Value, UsageReportError> {
    let mut map = serde_json::Map::new();

    for col in df.columns() {
        let name = col.name().to_string();

        let values: Vec<Value> = col
            .as_series()
            .ok_or(UsageReportError::NoneValueError {
                message: "Cannot get series".into(),
                file: file!().to_string(),
                line: line!(),
                column: column!(),
            })?
            .iter()
            .map(anyvalue_to_json)
            .collect();

        map.insert(name, Value::Array(values));
    }

    Ok(Value::Object(map))
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_df_to_columnar_json() {
        let df = df!(
            "a" => vec![1, 2, 3, 4],
            "b" => vec![Some("a"), None, Some("c"), Some("d")]
        )
        .unwrap();
        let v = df_to_columnar_json(&df).unwrap();
        assert_eq!(
            v,
            json!({
                "a": [1, 2, 3, 4],
                "b": ["a", Value::Null, "c", "d"]
            })
        )
    }
}
