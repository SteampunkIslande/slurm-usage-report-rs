pub mod aggregates;
pub use aggregates::*;

pub mod columns_add;
pub use columns_add::*;

pub mod conversions;
pub use conversions::*;

use polars::prelude::*;

use crate::UsageReportError;

/// Generates a LazyFrame with the most interesting columns to get a general idea of resource consumption, especially memory.
///
/// This function should be called from a function that has taken one or more Parquet files as input.
/// It produces a LazyFrame summarizing the resources used, answering the question:
/// for each job, what percentage of what was requested was actually used.
///
/// # Arguments
/// * `lf` - A Polars LazyFrame containing SLURM job data
///
/// # Returns
/// A new LazyFrame with aggregated metrics and efficiency ratios
///
/// # Pipeline Steps
/// 1. Adds JobRoot and JobInfoType columns
/// 2. Converts MaxRSS to gigabytes
/// 3. Converts ReqMem to gigabytes
/// 4. Aggregates metrics per allocation (by JobRoot)
/// 5. Calculates memory efficiency ratio and percentage
/// 6. Parses TotalCPU column to seconds
/// 7. Calculates CPU efficiency percentage
pub fn generic_report(mut lf: LazyFrame) -> Result<LazyFrame, UsageReportError> {
    // Add JobRoot and JobInfoType columns (useful for the rest)
    lf = add_slurm_jobinfo_type_columns(lf);

    // Process MaxRSS column
    lf = add_units_kmg(lf, "MaxRSS");
    lf = convert_kmg_col(lf, "MaxRSS");
    lf = col_to_gigabytes(lf, "MaxRSS", true);

    // Process ReqMem column
    lf = add_units_kmg(lf, "ReqMem");
    lf = convert_kmg_col(lf, "ReqMem");
    lf = col_to_gigabytes(lf, "ReqMem", true);

    // Note: all aggregated fields will only be aggregated if they are numeric
    // Aggregate metrics per allocation (by JobRoot)
    lf = aggregate_per_alloc(lf, "JobRoot")?;

    // Calculate memory efficiency ratio: MaxRSS / ReqMem
    lf = lf.with_columns([(col("MaxRSS").cast(DataType::Float64)
        / col("ReqMem").cast(DataType::Float64))
    .alias("MemEfficiencyRatio")]);

    // Calculate memory efficiency percentage
    lf = lf.with_columns([(col("MemEfficiencyRatio") * lit(100.0)).alias("MemEfficiencyPercent")]);

    // Parse TotalCPU column to seconds
    lf = parse_total_cpu_col(lf);

    // Calculate CPU efficiency percentage: (TotalCPU_seconds / CPUTimeRAW) * 100
    lf = lf.with_columns([((col("TotalCPU_seconds").cast(DataType::Float64)
        / col("CPUTimeRAW").cast(DataType::Float64))
    .fill_nan(0.0)
        * lit(100.0))
    .alias("CPUEfficiencyPercent")]);

    Ok(lf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::df;

    #[test]
    fn test_generic_report_basic() {
        // Two jobs under the same JobRoot (same allocation)
        let df = df!(
            "JobID" => vec!["100", "100.batch"],
            "MaxRSS" => vec!["512K", "1024K"],
            "ReqMem" => vec!["4G", "4G"],
            "TotalCPU" => vec!["0:30:00", "1:00:00"],
            "CPUTimeRAW" => vec![1800i64, 3600],
            "AllocCPUS" => vec![4i64, 4],
            "QOS" => vec!["normal", "normal"],
            "Account" => vec!["bio", "bio"],
            "NodeList" => vec!["node1", "node1"]
        )
        .unwrap();
        let lf = df.lazy();
        let result = generic_report(lf).unwrap().collect().unwrap();

        // After aggregation per JobRoot, should have 1 row
        // (both jobs have JobRoot = "100")
        assert_eq!(result.height(), 1);

        // Verify key columns exist
        assert!(result.column("JobRoot").is_ok());
        assert!(result.column("MemEfficiencyRatio").is_ok());
        assert!(result.column("MemEfficiencyPercent").is_ok());
        assert!(result.column("TotalCPU_seconds").is_ok());
        assert!(result.column("CPUEfficiencyPercent").is_ok());
        assert!(result.column("MaxRSS_G").is_ok());
        assert!(result.column("ReqMem_G").is_ok());
    }

    #[test]
    fn test_generic_report_multiple_allocations() {
        let df = df!(
            "JobID" => vec!["100", "200"],
            "MaxRSS" => vec!["2G", "1G"],
            "ReqMem" => vec!["4G", "2G"],
            "TotalCPU" => vec!["1:00:00", "0:30:00"],
            "CPUTimeRAW" => vec![3600i64, 1800],
            "AllocCPUS" => vec![2i64, 4],
            "QOS" => vec!["normal", "high"],
            "Account" => vec!["bio", "chem"],
            "NodeList" => vec!["node1", "node2"]
        )
        .unwrap();
        let lf = df.lazy();
        let result = generic_report(lf).unwrap().collect().unwrap();

        // Two different allocations, should have 2 rows
        assert_eq!(result.height(), 2);

        let result = result
            .sort(["JobRoot"], SortMultipleOptions::default())
            .unwrap();

        // JobRoot 100: MaxRSS = 2G / ReqMem = 4G = 0.5 ratio = 50%
        let mem_eff = result.column("MemEfficiencyPercent").unwrap();
        let val_100 = mem_eff.get(0).unwrap().try_extract::<f64>().unwrap();
        assert!(
            (val_100 - 50.0).abs() < 1.0,
            "Expected ~50%, got {}",
            val_100
        );
    }

    #[test]
    fn test_generic_report_cpu_efficiency() {
        let df = df!(
            "JobID" => vec!["300"],
            "MaxRSS" => vec!["1G"],
            "ReqMem" => vec!["2G"],
            "TotalCPU" => vec!["0:45:00"],
            "CPUTimeRAW" => vec![3600i64],
            "AllocCPUS" => vec![1i64],
            "QOS" => vec!["normal"],
            "Account" => vec!["bio"],
            "NodeList" => vec!["node1"]
        )
        .unwrap();
        let lf = df.lazy();
        let result = generic_report(lf).unwrap().collect().unwrap();

        assert_eq!(result.height(), 1);

        // TotalCPU = 45 min = 2700s, CPUTimeRAW = 3600s => 75%
        let cpu_eff = result.column("CPUEfficiencyPercent").unwrap();
        let val = cpu_eff.get(0).unwrap().try_extract::<f64>().unwrap();
        assert!(
            (val - 75.0).abs() < 1.0,
            "Expected ~75% CPU efficiency, got {}",
            val
        );
    }
}
