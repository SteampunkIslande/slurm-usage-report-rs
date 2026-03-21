pub mod aggregates;
pub use aggregates::*;

pub mod columns_add;
pub use columns_add::*;

pub mod conversions;
pub use conversions::*;

use polars::prelude::*;

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
pub fn generic_report(mut lf: LazyFrame) -> LazyFrame {
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
    lf = aggregate_per_alloc(lf, "JobRoot");

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

    lf
}
