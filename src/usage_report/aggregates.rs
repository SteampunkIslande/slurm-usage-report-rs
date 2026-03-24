//! Aggregation functions for SLURM usage reporting.
//!
//! This module provides functions for aggregating SLURM job data,
//! including aggregation per allocation and per Snakemake rule.
//!
//! ## Important: Aggregation order and behavior
//!
//! The aggregation functions must be called in a specific pipeline order:
//! 1. First, add necessary columns (JobRoot, JobInfoType, etc.) using columns_add functions
//! 2. Then, perform unit conversions using conversions functions
//! 3. Finally, aggregate the data using these functions
//!
//! The aggregation behavior for each column type is:
//! - Int64 columns: takes the MAX value
//! - Float64 columns: takes the MAX value
//! - String columns: takes the first non-null value

use polars::prelude::*;

/// Aggregates metrics per allocation (by JobRoot or specified group column).
///
/// This function groups the LazyFrame by the specified column and aggregates
/// all other columns using type-specific aggregation functions:
/// - Int64/Float64: max value
/// - String: first non-null value
///
/// # Arguments
/// * `lf` - A Polars LazyFrame
/// * `group_col` - The column to group by (default: "JobRoot")
///
/// # Returns
/// A new LazyFrame with aggregated metrics per group
pub fn aggregate_per_alloc(mut lf: LazyFrame, group_col: &str) -> LazyFrame {
    // Build aggregation expressions for common column types
    // We'll aggregate all columns that exist in the dataframe
    // If a column is a numeric type, aggregate using the highest value. Otherwise, just take the first non-null value.
    let aggs: Vec<Expr> = lf
        .collect_schema()
        .expect("Error collecting schema")
        .iter_names_and_dtypes()
        .filter_map(|(name, dtype)| {
            if name.as_str() != group_col {
                if dtype.is_numeric() {
                    Some(col(name.clone()).max())
                } else {
                    Some(col(name.clone()).drop_nulls().first())
                }
            } else {
                None
            }
        })
        .collect();

    lf.group_by([col(group_col)]).agg(aggs)
}

/// Aggregates metrics per Snakemake rule.
///
/// This function groups the LazyFrame by "rule_name" and calculates various
/// statistics for each rule including:
/// - Memory efficiency (mean, median, std, min, max)
/// - CPU efficiency (mean, median, std, min, max)
/// - Elapsed time (mean, median, std, min, max)
/// - Input size related metrics (if input_sizes is true)
///
/// # Arguments
/// * `lf` - A Polars LazyFrame containing rule-based job data
/// * `input_sizes` - Whether to include input size related metrics
///
/// # Returns
/// A new LazyFrame with aggregated metrics per rule
pub fn aggregate_per_snakemake_rule(lf: LazyFrame, input_sizes: bool) -> LazyFrame {
    let mut aggs: Vec<Expr> = vec![
        // Memory efficiency metrics
        col("MemEfficiencyPercent").mean().name().suffix("_mean"),
        col("MemEfficiencyPercent")
            .median()
            .name()
            .suffix("_median"),
        col("MemEfficiencyPercent").std(1).name().suffix("_std"),
        col("MemEfficiencyPercent").min().name().suffix("_min"),
        col("MemEfficiencyPercent").max().name().suffix("_max"),
        // CPU efficiency metrics
        col("CPUEfficiencyPercent").mean().name().suffix("_mean"),
        col("CPUEfficiencyPercent")
            .median()
            .name()
            .suffix("_median"),
        col("CPUEfficiencyPercent").std(1).name().suffix("_std"),
        col("CPUEfficiencyPercent").min().name().suffix("_min"),
        col("CPUEfficiencyPercent").max().name().suffix("_max"),
        // Elapsed time metrics
        col("ElapsedRaw").mean().name().suffix("_mean"),
        col("ElapsedRaw").median().name().suffix("_median"),
        col("ElapsedRaw").std(1).name().suffix("_std"),
        col("ElapsedRaw").min().name().suffix("_min"),
        col("ElapsedRaw").max().name().suffix("_max"),
        // Elapsed time (formatted) - min and max
        col("Elapsed").min().alias("Elapsed_min"),
        col("Elapsed").max().alias("Elapsed_max"),
        // Categorical columns - take first non-null
        col("QOS").drop_nulls().first(),
        col("Account").drop_nulls().first(),
        col("NodeList").drop_nulls().first(),
    ];

    // Add input size related metrics if requested
    if input_sizes {
        let input_size_aggs: Vec<Expr> = vec![
            col("UsedRAMPerMo").mean().name().suffix("_mean"),
            col("UsedRAMPerMo").median().name().suffix("_median"),
            col("UsedRAMPerMo").std(1).name().suffix("_std"),
            col("UsedRAMPerMo").min().name().suffix("_min"),
            col("UsedRAMPerMo").max().name().suffix("_max"),
            col("MinPerMo").mean().name().suffix("_mean"),
            col("MinPerMo").median().name().suffix("_median"),
            col("MinPerMo").std(1).name().suffix("_std"),
            col("MinPerMo").min().name().suffix("_min"),
            col("MinPerMo").max().name().suffix("_max"),
        ];
        aggs.extend(input_size_aggs);
    }

    lf.group_by([col("rule_name")]).agg(aggs)
}
