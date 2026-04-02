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

use crate::UsageReportError;

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
pub fn aggregate_per_alloc(
    mut lf: LazyFrame,
    group_col: &str,
) -> Result<LazyFrame, UsageReportError> {
    // Build aggregation expressions for common column types
    // We'll aggregate all columns that exist in the dataframe
    // If a column is a numeric type, aggregate using the highest value. Otherwise, just take the first non-null value.
    let aggs: Vec<Expr> = lf
        .collect_schema()?
        .iter_names_and_dtypes()
        .filter_map(|(name, dtype)| {
            if name.as_str() != group_col {
                //Special columns
                match name.as_str() {
                    "JobName" => Some(
                        col(name.clone())
                            .filter(col(name.clone()).str().len_chars().eq(lit(36)))
                            .first(),
                    ),
                    _ => {
                        if dtype.is_numeric() {
                            Some(col(name.clone()).max())
                        } else {
                            Some(col(name.clone()).drop_nulls().first())
                        }
                    }
                }
            } else {
                None
            }
        })
        .collect();

    lf = lf.group_by([col(group_col)]).agg(aggs);
    Ok(lf)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn av_str(v: &AnyValue) -> String {
        match v {
            AnyValue::String(s) => s.to_string(),
            AnyValue::StringOwned(s) => s.to_string(),
            _ => panic!("Expected string AnyValue, got: {:?}", v),
        }
    }

    #[test]
    fn test_aggregate_per_alloc_numeric_max() {
        let df = df!(
            "JobRoot" => vec!["100", "100", "200"],
            "AllocCPUS" => vec![4i64, 8, 2],
            "ElapsedRaw" => vec![100i64, 200, 50]
        )
        .unwrap();
        let lf = df.lazy();
        let result = aggregate_per_alloc(lf, "JobRoot")
            .unwrap()
            .collect()
            .unwrap();

        // Sort by JobRoot for deterministic ordering
        let result = result
            .sort(["JobRoot"], SortMultipleOptions::default())
            .unwrap();

        let cpus = result.column("AllocCPUS").unwrap();
        // JobRoot 100: max(4, 8) = 8
        assert_eq!(cpus.get(0).unwrap().try_extract::<i64>().unwrap(), 8);
        // JobRoot 200: max(2) = 2
        assert_eq!(cpus.get(1).unwrap().try_extract::<i64>().unwrap(), 2);

        let elapsed = result.column("ElapsedRaw").unwrap();
        // JobRoot 100: max(100, 200) = 200
        assert_eq!(elapsed.get(0).unwrap().try_extract::<i64>().unwrap(), 200);
        // JobRoot 200: max(50) = 50
        assert_eq!(elapsed.get(1).unwrap().try_extract::<i64>().unwrap(), 50);
    }

    #[test]
    fn test_aggregate_per_alloc_string_first_non_null() {
        let df = df!(
            "JobRoot" => vec!["100", "100", "200"],
            "QOS" => vec![None, Some("normal"), Some("high")],
            "Account" => vec![Some("bio"), None, Some("chem")]
        )
        .unwrap();
        let lf = df.lazy();
        let result = aggregate_per_alloc(lf, "JobRoot")
            .unwrap()
            .collect()
            .unwrap();

        let result = result
            .sort(["JobRoot"], SortMultipleOptions::default())
            .unwrap();

        // JobRoot 100: first non-null QOS = "normal"
        let qos = result.column("QOS").unwrap();
        assert_eq!(av_str(&qos.get(0).unwrap()), "normal");

        // JobRoot 100: first non-null Account = "bio"
        let account = result.column("Account").unwrap();
        assert_eq!(av_str(&account.get(0).unwrap()), "bio");

        // JobRoot 200: QOS = "high"
        assert_eq!(av_str(&qos.get(1).unwrap()), "high");
        // JobRoot 200: Account = "chem"
        assert_eq!(av_str(&account.get(1).unwrap()), "chem");
    }

    #[test]
    fn test_aggregate_per_alloc_single_row_groups() {
        let df = df!(
            "JobRoot" => vec!["100", "200", "300"],
            "AllocCPUS" => vec![4i64, 8, 2],
            "QOS" => vec!["normal", "high", "low"]
        )
        .unwrap();
        let lf = df.lazy();
        let result = aggregate_per_alloc(lf, "JobRoot")
            .unwrap()
            .collect()
            .unwrap();

        let result = result
            .sort(["JobRoot"], SortMultipleOptions::default())
            .unwrap();

        // Each group has a single row, values should be preserved
        assert_eq!(
            result
                .column("AllocCPUS")
                .unwrap()
                .get(0)
                .unwrap()
                .try_extract::<i64>()
                .unwrap(),
            4
        );
        assert_eq!(
            result
                .column("AllocCPUS")
                .unwrap()
                .get(1)
                .unwrap()
                .try_extract::<i64>()
                .unwrap(),
            8
        );
        assert_eq!(
            result
                .column("AllocCPUS")
                .unwrap()
                .get(2)
                .unwrap()
                .try_extract::<i64>()
                .unwrap(),
            2
        );
    }

    #[test]
    fn test_aggregate_per_alloc_float_columns() {
        let df = df!(
            "JobRoot" => vec!["100", "100"],
            "MemEfficiencyPercent" => vec![45.5f64, 78.2]
        )
        .unwrap();
        let lf = df.lazy();
        let result = aggregate_per_alloc(lf, "JobRoot")
            .unwrap()
            .collect()
            .unwrap();

        let mem = result.column("MemEfficiencyPercent").unwrap();
        // max(45.5, 78.2) = 78.2
        assert!((mem.get(0).unwrap().try_extract::<f64>().unwrap() - 78.2).abs() < 0.001);
    }

    #[test]
    fn test_aggregate_per_snakemake_rule_basic() {
        let df = df!(
            "rule_name" => vec!["align", "align", "merge"],
            "MemEfficiencyPercent" => vec![50.0f64, 70.0, 90.0],
            "CPUEfficiencyPercent" => vec![60.0f64, 80.0, 95.0],
            "ElapsedRaw" => vec![100i64, 200, 150],
            "Elapsed" => vec!["00:01:40", "00:03:20", "00:02:30"],
            "QOS" => vec!["normal", "normal", "high"],
            "Account" => vec!["bio", "bio", "chem"],
            "NodeList" => vec!["node1", "node2", "node3"]
        )
        .unwrap();
        let lf = df.lazy();
        let result = aggregate_per_snakemake_rule(lf, false).collect().unwrap();

        let result = result
            .sort(["rule_name"], SortMultipleOptions::default())
            .unwrap();

        // Should have 2 groups: "align" and "merge"
        assert_eq!(result.height(), 2);

        // Check MemEfficiencyPercent_mean for align: (50+70)/2 = 60
        let mem_mean = result.column("MemEfficiencyPercent_mean").unwrap();
        assert!((mem_mean.get(0).unwrap().try_extract::<f64>().unwrap() - 60.0).abs() < 0.001);

        // Check CPUEfficiencyPercent_mean for align: (60+80)/2 = 70
        let cpu_mean = result.column("CPUEfficiencyPercent_mean").unwrap();
        assert!((cpu_mean.get(0).unwrap().try_extract::<f64>().unwrap() - 70.0).abs() < 0.001);

        // Check ElapsedRaw_max for align: max(100, 200) = 200
        let elapsed_max = result.column("ElapsedRaw_max").unwrap();
        assert_eq!(
            elapsed_max.get(0).unwrap().try_extract::<i64>().unwrap(),
            200
        );
    }

    #[test]
    fn test_aggregate_per_snakemake_rule_with_input_sizes() {
        let df = df!(
            "rule_name" => vec!["align"],
            "MemEfficiencyPercent" => vec![50.0f64],
            "CPUEfficiencyPercent" => vec![60.0f64],
            "ElapsedRaw" => vec![100i64],
            "Elapsed" => vec!["00:01:40"],
            "QOS" => vec!["normal"],
            "Account" => vec!["bio"],
            "NodeList" => vec!["node1"],
            "UsedRAMPerMo" => vec![1.5f64],
            "MinPerMo" => vec![2.0f64]
        )
        .unwrap();
        let lf = df.lazy();
        let result = aggregate_per_snakemake_rule(lf, true).collect().unwrap();

        // With input_sizes=true, should have UsedRAMPerMo and MinPerMo aggregations
        assert!(result.column("UsedRAMPerMo_mean").is_ok());
        assert!(result.column("MinPerMo_mean").is_ok());
        assert!(result.column("UsedRAMPerMo_max").is_ok());
        assert!(result.column("MinPerMo_max").is_ok());
    }

    #[test]
    fn test_aggregate_per_snakemake_rule_std() {
        let df = df!(
            "rule_name" => vec!["align", "align", "align"],
            "MemEfficiencyPercent" => vec![50.0f64, 60.0, 70.0],
            "CPUEfficiencyPercent" => vec![50.0f64, 60.0, 70.0],
            "ElapsedRaw" => vec![100i64, 200, 300],
            "Elapsed" => vec!["00:01:40", "00:03:20", "00:05:00"],
            "QOS" => vec!["normal", "normal", "normal"],
            "Account" => vec!["bio", "bio", "bio"],
            "NodeList" => vec!["node1", "node2", "node3"]
        )
        .unwrap();
        let lf = df.lazy();
        let result = aggregate_per_snakemake_rule(lf, false).collect().unwrap();

        // std(50, 60, 70) with ddof=1 = 10.0
        let mem_std = result.column("MemEfficiencyPercent_std").unwrap();
        assert!((mem_std.get(0).unwrap().try_extract::<f64>().unwrap() - 10.0).abs() < 0.1);
    }
}
