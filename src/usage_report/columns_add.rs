//! Column addition functions for SLURM usage reporting.
//!
//! This module provides functions for adding new columns to LazyFrames,
//! including:
//! - JobRoot and JobInfoType columns (derived from JobID)
//! - Snakemake rule columns (rule_name, wildcards)
//! - Generic report generation combining multiple transformations
//!
//! ## Important: Pipeline order
//!
//! These functions are typically the first step in a data transformation pipeline.
//! The typical order is:
//! 1. Add columns using functions in this module (columns_add)
//! 2. Convert units using functions in conversions module
//! 3. Aggregate using functions in aggregates module

use std::path::Path;

use duckdb::Connection;
use polars::prelude::*;

use crate::UsageReportError;

/// Adds JobRoot and JobInfoType columns based on JobID.
///
/// This function extracts:
/// - JobRoot: The parent JobID (before the '.')
/// - JobInfoType: The job type (allocation, batch, extern, step, or unknown)
///
/// Classification based on JobID suffix:
/// - 12345        -> allocation
/// - 12345.batch  -> batch
/// - 12345.extern -> extern
/// - 12345.0      -> step (srun step)
/// - 12345.1      -> step (srun step)
/// - Other        -> unknown
///
/// Note: This function creates intermediate columns (_JobSuffix) that can be dropped afterwards.
///
/// # Arguments
/// * `lf` - A Polars LazyFrame with a "JobID" column
///
/// # Returns
/// A new LazyFrame with JobRoot and JobInfoType columns added
pub fn add_slurm_jobinfo_type_columns(mut lf: LazyFrame) -> LazyFrame {
    // First, extract JobRoot from JobID (everything before the '.')
    let job_root = col("JobID").str().extract(lit(r"^(\d+)"), 1);

    // Extract the suffix (everything after the '.')
    let job_suffix = col("JobID").str().extract(lit(r"^\d+\.(.+)$"), 1);

    lf = lf.with_columns([job_root.alias("JobRoot"), job_suffix.alias("_JobSuffix")]);

    // Now determine JobInfoType based on the suffix
    let job_info_type = when(col("_JobSuffix").is_null())
        .then(lit("allocation"))
        .when(col("_JobSuffix").eq(lit("batch")))
        .then(lit("batch"))
        .when(col("_JobSuffix").eq(lit("extern")))
        .then(lit("extern"))
        // Numeric suffix -> step srun
        .when(col("_JobSuffix").str().contains(lit(r"^\d+$"), false))
        .then(lit("step"))
        .otherwise(lit("unknown"))
        .alias("JobInfoType");

    lf = lf.with_columns([job_info_type]);
    lf = lf.drop(by_name(["_JobSuffix"], true, false));

    lf
}

/// Adds Snakemake rule columns based on the Comment field.
///
/// This function extracts from the Comment column:
/// - rule_name: The name of the Snakemake rule
/// - wildcards: The wildcards used in the rule (if any)
///
/// Pattern matching:
/// - "^rule_.+?_wildcards_.+" -> extracts both rule_name and wildcards
/// - "^rule_.+?" -> extracts only rule_name (no wildcards)
/// - Other -> both fields are null
///
/// Note: This function creates intermediate columns that can be dropped afterwards.
///
/// # Arguments
/// * `lf` - A Polars LazyFrame with a "Comment" column
///
/// # Returns
/// A new LazyFrame with rule_name and wildcards columns added
pub fn add_snakerule_col(mut lf: LazyFrame) -> LazyFrame {
    // First check which pattern matches
    let has_wildcards = col("Comment")
        .str()
        .contains(lit(r"^rule_.+?_wildcards_.+"), false);
    let has_rule = col("Comment").str().contains(lit(r"^rule_"), false);

    // Extract rule name - different patterns
    let rule_with_wildcards = col("Comment")
        .str()
        .extract(lit(r"^rule_(.+?)_wildcards_(.+)$"), 1);

    let rule_simple = col("Comment").str().extract(lit(r"^rule_(.+)$"), 1);

    // Extract wildcards - only for the pattern with wildcards
    let wildcards = col("Comment")
        .str()
        .extract(lit(r"^rule_.+?_wildcards_(.+)$"), 1);

    // Build the expression
    let rule_name = when(has_wildcards)
        .then(rule_with_wildcards)
        .when(has_rule)
        .then(rule_simple)
        .otherwise(lit(""));

    lf = lf.with_columns([rule_name.alias("rule_name"), wildcards.alias("_wildcards")]);

    // Set wildcards to null if it doesn't match the pattern (empty string case)
    let wildcards_final = when(col("_wildcards").str().contains(lit(r"^.+$"), false))
        .then(col("_wildcards"))
        .otherwise(lit(""))
        .alias("wildcards");

    lf = lf
        .with_columns([wildcards_final])
        .drop(by_name(["_wildcards"], true, false));
    lf
}

/// Extracts numeric value and unit from a column with K/M/G suffix.
///
/// This function adds two columns:
/// - `{colname}`: the numeric value as string (replaces original)
/// - `{colname}_unit`: the unit suffix (K, M, G, or T)
///
/// Note: After calling convert_kmg_col, you may want to drop the _unit column.
///
/// # Arguments
/// * `lf` - A Polars LazyFrame
/// * `colname` - The name of the column to extract units from
///
/// # Returns
/// A new LazyFrame with extracted numeric value and unit columns
pub fn add_units_kmg(mut lf: LazyFrame, colname: &str) -> LazyFrame {
    let unit_col = format!("{}_unit", colname);

    // Extract the numeric part (first capture group) and unit (second capture group)
    // Pattern: (?<value>\d+)(?<unit>[KMGT])
    let numeric_part = col(colname)
        .str()
        .extract(lit(r"(?<value>\d+)(?<unit>[KMGT])"), 1);

    let unit_part = col(colname)
        .str()
        .extract(lit(r"(?<value>\d+)(?<unit>[KMGT])"), 2);

    lf = lf.with_columns([numeric_part.alias(colname), unit_part.alias(unit_col)]);

    lf
}

/// Adds wait time columns to a LazyFrame.
///
/// Adds columns:
/// - `wait_time_seconds`: Wait time in seconds (time between Submit and Start)
/// - `wait_time_hours`: Wait time in hours (fractional)
///
/// # Arguments
/// * `lf` - A Polars LazyFrame containing 'Start' and 'Submit' columns
///
/// # Returns
/// A new LazyFrame with the added wait time columns
pub fn add_wait_time_cols(mut lf: LazyFrame) -> LazyFrame {
    let datetime_format = "%Y-%m-%dT%H:%M:%S";

    let datetime_conversion_options: StrptimeOptions = StrptimeOptions {
        format: Some(datetime_format.into()),
        cache: true,
        ..Default::default()
    };

    lf = lf.with_columns([(col("Start").str().to_datetime(
        None,
        None,
        datetime_conversion_options.clone(),
        lit("raise"),
    ) - col("Submit").str().to_datetime(
        None,
        None,
        datetime_conversion_options.clone(),
        lit("raise"),
    ))
    .alias("wait_duration")]);

    lf = lf
        .with_columns([(col("wait_duration")
            .dt()
            .total_seconds(false)
            .alias("wait_time_seconds"))])
        .with_columns([(col("wait_time_seconds") / lit(3600.0)).alias("wait_time_hours")]);
    lf
}

/// Adds job duration columns to a LazyFrame.
///
/// Adds column:
/// - `job_duration_seconds`: Duration in seconds (time between Start and End)
///
/// # Arguments
/// * `lf` - A Polars LazyFrame containing 'Start' and 'End' columns
///
/// # Returns
/// A new LazyFrame with the added duration column
pub fn add_job_duration_cols(mut lf: LazyFrame) -> LazyFrame {
    let datetime_format = "%Y-%m-%dT%H:%M:%S";

    let datetime_conversion_options: StrptimeOptions = StrptimeOptions {
        format: Some(datetime_format.into()),
        cache: true,
        ..Default::default()
    };

    lf = lf.with_columns([(col("End").str().to_datetime(
        None,
        None,
        datetime_conversion_options.clone(),
        lit("raise"),
    ) - col("Start").str().to_datetime(
        None,
        None,
        datetime_conversion_options.clone(),
        lit("raise"),
    ))
    .dt()
    .total_seconds(true)
    .alias("job_duration_seconds")]);
    lf
}

/// Adds a column for daily duration calculation.
///
/// Adds columns:
/// - `daily_duration_hours`: Duration in hours for a specific target date
/// - `date`: The target date
///
/// # Arguments
/// * `lf` - A Polars LazyFrame containing 'Start' and 'End' columns
/// * `date` - Target date as a string in 'YYYY-MM-DD' format
///
/// # Returns
/// A new LazyFrame with the added daily duration and date columns
///
/// # Case handling:
/// - Job started and ended on same day: full duration (End - Start)
/// - Job started previous day, ended on target day: duration from midnight to End
/// - Job started on target day, ended next day: duration from Start to midnight
/// - Job spanned multiple days including target day: 24 hours
/// - Job did not run on target day: 0 hours
pub fn add_daily_duration<S: AsRef<str>>(lf: LazyFrame, date: S) -> LazyFrame {
    let date_str = date.as_ref();

    // Create day boundaries
    let day_start = format!("{}T00:00:00", date_str);
    let day_end = format!("{}T23:59:59", date_str);

    // Convert Start and End to datetime
    let datetime_format = "%Y-%m-%dT%H:%M:%S";
    let datetime_conversion_options: StrptimeOptions = StrptimeOptions {
        format: Some(datetime_format.into()),
        strict: false,
        exact: false,
        cache: true,
    };

    let start_dt = col("Start").str().to_datetime(
        Default::default(),
        Default::default(),
        datetime_conversion_options.clone(),
        lit("raise"),
    );
    let end_dt = col("End").str().to_datetime(
        Default::default(),
        Default::default(),
        datetime_conversion_options.clone(),
        lit("raise"),
    );

    // Create literals for day boundaries
    let day_start_lit = lit(day_start.clone())
        .alias("day_start_lit")
        .str()
        .to_datetime(
            Default::default(),
            Default::default(),
            datetime_conversion_options.clone(),
            lit("raise"),
        )
        .alias("day_start");
    let day_end_lit = lit(day_end.clone())
        .alias("day_end_lit")
        .str()
        .to_datetime(
            Default::default(),
            Default::default(),
            datetime_conversion_options.clone(),
            lit("raise"),
        )
        .alias("day_end");

    // Build the conditional expression for daily duration
    // Convert target_date to a string for consistent comparison
    let target_date_col = lit(date_str).str().to_date(StrptimeOptions {
        format: Some("%Y-%m-%d".into()),
        ..Default::default()
    });

    // Case 1: Job started and ended on the same day (target day)
    let same_day = start_dt
        .clone()
        .dt()
        .date()
        .eq(target_date_col.clone())
        .and(end_dt.clone().dt().date().eq(target_date_col.clone()));
    // Note: subtracting two datetimes gives a Duration in microseconds.
    // Convert microseconds to hours: divide by 3600000000 (microseconds in an hour)
    let duration_same_day = (end_dt.clone() - start_dt.clone()).dt().total_hours(true);

    // Case 2: Job started before target day, ended on target day
    let started_before = start_dt.clone().lt(day_start_lit.clone());
    let ended_on_day = end_dt.clone().dt().date().eq(target_date_col.clone());
    let case_started_before = started_before.clone().and(ended_on_day);
    // Duration - convert microseconds to hours
    let duration_started_before = (end_dt.clone() - day_start_lit.clone())
        .dt()
        .total_hours(true);

    // Case 3: Job started on target day, ended after
    let started_on_day = start_dt.clone().dt().date().eq(target_date_col.clone());
    let ended_after = end_dt.gt_eq(day_end_lit.clone());
    let case_ended_after = started_on_day.and(ended_after.clone());
    // Duration - convert microseconds to hours
    let duration_ended_after = (day_end_lit - start_dt.clone()).dt().total_hours(true);

    // Case 4: Job spanned multiple days (started before and ended after)
    let spanning = started_before.and(ended_after.clone());
    let duration_spanning: Expr = lit(24.0);

    // Combine cases with when/then/otherwise
    let daily_duration = when(same_day)
        .then(duration_same_day)
        .when(case_started_before)
        .then(duration_started_before)
        .when(case_ended_after)
        .then(duration_ended_after)
        .when(spanning)
        .then(duration_spanning)
        .otherwise(lit(0.0))
        .alias("daily_duration_hours");

    // Add date column
    let date_col = lit(date_str.to_string())
        .str()
        .to_date(StrptimeOptions {
            format: Some("%Y-%m-%d".into()),
            ..Default::default()
        })
        .alias("date");

    lf.with_columns([daily_duration, date_col])
}

/// Parses the TotalCPU column and converts it to seconds.
///
/// TotalCPU represents CPU time (user + system) in seconds.
/// Possible formats:
/// - HH:MM:SS
/// - MM:SS.ms
/// - JJ-HH:MM:SS (days-hours:minutes:seconds)
///
/// This function adds a `TotalCPU_seconds` column with the parsed duration in seconds.
///
/// # Arguments
/// * `lf` - A Polars LazyFrame with a "TotalCPU" column
///
/// # Returns
/// A new LazyFrame with the TotalCPU_seconds column added
pub fn parse_total_cpu_col(mut lf: LazyFrame) -> LazyFrame {
    // First, determine which format matches and extract groups
    // Format 1: JJ-HH:MM:SS (days-hours:minutes:seconds)
    let is_days_format = col("TotalCPU")
        .str()
        .contains(lit(r"^\d+-\d+:\d+:\d+$"), false);

    // Format 2: HH:MM:SS (hours:minutes:seconds)
    let is_hms_format = col("TotalCPU").str().contains(lit(r"^\d+:\d+:\d+$"), false);

    // Format 3: MM:SS.ms (minutes:seconds.milliseconds)
    let is_ms_format = col("TotalCPU")
        .str()
        .contains(lit(r"^\d+:\d+\.\d+$"), false);

    // Extract days, hours, minutes, seconds for each format
    // Format JJ-HH:MM:SS
    let days_hms_extract = col("TotalCPU")
        .str()
        .extract(
            lit(r"(?<days>\d+)-(?<hours>\d+):(?<minutes>\d+):(?<seconds>\d+)"),
            1,
        )
        .cast(DataType::Int64);

    let hours_hms_extract = col("TotalCPU")
        .str()
        .extract(
            lit(r"(?<days>\d+)-(?<hours>\d+):(?<minutes>\d+):(?<seconds>\d+)"),
            2,
        )
        .cast(DataType::Int64);

    let minutes_hms_extract = col("TotalCPU")
        .str()
        .extract(
            lit(r"(?<days>\d+)-(?<hours>\d+):(?<minutes>\d+):(?<seconds>\d+)"),
            3,
        )
        .cast(DataType::Int64);

    let seconds_hms_extract = col("TotalCPU")
        .str()
        .extract(
            lit(r"(?<days>\d+)-(?<hours>\d+):(?<minutes>\d+):(?<seconds>\d+)"),
            4,
        )
        .cast(DataType::Int64);

    // Format HH:MM:SS (without days, set days to 0)
    let hours_only_extract = col("TotalCPU")
        .str()
        .extract(lit(r"(?<hours>\d+):(?<minutes>\d+):(?<seconds>\d+)"), 1)
        .cast(DataType::Int64);

    let minutes_only_extract = col("TotalCPU")
        .str()
        .extract(lit(r"(?<hours>\d+):(?<minutes>\d+):(?<seconds>\d+)"), 2)
        .cast(DataType::Int64);

    let seconds_only_extract = col("TotalCPU")
        .str()
        .extract(lit(r"(?<hours>\d+):(?<minutes>\d+):(?<seconds>\d+)"), 3)
        .cast(DataType::Int64);

    // Format MM:SS.ms (minutes:seconds.milliseconds, set days and hours to 0)
    let minutes_ms_extract = col("TotalCPU")
        .str()
        .extract(lit(r"(?<minutes>\d+):(?<seconds>\d+)"), 1)
        .cast(DataType::Int64);

    let seconds_ms_extract = col("TotalCPU")
        .str()
        .extract(lit(r"(?<minutes>\d+):(?<seconds>\d+)"), 2)
        .str()
        .extract(lit(r"^(\d+)"), 1) // Extract only integer part of seconds
        .cast(DataType::Int64);

    // Build the final values using when/then/otherwise
    let days_value = when(is_days_format.clone())
        .then(days_hms_extract)
        .when(is_hms_format.clone())
        .then(lit(0i64))
        .when(is_ms_format.clone())
        .then(lit(0i64))
        .otherwise(lit(0i64));

    let hours_value = when(is_days_format.clone())
        .then(hours_hms_extract)
        .when(is_hms_format.clone())
        .then(hours_only_extract)
        .when(is_ms_format.clone())
        .then(lit(0i64))
        .otherwise(lit(0i64));

    let minutes_value = when(is_days_format.clone())
        .then(minutes_hms_extract)
        .when(is_hms_format.clone())
        .then(minutes_only_extract)
        .when(is_ms_format.clone())
        .then(minutes_ms_extract)
        .otherwise(lit(0i64));

    let seconds_value = when(is_days_format.clone())
        .then(seconds_hms_extract)
        .when(is_hms_format.clone())
        .then(seconds_only_extract)
        .when(is_ms_format.clone())
        .then(seconds_ms_extract)
        .otherwise(lit(0i64));

    // Calculate total seconds: days * 86400 + hours * 3600 + minutes * 60 + seconds
    let total_seconds = (days_value.clone() * lit(86400i64))
        + (hours_value.clone() * lit(3600i64))
        + (minutes_value * lit(60i64))
        + seconds_value;

    lf = lf.with_columns([days_value.alias("_days"), hours_value.alias("_hours")]);

    lf = lf.with_columns([total_seconds.alias("TotalCPU_seconds")]);

    // Drop intermediate columns
    lf = lf.drop(by_name(["_days", "_hours"], true, false));

    lf
}

/// Adds columns to input_parquet (in-place)
///
/// # Arguments
/// - `input_parquet`: Path to the parquet you'd like to enrich
/// - `input_sizes`: Path to a CSV file with actual pipeline inputs sizes
pub fn add_metrics_relative_to_input_size_inplace<P>(
    input_parquet: P,
    input_sizes: P,
) -> Result<(), UsageReportError>
where
    P: AsRef<Path>,
{
    let intermediary_file = input_parquet.as_ref().with_extension(".tmp.parquet");

    let conn: Connection = duckdb::Connection::open_in_memory()?;
    let query = format!(
        r#"
        COPY (
            SELECT run_metrics.*,
                ((run_metrics.ElapsedRaw / 60.0) / (insizes.input_size_bytes / pow(2, 20))) AS MinPerMo,
                ((run_metrics.ReqMem / pow(2, 20)) / (insizes.input_size_bytes / pow(2, 20))) AS UsedRAMPerMo,
                input_size_bytes,
                inputs
            FROM read_parquet('{}') run_metrics
            LEFT JOIN read_csv('{}', delim='|') insizes
            ON insizes.slurm_jobid = run_metrics.JobID
        ) TO '{}'
        "#,
        input_parquet.as_ref().display(),
        input_sizes.as_ref().display(),
        intermediary_file.display()
    );

    conn.execute(&query, [])?;

    std::fs::rename(&intermediary_file, input_parquet)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::df;

    fn create_test_lazyframe(data: &[(&str, &str, &str, &str)]) -> LazyFrame {
        let df = df!(
            "JobID" => data.iter().map(|(id, _, _, _)| (*id).to_string()).collect::<Vec<String>>(),
            "Start" => data.iter().map(|(_, start, _,_)| (*start).to_string()).collect::<Vec<String>>(),
            "End" => data.iter().map(|(_, _, end,_)| (*end).to_string()).collect::<Vec<String>>(),
            "Submit"=>data.iter().map(|(_, _, _,submit)| (*submit).to_string()).collect::<Vec<String>>()
        )
        .unwrap();
        df.lazy()
    }

    #[test]
    fn test_same_day_job() {
        let lf = create_test_lazyframe(&[(
            "1",
            "2026-02-24T10:00:00",
            "2026-02-24T14:00:00",
            "2026-02-24T10:00:00",
        )]);
        let result = add_daily_duration(lf, "2026-02-24").collect().unwrap();

        let daily_duration = result
            .column("daily_duration_hours")
            .unwrap()
            .get(0)
            .unwrap();
        assert!((daily_duration.try_extract::<f64>().unwrap() - 4.0).abs() < 0.001);
    }

    #[test]
    fn test_job_started_previous_day() {
        let lf = create_test_lazyframe(&[(
            "2",
            "2026-02-23T22:00:00",
            "2026-02-24T02:30:00",
            "2026-02-23T22:00:00",
        )]);
        let result = add_daily_duration(lf, "2026-02-24").collect().unwrap();

        // From midnight (00:00) to 02:30 = 2 hours and a half
        let daily_duration = result
            .column("daily_duration_hours")
            .unwrap()
            .get(0)
            .unwrap();
        assert!((daily_duration.try_extract::<f64>().unwrap() - 2.5).abs() < 0.001);
    }

    #[test]
    fn test_job_ends_next_day() {
        let lf = create_test_lazyframe(&[(
            "3",
            "2026-02-24T20:00:00",
            "2026-02-25T04:00:00",
            "2026-02-24T20:00:00",
        )]);
        let result = add_daily_duration(lf, "2026-02-24").collect().unwrap();

        // From 20:00 to midnight (24:00) = 4 hours
        let daily_duration = result
            .column("daily_duration_hours")
            .unwrap()
            .get(0)
            .unwrap();
        assert!((daily_duration.try_extract::<f64>().unwrap() - 4.0).abs() < 0.001);
    }

    #[test]
    fn test_spanning_job() {
        let lf = create_test_lazyframe(&[(
            "4",
            "2026-02-23T12:00:00",
            "2026-02-25T12:00:00",
            "2026-02-23T12:00:00",
        )]);
        let result = add_daily_duration(lf, "2026-02-24").collect().unwrap();

        // Full day = 24 hours
        let daily_duration = result
            .column("daily_duration_hours")
            .unwrap()
            .get(0)
            .unwrap();
        assert!((daily_duration.try_extract::<f64>().unwrap() - 24.0).abs() < 0.001);
    }

    #[test]
    fn test_job_not_on_target_day() {
        let lf = create_test_lazyframe(&[(
            "5",
            "2026-02-22T10:00:00",
            "2026-02-22T14:00:00",
            "2026-02-22T10:00:00",
        )]);
        let result = add_daily_duration(lf, "2026-02-24").collect().unwrap();

        let daily_duration = result
            .column("daily_duration_hours")
            .unwrap()
            .get(0)
            .unwrap();
        assert!((daily_duration.try_extract::<f64>().unwrap() - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_job_runs_future_day() {
        let lf = create_test_lazyframe(&[(
            "6",
            "2026-02-25T10:00:00",
            "2026-02-25T14:00:00",
            "2026-02-25T10:00:00",
        )]);
        let result = add_daily_duration(lf, "2026-02-24").collect().unwrap();

        let daily_duration = result
            .column("daily_duration_hours")
            .unwrap()
            .get(0)
            .unwrap();
        assert!((daily_duration.try_extract::<f64>().unwrap() - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_midnight_to_midnight() {
        let lf = create_test_lazyframe(&[(
            "8",
            "2026-02-24T00:00:00",
            "2026-02-25T00:00:00",
            "2026-02-24T00:00:00",
        )]);
        let result = add_daily_duration(lf, "2026-02-24").collect().unwrap();

        // This should be 24 hours since end is exclusive (at midnight next day)
        let daily_duration = result
            .column("daily_duration_hours")
            .unwrap()
            .get(0)
            .unwrap();
        eprintln!("{}", daily_duration);
        assert!((daily_duration.try_extract::<f64>().unwrap() - 24.0).abs() < 0.001);
    }

    #[test]
    fn test_job_wait_duration() {
        let lf = create_test_lazyframe(&[
            (
                "1",
                "2026-02-24T12:00:00",
                "2026-02-24T14:00:00",
                "2026-02-24T10:00:00",
            ), // 2h wait time
            (
                "2",
                "2026-02-23T22:00:00",
                "2026-02-24T02:00:00",
                "2026-02-23T21:00:00",
            ), // 1h wait time
            (
                "3",
                "2026-02-24T20:00:00",
                "2026-02-25T04:00:00",
                "2026-02-24T18:30:00",
            ), // 1h30min wait time
            (
                "4",
                "2026-02-23T12:00:00",
                "2026-02-25T12:00:00",
                "2026-02-23T11:45:00",
            ), // 15min wait time
            (
                "5",
                "2026-02-22T10:00:00",
                "2026-02-22T14:00:00",
                "2026-02-22T09:40:00",
            ), // 20min wait time
        ]);
        let result = add_wait_time_cols(lf).collect().unwrap();

        let expected_hours = [2.0, 1.0, 1.5, 0.25, 0.333333];
        let expected_seconds = [7200, 3600, 5400, 900, 1200];
        for (i, (&exp_hours, &exp_secs)) in expected_hours
            .iter()
            .zip(expected_seconds.iter())
            .enumerate()
        {
            let wait_time_hours = result.column("wait_time_hours").unwrap().get(i).unwrap();
            assert!(
                (wait_time_hours.try_extract::<f64>().unwrap() - exp_hours).abs() < 0.001,
                "Wait time for job {} is incorrect: expected {} hours, got {} hours",
                i + 1,
                exp_hours,
                wait_time_hours.try_extract::<f64>().unwrap()
            );

            let wait_time_seconds = result.column("wait_time_seconds").unwrap().get(i).unwrap();
            assert!(
                wait_time_seconds.try_extract::<i32>().unwrap() == exp_secs,
                "Wait time for job {} is incorrect: expected {} seconds, got {} seconds",
                i + 1,
                exp_secs,
                wait_time_seconds.try_extract::<i32>().unwrap()
            );
        }
    }

    #[test]
    fn test_multiple_jobs() {
        let lf = create_test_lazyframe(&[
            (
                "1",
                "2026-02-24T10:00:00",
                "2026-02-24T14:00:00",
                "2026-02-24T10:00:00",
            ), // 4h
            (
                "2",
                "2026-02-23T22:00:00",
                "2026-02-24T02:00:00",
                "2026-02-23T22:00:00",
            ), // 2h
            (
                "3",
                "2026-02-24T20:00:00",
                "2026-02-25T04:00:00",
                "2026-02-24T20:00:00",
            ), // 4h
            (
                "4",
                "2026-02-23T12:00:00",
                "2026-02-25T12:00:00",
                "2026-02-23T12:00:00",
            ), // 24h
            (
                "5",
                "2026-02-22T10:00:00",
                "2026-02-22T14:00:00",
                "2026-02-22T10:00:00",
            ), // 0h
        ]);
        let result = add_daily_duration(lf, "2026-02-24").collect().unwrap();

        let expected = [4.0, 2.0, 4.0, 24.0, 0.0];
        for (i, &exp) in expected.iter().enumerate() {
            let daily_duration = result
                .column("daily_duration_hours")
                .unwrap()
                .get(i)
                .unwrap();
            assert!(
                (daily_duration.try_extract::<f64>().unwrap() - exp).abs() < 0.001,
                "Job {} failed: expected {}, got {}",
                i + 1,
                exp,
                daily_duration.try_extract::<f64>().unwrap()
            );
        }
    }

    #[test]
    fn test_half_hour_job() {
        let lf = create_test_lazyframe(&[(
            "1",
            "2026-02-24T10:00:00",
            "2026-02-24T10:30:00",
            "2026-02-24T10:00:00",
        )]);
        let result = add_daily_duration(lf, "2026-02-24").collect().unwrap();

        // Print all columns including debug
        eprintln!("Columns: {:?}", result.get_column_names());
        eprintln!("Result:\n{}", result);

        let daily_duration = result
            .column("daily_duration_hours")
            .unwrap()
            .get(0)
            .unwrap();
        let duration_value = daily_duration.try_extract::<f64>().unwrap();
        eprintln!("test_half_hour_job: got {} hours", duration_value);

        assert!((duration_value - 0.5).abs() < 0.001);
    }
}
