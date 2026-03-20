//! Utility functions for SLURM usage reporting.
//!
//! This module provides functions for processing SLURM job data using Polars.

use chrono::NaiveDate;
use polars::prelude::*;

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
pub fn get_color_map(name: &str) -> Option<Vec<ColorMapEntry>> {
    match name {
        "default" => Some(get_default_cmap()),
        _ => Some(get_default_cmap()),
    }
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
        strict: false,
        exact: false,
        cache: true,
    };

    lf = lf.with_columns([col("Start").str().to_datetime(
        Default::default(),
        Default::default(),
        datetime_conversion_options.clone(),
        lit("raise"),
    )]);

    lf.with_columns([(col("Start").str().to_datetime(
        Default::default(),
        Default::default(),
        datetime_conversion_options.clone(),
        lit("raise"),
    ) - col("Submit").str().to_datetime(
        Default::default(),
        Default::default(),
        datetime_conversion_options.clone(),
        lit("raise"),
    ))
    .alias("wait_dt")])
        .with_columns([
            col("wait_dt")
                .dt()
                .total_seconds(true)
                .alias("wait_time_seconds"),
            col("wait_dt")
                .dt()
                .total_hours(true)
                .alias("wait_time_hours"),
        ])
        .drop(by_name(["wait_dt"], false, false))
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
pub fn add_job_duration_cols(lf: LazyFrame) -> LazyFrame {
    let datetime_format = "%Y-%m-%dT%H:%M:%S";

    let datetime_conversion_options: StrptimeOptions = StrptimeOptions {
        format: Some(datetime_format.into()),
        strict: false,
        exact: false,
        cache: true,
    };

    lf.with_columns([(col("End").str().to_datetime(
        Default::default(),
        Default::default(),
        datetime_conversion_options.clone(),
        lit("raise"),
    ) - col("Start").str().to_datetime(
        Default::default(),
        Default::default(),
        datetime_conversion_options.clone(),
        lit("raise"),
    ))
    .alias("duration_dt")])
        .with_columns([col("duration_dt")
            .dt()
            .total_seconds(true)
            .alias("job_duration_seconds")])
        .drop(by_name(["duration_dt"], false, false))
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

    // Parse the target date
    let target_date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .expect("Invalid date format, expected YYYY-MM-DD");
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

    // Create day boundaries as chrono NaiveDateTime
    let day_start_ndt = target_date.and_hms_opt(0, 0, 0).expect("Invalid day start");
    let next_day = target_date + chrono::TimeDelta::days(1);
    let day_end_ndt = next_day.and_hms_opt(0, 0, 0).expect("Invalid day end");

    // Convert to microseconds since epoch for Polars datetime literals
    let day_start_us = day_start_ndt.and_utc().timestamp_micros();
    let day_end_us = day_end_ndt.and_utc().timestamp_micros();

    // Target date as days since epoch for Date literal
    let target_days = (target_date - epoch).num_days() as i32;
    let target_date_lit = lit(target_days).cast(DataType::Date);

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

    // Create datetime literals for day boundaries using timestamp microseconds
    let day_start_lit = lit(day_start_us)
        .cast(DataType::Datetime(TimeUnit::Microseconds, None))
        .alias("day_start");
    let day_end_lit = lit(day_end_us)
        .cast(DataType::Datetime(TimeUnit::Microseconds, None))
        .alias("day_end");

    // Build the conditional expression for daily duration
    // Case 1: Job started and ended on the same day (target day)
    let same_day = start_dt
        .clone()
        .dt()
        .date()
        .eq(target_date_lit.clone())
        .and(end_dt.clone().dt().date().eq(target_date_lit.clone()));
    let duration_same_day =
        (end_dt.clone() - start_dt.clone()).dt().total_seconds(true) / lit(3600.0);

    // Case 2: Job started before target day, ended on target day
    let started_before = start_dt.clone().lt(day_start_lit.clone());
    let ended_on_day = end_dt.clone().dt().date().eq(target_date_lit.clone());
    let case_started_before = started_before.clone().and(ended_on_day);
    let duration_started_before = (end_dt.clone() - day_start_lit.clone())
        .dt()
        .total_seconds(true)
        / lit(3600.0);

    // Case 3: Job started on target day, ended after
    let started_on_day = start_dt.clone().dt().date().eq(target_date_lit.clone());
    let ended_after = end_dt.gt_eq(day_end_lit.clone());
    let case_ended_after = started_on_day.and(ended_after.clone());
    let duration_ended_after =
        (day_end_lit - start_dt.clone()).dt().total_seconds(true) / lit(3600.0);

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

    // Add date column using days since epoch
    let date_col = target_date_lit.alias("date");

    lf.with_columns([daily_duration, date_col])
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::df;

    fn create_test_lazyframe(data: &[(&str, &str, &str)]) -> LazyFrame {
        let df = df!(
            "JobID" => data.iter().map(|(id, _, _)| (*id).to_string()).collect::<Vec<String>>(),
            "Start" => data.iter().map(|(_, start, _)| (*start).to_string()).collect::<Vec<String>>(),
            "End" => data.iter().map(|(_, _, end)| (*end).to_string()).collect::<Vec<String>>(),
        )
        .unwrap();
        df.lazy()
    }

    #[test]
    fn test_same_day_job() {
        let lf = create_test_lazyframe(&[("1", "2026-02-24T10:00:00", "2026-02-24T14:00:00")]);
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
        let lf = create_test_lazyframe(&[("2", "2026-02-23T22:00:00", "2026-02-24T02:00:00")]);
        let result = add_daily_duration(lf, "2026-02-24").collect().unwrap();

        // From midnight (00:00) to 02:00 = 2 hours
        let daily_duration = result
            .column("daily_duration_hours")
            .unwrap()
            .get(0)
            .unwrap();
        assert!((daily_duration.try_extract::<f64>().unwrap() - 2.0).abs() < 0.001);
    }

    #[test]
    fn test_job_ends_next_day() {
        let lf = create_test_lazyframe(&[("3", "2026-02-24T20:00:00", "2026-02-25T04:00:00")]);
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
        let lf = create_test_lazyframe(&[("4", "2026-02-23T12:00:00", "2026-02-25T12:00:00")]);
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
        let lf = create_test_lazyframe(&[("5", "2026-02-22T10:00:00", "2026-02-22T14:00:00")]);
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
        let lf = create_test_lazyframe(&[("6", "2026-02-25T10:00:00", "2026-02-25T14:00:00")]);
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
        let lf = create_test_lazyframe(&[("8", "2026-02-24T00:00:00", "2026-02-25T00:00:00")]);
        let result = add_daily_duration(lf, "2026-02-24").collect().unwrap();

        // This should be 24 hours since end is exclusive (at midnight next day)
        let daily_duration = result
            .column("daily_duration_hours")
            .unwrap()
            .get(0)
            .unwrap();
        assert!((daily_duration.try_extract::<f64>().unwrap() - 24.0).abs() < 0.001);
    }

    #[test]
    fn test_multiple_jobs() {
        let lf = create_test_lazyframe(&[
            ("1", "2026-02-24T10:00:00", "2026-02-24T14:00:00"), // 4h
            ("2", "2026-02-23T22:00:00", "2026-02-24T02:00:00"), // 2h
            ("3", "2026-02-24T20:00:00", "2026-02-25T04:00:00"), // 4h
            ("4", "2026-02-23T12:00:00", "2026-02-25T12:00:00"), // 24h
            ("5", "2026-02-22T10:00:00", "2026-02-22T14:00:00"), // 0h
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
        let lf = create_test_lazyframe(&[("1", "2026-02-24T10:00:00", "2026-02-24T10:30:00")]);
        let result = add_daily_duration(lf, "2026-02-24").collect().unwrap();

        let daily_duration = result
            .column("daily_duration_hours")
            .unwrap()
            .get(0)
            .unwrap();
        assert!((daily_duration.try_extract::<f64>().unwrap() - 0.5).abs() < 0.001);
    }
}
