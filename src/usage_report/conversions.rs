//! Column type conversion functions for SLURM usage reporting.
//!
//! This module provides functions for converting column values to different
//! units or types, including:
//! - K/M/G unit conversion to bytes
//! - Conversion to gigabytes
//! - Time parsing (TotalCPU column)
//!
//! ## Important: Conversion order and behavior
//!
//! The conversion functions must be called in a specific pipeline order:
//! 1. First, use `add_units_kmg` to extract numeric value and unit
//! 2. Then, use `convert_kmg_col` to convert to bytes (this removes the _unit column)
//! 3. Finally, use `col_to_gigabytes` if needed

use polars::prelude::*;

/// Converts a column with K/M/G units to bytes.
///
/// This function requires a `{colname}_unit` column to exist (created by `add_units_kmg`).
/// The conversion is:
/// - K: multiply by 1024
/// - M: multiply by 1024^2
/// - G: multiply by 1024^3
/// - T: multiply by 1024^4
///
/// Note: This function does NOT drop the _unit column. Call `.drop(col("unit_col"))` afterwards
/// if you want to remove it.
///
/// # Arguments
/// * `lf` - A Polars LazyFrame with `{colname}` and `{colname}_unit` columns
/// * `colname` - The name of the column to convert
///
/// # Returns
/// A new LazyFrame with the column converted to bytes (Int64)
pub fn convert_kmg_col(lf: LazyFrame, colname: &str) -> LazyFrame {
    let unit_col = format!("{}_unit", colname);

    // Convert string to int64 first
    let value = col(colname).cast(DataType::Int64);

    // Create expressions for each unit multiplier
    let k_val = value.clone() * lit(1024i64);
    let m_val = value.clone() * lit(1024i64.pow(2));
    let g_val = value.clone() * lit(1024i64.pow(3));
    let t_val = value * lit(1024i64.pow(4));

    let converted = when(col(&unit_col).is_null())
        .then(lit(0i64))
        .when(col(&unit_col).eq(lit("K")))
        .then(k_val)
        .when(col(&unit_col).eq(lit("M")))
        .then(m_val)
        .when(col(&unit_col).eq(lit("G")))
        .then(g_val)
        .when(col(&unit_col).eq(lit("T")))
        .then(t_val)
        .otherwise(lit(0i64))
        .alias(colname);

    lf.with_columns([converted])
}

/// Converts a column to gigabytes.
///
/// # Arguments
/// * `lf` - A Polars LazyFrame
/// * `colname` - The name of the column to convert
/// * `keep_original` - If true, keeps the original column with `_G` suffix; if false, replaces it
///
/// # Returns
/// A new LazyFrame with the column in gigabytes
///
/// # TODO
/// Maybe this function is not that useful. I mean, if I want to display memory consumption, I could easily do it from bytes directly
pub fn col_to_gigabytes(lf: LazyFrame, colname: &str, keep_original: bool) -> LazyFrame {
    let new_name = if keep_original {
        format!("{}_G", colname)
    } else {
        colname.to_string()
    };

    // Convert to float64 first, then divide by 1024^3
    let divisor: f64 = 1024.0_f64.powi(3);
    let result = col(colname).cast(DataType::Float64) / lit(divisor);

    lf.with_columns([result.alias(new_name)])
}
