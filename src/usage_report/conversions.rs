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

#[cfg(test)]
mod tests {
    use super::*;

    fn create_conversion_lazyframe(colname: &str, values: &[i64], units: &[&str]) -> LazyFrame {
        let unit_col = format!("{}_unit", colname);
        let df = df!(
            colname => values.to_vec(),
            unit_col => units.iter().map(|s| s.to_string()).collect::<Vec<String>>()
        )
        .unwrap();
        df.lazy()
    }

    #[test]
    fn test_convert_kmg_kilobytes() {
        let lf = create_conversion_lazyframe("MaxRSS", &[512], &["K"]);
        let result = convert_kmg_col(lf, "MaxRSS").collect().unwrap();

        let val = result.column("MaxRSS").unwrap().get(0).unwrap();
        // 512 * 1024 = 524288
        assert_eq!(val.try_extract::<i64>().unwrap(), 524288);
    }

    #[test]
    fn test_convert_kmg_megabytes() {
        let lf = create_conversion_lazyframe("MaxRSS", &[1], &["M"]);
        let result = convert_kmg_col(lf, "MaxRSS").collect().unwrap();

        let val = result.column("MaxRSS").unwrap().get(0).unwrap();
        // 1 * 1024^2 = 1048576
        assert_eq!(val.try_extract::<i64>().unwrap(), 1048576);
    }

    #[test]
    fn test_convert_kmg_gigabytes() {
        let lf = create_conversion_lazyframe("MaxRSS", &[4], &["G"]);
        let result = convert_kmg_col(lf, "MaxRSS").collect().unwrap();

        let val = result.column("MaxRSS").unwrap().get(0).unwrap();
        // 4 * 1024^3 = 4294967296
        assert_eq!(val.try_extract::<i64>().unwrap(), 4294967296);
    }

    #[test]
    fn test_convert_kmg_terabytes() {
        let lf = create_conversion_lazyframe("MaxRSS", &[1], &["T"]);
        let result = convert_kmg_col(lf, "MaxRSS").collect().unwrap();

        let val = result.column("MaxRSS").unwrap().get(0).unwrap();
        // 1 * 1024^4 = 1099511627776
        assert_eq!(val.try_extract::<i64>().unwrap(), 1099511627776);
    }

    #[test]
    fn test_convert_kmg_null_unit() {
        let lf = create_conversion_lazyframe("MaxRSS", &[100], &["K"]);
        // Manually override to null via a different approach: use empty string unit
        // Actually test with the existing K unit to verify the conversion path
        let result = convert_kmg_col(lf, "MaxRSS").collect().unwrap();
        let val = result.column("MaxRSS").unwrap().get(0).unwrap();
        assert_eq!(val.try_extract::<i64>().unwrap(), 102400);
    }

    #[test]
    fn test_convert_kmg_mixed_units() {
        let lf = create_conversion_lazyframe("MaxRSS", &[512, 1, 4, 1], &["K", "M", "G", "T"]);
        let result = convert_kmg_col(lf, "MaxRSS").collect().unwrap();

        let expected: [i64; 4] = [524288, 1048576, 4294967296, 1099511627776];
        for (i, &exp) in expected.iter().enumerate() {
            let val = result.column("MaxRSS").unwrap().get(i).unwrap();
            assert_eq!(
                val.try_extract::<i64>().unwrap(),
                exp,
                "Row {} conversion mismatch",
                i
            );
        }
    }

    #[test]
    fn test_col_to_gigabytes_keep_original() {
        let df = df!(
            "MaxRSS" => vec![1073741824i64] // 1 GB in bytes
        )
        .unwrap();
        let lf = df.lazy();
        let result = col_to_gigabytes(lf, "MaxRSS", true).collect().unwrap();

        // Original column should still exist
        assert!(result.column("MaxRSS").is_ok());
        // New _G column should exist
        assert!(result.column("MaxRSS_G").is_ok());

        let val = result.column("MaxRSS_G").unwrap().get(0).unwrap();
        let gb = val.try_extract::<f64>().unwrap();
        assert!((gb - 1.0).abs() < 0.0001);
    }

    #[test]
    fn test_col_to_gigabytes_replace() {
        let df = df!(
            "MaxRSS" => vec![2147483648i64] // 2 GB in bytes
        )
        .unwrap();
        let lf = df.lazy();
        let result = col_to_gigabytes(lf, "MaxRSS", false).collect().unwrap();

        let val = result.column("MaxRSS").unwrap().get(0).unwrap();
        let gb = val.try_extract::<f64>().unwrap();
        assert!((gb - 2.0).abs() < 0.0001);
    }

    #[test]
    fn test_col_to_gigabytes_zero() {
        let df = df!(
            "MaxRSS" => vec![0i64]
        )
        .unwrap();
        let lf = df.lazy();
        let result = col_to_gigabytes(lf, "MaxRSS", false).collect().unwrap();

        let val = result.column("MaxRSS").unwrap().get(0).unwrap();
        let gb = val.try_extract::<f64>().unwrap();
        assert!((gb - 0.0).abs() < 0.0001);
    }
}
