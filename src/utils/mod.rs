//! Utilities module for SLURM usage reporting.
//!
//! This module provides utility functions for processing SLURM job data,
//! including column definitions, color maps, and data transformation functions.

pub mod utils;

pub use utils::{
    ALL_COLUMNS, ColorMapEntry, INTERESTING_COLUMNS, USEFUL_COLUMNS, add_daily_duration,
    add_job_duration_cols, add_wait_time_cols, get_color_map, get_default_cmap,
};
