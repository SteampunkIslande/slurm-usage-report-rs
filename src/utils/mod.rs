//! Utilities module for SLURM usage reporting.
//!
//! This module provides utility functions for processing SLURM job data,
//! including column definitions, color maps, and data transformation functions.

pub mod utils;
pub use utils::*;

pub mod lazyframe_edit;
pub use lazyframe_edit::*;
