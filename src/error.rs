use thiserror::Error;

#[derive(Error, Debug)]
pub enum UsageReportError {
    #[error(transparent)]
    PolarsError(#[from] polars::error::PolarsError),
    #[error(transparent)]
    DuckDBError(#[from] duckdb::Error),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error("Option is unexpectedly None ({message}): {file}:{line}:{column}")]
    NoneValueError {
        message: String,
        file: String,
        line: u32,
        column: u32,
    },
    #[error("External process error (command: {cmd})")]
    ExternalProcessError { cmd: String },
    #[error(transparent)]
    JinjaError(#[from] minijinja::Error),
    #[error(transparent)]
    InPlaceError(#[from] in_place::InPlaceError),
    #[error(transparent)]
    CsvError(#[from] csv::Error),
    #[error("Empty file: {} is empty.\n {}",.filename,.message)]
    EmptyFile { filename: String, message: String },
    #[error("Empty list: {} is empty.\n {}",.listname,.message)]
    EmptyList { listname: String, message: String },
}
