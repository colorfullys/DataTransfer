use std::fmt;

/// Errors produced by the orchestrator.
#[derive(Debug)]
pub enum AppError {
    Config(String),
    Datasource(String),
    Etl(String),
    Template(String),
    State(String),
    Schedule(String),
    Io(std::io::Error),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::Config(s) => write!(f, "config error: {s}"),
            AppError::Datasource(s) => write!(f, "datasource error: {s}"),
            AppError::Etl(s) => write!(f, "etl error: {s}"),
            AppError::Template(s) => write!(f, "template error: {s}"),
            AppError::State(s) => write!(f, "state error: {s}"),
            AppError::Schedule(s) => write!(f, "schedule error: {s}"),
            AppError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for AppError {}

impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        AppError::Io(e)
    }
}

impl From<libdatasource::DatasourceError> for AppError {
    fn from(e: libdatasource::DatasourceError) -> Self {
        AppError::Datasource(e.to_string())
    }
}

impl From<libetl::EtlError> for AppError {
    fn from(e: libetl::EtlError) -> Self {
        AppError::Etl(e.to_string())
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        AppError::State(e.to_string())
    }
}

pub type AppResult<T> = Result<T, AppError>;