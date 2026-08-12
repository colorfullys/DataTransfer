use std::fmt;

/// Error type for the ETL pipeline and processors.
#[derive(Debug, thiserror::Error)]
pub enum EtlError {
    #[error("etl processing error: {0}")]
    Processing(String),

    #[error("etl configuration error: {0}")]
    Config(String),

    #[error("etl expression error: {0}")]
    Expression(String),

    #[error("lookup error: {0}")]
    Lookup(String),

    #[error("serialization error: {0}")]
    Serialize(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl EtlError {
    pub fn proc<E: fmt::Display>(e: E) -> Self {
        EtlError::Processing(e.to_string())
    }

    pub fn config<E: fmt::Display>(e: E) -> Self {
        EtlError::Config(e.to_string())
    }
}

impl From<serde_json::Error> for EtlError {
    fn from(e: serde_json::Error) -> Self {
        EtlError::Serialize(e.to_string())
    }
}

impl From<std::io::Error> for EtlError {
    fn from(e: std::io::Error) -> Self {
        EtlError::Io(e.to_string())
    }
}

impl From<libdatasource::DatasourceError> for EtlError {
    fn from(e: libdatasource::DatasourceError) -> Self {
        EtlError::Lookup(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, EtlError>;