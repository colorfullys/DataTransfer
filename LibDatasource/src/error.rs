use std::fmt;

/// Error type shared by the datasource SDK and every plugin implementation.
#[derive(Debug, thiserror::Error)]
pub enum DatasourceError {
    /// Underlying database driver error.
    #[error("database error: {0}")]
    Database(String),

    /// Invalid SQL or invalid arguments passed to a driver call.
    #[error("invalid arguments: {0}")]
    InvalidArgument(String),

    /// Connection level failure (connect / ping / pool exhausted).
    #[error("connection error: {0}")]
    Connection(String),

    /// Schema related failure (table not found, column mismatch, ...).
    #[error("schema error: {0}")]
    Schema(String),

    /// Value cannot be represented in the target database type.
    #[error("value conversion error: {0}")]
    Conversion(String),

    /// Feature not supported by this datasource implementation.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// Serialization failure at the FFI boundary.
    #[error("serialization error: {0}")]
    Serialize(String),

    /// Filesystem / IO failure (plugin file, state files, ...).
    #[error("io error: {0}")]
    Io(String),

    /// Any other error.
    #[error("{0}")]
    Other(String),
}

impl DatasourceError {
    pub fn db<E: fmt::Display>(e: E) -> Self {
        DatasourceError::Database(e.to_string())
    }

    pub fn conn<E: fmt::Display>(e: E) -> Self {
        DatasourceError::Connection(e.to_string())
    }

    pub fn conversion<E: fmt::Display>(e: E) -> Self {
        DatasourceError::Conversion(e.to_string())
    }
}

impl From<serde_json::Error> for DatasourceError {
    fn from(e: serde_json::Error) -> Self {
        DatasourceError::Serialize(e.to_string())
    }
}

impl From<std::io::Error> for DatasourceError {
    fn from(e: std::io::Error) -> Self {
        DatasourceError::Io(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, DatasourceError>;
