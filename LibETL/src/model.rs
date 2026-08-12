//! Data model passed between the framework and ETL processors.

use libdatasource::model::{Row, TableSchema};
use serde::{Deserialize, Serialize};

/// The logical input row given to an ETL processor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtlRow {
    /// Name of the source connection this row came from.
    pub source_connection: String,
    /// Name of the source table this row came from.
    pub source_table: String,
    /// Target connection(s) configured for the job.
    pub target_connections: Vec<String>,
    /// The row data.
    pub row: Row,
    /// Structure of the source table, when available.
    #[serde(default)]
    pub source_schema: Option<TableSchema>,
    /// Structure of the (first) target table, when available.
    #[serde(default)]
    pub target_schema: Option<TableSchema>,
}

/// A single output row produced by a processor. `table` selects the target
/// table this row is written to (used for one-to-many / splitting); when the
/// job has a single target it can be left empty and the job default is used.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtlOutputRow {
    #[serde(default)]
    pub table: String,
    pub row: Row,
}

impl EtlOutputRow {
    pub fn new(table: impl Into<String>, row: Row) -> Self {
        EtlOutputRow {
            table: table.into(),
            row,
        }
    }
}

/// Payload received by a dynamic ETL plugin over the C ABI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtlInput {
    pub source_connection: String,
    pub source_table: String,
    pub target_connections: Vec<String>,
    pub row: Row,
    #[serde(default)]
    pub source_schema: Option<TableSchema>,
    #[serde(default)]
    pub target_schema: Option<TableSchema>,
}

impl EtlInput {
    pub fn to_etl_row(&self) -> EtlRow {
        EtlRow {
            source_connection: self.source_connection.clone(),
            source_table: self.source_table.clone(),
            target_connections: self.target_connections.clone(),
            row: self.row.clone(),
            source_schema: self.source_schema.clone(),
            target_schema: self.target_schema.clone(),
        }
    }
}

impl From<EtlRow> for EtlInput {
    fn from(r: EtlRow) -> Self {
        EtlInput {
            source_connection: r.source_connection,
            source_table: r.source_table,
            target_connections: r.target_connections,
            row: r.row,
            source_schema: r.source_schema,
            target_schema: r.target_schema,
        }
    }
}
