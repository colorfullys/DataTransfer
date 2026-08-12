//! The ETL processor trait and the lookup capability that lets a processor
//! read other tables ("get other table data").
//!
//! Even though the DataTransfer process no longer depends on any concrete
//! database driver, a processor can still query any configured source: the
//! framework implements [`TableLookup`] over the datasource plugin registry
//! and injects it into every [`EtlContext`].

use crate::error::Result;
use crate::model::{EtlOutputRow, EtlRow};
use libdatasource::model::{Row, TableSchema};

/// A handle to query other tables through the framework's loaded datasources.
/// This is the only way an ETL processor gets external data, which keeps the
/// core database-agnostic.
pub trait TableLookup {
    /// Run a read-only query against `connection.table` and return up to
    /// `limit` rows of `columns`.
    ///
    /// * `where_clause` is a raw SQL condition (optional).
    /// * `params` are positional bind values for `?`-style placeholders of
    ///   the underlying database. Pass `Value::Null` to test for NULL.
    fn lookup(
        &self,
        connection: &str,
        table: &str,
        columns: Option<&[String]>,
        where_clause: Option<&str>,
        params: &[libdatasource::model::Value],
        limit: u64,
    ) -> Result<Vec<Row>>;
}

/// Execution context handed to every processor. Exposes the current table /
/// connection names and the [`TableLookup`] capability.
pub struct EtlContext<'a> {
    pub source_connection: &'a str,
    pub source_table: &'a str,
    pub target_connections: &'a [String],
    pub source_schema: Option<&'a TableSchema>,
    pub target_schema: Option<&'a TableSchema>,
    pub lookup: &'a dyn TableLookup,
}

impl<'a> std::fmt::Debug for EtlContext<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtlContext")
            .field("source_connection", &self.source_connection)
            .field("source_table", &self.source_table)
            .field("target_connections", &self.target_connections)
            .field("source_schema", &self.source_schema)
            .field("target_schema", &self.target_schema)
            .finish()
    }
}

impl<'a> EtlContext<'a> {
    /// Reconstruct a context from an input row plus the lookup capability.
    /// Used both by the host (built-in processors) and by dynamic plugin
    /// wrappers.
    pub fn from_row(input: &'a crate::model::EtlRow, lookup: &'a dyn TableLookup) -> Self {
        EtlContext {
            source_connection: &input.source_connection,
            source_table: &input.source_table,
            target_connections: &input.target_connections,
            source_schema: input.source_schema.as_ref(),
            target_schema: input.target_schema.as_ref(),
            lookup,
        }
    }

    /// Name of the table the current row belongs to.
    pub fn current_table(&self) -> &str {
        self.source_table
    }

    /// Name of the connection the current row came from.
    pub fn current_connection(&self) -> &str {
        self.source_connection
    }

    /// List of target connections configured for this job.
    pub fn target_connections(&self) -> &[String] {
        self.target_connections
    }

    /// Query another table through the framework. `params` may be empty and
    /// `where_clause` may be None for a full read.
    pub fn lookup_table(
        &self,
        connection: &str,
        table: &str,
        columns: Option<&[String]>,
        where_clause: Option<&str>,
        params: &[libdatasource::model::Value],
        limit: u64,
    ) -> Result<Vec<Row>> {
        self.lookup.lookup(connection, table, columns, where_clause, params, limit)
    }
}

/// A transformation step of the ETL pipeline.
///
/// `process` receives the current row plus context and returns zero or more
/// output rows:
/// * an empty `Vec` drops the row;
/// * one element keeps the row (possibly modified) for the default target;
/// * several elements split the row into multiple rows/targets.
pub trait EtlProcessor: Send + Sync {
    /// Canonical name used in configuration & logs.
    fn name(&self) -> &str;

    /// Transform a row.
    fn process(&self, ctx: &mut EtlContext, input: &EtlRow) -> Result<Vec<EtlOutputRow>>;
}

/// Optional per-instance configuration applied by the plugin host after
/// construction. A plugin type that only uses [`Default`] may skip
/// implementing this (the host passes the config as `serde_json::Value`
/// regardless; the default implementation ignores it).
pub trait EtlConfigure {
    fn configure(&mut self, config: &serde_json::Value) -> Result<()>;
}