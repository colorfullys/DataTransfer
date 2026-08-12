use crate::error::Result;
use crate::model::{ConnectionConfig, Row, SyncMode, TableSchema, Value};
/// Standard datasource interface.
///
/// This is the contract every datasource plugin implements. The plugin itself
/// is free to use whatever driver it wants; only this interface crosses the
/// process boundary (serialised over the C ABI in `ffi`).
pub trait Datasource: Send + Sync {
    /// Short identifier of the datasource type, e.g. `mysql`.
    fn name(&self) -> &'static str;

    /// Establish the connection (or connection pool) described by `cfg`.
    fn connect(&mut self, cfg: &ConnectionConfig) -> Result<()>;

    /// Read the structure of `table`.
    fn get_schema(&self, table: &str) -> Result<TableSchema>;

    /// Run a read-only SQL statement and return every row.
    fn query(&self, sql: &str) -> Result<Vec<Row>>;

    /// Run a read-only SQL statement and return a page of rows.
    fn query_page(&self, sql: &str, offset: u64, limit: u64) -> Result<Vec<Row>>;

    /// Run a read-only SQL with positional parameters (used by ETL lookups).
    /// The SQL uses the datasource's native placeholder syntax (`?`, `$1`, `:1`).
    fn query_params(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>>;

    /// Run a write SQL statement (INSERT/UPDATE/DELETE/DDL) with optional
    /// positional parameters. Returns the affected row count.
    fn execute(&self, sql: &str, params: &[Value]) -> Result<u64>;

    /// Insert a batch of rows into `table`.
    ///
    /// * `columns` - the exact column order of `rows`.
    /// * `rows`    - `rows[i]` is aligned to `columns`.
    /// * `mode`    - `Full`, `Upsert` or `Append`.
    /// * `pk_columns` - primary key columns; used by `Upsert` conflict clause.
    fn batch_insert(
        &self,
        table: &str,
        columns: &[String],
        rows: &[Vec<Value>],
        mode: SyncMode,
        pk_columns: &[String],
    ) -> Result<u64>;

    /// Remove all rows from `table` (used by `Full` mode).
    fn truncate(&self, table: &str) -> Result<()>;

    /// Lightweight liveness check.
    fn ping(&self) -> Result<()>;

    /// Release all resources held by this datasource.
    fn close(&self) {}
}
