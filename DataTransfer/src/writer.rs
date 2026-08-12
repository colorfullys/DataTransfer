//! Batch writer to a target datasource with retry + backoff.
//!
//! Rows are column-normalised (first-seen column order, NULL for missing) and
//! shipped via `Datasource::batch_insert`. `Full` mode truncates each table
//! once per run, before its first insert. The truncate-once set is shared
//! across writers so parallel writer threads do not truncate each other's
//! already-inserted rows.
//!
//! Multiple `Writer`s may share one `Arc<dyn Datasource>`; datasource plugins
//! backed by connection pools serve the writers with independent connections.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use libdatasource::datasource::Datasource;
use libdatasource::model::{Row, SyncMode, Value};

use crate::error::{AppError, AppResult};

pub struct Writer {
    ds: Arc<dyn Datasource>,
    mode: SyncMode,
    retry: u32,
    truncated: Arc<Mutex<HashSet<String>>>,
    /// Short identity used in logs (`main`, `w0`, `w1`, ...).
    tag: String,
}

impl Writer {
    pub fn new(
        ds: Arc<dyn Datasource>,
        mode: SyncMode,
        retry: u32,
        truncated: Arc<Mutex<HashSet<String>>>,
        tag: impl Into<String>,
    ) -> Writer {
        Writer {
            ds,
            mode,
            retry,
            truncated,
            tag: tag.into(),
        }
    }

    /// Write a batch of rows to `table`. In `Full` mode the table is truncated
    /// once per run (co-ordinated across all writers of the same run).
    pub fn write_batch(&mut self, table: &str, rows: &[Row], pk: &[String]) -> AppResult<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        if self.mode == SyncMode::Full {
            let first = {
                let mut set = self.truncated.lock().unwrap();
                set.insert(table.to_string())
            };
            if first {
                self.ds
                    .truncate(table)
                    .map_err(|e| AppError::Datasource(format!("truncate {table}: {e}")))?;
                log::info!("writer[{}] truncated target table {table}", self.tag);
            }
        }

        let (columns, matrix) = normalise(rows);
        let mut attempt = 0u32;
        loop {
            match self.ds.batch_insert(table, &columns, &matrix, self.mode, pk) {
                Ok(n) => {
                    log::info!(
                        "writer[{}] flushed {} row(s) → {} ({})",
                        self.tag,
                        n,
                        table,
                        self.mode.as_str()
                    );
                    return Ok(n);
                }
                Err(e) => {
                    attempt += 1;
                    if attempt > self.retry {
                        return Err(AppError::Datasource(format!(
                            "batch_insert {table} ({} rows) failed after {} attempts: {e}",
                            rows.len(),
                            attempt
                        )));
                    }
                    let backoff = Duration::from_secs(1u64 << (attempt.saturating_sub(1)));
                    log::warn!(
                        "writer[{}] batch_insert {table} attempt {}/{} failed: {e}; retrying in {:?}",
                        self.tag,
                        attempt,
                        self.retry + 1,
                        backoff
                    );
                    std::thread::sleep(backoff);
                }
            }
        }
    }
}

/// Column-normalise rows: deterministic first-seen column order; every row is
/// padded with NULL for columns it does not contain.
fn normalise(rows: &[Row]) -> (Vec<String>, Vec<Vec<Value>>) {
    let mut columns: Vec<String> = Vec::new();
    for r in rows {
        for c in r.data.keys() {
            if !columns.contains(c) {
                columns.push(c.clone());
            }
        }
    }
    let matrix: Vec<Vec<Value>> = rows.iter().map(|r| r.extract(&columns)).collect();
    (columns, matrix)
}