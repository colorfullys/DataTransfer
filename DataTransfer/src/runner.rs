//! Executes a single job pass: read -> (map columns) -> ETL -> route -> write.

use std::collections::HashMap;
use std::sync::Arc;

use libdatasource::model::{Row, Value};
use libetl::model::EtlRow;

use crate::config::JobConfig;
use crate::connections::ConnectionManager;
use crate::error::AppResult;
use crate::etl::{AppLookup, EtlPipeline};
use crate::reader::{value_gt, Reader};
use crate::state::StateStore;
use crate::writer::Writer;

const FLUSH_THRESHOLD: usize = 500;

pub struct JobRunner {
    job: JobConfig,
    conns: Arc<ConnectionManager>,
    lookup: Arc<AppLookup>,
    etl: EtlPipeline,
    state: StateStore,
    retry: u32,
    page_size: u64,
    flush_threshold: usize,
}

impl JobRunner {
    pub fn build(
        job: JobConfig,
        conns: Arc<ConnectionManager>,
        lookup: Arc<AppLookup>,
        etl: EtlPipeline,
        state_dir: &std::path::Path,
        retry: u32,
        page_size: u64,
    ) -> AppResult<JobRunner> {
        let state = StateStore::load(state_dir, &job.name)?;
        Ok(JobRunner {
            job,
            conns,
            lookup,
            etl,
            state,
            retry,
            page_size: page_size.max(1),
            flush_threshold: FLUSH_THRESHOLD,
        })
    }

    /// Run the job once. Returns `(rows_written, rows_read)`.
    pub fn run_once(&mut self) -> AppResult<(u64, usize)> {
        let job = &self.job;
        let src_ds = self.conns.get(&job.source.connection)?;
        let tgt_ds = self.conns.get(&job.target.connection)?;
        let src_conn_type = self.conns.conn_type(&job.source.connection)?.to_string();

        let specs = job.state.clone();
        log::debug!(
            "job '{}': source pk {:?}", job.name, job.source.primary_key
        );
        let mut reader = Reader::new(
            src_ds,
            &src_conn_type,
            job.source.clone(),
            &specs,
            &self.state,
            self.page_size,
        )?;

        let mut writer = Writer::new(tgt_ds, job.mode, self.retry);
        let mut buckets: HashMap<String, Vec<Row>> = HashMap::new();
        let mut written: u64 = 0;
        let mut read_rows: usize = 0;

        while let Some(page) = reader.next_page()? {
            read_rows += page.len();

            update_state(&mut self.state, &page, &specs);

            for row in page {
                let base = map_columns(row, &job.source.columns, &job.target.columns);
                let etl_input = EtlRow {
                    source_connection: job.source.connection.clone(),
                    source_table: job.source.table.clone(),
                    target_connections: vec![job.target.connection.clone()],
                    row: base,
                    source_schema: reader.schema().cloned(),
                    target_schema: None,
                };
                let outputs = self.etl.run(&etl_input, &*self.lookup, &job.target.table)?;
                for out in outputs {
                    let table = if out.table.is_empty() {
                        job.target.table.clone()
                    } else {
                        out.table
                    };
                    let bucket = buckets.entry(table.clone()).or_default();
                    bucket.push(out.row);
                    if bucket.len() >= self.flush_threshold {
                        written += self.flush_bucket(&mut buckets, &table, &mut writer)?;
                    }
                }
            }
        }

        let tables: Vec<String> = buckets.keys().cloned().collect();
        for t in tables {
            written += self.flush_bucket(&mut buckets, &t, &mut writer)?;
        }

        self.state.save()?;
        log::info!(
            "job '{}' finished: read {} rows, wrote {} rows",
            job.name,
            read_rows,
            written
        );
        Ok((written, read_rows))
    }

    /// Advance each `max` state spec with the rows of the current page.
    fn flush_bucket(
        &self,
        buckets: &mut HashMap<String, Vec<Row>>,
        table: &str,
        writer: &mut Writer,
    ) -> AppResult<u64> {
        let rows = buckets.remove(table).unwrap_or_default();
        if rows.is_empty() {
            return Ok(0);
        }
        let pk = if table == self.job.target.table {
            self.job.target.primary_key.clone()
        } else {
            vec![]
        };
        writer.write_batch(table, &rows, &pk)
    }
}

/// Positional column mapping: `target.columns[i]` receives `source.columns[i]`.
/// A row keyed by source names is turned into one keyed by target names.
fn map_columns(row: Row, src_columns: &[String], tgt_columns: &[String]) -> Row {
    let mut out = Row::new();
    if src_columns.is_empty() && tgt_columns.is_empty() {
        return row;
    }
    if src_columns.is_empty() {
        // target list given, source is SELECT *: keep only target names
        for c in tgt_columns {
            out.insert(c.clone(), row.get(c).cloned().unwrap_or(Value::Null));
        }
        return out;
    }
    if tgt_columns.is_empty() {
        // only source list: keep those columns under the same names
        for c in src_columns {
            out.insert(c.clone(), row.get(c).cloned().unwrap_or(Value::Null));
        }
        return out;
    }
    let n = src_columns.len().min(tgt_columns.len());
    for i in 0..n {
        out.insert(
            tgt_columns[i].clone(),
            row.get(&src_columns[i]).cloned().unwrap_or(Value::Null),
        );
    }
    out
}

/// Advance each `max` state spec with the rows of the current page.
fn update_state(
    state: &mut StateStore,
    rows: &[Row],
    specs: &std::collections::BTreeMap<String, crate::config::StateSpec>,
) {
    for (key, spec) in specs {
        if spec.kind != "max" {
            log::warn!(
                "state key '{}' has unsupported type '{}' (only 'max'); ignoring",
                key,
                spec.kind
            );
            continue;
        }
        if spec.column.is_empty() {
            log::warn!("state key '{}' has no column; ignoring", key);
            continue;
        }
        let mut best: Option<&Value> = None;
        for r in rows {
            if let Some(v) = r.get(&spec.column) {
                if v.is_null() {
                    continue;
                }
                best = Some(match best {
                    None => v,
                    Some(b) => {
                        if value_gt(v, b) { v } else { b }
                    }
                });
            }
        }
        if let Some(v) = best {
            let should_update = match state.get(key) {
                None => true,
                Some(cur) => value_gt(v, cur),
            };
            if should_update {
                state.set(key.clone(), v.clone());
            }
        }
    }
}