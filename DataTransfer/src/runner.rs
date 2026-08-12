//! Executes a single job pass: read -> (map columns) -> ETL -> route -> write.
//!
//! `JobConfig::writers` controls parallelism. With more than one writer the
//! ETL output rows are routed to independent writer threads *after* the ETL
//! pipeline; each thread batches its own rows and flushes them through a
//! `Writer` sharing the target datasource (which uses a connection pool).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use libdatasource::model::{Row, Value};
use libetl::model::EtlRow;

use crate::config::JobConfig;
use crate::connections::ConnectionManager;
use crate::error::{AppError, AppResult};
use crate::etl::{AppLookup, EtlPipeline};
use crate::reader::{value_gt, Reader};
use crate::router::Router;
use crate::state::StateStore;
use crate::writer::Writer;

const FLUSH_THRESHOLD: usize = 500;
const WRITER_CHANNEL_CAPACITY: usize = 4096;

/// Destination of one ETL output row: the target table plus the row data.
type RoutedRow = (String, Row);
/// Bounded channel for shipping rows to one writer thread.
type RowSender = std::sync::mpsc::SyncSender<RoutedRow>;
type RowReceiver = std::sync::mpsc::Receiver<RoutedRow>;
type WriterHandle = std::thread::JoinHandle<AppResult<(u64, TableStats)>>;
/// Per-target-table row counts accumulated during one job run.
type TableStats = HashMap<String, u64>;

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
        let job_name = self.job.name.clone();
        let writers = self.job.writers.max(1);
        let src_ds = self.conns.get(&self.job.source.connection)?;
        let tgt_ds = self.conns.get(&self.job.target.connection)?;
        let src_conn_type = self.conns.conn_type(&self.job.source.connection)?.to_string();

        if let Some(pool) = self.conns.pool_size(&self.job.target.connection) {
            if writers > pool {
                log::warn!(
                    "job '{job_name}': {writers} writer(s) > target pool size {pool}; writers will serialise on the pool (raise max_pool_size to parallelise)"
                );
            }
        }

        let specs = self.job.state.clone();
        log::debug!(
            "job '{job_name}': source pk {:?}", self.job.source.primary_key
        );
        let mut reader = Reader::new(
            src_ds,
            &src_conn_type,
            self.job.source.clone(),
            &specs,
            &self.state,
            self.page_size,
        )?;

        // Truncate-once coordination shared by every writer of this run.
        let truncate = Arc::new(Mutex::new(HashSet::new()));
        let mut counters = RunCounters::default();
        let started = std::time::Instant::now();

        let written = if writers == 1 {
            // Single writer survives on the job thread.
            let mut writer = Writer::new(tgt_ds, self.job.mode, self.retry, truncate, "main");
            let mut buckets: HashMap<String, Vec<Row>> = HashMap::new();
            let mut written =
                self.drain_single(&mut reader, &mut writer, &mut buckets, &mut counters)?;
            let tables: Vec<String> = buckets.keys().cloned().collect();
            for t in tables {
                written += self.flush_bucket(&mut buckets, &t, &mut writer, &mut counters)?;
            }
            written
        } else {
            self.drain_multi(&mut reader, tgt_ds, truncate, writers, &mut counters)?
        };

        self.state.save()?;
        let RunCounters {
            read_rows,
            pages,
            etl_in,
            etl_out,
            stats,
        } = &counters;
        let per_table = format_table_stats(stats);
        let mut etl_note = String::new();
        if !self.job.etl.is_empty() {
            etl_note = format!(" etl={etl_in}->{etl_out}");
        }
        let elapsed = started.elapsed();
        log::info!(
            "job '{job_name}' finished: read {read_rows} rows, wrote {written} rows in {elapsed:?} \
             | mode={} src={}@{} -> dst={}@{} writers={writers} pages={pages}{etl_note} writes=[{per_table}]",
            self.job.mode.as_str(),
            source_display(&self.job),
            self.job.source.connection,
            self.job.target.table,
            self.job.target.connection,
        );
        Ok((written, *read_rows))
    }

    /// Single-writer pass: read a page, run ETL, batch rows per target table.
    fn drain_single(
        &mut self,
        reader: &mut Reader,
        writer: &mut Writer,
        buckets: &mut HashMap<String, Vec<Row>>,
        counters: &mut RunCounters,
    ) -> AppResult<u64> {
        let mut written = 0u64;
        let specs = self.job.state.clone();
        while let Some(page) = reader.next_page()? {
            counters.pages += 1;
            counters.read_rows += page.len();
            update_state(&mut self.state, &page, &specs);
            for row in page {
                let outputs = self.run_etl(reader, row)?;
                counters.etl_in += 1;
                counters.etl_out += outputs.len() as u64;
                for out in outputs {
                    let table = target_table(&out.table, &self.job.target.table);
                    let bucket = buckets.entry(table.clone()).or_default();
                    bucket.push(out.row);
                    if bucket.len() >= self.flush_threshold {
                        written += self.flush_bucket(buckets, &table, writer, counters)?;
                    }
                }
            }
        }
        Ok(written)
    }

    /// Multi-writer pass: run ETL in this thread, route every output row to one
    /// of `writers` independent writer threads.
    fn drain_multi(
        &mut self,
        reader: &mut Reader,
        tgt_ds: Arc<dyn libdatasource::datasource::Datasource>,
        truncate: Arc<Mutex<HashSet<String>>>,
        writers: usize,
        counters: &mut RunCounters,
    ) -> AppResult<u64> {
        let router = Router::new(
            writers,
            self.job.target.table.clone(),
            self.job.target.primary_key.clone(),
        );
        let specs = self.job.state.clone();

        let (senders, handles) = self.spawn_writers(&tgt_ds, &truncate, writers)?;

        while let Some(page) = reader.next_page()? {
            counters.pages += 1;
            counters.read_rows += page.len();
            update_state(&mut self.state, &page, &specs);
            for row in page {
                let outputs = self.run_etl(reader, row)?;
                counters.etl_in += 1;
                counters.etl_out += outputs.len() as u64;
                for out in outputs {
                    let table = target_table(&out.table, &self.job.target.table);
                    let idx = router.route(&table, &out.row);
                    senders[idx]
                        .send((table, out.row))
                        .map_err(|_| AppError::Datasource("writer channel closed".into()))?;
                }
            }
        }
        drop(senders);

        let mut written = 0u64;
        for handle in handles {
            let (w, s) = handle
                .join()
                .map_err(|_| AppError::Datasource("writer thread panicked".into()))??;
            written += w;
            for (t, n) in s {
                *counters.stats.entry(t).or_default() += n;
            }
        }
        Ok(written)
    }

    /// Spawn `writers` worker threads, each owning one `Writer` and receiving
    /// `(table, row)` batches through its own bounded channel.
    fn spawn_writers(
        &self,
        tgt_ds: &Arc<dyn libdatasource::datasource::Datasource>,
        truncate: &Arc<Mutex<HashSet<String>>>,
        writers: usize,
    ) -> AppResult<(Vec<RowSender>, Vec<WriterHandle>)> {
        use std::sync::mpsc::sync_channel;

        let mut senders = Vec::with_capacity(writers);
        let mut handles = Vec::with_capacity(writers);
        let default_table = self.job.target.table.clone();
        let default_pk = self.job.target.primary_key.clone();

        for i in 0..writers {
            let (tx, rx) = sync_channel::<RoutedRow>(WRITER_CHANNEL_CAPACITY);
            let job_name = self.job.name.clone();
            let tag = format!("w{i}");
            log::info!("job '{job_name}': started writer {tag} of {writers}");
            let config = WriterConfig {
                ds: Arc::clone(tgt_ds),
                truncate: Arc::clone(truncate),
                mode: self.job.mode,
                retry: self.retry,
                default_table: default_table.clone(),
                default_pk: default_pk.clone(),
                tag: tag.clone(),
            };
            let handle = std::thread::Builder::new()
                .name(format!("{job_name}-writer-{i}"))
                .spawn(move || -> AppResult<(u64, TableStats)> { run_writer(config, rx) })
                .map_err(|e| AppError::Datasource(format!("cannot spawn writer thread: {e}")))?;
            senders.push(tx);
            handles.push(handle);
        }
        Ok((senders, handles))
    }

    /// Map source columns, then run the job's ETL pipeline for one source row.
    fn run_etl(
        &self,
        reader: &Reader,
        row: Row,
    ) -> AppResult<Vec<libetl::model::EtlOutputRow>> {
        let base = map_columns(row, &self.job.source.columns, &self.job.target.columns);
        let etl_input = EtlRow {
            source_connection: self.job.source.connection.clone(),
            source_table: self.job.source.table.clone(),
            target_connections: vec![self.job.target.connection.clone()],
            row: base,
            source_schema: reader.schema().cloned(),
            target_schema: None,
        };
        self.etl.run(&etl_input, &*self.lookup, &self.job.target.table)
    }

    /// Advance each `max` state spec with the rows of the current page.
    fn flush_bucket(
        &self,
        buckets: &mut HashMap<String, Vec<Row>>,
        table: &str,
        writer: &mut Writer,
        counters: &mut RunCounters,
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
        let n = writer.write_batch(table, &rows, &pk)?;
        *counters.stats.entry(table.to_string()).or_default() += n;
        Ok(n)
    }
}

/// Row/page/ETL counters accumulated during one job run, shown in the summary.
#[derive(Default)]
struct RunCounters {
    read_rows: usize,
    pages: u64,
    /// Source rows fed into the ETL pipeline.
    etl_in: u64,
    /// Rows produced by the ETL pipeline (after all steps).
    etl_out: u64,
    /// Per-target-table written row counts.
    stats: TableStats,
}

/// Everything a writer thread needs, beyond the rows it receives.
struct WriterConfig {
    ds: Arc<dyn libdatasource::datasource::Datasource>,
    truncate: Arc<Mutex<HashSet<String>>>,
    mode: libdatasource::model::SyncMode,
    retry: u32,
    default_table: String,
    default_pk: Vec<String>,
    tag: String,
}

/// Write loop for one parallel writer: accumulate rows per table, flush each
/// batch when it reaches the threshold, then flush everything on shutdown.
/// Returns the row count and a per-table breakdown for the run summary.
fn run_writer(config: WriterConfig, rx: RowReceiver) -> AppResult<(u64, TableStats)> {
    let WriterConfig {
        ds,
        truncate,
        mode,
        retry,
        default_table,
        default_pk,
        tag,
    } = config;
    let mut writer = Writer::new(ds, mode, retry, truncate, tag.clone());
    let mut buckets: HashMap<String, Vec<Row>> = HashMap::new();
    let mut stats: TableStats = HashMap::new();
    let mut written = 0u64;

    while let Ok((table, row)) = rx.recv() {
        let bucket = buckets.entry(table.clone()).or_default();
        bucket.push(row);
        if bucket.len() >= FLUSH_THRESHOLD {
            let pk = pk_for(&table, &default_table, &default_pk);
            let n = writer.write_batch(&table, bucket, &pk)?;
            written += n;
            *stats.entry(table.clone()).or_default() += n;
            buckets.remove(&table);
        }
    }

    let tables: Vec<String> = buckets.keys().cloned().collect();
    for t in tables {
        let rows = buckets.remove(&t).unwrap_or_default();
        if !rows.is_empty() {
            let pk = pk_for(&t, &default_table, &default_pk);
            let n = writer.write_batch(&t, &rows, &pk)?;
            written += n;
            *stats.entry(t.clone()).or_default() += n;
        }
    }
    log::info!(
        "writer[{tag}] done: wrote {written} row(s) into {}",
        format_table_stats(&stats)
    );
    Ok((written, stats))
}

/// Compact `table: n, ...` rendering for summaries.
fn format_table_stats(stats: &TableStats) -> String {
    if stats.is_empty() {
        return "none".to_string();
    }
    stats
        .iter()
        .map(|(t, n)| format!("{t}: {n}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Summary label for the source side (jobs without a `table` use `select`).
fn source_display(job: &JobConfig) -> &str {
    if job.source.table.is_empty() {
        "<custom-select>"
    } else {
        &job.source.table
    }
}

fn pk_for(table: &str, default_table: &str, default_pk: &[String]) -> Vec<String> {
    if table == default_table {
        default_pk.to_vec()
    } else {
        Vec::new()
    }
}

fn target_table(out_table: &str, job_table: &str) -> String {
    if out_table.is_empty() {
        job_table.to_string()
    } else {
        out_table.to_string()
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