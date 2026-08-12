//! Host side of the ETL framework.
//!
//! * [`AppLookup`] implements [`libetl::TableLookup`] by running parameterised
//!   queries through the loaded datasource plugins, so ETL processors can read
//!   any configured table.
//! * `etl_lookup_cb` is the C function the DataTransfer binary hands to
//!   dynamic ETL plugins (they forward lookup requests back over this ABI).
//! * [`EtlPipeline`] runs a job's ordered list of built-in + plugin steps.

use std::ffi::{c_char, c_void, CStr, CString};
use std::sync::Arc;

use libdatasource::model::{Row, Value};
use libetl::model::{EtlOutputRow, EtlRow};
use libetl::registry::EtlRegistry;
use libetl::trait_def::{EtlContext, EtlProcessor, TableLookup};

use crate::config::{EtlStepKind, JobConfig};
use crate::connections::ConnectionManager;
use crate::error::{AppError, AppResult};

/// `TableLookup` used by every ETL processor in this process.
pub struct AppLookup {
    pub conns: Arc<ConnectionManager>,
}

impl AppLookup {
    fn quote_ident(&self, conn: &str, name: &str) -> AppResult<String> {
        let ty = self.conns.conn_type(conn)?;
        if ty == "mysql" {
            Ok(format!("`{}`", name.replace('`', "``")))
        } else {
            Ok(format!("\"{}\"", name.replace('"', "\"\"")))
        }
    }

    fn limit_clause(&self, conn: &str, limit: u64) -> AppResult<String> {
        let ty = self.conns.conn_type(conn)?;
        if ty == "oracle" {
            Ok(format!("FETCH FIRST {limit} ROWS ONLY"))
        } else {
            Ok(format!("LIMIT {limit}"))
        }
    }

    fn do_lookup(
        &self,
        connection: &str,
        table: &str,
        columns: Option<&[String]>,
        where_clause: Option<&str>,
        params: &[Value],
        limit: u64,
    ) -> libetl::Result<Vec<Row>> {
        let ds = self
            .conns
            .get(connection)
            .map_err(|e| libetl::EtlError::Lookup(e.to_string()))?;
        let mut sql = String::from("SELECT ");
        match columns {
            Some(cols) if !cols.is_empty() => {
                let mut parts = Vec::new();
                for c in cols {
                    parts.push(self.quote_ident(connection, c).map_err(lookup_err)?);
                }
                sql.push_str(&parts.join(", "));
            }
            _ => sql.push('*'),
        }
        sql.push_str(" FROM ");
        sql.push_str(&self.quote_ident(connection, table).map_err(lookup_err)?);
        if let Some(w) = where_clause {
            if !w.trim().is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(w.trim());
            }
        }
        sql.push(' ');
        sql.push_str(&self.limit_clause(connection, limit).map_err(lookup_err)?);
        ds.query_params(&sql, params)
            .map_err(|e| libetl::EtlError::Lookup(e.to_string()))
    }
}

fn lookup_err(e: AppError) -> libetl::EtlError {
    libetl::EtlError::Lookup(e.to_string())
}

impl TableLookup for AppLookup {
    fn lookup(
        &self,
        connection: &str,
        table: &str,
        columns: Option<&[String]>,
        where_clause: Option<&str>,
        params: &[Value],
        limit: u64,
    ) -> libetl::Result<Vec<Row>> {
        self.do_lookup(connection, table, columns, where_clause, params, limit)
    }
}

/// C entry point handed to dynamic ETL plugins. `ctx` is a `*const AppLookup`.
pub unsafe extern "C" fn etl_lookup_cb(
    ctx: *mut c_void,
    conn: *const c_char,
    table: *const c_char,
    columns: *const c_char,
    where_clause: *const c_char,
    params: *const c_char,
    limit: u64,
    out: *mut *const c_char,
) -> i32 {
    let cstr = |p: *const c_char| -> Option<String> {
        if p.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned())
        }
    };
    let rc = (|| -> libetl::Result<()> {
        let lookup = unsafe { &*(ctx as *const AppLookup) };
        let conn_s = cstr(conn).ok_or_else(|| libetl::EtlError::Lookup("NULL conn".into()))?;
        let table_s = cstr(table).ok_or_else(|| libetl::EtlError::Lookup("NULL table".into()))?;
        let cols_s = cstr(columns).unwrap_or_else(|| "null".into());
        let where_s = cstr(where_clause);
        let params_s = cstr(params).unwrap_or_else(|| "[]".into());

        let cols: Option<Vec<String>> = if cols_s == "null" || cols_s == "NULL" {
            None
        } else {
            Some(serde_json::from_str(&cols_s).map_err(|e| {
                libetl::EtlError::Lookup(format!("bad columns: {e}"))
            })?)
        };
        let where_opt: Option<&str> = where_s.as_deref();
        let params: Vec<Value> = serde_json::from_str(&params_s)
            .map_err(|e| libetl::EtlError::Lookup(format!("bad params: {e}")))?;

        let rows = lookup.do_lookup(&conn_s, &table_s, cols.as_deref(), where_opt, &params, limit)?;
        let json = serde_json::to_string(&rows)
            .map_err(|e| libetl::EtlError::Serialize(e.to_string()))?;
        let c = CString::new(json)
            .map_err(|e| libetl::EtlError::Serialize(e.to_string()))?;
        unsafe { *out = c.into_raw() };
        Ok(())
    })();

    match rc {
        Ok(()) => 0,
        Err(e) => {
            let msg = serde_json::json!({ "error": e.to_string() }).to_string();
            if let Ok(c) = CString::new(msg) {
                unsafe { *out = c.into_raw() };
            }
            1
        }
    }
}

/// A job's ordered ETL pipeline. Stateless between rows.
pub struct EtlPipeline {
    steps: Vec<Arc<dyn EtlProcessor>>,
}

impl EtlPipeline {
    /// Build the pipeline for a job. Plugin steps are instantiated once and
    /// shared across all rows of every run.
    pub fn build(
        job: &JobConfig,
        etl_registry: &EtlRegistry,
        lookup: &Arc<AppLookup>,
    ) -> AppResult<EtlPipeline> {
        let mut steps = Vec::new();
        for (step, config) in job.etl.iter().zip(job.etl_configs.iter()) {
            match step.kind {
                EtlStepKind::Builtin => {
                    let proc: Arc<dyn EtlProcessor> = match step.name.as_str() {
                        "rename" => {
                            let inner = serde_json::from_value::<libetl::builtin::RenameConfig>(config.clone())
                                .map_err(|e| AppError::Config(format!("job '{}' rename: {e}", job.name)))?;
                            Arc::new(libetl::builtin::RenameProcessor { rename: inner })
                        }
                        "set" => {
                            let inner = serde_json::from_value::<libetl::builtin::SetConfig>(config.clone())
                                .map_err(|e| AppError::Config(format!("job '{}' set: {e}", job.name)))?;
                            Arc::new(libetl::builtin::SetProcessor { set: inner })
                        }
                        "filter" => {
                            let inner = serde_json::from_value::<libetl::builtin::FilterConfig>(config.clone())
                                .map_err(|e| AppError::Config(format!("job '{}' filter: {e}", job.name)))?;
                            Arc::new(libetl::builtin::FilterProcessor { filter: inner })
                        }
                        "cast" => {
                            let inner = serde_json::from_value::<libetl::builtin::CastConfig>(config.clone())
                                .map_err(|e| AppError::Config(format!("job '{}' cast: {e}", job.name)))?;
                            Arc::new(libetl::builtin::CastProcessor { cast: inner })
                        }
                        other => {
                            return Err(AppError::Config(format!(
                                "job '{}': unknown builtin etl step '{other}'",
                                job.name
                            )))
                        }
                    };
                    steps.push(proc);
                }
                EtlStepKind::Plugin => {
                    let handle = etl_registry
                        .get(&step.name)
                        .map_err(|e| AppError::Config(format!("job '{}': {e}", job.name)))?;
                    let ctx_ptr = Arc::as_ptr(lookup) as *mut c_void;
                    let proc = handle
                        .instantiate(config, etl_lookup_cb, ctx_ptr)
                        .map_err(|e| AppError::Etl(format!("job '{}': {e}", job.name)))?;
                    steps.push(proc);
                }
            }
        }
        Ok(EtlPipeline { steps })
    }

    /// Run every step on one row, chaining outputs into the next step. An
    /// empty pipeline returns the input row untouched.
    pub fn run(
        &self,
        input: &EtlRow,
        lookup: &dyn TableLookup,
        default_table: &str,
    ) -> AppResult<Vec<EtlOutputRow>> {
        let mut current = vec![EtlOutputRow::new(default_table, input.row.clone())];
        for step in &self.steps {
            let mut next = Vec::new();
            for out in &current {
                let row = EtlRow {
                    source_connection: input.source_connection.clone(),
                    source_table: input.source_table.clone(),
                    target_connections: input.target_connections.clone(),
                    row: out.row.clone(),
                    source_schema: input.source_schema.clone(),
                    target_schema: input.target_schema.clone(),
                };
                let mut ctx = EtlContext::from_row(&row, lookup);
                next.extend(
                    step.process(&mut ctx, &row)
                        .map_err(|e| AppError::Etl(format!("etl step '{}': {e}", step.name())))?,
                );
            }
            current = next;
        }
        Ok(current)
    }
}