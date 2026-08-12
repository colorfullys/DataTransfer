//! PostgreSQL datasource plugin.
//!
//! Implemented with the synchronous `postgres` crate plus `r2d2` pooling.
//! DataTransfer never depends on this driver; everything crosses the plugin
//! boundary through the `libdatasource` C ABI.
//!
//! TLS: connections currently use `NoTls`. Setting `params.ssl_mode` to
//! `require`/`verify-full` returns an explicit "unsupported" error so it can
//! never silently degrade security.

use std::time::Duration;

use libdatasource::datasource::Datasource;
use libdatasource::error::{DatasourceError, Result};
use libdatasource::model::{
    Column, ConnectionConfig, Row, SyncMode, TableSchema, Value,
};
use postgres::types::{ToSql, Type};
use postgres::{Client, NoTls};
use r2d2_postgres::PostgresConnectionManager;

type PgMgr = PostgresConnectionManager<NoTls>;
type PgPool = r2d2::Pool<PgMgr>;

#[derive(Default)]
pub struct PostgresDatasource {
    pool: Option<PgPool>,
    cfg: Option<ConnectionConfig>,
}

impl PostgresDatasource {
    fn pool(&self) -> Result<&PgPool> {
        self.pool.as_ref().ok_or_else(|| {
            DatasourceError::Connection("postgres datasource not connected".into())
        })
    }

    fn with_client<R>(&self, f: impl FnOnce(&mut Client) -> Result<R>) -> Result<R> {
        let pool = self.pool()?;
        let mut conn = pool.get().map_err(DatasourceError::conn)?;
        f(&mut conn)
    }

    fn quote_ident(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

impl Datasource for PostgresDatasource {
    fn name(&self) -> &'static str {
        "postgresql"
    }

    fn connect(&mut self, cfg: &ConnectionConfig) -> Result<()> {
        if cfg.conn_type != "postgresql" {
            return Err(DatasourceError::Connection(format!(
                "plugin 'postgresql' cannot open connection type '{}'",
                cfg.conn_type
            )));
        }

        if let Some(ssl) = cfg.param("ssl_mode") {
            match ssl.to_ascii_lowercase().as_str() {
                "disable" | "prefer" | "allow" => {}
                other => {
                    return Err(DatasourceError::Unsupported(format!(
                        "postgres ssl_mode '{other}' requires a TLS connector which is not \
                         compiled into this plugin; use 'disable'/'prefer'"
                    )))
                }
            }
        }

        let mut pg = postgres::Config::new();
        let app_name = format!("DataTransfer/{}", cfg.name);
        pg.host(&cfg.host)
            .port(cfg.port)
            .user(&cfg.username)
            .password(&cfg.password)
            .dbname(&cfg.database)
            .connect_timeout(Duration::from_secs(cfg.connect_timeout_secs.max(1)))
            .application_name(&app_name);
        if let Some(schema) = &cfg.schema {
            let options = format!("-csearch_path={}", schema.replace('"', "\"\""));
            pg.options(&options);
        }

        let mgr = PostgresConnectionManager::new(pg, NoTls);
        let pool = r2d2::Pool::builder()
            .max_size(cfg.max_pool_size.max(1))
            .connection_timeout(Duration::from_secs(cfg.connect_timeout_secs.max(1)))
            .build(mgr)
            .map_err(DatasourceError::conn)?;

        // Fail fast: verify the connection works.
        let mut conn = pool.get().map_err(DatasourceError::conn)?;
        let _: i32 = conn.query_one("SELECT 1", &[]).map_err(DatasourceError::conn)?.get(0);
        drop(conn);

        self.pool = Some(pool);
        self.cfg = Some(cfg.clone());
        log::info!("postgres connected to {}:{}", cfg.host, cfg.port);
        Ok(())
    }

    fn get_schema(&self, table: &str) -> Result<TableSchema> {
        let sql = concat!(
            "SELECT c.column_name, c.data_type, c.is_nullable, c.ordinal_position, ",
            "       (kcu.column_name IS NOT NULL) AS is_primary ",
            "FROM information_schema.columns c ",
            "LEFT JOIN (",
            "  SELECT kcu.table_schema, kcu.table_name, kcu.column_name ",
            "  FROM information_schema.key_column_usage kcu ",
            "  JOIN information_schema.table_constraints tc ",
            "    ON tc.constraint_name = kcu.constraint_name ",
            "   AND tc.table_schema = kcu.table_schema ",
            "   AND tc.table_name = kcu.table_name ",
            "  WHERE tc.constraint_type = 'PRIMARY KEY'",
            ") kcu ",
            "  ON kcu.table_schema = c.table_schema ",
            " AND kcu.table_name = c.table_name ",
            " AND kcu.column_name = c.column_name ",
            "WHERE c.table_schema = COALESCE($1::text, current_schema()) ",
            "  AND c.table_name = $2 ",
            "ORDER BY c.ordinal_position",
        );
        let schema = self.cfg.as_ref().and_then(|c| c.schema.clone());

        self.with_client(|client| {
            let rows = client.query(sql, &[&schema, &table]).map_err(DatasourceError::conn)?;
            let columns: Vec<Column> = rows
                .iter()
                .map(|r| Column {
                    name: r.get(0),
                    data_type: r.get(1),
                    nullable: r.get::<_, String>(2) == "YES",
                    is_primary: r.get(4),
                    ordinal: r.get::<_, i32>(3) as usize,
                })
                .collect();
            if columns.is_empty() {
                return Err(DatasourceError::Schema(format!(
                    "table '{}' not found in schema '{}'",
                    table,
                    schema.as_deref().unwrap_or("default")
                )));
            }
            Ok(TableSchema {
                table: table.to_string(),
                columns,
            })
        })
    }

    fn query(&self, sql: &str) -> Result<Vec<Row>> {
        self.with_client(|client| {
            let rows = client.query(sql, &[]).map_err(DatasourceError::conn)?;
            Ok(rows.iter().map(pg_row).collect())
        })
    }

    fn query_page(&self, sql: &str, offset: u64, limit: u64) -> Result<Vec<Row>> {
        let paged = format!("{} LIMIT {} OFFSET {}", trim_sql(sql), limit, offset);
        self.query(&paged)
    }

    fn query_params(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>> {
        let boxed: Vec<Box<dyn ToSql + Sync>> = params.iter().map(value_to_sql).collect();
        let refs: Vec<&(dyn ToSql + Sync)> = boxed.iter().map(|b| b.as_ref()).collect();
        self.with_client(|client| {
            let rows = client.query(sql, &refs).map_err(DatasourceError::conn)?;
            Ok(rows.iter().map(pg_row).collect())
        })
    }

    fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        let boxed: Vec<Box<dyn ToSql + Sync>> = params.iter().map(value_to_sql).collect();
        let refs: Vec<&(dyn ToSql + Sync)> = boxed.iter().map(|b| b.as_ref()).collect();
        self.with_client(|client| client.execute(sql, &refs).map_err(DatasourceError::conn))
    }

    fn batch_insert(
        &self,
        table: &str,
        columns: &[String],
        rows: &[Vec<Value>],
        mode: SyncMode,
        pk_columns: &[String],
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        if columns.is_empty() {
            return Err(DatasourceError::InvalidArgument(
                "batch_insert requires at least one column".into(),
            ));
        }

        let col_list = columns
            .iter()
            .map(|c| self.quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");

        let mut placeholders = String::new();
        let mut all_params: Vec<Box<dyn ToSql + Sync>> = Vec::with_capacity(rows.len() * columns.len());
        let mut i = 1usize;
        for (row_idx, row) in rows.iter().enumerate() {
            if row.len() != columns.len() {
                return Err(DatasourceError::InvalidArgument(format!(
                    "row {} has {} values, expected {}",
                    row_idx,
                    row.len(),
                    columns.len()
                )));
            }
            if row_idx > 0 {
                placeholders.push(',');
            }
            placeholders.push('(');
            for (col_idx, v) in row.iter().enumerate() {
                if col_idx > 0 {
                    placeholders.push(',');
                }
                placeholders.push_str(&format!("${}", i));
                i += 1;
                all_params.push(value_to_sql(v));
            }
            placeholders.push(')');
        }

        let mut sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.quote_ident(table),
            col_list,
            placeholders
        );

        if mode == SyncMode::Upsert && !pk_columns.is_empty() {
            let pk_list = pk_columns
                .iter()
                .map(|c| self.quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ");
            let update_cols: Vec<&String> = columns
                .iter()
                .filter(|c| !pk_columns.contains(c))
                .collect();
            if !update_cols.is_empty() {
                let updates = update_cols
                    .iter()
                    .map(|c| format!("{} = EXCLUDED.{}", self.quote_ident(c), self.quote_ident(c)))
                    .collect::<Vec<_>>()
                    .join(", ");
                sql.push_str(&format!(
                    " ON CONFLICT ({}) DO UPDATE SET {}",
                    pk_list, updates
                ));
            } else {
                sql.push_str(&format!(
                    " ON CONFLICT ({}) DO NOTHING",
                    pk_list
                ));
            }
        }

        let params: Vec<&(dyn ToSql + Sync)> = all_params.iter().map(|b| b.as_ref()).collect();
        self.with_client(|client| {
            client.execute(&sql, &params).map_err(DatasourceError::conn)
        })
    }

    fn truncate(&self, table: &str) -> Result<()> {
        let sql = format!("TRUNCATE TABLE {}", self.quote_ident(table));
        self.with_client(|client| {
            client.batch_execute(&sql).map_err(DatasourceError::conn)?;
            Ok(())
        })
    }

    fn ping(&self) -> Result<()> {
        self.with_client(|client| {
            client.query_one("SELECT 1", &[]).map_err(DatasourceError::conn)?;
            Ok(())
        })
    }
}

fn trim_sql(sql: &str) -> &str {
    sql.trim().trim_end_matches(';')
}

fn pg_row(row: &postgres::Row) -> Row {
    let mut out = Row::new();
    for (idx, col) in row.columns().iter().enumerate() {
        out.insert(col.name().to_string(), pg_value(row, idx, col.type_()));
    }
    out
}

fn pg_value(row: &postgres::Row, idx: usize, ty: &Type) -> Value {
    fn opt<'a, T: postgres::types::FromSql<'a>>(row: &'a postgres::Row, idx: usize) -> Option<T> {
        row.try_get::<usize, Option<T>>(idx).unwrap_or(None)
    }

    if let Ok(None::<String>) = row.try_get::<usize, Option<String>>(idx) {
        return Value::Null;
    }

    match *ty {
        Type::BOOL => opt::<bool>(row, idx).map(Value::Bool).unwrap_or(Value::Null),
        Type::INT2 | Type::INT4 => opt::<i32>(row, idx).map(|v| Value::Int(v as i64)).unwrap_or(Value::Null),
        Type::INT8 => opt::<i64>(row, idx).map(Value::Int).unwrap_or(Value::Null),
        Type::FLOAT4 => opt::<f32>(row, idx).map(|v| Value::Float(v as f64)).unwrap_or(Value::Null),
        Type::FLOAT8 => opt::<f64>(row, idx).map(Value::Float).unwrap_or(Value::Null),
        Type::NUMERIC => opt::<String>(row, idx).map(Value::Decimal).unwrap_or(Value::Null),
        Type::BYTEA => opt::<Vec<u8>>(row, idx).map(Value::Bytes).unwrap_or(Value::Null),
        Type::DATE => opt::<chrono::NaiveDate>(row, idx)
            .map(|d| Value::Date(d.format("%Y-%m-%d").to_string()))
            .unwrap_or(Value::Null),
        Type::TIMESTAMP => opt::<chrono::NaiveDateTime>(row, idx)
            .map(|d| Value::Date(d.format("%Y-%m-%d %H:%M:%S%.6f").to_string()))
            .unwrap_or(Value::Null),
        Type::TIMESTAMPTZ => opt::<chrono::DateTime<chrono::Utc>>(row, idx)
            .map(|d| Value::Date(d.format("%Y-%m-%d %H:%M:%S%.6f").to_string()))
            .unwrap_or(Value::Null),
        Type::JSON | Type::JSONB => opt::<String>(row, idx).map(Value::String).unwrap_or(Value::Null),
        Type::UUID => opt::<String>(row, idx).map(Value::String).unwrap_or(Value::Null),
        _ => {
            // Fallback: read the textual representation.
            opt::<String>(row, idx).map(Value::String).unwrap_or(Value::Null)
        }
    }
}

fn value_to_sql(v: &Value) -> Box<dyn ToSql + Sync> {
    match v {
        Value::Null => Box::new(None::<String>),
        Value::Bool(b) => Box::new(*b),
        Value::Int(i) => Box::new(*i),
        Value::UInt(u) => Box::new(*u as i64),
        Value::Float(f) => Box::new(*f),
        // Decimal/Date are transferred as text; PostgreSQL coerces them to the
        // target column type, preserving precision.
        Value::Decimal(s) | Value::String(s) | Value::Date(s) => Box::new(s.clone()),
        Value::Bytes(b) => Box::new(b.clone()),
    }
}

libdatasource::export_datasource_plugin!(PostgresDatasource, "postgresql");
