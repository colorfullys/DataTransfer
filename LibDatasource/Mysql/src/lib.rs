//! MySQL datasource plugin.
//!
//! Implemented with the pure-Rust `mysql` crate. DataTransfer never depends on
//! this driver; everything crosses the plugin boundary through the
//! `libdatasource` C ABI.

use std::time::Duration;

use libdatasource::datasource::Datasource;
use libdatasource::error::{DatasourceError, Result};
use libdatasource::model::{
    Column, ConnectionConfig, Row, SyncMode, TableSchema, Value,
};
use mysql::prelude::Queryable;
use mysql::{OptsBuilder, Pool, PoolConstraints, PoolOpts, Row as MysqlRow};

#[derive(Default)]
pub struct MysqlDatasource {
    pool: Option<Pool>,
    cfg: Option<ConnectionConfig>,
}

impl MysqlDatasource {
    fn pool(&self) -> Result<&Pool> {
        self.pool
            .as_ref()
            .ok_or_else(|| DatasourceError::Connection("mysql datasource not connected".into()))
    }

    fn quote_ident(&self, name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }
}

impl Datasource for MysqlDatasource {
    fn name(&self) -> &'static str {
        "mysql"
    }

    fn connect(&mut self, cfg: &ConnectionConfig) -> Result<()> {
        if cfg.conn_type != "mysql" {
            return Err(DatasourceError::Connection(format!(
                "plugin 'mysql' cannot open connection type '{}'",
                cfg.conn_type
            )));
        }

        let timeout = Duration::from_secs(cfg.connect_timeout_secs.max(1));
        let mut opts = OptsBuilder::new()
            .ip_or_hostname(Some(cfg.host.clone()))
            .tcp_port(cfg.port)
            .db_name(Some(cfg.database.clone()))
            .user(Some(cfg.username.clone()))
            .pass(Some(cfg.password.clone()))
            .tcp_connect_timeout(Some(timeout))
            .read_timeout(Some(Duration::from_secs(600)))
            .write_timeout(Some(Duration::from_secs(600)));

        let constraints = PoolConstraints::new(0, cfg.max_pool_size.max(1) as usize)
            .unwrap_or(PoolConstraints::DEFAULT);
        opts = opts.pool_opts(PoolOpts::new().with_constraints(constraints));

        if let Some(charset) = cfg.param("charset") {
            // `SET NAMES` on every new connection; charset names are whitelisted
            // so they can never be used for SQL injection.
            let name = match charset.to_ascii_lowercase().as_str() {
                "utf8" | "utf8mb4" => "utf8mb4",
                "latin1" => "latin1",
                "gbk" => "gbk",
                "big5" => "big5",
                other => {
                    return Err(DatasourceError::InvalidArgument(format!(
                        "unsupported mysql charset '{other}'"
                    )))
                }
            };
            opts = opts.init(vec![format!("SET NAMES {name}")]);
        }
        if let Some(ssl_mode) = cfg.param("ssl_mode") {
            match ssl_mode.to_ascii_lowercase().as_str() {
                "require" | "required" | "verify_ca" | "verify_identity" => {
                    opts = opts.ssl_opts(mysql::SslOpts::default());
                }
                _ => {}
            }
        }

        let pool = Pool::new(opts).map_err(DatasourceError::conn)?;
        // Fail fast: verify the connection works.
        let mut conn = pool.get_conn().map_err(DatasourceError::conn)?;
        let _: Option<u8> = conn.query_first("SELECT 1").map_err(DatasourceError::conn)?;
        drop(conn);

        self.pool = Some(pool);
        self.cfg = Some(cfg.clone());
        log::info!("mysql connected to {}:{}", cfg.host, cfg.port);
        Ok(())
    }

    fn get_schema(&self, table: &str) -> Result<TableSchema> {
        let pool = self.pool()?;
        let mut conn = pool.get_conn().map_err(DatasourceError::conn)?;

        let sql = concat!(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, ORDINAL_POSITION ",
            "FROM information_schema.COLUMNS ",
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ",
            "ORDER BY ORDINAL_POSITION"
        );
        let cols: Vec<MysqlRow> = conn
            .exec(sql, (table,))
            .map_err(DatasourceError::conn)?;

        let columns: Vec<Column> = cols
            .into_iter()
            .map(|r| {
                let name: String = r.get::<String, usize>(0).unwrap_or_default();
                let data_type: String = r.get::<String, usize>(1).unwrap_or_default();
                let nullable: String = r.get::<String, usize>(2).unwrap_or_default();
                let col_key: String = r.get::<String, usize>(3).unwrap_or_default();
                let ordinal: usize = r.get::<usize, usize>(4).unwrap_or(0);
                Column {
                    name,
                    data_type,
                    nullable: nullable == "YES",
                    is_primary: col_key == "PRI",
                    ordinal,
                }
            })
            .collect();

        if columns.is_empty() {
            return Err(DatasourceError::Schema(format!(
                "table '{}' not found in database",
                table
            )));
        }
        Ok(TableSchema {
            table: table.to_string(),
            columns,
        })
    }

    fn query(&self, sql: &str) -> Result<Vec<Row>> {
        let pool = self.pool()?;
        let mut conn = pool.get_conn().map_err(DatasourceError::conn)?;
        let rows = conn
            .query(sql)
            .map_err(DatasourceError::conn)?;
        Ok(rows.into_iter().map(row_from_mysql).collect())
    }

    fn query_page(&self, sql: &str, offset: u64, limit: u64) -> Result<Vec<Row>> {
        let paged = format!("{} LIMIT {} OFFSET {}", trim_sql(sql), limit, offset);
        self.query(&paged)
    }

    fn query_params(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>> {
        let pool = self.pool()?;
        let mut conn = pool.get_conn().map_err(DatasourceError::conn)?;
        let params: Vec<mysql::Value> = params.iter().map(value_to_mysql).collect();
        let rows: Vec<MysqlRow> = conn
            .exec(sql, mysql::Params::from(params))
            .map_err(DatasourceError::conn)?;
        Ok(rows.into_iter().map(row_from_mysql).collect())
    }

    fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        let pool = self.pool()?;
        let mut conn = pool.get_conn().map_err(DatasourceError::conn)?;
        let params: Vec<mysql::Value> = params.iter().map(value_to_mysql).collect();
        conn.exec_drop(sql, mysql::Params::from(params))
            .map_err(DatasourceError::conn)?;
        // The MySQL driver does not surface the affected row count on DML;
        // callers treat the affected count as informational.
        Ok(0)
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
        let mut params: Vec<mysql::Value> = Vec::with_capacity(rows.len() * columns.len());
        for (i, row) in rows.iter().enumerate() {
            if row.len() != columns.len() {
                return Err(DatasourceError::InvalidArgument(format!(
                    "row {} has {} values, expected {}",
                    i,
                    row.len(),
                    columns.len()
                )));
            }
            if i > 0 {
                placeholders.push(',');
            }
            placeholders.push('(');
            placeholders.push_str(&vec!["?"; columns.len()].join(","));
            placeholders.push(')');
            for v in row {
                params.push(value_to_mysql(v));
            }
        }

        let mut sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.quote_ident(table),
            col_list,
            placeholders
        );

        if mode == SyncMode::Upsert {
            let update_cols: Vec<String> = columns
                .iter()
                .filter(|c| !pk_columns.contains(c))
                .cloned()
                .collect();
            if !update_cols.is_empty() {
                let updates = update_cols
                    .iter()
                    .map(|c| format!("{} = VALUES({})", self.quote_ident(c), self.quote_ident(c)))
                    .collect::<Vec<_>>()
                    .join(", ");
                sql.push_str(&format!(" ON DUPLICATE KEY UPDATE {}", updates));
            }
        }

        let mut conn = self.pool()?.get_conn().map_err(DatasourceError::conn)?;
        conn.exec_drop(&sql, mysql::Params::from(params))
            .map_err(DatasourceError::conn)?;
        Ok(rows.len() as u64)
    }

    fn truncate(&self, table: &str) -> Result<()> {
        let sql = format!("TRUNCATE TABLE {}", self.quote_ident(table));
        let mut conn = self.pool()?.get_conn().map_err(DatasourceError::conn)?;
        conn.query_drop(&sql).map_err(DatasourceError::conn)?;
        Ok(())
    }

    fn ping(&self) -> Result<()> {
        let pool = self.pool()?;
        let mut conn = pool.get_conn().map_err(DatasourceError::conn)?;
        let _: Option<u8> = conn.query_first("SELECT 1").map_err(DatasourceError::conn)?;
        Ok(())
    }
}

fn trim_sql(sql: &str) -> &str {
    sql.trim().trim_end_matches(';')
}

fn row_from_mysql(row: MysqlRow) -> Row {
    let names: Vec<String> = row
        .columns_ref()
        .iter()
        .map(|c| c.name_str().as_ref().to_string())
        .collect();
    let values = row.unwrap();
    let mut out = Row::new();
    for (name, val) in names.into_iter().zip(values) {
        out.insert(name, value_from_mysql(val));
    }
    out
}

fn value_from_mysql(v: mysql::Value) -> Value {
    use mysql::Value as M;
    match v {
        M::NULL => Value::Null,
        M::Bytes(b) => Value::Bytes(b),
        M::Int(i) => Value::Int(i),
        M::UInt(u) => Value::UInt(u),
        M::Float(f) => Value::Float(f as f64),
        M::Double(f) => Value::Float(f),
        M::Date(y, mo, d, h, mi, s, us) => Value::Date(format_date(y, mo, d, h, mi, s, us)),
        M::Time(_neg, days, h, mi, s, _us) => {
            let total_hours = days * 24 + u32::from(h);
            Value::String(format!("{:02}:{:02}:{:02}", total_hours, mi, s))
        }
    }
}

fn format_date(y: u16, mo: u8, d: u8, h: u8, mi: u8, s: u8, us: u32) -> String {
    if h == 0 && mi == 0 && s == 0 && us == 0 {
        format!("{:04}-{:02}-{:02}", y, mo, d)
    } else if us == 0 {
        format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, mo, d, h, mi, s)
    } else {
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
            y, mo, d, h, mi, s, us
        )
    }
}

fn value_to_mysql(v: &Value) -> mysql::Value {
    match v {
        Value::Null => mysql::Value::NULL,
        Value::Bool(b) => mysql::Value::Int(*b as i64),
        Value::Int(i) => mysql::Value::Int(*i),
        Value::UInt(u) => mysql::Value::UInt(*u),
        Value::Float(f) => mysql::Value::Double(*f),
        // Decimal/Date/Time are transferred as strings and coerced by the
        // server to the target column type; preserves precision exactly.
        Value::Decimal(s) | Value::String(s) | Value::Date(s) => {
            mysql::Value::Bytes(s.clone().into_bytes())
        }
        Value::Bytes(b) => mysql::Value::Bytes(b.clone()),
    }
}

libdatasource::export_datasource_plugin!(MysqlDatasource, "mysql");
