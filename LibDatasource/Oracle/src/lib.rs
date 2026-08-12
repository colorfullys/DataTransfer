//! Oracle datasource plugin.
//!
//! Implemented with the `oracle` crate, which dynamically loads the Oracle
//! OCI client (`libclntsh`) at runtime. The host process therefore needs the
//! Oracle Instant Client installed and `LD_LIBRARY_PATH`/`PATH` configured,
//! but never compiles against it.
//!
//! There is no connection pool; a single connection is shared behind the
//! plugin mutex. Create several plugin instances to scale writers.

use libdatasource::datasource::Datasource;
use libdatasource::error::{DatasourceError, Result};
use libdatasource::model::{
    Column, ConnectionConfig, Row, SyncMode, TableSchema, Value,
};
use oracle::sql_type::OracleType;
use oracle::{Connection, Row as OracleRow};

#[derive(Default)]
pub struct OracleDatasource {
    conn: Option<Connection>,
    cfg: Option<ConnectionConfig>,
}

impl OracleDatasource {
    fn conn(&self) -> Result<&Connection> {
        self.conn
            .as_ref()
            .ok_or_else(|| DatasourceError::Connection("oracle datasource not connected".into()))
    }

    fn quote_ident(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

impl Datasource for OracleDatasource {
    fn name(&self) -> &'static str {
        "oracle"
    }

    fn connect(&mut self, cfg: &ConnectionConfig) -> Result<()> {
        if cfg.conn_type != "oracle" {
            return Err(DatasourceError::Connection(format!(
                "plugin 'oracle' cannot open connection type '{}'",
                cfg.conn_type
            )));
        }

        let connect_string = format!("//{}:{}/{}", cfg.host, cfg.port, cfg.database);
        let conn = oracle::Connection::connect(&cfg.username, &cfg.password, &connect_string)
            .map_err(|e| {
                DatasourceError::Connection(format!(
                    "oracle connect to {} failed: {e}",
                    connect_string
                ))
            })?;

        if let Some(schema) = &cfg.schema {
            let sql = format!("ALTER SESSION SET CURRENT_SCHEMA = {}", self.quote_ident(schema));
            conn.execute(&sql, &[]).map_err(DatasourceError::db)?;
        }
        let _: i64 = conn
            .query_row_as("SELECT 1 FROM DUAL", &[])
            .map_err(DatasourceError::db)?;

        self.conn = Some(conn);
        self.cfg = Some(cfg.clone());
        log::info!("oracle connected to {}:{}", cfg.host, cfg.port);
        Ok(())
    }

    fn get_schema(&self, table: &str) -> Result<TableSchema> {
        let conn = self.conn()?;
        let cols_sql = concat!(
            "SELECT column_name, data_type, nullable, column_id ",
            "FROM all_tab_columns ",
            "WHERE owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA') AND table_name = :1 ",
            "ORDER BY column_id"
        );
        let rows = conn.query(cols_sql, &[&table]).map_err(DatasourceError::db)?;

        let pk_sql = concat!(
            "SELECT cc.column_name ",
            "FROM all_constraints c ",
            "JOIN all_cons_columns cc ",
            "  ON cc.constraint_name = c.constraint_name AND cc.owner = c.owner ",
            "WHERE c.constraint_type = 'P' ",
            "  AND c.owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA') ",
            "  AND c.table_name = :1"
        );
        let pk_rows = conn.query(pk_sql, &[&table]).map_err(DatasourceError::db)?;
        let mut pks = Vec::new();
        for r in pk_rows {
            let row = r.map_err(DatasourceError::db)?;
            pks.push(row.get::<usize, String>(0).map_err(DatasourceError::db)?);
        }

        let mut columns = Vec::new();
        for (ordinal, row) in rows.enumerate() {
            let row = row.map_err(DatasourceError::db)?;
            let name: String = row.get(0).map_err(DatasourceError::db)?;
            let data_type: String = row.get(1).map_err(DatasourceError::db)?;
            let nullable: String = row.get(2).map_err(DatasourceError::db)?;
            columns.push(Column {
                name: name.clone(),
                data_type,
                nullable: nullable == "Y",
                is_primary: pks.contains(&name),
                ordinal,
            });
        }

        if columns.is_empty() {
            return Err(DatasourceError::Schema(format!(
                "table '{}' not found in schema",
                table
            )));
        }
        Ok(TableSchema {
            table: table.to_string(),
            columns,
        })
    }

    fn query(&self, sql: &str) -> Result<Vec<Row>> {
        let conn = self.conn()?;
        let rows = conn.query(sql, &[]).map_err(DatasourceError::db)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(oracle_row(row.map_err(DatasourceError::db)?));
        }
        Ok(out)
    }

        fn query_page(&self, sql: &str, offset: u64, limit: u64) -> Result<Vec<Row>> {
        let paged = format!(
            "{} OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
            trim_sql(sql),
            offset,
            limit
        );
        self.query(&paged)
    }

    fn query_params(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>> {
        let conn = self.conn()?;
        let boxed: Vec<Box<dyn oracle::sql_type::ToSql>> =
            params.iter().map(value_to_sql).collect();
        let refs: Vec<&dyn oracle::sql_type::ToSql> =
            boxed.iter().map(|b| b.as_ref()).collect();
        let rows = conn.query(sql, &refs).map_err(DatasourceError::db)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(oracle_row(row.map_err(DatasourceError::db)?));
        }
        Ok(out)
    }

    fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        let conn = self.conn()?;
        let boxed: Vec<Box<dyn oracle::sql_type::ToSql>> =
            params.iter().map(value_to_sql).collect();
        let refs: Vec<&dyn oracle::sql_type::ToSql> =
            boxed.iter().map(|b| b.as_ref()).collect();
        let stmt = conn.execute(sql, &refs).map_err(DatasourceError::db)?;
        stmt.row_count().map_err(DatasourceError::db)
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

        let conn = self.conn()?;
        let quoted_cols = columns
            .iter()
            .map(|c| self.quote_ident(c))
            .collect::<Vec<_>>();

        let mut bind = 1usize;
        let mut params: Vec<Box<dyn oracle::sql_type::ToSql>> =
            Vec::with_capacity(rows.len() * columns.len());
        for row in rows.iter() {
            if row.len() != columns.len() {
                return Err(DatasourceError::InvalidArgument(format!(
                    "row has {} values, expected {}",
                    row.len(),
                    columns.len()
                )));
            }
            for v in row {
                params.push(value_to_sql(v));
            }
        }

        let sql = if mode == SyncMode::Upsert && !pk_columns.is_empty() {
            // MERGE upsert: one sub-select row per input row.
            let mut using = String::new();
            for ri in 0..rows.len() {
                if ri > 0 {
                    using.push_str(" UNION ALL ");
                }
                using.push_str("SELECT ");
                for (i, c) in quoted_cols.iter().enumerate() {
                    if i > 0 {
                        using.push(',');
                    }
                    using.push_str(&format!(":{} AS {}", bind, c));
                    bind += 1;
                }
                using.push_str(" FROM DUAL");
            }

            let on_clause = pk_columns
                .iter()
                .map(|pk| {
                    format!(
                        "tgt.{} = src.{}",
                        self.quote_ident(pk),
                        self.quote_ident(pk)
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND ");

            let update_cols: Vec<&String> = columns
                .iter()
                .filter(|c| !pk_columns.contains(c))
                .collect();
            let insert_cols = quoted_cols.join(", ");
            let insert_vals = quoted_cols
                .iter()
                .map(|c| format!("src.{}", c))
                .collect::<Vec<_>>()
                .join(", ");

            if update_cols.is_empty() {
                format!(
                    "MERGE INTO {} tgt USING ({}) src ON ({}) WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
                    self.quote_ident(table),
                    using,
                    on_clause,
                    insert_cols,
                    insert_vals,
                )
            } else {
                let set_clause = update_cols
                    .iter()
                    .map(|c| {
                        format!("tgt.{} = src.{}", self.quote_ident(c), self.quote_ident(c))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "MERGE INTO {} tgt USING ({}) src ON ({}) WHEN MATCHED THEN UPDATE SET {} WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
                    self.quote_ident(table),
                    using,
                    on_clause,
                    set_clause,
                    insert_cols,
                    insert_vals,
                )
            }
        } else {
            // Append / Full (truncate is handled by the caller): INSERT ALL.
            let mut buf = String::from("INSERT ALL");
            for _ in rows {
                buf.push_str(&format!(
                    " INTO {} ({}) VALUES (",
                    self.quote_ident(table),
                    quoted_cols.join(", ")
                ));
                for i in 0..columns.len() {
                    if i > 0 {
                        buf.push(',');
                    }
                    buf.push_str(&format!(":{}", bind));
                    bind += 1;
                }
                buf.push(')');
            }
            buf.push_str(" SELECT 1 FROM DUAL");
            buf
        };

        let refs: Vec<&dyn oracle::sql_type::ToSql> =
            params.iter().map(|b| b.as_ref()).collect();
        let stmt = conn.execute(&sql, &refs).map_err(DatasourceError::db)?;
        let affected = stmt.row_count().map_err(DatasourceError::db)?;
        Ok(affected.max(rows.len() as u64))
    }

    fn truncate(&self, table: &str) -> Result<()> {
        let sql = format!("TRUNCATE TABLE {}", self.quote_ident(table));
        let conn = self.conn()?;
        conn.execute(&sql, &[]).map_err(DatasourceError::db)?;
        Ok(())
    }

    fn ping(&self) -> Result<()> {
        let conn = self.conn()?;
        let _: i64 = conn
            .query_row_as("SELECT 1 FROM DUAL", &[])
            .map_err(DatasourceError::db)?;
        Ok(())
    }
}

fn trim_sql(sql: &str) -> &str {
    sql.trim().trim_end_matches(';')
}

fn oracle_row(row: OracleRow) -> Row {
    let mut out = Row::new();
    for (idx, info) in row.column_info().iter().enumerate() {
        out.insert(info.name().to_string(), oracle_value(&row, idx, info.oracle_type()));
    }
    out
}

fn oracle_value(row: &OracleRow, idx: usize, ty: &OracleType) -> Value {
    let null = row
        .get::<usize, Option<String>>(idx)
        .map(|v| v.is_none())
        .unwrap_or(false);
    if null {
        return Value::Null;
    }

    match ty {
        OracleType::Number(_p, s) if *s == 0 && *(_p) > 0 && *_p <= 18 => {
            match row.get::<usize, i64>(idx) {
                Ok(v) => Value::Int(v),
                Err(_) => row
                    .get::<usize, String>(idx)
                    .map(Value::Decimal)
                    .unwrap_or(Value::Null),
            }
        }
        OracleType::Number(..) => row
            .get::<usize, String>(idx)
            .map(Value::Decimal)
            .unwrap_or(Value::Null),
        OracleType::BinaryFloat | OracleType::BinaryDouble => row
            .get::<usize, f64>(idx)
            .map(Value::Float)
            .unwrap_or(Value::Null),
        OracleType::Date | OracleType::Timestamp(_) => row
            .get::<usize, chrono::NaiveDateTime>(idx)
            .map(|d| Value::Date(d.format("%Y-%m-%d %H:%M:%S%.6f").to_string()))
            .unwrap_or(Value::Null),
        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => row
            .get::<usize, oracle::sql_type::Timestamp>(idx)
            .map(|ts| Value::Date(ts.to_string()))
            .unwrap_or(Value::Null),
        OracleType::Raw(_) | OracleType::BLOB | OracleType::BFILE => row
            .get::<usize, Vec<u8>>(idx)
            .map(Value::Bytes)
            .unwrap_or(Value::Null),
        OracleType::Boolean => row
            .get::<usize, bool>(idx)
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        OracleType::CLOB | OracleType::NCLOB | OracleType::Long => row
            .get::<usize, String>(idx)
            .map(Value::String)
            .unwrap_or(Value::Null),
        _ => row
            .get::<usize, String>(idx)
            .map(Value::String)
            .unwrap_or(Value::Null),
    }
}

fn value_to_sql(v: &Value) -> Box<dyn oracle::sql_type::ToSql> {
    match v {
        Value::Null => Box::new(None::<String>),
        // Oracle < 23c has no BOOLEAN bind type; use 0/1 numbers.
        Value::Bool(b) => Box::new(if *b { 1i64 } else { 0i64 }),
        Value::Int(i) => Box::new(*i),
        Value::UInt(u) => Box::new(*u as i64),
        Value::Float(f) => Box::new(*f),
        // Decimal/Date are transferred as text; Oracle coerces them to the
        // target column type based on the statement context.
        Value::Decimal(s) | Value::String(s) | Value::Date(s) => Box::new(s.clone()),
        Value::Bytes(b) => Box::new(b.clone()),
    }
}

libdatasource::export_datasource_plugin!(OracleDatasource, "oracle");
