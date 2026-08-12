//! # OrderSplitPlugin
//!
//! Splits one source row (an order header) into:
//!
//! 1. a **header row** containing only the configured `keep` columns, written
//!    to the job's default target table; and
//! 2. zero or more **detail rows** looked up from the source database through
//!    the host `TableLookup` callback, written to `detail_table`.
//!
//! Demonstrates the full LibETL plugin capabilities: `configure` for config,
//! cross-table `lookup_table` reads and one-to-many / multi-table output.
//!
//! ```yaml
//! etl:
//!   - plugin: order_split
//!     config:
//!       detail_table: order_item       # target detail table
//!       source_detail_table: t_order_item  # source table to read details from
//!       detail_columns: [item_id, product, qty, price]
//!       parent_key: order_id           # source column of the header
//!       child_key: order_id            # FK column on the detail source table
//!       keep: [order_id, user_id, amount]
//!       limit: 1000
//! ```

use std::sync::OnceLock;

use libdatasource::model::Row;
use libetl::error::{EtlError, Result};
use libetl::model::{EtlOutputRow, EtlRow};
use libetl::trait_def::{EtlConfigure, EtlContext, EtlProcessor};
use libetl::export_etl_plugin;
use serde::Deserialize;

/// Configuration deserialised from the job's `config:` block.
#[derive(Debug, Default, Deserialize)]
struct Config {
    /// Target detail table the detail rows are written to.
    detail_table: String,
    /// Source table the detail rows are read from (default: `detail_table`).
    #[serde(default)]
    source_detail_table: Option<String>,
    /// Columns read from the source detail table (default: all).
    #[serde(default)]
    detail_columns: Vec<String>,
    /// Source column that identifies the parent (header), e.g. `order_id`.
    parent_key: String,
    /// FK column on the detail source table (default: `parent_key`).
    #[serde(default)]
    child_key: Option<String>,
    /// Columns kept on the header row (default: all source columns).
    #[serde(default)]
    keep: Vec<String>,
    /// Maximum number of detail rows looked up per header (default: 1000).
    #[serde(default = "default_limit")]
    limit: u64,
}

const fn default_limit() -> u64 {
    1000
}

/// The plugin processor. A default instance is created by the host, then
/// `configure` is called with the JSON `config:` block.
#[derive(Default)]
pub struct OrderSplit {
    config: OnceLock<Config>,
}

impl EtlProcessor for OrderSplit {
    fn name(&self) -> &str {
        "order_split"
    }

    fn process(&self, ctx: &mut EtlContext, input: &EtlRow) -> Result<Vec<EtlOutputRow>> {
        let cfg = self
            .config
            .get()
            .ok_or_else(|| EtlError::Config("order_split: plugin not configured".into()))?;

        // --- header row: keep only the configured columns ---
        let header = keep_columns(&input.row, &cfg.keep);

        let mut out = vec![EtlOutputRow::new(String::new(), header)];

        // --- detail rows: look up the source table via the host callback ---
        let parent_value = match input.row.get(&cfg.parent_key) {
            Some(v) if !v.is_null() => v.clone(),
            _ => {
                log::warn!(
                    "order_split: row has no non-null '{}', emitting header only",
                    cfg.parent_key
                );
                return Ok(out);
            }
        };

        let child_key = cfg.child_key.as_deref().unwrap_or(&cfg.parent_key);
        let source_table = cfg
            .source_detail_table
            .as_deref()
            .unwrap_or(&cfg.detail_table);

        let detail_columns = if cfg.detail_columns.is_empty() {
            None
        } else {
            Some(cfg.detail_columns.as_slice())
        };
        let where_clause = format!("{child_key} = ?");

        let detail_rows = ctx.lookup_table(
            ctx.current_connection(),
            source_table,
            detail_columns,
            Some(&where_clause),
            std::slice::from_ref(&parent_value),
            cfg.limit,
        )?;

        for row in detail_rows {
            out.push(EtlOutputRow::new(cfg.detail_table.clone(), row));
        }
        Ok(out)
    }
}

impl EtlConfigure for OrderSplit {
    fn configure(&mut self, config: &serde_json::Value) -> Result<()> {
        let cfg: Config = serde_json::from_value(config.clone())
            .map_err(|e| EtlError::Config(format!("order_split: bad config: {e}")))?;
        if cfg.detail_table.trim().is_empty() {
            return Err(EtlError::Config(
                "order_split: config.detail_table is required".into(),
            ));
        }
        if cfg.parent_key.trim().is_empty() {
            return Err(EtlError::Config(
                "order_split: config.parent_key is required".into(),
            ));
        }
        log::info!(
            "order_split: configured (detail_table={}, source_detail_table={}, parent_key={}, child_key={}, keep={})",
            cfg.detail_table,
            cfg.source_detail_table.as_deref().unwrap_or(&cfg.detail_table),
            cfg.parent_key,
            cfg.child_key.as_deref().unwrap_or(&cfg.parent_key),
            if cfg.keep.is_empty() {
                "all".to_string()
            } else {
                cfg.keep.join(",")
            }
        );
        let _ = self.config.set(cfg);
        Ok(())
    }
}

/// Return a new row containing only `keep`; an empty `keep` keeps all columns.
fn keep_columns(row: &Row, keep: &[String]) -> Row {
    if keep.is_empty() {
        return row.clone();
    }
    let mut out = Row::new();
    for k in keep {
        if let Some(v) = row.get(k) {
            out.insert(k.clone(), v.clone());
        }
    }
    out
}

// Wire the generic C ABI glue to this processor. The exported symbol is
// `etl_get_api`; the plugin is registered under the name `order_split`.
export_etl_plugin!(OrderSplit, "order_split");

#[cfg(test)]
mod tests {
    use super::*;
    use libdatasource::model::Value;
    use libetl::error::Result;
    use libetl::model::EtlRow;
    use libetl::trait_def::TableLookup;

    /// Fake host lookup that just returns the pre-built detail rows.
    struct FakeLookup(Vec<Row>);

    impl TableLookup for FakeLookup {
        fn lookup(
            &self,
            _connection: &str,
            _table: &str,
            _columns: Option<&[String]>,
            _where_clause: Option<&str>,
            _params: &[Value],
            _limit: u64,
        ) -> Result<Vec<Row>> {
            Ok(self.0.clone())
        }
    }

    fn run(config: serde_json::Value, row: Row, lookup: FakeLookup) -> Vec<EtlOutputRow> {
        let mut plugin = OrderSplit::default();
        plugin.configure(&config).unwrap();
        let input = EtlRow {
            source_connection: "mysql_prod".into(),
            source_table: "t_order".into(),
            target_connections: vec!["pg_test".into()],
            row,
            source_schema: None,
            target_schema: None,
        };
        let mut ctx = EtlContext::from_row(&input, &lookup);
        plugin.process(&mut ctx, &input).unwrap()
    }

    fn row(pairs: &[(&str, Value)]) -> Row {
        let mut r = Row::new();
        for (k, v) in pairs {
            r.insert(*k, v.clone());
        }
        r
    }

    #[test]
    fn splits_header_and_details() {
        let config = serde_json::json!({
            "detail_table": "order_item",
            "source_detail_table": "t_order_item",
            "detail_columns": ["item_id", "qty"],
            "parent_key": "order_id",
            "child_key": "order_id",
            "keep": ["order_id", "user_id"],
        });
        let header = row(&[
            ("order_id", Value::Int(7)),
            ("user_id", Value::Int(42)),
            ("amount", Value::Decimal("12.50".into())),
        ]);
        let details = vec![
            row(&[("item_id", Value::Int(1)), ("qty", Value::Int(2))]),
            row(&[("item_id", Value::Int(2)), ("qty", Value::Int(1))]),
        ];

        let out = run(config, header, FakeLookup(details));

        assert_eq!(out.len(), 3);
        assert_eq!(out[0].table, ""); // header -> job default target
        assert_eq!(out[0].row.get("order_id"), Some(&Value::Int(7)));
        assert_eq!(out[0].row.get("user_id"), Some(&Value::Int(42)));
        assert_eq!(out[0].row.get("amount"), None); // dropped by keep
        assert_eq!(out[1].table, "order_item");
        assert_eq!(out[1].row.get("item_id"), Some(&Value::Int(1)));
        assert_eq!(out[2].table, "order_item");
        assert_eq!(out[2].row.get("qty"), Some(&Value::Int(1)));
    }

    #[test]
    fn missing_parent_key_emits_header_only() {
        let config = serde_json::json!({
            "detail_table": "order_item",
            "parent_key": "order_id",
        });
        let out = run(
            config,
            row(&[("order_id", Value::Null), ("user_id", Value::Int(1))]),
            FakeLookup(vec![]),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].table, "");
        assert_eq!(out[0].row.get("user_id"), Some(&Value::Int(1)));
    }

    #[test]
    fn unconfigured_plugin_is_an_error() {
        let plugin = OrderSplit::default();
        let input = EtlRow {
            source_connection: "mysql_prod".into(),
            source_table: "t_order".into(),
            target_connections: vec!["pg_test".into()],
            row: row(&[("order_id", Value::Int(1))]),
            source_schema: None,
            target_schema: None,
        };
        let lookup = FakeLookup(vec![]);
        let mut ctx = EtlContext::from_row(&input, &lookup);
        assert!(plugin.process(&mut ctx, &input).is_err());
    }
}
