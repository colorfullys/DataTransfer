//! Row router: distributes ETL output rows to one of `n` parallel writers.
//!
//! * Rows destined for the job's main target table are routed by primary-key
//!   hash, so updates to the same key always land on the same writer (keeps
//!   upsert ordering well-defined while optimising for parallelism).
//! * Rows for any other table (e.g. detail tables produced by a splitter) and
//!   rows without a primary key are load-balanced round-robin.

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::sync::atomic::{AtomicUsize, Ordering};

use libdatasource::model::{Row, Value};

pub struct Router {
    n: usize,
    main_table: String,
    main_pk: Vec<String>,
    round: AtomicUsize,
}

impl Router {
    pub fn new(n: usize, main_table: String, main_pk: Vec<String>) -> Router {
        Router {
            n: n.max(1),
            main_table,
            main_pk,
            round: AtomicUsize::new(0),
        }
    }

    /// Which writer (`0..n`) receives this row?
    pub fn route(&self, table: &str, row: &Row) -> usize {
        if self.n <= 1 {
            return 0;
        }
        if table == self.main_table && !self.main_pk.is_empty() {
            let h = hash_row(row, &self.main_pk);
            (h as usize) % self.n
        } else {
            let r = self.round.fetch_add(1, Ordering::Relaxed);
            r % self.n
        }
    }
}

/// Deterministic hash of the values at `pk` columns.
fn hash_row(row: &Row, pk: &[String]) -> u64 {
    let mut h = DefaultHasher::new();
    for c in pk {
        h.write(c.as_bytes());
        h.write_u8(0xFF);
        match row.get(c) {
            None => h.write_u8(0x00),
            Some(Value::Null) => h.write_u8(0x01),
            Some(Value::Bool(b)) => h.write_u8(if *b { 2 } else { 3 }),
            Some(Value::Int(i)) => h.write_i64(*i),
            Some(Value::UInt(u)) => h.write_u64(*u),
            Some(Value::Float(f)) => h.write_u64(f.to_bits()),
            Some(Value::Decimal(s)) => h.write(s.as_bytes()),
            Some(Value::String(s)) => h.write(s.as_bytes()),
            Some(Value::Date(s)) => h.write(s.as_bytes()),
            Some(Value::Bytes(b)) => h.write(b),
        }
        h.write_u8(0xFE);
    }
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(v: i64) -> Row {
        let mut r = Row::new();
        r.insert("id".to_string(), Value::Int(v));
        r
    }

    #[test]
    fn single_writer_always_zero() {
        let r = Router::new(1, "t".into(), vec!["id".into()]);
        for v in [1, 2, 3, 99] {
            assert_eq!(r.route("t", &row(v)), 0);
        }
    }

    #[test]
    fn pk_hash_is_deterministic() {
        let r1 = Router::new(4, "t".into(), vec!["id".into()]);
        let r2 = Router::new(4, "t".into(), vec!["id".into()]);
        for v in (0..100).step_by(7) {
            let row = row(v);
            assert_eq!(r1.route("t", &row), r2.route("t", &row));
        }
    }

    #[test]
    fn no_pk_round_robins() {
        let r = Router::new(3, "t".into(), Vec::new());
        let mut seen = std::collections::HashSet::new();
        for v in 0..9 {
            seen.insert(r.route("t", &row(v)));
        }
        assert_eq!(seen.len(), 3);
        assert!(seen.iter().all(|i| (0..3).contains(i)));
    }
}