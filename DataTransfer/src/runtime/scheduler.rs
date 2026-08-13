//! Cron scheduling for job threads.

use std::time::Duration;

use chrono::Utc;
use cron::Schedule;

use crate::error::{AppError, AppResult};

/// Run `f` at every occurrence of the cron expression (blocking the current
/// thread forever).
pub fn run_on_schedule<F>(expr: &str, mut f: F) -> AppResult<()>
where
    F: FnMut(),
{
    let schedule: Schedule = expr
        .parse()
        .map_err(|e| AppError::Schedule(format!("invalid cron '{expr}': {e}")))?;

    let mut iterator = schedule.after(&Utc::now());
    loop {
        let Some(next) = iterator.next() else {
            return Err(AppError::Schedule(format!(
                "cron '{expr}' produced no future occurrences"
            )));
        };
        let now = Utc::now();
        let wait = (next - now).to_std().unwrap_or(Duration::ZERO);
        if wait > Duration::ZERO {
            std::thread::sleep(wait);
        }
        f();
    }
}

/// Token-bucket gate limiting concurrent job runs.
pub struct WorkerGate {
    inner: std::sync::Arc<GateInner>,
}

struct GateInner {
    available: std::sync::Mutex<usize>,
    cond: std::sync::Condvar,
}

impl Clone for WorkerGate {
    fn clone(&self) -> Self {
        WorkerGate {
            inner: std::sync::Arc::clone(&self.inner),
        }
    }
}

impl WorkerGate {
    pub fn new(max: usize) -> WorkerGate {
        WorkerGate {
            inner: std::sync::Arc::new(GateInner {
                available: std::sync::Mutex::new(max.max(1)),
                cond: std::sync::Condvar::new(),
            }),
        }
    }

    /// Block until a worker slot is free, returning a guard that releases it.
    pub fn acquire(&self) -> WorkerGuard {
        let mut avail = self.inner.available.lock().unwrap();
        while *avail == 0 {
            avail = self.inner.cond.wait(avail).unwrap();
        }
        *avail -= 1;
        WorkerGuard {
            inner: std::sync::Arc::clone(&self.inner),
        }
    }
}

pub struct WorkerGuard {
    inner: std::sync::Arc<GateInner>,
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        let mut avail = self.inner.available.lock().unwrap();
        *avail += 1;
        self.inner.cond.notify_one();
    }
}