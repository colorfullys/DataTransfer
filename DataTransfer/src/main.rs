//! DataTransfer orchestrator entry point.
//!
//! Usage: `DataTransfer <config.yaml> [state-dir]`
//!
//! Loads datasource plugins + connections and ETL plugins from the config,
//! builds a pipeline per enabled job and runs each on its cron schedule.

mod config;
mod datasource;
mod error;
mod etl;
mod runtime;
mod support;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use libetl::registry::EtlRegistry;

use crate::config::JobConfig;
use crate::datasource::ConnectionManager;
use crate::etl::{AppLookup, EtlPipeline};
use crate::runtime::{JobRunner, WorkerGate};

fn fatal(e: error::AppError) -> ! {
    eprintln!("DataTransfer: {e}");
    log::error!("fatal: {e}");
    std::process::exit(1);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let config_path = PathBuf::from(args.get(1).map(|s| s.as_str()).unwrap_or("config.yaml"));

    // ---- config ----
    let mut app = match config::AppConfig::load(&config_path) {
        Ok(a) => a,
        Err(e) => fatal(e),
    };
    if let Err(e) = app.load_datasources(&app.datasource_file.clone()) {
        fatal(e);
    }
    support::init(&app.log_level, app.log_file.as_deref());

    // ---- datasource plugins & connections ----
    let mut conns = ConnectionManager::new();
    if let Err(e) = conns.load_plugins(&app) {
        fatal(e);
    }
    if let Err(e) = conns.connect_all(&app.connections) {
        fatal(e);
    }
    let conns = Arc::new(conns);
    log::info!("connected {} datasource connection(s)", conns.names().len());

    // ---- ETL plugins ----
    let mut etl_registry = EtlRegistry::new();
    for path in &app.etl_plugin_paths {
        if let Err(e) = etl_registry
            .load_file(Path::new(path))
            .map_err(|e| error::AppError::Etl(e.to_string()))
        {
            fatal(e);
        }
    }
    if !app.etl_plugin_paths.is_empty() {
        log::info!("loaded etl plugin(s): {:?}", etl_registry.names());
    }

    let lookup = Arc::new(AppLookup {
        conns: Arc::clone(&conns),
    });

    // ---- jobs ----
    let mut jobs = Vec::new();
    if app.job_files.is_empty() {
        log::warn!("no jobs configured");
    }
    let mut seen = HashSet::new();
    for file in &app.job_files {
        match JobConfig::load(Path::new(file)) {
            Ok(job) => {
                if !seen.insert(job.name.clone()) {
                    fatal(error::AppError::Config(format!(
                        "duplicate job name '{}' (from {})",
                        job.name, file
                    )));
                }
                log::info!(
                    "loaded job '{}' {} ({}), {} etl step(s)",
                    job.name,
                    if job.description.is_empty() {
                        String::new()
                    } else {
                        format!("- {}", job.description)
                    },
                    if job.enabled { "enabled" } else { "disabled" },
                    job.etl.len()
                );
                jobs.push(job);
            }
            Err(e) => fatal(e),
        }
    }

    // ---- spawn scheduler threads ----
    let gate = WorkerGate::new(app.workers);
    let mut spawned = 0usize;
    for job in jobs {
        if !job.enabled {
            log::info!("job '{}' disabled, skipping", job.name);
            continue;
        }
        let pipeline = match EtlPipeline::build(&job, &etl_registry, &lookup) {
            Ok(p) => p,
            Err(e) => fatal(e),
        };
        let state_dir = app.base_dir.join(&app.state_dir);
        let mut runner = match JobRunner::build(
            job.clone(),
            Arc::clone(&conns),
            Arc::clone(&lookup),
            pipeline,
            &state_dir,
            app.retry,
            app.page_size,
        ) {
            Ok(r) => r,
            Err(e) => fatal(e),
        };

        let cron_expr = job.cron.clone();
        let job_name = job.name.clone();
        let gate = gate.clone();
        std::thread::Builder::new()
            .name(format!("job-{job_name}"))
            .spawn(move || {
                log::info!("job '{job_name}' scheduled with cron '{cron_expr}'");
                let inner_name = job_name.clone();
                if let Err(e) = runtime::run_on_schedule(&cron_expr, move || {
                    let _guard = gate.acquire();
                    log::info!("job '{inner_name}' starting");
                    match runner.run_once() {
                        Ok(_) => {}
                        Err(e) => log::error!("job '{inner_name}' failed: {e}"),
                    }
                }) {
                    log::error!("job '{job_name}' scheduler stopped: {e}");
                }
            })
            .expect("spawn job thread");
        spawned += 1;
    }

    log::info!("DataTransfer started: {spawned} job(s) scheduled, {} worker(s)", app.workers);
    loop {
        std::thread::park();
    }
}