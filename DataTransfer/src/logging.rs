//! Logging setup from `config.yaml`.

use env_logger::{Builder, Target};
use log::LevelFilter;

pub fn init(level: &str, file: Option<&str>) {
    let lvl = level.parse::<LevelFilter>().unwrap_or(LevelFilter::Info);
    let mut builder = Builder::new();
    builder.filter_level(lvl);
    builder.format_timestamp_secs();
    builder.format_target(false);

    match file {
        Some(path) => {
            if !path.trim().is_empty() {
                if let Some(parent) = std::path::Path::new(path).parent() {
                    if !parent.as_os_str().is_empty() {
                        if let Err(e) = std::fs::create_dir_all(parent) {
                            eprintln!(
                                "warning: cannot create log directory '{}': {e}",
                                parent.display()
                            );
                        }
                    }
                }
            }
            match std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            {
                Ok(f) => {
                    builder.target(Target::Pipe(Box::new(f)));
                }
                Err(e) => {
                    eprintln!("warning: cannot open log file '{path}': {e}; logging to stderr");
                }
            }
        }
        None => {
            builder.target(Target::Stderr);
        }
    }

    if builder.try_init().is_err() {
        eprintln!("warning: logger already initialised");
    }
}