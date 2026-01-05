use anyhow::Result;
use log::LevelFilter;
use simple_logger::SimpleLogger;
use time::macros::format_description;

/// Parse a log level string into a LevelFilter
pub fn parse_log_level(level: &str) -> LevelFilter {
    match level.to_uppercase().as_str() {
        "DEBUG" => LevelFilter::Debug,
        "INFO" => LevelFilter::Info,
        "WARN" | "WARNING" => LevelFilter::Warn,
        "ERROR" => LevelFilter::Error,
        _ => {
            eprintln!("Invalid log level '{}', defaulting to INFO.", level);
            LevelFilter::Info
        }
    }
}

/// Set up logging with the specified level
pub fn setup_logging(log_level: &str) -> Result<()> {
    let level = parse_log_level(log_level);
    SimpleLogger::new()
        .with_level(level)
        .with_timestamp_format(format_description!("[year]-[month]-[day] [hour]:[minute]:[second]"))
        .init()?;
    Ok(())
}
