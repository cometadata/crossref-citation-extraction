use anyhow::Result;
use log::LevelFilter;
use simple_logger::SimpleLogger;
use time::macros::format_description;

pub fn parse_log_level(level: &str) -> LevelFilter {
    match level.to_uppercase().as_str() {
        "OFF" => LevelFilter::Off,
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

pub fn setup_logging(log_level: &str) -> Result<()> {
    let level = parse_log_level(log_level);
    let _ = SimpleLogger::new()
        .with_level(level)
        .with_timestamp_format(format_description!("[year]-[month]-[day] [hour]:[minute]:[second]"))
        .init();
    Ok(())
}
