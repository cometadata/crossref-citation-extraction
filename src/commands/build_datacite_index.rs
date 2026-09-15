use anyhow::Result;
use log::info;
use std::path::Path;

use crate::common::setup_logging;
use crate::index::build_datacite_index;

/// Arguments for the build-datacite-index command
#[derive(Debug, Clone)]
pub struct BuildDataciteIndexArgs {
    /// Root of the verified DataCite monthly release.
    pub input: String,
    /// Bundle directory to create; must not already hold a completed bundle.
    pub output: String,
    /// External sort memory budget in MiB.
    pub sort_memory_mb: usize,
    pub log_level: String,
}

pub fn run_build_datacite_index(args: BuildDataciteIndexArgs) -> Result<()> {
    setup_logging(&args.log_level)?;

    info!("Building DataCite index bundle");
    info!("Input: {}", args.input);
    info!("Output: {}", args.output);
    info!("Sort memory: {} MiB", args.sort_memory_mb);

    let manifest = build_datacite_index(
        Path::new(&args.input),
        Path::new(&args.output),
        args.sort_memory_mb,
    )?;

    info!(
        "DataCite index bundle built successfully: {} (release {}, {} DOIs, {} URLs)",
        args.output,
        manifest.release,
        manifest.counts.get("dois").copied().unwrap_or_default(),
        manifest.counts.get("urls").copied().unwrap_or_default()
    );

    Ok(())
}
