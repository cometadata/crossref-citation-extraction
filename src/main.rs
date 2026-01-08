mod cli;
mod commands;
mod common;
mod extract;
mod index;
mod streaming;

use anyhow::Result;
use clap::Parser;

use cli::{Cli, Commands};
use commands::{run_pipeline, run_validate};

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Pipeline(args) => {
            run_pipeline(args)?;
        }
        Commands::Validate(args) => {
            run_validate(args)?;
        }
    }

    Ok(())
}
