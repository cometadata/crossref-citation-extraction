mod cli;
mod commands;
mod common;
mod extract;

use anyhow::Result;
use clap::Parser;

use cli::{Cli, Commands};
use commands::{run_extract, run_invert, run_validate, run_pipeline};

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Extract(args) => {
            run_extract(args)?;
        }
        Commands::Invert(args) => {
            run_invert(args)?;
        }
        Commands::Validate(args) => {
            run_validate(args)?;
        }
        Commands::Pipeline(args) => {
            run_pipeline(args)?;
        }
    }

    Ok(())
}
