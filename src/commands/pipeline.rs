use anyhow::Result;
use log::info;
use std::path::Path;

use crate::cli::{PipelineArgs, Source};
use crate::common::setup_logging;

pub fn run_pipeline(args: PipelineArgs) -> Result<()> {
    setup_logging(&args.log_level)?;

    info!("Starting citation extraction pipeline");
    info!("Input: {}", args.input);
    info!("Source mode: {}", args.source);

    // Validate required arguments based on source mode
    validate_args(&args)?;

    if !Path::new(&args.input).exists() {
        return Err(anyhow::anyhow!("Input file does not exist: {}", args.input));
    }

    // TODO: Implement full pipeline
    info!("Pipeline stub - implementation in progress");

    Ok(())
}

fn validate_args(args: &PipelineArgs) -> Result<()> {
    match args.source {
        Source::All => {
            if args.output_crossref.is_none() || args.output_datacite.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'all' requires both --output-crossref and --output-datacite"
                ));
            }
        }
        Source::Crossref => {
            if args.output_crossref.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'crossref' requires --output-crossref"
                ));
            }
        }
        Source::Datacite => {
            if args.output_datacite.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'datacite' requires --output-datacite"
                ));
            }
            if args.datacite_records.is_none() && args.load_datacite_index.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'datacite' requires --datacite-records or --load-datacite-index"
                ));
            }
        }
        Source::Arxiv => {
            if args.output_arxiv.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'arxiv' requires --output-arxiv"
                ));
            }
            if args.datacite_records.is_none() && args.load_datacite_index.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'arxiv' requires --datacite-records or --load-datacite-index"
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::PipelineArgs;

    fn default_args() -> PipelineArgs {
        PipelineArgs {
            input: "test.tar.gz".to_string(),
            datacite_records: None,
            source: Source::All,
            output_crossref: None,
            output_datacite: None,
            output_arxiv: None,
            output_crossref_failed: None,
            output_datacite_failed: None,
            output_arxiv_failed: None,
            http_fallback: vec![],
            load_crossref_index: None,
            save_crossref_index: None,
            load_datacite_index: None,
            save_datacite_index: None,
            log_level: "INFO".to_string(),
            concurrency: 50,
            timeout: 5,
            keep_intermediates: false,
            temp_dir: None,
            batch_size: 5000000,
        }
    }

    #[test]
    fn test_validate_args_all_requires_both_outputs() {
        let args = default_args();
        let result = validate_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--output-crossref"));
    }

    #[test]
    fn test_validate_args_all_with_both_outputs() {
        let mut args = default_args();
        args.output_crossref = Some("crossref.jsonl".to_string());
        args.output_datacite = Some("datacite.jsonl".to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_crossref_requires_output() {
        let mut args = default_args();
        args.source = Source::Crossref;
        let result = validate_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--output-crossref"));
    }

    #[test]
    fn test_validate_args_crossref_with_output() {
        let mut args = default_args();
        args.source = Source::Crossref;
        args.output_crossref = Some("crossref.jsonl".to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_datacite_requires_output() {
        let mut args = default_args();
        args.source = Source::Datacite;
        args.datacite_records = Some("records.jsonl.gz".to_string());
        let result = validate_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--output-datacite"));
    }

    #[test]
    fn test_validate_args_datacite_requires_records() {
        let mut args = default_args();
        args.source = Source::Datacite;
        args.output_datacite = Some("datacite.jsonl".to_string());
        let result = validate_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--datacite-records"));
    }

    #[test]
    fn test_validate_args_datacite_with_records_file() {
        let mut args = default_args();
        args.source = Source::Datacite;
        args.output_datacite = Some("datacite.jsonl".to_string());
        args.datacite_records = Some("records.jsonl.gz".to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_datacite_with_index() {
        let mut args = default_args();
        args.source = Source::Datacite;
        args.output_datacite = Some("datacite.jsonl".to_string());
        args.load_datacite_index = Some("index.parquet".to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_arxiv_requires_output() {
        let mut args = default_args();
        args.source = Source::Arxiv;
        args.datacite_records = Some("records.jsonl.gz".to_string());
        let result = validate_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--output-arxiv"));
    }

    #[test]
    fn test_validate_args_arxiv_requires_records() {
        let mut args = default_args();
        args.source = Source::Arxiv;
        args.output_arxiv = Some("arxiv.jsonl".to_string());
        let result = validate_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--datacite-records"));
    }

    #[test]
    fn test_validate_args_arxiv_with_all_required() {
        let mut args = default_args();
        args.source = Source::Arxiv;
        args.output_arxiv = Some("arxiv.jsonl".to_string());
        args.datacite_records = Some("records.jsonl.gz".to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }
}
