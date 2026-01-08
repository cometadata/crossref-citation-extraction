use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "crossref-arxiv-citation-extraction")]
#[command(about = "Unified CLI for extracting, inverting, and validating arXiv references from Crossref data")]
#[command(version = "1.0.0")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Convert Crossref tar.gz to Parquet with flattened references
    Convert(ConvertArgs),

    /// Extract arXiv references from Parquet file (vectorized using Polars)
    Extract(ExtractArgs),

    /// Invert arXiv reference relationships: map arXiv IDs to citing DOIs (using Polars)
    Invert(InvertArgs),

    /// Validate arXiv citations against DataCite records and DOI resolution
    Validate(ValidateArgs),

    /// Run the full pipeline: convert -> extract -> invert -> validate
    Pipeline(PipelineArgs),
}

#[derive(Parser, Clone)]
pub struct ConvertArgs {
    /// Path to the Crossref snapshot tar.gz file
    #[arg(short, long, required = true)]
    pub input: String,

    /// Output Parquet file
    #[arg(short, long, default_value = "references.parquet")]
    pub output: String,

    /// Row group size for Parquet output
    #[arg(long, default_value = "250000")]
    pub row_group_size: usize,

    /// Batch size (number of references) for chunked processing to limit memory usage
    #[arg(long, default_value = "5000000")]
    pub batch_size: usize,

    /// Logging level (DEBUG, INFO, WARN, ERROR)
    #[arg(short, long, default_value = "INFO")]
    pub log_level: String,
}

#[derive(Parser, Clone)]
pub struct ExtractArgs {
    /// Input Parquet file from convert step
    #[arg(short, long, required = true)]
    pub input: String,

    /// Output Parquet file with extracted arXiv IDs
    #[arg(short, long, default_value = "extracted.parquet")]
    pub output: String,

    /// Logging level (DEBUG, INFO, WARN, ERROR)
    #[arg(short, long, default_value = "INFO")]
    pub log_level: String,
}

#[derive(Parser, Clone)]
pub struct InvertArgs {
    /// Input Parquet file from extract step
    #[arg(short, long, required = true)]
    pub input: String,

    /// Output Parquet file with inverted citations
    #[arg(short, long, default_value = "inverted.parquet")]
    pub output: String,

    /// Logging level (DEBUG, INFO, WARN, ERROR)
    #[arg(short, long, default_value = "INFO")]
    pub log_level: String,

    /// Also output as JSONL for compatibility with validate step
    #[arg(long)]
    pub output_jsonl: Option<String>,
}

#[derive(Parser, Clone)]
pub struct ValidateArgs {
    /// Input arxiv_citations.jsonl file
    #[arg(short, long, required = true)]
    pub input: String,

    /// DataCite records.jsonl.gz file
    #[arg(short, long, required = true)]
    pub records: String,

    /// Output file for valid DOIs
    #[arg(long, default_value = "arxiv_citations_valid.jsonl")]
    pub output_valid: String,

    /// Output file for failed DOIs
    #[arg(long, default_value = "arxiv_citations_failed.jsonl")]
    pub output_failed: String,

    /// Concurrent HTTP requests
    #[arg(short, long, default_value = "50")]
    pub concurrency: usize,

    /// Timeout in seconds per request
    #[arg(short, long, default_value = "5")]
    pub timeout: u64,

    /// Logging level (DEBUG, INFO, WARN, ERROR)
    #[arg(short, long, default_value = "INFO")]
    pub log_level: String,
}

#[derive(Parser, Clone)]
pub struct PipelineArgs {
    /// Path to the Crossref snapshot tar.gz file
    #[arg(short, long, required = true)]
    pub input: String,

    /// DataCite records.jsonl.gz file for validation
    #[arg(short, long, required = true)]
    pub records: String,

    /// Output file for valid arXiv citations
    #[arg(short, long, default_value = "arxiv_citations_valid.jsonl")]
    pub output: String,

    /// Also output failed validations to this file (optional)
    #[arg(long)]
    pub output_failed: Option<String>,

    /// Logging level (DEBUG, INFO, WARN, ERROR)
    #[arg(short, long, default_value = "INFO")]
    pub log_level: String,

    /// Concurrent HTTP requests for validation
    #[arg(short, long, default_value = "50")]
    pub concurrency: usize,

    /// Timeout in seconds per validation request
    #[arg(long, default_value = "5")]
    pub timeout: u64,

    /// Keep intermediate files instead of deleting them
    #[arg(long, default_value = "false")]
    pub keep_intermediates: bool,

    /// Directory for intermediate files (default: system temp)
    #[arg(long)]
    pub temp_dir: Option<String>,

    /// Batch size (number of references) for chunked processing in convert step
    #[arg(long, default_value = "5000000")]
    pub batch_size: usize,
}
