# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Git Commits

Use simple, concise commit messages. No attribution or Co-Authored-By lines.

## Build and Test Commands

```bash
# Build release binary
cargo build --release

# Build debug binary
cargo build

# Run tests
cargo test

# Run a single test
cargo test test_name

# Run tests in a specific module
cargo test streaming::tests

# Check without building
cargo check

# Format code
cargo fmt

# Lint
cargo clippy
```

The release binary is at `target/release/crossref-arxiv-citation-extraction`.

## Architecture

This is a Rust CLI that extracts arXiv references from Crossref snapshot data files, aggregates citations by arXiv work, and validates them against DataCite records.

### Data Flow

```
Crossref tar.gz → Extract arXiv refs → Partition by arXiv ID prefix → Invert (aggregate by arXiv work) → Validate against DataCite
```

### Module Structure

- **`cli.rs`** - Clap-based command definitions (`pipeline` and `validate` subcommands)
- **`commands/`** - Command implementations
  - `pipeline.rs` - Full pipeline: streams tar.gz, extracts refs, partitions, inverts, validates
  - `validate.rs` - Standalone validation against DataCite + DOI resolution
- **`extract/`** - arXiv ID extraction
  - `patterns.rs` - Regex patterns for modern (`2403.12345`), old (`hep-ph/9901234`), DOI, and URL formats. IDs are normalized to lowercase with version suffixes stripped.
- **`streaming/`** - Partition-based processing
  - `partition_writer.rs` - Writes extracted refs to per-partition Parquet files (partitioned by first 4 chars of arXiv ID)
  - `partition_invert.rs` - Parallel inversion using Polars: groups by `arxiv_id`, aggregates `cited_by` lists
  - `checkpoint.rs` - Resume support for long-running pipeline
- **`common/`** - Shared types (`ArxivMatch`, `ArxivCitationsSimple`, `ValidateStats`), logging, progress bars, utilities

### Key Design Decisions

1. **Streaming architecture**: Tar.gz is streamed without full extraction. References are partitioned to disk immediately to bound memory usage.

2. **Partition key**: First 4 characters of arXiv ID (e.g., `2403`, `hep-` for old format). Slashes are replaced with underscores for filesystem safety.

3. **Polars for aggregation**: Partitions are processed as Parquet files using Polars for efficient group-by operations.

4. **Two-stage validation**: First checks DataCite records (fast, local HashSet lookup), then falls back to HTTP HEAD requests to doi.org for unmatched DOIs.

5. **Checkpoint/resume**: Long-running pipeline can be resumed from checkpoint files stored in the partition directory.

### arXiv ID Patterns

The extractor recognizes:
- Modern: `arXiv:2403.03542`, `arXiv.2403.03542v2`
- Old: `arXiv:hep-ph/9901234`, `arXiv:cs.DM/9910013`
- DOI: `10.48550/arXiv.2403.03542`
- URL: `arxiv.org/abs/2403.03542`

References must contain "arxiv" context to match (bare `2403.03542` won't match).
