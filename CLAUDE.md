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

The release binary is at `target/release/crossref-citation-extraction`.

## Architecture

This is a Rust CLI that extracts DOI references from Crossref snapshot data files, aggregates citations by cited work, and validates them against Crossref and DataCite records.

### Data Flow

```
Crossref tar.gz → Extract DOIs → Partition by DOI prefix → Invert (aggregate by cited work) → Validate against indexes → Output
```

### Module Structure

- **`cli.rs`** - Clap-based command definitions with Source enum
- **`commands/`** - Command implementations
  - `pipeline.rs` - Full pipeline: streams tar.gz, extracts refs, partitions, inverts, validates
  - `validate.rs` - Standalone validation against indexes
- **`extract/`** - DOI and arXiv ID extraction
  - `doi.rs` - Generic DOI extraction patterns and normalization
  - `arxiv.rs` - arXiv-specific patterns
- **`index/`** - DOI index management
  - `mod.rs` - DoiIndex type with prefix tracking
  - `builder.rs` - Build indexes from JSONL.gz files
  - `persistence.rs` - Parquet save/load for indexes
- **`streaming/`** - Partition-based processing
  - `partition_writer.rs` - Writes to per-partition Parquet files
  - `partition_invert.rs` - Parallel inversion with multi-format output
  - `checkpoint.rs` - Resume support
- **`validation/`** - Multi-source validation
  - `prefix_filter.rs` - Fast prefix-based filtering
  - `lookup.rs` - Index-based DOI lookup
  - `http.rs` - HTTP fallback validation
  - `runner.rs` - Validation orchestration
- **`common/`** - Shared types and utilities

### Key Design Decisions

1. **Streaming architecture**: Tar.gz is streamed without full extraction. References are partitioned to disk immediately to bound memory usage.

2. **Partition key**: For DOIs, uses the DOI prefix (e.g., `10.1234`). For arXiv IDs, uses first 4 characters (e.g., `2403`, `hep-` for old format). Slashes are replaced with underscores for filesystem safety.

3. **Polars for aggregation**: Partitions are processed as Parquet files using Polars for efficient group-by operations.

4. **Multi-source validation**: Validates against Crossref index, DataCite index, or both depending on source mode. Falls back to HTTP HEAD requests to doi.org for unmatched DOIs when enabled.

5. **Checkpoint/resume**: Long-running pipeline can be resumed from checkpoint files stored in the partition directory.

6. **Source modes**: Supports `all` (all DOIs), `crossref` (Crossref DOIs only), `datacite` (DataCite DOIs only), and `arxiv` (arXiv DOIs with DataCite validation).

### DOI Patterns

The extractor recognizes:
- Standard DOIs: `10.1234/example`, `doi:10.1234/example`
- arXiv DOIs: `10.48550/arXiv.2403.03542`
- URL format: `doi.org/10.1234/example`, `dx.doi.org/10.1234/example`

### arXiv ID Patterns

For arXiv-specific extraction:
- Modern: `arXiv:2403.03542`, `arXiv.2403.03542v2`
- Old: `arXiv:hep-ph/9901234`, `arXiv:cs.DM/9910013`
- DOI: `10.48550/arXiv.2403.03542`
- URL: `arxiv.org/abs/2403.03542`

References must contain "arxiv" context to match (bare `2403.03542` won't match).
