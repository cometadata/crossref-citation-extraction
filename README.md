# crossref-citation-extraction

CLI for extracting arXiv references from Crossref data files, validating them against arXiv (DataCite) records, and aggregating citations by cited work.

## Overview

This tool processes Crossref snapshot data to identify works that cite arXiv papers. It extracts arXiv IDs from reference metadata using pattern matching, validates them against an arXiv index (built from DataCite records), and outputs citation data organized by cited arXiv work.

```
Crossref tar.gz → Extract arXiv IDs → Partition by arXiv prefix → Aggregate by cited work → Validate against arXiv index → Output
```

## Building

```bash
cargo build --release
```

The binary will be at `target/release/crossref-citation-extraction`.

## Usage

### Build arXiv Index

Pre-build a finite state tranducer (FST) index from arXiv (DataCite) records for faster pipeline runs:

```bash
./target/release/crossref-citation-extraction build-index \
  --input arxiv-datacite-records.jsonl.gz \
  --output arxiv.fst
```

### Run Pipeline

Extract and validate arXiv citations using a pre-built FST index:

```bash
./target/release/crossref-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --arxiv-fst arxiv.fst \
  --output-dir ./results
```

Alternatively, build the FST on-the-fly from arXiv records:

```bash
./target/release/crossref-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --arxiv-records arxiv-records.jsonl.gz \
  --output-dir ./results
```

Filter by provenance to include only publisher-asserted or Crossref-matched references:

```bash
./target/release/crossref-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --arxiv-fst arxiv.fst \
  --provenance publisher,crossref \
  --output-dir ./results
```

Re-run aggregation over existing partitions (skips the extraction phase). `--input` is still required, since the output filenames derive from it:

```bash
./target/release/crossref-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --partitions-dir ./temp/partitions \
  --arxiv-fst arxiv.fst \
  --output-dir ./results
```

### Pipeline Options

| Option | Description |
|--------|-------------|
| `--input` | Crossref snapshot (tar.gz, directory, or JSON file). Required; its filename stem names the output files |
| `--arxiv-fst` | Path to pre-built arXiv FST index |
| `--arxiv-records` | Path to arXiv records JSONL.gz (builds FST on-the-fly) |
| `--output-dir` | Directory for output files (default: current directory) |
| `--outputs` | Output types: valid, failed, publisher, crossref, mined, or all (default: all) |
| `--provenance` | Filter by provenance: publisher, crossref, mined |
| `--temp-dir` | Directory for intermediate partition files (default: system temp). Without `--keep-intermediates` this directory itself is deleted after a successful run, so do not point it at a directory holding other files |
| `--partitions-dir` | Use existing partitions (skip extraction phase); these partitions are always kept |
| `--batch-size` | Batch size for memory management (default: 5000000) |
| `--resume` | Resume extraction and aggregation from checkpoint files in the partition directory |
| `--checkpoint-interval` | Partitions between aggregation checkpoints (default: 1; 0 disables checkpointing) |
| `--keep-intermediates` | Keep partition files after completion; without it the partition directory is deleted once the run succeeds |
| `--log-level` | Logging level: DEBUG, INFO, WARN, ERROR |

### Input Formats

Crossref input (`--input`) accepts tar.gz archives (streamed without extraction), directories containing JSON files, or single JSON files for testing.

arXiv records (`--arxiv-records`) accepts gzipped JSONL files, uncompressed JSONL files, directories with `updated_*/` subdirectories, or directories containing JSONL.gz files.

## Output Format

Each line of the output is a JSON object representing a cited arXiv work:

```json
{
  "doi": "10.48550/arXiv.2403.03542",
  "citation_count": 2,
  "cited_by": [
    {
      "doi": "10.5678/citing-paper",
      "provenance": "publisher",
      "raw_match": "10.48550/arXiv.2403.03542",
      "reference": {"DOI": "10.48550/arXiv.2403.03542", "doi-asserted-by": "publisher"}
    },
    {
      "doi": "10.5678/another-citing-paper",
      "provenance": "mined",
      "raw_match": "arXiv:2403.03542",
      "reference": {"unstructured": "See arXiv:2403.03542 for details"}
    }
  ]
}
```

`raw_match` is the exact substring that matched an arXiv identifier pattern.

The `--outputs` option controls which files are generated:

| Output Type | Filename | Description |
|-------------|----------|-------------|
| valid | `<input-stem>_valid.jsonl` | All validated arXiv citations |
| failed | `<input-stem>_failed.jsonl` | Citations that failed validation |
| publisher | `<input-stem>_publisher.jsonl` | Publisher-asserted references only |
| crossref | `<input-stem>_crossref.jsonl` | Crossref-matched references only |
| mined | `<input-stem>_mined.jsonl` | Text-extracted references only |

`<input-stem>` is the `--input` filename with its extension stripped, so `crossref-snapshot.tar.gz` produces `crossref-snapshot_valid.jsonl`.

Use `--outputs all` to generate all output types. Omitting `--outputs` also generates all of them.

### Provenance

Each citation includes a `provenance` field indicating how the arXiv ID was obtained: `publisher` means the ID was explicitly provided in the reference metadata, `crossref` means it was matched by Crossref, and `mined` means it was extracted from unstructured text.

## arXiv ID Patterns

The extractor recognizes modern format (`arXiv:2403.03542`, `arXiv.2403.03542v2`), old format (`arXiv:hep-ph/9901234`, `arXiv:cs.DM/9910013`), DOI format (`10.48550/arXiv.2403.03542`), and URL format (`arxiv.org/abs/2403.03542`).

References must contain "arxiv" context to match—bare `2403.03542` won't match.

## Architecture

The tool uses a streaming architecture to process large datasets with bounded memory. Tar.gz input files are streamed without full extraction. References are written to per-partition parquet segments keyed by the arXiv ID (first 4 characters of the ID after `arXiv.`, with `/` replaced by `_`). Small partitions are aggregated in memory; large partitions (>100 segments) via parallel per-segment sorting and a k-way streaming merge. Validation uses a memory-efficient finite state transducer for arXiv ID lookup.

A read error in the input archive — a truncated or corrupt tar.gz, for example — aborts the run with an error rather than logging a warning and exiting successfully with partial output.

Long-running pipelines can be resumed with `--resume` from checkpoint files stored in the partition directory.

## Development

```bash
cargo build          # Debug build
cargo build --release # Release build
cargo test           # Run tests
cargo fmt            # Format code
cargo clippy         # Lint
```
