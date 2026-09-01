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

## Hugging Face Job

A production workload that runs this extractor against the monthly Crossref
Metadata Plus snapshot and updates
[`cometadata/crossref-arxiv-citations`](https://huggingface.co/datasets/cometadata/crossref-arxiv-citations).
Python orchestration lives in `citations_pipeline/`; the job entry is
`jobs/pipeline.py`; runs are submitted manually with `submit_job.py`.

### How it works

1. **Snapshot** — reuses the Crossref snapshot mirrored by the affiliations
   pipeline (`shared-storage/crossref-affiliations-latest/`, consumed via its
   `_PIPELINE_COMPLETE.json` contract). If no valid mirror exists, and
   `CROSSREF_API_TOKEN` is set, the job downloads the snapshot itself into
   `crossref-arxiv-citations-latest/raw-generations/`.
2. **arXiv index** — built from the DataCite monthly data file already
   mirrored in the bucket (`datacite-monthly-data-file-<release>/dois/`),
   cached as `crossref-arxiv-citations-latest/fst/<release>-v1/arxiv.fst`.
3. **Extract** — this crate is built in-job (`cargo build --release --locked`)
   from the staged source volume and streams the 280 GB archive on local disk.
4. **Transform + publish** — the valid output is reshaped into
   `data/{all,asserted,mined}.parquet` (the dataset's published schema) and
   committed to the dataset in one parent-SHA-guarded commit together with a
   refreshed dataset card.

Durable state lives under
`hf://buckets/cometadata/shared-storage/crossref-arxiv-citations-latest/`:
`derived-generations/<snapshot>-<run>/` (outputs), root markers
`_EXTRACTION_COMPLETE.json` (retry pointer) and `_PIPELINE_COMPLETE.json`
(authoritative; written only after the dataset commit is verified). Retried
runs reuse verified work via the markers; `--force` recomputes everything but
never skips verification.

### Run a smoke job

Runs the full chain (Rust build, FST build, extraction, transform) on an
embedded deterministic archive. No bucket mount, no dataset write, no tokens.

```bash
hf auth login
uv run submit_job.py --smoke
```

Inspect asynchronously (no busy polling):

```bash
hf jobs inspect JOB_ID --namespace cometadata
hf jobs logs JOB_ID --namespace cometadata
```

A successful smoke log ends with `RESULT_JSON=` containing
`"status":"smoke-completed"`.

### Run production

```bash
hf auth login
uv run submit_job.py
```

Production submits in namespace `cometadata` on `cpu-xl` with a 24-hour
timeout and label `pipeline=crossref-arxiv-citations`. At roughly $1/hour the
timeout is an approximate $24 ceiling per run; the first run against a new
DataCite release spends about an extra hour building the FST. Confirm current
Jobs pricing and approve the cost before each billable submission.

The submitter checks the bucket for the affiliations mirror and only demands
`CROSSREF_API_TOKEN` when the mirror is missing. Optional flags:
`--force` (recompute reusable phases), `--datacite-release YYYY-MM` (pin the
DataCite release).

Telemetry is emitted as `PIPELINE_EVENT=` JSON lines (phase start/complete/
fail, progress with throughput and ETA). Events never contain credentials or
signed URLs.

### Local verification

```bash
uv run pytest -q
uv run ruff check .
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo build --release --locked
```

Local smoke (same entry point the job runs):

```bash
SOURCE_DIGEST="$(uv run python -c 'from pathlib import Path; from citations_pipeline.paths import runtime_source_digest; print(runtime_source_digest(Path.cwd()))')"
uv run jobs/pipeline.py --smoke --source-root "$PWD" --source-digest "$SOURCE_DIGEST" --work-root /tmp/citations-smoke-work
```
