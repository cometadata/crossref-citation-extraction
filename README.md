# crossref-arxiv-citation-extraction

CLI for extracting and validating arXiv references from Crossref data files.

## Overview

This tool processes Crossref snapshot data to identify works that cite arXiv preprints, then validates those citations against DataCite records and DOI resolution. The pipeline consists of four steps:

1. The convert command is used to flatten Crossref snapshot to one row per reference, allowing for faster processing
2. Extract then scans references for arXiv identifiers
3. Invert transforms DOI-to-arXiv references to arXiv DOI-to-citing DOIs
4. With validate, we then verify that the extracted arXiv DOIs exist in DataCite or resolve

## Building

```bash
cargo build --release
```

The binary will be at `target/release/crossref-arxiv-citation-extraction`.

## Usage

### Convert

Flatten the Crossref snapshot to a Parquet file with one row per reference:

```bash
crossref-arxiv-citation-extraction convert \
  --input crossref-snapshot.tar.gz \
  --output references.parquet
```

This step only needs to be run once per snapshot. The output can be reused for multiple extraction runs.

Options:
- `--row-group-size` - Row group size for output (default: 250000)

### Extract

Extract arXiv IDs from the flattened references:

```bash
crossref-arxiv-citation-extraction extract \
  --input references.parquet \
  --output extracted.parquet
```

### Invert

Aggregate citations by arXiv ID:

```bash
crossref-arxiv-citation-extraction invert \
  --input extracted.parquet \
  --output inverted.parquet \
  --output-jsonl arxiv_citations.jsonl
```

Options:
- `--output-jsonl` - Also write JSONL output for use with the validate step

### Validate

Validate arXiv DOIs against DataCite records and DOI resolution:

```bash
crossref-arxiv-citation-extraction validate \
  --input arxiv_citations.jsonl \
  --records datacite-records.jsonl.gz \
  --output-valid arxiv_citations_valid.jsonl \
  --output-failed arxiv_citations_failed.jsonl
```

Options:
- `--concurrency` - Concurrent HTTP requests (default: 50)
- `--timeout` - Seconds per request (default: 5)

### Complete Workflow

```bash
# Flatten references (one-time per snapshot)
crossref-arxiv-citation-extraction convert \
  --input crossref-snapshot.tar.gz \
  --output references.parquet

# Extract arXiv IDs
crossref-arxiv-citation-extraction extract \
  --input references.parquet \
  --output extracted.parquet

# Aggregate by arXiv ID
crossref-arxiv-citation-extraction invert \
  --input extracted.parquet \
  --output inverted.parquet \
  --output-jsonl arxiv_citations.jsonl

# Validate against DataCite
crossref-arxiv-citation-extraction validate \
  --input arxiv_citations.jsonl \
  --records datacite-records.jsonl.gz \
  --output-valid arxiv_citations_valid.jsonl
```

## Output Formats

### Invert Output (JSONL)

One record per arXiv work, sorted by citation count:

```json
{
  "arxiv_doi": "10.48550/arXiv.2403.03542",
  "arxiv_id": "2403.03542",
  "reference_count": 5,
  "citation_count": 3,
  "cited_by": [
    {
      "doi": "10.1234/paper1",
      "matches": [
        {
          "raw_match": "arXiv:2403.03542",
          "reference": {"unstructured": "...", "key": "ref1"}
        }
      ]
    }
  ]
}
```

### Validate Output

Same format as invert output, split into valid and failed files based on whether the arXiv DOI could be verified.

### Parquet Files

Intermediate Parquet files can be inspected with tools like DuckDB or pandas:

```sql
-- Example: query with DuckDB
SELECT arxiv_id, citation_count
FROM 'inverted.parquet'
ORDER BY citation_count DESC
LIMIT 10;
```

## ArXiv ID Patterns

The extractor recognizes these arXiv identifier formats:

- Modern: `arXiv:2403.03542`, `arXiv.2403.03542v2`
- Old format: `arXiv:hep-ph/9901234`, `arXiv:cs.DM/9910013`
- DOI format: `10.48550/arXiv.2403.03542`
- URL format: `arxiv.org/abs/2403.03542`, `arxiv.org/pdf/2403.03542`

IDs are normalized to lowercase with version suffixes stripped.

## Validation Logic

1. Check if arXiv DOI exists in DataCite records (fast, local)
2. For unmatched DOIs, attempt HTTP HEAD request to doi.org
3. DOI is valid if DataCite contains it OR doi.org returns 2xx/3xx
