# crossref-citation-extraction

CLI for extracting and validating DOI references from Crossref data files.

## Overview

This tool processes Crossref snapshot data to identify works that cite other DOIs, validates those citations against Crossref and DataCite records, and outputs citation data organized by cited work.

Supported extraction modes:
- **all**: Extract all DOIs, validate against both Crossref and DataCite
- **crossref**: Extract DOIs, validate only against Crossref records
- **datacite**: Extract DOIs, validate only against DataCite records
- **arxiv**: Extract arXiv references using pattern matching, validate against DataCite

## Building

```bash
cargo build --release
```

The binary will be at `target/release/crossref-citation-extraction`.

## Usage

### Full Pipeline (All Sources)

```bash
crossref-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --datacite-records datacite-records.jsonl.gz \
  --source all \
  --output-crossref crossref_citations.jsonl \
  --output-datacite datacite_citations.jsonl
```

### Crossref Only

```bash
crossref-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --source crossref \
  --output-crossref crossref_citations.jsonl
```

### arXiv Only (Original Behavior)

```bash
crossref-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --datacite-records datacite-records.jsonl.gz \
  --source arxiv \
  --output-arxiv arxiv_citations.jsonl
```

### Options

**Source selection:**
- `--source all|crossref|datacite|arxiv` - Which source to extract and validate

**Input files:**
- `--input` - Crossref snapshot tar.gz (required)
- `--datacite-records` - DataCite records JSONL.gz (required for datacite/arxiv modes)

**Output files:**
- `--output-crossref` - Crossref citations output
- `--output-datacite` - DataCite citations output
- `--output-arxiv` - arXiv citations output (arxiv mode)
- `--output-*-failed` - Failed validation output for each source

**Index persistence:**
- `--save-crossref-index path.parquet` - Save Crossref DOI index
- `--load-crossref-index path.parquet` - Load Crossref DOI index
- `--save-datacite-index path.parquet` - Save DataCite DOI index
- `--load-datacite-index path.parquet` - Load DataCite DOI index

**Validation:**
- `--http-fallback crossref,datacite` - Enable HTTP validation for specified sources
- `--concurrency N` - Concurrent HTTP requests (default: 50)
- `--timeout N` - Seconds per request (default: 5)

**Other:**
- `--keep-intermediates` - Keep partition files after completion
- `--temp-dir` - Directory for intermediate files
- `--batch-size` - Batch size for memory management

### Standalone Validation

Validate a previously generated JSONL file:

```bash
crossref-citation-extraction validate \
  --input citations.jsonl \
  --datacite-records datacite.jsonl.gz \
  --source datacite \
  --output-valid valid.jsonl \
  --output-failed failed.jsonl
```

## Output Format

### Crossref/DataCite Output

```json
{
  "doi": "10.1234/example",
  "reference_count": 5,
  "citation_count": 3,
  "cited_by": [
    {
      "doi": "10.5678/citing-paper",
      "raw_match": "10.1234/example",
      "reference": {"unstructured": "..."}
    }
  ]
}
```

### arXiv Output

```json
{
  "arxiv_doi": "10.48550/arXiv.2403.03542",
  "arxiv_id": "2403.03542",
  "reference_count": 5,
  "citation_count": 3,
  "cited_by": [...]
}
```

## DOI Patterns

The extractor recognizes these DOI formats:
- Bare DOI: `10.1234/example`
- Prefixed: `doi:10.1234/example`
- URL: `https://doi.org/10.1234/example`, `http://dx.doi.org/10.1234/example`

## arXiv ID Patterns (arxiv mode)

- Modern: `arXiv:2403.03542`, `arXiv.2403.03542v2`
- Old format: `arXiv:hep-ph/9901234`, `arXiv:cs.DM/9910013`
- DOI format: `10.48550/arXiv.2403.03542`
- URL format: `arxiv.org/abs/2403.03542`

## Validation Logic

1. Check DOI against local index (fast, O(1) lookup)
2. For unmatched DOIs with `--http-fallback`, attempt HTTP HEAD to doi.org
3. DOI is valid if found in index OR doi.org returns 2xx/3xx
