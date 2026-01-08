# crossref-arxiv-citation-extraction

CLI for extracting and validating arXiv references from Crossref data files.

## Overview

This tool processes Crossref snapshot data to identify works that cite arXiv preprints, then validates those citations against DataCite records and DOI resolution. It works by:

1. Streaming through the data file tar.gz, extracting arXiv references inline
2. Partition by arXiv ID prefix to allow for parallel processing
3. Inverting the data to aggregate citations by arXiv work
4. Finally, validating against DataCite records in the DataCite data file and through DOI resolution

## Building

```bash
cargo build --release
```

The binary will be at `target/release/crossref-arxiv-citation-extraction`.

## Usage

### Pipeline

Run the full extraction and validation pipeline:

```bash
crossref-arxiv-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --records datacite-records.jsonl.gz \
  --output arxiv_citations_valid.jsonl
```

Options:
- `--output-failed` - Output file for failed validations
- `--concurrency` - Concurrent HTTP requests for validation (default: 50)
- `--timeout` - Seconds per validation request (default: 5)
- `--keep-intermediates` - Keep partition files after completion
- `--temp-dir` - Directory for intermediate files (default: system temp)
- `--batch-size` - Batch size for memory management (default: 5000000)

### Validate

Validate a previously generated JSONL file without re-running the full pipeline:

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

## Output Format

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

## arXiv ID Patterns

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
