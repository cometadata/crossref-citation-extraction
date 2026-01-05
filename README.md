# crossref-arxiv-citation-extraction

CLI for extracting and validating arXiv references from Crossref data files.

## Overview

This tool processes Crossref snapshot data to identify works that cite arXiv preprints, then validates those citations against DataCite records and DOI resolution. The pipeline consists of three steps:

1. **Extract** - Scan Crossref snapshot for references containing arXiv identifiers
2. **Invert** - Transform from "DOI → arXiv refs" to "arXiv DOI → citing DOIs"
3. **Validate** - Verify arXiv DOIs exist in DataCite or resolve via doi.org

## Building

```bash
cargo build --release
```

The binary will be at `target/release/crossref-arxiv-citation-extraction`.

## Usage

### Full Pipeline

Run all three steps automatically with intermediate file management:

```bash
crossref-arxiv-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --records datacite-records.jsonl.gz \
  --output arxiv_citations_valid.jsonl
```

With all options:

```bash
crossref-arxiv-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --records datacite-records.jsonl.gz \
  --output arxiv_citations_valid.jsonl \
  --output-failed arxiv_citations_failed.jsonl \
  --threads 8 \
  --batch-size 1000 \
  --concurrency 100 \
  --timeout 10 \
  --keep-intermediates \
  --temp-dir /tmp/pipeline \
  --log-level INFO
```

### Individual Commands

#### Extract

Scan a Crossref snapshot tar.gz for DOIs that reference arXiv works:

```bash
crossref-arxiv-citation-extraction extract \
  --input crossref-snapshot.tar.gz \
  --output arxiv_references.jsonl
```

Options:
- `--threads` - Number of threads (0 = auto-detect)
- `--batch-size` - Records per batch for parallel processing (default: 1000)
- `--stats-interval` - Seconds between progress logs (default: 60)

#### Invert

Transform extract output to map arXiv DOIs to their citing works:

```bash
crossref-arxiv-citation-extraction invert \
  --input arxiv_references.jsonl \
  --output arxiv_citations.jsonl
```

#### Validate

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

## Output Formats

### Extract Output

JSONL with one record per citing DOI:

```json
{
  "doi": "10.1234/example.paper",
  "arxiv_matches": [
    {
      "id": "2403.03542",
      "raw": "arXiv:2403.03542",
      "arxiv_doi": "10.48550/arXiv.2403.03542"
    }
  ],
  "references": [
    {"unstructured": "Smith J. (2024). arXiv:2403.03542", "key": "ref1"}
  ]
}
```

### Invert Output

JSONL with one record per arXiv work, sorted by citation count:

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
