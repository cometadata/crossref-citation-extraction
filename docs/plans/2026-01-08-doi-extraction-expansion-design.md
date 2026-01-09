# DOI Extraction Expansion Design

## Overview

Expand the CLI from arXiv-only extraction to support any DOI, with validation against multiple sources (Crossref and DataCite). The tool will be renamed from `crossref-arxiv-citation-extraction` to `crossref-citation-extraction`.

## CLI Interface

### Core Flags

```bash
crossref-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \        # Source to scan (also Crossref validation)
  --datacite-records datacite.jsonl.gz \    # DataCite validation records
  --source all \                            # all|crossref|datacite|arxiv
  --output-crossref crossref_cites.jsonl \  # Crossref citations output
  --output-datacite datacite_cites.jsonl \  # DataCite citations output
  --http-fallback datacite                  # Enable HTTP for listed sources
```

### Source Modes

| Mode | Extraction | Validation | Required Inputs | Outputs |
|------|------------|------------|-----------------|---------|
| `all` | All DOIs | Crossref + DataCite | `--input`, `--datacite-records` | `--output-crossref`, `--output-datacite` |
| `crossref` | All DOIs | Crossref only | `--input` | `--output-crossref` |
| `datacite` | All DOIs | DataCite only | `--input`, `--datacite-records` | `--output-datacite` |
| `arxiv` | arXiv patterns | DataCite | `--input`, `--datacite-records` | `--output-arxiv` |

In `all` mode, arXiv DOIs appear in the DataCite output (arXiv is a subset of DataCite).

### Index Persistence Flags

- `--save-crossref-index path.parquet` - Save Crossref DOIs + prefixes
- `--load-crossref-index path.parquet` - Load Crossref DOIs + prefixes
- `--save-datacite-index path.parquet` - Save DataCite DOIs + prefixes
- `--load-datacite-index path.parquet` - Load DataCite DOIs + prefixes

### Other Flags (preserved)

- `--concurrency` - Concurrent HTTP requests for validation
- `--timeout` - Seconds per validation request
- `--keep-intermediates` - Keep partition files after completion
- `--temp-dir` - Directory for intermediate files
- `--batch-size` - Batch size for memory management
- `--output-crossref-failed`, `--output-datacite-failed`, `--output-arxiv-failed` - Failed validation outputs

## Extraction Logic

### DOI Extraction (all/crossref/datacite modes)

**Capture patterns:**
- Bare DOI: `10.\d{4,}/...`
- Prefixed: `doi:10.\d{4,}/...`
- URL forms: `doi.org/10....`, `dx.doi.org/10....`, `https://doi.org/10....`
- HTML-encoded: `10.1234%2Fexample`

**Cleanup phase:**
- Strip trailing punctuation (`. , ; ) ] >` etc.)
- Decode URL-encoded characters (`%2F` → `/`)
- Remove surrounding quotes/brackets
- Normalize to lowercase
- Deduplicate per reference

### arXiv Extraction (arxiv mode only)

Existing logic preserved unchanged:
- Patterns: `arXiv:XXXX`, `arXiv.XXXX`, `arxiv.org/abs/XXXX`, `arxiv.org/pdf/XXXX`, old format `hep-ph/XXXX`
- Convert matches to canonical arXiv DOI format (`10.48550/arXiv.XXXX`) for validation
- Strip version suffixes, normalize to lowercase

## Validation Flow

### Startup Phase

1. If `--load-datacite-index`: load DOIs + prefixes from Parquet
   Otherwise: stream `--datacite-records`, extract DOIs and prefixes
2. If `--load-crossref-index`: load DOIs + prefixes from Parquet
   Otherwise: build during extraction pass

### Single-Pass Extraction + Deferred Validation

1. Stream through input tar.gz, extract DOIs from references
2. Simultaneously collect Crossref DOIs + prefixes from records being scanned
3. Write extracted DOIs to partitioned intermediate files
4. After streaming completes, Crossref index is complete

### Validation Phase

1. **Fast prefix filter** - Discard DOIs with unknown prefixes (using loaded/built prefix sets)
2. **Crossref lookup** - Check against Crossref DOI set
3. **DataCite lookup** - Check against DataCite DOI set
4. **HTTP fallback** - For unmatched DOIs where `--http-fallback` includes that source

## Output Structure

### Crossref/DataCite Output Schema

```json
{
  "doi": "10.1234/example",
  "reference_count": 5,
  "citation_count": 3,
  "cited_by": [
    {
      "doi": "10.5678/citing-paper",
      "matches": [
        {
          "raw_match": "10.1234/example",
          "reference": {"unstructured": "...", "key": "ref1"}
        }
      ]
    }
  ]
}
```

### arXiv Output Schema (unchanged)

```json
{
  "arxiv_doi": "10.48550/arXiv.2403.03542",
  "arxiv_id": "2403.03542",
  "reference_count": 5,
  "citation_count": 3,
  "cited_by": [...]
}
```

### Deduplication & Filtering

- Drop self-citations: exclude where citing DOI equals cited DOI
- Deduplicate citations: if same citing DOI matches same cited DOI multiple times, aggregate matches under single `cited_by` entry
- Deduplicate DOIs across references: normalize before comparison

### Sorting

Output sorted by citation count (descending).

## Module Architecture

```
src/
├── main.rs
├── cli.rs                    # Updated CLI definitions
├── extract/
│   ├── mod.rs
│   ├── doi.rs               # New: DOI extraction patterns + cleanup
│   └── arxiv.rs             # Renamed from patterns.rs, existing arXiv logic
├── index/
│   ├── mod.rs               # New: index management
│   ├── builder.rs           # Build indexes from streaming data
│   └── persistence.rs       # Parquet save/load
├── streaming/
│   ├── mod.rs
│   ├── checkpoint.rs
│   ├── partition_writer.rs
│   └── partition_invert.rs
├── validation/
│   ├── mod.rs               # New: validation orchestration
│   ├── prefix_filter.rs     # Fast prefix check
│   ├── lookup.rs            # Index-based validation
│   └── http.rs              # HTTP fallback logic
├── commands/
│   ├── mod.rs
│   ├── pipeline.rs          # Updated for multi-source
│   └── validate.rs          # Updated for multi-source
└── common/
    ├── mod.rs
    ├── types.rs             # Updated types for generic DOI handling
    ├── logging.rs
    ├── progress.rs
    └── utils.rs
```

## Performance & Streaming Architecture

### Memory Efficiency

- Prefix sets: ~50-100K unique prefixes, small memory footprint
- DOI indexes: stored as Polars DataFrames with string interning, or as HashSets for lookup
- Partitioned intermediates: write to disk during extraction, process partition-by-partition during inversion

### Fused Streaming

- Single pass through input tar.gz builds Crossref index while extracting references
- DataCite records streamed once at startup to build index (or loaded from Parquet)
- Partition files processed incrementally, not loaded entirely into memory

### Parallelism

- Partition-based parallelism for inversion (existing)
- Concurrent HTTP requests for fallback validation (configurable via `--concurrency`)
- Index building is sequential (streaming), but validation lookups are O(1) hash lookups

### Polars Usage

- Index persistence via Parquet (native Polars format)
- Intermediate partition files remain Parquet
- Final aggregation/sorting uses Polars lazy frames

## CLI Examples

### Full Pipeline (all sources)

```bash
crossref-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --datacite-records datacite.jsonl.gz \
  --source all \
  --output-crossref crossref_citations.jsonl \
  --output-datacite datacite_citations.jsonl \
  --output-crossref-failed crossref_failed.jsonl \
  --output-datacite-failed datacite_failed.jsonl \
  --http-fallback datacite \
  --save-crossref-index crossref_index.parquet \
  --save-datacite-index datacite_index.parquet
```

### Subsequent Run with Cached Indexes

```bash
crossref-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --source crossref \
  --output-crossref crossref_citations.jsonl \
  --load-crossref-index crossref_index.parquet
```

### arXiv-Only (existing behavior)

```bash
crossref-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --datacite-records datacite.jsonl.gz \
  --source arxiv \
  --output-arxiv arxiv_citations.jsonl
```

## Breaking Changes

- Tool renamed from `crossref-arxiv-citation-extraction` to `crossref-citation-extraction`
- `--records` flag renamed to `--datacite-records`
- Output flags changed from `--output` to source-specific `--output-crossref`, `--output-datacite`, `--output-arxiv`
- `--source` flag required (default could be `all`)
