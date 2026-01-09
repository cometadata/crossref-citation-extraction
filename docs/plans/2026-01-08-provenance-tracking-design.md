# DOI Provenance Tracking Design

## Overview

Track whether each DOI reference was explicitly asserted in Crossref metadata or mined from unstructured text, enabling quality-based filtering of citation data.

## Provenance Values

- `"publisher"` - DOI field present with `"doi-asserted-by": "publisher"`
- `"crossref"` - DOI field present with `"doi-asserted-by": "crossref"`
- `"mined"` - DOI extracted from `unstructured` text or other fields (no explicit DOI field, or DOI field without `doi-asserted-by`)

## Output Schema

Provenance is stored per `cited_by` entry:

```json
{
  "doi": "10.1234/example",
  "reference_count": 5,
  "citation_count": 3,
  "cited_by": [
    {
      "doi": "10.5678/citing-paper",
      "provenance": "publisher",
      "raw_match": "10.1234/example",
      "reference": {"unstructured": "...", "key": "ref1"}
    },
    {
      "doi": "10.9999/another-paper",
      "provenance": "mined",
      "raw_match": "doi.org/10.1234/example",
      "reference": {"unstructured": "See https://doi.org/10.1234/example"}
    }
  ]
}
```

The same cited DOI can appear with different provenance values from different citing works.

## CLI & Output Files

### Automatic File Splitting

When you specify an output file, the tool automatically produces three files:

```bash
--output-crossref results.jsonl
```

Produces:
- `results.jsonl` - All citations
- `results_asserted.jsonl` - Only `publisher` + `crossref` provenance
- `results_mined.jsonl` - Only `mined` provenance

### Applies to All Output Flags

- `--output-crossref` → `*.jsonl`, `*_asserted.jsonl`, `*_mined.jsonl`
- `--output-datacite` → same pattern
- `--output-arxiv` → same pattern
- `--output-crossref-failed` → same pattern
- `--output-datacite-failed` → same pattern
- `--output-arxiv-failed` → same pattern

### Example

```bash
crossref-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --datacite-records datacite.jsonl.gz \
  --source all \
  --output-crossref crossref_cites.jsonl \
  --output-datacite datacite_cites.jsonl
```

Produces 6 files:
- `crossref_cites.jsonl`, `crossref_cites_asserted.jsonl`, `crossref_cites_mined.jsonl`
- `datacite_cites.jsonl`, `datacite_cites_asserted.jsonl`, `datacite_cites_mined.jsonl`

## Extraction Logic

### Provenance Detection (per reference object)

```
1. If reference has "DOI" field:
   - If "doi-asserted-by" == "publisher" → provenance = "publisher"
   - If "doi-asserted-by" == "crossref" → provenance = "crossref"
   - If "doi-asserted-by" missing → provenance = "mined"

2. If DOI extracted from "unstructured" or other text fields:
   → provenance = "mined"
```

### Type Changes

```rust
// In extract/doi.rs
pub struct DoiMatch {
    pub doi: String,
    pub raw: String,
    pub provenance: Provenance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Provenance {
    Publisher,
    Crossref,
    Mined,
}
```

### Deduplication

If the same DOI is found both in the explicit `DOI` field AND in `unstructured` text within the same reference, prefer the asserted provenance (`publisher` > `crossref` > `mined`).

## Output Writing

During the inversion/aggregation phase:

1. Build the full citation record with provenance on each `cited_by` entry
2. Write to main output file (all records)
3. Filter and write to `_asserted` file (only entries where provenance is `publisher` or `crossref`)
4. Filter and write to `_mined` file (only entries where provenance is `mined`)

### Filtering Logic

For a given cited DOI record:
- `_asserted` file: Include only `cited_by` entries with `provenance` in [`publisher`, `crossref`]. If no entries remain, skip this record entirely.
- `_mined` file: Include only `cited_by` entries with `provenance` == `mined`. If no entries remain, skip this record entirely.

### Count Recalculation

When filtering `cited_by`, update `citation_count` to reflect the filtered list length:

```json
// Main file: citation_count = 3
// _asserted file: citation_count = 2 (if 2 were asserted)
// _mined file: citation_count = 1 (if 1 was mined)
```

### File Handles

Open all three output files at once, write records as they're processed to avoid multiple passes.

## Module Changes

```
src/
├── extract/
│   └── doi.rs          # Add Provenance enum, update DoiMatch struct
├── common/
│   └── types.rs        # Update CitationRecord cited_by structure
├── streaming/
│   ├── partition_writer.rs   # Store provenance in partition files
│   └── partition_invert.rs   # Carry provenance through aggregation
├── commands/
│   ├── pipeline.rs     # Extract provenance from reference objects,
│   │                   # write to split output files
│   └── validate.rs     # Handle provenance in standalone validation
└── cli.rs              # No changes needed (output paths auto-derived)
```

### Partition File Schema

Add `provenance` column to the intermediate Parquet files:

```
| citing_doi | cited_doi | raw_match | reference_json | provenance |
```

## Edge Cases

1. **Same DOI found twice in one reference** (explicit field + unstructured text): Keep the higher-quality provenance (`publisher` > `crossref` > `mined`)

2. **DOI field exists but no `doi-asserted-by`**: Treat as `mined` (can't confirm who asserted it)

3. **arXiv mode**: Most arXiv IDs are mined from text patterns. If an explicit `DOI` field contains `10.48550/arXiv.*` with `doi-asserted-by`, that's `publisher` or `crossref`.

4. **Empty split files**: If all citations for a source are asserted (or all mined), one split file may be empty. Write empty file rather than omit it.
