# crossref-citation-extraction

Rust CLI and Hugging Face Job that mine Crossref reference metadata for
citations to DataCite DOIs — matched by DOI or by the landing-page URL
registered with DataCite — and to arXiv papers, then aggregate the evidence
by cited work. One full run over the monthly Crossref snapshot publishes
both
[`cometadata/crossref-datacite-citations`](https://huggingface.co/datasets/cometadata/crossref-datacite-citations)
(every DataCite DOI) and
[`cometadata/crossref-arxiv-citations`](https://huggingface.co/datasets/cometadata/crossref-arxiv-citations)
(the arXiv subset, in its established schema).

## Overview

The extractor has two modes. Both stream the Crossref archive once, write
matched references to partitioned Parquet segments, and aggregate them by
cited work; they differ in what they look for and how they partition.

| Mode | Index command | Pipeline option | Targets | Partitioning |
| --- | --- | --- | --- | --- |
| Full DataCite (production) | `build-datacite-index` | `--datacite-index <bundle>` | every DOI in a DataCite monthly release, by DOI, registered landing-page URL, or arXiv identifier | hash of the complete canonical cited DOI (`fnv1a64-v1`), fixed power-of-two count |
| Legacy arXiv | `build-index` | `--arxiv-fst <fst>` or `--arxiv-records <jsonl>` | arXiv identifiers only | arXiv ID prefix |

```
Full:   DataCite release → build-datacite-index → bundle
        Crossref tar.gz → match DOI / URL / arXiv per reference → resolve shared URLs
                        → hashed partitions → aggregate by cited DOI → JSONL + ambiguity + report
Legacy: Crossref tar.gz → extract arXiv IDs → partition by arXiv prefix → aggregate → validate against FST
```

The two modes are selected explicitly. `--datacite-index` cannot be combined
with `--arxiv-fst`, `--arxiv-records`, or `--partitions-dir`; a legacy
invocation stays arXiv-only and its id-only FST input contract is unchanged.

## Building

```bash
cargo build --release
```

The binary will be at `target/release/crossref-citation-extraction`. The
declared Rust floor is 1.87 (`rust-version` in `Cargo.toml`); `Cargo.lock` is
pinned so that `cargo +1.87.0 check --locked` passes. That floor depends on
the lockfile pins, not on `Cargo.toml` alone: a plain `cargo update` can pull
dependency releases that need a newer compiler, so rerun
`cargo +1.87.0 check --locked` after any lockfile change.

## Full DataCite mode

### Inputs: the DataCite monthly release

`build-datacite-index --input` takes the root of one mirrored
[DataCite monthly data file](https://support.datacite.org/docs/datacite-monthly-data-file)
release (for example `datacite-monthly-data-file-2026-07/`), holding:

- `MANIFEST.json` — the release's file list with sizes and base64 SHA-256
  digests; every listed `dois/updated_YYYY-MM/*.jsonl.gz` and
  `dois/updated_YYYY-MM/*.csv.gz` entry is an expected input and is verified
  against its digest.
- `STATUS.json` — must report `"status": "Complete"` for the release month.
  Folder months are not checked against it: the 2026-07 release, generated
  on 1 August, also lists `dois/updated_2026-08/` for the DOIs updated
  before the snapshot, and that folder is an input like any other.
- `dois/updated_YYYY-MM/part_NNNN.jsonl.gz` — DOI metadata records. Both
  unwrapped `{"id", "attributes": {...}}` records and API-style
  `{"data": {...}}` singletons are accepted; the registered landing page is
  read from `attributes.url` only (never from `relatedIdentifiers`,
  `contentUrl`, or other metadata links).
- `dois/updated_YYYY-MM/<month>.csv.gz` — the DOI state reports. These list
  both `findable` and `registered` DOIs, including DOIs that have no JSONL
  metadata record.

**The DataCite release defines DOI and URL coverage.** The DOI membership
set is the union of every valid DOI in the JSONL records and the CSV state
reports; a DOI that is not in the selected release can never be matched, and
a DOI with no usable URL can still match by DOI. The release is a snapshot,
not a historical URL registry: a landing page that changed after an older
citation was written stays unmatched. There are no live DOI lookups,
redirects, crawling, fuzzy title matching, or repository-specific URL
rewrites. Production uses the complete release, CSV reports included, and
fails rather than silently falling back to JSONL-only coverage when an
expected input is missing; a missing or unreadable listed file, truncated
gzip data, a malformed record, an unknown state-report state, or two valid
but conflicting DOI identifiers inside one record all fail the build. Null
or invalid landing-page values are counted without discarding the DOI. A
state-report row or metadata record whose DOI is not syntactically valid
(the registry spells a handful with whitespace inside the suffix, e.g.
`10.3929/ethz-a- 000142255`) is counted as `invalid_dois` and skipped
whole: it is never repaired into a guessed spelling, its landing page is
not indexed, and it can never be matched.

### Build the index bundle

```bash
./target/release/crossref-citation-extraction build-datacite-index \
  --input /mnt/shared-storage/datacite-monthly-data-file-2026-07 \
  --output ./datacite-bundle \
  --sort-memory-mb 4096
```

The builder scans the release once, emits compact DOI and URL/DOI records,
externally sorts and deduplicates them within `--sort-memory-mb` (default
4096 MiB), and writes an immutable bundle into a new directory (it refuses an
output that already holds a completed bundle). Its only other option is
`--log-level` (`DEBUG`, `INFO`, `WARN`, `ERROR`; default `INFO`):

| File | Contents |
| --- | --- |
| `dois.fst` | memory-mapped FST set of canonical DOIs |
| `hosts.fst` | registered hosts, used to reject URL candidates cheaply before a full lookup |
| `urls.fst` | normalized landing-page URL keys, each pointing into the sidecar |
| `url-targets.bin` | memory-mapped sidecar with the sorted, distinct candidate DOIs per URL |
| `_INDEX_COMPLETE.json` | manifest: release, source inventory digest, index format and normalization versions, per-file sizes and SHA-256 digests, counts of DOIs, URLs, missing/invalid URLs, skipped invalid DOIs, and shared URLs; written last, only after every artifact is verified |

Candidates that share an authority and path across `http` and `https` keep
the union of their DOI mappings, so a scheme-less reference cannot hide an
ambiguity. Workers share the memory-mapped bundle read-only; nothing copies
the registry into process memory.

### Run the pipeline

```bash
./target/release/crossref-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --datacite-index ./datacite-bundle \
  --output-dir ./results \
  --temp-dir ./work
```

A successful run ends with `event=phase phase=pipeline status=completed` and
exits 0; every failure exits nonzero and leaves no `extraction-report.json`.

#### DataCite-mode options

| Option | Description |
| --- | --- |
| `--input` | Crossref snapshot (tar.gz streamed without extraction, a directory of JSON files, or one JSON file). Required; its stem names the outputs |
| `--datacite-index` | Bundle directory holding `_INDEX_COMPLETE.json`. Selects DataCite mode; rejected together with `--arxiv-fst`, `--arxiv-records`, or `--partitions-dir` |
| `--run-identity` | JSON object naming the run. Required and validated keys: `extraction_mode` (`datacite`), `partition_algorithm` (`fnv1a64-v1`), `partitions` (equal to `--partitions`), `bundle_manifest_sha256` (SHA-256 of the bundle's `_INDEX_COMPLETE.json` bytes). Its canonical SHA-256 (keys sorted recursively, no whitespace) is stamped into the checkpoint header, every aggregation `_COMMIT.json`, and the report. Without it a derived identity is used for local runs |
| `--output-dir` | Directory for the output files (default: current directory) |
| `--temp-dir` | Working directory for partitions, ambiguity segments, the checkpoint, and aggregation commits (default: a fresh `datacite-extraction-<uuid>` under the system temp). A non-resume run first removes exactly the pipeline-owned entries there; after success the entries are removed and the directory itself is deleted if it is then empty |
| `--outputs` | `valid,failed,publisher,crossref,mined` or `all` (default: all). The ambiguity file is always written |
| `--provenance` | Keep only matches with these provenances (`publisher,crossref,mined`, case-insensitive; default: all). Dropped rows are counted as `filtered_provenance` |
| `--resume` | Continue from the checkpoint in `--temp-dir` (see [Local-only extraction resume](#local-only-extraction-resume)) |
| `--checkpoint-interval` | Commit extraction once this many members are pending (default 2000; must be positive). Secondary to the buffered-byte trigger |
| `--keep-intermediates` | Keep the partitions, ambiguity segments, completed checkpoint, and aggregation commits |
| `--batch-size` | No effect in DataCite mode (segment size is fixed; the byte budget governs flushing) |
| `--log-level` | `DEBUG`, `INFO`, `WARN`, `ERROR` (default `INFO`) |

Argument problems are rejected before anything is created: mixed index
options, `--partitions-dir`, a partition count that is not a power of two,
`--aggregation-memory-mb` below `--writer-memory-mb`, an unknown
`--provenance` value, `--checkpoint-interval 0`, zero for any resource flag
(`--merge-fan-in` below 2), a bundle that fails to load, or an identity file
that fails validation.

#### Resource flags

All memory flags are in MiB. The first nine are extractor flags; the last two
are job-level settings that the Python orchestration forwards to the bundle
build (`build-datacite-index --sort-memory-mb`) and to the DuckDB transform.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--partitions` | 256 | Hashed partition count (a positive power of two); recorded with the hash algorithm in the run identity |
| `--writer-memory-mb` | 4096 | Partition writer buffer budget; the writer flushes its largest buffers before exceeding it, and commits fire at half of it |
| `--result-batch-mb` | 16 | Largest result batch a worker sends to the writer; a single row over this budget is an error |
| `--queue-batches` | 8 | Result batches buffered between the workers and the writer |
| `--aggregation-memory-mb` | 16384 | Aggregation budget; a partition whose segments exceed it is sorted externally, and every segment must fit it |
| `--merge-fan-in` | 32 | Most sorted runs merged in one pass (multi-pass merges when there are more) |
| `--sort-workers` | 4 | Threads sorting segments concurrently |
| `--lookup-memory-mb` | 64 | Budget for one landing-page candidate lookup; a URL whose candidate list exceeds it is a fatal error rather than a truncated result |
| `--max-entry-mb` | 512 | Largest Crossref tar member accepted; a larger member is a fatal source error |
| `--index-sort-memory-mb` | 4096 | (job level) external-sort budget of the bundle build |
| `--duckdb-memory-mb` | 32768 | (job level) DuckDB memory limit for the dataset transform |

Guidance: the extractor requires `--aggregation-memory-mb >=
--writer-memory-mb`; keep it at least **2 × writer** (the benchmark refuses
smaller ratios), because a segment's decoded-size estimate can exceed the
writer budget that produced it by an abandoned Parquet dictionary page.
Extraction runs `CPUs − 2` workers (at least one); the benchmark's memory
estimate for extraction is `writer + aggregation + result_batch ×
queue_batches + lookup × workers + max_entry + bundle size`, and for the
transform `duckdb × 1.25` (pyarrow merge buffers live outside DuckDB's limit).
Partition sizes are independent of DOI prefixes because the hash covers the
complete DOI. Nothing is ever truncated to satisfy a limit; the run fails
explicitly when a configuration cannot process its input.

#### Outputs

`<stem>` is the `--input` stem (`crossref.tar.gz` → `crossref`; a directory →
its name; `all.json.tar.gz` → `all.json`).

| File | Contents |
| --- | --- |
| `<stem>_valid.jsonl` | Every confirmed citation, one line per cited DOI |
| `<stem>_publisher.jsonl`, `<stem>_crossref.jsonl`, `<stem>_mined.jsonl` | The same rows restricted to one provenance |
| `<stem>_failed.jsonl` | arXiv identifiers the arXiv parser accepted but the release does not hold (`match_type: arxiv`) |
| `<stem>_ambiguous_urls.jsonl` | Unresolved shared-URL evidence (always written; empty when none), sorted by `(citing_doi, ref_index, url)` |
| `extraction-report.json` | Written only on success; any previous report is removed when a run starts |

Citation rows are not globally sorted:

```json
{"doi": "10.1234/a", "citation_count": 2, "cited_by": [
  {"doi": "10.5678/citing", "ref_index": 0, "provenance": "publisher", "match_type": "doi",
   "raw_match": "10.1234/a", "reference": {"key": "ref0", "DOI": "10.1234/a", "doi-asserted-by": "publisher"}},
  {"doi": "10.5678/other", "ref_index": 3, "provenance": "mined", "match_type": "url",
   "raw_match": "https://example.org/a", "reference": {"key": "ref3", "unstructured": "See https://example.org/a."}}]}
```

Ambiguity rows:

```json
{"citing_doi": "10.5678/citing", "ref_index": 1, "url": "https://example.org/shared",
 "raw_match": "https://example.org/shared", "candidate_dois": ["10.1234/b", "10.1234/c"],
 "reference": {"key": "ref1", "URL": "https://example.org/shared"}}
```

`extraction-report.json`:

```json
{
  "kind": "datacite-extraction-report",
  "schema_version": 2,
  "identity_sha256": "<64 hex>",
  "completed": true,
  "files_processed": 1,
  "fatal_source_errors": 0,
  "stats": {
    "items": 1, "references": 3, "confirmed_matches": 3,
    "doi_matches": 1, "url_matches": 1, "arxiv_matches": 1,
    "ambiguous_urls": 0, "invalid_arxiv": 0,
    "unmatched_dois": 0, "unmatched_urls": 0, "degenerate_urls": 0,
    "malformed_references": 0, "malformed_fields": 0,
    "filtered_self_citations": 0, "filtered_provenance": 0
  },
  "aggregation": {
    "partitions_processed": 2, "valid_count": 2, "failed_count": 0, "total_citations": 3,
    "publisher_citations": 1, "crossref_citations": 0, "mined_citations": 2,
    "in_memory_partitions": 2, "external_partitions": 0, "reused_partitions": 0
  }
}
```

`doi_matches + url_matches + arxiv_matches == confirmed_matches`. An
unreadable archive, a member over `--max-entry-mb`, or invalid top-level JSON
in a member is fatal; a malformed individual reference (including one nested
too deeply for the JSON parser inside an otherwise valid member) or a
non-string searchable field is counted (`malformed_references`,
`malformed_fields`) and the rest of the work keeps being mined. Progress goes
to stdout as `event=phase`, `event=progress` (members, every counter,
throughput, buffered bytes), `event=checkpoint`, `event=resource_estimate`,
`event=identity`, `event=resume`, and `event=cleanup` lines; the
`phase=index_load` and `phase=aggregation` completion lines carry their own
`elapsed_s`.

### Matching and normalization rules

For each reference object the fields `DOI`, `URL`, `article-title`,
`journal-title`, and `unstructured` are visited in that order. A structured
`DOI`/`URL` value is checked as a whole first, then the arXiv parser runs
when the field has arXiv context, then embedded URL and DOI tokens are
scanned in the text. JSON escapes are decoded while parsing; HTML entities in
text are decoded for matching with offsets preserved, so `raw_match` is
always a substring of the original evidence. Scanning never crosses a field
boundary. One implementation (`src/common/identifiers.rs`) normalizes both
the registry during the bundle build and the candidates during matching.

**DOIs.** Recognized forms are the structured `DOI` field, bare `10.…`
tokens in text (a token runs to the next whitespace or the next
`10.<registrant>/` start, so comma-joined DOIs are separate tokens; a token
cut short by such a start is first tried whole, up to the next whitespace,
and a registered whole token wins and claims its text, so
`10.5555/data/10.2/set` still matches and `10.7777/outer.10.8888/inner`
cites only the outer DOI when both are registered; a DOI-shaped token longer
than 1,024 bytes is counted as unmatched and never looked up), `doi:`
wrappers (case-insensitive, whitespace after the colon allowed), and
resolver URLs on `doi.org`, `dx.doi.org`, or
`www.doi.org` with or without a scheme (the DOI is the resolver path,
percent-decoded once; a query or fragment is not part of it). The canonical
form is `10.<registrant>/<suffix>` with ASCII letters lowercased; the
registrant is digits, optionally in dot-separated sub-elements, and there is
no allowlist of DataCite prefixes. Whitespace and control characters make a
candidate invalid; non-ASCII suffix characters are kept. The complete token
is looked up first; only for trailing citation punctuation are bounded
boundary variants tested (one trailing boundary character removed at a time,
a small fixed number of times, never a closing bracket that balances an
opener inside the token), and the longest registered DOI wins. Arbitrary
suffix characters are never truncated to force a match. Version suffixes
are removed only by the arXiv rules (`v2` on an arXiv id); general DOI
versions such as `10.1234/data.v1` and `10.1234/data.v2` are distinct DOIs.

**Landing-page URLs.** Explicit `http`/`https` URLs and scheme-less
host/path references are recognized. Normalization lowercases the scheme
and host, drops default ports (explicit non-default ports stay), decodes
percent-encoded unreserved characters and uppercases the hex digits of the
encodings that remain, turns an empty path into `/`, and drops an empty `?`
or `#`. It preserves path case, non-root trailing slashes, query parameters
and their order, nonempty fragments, and `www`; tracking parameters are
never stripped. Lookups are exact on the complete normalized key, never on a
domain or path prefix, after a cheap registered-host check. Explicit `http`
and `https` URLs are distinct keys. A scheme-less reference is looked up
under both schemes and its candidates are the union of both, so it can be
ambiguous when the two registrations differ. Surrounding citation
punctuation is removed only through the same bounded variants, validated
against the URL index.

**Degenerate landing pages.** Some records register a landing page that
identifies nothing, such as `https://doi.org/`, `http://dx.doi.org/`, or
`http://www`. A normalized lookup key is skipped (never looked up, so it
yields neither a match nor an ambiguity row) when (a) its host is a DOI
resolver (`doi.org`, `dx.doi.org`, `www.doi.org`), whatever the path; (b) it
is a bare site root: path exactly `/` with no query and no fragment; or (c)
its host is a domain name without a dot (`www`, `localhost`). IP-address
hosts are subject only to (b). The rule applies at match time to every key,
including both keys of a scheme-less reference. The index bundle is
unchanged, and a record registered with such a URL can still be matched by
DOI; a resolver URL with a DOI path, like `https://doi.org/10.1234/a`, is DOI
evidence as before. Skipped candidates are counted in `degenerate_urls`
(once per distinct URL per reference, like `unmatched_urls`) and never in
`unmatched_urls`. The predicate is `is_degenerate_landing_url` in
`src/common/identifiers.rs`.

**arXiv identifiers.** The legacy recognizers are kept unchanged (see
[arXiv ID patterns](#arxiv-id-patterns)); their results are converted to the
canonical `10.48550/arxiv.<id>` DOI before deduplication, and must be in the
release's DOI set like every other candidate. The full run may find further
arXiv citations through DOI or URL matching and retains every previously
recognized valid arXiv citation; the arXiv projection renders the
established `10.48550/arXiv.<id>` form.

### Ambiguity exclusion

Direct DOI and arXiv candidates are collected and validated before any
landing page is resolved. A URL registered to exactly one DOI maps to it
(`match_type: url`, provenance `mined`). A **shared URL** — one registered
landing page for several DOIs — resolves only when the intersection of its
candidate DOIs and the DOIs the same reference identified independently
contains exactly one member. An unrelated DOI in the reference does not
disambiguate it, and when the intersection holds several members the
direct DOI citations still count individually while the URL evidence stays
ambiguous. Unresolved evidence is written to `<stem>_ambiguous_urls.jsonl`
(one row per normalized URL per reference, repeated sightings deduplicated,
`candidate_dois` sorted) and published as the dataset's `ambiguous_urls`
configuration. It contributes to no citation count. Candidates absent from
the release only increment the `unmatched_dois` / `unmatched_urls`
diagnostics, and degenerate landing-page candidates the `degenerate_urls`
diagnostic; they are never written out.

### Counting: reference count versus citation count

A confirmed match is unique by canonical citing DOI, original reference
index, and canonical cited DOI. DOI, URL, and arXiv evidence for the same
tuple never multiplies it; distinct reference positions citing the same DOI
are distinct tuples; self-citations (canonical DOI equality) are excluded.
One representative match is chosen deterministically: highest provenance,
then structured DOI evidence, arXiv evidence, other DOI evidence, URL
evidence, then field order and substring position — so an arXiv
`raw_match` stays stable when general mining finds the same target.

Provenance is `publisher` or `crossref` only when an equivalent DOI (wrappers
and case ignored; an arXiv version suffix is not stripped) is in the
structured `DOI` field with a recognized `doi-asserted-by` value; URL-derived
matches and every other text match are `mined`.

For a published work, `citation_count` is the number of distinct citing
works and equals `len(cited_by)`; `reference_count` is the number of
confirmed reference tuples and equals the sum of the match-list lengths. A
citing work's provenance is its strongest retained match, and each
configuration filters by provenance before recounting, so across
configurations **`all` references = `asserted` + `mined` references**, while
citation totals are not additive (one citing work can have matches in
both). The arXiv projection equals the arXiv-DOI subset of each general
configuration.

### Local-only extraction resume

Extraction is checkpointed in `--temp-dir/datacite-extraction.checkpoint`
(a JSONL file: a header carrying the identity digest, selected outputs and
provenances; one record per completed member; a completion marker). A
commit — every buffered partition segment and the open ambiguity segment
fsynced and renamed into place, then the pending member records appended —
fires when the writer holds half of `--writer-memory-mb` in rows
(`trigger=bytes`) or when `--checkpoint-interval` members are pending
(`trigger=members`), and once at the end (`trigger=final`). A member is
recorded only after all of its rows are durable.

`--resume` with the same `--input`, `--temp-dir`, identity, `--outputs`,
and `--provenance` skips the recorded members and replays at most one
commit's worth of members; their rows may already be on disk, and the
aggregators deduplicate by `(cited, citing, ref_index)` and
`(citing, ref_index, url)`. A header mismatch is refused; a torn trailing
record is dropped; corruption before that is an error. When the checkpoint
is complete, extraction is skipped and any aggregation commit
(`partitions/_aggregation/<partition>/_COMMIT.json`, stamped with the same
identity) is reused rather than rewritten, so a crash between writing a
partition's output and recording its completion never duplicates it. A
run without `--resume` removes the pipeline-owned entries first.

This is local-only. The Hugging Face job never passes `--resume`: Job disk
is ephemeral, so there is no cross-job mid-extraction recovery. Jobs
recover at phase level instead — a completed, verified derived generation is
reused through `_EXTRACTION_COMPLETE.json` (see
[Reuse and paired publication recovery](#reuse-and-paired-publication-recovery)).

## Legacy arXiv mode

The original arXiv-only pipeline is unchanged and remains available.

### Build arXiv index

Pre-build a finite state transducer (FST) index from arXiv (DataCite)
records for faster pipeline runs:

```bash
./target/release/crossref-citation-extraction build-index \
  --input arxiv-datacite-records.jsonl.gz \
  --output arxiv.fst
```

`build-index` accepts only `--input`, `--output`, and `--log-level` (default
`INFO`); see [Input formats](#input-formats) for what `--input` may be.

### Run pipeline

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

### Legacy pipeline options

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

### Input formats

Crossref input (`--input`) accepts tar.gz archives (streamed without extraction), directories containing JSON files, or single JSON files for testing.

arXiv records (`--arxiv-records`) accepts gzipped JSONL files, uncompressed JSONL files, directories with `updated_*/` subdirectories, or directories containing JSONL.gz files.

### Legacy output format

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

### arXiv ID patterns

The extractor recognizes modern format (`arXiv:2403.03542`, `arXiv.2403.03542v2`), old format (`arXiv:hep-ph/9901234`, `arXiv:cs.DM/9910013`), DOI format (`10.48550/arXiv.2403.03542`), and URL format (`arxiv.org/abs/2403.03542`).

References must contain "arxiv" context to match—bare `2403.03542` won't match.

### Legacy architecture

The tool uses a streaming architecture to process large datasets with bounded memory. Tar.gz input files are streamed without full extraction. References are written to per-partition parquet segments keyed by the arXiv ID (first 4 characters of the ID after `arXiv.`, with `/` replaced by `_`). Small partitions are aggregated in memory; large partitions (>100 segments) via parallel per-segment sorting and a k-way streaming merge. Validation uses a memory-efficient finite state transducer for arXiv ID lookup.

A read error in the input archive — a truncated or corrupt tar.gz, for example — aborts the run with an error rather than logging a warning and exiting successfully with partial output.

Long-running pipelines can be resumed with `--resume` from checkpoint files stored in the partition directory.

## Datasets

Both datasets are produced from one extraction by the Python transform
(`citations_pipeline/datacite_publish.py`) and published with
`citations_pipeline/paired_publish.py`. Reference values are stringified
with the established convention (strings verbatim, everything else as
compact key-sorted JSON), absent keys are null, field order is
deterministic, and each configuration's `reference` struct declares only
the keys observed in that configuration's own references. Works are
ordered by descending citation count then DOI, citing works by DOI, and
matches by original reference position, so results are logically identical
regardless of worker completion order or aggregation path.

### `cometadata/crossref-datacite-citations` (general)

Public, CC0. Files live at `data/<config>/part-NNNNN.parquet` (a new shard
before 1 GiB of Arrow data; at least one schema-bearing file per
configuration, empty configurations included).

| Configuration | Contents |
| --- | --- |
| `all` (default) | All confirmed matches |
| `asserted` | Publisher and Crossref provenance matches |
| `mined` | Mined matches |
| `ambiguous_urls` | Unresolved shared-URL evidence, separate from every citation count |

Citation configurations:

| Column | Type | Meaning |
| --- | --- | --- |
| `doi` | string | Canonical lowercase DataCite DOI |
| `citation_count` | int64 | Distinct citing works in this configuration (`= len(cited_by)`) |
| `cited_by` | list of struct | `doi` (citing work), `matches` (list of struct), `provenance` (the citing work's strongest match) |
| `reference_count` | int64 | Confirmed reference tuples (`= sum(len(matches))`) |

Each `matches` entry has `provenance`, `match_type` (`doi`, `arxiv`, or
`url`), `raw_match`, and `reference` (a string-valued struct of the original
reference). The `ambiguous_urls` configuration has `citing_doi`,
`ref_index` (int64), `url`, `raw_match`, `candidate_dois` (list of string,
sorted), and `reference`.

```python
from datasets import load_dataset
all_citations = load_dataset("cometadata/crossref-datacite-citations", "all")
ambiguous = load_dataset("cometadata/crossref-datacite-citations", "ambiguous_urls")
```

Each published revision also carries `metadata/pipeline-manifest.json`
(the run identity, snapshot, extractor version, configuration files,
counts, ambiguity row count, and the size and SHA-256 of every data file
and the card) and a dataset card with the counts, the build identity, and
the ambiguity policy.

### `cometadata/crossref-arxiv-citations` (legacy schema)

The arXiv subset of the same extraction, in the published schema:
`data/{all,asserted,mined}.parquet`, top-level columns `arxiv_doi`
(rendered as `10.48550/arXiv.<id>`), `arxiv_id`, `citation_count`,
`cited_by`, `reference_count`, and nested match fields `provenance`,
`raw_match`, `reference` — no `match_type`. Its reference struct keys are
derived per arXiv configuration, without keys present only in non-arXiv
matches. The transform verifies that this projection equals the arXiv-DOI
subset of each general configuration (same works, per-work counts, and
citers) before anything is published.

## Hugging Face Job

A production workload that runs this extractor against the monthly Crossref
Metadata Plus snapshot and updates both datasets. Python orchestration lives
in `citations_pipeline/`; the job entry is `jobs/pipeline.py`; runs are
submitted manually with `submit_job.py`, which submits the full DataCite
pipeline by default.

### How it works

1. **Verify source and resolve inputs** — the staged source tree must hash
   to the submitted `--source-digest`. The Crossref snapshot is the one
   mirrored by the affiliations pipeline
   (`shared-storage/crossref-affiliations-latest/`, consumed via its
   `_PIPELINE_COMPLETE.json` contract); if no valid mirror exists and
   `CROSSREF_API_TOKEN` is set, the job downloads the snapshot itself into
   `raw-generations/`. The newest complete DataCite monthly release under
   the bucket root (`datacite-monthly-data-file-<release>/` with
   `_TRANSFER_COMPLETE.json` and a `Complete` status) is selected unless
   `--datacite-release` pins one; a pinned release that is incomplete fails
   rather than falling back to an older one.
2. **Bundle** — `indexes/<release>-v2-<inventory-digest>/` is reused after
   its pointer and every file are verified by size and digest (a transient
   read error while copying a cached file is retried twice before the cache
   is rejected); otherwise the bundle is built in-job with
   `build-datacite-index`, verified, and persisted as a new immutable
   generation with the pointer written last.
3. **Stage** — the archive is copied to local disk while its SHA-256 is
   checked against the mirror's recorded digest.
4. **Extract** — this crate is built in-job (`cargo build --release
   --locked`) from the staged source volume and runs `pipeline
   --datacite-index` once over the complete snapshot with the job's
   `--run-identity` and resource flags; the report is validated against the
   identity digest.
5. **Transform and prepare** — both datasets, the `ambiguous_urls`
   configuration, and both cards and manifests are built and validated
   (count invariants, the arXiv projection check); production requires
   nonempty `all` results for both datasets.
6. **Persist** — the derived generation (prepared datasets, the six raw
   outputs as `.jsonl.zst`, the extraction report, and its own copy of the
   extraction marker) is uploaded, read back through the mount and
   re-hashed, and only then is `_EXTRACTION_COMPLETE.json` written. Files
   the mount does not show yet (missing, or the wrong size) are checked
   again with backoff for about five minutes (`upload_verification_retry`
   events) before the upload fails; a digest mismatch fails at once.
7. **Publish** — every prepared file is hashed against its manifest locally,
   then the DataCite dataset is committed and verified, its receipt
   journaled, then the arXiv dataset likewise.
8. **Activate** — `_PIPELINE_COMPLETE.json` records both commits; then every
   derived and raw generation under the owned prefix other than this run's
   is deleted.

The run identity that names every reusable artifact covers the Crossref
archive digest, the DataCite release and its inventory digest, the bundle
manifest digest, the normalization/extraction/index versions, the runtime
source digest, the extraction mode, the partition algorithm and count, and
both publication schema versions. Legacy arXiv caches, markers, and
checkpoints never satisfy it.

### Bucket layout

Durable state lives under
`hf://buckets/cometadata/shared-storage/crossref-datacite-citations-latest/`:

```
indexes/<release>-v2-<inventory_sha256>/
    _BUNDLE_CURRENT.json                       pointer to the verified generation
    generations/<uuid>/                        dois.fst hosts.fst urls.fst url-targets.bin _INDEX_COMPLETE.json
raw-generations/<snapshot>-<run>/              all.json.tar.gz, _TRANSFER_COMPLETE.json (fallback download only)
derived-generations/<snapshot>-<run>/
    datacite/README.md
    datacite/metadata/pipeline-manifest.json
    datacite/data/{all,asserted,mined,ambiguous_urls}/part-NNNNN.parquet
    arxiv/README.md
    arxiv/metadata/pipeline-manifest.json
    arxiv/data/{all,asserted,mined}.parquet
    outputs/{valid,failed,publisher,crossref,mined,ambiguous_urls}.jsonl.zst
    extraction-report.json
    _EXTRACTION_COMPLETE.json                  generation copy of the marker
    _PUBLICATIONS.json                         receipt journal ({"datacite": ..., "arxiv": ...}; partial allowed)
benchmarks/<run-id>/                           benchmark-report.json, production-settings.json, sample-outputs/
_EXTRACTION_COMPLETE.json                      verified reusable derived data (identity, files with sizes and digests, counts)
_PIPELINE_COMPLETE.json                        authoritative; written only after both publications are verified
_TRANSFER_COMPLETE.json                        fallback download marker (only when the job downloaded the snapshot itself)
```

`<run>` is a 32-hex run id generated per job. The legacy pipeline's prefix
`crossref-arxiv-citations-latest/` (its `fst/<release>-v1/arxiv.fst` cache and
generations) is left intact and is never read by the full pipeline.

### Reuse and paired publication recovery

Retried jobs never trust a marker by existence. A valid
`_EXTRACTION_COMPLETE.json` with an equal identity makes the job fetch every
listed derived file, verify each size and SHA-256 against both the marker
and the dataset manifests, and publish only what is missing; any mismatch
is a named error, never a silent recompute. When the root marker is absent
or names another identity (a job died, or the marker upload failed, after
the generation was uploaded), the job looks in `derived-generations/*` for a
generation whose own `_EXTRACTION_COMPLETE.json` carries the current
identity, names its own prefix and the resolved snapshot, lists every
required output, and verifies file by file; the newest such generation
passes the same reuse checks and then becomes the root marker
(`extraction_generation_adopted`) instead of a re-extraction. Partial,
corrupt, or mismatched generations are reported
(`extraction_generation_rejected`) and never adopted. `--force` recomputes
the reusable phases, never adopts, and keeps every verification.

Each dataset update is one parent-SHA-guarded commit carrying its data
files, card, and `metadata/pipeline-manifest.json`; the manifest's digest
is the publication's artifact digest and is embedded in the commit. A
parent conflict fails the publication instead of overwriting a concurrent
change. The two commits are independent, so a job can end with only the
DataCite dataset updated; that is reported honestly, the pipeline marker is
not written, and the next run publishes only the other destination. The
receipt journal `_PUBLICATIONS.json` records each verified commit; if the
job died after committing but before journaling, the next run recognizes
the completed publication by finding the manifest digest already at the
repository head and verifies it without a new commit. Before activation, and
before answering "no-op" to a rerun with the same identity, the current
repository files are compared with the recorded artifacts; if they no
longer match, the run reports a concurrent change rather than treating a
historical commit as the current dataset; a failure to read the Hub during
that check is reported as that failure, not as a concurrent change. A
result is a success only when the commit was verified and the marker
written — never because a dataset exists or a commit request returned an
id.

Operational limits:

- **One job at a time.** Cleanup after activation deletes every derived and
  raw generation under `crossref-datacite-citations-latest/` except the
  current run's, including one another job may still be writing. Concurrent
  full-pipeline jobs are unsupported.
- **A failed `--force` run needs another `--force`.** A `--force` run that
  published the DataCite dataset and then failed before activation leaves
  `_PIPELINE_COMPLETE.json` naming the previous commits with the same
  identity, so a plain rerun reports a concurrent change on the DataCite
  repository. Rerun with `--force`: the recomputed artifact is recognized at
  the DataCite head without a new commit, and the arXiv dataset is
  published.
- **The legacy pipeline cannot publish over the paired dataset.**
  `--legacy-arxiv` checks `cometadata/crossref-arxiv-citations` before any
  work and again at its commit's parent revision; if the repository carries
  `metadata/pipeline-manifest.json` with `manifest_version` 2 (or the
  manifest cannot be read), the full DataCite pipeline owns the dataset and
  the legacy run fails instead of committing data and a card over it.

### Submitting jobs

```bash
hf auth login                                                     # once
uv run submit_job.py --smoke                                      # deterministic smoke: no bucket, no secrets
uv run submit_job.py --benchmark                                  # measure the workload, write production settings
uv run submit_job.py --settings /tmp/datacite-production-settings.json   # full DataCite run at measured settings
uv run submit_job.py --legacy-arxiv                               # the legacy arXiv-only pipeline (refused once the full pipeline owns the arXiv dataset)
uv run submit_job.py                                              # full DataCite run at the default settings
```

| Mode | Flag | Flavor / timeout | Label `pipeline=` | Bucket mount | Secrets |
| --- | --- | --- | --- | --- | --- |
| smoke | `--smoke` | `cpu-basic` / `30m` | `crossref-datacite-smoke` | none | none |
| benchmark | `--benchmark` | `cpu-xl` / `24h` | `crossref-datacite-benchmark` | read-only | `HF_TOKEN`, `CROSSREF_API_TOKEN` if set |
| datacite (default) | — | `cpu-xl` / `24h` | `crossref-datacite-citations` | read-only | `HF_TOKEN`, `CROSSREF_API_TOKEN` if set |
| legacy | `--legacy-arxiv` | `cpu-xl` / `24h` | `crossref-arxiv-citations` | read-only | `HF_TOKEN`, `CROSSREF_API_TOKEN` if set |

Every job carries labels `pipeline`, `mode`, and `source` (the first 12
characters of the source digest) in namespace `cometadata`. The runtime
source digest (`citations_pipeline.paths.runtime_source_digest`) covers
`jobs/**/*.py`, `citations_pipeline/**/*.py`, `src/**/*.rs`,
`benches/**/*.rs`, `Cargo.toml`, and `Cargo.lock` — so a dependency pin or
version bump moves it, while README and test changes do not. The submitter
stages the runtime source files into a read-only volume named by their
digest, mounts `cometadata/shared-storage` read-only at
`/mnt/shared-storage` for every mode but smoke, probes the bucket for the
affiliations mirror, and refuses to submit when the mirror is unavailable
and `CROSSREF_API_TOKEN` is unset. Options: `--force` (recompute reusable
phases), `--datacite-release YYYY-MM`, `--snapshot-id YYYY-MM` (fail unless
the resolved Crossref snapshot matches), `--flavor`, `--timeout`, and
`--settings` (see below); command-line values override the settings file.
The `--help` output of `submit_job.py`, `jobs/pipeline.py`, and the Rust
binary matches the flags documented here.

Submission prints one JSON line (`id`, `url`, `status`, `mode`, `labels`,
`flavor`, `timeout`, `source_tree_sha256`, `resources`, `datacite_release`,
`expected_snapshot_id`, `benchmark_report_sha256`). Refusals exit 1 with
`submit_job: <message>` on stderr. Inspect asynchronously (no busy polling):

```bash
hf jobs inspect JOB_ID --namespace cometadata
hf jobs logs JOB_ID --namespace cometadata
```

The job log carries `PIPELINE_EVENT=` JSON lines (phase start/complete/fail,
progress with throughput and ETA; never credentials or signed URLs) and ends
with one `RESULT_JSON=` line. A submitted or running job is not a passed
run; require terminal `COMPLETED` and the mode's result:

- smoke: `"status": "smoke-completed"` with exactly
  `{"datacite": {"all": {"works": 5, "citations": 8, "references": 9}, "asserted": {"works": 3, "citations": 3, "references": 3}, "mined": {"works": 5, "citations": 6, "references": 6}}, "arxiv": {"all": {"works": 1, "citations": 2, "references": 2}, "asserted": {"works": 1, "citations": 1, "references": 1}, "mined": {"works": 1, "citations": 1, "references": 1}}, "ambiguous_urls": 2, "failed_arxiv_works": 1}`
  as `counts`, plus `source_tree_sha256`, `identity_sha256`,
  `datacite_release` (`2026-07`), `snapshot_id` (`smoke`);
- datacite: `"status": "completed"` (or `"no-op"` for an already-published
  identity) with `dataset_urls` and `publications` (per dataset: `repo_id`,
  `commit_sha`, `commit_url`, `artifact_digest`), `derived_prefix`,
  `identity_sha256`, `source_tree_sha256`, `extraction_reused`, and
  `counts`;
- benchmark: `"status": "benchmark-ready"` or `"benchmark-not-ready"` (see
  below);
- legacy: the legacy result (`status`, `dataset_commit`, `counts`, …).

A failure raises inside the job (no `RESULT_JSON`); the last `phase_failed`
event names the phase.

The job entry `jobs/pipeline.py` takes `--source-root`, `--source-digest`,
`--bucket-root`, `--work-root` (default `/tmp/crossref-arxiv-citations`),
`--mode {legacy,datacite,smoke,benchmark}`, `--datacite-release`,
`--snapshot-id`, `--force`, `--flavor` (exported as `BENCHMARK_FLAVOR`),
`--smoke` (alias of `--mode smoke`), and the eleven resource flags listed
above. It verifies the source digest before installing anything, installs
the Rust toolchain only when `cargo` is absent, and dispatches on the mode.

### The smoke

`--smoke` runs the complete chain — Rust build, bundle build from a generated
DataCite release (JSONL metadata plus a CSV state report holding a
registered-only DOI), extraction twice (once in memory, once with budgets
that force the external aggregation path; the outputs must be identical),
the transform of both datasets — against a deterministic archive covering
arXiv, non-arXiv DOIs, registered-only membership, unique and shared
landing pages, DOI-based disambiguation, duplicates, every provenance,
self-citation, and invalid candidates, and checks the exact rows, Parquet
records, and counts above. It mounts no bucket, uses no token, and
publishes nothing. Local and remote smoke run the same entry point.

### The benchmark and production settings

`--benchmark` measures the real workload before a production run, using the
full DataCite bundle (built or reused from `indexes/`) and the real Crossref
snapshot: it stages and checksums the whole archive, samples whole tar
members in one streaming pass (stride 5000, windows of 8; the sample is
representative only when the source holds at least 2 × stride members) and
also keeps the DOI-heaviest and URL-heaviest windows as separate inputs,
extracts every window and the whole sample, runs the legacy arXiv pipeline
on the same sample and checks that every legacy `(citing, arXiv DOI)` pair is
present in the full-mode projection, transforms the sample, compresses and
persists the outputs to measure the persistence rate, and runs two
synthetic stress cases (a DOI cited by 200,000 works and a shared URL cited
by 200,000 works) that are reported but never extrapolated; the shared-URL
case confirms nothing by design and is recorded as a zero-citation transform
with its ambiguity counts. The legacy comparison reuses the legacy pipeline's
verified arXiv FST from `crossref-arxiv-citations-latest/fst/<release>-v1/`
(read-only) when one verifies for the selected release and builds one only
otherwise (`legacy.fst_source` is `cache` or `built`). Sampling decompresses
the full archive a second time, in single-core Python (about an hour on the
2026-07 snapshot); that is a known cost of the benchmark.

It records index build time, references per second,
match/ambiguity/arXiv/unmatched/malformed rates, memory, disk, transform and
persistence time, and a projection of the full run with a 25% time band and
a 1.5× disk band:

- **Time.** Each window's extraction time minus the index load and
  aggregation seconds its extractor logged is its variable cost; the
  slowest window's variable cost per member × total members is the
  extraction estimate, and the full run adds one index load (the slowest
  measured) and one aggregation (the sample's aggregation × the row ratio,
  never below the slowest measured). Rows and bytes follow the densest
  window, never the sample mean.
- **Memory.** Every extractor child's own peak resident set size
  (`os.wait4`), scaled for larger cited-work groups and never below the
  configured budgets, and the cgroup non-reclaimable high-water mark of the
  phases production runs. The Rust build, the legacy index build and
  extraction, and the stress cases are reported but never count.
- **Disk.** The sizes of the trees the job owns under the work root (staged
  archive, bundle copy, build, partitions, outputs, transform scratch),
  sampled every second and at phase boundaries, never below the observed
  high-water mark. The node filesystem's usage (`shutil.disk_usage`, which
  includes other tenants) is recorded for context only.

Readiness for a `(flavor, timeout)` requires projected peak memory ≤ 75% of
the flavor's RAM, projected peak disk ≤ 75% of its ephemeral storage, and
projected total time ≤ 80% of the timeout. The ladder `cpu-xl/24h` →
`cpu-xl/48h` → `cpu-performance/24h` → `cpu-performance/48h` is walked in
order (CPU-bound components are rescaled by 0.6 when moving to
`cpu-performance`; memory and disk are not), and the first ready rung is
selected. `production-settings.json` is written only when a rung is ready,
the sample is representative, and the legacy comparison is consistent;
otherwise the report names the `withheld_reason`. Both files land in the
bucket under `crossref-datacite-citations-latest/benchmarks/<run-id>/`
(`run-id` is `RESULT_JSON["remote_prefix"]`):

```bash
hf buckets cp hf://buckets/cometadata/shared-storage/crossref-datacite-citations-latest/benchmarks/<run-id>/benchmark-report.json /tmp/datacite-benchmark-report.json
hf buckets cp hf://buckets/cometadata/shared-storage/crossref-datacite-citations-latest/benchmarks/<run-id>/production-settings.json /tmp/datacite-production-settings.json
shasum -a 256 /tmp/datacite-benchmark-report.json   # must equal production-settings.json["benchmark_report_sha256"]
uv run submit_job.py --settings /tmp/datacite-production-settings.json
```

The settings file has exactly these keys: `source_tree_sha256` (must equal
the current runtime source digest, or the submitter refuses with
`settings were measured on source tree … but the current source tree is …`),
`snapshot_id` and `datacite_release` (`YYYY-MM`, pinned into the run),
`flavor`, `timeout` (`24h` or `48h`), `resources` (the eleven resource
settings the benchmark ran with), and `benchmark_report_sha256`. Unknown
keys or resource names are refused. Never pass `--settings` to `--smoke`.

### Job hardware and pricing

Prices checked on 2026-09-15 at
[Hugging Face Jobs pricing](https://huggingface.co/docs/hub/jobs-pricing);
billing is per minute of runtime. Re-check before each billable submission
with `hf jobs hardware`, which lists the current flavors and unit costs (the
benchmark also fetches live prices and falls back to this table).

| Flavor | vCPU | RAM | Ephemeral disk | Price |
| --- | --- | --- | --- | --- |
| `cpu-basic` | 2 | 16 GB | 50 GB | $0.01 / hour |
| `cpu-upgrade` | 8 | 32 GB | 50 GB | $0.03 / hour |
| `cpu-xl` | 16 | 124 GB | 1000 GB | $1.00 / hour |
| `cpu-performance` | 32 | 256 GB | 1024 GB | $1.90 / hour |

Ceilings at the default timeouts: a smoke is at most 30 minutes on
`cpu-basic` (under $0.01); a benchmark or production run on `cpu-xl` is at
most $24 for 24 billable hours ($48 at `48h`; $45.60 / $91.20 on
`cpu-performance`), excluding separate storage charges. The first run
against a new DataCite release also builds and persists the bundle. These
are planning ceilings, not measured durations; the benchmark reports the
projected duration and cost, and every billable submission needs its cost
approved.

## Development

```bash
cargo build          # Debug build
cargo build --release # Release build
cargo test           # Run tests
cargo fmt            # Format code
cargo clippy         # Lint
```

### Local verification

Run the complete set after any change:

```bash
export CARGO_TARGET_DIR="$PWD/target"   # lets the real-binary smoke tests reuse the release build
uv run pytest -q
uv run ruff check .
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo build --release --locked
cargo +1.87.0 check --locked            # the declared Rust floor; rustup toolchain install 1.87.0 if absent
```

The 1.87 check guards the declared floor through the pins in `Cargo.lock`;
a plain `cargo update` can move dependencies to releases that require a
newer Rust and silently undo it, so never skip this check after touching
the lockfile.

With `CARGO_TARGET_DIR` set, `tests/test_datacite_smoke.py` builds into (and
reuses) `target/`; without it each real-binary test builds the release
binary into its own temporary directory, which is slow and disk-heavy.

Local smoke through the exact entry point the job runs:

```bash
export CARGO_TARGET_DIR="$PWD/target"
uv run jobs/pipeline.py --mode smoke --source-root "$PWD" \
  --source-digest "$(uv run python -c 'from pathlib import Path; from citations_pipeline.paths import runtime_source_digest; print(runtime_source_digest(Path.cwd()))')" \
  --work-root /tmp/datacite-citations-smoke-entry
```

It must end with `RESULT_JSON=` carrying `"status": "smoke-completed"` and
exactly the counts listed under [Submitting jobs](#submitting-jobs); remove
`/tmp/datacite-citations-smoke-entry` afterwards. The remote smoke
(`uv run submit_job.py --smoke`) runs this same entry point without
`CARGO_TARGET_DIR`, building the binary into `<work-root>/cargo-target`.
