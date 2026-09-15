# DataCite citation mining and full Hugging Face run

Date: 2026-09-15
Status: chat design approved; written spec awaiting review

## Objective

Extend the existing pipeline to identify Crossref references to any DOI
represented in a selected DataCite monthly release, either through a DOI or
its registered landing-page URL. Preserve the arXiv mining logic. Process
the complete Crossref snapshot once and use that extraction to publish both
`cometadata/crossref-datacite-citations` and
`cometadata/crossref-arxiv-citations`.

This document specifies the implementation and its acceptance criteria.

## Approved decisions

- Target URLs are DataCite's registered landing-page URLs. They are not an
  arbitrary domain list or every URL occurring in Crossref references.
- Keep shared-URL matches as ambiguous evidence and exclude them from
  citation counts unless DOI evidence identifies exactly one candidate.
- Create a general DataCite dataset and update the existing arXiv dataset
  from the same full run. Preserve the arXiv dataset's published schema.
- Build reusable local indexes and stream Crossref once. Keep the existing
  arXiv recognizers alongside the new DOI and URL matchers.
- Bound extraction buffers, distribute work by the full DOI, and select
  disk-based aggregation using estimated memory requirements.
- Verify both dataset commits before marking a production run complete.
- Run regression tests, an expanded smoke test, and a representative
  performance benchmark before the full production run.

## Current implementation and approach

The Rust pipeline already streams Crossref tar members through parse
workers into segmented Parquet partitions. It searches `DOI`, `URL`,
`article-title`, `journal-title`, and `unstructured`, but rejects references
without arXiv context. The existing FST stores DOI membership only and is
queried during aggregation. Non-arXiv DOIs currently partition by prefix;
buffers have a per-partition row threshold without a global byte budget.

Python builds the extractor, discovers a mirrored DataCite release, caches
the FST, stages Crossref, transforms output, persists a derived generation,
and publishes one arXiv dataset. Its schema, paths, and completion markers
currently assume that single destination.

Keep these component boundaries and generalize the matching and
publication contracts. A bulk database join would require materializing
many more unmatched candidates. Per-reference API calls would introduce
network latency, rate limits, and results that change during a run. Local
indexes make matching reproducible against the selected release.

## DataCite input and indexes

### Coverage

The full-run DOI membership set is the union of valid DOI identifiers in
the release's JSONL records and CSV state reports. CSV reports include
both Findable and Registered state DOIs. Read registered landing pages
from `attributes.url` in the JSONL metadata. A DOI with no usable URL can
still match by DOI. Do not derive landing pages from `relatedIdentifiers`,
`contentUrl`, or general metadata links.

Support unwrapped `id`/`attributes` records and API singleton `data`
wrappers. Retain support for the existing id-only JSONL inputs in legacy
index-building mode. Production uses the complete monthly release,
including its CSV reports; it must not silently fall back to JSONL-only
coverage when an expected input is absent.

The release is a snapshot, not a historical URL registry. URLs that have
changed since an older citation was written can remain unmatched. There
are no live DOI lookups, redirects, web crawling, fuzzy title matching, or
repository-specific URL rewrite rules in this change.

### Index bundle

Build an immutable, versioned bundle containing:

- A memory-mapped FST set of canonical DOI identifiers.
- An FST URL map whose values point into a memory-mapped sidecar containing
  the sorted, distinct candidate DOIs for each URL.
- Lookup support for scheme-less URL candidates on registered hosts.
  Candidates sharing an authority/path across schemes retain the union of
  their DOI mappings, so omitted schemes cannot hide ambiguity.
- A manifest recording the DataCite release, source inventory digest,
  index format and normalization versions, file sizes and SHA-256 digests,
  and counts for DOIs, URLs, missing/invalid URLs, and shared URLs.

Parse the DataCite files once, emit compact DOI and URL/DOI records, and
externally sort and deduplicate them before building the indexes. Bound
sort memory and temporary storage. Do not hold the full registry or all
URL associations in a process-wide hash map. Workers share read-only
indexes; they do not make copies of the registry.

Use the monthly manifest to identify and verify expected input files.
Missing/unreadable files, truncated gzip input, malformed records, or
conflicting record DOI identifiers fail the full index build. Null or
invalid landing-page values are counted without discarding a valid DOI.
Write the bundle completion marker only after every artifact is verified.

## Identifier normalization and extraction

### DOI matching

- Recognize structured DOI fields, bare DOIs in text, `doi:` wrappers, and
  DOI resolver URLs, including `doi.org` and `dx.doi.org`.
- Normalize wrappers and case consistently in the registry and extracted
  candidates. Preserve the original substring as evidence.
- Do not restrict membership to a fixed list of DataCite DOI prefixes.
- Check the full candidate against the index first. For trailing citation
  punctuation, test bounded boundary variants and prefer the longest
  registered DOI. Do not truncate arbitrary suffix characters to obtain a
  match or strip balanced punctuation that belongs to a DOI.
- Apply arXiv version removal only through the existing arXiv rules.
  General DataCite DOI version suffixes identify distinct DOIs.

### Landing-page URL matching

Recognize explicit HTTP/HTTPS URLs and scheme-less host/path references
in the searchable fields. Check complete normalized URL keys, not domains
or path prefixes. Use registered hosts to reject irrelevant candidates
cheaply before full URL lookup.

Normalize scheme and hostname case, default ports, and equivalent
unreserved percent encodings. Preserve path case, non-root trailing
slashes, query parameters and their order, and nonempty fragments. Empty
paths normalize to `/`. Do not strip `www`, tracking parameters, or
nonempty fragments. Explicit HTTP and HTTPS URLs remain distinct; a
scheme-less reference is checked across both and can be ambiguous.

Decode JSON escapes during input parsing. Any HTML entity decoding used
for a textual URL must preserve offsets back to the original evidence.
Remove surrounding citation punctuation only through bounded candidate
variants validated against the URL index. Never scan across a reference
field boundary to manufacture a DOI or URL.

### arXiv compatibility

Keep the recognizers in `src/extract/arxiv.rs`, including modern IDs,
legacy category IDs, DOI and URL forms, case handling, whitespace, and
version normalization. Bare numeric IDs still require arXiv context.
Convert their results to the same canonical DOI identity used by the
general matcher before deduplication.

Preserve the existing arXiv-only CLI invocation and id-only FST input
contract. Add an explicit full-DataCite index build command and a pipeline
option for its bundle. Legacy `--arxiv-fst`/`--arxiv-records` invocations
remain arXiv-only; the full job selects DataCite mode explicitly. Reject
conflicting legacy and full-mode index options.

The full run may discover additional arXiv citations through general DOI
or registered-URL matching. It must retain previously recognized valid
arXiv citations. The arXiv publication projection renders the established
`10.48550/arXiv.<id>` form and preserves the published schema.

## Evidence, ambiguity, and counting

All direct DOI and arXiv candidates are collected before shared URLs are
resolved. Each candidate must be in the selected DOI membership set.
An unambiguous URL maps to its one DOI. A shared URL resolves only when
the intersection of its candidate DOIs and the independently identified
DOIs in that reference contains exactly one member. An unrelated DOI
does not disambiguate a URL. If the intersection contains multiple
members, direct DOI citations still count individually and the URL
evidence remains ambiguous.

A confirmed reference match is unique by canonical citing DOI, original
reference index, and canonical cited DOI. DOI, URL, and arXiv matches for
that tuple must not multiply the reference count. Keep distinct reference
positions and exclude self-citations using canonical DOI equality.

Each confirmed match retains `provenance`, `raw_match`, the original
`reference`, and `match_type` with values `doi`, `arxiv`, or `url`.
Only an equivalent DOI in the structured `DOI` field with a recognized
`doi-asserted-by` value receives `publisher` or `crossref` provenance.
DOI equivalence for this assertion check removes wrappers and case
differences; it does not strip an arXiv version suffix from an asserted
DOI. URL-derived DOI matches and other text matches are `mined`.

Choose one representative match deterministically: highest provenance,
then structured DOI evidence, arXiv evidence, other DOI evidence, and URL
evidence. Break remaining ties by the existing searchable-field order
and substring position. This keeps arXiv raw matches stable when general
mining finds the same target. The original reference retains the context
for additional evidence.

Published `citation_count` is the number of distinct citing works.
`reference_count` is the number of confirmed reference tuples. A citing
work's provenance is its strongest retained match. Apply provenance
filters before recomputing counts for each dataset configuration.
Retain the original reference index in full-mode intermediate artifacts
through aggregation so publication can order matches reliably.

For each published work, `citation_count` equals the length of `cited_by`
and `reference_count` equals the sum of its match-list lengths. Across
configurations, the `all` reference total equals `asserted` plus `mined`.
Citation totals are not additive because one citing work can have matches
in both configurations. The arXiv projection must equal the corresponding
arXiv DOI subset of each general citation configuration.

Ambiguous URL evidence has its own output with `citing_doi`, `ref_index`,
normalized `url`, `raw_match`, sorted `candidate_dois`, and the original
`reference`. Deduplicate repeated sightings of the same normalized URL
within a reference. These rows contribute to no citation configuration.
Persist invalid arXiv validation evidence as before. Other candidates
absent from DataCite contribute diagnostic counters, not a potentially
enormous output of unrelated Crossref DOIs and URLs.

## Processing and resource limits

Load the local bundle once and retain the existing archive reader,
parallel parse workers, and single partition writer. Broaden cheap
candidate checks beyond arXiv text. Perform membership and URL resolution
before writing matched rows to disk.

Replace whole-file result vectors with bounded result batches and a
separate file-completion event. Account for both queued batches and
worker-local output. Checkpoint a file only after all its rows are
durable. Raw tar-member buffers remain part of the resource estimate.

In DataCite mode, select partitions using a stable, documented hash of the
complete canonical cited DOI. Use a fixed configurable partition count,
recorded with the hash algorithm in the extraction identity. Enforce a
global buffered-byte budget as well as per-segment limits. Large DOI
prefixes must not determine partition size.

Select in-memory or external aggregation from row counts and estimated
decoded bytes, not segment count alone. Bound sort concurrency, merge
fan-in, merge buffers, and DuckDB memory; use multi-pass merges when
needed. Account for unusually highly cited works in performance tests.
Never truncate citations or candidate lists to satisfy a resource limit;
fail explicitly if the selected configuration cannot process them.

Keep intermediate disk use bounded through staged cleanup after each
artifact becomes unnecessary. Release the local archive after successful
extraction. Durable source archives and current derived generations are
not scratch files. Preserve the complete original reference JSON in
durable extraction artifacts, including fields the publication transform
cannot represent in its string-valued struct.

## Dataset contracts

### General DataCite dataset

Publish `cometadata/crossref-datacite-citations` as a public CC0 dataset.

| Configuration | Contents |
| --- | --- |
| `all` | All confirmed matches |
| `asserted` | Publisher and Crossref provenance matches |
| `mined` | Mined matches |
| `ambiguous_urls` | Unresolved URL evidence, separate from citation counts |

The three citation configurations use the following top-level columns:

| Column | Meaning |
| --- | --- |
| `doi` | Canonical lowercase DataCite DOI |
| `citation_count` | Number of distinct citing works in this configuration |
| `cited_by` | Citing works with `doi`, `matches`, and strongest `provenance` |
| `reference_count` | Number of confirmed reference tuples |

Each `matches` entry has `provenance`, `match_type`, `raw_match`, and
`reference`. Retain the existing publication conventions for stringifying
reference values, nulls for absent keys, and deterministic field order.
An empty ambiguity configuration still has a declared, readable schema.

### Existing arXiv dataset

Derive the arXiv subset from confirmed canonical arXiv DOIs in the same
extraction. Keep `data/{all,asserted,mined}.parquet`, top-level columns
`arxiv_doi`, `arxiv_id`, `citation_count`, `cited_by`, `reference_count`,
and nested match fields `provenance`, `raw_match`, `reference`. Omit the
new `match_type` field from this compatibility projection. Derive the
reference struct keys per arXiv configuration, without adding keys only
present in non-arXiv matches.

For both datasets, order works by descending citation count then DOI,
citing works by DOI, and matches by original reference position. Results
must be logically identical regardless of worker completion order or
aggregation path. Require byte reproducibility only within a pinned
serialization/dependency configuration.

## Job orchestration, state, and publication

Update `submit_job.py`, `jobs/pipeline.py`, and `citations_pipeline/` to
run the expanded pipeline. Keep manual submission, runtime source-volume
staging and digest checks, a read-only shared-storage mount, and bucket
API writes. Continue to consume the affiliations snapshot mirror and use
the existing authenticated Crossref download fallback.

The new full pipeline owns
`cometadata/shared-storage/crossref-datacite-citations-latest/`:

- `indexes/<release>-v<format>-<input-digest>/` contains verified bundles.
- `raw-generations/<snapshot>-<run>/` contains fallback downloads.
- `derived-generations/<snapshot>-<run>/` contains durable extractor
  evidence, both transformed datasets, ambiguity output, and a report.
- `_EXTRACTION_COMPLETE.json` identifies verified reusable derived data.
- A per-generation publication record tracks the two verified commits.
- `_PIPELINE_COMPLETE.json` identifies a run only after both publications
  have been verified.

Use distinct full-mode marker and artifact versions. Reuse identity
includes the Crossref archive digest, DataCite release and inventory
digest, bundle digest, normalization and extraction versions, runtime
source digest, extraction mode, partition settings, and publication
schema versions. A legacy arXiv cache or checkpoint cannot satisfy that
identity. `--force` recomputes reusable phases while retaining verification.

Production proceeds through these phases:

1. Verify source, resolve Crossref, and select a complete DataCite release.
2. Build or fetch and verify the complete index bundle.
3. Stage and checksum the Crossref archive while copying it locally.
4. Extract the complete snapshot once; aggregate confirmed citations and
   retain diagnostics and ambiguity evidence.
5. Prepare and validate all configurations and dataset cards for both
   repositories. Record counts, input identities, and artifact digests.
6. Persist the complete derived generation and extraction marker.
7. Publish the DataCite dataset, verify its commit and expected files, and
   persist that publication result. Then publish and verify arXiv.
8. Activate the full pipeline marker containing both commit identities.
   Clean up only the previous generations owned by the full pipeline.

Each repository update uses a single parent-SHA-guarded commit for its
data and card. Create the new repository idempotently. A parent conflict
fails publication rather than overwriting concurrent changes. Repository
commits are independent; a temporary interval in which only DataCite has
updated is possible. Report it honestly and leave the run incomplete.

On retry, verify persisted outputs by digest and verify recorded remote
commits and their files. Reuse completed extraction and publish only the
missing destination. Include the artifact manifest digest in the dataset
commit so recovery can recognize a completed publication even if the job
failed before saving its local publication record. Never declare success
because a dataset merely exists or a commit request returned an ID.
Check the current repository files before activation or a no-op result.
If they no longer match the recorded artifacts, report a concurrent
change instead of treating a historical commit as the current dataset.

Keep old arXiv-owned bucket generations intact. The new full-run marker
is the authority for the paired publication. The submitter selects the
new mode and label by default for production and reports the submitted
source identity, job ID, and monitoring URL. The job's final result emits
both dataset URLs and commit IDs after publication.

Phase-level recovery across Jobs is required. Local extraction checkpoints
remain useful only when their matching partition files are available;
ephemeral Job disk does not provide cross-job mid-extraction recovery.
Reject incompatible local checkpoints. A compatible local aggregation
retry must not duplicate already-written partitions, including a crash
between writing output and recording completion.

## Verification and full-run readiness

### Correctness

Retain existing Rust and Python regressions and add focused tests for:

- JSONL and CSV membership, wrapped input records, missing URLs, absent
  expected files, malformed data, corrupt gzip files, and bundle digests.
- Structured and textual DOIs, resolver URLs, case, escapes, valid suffix
  punctuation, trailing citation punctuation, and non-DataCite candidates.
- Exact and scheme-less URLs, meaningful queries/fragments/path case,
  repeated URLs, shared URLs, and DOI-based disambiguation.
- Every existing arXiv form, context requirements, version handling,
  provenance, and stable raw evidence when multiple matchers overlap.
- Duplicate reference evidence, unique-citer counts, self-citations, and
  identical results from in-memory aggregation, external aggregation,
  differing worker orders, and compatible local resume.
- Both publication schemas, per-config counts, empty ambiguity data,
  stale caches, a first successful publication followed by a second
  failure, lost publication records, parent conflicts, and retry reuse.

The expanded deterministic smoke fixture includes arXiv, non-arXiv DOIs,
registered-only DOI membership, unique and shared landing pages,
disambiguation, duplicates, all provenances, and invalid candidates. It
executes the real Rust build, bundle build, extraction, and both transforms
with exact expected records and counts. Local and remote smoke use the
same entry point. Smoke mounts no bucket and publishes no dataset.

Run the repository's Rust build/test/fmt/clippy and Python pytest/ruff
checks, plus the local smoke. After those pass, run a remote smoke and a
representative benchmark before launching the full job.

### Performance and execution

Benchmark against the full DataCite bundle and several separated portions
of the real Crossref snapshot, including DOI-heavy, URL-heavy, and highly
cited targets. Measure index build time, references per second, match and
ambiguity rates, peak memory, disk high-water mark, transform/persistence
time, and projected full-run duration. Compare legacy arXiv behavior on
the same inputs. Synthetic microbenchmarks alone cannot establish full-run
readiness.

Start planning from the existing `cpu-xl` configuration: 16 vCPU, 124 GB
RAM, 1,000 GB ephemeral storage, and a 24-hour timeout. At the price
checked on 2026-09-15, compute is $1/hour, or approximately $24 for 24
billable hours, excluding separate storage charges and validation jobs.
This is a planning configuration, not a measured completion guarantee.

Select production settings from the benchmark with at least 25% memory
and disk headroom and 20% timeout headroom over the projected complete
run, including index preparation and publication. Expose hardware and
timeout settings in the submitter. If the workload cannot meet those
limits, revise the configuration and report the concrete cost before
submission. Do not silently truncate the snapshot or publish a sample.

Emit progress and phase events with counts for DOI, URL, arXiv, ambiguous,
unmatched, and malformed inputs, plus throughput and resource measurements.
Unreadable Crossref members or invalid top-level JSON fail production.
Count malformed individual references or unusable field values explicitly;
continue mining usable fields in otherwise valid references. Resource
errors and publication errors leave the pipeline incomplete. Missing
required artifacts fail verification. Legitimately empty configurations
must be valid Parquet with their declared schemas; production requires
nonempty `all` results for both datasets.

After a full run, verify input identities, complete archive traversal,
configuration count invariants, readable published schemas, both remote
commits, and the activated marker. Report actual job status and links;
submission alone does not mean the full run completed.

## Implementation boundaries

| Area | Responsibility |
| --- | --- |
| `src/input/`, `src/index/`, index commands | Release input, normalized registry records, bundle building and lookup |
| `src/extract/`, `src/common/`, `src/commands/pipeline.rs` | Candidate mining, arXiv compatibility, evidence selection and ambiguity |
| `src/streaming/` | Bounded batches and buffers, full-DOI partitioning, aggregation and local retry correctness |
| `citations_pipeline/arxiv_index.py` and its generalized replacement | Release discovery, bundle cache, manifest verification |
| `citations_pipeline/paths.py`, `state.py`, `pipeline.py` | Full-mode identity, durable artifacts, phases and paired publication state |
| `citations_pipeline/publish.py` | General dataset transform, arXiv projection, ambiguity configuration, guarded publication |
| `submit_job.py`, `jobs/pipeline.py`, `citations_pipeline/extractor.py` | Explicit full-mode invocation, job settings and runtime packaging |
| Smoke fixtures, Rust/Python tests, benchmarks, README | Regression evidence, readiness measurements and operating instructions |

Changes to normalization must have one authoritative implementation used
for index construction and reference matching. Keep general matching
outside the arXiv parser. This is one coordinated feature; unrelated
refactors, scheduled jobs, historical URL harvesting, and API enrichment
remain outside its scope.

## Sources

- [DataCite monthly data file](https://support.datacite.org/docs/datacite-monthly-data-file),
  checked 2026-09-15 for JSONL/CSV coverage, record format, and manifests.
- [DataCite DOI response fields](https://support.datacite.org/docs/api-get-doi),
  checked 2026-09-15 for the registered landing-page `url` field.
- [Hugging Face Jobs pricing](https://huggingface.co/docs/hub/jobs-pricing),
  checked 2026-09-15 for CPU XL resources and hourly compute cost.
- Existing repository code and
  `docs/superpowers/specs/2026-08-31-crossref-arxiv-citations-hf-job-design.md`
  for the arXiv publication contract and current job architecture.
