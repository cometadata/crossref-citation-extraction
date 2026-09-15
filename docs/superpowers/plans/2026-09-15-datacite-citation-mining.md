# DataCite citation mining implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mine DataCite DOI and registered landing-page references from one complete Crossref extraction, preserve arXiv mining, and publish both DataCite and arXiv datasets through a verified Hugging Face job.

**Architecture:** Build a versioned DOI FST and URL-to-DOI FST bundle from the complete DataCite release. Match references locally, write bounded batches into full-DOI hash partitions, and aggregate with resource limits and crash-safe output fragments. Python prepares both publication schemas, persists reusable artifacts, and verifies the two independent dataset commits before activation.

**Tech Stack:** Rust 2021, serde, fst/memmap2, regex, flate2, Polars/Parquet, crossbeam, Python, DuckDB, PyArrow, huggingface_hub, pytest, and Criterion.

**Spec:** [Approved design](../specs/2026-09-15-datacite-citation-mining-design.md). Read the complete spec before implementing a task. Baseline implementation is commit `39256c6`; the following planning commit changes documentation only.

## Global constraints

- "Preserve the arXiv mining logic."
- "Process the complete Crossref snapshot once and use that extraction to publish both `cometadata/crossref-datacite-citations` and `cometadata/crossref-arxiv-citations`."
- "Keep shared-URL matches as ambiguous evidence and exclude them from citation counts unless DOI evidence identifies exactly one candidate."
- "Preserve the arXiv dataset's published schema."
- "A legacy arXiv cache or checkpoint cannot satisfy that identity."
- "Phase-level recovery across Jobs is required."
- "Never truncate citations or candidate lists to satisfy a resource limit; fail explicitly if the selected configuration cannot process them."
- "Select production settings from the benchmark with at least 25% memory and disk headroom and 20% timeout headroom over the projected complete run, including index preparation and publication."
- Keep the repository's Rust version floor `1.87`, Python package floor `3.11`, and remote job floor `3.12`. Keep current Polars `0.46`, fst `0.4`, and Python dependency bounds. Add only the dependencies named in the relevant task and update the lockfile without unrelated upgrades.
- Existing CLI `build-index`, `--arxiv-fst`, and `--arxiv-records` retain their arXiv-only behavior. Full mode is selected by `--datacite-index`.
- Credentials remain in environment/secrets. Source digests and markers must contain no credentials or signed download URLs.
- The source registry must still ship Cargo's declared bench targets. Runtime Python files belong under `citations_pipeline/` or `jobs/` and must be included in the submitted source volume.
- Existing `docs/` is ignored. Stage only intended planning/report files with explicit `git add -f -- <path>`; do not change the ignore policy.
- Test each behavior before its implementation, run the named focused checks, and commit each independently testable task. A failed baseline check must be investigated before attributing a later failure to this feature.

---

## Execution setup

- [ ] Read the spec and the repository's applicable instructions. At execution time, use the worktree skill to determine whether the current checkout already provides isolation. Preserve other contributors' edits.
- [ ] Record the starting commit and establish the baseline before adding dependencies:

```bash
git status --short --branch
cargo test --locked
uv run pytest -q
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
uv run ruff check .
```

Expected: all checks pass, or a documented pre-existing failure is resolved before proceeding. The implementation plan itself has not run these implementation checks.

## File ownership and dependency order

| Task | Files and responsibility | Depends on |
| --- | --- | --- |
| 1 | `src/common/identifiers.rs`: shared DOI/URL normalization | Setup |
| 2 | `src/input/datacite_release.rs`: manifest, JSONL, CSV and verified streaming input | 1 |
| 3 | `src/index/datacite.rs`, `datacite_builder.rs`, new index command: bundle storage and lookup | 1, 2 |
| 4 | `src/extract/reference.rs`, `datacite.rs`: candidates, evidence selection and ambiguity | 1, 3 |
| 5 | `src/streaming/limits.rs`, `partitioning.rs`, existing writer: byte budgets and row format | 4 |
| 6 | Existing aggregation/merge plus `src/streaming/partition_commit.rs`: resource limits and local recovery | 3, 5 |
| 7 | `src/commands/datacite_pipeline.rs`: full-mode reader/workers/writer | 3, 4, 5, 6 |
| 8 | `citations_pipeline/datacite_index.py`, `full_state.py`, paths: verified bundle caching and identity | 3 |
| 9 | `citations_pipeline/datacite_publish.py`: both schemas and ambiguity output | 6, 7, 8 |
| 10 | `citations_pipeline/paired_publish.py`: independent guarded commits and retry journal | 8, 9 |
| 11 | `citations_pipeline/full_pipeline.py`, extractor wrapper: full orchestration | 7, 8, 9, 10 |
| 12 | `citations_pipeline/datacite_smoke.py`: deterministic full-chain fixtures | 11 |
| 13 | `citations_pipeline/benchmark.py`: representative sampling and production settings | 11, 12 |
| 14 | Submitter and job entry: full mode, explicit settings and packaging | 11, 12, 13 |
| 15 | README and verification: local checks, remote smoke, benchmark | 1-14 |
| 16 | Full job submission, paired publication verification, run report | 15 |

Tasks 8-10 can be implemented after their prerequisites without waiting for every Rust task. File owners must coordinate edits to shared registries. Do not create parallel edits to `pipeline.py`, `paths.py`, `Cargo.toml`, or a common test helper.

## Contract conventions

- Rust public functions below use `anyhow::Result` when fallible. Expose new modules from the existing `mod.rs` files. Existing types such as `Provenance`, `PartitionRow`, `CrossrefEntry`, and `OutputPaths` retain their home modules.
- Full-mode constants are `DATACITE_INDEX_VERSION = 2`, `NORMALIZATION_VERSION = 1`, `DATACITE_EXTRACTION_VERSION = 2`, and `DATACITE_MARKER_VERSION = 2`. General publication schema is version 2; arXiv publication remains version 1. Legacy FST/marker constants retain their current values.
- Full Rust output names are `<stem>_{valid,failed,publisher,crossref,mined}.jsonl`, `<stem>_ambiguous_urls.jsonl`, and `extraction-report.json`. `<stem>` uses the existing `extract_input_stem`; `all.json.tar.gz` yields `all.json`.
- Full confirmed output retains the existing grouped `doi`/`cited_by` shape. Each internal `cited_by` entry additionally carries `ref_index` and `match_type`. The legacy output serializer omits those additions. Ambiguity JSONL rows use exactly the fields in the spec.
- Python keys for the two destinations are always `datacite` and `arxiv`. Citation configuration names are `all`, `asserted`, and `mined`; only DataCite adds `ambiguous_urls`.

### Initial tunable resource settings

These are starting values for validation, not measured production claims. Store them in one Python `ResourceSettings` dataclass in Task 8 and forward them explicitly to Rust in Task 11.

| Field / CLI flag | Initial value |
| --- | --- |
| `partitions` / `--partitions` | 256, a positive power of two |
| `writer_memory_mb` / `--writer-memory-mb` | 4096 |
| `result_batch_mb` / `--result-batch-mb` | 16 |
| `queue_batches` / `--queue-batches` | 8 |
| `aggregation_memory_mb` / `--aggregation-memory-mb` | 16384 |
| `merge_fan_in` / `--merge-fan-in` | 32, minimum 2 |
| `sort_workers` / `--sort-workers` | 4 |
| `lookup_memory_mb` / `--lookup-memory-mb` | 64 |
| `max_entry_mb` / `--max-entry-mb` | 512 |
| `index_sort_memory_mb` / index `--sort-memory-mb` | 4096 |
| `duckdb_memory_mb` / Python transform setting | 32768 |

All byte conversions use MiB, `1024 * 1024`. Reject nonpositive values and integer overflow. In smoke tests use small budgets with enough space for the fixture. Preserve the old `--batch-size` behavior in legacy mode.

## Task 1: Shared identifier normalization

**Files:** Create `src/common/identifiers.rs`, `tests/datacite_normalization.rs`. Modify `src/common/mod.rs`, `Cargo.toml`, `Cargo.lock`.

**Interfaces:** Produce `normalize_doi(&str) -> Option<String>`, `normalize_landing_url(&str) -> Option<String>`, and `url_lookup_keys(&str) -> Vec<String>`. DOI assertion equivalence uses `normalize_doi`, which never removes arXiv versions. Only the existing arXiv parser removes those versions.

- [ ] Add the first normalization tests:

```rust
use crossref_citation_extraction::common::identifiers::{
    normalize_doi, normalize_landing_url, url_lookup_keys,
};

#[test]
fn normalizes_doi_wrappers_without_merging_versions() {
    assert_eq!(normalize_doi("https://doi.org/10.1234/AbC"), Some("10.1234/abc".into()));
    assert_eq!(normalize_doi("doi:10.1234/data.v2"), Some("10.1234/data.v2".into()));
    assert_ne!(normalize_doi("10.1234/data.v1"), normalize_doi("10.1234/data.v2"));
}

#[test]
fn preserves_meaningful_url_components() {
    assert_eq!(normalize_landing_url("https://EXAMPLE.org:443/%7Eme?x=1#part"),
               Some("https://example.org/~me?x=1#part".into()));
    assert_ne!(normalize_landing_url("https://example.org/A"), normalize_landing_url("https://example.org/a"));
    assert_eq!(url_lookup_keys("example.org/A"),
               vec!["http://example.org/A", "https://example.org/A"]);
}
```

- [ ] Run `cargo test --test datacite_normalization`. Expected: compilation fails because the new module/functions do not exist.
- [ ] Add `url = "2.5"` and implement the normalizers. Validate a DOI's numeric registrant prefix and nonempty suffix without a fixed prefix allowlist or suffix character whitelist. Reject embedded control characters. Check a complete structured value before text tokenization. Parse resolver URLs as URLs and decode the DOI path once; do not interpret a resolver query/fragment as part of its DOI.
- [ ] Implement URL canonicalization with `url::Url`: accept HTTP/HTTPS, preserve explicit schemes, path case, non-root trailing slash, query order and nonempty fragments, canonicalize host/default port and unreserved percent encodings. Detect controls before parsing. Implement the scheme-less lookup algorithm exactly as two explicit-scheme lookups:

```rust
pub fn url_lookup_keys(value: &str) -> Vec<String> {
    let raw = value.trim();
    if raw.contains("://") {
        return normalize_landing_url(raw).into_iter().collect();
    }
    let raw = raw.strip_prefix("//").unwrap_or(raw);
    ["http://", "https://"]
        .into_iter()
        .filter_map(|scheme| normalize_landing_url(&format!("{scheme}{raw}")))
        .collect()
}
```

The caller requires a syntactically plausible host and later checks registered hosts. Parsing the two schemes separately preserves explicit-port semantics. Do not build a scheme-stripped alias that could match the wrong port.

- [ ] Add tests for `dx.doi.org`, encoded resolver paths, Unicode DOI suffixes, parentheses, empty suffixes, explicit ports, query order, meaningful fragments, tracking parameters, `www`, and malformed URLs. Run `cargo test --test datacite_normalization` and `cargo test extract::arxiv`.
- [ ] Commit the normalization module, dependency/lockfile changes, and tests: `git commit -m 'feat: normalize DataCite DOI and landing-page keys'` after staging only this task's files.

## Task 2: Verified DataCite release reader

**Files:** Create `src/input/datacite_release.rs`, `tests/datacite_release.rs`, `tests/support/mod.rs`. Modify `src/input/mod.rs`, `Cargo.toml`, `Cargo.lock`.

**Interfaces:** Produce the following types and methods. SHA-256 strings inside the program are lowercase hex; decode base64 checksums supplied by DataCite manifests.

```rust
pub enum RecordSource { Metadata, StateReport }
pub struct RegistryRecord {
    pub doi: String,
    pub url: Option<String>,
    pub url_invalid: bool,
    pub source: RecordSource,
}
pub struct SourceFile { pub path: std::path::PathBuf, pub size: u64, pub sha256: String }
pub struct SourceStats {
    pub metadata_records: u64, pub csv_records: u64,
    pub missing_urls: u64, pub invalid_urls: u64,
}
pub struct MonthlyRelease {
    pub root: std::path::PathBuf, pub release: String,
    pub files: Vec<SourceFile>, pub inventory_sha256: String,
}
```

Implement `MonthlyRelease::open(root: &Path) -> Result<Self>`, `MonthlyRelease::scan(&self, emit: &mut dyn FnMut(RegistryRecord) -> Result<()>) -> Result<SourceStats>`, and `decode_metadata_record(line: &str) -> Result<RegistryRecord>`.

- [ ] Add a wrapper/identifier disagreement test:

```rust
use crossref_citation_extraction::input::datacite_release::decode_metadata_record;

#[test]
fn accepts_singleton_and_rejects_conflicting_dois() {
    let row = decode_metadata_record(r#"{"data":{"id":"10.1234/A","attributes":{"doi":"10.1234/a","url":"https://example.org/a"}}}"#).unwrap();
    assert_eq!(row.doi, "10.1234/a");
    assert_eq!(row.url.as_deref(), Some("https://example.org/a"));
    assert!(decode_metadata_record(r#"{"id":"10.1234/a","attributes":{"doi":"10.1234/b"}}"#).is_err());
}
```

- [ ] Run `cargo test --test datacite_release`. Expected: missing module/functions, then a failing behavioral assertion as parsing is implemented.
- [ ] Add `csv = "1.3"`, `sha2 = "0.10"`, and `base64 = "0.22"`. Implement typed borrowed parsing of the root fields and `RawValue` for optional `data`/URL values; do not allocate the entire metadata tree. Accept id-only rows. Null/missing URL means `url_invalid=false`; a non-string URL means `url_invalid=true` and retains the DOI.
- [ ] Implement manifest selection and integrity checks. Prefer `MANIFEST.json`, accept the documented text `MANIFEST` form if JSON is absent, and include only listed `dois/` JSONL.gz and CSV.gz inputs. Reject duplicate paths, traversal, missing listed files and unexpected source files. Require `STATUS.json` with matching month and `Complete`. Compute the inventory SHA-256 from compact UTF-8 JSON containing rows `[relative_path,size,sha256_hex]` sorted by relative path, using `/` separators and lowercase decoded digests. Include no whitespace or trailing newline. Hash compressed bytes while reading; use `MultiGzDecoder` and consume to EOF before accepting each file. Return a contextual error with path/line for malformed input. CSV headers are `doi,state,client_id,updated`; accept `findable` and `registered`, rejecting other states.
- [ ] Add the test-only helper `support::make_release(root: &Path, metadata: &[serde_json::Value], states: &[(&str, &str)]) -> anyhow::Result<()>`. It writes deterministic gzip JSONL/CSV under `dois/updated_2026-07/`, computes their base64 SHA-256 values, writes the real manifest shape and `STATUS.json`. Use `#![allow(dead_code)]` only inside this shared test helper module. Test the union of metadata and registered-only identifiers:

```rust
#[test]
fn scans_registered_only_dois_from_csv() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    support::make_release(tmp.path(), &[serde_json::json!({"id":"10.1234/a"})],
                          &[("10.1234/a", "findable"), ("10.1234/retired", "registered")])?;
    let release = MonthlyRelease::open(tmp.path())?;
    let mut seen = std::collections::BTreeSet::new();
    release.scan(&mut |row| { seen.insert(row.doi); Ok(()) })?;
    assert_eq!(seen.into_iter().collect::<Vec<_>>(), vec!["10.1234/a", "10.1234/retired"]);
    Ok(())
}
```

Import `MonthlyRelease` and `mod support;` in this test file. Add missing-CSV, same-size corruption, truncated/multi-member gzip, malformed JSON, invalid DOI, missing URL, and non-string URL cases. Verify checksums before reporting scan success.

- [ ] Run `cargo test --test datacite_release` and existing DataCite input tests. Commit as `feat: stream and verify complete DataCite releases`.

## Task 3: Build and query the DataCite bundle

**Files:** Create `src/index/datacite.rs`, `src/index/datacite_builder.rs`, `src/commands/build_datacite_index.rs`, `tests/datacite_index.rs`. Modify module exports, `src/cli.rs`, and `src/main.rs`.

**Interfaces:** Consume `MonthlyRelease` and Task 1 normalizers. Produce `build_datacite_index(release_root: &Path, output_dir: &Path, sort_memory_mb: usize) -> Result<BundleManifest>`, `DataciteIndex::load(directory: &Path) -> Result<Self>`, and this lookup contract:

```rust
pub trait TargetLookup: Sync {
    fn contains_doi(&self, doi: &str) -> bool;
    fn contains_host(&self, host: &str) -> bool;
    fn url_dois(&self, key: &str, max_bytes: usize) -> anyhow::Result<Vec<String>>;
}
```

`BundleManifest` is serde-serializable with `kind="datacite-index"`, `format_version=2`, `normalization_version=1`, `release`, `source_inventory_sha256`, `files: BTreeMap<String, IndexFile>`, and `counts: BTreeMap<String,u64>`. `IndexFile` has `size: u64` and `sha256: String`. Store it as `_INDEX_COMPLETE.json` and define the bundle digest as the SHA-256 of those exact bytes.

- [ ] Write the real-bundle test, using Task 2's helper:

```rust
#[test]
fn preserves_all_dois_for_a_shared_url() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let source = tmp.path().join("source");
    support::make_release(&source, &[
        serde_json::json!({"id":"10.1234/a","attributes":{"url":"https://example.org/shared"}}),
        serde_json::json!({"id":"10.1234/b","attributes":{"url":"https://example.org/shared"}}),
    ], &[("10.1234/retired", "registered")])?;
    let out = tmp.path().join("bundle");
    build_datacite_index(&source, &out, 64)?;
    let index = DataciteIndex::load(&out)?;
    assert!(index.contains_doi("10.1234/retired"));
    assert_eq!(index.url_dois("https://example.org/shared", 1024)?,
               vec!["10.1234/a", "10.1234/b"]);
    assert!(index.url_dois("https://example.org/shared", 1).is_err());
    Ok(())
}
```

Import `build_datacite_index`, `DataciteIndex`, `TargetLookup`, and `mod support;`.

- [ ] Run `cargo test --test datacite_index`. Expected: missing bundle API, then missing/incorrect URL associations until implemented.
- [ ] Implement one registry scan emitting DOI keys, URL/DOI pairs and host keys into temporary files. Hex-encode UTF-8 keys in sort input so delimiters cannot corrupt records; hex lexical order preserves byte order. Run system sort with `LC_ALL=C`, `-u`, explicit `-S <MiB>M`, and `-T <owned-temp-dir>`. Stream sorted files into `dois.fst`, `hosts.fst`, and `urls.fst`. Keep counters distinct for input metadata records, CSV records, unique DOIs, unique URLs and shared URLs.
- [ ] Implement `url-targets.bin`. Each FST map value is a byte offset; at that offset store little-endian `u64` candidate count followed by repeated little-endian `u32` byte length and UTF-8 DOI bytes. Stream each group directly to disk and seek back to fill its count. Validate every offset/length against file size and the caller's allocation budget when reading. The existing `FstIndex` remains a legacy membership index. Share mapped bundle files across workers.

```rust
// Core bounded lookup check, before allocating a candidate string.
let end = cursor.checked_add(length).ok_or_else(|| anyhow::anyhow!("URL posting overflow"))?;
anyhow::ensure!(end <= bytes.len(), "truncated URL posting");
used_bytes = used_bytes.checked_add(length).ok_or_else(|| anyhow::anyhow!("lookup budget overflow"))?;
anyhow::ensure!(used_bytes <= max_bytes, "URL candidate memory budget exceeded");
let doi = std::str::from_utf8(&bytes[cursor..end])?;
```

Also budget the count/Vec allocation before allocating it. The manifest excludes itself from `files`; hash all other files, fsync, write the marker last, and atomically rename an owned staging directory into place. Refuse to overwrite an existing completed bundle.

- [ ] Add `build-datacite-index --input <release-root> --output <bundle-dir> --sort-memory-mb 4096`. Test it through `env!("CARGO_BIN_EXE_crossref-citation-extraction")` with the generated release. Add corruption of each bundle artifact, invalid manifest version, duplicate URL/DOI pairs, different schemes on one host, missing URLs and zero DOI membership tests. Repeated builds with identical settings must have identical index bytes.
- [ ] Run `cargo test --test datacite_index` and legacy `build-index` regressions. Commit as `feat: build memory-mapped DataCite DOI and URL indexes`.

## Task 4: Mine references and resolve ambiguity

**Files:** Create `src/extract/reference.rs`, `src/extract/datacite.rs`, `tests/datacite_matching.rs`. Modify `src/extract/mod.rs`, `src/common/types.rs`; preserve `src/extract/arxiv.rs` behavior.

**Interfaces:** Produce `MatchType { Doi, Arxiv, Url }` in `src/common/types.rs`, deriving `Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize` with lowercase serialized values. Its `as_str(&self) -> &'static str` returns `doi`, `arxiv`, or `url`; `parse(&str) -> Result<Self>` accepts exactly those values and errors otherwise. Define `ReferenceMatch { doi: String, provenance: Provenance, raw_match: String, match_type: MatchType }`, `UnresolvedUrl { url: String, raw_match: String, candidate_dois: Vec<String> }`, and `ReferenceMatches { confirmed: Vec<ReferenceMatch>, ambiguous: Vec<UnresolvedUrl>, failed_arxiv: Vec<ArxivMatch>, unmatched_dois: u64, unmatched_urls: u64, malformed_fields: u64 }`. The entry point is `match_reference(reference: &serde_json::Value, index: &dyn TargetLookup, max_lookup_bytes: usize) -> Result<ReferenceMatches>`. Only the reference object, not the whole Crossref work, is materialized as a Value.

- [ ] In `tests/datacite_matching.rs`, define a local `MemoryIndex` implementing Task 3's trait with `BTreeSet<String>` membership and `BTreeMap<String,Vec<String>>` URL mappings. Implement its allocation-budget check. Add the first ambiguity test:

```rust
#[test]
fn a_doi_disambiguates_only_its_own_shared_url() -> anyhow::Result<()> {
    let index = MemoryIndex {
        dois: ["10.1234/a", "10.1234/b", "10.1234/c"].map(String::from).into(),
        urls: [("https://example.org/shared".into(), vec!["10.1234/a".into(), "10.1234/b".into()])].into(),
    };
    let ambiguous = match_reference(&serde_json::json!({"URL":"https://example.org/shared"}), &index, 4096)?;
    assert!(ambiguous.confirmed.is_empty());
    assert_eq!(ambiguous.ambiguous.len(), 1);
    let resolved = match_reference(&serde_json::json!({"DOI":"10.1234/a","doi-asserted-by":"publisher","URL":"https://example.org/shared"}), &index, 4096)?;
    assert_eq!(resolved.confirmed.len(), 1);
    assert_eq!(resolved.confirmed[0].provenance, Provenance::Publisher);
    assert!(resolved.ambiguous.is_empty());
    let unrelated = match_reference(&serde_json::json!({"DOI":"10.1234/c","URL":"https://example.org/shared"}), &index, 4096)?;
    assert_eq!(unrelated.confirmed.len(), 1);
    assert_eq!(unrelated.ambiguous.len(), 1);
    Ok(())
}
```

- [ ] Run `cargo test --test datacite_matching`. Expected: missing matching API, then incorrect ambiguity/count behavior.
- [ ] Implement per-field extraction in the established order `DOI`, `URL`, `article-title`, `journal-title`, `unstructured`. Inspect the structured DOI as a whole first. Scan plausible DOI/URL starts, reject candidates outside the index, and call the existing arXiv parser only when the field contains arXiv context. Do not concatenate fields. Ignore/count non-string field values while mining remaining fields. Token boundaries and punctuation variants must be validated against complete index keys; never shorten arbitrary alphanumeric suffixes. Reuse scratch buffers and limit repeated candidate lookups within a reference.
- [ ] Collect independently validated DOI/arXiv targets first, then resolve each URL's sorted distinct candidate list. Use the exact intersection rule:

```rust
let identified: Vec<&String> = candidates.iter()
    .filter(|doi| independently_identified.contains(*doi))
    .collect();
let resolved = if candidates.len() == 1 {
    candidates.first()
} else if identified.len() == 1 {
    identified.first().copied()
} else {
    None
};
```

Here `candidates: Vec<String>` is the union of the URL lookup keys and `independently_identified: BTreeSet<String>` comes only from DOI/arXiv evidence. `None` with multiple candidates produces `UnresolvedUrl`; an empty candidate list increments an unmatched counter. Deduplicate by canonical target DOI, using strongest provenance, then structured DOI, arXiv, other DOI, URL, field order and substring offset. Preserve raw substrings by slicing the original field. Implement a small source-span map if decoding HTML URL entities; test named/numeric ampersands without changing raw evidence.

- [ ] Add cases for multiple DOI targets in a single reference, two identifying members of a shared URL, HTTP/HTTPS ambiguity, false path-prefix matches, punctuation, encoded DOI resolver URLs, HTML escapes, repeated URL evidence, all legacy arXiv patterns, invalid arXiv, no bare numeric arXiv matches, and non-string `DOI` plus usable `unstructured`. Verify arXiv DOI version assertions remain mined when the asserted version differs from the registered base DOI.
- [ ] Run `cargo test --test datacite_matching` and `cargo test extract::arxiv`. Commit as `feat: mine DataCite references with explicit URL ambiguity`.

## Task 5: Bounded partition storage and evidence columns

**Files:** Create `src/streaming/limits.rs`, `src/streaming/partitioning.rs`, `tests/datacite_storage.rs`. Modify `src/streaming/mod.rs`, `segmented_writer.rs`, `kway_merge.rs`, and existing row-construction tests.

**Interfaces:** Add `match_type: MatchType` to `PartitionRow` and `MergeRow`. Produce `PartitionMode { Legacy, Hashed { count: u32 } }`, `WriterLimits { memory_bytes: usize, segment_rows: usize, mode: PartitionMode }`, `SegmentedPartitionWriter::with_limits(&Path, WriterLimits) -> Result<Self>`, `buffered_bytes(&self) -> usize`, and `hashed_partition_key(&str,u32) -> Result<String>`. Keep `SegmentedPartitionWriter::new` as the legacy adapter.

- [ ] Add a single-prefix distribution test with real rows and a small writer budget:

```rust
use crossref_citation_extraction::common::types::MatchType;
use crossref_citation_extraction::streaming::limits::WriterLimits;
use crossref_citation_extraction::streaming::partitioning::PartitionMode;
use crossref_citation_extraction::streaming::segmented_writer::{PartitionRow, SegmentedPartitionWriter};

#[test]
fn bounds_buffers_and_distributes_one_large_prefix() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let mut writer = SegmentedPartitionWriter::with_limits(tmp.path(), WriterLimits {
        memory_bytes: 131_072, segment_rows: 100_000,
        mode: PartitionMode::Hashed { count: 256 },
    })?;
    for i in 0..1_000_u32 {
        let doi = format!("10.5281/zenodo.{i}");
        writer.write(PartitionRow {
            citing_doi: "10.9999/citer".into(), ref_index: i,
            cited_id: doi.clone(), provenance: "mined".into(),
            match_type: MatchType::Doi, raw_match: doi.clone(),
            ref_json: serde_json::json!({"unstructured": doi}).to_string(),
        })?;
        assert!(writer.buffered_bytes() <= 131_072);
    }
    writer.flush_all()?;
    assert_eq!(writer.total_rows_written(), 1_000);
    assert!(writer.partition_count() > 200 && writer.partition_count() <= 256);
    Ok(())
}
```

Also read the resulting Parquet files and verify all input rows, `match_type`, and `raw_match` round-trip. Repeat with many DOI prefixes to check the full identifier determines placement.

- [ ] Run `cargo test --test datacite_storage`. Expected: missing configuration API or failed memory/distribution assertions.
- [ ] Implement stable FNV-1a partitioning and record the algorithm as `fnv1a64-v1`:

```rust
pub fn hashed_partition_key(doi: &str, count: u32) -> anyhow::Result<String> {
    anyhow::ensure!(count.is_power_of_two(), "partitions must be a positive power of two");
    let hash = doi.as_bytes().iter().fold(0xcbf29ce484222325_u64,
        |state, byte| (state ^ u64::from(*byte)).wrapping_mul(0x100000001b3));
    Ok(format!("h{:04x}", hash & u64::from(count - 1)))
}
```

Inputs are already canonical DOIs. Do not use a randomized/default Rust hasher for persistent partition identity.

- [ ] Account for string capacities and vector/storage overhead across all buffers. Before admitting a row, flush largest buffers until it fits. An individually oversized row must return a contextual resource error rather than bypassing the budget. Count serialization/sort allocations separately in aggregation limits. Write a `.part` Parquet file, close/fsync it, then rename. Recover segment counters from existing committed filenames so resume never overwrites earlier segments. Ignore or delete only owned unfinished `.part` files.
- [ ] Extend segment read/write and merge rows with `match_type`. Legacy segments lacking the column default to `arxiv` only in legacy mode. Full mode rejects an old schema. Update all `PartitionRow`/`MergeRow` construction sites found by `rg -n 'PartitionRow \{|MergeRow \{' src tests`. Keep legacy partition keys unchanged.
- [ ] Run `cargo test --test datacite_storage`, `cargo test streaming::segmented_writer`, and `cargo test streaming::kway_merge`. Commit as `perf: bound partition buffers and distribute full DOI keys`.

## Task 6: Resource-aware aggregation and crash-safe resume

**Files:** Create `src/streaming/partition_commit.rs`, `tests/datacite_aggregation.rs`. Modify `src/streaming/aggregator.rs`, `kway_merge.rs`, `checkpoint.rs`, and `limits.rs`.

**Interfaces:** Produce `AggregationLimits { memory_bytes: usize, merge_fan_in: usize, sort_workers: usize }` in `limits.rs` and `aggregate_datacite(partition_dir: &Path, index: &DataciteIndex, outputs: &OutputPaths, limits: AggregationLimits, identity_sha256: &str, resume: bool) -> Result<AggregationStats>`. Add `PartitionCommit { partition: String, identity_sha256: String, files: BTreeMap<String,IndexFile>, stats: AggregationStatsSnapshot }` with `load_verified(&Path,&str) -> Result<Option<Self>>` and `save_atomic(&self,&Path) -> Result<()>`. Reuse `IndexFile` from Task 3 for file metadata. Verify each commit against the caller's identity fingerprint. Produce `aggregate_ambiguities(input_dir: &Path, output: &Path, limits: &AggregationLimits, identity_sha256: &str, resume: bool) -> Result<u64>` for sorted, deduplicated ambiguity JSONL; its result is the retained row count. Keep existing `OutputPaths` fields unchanged.

- [ ] Build the same fixture in enough small segments to exercise both aggregation paths. Include duplicate rows across segments and two references from the same citing work:

```rust
mod support;
use crossref_citation_extraction::{
    common::{output::OutputPaths, types::MatchType},
    index::{datacite::DataciteIndex, datacite_builder::build_datacite_index},
    streaming::{
        aggregator::aggregate_datacite,
        limits::{AggregationLimits, WriterLimits},
        partitioning::PartitionMode,
        segmented_writer::{PartitionRow, SegmentedPartitionWriter},
    },
};

#[test]
fn both_aggregation_paths_deduplicate_reference_tuples() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let source = tmp.path().join("source");
    support::make_release(&source, &[serde_json::json!({"id":"10.1234/a"})],
                          &[("10.1234/a", "findable")])?;
    let bundle = tmp.path().join("bundle");
    build_datacite_index(&source, &bundle, 64)?;
    let index = DataciteIndex::load(&bundle)?;
    let mut results = Vec::new();
    for (name, memory_bytes) in [("memory", 16 * 1024 * 1024), ("external", 64 * 1024)] {
        let parts = tmp.path().join(name);
        let mut writer = SegmentedPartitionWriter::with_limits(&parts, WriterLimits {
            memory_bytes: 131_072, segment_rows: 2,
            mode: PartitionMode::Hashed { count: 1 },
        })?;
        for i in 0..512_u32 {
            writer.write(PartitionRow {
                citing_doi: "10.9999/citer".into(), ref_index: i % 2,
                cited_id: "10.1234/a".into(), provenance: "mined".into(),
                match_type: MatchType::Doi, raw_match: "10.1234/a".into(),
                ref_json: serde_json::json!({"unstructured":"10.1234/a"}).to_string(),
            })?;
        }
        writer.flush_all()?;
        drop(writer);
        let path = tmp.path().join(format!("{name}.jsonl"));
        let outputs = OutputPaths { valid: Some(path.clone()), ..Default::default() };
        aggregate_datacite(&parts, &index, &outputs,
            AggregationLimits { memory_bytes, merge_fan_in: 2, sort_workers: 1 },
            &"a".repeat(64), false)?;
        let rows: Vec<serde_json::Value> = std::fs::read_to_string(path)?
            .lines().map(serde_json::from_str).collect::<Result<_, _>>()?;
        results.push(rows);
    }
    assert_eq!(results[0], results[1]);
    assert_eq!(results[0][0]["cited_by"].as_array().unwrap().len(), 2);
    Ok(())
}
```

Confirm through a test-only path-selection observer that these budgets actually exercise both paths; expose no production flag solely for testing. The two retained entries are distinct reference indices from one citer; Python later reports one citation and two references. This preserves the existing internal grouped-output convention.

- [ ] Run `cargo test --test datacite_aggregation`. Expected: missing full aggregator or different results across paths.
- [ ] Estimate decoded partition bytes from Parquet metadata, stored row byte counts and conservative string overhead. Use that estimate and live group limits to choose in-memory processing. Bound Rayon sort parallelism with a dedicated pool, limit merge fan-in, and merge sorted runs in successive groups until the final merge fits. Do not set per-partition global Polars environment variables from concurrent partitions. Release intermediate runs only after their replacement is closed and verified.
- [ ] Carry `ref_index` and `match_type` through both aggregation paths. Deduplicate on `(cited_id,citing_doi,ref_index)` before counting or emitting. Use deterministic evidence selection for duplicate keys. Sort consistently by that key; enforce a budget on a single cited-work group and return an explicit error on overflow. Validate with the full DOI index while retaining failed arXiv evidence.
- [ ] Write complete per-partition output fragments under an owned staging directory, checksum them, and atomically promote the directory together with `PartitionCommit`. A retry reuses only fragments whose identity and digests verify. Final JSONL outputs are built from committed fragments into temporary files and atomically replaced, never appended onto old aggregate output.

```rust
// Promotion boundary: the marker and fragments are already synced here.
anyhow::ensure!(!committed_dir.exists(), "partition already committed");
std::fs::rename(&staging_dir, &committed_dir)?;
```

Add failpoints in test-only commit boundaries: before rename, after rename/before progress update, and during final assembly. Compare resumed results to a clean run and verify a rerun never overwrites earlier extraction segments. Implement `aggregate_ambiguities` using the same bounded sort/commit approach. Its input is the `.jsonl` segments in the supplied directory, with each row using the spec's ambiguity fields. Deduplicate occurrences by `(citing_doi,ref_index,url)`; reject conflicting candidate lists for that key under the same identity. Create an empty output file when no ambiguity segments exist.

- [ ] Run `cargo test --test datacite_aggregation` and existing streaming tests. Commit as `fix: make full citation aggregation bounded and replay-safe`.

## Task 7: Full-mode streaming extraction

**Files:** Create `src/commands/datacite_pipeline.rs`, `tests/datacite_pipeline.rs`. Modify `src/cli.rs`, `src/commands/mod.rs`, `src/commands/pipeline.rs`, `src/input/crossref.rs`, and `src/streaming/checkpoint.rs`.

**Interfaces:** Add `PipelineArgs.datacite_index: Option<String>` and the resource flags above. Produce `run_datacite_pipeline(args: &PipelineArgs) -> Result<()>`. At the existing `run_pipeline` boundary, dispatch to it only when `--datacite-index` is present. Require exactly one full bundle or a legacy index source; reject mixed sources before any filesystem mutation. Add `--run-identity <json-path>` for the supplied identity in Task 8, with a Rust-only derived identity for local CLI use.

Define internal `WorkerEvent` variants `Rows(Vec<PartitionRow>)`, `Ambiguous(Vec<serde_json::Value>)`, and `FileComplete { filename: String, stats: FileStats }`. `FileStats` includes `items`, `references`, `confirmed_matches`, `ambiguous_urls`, `invalid_arxiv`, `unmatched_dois`, `unmatched_urls`, `malformed_references`, `malformed_fields`, and `filtered_self_citations`, all `u64`.

The final `extraction-report.json` has exactly `kind="datacite-extraction-report"`, `schema_version=2`, `identity_sha256: String`, `completed: bool`, `files_processed: u64`, `fatal_source_errors: u64`, `stats: FileStats`, and `aggregation: AggregationStatsSnapshot`. Write a successful report only after extraction and aggregation finish, with `completed=true` and `fatal_source_errors=0`. Checkpoint progress files use a different filename and cannot satisfy this report contract.

- [ ] Add a CLI integration test that exercises the completed extraction/aggregation path:

```rust
mod support;
use crossref_citation_extraction::index::datacite_builder::build_datacite_index;
use std::{collections::BTreeSet, process::Command};

#[test]
fn full_mode_retains_doi_url_and_arxiv_matches() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let source = tmp.path().join("source");
    support::make_release(&source, &[
        serde_json::json!({"id":"10.1234/a","attributes":{"url":"https://example.org/a"}}),
        serde_json::json!({"id":"10.48550/arxiv.2403.03542"}),
    ], &[("10.1234/a", "findable"), ("10.48550/arxiv.2403.03542", "findable")])?;
    let bundle = tmp.path().join("bundle");
    build_datacite_index(&source, &bundle, 64)?;
    let input = tmp.path().join("crossref.json");
    std::fs::write(&input, serde_json::to_vec(&serde_json::json!({"items":[{
        "DOI":"10.9999/citer", "reference":[
            {"DOI":"10.1234/a","doi-asserted-by":"publisher"},
            {"URL":"https://example.org/a"},
            {"unstructured":"arXiv:2403.03542"}
        ]
    }]}))?)?;
    let output = tmp.path().join("out");
    let status = Command::new(env!("CARGO_BIN_EXE_crossref-citation-extraction"))
        .arg("pipeline").arg("--input").arg(&input)
        .arg("--datacite-index").arg(&bundle)
        .arg("--output-dir").arg(&output)
        .arg("--temp-dir").arg(tmp.path().join("parts"))
        .status()?;
    assert!(status.success());
    let targets: BTreeSet<String> = std::fs::read_to_string(output.join("crossref_valid.jsonl"))?
        .lines().map(|line| {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            row["doi"].as_str().unwrap().to_owned()
        }).collect();
    assert_eq!(targets, ["10.1234/a".into(), "10.48550/arxiv.2403.03542".into()].into());
    Ok(())
}
```

Add a malformed top-level JSON test requiring a nonzero exit and no successful extraction report.

- [ ] Run `cargo test --test datacite_pipeline`. Expected: unrecognized flag or no full-mode output.
- [ ] Implement bounded worker events with `crossbeam_channel::bounded(queue_batches)`. Flush a worker batch before its estimated capacity exceeds `result_batch_mb`; apply the same limit to ambiguity batches. Check the lookup limit before materializing large candidate lists. The writer handles all partition and ambiguity files. Store ambiguity batches as synced, atomically renamed `ambiguities/segment_<sequence>.jsonl` files and recover their next sequence on resume. Do not return a complete member's rows in one `FileResult`.

```rust
if batch_bytes.checked_add(row_bytes).ok_or_else(|| anyhow::anyhow!("batch size overflow"))? > batch_limit {
    if !batch.is_empty() {
        tx.send(WorkerEvent::Rows(std::mem::take(&mut batch)))?;
        batch_bytes = 0;
    }
}
anyhow::ensure!(row_bytes <= batch_limit, "reference exceeds result batch budget");
batch.push(row);
batch_bytes += row_bytes;
```

Here `row: PartitionRow`, `row_bytes` uses the writer's shared row-size accounting, and `batch_limit` is the converted CLI limit. Drop receivers on writer failure and propagate worker/reader errors so full queues cannot deadlock shutdown.

- [ ] Reuse Crossref source traversal while adding a maximum member allocation in full mode. Preserve the legacy wrapper signature. Top-level JSON and archive errors abort; malformed references/field values increment counters without discarding other usable fields. Canonicalize citing DOIs, filter self-citations and provenance, attach `ref_index`, serialize original reference JSON only for retained evidence, and route failed arXiv candidates to validation/failed output. Include them in partition storage without admitting arbitrary unmatched DOI candidates.
- [ ] Persist a full-mode identity before resumable work. Also record the canonical provenance/output selections in the local checkpoint and reject reuse with different selections. Store completed filenames and their counters only after partition and ambiguity data are flushed and synced. Ignore incomplete trailing checkpoint records after a crash. Replayed uncommitted rows are allowed internally but must deduplicate by their stable keys before final output. After every member and worker succeeds, call `aggregate_datacite` and `aggregate_ambiguities` with the identity fingerprint. Assemble counters from completed-file records so retries do not double-count them. Emit `extraction-report.json` and structured progress events. Update existing `PipelineArgs` construction sites found by `rg -n 'PipelineArgs \{' src tests` with legacy-compatible defaults for added fields.
- [ ] Test many matches in one member, one oversized member, oversized ambiguity candidates, invalid non-string DOI fields, writer failure with full queues, source errors, duplicate replay and mixed CLI index arguments. Run `cargo test --test datacite_pipeline` plus legacy integration tests. Commit as `feat: stream full DataCite reference extraction`.

## Task 8: Full-mode Python identity and verified bundle cache

**Files:** Create `citations_pipeline/full_state.py`, `citations_pipeline/datacite_index.py`, `tests/test_full_state.py`, `tests/test_datacite_index.py`. Modify `citations_pipeline/paths.py`, `citations_pipeline/extractor.py`, and `tests/conftest.py`.

**Interfaces:** Keep old `arxiv_index.py` and legacy marker classes operational. Add keyword-only `prefix_name` to `PipelinePaths.from_roots`, defaulting to the existing prefix; full mode uses `crossref-datacite-citations-latest`. Add `bundle_prefix(release: str, inventory_sha256: str) -> Path` and validate both inputs. Bundle files are cached beneath `indexes/<release>-v2-<inventory-sha256>/generations/<uuid>/`. The cache-key directory contains `_BUNDLE_CURRENT.json` with `kind="datacite-bundle-cache"`, `schema_version=2`, `release`, `source_inventory_sha256`, `relative_generation`, and `generation_manifest_sha256`. Validate `relative_generation` as exactly `generations/<uuid>` before resolving it.

Define `ResourceSettings` with the integer fields and defaults from the resource table, plus `validate() -> None` and `rust_args() -> list[str]`. `rust_args` forwards extraction flags only; `index_sort_memory_mb` goes to the index command and `duckdb_memory_mb` goes to the Python transform. Define immutable `RunIdentity` with these fields:

```python
@dataclass(frozen=True)
class RunIdentity:
    archive_sha256: str
    datacite_release: str
    source_inventory_sha256: str
    bundle_manifest_sha256: str
    source_tree_sha256: str
    extraction_mode: str
    index_version: int
    normalization_version: int
    extraction_version: int
    partition_algorithm: str
    partitions: int
    datacite_publish_version: int
    arxiv_publish_version: int

    def fingerprint(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(payload).hexdigest()
```

Import `dataclass`, `asdict`, `hashlib`, and `json` in `full_state.py`. `from_dict` validates exact keys, lowercase SHA-256, release form, versions and `extraction_mode="datacite"`. Define `FileMeta(size:int,sha256:str)` and `FullExtractionMarker(kind:str,schema_version:int,identity:RunIdentity,snapshot_id:str,snapshot_provenance:str,derived_prefix:str,files:dict[str,FileMeta],counts:dict[str,Any],completed_at:str)` with strict `to_dict`/`from_dict`; kind is `datacite-extraction`, schema is 2. Permit zero-byte diagnostic JSONL, while validating required files separately.

`DataciteRelease` in the new index module has `release:str`, `root:Path`, `inventory_sha256:str`. `BundleResult` has `directory:Path`, `release:str`, `inventory_sha256:str`, `manifest_sha256:str`, and `reused:bool`. Produce:

- `discover_release(bucket_root: Path, pinned: str | None = None) -> DataciteRelease`.
- `ensure_bundle(*, paths: PipelinePaths, release: DataciteRelease, build_index: Callable[[Path,Path],None], store: BucketStore | None, events: Any, force: bool = False) -> BundleResult`.
- `run_build_datacite_index(binary: ExtractorBinary, records: Path, output: Path, sort_memory_mb: int, runner: Runner = subprocess.run) -> None` in the extractor wrapper.

- [ ] Write the first marker/resource tests:

```python
from citations_pipeline.full_state import ResourceSettings, RunIdentity

def test_resources_reject_non_power_of_two_partitions():
    with pytest.raises(ValueError, match="power of two"):
        ResourceSettings(partitions=3).validate()

def test_identity_changes_when_registry_changes():
    from dataclasses import replace
    identity = RunIdentity("a"*64, "2026-07", "b"*64, "c"*64, "d"*64,
                           "datacite", 2, 1, 2, "fnv1a64-v1", 256, 2, 1)
    assert identity.fingerprint() != replace(identity, bundle_manifest_sha256="e"*64).fingerprint()
```

Import `pytest`. Add serialization round trips and rejection of every legacy marker kind/version.

- [ ] Run `uv run pytest -q tests/test_full_state.py tests/test_datacite_index.py`. Expected: import failures, then cache validation failures.
- [ ] Implement discovery using `_TRANSFER_COMPLETE.json`, complete `STATUS.json`, and the selected monthly inventory. Match the Rust inventory serialization exactly using ordered rows `[relative_path,size,sha256_hex]` and compact UTF-8 JSON; add a cross-language fixture comparison to prevent digest drift. Missing CSV inventory files fail discovery/build. Use local staged bundle files, not FUSE mappings, during extraction.
- [ ] Implement complete cache verification. Resolve `_BUNDLE_CURRENT.json`, verify the referenced `_INDEX_COMPLETE.json` digest, then copy every listed bundle file plus marker locally. Verify all SHA-256 values and format/normalization versions before reuse. A same-size sidecar mutation must force a rebuild. Persist a newly built bundle to a fresh immutable generation and write its marker last. Verify that generation before atomically replacing the local pointer and uploading the pointer through the bucket API. Existing generations remain untouched. Treat `--force` as recomputation, not bypassing verification. `BundleResult.directory` always names the verified local copy.

```python
# After copying the candidate bundle into an owned local staging directory:
for name, metadata in manifest["files"].items():
    candidate = local_staging / name
    if candidate.stat().st_size != metadata["size"]:
        raise ValueError(f"bundle size mismatch: {name}")
    if sha256_file(candidate) != metadata["sha256"]:
        raise ValueError(f"bundle checksum mismatch: {name}")
```

Validate relative filenames before this loop. `sha256_file` is the existing state helper.

- [ ] Add a `full_datacite_release` pytest fixture in `tests/conftest.py` that creates the same monthly file formats as the Rust support helper, including actual checksums. Test pinned/latest-complete discovery, absent CSV, corrupt sidecar, wrong normalization version, force rebuild, incomplete cache and successful reuse without calling the builder. Run the named Python tests plus existing index/path/state tests.
- [ ] Commit as `feat: cache verified DataCite bundles with full-run identity`.

## Task 9: General citation data and arXiv compatibility projection

**Files:** Create `citations_pipeline/datacite_publish.py`, `citations_pipeline/publish_common.py`, `tests/test_datacite_transform.py`. Modify `citations_pipeline/publish.py` only to share stable helper functions and retain its legacy interfaces.

**Interfaces:** Consume the full JSONL contracts and `ResourceSettings`. Produce `transform_datacite_outputs(valid_path: Path, ambiguous_path: Path, transform_dir: Path, events: Any, resources: ResourceSettings) -> dict[str, DatasetTransform]` keyed by `datacite`/`arxiv`. Define:

```python
@dataclass(frozen=True)
class DatasetTransform:
    files: dict[str, Path]
    config_files: dict[str, tuple[str, ...]]
    stats: dict[str, ConfigStats]
    reference_keys: dict[str, tuple[str, ...]]
    dropped_reference_keys: tuple[str, ...]
    ambiguity_rows: int
```

`ConfigStats` is the existing type in `publish.py`. `files` maps repository-relative file names to local paths; `config_files` maps configurations to ordered repository-relative names. General output uses `data/<config>/part-00000.parquet` and further numbered shards. arXiv retains exactly `data/{all,asserted,mined}.parquet`. General shard targets are 1 GiB of uncompressed Arrow batches, with a boundary only between complete rows. Record actual compressed sizes for the production estimate.

- [ ] Add a transform test with two targets and two references from one citing work:

```python
def test_pair_counts_and_legacy_arxiv_schema(tmp_path):
    def entry(citer, ref_index, provenance, match_type, reference):
        return {"doi": citer, "ref_index": ref_index, "provenance": provenance,
                "match_type": match_type, "raw_match": next(iter(reference.values())),
                "reference": reference}

    valid = tmp_path / "valid.jsonl"
    valid.write_text("\n".join(json.dumps(record) for record in [
        {"doi": "10.1234/a", "cited_by": [
            entry("10.9999/w1", 0, "publisher", "doi", {"DOI": "10.1234/a"}),
            entry("10.9999/w1", 1, "mined", "url", {"URL": "https://example.org/a"})]},
        {"doi": "10.48550/arxiv.2403.03542", "cited_by": [
            entry("10.9999/w2", 0, "mined", "arxiv", {"unstructured": "arXiv:2403.03542"})]},
    ]) + "\n")
    ambiguous = tmp_path / "ambiguous.jsonl"
    ambiguous.write_text("")
    result = transform_datacite_outputs(valid, ambiguous, tmp_path / "out", EventLogger(),
                                        ResourceSettings(duckdb_memory_mb=256))
    assert result["datacite"].stats["all"] == ConfigStats(works=2, citations=2, references=3)
    assert result["datacite"].stats["mined"] == ConfigStats(works=2, citations=2, references=2)
    arxiv_file = result["arxiv"].files["data/all.parquet"]
    assert pq.read_schema(arxiv_file).names == ["arxiv_doi", "arxiv_id", "citation_count", "cited_by", "reference_count"]
    row = pq.read_table(arxiv_file).to_pylist()[0]
    assert row["arxiv_doi"] == "10.48550/arXiv.2403.03542"
    assert set(row["cited_by"][0]["matches"][0]) == {"provenance", "raw_match", "reference"}
    assert result["datacite"].ambiguity_rows == 0
```

Import `json`, `pyarrow.parquet as pq`, `ConfigStats`, `EventLogger`, `ResourceSettings` and `transform_datacite_outputs` from their named modules.

- [ ] Run `uv run pytest -q tests/test_datacite_transform.py`. Expected: import failure, then count/schema differences.
- [ ] Factor only the existing quoting/stringification helpers into `publish_common.py`, keeping compatibility imports in `publish.py`. Flatten confirmed JSONL once into columns `target_doi,citing_doi,ref_index,provenance,match_type,raw_match,ref__<key>`. Scan reference keys separately for each destination and configuration. Preserve arbitrary original JSON in raw artifacts and report keys excluded from published struct fields.
- [ ] Implement the two grouping levels in DuckDB with explicit memory and spill-directory settings. The core query for each destination/configuration follows this structure; build identifiers with the shared quoting helper:

```sql
WITH per_citer AS (
  SELECT target_doi, citing_doi,
         list(struct_pack(provenance := provenance,
                          match_type := match_type,
                          raw_match := raw_match,
                          reference := reference_value)
              ORDER BY ref_index) AS matches,
         CASE WHEN bool_or(provenance = 'publisher') THEN 'publisher'
              WHEN bool_or(provenance = 'crossref') THEN 'crossref'
              ELSE 'mined' END AS provenance,
         count(*)::BIGINT AS reference_count
  FROM selected_references
  GROUP BY target_doi, citing_doi
)
SELECT target_doi AS doi,
       count(*)::BIGINT AS citation_count,
       list(struct_pack(doi := citing_doi, matches := matches, provenance := provenance)
            ORDER BY citing_doi) AS cited_by,
       sum(reference_count)::BIGINT AS reference_count
FROM per_citer
GROUP BY target_doi
ORDER BY citation_count DESC, doi
```

`selected_references` is a temporary view over flat Parquet, filtered by the destination's DOI scope and configuration's provenance set; `reference_value` is `struct_pack` of that scope's ordered reference keys. For arXiv, build the inner struct without `match_type`, render the `arXiv.` DOI casing, add `arxiv_id`, and retain the legacy top-level order. Do not filter arXiv after computing counts over all targets.

- [ ] Implement `write_dataset_shards(reader: pa.RecordBatchReader, out_dir: Path, repo_prefix: str, target_bytes: int, single_file: bool) -> dict[str,Path]`. Write batches in query order, close a shard before the next batch exceeds the target, and create a schema-bearing empty file for an empty configuration. For an empty config use that destination's `all` reference schema; an empty ambiguity config uses the general dataset's reference schema. Write original reference values using the established stringification convention. Budget the Arrow batches and fail on an oversized single result rather than dropping it.
- [ ] Transform ambiguity JSONL independently, with `citing_doi`, `ref_index`, `url`, `raw_match`, `candidate_dois`, `reference`. Deduplicate its stable key and verify candidate lists are sorted/unique and have at least two members. Validate `citation_count == len(cited_by)`, reference totals, `all.references == asserted.references + mined.references`, and exact arXiv subset equivalence. Test mixed provenance, empty asserted/mined/ambiguity configurations, invalid match types, retained ref ordering, Unicode, malformed JSONL, and repeatability under different input order.
- [ ] Run the new transform tests and `tests/test_publish_transform.py`. Commit as `feat: prepare DataCite datasets and preserve the arXiv schema`.

## Task 10: Guarded paired publication and recoverable receipts

**Files:** Create `citations_pipeline/paired_publish.py`, `tests/test_paired_publish.py`. Modify `citations_pipeline/full_state.py` and add focused API verification cases alongside `tests/test_publish_commit.py`.

**Interfaces:** Produce `PreparedDataset(repo_id:str,files:dict[str,Path],artifact_digest:str)`. Its `files` includes the dataset card and `metadata/pipeline-manifest.json`. The manifest has `kind="citation-dataset"`, `manifest_version=2`, `dataset_kind`, `publication_schema_version`, `identity`, `snapshot_id`, `extractor_version`, `config_files`, `counts`, `ambiguity_rows`, and `files`. `identity` is serialized `RunIdentity`; `counts` maps citation configurations to serialized `ConfigStats`; `files` maps repository-relative data/README paths to `FileMeta`. Exclude the manifest itself from that list to avoid a circular digest. Serialize with `sort_keys=True`, compact separators, UTF-8, and no trailing newline. `artifact_digest` is the SHA-256 of those canonical manifest bytes.

Define `PublicationReceipt(repo_id:str,commit_sha:str,commit_url:str,artifact_digest:str)` and `FullPipelineMarker(kind:str,schema_version:int,identity:RunIdentity,snapshot_id:str,derived_prefix:str,publications:dict[str,PublicationReceipt],completed_at:str)` with strict serialization. Kind is `datacite-pipeline`, version is 2, and publications must contain exactly `datacite` and `arxiv`.

Produce these functions:

- `prepare_dataset(kind: str, transform: DatasetTransform, identity: RunIdentity, snapshot_id: str, extractor_version: str, out_dir: Path) -> PreparedDataset`.
- `publish_prepared(*, api: Any, token: str | None, prepared: PreparedDataset, events: Any) -> PublicationReceipt`.
- `verify_current(*, api: Any, token: str | None, prepared: PreparedDataset, receipt: PublicationReceipt) -> None`.
- `verify_manifest_files(*, api: Any, token: str | None, repo_id: str, revision: str, manifest: dict[str,Any]) -> None`, shared by publication and independent post-run verification.
- `publish_pair(prepared: dict[str,PreparedDataset], receipts: dict[str,PublicationReceipt], commit_one: Callable[[PreparedDataset],PublicationReceipt], save_receipt: Callable[[str,PublicationReceipt],None], check_current: Callable[[PreparedDataset,PublicationReceipt],None]) -> dict[str,PublicationReceipt]`.

- [ ] Add the paired retry test without network calls:

```python
def test_retry_publishes_only_the_missing_destination(tmp_path):
    prepared = {
        "datacite": PreparedDataset("cometadata/crossref-datacite-citations", {}, "a"*64),
        "arxiv": PreparedDataset("cometadata/crossref-arxiv-citations", {}, "b"*64),
    }
    calls, receipts = [], {}
    fail_arxiv = True

    def commit_one(dataset):
        calls.append(dataset.repo_id)
        if dataset.repo_id.endswith("arxiv-citations") and fail_arxiv:
            raise RuntimeError("injected publication failure")
        return PublicationReceipt(dataset.repo_id, "c"*40,
                                  "https://huggingface.co/commit/c", dataset.artifact_digest)

    def save(kind, receipt):
        receipts[kind] = receipt

    with pytest.raises(RuntimeError, match="injected"):
        publish_pair(prepared, receipts, commit_one, save, lambda dataset, receipt: None)
    assert set(receipts) == {"datacite"}
    fail_arxiv = False
    result = publish_pair(prepared, receipts, commit_one, save, lambda dataset, receipt: None)
    assert set(result) == {"datacite", "arxiv"}
    assert calls.count(prepared["datacite"].repo_id) == 1
    assert calls.count(prepared["arxiv"].repo_id) == 2
```

Import the declared classes/functions and `pytest`. The no-op checker here isolates orchestration; separate tests exercise actual remote verification.

- [ ] Run `uv run pytest -q tests/test_paired_publish.py`. Expected: absent functions, then the duplicate-publication assertion fails until retry handling exists.
- [ ] Implement dataset cards with exact configuration paths, CC0, snapshot/release identities, counts, ambiguity policy and limitations. Create the new public repository idempotently. Read its parent SHA, then use one `create_commit` for every data file, README and manifest. Delete obsolete managed general-data shards in that same guarded commit so old rows cannot remain under config globs. Preserve unrelated repository files. Use the current existing arXiv file paths.
- [ ] Implement recovery and verification. First inspect the current manifest: if it has the expected artifact digest, verify the actual remote files and reconstruct a receipt without another commit. Otherwise a new publication uses the observed parent SHA. After committing, verify both that revision and the current files. `verify_manifest_files` validates manifest field types and relative paths, checks the complete managed-file set, and verifies every listed size and digest at the supplied immutable revision. Use authoritative remote file size and SHA-256 metadata for large files when available; download/hash files when that metadata is absent. For small Git blobs, download/hash rather than comparing a Git object ID to a file SHA-256. `verify_current` resolves current HEAD, checks its manifest digest against the receipt, calls the shared verifier, and checks that HEAD did not move during verification.
- [ ] Implement paired orchestration in fixed order:

```python
result = dict(receipts)
for kind in ("datacite", "arxiv"):
    if kind in result:
        check_current(prepared[kind], result[kind])
    else:
        result[kind] = commit_one(prepared[kind])
        save_receipt(kind, result[kind])
for kind in ("datacite", "arxiv"):
    check_current(prepared[kind], result[kind])
return result
```

The caller activates only after this returns. A receipt with wrong repo/artifact identity or changed current files raises an error. A receipt-journal write failure is recoverable through the committed remote manifest. Do not roll back the first repository when the second fails.

- [ ] Test parent conflicts, changed current files after a successful historical commit, same-size remote mutation, unexpected/missing managed shards, lost receipts, malformed manifests, first/second commit failures, and failure of the last verification pass. Assert no complete pipeline marker is written on any partial failure. Run new paired-publication tests and existing publish-commit tests. Commit as `feat: publish paired citation datasets with verified retry state`.

## Task 11: Full job orchestration and artifact persistence

**Files:** Create `citations_pipeline/full_pipeline.py`, `citations_pipeline/pipeline_io.py`, `tests/test_full_pipeline.py`. Modify `citations_pipeline/pipeline.py`, `extractor.py`, and `full_state.py`.

**Interfaces:** Extend `PipelineConfig` with `mode:str="legacy"`, `resources:ResourceSettings=field(default_factory=ResourceSettings)`, and `expected_snapshot_id:str | None=None`; import `field` from `dataclasses`. `mode` is one of `legacy`, `datacite`, `smoke`, or `benchmark`. Produce `FullExtractionOutputs(valid:Path,failed:Path,publisher:Path,crossref:Path,mined:Path,ambiguous_urls:Path,report:Path)` and `run_datacite_extraction(binary: ExtractorBinary, *, archive: Path, bundle: Path, identity_path: Path, output_dir: Path, temp_dir: Path, resources: ResourceSettings, events: Any, runner: Runner = subprocess.run) -> FullExtractionOutputs`.

Produce `run_full_pipeline(config: PipelineConfig, deps: FullDependencies | None = None) -> dict[str,Any]`. `FullDependencies` is a frozen dataclass of callables with fields `events`, `api_factory`, `store_factory`, `resolve_mirror`, `fallback_transfer`, `build_extractor`, `discover_release`, `ensure_bundle`, `run_extraction`, `transform`, `prepare_dataset`, `publish_pair`, `verify_current`, and `publish_prepared`. Default callables use the named functions from prior tasks and existing snapshot/bucket modules. Test injection must use these same names.

The returned dictionary has `status` equal to `completed` or `no-op`, `snapshot_id`, `datacite_release`, `derived_prefix`, `identity_sha256`, `source_tree_sha256`, `extraction_reused`, `dataset_urls`, `publications`, and `counts`. `dataset_urls` and `publications` have exactly `datacite`/`arxiv` keys; publications are serialized receipts, and counts contain each destination's configuration statistics plus the ambiguity row count. Return this success result only after both destinations and the activation marker verify. Failures raise an exception and retain phase events without a success result.

- [ ] Create `full_pipeline_case` in `tests/test_full_pipeline.py`, using `SimpleNamespace` or the defined dataclasses to supply a matching source digest, resolved snapshot, verified bundle, seven real fixture artifacts, paired transforms and fake publication receipts. It records phase calls and uploaded marker kinds. Add the failure test:

```python
def test_failed_second_publication_keeps_extraction_reusable(full_pipeline_case):
    case = full_pipeline_case
    case.fail_repo = "arxiv"
    with pytest.raises(RuntimeError, match="publication"):
        run_full_pipeline(case.config, case.dependencies())
    assert "datacite-extraction" in case.persisted_marker_kinds
    assert "datacite-pipeline" not in case.persisted_marker_kinds
    case.fail_repo = None
    run_full_pipeline(case.config, case.dependencies())
    assert case.extraction_calls == 1
    assert case.publication_calls["datacite"] == 1
    assert case.publication_calls["arxiv"] == 2
```

Implement the fixture's `dependencies()` using `FullDependencies`; use `LocalBucketStore` for durable files so retry reads real marker contents instead of fabricated return values.

- [ ] Run `uv run pytest -q tests/test_full_pipeline.py`. Expected: missing runner/dependency types, then incorrect retry behavior.
- [ ] Move only reusable source staging/compression helpers into `pipeline_io.py`. Preserve the legacy import/call behavior. Hash Crossref during the staging copy and compare the expected archive digest. Reuse fallback transfer's verified local archive. Validate `expected_snapshot_id` before expensive work. Persist benchmark-selected snapshot and DataCite release identities in the run identity rather than silently selecting newer inputs later.
- [ ] Implement the Rust wrapper with explicit bundle, identity and resource arguments. Verify all seven required paths and parse `extraction-report.json`; never call `touch()` to manufacture missing outputs. Empty failed/ambiguity JSONL is valid. Full extraction must report complete source traversal and zero fatal source errors. Confirm the returned identity and counters match the invocation.

```python
arguments = [str(binary.path), "pipeline", "--input", str(archive),
             "--datacite-index", str(bundle), "--run-identity", str(identity_path),
             "--outputs", "all", "--output-dir", str(output_dir),
             "--temp-dir", str(temp_dir), *resources.rust_args()]
_run(runner, arguments)
```

`_run` is the existing extractor helper. Verify missing-path failure and flag forwarding in `tests/test_extractor.py`.

- [ ] Implement the spec's phase sequence with the new bundle cache, full extraction, both transforms, dataset preparation, durable compressed raw outputs and prepared data/card manifests. The derived generation holds `datacite/` and `arxiv/` subtrees. Store the extraction marker only after every listed file verifies. On reuse, download/hash each needed file before using it; a corrupt derived file causes recomputation or an explicit error, never unchecked publication.
- [ ] Save each `PublicationReceipt` through a per-generation `_PUBLICATIONS.json` journal using atomic local JSON plus the bucket API. Activate `_PIPELINE_COMPLETE.json` only after both current remote publications verify. Keep old arXiv-owned prefixes intact and clean only the prior full-pipeline generation after activation. Failures before activation leave existing active markers intact. Retain the source identity in reports and report `no-op` only when all current files still match.
- [ ] Add tests for mirror and fallback sources, snapshot mismatch, bundle corruption, extraction failure, missing output, empty required `all`, transformed-file corruption, force behavior, receipt-write failure, no-op current-file verification, and cleanup boundaries. Run full pipeline tests plus existing pipeline/mirror/transfer/extractor tests. Commit as `feat: orchestrate full DataCite extraction and paired publication`.

## Task 12: Deterministic full-chain smoke and compatibility coverage

**Files:** Create `citations_pipeline/datacite_smoke.py`, `tests/test_datacite_smoke.py`. Use the completed extraction/transform interfaces without changing job dispatch yet.

**Interfaces:** Produce `create_datacite_smoke(root: Path) -> tuple[Path,Path]`, returning a Crossref archive and complete DataCite release root, and `run_datacite_smoke(config: PipelineConfig) -> dict[str,Any]`. Build real index/engine/transform artifacts; inject no network-dependent dependencies. Generate gzip with `mtime=0`, deterministic tar metadata, real CSV/JSONL/manifest/STATUS, and no tokens or mount.

Use this exact registry:

| DOI | Landing page | Source |
| --- | --- | --- |
| `10.1234/a` | `https://example.org/a` | JSONL + CSV |
| `10.1234/b` | `https://example.org/shared` | JSONL + CSV |
| `10.1234/c` | `https://example.org/shared` | JSONL + CSV |
| `10.48550/arxiv.2403.03542` | `https://arxiv.org/abs/2403.03542` | JSONL + CSV |
| `10.1234/retired` | Missing | Registered-state CSV only |

Use citing DOIs `10.9999/w1`, `10.9999/w2`, `10.9999/w3` and these zero-based references:

| Citer / index | Reference |
| --- | --- |
| w1 / 0 | Publisher DOI `10.1234/a`, its URL, and the same DOI in text |
| w1 / 1 | URL `https://example.org/a` |
| w1 / 2 | `unstructured: arXiv:2403.03542v2` |
| w2 / 0 | DOI `10.48550/arXiv.2403.03542`, asserted by Crossref |
| w2 / 1 | Shared URL only |
| w2 / 2 | Publisher DOI `10.1234/b` plus shared URL |
| w3 / 0 | DOI `10.1234/retired`, no assertion field |
| w3 / 1 | `unstructured: arXiv:2499.99999` |
| w3 / 2 | DOI `10.9999/not-in-registry` |
| w3 / 3 | Boolean `DOI: false`, usable `unstructured: https://example.org/a` |
| w3 / 4 | Both `10.1234/b` and `10.1234/c` in text plus shared URL |
| w3 / 5 | A bare string instead of a reference object |

Add a work `10.1234/a` with a reference to itself to test self-citation filtering; it adds no published records. This fixture's expected counts are:

```python
SMOKE_COUNTS = {
    "datacite": {
        "all": {"works": 5, "citations": 8, "references": 9},
        "asserted": {"works": 3, "citations": 3, "references": 3},
        "mined": {"works": 5, "citations": 6, "references": 6},
    },
    "arxiv": {
        "all": {"works": 1, "citations": 2, "references": 2},
        "asserted": {"works": 1, "citations": 1, "references": 1},
        "mined": {"works": 1, "citations": 1, "references": 1},
    },
    "ambiguous_urls": 2,
    "failed_arxiv_works": 1,
}
```

- [ ] Put the literal `SMOKE_COUNTS` dictionary above in the test module and add these tests:

```python
from pathlib import Path
from citations_pipeline.datacite_smoke import create_datacite_smoke, run_datacite_smoke
from citations_pipeline.paths import runtime_source_digest
from citations_pipeline.pipeline import PipelineConfig

def test_smoke_fixture_is_reproducible(tmp_path):
    left, right = tmp_path / "left", tmp_path / "right"
    create_datacite_smoke(left)
    create_datacite_smoke(right)
    def contents(root):
        return {p.relative_to(root).as_posix(): p.read_bytes()
                for p in root.rglob("*") if p.is_file()}
    assert contents(left) == contents(right)

def test_real_smoke_counts(tmp_path):
    root = Path(__file__).resolve().parents[1]
    result = run_datacite_smoke(PipelineConfig(
        source_root=root, source_digest=runtime_source_digest(root),
        work_root=tmp_path, bucket_root=None, smoke=True, force=False,
        datacite_release=None, hf_token=None, crossref_token=None,
        run_id="test-datacite-smoke", mode="smoke",
    ))
    assert result["status"] == "smoke-completed"
    assert result["counts"] == SMOKE_COUNTS
```

Run `uv run pytest -q tests/test_datacite_smoke.py`. Expected: missing generator/smoke function.

- [ ] Implement fixture generation and assert record identities as well as counts: A has three references from two citers; arXiv has two citations; B is disambiguated for w2; w3's two independent DOI citations remain while its shared URL stays ambiguous; the retired DOI survives; self-citations and unknown DOI are absent.
- [ ] Run the real Rust build, bundle build, extraction and both transforms with bounded smoke settings. Compare all nested output schemas and records. Re-run on the same fixture with forced external aggregation and compare logical output to the in-memory path. Do not publish from smoke even if local HF tokens are present.
- [ ] Return a dictionary with `status="smoke-completed"`, exact counts, source identity, and no dataset commit claims. The job-entry task serializes it into `RESULT_JSON=`. Retain legacy smoke fixtures/tests for the old mode.
- [ ] Run `uv run pytest -q tests/test_datacite_smoke.py`, then call the new smoke runner locally:

```bash
uv run python - <<'PY'
from pathlib import Path
from citations_pipeline.datacite_smoke import run_datacite_smoke
from citations_pipeline.paths import runtime_source_digest
from citations_pipeline.pipeline import PipelineConfig
root = Path.cwd()
result = run_datacite_smoke(PipelineConfig(
    source_root=root, source_digest=runtime_source_digest(root),
    work_root=Path("/tmp/datacite-citations-smoke"), bucket_root=None,
    smoke=True, force=False, datacite_release=None, hf_token=None,
    crossref_token=None, run_id="local-datacite-smoke", mode="smoke",
))
assert result["status"] == "smoke-completed"
print(result)
PY
```

- [ ] Commit as `test: exercise full DataCite and arXiv smoke contracts`.

## Task 13: Representative benchmark and measured job settings

**Files:** Create `citations_pipeline/benchmark.py`, `tests/test_datacite_benchmark.py`. Modify `benches/extraction_bench.rs`. The subsequent job-entry task registers benchmark dispatch.

**Interfaces:** Produce `sample_crossref_archive(source: Path, destination: Path, stride: int, window: int) -> dict[str,Any]`, `evaluate_readiness(report: dict[str,Any], *, memory_bytes: int, disk_bytes: int, timeout_seconds: int) -> dict[str,Any]`, and `run_benchmark(config: PipelineConfig) -> dict[str,Any]`. Benchmark mode shares acquisition, full bundle building, extraction and transformation code, but never invokes publication. Persist reports under the full pipeline's `benchmarks/<run-id>/`.

- [ ] Add the sampler and readiness tests:

```python
import io
import tarfile
from citations_pipeline.benchmark import evaluate_readiness, sample_crossref_archive

def test_sample_preserves_separated_whole_members(tmp_path):
    source, sample = tmp_path / "source.tar.gz", tmp_path / "sample.tar.gz"
    with tarfile.open(source, "w:gz") as archive:
        for number in range(12):
            content = f'{{"items":[],"number":{number}}}'.encode()
            member = tarfile.TarInfo(f"{number}.json")
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
    sample_crossref_archive(source, sample, stride=5, window=2)
    with tarfile.open(sample, "r:gz") as archive:
        actual = {member.name: archive.extractfile(member).read()
                  for member in archive.getmembers()}
    assert actual == {f"{n}.json": f'{{"items":[],"number":{n}}}'.encode()
                      for n in [0, 1, 5, 6, 10, 11]}

def test_readiness_requires_memory_disk_and_time_headroom():
    report = {"peak_memory_bytes": 76, "projected_peak_disk_bytes": 60,
              "projected_total_seconds": 70}
    result = evaluate_readiness(report, memory_bytes=100, disk_bytes=100, timeout_seconds=100)
    assert result["ready"] is False
    assert "memory" in result["failed_limits"]
    report["peak_memory_bytes"] = 75
    report["projected_total_seconds"] = 81
    assert evaluate_readiness(report, memory_bytes=100, disk_bytes=100,
                              timeout_seconds=100)["failed_limits"] == ["time"]
```

- [ ] Run `uv run pytest -q tests/test_datacite_benchmark.py`. Expected: absent sampler/evaluator.
- [ ] Implement streaming sample selection using `tarfile` stream mode and whole original members, default `stride=5000,window=8`. Record every selected member and total traversal count. Sampling still decompresses the source stream; measure its read/decompression time separately from extraction on the compact sample. State the coverage and sample size in the report. A sample must include enough windows to span the archive; a tiny source uses all members and is marked unsuitable for production extrapolation.
- [ ] Use the full DataCite bundle. Benchmark both the expanded matcher and legacy arXiv extraction on the same sampled archive. Record actual expected/retained arXiv records for semantic comparison, not just timings. Measure full-mode extraction, both transforms, index preparation, source transfer/decompression, compression and bucket persistence separately. Include DOI-heavy and URL-heavy samples, plus an explicit high-degree synthetic target and shared-URL stress case. Keep semantic results from real samples separate from synthetic stress results.
- [ ] Record peak process-tree/cgroup memory and work-volume disk high-water marks throughout the run. Use process metrics or Linux cgroup counters, with a portable local fallback; do not infer memory solely from serialized output size. Estimate full matched-output size from per-window rates conservatively, include both raw/derived copies and temporary sort files, and account for archive deletion after extraction. Extrapolate from the slowest representative window and include measured fixed costs; report formulas and uncertainty.
- [ ] Implement the acceptance calculation:

```python
failed = []
if report["peak_memory_bytes"] > memory_bytes * 0.75:
    failed.append("memory")
if report["projected_peak_disk_bytes"] > disk_bytes * 0.75:
    failed.append("disk")
if report["projected_total_seconds"] > timeout_seconds * 0.80:
    failed.append("time")
return {"ready": not failed, "failed_limits": failed}
```

Write `benchmark-report.json` with source/archive/bundle identities and, only when ready, `production-settings.json` with `source_tree_sha256`, the measured `snapshot_id`, `datacite_release`, `flavor`, `timeout`, complete `resources`, and `benchmark_report_sha256`. Include pricing fetched at execution time and separate cost estimates for smoke, benchmark and full production. If not ready, adjust the explicit configuration, re-evaluate or benchmark the changed bottleneck, and retain the failed report.

- [ ] Add representative Criterion cases for no match, direct DOI, URL, shared URL, and arXiv overlap, with the index built outside the timed loop. Run benchmark unit tests and `cargo bench --bench extraction_bench --no-run`. Commit as `perf: measure full citation workload before production`.

## Task 14: Job submission and settings contract

**Files:** Modify `submit_job.py`, `jobs/pipeline.py`, `citations_pipeline/paths.py`, `citations_pipeline/pipeline.py`, `tests/test_submit_job.py`, `tests/test_job_entry.py`, `tests/test_scaffolding.py`.

**Interfaces:** Extend `SubmissionConfig` with `mode:str="datacite"`, `flavor:str | None=None`, `timeout:str | None=None`, `resources:ResourceSettings=field(default_factory=ResourceSettings)`, and `expected_snapshot_id:str | None=None`. Import `field` from `dataclasses`. Retain the existing `smoke`, `force`, and `datacite_release` fields; an existing `smoke=True` invocation selects smoke. Expose mutually exclusive `--smoke`, `--benchmark`, and `--legacy-arxiv`; default is full DataCite. Expose `--flavor`, `--timeout`, `--snapshot-id`, and `--settings <json-path>`. Settings JSON contains `source_tree_sha256`, `datacite_release`, `snapshot_id`, `flavor`, `timeout`, `resources`, and optional `benchmark_report_sha256`; reject unknown keys. Verify the source digest before submission and again at runtime. Explicit CLI values override corresponding settings values.

- [ ] Extend the existing submitter tests using their `FakeApi`, `make_services`, and `make_source_root` helpers:

```python
def test_full_submission_forwards_selected_resources(tmp_path, monkeypatch):
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=MIRROR_FILES)
    config = SubmissionConfig(source_root=make_source_root(tmp_path), smoke=False,
        force=False, datacite_release="2026-07", mode="datacite", flavor="cpu-xl",
        timeout="24h", resources=ResourceSettings(partitions=512),
        expected_snapshot_id="2026-07")
    submit(config, make_services(api))
    args = api.run_kwargs["script_args"]
    assert args[args.index("--mode") + 1] == "datacite"
    assert args[args.index("--partitions") + 1] == "512"
    assert api.run_kwargs["labels"]["pipeline"] == "crossref-datacite-citations"
    assert api.run_kwargs["flavor"] == "cpu-xl"
    assert api.run_kwargs["timeout"] == "24h"
```

Import `ResourceSettings`. Preserve the smoke no-secret/no-bucket test.

- [ ] Run `uv run pytest -q tests/test_submit_job.py tests/test_job_entry.py`. Expected: missing config fields/options or legacy label/flags.
- [ ] Implement mode-specific defaults: smoke `cpu-basic`/`30m`, full `cpu-xl`/`24h`, benchmark `cpu-xl`/`24h`. These are overridable, and benchmark cost is separate from full-run cost. Keep namespace `cometadata`. Pass a secret only to modes requiring bucket/dataset access. Continue to probe the mirror at submission and verify it again inside the job.
- [ ] Stage every new runtime module and the declared benches; keep the source volume read-only and name it with the source digest. Forward validated settings as individual arguments. Settings files are not runtime source inputs, preventing a self-referential source digest. The remote parser accepts `--mode`, `--snapshot-id`, all extraction resource flags, and `--duckdb-memory-mb`/`--index-sort-memory-mb` for Python phases.

```python
# Remote dispatch after source verification and environment bootstrap.
if args.mode == "legacy":
    result = run_pipeline(config_from_args(args))
elif args.mode == "datacite":
    result = run_full_pipeline(config_from_args(args))
elif args.mode == "smoke":
    result = run_datacite_smoke(config_from_args(args))
else:
    result = run_benchmark(config_from_args(args))
payload = result.to_dict() if hasattr(result, "to_dict") else result
print("RESULT_JSON=" + json.dumps(payload, sort_keys=True), flush=True)
```

`config_from_args(args: argparse.Namespace) -> PipelineConfig` is a local job-entry helper introduced in this task. It forwards every parsed setting and obtains credentials from the environment. All four dispatch functions exist before wiring this entry point.

- [ ] Ensure submission JSON reports job ID, URL, mode, flavor, timeout and source identity; only the remote final result can contain completed dataset commits. Test config validation, settings overrides, missing tokens only when required, force flags, benchmark labels, and source-digest changes when a new runtime file changes.
- [ ] Run submitter/job-entry/scaffolding/path tests and `uv run ruff check .`. Then run local smoke through the exact remote entry point:

```bash
uv run python - <<'PY'
from pathlib import Path
import subprocess
from citations_pipeline.paths import runtime_source_digest
root = Path.cwd()
subprocess.run(["uv", "run", "jobs/pipeline.py", "--mode", "smoke",
                "--source-root", str(root), "--source-digest", runtime_source_digest(root),
                "--work-root", "/tmp/datacite-citations-smoke-entry"], check=True)
PY
```

Require the exact smoke counts and `smoke-completed` result. Commit as `feat: configure full DataCite Hugging Face jobs`.

## Task 15: Documentation, complete checks, and remote validation

**Files:** Modify `README.md`, bump the crate to `2.2.0` in `Cargo.toml` and its own `Cargo.lock` package entry, and update the package descriptions in `Cargo.toml` and `pyproject.toml` to mention full DataCite mining. Keep dependency versions unchanged. Add `docs/superpowers/reports/2026-09-15-datacite-validation.md` during execution with real results.

**Interfaces:** No new runtime contract. Consume all earlier commands and result schemas.

- [ ] Document both CLI modes, bundle inputs including CSV, normalization rules, ambiguity exclusion, reference/citation count differences, both dataset schemas and output locations. Document local-only extraction resume, paired publication recovery, all resource flags, benchmark settings, and current job pricing. Explain that the DataCite release defines DOI/URL coverage.
- [ ] Update the existing manual job examples to `uv run submit_job.py --smoke`, `uv run submit_job.py --benchmark`, and `uv run submit_job.py --settings /tmp/datacite-production-settings.json`. Document `--legacy-arxiv` for the old behavior. Show the concrete bucket/report paths generated by the program. Ensure `--help` matches every documented flag.
- [ ] Run the complete required local verification once after final code changes:

```bash
uv run pytest -q
uv run ruff check .
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo build --release --locked
cargo +1.87.0 check --locked
```

Install the declared Rust toolchain if absent, or record an actual environment blocker; do not claim the MSRV check passed without running it. If a new dependency violates the existing floor, pin a compatible dependency version and rerun the affected checks. Repeat the local smoke entry command from Task 14 after the last runtime change.

- [ ] Commit final documentation/fixes only after their checks pass, and record the source digest. Submit a remote smoke with the finalized source:

```bash
uv run submit_job.py --smoke > /tmp/datacite-smoke-job.json
```

This is a billable submission, even on CPU Basic. Honor existing session authorization; if the specific spend still needs approval, present the tested source, configuration and current estimated cost before submission. Do not request the same approval again after it has been granted. If using the invoked HF Jobs tool, submit the same staged source/configuration through it; preserve the tested submitter as the reproducible repository entry point.

- [ ] Inspect the saved job ID and logs with the installed client:

```bash
uv run python - <<'PY'
import json
from pathlib import Path
from huggingface_hub import HfApi
api = HfApi()
job = json.loads(Path("/tmp/datacite-smoke-job.json").read_text())
info = api.inspect_job(job_id=job["id"], namespace="cometadata")
print(info.status)
for line in api.fetch_job_logs(job_id=job["id"], namespace="cometadata"):
    print(line)
PY
```

Require terminal `COMPLETED` and the exact `smoke-completed` result/counts. Check asynchronously at useful phase boundaries; do not busy-poll or block user communication for long waits. A submitted/running job is not a passed smoke.

- [ ] Submit the real benchmark using the same source and explicit resources, subject to the applicable existing cost authorization. Persist its actual job ID/URL and generated report URLs. Retrieve the reported `production-settings.json` with `hf buckets cp` to `/tmp/datacite-production-settings.json`; validate its source/release identities and headroom. If the runtime changes after this benchmark, repeat only validation affected by the change and regenerate source-bound settings.
- [ ] Record local checks, smoke counts, benchmark measurements, hardware/timeout and projected production cost in the validation report. Stage that ignored report explicitly and commit it as `docs: record DataCite pipeline validation`. This task is complete only when measured readiness is true; unresolved resource or correctness failures are recorded as incomplete work.

## Task 16: Full production run and independent publication verification

**Files:** Add `docs/superpowers/reports/2026-09-15-datacite-full-run.md` during execution, containing actual run results and links. No new implementation should be needed if Task 15 passed.

**Interfaces:** Consume validated `production-settings.json`, submission JSON, remote `RESULT_JSON`, both dataset manifests, and `_PIPELINE_COMPLETE.json`.

- [ ] Confirm the benchmark-selected snapshot/release still exist, validate available disk/resources, and verify current pricing. Present the exact source digest, selected hardware/timeout, dataset destinations, and measured cost forecast before a billable submission if those details are not already authorized. Preserve authorization already given in this session. Do not silently increase the approved compute budget.
- [ ] Submit the complete snapshot using the tested settings:

```bash
uv run submit_job.py --settings /tmp/datacite-production-settings.json > /tmp/datacite-production-job.json
```

No sample/limit flags may enter this invocation. Record the actual job ID, monitoring URL, source digest and input releases immediately. Use the same staged source/configuration if submitting through the HF Jobs tool.

- [ ] Inspect progress and final status asynchronously using the inspection/log code from Task 15 with `/tmp/datacite-production-job.json`. Preserve phase events, resource metrics and the final structured result. On failure, inspect the failed phase and use verified cached work for a justified retry within existing authorization. Do not automatically launch repeated full recomputations.
- [ ] After terminal completion, independently fetch the activation marker and both dataset manifests:

```bash
hf buckets cp hf://buckets/cometadata/shared-storage/crossref-datacite-citations-latest/_PIPELINE_COMPLETE.json /tmp/datacite-pipeline-complete.json
uv run python - <<'PY'
import hashlib
import json
from pathlib import Path
from huggingface_hub import HfApi, hf_hub_download
from citations_pipeline.full_state import FullPipelineMarker
from citations_pipeline.paired_publish import verify_manifest_files
marker = FullPipelineMarker.from_dict(json.loads(Path("/tmp/datacite-pipeline-complete.json").read_text()))
api = HfApi()
assert set(marker.publications) == {"datacite", "arxiv"}
for kind, receipt in marker.publications.items():
    api.dataset_info(receipt.repo_id, revision=receipt.commit_sha)
    current = api.dataset_info(receipt.repo_id).sha
    for revision in (receipt.commit_sha, current):
        manifest_path = hf_hub_download(receipt.repo_id, "metadata/pipeline-manifest.json",
                                       repo_type="dataset", revision=revision)
        manifest_bytes = Path(manifest_path).read_bytes()
        assert hashlib.sha256(manifest_bytes).hexdigest() == receipt.artifact_digest
        verify_manifest_files(api=api, token=None, repo_id=receipt.repo_id,
                              revision=revision, manifest=json.loads(manifest_bytes))
    assert api.dataset_info(receipt.repo_id).sha == current
    print(kind, receipt.commit_url)
PY
```

Use Task 10's actual-file verification logic to compare the current remote data and cards to those manifests, not merely the historical commit's existence. Verify the marker's archive, registry, bundle and source identity against the submitted job and benchmark settings. Confirm complete archive traversal, no fatal source errors, all reference-count invariants, and arXiv subset equivalence. Read the published Parquet schemas and representative rows, including ambiguity examples.

- [ ] Write the full-run report with actual status, input sizes/releases, index/extraction/transform/publish durations, counts for both datasets, ambiguity/failed-arXiv counts, measured resource high-water marks, cost information available from the job, both verified commit URLs, and the activation marker identity. Commit the report with `git add -f -- docs/superpowers/reports/2026-09-15-datacite-full-run.md` and a specific documentation commit.
- [ ] Deliver a concise summary linking the job, both datasets and the run report. Say the full run completed only when its terminal status, artifacts and paired publication checks all succeeded. If it is still running or genuinely blocked, report that exact state and the remaining work.

## Spec coverage and review checklist

| Spec requirement | Implementation tasks |
| --- | --- |
| Complete JSONL/CSV registry and verified inventory | 2, 3, 8 |
| Registered URL scope and conservative normalization | 1, 3, 4 |
| Cached memory-mapped membership and multi-DOI URL mapping | 3, 8 |
| arXiv patterns, raw evidence and legacy CLI compatibility | 1, 4, 7, 9, 12 |
| DOI-based shared-URL disambiguation and separate evidence | 4, 6, 9, 12 |
| Canonical deduplication, provenance and unique-citer counts | 4, 6, 9, 12 |
| Bounded batches/buffers and full-DOI partition distribution | 5, 6, 7, 13 |
| Corrupt-source failure and malformed-field accounting | 2, 7, 11, 12 |
| Local replay safety and incompatible-checkpoint rejection | 6, 7 |
| General dataset schema and exact arXiv projection | 9, 12 |
| Immutable derived artifacts and phase-level reuse | 8, 10, 11 |
| Two guarded commits, lost receipts and current-file checks | 10, 11, 16 |
| Shared-storage ownership and safe generation cleanup | 8, 11 |
| Runtime source packaging, tokens, resource settings | 14, 15 |
| Deterministic smoke and full-registry representative benchmark | 12, 13, 15 |
| Complete full run, verified outputs and honest status | 15, 16 |

- [x] At plan review, confirm that every interface consumed by a later task is defined here or already exists in the repository.
- [x] Confirm the typed contracts and field names in Rust output, Python transforms, manifests, receipts and remote results agree.
- [ ] Confirm every job mode dispatches to a completed, tested function before remote validation.
- [ ] During execution, check off a task only after its stated validation passes and its changes are committed.

## References for executors

- [DataCite monthly data file](https://support.datacite.org/docs/datacite-monthly-data-file): source formats, registered-state CSV coverage and checksums.
- [DataCite DOI response fields](https://support.datacite.org/docs/api-get-doi): registered landing-page `url` semantics.
- [fst Map](https://docs.rs/fst/0.4.7/fst/struct.Map.html): string-to-u64 mapping used for sidecar offsets.
- [URL parsing](https://docs.rs/url/2.5.4/url/struct.Url.html): parser operations; normalization requirements come from the approved spec.
- [Hugging Face Jobs guide](https://huggingface.co/docs/huggingface_hub/guides/jobs): source staging, explicit job settings, status and logs.
- [Hugging Face Jobs pricing](https://huggingface.co/docs/hub/jobs-pricing): recheck during execution rather than treating planning-time prices as permanent.

## Execution handoff

This plan can be executed with task-by-task subagents and review, or inline using the executing-plans skill. The execution choice is made after this plan is delivered. No implementation or billable job submission is performed by writing the plan.
