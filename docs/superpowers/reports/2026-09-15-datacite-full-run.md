# DataCite citation mining: full production run and publication verification (Task 16)

**Status: COMPLETED, all verification checks passed, with concerns.** Production job `6aaadf205527934177eeb496` finished `COMPLETED`. Both datasets were published, and every independent check below passed. Three things need attention:
- **URL false positives.** Some DataCite DOIs are registered with a degenerate landing URL (a DOI resolver root, or `http://www`). References to those URLs produce false-positive matches: about half of all URL matches, and 90.7% of ambiguity rows (section 9).
- **arXiv changes.** Compared with the previous revision, 4 references were lost and 1 was gained (section 8), and `raw_match` is written differently for 14,692 matches.
- **Carried-forward issues.** See section 11.

Plan: `docs/superpowers/plans/2026-09-15-datacite-citation-mining.md`. Validation report (Task 15): [`2026-09-15-datacite-validation.md`](2026-09-15-datacite-validation.md). Branch `mine-all-datacite-citations`. Written 2026-09-16.

## Sources and conventions

| Tag | Source |
| --- | --- |
| [log] | `.superpowers/sdd/2026-09-15-datacite-citation-mining/production-remote.log` (git-ignored): `PIPELINE_EVENT=` lines and Rust `event=` lines. It has one `RESULT_JSON` line, identical to `production-result.json` (checked). |
| [result] | `production-result.json` (the job's `RESULT_JSON`) |
| [submit] | `/tmp/datacite-production-job.json` (submission output) |
| [ledger] | `.superpowers/sdd/2026-09-15-datacite-citation-mining/progress.md` (git-ignored) |
| [hub] | Read-only Hugging Face Hub API calls made on 2026-09-16, before 20:21Z: `inspect_job`, `dataset_info`, `list_repo_tree`, `list_repo_commits`, and file downloads at pinned commits |
| [bucket] | Read-only copies from `hf://buckets/cometadata/shared-storage/crossref-datacite-citations-latest/` |
| [computed] | My own computation: Python/DuckDB 1.5.5 over the downloaded Parquet files at the pinned commits, or arithmetic on the figures above |
| [datacite-api] | Public `https://api.datacite.org/dois/<doi>` GET requests (current registration, 2026-09-16) |

The Parquet and card downloads were 2,181,054,130 B (2.03 GiB) in total and were deleted after verification. Units: GB = 10^9 bytes, GiB = 2^30 bytes.

## 1. Job, timeline and settings

| Field | Value |
| --- | --- |
| Job | `6aaadf205527934177eeb496`, <https://huggingface.co/jobs/cometadata/6aaadf205527934177eeb496> |
| Final stage | `COMPLETED` [hub] |
| Created / started / finished | 2026-09-16 18:25:36.511Z / 18:25:41.982Z / 20:05:33.552Z [hub] |
| Hub durations | scheduling 5 s, running 5,991 s, total 5,997 s [hub] |
| First / last pipeline event | 18:25:43.761Z `verify_entry_source` / 20:05:25.582Z `pipeline_activated` (span 5,981.8 s) [log, computed] |
| Mode, flavor, timeout | `datacite`, `cpu-xl` (16 vCPU, 124 GB RAM, 1,000 GB disk, $1.00/h), `24h` [submit; hardware from the validation report §6] |
| Image | `ghcr.io/astral-sh/uv:python3.12-bookworm` [hub] |
| Pins | `--snapshot-id 2026-07`, `--datacite-release 2026-07` [hub job command; submit `expected_snapshot_id`, `datacite_release`] |
| Resources (defaults) | partitions 256; writer 4,096 MiB; result batch 16 MiB; queue batches 8; aggregation 16,384 MiB; merge fan-in 32; sort workers 4; lookup 64 MiB; max entry 512 MiB; index sort 4,096 MiB; DuckDB 32,768 MiB [submit, hub job command] |
| `benchmark_report_sha256` | `null` [submit] |

**Why default settings.** Benchmark attempt 4 wrote no `production-settings.json` (validation report §4). On 2026-09-16 (~18:30Z) the user decided to run production on the default settings (`cpu-xl`/`24h`), pinned to snapshot `2026-07` and release `2026-07`, without rerunning the benchmark. The user also gave explicit approval to publish both datasets [ledger, "User decision"]. This departs from the spec's rule that benchmark-selected settings drive production. The user took that decision knowing the measured memory headroom and the unmeasured disk risk.

### Phase durations [log]

| Phase | Start (Z) | Duration (s) | Notes |
| --- | --- | --- | --- |
| `verify_entry_source` | 18:25:43.761 | 5.764 | |
| `bootstrap_packages` | 18:25:49.524 | 4.729 | |
| `bootstrap_rust` | 18:25:54.254 | 8.637 | |
| `verify_source` | 18:26:04.689 | 0.501 | |
| `build_extractor` | 18:26:05.191 | 435.245 | in-job release build |
| `resolve_snapshot` | 18:33:20.436 | 0.490 | `mirror`, `2026-07` |
| `select_release` | 18:33:20.926 | 0.950 | pinned `2026-07`, inventory `8698c3ff…e359` |
| `fetch_cached_bundle` | 18:33:21.941 | 16.370 | `bundle_cache_reused`, manifest `3d96f980…a0bc`, generation `generations/c30ac6c9c1d4448f895d26fa13792919` |
| `stage_snapshot` | 18:33:38.481 | 1,249.609 | 280,078,964,653 B, `verified: true`; 224.1 MB/s [computed] |
| `extraction` (`extract`) | 18:54:28.090 | 3,552.274 | Rust `index_load` 21.5 s; members phase `elapsed_s=3440.2`; aggregation 19:52:09 → 19:53:39 (≈ 90 s at 1 s log resolution, computed); 19 checkpoints (18 × 2,000 members + final 1,011) |
| `flatten_valid_output` | 19:53:48.845 | 124.059 | after `archive_released` at 19:53:48.844 |
| `flatten_ambiguous_urls` | 19:55:52.903 | 0.932 | |
| `scan_reference_keys` | 19:55:54.043 | 0.427 | |
| `widen_flat_references` | 19:55:54.470 | 15.709 | |
| `check_flat_references` | 19:56:10.179 | 0.504 | |
| `build_datacite_all` | 19:56:10.684 | 136.193 | 6 ranges, 3 files |
| `build_datacite_asserted` | 19:58:26.878 | 20.352 | 1 range, 1 file |
| `build_datacite_mined` | 19:58:47.230 | 117.347 | 5 ranges, 3 files |
| `build_arxiv_all` | 20:00:44.577 | 110.552 | 5 ranges, 1 file |
| `build_arxiv_asserted` | 20:02:35.130 | 7.897 | |
| `build_arxiv_mined` | 20:02:43.028 | 103.862 | 4 ranges, 1 file |
| `build_datacite_ambiguous_urls` | 20:04:26.890 | 1.117 | `duplicates_removed` 0 |
| `validate_arxiv_projection` | 20:04:28.007 | 4.479 | |
| `prepare_datasets` | 20:04:33.616 | 1.253 | |
| `compress_outputs` | 20:04:34.869 | 23.074 | |
| `hash_derived` | 20:04:57.944 | 1.999 | |
| `persist_derived` | 20:04:59.949 | 3.488 | 2,596,112,631 B uploaded |
| `verify_derived` | 20:05:03.437 | 17.205 | completed; the ledger records no retry needed |
| `persist_extraction_marker` | 20:05:20.642 | 0.169 | `extraction_persisted`, 22 files |
| `publish_datasets` | 20:05:21.150 | 4.167 | DataCite 1.640 s, arXiv 1.729 s |
| `activate` | 20:05:25.322 | 0.259 | `pipeline_activated` |

Phase groups [computed]:

| Group | Seconds |
| --- | --- |
| Setup: `verify_entry_source` start → `fetch_cached_bundle` end | 474.6 |
| Staging | 1,249.6 |
| Extraction, including aggregation | 3,552.3 |
| Transform: `flatten_valid_output` start → `transform_completed` | 644.8 |
| Persist, publish and activate: `prepare_datasets` start → `pipeline_activated` | 52.0 |

The log has no `Traceback`, panic or `phase_failed` line [log].

**Resource high-water marks.** These were not measured:
- Production mode emits no `phase_measured` events [log].
- `HfApi.fetch_job_metrics` returned `406 Not Acceptable` for the completed job [hub].

Planned budgets only:
- Rust `resource_estimate`: 14 workers, `total_bytes=15099494400` [log].
- Transform `budget_bytes` 17,179,869,184 per configuration [log].
- Aggregation used 256 in-memory partitions and 0 external ones, so no partition took the external spill path [log].

## 2. Input identities

| Identity | Value | Source |
| --- | --- | --- |
| Crossref snapshot | `2026-07` (`mirror`), archive 280,078,964,653 B, `archive_sha256` `ad83bc4b0b5a889933ad78b455a24f7ef8c793e1ad8bde595eacd3c6cd632669` | log `snapshot_staged`, marker |
| DataCite release | `2026-07`, `source_inventory_sha256` `8698c3ff681333a96291b05ea0478220e1a1198f133ab33da164f36b2e0de359` | log `release_selected`, marker |
| Index bundle | `bundle_manifest_sha256` `3d96f98000712d5505e03f670f664255789e0a47cac5fc52d6a99f5728cea0bc`, the bundle built by benchmark attempt 4 | log `bundle_cache_reused`; bucket `_BUNDLE_CURRENT.json` `generation_manifest_sha256`; the bucket's `_INDEX_COMPLETE.json` hashes to this value [computed] |
| Bundle counts | DOIs 140,790,372; URLs 132,273,455; shared URLs 512,262; hosts 13,378; invalid DOIs 26; invalid URLs 285; missing URLs 1,610 | bucket `_INDEX_COMPLETE.json` |
| Source | `source_tree_sha256` `4d6b70aae80690cb40b1b796801e8f8bbd4fcc959d9184fe84aa8dfd9f03b09b`, commit `1c54e57` | submit, result, marker; `runtime_source_digest` over `git archive 1c54e57` reproduces the digest [computed] |
| Versions | extraction mode `datacite`; index 2; normalization 1; extraction 2; partition algorithm `fnv1a64-v1`; partitions 256; DataCite publish 2; arXiv publish 1; marker schema 2; extractor 2.2.0 | marker, dataset manifests |
| Run identity | `identity_sha256` `6ef72a4272487c1b91c8481aec59b37bef7fc5b862c9f6194f3415d2a2a26d42` | `RunIdentity.fingerprint()` of the marker identity [computed] |
| Derived generation | `derived-generations/2026-07-346ddc7bd7ea4eae98a36df6807c71f8` | result, marker |

## 3. Extraction statistics

Sources:
- `extraction-report.json` in the derived generation [bucket]. Its SHA-256 `12477a4a…48db` and size 1,003 B match the extraction marker's file entry [computed].
- The same values appear in the log's `extraction_completed` event [log].

Report header: `completed: true`, `files_processed` 37,011, `fatal_source_errors` 0.

| Statistic | Value |
| --- | --- |
| Archive members processed | 37,011. Benchmark attempt 4 counted `total_members` 37,011; Rust `members_durable members=37011 skipped=0` |
| Items (works) | 185,051,032 |
| References scanned | 2,851,362,656 (828,836 refs/s, 10.76 members/s over 3,440.2 s [computed]) |
| Confirmed matches | 7,515,103 |
| ↳ DOI | 1,772,069 |
| ↳ URL | 168,468 |
| ↳ arXiv | 5,574,566 |
| Ambiguous URLs | 47,808 |
| Invalid arXiv | 36,134 |
| Unmatched DOIs | 2,071,274,324 |
| Unmatched URLs | 57,523,305 |
| Malformed references / fields | 0 / 0 |
| Filtered self-citations | 16 |
| Filtered provenance | 0 |

Aggregation:

| Count | Value |
| --- | --- |
| Partitions processed | 256 (256 in memory, 0 external, 0 reused) |
| Valid works | 1,715,770 |
| Failed (arXiv) works | 25,778 |
| Total citation rows | 7,551,221 |
| Publisher / Crossref / mined | 1,199,502 / 78,252 / 6,237,333 |

Consistency [computed]:
- 1,772,069 + 168,468 + 5,574,566 = 7,515,103 confirmed.
- 7,515,103 − 16 self-citations = 7,515,087. This is the DataCite `all` reference count.
- 7,515,087 + 36,134 invalid arXiv = 7,551,221 total rows.
- 1,199,502 + 78,252 = 1,277,754. This is the `asserted` reference count.
- In the published DataCite `all`, `match_type` counts are `doi` 1,772,053, `url` 168,468 and `arxiv` 5,574,566. All 16 filtered self-citations were DOI matches.

## 4. Published counts

The table shows works / citations / references. The Hub manifest, the `RESULT_JSON`, the extraction marker and my Parquet aggregates agree exactly [hub, result, bucket, computed].

| Dataset | `all` | `asserted` | `mined` |
| --- | --- | --- | --- |
| DataCite `cometadata/crossref-datacite-citations` | 1,715,770 / 7,426,049 / 7,515,087 | 582,302 / 1,269,805 / 1,277,754 | 1,325,905 / 6,157,532 / 6,237,333 |
| arXiv `cometadata/crossref-arxiv-citations` | 1,042,011 / 5,999,692 / 6,028,757 | 160,091 / 452,505 / 454,190 | 998,112 / 5,548,054 / 5,574,567 |

DataCite `ambiguous_urls` [computed]:
- 47,808 rows, with 47,808 distinct `(citing_doi, ref_index, url)`.
- 906 distinct URLs and 40,760 distinct citing works.
- Candidate DOIs per row: 2 → 45,097 rows; 3–10 → 2,074; 11–100 → 475; >100 → 162 (maximum 2,130).
- No row repeats a candidate.

The arXiv manifest has `ambiguity_rows` 0 [hub].

Files published [hub]:
- **DataCite** (9 managed files, 953,610,937 B): `data/all/part-0000{0,1,2}.parquet`, `data/asserted/part-00000.parquet`, `data/mined/part-0000{0,1,2}.parquet`, `data/ambiguous_urls/part-00000.parquet`, `README.md`, plus `metadata/pipeline-manifest.json`.
- **arXiv** (4 managed files, 668,357,977 B): `data/{all,asserted,mined}.parquet`, `README.md`, plus `metadata/pipeline-manifest.json`.

## 5. Verification results

| # | Check | Result |
| --- | --- | --- |
| 1 | Marker and receipts: `_PIPELINE_COMPLETE.json` parses as `FullPipelineMarker` with publications `{datacite, arxiv}`. For each receipt: the commit exists; `metadata/pipeline-manifest.json` at the receipt commit and at current HEAD hashes to `artifact_digest`; `verify_manifest_files` passes at both revisions (managed paths exactly the manifest's files; LFS size and SHA-256 per file); HEAD did not move during the check. Local SHA-256 and size of every downloaded managed file (data and `README.md`) also match the manifest [computed] | **PASS** |
| 2 | Identity cross-check (26 assertions): marker vs submission JSON, `RESULT_JSON`, extraction marker, `extraction-report.json`, log `run_identity`, Rust `event=identity` JSON and `pipeline_activated`. Covers source digest, snapshot `2026-07`, release `2026-07`, bundle `3d96f980…a0bc`, partitions 256, versions, derived prefix, receipts and counts. `RunIdentity.fingerprint()` = `6ef72a42…6d42` everywhere. Commit `1c54e57` reproduces the source digest | **PASS** |
| 3 | Complete traversal: `completed: true`, `files_processed` 37,011, `fatal_source_errors` 0, malformed 0. The root and generation `_EXTRACTION_COMPLETE.json` are byte-identical, and their dataset file entries equal both Hub manifests | **PASS** |
| 4a | Count invariants over every row of every config, computed with DuckDB whole-file aggregates. Per row, `citation_count == len(cited_by)` and `reference_count == Σ len(matches)` (0 violations in 6 configs). No duplicate work keys, duplicate citers or self-citations. Every citer's `provenance` equals the strongest of its matches. Totals equal `RESULT_JSON`. `all = asserted + mined` holds for references in both datasets (7,515,087 = 1,277,754 + 6,237,333; 6,028,757 = 454,190 + 5,574,567). Provenance separation holds: `asserted` holds only `publisher`/`crossref` matches, `mined` only `mined` | **PASS** |
| 4b | arXiv projection equals the DataCite arXiv subset. For each config, works with `doi` starting `10.48550/arxiv.` were joined to the projection on `'10.48550/arxiv.' ‖ arxiv_id`. Result: 0 missing, 0 extra, 0 count mismatches, and 0 nested mismatches of the full `cited_by` (hash of the DataCite `cited_by` with `match_type` removed vs the arXiv `cited_by`, order included). Totals: `all` 1,042,011 / 5,999,692 / 6,028,757; `asserted` 160,091 / 452,505 / 454,190; `mined` 998,112 / 5,548,054 / 5,574,567. `arxiv_doi = '10.48550/arXiv.' ‖ arxiv_id` for every row. Negative control: reversing `cited_by` changed the hash for all 45,148 multi-citer works in arXiv `asserted` | **PASS** |
| 4c | Schemas. All seven DataCite work files share one schema: `doi`, `citation_count`, `cited_by` list<struct<`doi`, `matches` list<struct<`provenance`, `match_type`, `raw_match`, `reference` struct(19 fields)>>, `provenance`>>, `reference_count`. `ambiguous_urls`: `citing_doi`, `ref_index`, `url`, `raw_match`, `candidate_dois` list<string>, `reference`. arXiv, for each of `all`/`asserted`/`mined`, compared with the previous revision `9056d87`: pyarrow `Schema.equals` is true, and a flattened comparison of names, order, types and nullability over 32 fields is identical. The only DataCite-vs-arXiv differences are `doi` → `arxiv_doi`/`arxiv_id` and the added `match_type` | **PASS** |
| 5 | Representative rows (section 6) | shown |
| 6 | arXiv regression comparison (section 8) | explained: −4 legacy cross-field matches, +1 URL match |

Current dataset HEADs, checked at 20:20Z [hub]:

| Dataset | Receipt commit | Parent | Current HEAD |
| --- | --- | --- | --- |
| `cometadata/crossref-datacite-citations` | [`343fe4e26abffd161cb426013ceb435400d9fa3c`](https://huggingface.co/datasets/cometadata/crossref-datacite-citations/commit/343fe4e26abffd161cb426013ceb435400d9fa3c) (20:05:22Z), artifact `bb023ebb…137e` | `e3e8aaa7` "initial commit" | `343fe4e26abffd161cb426013ceb435400d9fa3c` |
| `cometadata/crossref-arxiv-citations` | [`cde56bb0d239ae45f1fb7ef83feed254fb2df793`](https://huggingface.co/datasets/cometadata/crossref-arxiv-citations/commit/cde56bb0d239ae45f1fb7ef83feed254fb2df793) (20:05:24Z), artifact `994043a6…cce9` | `9056d87f` (2026-09-01 11:56Z) | `cde56bb0d239ae45f1fb7ef83feed254fb2df793` |

Physical Parquet layout changed but the schema did not: the new arXiv `all.parquet` was written by `parquet-cpp-arrow 21.0.0` (format 2.6, 1,032 row groups), while the previous one was written by DuckDB v1.5.5 (format 1.0, 4 row groups) [computed].

## 6. Representative rows [computed]

"Top" is rank 1 by `citation_count DESC, doi`. "Mid" is the row at rank ⌊n/2⌋ in the same order, chosen for reproducibility. Only the first `cited_by` entry and its first match are shown. Long strings are cut at 110 characters.

**DataCite, top (rank 1 of 1,715,770):**
```json
{"doi": "10.48550/arxiv.1412.6980", "citation_count": 44907, "reference_count": 45061,
 "cited_by[1]": {"doi": "10.1001/jamadermatol.2022.5415", "provenance": "mined", "matches": 1,
  "match[1]": {"provenance": "mined", "match_type": "arxiv", "raw_match": "arxiv.org/abs/1412.6980",
   "reference": {"key": "doi220062r38", "unstructured": "Kingma? DP, Ba? J. Adam: A Method for Stochastic Optimization.? ArXiv. Preprint posted online January 30, 2..."}}}}
```

**DataCite, mid (rank 857,885):**
```json
{"doi": "10.18721/jpm.153.353", "citation_count": 1, "reference_count": 1,
 "cited_by[1]": {"doi": "10.1134/s1063782624010068", "provenance": "publisher", "matches": 1,
  "match[1]": {"provenance": "publisher", "match_type": "doi", "raw_match": "10.18721/JPM.153.353",
   "reference": {"DOI": "10.18721/JPM.153.353", "doi-asserted-by": "publisher", "key": "3373_CR3", "unstructured": "I. A. Gulyaeva., I. O. Ignatieva, E. M. Bayan, V. V. Petrov. St. Petersb. State Polytech. Univ. J. Phys. Ma..."}}}}
```

**DataCite, top non-arXiv work (rank 2).** This row is a false positive (section 9):
```json
{"doi": "10.26091/9prt-kt79", "citation_count": 37719, "reference_count": 73877,
 "cited_by[1]": {"doi": "10.1002/2017jd027188", "provenance": "mined", "matches": 1,
  "match[1]": {"provenance": "mined", "match_type": "url", "raw_match": "https://doi.org/",
   "reference": {"key": "#cr-split#-e_1_2_9_29_1.2", "unstructured": "(C1) Atmospheric Radiation Measurement (ARM) Climate Research Facility Data Archive Oak Ridge Tennessee USA..."}}}}
```

**arXiv, top (rank 1 of 1,042,011):**
```json
{"arxiv_doi": "10.48550/arXiv.1412.6980", "arxiv_id": "1412.6980", "citation_count": 44907, "reference_count": 45061,
 "cited_by[1]": {"doi": "10.1001/jamadermatol.2022.5415", "provenance": "mined", "matches": 1,
  "match[1]": {"provenance": "mined", "raw_match": "arxiv.org/abs/1412.6980", "reference": {"key": "doi220062r38", "unstructured": "Kingma? DP, Ba? J. Adam: ..."}}}}
```

**arXiv, mid (rank 521,005):**
```json
{"arxiv_doi": "10.48550/arXiv.2405.01327", "arxiv_id": "2405.01327", "citation_count": 2, "reference_count": 2,
 "cited_by[1]": {"doi": "10.1007/s11370-025-00668-0", "provenance": "mined", "matches": 1,
  "match[1]": {"provenance": "mined", "raw_match": "arXiv:2405.01327",
   "reference": {"key": "668_CR21", "unstructured": "Sun Z, He S, Miao F, Zou S (2024) Constrained reinforcement learning under model mismatch. Preprint at arXi..."}}}}
```

**DataCite `ambiguous_urls`.** The three rows are the first, middle and last rows when sorted by candidate count, then `citing_doi`, `ref_index`, `url`:

| citing_doi / ref_index | url (raw_match) | candidate_dois | reference (trimmed) |
| --- | --- | --- | --- |
| `10.1001/archfaci.10.6.421` / 0 | `http://www/` (`http://www`) | 2: `10.25768/10.25768/fal.eikon-n05-a06`, `10.25768/10.25768/l-22-002` | key `p_1`; "Quotes and Poems.com Web site. http://www .quotesandpoem.com/quotes/showquotes/author /alan-k.simpson/12505..." |
| `10.1207/s15506878jobem5003_5` / 28 | `http://www/` (`http://www`) | 2: the same two DOIs | key `p_29_46`; "Canadian Radio-Television and Telecommunications Commission. (1995b). Decision CRTC 95-71: License renewal ..." |
| `10.1002/ece3.3173` / 30 | `http://www.ices.dk/sites/pub/` | 2,130: `10.17895/ices.pub.1905`, `…1906`, `…1907`, … | key `e_1_2_7_32_1`; "ICES. (2013a).WGBIFS BITS Manual 2013. Manual for the Baltic International Trawl Surveys March 2013 Tartu E..." |

URLs with the most ambiguity rows:

| URL | Rows | Candidates |
| --- | --- | --- |
| `http://www/` | 43,372 | 2 |
| `http://www.google.com/` | 897 | 8 |
| `https://waterdata.usgs.gov/nwis` | 309 | 3 |
| `https://landscan.ornl.gov/` | 194 | 84 |
| `https://www.worldvaluessurvey.org/WVSDocumentationWVL.jsp` | 115 | 3 |

## 7. Card and manifest details

- Both manifests record `manifest_version` 2, `extractor_version` `2.2.0` and snapshot `2026-07`, and embed the run identity [hub].
- `publication_schema_version` is 2 for DataCite and 1 for arXiv [hub].
- The `README.md` cards are listed in the manifests and verified by check 1.
- The cards still use the pre-fix wording (section 11) [computed from the downloaded cards].

## 8. arXiv regression check against the previous revision

**Revisions.** The previous revision is `9056d87fbf05b7c9642c1ec048df3f736346a25f`. It is the parent of `cde56bb0…` according to the log's `dataset_committed` event and `list_repo_commits` [log, hub]. It holds `data/{all,asserted,mined}.parquet` (279,303,002 / 18,977,441 / 260,799,790 B) and a legacy card, and has no pipeline manifest.

### Per-config counts [computed; the old counts also match the old card's table]

| Config | Previous `9056d87` (works / citations / refs) | New `cde56bb0` | Δ |
| --- | --- | --- | --- |
| `all` | 1,042,011 / 5,999,695 / 6,028,760 | 1,042,011 / 5,999,692 / 6,028,757 | 0 / −3 / −3 |
| `asserted` | 160,089 / 452,501 / 454,186 | 160,091 / 452,505 / 454,190 | +2 / +4 / +4 |
| `mined` | 998,112 / 5,548,061 / 5,574,574 | 998,112 / 5,548,054 / 5,574,567 | 0 / −7 / −7 |

The set of arXiv works in `all` is identical, with 0 differences in either direction. The old `asserted` has no work that the new one lacks. The new one adds `10.48550/arXiv.2011.03885` and `10.48550/arXiv.2305.13540`.

### Match-level diff of `all`

**Key choice.** The previous files carry no reference index, and `reference.key` is not unique within a citing work: the old `all` has 6,026,341 distinct `(arxiv_doi, citing doi, key)` for 6,028,760 matches. Two fields legitimately changed between versions for the same reference: `provenance` and `raw_match`. So I keyed each match on `(arxiv_doi, citing doi, hash(reference struct))`:
- The fingerprint covers all 19 reference fields, including `key`, `DOI` and `unstructured`.
- Both files have the same struct schema, so the fingerprint means the same thing on both sides.
- The diff uses multiset `EXCEPT ALL`, because identical references can repeat within a citing work. The old file has 6,026,357 distinct keys for 6,028,760 matches.

As a second pass, I added `provenance` and `raw_match` to the key.

| Diff | Previous-only | New-only |
| --- | --- | --- |
| Reference identity `(arxiv_doi, citing, reference fingerprint)` | **4** | **1** |
| Full tuple (+ `provenance`, `raw_match`) | 14,700 | 14,697 |

Pairing the full-tuple differences on reference identity gives four classes:

| Class | Count | Effect |
| --- | --- | --- |
| A. Legacy cross-field arXiv matches (lost) | 4 | `all` −4 refs and −4 citations; `mined` −4 |
| B. URL match to a registered arXiv landing page (gained) | 1 | `all` +1 ref and +1 citation; `mined` +1 |
| C. Reference DOI field ends in U+00A0 (NBSP): `mined` → `publisher` | 4 | `asserted` +4 refs and +2 works; `mined` −4 |
| D. `raw_match` rendering for old-style (category/number) arXiv DOIs asserted by publishers | 14,692 | none (provenance unchanged; 7,484 works, 8,122 citing works) |

These classes reproduce every count delta above: `all` −4 + 1 = −3, `asserted` +4, `mined` −4 + 1 − 4 = −7 [computed]. No difference involves a versioned arXiv DOI.

**A: previous-only.** Four matches, all shown. In each one, the legacy scanner joined the end of `article-title` ("… arXiv", "… arXiv:") with the arXiv ID at the start of `journal-title`. The new per-field scan never crosses field boundaries; the spec forbids it ("Never scan across a reference field boundary"). The final review predicted this as finding M2.

| arXiv DOI | Citing DOI | Old `raw_match` | Reference (non-null fields) |
| --- | --- | --- | --- |
| `10.48550/arXiv.1306.0096` | `10.1117/12.2261686` | `arXiv 1306.0096` | article-title "Generation and Confirmation of a (100x100)-dimensional Quantum System, quant-ph arXiv"; journal-title "1306.0096, to be published in the Journal of the National Academy of Sciences"; author Krenn; key c15; year 2014 |
| `10.48550/arXiv.1306.6709` | `10.3233/jifs-189987` | `arXiv: 1306.6709` | article-title "A survey on metric learning for feature vectors and structured data, arXiv preprint arXiv:"; journal-title "1306.6709"; unstructured "…arXiv preprint arXiv:, 1306.6709 (2013)."; key e_1_3_1_2_2 |
| `10.48550/arXiv.1406.5823` | `10.1371/journal.pone.0285361` | `arXiv 1406.5823` | article-title "Fitting linear mixed-effects models using lme4. arXiv preprint arXiv"; journal-title "1406.5823"; author D Bates; key pone.0285361.ref036 |
| `10.48550/arXiv.1908.07906` | `10.1016/j.compbiomed.2021.104663` | `arXiv 1908.07906` | article-title "Pcrnet: point cloud registration network using pointnet encoding, arXiv preprint arXiv"; journal-title "1908.07906"; author Sarode; key 10.1016/j.compbiomed.2021.104663_bib23 |

**B: new-only.** One match. The reference is `10.1038/s41567-018-0394-3` → `10.48550/arXiv.1508.04358`, `raw_match` `https://arXiv.org:/abs/1508.04358`, key `394_CR20`, unstructured "Ramelow, S. et al. Silicon-nitride platform for narrowband entangled photon generation. Preprint at https://arXiv.org:/abs/1508.04358 (2015).". In the DataCite `all` config this is `match_type: url`, the only URL-type match among all matches to arXiv DOIs. The arXiv regex requires `arxiv.org/(abs|pdf)/` (`src/extract/arxiv.rs:22` at `1c54e57`). The DataCite URL normalizer drops the empty port in `arXiv.org:/abs/…` and finds the registered landing URL.

**C: provenance moves (4, all shown).** In each case the reference's `DOI` field is `10.48550/arXiv.<id>` followed by U+00A0, with `doi-asserted-by: publisher`. The previous version recorded a `mined` match with `raw_match` `10.48550/arXiv.<id>`. The new version recognizes the structured DOI as publisher-asserted, and `raw_match` keeps the trailing U+00A0.

| arXiv DOI | Citing DOI | Reference key |
| --- | --- | --- |
| `10.48550/arXiv.1804.02767` | `10.1088/1361-6560/ad3c0c` | `pmbad3c0cbib32` |
| `10.48550/arXiv.2011.03885` | `10.1145/3715275.3732072` | `e_1_3_3_2_8_2` |
| `10.48550/arXiv.2305.13540` | `10.1007/s40471-023-00341-z` | `341_CR49` |
| `10.48550/arXiv.2405.17044` | `10.1145/3715275.3732072` | `e_1_3_3_2_24_2` |

**D: `raw_match` rendering (14,692).** The previous revision stored publisher-asserted old-style IDs without the DOI prefix; the new one stores the full DOI substring:

| Form | Previous | New |
| --- | --- | --- |
| Old-style IDs, e.g. `astro-ph` | `arXiv.astro-ph/0001012` | `10.48550/arXiv.astro-ph/0001012` |
| New-style IDs (439,494 old / 439,498 new publisher matches) | `10.48550/arXiv.*` | `10.48550/arXiv.*` (unchanged) |

After the change, every publisher `raw_match` in the new file starts with `10.48550/arXiv.`, case-insensitively. Most affected: `astro-ph` 6,453, `hep-th` 1,983, `quant-ph` 1,426, `cond-mat` 1,401, `hep-ph` 1,047.

Example, `10.48550/arXiv.adap-org/9303003` cited by `10.1007/s40747-023-01330-x`:
- **Old `raw_match`:** `ARXIV.ADAP-ORG/9303003`.
- **New `raw_match`:** `10.48550/ARXIV.ADAP-ORG/9303003`.
- **Reference:** `DOI` "10.48550/ARXIV.ADAP-ORG/9303003", key `1330_CR51`.

Only the evidence string changes; counts and schema do not. Downstream users who parse `raw_match` will see the new form.

## 9. Finding: URL matches from degenerate registered landing URLs [computed, datacite-api]

The URL matcher does what the spec says: it checks complete normalized URL keys. Some DataCite DOIs, however, are registered with a landing URL that is a DOI resolver root or a bare host. Every reference containing that string then matches, and most such references are Crossref citations of unrelated DOIs.

| DataCite DOI | Registered URL [datacite-api] | Title / publisher [datacite-api] | Published citations / references (all URL matches) | Rank in `all` |
| --- | --- | --- | --- | --- |
| `10.26091/9prt-kt79` | `https://doi.org/` | "National survey of pesticides and emerging organic contaminants (EOCs) in groundwater 2018" / Institute of Environmental Science and Research Ltd | 37,719 / 73,877 | 2 |
| `10.4233/uuid:aca2b8dd-a24b-48f3-82ed-8a4d535d72d5` | `http://dx.doi.org/` | "vervallen" / Delft University of Technology | 6,227 / 11,720 | 20 |
| `10.25768/10.25768/fal.eikon-n05-a06`, `10.25768/10.25768/l-22-002` | `http://www` | Universidade da Beira Interior | 43,372 `ambiguous_urls` rows (36,597 citing works) | — |

Typical references behind these matches:
- `10.26091/9prt-kt79`, `raw_match` `https://doi.org/`: "… https://doi.org/ 10.1016/j.msec.2015.01.054."
- `10.26091/9prt-kt79`, `raw_match` `doi.org/`: "… doi.org/ 10.1016/j.aca.2013.04.026 ."

Here a space splits a Crossref DOI from its resolver prefix.

Impact:
- **URL matches.** The two resolver-root DOIs hold 85,597 of the 168,468 URL-type references (50.8%) and 43,946 citations. All are in `mined`, and they make up 1.37% of `mined` references.
- **Ambiguity rows.** `http://www/` accounts for 43,372 of the 47,808 rows (90.7%).
- **Host-only URL matches.** Across the dataset, 86,164 URL matches (202 DOIs) have a `raw_match` of the form `scheme://host` or `scheme://host/`. Some of these may be legitimate host-level landing pages, such as `http://wsc.nmbe.ch`.
- **arXiv dataset.** Not affected: its only URL match is the one in section 8 B.

Recommendation for the next run: exclude registered URLs that are DOI resolver roots (`doi.org`, `dx.doi.org`) or that have no registrable host (`http://www`) when building the index, or when matching. This needs a spec ruling; it is not a verification failure.

## 10. Costs

| Item | Basis | Cost |
| --- | --- | --- |
| Production job `6aaadf20…b496` | 5,991 running s [hub] → 100 billed minutes × $1.00/h (computed) | **$1.67** (per-second prorated $1.66) |
| Validation spend: 3 smokes, 4 benchmark attempts | ledger ≈ $5.31; validation report recomputation $5.29 (prorated) – $5.34 (whole minutes) | ≈ $5.31 |
| **Cumulative (validation + production)** | computed | **≈ $6.98** (range $6.95–$7.01) |

This excludes the 2026-09-01 arXiv-only runs. The whole spend is well inside the approved envelope of 24 h × $1/h per `cpu-xl` job [ledger].

## 11. Known issues carried forward

1. **Published cards still have the pre-fix wording** (final review I3, fixed on the branch after this run, never republished) [computed from the downloaded cards]:
   - The arXiv card has `size_categories: 100K<n<1M`, but `all` has 1,042,011 works.
   - Both cards describe `mined` as "extracted from unstructured text". Yet 354,132 of the 6,237,333 DataCite `mined` references have no `unstructured` field: 350,359 arXiv, 3,523 DOI and 250 URL matches. An example is `journal-title` "arXiv preprint arXiv:1412.6980" [computed].
   - The arXiv card describes `raw_match` as "the exact substring that matched an arXiv identifier pattern". Publisher matches now hold the DOI field substring (section 8 D).

   The cards change only when a later run republishes.
2. **Embedded-DOI token regression.** It exists only at branch HEAD: it was introduced by the I1 fix in the final fix wave (`d8936fc..7f8e1a4`). This run used source `4d6b70aa…b09b` (`1c54e57`) and was not affected. HEAD must not be used for a benchmark or production run until the reviewer's bounded fix lands [ledger].
3. **Benchmark measurement fixes landed after this run** (C1 a–d and the legacy-index reuse, commits `d8936fc..7f8e1a4`). This run's settings are therefore defaults, not benchmark-validated. No benchmark has yet produced a `production-settings.json` [ledger, validation report].
4. **Degenerate registered URLs** inflate URL matches and ambiguity rows (section 9). New finding; needs a ruling.
5. **Legacy cross-field arXiv matches are gone** (4 references; section 8 A). This follows the spec, as final review M2 predicted.
6. **`raw_match` form changed** for 14,692 publisher-asserted old-style arXiv matches (section 8 D).
7. **No resource high-water marks for production.** Production mode emits no `phase_measured` events, and the Hub metrics endpoint returns 406 once a job has completed. Disk use at full scale is still unmeasured, although the run finished on the 1,000 GB flavor.
8. **Open items from the validation report** still stand. They are listed in [its section 8](2026-09-15-datacite-validation.md#8-open-items) and include 26 skipped invalid DataCite DOI occurrences and the stale CLI help text.
