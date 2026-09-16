# DataCite citation mining: full production run and publication verification (Task 16)

**Current published state (updated 2026-09-16): the rerun, job `6aab03e4f76d6a098a711af9`, source `33ccb779…b4b8` (`bba775c`).** Its revisions are DataCite `71c8cd7f…6d82` and arXiv `63216439…a95`. Checks 1–9 passed, with concerns; see [Rerun after the degenerate landing-page fix](#rerun-after-the-degenerate-landing-page-fix-2026-09-16). Sections 1–11 below describe run 1, which is no longer the published state.

**Run 1 status: COMPLETED, all verification checks passed, with concerns.** Production job `6aaadf205527934177eeb496` finished `COMPLETED`. Both datasets were published, and every independent check below passed. Three things need attention:
- **URL false positives** (fixed by the rerun). Some DataCite DOIs are registered with a degenerate landing URL (a DOI resolver root, or `http://www`). References to those URLs produce false-positive matches: about half of all URL matches, and 90.7% of ambiguity rows (section 9).
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

## Rerun after the degenerate landing-page fix (2026-09-16)

**Status: COMPLETED; checks 1–9 passed, with concerns.** Job `6aab03e4f76d6a098a711af9` republished both datasets from source `33ccb779…b4b8` (commit `bba775c`). These are the current revisions: DataCite `71c8cd7f60a089c11dd7d026c78d17561ad6ed82` and arXiv `63216439e0d51e428b589c79bc05840f5274fa95`. Three concerns:
- **The bare-site-root loss is far larger than estimated.** The post-production fix report and the ledger put it at "~567" non-resolver host-only matches. Measured: 9,523 URL references on 237 DOIs and 235 hosts, 8,955 citations in `all`, and 134 works gone from `all` (R7). Some are plausible host-level landing pages, such as `wsc.nmbe.ch`, `cccbdb.nist.gov` and `earthexplorer.usgs.gov`. The user accepted this trade-off in principle, but the size is new information.
- **Unexplained job tail.** The job stayed `RUNNING` for 506 s after its last log line (run 1: 8 s). That tail was billed, ≈ $0.14 (R1).
- **`ambiguous_urls.reference` narrowed from 19 to 14 struct fields.** Reference keys are derived per configuration from the rows present, and the 5 dropped keys were null in every remaining row (R5).

### Why the rerun happened

[Section 9](#9-finding-url-matches-from-degenerate-registered-landing-urls-computed-datacite-api) found DataCite DOIs registered with degenerate landing URLs:
- `10.26091/9prt-kt79` → `https://doi.org/`
- `10.4233/uuid:aca2b8dd-…` → `http://dx.doi.org/`
- two `10.25768` DOIs → `http://www`

Together they absorbed 85,597 URL matches and 43,372 ambiguity rows.

The user decided on option 1 (≈20:35Z), which changed the spec [ledger, "User decision"]. At match time, the matcher now ignores registered landing-page keys that:
- are on DOI resolver hosts (`doi.org`, `dx.doi.org`, `www.doi.org`),
- are bare site roots (path `/`, no query, no fragment), or
- have dotless hosts.

It counts them as `degenerate_urls` (`2adb479`; spec `867e780`). The same decision also:
- applied the bounded fix for the embedded-DOI token regression (`bba775c`; section 11 item 2);
- republished the corrected cards (`d86d405`; section 11 item 1).

Before the rerun [ledger, hub]:
- The fix review approved `b6cb8b0..bba775c` with 0 Critical and 0 Important findings.
- Remote smoke 4 (`6aab0118f76d6a098a711ad2`, `cpu-basic`) finished `COMPLETED` with exact counts.

### Sources for this section

| Tag | Source |
| --- | --- |
| [log2] | `.superpowers/sdd/2026-09-15-datacite-citation-mining/production2-remote.log` (git-ignored). It has one `RESULT_JSON` line, identical to `production2-result.json` (checked) |
| [result2] | `production2-result.json` |
| [submit2] | `/tmp/datacite-production-job-2.json` |
| [ledger] | `progress.md`, as above |
| [hub] | Read-only Hub calls made on 2026-09-16 between 23:27Z and 23:47Z: `inspect_job`, `list_jobs_hardware`, `dataset_info`, `list_repo_commits`, `verify_manifest_files` (tree and LFS metadata), and file downloads at pinned commits |
| [bucket] | Read-only listings and copies from `hf://buckets/cometadata/shared-storage/` |
| [git] | `git diff 1c54e57..bba775c -- src` |
| [computed] | Python, DuckDB 1.5.5 and pyarrow 21.0.0 over files downloaded at pinned commits. Run-1 figures come from sections 1–9 |

Downloads were about 3.10 GB (2.89 GiB) and were deleted after verification. These were the new revisions' 13 managed files; run 1's DataCite `all`, `mined` and `ambiguous_urls` files; run 1's arXiv `all` and `mined` files; and both run-1 cards. A scratch DuckDB database briefly reached 4.92 GB and was also deleted.

### R1. Job, timeline and settings

| Field | Rerun | Run 1 |
| --- | --- | --- |
| Job | [`6aab03e4f76d6a098a711af9`](https://huggingface.co/jobs/cometadata/6aab03e4f76d6a098a711af9) | `6aaadf205527934177eeb496` |
| Final stage | `COMPLETED` [hub] | `COMPLETED` |
| Created / started / finished | 21:02:28.370Z / 21:02:34.675Z / 23:32:35.179Z [hub] | 18:25:36.511Z / 18:25:41.982Z / 20:05:33.552Z |
| Hub durations | scheduling 6 s, running 9,000 s, total 9,006 s [hub] | 5 / 5,991 / 5,997 s |
| First event → `pipeline_activated` | 21:02:35.649Z → 23:24:09.067Z, 8,493.4 s [log2, computed] | 5,981.8 s |
| Last log line → Hub finish | `cleanup_removed` at 23:24:09.160Z, then `RESULT_JSON`; the job finished 506.0 s later. Nothing in the log explains the gap [log2, hub, computed] | ≈ 8 s |
| Mode, flavor, timeout | `datacite`, `cpu-xl`, `24h` [submit2, hub] | same |
| Pins | `--snapshot-id 2026-07`, `--datacite-release 2026-07` [hub job command, submit2] | same |
| Resources | Defaults, identical to run 1 (partitions 256 … DuckDB 32,768 MiB) [submit2, hub job command] | same |
| `benchmark_report_sha256` | `null` [submit2] | `null` |
| Labels | `pipeline=crossref-datacite-citations`, `mode=datacite`, `source=33ccb779afcb` [hub] | `source=4d6b70aae806` |
| Reuse | Not reused. `extraction_marker_stale` (marker `6ef72a42…` ≠ `f507938d…`); `find_uploaded_generation` found nothing in 0.034 s; `extraction_reused: false` [log2, result2] | — |

The log has no `Traceback`, panic or `phase_failed` line [log2].

**Phase durations** [log2; run 1 from section 1]:

| Phase | Start (Z) | Rerun (s) | Run 1 (s) | Notes |
| --- | --- | --- | --- | --- |
| `verify_entry_source` | 21:02:35.649 | 5.609 | 5.764 | |
| `bootstrap_packages` | 21:02:41.258 | 8.163 | 4.729 | |
| `bootstrap_rust` | 21:02:49.421 | 10.167 | 8.637 | |
| `verify_source` | 21:03:01.271 | 0.907 | 0.501 | |
| `build_extractor` | 21:03:02.179 | 458.315 | 435.245 | |
| `resolve_snapshot` | 21:10:40.495 | 0.439 | 0.490 | `mirror`, `2026-07` |
| `select_release` | 21:10:40.934 | 0.829 | 0.950 | pinned `2026-07` |
| `fetch_cached_bundle` | 21:10:41.835 | 16.211 | 16.370 | `bundle_cache_reused` |
| `find_uploaded_generation` | 21:10:58.572 | 0.034 | — | new phase (`bab6ca4`), none found |
| `stage_snapshot` | 21:10:58.628 | 1,527.206 | 1,249.609 | 280,078,964,653 B, `verified: true`; 183.4 MB/s vs 224.1 MB/s [computed] |
| `extraction` | 21:36:25.835 | 5,759.466 | 3,552.274 | Rust: `index_load` 45.1 s (run 1: 21.5 s); members 5,504.4 s (3,440.2 s); aggregation 208.5 s (≈ 90 s); 19 checkpoints (18 × 2,000 + 1,011) |
| `flatten_valid_output` | 23:12:31.422 | 119.347 | 124.059 | |
| `flatten_ambiguous_urls` | 23:14:30.769 | 0.086 | 0.932 | |
| `scan_reference_keys` | 23:14:31.052 | 0.440 | 0.427 | |
| `widen_flat_references` | 23:14:31.492 | 16.836 | 15.709 | |
| `check_flat_references` | 23:14:48.329 | 0.563 | 0.504 | |
| `build_datacite_all` | 23:14:48.893 | 146.050 | 136.193 | 39 buckets (run 1: 40), 5 ranges (6), 3 files |
| `build_datacite_asserted` | 23:17:14.944 | 20.738 | 20.352 | 1 range |
| `build_datacite_mined` | 23:17:35.683 | 118.150 | 117.347 | 5 ranges, 3 files |
| `build_arxiv_all` | 23:19:33.833 | 110.670 | 110.552 | 5 ranges |
| `build_arxiv_asserted` | 23:21:24.504 | 6.657 | 7.897 | 1 range |
| `build_arxiv_mined` | 23:21:31.162 | 105.093 | 103.862 | 4 ranges |
| `build_datacite_ambiguous_urls` | 23:23:16.255 | 0.380 | 1.117 | `duplicates_removed` 0 |
| `validate_arxiv_projection` | 23:23:16.635 | 4.129 | 4.479 | |
| `prepare_datasets` | 23:23:21.749 | 1.151 | 1.253 | |
| `compress_outputs` | 23:23:22.901 | 19.785 | 23.074 | |
| `hash_derived` | 23:23:42.686 | 1.752 | 1.999 | |
| `persist_derived` | 23:23:44.477 | 3.224 | 3.488 | 2,529,215,673 B uploaded (run 1: 2,596,112,631) |
| `verify_derived` | 23:23:47.701 | 15.235 | 17.205 | |
| `persist_extraction_marker` | 23:24:02.936 | 0.193 | 0.169 | `extraction_persisted`, 22 files |
| `publish_datasets` | 23:24:03.570 | 5.222 | 4.167 | DataCite 1.687 s, arXiv 1.615 s |
| `activate` | 23:24:08.842 | 0.224 | 0.259 | then `cleanup_removed` of the run-1 generation |

Phase groups, rerun vs run 1 [computed]:
- Setup: 503.0 s vs 474.6 s. It runs from the `verify_entry_source` start to the end of `find_uploaded_generation`.
- Staging: 1,527.2 s vs 1,249.6 s.
- Extraction: 5,759.5 s vs 3,552.3 s.
- Transform: 650.3 s vs 644.8 s, from the `flatten_valid_output` start to `transform_completed`.
- Persist, publish and activate: 47.3 s vs 52.0 s, from the `prepare_datasets` start to `pipeline_activated`.

**Why extraction took 5,759 s instead of 3,552 s.** The log shows the slowdown but cannot isolate its cause.
- **Member scan.** It took 5,504.4 s vs 3,440.2 s (1.60×): 518,015 refs/s and 6.72 members/s, vs 828,836 and 10.76. All 19 checkpoint windows were slower, by 1.29× to 1.89× [log2, log, computed].
- **Unchanged code was also slower.** Between `1c54e57` and `bba775c` the Rust changes touch only the matcher, the token scanner, the malformed-reference path, and counters and timers [git]. Even so:
  - index load took 45.1 s vs 21.5 s (2.1×);
  - aggregation took 208.5 s vs ≈ 90 s (2.3×), on 1.3% fewer rows;
  - staging throughput fell 18%;
  - `bootstrap_packages` took 1.7× as long [log2, log, computed].
- **DuckDB transforms were not slower** (650.3 s vs 644.8 s).
- **Likely cause.** Part of the slowdown is host variance on this job's node, as the unchanged phases show. The new matcher work probably adds to it: every URL lookup key is now parsed for the degenerate check (5,551,938 degenerate candidates were counted), and cut DOI tokens get whole-token probes. The ledger's mid-run guess named only the matcher work, and no measurement separates the two.

### R2. Identities

| Identity | Rerun | Changed from run 1? | Source |
| --- | --- | --- | --- |
| Crossref snapshot | `2026-07` (`mirror`), 280,078,964,653 B, `archive_sha256` `ad83bc4b…2669` | no | log2 `snapshot_staged`, marker |
| DataCite release | `2026-07`, inventory `8698c3ff…e359` | no | log2 `release_selected`, marker |
| Index bundle | `3d96f98000712d5505e03f670f664255789e0a47cac5fc52d6a99f5728cea0bc`, `generations/c30ac6c9c1d4448f895d26fa13792919` | no | log2 `bundle_cache_reused`; bucket `_BUNDLE_CURRENT.json`; `_INDEX_COMPLETE.json` hashes to it [computed] |
| Source | `33ccb779afcb1281716b2bbd0b479884ca7dda2b364f1c681054de642327b4b8`, commit `bba775c` | yes (was `4d6b70aa…`, `1c54e57`) | submit2, result2, marker; `runtime_source_digest` over `git archive bba775c` reproduces it [computed] |
| Versions | Mode `datacite`; index 2; normalization 1; extraction 2; `fnv1a64-v1`; partitions 256; DataCite publish 2; arXiv publish 1; marker schema 2; manifest 2; extractor 2.2.0 | no | marker, manifests |
| Run identity | `f507938de2061f378eede88fccffc72bb5708f350e9677bb8e6edfd40e75f9d5` | yes, because the source changed | `RunIdentity.fingerprint()` [computed] |
| Derived generation | `derived-generations/2026-07-4aad4a3a40c94e56b44419daf1a78357` | yes | result2, marker |
| Receipts | DataCite `71c8cd7f…6d82`, artifact `f7eacd92e4bde1653f19d01da83fe19beb0ca45a4399f7296eaee1ad1655f93f`; arXiv `63216439…a95`, artifact `d3513d53388eb5fe3ad132a84431a0a1967bfe2db9095e10a3009b27baaf3520` | yes | result2, marker, hub |

### R3. Extraction statistics

Sources:
- `extraction-report.json` in the new generation [bucket]. Its SHA-256 `4df16bf8…8547` and size 1,033 B match the marker entry [computed].
- The log's `extraction_completed` event and the Rust `extraction status=completed` line give the same values [log2].

| Statistic | Run 1 | Rerun | Δ |
| --- | --- | --- | --- |
| `completed` / `files_processed` / `fatal_source_errors` | true / 37,011 / 0 | true / 37,011 / 0 | — |
| Members durable / skipped | 37,011 / 0 | 37,011 / 0 | 0 |
| Items | 185,051,032 | 185,051,032 | 0 |
| References scanned | 2,851,362,656 | 2,851,362,656 | 0 |
| Confirmed matches | 7,515,103 | 7,420,043 | −95,060 |
| ↳ DOI | 1,772,069 | 1,772,128 | +59 |
| ↳ URL | 168,468 | 73,349 | −95,119 |
| ↳ arXiv | 5,574,566 | 5,574,566 | 0 |
| Ambiguous URLs | 47,808 | 2,631 | −45,177 |
| Invalid arXiv | 36,134 | 36,134 | 0 |
| Unmatched DOIs | 2,071,274,324 | 2,071,190,015 | −84,309 |
| Unmatched URLs | 57,523,305 | 52,112,212 | −5,411,093 |
| `degenerate_urls` | (not counted) | 5,551,938 | new |
| Malformed references / fields | 0 / 0 | 0 / 0 | 0 |
| Filtered self-citations / provenance | 16 / 0 | 16 / 0 | 0 |
| Aggregation valid / failed works | 1,715,770 / 25,778 | 1,715,666 / 25,778 | −104 / 0 |
| Total citation rows | 7,551,221 | 7,456,161 | −95,060 |
| Publisher / Crossref / mined | 1,199,502 / 78,252 / 6,237,333 | 1,199,502 / 78,252 / 6,142,273 | 0 / 0 / −95,060 |
| Partitions (in memory / external / reused) | 256 / 0 / 0 | 256 / 0 / 0 | 0 |

Consistency [computed]:
- 1,772,128 + 73,349 + 5,574,566 = 7,420,043.
- Subtracting the 16 self-citations gives 7,420,027, the `all` reference count.
- Adding the 36,134 invalid arXiv rows gives 7,456,161 rows.
- `all` match types: `doi` 1,772,112 (publisher 1,199,502, crossref 78,252, mined 494,358), `url` 73,349, `arxiv` 5,574,566.

Unit note: `degenerate_urls` counts distinct URL candidates per reference and is never also counted as unmatched (`src/extract/datacite.rs` at `bba775c`). The unmatched-URL drop plus the removed URL matches and ambiguity rows (5,411,093 + 95,119 + 45,177 = 5,551,389) is 549 short of the degenerate count. The units differ, so exact equality is not expected. The unmatched-DOI change comes from the new token segmentation (R7) [computed].

### R4. Published counts

Works / citations / references. `RESULT_JSON`, the extraction marker, both Hub manifests and my Parquet aggregates agree exactly [result2, bucket, hub, computed].

| Config | Run 1 | Rerun | Δ |
| --- | --- | --- | --- |
| DataCite `all` | 1,715,770 / 7,426,049 / 7,515,087 | 1,715,666 / 7,373,206 / 7,420,027 | −104 / −52,843 / −95,060 |
| DataCite `asserted` | 582,302 / 1,269,805 / 1,277,754 | 582,302 / 1,269,805 / 1,277,754 | 0 / 0 / 0 (Parquet byte-identical) |
| DataCite `mined` | 1,325,905 / 6,157,532 / 6,237,333 | 1,325,785 / 6,104,684 / 6,142,273 | −120 / −52,848 / −95,060 |
| DataCite `ambiguous_urls` rows | 47,808 | 2,631 | −45,177 |
| arXiv `all` | 1,042,011 / 5,999,692 / 6,028,757 | 1,042,011 / 5,999,692 / 6,028,757 | 0 |
| arXiv `asserted` | 160,091 / 452,505 / 454,190 | 160,091 / 452,505 / 454,190 | 0 |
| arXiv `mined` | 998,112 / 5,548,054 / 5,574,567 | 998,112 / 5,548,054 / 5,574,567 | 0 |

Files published [hub]:
- **DataCite:** the same 9 managed paths, 917,041,345 B (run 1: 953,610,937), plus `metadata/pipeline-manifest.json`.
- **arXiv:** 4 managed files, 668,752,142 B (run 1: 668,357,977).

DataCite `ambiguous_urls` [computed]:
- 2,631 rows, with 2,631 distinct `(citing_doi, ref_index, url)`.
- 864 distinct URLs and 2,476 citing works.
- Candidates per row: 2 → 1,392; 3–10 → 1,041; 11–100 → 172; >100 → 26 (maximum 2,130).
- No row repeats a candidate, and none has fewer than 2.

### R5. Verification results

| # | Check | Result |
| --- | --- | --- |
| 1 | **Job state.** `inspect_job` reports `COMPLETED`, running 9,000 s [hub]. The job was still `RUNNING` at 23:27Z–23:32Z and finished at 23:32:35Z (R1) | **PASS** |
| 2 | **Marker and receipts.** The root `_PIPELINE_COMPLETE.json` (written 23:24:08Z) parses as `FullPipelineMarker`. Its identity is `f507938d…`, its prefix `4aad4a3a…`, and its publications `{datacite: 71c8cd7f…, arxiv: 63216439…}`, equal to `RESULT_JSON` and to the generation's `_PUBLICATIONS.json`. For each receipt: `metadata/pipeline-manifest.json` at the receipt commit and at HEAD hashes to the artifact digest, and `verify_manifest_files` passes at both revisions. HEAD equalled the receipt commit before and after the check (23:28:36Z and 23:28:39Z) and again at 23:46:19Z. The parent of `71c8cd7f` is `343fe4e2`, and the parent of `63216439` is `cde56bb0`; `list_repo_commits` and the log's `dataset_committed` events agree. Each new commit is HEAD with no commit in between, and `publication_superseding` saw the run-1 artifacts `bb023ebb…`/`994043a6…` at HEAD. All 13 downloaded managed files match their manifest SHA-256 and size [hub, bucket, log2, computed] | **PASS** |
| 3 | **Identity cross-check (43 assertions).** Sources: marker, `_EXTRACTION_COMPLETE.json`, `extraction-report.json`, submission JSON, `RESULT_JSON`, log `run_identity`, the Rust `event=identity` JSON, `pipeline_activated` and both Hub manifests. Covered: source digest `33ccb779…`, snapshot `2026-07` (`mirror`, archive SHA), release `2026-07` (inventory SHA), bundle `3d96f980…a0bc`, partitions 256, all versions, derived prefix, receipts, counts, and the manifest file entries against the marker. `fingerprint()` = `f507938d…f9d5` everywhere. `git archive bba775c` reproduces the source digest [computed] | **PASS** |
| 4 | **Traversal.** `completed: true`, 37,011 files, 0 fatal, malformed 0 / 0, `members_durable members=37011 skipped=0`. The root and generation `_EXTRACTION_COMPLETE.json` are byte-identical. Stats and the `degenerate_urls` total are in R3 | **PASS** |
| 5 | **Count invariants and arXiv projection.** Invariants on all six configs: per row, `citation_count == len(cited_by)` and `reference_count == Σ len(matches)` (0 violations); no duplicate work keys, duplicate citers, self-citations, empty `matches` or non-positive counts; citer `provenance` is the strongest of its matches; `asserted` holds only publisher/crossref matches and `mined` only mined. Totals equal `RESULT_JSON`. `all = asserted + mined` holds: 7,420,027 = 1,277,754 + 6,142,273 and 6,028,757 = 454,190 + 5,574,567. `arxiv_doi = '10.48550/arXiv.' ‖ arxiv_id` for every row. The arXiv projection matches the DataCite arXiv subset in every config: 0 missing, 0 extra, 0 count mismatches, 0 nested `cited_by` hash mismatches. As a negative control, reversing `cited_by` changed the hash for all 566,446 / 45,148 / 536,402 multi-citer works. Schemas: DataCite `all`/`mined` `cited_by` types are identical to run 1, and `asserted` is byte-identical. Note: `ambiguous_urls.reference` now has 14 fields instead of 19 (`ISBN`, `ISSN`, `edition`, `isbn-type`, `issn-type` dropped). The 13 run-1 rows with any of these fields set were all removed as degenerate [computed] | **PASS** |
| 5b | **arXiv files vs run 1.** `asserted.parquet` is byte-identical (SHA-256 `220139da…1203`). `all.parquet` (332,971,651 → 333,515,081 B) and `mined.parquet` (312,368,937 → 312,219,158 B) differ in bytes but are logically identical: full-row MD5 over 1,042,011 and 998,112 rows shows 0 old-only, 0 new-only and 0 positional mismatches, so row order is the same. The row-group count is the same (1,032 / 988), but the boundaries moved; for example, the first groups hold 8/8/16/32/64 rows in run 1 and 7/8/16/31/62 now. Likely cause: the transform plans its hash-bucket count from the whole flat table, and the smaller DataCite table (7,420,027 rows) gave 39 buckets instead of 40. That changes which rows the multi-range arXiv configs (`all` 5 ranges, `mined` 4) stream per batch, and `_row_group_pieces` splits batches into row groups. The single-range `asserted` is unaffected. The README differs through the card fixes (R8), and the manifest differs only in `source_tree_sha256` and those file hashes [hub, log2, computed] | **PASS** (logically identical) |
| 6 | **Fix effectiveness** (R6): 0 junk-page URL matches and 0 junk ambiguity rows remain | **PASS** |
| 7 | **Delta explanation** (R7): every removed and added match, citation, work and ambiguity row is classified; residue 0 | **PASS** |
| 8 | **Dataset cards** (R8): corrected `mined` wording, broadened `raw_match`, computed `size_categories`, degenerate landing-page limitation | **PASS** |
| 9 | **Bucket state** (R9): run-1 generation removed; rerun generation present; `indexes/`, `benchmarks/` and the arXiv prefix untouched | **PASS** |

### R6. Fix effectiveness [computed]

| DataCite DOI | Run 1 in `all` | Rerun |
| --- | --- | --- |
| `10.26091/9prt-kt79` (`https://doi.org/`) | 37,719 citations / 73,877 references, all `url`/`mined` | In no config; 0 matches of any type, so no DOI evidence either |
| `10.4233/uuid:aca2b8dd-a24b-48f3-82ed-8a4d535d72d5` (`http://dx.doi.org/`) | 6,227 / 11,720, all `url`/`mined` | In no config; 0 matches |
| `10.25768/10.25768/fal.eikon-n05-a06`, `10.25768/10.25768/l-22-002` (`http://www`) | Not in `all`; 43,372 `ambiguous_urls` rows | Not in `all`; 0 ambiguity rows |

Junk checks:
- **URL matches.** 0 of 73,349 URL matches in the rerun's `all` have a `raw_match` that normalizes to a resolver host, a bare site root or a dotless host. In run 1 it was 95,120 of 168,468: 85,597 resolver, 9,523 bare root, 0 dotless.
- **Ambiguity rows.** 0 of 2,631 `ambiguous_urls` rows have a `url` on a resolver host, at a bare root or on a dotless host. In run 1 it was 45,177: 43,372 dotless (`http://www/`) and 1,805 bare root.

The classifier is a Python emulation of `is_degenerate_landing` at `bba775c`:
- the host is lower-cased, userinfo and port are removed, and zero-width characters are dropped, as the WHATWG host parser drops them;
- dot segments are resolved, an empty path becomes `/`, and an empty query or fragment is dropped;
- a scheme-less value is tried as both `http://` and `https://`.

Top 10 by `citation_count DESC, doi`, citations / references:

| Rank | Run 1, overall | Rerun, overall | Run 1, excluding `10.48550/arxiv.` | Rerun, excluding `10.48550/arxiv.` |
| --- | --- | --- | --- | --- |
| 1 | `arxiv.1412.6980` 44,907 / 45,061 | `arxiv.1412.6980` 44,907 / 45,061 | `10.26091/9prt-kt79` 37,719 / 73,877 | `10.18434/t4d303` 4,384 / 4,449 |
| 2 | `10.26091/9prt-kt79` 37,719 / 73,877 | `arxiv.1706.03762` 36,818 / 37,014 | `10.4233/uuid:aca2b8dd-…` 6,227 / 11,720 | `10.24381/cds.adbb2d47` 2,807 / 2,923 |
| 3 | `arxiv.1706.03762` 36,818 / 37,014 | `arxiv.1409.1556` 31,493 / 31,709 | `10.18434/t4d303` 4,384 / 4,449 | `10.5281/zenodo.3509134` 2,025 / 2,039 |
| 4 | `arxiv.1409.1556` 31,493 / 31,709 | `arxiv.1810.04805` 23,043 / 23,232 | `10.24381/cds.adbb2d47` 2,807 / 2,923 | `10.5065/d68s4mvh` 1,823 / 1,898 |
| 5 | `arxiv.1810.04805` 23,043 / 23,232 | `arxiv.2010.11929` 21,677 / 21,845 | `10.5066/p956phzq` 2,757 / 2,865 | `10.7275/jyj1-4868` 1,608 / 1,611 |
| 6 | `arxiv.2010.11929` 21,677 / 21,845 | `arxiv.1804.02767` 10,978 / 11,047 | `10.24436/2` 2,094 / 2,169 | `10.24381/cds.bd0915c6` 1,562 / 1,614 |
| 7 | `arxiv.1804.02767` 10,978 / 11,047 | `arxiv.1704.04861` 10,406 / 10,472 | `10.5281/zenodo.3509134` 2,025 / 2,039 | `10.18434/t4w30f` 1,150 / 1,150 |
| 8 | `arxiv.1704.04861` 10,406 / 10,472 | `arxiv.1609.02907` 9,983 / 10,044 | `10.5065/d68s4mvh` 1,823 / 1,898 | `10.15468/39omei` 1,107 / 1,287 |
| 9 | `arxiv.1609.02907` 9,983 / 10,044 | `arxiv.1301.3781` 9,546 / 9,612 | `10.7275/jyj1-4868` 1,608 / 1,611 | `10.5066/f7p55kjn` 1,083 / 1,237 |
| 10 | `arxiv.1301.3781` 9,546 / 9,612 | `arxiv.1907.11692` 9,387 / 9,453 | `10.24381/cds.bd0915c6` 1,562 / 1,614 | `10.7289/v5c8276m` 1,075 / 1,097 |

`arxiv.` abbreviates `10.48550/arxiv.`. Two works also left the non-arXiv top 10 because of the bare-root rule: `10.5066/p956phzq` (landing page `earthexplorer.usgs.gov`) fell from 2,757 citations to 0, and `10.24436/2` (`wsc.nmbe.ch`) from 2,094 to 747.

### R7. Delta classification for DataCite `all` [computed]

**Method.** I diffed the two revisions' match sets as multisets (`EXCEPT ALL`). The key was `(doi, citing doi, match_type, raw_match, md5(reference struct))`, over 7,515,087 run-1 and 7,420,027 rerun matches, with the same 19-field reference schema on both sides. Adding `provenance` to the key changed nothing (95,124 / 64), so no match only changed provenance. Removed and added rows were then paired on `(doi, citing, match_type, reference)` and on `(citing, reference)` to find evidence that was only re-rendered or re-targeted. Every removed and added match has `mined` provenance, which is why `asserted` is unchanged.

**References** (matches):

| Class | Removed | Added | Net | Cause |
| --- | --- | --- | --- | --- |
| URL on a DOI resolver host (`doi.org`, `dx.doi.org`) | 85,597 | 0 | −85,597 | `2adb479`; all on `10.26091/9prt-kt79` (73,877) and `10.4233/uuid:aca2b8dd-…` (11,720) |
| URL at another bare site root | 9,522 | 0 | −9,522 | `2adb479`; the user-accepted loss (top hosts below) |
| URL at a bare site root, re-targeted to a deeper registered page | 1 | 1 | 0 | `2adb479`; example below |
| URL on a dotless host | 0 | 0 | 0 | `http://www` never produced a confirmed match: its 2 candidates always made it ambiguous |
| DOI recovered from text that joins DOIs without whitespace | 0 | 61 | +61 | token now ends at the next DOI start (`d8936fc`, kept by `bba775c`) |
| Inner DOI no longer emitted beside its registered outer DOI | 2 | 0 | −2 | `bba775c` whole-token claim |
| DOI `raw_match` re-rendered, same reference and DOI | 2 | 2 | 0 | token cut: the first occurrence now matches |
| Residue | 0 | 0 | 0 | |
| **Total** | **95,124** | **64** | **−95,060** | equals the published delta |

**Citations** (distinct `(doi, citing)` pairs): 52,903 lost and 60 gained, net −52,843.
- Lost: 43,946 resolver-host; 8,954 bare root; 1 re-targeted; 2 inner DOI.
- Gained: 59 recovered DOI; 1 re-targeted.

**Works:** 136 lost and 32 gained, net −104.
- Lost: 133 bare root; 2 resolver-host (the two DOIs above); 1 re-target source (`10.6100/atomiclimits`).
- Gained: all 32 by recovered DOIs.

**`mined` config.** References are the same −95,060.
- Citations: 52,908 lost (43,946 resolver-host, 8,959 bare root, 2 inner, 1 re-targeted) and 60 gained, net −52,848.
- Works: 156 lost (152 bare root, 2 resolver-host, 1 re-target source, 1 inner DOI) and 36 gained, net −120.
- The 5 bare-root citations lost in `mined` but kept in `all` still have asserted evidence.

**Ambiguity rows.** 45,177 removed and 0 added; the other 2,631 rows are unchanged. The comparison covered `citing_doi`, `ref_index`, `url`, `raw_match`, the candidate list, and the 14 shared reference fields.

| Class | Removed rows | URLs |
| --- | --- | --- |
| Dotless host | 43,372 | `http://www/` |
| Bare site root | 1,805 | 41 URLs: `http://www.google.com/` 897, `https://landscan.ornl.gov/` 194, `https://hal.archives-ouvertes.fr/` 97, `https://www.nhgis.org/` 89, `https://rosap.ntl.bts.gov/` 73, `https://www.re3data.org/` 69, `https://datadryad.org/` 53, `https://google.com/` 53, `https://www.example.com/` 49, `https://datacite.org/` 38, … |
| Resolver host | 0 | |
| Residue | 0 | |
| **Total** | **45,177** | 47,808 → 2,631 |

**Bare-root URL loss by host.** 9,523 references (including the re-targeted one) on 235 hosts and 237 DOIs, from 8,917 citing works:

| Host | References removed | DOI | Citations in `all`, run 1 → rerun |
| --- | --- | --- | --- |
| `earthexplorer.usgs.gov` | 2,865 | `10.5066/p956phzq` | 2,757 → 0 |
| `wsc.nmbe.ch` | 1,414 | `10.24436/2` | 2,094 → 747 |
| `cccbdb.nist.gov` | 1,184 | `10.18434/t47c7z` | 1,371 → 191 |
| `elibrary.ru` | 459 | `10.34983/dtipb.2022.69.29.002` | 374 → 0 |
| `www.cnki.net` | 428 | `10.23650/001` | 219 → 0 |
| `www.itis.gov` | 356 | `10.5066/f7kh0kbk` | |
| `www.usgs.gov` | 356 | `10.5066/f7hh6h38` | |
| `janaf.nist.gov` | 208 | `10.18434/t42s31` | |
| `figshare.com` | 192 | `10.4225/03/58be30f9dc88d` | |
| `www.mrlc.gov` | 138 | `10.5066/p937pn4z` | |
| 225 other hosts | 1,923 | | |

Why "~567" understated the loss: it was section 9's host-only `raw_match` count (86,164) minus the resolver-root matches (85,597). Many removed matches are not in the `scheme://host[/]` form:
- **Resolver-host matches:** 79,403 host-only, 6,194 other forms.
- **Bare-root matches:** 6,938 host-only; 2,506 end in a dot segment (such as `https://earthexplorer.usgs.gov/.`); 78 are scheme-less; 1 has an empty query.

**Examples.**
- **URL re-targeted.** The reference is `10.3724/jfmd.2507043`: "… https://www.atomiclimits.com/alddatabase/..".
  - Run 1 matched `10.6100/atomiclimits`: `raw_match` `…/alddatabase/..` normalizes to the site root.
  - The rerun skips that key and matches `10.6100/alddatabase` with `…/alddatabase/.`.
- **DOI recovered (61).** The separator between the matched DOI and the next DOI was `.` 42 times, `;` 8, `,` 7 and `">` 4. Fields: `unstructured` 45, `DOI` 16. The matches cover 58 DOIs and 51 citing works.
  - `10.22004/ag.econ.159299` ← `10.2478/euco-2022-0006`: "DOI: 10.22004/ag.econ.159299.10.1017/S0163548400004556".
  - `10.6103/share.w1.611`, `w2`, `w3` ← `10.2139/ssrn.3372999`: the publisher `DOI` field "10.6103/SHARE.w1.611,10.6103/SHARE.w2.611,…". The value as a whole is not a DOI, so these are `mined`.
- **Inner DOI dropped (2).** Both are cases where DataCite registers the doubled-prefix DOI itself.
  - `10.23638/lmcs-15(4:15)2019` ← `10.1007/978-3-031-30829-1_12`. The `DOI` field is "10.23638/10.23638/lmcs-15(4:15)2019" (`crossref`). That outer DOI is registered and is still matched with `crossref` provenance; run 1 also emitted the inner DOI as `mined`.
  - Likewise `10.18426/obm.3mbbectm2bmg` from "10.18426/10.18426/obm.3mbbectm2bmg" (`publisher`).
- **`raw_match` re-rendered (2).**
  - `10.5281/zenodo.8211905` ← `10.1126/science.adh4265`: "…https:/doi.org/10.5281/zenodo.8211905.10.5281/ZENODO.8211905". `raw_match` was `10.5281/ZENODO.8211905` and is now `10.5281/zenodo.8211905`.
  - Likewise `10.18757/ejtir.2015.15.1.3057` ← `10.2478/ttj-2022-0016`.

### R8. Dataset cards at the new commits [hub, computed]

The relevant changed lines are quoted below; the diff is against `343fe4e2` and `cde56bb0`. The other changed lines are the count rows, the DataCite fingerprint, the `publisher`/`crossref`/`mined` bullet definitions (reworded to match), and the closing limitation, where "unstructured references" became "reference metadata".

**DataCite** (`71c8cd7f`, `README.md` 8,428 B):
- `size_categories`: `  - 1M<n<10M` (computed from 1,715,666 works).
- Provenance paragraph: "`publisher` and `crossref` matches come from references whose structured `DOI` field holds the cited DOI with a `doi-asserted-by` value of `publisher` or `crossref`. Every other match is `mined`: a DOI, arXiv identifier, or registered landing-page URL found in the reference's `DOI` (without that assertion), `URL`, `article-title`, `journal-title`, or `unstructured` field." Previously: "`mined` matches were extracted from unstructured reference text".
- Config table: "| `mined` | Citations mined from reference metadata without a publisher or Crossref DOI assertion | 1,325,785 | 6,104,684 | 6,142,273 |".
- `raw_match`: "The exact reference text that matched (a substring, or a whole structured value)". Previously: "The exact substring that matched".
- New limitation: "Registered landing pages on DOI resolver hosts (`doi.org`, `dx.doi.org`, `www.doi.org`), bare site roots, and hosts without a dot are ignored for URL matching, so records registered with such a URL are matched only by DOI."
- Also: "The `ambiguous_urls` configuration holds 2,631 unresolved …" and the fingerprint `f507938d…f9d5`.

**arXiv** (`63216439`, `README.md` 5,497 B):
- `size_categories`: `  - 1M<n<10M` (was `100K<n<1M`; computed from 1,042,011 works by `size_category`).
- Config table: "| `mined` | Citations mined from reference metadata without a publisher or Crossref DOI assertion | 998,112 | 5,548,054 | 5,574,567 |".
- `raw_match`: "The exact reference text that identified the arXiv work (a substring, or a whole structured value): an arXiv identifier, a DOI, or a URL such as the work's registered landing page". Previously: "…that matched an arXiv identifier pattern".
- Added: "When the dataset is built by the full DataCite pipeline, an arXiv work can also be matched by its DOI anywhere in the reference or by its registered DataCite landing-page URL."

### R9. Bucket state [bucket, log2]

- **Run-1 generation removed.** `derived-generations/2026-07-346ddc7bd7ea4eae98a36df6807c71f8` no longer appears in the listing, and the log recorded `cleanup_removed` at 23:24:09.160Z.
- **Rerun generation present.** `derived-generations/2026-07-4aad4a3a40c94e56b44419daf1a78357` is the only generation. It holds 22 marker-listed files plus `_EXTRACTION_COMPLETE.json` and `_PUBLICATIONS.json`. The root `_EXTRACTION_COMPLETE.json` (5,103 B, 23:24:02Z) and `_PIPELINE_COMPLETE.json` (1,689 B, 23:24:08Z) point to it.
- **`indexes/` untouched.** Its newest upload is 15:09:50Z. `_BUNDLE_CURRENT.json` still names `generations/c30ac6c9…` and manifest `3d96f980…a0bc`.
- **`benchmarks/` untouched.** One attempt directory, `eb559193…`, with all uploads at 18:17:08Z.
- **arXiv prefix untouched.** `crossref-arxiv-citations-latest/` has 14 files, all uploaded on or before 2026-09-01 11:56:13Z.

### R10. Costs

Rates per `list_jobs_hardware` [hub]: `cpu-xl` $0.016667/min, `cpu-basic` $0.000167/min.

| Item | Basis | Cost |
| --- | --- | --- |
| Rerun job `6aab03e4…1af9` | 9,000 running s [hub] → 150 billed minutes × $1.00/h [computed] | **$2.50** (prorated $2.50) |
| ↳ of which the post-log tail | 506 s [computed] | ≈ $0.14 |
| Remote smoke 4, `6aab0118…1ad2` | `cpu-basic`, 614 running s [hub] → 11 billed minutes × $0.01/h | $0.0018 (prorated $0.0017) |
| Post-production spend (smoke 4 + rerun) | computed | ≈ $2.50 |
| **Cumulative:** validation ≈ $5.31 + run 1 $1.67 + smoke 4 + rerun | computed | **≈ $9.48** (range $9.46–$9.51) |

This excludes the 2026-09-01 arXiv-only runs. Each `cpu-xl` job stayed well inside the approved 24 h × $1/h envelope [ledger].

### R11. Remaining known issues

Deferred minor items from the post-production fix review [ledger, "Post-production fix: minor (deferred)"]:
1. `README.md` line 273 should qualify outer-only emission with "when followed by whitespace or end of field". In a comma-joined list, the inner DOI is credited.
2. An unregistered DOI that embeds another DOI prefix counts as two unmatched fragments.
3. A manual CLI `--resume` across binaries can silently replay from zero when there is a single old checkpoint record; the implementer also noted old checkpoints fail to parse. The Python pipeline never resumes, so this should be documented as unsupported.
4. The 8,000-DOI test's byte bound is too loose to catch a dropped whole-token cap.
5. Hosts with a trailing dot are not special-cased.
6. One test asserts `>=` where the exact value is knowable.

Implementer concerns the review judged minor [ledger]:
- `degenerate_urls` counts per distinct candidate per reference.
- Adversarial input dense with DOI starts is still CPU-heavy (measured linear: a 2 MB field in 1.27 s).

Still deferred from earlier reviews [ledger]:
- final review M4–M10;
- the single-pass benchmark sampling redesign (second half of C1 e).

Section 11 status after this rerun:
- **Resolved:** items 1 (cards republished, R8), 2 (the embedded-DOI fix is in the run source, R7) and 4 (degenerate URLs, R6).
- **Still open:** item 3 (the rerun also used default settings, not benchmark-validated ones), items 5–6 (arXiv rows are identical to run 1, so the differences from `9056d87` remain), item 7 (no resource high-water marks) and item 8 (open items from the validation report).

New findings from this verification:
- the size of the bare-root loss (Status, R7);
- the 506 s post-log job tail (R1);
- the `ambiguous_urls.reference` schema narrowing (R5).
