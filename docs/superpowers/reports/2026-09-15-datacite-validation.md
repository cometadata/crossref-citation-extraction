# DataCite citation mining: validation report (Task 15)

Plan: `docs/superpowers/plans/2026-09-15-datacite-citation-mining.md`. Branch: `mine-all-datacite-citations`. Written 2026-09-16.

## Sources and conventions

Every figure below comes from one of these sources. Values I computed myself (sums, ratios, unit conversions, projections) are marked "computed".

| Source | What it provides |
| --- | --- |
| `.superpowers/sdd/2026-09-15-datacite-citation-mining/progress.md` (ledger, git-ignored) | commits, job ids, rulings, user decisions, cost estimates |
| `.superpowers/sdd/2026-09-15-datacite-citation-mining/task-15-report.md` (git-ignored) | local check results, smoke and benchmark attempt details, fix rounds |
| `benchmark-remote-4.log` (same directory) | `PIPELINE_EVENT=` events of benchmark attempt 4 |
| `smoke-remote-3.log` (same directory) | the passing remote smoke on the final digest |
| Hugging Face Hub, read-only `inspect_job` / `fetch_job_logs` calls (2026-09-16 18:29Z) | `running_secs` per job; phase durations of the 2026-09-01 reference run, where the ledger records only `build_fst` |
| `hf jobs hardware` (2026-09-16) | hardware and prices |

Units: GiB = 2^30 bytes and GB = 10^9 bytes. Byte counts are quoted exactly as logged.

## 1. Summary

This report covers validation of the full DataCite mining pipeline (crate version 2.2.0). The pipeline mines Crossref references for every DataCite DOI and registered landing-page URL, keeps arXiv extraction, and publishes the DataCite and arXiv citation datasets on Hugging Face. Validation had three parts: the local check list, a remote smoke on `cpu-basic`, and a remote benchmark on `cpu-xl` against Crossref snapshot `2026-07` and DataCite release `2026-07`.

- **Final source:** commit `1c54e57` (`fix: count and skip invalid DataCite DOIs instead of failing the build`). Runtime source digest: `4d6b70aae80690cb40b1b796801e8f8bbd4fcc959d9184fe84aa8dfd9f03b09b`.
- **Local checks:** all seven commands passed on the final tree. The local entry-point smoke passed.
- **Remote smoke:** passed on the final digest (job `6aaa942b5527934177eea207`). Counts matched `SMOKE_COUNTS` exactly, and the identity matched the local run.
- **Benchmark:** **no `production-settings.json` was produced.** Four attempts were made:
  - Attempts 1 and 3 found two defects that stopped the run on the real release. Both were fixed.
  - Attempt 2 failed on a platform volume mount.
  - Attempt 4 (`6aaa96e7f76d6a098a710c56`) ran for 5.0 h and measured every real phase. It then failed in its last phase, a synthetic stress transform. No benchmark report or settings file was written.
  - Attempt 4 also showed that its disk and time readings are not reliable (section 5).
- **Production decision:** on 2026-09-16 the user chose to run production on the default settings without rerunning the benchmark:
  - hardware and timeout: `cpu-xl` with a 24 h cap
  - inputs: Crossref snapshot `2026-07` and DataCite release `2026-07`
  - production job: `6aaadf205527934177eeb496`
- **Status:** the plan says Task 15 is complete only when measured readiness is true. **This validation is therefore recorded as incomplete on measured readiness, with production run under the user's override.**

## 2. Local verification

### Version, descriptions and MSRV pins (Part A, commit `6d7c9af`)

Part A made these changes:
- Set `Cargo.toml` to `version = "2.2.0"`.
- Updated the package descriptions in `Cargo.toml` and `pyproject.toml`.
- Rewrote the README.

`rust-version = "1.87"` and every dependency bound stayed unchanged. The 1.87 floor is met through `Cargo.lock` downgrades only:

| Crate | Before | Pinned |
| --- | --- | --- |
| `ar_archive_writer` | 0.5.3 | 0.2.0 |
| `home` | 0.5.12 | 0.5.11 |
| `icu_collections`, `icu_locale_core`, `icu_normalizer`, `icu_normalizer_data`, `icu_properties`, `icu_properties_data` | 2.3.0 | 2.2.0 |
| `icu_provider` | 2.3.1 | 2.2.0 |
| `psm` | 0.1.32 | 0.1.28 |
| `time` / `time-core` / `time-macros` | 0.3.55 / 0.1.9 / 0.2.32 | 0.3.45 / 0.1.7 / 0.2.25 |

The downgrades also changed some transitive entries, for example `comfy-table` 7.2.2 → 7.2.1 and `simple_logger` 5.2.0 → 5.1.0. Toolchains: stable cargo 1.90; `cargo 1.87.0 (99624be96 2025-05-06)` and `rustc 1.87.0 (17067e9ac 2025-05-09)` for the floor check.

### Seven-command check list

The full list ran after each change to runtime source. The final tree is `1c54e57`. Between fix rounds 1 and 2, commit `aa77716` (tests only) fixed the flaky checkpoint test. It passed a 20/20 loop, got a clean re-review, and did not change the digest.

| # | Command | Part A @ `6d7c9af` | Fix round 1 @ `b005af4` | Fix round 2 @ `1c54e57` (final) |
| --- | --- | --- | --- | --- |
| 1 | `CARGO_TARGET_DIR=$PWD/target uv run pytest -q` | 0: `508 passed in 8.73s` | 0: `511 passed in 7.80s` | 0: `511 passed in 8.10s` |
| 2 | `uv run ruff check .` | 0: `All checks passed!` | 0: `All checks passed!` | 0: `All checks passed!` |
| 3 | `cargo fmt --check` | 0 | 0 (after one `cargo fmt` fix and a restart of the list) | 0 |
| 4 | `cargo clippy --locked --all-targets -- -D warnings` | 0, no warnings | 0, no warnings | 0, no warnings |
| 5 | `cargo test --locked` | 0 (later binaries' summary lines truncated by an output filter) | first run 101: flaky `explicit_checkpoint_interval_commits_by_member_count`, 1 segment where 3 were expected; rerun on the same tree 0 | 0: 252 passed, 0 failed (lib 134, `datacite_aggregation` 10, `datacite_index` 15, `datacite_matching` 16, `datacite_normalization` 23, `datacite_pipeline` 15, `datacite_release` 23, `datacite_storage` 9, `integration_test` 7) |
| 6 | `cargo build --release --locked` | 0; binary reports `2.2.0` | 0: `3m 09s` | 0: `0.08s` (binary built in 3 m 11 s for the real-evidence step; only a test file changed after that) |
| 7 | `cargo +1.87.0 check --locked` | 0: `0.37s` | 0: `1.59s` | 0: `1.41s` |

Part A also ran `uv lock --check`, which exited 0.

### Entry-point smoke

Command: `uv run jobs/pipeline.py --mode smoke --source-root "$PWD" --source-digest "$DIGEST" --work-root /tmp/datacite-citations-smoke-entry`, with `CARGO_TARGET_DIR=$PWD/target`.

| Tree | Digest | Exit | `status` | `counts == SMOKE_COUNTS` | `identity_sha256` |
| --- | --- | --- | --- | --- | --- |
| `6d7c9af` | `9a908d26…59f9a` | 0 | `smoke-completed` | true | `3f8d3a3d…d381` (macOS; see the smoke 1 note) |
| `b005af4` | `84d309ec…1a30` | 0 | `smoke-completed` | true | `d8538f8d…8ad8` |
| `1c54e57` (final) | `4d6b70aa…b09b` | 0 | `smoke-completed` | true | `70e69c3547fc99c14fe9ba91a91292f7baedb9a9d59dd1260a9aa254b5baa184` |

`SMOKE_COUNTS` asserted by every smoke:

| Dataset | Configuration | Works | Citations | References |
| --- | --- | --- | --- | --- |
| DataCite | all | 5 | 8 | 9 |
| DataCite | asserted | 3 | 3 | 3 |
| DataCite | mined | 5 | 6 | 6 |
| arXiv | all | 1 | 2 | 2 |
| arXiv | asserted | 1 | 1 | 1 |
| arXiv | mined | 1 | 1 | 1 |

The smoke also checks `ambiguous_urls` 2 and `failed_arxiv_works` 1.

### Checks against real release files (no job, no spend)

- **Real manifest, fix round 1.** Checked against a read-only copy of the real `2026-07` `MANIFEST.json`: 14,410 input entries in 186 folders (`updated_2011-03` … `updated_2026-08`). The inventory digest is `8698c3ff681333a96291b05ea0478220e1a1198f133ab33da164f36b2e0de359`.
- **Real 2012-10 files, fix round 2.** A local `build-datacite-index` ran over the two saved bucket files for 2012-10 and exited 0. The build counted 6 skipped invalid DOIs: 3 identifiers, each counted once in the CSV and once in the JSONL.

## 3. Remote smokes

All smokes ran on `cpu-basic` with a `30m` timeout. Running seconds come from Hub `inspect_job`. Cost = running seconds × $0.01/h (computed). The ledger gives ≈ $0.002 for smokes 1 and 2 and no figure for smoke 3.

| Job | Commit / digest | Status | Running (s) | In-job release build (s) | Cost | Remote `identity_sha256` vs local |
| --- | --- | --- | --- | --- | --- | --- |
| `6aaa74065527934177ee9b28` | `6d7c9af` / `9a908d26…59f9a` | COMPLETED; exact counts | 604 | 581.2 | ≈ $0.0017 | `1cb70c27…ba20` vs `3f8d3a3d…d381`: **differs** (see note) |
| `6aaa81815527934177ee9e09` | `b005af4` / `84d309ec…1a30` | COMPLETED; exact counts | 583 | 558.9 | ≈ $0.0016 | `d8538f8d…8ad8`: **match** |
| `6aaa942b5527934177eea207` | `1c54e57` / `4d6b70aa…b09b` | COMPLETED; exact counts | 581 | 555.3 | ≈ $0.0016 | `70e69c35…a184`: **match** |

Details from `smoke-remote-3.log`:
- The log has 663 lines.
- There is exactly one `RESULT_JSON`, with `status` `smoke-completed` and `source_tree_sha256` `4d6b70aa…b09b`.
- The primary (4-partition) run's `event=identity` line matches `RESULT_JSON` at `70e69c35…a184`. The secondary 1-partition run logs its own identity, `35abae31…994a`.
- The log contains no `Traceback`, panic or `phase_failed` line.
- `bundle_built` reported 5 DOIs, 2 hosts, 3 URLs, 4 URL/DOI pairs, 1 shared URL and `invalid_dois` 0.
- Aggregation paths:
  - `extract_in_memory`: 3 in-memory partitions, 0 external.
  - `extract_external`: 1 external partition.

**Why smoke 1's identity differed.** `gzip.compress(..., mtime=0)` lets zlib write the gzip header. That header's OS byte is `0x13` on macOS and `0x03` on Linux, so the fixture inventory digest, and with it the identity, depended on the platform. Commit `b005af4` writes the fixtures through `GzipFile(mtime=0)`, which always writes OS byte 255. The later smokes' matching identities confirm the fix.

Smoke 1 also confirmed that GNU `sort` on the Linux job image accepts the bundle builder's `sort -u -S -T -o` call. This resolves the Task 3 open question.

## 4. Benchmark attempts

All attempts ran on `cpu-xl` with a `24h` timeout and the default resource settings:
- partitions 256
- writer memory 4,096 MiB; result batch 16 MiB; queue batches 8
- aggregation memory 16,384 MiB; merge fan-in 32; sort workers 4
- lookup memory 64 MiB; max entry 512 MiB
- index sort memory 4,096 MiB; DuckDB memory 32,768 MiB

| # | Job | Commit / digest | How far it got | Failure | Cause | Fix | Running (s) | Cost (ledger) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `6aaa7883f76d6a098a7108d9` | `6d7c9af` / `9a908d26…59f9a` | build (405.7 s), `resolve_snapshot` (mirror `2026-07`), failed in `select_release` | `DataciteIndexError: … entry 'dois/updated_2026-08/2026-08.csv.gz': folder month 2026-08 is later than release 2026-07` | The Task 2 rule "folder month ≤ STATUS.json month" is wrong for real releases. The `2026-07` release was generated on 2026-08-01 10:18 UTC and lists `dois/updated_2026-08/` | `e12453a`: accept any well-formed `dois/updated_YYYY-MM/` folder, in Rust and Python | 432 | ≈ $0.12 |
| 2 | `6aaa8493f76d6a098a7109f7` | `b005af4` / `84d309ec…1a30` | never started (`started_at` None; 493 s scheduling) | `Volume mount failed … Hub API error (401): resolve bucket cometadata/jobs-artifacts` | Platform CSI sidecar. The same volume mounted fine for smoke 2 thirteen minutes earlier | none; one identical resubmission was authorized | 0 | $0 |
| 3 | `6aaa87615527934177ee9f3c` | `b005af4` / `84d309ec…1a30` | build (383.8 s), `select_release` OK (14,410 inputs, inventory `8698c3ff…e359`), failed in `build_datacite_index` after 195.3 s | `…/dois/updated_2012-10/2012-10.csv.gz:2: invalid DOI "10.3929/ethz-a- 000142255"` | Invalid DOIs were fatal. A scan of all 186 CSVs (140,790,442 rows) found exactly 21 invalid DOIs, all with whitespace in the suffix | `1c54e57`: count and skip invalid DOIs (`invalid_dois`); structural errors stay fatal | 606 | ≈ $0.17 |
| 4 | `6aaa96e7f76d6a098a710c56` | `1c54e57` / `4d6b70aa…b09b` | every phase through `synthetic_shared_url_extract`; failed in `synthetic_shared_url_transform`, the last phase | `PublishError: no confirmed citations to publish` (`datacite_publish.py:1427`, via `benchmark.py:1443` `_run_stress`) | The shared-URL stress fixture yields 0 confirmed citations by design, and the transform raises on an entirely empty result (Task 9 ruling). The ruling's instruction to handle this in Task 13 never reached the Task 13 dispatch | **not fixed** (deferred; section 8) | 18,001 | ≈ $5.00 |

Attempt 4 wrote no `benchmark-report.json` and no `production-settings.json`. All measurements below come from its log.

### Attempt 4: phase durations (`phase_completed`)

Job timeline: started 13:17:32.994Z. The failure event is at 18:17:19.874Z, and the Hub reports the job finished at 18:17:34.746Z.

| Phase | Duration (s) | Notes |
| --- | --- | --- |
| `verify_entry_source` | 5.527 | |
| `bootstrap_packages` | 5.280 | |
| `bootstrap_rust` | 9.842 | |
| `verify_source` | 0.580 | |
| `build_extractor` | 555.923 | `Finished release profile … in 9m 15s` |
| `resolve_snapshot` | 0.431 | mirror, `2026-07` |
| `select_release` | 0.764 | `2026-07`, inventory `8698c3ff…e359` |
| `prepare_index` | 6,154.434 | contains the two rows below |
| ↳ `build_datacite_index` | 6,136.216 | Rust log: 178 s manifest verification (13:27:15 → 13:30:13); release scan 5,571.8 s; bundle built in 5,954.1 s |
| ↳ `persist_bundle` | 18.184 | generation `generations/c30ac6c9c1d4448f895d26fa13792919` |
| `source_transfer` / `stage_snapshot` | 1,220.354 / 1,220.337 | archive 280,078,964,653 B, `verified: true` |
| `sample_archive` | 3,664.875 | Python pass over the snapshot |
| `extract_window_00` … `_07` | 23.160, 11.322, 10.947, 10.233, 8.991, 7.770, 6.263, 4.922 | 83.608 s in total (computed) |
| `extract_sample` | 14.990 | 64 files |
| `build_legacy_fst` | 6,236.282 | Rust: 132,965,914 DOIs extracted; FST with 132,965,865 DOIs in 6,057.5 s |
| `extract_legacy` | 8.396 | |
| `transform_sample` | 1.894 | |
| `compress_outputs` | 0.066 | |
| `persist_sample_outputs` | 0.417 | 6,102,354 B uploaded |
| synthetic high-degree: `build_datacite_index` / `_extract` / `_transform` | 0.033 / 0.951 / 5.902 | 200,000 references → 200,000 confirmed DOI matches; transform succeeded |
| synthetic shared-URL: `build_datacite_index` / `_extract` / `_transform` | 0.046 / 1.386 / **failed** at 0.001 | 200,000 references → 200,000 ambiguous URLs, 0 confirmed |

Five phases account for 17,831.9 s of the 18,001 s run (computed): `build_extractor`, `prepare_index`, `source_transfer`, `sample_archive` and `build_legacy_fst`.

### Attempt 4: memory (`phase_measured`)

Column meanings:
- **Resident peak:** `cgroup_resident_peak_bytes`, reset for each phase. This is the non-reclaimable measure adopted in the Task 13 fix round.
- **Children maxrss:** `children_maxrss_after_bytes`, the running `RUSAGE_CHILDREN` maximum. It never goes down.
- **cgroup peak:** `cgroup_peak_after_bytes`. It includes page cache, so it is not used for headroom.

| Phase | Resident peak (B) | Resident peak (GiB, computed) | Children maxrss after (GiB, computed) | cgroup peak after (GiB, computed) |
| --- | --- | --- | --- | --- |
| `prepare_index` | 4,740,812,800 | 4.42 | 4.16 | 101.78 |
| `source_transfer` | 1,095,782,400 | 1.02 | 4.16 | 124.80 |
| `sample_archive` | 1,202,262,016 | 1.12 | 4.16 | 124.80 |
| `extract_window_00` | 1,370,697,728 | 1.28 | 4.16 | 124.80 |
| `extract_window_01` | 1,273,090,048 | 1.19 | 4.16 | 124.80 |
| `extract_window_02` | 1,258,831,872 | 1.17 | 4.16 | 124.80 |
| `extract_window_03` | 1,262,645,248 | 1.18 | 4.16 | 124.80 |
| `extract_window_04` | 1,224,216,576 | 1.14 | 4.16 | 124.80 |
| `extract_window_05` | 1,193,328,640 | 1.11 | 4.16 | 124.80 |
| `extract_window_06` | 1,094,868,992 | 1.02 | 4.16 | 124.80 |
| `extract_window_07` | 1,052,319,744 | 0.98 | 4.16 | 124.80 |
| `extract_sample` | 1,455,271,936 | 1.36 | 4.16 | 124.80 |
| `build_legacy_fst` | 21,523,263,488 | 20.05 | 19.06 | 124.80 |
| `extract_legacy` | 1,213,108,224 | 1.13 | 19.06 | 124.80 |
| `transform_sample` | 1,085,902,848 | 1.01 | 19.06 | 124.80 |
| `compress_outputs` | 830,689,280 | 0.77 | 19.06 | 124.80 |
| `persist_sample_outputs` | 850,145,280 | 0.79 | 19.06 | 124.80 |
| `synthetic_high_degree_extract` | 1,033,691,136 | 0.96 | 19.06 | 124.80 |
| `synthetic_high_degree_transform` | 1,742,180,352 | 1.62 | 19.06 | 124.80 |
| `synthetic_shared_url_extract` | 1,019,654,144 | 0.95 | 19.06 | 124.80 |

### Attempt 4: DataCite index bundle (`bundle_built`, release `2026-07`)

| Count | Value |
| --- | --- |
| `csv_records` | 140,790,421 |
| `metadata_records` | 132,965,909 |
| `dois` | 140,790,372 |
| `urls` | 132,273,455 |
| `url_pairs` | 132,963,965 |
| `shared_urls` | 512,262 |
| `hosts` | 13,378 |
| `missing_urls` | 1,610 |
| `invalid_urls` | 285 |
| `invalid_dois` | 26 |

- **Bundle manifest.** `manifest_sha256` is `3d96f98000712d5505e03f670f664255789e0a47cac5fc52d6a99f5728cea0bc`. It was persisted under `crossref-datacite-citations-latest/indexes/2026-07-v2-8698c3ff…e359/`.
- **File sizes.** From the upload progress bars (rounded): `dois.fst` 341 MB, `urls.fst` 705 MB, `url-targets.bin` 4.91 GB, `hosts.fst` 152 kB, `_INDEX_COMPLETE.json` 1.04 kB. Total ≈ 5.96 GB (computed).
- **`invalid_dois`.** Counts occurrences, not distinct identifiers. The ledger reads the 26 as 21 CSV rows plus 5 JSONL records, which matches the 21-row CSV scan.
- **`invalid_urls`.** The Rust scan summary reports `0 invalid URLs`. The manifest count is the scan count plus URLs that could not be normalized (`src/index/datacite_builder.rs:229-230`), so all 285 are unnormalizable URLs.
- **Run identity.** `identity_sha256` `6ef72a42…6d42`; archive `ad83bc4b…2669`.

### Attempt 4: sampling and sample results

`archive_sampled` recorded `total_members` 37,011, `windows` 8, `selected_members` 64 and `representative: true`. The sample is 0.17% of snapshot files (computed). The defaults were `DEFAULT_STRIDE 5000` and `DEFAULT_WINDOW 8`.

| Run | Files | Items | References | Confirmed matches (DOI / URL / arXiv) | Ambiguous URLs | Rust `extract` (s) | Phase elapsed (s) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| window 00 | 8 | 40,000 | 808,740 | 15 (11 / 1 / 3) | 62 | 22.915 | 23.160 |
| window 01 | 8 | 40,000 | 789,050 | 3,789 (1,243 / 81 / 2,465) | 19 | 11.009 | 11.322 |
| window 02 | 8 | 40,000 | 934,127 | 4,073 (868 / 80 / 3,125) | 12 | 10.567 | 10.947 |
| window 03 | 8 | 40,000 | 597,509 | 1,732 (225 / 22 / 1,485) | 8 | 9.941 | 10.233 |
| window 04 | 8 | 40,000 | 475,457 | 730 (52 / 6 / 672) | 16 | 8.929 | 8.991 |
| window 05 | 8 | 40,000 | 819,596 | 260 (17 / 1 / 242) | 5 | 7.707 | 7.770 |
| window 06 | 8 | 40,000 | 362,817 | 117 (2 / 3 / 112) | 4 | 6.227 | 6.263 |
| window 07 | 8 | 40,000 | 666,870 | 7 (1 / 0 / 6) | 2 | 4.871 | 4.922 |
| sample (all 64) | 64 | 320,000 | 5,454,166 | 10,723 (2,419 / 194 / 8,110) | 128 | 14.617 | 14.990 |

**Sample aggregation.** 256 in-memory partitions and 0 external. Counts: `total_citations` 10,774, `valid_count` 8,829, `failed_count` 50. By source: `publisher_citations` 1,539, `crossref_citations` 111, `mined_citations` 9,073.

**Sample transform** (`transform_completed`):

| Dataset | Works: all | Works: asserted | Works: mined | Rows | Bytes |
| --- | --- | --- | --- | --- | --- |
| DataCite | 8,829 | 1,473 | 7,464 | — | 2,241,345 |
| arXiv | 6,990 | 392 | 6,689 | 8,627 | 1,729,599 |

The event also reports `rows` 10,723 and `ambiguity_rows` 128.

**Legacy comparison** (`semantic_comparison`): `consistent: true`. The full pipeline's arXiv projection and the legacy arXiv pipeline both produced 8,573 citation pairs over 6,990 works, with 0 pairs unique to either side.

## 5. Measurement defects found

Attempt 4's readings could not have produced trustworthy settings even without the crash. All four findings are recorded in the ledger (entries for 2026-09-16) and are backed by the log numbers below.

**(a) The disk probe measures the node filesystem, not the job.** The probe is `shutil.disk_usage(work_root).used` (`citations_pipeline/benchmark.py:486`). Evidence that the reading is not this job's own usage:
- **Readings exceed the allocation.** `phase_peak_used_bytes` ranged from 987,591,671,808 B to 1,355,672,367,104 B, which is 919.8–1,262.6 GiB or 987.6–1,355.7 GB (computed; the ledger's "920–1,263 GB" are the GiB values). The upper readings exceed the `cpu-xl` allocation of 1,000 GB in either unit.
- **A high reading came before the snapshot was staged.** The first reading, `prepare_index`, was already 1,145,529,225,216 B (1,145.5 GB), before the 280.1 GB archive was staged.
- **The reading tracks the job's files on top of an unknown baseline.** Between `extract_window_00` (1,267.1 GB) and `extract_window_01` (987.7 GB) it fell by 279.4 GB (computed), right after the 280.1 GB staged archive was released at 16:31:21Z.

| Phase | `phase_peak_used_bytes` | GB (computed) | GiB (computed) |
| --- | --- | --- | --- |
| `prepare_index` | 1,145,529,225,216 | 1,145.5 | 1,066.9 |
| `source_transfer` | 1,354,891,792,384 | 1,354.9 | 1,261.8 |
| `sample_archive` | 1,355,672,367,104 | 1,355.7 | 1,262.6 |
| `extract_window_00` | 1,267,131,826,176 | 1,267.1 | 1,180.1 |
| `extract_window_01` … `_07`, `extract_sample` | 987,591,671,808 – 987,691,008,000 | 987.6 – 987.7 | 919.8 – 919.9 |
| `build_legacy_fst` | 1,097,644,236,800 | 1,097.6 | 1,022.3 |
| `extract_legacy` … `synthetic_shared_url_extract` | 1,021,402,931,200 – 1,022,881,189,888 | 1,021.4 – 1,022.9 | 951.3 – 952.6 |

**(b) The slowest-window time projection is dominated by fixed overhead.**
- **Window time does not track work.** All eight windows processed 8 files and 40,000 items each, yet their elapsed times fell steadily from 23.16 s to 4.92 s. Two examples:
  - Window 00 (808,740 references) took 23.16 s, while window 05 (819,596 references) took 7.77 s.
  - Window 02, with the most references and matches, took 10.95 s.
- **Most time is fixed overhead.** The single 64-file sample run took 14.99 s, while the eight 8-file runs took 83.61 s together (computed). Per the ledger's assessment, per-run startup and a cold page cache account for most of each window's time.
- **The projection swings by an order of magnitude.** Scaling one run up to 37,011 files gives (computed):
  - Slowest window: 23.16 s / 8 files × 37,011 files = 107,147 s ≈ 29.8 h. The ledger's "≈ 30 h" is this figure, which alone exceeds the 24 h timeout.
  - Fastest window: 4.922 s / 8 × 37,011 = 22,771 s ≈ 6.3 h.
  - Whole sample: 14.99 s / 64 × 37,011 = 8,669 s ≈ 2.4 h.

  **None of these is a valid production estimate.** The spread shows that the method is dominated by fixed overhead.

**(c) The shared-URL stress case cannot pass the transform.**
- **The fixture yields no confirmed citations.** Its extraction reported `references` 200,000, `ambiguous_urls` 200,000 and `confirmed_matches` 0, with every aggregation count 0.
- **The transform rejects an empty result.** It raises `PublishError("no confirmed citations to publish")` whenever the valid JSONL is empty for every destination, as the Task 9 ruling allows.
- **The benchmark does not handle that error.** The same ruling required Task 13 to treat this error as "sample yielded no citations", but that requirement never reached the Task 13 implementation. The benchmark therefore crashed in its last phase, after about 5 h of measurement.

**(d) The benchmark repeats two full-size passes that production does not need.**
- **Legacy arXiv index rebuild.** `build_legacy_fst` rebuilds the legacy arXiv FST from the full DataCite release (input `/mnt/shared-storage/datacite-monthly-data-file-2026-07/dois`, 14,224 files, 132,965,914 DOIs). It took 6,236.282 s and had the run's largest resident peak, 20.05 GiB. The only purpose is to compare against the 64-file sample, whose legacy extraction then took 8.396 s. The 2026-09-01 production run measured the same kind of build at 5,848 s (ledger).
- **Second pass over the snapshot.** `sample_archive` decompressed the whole snapshot again in Python (`tarfile`, stock zlib, one core) for 3,664.875 s, even though `stage_snapshot` had already read the full archive (1,220.337 s).
- **Combined cost.** Together these took 9,901.157 s ≈ 2.75 h, 55% of the 18,001 s run (computed).

## 6. Hardware and pricing

Checked with `hf jobs hardware` on 2026-09-16. The values are unchanged from the documented table read on 2026-09-15 (ledger).

| Flavor | vCPU | RAM | Storage | $/min | $/h |
| --- | --- | --- | --- | --- | --- |
| `cpu-basic` | 2 | 16 GB | 50 GB | 0.0002 | 0.01 |
| `cpu-upgrade` | 8 | 32 GB | 50 GB | 0.0005 | 0.03 |
| `cpu-xl` | 16 | 124 GB | 1,000 GB | 0.0167 | 1.00 |
| `cpu-performance` | 32 | 256 GB | 1,024 GB | 0.0317 | 1.90 |

Billing is per minute while a job is Starting or Running, and the default timeout is 30 m (ledger pricing check, 2026-09-15).

### Validation spend

Method: the ledger's per-job estimates, cross-checked against Hub `running_secs` × hourly rate. The prorated column is computed per second. The whole-minute column is computed as started minutes × $/h ÷ 60.

| Job | Flavor | Running (s) | Ledger estimate | Prorated (computed) | Whole minutes (computed) |
| --- | --- | --- | --- | --- | --- |
| smoke `6aaa7406…9b28` | cpu-basic | 604 | ≈ $0.002 | $0.0017 | $0.0018 |
| benchmark 1 `6aaa7883…08d9` | cpu-xl | 432 | ≈ $0.12 | $0.1200 | $0.1333 |
| smoke `6aaa8181…9e09` | cpu-basic | 583 | ≈ $0.002 | $0.0016 | $0.0017 |
| benchmark 2 `6aaa8493…09f7` | cpu-xl | 0 | $0 | $0 | $0 |
| benchmark 3 `6aaa8761…9f3c` | cpu-xl | 606 | ≈ $0.17 | $0.1683 | $0.1833 |
| smoke `6aaa942b…a207` | cpu-basic | 581 | not stated | $0.0016 | $0.0017 |
| benchmark 4 `6aaa96e7…0c56` | cpu-xl | 18,001 | ≈ $5.00 | $5.0003 | $5.0167 |
| **Total** | | | **≈ $5.31** | **$5.29** | **$5.34** |

The ledger's running total, ≈ $5.31, is its ≈ $0.31 cumulative figure through attempt 3 plus ≈ $5.00 for attempt 4. The per-second and whole-minute recomputations bracket that total. The spend excludes the production job and the 2026-09-01 runs.

## 7. Production decision and risks

### Decision

After attempt 4 failed, the controller put three options to the user:
1. Run production now on the defaults, with snapshot and release pinned.
2. Fix the benchmark's measurement defects and rerun it.
3. Fix only the crash and rerun.

On 2026-09-16 the user chose option 1:
- Run production on the default settings (`cpu-xl` / `24h`) pinned to snapshot `2026-07` and DataCite release `2026-07`.
- Skip a benchmark rerun.
- Publish the new `cometadata/crossref-datacite-citations` (CC0) and update `cometadata/crossref-arxiv-citations`.

The ledger records this as a deviation from the spec's rule that benchmark-selected settings drive production. The user made it with the measured memory headroom and the unmeasured disk risk stated.

### Production job

| Field | Value |
| --- | --- |
| Job | `6aaadf205527934177eeb496` (<https://huggingface.co/jobs/cometadata/6aaadf205527934177eeb496>) |
| Created / started (Hub) | 2026-09-16 18:25:36Z / 18:25:42Z |
| Mode, flavor, timeout | `datacite`, `cpu-xl`, `24h` |
| Pins | `expected_snapshot_id` `2026-07`, `datacite_release` `2026-07` |
| Source | `4d6b70aae80690cb40b1b796801e8f8bbd4fcc959d9184fe84aa8dfd9f03b09b` (label `source=4d6b70aae806`) |
| Resources | the defaults listed in section 4; `benchmark_report_sha256` null |
| State when this report was written | `RUNNING` (Hub, 18:29Z); `verify_entry_source`, `bootstrap_packages`, `bootstrap_rust` and `verify_source` completed |
| Cost ceiling | 24 h × $1.00/h = $24 (computed; the ledger's approved envelope) |

This report does not record the production outcome.

### Memory: measured headroom

- **Index build.** The largest resident peak of a production phase was `prepare_index`, the full DataCite index build: 4,740,812,800 B (4.42 GiB). That is 3.8% of 124 GB (computed).
- **Benchmark only.** The largest resident peak overall was `build_legacy_fst` at 21,523,263,488 B (20.05 GiB), about 16–17% of 124 GB depending on whether GB means 10^9 or 2^30 bytes (computed). Production does not run this phase.
- **Sample-scale phases.** Extraction and transform phases peaked at 1.02–1.74 GB resident (computed). Examples: 1,455,271,936 B for the 64-file sample and 1,742,180,352 B for the 200,000-citation high-degree transform.
- **Not measured.** Memory for extraction, aggregation and the DuckDB transform over the full snapshot. At that scale, production relies on the configured budgets (writer 4,096 MiB, aggregation 16,384 MiB, DuckDB 32,768 MiB, index sort 4,096 MiB) and on the aggregation ≥ 2 × writer rule. By the documented `num_cpus − 2` formula, extraction uses 14 workers on 16 vCPU (computed).

### Disk: unmeasured risk

The benchmark's disk readings cannot be used (defect (a)), so the job's own disk use at full scale is unknown. The flavor's allocation is 1,000 GB. Known sizes:
- **Staged archive:** 280,078,964,653 B (280.1 GB).
- **DataCite bundle:** ≈ 5.96 GB.
- **Not measured:** extraction partitions, aggregation spill, flattened JSONL, Parquet shards and DuckDB temporary files over the full snapshot.

### Time: reference point

No valid production time projection exists (defect (b)). The closest reference is the 2026-09-01 arXiv-only production run, job `6a9691cb21c5aa7c8364b0be` on `cpu-xl`, which completed.

| 2026-09-01 phase | Duration (s) | Source |
| --- | --- | --- |
| `build_extractor` | 352.785 | Hub job log |
| `build_fst` | 5,847.579 | ledger ("5,848 s") and Hub job log |
| `stage_snapshot` | 1,041.957 | Hub job log |
| `extract` | 3,470.854 | Hub job log |
| running total | 11,151 (≈ 3.10 h, computed) | Hub `inspect_job` |

At $1.00/h, that run cost ≈ $3.10 (computed).

Both job logs report the same Crossref snapshot, `2026-07`. For comparison, this branch measured:

| Phase | This branch (attempt 4) | 2026-09-01 equivalent |
| --- | --- | --- |
| DataCite index build (`build_datacite_index`) | 6,136.216 s | `build_fst` 5,847.579 s |
| Staging | 1,220.337 s | 1,041.957 s |

Full-snapshot extraction with DOI and URL matching against the DataCite index has not been timed. Because attempt 4 persisted a bundle for this exact release and inventory, production can reuse it rather than rebuild it; this report does not check whether it did.

### Also first exercised in production

- Paired publication to two datasets, including the Hub tree `size` check for LFS entries (Task 10 open question).
- The `verify_derived` read-back through the bucket mount after upload (Task 11 open question). The benchmark uploaded with `put_file` / `put_tree` (the bundle and 6,102,354 B of sample outputs, both successful) but never calls `verify_derived`.

## 8. Open items

### Deferred benchmark fixes (needed before any benchmark can produce `production-settings.json`)

1. **Stress-case handling.** Handle "no confirmed citations to publish" in `_run_stress` as a zero-citation result for the shared-URL case (defect (c)).
2. **Job-scoped disk measure.** Replace `shutil.disk_usage(work_root).used` with a measure scoped to the job (defect (a)).
3. **Time projection.** Separate per-run startup and cold-cache overhead from per-file cost before projecting (defect (b)).
4. **Legacy FST rebuild.** Stop rebuilding the legacy arXiv FST from the full release. The ledger suggests reusing the cached arXiv-prefix FST or building from the sample's relevant arXiv DOIs only (defect (d)).
5. **Second snapshot pass.** Stop decompressing the snapshot a second time in Python. The ledger suggests sampling in the Rust reader, or selecting members during staging so the archive is read once (defect (d)).

Changing `citations_pipeline/` or `src/` moves the runtime source digest. So these fixes, and any settings a new benchmark derives, apply to a later source tree, not to `4d6b70aa…b09b`.

### Other open concerns in the ledger for Tasks 15–16

- **Stale help text.** The clap `about`/help prose is out of date (`src/cli.rs:34`, the `pipeline` subcommand text, and the `--datacite-index` conflict note). Deferred until after production to keep the digest stable.
- **README and submitter minors:**
  - README:815 settings-timeout wording.
  - README:853 omits `uv lock --check`.
  - The README manifest row should say rows/records are skipped per occurrence (26 on the real release).
  - No submitter guard against `--settings` with `--smoke`.
- **Job tooling:**
  - The bootstrap captures rustup/cargo output, so job logs carry no toolchain version.
  - `HfApi.fetch_job_logs` returns the buffered log and closes while a job is still running. Runbook: poll `inspect_job`, then fetch logs once the job is terminal.
- **Fix round 1 minors:**
  - The `GzipFile(mtime=0)` constructor is spelled at two sites in `datacite_smoke.py`.
  - `release_inventory_sha256`'s `release` argument is validated but no longer affects the result.
- **Flaky-test fix minor.** The segment-count equality depends on `--partitions 1`, which is undocumented at `tests/datacite_pipeline.rs:457`.
- **Fix round 2 minors:**
  - No test pins "every row invalid → the build still fails with no DOIs".
  - The module doc calls invalid DOIs the one tolerated defect, but invalid URLs are tolerated too.
  - Empty-string identifiers are skipped, while absent identifiers stay fatal.
  - A test comment cites a file outside the code tree.
- **Known data limitations:**
  - `invalid_dois` is an occurrence count, and per-identifier detail is logged only at DEBUG.
  - The skipped DataCite DOIs with whitespace in the suffix (21 distinct identifiers found in the CSV scan) cannot be matched.
- **Task 16 still open:**
  - The production run's outcome.
  - Verification of both dataset publications.
  - The final whole-branch review (`fe7ff10..1c54e57`, dispatched in parallel with production).
  - Finishing the development branch.
