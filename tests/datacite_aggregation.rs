//! Full-mode aggregation: resource-aware path selection, deterministic
//! evidence selection, crash-safe per-partition commits with resume, the
//! ambiguity aggregator, and legacy schema tolerance.

mod support;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use crossref_citation_extraction::index::datacite::IndexFile;
use crossref_citation_extraction::index::{FstIndex, FstIndexBuilder};
use crossref_citation_extraction::streaming::aggregator::{
    aggregate_and_validate, AggregationStats,
};
use crossref_citation_extraction::streaming::ambiguity::aggregate_ambiguities;
use crossref_citation_extraction::streaming::partition_commit::{
    self, PartitionCommit, FAILPOINT_AFTER_RENAME_BEFORE_PROGRESS, FAILPOINT_BEFORE_RENAME,
    FAILPOINT_DURING_FINAL_ASSEMBLY,
};
use crossref_citation_extraction::streaming::partitioning::hashed_partition_key;
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
use polars::prelude::*;
use serde_json::{json, Value};

#[test]
fn both_aggregation_paths_deduplicate_reference_tuples() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let source = tmp.path().join("source");
    support::make_release(
        &source,
        &[serde_json::json!({"id":"10.1234/a"})],
        &[("10.1234/a", "findable")],
    )?;
    let bundle = tmp.path().join("bundle");
    build_datacite_index(&source, &bundle, 64)?;
    let index = DataciteIndex::load(&bundle)?;
    let mut results = Vec::new();
    for (name, memory_bytes) in [("memory", 16 * 1024 * 1024), ("external", 64 * 1024)] {
        let parts = tmp.path().join(name);
        let mut writer = SegmentedPartitionWriter::with_limits(
            &parts,
            WriterLimits {
                memory_bytes: 131_072,
                segment_rows: 2,
                mode: PartitionMode::Hashed { count: 1 },
            },
        )?;
        for i in 0..512_u32 {
            writer.write(PartitionRow {
                citing_doi: "10.9999/citer".into(),
                ref_index: i % 2,
                cited_id: "10.1234/a".into(),
                provenance: "mined".into(),
                match_type: MatchType::Doi,
                raw_match: "10.1234/a".into(),
                ref_json: serde_json::json!({"unstructured":"10.1234/a"}).to_string(),
            })?;
        }
        writer.flush_all()?;
        drop(writer);
        let path = tmp.path().join(format!("{name}.jsonl"));
        let outputs = OutputPaths {
            valid: Some(path.clone()),
            ..Default::default()
        };
        let stats = aggregate_datacite(
            &parts,
            &index,
            &outputs,
            AggregationLimits {
                memory_bytes,
                merge_fan_in: 2,
                sort_workers: 1,
            },
            &"a".repeat(64),
            false,
        )?;
        // The path-selection observer: each budget must exercise its path.
        let expected = if name == "memory" { (1, 0) } else { (0, 1) };
        assert_eq!(
            (stats.in_memory_partitions, stats.external_partitions),
            expected,
            "{name} budget selected the wrong path"
        );
        let rows: Vec<serde_json::Value> = std::fs::read_to_string(path)?
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()?;
        results.push(rows);
    }
    assert_eq!(results[0], results[1]);
    assert_eq!(results[0][0]["cited_by"].as_array().unwrap().len(), 2);
    Ok(())
}

// ---------------------------------------------------------------- fixtures

fn build_index(root: &Path, dois: &[&str]) -> Result<DataciteIndex> {
    let source = root.join("source");
    let metadata: Vec<Value> = dois.iter().map(|d| json!({ "id": d })).collect();
    let states: Vec<(&str, &str)> = dois.iter().map(|d| (*d, "findable")).collect();
    support::make_release(&source, &metadata, &states)?;
    let bundle = root.join("bundle");
    build_datacite_index(&source, &bundle, 64)?;
    DataciteIndex::load(&bundle)
}

fn row(
    citing: &str,
    ref_index: u32,
    cited: &str,
    provenance: &str,
    match_type: MatchType,
    raw: &str,
) -> PartitionRow {
    PartitionRow {
        citing_doi: citing.into(),
        ref_index,
        cited_id: cited.into(),
        provenance: provenance.into(),
        match_type,
        raw_match: raw.into(),
        ref_json: json!({ "unstructured": format!("{citing} cites {cited} via {raw}") })
            .to_string(),
    }
}

/// Two-row segments across 16 hashed partitions.
fn write_fixture(parts: &Path, rows: &[PartitionRow]) -> Result<()> {
    let mut writer = SegmentedPartitionWriter::with_limits(
        parts,
        WriterLimits {
            memory_bytes: 131_072,
            segment_rows: 2,
            mode: PartitionMode::Hashed { count: 16 },
        },
    )?;
    for row in rows {
        writer.write(row.clone())?;
    }
    writer.flush_all()?;
    Ok(())
}

const INDEXED: [&str; 6] = [
    "10.1234/a",
    "10.1234/b",
    "10.1234/c",
    "10.1234/d",
    "10.1234/e",
    "10.1234/f",
];
const UNINDEXED_ARXIV: &str = "10.48550/arxiv.9999.99999";

/// Duplicate keys with competing evidence, a self-citation, an unindexed
/// arXiv candidate, and several cited works spread over partitions; every
/// row is written twice so duplicates cross segments.
fn evidence_rows() -> Vec<PartitionRow> {
    let mut rows = vec![
        // Same key three times: publisher outranks crossref outranks mined.
        row(
            "10.9999/x",
            0,
            "10.1234/a",
            "mined",
            MatchType::Url,
            "https://example.org/a",
        ),
        row(
            "10.9999/x",
            0,
            "10.1234/a",
            "publisher",
            MatchType::Doi,
            "10.1234/a",
        ),
        row(
            "10.9999/x",
            0,
            "10.1234/a",
            "crossref",
            MatchType::Doi,
            "10.1234/a",
        ),
        // Same provenance: doi evidence outranks arxiv evidence.
        row(
            "10.9999/x",
            3,
            "10.1234/a",
            "mined",
            MatchType::Arxiv,
            "arXiv:1234.00001",
        ),
        row(
            "10.9999/x",
            3,
            "10.1234/a",
            "mined",
            MatchType::Doi,
            "10.1234/a",
        ),
        row(
            "10.9999/w",
            1,
            "10.1234/a",
            "mined",
            MatchType::Url,
            "https://example.org/a",
        ),
        // Self-citation: dropped.
        row(
            "10.1234/a",
            5,
            "10.1234/a",
            "publisher",
            MatchType::Doi,
            "10.1234/a",
        ),
        row(
            "10.9999/y",
            2,
            "10.1234/b",
            "crossref",
            MatchType::Doi,
            "10.1234/b",
        ),
        // arXiv candidate the index lacks: retained as failed evidence.
        row(
            "10.9999/x",
            4,
            UNINDEXED_ARXIV,
            "mined",
            MatchType::Arxiv,
            "arXiv:9999.99999",
        ),
    ];
    for (i, cited) in INDEXED[2..].iter().enumerate() {
        rows.push(row(
            "10.9999/z",
            i as u32,
            cited,
            "mined",
            MatchType::Doi,
            cited,
        ));
    }
    let again = rows.clone();
    rows.extend(again);
    rows
}

fn all_outputs(dir: &Path) -> OutputPaths {
    OutputPaths {
        valid: Some(dir.join("valid.jsonl")),
        failed: Some(dir.join("failed.jsonl")),
        publisher: Some(dir.join("publisher.jsonl")),
        crossref: Some(dir.join("crossref.jsonl")),
        mined: Some(dir.join("mined.jsonl")),
    }
}

fn limits(memory_bytes: usize) -> AggregationLimits {
    AggregationLimits {
        memory_bytes,
        merge_fan_in: 2,
        sort_workers: 1,
    }
}

fn identity() -> String {
    "a".repeat(64)
}

fn read_jsonl(path: &Path) -> Result<Vec<Value>> {
    fs::read_to_string(path)?
        .lines()
        .map(|line| Ok(serde_json::from_str(line)?))
        .collect()
}

/// Every output file's bytes, keyed by file name.
fn output_bytes(dir: &Path) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut out = BTreeMap::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_file() {
            let name = path.file_name().unwrap().to_str().unwrap().to_owned();
            out.insert(name, fs::read(&path)?);
        }
    }
    Ok(out)
}

/// Digest of every committed extraction segment under `parts`.
fn segment_digests(parts: &Path) -> Result<BTreeMap<PathBuf, IndexFile>> {
    let mut out = BTreeMap::new();
    for entry in fs::read_dir(parts)? {
        let partition = entry?.path();
        let name = partition.file_name().unwrap().to_str().unwrap();
        if !partition.is_dir() || name.starts_with('_') {
            continue;
        }
        for file in fs::read_dir(&partition)? {
            let path = file?.path();
            if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                out.insert(path.clone(), IndexFile::of_file(&path)?);
            }
        }
    }
    Ok(out)
}

fn aggregation_entries(parts: &Path) -> Result<(Vec<String>, Vec<String>)> {
    let mut committed = Vec::new();
    let mut staging = Vec::new();
    let dir = parts.join("_aggregation");
    if dir.exists() {
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            let name = path.file_name().unwrap().to_str().unwrap().to_owned();
            if name.contains(".staging-") {
                staging.push(name);
            } else if path.is_dir() {
                committed.push(name);
            }
        }
    }
    committed.sort();
    staging.sort();
    Ok((committed, staging))
}

fn cited_by_summary(record: &Value) -> Vec<(String, u64, String, String, String)> {
    record["cited_by"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            (
                entry["doi"].as_str().unwrap().to_owned(),
                entry["ref_index"].as_u64().unwrap(),
                entry["provenance"].as_str().unwrap().to_owned(),
                entry["match_type"].as_str().unwrap().to_owned(),
                entry["raw_match"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

fn summary(items: &[(&str, u64, &str, &str, &str)]) -> Vec<(String, u64, String, String, String)> {
    items
        .iter()
        .map(|(d, i, p, m, r)| {
            (
                d.to_string(),
                *i,
                p.to_string(),
                m.to_string(),
                r.to_string(),
            )
        })
        .collect()
}

// ------------------------------------------------------------------- tests

#[test]
fn evidence_selection_and_validation_are_identical_across_paths() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let index = build_index(tmp.path(), &INDEXED)?;
    let rows = evidence_rows();
    let partitions: std::collections::BTreeSet<String> = rows
        .iter()
        .map(|r| hashed_partition_key(&r.cited_id, 16))
        .collect::<Result<_>>()?;
    assert!(partitions.len() >= 2, "fixture must span partitions");

    let mut all_bytes = Vec::new();
    let mut all_stats = Vec::new();
    for (name, memory_bytes) in [("memory", 16 << 20), ("external", 4096)] {
        let parts = tmp.path().join(name);
        write_fixture(&parts, &rows)?;
        let out = tmp.path().join(format!("{name}-out"));
        fs::create_dir_all(&out)?;
        let stats = aggregate_datacite(
            &parts,
            &index,
            &all_outputs(&out),
            limits(memory_bytes),
            &identity(),
            false,
        )?;
        // Selection is per partition: under 4 KiB the partition holding the
        // competing evidence for 10.1234/a is sorted externally while the
        // two-row partitions still fit in memory.
        let heavy = parts
            .join("_aggregation")
            .join(hashed_partition_key("10.1234/a", 16)?);
        let heavy = PartitionCommit::load_verified(&heavy, &identity())?.expect("committed");
        if name == "memory" {
            assert_eq!(stats.in_memory_partitions, partitions.len());
            assert_eq!(stats.external_partitions, 0);
            assert_eq!(heavy.stats.in_memory_partitions, 1);
        } else {
            assert!(stats.external_partitions >= 1);
            assert_eq!(
                stats.in_memory_partitions + stats.external_partitions,
                partitions.len()
            );
            assert_eq!(heavy.stats.external_partitions, 1);
        }
        all_bytes.push(output_bytes(&out)?);
        all_stats.push(stats);
    }
    assert_eq!(
        all_bytes[0], all_bytes[1],
        "paths must produce identical bytes"
    );
    let logical = |stats: &AggregationStats| AggregationStats {
        in_memory_partitions: 0,
        external_partitions: 0,
        ..stats.clone()
    };
    assert_eq!(logical(&all_stats[0]), logical(&all_stats[1]));

    let stats = &all_stats[0];
    assert_eq!(stats.partitions_processed, partitions.len());
    assert_eq!(stats.valid_count, 6);
    assert_eq!(stats.failed_count, 1);
    assert_eq!(stats.total_citations, 9);
    assert_eq!(stats.publisher_citations, 1);
    assert_eq!(stats.crossref_citations, 1);
    assert_eq!(stats.mined_citations, 6);
    assert_eq!(stats.reused_partitions, 0);

    let out = tmp.path().join("memory-out");
    let valid: BTreeMap<String, Value> = read_jsonl(&out.join("valid.jsonl"))?
        .into_iter()
        .map(|r| (r["doi"].as_str().unwrap().to_owned(), r))
        .collect();
    assert_eq!(valid.len(), 6);
    let a = &valid["10.1234/a"];
    assert_eq!(a["citation_count"], 3);
    assert_eq!(
        cited_by_summary(a),
        summary(&[
            ("10.9999/w", 1, "mined", "url", "https://example.org/a"),
            ("10.9999/x", 0, "publisher", "doi", "10.1234/a"),
            ("10.9999/x", 3, "mined", "doi", "10.1234/a"),
        ])
    );
    // The original reference survives as an object, not a string.
    assert_eq!(
        a["cited_by"][1]["reference"]["unstructured"],
        "10.9999/x cites 10.1234/a via 10.1234/a"
    );
    assert_eq!(
        cited_by_summary(&valid["10.1234/b"]),
        summary(&[("10.9999/y", 2, "crossref", "doi", "10.1234/b")])
    );

    let failed = read_jsonl(&out.join("failed.jsonl"))?;
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["doi"], UNINDEXED_ARXIV);
    assert_eq!(
        cited_by_summary(&failed[0]),
        summary(&[("10.9999/x", 4, "mined", "arxiv", "arXiv:9999.99999")])
    );

    let publisher = read_jsonl(&out.join("publisher.jsonl"))?;
    assert_eq!(publisher.len(), 1);
    assert_eq!(publisher[0]["doi"], "10.1234/a");
    assert_eq!(publisher[0]["citation_count"], 1);
    let crossref = read_jsonl(&out.join("crossref.jsonl"))?;
    assert_eq!(crossref.len(), 1);
    assert_eq!(crossref[0]["doi"], "10.1234/b");
    let mined: BTreeMap<String, Value> = read_jsonl(&out.join("mined.jsonl"))?
        .into_iter()
        .map(|r| (r["doi"].as_str().unwrap().to_owned(), r))
        .collect();
    assert_eq!(mined.len(), 5);
    assert_eq!(
        cited_by_summary(&mined["10.1234/a"]),
        summary(&[
            ("10.9999/w", 1, "mined", "url", "https://example.org/a"),
            ("10.9999/x", 3, "mined", "doi", "10.1234/a"),
        ])
    );
    // Works are in cited-DOI order within each partition's run of records.
    let valid_order: Vec<String> = read_jsonl(&out.join("valid.jsonl"))?
        .iter()
        .map(|r| r["doi"].as_str().unwrap().to_owned())
        .collect();
    let mut by_partition: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for doi in &valid_order {
        by_partition
            .entry(hashed_partition_key(doi, 16)?)
            .or_default()
            .push(doi.clone());
    }
    for dois in by_partition.values() {
        let mut sorted = dois.clone();
        sorted.sort();
        assert_eq!(*dois, sorted);
    }
    Ok(())
}

#[test]
fn in_memory_guard_falls_back_when_metadata_underestimates() -> Result<()> {
    // Dictionary encoding stores a repeated 4 KiB reference once per
    // segment, so the Parquet estimate is far below the decoded size.
    let tmp = tempfile::tempdir()?;
    let cited: Vec<String> = (0..10).map(|i| format!("10.1234/w{i}")).collect();
    let cited_refs: Vec<&str> = cited.iter().map(String::as_str).collect();
    let index = build_index(tmp.path(), &cited_refs)?;
    let filler = "x".repeat(4096);
    let rows: Vec<PartitionRow> = (0..100)
        .map(|i| PartitionRow {
            citing_doi: format!("10.9999/c{i}"),
            ref_index: 0,
            cited_id: cited[i % 10].clone(),
            provenance: "mined".into(),
            match_type: MatchType::Doi,
            raw_match: cited[i % 10].clone(),
            ref_json: json!({ "unstructured": filler }).to_string(),
        })
        .collect();
    let parts = tmp.path().join("parts");
    let mut writer = SegmentedPartitionWriter::with_limits(
        &parts,
        WriterLimits {
            memory_bytes: 1 << 20,
            segment_rows: 50,
            mode: PartitionMode::Hashed { count: 1 },
        },
    )?;
    for row in &rows {
        writer.write(row.clone())?;
    }
    writer.flush_all()?;
    drop(writer);

    let out = tmp.path().join("out");
    fs::create_dir_all(&out)?;
    let outputs = OutputPaths {
        valid: Some(out.join("valid.jsonl")),
        ..Default::default()
    };
    // Decoded rows need ~430 KiB while the estimate sees two dictionary
    // pages; each ten-row cited work still fits the budget.
    let stats = aggregate_datacite(
        &parts,
        &index,
        &outputs,
        limits(200 * 1024),
        &identity(),
        false,
    )?;
    assert_eq!(stats.in_memory_partitions, 0);
    assert_eq!(stats.external_partitions, 1);
    assert_eq!(stats.total_citations, 100);
    let valid = read_jsonl(&out.join("valid.jsonl"))?;
    assert_eq!(valid.len(), 10);
    assert!(valid.iter().all(|record| record["citation_count"] == 10));
    Ok(())
}

#[test]
fn resume_after_each_failpoint_matches_a_clean_run() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let index = build_index(tmp.path(), &INDEXED)?;
    let rows = evidence_rows();

    let clean_parts = tmp.path().join("clean");
    write_fixture(&clean_parts, &rows)?;
    let clean_out = tmp.path().join("clean-out");
    fs::create_dir_all(&clean_out)?;
    let clean_stats = aggregate_datacite(
        &clean_parts,
        &index,
        &all_outputs(&clean_out),
        limits(4096),
        &identity(),
        false,
    )?;
    let clean_bytes = output_bytes(&clean_out)?;
    let partition_count = clean_stats.partitions_processed;
    assert!(partition_count >= 2);

    for failpoint in [
        FAILPOINT_BEFORE_RENAME,
        FAILPOINT_AFTER_RENAME_BEFORE_PROGRESS,
        FAILPOINT_DURING_FINAL_ASSEMBLY,
    ] {
        let parts = tmp.path().join(failpoint);
        write_fixture(&parts, &rows)?;
        let before = segment_digests(&parts)?;
        let out = tmp.path().join(format!("{failpoint}-out"));
        fs::create_dir_all(&out)?;
        let outputs = all_outputs(&out);

        // Scoped to this test's directory, so parallel tests are unaffected.
        let armed = partition_commit::arm_failpoint(failpoint, tmp.path());
        let err = aggregate_datacite(&parts, &index, &outputs, limits(4096), &identity(), false)
            .expect_err("armed failpoint must abort the run");
        drop(armed);
        assert!(format!("{err:#}").contains(failpoint), "{err:#}");
        assert_eq!(
            segment_digests(&parts)?,
            before,
            "{failpoint} touched segments"
        );

        let (committed, staging) = aggregation_entries(&parts)?;
        let expected_reuse = match failpoint {
            FAILPOINT_BEFORE_RENAME => {
                assert!(committed.is_empty(), "{committed:?}");
                assert_eq!(staging.len(), 1, "{staging:?}");
                0
            }
            FAILPOINT_AFTER_RENAME_BEFORE_PROGRESS => {
                assert_eq!(committed.len(), 1, "{committed:?}");
                assert!(staging.is_empty(), "{staging:?}");
                1
            }
            _ => {
                assert_eq!(committed.len(), partition_count, "{committed:?}");
                assert!(staging.is_empty(), "{staging:?}");
                assert!(!out.join("valid.jsonl").exists(), "target written early");
                assert!(out.join("valid.jsonl.tmp").exists(), "assembly left no tmp");
                partition_count
            }
        };

        let resumed =
            aggregate_datacite(&parts, &index, &outputs, limits(4096), &identity(), true)?;
        assert_eq!(resumed.reused_partitions, expected_reuse, "{failpoint}");
        assert_eq!(
            AggregationStats {
                reused_partitions: 0,
                ..resumed.clone()
            },
            clean_stats,
            "{failpoint} stats"
        );
        let resumed_bytes = output_bytes(&out)?;
        assert!(!resumed_bytes.contains_key("valid.jsonl.tmp"));
        assert_eq!(resumed_bytes, clean_bytes, "{failpoint} outputs differ");
        assert_eq!(
            segment_digests(&parts)?,
            before,
            "{failpoint} resume touched segments"
        );
        let (committed, staging) = aggregation_entries(&parts)?;
        assert_eq!(committed.len(), partition_count);
        assert!(staging.is_empty());
    }
    Ok(())
}

#[test]
fn a_rerun_without_resume_recomputes_and_keeps_segments_intact() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let index = build_index(tmp.path(), &INDEXED)?;
    let parts = tmp.path().join("parts");
    write_fixture(&parts, &evidence_rows())?;
    let before = segment_digests(&parts)?;
    let out = tmp.path().join("out");
    fs::create_dir_all(&out)?;
    let outputs = all_outputs(&out);

    let first = aggregate_datacite(
        &parts,
        &index,
        &outputs,
        limits(16 << 20),
        &identity(),
        false,
    )?;
    let first_bytes = output_bytes(&out)?;
    // A resumed run over a completed aggregation reuses everything.
    let resumed = aggregate_datacite(
        &parts,
        &index,
        &outputs,
        limits(16 << 20),
        &identity(),
        true,
    )?;
    assert_eq!(resumed.reused_partitions, first.partitions_processed);
    assert_eq!(output_bytes(&out)?, first_bytes);
    // A fresh run discards the commits and recomputes over the old outputs.
    let again = aggregate_datacite(&parts, &index, &outputs, limits(4096), &identity(), false)?;
    assert_eq!(again.reused_partitions, 0);
    assert!(again.external_partitions >= 1);
    assert_eq!(
        again.in_memory_partitions + again.external_partitions,
        first.partitions_processed
    );
    assert_eq!(output_bytes(&out)?, first_bytes);
    assert_eq!(segment_digests(&parts)?, before);

    // Commits made with only `valid` enabled cannot serve a resumed run
    // that also wants `failed`: the fragment was never written.
    let narrow = OutputPaths {
        valid: Some(out.join("valid.jsonl")),
        ..Default::default()
    };
    aggregate_datacite(
        &parts,
        &index,
        &narrow,
        limits(16 << 20),
        &identity(),
        false,
    )?;
    let err = aggregate_datacite(
        &parts,
        &index,
        &outputs,
        limits(16 << 20),
        &identity(),
        true,
    )
    .unwrap_err();
    assert!(
        format!("{err:#}").contains("different output selection"),
        "{err:#}"
    );
    Ok(())
}

#[test]
fn a_commit_that_fails_verification_is_an_error_not_a_recompute() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let index = build_index(tmp.path(), &INDEXED)?;
    let parts = tmp.path().join("parts");
    write_fixture(&parts, &evidence_rows())?;
    let out = tmp.path().join("out");
    fs::create_dir_all(&out)?;
    let outputs = all_outputs(&out);
    aggregate_datacite(
        &parts,
        &index,
        &outputs,
        limits(16 << 20),
        &identity(),
        false,
    )?;

    let (committed, _) = aggregation_entries(&parts)?;
    let dir = parts.join("_aggregation").join(&committed[0]);
    let commit = PartitionCommit::load_verified(&dir, &identity())?.expect("commit exists");
    assert_eq!(commit.partition, committed[0]);
    assert_eq!(commit.identity_sha256, identity());
    assert!(!commit.files.is_empty());

    // Another identity cannot claim this commit.
    let err = PartitionCommit::load_verified(&dir, &"b".repeat(64)).unwrap_err();
    assert!(format!("{err:#}").contains("identity"), "{err:#}");

    // A corrupt fragment fails verification, and a resumed run refuses to
    // paper over it.
    let fragment = commit.files.keys().next().unwrap().clone();
    let mut bytes = fs::read(dir.join(&fragment))?;
    bytes.push(b'\n');
    fs::write(dir.join(&fragment), &bytes)?;
    let err = PartitionCommit::load_verified(&dir, &identity()).unwrap_err();
    assert!(format!("{err:#}").contains(&fragment), "{err:#}");
    let err = aggregate_datacite(
        &parts,
        &index,
        &outputs,
        limits(16 << 20),
        &identity(),
        true,
    )
    .unwrap_err();
    assert!(format!("{err:#}").contains(&fragment), "{err:#}");
    assert_eq!(
        fs::read(dir.join(&fragment))?,
        bytes,
        "corrupt commit was rewritten"
    );

    // No marker at all means nothing to reuse.
    assert!(PartitionCommit::load_verified(&tmp.path().join("nowhere"), &identity())?.is_none());
    Ok(())
}

#[test]
fn a_single_cited_work_over_budget_is_an_explicit_error() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let index = build_index(tmp.path(), &["10.1234/big"])?;
    let rows: Vec<PartitionRow> = (0..40)
        .map(|i| PartitionRow {
            citing_doi: format!("10.9999/citer{i:03}"),
            ref_index: 0,
            cited_id: "10.1234/big".into(),
            provenance: "mined".into(),
            match_type: MatchType::Doi,
            raw_match: "10.1234/big".into(),
            ref_json: json!({ "unstructured": format!("{i}: {}", "y".repeat(200)) }).to_string(),
        })
        .collect();
    let parts = tmp.path().join("parts");
    write_fixture(&parts, &rows)?;
    let before = segment_digests(&parts)?;
    let out = tmp.path().join("out");
    fs::create_dir_all(&out)?;
    let outputs = all_outputs(&out);

    let err =
        aggregate_datacite(&parts, &index, &outputs, limits(8192), &identity(), false).unwrap_err();
    let message = format!("{err:#}");
    assert!(message.contains("10.1234/big"), "{message}");
    assert!(message.contains("8192"), "{message}");
    assert!(message.contains("bytes"), "{message}");
    assert!(!out.join("valid.jsonl").exists());
    let (committed, _) = aggregation_entries(&parts)?;
    assert!(committed.is_empty(), "a failed partition must not commit");
    assert_eq!(segment_digests(&parts)?, before);

    // The same data under a sufficient budget is fine.
    let stats = aggregate_datacite(
        &parts,
        &index,
        &outputs,
        limits(1 << 20),
        &identity(),
        false,
    )?;
    assert_eq!(stats.total_citations, 40);
    Ok(())
}

// -------------------------------------------------------------- ambiguities

fn ambiguity(citing: &str, ref_index: u32, url: &str, candidates: &[&str], note: &str) -> Value {
    json!({
        "citing_doi": citing,
        "ref_index": ref_index,
        "url": url,
        "raw_match": format!("{url}/raw"),
        "candidate_dois": candidates,
        "reference": { "unstructured": note, "nested": { "keep": [1, 2, 3] } },
    })
}

fn write_segment(dir: &Path, name: &str, rows: &[Value]) -> Result<()> {
    fs::create_dir_all(dir)?;
    let mut text = String::new();
    for row in rows {
        text.push_str(&serde_json::to_string(row)?);
        text.push('\n');
    }
    fs::write(dir.join(name), text)?;
    Ok(())
}

fn ambiguity_key(row: &Value) -> (String, u64, String) {
    (
        row["citing_doi"].as_str().unwrap().to_owned(),
        row["ref_index"].as_u64().unwrap(),
        row["url"].as_str().unwrap().to_owned(),
    )
}

#[test]
fn ambiguities_are_deduplicated_sorted_and_conflicts_rejected() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let input = tmp.path().join("ambiguities");
    let shared = ambiguity(
        "10.9999/x",
        2,
        "https://example.org/shared",
        &["10.1/a", "10.1/b"],
        "first",
    );
    write_segment(
        &input,
        "segment_0000.jsonl",
        &[
            ambiguity(
                "10.9999/z",
                0,
                "https://example.org/z",
                &["10.1/z1", "10.1/z2"],
                "z",
            ),
            shared.clone(),
            ambiguity(
                "10.9999/x",
                1,
                "https://example.org/one",
                &["10.1/a", "10.1/c"],
                "one",
            ),
        ],
    )?;
    write_segment(
        &input,
        "segment_0001.jsonl",
        &[
            shared.clone(),
            ambiguity(
                "10.9999/x",
                2,
                "https://example.org/other",
                &["10.1/a", "10.1/b"],
                "other",
            ),
        ],
    )?;
    // An unfinished segment is never read.
    fs::write(
        input.join("segment_0002.jsonl.part"),
        "{\"garbage\": true}\n",
    )?;

    let output = tmp.path().join("ambiguities.jsonl");
    let retained = aggregate_ambiguities(&input, &output, &limits(16 << 20), &identity(), false)?;
    assert_eq!(retained, 4);
    let rows = read_jsonl(&output)?;
    assert_eq!(rows.len(), 4);
    let keys: Vec<_> = rows.iter().map(ambiguity_key).collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
    assert_eq!(
        keys[1],
        (
            "10.9999/x".to_owned(),
            2,
            "https://example.org/other".to_owned()
        )
    );
    assert_eq!(rows[2]["candidate_dois"], json!(["10.1/a", "10.1/b"]));
    assert_eq!(rows[2]["reference"]["nested"]["keep"], json!([1, 2, 3]));
    assert_eq!(rows[2]["raw_match"], "https://example.org/shared/raw");

    // A resumed run reuses the committed result byte for byte.
    let first = fs::read(&output)?;
    let retained = aggregate_ambiguities(&input, &output, &limits(16 << 20), &identity(), true)?;
    assert_eq!(retained, 4);
    assert_eq!(fs::read(&output)?, first);

    // A crash at either commit boundary leaves the old output untouched,
    // and the resumed run reproduces it exactly.
    for failpoint in [FAILPOINT_BEFORE_RENAME, FAILPOINT_DURING_FINAL_ASSEMBLY] {
        // Scoped to this test's directory, so parallel tests are unaffected.
        let armed = partition_commit::arm_failpoint(failpoint, tmp.path());
        let err = aggregate_ambiguities(&input, &output, &limits(16 << 20), &identity(), false)
            .expect_err("armed failpoint must abort the run");
        drop(armed);
        assert!(format!("{err:#}").contains(failpoint), "{err:#}");
        assert_eq!(fs::read(&output)?, first, "{failpoint} replaced the output");
        let (committed, staging) = aggregation_entries(&input)?;
        if failpoint == FAILPOINT_BEFORE_RENAME {
            assert!(committed.is_empty(), "{committed:?}");
            assert_eq!(staging.len(), 1, "{staging:?}");
        } else {
            assert_eq!(committed, vec!["ambiguities".to_string()]);
            assert!(staging.is_empty(), "{staging:?}");
        }
        let retained =
            aggregate_ambiguities(&input, &output, &limits(16 << 20), &identity(), true)?;
        assert_eq!(retained, 4, "{failpoint}");
        assert_eq!(fs::read(&output)?, first, "{failpoint} output differs");
        assert!(!tmp.path().join("ambiguities.jsonl.tmp").exists());
    }

    // The same key with a different candidate list is a conflict.
    write_segment(
        &input,
        "segment_0003.jsonl",
        &[ambiguity(
            "10.9999/x",
            2,
            "https://example.org/shared",
            &["10.1/a", "10.1/q"],
            "conflict",
        )],
    )?;
    let err =
        aggregate_ambiguities(&input, &output, &limits(16 << 20), &identity(), false).unwrap_err();
    let message = format!("{err:#}");
    assert!(message.contains("10.9999/x"), "{message}");
    assert!(message.contains("https://example.org/shared"), "{message}");
    assert_eq!(
        fs::read(&output)?,
        first,
        "a failed run must not replace the output"
    );
    Ok(())
}

#[test]
fn ambiguity_external_merge_matches_in_memory_and_empty_input_is_empty() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let input = tmp.path().join("ambiguities");
    let mut rows = Vec::new();
    for i in 0..60_u32 {
        let citing = format!("10.9999/c{}", i % 7);
        let url = format!("https://example.org/u{}", i % 11);
        rows.push(ambiguity(&citing, i % 5, &url, &["10.1/a", "10.1/b"], "n"));
    }
    // Rows are spread over files in interleaved order, duplicates included.
    for (f, chunk) in rows.chunks(25).enumerate() {
        let mut spread: Vec<Value> = chunk.to_vec();
        spread.extend(rows.iter().take(5).cloned());
        write_segment(&input, &format!("segment_{f:04}.jsonl"), &spread)?;
    }
    let big = tmp.path().join("big.jsonl");
    let small = tmp.path().join("small.jsonl");
    let expected = aggregate_ambiguities(&input, &big, &limits(16 << 20), &identity(), false)?;
    // A budget of a few rows forces sorted runs and multi-pass merges.
    let retained = aggregate_ambiguities(&input, &small, &limits(2048), &identity(), false)?;
    assert_eq!(retained, expected);
    assert_eq!(fs::read(&big)?, fs::read(&small)?);
    let distinct: std::collections::BTreeSet<_> =
        read_jsonl(&big)?.iter().map(ambiguity_key).collect();
    assert_eq!(distinct.len() as u64, expected);
    assert!(!input
        .join("_aggregation")
        .join("ambiguities")
        .join("runs")
        .exists());

    let empty_input = tmp.path().join("none");
    let empty_output = tmp.path().join("empty.jsonl");
    assert_eq!(
        aggregate_ambiguities(
            &empty_input,
            &empty_output,
            &limits(2048),
            &identity(),
            false
        )?,
        0
    );
    assert_eq!(fs::read(&empty_output)?, b"");
    fs::create_dir_all(&empty_input)?;
    assert_eq!(
        aggregate_ambiguities(
            &empty_input,
            &empty_output,
            &limits(2048),
            &identity(),
            false
        )?,
        0
    );
    assert_eq!(fs::read(&empty_output)?, b"");
    Ok(())
}

// ------------------------------------------------------------------ legacy

/// A segment in the pre-`match_type` six-column layout.
fn write_old_schema_segment(path: &Path, citing: &str, cited: &str) -> Result<()> {
    let mut df = DataFrame::new(vec![
        Column::new("citing_doi".into(), &[citing]),
        Column::new("ref_index".into(), &[0u32]),
        Column::new("cited_id".into(), &[cited]),
        Column::new("provenance".into(), &["mined"]),
        Column::new("raw_match".into(), &["arXiv:2403.00001"]),
        Column::new("ref_json".into(), &[r#"{"unstructured":"old"}"#]),
    ])?;
    fs::create_dir_all(path.parent().unwrap())?;
    ParquetWriter::new(fs::File::create(path)?).finish(&mut df)?;
    Ok(())
}

#[test]
fn legacy_in_memory_aggregation_tolerates_mixed_segment_schemas() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let parts = tmp.path().join("parts");
    let cited = "10.48550/arxiv.2403.00001";
    // segment_0000 from the old binary, segment_0001 from this one.
    write_old_schema_segment(
        &parts.join("2403").join("segment_0000.parquet"),
        "10.1111/old",
        cited,
    )?;
    let mut writer = SegmentedPartitionWriter::new(&parts, 100)?;
    writer.write(PartitionRow {
        citing_doi: "10.1111/new".into(),
        ref_index: 0,
        cited_id: cited.into(),
        provenance: "publisher".into(),
        match_type: MatchType::Arxiv,
        raw_match: "arXiv:2403.00001".into(),
        ref_json: r#"{"unstructured":"new"}"#.into(),
    })?;
    writer.flush_all()?;
    drop(writer);
    assert!(parts.join("2403").join("segment_0001.parquet").exists());

    let fst_path = tmp.path().join("arxiv.fst");
    let mut builder = FstIndexBuilder::new(&fst_path)?;
    builder.insert(cited)?;
    builder.finish()?;
    let index = FstIndex::load(&fst_path)?;

    let outputs = OutputPaths {
        valid: Some(tmp.path().join("valid.jsonl")),
        failed: Some(tmp.path().join("failed.jsonl")),
        ..Default::default()
    };
    let stats = aggregate_and_validate(&parts, Some(&index), &outputs, false, 0)?;
    assert_eq!(stats.valid_count, 1);
    assert_eq!(stats.total_citations, 2);
    assert_eq!(stats.in_memory_partitions, 1);
    assert_eq!(stats.external_partitions, 0);
    let valid = read_jsonl(&tmp.path().join("valid.jsonl"))?;
    assert_eq!(valid.len(), 1);
    assert_eq!(valid[0]["citation_count"], 2);
    let citers: Vec<&str> = valid[0]["cited_by"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["doi"].as_str().unwrap())
        .collect();
    assert_eq!(citers, vec!["10.1111/new", "10.1111/old"]);
    // The legacy serializer is unchanged: no ref_index or match_type.
    assert!(valid[0]["cited_by"][0].get("match_type").is_none());
    assert!(valid[0]["cited_by"][0].get("ref_index").is_none());
    Ok(())
}
