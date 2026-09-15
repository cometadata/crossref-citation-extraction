//! Bounded partition storage: the global buffer budget, hashed placement by
//! the complete cited DOI, durable segment commits with resume, and the
//! `match_type` evidence column in both partition modes.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use anyhow::Result;
use crossref_citation_extraction::common::types::MatchType;
use crossref_citation_extraction::streaming::limits::WriterLimits;
use crossref_citation_extraction::streaming::partitioning::{hashed_partition_key, PartitionMode};
use crossref_citation_extraction::streaming::segmented_writer::{
    committed_segments, read_segment, PartitionRow, SegmentedPartitionWriter,
};
use polars::prelude::*;

fn doi_row(ref_index: u32, cited: &str) -> PartitionRow {
    PartitionRow {
        citing_doi: "10.9999/citer".into(),
        ref_index,
        cited_id: cited.into(),
        provenance: "mined".into(),
        match_type: MatchType::Doi,
        raw_match: cited.into(),
        ref_json: serde_json::json!({ "unstructured": cited }).to_string(),
    }
}

/// Every committed row under `dir`, tagged with its partition directory name.
fn read_all(dir: &Path, mode: PartitionMode) -> Result<Vec<(String, PartitionRow)>> {
    let mut partitions: Vec<PathBuf> = fs::read_dir(dir)?
        .map(|entry| Ok(entry?.path()))
        .collect::<Result<_>>()?;
    partitions.sort();
    let mut out = Vec::new();
    for partition in partitions.into_iter().filter(|p| p.is_dir()) {
        let name = partition.file_name().unwrap().to_str().unwrap().to_owned();
        for segment in committed_segments(&partition)? {
            for row in read_segment(&segment, mode)? {
                out.push((name.clone(), row));
            }
        }
    }
    Ok(out)
}

/// Unfinished `.part` files anywhere under `dir`.
fn part_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            out.extend(part_files(&path)?);
        } else if path.extension().and_then(|e| e.to_str()) == Some("part") {
            out.push(path);
        }
    }
    Ok(out)
}

/// A segment in the pre-`match_type` six-column layout.
fn write_old_schema_segment(path: &Path, cited: &[&str]) -> Result<()> {
    let mut df = DataFrame::new(vec![
        Column::new(
            "citing_doi".into(),
            cited.iter().map(|_| "10.9999/citer").collect::<Vec<_>>(),
        ),
        Column::new(
            "ref_index".into(),
            (0..cited.len() as u32).collect::<Vec<_>>(),
        ),
        Column::new("cited_id".into(), cited.to_vec()),
        Column::new(
            "provenance".into(),
            cited.iter().map(|_| "mined").collect::<Vec<_>>(),
        ),
        Column::new(
            "raw_match".into(),
            cited
                .iter()
                .map(|c| format!("arXiv:{c}"))
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "ref_json".into(),
            cited.iter().map(|_| "{}").collect::<Vec<_>>(),
        ),
    ])?;
    fs::create_dir_all(path.parent().unwrap())?;
    ParquetWriter::new(File::create(path)?).finish(&mut df)?;
    Ok(())
}

#[test]
fn bounds_buffers_and_distributes_one_large_prefix() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let mut writer = SegmentedPartitionWriter::with_limits(
        tmp.path(),
        WriterLimits {
            memory_bytes: 131_072,
            segment_rows: 100_000,
            mode: PartitionMode::Hashed { count: 256 },
        },
    )?;
    for i in 0..1_000_u32 {
        let doi = format!("10.5281/zenodo.{i}");
        writer.write(PartitionRow {
            citing_doi: "10.9999/citer".into(),
            ref_index: i,
            cited_id: doi.clone(),
            provenance: "mined".into(),
            match_type: MatchType::Doi,
            raw_match: doi.clone(),
            ref_json: serde_json::json!({"unstructured": doi}).to_string(),
        })?;
        assert!(writer.buffered_bytes() <= 131_072);
    }
    writer.flush_all()?;
    assert_eq!(writer.total_rows_written(), 1_000);
    assert!(writer.partition_count() > 200 && writer.partition_count() <= 256);

    // Every input row round-trips through Parquet with its evidence intact,
    // and each landed in the partition the documented hash names.
    assert_eq!(writer.buffered_bytes(), 0);
    assert!(part_files(tmp.path())?.is_empty());
    let mut rows = read_all(tmp.path(), PartitionMode::Hashed { count: 256 })?;
    assert_eq!(rows.len(), 1_000);
    rows.sort_by_key(|(_, row)| row.ref_index);
    for (i, (partition, row)) in (0..1_000_u32).zip(&rows) {
        let doi = format!("10.5281/zenodo.{i}");
        assert_eq!(row.ref_index, i);
        assert_eq!(row.citing_doi, "10.9999/citer");
        assert_eq!(row.cited_id, doi);
        assert_eq!(row.provenance, "mined");
        assert_eq!(row.match_type, MatchType::Doi);
        assert_eq!(row.raw_match, doi);
        assert_eq!(
            row.ref_json,
            serde_json::json!({"unstructured": doi}).to_string()
        );
        assert_eq!(*partition, hashed_partition_key(&doi, 256)?);
    }
    Ok(())
}

#[test]
fn full_identifier_determines_placement_across_many_prefixes() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let mode = PartitionMode::Hashed { count: 64 };
    let limits = WriterLimits {
        memory_bytes: 1 << 20,
        segment_rows: 1_000,
        mode,
    };
    // 300 distinct prefixes plus 40 DOIs sharing one prefix.
    let mut cited: Vec<String> = (0..300)
        .map(|i| format!("10.{}/item.{i}", 1000 + i))
        .collect();
    cited.extend((0..40).map(|i| format!("10.5281/zenodo.{i}")));

    let mut writer = SegmentedPartitionWriter::with_limits(tmp.path(), limits)?;
    for (i, doi) in cited.iter().enumerate() {
        writer.write(doi_row(i as u32, doi))?;
    }
    writer.flush_all()?;

    let rows = read_all(tmp.path(), mode)?;
    assert_eq!(rows.len(), cited.len());
    for (partition, row) in &rows {
        assert_eq!(*partition, hashed_partition_key(&row.cited_id, 64)?);
        assert!(partition.starts_with('h'), "hashed key {partition}");
    }
    let shared_prefix_partitions: BTreeSet<&str> = rows
        .iter()
        .filter(|(_, row)| row.cited_id.starts_with("10.5281/"))
        .map(|(partition, _)| partition.as_str())
        .collect();
    assert!(
        shared_prefix_partitions.len() > 1,
        "one prefix must spread across partitions, got {shared_prefix_partitions:?}"
    );
    assert!(writer.partition_count() <= 64);

    // Placement is a pure function of the identifier: a second writer over
    // the same rows reproduces the same partition for every row.
    let again = tempfile::tempdir()?;
    let mut writer = SegmentedPartitionWriter::with_limits(again.path(), limits)?;
    for (i, doi) in cited.iter().enumerate() {
        writer.write(doi_row(i as u32, doi))?;
    }
    writer.flush_all()?;
    let mut first: Vec<(String, String)> = rows
        .iter()
        .map(|(p, r)| (r.cited_id.clone(), p.clone()))
        .collect();
    let mut second: Vec<(String, String)> = read_all(again.path(), mode)?
        .iter()
        .map(|(p, r)| (r.cited_id.clone(), p.clone()))
        .collect();
    first.sort();
    second.sort();
    assert_eq!(first, second);
    Ok(())
}

#[test]
fn oversized_row_is_rejected_without_bypassing_budget() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let mode = PartitionMode::Hashed { count: 16 };
    let mut writer = SegmentedPartitionWriter::with_limits(
        tmp.path(),
        WriterLimits {
            memory_bytes: 4096,
            segment_rows: 100,
            mode,
        },
    )?;
    writer.write(doi_row(0, "10.1234/small"))?;
    let before = writer.buffered_bytes();
    assert!(before > 0);

    let mut huge = doi_row(1, "10.1234/huge");
    huge.ref_json = "x".repeat(8192);
    let err = writer.write(huge).unwrap_err();
    let message = format!("{err:#}");
    assert!(message.contains("budget"), "unexpected error: {message}");
    assert!(
        message.contains("10.1234/huge"),
        "unexpected error: {message}"
    );
    assert_eq!(writer.buffered_bytes(), before);

    writer.flush_all()?;
    let rows = read_all(tmp.path(), mode)?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.cited_id, "10.1234/small");
    Ok(())
}

#[test]
fn buffered_bytes_covers_rows_and_returns_to_zero_after_flush() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let mut writer = SegmentedPartitionWriter::with_limits(
        tmp.path(),
        WriterLimits {
            memory_bytes: 1 << 20,
            segment_rows: 1_000,
            mode: PartitionMode::Hashed { count: 4 },
        },
    )?;
    assert_eq!(writer.buffered_bytes(), 0);
    let mut accounted = 0;
    for i in 0..10 {
        let row = doi_row(i, &format!("10.1234/{i}"));
        accounted += row.accounted_bytes();
        writer.write(row)?;
    }
    assert!(writer.buffered_bytes() >= accounted);
    writer.flush_all()?;
    assert_eq!(writer.buffered_bytes(), 0);
    writer.write(doi_row(10, "10.1234/again"))?;
    assert!(writer.buffered_bytes() > 0);
    writer.flush_all()?;
    assert_eq!(writer.buffered_bytes(), 0);
    assert_eq!(writer.total_rows_written(), 11);
    Ok(())
}

#[test]
fn resume_recovers_segment_numbers_and_removes_only_owned_part_files() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let mode = PartitionMode::Hashed { count: 1 };
    let limits = WriterLimits {
        memory_bytes: 1 << 20,
        segment_rows: 2,
        mode,
    };
    let partition = tmp.path().join("h0000");
    {
        let mut writer = SegmentedPartitionWriter::with_limits(tmp.path(), limits)?;
        for i in 0..4 {
            writer.write(doi_row(i, &format!("10.1234/{i}")))?;
        }
        writer.flush_all()?;
    }
    assert!(partition.join("segment_0000.parquet").exists());
    assert!(partition.join("segment_0001.parquet").exists());
    let first_before = fs::read(partition.join("segment_0000.parquet"))?;
    let second_before = fs::read(partition.join("segment_0001.parquet"))?;

    // Leftovers from an interrupted run and files the writer does not own.
    let unfinished = partition.join("segment_0002.parquet.part");
    fs::write(&unfinished, b"truncated")?;
    let sorted_temp = partition.join("_sorted_00000.parquet");
    fs::write(&sorted_temp, b"aggregator temp")?;
    let notes = partition.join("notes.txt");
    fs::write(&notes, b"keep me")?;
    let checkpoint = tmp.path().join("extraction.checkpoint");
    fs::write(&checkpoint, b"keep me too")?;

    let mut writer = SegmentedPartitionWriter::with_limits(tmp.path(), limits)?;
    assert!(
        !unfinished.exists(),
        "owned .part file is removed on resume"
    );
    assert!(sorted_temp.exists());
    assert!(notes.exists());
    assert!(checkpoint.exists());
    assert_eq!(writer.partition_count(), 1);

    writer.write(doi_row(4, "10.1234/4"))?;
    writer.flush_all()?;
    assert!(partition.join("segment_0002.parquet").exists());
    assert!(!partition.join("segment_0003.parquet").exists());
    assert_eq!(
        fs::read(partition.join("segment_0000.parquet"))?,
        first_before
    );
    assert_eq!(
        fs::read(partition.join("segment_0001.parquet"))?,
        second_before
    );

    let cited: BTreeSet<String> = read_all(tmp.path(), mode)?
        .into_iter()
        .map(|(_, row)| row.cited_id)
        .collect();
    let expected: BTreeSet<String> = (0..5).map(|i| format!("10.1234/{i}")).collect();
    assert_eq!(cited, expected);
    Ok(())
}

#[test]
fn legacy_mode_defaults_missing_match_type_to_arxiv() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let segment = tmp.path().join("10.1234").join("segment_0000.parquet");
    write_old_schema_segment(&segment, &["10.1234/a", "10.1234/b"])?;

    let rows = read_segment(&segment, PartitionMode::Legacy)?;
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.match_type == MatchType::Arxiv));
    assert_eq!(rows[0].cited_id, "10.1234/a");
    assert_eq!(rows[0].raw_match, "arXiv:10.1234/a");
    assert_eq!(rows[1].ref_index, 1);
    Ok(())
}

#[test]
fn full_mode_rejects_old_schema() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let segment = tmp.path().join("h0000").join("segment_0000.parquet");
    write_old_schema_segment(&segment, &["10.1234/a"])?;

    let err = read_segment(&segment, PartitionMode::Hashed { count: 1 }).unwrap_err();
    let message = format!("{err:#}");
    assert!(
        message.contains("match_type"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("segment_0000.parquet"),
        "unexpected error: {message}"
    );
    Ok(())
}

#[test]
fn legacy_writer_keeps_prefix_keys_and_row_threshold() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let mut writer = SegmentedPartitionWriter::new(tmp.path(), 2)?;
    let legacy_row = |ref_index: u32, cited: &str| PartitionRow {
        citing_doi: "10.9999/citer".into(),
        ref_index,
        cited_id: cited.into(),
        provenance: "mined".into(),
        match_type: MatchType::Arxiv,
        raw_match: "arXiv:2403.12345".into(),
        ref_json: "{}".into(),
    };
    for i in 0..5 {
        writer.write(legacy_row(i, "10.48550/arxiv.2403.12345"))?;
    }
    writer.write(legacy_row(5, "10.1234/plain"))?;
    writer.write(legacy_row(6, "hep-ph/9901234"))?;
    writer.flush_all()?;

    // Prefix keys, not hashes.
    let arxiv = tmp.path().join("2403");
    assert!(arxiv.is_dir());
    assert!(tmp.path().join("10.1234").is_dir());
    assert!(tmp.path().join("hep-").is_dir());
    assert!(part_files(tmp.path())?.is_empty());

    // Row threshold of 2 still cuts 5 rows into 2 + 2 + 1.
    let segments = committed_segments(&arxiv)?;
    assert_eq!(segments.len(), 3);
    let counts: Vec<usize> = segments
        .iter()
        .map(|s| read_segment(s, PartitionMode::Legacy).map(|rows| rows.len()))
        .collect::<Result<_>>()?;
    assert_eq!(counts, vec![2, 2, 1]);

    // New legacy segments carry the evidence column explicitly.
    let df = LazyFrame::scan_parquet(&segments[0], Default::default())?.collect()?;
    let names: Vec<&str> = df.get_column_names_str();
    assert!(names.contains(&"match_type"), "columns: {names:?}");
    assert!(names.contains(&"ref_index"), "columns: {names:?}");
    let types = df.column("match_type")?.str()?;
    assert_eq!(types.get(0), Some("arxiv"));
    assert_eq!(writer.total_rows_written(), 7);
    assert_eq!(writer.partition_count(), 3);
    Ok(())
}

#[test]
fn rejects_invalid_limits() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bad_count = SegmentedPartitionWriter::with_limits(
        tmp.path(),
        WriterLimits {
            memory_bytes: 1 << 20,
            segment_rows: 10,
            mode: PartitionMode::Hashed { count: 3 },
        },
    );
    assert!(bad_count.is_err());
    assert!(hashed_partition_key("10.1234/a", 0).is_err());

    let no_budget = SegmentedPartitionWriter::with_limits(
        tmp.path(),
        WriterLimits {
            memory_bytes: 0,
            segment_rows: 10,
            mode: PartitionMode::Legacy,
        },
    );
    assert!(no_budget.is_err());
    Ok(())
}
