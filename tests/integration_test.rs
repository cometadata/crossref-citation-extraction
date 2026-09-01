use crossref_citation_extraction::index::FstIndexBuilder;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::process::Command;
use tempfile::tempdir;

#[test]
fn test_pipeline_help() {
    let status = Command::new("cargo")
        .args(["run", "--", "pipeline", "--help"])
        .status()
        .expect("Failed to run pipeline --help");

    assert!(status.success(), "Pipeline --help should succeed");
}

#[test]
fn test_build_index_help() {
    let status = Command::new("cargo")
        .args(["run", "--", "build-index", "--help"])
        .status()
        .expect("Failed to run build-index --help");

    assert!(status.success(), "build-index --help should succeed");
}

#[test]
fn test_arxiv_extraction_end_to_end() {
    let dir = tempdir().unwrap();

    let crossref_dir = dir.path().join("crossref");
    fs::create_dir_all(&crossref_dir).unwrap();

    let crossref_file = crossref_dir.join("test.json");
    let crossref_data = serde_json::json!({
        "items": [
            {
                "DOI": "10.1111/citing1",
                "reference": [
                    {"unstructured": "See arXiv:2403.12345 for details"},
                    {"DOI": "10.48550/arXiv.2403.67890", "doi-asserted-by": "publisher"}
                ]
            }
        ]
    });
    fs::write(
        &crossref_file,
        serde_json::to_string_pretty(&crossref_data).unwrap(),
    )
    .unwrap();

    let fst_path = dir.path().join("arxiv.fst");
    {
        let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
        builder.insert("10.48550/arxiv.2403.12345").unwrap();
        builder.insert("10.48550/arxiv.2403.67890").unwrap();
        builder.finish().unwrap();
    }

    let output_path = dir.path().join("crossref_valid.jsonl");
    let partition_dir = dir.path().join("partitions");

    let status = Command::new(env!("CARGO_BIN_EXE_crossref-citation-extraction"))
        .args([
            "pipeline",
            "--input",
            crossref_dir.to_str().unwrap(),
            "--outputs",
            "valid",
            "--output-dir",
            dir.path().to_str().unwrap(),
            "--arxiv-fst",
            fst_path.to_str().unwrap(),
            "--temp-dir",
            partition_dir.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to execute command");

    assert!(status.success(), "Pipeline failed");
    assert!(output_path.exists(), "Output file should exist");

    let output_content = fs::read_to_string(&output_path).unwrap();
    assert!(
        output_content.contains("10.48550/arXiv.2403.12345")
            || output_content.contains("10.48550/arxiv.2403.12345"),
        "Output should contain validated arXiv DOI"
    );
}

#[test]
fn test_arxiv_extraction_with_provenance() {
    let dir = tempdir().unwrap();

    let crossref_dir = dir.path().join("crossref");
    fs::create_dir_all(&crossref_dir).unwrap();

    let crossref_file = crossref_dir.join("test.json");
    let crossref_data = serde_json::json!({
        "items": [
            {
                "DOI": "10.1111/citing1",
                "reference": [
                    {"DOI": "10.48550/arXiv.2403.11111", "doi-asserted-by": "publisher"},
                    {"DOI": "10.48550/arXiv.2403.22222", "doi-asserted-by": "crossref"},
                    {"unstructured": "See arXiv:2403.33333 for details"}
                ]
            },
            {
                "DOI": "10.1111/citing2",
                "reference": [
                    {"DOI": "10.48550/arXiv.2403.11111", "doi-asserted-by": "publisher"}
                ]
            }
        ]
    });
    fs::write(
        &crossref_file,
        serde_json::to_string_pretty(&crossref_data).unwrap(),
    )
    .unwrap();

    let fst_path = dir.path().join("arxiv.fst");
    {
        let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
        builder.insert("10.48550/arxiv.2403.11111").unwrap();
        builder.insert("10.48550/arxiv.2403.22222").unwrap();
        builder.insert("10.48550/arxiv.2403.33333").unwrap();
        builder.finish().unwrap();
    }

    let output_path = dir.path().join("crossref_valid.jsonl");
    let partition_dir = dir.path().join("partitions");

    let status = Command::new(env!("CARGO_BIN_EXE_crossref-citation-extraction"))
        .args([
            "pipeline",
            "--input",
            crossref_dir.to_str().unwrap(),
            "--outputs",
            "valid",
            "--output-dir",
            dir.path().to_str().unwrap(),
            "--arxiv-fst",
            fst_path.to_str().unwrap(),
            "--temp-dir",
            partition_dir.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to execute command");

    assert!(status.success(), "Pipeline should succeed");
    assert!(output_path.exists(), "Output file should exist");

    let file = File::open(&output_path).unwrap();
    let reader = BufReader::new(file);
    let mut records: Vec<serde_json::Value> = Vec::new();

    for line in reader.lines() {
        let line = line.unwrap();
        let record: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert!(record.get("doi").is_some(), "Should have doi field");
        assert!(
            record.get("citation_count").is_some(),
            "Should have citation_count field"
        );
        assert!(
            record.get("cited_by").is_some(),
            "Should have cited_by field"
        );

        let cited_by = record.get("cited_by").unwrap().as_array().unwrap();
        for citing in cited_by {
            assert!(
                citing.get("provenance").is_some(),
                "cited_by entry should have provenance field"
            );
            let provenance = citing.get("provenance").unwrap().as_str().unwrap();
            assert!(
                ["publisher", "crossref", "mined"].contains(&provenance),
                "Provenance should be one of publisher, crossref, or mined"
            );
        }
        records.push(record);
    }

    assert!(!records.is_empty(), "Should have output records");

    let mut publisher_count = 0;
    let mut crossref_count = 0;
    let mut mined_count = 0;

    for record in &records {
        let cited_by = record.get("cited_by").unwrap().as_array().unwrap();
        for citing in cited_by {
            let provenance = citing.get("provenance").unwrap().as_str().unwrap();
            match provenance {
                "publisher" => publisher_count += 1,
                "crossref" => crossref_count += 1,
                "mined" => mined_count += 1,
                _ => panic!("Unexpected provenance: {}", provenance),
            }
        }
    }

    assert!(
        publisher_count > 0,
        "Should have publisher provenance citations"
    );
    assert!(
        crossref_count > 0,
        "Should have crossref provenance citations"
    );
    assert!(mined_count > 0, "Should have mined provenance citations");
}

#[test]
fn test_arxiv_extraction_with_directory_input() {
    let dir = tempdir().unwrap();

    let crossref_dir = dir.path().join("crossref_dir");
    fs::create_dir_all(&crossref_dir).unwrap();

    let crossref_file = crossref_dir.join("0.json");
    let crossref_data = serde_json::json!({
        "items": [
            {
                "DOI": "10.1111/citing1",
                "reference": [
                    {"DOI": "10.48550/arXiv.2403.99999", "doi-asserted-by": "publisher"}
                ]
            }
        ]
    });
    fs::write(
        &crossref_file,
        serde_json::to_string_pretty(&crossref_data).unwrap(),
    )
    .unwrap();

    let fst_path = dir.path().join("arxiv.fst");
    {
        let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
        builder.insert("10.48550/arxiv.2403.99999").unwrap();
        builder.finish().unwrap();
    }

    let output_path = dir.path().join("crossref_dir_valid.jsonl");
    let partition_dir = dir.path().join("partitions");

    let status = Command::new(env!("CARGO_BIN_EXE_crossref-citation-extraction"))
        .args([
            "pipeline",
            "--input",
            crossref_dir.to_str().unwrap(),
            "--outputs",
            "valid",
            "--output-dir",
            dir.path().to_str().unwrap(),
            "--arxiv-fst",
            fst_path.to_str().unwrap(),
            "--temp-dir",
            partition_dir.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run pipeline with directory input");

    assert!(
        status.success(),
        "Pipeline with directory input should succeed"
    );
    assert!(output_path.exists(), "Output file should exist");

    let file = File::open(&output_path).unwrap();
    let reader = BufReader::new(file);
    let lines: Vec<_> = reader.lines().collect();
    assert!(!lines.is_empty(), "Should have output records");
}

#[test]
fn test_batched_processing_for_large_partitions() {
    use crossref_citation_extraction::common::OutputPaths;
    use crossref_citation_extraction::index::FstIndex;
    use crossref_citation_extraction::streaming::{PartitionRow, SegmentedPartitionWriter};

    let dir = tempdir().unwrap();
    let partition_dir = dir.path().join("partitions");
    let valid_output = dir.path().join("valid.jsonl");
    let failed_output = dir.path().join("failed.jsonl");

    {
        let mut partition_writer = SegmentedPartitionWriter::new(&partition_dir, 2).unwrap();

        for i in 0..250 {
            partition_writer
                .write(PartitionRow {
                    citing_doi: format!("10.1111/citing{}", i),
                    ref_index: 0,
                    cited_id: "10.48550/arxiv.2403.12345".to_string(),
                    provenance: if i % 3 == 0 {
                        "publisher"
                    } else if i % 3 == 1 {
                        "crossref"
                    } else {
                        "mined"
                    }
                    .to_string(),
                    raw_match: "arXiv:2403.12345".to_string(),
                    ref_json: format!(r#"{{"unstructured": "ref {}"}}"#, i),
                })
                .unwrap();
        }

        partition_writer.flush_all().unwrap();
    }

    let partition_path = partition_dir.join("2403");
    let segment_count = std::fs::read_dir(&partition_path)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .ok()
                .map(|e| {
                    e.path()
                        .extension()
                        .map(|e| e == "parquet")
                        .unwrap_or(false)
                })
                .unwrap_or(false)
        })
        .count();

    assert!(
        segment_count > 100,
        "Should have > 100 segments to trigger batched processing, got {}",
        segment_count
    );

    let fst_path = dir.path().join("test.fst");
    {
        let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
        builder.insert("10.48550/arxiv.2403.12345").unwrap();
        builder.finish().unwrap();
    }
    let index = FstIndex::load(&fst_path).unwrap();

    let outputs = OutputPaths {
        valid: Some(valid_output.clone()),
        failed: Some(failed_output.clone()),
        publisher: None,
        crossref: None,
        mined: None,
    };

    let stats = crossref_citation_extraction::streaming::aggregate_and_validate(
        &partition_dir,
        Some(&index),
        &outputs,
        false, // resume
        0,     // checkpoint_interval (0 = no checkpointing)
    )
    .unwrap();

    assert_eq!(stats.valid_count, 1, "Should have 1 valid cited work");
    assert_eq!(stats.failed_count, 0, "Should have 0 failed works");
    assert_eq!(
        stats.total_citations, 250,
        "Should have 250 citations (one per row)"
    );
    assert_eq!(
        stats.partitions_processed, 1,
        "Should have processed 1 partition"
    );

    let valid_content = std::fs::read_to_string(&valid_output).unwrap();
    assert!(
        valid_content.contains("10.48550/arxiv.2403.12345"),
        "Output should contain cited DOI"
    );
    assert!(
        valid_content.contains(r#""citation_count":250"#),
        "Should have 250 citations"
    );
}

#[test]
fn test_intermediates_removed_unless_kept() {
    let dir = tempdir().unwrap();
    let crossref_dir = dir.path().join("crossref_dir");
    fs::create_dir_all(&crossref_dir).unwrap();
    fs::write(
        crossref_dir.join("0.json"),
        serde_json::to_string(&serde_json::json!({
            "items": [{
                "DOI": "10.1111/citing1",
                "reference": [{"DOI": "10.48550/arXiv.2403.99999", "doi-asserted-by": "publisher"}]
            }]
        }))
        .unwrap(),
    )
    .unwrap();

    let fst_path = dir.path().join("arxiv.fst");
    {
        let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
        builder.insert("10.48550/arxiv.2403.99999").unwrap();
        builder.finish().unwrap();
    }

    // Run 1: no flag -> partition dir removed after success.
    let partitions_a = dir.path().join("partitions_a");
    let status = Command::new(env!("CARGO_BIN_EXE_crossref-citation-extraction"))
        .args([
            "pipeline",
            "--input",
            crossref_dir.to_str().unwrap(),
            "--outputs",
            "valid",
            "--output-dir",
            dir.path().to_str().unwrap(),
            "--arxiv-fst",
            fst_path.to_str().unwrap(),
            "--temp-dir",
            partitions_a.to_str().unwrap(),
        ])
        .status()
        .unwrap();
    assert!(status.success());
    assert!(
        !partitions_a.exists(),
        "intermediates should be removed by default"
    );

    // Run 2: --keep-intermediates -> partition dir kept.
    let partitions_b = dir.path().join("partitions_b");
    let status = Command::new(env!("CARGO_BIN_EXE_crossref-citation-extraction"))
        .args([
            "pipeline",
            "--input",
            crossref_dir.to_str().unwrap(),
            "--outputs",
            "valid",
            "--output-dir",
            dir.path().to_str().unwrap(),
            "--arxiv-fst",
            fst_path.to_str().unwrap(),
            "--temp-dir",
            partitions_b.to_str().unwrap(),
            "--keep-intermediates",
        ])
        .status()
        .unwrap();
    assert!(status.success());
    assert!(
        partitions_b.exists(),
        "--keep-intermediates should preserve them"
    );
}
