use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::process::Command;
use tar::Builder;
use tempfile::tempdir;

/// Create a minimal Crossref tar.gz for testing
fn create_test_crossref_tar_gz(dir: &std::path::Path) -> std::path::PathBuf {
    let tar_path = dir.join("test_crossref.tar.gz");
    let file = File::create(&tar_path).unwrap();
    let encoder = GzEncoder::new(file, Compression::default());
    let mut builder = Builder::new(encoder);

    // Create a JSON file with test data
    // Note: 10.1234/citing-paper cites 10.1234/other-paper (cross-citation, valid)
    // and 10.1234/other-paper cites itself (self-citation, should be filtered)
    let json_content = r#"{
        "items": [
            {
                "DOI": "10.1234/citing-paper",
                "reference": [
                    {"unstructured": "See arXiv:2403.12345 for details"},
                    {"DOI": "10.5678/another-paper"},
                    {"unstructured": "Also 10.9999/datacite-doi"},
                    {"DOI": "10.1234/other-paper"}
                ]
            },
            {
                "DOI": "10.1234/other-paper",
                "reference": [
                    {"DOI": "10.1234/other-paper"}
                ]
            }
        ]
    }"#;

    let json_bytes = json_content.as_bytes();
    let mut header = tar::Header::new_gnu();
    header.set_path("test/file1.json").unwrap();
    header.set_size(json_bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();

    builder.append(&header, json_bytes).unwrap();
    builder.into_inner().unwrap().finish().unwrap();

    tar_path
}

/// Create a minimal DataCite records file
fn create_test_datacite_records(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("datacite.jsonl.gz");
    let file = File::create(&path).unwrap();
    let encoder = GzEncoder::new(file, Compression::default());
    let mut writer = std::io::BufWriter::new(encoder);

    writeln!(writer, r#"{{"id": "10.48550/arXiv.2403.12345"}}"#).unwrap();
    writeln!(writer, r#"{{"id": "10.9999/datacite-doi"}}"#).unwrap();

    writer.into_inner().unwrap().finish().unwrap();
    path
}

#[test]
fn test_pipeline_help() {
    let status = Command::new("cargo")
        .args(["run", "--", "pipeline", "--help"])
        .status()
        .expect("Failed to run pipeline --help");

    assert!(status.success(), "Pipeline --help should succeed");
}

#[test]
fn test_validate_help() {
    let status = Command::new("cargo")
        .args(["run", "--", "validate", "--help"])
        .status()
        .expect("Failed to run validate --help");

    assert!(status.success(), "Validate --help should succeed");
}

#[test]
fn test_crossref_mode_extraction() {
    let dir = tempdir().unwrap();
    let tar_path = create_test_crossref_tar_gz(dir.path());
    let output_path = dir.path().join("output.jsonl");

    let status = Command::new("cargo")
        .args([
            "run",
            "--",
            "pipeline",
            "--input",
            tar_path.to_str().unwrap(),
            "--source",
            "crossref",
            "--output-crossref",
            output_path.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run pipeline");

    assert!(status.success(), "Pipeline should succeed");
    assert!(output_path.exists(), "Output file should exist");

    // Verify output format
    let file = File::open(&output_path).unwrap();
    let reader = BufReader::new(file);
    let mut has_records = false;

    for line in reader.lines() {
        let line = line.unwrap();
        let record: serde_json::Value = serde_json::from_str(&line).unwrap();

        assert!(record.get("doi").is_some(), "Should have doi field");
        assert!(
            record.get("citation_count").is_some(),
            "Should have citation_count field"
        );
        has_records = true;
    }

    assert!(has_records, "Should have output records");
}

#[test]
fn test_index_save_and_load() {
    let dir = tempdir().unwrap();
    let datacite_path = create_test_datacite_records(dir.path());
    let index_path = dir.path().join("datacite_index.parquet");

    // First run: build and save index
    let tar_path = create_test_crossref_tar_gz(dir.path());
    let output1 = dir.path().join("output1.jsonl");

    let status = Command::new("cargo")
        .args([
            "run",
            "--",
            "pipeline",
            "--input",
            tar_path.to_str().unwrap(),
            "--datacite-records",
            datacite_path.to_str().unwrap(),
            "--source",
            "datacite",
            "--output-datacite",
            output1.to_str().unwrap(),
            "--save-datacite-index",
            index_path.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run pipeline");

    assert!(status.success());
    assert!(index_path.exists(), "Index file should be saved");

    // Second run: load index (no datacite-records)
    let tar_path2 = create_test_crossref_tar_gz(dir.path());
    let output2 = dir.path().join("output2.jsonl");

    let status = Command::new("cargo")
        .args([
            "run",
            "--",
            "pipeline",
            "--input",
            tar_path2.to_str().unwrap(),
            "--load-datacite-index",
            index_path.to_str().unwrap(),
            "--source",
            "datacite",
            "--output-datacite",
            output2.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run pipeline with loaded index");

    assert!(status.success());
}
