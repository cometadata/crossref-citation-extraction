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

/// Create a test tar.gz with provenance test data
fn create_provenance_test_tar_gz(dir: &std::path::Path) -> std::path::PathBuf {
    let tar_path = dir.join("provenance_test.tar.gz");
    let file = File::create(&tar_path).unwrap();
    let encoder = GzEncoder::new(file, Compression::default());
    let mut builder = Builder::new(encoder);

    // Create JSON with references having different provenances:
    // - DOI field + "doi-asserted-by": "publisher" -> Publisher provenance
    // - DOI field + "doi-asserted-by": "crossref" -> Crossref provenance
    // - Only "unstructured" field with DOI in text -> Mined provenance
    //
    // Important: The cited DOIs must also exist as items in the Crossref data
    // so they are added to the Crossref index and pass validation.
    let json_content = r#"{
        "items": [
            {
                "DOI": "10.1234/citing-paper-1",
                "reference": [
                    {"DOI": "10.5678/publisher-asserted", "doi-asserted-by": "publisher"},
                    {"DOI": "10.5678/crossref-asserted", "doi-asserted-by": "crossref"},
                    {"unstructured": "See doi:10.5678/mined-from-text for details"}
                ]
            },
            {
                "DOI": "10.1234/citing-paper-2",
                "reference": [
                    {"DOI": "10.5678/publisher-asserted", "doi-asserted-by": "publisher"},
                    {"unstructured": "Reference to 10.5678/another-mined-doi in the text"}
                ]
            },
            {
                "DOI": "10.5678/publisher-asserted",
                "title": "Publisher Asserted Paper"
            },
            {
                "DOI": "10.5678/crossref-asserted",
                "title": "Crossref Asserted Paper"
            },
            {
                "DOI": "10.5678/mined-from-text",
                "title": "Mined From Text Paper"
            },
            {
                "DOI": "10.5678/another-mined-doi",
                "title": "Another Mined Paper"
            }
        ]
    }"#;

    let json_bytes = json_content.as_bytes();
    let mut header = tar::Header::new_gnu();
    header.set_path("test/provenance_test.json").unwrap();
    header.set_size(json_bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();

    builder.append(&header, json_bytes).unwrap();
    builder.into_inner().unwrap().finish().unwrap();

    tar_path
}

#[test]
fn test_provenance_end_to_end() {
    let dir = tempdir().unwrap();
    let tar_path = create_provenance_test_tar_gz(dir.path());
    let output_path = dir.path().join("output.jsonl");

    // Run pipeline in crossref mode (which extracts all Crossref DOIs)
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

    // Verify main output file exists
    assert!(output_path.exists(), "Main output file should exist");

    // Verify split output files exist
    let asserted_path = dir.path().join("output_asserted.jsonl");
    let mined_path = dir.path().join("output_mined.jsonl");

    assert!(
        asserted_path.exists(),
        "Asserted output file should exist at {:?}",
        asserted_path
    );
    assert!(
        mined_path.exists(),
        "Mined output file should exist at {:?}",
        mined_path
    );

    // Read and verify main output file
    let main_file = File::open(&output_path).unwrap();
    let main_reader = BufReader::new(main_file);
    let mut main_records: Vec<serde_json::Value> = Vec::new();

    for line in main_reader.lines() {
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

        // Verify cited_by entries have provenance field
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
        main_records.push(record);
    }

    assert!(!main_records.is_empty(), "Main output should have records");

    // Read and verify asserted output file (should only have publisher/crossref provenance)
    let asserted_file = File::open(&asserted_path).unwrap();
    let asserted_reader = BufReader::new(asserted_file);
    let mut asserted_count = 0;

    for line in asserted_reader.lines() {
        let line = line.unwrap();
        let record: serde_json::Value = serde_json::from_str(&line).unwrap();

        let cited_by = record.get("cited_by").unwrap().as_array().unwrap();
        for citing in cited_by {
            let provenance = citing.get("provenance").unwrap().as_str().unwrap();
            assert!(
                provenance == "publisher" || provenance == "crossref",
                "Asserted file should only contain publisher or crossref provenance, found: {}",
                provenance
            );
        }
        asserted_count += 1;
    }

    assert!(asserted_count > 0, "Asserted output should have records");

    // Read and verify mined output file (should only have mined provenance)
    let mined_file = File::open(&mined_path).unwrap();
    let mined_reader = BufReader::new(mined_file);
    let mut mined_count = 0;

    for line in mined_reader.lines() {
        let line = line.unwrap();
        let record: serde_json::Value = serde_json::from_str(&line).unwrap();

        let cited_by = record.get("cited_by").unwrap().as_array().unwrap();
        for citing in cited_by {
            let provenance = citing.get("provenance").unwrap().as_str().unwrap();
            assert!(
                provenance == "mined",
                "Mined file should only contain mined provenance, found: {}",
                provenance
            );
        }
        mined_count += 1;
    }

    assert!(mined_count > 0, "Mined output should have records");

    // Verify we have the expected DOIs in the output
    let main_dois: Vec<String> = main_records
        .iter()
        .map(|r| r.get("doi").unwrap().as_str().unwrap().to_string())
        .collect();

    // We should see the cited DOIs (not the citing papers)
    // - 10.5678/publisher-asserted (cited by two papers with publisher provenance)
    // - 10.5678/crossref-asserted (cited by one paper with crossref provenance)
    // - 10.5678/mined-from-text (mined from unstructured)
    // - 10.5678/another-mined-doi (mined from unstructured)

    // The DOIs are normalized to lowercase
    assert!(
        main_dois.contains(&"10.5678/publisher-asserted".to_string()),
        "Should contain publisher-asserted DOI"
    );
    assert!(
        main_dois.contains(&"10.5678/crossref-asserted".to_string()),
        "Should contain crossref-asserted DOI"
    );
    assert!(
        main_dois.contains(&"10.5678/mined-from-text".to_string()),
        "Should contain mined-from-text DOI"
    );
    assert!(
        main_dois.contains(&"10.5678/another-mined-doi".to_string()),
        "Should contain another-mined-doi DOI"
    );
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
