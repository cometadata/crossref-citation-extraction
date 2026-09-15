//! Shared fixtures for DataCite release integration tests.
//!
//! `make_release` writes the single-folder layout the task brief specifies
//! (`dois/updated_2026-07/` with one metadata part and one state report),
//! the real `MANIFEST.json` shape with base64 SHA-256 digests, and a
//! `Complete` `STATUS.json`. `write_release` is the lower-level builder for
//! tests that need hand-crafted bytes such as truncated or multi-member gzip.
#![allow(dead_code)]

use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Digest, Sha256};

/// Release month written by `make_release`.
pub const MONTH: &str = "2026-07";

/// Manifest path of the metadata part `index` for `month`.
pub fn part_path(month: &str, index: u32) -> String {
    format!("dois/updated_{month}/part_{index:04}.jsonl.gz")
}

/// Manifest path of the state report for `month`.
pub fn csv_path(month: &str) -> String {
    format!("dois/updated_{month}/{month}.csv.gz")
}

/// Gzip `bytes` as a single member; flate2 writes no timestamp, so the
/// output is deterministic.
pub fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(bytes)
        .expect("write to in-memory gzip encoder");
    encoder.finish().expect("finish in-memory gzip encoder")
}

/// Serialize metadata records as newline-delimited JSON.
pub fn metadata_jsonl(records: &[serde_json::Value]) -> Vec<u8> {
    let mut out = Vec::new();
    for record in records {
        serde_json::to_writer(&mut out, record).expect("serialize fixture record");
        out.push(b'\n');
    }
    out
}

/// Render a state report with the documented `doi,state,client_id,updated`
/// header and one row per `(doi, state)` pair.
pub fn state_csv(states: &[(&str, &str)]) -> Vec<u8> {
    let mut out = String::from("doi,state,client_id,updated\n");
    for (doi, state) in states {
        out.push_str(&format!(
            "{doi},{state},example.repo,2026-07-15T00:00:00.000Z\n"
        ));
    }
    out.into_bytes()
}

/// Base64 SHA-256 as DataCite manifests record it.
pub fn sha256_base64(bytes: &[u8]) -> String {
    STANDARD.encode(Sha256::digest(bytes))
}

/// Lowercase hex SHA-256 as the reader reports it.
pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Write `MANIFEST.json` listing `entries` (`filename`, `size`, `sha256`).
pub fn write_manifest_json(root: &Path, entries: &[serde_json::Value]) -> Result<()> {
    let path = root.join("MANIFEST.json");
    fs::write(&path, serde_json::to_vec_pretty(entries)?)
        .with_context(|| format!("write {}", path.display()))
}

/// Write `STATUS.json` for `month` with the given `status`.
pub fn write_status(root: &Path, month: &str, status: &str) -> Result<()> {
    let path = root.join("STATUS.json");
    let status = serde_json::json!({
        "month": month,
        "datetime": "2026-08-01T07:53:35.713625+00:00",
        "status": status,
    });
    fs::write(&path, serde_json::to_vec_pretty(&status)?)
        .with_context(|| format!("write {}", path.display()))
}

/// Write a release at `root`: each `(relative_path, bytes)` file, a
/// `MANIFEST.json` describing exactly those files, and a `Complete`
/// `STATUS.json` for `month`.
pub fn write_release(root: &Path, month: &str, files: &[(&str, Vec<u8>)]) -> Result<()> {
    let mut entries = Vec::with_capacity(files.len());
    for (relative, bytes) in files {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        fs::write(&path, bytes).with_context(|| format!("write {}", path.display()))?;
        entries.push(serde_json::json!({
            "filename": relative,
            "size": bytes.len(),
            "sha256": sha256_base64(bytes),
        }));
    }
    write_manifest_json(root, &entries)?;
    write_status(root, month, "Complete")
}

/// Write a complete single-month release under `dois/updated_2026-07/`
/// holding `metadata` as `part_0000.jsonl.gz` and `states` as
/// `2026-07.csv.gz`.
pub fn make_release(
    root: &Path,
    metadata: &[serde_json::Value],
    states: &[(&str, &str)],
) -> Result<()> {
    let part = part_path(MONTH, 0);
    let csv = csv_path(MONTH);
    write_release(
        root,
        MONTH,
        &[
            (part.as_str(), gzip(&metadata_jsonl(metadata))),
            (csv.as_str(), gzip(&state_csv(states))),
        ],
    )
}
