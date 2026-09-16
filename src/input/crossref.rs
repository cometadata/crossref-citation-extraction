//! Crossref input source implementations.

use anyhow::{ensure, Context, Result};
use flate2::read::GzDecoder;
use log::info;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use tar::Archive;

use super::CrossrefInput;

/// A single entry from a Crossref source (filename + raw JSON bytes).
pub struct CrossrefEntry {
    pub filename: String,
    pub bytes: Vec<u8>,
}

/// Visit every JSON entry in a Crossref input, in source order.
/// The closure receives filename + raw bytes; parse failures are the
/// caller's concern (it has the bytes), I/O failures abort the visit.
pub fn visit_crossref_entries(
    input: CrossrefInput,
    f: impl FnMut(CrossrefEntry) -> Result<()>,
) -> Result<()> {
    visit_crossref_entries_limited(input, usize::MAX, f)
}

/// [`visit_crossref_entries`] with a cap on the bytes one member may
/// occupy in memory. A member above `max_entry_bytes` (by its declared
/// size, or by what it actually holds when the declaration lies) aborts
/// the visit with an error before the caller sees it, as does any member
/// that cannot be read.
pub fn visit_crossref_entries_limited(
    input: CrossrefInput,
    max_entry_bytes: usize,
    mut f: impl FnMut(CrossrefEntry) -> Result<()>,
) -> Result<()> {
    match input {
        CrossrefInput::TarGz(path) => {
            info!(
                "Detected Crossref input: tar.gz archive at {}",
                path.display()
            );
            let file =
                File::open(&path).with_context(|| format!("Failed to open: {}", path.display()))?;
            let mut archive = Archive::new(GzDecoder::new(file));
            let entries = archive
                .entries()
                .with_context(|| format!("Failed to read archive {}", path.display()))?;
            for entry in entries {
                let mut entry =
                    entry.with_context(|| format!("Failed to read archive {}", path.display()))?;
                let member_path = entry
                    .path()
                    .with_context(|| format!("Unreadable member path in {}", path.display()))?
                    .to_path_buf();
                let path_str = member_path.to_string_lossy();
                if !path_str.ends_with(".json") {
                    continue;
                }
                let filename = path_str.to_string();
                check_member_size(&filename, entry.size(), max_entry_bytes)?;
                // The size comes from the tar header, so it is only a hint: a
                // corrupt one could otherwise ask for a huge allocation up
                // front. read_to_end grows past the hint when the entry is
                // genuinely larger.
                const MAX_PREALLOC: usize = 16 * 1024 * 1024;
                let mut bytes = Vec::with_capacity((entry.size() as usize).min(MAX_PREALLOC));
                read_member(&mut entry, &filename, max_entry_bytes, &mut bytes)?;
                f(CrossrefEntry { filename, bytes })?;
            }
            Ok(())
        }
        CrossrefInput::Directory(path) => {
            let mut files: Vec<PathBuf> = std::fs::read_dir(&path)
                .with_context(|| format!("Failed to read directory: {}", path.display()))?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().map(|e| e == "json").unwrap_or(false))
                .collect();
            files.sort_by(|a, b| {
                let num = |p: &PathBuf| {
                    p.file_stem()
                        .and_then(|s| s.to_str())
                        .and_then(|s| s.parse::<u64>().ok())
                };
                match (num(a), num(b)) {
                    (Some(a), Some(b)) => a.cmp(&b),
                    _ => a.cmp(b),
                }
            });
            info!(
                "Detected Crossref input: directory with {} JSON files",
                files.len()
            );
            for path in files {
                let filename = path
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default();
                let bytes = read_member_file(&path, &filename, max_entry_bytes)?;
                f(CrossrefEntry { filename, bytes })?;
            }
            Ok(())
        }
        CrossrefInput::SingleJson(path) => {
            info!(
                "Detected Crossref input: single JSON file at {}",
                path.display()
            );
            let filename = path
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();
            let bytes = read_member_file(&path, &filename, max_entry_bytes)?;
            f(CrossrefEntry { filename, bytes })
        }
    }
}

/// Reject a member whose declared size is above the limit before any of it
/// is read.
fn check_member_size(filename: &str, declared: u64, max_entry_bytes: usize) -> Result<()> {
    ensure!(
        u64::try_from(max_entry_bytes).is_ok_and(|max| declared <= max),
        "member {filename} is {declared} bytes, above the {max_entry_bytes}-byte member limit"
    );
    Ok(())
}

/// Read a member into `bytes`, failing if it holds more than the limit
/// regardless of what its header declared.
fn read_member(
    source: &mut impl Read,
    filename: &str,
    max_entry_bytes: usize,
    bytes: &mut Vec<u8>,
) -> Result<()> {
    let cap = u64::try_from(max_entry_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    source
        .take(cap)
        .read_to_end(bytes)
        .with_context(|| format!("Failed to read member {filename}"))?;
    ensure!(
        bytes.len() <= max_entry_bytes,
        "member {filename} holds more than the {max_entry_bytes}-byte member limit"
    );
    Ok(())
}

/// Read one on-disk member under the same limit as an archive member.
fn read_member_file(path: &Path, filename: &str, max_entry_bytes: usize) -> Result<Vec<u8>> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("Failed to read member {}", path.display()))?;
    check_member_size(filename, metadata.len(), max_entry_bytes)?;
    let mut file =
        File::open(path).with_context(|| format!("Failed to read member {}", path.display()))?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    read_member(&mut file, filename, max_entry_bytes, &mut bytes)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::fs::File;
    use tar::Builder;
    use tempfile::tempdir;

    fn create_test_tar_gz(dir: &std::path::Path) -> std::path::PathBuf {
        let tar_path = dir.join("test.tar.gz");
        let file = File::create(&tar_path).unwrap();
        let encoder = GzEncoder::new(file, Compression::default());
        let mut builder = Builder::new(encoder);

        let json_content = r#"{"items": [{"DOI": "10.1234/test"}]}"#;
        let json_bytes = json_content.as_bytes();
        let mut header = tar::Header::new_gnu();
        header.set_path("0.json").unwrap();
        header.set_size(json_bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append(&header, json_bytes).unwrap();

        builder.into_inner().unwrap().finish().unwrap();
        tar_path
    }

    fn collect_entries(input: CrossrefInput) -> Vec<CrossrefEntry> {
        let mut entries = Vec::new();
        visit_crossref_entries(input, |entry| {
            entries.push(entry);
            Ok(())
        })
        .unwrap();
        entries
    }

    #[test]
    fn test_tar_gz_source() {
        let dir = tempdir().unwrap();
        let tar_path = create_test_tar_gz(dir.path());

        let entries = collect_entries(CrossrefInput::TarGz(tar_path));

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].filename, "0.json");
        let v: serde_json::Value = serde_json::from_slice(&entries[0].bytes).unwrap();
        assert!(v.get("items").is_some());
    }

    #[test]
    fn test_directory_source() {
        let dir = tempdir().unwrap();
        let json_dir = dir.path().join("crossref");
        std::fs::create_dir(&json_dir).unwrap();

        std::fs::write(
            json_dir.join("0.json"),
            r#"{"items": [{"DOI": "10.1234/first"}]}"#,
        )
        .unwrap();
        std::fs::write(
            json_dir.join("1.json"),
            r#"{"items": [{"DOI": "10.1234/second"}]}"#,
        )
        .unwrap();

        let entries = collect_entries(CrossrefInput::Directory(json_dir));

        assert_eq!(entries.len(), 2);
        // Should be sorted by filename
        assert_eq!(entries[0].filename, "0.json");
        assert_eq!(entries[1].filename, "1.json");
        let v: serde_json::Value = serde_json::from_slice(&entries[0].bytes).unwrap();
        assert!(v.get("items").is_some());
    }

    #[test]
    fn test_single_json_source() {
        let dir = tempdir().unwrap();
        let json_path = dir.path().join("test.json");
        std::fs::write(&json_path, r#"{"items": [{"DOI": "10.1234/single"}]}"#).unwrap();

        let entries = collect_entries(CrossrefInput::SingleJson(json_path));

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].filename, "test.json");
        let v: serde_json::Value = serde_json::from_slice(&entries[0].bytes).unwrap();
        assert!(v.get("items").is_some());
    }

    #[test]
    fn test_directory_numeric_sort() {
        let dir = tempdir().unwrap();
        let json_dir = dir.path().join("crossref");
        std::fs::create_dir(&json_dir).unwrap();

        // Create files that would sort wrong lexically: 1, 10, 2
        for i in [1, 2, 10] {
            std::fs::write(
                json_dir.join(format!("{}.json", i)),
                format!(r#"{{"items": [{{"DOI": "10.1234/{}"}}]}}"#, i),
            )
            .unwrap();
        }

        let filenames: Vec<String> = collect_entries(CrossrefInput::Directory(json_dir))
            .into_iter()
            .map(|e| e.filename)
            .collect();

        // Should be numerically sorted: 1, 2, 10
        assert_eq!(filenames, vec!["1.json", "2.json", "10.json"]);
    }

    #[test]
    fn test_member_limit_rejects_oversized_members_before_the_visitor() {
        let dir = tempdir().unwrap();
        let tar_path = create_test_tar_gz(dir.path());
        let member_len = r#"{"items": [{"DOI": "10.1234/test"}]}"#.len();

        let mut seen = 0usize;
        let err = visit_crossref_entries_limited(
            CrossrefInput::TarGz(tar_path.clone()),
            member_len - 1,
            |_entry| {
                seen += 1;
                Ok(())
            },
        )
        .unwrap_err();
        assert_eq!(seen, 0, "an oversized member never reaches the visitor");
        assert!(err.to_string().contains("member limit"), "{err}");

        // Exactly at the limit is accepted, for archives and plain files.
        let entries = {
            let mut entries = Vec::new();
            visit_crossref_entries_limited(CrossrefInput::TarGz(tar_path), member_len, |e| {
                entries.push(e);
                Ok(())
            })
            .unwrap();
            entries
        };
        assert_eq!(entries.len(), 1);

        let json_path = dir.path().join("single.json");
        std::fs::write(&json_path, r#"{"items": []}"#).unwrap();
        let err =
            visit_crossref_entries_limited(CrossrefInput::SingleJson(json_path), 3, |_| Ok(()))
                .unwrap_err();
        assert!(err.to_string().contains("member limit"), "{err}");
    }

    #[test]
    fn test_visitor_error_aborts_visit() {
        let dir = tempdir().unwrap();
        let json_dir = dir.path().join("crossref");
        std::fs::create_dir(&json_dir).unwrap();
        for i in [0, 1, 2] {
            std::fs::write(json_dir.join(format!("{}.json", i)), "{}").unwrap();
        }

        let mut seen = 0usize;
        let err = visit_crossref_entries(CrossrefInput::Directory(json_dir), |_entry| {
            seen += 1;
            Err(anyhow::anyhow!("consumer hung up"))
        })
        .unwrap_err();

        assert_eq!(seen, 1, "visit stops at the first closure error");
        assert!(err.to_string().contains("consumer hung up"));
    }
}
