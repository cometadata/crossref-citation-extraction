//! Integration tests for the DataCite index bundle: building from a real
//! monthly release fixture, loading the memory-mapped bundle, and the
//! bounded URL lookup contract.

use crossref_citation_extraction::index::datacite::{DataciteIndex, TargetLookup};
use crossref_citation_extraction::index::datacite_builder::build_datacite_index;

mod support;

#[test]
fn preserves_all_dois_for_a_shared_url() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let source = tmp.path().join("source");
    support::make_release(
        &source,
        &[
            serde_json::json!({"id":"10.1234/a","attributes":{"url":"https://example.org/shared"}}),
            serde_json::json!({"id":"10.1234/b","attributes":{"url":"https://example.org/shared"}}),
        ],
        &[("10.1234/retired", "registered")],
    )?;
    let out = tmp.path().join("bundle");
    build_datacite_index(&source, &out, 64)?;
    let index = DataciteIndex::load(&out)?;
    assert!(index.contains_doi("10.1234/retired"));
    assert_eq!(
        index.url_dois("https://example.org/shared", 1024)?,
        vec!["10.1234/a", "10.1234/b"]
    );
    assert!(index.url_dois("https://example.org/shared", 1).is_err());
    Ok(())
}

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crossref_citation_extraction::common::identifiers::{normalize_landing_url, url_lookup_keys};
use crossref_citation_extraction::index::datacite::{
    IndexFile, BUNDLE_FILES, DOIS_FILE, HOSTS_FILE, MARKER_FILE, URLS_FILE, URL_TARGETS_FILE,
};
use crossref_citation_extraction::input::datacite_release::MonthlyRelease;
use serde_json::json;

/// Write a release under `<tmp>/source` and build it into `<tmp>/bundle`.
fn build_fixture(
    tmp: &Path,
    metadata: &[serde_json::Value],
    states: &[(&str, &str)],
) -> anyhow::Result<(PathBuf, PathBuf)> {
    let source = tmp.join("source");
    support::make_release(&source, metadata, states)?;
    let bundle = tmp.join("bundle");
    build_datacite_index(&source, &bundle, 64)?;
    Ok((source, bundle))
}

fn copy_bundle(from: &Path, to: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        fs::copy(entry.path(), to.join(entry.file_name()))?;
    }
    Ok(())
}

fn error_text(err: &anyhow::Error) -> String {
    format!("{err:#}")
}

fn read_marker(bundle: &Path) -> anyhow::Result<serde_json::Value> {
    Ok(serde_json::from_slice(&fs::read(
        bundle.join(MARKER_FILE),
    )?)?)
}

fn write_marker(bundle: &Path, marker: &serde_json::Value) -> anyhow::Result<()> {
    fs::write(bundle.join(MARKER_FILE), serde_json::to_vec(marker)?)?;
    Ok(())
}

#[test]
fn builds_a_bundle_through_the_cli() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let source = tmp.path().join("source");
    support::make_release(
        &source,
        &[json!({"id":"10.1234/a","attributes":{"url":"https://example.org/a"}})],
        &[("10.1234/b", "findable")],
    )?;
    let bundle = tmp.path().join("nested").join("bundle");
    let binary = env!("CARGO_BIN_EXE_crossref-citation-extraction");
    let run = |args: &[&str]| {
        Command::new(binary)
            .arg("build-datacite-index")
            .args(args)
            .output()
            .expect("run build-datacite-index")
    };
    let source_arg = source.to_str().unwrap();
    let bundle_arg = bundle.to_str().unwrap();

    let output = run(&[
        "--input",
        source_arg,
        "--output",
        bundle_arg,
        "--sort-memory-mb",
        "64",
    ]);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let index = DataciteIndex::load(&bundle)?;
    assert!(index.contains_doi("10.1234/a") && index.contains_doi("10.1234/b"));
    assert_eq!(
        index.url_dois("https://example.org/a", 4096)?,
        ["10.1234/a"]
    );

    // A completed bundle is never overwritten.
    let output = run(&["--input", source_arg, "--output", bundle_arg]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("already exists"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // The sort budget must be positive; the default is 4096 MiB.
    let other = tmp.path().join("other");
    let output = run(&[
        "--input",
        source_arg,
        "--output",
        other.to_str().unwrap(),
        "--sort-memory-mb",
        "0",
    ]);
    assert!(!output.status.success());
    assert!(!other.exists());
    let output = run(&["--help"]);
    assert!(output.status.success());
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(help.contains("4096"), "{help}");
    Ok(())
}

#[test]
fn manifest_lists_every_artifact_and_defines_the_bundle_digest() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let (source, bundle) = build_fixture(
        tmp.path(),
        &[
            json!({"id":"10.1234/a","attributes":{"url":"https://example.org/shared"}}),
            json!({"id":"10.1234/b","attributes":{"url":"https://example.org/shared"}}),
        ],
        &[("10.1234/retired", "registered")],
    )?;
    let manifest = build_datacite_index(&source, &tmp.path().join("again"), 64)?;
    let index = DataciteIndex::load(&bundle)?;
    assert_eq!(index.manifest(), &manifest);
    assert_eq!(index.directory(), bundle);

    assert_eq!(manifest.kind, "datacite-index");
    assert_eq!(manifest.format_version, 2);
    assert_eq!(manifest.normalization_version, 1);
    assert_eq!(manifest.release, support::MONTH);
    assert_eq!(
        manifest.source_inventory_sha256,
        MonthlyRelease::open(&source)?.inventory_sha256
    );

    let listed: BTreeSet<&str> = manifest.files.keys().map(String::as_str).collect();
    assert_eq!(listed, BUNDLE_FILES.into_iter().collect());
    for (name, meta) in &manifest.files {
        let bytes = fs::read(bundle.join(name))?;
        assert_eq!(meta.size, bytes.len() as u64, "{name}");
        assert_eq!(meta.sha256, support::sha256_hex(&bytes), "{name}");
        assert_eq!(meta, &IndexFile::of_file(&bundle.join(name))?, "{name}");
    }
    let on_disk: BTreeSet<String> = fs::read_dir(&bundle)?
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    let mut expected: BTreeSet<String> = BUNDLE_FILES.iter().map(|s| s.to_string()).collect();
    expected.insert(MARKER_FILE.to_string());
    assert_eq!(on_disk, expected);

    let counts: Vec<(&str, u64)> = manifest
        .counts
        .iter()
        .map(|(key, value)| (key.as_str(), *value))
        .collect();
    assert_eq!(
        counts,
        [
            ("csv_records", 1),
            ("dois", 3),
            ("hosts", 1),
            ("invalid_urls", 0),
            ("metadata_records", 2),
            ("missing_urls", 0),
            ("shared_urls", 1),
            ("url_pairs", 2),
            ("urls", 1),
        ]
    );

    let marker = fs::read(bundle.join(MARKER_FILE))?;
    assert_eq!(index.manifest_sha256(), support::sha256_hex(&marker));
    assert_eq!(manifest.to_marker_bytes()?, marker);
    Ok(())
}

#[test]
fn rejects_corruption_of_each_artifact() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let (_, bundle) = build_fixture(
        tmp.path(),
        &[json!({"id":"10.1234/a","attributes":{"url":"https://example.org/a"}})],
        &[],
    )?;
    for name in BUNDLE_FILES {
        let pristine = fs::read(bundle.join(name))?;

        let flipped = tmp.path().join(format!("flipped-{name}"));
        copy_bundle(&bundle, &flipped)?;
        let mut bytes = pristine.clone();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 0x01;
        fs::write(flipped.join(name), &bytes)?;
        let err = DataciteIndex::load(&flipped).unwrap_err();
        let text = error_text(&err);
        assert!(
            text.contains(name) && text.contains("SHA-256"),
            "{name}: {text}"
        );

        let truncated = tmp.path().join(format!("truncated-{name}"));
        copy_bundle(&bundle, &truncated)?;
        fs::write(truncated.join(name), &pristine[..pristine.len() - 1])?;
        let err = DataciteIndex::load(&truncated).unwrap_err();
        let text = error_text(&err);
        assert!(
            text.contains(name) && text.contains("bytes"),
            "{name}: {text}"
        );

        let missing = tmp.path().join(format!("missing-{name}"));
        copy_bundle(&bundle, &missing)?;
        fs::remove_file(missing.join(name))?;
        let err = DataciteIndex::load(&missing).unwrap_err();
        assert!(error_text(&err).contains(name), "{name}: {err:#}");
    }

    // Editing a listed digest inside the marker changes the bundle digest
    // and no longer matches the file.
    let edited = tmp.path().join("edited-marker");
    copy_bundle(&bundle, &edited)?;
    let mut marker = read_marker(&bundle)?;
    marker["files"][URL_TARGETS_FILE]["sha256"] = json!("0".repeat(64));
    write_marker(&edited, &marker)?;
    let err = DataciteIndex::load(&edited).unwrap_err();
    assert!(error_text(&err).contains(URL_TARGETS_FILE), "{err:#}");

    let err = DataciteIndex::load(&tmp.path().join("absent")).unwrap_err();
    assert!(error_text(&err).contains(MARKER_FILE), "{err:#}");
    Ok(())
}

#[test]
fn rejects_invalid_manifest_versions_and_kinds() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let (_, bundle) = build_fixture(tmp.path(), &[json!({"id":"10.1234/a"})], &[])?;
    let pristine = read_marker(&bundle)?;
    type Mutation = Box<dyn Fn(&mut serde_json::Value)>;
    let cases: Vec<(&str, Mutation, &str)> = vec![
        (
            "format",
            Box::new(|m| m["format_version"] = json!(1)),
            "format_version",
        ),
        (
            "normalization",
            Box::new(|m| m["normalization_version"] = json!(2)),
            "normalization_version",
        ),
        (
            "kind",
            Box::new(|m| m["kind"] = json!("arxiv-index")),
            "kind",
        ),
        (
            "unknown-field",
            Box::new(|m| m["extra"] = json!(true)),
            "unknown field",
        ),
        (
            "unlisted-artifact",
            Box::new(|m| {
                m["files"].as_object_mut().unwrap().remove(HOSTS_FILE);
            }),
            "does not list hosts.fst",
        ),
        (
            "traversing-name",
            Box::new(|m| {
                let files = m["files"].as_object_mut().unwrap();
                let entry = files[DOIS_FILE].clone();
                files.insert("../escape".to_string(), entry);
            }),
            "invalid bundle file name",
        ),
        (
            "self-listed",
            Box::new(|m| {
                let files = m["files"].as_object_mut().unwrap();
                let entry = files[DOIS_FILE].clone();
                files.insert(MARKER_FILE.to_string(), entry);
            }),
            "invalid bundle file name",
        ),
    ];
    for (label, mutate, expected) in cases {
        let dir = tmp.path().join(label);
        copy_bundle(&bundle, &dir)?;
        let mut marker = pristine.clone();
        mutate(&mut marker);
        write_marker(&dir, &marker)?;
        let err = DataciteIndex::load(&dir).unwrap_err();
        assert!(
            error_text(&err).contains(expected),
            "{label}: {}",
            error_text(&err)
        );
    }

    let malformed = tmp.path().join("malformed");
    copy_bundle(&bundle, &malformed)?;
    fs::write(malformed.join(MARKER_FILE), b"{not json")?;
    let err = DataciteIndex::load(&malformed).unwrap_err();
    assert!(error_text(&err).contains("malformed"), "{err:#}");
    Ok(())
}

#[test]
fn deduplicates_repeated_url_doi_pairs() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let (_, bundle) = build_fixture(
        tmp.path(),
        &[
            json!({"id":"10.1234/a","attributes":{"url":"https://example.org/x"}}),
            json!({"id":"10.1234/A","attributes":{"url":"https://example.org/x"}}),
            json!({"id":"10.1234/a","attributes":{"url":"HTTPS://EXAMPLE.ORG:443/x"}}),
        ],
        &[("10.1234/a", "findable")],
    )?;
    let index = DataciteIndex::load(&bundle)?;
    assert_eq!(
        index.url_dois("https://example.org/x", 4096)?,
        ["10.1234/a"]
    );
    let counts = &index.manifest().counts;
    assert_eq!(counts["metadata_records"], 3);
    assert_eq!(counts["csv_records"], 1);
    assert_eq!(counts["dois"], 1);
    assert_eq!(counts["urls"], 1);
    assert_eq!(counts["url_pairs"], 1);
    assert_eq!(counts["hosts"], 1);
    assert_eq!(counts["shared_urls"], 0);
    Ok(())
}

#[test]
fn keeps_http_and_https_keys_distinct_on_one_host() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let (_, bundle) = build_fixture(
        tmp.path(),
        &[
            json!({"id":"10.1234/a","attributes":{"url":"http://Example.org/p"}}),
            json!({"id":"10.1234/b","attributes":{"url":"https://example.org/p"}}),
            json!({"id":"10.1234/c","attributes":{"url":"https://example.org:8443/p"}}),
        ],
        &[],
    )?;
    let index = DataciteIndex::load(&bundle)?;
    assert_eq!(index.url_dois("http://example.org/p", 4096)?, ["10.1234/a"]);
    assert_eq!(
        index.url_dois("https://example.org/p", 4096)?,
        ["10.1234/b"]
    );
    assert_eq!(
        index.url_dois("https://example.org:8443/p", 4096)?,
        ["10.1234/c"]
    );
    // A scheme-less reference expands to both keys; the matcher unions
    // the lookups, so no scheme-less alias is stored.
    let mut union = Vec::new();
    for key in url_lookup_keys("example.org/p") {
        union.extend(index.url_dois(&key, 4096)?);
    }
    assert_eq!(union, ["10.1234/a", "10.1234/b"]);
    assert!(index.url_dois("example.org/p", 4096)?.is_empty());

    assert!(index.contains_host("example.org"));
    assert!(!index.contains_host("Example.org"));
    assert!(!index.contains_host("example.org:8443"));
    assert!(!index.contains_host("other.org"));
    let counts = &index.manifest().counts;
    assert_eq!(counts["hosts"], 1);
    assert_eq!(counts["urls"], 3);
    assert_eq!(counts["shared_urls"], 0);
    Ok(())
}

#[test]
fn counts_missing_and_invalid_urls_without_dropping_dois() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let (_, bundle) = build_fixture(
        tmp.path(),
        &[
            json!({"id":"10.1234/m1"}),
            json!({"id":"10.1234/m2","attributes":{"url":null}}),
            json!({"id":"10.1234/i1","attributes":{"url":42}}),
            json!({"id":"10.1234/i2","attributes":{"url":""}}),
            json!({"id":"10.1234/i3","attributes":{"url":"ftp://example.org/x"}}),
            json!({"id":"10.1234/i4","attributes":{"url":"not a url"}}),
            json!({"id":"10.1234/ok","attributes":{"url":"https://example.org/ok"}}),
        ],
        &[],
    )?;
    let index = DataciteIndex::load(&bundle)?;
    for doi in ["m1", "m2", "i1", "i2", "i3", "i4", "ok"] {
        assert!(index.contains_doi(&format!("10.1234/{doi}")), "{doi}");
    }
    assert!(!index.contains_doi("10.1234/absent"));
    assert_eq!(
        index.url_dois("https://example.org/ok", 4096)?,
        ["10.1234/ok"]
    );
    assert!(index.url_dois("https://example.org/none", 4096)?.is_empty());
    let counts = &index.manifest().counts;
    assert_eq!(counts["metadata_records"], 7);
    assert_eq!(counts["csv_records"], 0);
    assert_eq!(counts["dois"], 7);
    assert_eq!(counts["missing_urls"], 2);
    assert_eq!(counts["invalid_urls"], 4);
    assert_eq!(counts["urls"], 1);
    assert_eq!(counts["hosts"], 1);
    Ok(())
}

#[test]
fn fails_when_the_registry_has_no_dois() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let source = tmp.path().join("source");
    support::make_release(&source, &[], &[])?;
    let bundle = tmp.path().join("bundle");
    let err = build_datacite_index(&source, &bundle, 64).unwrap_err();
    assert!(error_text(&err).contains("no DOIs"), "{err:#}");
    assert!(!bundle.exists());
    assert!(build_datacite_index(&source, &bundle, 0).is_err());
    Ok(())
}

#[test]
fn repeated_builds_produce_identical_bytes() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let (source, first) = build_fixture(
        tmp.path(),
        &[
            json!({"id":"10.1234/b","attributes":{"url":"https://example.org/shared"}}),
            json!({"id":"10.1234/a","attributes":{"url":"https://example.org/shared"}}),
            json!({"id":"10.1234/c","attributes":{"url":"https://other.example/c"}}),
        ],
        &[("10.1234/d", "registered")],
    )?;
    let second = tmp.path().join("second");
    build_datacite_index(&source, &second, 64)?;
    for name in BUNDLE_FILES.iter().chain([&MARKER_FILE]) {
        assert_eq!(
            fs::read(first.join(name))?,
            fs::read(second.join(name))?,
            "{name}"
        );
    }
    assert_eq!(
        DataciteIndex::load(&first)?.manifest_sha256(),
        DataciteIndex::load(&second)?.manifest_sha256()
    );
    Ok(())
}

#[test]
fn leaves_nothing_behind_when_the_source_fails_verification() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let source = tmp.path().join("source");
    support::make_release(&source, &[json!({"id":"10.1234/a"})], &[])?;
    // Byte 4 is the gzip MTIME: every record still decodes, so only the
    // manifest digest check at the end of the scan can fail the build.
    let part = source.join(support::part_path(support::MONTH, 0));
    let mut bytes = fs::read(&part)?;
    bytes[4] ^= 0x01;
    fs::write(&part, &bytes)?;

    let bundle = tmp.path().join("bundle");
    let err = build_datacite_index(&source, &bundle, 64).unwrap_err();
    assert!(error_text(&err).contains("SHA-256"), "{err:#}");
    assert!(!bundle.exists());
    let leftovers: Vec<String> = fs::read_dir(tmp.path())?
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(leftovers, ["source"]);
    Ok(())
}

#[test]
fn refuses_to_overwrite_a_completed_bundle_or_a_nonempty_directory() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let (source, bundle) = build_fixture(tmp.path(), &[json!({"id":"10.1234/a"})], &[])?;
    let before = fs::read(bundle.join(MARKER_FILE))?;
    let err = build_datacite_index(&source, &bundle, 64).unwrap_err();
    assert!(error_text(&err).contains("already exists"), "{err:#}");
    assert_eq!(fs::read(bundle.join(MARKER_FILE))?, before);

    let occupied = tmp.path().join("occupied");
    fs::create_dir(&occupied)?;
    fs::write(occupied.join("note.txt"), b"keep")?;
    let err = build_datacite_index(&source, &occupied, 64).unwrap_err();
    assert!(error_text(&err).contains("not empty"), "{err:#}");
    assert_eq!(fs::read(occupied.join("note.txt"))?, b"keep");

    // An existing empty directory is acceptable.
    let empty = tmp.path().join("empty");
    fs::create_dir(&empty)?;
    build_datacite_index(&source, &empty, 64)?;
    assert!(DataciteIndex::load(&empty)?.contains_doi("10.1234/a"));
    Ok(())
}

#[test]
fn index_is_shareable_and_budgets_the_result_vector() -> anyhow::Result<()> {
    fn assert_shareable<T: Send + Sync>() {}
    assert_shareable::<DataciteIndex>();

    let tmp = tempfile::tempdir()?;
    let (_, bundle) = build_fixture(
        tmp.path(),
        &[
            json!({"id":"10.1234/a","attributes":{"url":"https://example.org/shared"}}),
            json!({"id":"10.1234/b","attributes":{"url":"https://example.org/shared"}}),
        ],
        &[],
    )?;
    let index = DataciteIndex::load(&bundle)?;
    let key = "https://example.org/shared";
    let needed = 2 * std::mem::size_of::<String>() + "10.1234/a".len() + "10.1234/b".len();
    assert_eq!(index.url_dois(key, needed)?, ["10.1234/a", "10.1234/b"]);
    let err = index.url_dois(key, needed - 1).unwrap_err();
    assert!(error_text(&err).contains("budget"), "{err:#}");
    // An unregistered URL costs nothing.
    assert!(index.url_dois("https://example.org/none", 0)?.is_empty());
    Ok(())
}

#[test]
fn keys_survive_non_ascii_dois_and_encoded_paths() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let (_, bundle) = build_fixture(
        tmp.path(),
        &[
            json!({"id":"10.1234/données(1):x","attributes":{"url":"https://example.org/données?q=a b#frag"}}),
            json!({"id":"10.1234/plain","attributes":{"url":"https://example.org/donn%c3%a9es?q=a%20b#frag"}}),
        ],
        &[("10.5555/ÜBER", "registered")],
    )?;
    let index = DataciteIndex::load(&bundle)?;
    assert!(index.contains_doi("10.1234/données(1):x"));
    assert!(index.contains_doi("10.5555/Über"));
    assert!(!index.contains_doi("10.5555/über"));
    let key = normalize_landing_url("https://example.org/données?q=a b#frag").unwrap();
    assert_eq!(key, "https://example.org/donn%C3%A9es?q=a%20b#frag");
    assert_eq!(
        index.url_dois(&key, 4096)?,
        ["10.1234/données(1):x", "10.1234/plain"]
    );
    assert_eq!(index.manifest().counts["urls"], 1);
    assert_eq!(index.manifest().counts["shared_urls"], 1);
    assert!(index.contains_host("example.org"));
    assert!(!fs::read(bundle.join(URLS_FILE))?.is_empty());
    Ok(())
}
