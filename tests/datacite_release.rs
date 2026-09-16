use std::fs;

use crossref_citation_extraction::input::datacite_release::{
    decode_metadata_record, MonthlyRelease, RecordSource, RegistryRecord, SourceStats,
};
use serde_json::json;

mod support;

use support::{
    csv_path, gzip, make_release, metadata_jsonl, part_path, sha256_base64, sha256_hex, state_csv,
    write_manifest_json, write_release, write_status, MONTH,
};

#[test]
fn accepts_singleton_and_rejects_conflicting_dois() {
    let row = decode_metadata_record(r#"{"data":{"id":"10.1234/A","attributes":{"doi":"10.1234/a","url":"https://example.org/a"}}}"#).unwrap();
    assert_eq!(row.doi, "10.1234/a");
    assert_eq!(row.url.as_deref(), Some("https://example.org/a"));
    assert!(
        decode_metadata_record(r#"{"id":"10.1234/a","attributes":{"doi":"10.1234/b"}}"#).is_err()
    );
}

#[test]
fn scans_registered_only_dois_from_csv() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    support::make_release(
        tmp.path(),
        &[serde_json::json!({"id":"10.1234/a"})],
        &[("10.1234/a", "findable"), ("10.1234/retired", "registered")],
    )?;
    let release = MonthlyRelease::open(tmp.path())?;
    let mut seen = std::collections::BTreeSet::new();
    release.scan(&mut |row| {
        seen.insert(row.doi);
        Ok(())
    })?;
    assert_eq!(
        seen.into_iter().collect::<Vec<_>>(),
        vec!["10.1234/a", "10.1234/retired"]
    );
    Ok(())
}

#[test]
fn decodes_id_only_rows_with_missing_url() {
    for line in [
        r#"{"id":"10.1234/a"}"#,
        r#"{"id":"10.1234/a","attributes":{}}"#,
        r#"{"id":"10.1234/a","attributes":{"url":null}}"#,
        r#"{"data":{"id":"10.1234/a","attributes":{"doi":null,"url":null}}}"#,
    ] {
        let row = decode_metadata_record(line).unwrap();
        assert_eq!(row.doi, "10.1234/a", "{line}");
        assert_eq!(row.url, None, "{line}");
        assert!(!row.url_invalid, "{line}");
        assert_eq!(row.source, RecordSource::Metadata);
    }
}

#[test]
fn flags_non_string_urls_without_dropping_the_doi() {
    for line in [
        r#"{"id":"10.1234/a","attributes":{"url":42}}"#,
        r#"{"id":"10.1234/a","attributes":{"url":true}}"#,
        r#"{"id":"10.1234/a","attributes":{"url":{"href":"https://example.org"}}}"#,
        r#"{"id":"10.1234/a","attributes":{"url":["https://example.org"]}}"#,
    ] {
        let row = decode_metadata_record(line).unwrap();
        assert_eq!(row.doi, "10.1234/a", "{line}");
        assert_eq!(row.url, None, "{line}");
        assert!(row.url_invalid, "{line}");
    }
}

#[test]
fn keeps_url_strings_verbatim_and_decodes_json_escapes() {
    let row = decode_metadata_record(
        r#"{"id":"10.1234/a","attributes":{"url":" HTTPS://Example.org/A?b=1 "}}"#,
    )
    .unwrap();
    assert_eq!(row.url.as_deref(), Some(" HTTPS://Example.org/A?b=1 "));

    let row = decode_metadata_record(
        r#"{"id":"10.1234/café","attributes":{"url":"https:\/\/example.org\/café"}}"#,
    )
    .unwrap();
    assert_eq!(row.doi, "10.1234/café");
    assert_eq!(row.url.as_deref(), Some("https://example.org/café"));
}

#[test]
fn normalizes_record_dois_and_rejects_invalid_ones() {
    let row = decode_metadata_record(
        r#"{"id":"https://doi.org/10.1234/AbC","attributes":{"doi":"10.1234/abc"}}"#,
    )
    .unwrap();
    assert_eq!(row.doi, "10.1234/abc");
    let row = decode_metadata_record(r#"{"attributes":{"doi":"10.1234/attr-only"}}"#).unwrap();
    assert_eq!(row.doi, "10.1234/attr-only");

    let err = decode_metadata_record(r#"{"id":"not a doi"}"#).unwrap_err();
    assert!(format!("{err:#}").contains("not a doi"), "{err:#}");
    for line in [
        r#"{"id":""}"#,
        r#"{"id":42}"#,
        r#"{"id":"10.1234/a","attributes":{"doi":"nope"}}"#,
        r#"{"attributes":{"url":"https://example.org/no-doi"}}"#,
    ] {
        assert!(decode_metadata_record(line).is_err(), "{line}");
    }
}

#[test]
fn rejects_malformed_json_and_non_singleton_wrappers() {
    for line in [
        "",
        "{",
        "[]",
        r#""10.1234/a""#,
        r#"{"id":"10.1234/a"} trailing"#,
        r#"{"data":[{"id":"10.1234/a"}]}"#,
        r#"{"data":{"id":"10.1234/a"},"id":"10.1234/a"}"#,
    ] {
        assert!(decode_metadata_record(line).is_err(), "{line:?}");
    }
}

// ---- open: manifest, status, and layout validation -----------------------

/// Collect every record a scan emits, in emission order, with its stats.
fn scan_all(release: &MonthlyRelease) -> anyhow::Result<(Vec<RegistryRecord>, SourceStats)> {
    let mut rows = Vec::new();
    let stats = release.scan(&mut |row| {
        rows.push(row);
        Ok(())
    })?;
    Ok((rows, stats))
}

/// The full error chain, so assertions can see the context and the cause.
fn error_text(err: &anyhow::Error) -> String {
    format!("{err:#}")
}

fn manifest_entry(filename: &str, bytes: &[u8]) -> serde_json::Value {
    json!({"filename": filename, "size": bytes.len(), "sha256": sha256_base64(bytes)})
}

#[test]
fn open_lists_sorted_files_with_hex_digests_and_inventory() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    make_release(
        tmp.path(),
        &[json!({"id":"10.1234/a"})],
        &[("10.1234/a", "findable")],
    )?;
    let release = MonthlyRelease::open(tmp.path())?;
    assert_eq!(release.release, MONTH);
    assert_eq!(release.root, tmp.path());
    let paths: Vec<_> = release
        .files
        .iter()
        .map(|file| file.path.to_string_lossy().into_owned())
        .collect();
    assert_eq!(paths, vec![csv_path(MONTH), part_path(MONTH, 0)]);

    let mut rows = Vec::new();
    for file in &release.files {
        let bytes = fs::read(tmp.path().join(&file.path))?;
        assert_eq!(file.size, bytes.len() as u64);
        assert_eq!(file.sha256, sha256_hex(&bytes));
        rows.push((
            file.path.to_string_lossy().into_owned(),
            file.size,
            file.sha256.clone(),
        ));
    }
    let inventory = serde_json::to_string(&rows)?;
    assert!(!inventory.contains(' ') && !inventory.ends_with('\n'));
    assert_eq!(release.inventory_sha256, sha256_hex(inventory.as_bytes()));
    Ok(())
}

#[test]
fn inventory_digest_matches_the_cross_language_fixture() -> anyhow::Result<()> {
    // Python (Task 8) must reproduce this digest byte for byte: rows are
    // [relative_path, size, sha256_hex] sorted by path, compact JSON, no
    // trailing newline. `open` never decodes, so the bytes need not be gzip.
    let tmp = tempfile::tempdir()?;
    write_release(
        tmp.path(),
        MONTH,
        &[
            (&part_path(MONTH, 0), b"x".to_vec()),
            (&csv_path(MONTH), b"yz".to_vec()),
        ],
    )?;
    let release = MonthlyRelease::open(tmp.path())?;
    assert_eq!(
        release.inventory_sha256,
        "eece7282f43778a37b55e1481455c501c7eb72adb5a4bba7f6dbfff2d701621b"
    );

    // A folder dated after the release month is an ordinary input: the real
    // 2026-07 data file, generated on 1 August, lists `dois/updated_2026-08/`
    // for the DOIs updated before the snapshot. Python pins this digest too.
    let tmp = tempfile::tempdir()?;
    write_release(
        tmp.path(),
        MONTH,
        &[
            (&part_path(MONTH, 0), b"x".to_vec()),
            (&csv_path(MONTH), b"yz".to_vec()),
            (&part_path("2026-08", 0), b"later".to_vec()),
            (&csv_path("2026-08"), b"rows".to_vec()),
        ],
    )?;
    let release = MonthlyRelease::open(tmp.path())?;
    assert_eq!(release.files.len(), 4);
    assert_eq!(
        release.inventory_sha256,
        "f4ce7500bce908603e88efe1e7285ccff4d2ebf628553c86654179bcf26bcb01"
    );
    Ok(())
}

#[test]
fn open_fails_when_month_folder_lists_jsonl_without_csv() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let jsonl = gzip(&metadata_jsonl(&[json!({"id":"10.1234/a"})]));
    write_release(tmp.path(), MONTH, &[(&part_path(MONTH, 0), jsonl)])?;
    let err = MonthlyRelease::open(tmp.path()).unwrap_err();
    assert!(error_text(&err).contains(".csv.gz state report"), "{err:#}");

    // A folder holding only a state report is a complete input.
    let tmp = tempfile::tempdir()?;
    let csv = gzip(&state_csv(&[("10.1234/a", "registered")]));
    write_release(tmp.path(), MONTH, &[(&csv_path(MONTH), csv)])?;
    let (rows, stats) = scan_all(&MonthlyRelease::open(tmp.path())?)?;
    assert_eq!(stats.csv_records, 1);
    assert_eq!(rows[0].doi, "10.1234/a");
    assert_eq!(rows[0].source, RecordSource::StateReport);
    Ok(())
}

#[test]
fn open_fails_when_a_listed_file_is_missing_or_resized() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    make_release(tmp.path(), &[json!({"id":"10.1234/a"})], &[])?;
    let csv = tmp.path().join(csv_path(MONTH));
    let original = fs::read(&csv)?;
    fs::remove_file(&csv)?;
    let err = MonthlyRelease::open(tmp.path()).unwrap_err();
    assert!(error_text(&err).contains("2026-07.csv.gz"), "{err:#}");

    // Truncated on disk behind a stale manifest: the size check catches it
    // before any decoding starts.
    fs::write(&csv, &original[..original.len() - 1])?;
    let err = MonthlyRelease::open(tmp.path()).unwrap_err();
    assert!(error_text(&err).contains("manifest says"), "{err:#}");
    Ok(())
}

#[test]
fn open_rejects_unexpected_duplicate_and_traversing_paths() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    make_release(
        tmp.path(),
        &[json!({"id":"10.1234/a"})],
        &[("10.1234/a", "findable")],
    )?;

    // A source file on disk that the manifest does not list.
    let extra = tmp.path().join(part_path(MONTH, 1));
    fs::write(&extra, gzip(b""))?;
    let err = MonthlyRelease::open(tmp.path()).unwrap_err();
    assert!(error_text(&err).contains("part_0001.jsonl.gz"), "{err:#}");
    fs::remove_file(&extra)?;

    // Files that are not JSONL/CSV inputs are ignored wherever they sit.
    fs::write(tmp.path().join("dois/updated_2026-07/.DS_Store"), b"")?;
    fs::write(tmp.path().join("README.txt"), b"")?;
    MonthlyRelease::open(tmp.path())?;

    let jsonl = fs::read(tmp.path().join(part_path(MONTH, 0)))?;
    let csv = fs::read(tmp.path().join(csv_path(MONTH)))?;
    let listed = vec![
        manifest_entry(&part_path(MONTH, 0), &jsonl),
        manifest_entry(&csv_path(MONTH), &csv),
    ];

    // Non-input entries are excluded rather than rejected.
    let mut entries = listed.clone();
    entries.push(manifest_entry("README.txt", b""));
    entries.push(manifest_entry("dois/updated_2026-07/notes.txt", b""));
    write_manifest_json(tmp.path(), &entries)?;
    assert_eq!(MonthlyRelease::open(tmp.path())?.files.len(), 2);

    for (bad, needle) in [
        ("dois/updated_2026-07/part_0000.jsonl.gz", "duplicate"),
        ("dois/../updated_2026-07/part_0000.jsonl.gz", ".."),
        ("/dois/updated_2026-07/part_0000.jsonl.gz", "/dois/"),
        ("dois/updated_2026-07/extra/part_0000.jsonl.gz", "extra"),
        ("dois/part_0000.jsonl.gz", "dois/part_0000"),
        ("other/updated_2026-07/part_0000.jsonl.gz", "other/"),
        ("dois/updated_2026-13/part_0000.jsonl.gz", "2026-13"),
        (
            "dois\\updated_2026-07\\part_0000.jsonl.gz",
            "updated_2026-07",
        ),
    ] {
        let mut entries = listed.clone();
        entries.push(manifest_entry(bad, &jsonl));
        write_manifest_json(tmp.path(), &entries)?;
        let err = MonthlyRelease::open(tmp.path()).unwrap_err();
        assert!(error_text(&err).contains(needle), "{bad}: {err:#}");
    }

    // A manifest listing no inputs at all is not a release.
    write_manifest_json(tmp.path(), &[manifest_entry("README.txt", b"")])?;
    assert!(MonthlyRelease::open(tmp.path()).is_err());

    // An undecodable digest is rejected up front.
    let mut entries = listed.clone();
    entries[0]["sha256"] = json!("not base64!");
    write_manifest_json(tmp.path(), &entries)?;
    let err = MonthlyRelease::open(tmp.path()).unwrap_err();
    assert!(error_text(&err).contains("base64"), "{err:#}");
    Ok(())
}

#[test]
fn open_requires_complete_status_and_accepts_folders_of_any_month() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    // The real data file is cumulative: folders for earlier months sit next
    // to the release month's own folder, and a release generated on the 1st
    // also carries the next month's folder.
    write_release(
        tmp.path(),
        "2026-07",
        &[
            (
                &part_path("2011-03", 0),
                gzip(&metadata_jsonl(&[json!({"id":"10.1234/old"})])),
            ),
            (
                &csv_path("2011-03"),
                gzip(&state_csv(&[("10.1234/old", "findable")])),
            ),
            (
                &part_path("2026-07", 0),
                gzip(&metadata_jsonl(&[json!({"id":"10.1234/new"})])),
            ),
            (
                &csv_path("2026-07"),
                gzip(&state_csv(&[("10.1234/new", "findable")])),
            ),
        ],
    )?;
    let release = MonthlyRelease::open(tmp.path())?;
    assert_eq!(release.release, "2026-07");
    assert_eq!(release.files.len(), 4);
    let (rows, stats) = scan_all(&release)?;
    assert_eq!(rows.len(), 4);
    assert_eq!(stats.metadata_records, 2);
    assert_eq!(stats.csv_records, 2);

    // A release with no folder for its own month is still consistent.
    write_status(tmp.path(), "2026-08", "Complete")?;
    assert_eq!(MonthlyRelease::open(tmp.path())?.release, "2026-08");

    for status in ["In progress", "Uploading", "complete"] {
        write_status(tmp.path(), "2026-07", status)?;
        let err = MonthlyRelease::open(tmp.path()).unwrap_err();
        assert!(error_text(&err).contains(status), "{err:#}");
    }

    // A folder dated after the release month is accepted and inventoried;
    // the inventory depends only on the manifest, never on the STATUS month.
    write_status(tmp.path(), "2026-06", "Complete")?;
    let later = MonthlyRelease::open(tmp.path())?;
    assert_eq!(later.release, "2026-06");
    assert_eq!(later.files, release.files);
    assert_eq!(later.inventory_sha256, release.inventory_sha256);

    write_status(tmp.path(), "2026/07", "Complete")?;
    let err = MonthlyRelease::open(tmp.path()).unwrap_err();
    assert!(error_text(&err).contains("2026/07"), "{err:#}");

    fs::remove_file(tmp.path().join("STATUS.json"))?;
    let err = MonthlyRelease::open(tmp.path()).unwrap_err();
    assert!(error_text(&err).contains("STATUS.json"), "{err:#}");
    Ok(())
}

#[test]
fn open_scans_a_folder_dated_after_the_release_month() -> anyhow::Result<()> {
    // Mirrors the real 2026-07 data file: `STATUS.json` says 2026-07, and
    // the manifest lists `dois/updated_2026-08/` (one part, one state
    // report) for the DOIs updated on 1 August before the snapshot.
    let tmp = tempfile::tempdir()?;
    write_release(
        tmp.path(),
        "2026-07",
        &[
            (
                &part_path("2026-07", 0),
                gzip(&metadata_jsonl(&[json!({"id":"10.1234/july"})])),
            ),
            (
                &csv_path("2026-07"),
                gzip(&state_csv(&[("10.1234/july", "findable")])),
            ),
            (
                &part_path("2026-08", 0),
                gzip(&metadata_jsonl(&[json!({"id":"10.1234/august"})])),
            ),
            (
                &csv_path("2026-08"),
                gzip(&state_csv(&[
                    ("10.1234/august", "findable"),
                    ("10.1234/august-registered", "registered"),
                ])),
            ),
        ],
    )?;
    let release = MonthlyRelease::open(tmp.path())?;
    assert_eq!(release.release, "2026-07");
    let paths: Vec<_> = release
        .files
        .iter()
        .map(|file| file.path.to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        paths,
        vec![
            csv_path("2026-07"),
            part_path("2026-07", 0),
            csv_path("2026-08"),
            part_path("2026-08", 0),
        ]
    );
    let (rows, stats) = scan_all(&release)?;
    assert_eq!((stats.metadata_records, stats.csv_records), (2, 3));
    let dois: std::collections::BTreeSet<_> = rows.into_iter().map(|row| row.doi).collect();
    assert_eq!(
        dois.into_iter().collect::<Vec<_>>(),
        vec![
            "10.1234/august",
            "10.1234/august-registered",
            "10.1234/july"
        ]
    );

    // The later folder is held to the same rule as every other: JSONL
    // metadata without a state report is refused.
    let tmp = tempfile::tempdir()?;
    write_release(
        tmp.path(),
        "2026-07",
        &[
            (
                &part_path("2026-07", 0),
                gzip(&metadata_jsonl(&[json!({"id":"10.1234/july"})])),
            ),
            (
                &csv_path("2026-07"),
                gzip(&state_csv(&[("10.1234/july", "findable")])),
            ),
            (
                &part_path("2026-08", 0),
                gzip(&metadata_jsonl(&[json!({"id":"10.1234/august"})])),
            ),
        ],
    )?;
    let err = MonthlyRelease::open(tmp.path()).unwrap_err();
    assert!(
        error_text(&err).contains("updated_2026-08 lists JSONL"),
        "{err:#}"
    );
    Ok(())
}

#[test]
fn open_accepts_text_manifest_only_when_json_is_absent() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    make_release(
        tmp.path(),
        &[json!({"id":"10.1234/a"})],
        &[("10.1234/a", "findable")],
    )?;
    let from_json = MonthlyRelease::open(tmp.path())?;
    let entries: Vec<serde_json::Value> =
        serde_json::from_slice(&fs::read(tmp.path().join("MANIFEST.json"))?)?;
    let text: String = entries
        .iter()
        .map(|entry| {
            format!(
                "{} {} {}\n",
                entry["filename"].as_str().unwrap(),
                entry["size"],
                entry["sha256"].as_str().unwrap()
            )
        })
        .collect();
    fs::write(tmp.path().join("MANIFEST"), &text)?;

    // While MANIFEST.json exists it is authoritative: malformed is an error,
    // never a fallback to the text form.
    fs::write(tmp.path().join("MANIFEST.json"), b"[{")?;
    let err = MonthlyRelease::open(tmp.path()).unwrap_err();
    assert!(error_text(&err).contains("MANIFEST.json"), "{err:#}");

    fs::remove_file(tmp.path().join("MANIFEST.json"))?;
    let from_text = MonthlyRelease::open(tmp.path())?;
    assert_eq!(from_text.files, from_json.files);
    assert_eq!(from_text.inventory_sha256, from_json.inventory_sha256);

    fs::write(
        tmp.path().join("MANIFEST"),
        format!("{text}\ndois/updated_2026-07/part_0001.jsonl.gz notasize abc=\n"),
    )?;
    let err = MonthlyRelease::open(tmp.path()).unwrap_err();
    assert!(error_text(&err).contains("line 4"), "{err:#}");

    fs::remove_file(tmp.path().join("MANIFEST"))?;
    let err = MonthlyRelease::open(tmp.path()).unwrap_err();
    assert!(error_text(&err).contains("MANIFEST"), "{err:#}");
    Ok(())
}

// ---- scan: streaming, counting, and verification --------------------------

#[test]
fn scan_counts_records_and_url_states() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    make_release(
        tmp.path(),
        &[
            json!({"id":"10.1234/a","attributes":{"url":"https://example.org/a"}}),
            json!({"id":"10.1234/b"}),
            json!({"id":"10.1234/c","attributes":{"url":null}}),
            json!({"id":"10.1234/d","attributes":{"url":7}}),
            json!({"data":{"id":"10.1234/E","attributes":{"doi":"10.1234/e","url":"https://example.org/e"}}}),
        ],
        &[("10.1234/a", "findable"), ("10.1234/gone", "registered")],
    )?;
    let (rows, stats) = scan_all(&MonthlyRelease::open(tmp.path())?)?;
    assert_eq!(
        stats,
        SourceStats {
            metadata_records: 5,
            csv_records: 2,
            missing_urls: 2,
            invalid_urls: 1,
        }
    );
    // The state report sorts before the part, and its rows never carry URLs.
    let csv_rows: Vec<_> = rows
        .iter()
        .take_while(|row| row.source == RecordSource::StateReport)
        .map(|row| (row.doi.as_str(), row.url.is_none() && !row.url_invalid))
        .collect();
    assert_eq!(csv_rows, vec![("10.1234/a", true), ("10.1234/gone", true)]);
    let metadata_rows: Vec<_> = rows
        .iter()
        .filter(|row| row.source == RecordSource::Metadata)
        .map(|row| (row.doi.as_str(), row.url.as_deref(), row.url_invalid))
        .collect();
    assert_eq!(
        metadata_rows,
        vec![
            ("10.1234/a", Some("https://example.org/a"), false),
            ("10.1234/b", None, false),
            ("10.1234/c", None, false),
            ("10.1234/d", None, true),
            ("10.1234/e", Some("https://example.org/e"), false),
        ]
    );

    // Errors from the consumer stop the scan.
    let err = MonthlyRelease::open(tmp.path())?
        .scan(&mut |_| anyhow::bail!("consumer refused"))
        .unwrap_err();
    assert!(error_text(&err).contains("consumer refused"), "{err:#}");
    Ok(())
}

#[test]
fn scan_reports_malformed_json_and_invalid_dois_with_path_and_line() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let empty_csv = gzip(&state_csv(&[]));
    let jsonl = b"{\"id\":\"10.1234/a\"}\n\n{not json\n".to_vec();
    write_release(
        tmp.path(),
        MONTH,
        &[
            (&part_path(MONTH, 0), gzip(&jsonl)),
            (&csv_path(MONTH), empty_csv.clone()),
        ],
    )?;
    let err = scan_all(&MonthlyRelease::open(tmp.path())?).unwrap_err();
    assert!(error_text(&err).contains("part_0000.jsonl.gz:3"), "{err:#}");

    let jsonl = metadata_jsonl(&[json!({"id":"10.1234/a"}), json!({"id":"not a doi"})]);
    write_release(
        tmp.path(),
        MONTH,
        &[
            (&part_path(MONTH, 0), gzip(&jsonl)),
            (&csv_path(MONTH), empty_csv),
        ],
    )?;
    let err = scan_all(&MonthlyRelease::open(tmp.path())?).unwrap_err();
    let text = error_text(&err);
    assert!(
        text.contains("part_0000.jsonl.gz:2") && text.contains("not a doi"),
        "{text}"
    );
    Ok(())
}

#[test]
fn scan_rejects_unknown_csv_states_headers_and_dois() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let empty_jsonl = gzip(&metadata_jsonl(&[]));
    let cases: [(&[u8], &str); 4] = [
        (
            b"doi,state,client_id,updated\n10.1234/a,findable,c,u\n10.1234/b,draft,c,u\n",
            "2026-07.csv.gz:3",
        ),
        (
            b"doi,state,client_id,updated\n10.1234/a,Findable,c,u\n",
            "Findable",
        ),
        (
            b"doi,state,client,updated\n10.1234/a,findable,c,u\n",
            "header",
        ),
        (
            b"doi,state,client_id,updated\nnot a doi,findable,c,u\n",
            "not a doi",
        ),
    ];
    for (csv, needle) in cases {
        write_release(
            tmp.path(),
            MONTH,
            &[
                (&part_path(MONTH, 0), empty_jsonl.clone()),
                (&csv_path(MONTH), gzip(csv)),
            ],
        )?;
        let err = scan_all(&MonthlyRelease::open(tmp.path())?).unwrap_err();
        let text = error_text(&err);
        assert!(
            text.contains("2026-07.csv.gz") && text.contains(needle),
            "{text}"
        );
    }
    Ok(())
}

#[test]
fn scan_fails_on_same_size_corruption_even_when_records_decode() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    make_release(tmp.path(), &[json!({"id":"10.1234/a"})], &[])?;
    let part = tmp.path().join(part_path(MONTH, 0));
    let pristine = fs::read(&part)?;

    // Byte 4 starts the gzip header MTIME field: changing it leaves the
    // member decodable, so only the manifest digest can catch it.
    let mut bytes = pristine.clone();
    bytes[4] ^= 0x01;
    fs::write(&part, &bytes)?;
    let release = MonthlyRelease::open(tmp.path())?;
    let mut seen = Vec::new();
    let err = release
        .scan(&mut |row| {
            seen.push(row.doi);
            Ok(())
        })
        .unwrap_err();
    assert!(error_text(&err).contains("SHA-256"), "{err:#}");
    // Records stream before the digest is known; the Err tells the caller
    // to discard them.
    assert_eq!(seen, vec!["10.1234/a"]);

    // Corrupting the deflate payload fails inside the decoder instead.
    let mut bytes = pristine;
    let last_payload_byte = bytes.len() - 9;
    bytes[last_payload_byte] ^= 0xFF;
    fs::write(&part, &bytes)?;
    let err = scan_all(&MonthlyRelease::open(tmp.path())?).unwrap_err();
    let text = error_text(&err);
    assert!(
        text.contains("part_0000.jsonl.gz:") && text.contains("unreadable line"),
        "{text}"
    );
    Ok(())
}

#[test]
fn scan_fails_on_truncated_gzip_even_when_manifest_matches() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let records: Vec<_> = (0..200)
        .map(|index| json!({"id": format!("10.1234/{index}")}))
        .collect();
    let full = gzip(&metadata_jsonl(&records));
    let truncated = full[..full.len() * 2 / 3].to_vec();
    write_release(
        tmp.path(),
        MONTH,
        &[
            (&part_path(MONTH, 0), truncated),
            (&csv_path(MONTH), gzip(&state_csv(&[]))),
        ],
    )?;
    let release = MonthlyRelease::open(tmp.path())?;
    let err = scan_all(&release).unwrap_err();
    let text = error_text(&err);
    assert!(
        text.contains("part_0000.jsonl.gz:") && text.contains("unreadable line"),
        "{text}"
    );

    let full = gzip(&state_csv(&[
        ("10.1234/a", "findable"),
        ("10.1234/b", "findable"),
    ]));
    let truncated = full[..full.len() - 10].to_vec();
    write_release(
        tmp.path(),
        MONTH,
        &[
            (&part_path(MONTH, 0), gzip(b"")),
            (&csv_path(MONTH), truncated),
        ],
    )?;
    let err = scan_all(&MonthlyRelease::open(tmp.path())?).unwrap_err();
    let text = error_text(&err);
    assert!(
        text.contains("2026-07.csv.gz") && text.contains("unreadable CSV"),
        "{text}"
    );
    Ok(())
}

#[test]
fn scan_reads_every_member_of_multi_member_gzip() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let mut parts = gzip(&metadata_jsonl(&[json!({"id":"10.1234/first"})]));
    parts.extend(gzip(&metadata_jsonl(&[json!({"id":"10.1234/second"})])));
    let mut csv = gzip(&state_csv(&[("10.1234/one", "registered")]));
    csv.extend(gzip(
        b"10.1234/two,registered,example.repo,2026-07-15T00:00:00.000Z\n",
    ));
    write_release(
        tmp.path(),
        MONTH,
        &[(&part_path(MONTH, 0), parts), (&csv_path(MONTH), csv)],
    )?;
    let (rows, stats) = scan_all(&MonthlyRelease::open(tmp.path())?)?;
    assert_eq!(stats.metadata_records, 2);
    assert_eq!(stats.csv_records, 2);
    let dois: Vec<_> = rows.iter().map(|row| row.doi.as_str()).collect();
    assert_eq!(
        dois,
        vec![
            "10.1234/one",
            "10.1234/two",
            "10.1234/first",
            "10.1234/second"
        ]
    );
    Ok(())
}
