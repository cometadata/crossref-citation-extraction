//! Full-mode (`--datacite-index`) pipeline: CLI validation, streaming
//! extraction with bounded batches, checkpoint/resume replay, fatal source
//! errors, and the extraction report contract, all through the real binary.

mod support;
use crossref_citation_extraction::index::datacite_builder::build_datacite_index;
use std::{collections::BTreeSet, process::Command};

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};

#[test]
fn full_mode_retains_doi_url_and_arxiv_matches() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let source = tmp.path().join("source");
    support::make_release(
        &source,
        &[
            serde_json::json!({"id":"10.1234/a","attributes":{"url":"https://example.org/a"}}),
            serde_json::json!({"id":"10.48550/arxiv.2403.03542"}),
        ],
        &[
            ("10.1234/a", "findable"),
            ("10.48550/arxiv.2403.03542", "findable"),
        ],
    )?;
    let bundle = tmp.path().join("bundle");
    build_datacite_index(&source, &bundle, 64)?;
    let input = tmp.path().join("crossref.json");
    std::fs::write(
        &input,
        serde_json::to_vec(&serde_json::json!({"items":[{
            "DOI":"10.9999/citer", "reference":[
                {"DOI":"10.1234/a","doi-asserted-by":"publisher"},
                {"URL":"https://example.org/a"},
                {"unstructured":"arXiv:2403.03542"}
            ]
        }]}))?,
    )?;
    let output = tmp.path().join("out");
    let status = Command::new(env!("CARGO_BIN_EXE_crossref-citation-extraction"))
        .arg("pipeline")
        .arg("--input")
        .arg(&input)
        .arg("--datacite-index")
        .arg(&bundle)
        .arg("--output-dir")
        .arg(&output)
        .arg("--temp-dir")
        .arg(tmp.path().join("parts"))
        .status()?;
    assert!(status.success());
    let targets: BTreeSet<String> = std::fs::read_to_string(output.join("crossref_valid.jsonl"))?
        .lines()
        .map(|line| {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            row["doi"].as_str().unwrap().to_owned()
        })
        .collect();
    assert_eq!(
        targets,
        ["10.1234/a".into(), "10.48550/arxiv.2403.03542".into()].into()
    );
    Ok(())
}

// ---------------------------------------------------------------- fixtures

/// A registered target: its DOI and optional landing-page URL.
type Target<'a> = (&'a str, Option<&'a str>);

/// Build a bundle at `<root>/bundle` from `targets`, all `findable`.
fn build_bundle(root: &Path, targets: &[Target<'_>]) -> Result<PathBuf> {
    let metadata: Vec<Value> = targets
        .iter()
        .map(|(doi, url)| match url {
            Some(url) => json!({"id": doi, "attributes": {"url": url}}),
            None => json!({"id": doi}),
        })
        .collect();
    let states: Vec<(&str, &str)> = targets.iter().map(|(doi, _)| (*doi, "findable")).collect();
    let source = root.join("source");
    support::make_release(&source, &metadata, &states)?;
    let bundle = root.join("bundle");
    build_datacite_index(&source, &bundle, 64)?;
    Ok(bundle)
}

/// Write `members` (name, JSON bytes) as a gzipped tar archive.
fn write_tar_gz(path: &Path, members: &[(&str, Vec<u8>)]) -> Result<()> {
    let file = fs::File::create(path)?;
    let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
    let mut builder = tar::Builder::new(encoder);
    for (name, bytes) in members {
        let mut header = tar::Header::new_gnu();
        header.set_path(name)?;
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append(&header, bytes.as_slice())?;
    }
    builder.into_inner()?.finish()?;
    Ok(())
}

/// A Crossref member holding `works`, each `(citing DOI, references)`.
fn member(works: &[(&str, Vec<Value>)]) -> Vec<u8> {
    let items: Vec<Value> = works
        .iter()
        .map(|(doi, references)| json!({"DOI": doi, "reference": references}))
        .collect();
    serde_json::to_vec(&json!({"items": items})).expect("fixture serializes")
}

struct Run {
    input: PathBuf,
    bundle: PathBuf,
    output_dir: PathBuf,
    temp_dir: PathBuf,
    extra: Vec<String>,
}

impl Run {
    fn new(input: &Path, bundle: &Path, output_dir: &Path, temp_dir: &Path) -> Self {
        Self {
            input: input.to_path_buf(),
            bundle: bundle.to_path_buf(),
            output_dir: output_dir.to_path_buf(),
            temp_dir: temp_dir.to_path_buf(),
            extra: Vec::new(),
        }
    }

    fn args(mut self, extra: &[&str]) -> Self {
        self.extra.extend(extra.iter().map(|s| s.to_string()));
        self
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_crossref-citation-extraction"));
        command
            .arg("pipeline")
            .arg("--input")
            .arg(&self.input)
            .arg("--datacite-index")
            .arg(&self.bundle)
            .arg("--output-dir")
            .arg(&self.output_dir)
            .arg("--temp-dir")
            .arg(&self.temp_dir)
            .args(&self.extra);
        command
    }

    /// Run the binary with captured output.
    fn output(&self) -> Result<Output> {
        self.command()
            .output()
            .context("running the pipeline binary")
    }

    fn report_path(&self) -> PathBuf {
        self.output_dir.join("extraction-report.json")
    }

    /// Run, require success, and return the parsed report.
    fn succeed(&self) -> Result<Value> {
        let output = self.output()?;
        ensure!(
            output.status.success(),
            "pipeline failed: {}",
            transcript(&output)
        );
        read_json(&self.report_path())
    }

    /// Run, require failure whose error output mentions `message`, and
    /// require that no report exists.
    fn fail_with(&self, message: &str) -> Result<String> {
        let output = self.output()?;
        let text = transcript(&output);
        ensure!(
            !output.status.success(),
            "pipeline unexpectedly succeeded:\n{text}"
        );
        ensure!(
            text.contains(message),
            "output does not mention {message:?}:\n{text}"
        );
        ensure!(
            !self.report_path().exists(),
            "a report exists after a failed run"
        );
        Ok(text)
    }
}

/// Log events (stdout, where the logger writes) followed by the error
/// `main` printed to stderr.
fn transcript(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn read_json(path: &Path) -> Result<Value> {
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))
}

fn read_jsonl(path: &Path) -> Result<Vec<Value>> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    text.lines()
        .map(|line| serde_json::from_str(line).with_context(|| format!("parsing {line}")))
        .collect()
}

/// `cited DOI -> cited_by entries` from a grouped JSONL output.
fn grouped(path: &Path) -> Result<BTreeMap<String, Vec<Value>>> {
    Ok(read_jsonl(path)?
        .into_iter()
        .map(|row| {
            (
                row["doi"].as_str().unwrap().to_string(),
                row["cited_by"].as_array().unwrap().clone(),
            )
        })
        .collect())
}

fn stat(report: &Value, name: &str) -> u64 {
    report["stats"][name]
        .as_u64()
        .unwrap_or_else(|| panic!("report stat {name} missing in {report}"))
}

fn count_files(dir: &Path, suffix: &str) -> usize {
    walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(suffix))
        .count()
}

// ------------------------------------------------------------------- tests

#[test]
fn malformed_top_level_json_fails_without_a_report() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bundle = build_bundle(tmp.path(), &[("10.1234/a", None)])?;
    let input = tmp.path().join("crossref.json");
    fs::write(&input, b"{\"items\": [{\"DOI\": \"10.9999/citer\"")?;
    let run = Run::new(
        &input,
        &bundle,
        &tmp.path().join("out"),
        &tmp.path().join("parts"),
    );
    run.fail_with("invalid top-level JSON in member crossref.json")?;
    // A `reference` field that is not an array only counts; a bare string
    // item and a non-object top level do not get that leniency.
    fs::write(&input, b"[]")?;
    run.fail_with("invalid top-level JSON")?;
    Ok(())
}

#[test]
fn unreadable_archive_is_a_fatal_source_error() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bundle = build_bundle(tmp.path(), &[("10.1234/a", None)])?;
    let input = tmp.path().join("crossref.tar.gz");
    let members: Vec<(&str, Vec<u8>)> = (0..4)
        .map(|i| {
            let name: &'static str = Box::leak(format!("{i}.json").into_boxed_str());
            let padding = "lorem ipsum ".repeat(4000);
            let works = vec![(
                "10.9999/citer",
                vec![json!({"DOI": "10.1234/a", "unstructured": padding})],
            )];
            (name, member(&works))
        })
        .collect();
    write_tar_gz(&input, &members)?;
    let bytes = fs::read(&input)?;
    fs::write(&input, &bytes[..bytes.len() / 2])?;
    let run = Run::new(
        &input,
        &bundle,
        &tmp.path().join("out"),
        &tmp.path().join("parts"),
    );
    run.fail_with("reading the Crossref source failed")?;
    Ok(())
}

#[test]
fn oversized_member_is_a_fatal_source_error() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bundle = build_bundle(tmp.path(), &[("10.1234/a", None)])?;
    let input = tmp.path().join("crossref.tar.gz");
    let padding = "lorem ipsum ".repeat(110_000); // ~1.3 MiB
    let big = member(&[(
        "10.9999/citer",
        vec![json!({"DOI": "10.1234/a", "unstructured": padding})],
    )]);
    let small = member(&[("10.9999/other", vec![json!({"DOI": "10.1234/a"})])]);
    write_tar_gz(&input, &[("0.json", small), ("1.json", big)])?;
    let run = Run::new(
        &input,
        &bundle,
        &tmp.path().join("out"),
        &tmp.path().join("parts"),
    )
    .args(&["--max-entry-mb", "1"]);
    run.fail_with("above the 1048576-byte member limit")?;
    assert!(
        tmp.path()
            .join("parts")
            .join("datacite-extraction.checkpoint")
            .exists(),
        "a failed run keeps its checkpoint for resume"
    );
    Ok(())
}

#[test]
fn many_matches_in_one_member_flow_through_bounded_batches() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bundle = build_bundle(
        tmp.path(),
        &[
            ("10.1234/a", Some("https://example.org/a")),
            ("10.1234/b", None),
        ],
    )?;
    let works: Vec<(String, Vec<Value>)> = (0..3000)
        .map(|i| {
            let padding = format!("{i}: {}", "lorem ipsum ".repeat(40));
            (
                format!("10.9999/citer-{i:04}"),
                vec![
                    json!({"DOI": "10.1234/a", "doi-asserted-by": "crossref", "unstructured": padding}),
                    json!({"URL": "https://example.org/a"}),
                    json!({"unstructured": format!("see doi:10.1234/b and {padding}")}),
                ],
            )
        })
        .collect();
    let borrowed: Vec<(&str, Vec<Value>)> = works
        .iter()
        .map(|(doi, refs)| (doi.as_str(), refs.clone()))
        .collect();
    let input = tmp.path().join("crossref.json");
    fs::write(&input, member(&borrowed))?;
    let output_dir = tmp.path().join("out");
    let run = Run::new(&input, &bundle, &output_dir, &tmp.path().join("parts")).args(&[
        "--result-batch-mb",
        "1",
        "--writer-memory-mb",
        "1",
        "--queue-batches",
        "2",
        "--partitions",
        "4",
        "--outputs",
        "valid,crossref,mined",
    ]);
    let output = run.output()?;
    let log = transcript(&output);
    ensure!(output.status.success(), "pipeline failed: {log}");
    let report = read_json(&run.report_path())?;

    assert_eq!(report["kind"], "datacite-extraction-report");
    assert_eq!(report["schema_version"], 2);
    assert_eq!(report["completed"], true);
    assert_eq!(report["files_processed"], 1);
    assert_eq!(report["fatal_source_errors"], 0);
    assert_eq!(report["identity_sha256"].as_str().unwrap().len(), 64);
    assert_eq!(stat(&report, "items"), 3000);
    assert_eq!(stat(&report, "references"), 9000);
    assert_eq!(stat(&report, "confirmed_matches"), 9000);
    assert_eq!(stat(&report, "doi_matches"), 6000);
    assert_eq!(stat(&report, "url_matches"), 3000);
    assert_eq!(stat(&report, "arxiv_matches"), 0);
    assert_eq!(report["aggregation"]["valid_count"], 2);
    assert_eq!(report["aggregation"]["total_citations"], 9000);
    assert_eq!(report["aggregation"]["crossref_citations"], 3000);
    assert_eq!(report["aggregation"]["mined_citations"], 6000);

    let valid = grouped(&output_dir.join("crossref_valid.jsonl"))?;
    assert_eq!(valid["10.1234/a"].len(), 6000);
    assert_eq!(valid["10.1234/b"].len(), 3000);
    let first = &valid["10.1234/a"][0];
    assert_eq!(first["doi"], "10.9999/citer-0000");
    assert_eq!(first["ref_index"], 0);
    assert_eq!(first["match_type"], "doi");
    assert_eq!(first["provenance"], "crossref");
    assert_eq!(first["reference"]["DOI"], "10.1234/a");
    assert_eq!(valid["10.1234/a"][1]["ref_index"], 1);
    assert_eq!(valid["10.1234/a"][1]["match_type"], "url");
    assert_eq!(
        grouped(&output_dir.join("crossref_crossref.jsonl"))?["10.1234/a"].len(),
        3000
    );
    assert!(!output_dir.join("crossref_failed.jsonl").exists());
    assert_eq!(
        read_jsonl(&output_dir.join("crossref_ambiguous_urls.jsonl"))?.len(),
        0
    );

    // Structured progress and phase events on the existing logger.
    for needle in [
        "event=resource_estimate ",
        "event=phase phase=extraction status=started ",
        "event=progress members=1 items=3000 references=9000 confirmed=9000 doi=6000 url=3000 arxiv=0 ",
        "event=phase phase=extraction status=completed files_processed=1 ",
        "event=phase phase=aggregation status=completed valid=2 ",
        "event=phase phase=report status=written ",
        "event=phase phase=pipeline status=completed ",
    ] {
        ensure!(log.contains(needle), "missing {needle:?} in:\n{log}");
    }
    // The benchmark subtracts the per-run fixed costs from each window, so
    // index loading and aggregation report their own durations.
    for phase in ["index_load", "aggregation"] {
        let prefix = format!("event=phase phase={phase} status=completed ");
        let line = log
            .lines()
            .find(|line| line.contains(&prefix))
            .with_context(|| format!("missing {prefix:?} in:\n{log}"))?;
        let seconds: f64 = line
            .split_whitespace()
            .find_map(|field| field.strip_prefix("elapsed_s="))
            .with_context(|| format!("no elapsed_s in {line:?}"))?
            .parse()?;
        ensure!(seconds >= 0.0, "negative duration in {line:?}");
    }
    assert!(!tmp.path().join("parts").exists(), "intermediates removed");
    Ok(())
}

/// One `event=checkpoint` log line.
///
/// Full mode parses members on `num_cpus - 2` workers in parallel, and a
/// worker's `Rows` batches and its `FileComplete` are separate channel
/// sends, so rows of members that have not completed yet can land before
/// the `FileComplete` that triggers a commit. Which commit flushes which
/// rows therefore varies between runs, and a commit that finds nothing
/// buffered records its members without writing a segment. The checkpoint
/// tests assert what does not vary: the trigger multiset, the per-member
/// accounting, and one segment per commit that had rows to flush.
#[derive(Debug)]
struct Checkpoint {
    trigger: String,
    members_committed: u64,
    /// Writer bytes buffered when the commit flushed.
    flushed_bytes: u64,
    /// Rows written to partition segments so far, cumulative.
    rows_written: u64,
}

impl Checkpoint {
    /// A commit writes a partition segment only when it had rows buffered.
    fn wrote_segment(&self) -> bool {
        self.flushed_bytes > 0
    }
}

/// Every checkpoint commit in a log, in commit order.
fn checkpoint_events(log: &str) -> Vec<Checkpoint> {
    log.lines()
        .filter_map(|line| {
            let fields: BTreeMap<&str, &str> = line
                .split_once("event=checkpoint ")?
                .1
                .split(' ')
                .filter_map(|pair| pair.split_once('='))
                .collect();
            let count = |name: &str| -> Option<u64> { fields.get(name)?.parse().ok() };
            Some(Checkpoint {
                trigger: fields.get("trigger")?.to_string(),
                members_committed: count("members_committed")?,
                flushed_bytes: count("flushed_bytes")?,
                rows_written: count("rows_written")?,
            })
        })
        .collect()
}

/// Sorted `(trigger, members_committed)` pairs: the commit multiset, which
/// does not depend on the order the workers finished in.
fn triggers(events: &[Checkpoint]) -> Vec<(&str, u64)> {
    let mut pairs: Vec<(&str, u64)> = events
        .iter()
        .map(|event| (event.trigger.as_str(), event.members_committed))
        .collect();
    pairs.sort();
    pairs
}

/// Member filenames recorded in `<temp_dir>/datacite-extraction.checkpoint`,
/// sorted because parallel workers complete members in any order.
fn checkpoint_member_files(temp_dir: &Path) -> Result<Vec<String>> {
    let mut names: Vec<String> = read_jsonl(&temp_dir.join("datacite-extraction.checkpoint"))?
        .into_iter()
        .filter(|record| record["record"] == "member")
        .map(|record| {
            record["filename"]
                .as_str()
                .map(str::to_owned)
                .with_context(|| format!("member record without a filename: {record}"))
        })
        .collect::<Result<_>>()?;
    names.sort();
    Ok(names)
}

/// Members of 600 rows each, about 0.75 MiB apiece as the writer accounts
/// them (string capacities plus column slots); four together are 2.9 MiB.
fn heavy_members(dir: &Path, count: usize) -> Result<()> {
    fs::create_dir_all(dir)?;
    for i in 0..count {
        let padding = "lorem ipsum ".repeat(80);
        let works: Vec<(String, Vec<Value>)> = (0..600)
            .map(|j| {
                (
                    format!("10.9999/citer-{i}-{j:03}"),
                    vec![json!({"DOI": "10.1234/a", "unstructured": padding})],
                )
            })
            .collect();
        let borrowed: Vec<(&str, Vec<Value>)> = works
            .iter()
            .map(|(doi, refs)| (doi.as_str(), refs.clone()))
            .collect();
        fs::write(dir.join(format!("{i}.json")), member(&borrowed))?;
    }
    Ok(())
}

#[test]
fn small_writer_budget_commits_by_bytes_before_the_member_interval() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bundle = build_bundle(tmp.path(), &[("10.1234/a", None)])?;
    let input = tmp.path().join("crossref");
    heavy_members(&input, 4)?;
    let parts = tmp.path().join("parts");
    // A 4 MiB budget commits at 2 MiB. The four members together are 2.9 MiB
    // as the writer accounts them: past the threshold, so a `bytes` commit is
    // certain once any member is complete, yet under the budget, so the
    // writer's own eviction (which empties the partition without a
    // checkpoint) can never pre-empt it. With a budget the rows exceed, an
    // ordering that delivers every `Rows` batch before any `FileComplete`
    // evicts instead, and only the final commit is ever observed.
    let run = Run::new(&input, &bundle, &tmp.path().join("out"), &parts).args(&[
        "--writer-memory-mb",
        "4",
        "--checkpoint-interval",
        "1000",
        "--partitions",
        "1",
        "--keep-intermediates",
    ]);
    let output = run.output()?;
    let log = transcript(&output);
    ensure!(output.status.success(), "{log}");
    ensure!(
        log.contains("commit_bytes=2097152 checkpoint_members=1000 "),
        "{log}"
    );
    let events = checkpoint_events(&log);
    let by_bytes: Vec<&Checkpoint> = events.iter().filter(|e| e.trigger == "bytes").collect();
    ensure!(!by_bytes.is_empty(), "no byte-triggered commit in:\n{log}");
    ensure!(
        by_bytes.iter().all(|e| e.flushed_bytes >= 2 * 1024 * 1024),
        "a bytes commit flushes at least the commit threshold:\n{log}"
    );
    ensure!(
        events.iter().all(|e| e.trigger != "members"),
        "the 1000-member interval must not fire for 4 members:\n{log}"
    );
    ensure!(
        events.iter().map(|e| e.members_committed).sum::<u64>() == 4,
        "every member is committed exactly once:\n{log}"
    );
    // No commit records all four members: a worker's rows precede its
    // `FileComplete`, so every row is buffered before the last one arrives
    // and the threshold was crossed at an earlier event with at least one
    // member pending, which committed then.
    ensure!(
        events.iter().all(|e| e.members_committed < 4),
        "commits happened before the end of the run:\n{log}"
    );
    ensure!(
        events.last().is_some_and(|e| e.rows_written == 2400),
        "every row is durable by the final commit:\n{log}"
    );
    // One segment per commit that had rows to flush. When every row arrived
    // before the bytes commit, the final commit finds nothing buffered and
    // writes none, so the count is one or two rather than fixed.
    let segments = count_files(&parts.join("partitions").join("h0000"), ".parquet");
    ensure!(
        segments == events.iter().filter(|e| e.wrote_segment()).count() && segments >= 1,
        "segments={segments} for commits:\n{log}"
    );
    assert_eq!(
        checkpoint_member_files(&parts)?,
        ["0.json", "1.json", "2.json", "3.json"]
    );
    let report = read_json(&run.report_path())?;
    assert_eq!(report["files_processed"], 4);
    assert_eq!(stat(&report, "confirmed_matches"), 2400);
    assert_eq!(report["aggregation"]["total_citations"], 2400);
    Ok(())
}

#[test]
fn explicit_checkpoint_interval_commits_by_member_count() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bundle = build_bundle(tmp.path(), &[("10.1234/a", None)])?;
    let input = tmp.path().join("crossref");
    fs::create_dir_all(&input)?;
    for i in 0..5 {
        let citer = format!("10.9999/citer-{i}");
        fs::write(
            input.join(format!("{i}.json")),
            member(&[(citer.as_str(), vec![json!({"DOI": "10.1234/a"})])]),
        )?;
    }
    let parts = tmp.path().join("parts");
    let run = Run::new(&input, &bundle, &tmp.path().join("out"), &parts).args(&[
        "--writer-memory-mb",
        "64",
        "--checkpoint-interval",
        "2",
        "--partitions",
        "1",
        "--keep-intermediates",
    ]);
    let output = run.output()?;
    let log = transcript(&output);
    ensure!(output.status.success(), "{log}");
    let events = checkpoint_events(&log);
    // The commit multiset is fixed: every second `FileComplete` commits by
    // count, and the fifth member is committed at the end.
    assert_eq!(
        triggers(&events),
        vec![("final", 1), ("members", 2), ("members", 2)],
        "two interval commits and the final one:\n{log}"
    );
    // The segment count is not fixed: `Rows` batches of members that have
    // not completed yet may land before the `FileComplete` that commits, so
    // one commit can flush the rows of several members and a later commit
    // finds nothing to write. Nothing approaches the 64 MiB budget, so no
    // segment comes from eviction: the segments are exactly the commits that
    // had rows buffered, between one (every row arrived before the first
    // commit) and three.
    let segments = count_files(&parts.join("partitions").join("h0000"), ".parquet");
    ensure!(
        segments == events.iter().filter(|e| e.wrote_segment()).count()
            && (1..=3).contains(&segments),
        "segments={segments} for commits:\n{log}"
    );
    ensure!(
        events.last().is_some_and(|e| e.rows_written == 5),
        "every row is durable by the final commit:\n{log}"
    );
    assert_eq!(
        checkpoint_member_files(&parts)?,
        ["0.json", "1.json", "2.json", "3.json", "4.json"]
    );
    ensure!(
        log.contains("commit_bytes=33554432 checkpoint_members=2 "),
        "{log}"
    );
    // Zero would never commit by count; it is only meaningful in arXiv mode.
    Run::new(
        &input,
        &bundle,
        &tmp.path().join("out0"),
        &tmp.path().join("parts0"),
    )
    .args(&["--checkpoint-interval", "0"])
    .fail_with("--checkpoint-interval must be positive in DataCite mode")?;
    assert!(!tmp.path().join("parts0").exists());
    Ok(())
}

#[test]
fn counters_cover_ambiguity_failed_arxiv_self_citations_and_unmatched() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bundle = build_bundle(
        tmp.path(),
        &[
            ("10.1234/a", Some("https://example.org/shared")),
            ("10.1234/b", Some("https://example.org/shared")),
            ("10.48550/arxiv.2403.03542", None),
        ],
    )?;
    let input = tmp.path().join("crossref.json");
    fs::write(
        &input,
        member(&[
            (
                "10.9999/citer",
                vec![
                    json!({"URL": "https://example.org/shared", "unstructured": "https://example.org/shared again"}),
                    json!({"DOI": "10.48550/arXiv.2403.99999", "doi-asserted-by": "publisher"}),
                    json!({"unstructured": "arXiv:2403.03542 and doi:10.7777/absent and https://nowhere.example/x"}),
                    json!("not an object"),
                    json!({"DOI": "10.9999/citer", "doi-asserted-by": "publisher"}),
                ],
            ),
            ("10.1234/a", vec![json!({"DOI": "10.1234/A"})]),
        ]),
    )?;
    let output_dir = tmp.path().join("out");
    let run = Run::new(&input, &bundle, &output_dir, &tmp.path().join("parts"))
        .args(&["--partitions", "2"]);
    let report = run.succeed()?;
    assert_eq!(stat(&report, "items"), 2);
    assert_eq!(stat(&report, "references"), 6);
    assert_eq!(stat(&report, "malformed_references"), 1);
    assert_eq!(
        stat(&report, "ambiguous_urls"),
        1,
        "one normalized shared URL"
    );
    assert_eq!(stat(&report, "invalid_arxiv"), 1);
    // 10.7777/absent, and 10.9999/citer citing itself: the citer is not a
    // registered target, so that self-citation is an unmatched DOI rather
    // than a filtered one.
    assert_eq!(stat(&report, "unmatched_dois"), 2);
    assert_eq!(stat(&report, "unmatched_urls"), 1, "nowhere.example");
    assert_eq!(
        stat(&report, "confirmed_matches"),
        2,
        "arXiv text match + self-cite"
    );
    assert_eq!(
        stat(&report, "filtered_self_citations"),
        1,
        "10.1234/a citing itself"
    );
    assert_eq!(stat(&report, "arxiv_matches"), 1);
    assert_eq!(stat(&report, "doi_matches"), 1);
    assert_eq!(report["aggregation"]["valid_count"], 1);
    assert_eq!(report["aggregation"]["failed_count"], 1);

    let failed = grouped(&output_dir.join("crossref_failed.jsonl"))?;
    let entry = &failed["10.48550/arxiv.2403.99999"];
    assert_eq!(entry.len(), 1);
    assert_eq!(entry[0]["doi"], "10.9999/citer");
    assert_eq!(entry[0]["ref_index"], 1);
    assert_eq!(entry[0]["match_type"], "arxiv");
    assert_eq!(
        entry[0]["provenance"], "publisher",
        "asserted invalid arXiv DOI"
    );
    assert_eq!(entry[0]["raw_match"], "10.48550/arXiv.2403.99999");
    assert_eq!(entry[0]["reference"]["doi-asserted-by"], "publisher");

    let valid = grouped(&output_dir.join("crossref_valid.jsonl"))?;
    assert_eq!(valid.len(), 1);
    assert_eq!(valid["10.48550/arxiv.2403.03542"][0]["ref_index"], 2);
    assert_eq!(
        valid["10.48550/arxiv.2403.03542"][0]["raw_match"],
        "arXiv:2403.03542"
    );

    let ambiguous = read_jsonl(&output_dir.join("crossref_ambiguous_urls.jsonl"))?;
    assert_eq!(ambiguous.len(), 1);
    assert_eq!(ambiguous[0]["citing_doi"], "10.9999/citer");
    assert_eq!(ambiguous[0]["ref_index"], 0);
    assert_eq!(ambiguous[0]["url"], "https://example.org/shared");
    assert_eq!(ambiguous[0]["raw_match"], "https://example.org/shared");
    assert_eq!(
        ambiguous[0]["candidate_dois"],
        json!(["10.1234/a", "10.1234/b"])
    );
    assert_eq!(
        ambiguous[0]["reference"]["URL"],
        "https://example.org/shared"
    );
    Ok(())
}

#[test]
fn provenance_filter_drops_rows_and_is_recorded() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bundle = build_bundle(tmp.path(), &[("10.1234/a", None), ("10.1234/b", None)])?;
    let input = tmp.path().join("crossref.json");
    fs::write(
        &input,
        member(&[(
            "10.9999/citer",
            vec![
                json!({"DOI": "10.1234/a", "doi-asserted-by": "publisher"}),
                json!({"unstructured": "doi:10.1234/b"}),
            ],
        )]),
    )?;
    let output_dir = tmp.path().join("out");
    let run = Run::new(&input, &bundle, &output_dir, &tmp.path().join("parts")).args(&[
        "--provenance",
        "Publisher",
        "--partitions",
        "1",
    ]);
    let report = run.succeed()?;
    assert_eq!(stat(&report, "confirmed_matches"), 2);
    assert_eq!(stat(&report, "filtered_provenance"), 1);
    let valid = grouped(&output_dir.join("crossref_valid.jsonl"))?;
    assert_eq!(valid.keys().collect::<Vec<_>>(), vec!["10.1234/a"]);
    let bad_out = tmp.path().join("out2");
    Run::new(&input, &bundle, &bad_out, &tmp.path().join("parts2"))
        .args(&["--provenance", "bogus"])
        .fail_with("unknown provenance \"bogus\"")?;
    assert!(!bad_out.exists(), "rejected before any mutation");
    Ok(())
}

#[test]
fn a_reference_nested_past_the_parser_depth_limit_is_malformed_not_fatal() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bundle = build_bundle(tmp.path(), &[("10.1234/a", None)])?;
    // Valid JSON nested 201 levels deep: past serde_json's recursion limit
    // for a `Value`, but the member itself is well-formed.
    let deep = format!(
        r#"{{"key":"r0","deep":{}{}}}"#,
        "[".repeat(200),
        "]".repeat(200)
    );
    let bytes = format!(
        r#"{{"items":[{{"DOI":"10.9999/citer","reference":[{deep},{{"key":"r1","DOI":"10.1234/a"}}]}}]}}"#
    )
    .into_bytes();
    let archive = tmp.path().join("crossref.tar.gz");
    write_tar_gz(&archive, &[("0.json", bytes)])?;
    let output_dir = tmp.path().join("out");
    let run = Run::new(&archive, &bundle, &output_dir, &tmp.path().join("parts"))
        .args(&["--partitions", "1"]);
    let report = run.succeed()?;
    assert_eq!(report["files_processed"], 1);
    assert_eq!(stat(&report, "references"), 2);
    assert_eq!(stat(&report, "malformed_references"), 1);
    assert_eq!(stat(&report, "confirmed_matches"), 1);
    let valid = grouped(&output_dir.join("crossref_valid.jsonl"))?;
    assert_eq!(valid["10.1234/a"].len(), 1);
    assert_eq!(valid["10.1234/a"][0]["ref_index"], 1);
    Ok(())
}

#[test]
fn non_string_doi_fields_count_as_malformed_without_aborting() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bundle = build_bundle(tmp.path(), &[("10.1234/a", None)])?;
    let input = tmp.path().join("crossref.json");
    let items = json!({"items": [
        {"DOI": 123, "reference": [{"DOI": "10.1234/a"}]},
        {"DOI": "10.9999/citer", "reference": [{"DOI": 123, "unstructured": "doi:10.1234/a"}]},
        {"DOI": "10.9999/other", "reference": "not an array"},
        {"DOI": "not a doi", "reference": []},
        "not an item",
        {"reference": [{"DOI": "10.1234/a"}]}
    ]});
    fs::write(&input, serde_json::to_vec(&items)?)?;
    let output_dir = tmp.path().join("out");
    let run = Run::new(&input, &bundle, &output_dir, &tmp.path().join("parts"))
        .args(&["--partitions", "1"]);
    let report = run.succeed()?;
    assert_eq!(stat(&report, "items"), 6);
    assert_eq!(
        stat(&report, "references"),
        1,
        "only the usable work's reference"
    );
    assert_eq!(
        stat(&report, "malformed_fields"),
        6,
        "numeric work DOI, numeric reference DOI, string reference list, invalid DOI, non-object item, missing DOI"
    );
    assert_eq!(stat(&report, "confirmed_matches"), 1);
    let valid = grouped(&output_dir.join("crossref_valid.jsonl"))?;
    assert_eq!(valid["10.1234/a"].len(), 1);
    assert_eq!(valid["10.1234/a"][0]["doi"], "10.9999/citer");
    assert_eq!(valid["10.1234/a"][0]["provenance"], "mined");
    assert_eq!(valid["10.1234/a"][0]["match_type"], "doi");
    assert_eq!(valid["10.1234/a"][0]["raw_match"], "10.1234/a");
    Ok(())
}

#[test]
fn oversized_url_candidate_lists_fail_the_lookup_budget() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let dois: Vec<String> = (0..30_000).map(|i| format!("10.5555/s{i:06}")).collect();
    let targets: Vec<Target<'_>> = dois
        .iter()
        .map(|doi| (doi.as_str(), Some("https://example.org/shared")))
        .collect();
    let bundle = build_bundle(tmp.path(), &targets)?;
    let input = tmp.path().join("crossref.json");
    fs::write(
        &input,
        member(&[(
            "10.9999/citer",
            vec![json!({"URL": "https://example.org/shared"})],
        )]),
    )?;
    let over = Run::new(
        &input,
        &bundle,
        &tmp.path().join("out"),
        &tmp.path().join("parts"),
    )
    .args(&["--lookup-memory-mb", "1", "--partitions", "1"]);
    over.fail_with("URL candidate memory budget exceeded")?;

    let output_dir = tmp.path().join("out2");
    let under = Run::new(&input, &bundle, &output_dir, &tmp.path().join("parts2")).args(&[
        "--lookup-memory-mb",
        "2",
        "--partitions",
        "1",
    ]);
    let report = under.succeed()?;
    assert_eq!(stat(&report, "ambiguous_urls"), 1);
    let ambiguous = read_jsonl(&output_dir.join("crossref_ambiguous_urls.jsonl"))?;
    assert_eq!(
        ambiguous[0]["candidate_dois"].as_array().unwrap().len(),
        30_000
    );
    Ok(())
}

#[test]
fn writer_failure_with_full_queues_exits_nonzero_without_deadlock() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bundle = build_bundle(tmp.path(), &[("10.1234/a", None)])?;
    let input = tmp.path().join("crossref.tar.gz");
    // Every member yields one row whose reference JSON alone exceeds the
    // 1 MiB writer budget, so the writer fails on its first batch while the
    // workers and reader still hold members and blocked sends.
    let padding = "lorem ipsum ".repeat(100_000);
    let members: Vec<(&str, Vec<u8>)> = (0..24)
        .map(|i| {
            let name: &'static str = Box::leak(format!("{i}.json").into_boxed_str());
            let works = vec![(
                "10.9999/citer",
                vec![json!({"DOI": "10.1234/a", "unstructured": padding})],
            )];
            (name, member(&works))
        })
        .collect();
    write_tar_gz(&input, &members)?;
    let run = Run::new(
        &input,
        &bundle,
        &tmp.path().join("out"),
        &tmp.path().join("parts"),
    )
    .args(&[
        "--writer-memory-mb",
        "1",
        "--result-batch-mb",
        "8",
        "--max-entry-mb",
        "4",
        "--queue-batches",
        "1",
        "--partitions",
        "1",
    ]);
    let mut child = run
        .command()
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if started.elapsed() > Duration::from_secs(120) {
            child.kill()?;
            anyhow::bail!("pipeline did not shut down after the writer failed");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let mut stderr = String::new();
    std::io::Read::read_to_string(child.stderr.as_mut().unwrap(), &mut stderr)?;
    ensure!(!status.success(), "pipeline must fail");
    ensure!(
        stderr.contains("extraction writer failed") && stderr.contains("memory budget"),
        "unexpected stderr:\n{stderr}"
    );
    ensure!(!run.report_path().exists());
    Ok(())
}

#[test]
fn mixed_index_arguments_are_rejected_before_any_mutation() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bundle = build_bundle(tmp.path(), &[("10.1234/a", None)])?;
    let input = tmp.path().join("crossref.json");
    fs::write(&input, member(&[("10.9999/citer", vec![])]))?;
    let fst = tmp.path().join("arxiv.fst");
    fs::write(&fst, b"")?;
    let output_dir = tmp.path().join("out");
    let temp_dir = tmp.path().join("parts");
    for extra in [
        ["--arxiv-fst", fst.to_str().unwrap()],
        ["--arxiv-records", fst.to_str().unwrap()],
    ] {
        Run::new(&input, &bundle, &output_dir, &temp_dir)
            .args(&extra)
            .fail_with("cannot be combined with --arxiv-fst or --arxiv-records")?;
        assert!(
            !output_dir.exists() && !temp_dir.exists(),
            "nothing was created"
        );
    }
    Run::new(&input, &bundle, &output_dir, &temp_dir)
        .args(&["--partitions", "12"])
        .fail_with("--partitions must be a positive power of two")?;
    Run::new(&input, &bundle, &output_dir, &temp_dir)
        .args(&["--aggregation-memory-mb", "1", "--writer-memory-mb", "2"])
        .fail_with("--aggregation-memory-mb (1) must be at least --writer-memory-mb (2)")?;
    Run::new(&input, &bundle, &output_dir, &temp_dir)
        .args(&["--partitions-dir", temp_dir.to_str().unwrap()])
        .fail_with("--partitions-dir is not supported in DataCite mode")?;
    assert!(
        !output_dir.exists() && !temp_dir.exists(),
        "nothing was created"
    );

    // A legacy invocation without any index source is still rejected, and
    // still says so in the legacy terms.
    let output = Command::new(env!("CARGO_BIN_EXE_crossref-citation-extraction"))
        .args(["pipeline", "--input", input.to_str().unwrap()])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--arxiv-records or --arxiv-fst"));
    Ok(())
}

#[test]
fn run_identity_file_is_validated_and_hashed_canonically() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bundle = build_bundle(tmp.path(), &[("10.1234/a", None)])?;
    let input = tmp.path().join("crossref.json");
    fs::write(
        &input,
        member(&[("10.9999/citer", vec![json!({"DOI": "10.1234/a"})])]),
    )?;
    let bundle_sha = support::sha256_hex(&fs::read(bundle.join("_INDEX_COMPLETE.json"))?);
    let identity_path = tmp.path().join("identity.json");
    // Keys deliberately unsorted and pretty-printed: the digest must not
    // depend on the file's own layout.
    fs::write(
        &identity_path,
        format!(
            "{{\n  \"partitions\": 4,\n  \"extraction_mode\": \"datacite\",\n  \"input_sha256\": \"abc\",\n  \"partition_algorithm\": \"fnv1a64-v1\",\n  \"bundle_manifest_sha256\": \"{bundle_sha}\",\n  \"nested\": {{\"z\": 1, \"a\": [true, null]}}\n}}\n"
        ),
    )?;
    let canonical = format!(
        "{{\"bundle_manifest_sha256\":\"{bundle_sha}\",\"extraction_mode\":\"datacite\",\"input_sha256\":\"abc\",\"nested\":{{\"a\":[true,null],\"z\":1}},\"partition_algorithm\":\"fnv1a64-v1\",\"partitions\":4}}"
    );
    let expected = support::sha256_hex(canonical.as_bytes());
    let output_dir = tmp.path().join("out");
    let run = Run::new(&input, &bundle, &output_dir, &tmp.path().join("parts")).args(&[
        "--partitions",
        "4",
        "--run-identity",
        identity_path.to_str().unwrap(),
    ]);
    let report = run.succeed()?;
    assert_eq!(report["identity_sha256"], expected);

    // Each validated field is checked before anything is created.
    for (field, value) in [
        ("partitions", json!(8)),
        ("bundle_manifest_sha256", json!("0".repeat(64))),
        ("extraction_mode", json!("arxiv")),
        ("partition_algorithm", json!("fnv1a64-v2")),
    ] {
        let mut identity: Value = serde_json::from_slice(&fs::read(&identity_path)?)?;
        identity[field] = value;
        let bad_path = tmp.path().join(format!("identity-{field}.json"));
        fs::write(&bad_path, serde_json::to_vec(&identity)?)?;
        let temp_dir = tmp.path().join(format!("parts-{field}"));
        let out_dir = tmp.path().join(format!("out-{field}"));
        Run::new(&input, &bundle, &out_dir, &temp_dir)
            .args(&[
                "--partitions",
                "4",
                "--run-identity",
                bad_path.to_str().unwrap(),
            ])
            .fail_with(&format!(
                "--run-identity {}: {field} is",
                bad_path.display()
            ))?;
        assert!(
            !temp_dir.exists() && !out_dir.exists(),
            "{field}: nothing was created"
        );
    }
    Ok(())
}

#[test]
fn resume_replays_uncommitted_members_without_duplicating_output() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bundle = build_bundle(
        tmp.path(),
        &[
            ("10.1234/a", Some("https://example.org/shared")),
            ("10.1234/b", Some("https://example.org/shared")),
            ("10.1234/c", None),
        ],
    )?;
    let input = tmp.path().join("crossref");
    fs::create_dir_all(&input)?;
    for i in 0..3 {
        let works: Vec<(String, Vec<Value>)> = (0..50)
            .map(|j| {
                (
                    format!("10.9999/citer-{i}-{j:02}"),
                    vec![
                        json!({"DOI": "10.1234/a", "doi-asserted-by": "publisher"}),
                        json!({"URL": "https://example.org/shared"}),
                        json!({"unstructured": "doi:10.1234/c or arXiv:2403.00001"}),
                    ],
                )
            })
            .collect();
        let borrowed: Vec<(&str, Vec<Value>)> = works
            .iter()
            .map(|(doi, refs)| (doi.as_str(), refs.clone()))
            .collect();
        fs::write(input.join(format!("{i}.json")), member(&borrowed))?;
    }
    let outputs = [
        "crossref_valid.jsonl",
        "crossref_failed.jsonl",
        "crossref_publisher.jsonl",
        "crossref_crossref.jsonl",
        "crossref_mined.jsonl",
        "crossref_ambiguous_urls.jsonl",
    ];

    // A clean run is the reference.
    let clean_out = tmp.path().join("clean");
    let clean = Run::new(&input, &bundle, &clean_out, &tmp.path().join("clean-parts"))
        .args(&["--partitions", "2"])
        .succeed()?;
    assert_eq!(clean["files_processed"], 3);
    assert_eq!(stat(&clean, "confirmed_matches"), 300);
    assert_eq!(stat(&clean, "invalid_arxiv"), 150);
    assert_eq!(stat(&clean, "ambiguous_urls"), 150);

    // A kept run, then a simulated crash: the last member's rows are on
    // disk but its checkpoint record is gone, and a torn record follows.
    let resumed_out = tmp.path().join("resumed");
    let parts = tmp.path().join("resumed-parts");
    let kept = Run::new(&input, &bundle, &resumed_out, &parts)
        .args(&["--partitions", "2", "--keep-intermediates"])
        .succeed()?;
    assert_eq!(kept["files_processed"], 3);
    let checkpoint = parts.join("datacite-extraction.checkpoint");
    let lines: Vec<String> = fs::read_to_string(&checkpoint)?
        .lines()
        .filter(|line| !line.contains("\"2.json\"") && !line.contains("\"complete\""))
        .map(String::from)
        .collect();
    assert_eq!(lines.len(), 3, "header + two member records");
    let mut file = fs::File::create(&checkpoint)?;
    for line in &lines {
        writeln!(file, "{line}")?;
    }
    write!(file, "{{\"record\":\"member\",\"filename\":\"2.js")?;
    drop(file);
    for name in outputs {
        fs::remove_file(resumed_out.join(name))?;
    }
    fs::remove_file(resumed_out.join("extraction-report.json"))?;
    let segments_before = count_files(&parts.join("partitions"), ".parquet");
    let ambiguity_segments_before = count_files(&parts.join("ambiguities"), ".jsonl");
    assert!(segments_before > 0 && ambiguity_segments_before > 0);

    // The resumed run must reject a different selection, then replay 2.json.
    Run::new(&input, &bundle, &resumed_out, &parts)
        .args(&["--partitions", "2", "--resume", "--outputs", "valid"])
        .fail_with("different identity or selection")?;
    let resumed = Run::new(&input, &bundle, &resumed_out, &parts)
        .args(&["--partitions", "2", "--resume", "--keep-intermediates"])
        .succeed()?;
    assert_eq!(resumed["files_processed"], 3);
    assert_eq!(
        resumed["stats"], clean["stats"],
        "totals come from checkpoint records"
    );
    assert_eq!(resumed["aggregation"], clean["aggregation"]);
    assert!(
        count_files(&parts.join("partitions"), ".parquet") > segments_before,
        "the replayed member wrote rows again"
    );
    assert!(count_files(&parts.join("ambiguities"), ".jsonl") > ambiguity_segments_before);
    for name in outputs {
        assert_eq!(
            fs::read(clean_out.join(name))?,
            fs::read(resumed_out.join(name))?,
            "{name} differs after replay"
        );
    }
    // A resume after everything was durable skips extraction and reuses
    // the aggregation commits; a final non-kept run removes all of it.
    let again = Run::new(&input, &bundle, &resumed_out, &parts)
        .args(&["--partitions", "2", "--resume"])
        .output()?;
    let log = transcript(&again);
    ensure!(again.status.success(), "{log}");
    ensure!(
        log.contains(
            "event=phase phase=extraction status=skipped reason=checkpoint_complete members=3"
        ),
        "{log}"
    );
    ensure!(log.contains("reused_partitions=2"), "{log}");
    assert!(!parts.exists(), "intermediates removed after success");
    for name in outputs {
        assert_eq!(
            fs::read(clean_out.join(name))?,
            fs::read(resumed_out.join(name))?
        );
    }
    Ok(())
}
