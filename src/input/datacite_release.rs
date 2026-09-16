//! Verified reader for complete DataCite monthly data files.
//!
//! A monthly data file is cumulative: `dois/updated_YYYY-MM/` folders for
//! every month since the registry began, each holding `part_NNNN.jsonl.gz`
//! metadata records and a `<YYYY-MM>.csv.gz` state report listing the
//! Findable and Registered DOIs updated that month. A release generated on
//! the 1st also carries the next month's folder for the DOIs updated before
//! the snapshot; every listed folder is an input whatever its month.
//! `MANIFEST.json` (or the whitespace-separated text `MANIFEST`) lists every
//! file with its size and base64 SHA-256; `STATUS.json` records the release
//! month and whether generation is `Complete`.
//!
//! [`MonthlyRelease::open`] validates the manifest against the on-disk
//! layout without decoding any data; [`MonthlyRelease::scan`] streams every
//! listed file, hashing the compressed bytes as they are decoded, and fails
//! unless each file's size and digest match the manifest. Nothing here falls
//! back to partial coverage: a missing state report, an unlisted or unexpected
//! file, truncated gzip, a malformed record, an unknown state, or a record
//! whose two valid identifiers disagree is an error. The one tolerated data
//! defect is a DOI that is not syntactically valid (the registry spells a
//! handful with whitespace inside the suffix): such a row or record is
//! counted in [`SourceStats::invalid_dois`] and skipped whole, never
//! repaired, so it can neither be matched nor stop the build.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, ensure, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use flate2::bufread::MultiGzDecoder;
use log::{debug, info};
use serde::Deserialize;
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

use crate::common::identifiers::normalize_doi;

const STATUS_FILE: &str = "STATUS.json";
const MANIFEST_JSON: &str = "MANIFEST.json";
const MANIFEST_TEXT: &str = "MANIFEST";
const STATUS_COMPLETE: &str = "Complete";
const DOIS_DIR: &str = "dois";
const MONTH_DIR_PREFIX: &str = "updated_";
const METADATA_SUFFIX: &str = ".jsonl.gz";
const STATE_REPORT_SUFFIX: &str = ".csv.gz";
const STATE_REPORT_HEADER: [&str; 4] = ["doi", "state", "client_id", "updated"];
/// Buffer size for compressed reads and for decoded line/CSV reads.
const READ_BUFFER: usize = 256 * 1024;

/// Which release file a record came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordSource {
    /// A `part_NNNN.jsonl.gz` metadata record.
    Metadata,
    /// A `<YYYY-MM>.csv.gz` state-report row.
    StateReport,
}

/// One DOI as registered in the release, with its landing page when the
/// metadata carries a string `attributes.url`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryRecord {
    /// Canonical lowercase DOI (see [`normalize_doi`]).
    pub doi: String,
    /// The raw `attributes.url` string, exactly as written; callers
    /// normalize it. Always `None` for state-report rows.
    pub url: Option<String>,
    /// `attributes.url` was present but neither a string nor null.
    pub url_invalid: bool,
    pub source: RecordSource,
}

/// A manifest-listed input with its expected size and lowercase hex SHA-256.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceFile {
    /// Path relative to the release root, `/`-separated as listed.
    pub path: PathBuf,
    pub size: u64,
    pub sha256: String,
}

/// Counts from a completed scan. The record counts cover emitted records
/// only; a row or record skipped for an invalid DOI is counted once in
/// `invalid_dois` and nowhere else. URL counts cover metadata records only;
/// state-report rows carry no landing page.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SourceStats {
    /// Metadata records emitted with a valid DOI.
    pub metadata_records: u64,
    /// State-report rows emitted with a valid DOI.
    pub csv_records: u64,
    /// Metadata records whose `attributes.url` is missing or null.
    pub missing_urls: u64,
    /// Metadata records whose `attributes.url` is neither string nor null.
    pub invalid_urls: u64,
    /// State-report rows and metadata records skipped because their DOI
    /// (`doi` column, `id`, or `attributes.doi`) is not syntactically valid.
    /// Each occurrence counts, so one DOI listed in both sources counts twice.
    pub invalid_dois: u64,
}

/// A validated DataCite monthly data file.
#[derive(Debug, Clone)]
pub struct MonthlyRelease {
    pub root: PathBuf,
    /// The `STATUS.json` month, e.g. `2026-07`.
    pub release: String,
    /// Listed inputs sorted by relative path.
    pub files: Vec<SourceFile>,
    /// SHA-256 of the compact JSON `[[path,size,sha256_hex],...]` over `files`.
    pub inventory_sha256: String,
}

impl MonthlyRelease {
    /// Validate the release rooted at `root` without decoding any data.
    ///
    /// Requires a `Complete` `STATUS.json`; a manifest (`MANIFEST.json`, or
    /// the text `MANIFEST` only when the JSON form is absent); that every
    /// listed JSONL/CSV input sits at `dois/updated_YYYY-MM/<file>` (any
    /// calendar month, before or after the release month) with its manifest
    /// size; that every folder listing metadata parts also lists a state
    /// report; and that no unlisted JSONL/CSV file exists under `dois/`.
    /// Listed entries that are not JSONL/CSV inputs are ignored.
    pub fn open(root: &Path) -> Result<Self> {
        let release = read_status(root)?;
        let (manifest_path, entries) = read_manifest(root)?;

        let mut listed: BTreeMap<String, SourceFile> = BTreeMap::new();
        let mut folders: BTreeMap<&str, FolderContents> = BTreeMap::new();
        for entry in &entries {
            let Some(kind) = source_kind(&entry.filename) else {
                continue;
            };
            let context = || format!("{}: entry {:?}", manifest_path.display(), entry.filename);
            let month = listed_month(&entry.filename).with_context(context)?;
            let sha256 = decode_manifest_digest(&entry.sha256).with_context(context)?;
            ensure!(
                !listed.contains_key(&entry.filename),
                "{}: duplicate entry",
                context()
            );
            listed.insert(
                entry.filename.clone(),
                SourceFile {
                    path: PathBuf::from(&entry.filename),
                    size: entry.size,
                    sha256,
                },
            );
            let folder = folders.entry(month).or_default();
            match kind {
                SourceKind::Metadata => folder.metadata = true,
                SourceKind::StateReport => folder.state_report = true,
            }
        }
        ensure!(
            !listed.is_empty(),
            "{}: lists no {DOIS_DIR}/ JSONL or CSV inputs",
            manifest_path.display()
        );
        for (month, contents) in &folders {
            ensure!(
                !contents.metadata || contents.state_report,
                "{}: {DOIS_DIR}/{MONTH_DIR_PREFIX}{month} lists JSONL metadata but no \
                 .csv.gz state report; refusing JSONL-only coverage",
                manifest_path.display()
            );
        }
        for file in listed.values() {
            let path = root.join(&file.path);
            let metadata = fs::metadata(&path).with_context(|| {
                format!("listed input is missing or unreadable: {}", path.display())
            })?;
            ensure!(
                metadata.is_file(),
                "listed input is not a file: {}",
                path.display()
            );
            ensure!(
                metadata.len() == file.size,
                "{}: file is {} bytes but the manifest says {}",
                path.display(),
                metadata.len(),
                file.size
            );
        }
        check_unlisted_sources(root, &listed)?;

        let files: Vec<SourceFile> = listed.into_values().collect();
        let inventory_sha256 = inventory_digest(&files);
        info!(
            "DataCite release {release} at {}: {} listed inputs, inventory sha256 {inventory_sha256}",
            root.display(),
            files.len()
        );
        Ok(Self {
            root: root.to_path_buf(),
            release,
            files,
            inventory_sha256,
        })
    }

    /// Stream every listed input in path order, handing each record to
    /// `emit`.
    ///
    /// Compressed bytes are hashed as the decoder consumes them, and after
    /// each file is read to EOF its size and SHA-256 must match the manifest
    /// or the scan fails. Records are emitted before their file is verified,
    /// so a caller that receives `Err` must discard everything it was
    /// handed. Metadata lines go through [`decode_metadata_record`];
    /// state-report rows must carry the documented header and a `findable`
    /// or `registered` state. A row or record whose DOI is not valid is
    /// counted in [`SourceStats::invalid_dois`] and skipped without being
    /// emitted; the file is still verified in full.
    pub fn scan(&self, emit: &mut dyn FnMut(RegistryRecord) -> Result<()>) -> Result<SourceStats> {
        let mut stats = SourceStats::default();
        for file in &self.files {
            let path = self.root.join(&file.path);
            let kind = source_kind(&file.path.to_string_lossy())
                .expect("open lists only metadata parts and state reports");
            debug!("Scanning {}", path.display());
            let reader = SourceReader::open(&path)?;
            let reader = match kind {
                SourceKind::Metadata => scan_metadata(reader, &path, emit, &mut stats)?,
                SourceKind::StateReport => scan_state_report(reader, &path, emit, &mut stats)?,
            };
            reader.verify(&path, file)?;
        }
        info!(
            "DataCite release {}: {} metadata records, {} state-report rows, \
             {} missing URLs, {} invalid URLs, {} invalid DOIs skipped",
            self.release,
            stats.metadata_records,
            stats.csv_records,
            stats.missing_urls,
            stats.invalid_urls,
            stats.invalid_dois
        );
        Ok(stats)
    }
}

#[derive(Deserialize)]
struct Status {
    month: String,
    status: String,
}

/// Read `STATUS.json` and return its month once the release is `Complete`.
fn read_status(root: &Path) -> Result<String> {
    let path = root.join(STATUS_FILE);
    let text =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let status: Status =
        serde_json::from_str(&text).with_context(|| format!("malformed {}", path.display()))?;
    ensure!(
        status.status == STATUS_COMPLETE,
        "{}: release {} status is {:?}, not {STATUS_COMPLETE:?}",
        path.display(),
        status.month,
        status.status
    );
    ensure!(
        parse_month(&status.month).is_some(),
        "{}: month {:?} is not YYYY-MM",
        path.display(),
        status.month
    );
    Ok(status.month)
}

/// Parse `YYYY-MM` into its `(year, month)` pair; `None` unless the month
/// is a calendar month.
fn parse_month(text: &str) -> Option<(u16, u8)> {
    let (year, month) = text.split_once('-')?;
    if year.len() != 4
        || month.len() != 2
        || !year
            .bytes()
            .chain(month.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let year = year.parse().ok()?;
    let month = month.parse().ok()?;
    (1..=12).contains(&month).then_some((year, month))
}

#[derive(Deserialize)]
struct ManifestEntry {
    filename: String,
    size: u64,
    sha256: String,
}

/// Read `MANIFEST.json`, or the text `MANIFEST` only when the JSON form is
/// absent. A present but unreadable or malformed `MANIFEST.json` is an error,
/// never a fallback.
fn read_manifest(root: &Path) -> Result<(PathBuf, Vec<ManifestEntry>)> {
    let json_path = root.join(MANIFEST_JSON);
    match fs::read_to_string(&json_path) {
        Ok(text) => {
            let entries = serde_json::from_str(&text)
                .with_context(|| format!("malformed {}", json_path.display()))?;
            Ok((json_path, entries))
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            let text_path = root.join(MANIFEST_TEXT);
            let text = fs::read_to_string(&text_path).with_context(|| {
                format!(
                    "failed to read {} ({} is absent)",
                    text_path.display(),
                    json_path.display()
                )
            })?;
            let entries = parse_text_manifest(&text)
                .with_context(|| format!("malformed {}", text_path.display()))?;
            Ok((text_path, entries))
        }
        Err(err) => Err(err).with_context(|| format!("failed to read {}", json_path.display())),
    }
}

/// Parse the documented text manifest: one `filename size sha256` per line,
/// fields separated by whitespace.
fn parse_text_manifest(text: &str) -> Result<Vec<ManifestEntry>> {
    let mut entries = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let number = index + 1;
        let mut fields = line.split_ascii_whitespace();
        let (Some(filename), Some(size), Some(sha256), None) =
            (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            bail!("line {number}: expected `filename size sha256`, found {line:?}");
        };
        let size = size
            .parse()
            .with_context(|| format!("line {number}: invalid size {size:?}"))?;
        entries.push(ManifestEntry {
            filename: filename.to_string(),
            size,
            sha256: sha256.to_string(),
        });
    }
    Ok(entries)
}

#[derive(Clone, Copy)]
enum SourceKind {
    Metadata,
    StateReport,
}

/// Classify a listed or on-disk file name; `None` means it is not an input.
fn source_kind(filename: &str) -> Option<SourceKind> {
    if filename.ends_with(METADATA_SUFFIX) {
        Some(SourceKind::Metadata)
    } else if filename.ends_with(STATE_REPORT_SUFFIX) {
        Some(SourceKind::StateReport)
    } else {
        None
    }
}

/// Which inputs a `dois/updated_YYYY-MM/` folder lists.
#[derive(Default)]
struct FolderContents {
    metadata: bool,
    state_report: bool,
}

/// Validate `dois/updated_YYYY-MM/<file>` and return the folder month.
/// Anything else (absolute paths, `..`, extra depth, backslashes, malformed
/// months) is rejected; the month is never compared with the release month.
fn listed_month(filename: &str) -> Result<&str> {
    let mut parts = filename.split('/');
    let (Some(top), Some(folder), Some(basename), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        bail!("expected {DOIS_DIR}/{MONTH_DIR_PREFIX}YYYY-MM/<file>");
    };
    ensure!(
        top == DOIS_DIR,
        "expected the first path component to be {DOIS_DIR:?}, found {top:?}"
    );
    let not_a_month = || anyhow!("folder {folder:?} is not {MONTH_DIR_PREFIX}YYYY-MM");
    let month = folder
        .strip_prefix(MONTH_DIR_PREFIX)
        .ok_or_else(not_a_month)?;
    parse_month(month).ok_or_else(not_a_month)?;
    ensure!(
        !basename.is_empty() && basename != "." && basename != ".." && !basename.contains('\\'),
        "invalid file name {basename:?}"
    );
    Ok(month)
}

/// Decode a manifest's base64 SHA-256 into lowercase hex.
fn decode_manifest_digest(encoded: &str) -> Result<String> {
    let bytes = BASE64
        .decode(encoded.trim())
        .with_context(|| format!("sha256 {encoded:?} is not valid base64"))?;
    ensure!(
        bytes.len() == 32,
        "sha256 {encoded:?} decodes to {} bytes, expected 32",
        bytes.len()
    );
    Ok(hex(&bytes))
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(out, "{byte:02x}").expect("writing to a String cannot fail");
    }
    out
}

/// Fail if any JSONL/CSV file under `dois/` is not in the manifest.
fn check_unlisted_sources(root: &Path, listed: &BTreeMap<String, SourceFile>) -> Result<()> {
    let dois = root.join(DOIS_DIR);
    for entry in WalkDir::new(&dois) {
        let entry = entry.with_context(|| format!("failed to walk {}", dois.display()))?;
        if !entry.file_type().is_file()
            || source_kind(&entry.file_name().to_string_lossy()).is_none()
        {
            continue;
        }
        let relative = entry
            .path()
            .strip_prefix(root)
            .with_context(|| format!("{} is outside {}", entry.path().display(), root.display()))?;
        let key = relative
            .components()
            .map(|component| component.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        ensure!(
            listed.contains_key(&key),
            "{}: source file {key} is not listed in the manifest",
            root.display()
        );
    }
    Ok(())
}

/// SHA-256 of the compact JSON `[[relative_path, size, sha256_hex], ...]`
/// over `files`, which are already sorted by path. No whitespace, no
/// trailing newline; Python reproduces these exact bytes.
fn inventory_digest(files: &[SourceFile]) -> String {
    let rows: Vec<(Cow<str>, u64, &str)> = files
        .iter()
        .map(|file| (file.path.to_string_lossy(), file.size, file.sha256.as_str()))
        .collect();
    let json = serde_json::to_string(&rows).expect("inventory rows are strings and integers");
    hex(&Sha256::digest(json.as_bytes()))
}

/// Streams one listed gzip file, hashing the compressed bytes as the
/// decoder pulls them so the manifest digest can be checked at EOF.
struct SourceReader {
    decoder: MultiGzDecoder<BufReader<HashingReader<File>>>,
}

impl SourceReader {
    fn open(path: &Path) -> Result<Self> {
        let file =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let compressed = BufReader::with_capacity(READ_BUFFER, HashingReader::new(file));
        Ok(Self {
            decoder: MultiGzDecoder::new(compressed),
        })
    }

    /// Consume anything left after the last gzip member, then check the
    /// file's size and digest against the manifest.
    fn verify(self, path: &Path, expected: &SourceFile) -> Result<()> {
        let mut compressed = self.decoder.into_inner();
        io::copy(&mut compressed, &mut io::sink())
            .with_context(|| format!("failed to read {}", path.display()))?;
        let (size, sha256) = compressed.into_inner().finish();
        ensure!(
            size == expected.size,
            "{}: read {size} bytes but the manifest says {}",
            path.display(),
            expected.size
        );
        ensure!(
            sha256 == expected.sha256,
            "{}: SHA-256 {sha256} does not match the manifest's {}",
            path.display(),
            expected.sha256
        );
        Ok(())
    }
}

impl Read for SourceReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.decoder.read(buf)
    }
}

/// Byte-counting SHA-256 pass-through over a reader.
struct HashingReader<R> {
    inner: R,
    hasher: Sha256,
    bytes: u64,
}

impl<R> HashingReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            bytes: 0,
        }
    }

    fn finish(self) -> (u64, String) {
        (self.bytes, hex(&self.hasher.finalize()))
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.hasher.update(&buf[..read]);
        self.bytes += read as u64;
        Ok(read)
    }
}

/// Decode every line of a metadata part, reusing one line buffer.
fn scan_metadata(
    reader: SourceReader,
    path: &Path,
    emit: &mut dyn FnMut(RegistryRecord) -> Result<()>,
    stats: &mut SourceStats,
) -> Result<SourceReader> {
    let mut lines = BufReader::with_capacity(READ_BUFFER, reader);
    let mut line = String::new();
    let mut number: u64 = 0;
    loop {
        line.clear();
        let read = lines.read_line(&mut line).with_context(|| {
            format!(
                "{}:{}: unreadable line (truncated or corrupt gzip?)",
                path.display(),
                number + 1
            )
        })?;
        if read == 0 {
            break;
        }
        number += 1;
        let text = line.trim_end_matches(['\r', '\n']);
        if text.trim().is_empty() {
            continue;
        }
        let decoded =
            decode_metadata_record(text).with_context(|| format!("{}:{number}", path.display()))?;
        match decoded {
            MetadataRecord::Valid(record) => {
                stats.metadata_records += 1;
                if record.url_invalid {
                    stats.invalid_urls += 1;
                } else if record.url.is_none() {
                    stats.missing_urls += 1;
                }
                emit(record)?;
            }
            MetadataRecord::InvalidDoi { field, value } => {
                stats.invalid_dois += 1;
                debug!(
                    "{}:{number}: skipping record with invalid DOI in {field}: {value:?}",
                    path.display()
                );
            }
        }
    }
    Ok(lines.into_inner())
}

/// Emit every row of a state report, reusing one CSV record buffer.
fn scan_state_report(
    reader: SourceReader,
    path: &Path,
    emit: &mut dyn FnMut(RegistryRecord) -> Result<()>,
    stats: &mut SourceStats,
) -> Result<SourceReader> {
    let mut csv = csv::ReaderBuilder::new()
        .buffer_capacity(READ_BUFFER)
        .from_reader(reader);
    let header = csv
        .headers()
        .with_context(|| format!("{}: unreadable CSV header", path.display()))?;
    ensure!(
        header.iter().eq(STATE_REPORT_HEADER),
        "{}: CSV header is {:?}, expected {STATE_REPORT_HEADER:?}",
        path.display(),
        header.iter().collect::<Vec<_>>()
    );
    let mut row = csv::StringRecord::new();
    loop {
        let read = csv
            .read_record(&mut row)
            .with_context(|| format!("{}: unreadable CSV row", path.display()))?;
        if !read {
            break;
        }
        let line = row.position().map_or(0, |position| position.line());
        let doi = row.get(0).unwrap_or_default();
        let state = row.get(1).unwrap_or_default();
        ensure!(
            matches!(state, "findable" | "registered"),
            "{}:{line}: state {state:?} is neither findable nor registered",
            path.display()
        );
        let Some(doi) = normalize_doi(doi) else {
            stats.invalid_dois += 1;
            debug!(
                "{}:{line}: skipping row with invalid DOI {doi:?}",
                path.display()
            );
            continue;
        };
        stats.csv_records += 1;
        emit(RegistryRecord {
            doi,
            url: None,
            url_invalid: false,
            source: RecordSource::StateReport,
        })?;
    }
    Ok(csv.into_inner())
}

/// The root of a metadata line: either a record or an API singleton
/// response wrapping one under `data`. Unknown fields (the bulk of the
/// metadata) are skipped without being materialized.
#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    data: Option<&'a RawValue>,
    #[serde(borrow)]
    id: Option<Cow<'a, str>>,
    #[serde(borrow)]
    attributes: Option<Attributes<'a>>,
}

/// The identifying fields of one DOI record.
#[derive(Deserialize)]
struct Record<'a> {
    #[serde(borrow)]
    id: Option<Cow<'a, str>>,
    #[serde(borrow)]
    attributes: Option<Attributes<'a>>,
}

/// The only attributes the registry needs; `url` stays raw so a non-string
/// value can be reported without failing the record.
#[derive(Deserialize)]
struct Attributes<'a> {
    #[serde(borrow)]
    doi: Option<Cow<'a, str>>,
    #[serde(borrow)]
    url: Option<&'a RawValue>,
}

/// The outcome of decoding one well-formed metadata line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataRecord {
    /// A record whose identifier is a valid DOI.
    Valid(RegistryRecord),
    /// A record whose `id` or `attributes.doi` is present but not a valid
    /// DOI. The caller counts it and skips the record whole: the other
    /// identifier field is not consulted, whitespace is not repaired, and
    /// the landing page is not indexed.
    InvalidDoi {
        /// `"id"` or `"attributes.doi"`, whichever failed first.
        field: &'static str,
        /// The offending value exactly as written.
        value: String,
    },
}

/// Decode one JSONL metadata line.
///
/// Accepts unwrapped `id`/`attributes` records and singleton `data`
/// wrappers, and id-only rows. Every DOI is canonicalized with
/// [`normalize_doi`]. Malformed JSON, a non-object or non-singleton `data`
/// value, a non-string identifier, a record with neither `id` nor
/// `attributes.doi`, and two valid identifiers that disagree after
/// normalization are errors. A present identifier that is not a valid DOI
/// yields [`MetadataRecord::InvalidDoi`] instead, for the caller to count
/// and skip. A missing or null `attributes.url` yields `url: None`; any
/// other non-string value sets `url_invalid` and keeps the DOI.
pub fn decode_metadata_record(line: &str) -> Result<MetadataRecord> {
    let envelope: Envelope = serde_json::from_str(line).context("malformed JSON record")?;
    let record = match envelope.data {
        Some(data) => {
            ensure!(
                envelope.id.is_none() && envelope.attributes.is_none(),
                "record has both a data wrapper and top-level id/attributes"
            );
            serde_json::from_str::<Record>(data.get())
                .context("data wrapper does not hold a single record")?
        }
        None => Record {
            id: envelope.id,
            attributes: envelope.attributes,
        },
    };
    let (attribute_doi, url) = match record.attributes {
        Some(attributes) => (attributes.doi, attributes.url),
        None => (None, None),
    };

    // Validate each present identifier on its own before comparing them: an
    // invalid value is a skip, never a fallback to the other field.
    let mut from_id = None;
    if let Some(id) = record.id.as_deref() {
        match normalize_doi(id) {
            Some(doi) => from_id = Some(doi),
            None => {
                return Ok(MetadataRecord::InvalidDoi {
                    field: "id",
                    value: id.to_string(),
                })
            }
        }
    }
    let mut from_attributes = None;
    if let Some(doi) = attribute_doi.as_deref() {
        match normalize_doi(doi) {
            Some(normalized) => from_attributes = Some(normalized),
            None => {
                return Ok(MetadataRecord::InvalidDoi {
                    field: "attributes.doi",
                    value: doi.to_string(),
                })
            }
        }
    }
    let doi = match (from_id, from_attributes) {
        (Some(id), Some(attribute)) => {
            ensure!(
                id == attribute,
                "conflicting DOI identifiers: id {id:?} vs attributes.doi {attribute:?}"
            );
            id
        }
        (Some(doi), None) | (None, Some(doi)) => doi,
        (None, None) => bail!("record has neither id nor attributes.doi"),
    };

    // The raw value is syntactically valid JSON, so the only way this
    // fails is a non-string, non-null type.
    let (url, url_invalid) = match url {
        None => (None, false),
        Some(raw) => match serde_json::from_str::<Option<String>>(raw.get()) {
            Ok(url) => (url, false),
            Err(_) => (None, true),
        },
    };

    Ok(MetadataRecord::Valid(RegistryRecord {
        doi,
        url,
        url_invalid,
        source: RecordSource::Metadata,
    }))
}
